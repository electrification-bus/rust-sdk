//! The host's way in: received messages, link changes and failed commands.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use ebus_mqtt::{Command, Link};
use tokio::sync::mpsc::UnboundedSender;

/// Most devices remembered as having lost messages between two drains; past
/// it, every known device is resubscribed.
const MAX_DROPPED_DEVICES: usize = 4096;

/// One input for the driver.
#[derive(Debug)]
pub(crate) enum Inbound {
    Message {
        topic: String,
        payload: Bytes,
        retain: Option<bool>,
        /// Bytes counted against the cap.
        size: usize,
    },
    Link(Link),
    CommandFailed(Command),
    /// Messages were dropped; the dropped set is not empty.
    Wake,
}

/// Devices whose messages were dropped, by `{domain}/5/{id}`, with whether a
/// `$state` was among them.
#[derive(Debug, Default)]
pub(crate) struct Dropped {
    pub(crate) devices: BTreeMap<String, bool>,
    /// More devices than [`MAX_DROPPED_DEVICES`] lost messages.
    pub(crate) all: bool,
}

impl Dropped {
    /// Records the device `prefix`, keeping at most [`MAX_DROPPED_DEVICES`].
    fn add(&mut self, prefix: &str, is_state: bool) {
        let room = self.devices.len() < MAX_DROPPED_DEVICES;
        match self.devices.get_mut(prefix) {
            Some(s) => *s |= is_state,
            None if room => {
                self.devices.insert(prefix.to_owned(), is_state);
            }
            None => self.all = true,
        }
    }

    pub(crate) fn merge(&mut self, other: Dropped) {
        self.all |= other.all;
        for (prefix, is_state) in &other.devices {
            self.add(prefix, *is_state);
        }
    }
}

/// State the inbox shares with the driver.
#[derive(Debug)]
pub(crate) struct Shared {
    /// Bytes of queued messages.
    pub(crate) bytes: AtomicUsize,
    pub(crate) cap: usize,
    pub(crate) dropped: Mutex<Dropped>,
    /// Set when `dropped` gained an entry the driver has not taken.
    pub(crate) has_dropped: AtomicBool,
    /// Clean-session CONNACKs the host reported.
    pub(crate) connacks: AtomicU64,
    /// Clean-session link-ups the driver handled.
    pub(crate) link_ups: AtomicU64,
    /// Commands the driver had sent when it handled the last of them.
    pub(crate) boundary: AtomicU64,
    /// Times the driver recomputed tree completeness.
    #[cfg(test)]
    pub(crate) tree_scans: AtomicU64,
}

impl Shared {
    pub(crate) fn new(cap: usize) -> Self {
        Shared {
            bytes: AtomicUsize::new(0),
            cap,
            dropped: Mutex::new(Dropped::default()),
            has_dropped: AtomicBool::new(false),
            connacks: AtomicU64::new(0),
            link_ups: AtomicU64::new(0),
            boundary: AtomicU64::new(0),
            #[cfg(test)]
            tree_scans: AtomicU64::new(0),
        }
    }

    /// The driver is handling a clean-session link-up, after sending `sent`
    /// commands.
    pub(crate) fn link_up(&self, sent: u64) {
        self.boundary.store(sent, Ordering::Release);
        self.link_ups.fetch_add(1, Ordering::AcqRel);
    }

    /// True when the command the driver sent as number `n` (from 1) was
    /// queued before it handled the last clean-session CONNACK reported.
    pub(crate) fn before_link_up(&self, n: u64) -> bool {
        let connacks = self.connacks.load(Ordering::Acquire);
        let link_ups = self.link_ups.load(Ordering::Acquire);
        link_ups < connacks || n <= self.boundary.load(Ordering::Acquire)
    }

    /// The dropped set; a panic elsewhere while it was held does not lose it.
    pub(crate) fn lock_dropped(&self) -> MutexGuard<'_, Dropped> {
        self.dropped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Takes the dropped set, when anything was added since the last take.
    pub(crate) fn take_dropped(&self) -> Option<Dropped> {
        if !self.has_dropped.swap(false, Ordering::AcqRel) {
            return None;
        }
        Some(std::mem::take(&mut *self.lock_dropped()))
    }

    /// Releases the bytes of a message the driver received.
    pub(crate) fn release(&self, size: usize) {
        self.bytes.fetch_sub(size, Ordering::AcqRel);
    }
}

/// The host's handle for feeding the controller.
///
/// Every method is synchronous and returns without waiting for the driver, so
/// it is safe to call from a `std::thread` reader loop or from inside a tokio
/// task. A dropped message takes a lock for one map insert, and nothing else
/// does.
///
/// Forward to it:
///
/// 1. every CONNACK, through [`connected`](Self::connected), including one
///    for a client that was already connected when the controller attached
///    (`session_present: false`);
/// 2. every lost connection, through [`disconnected`](Self::disconnected);
/// 3. every received PUBLISH in the controller's domain, through
///    [`message`](Self::message);
/// 4. every command the host could not hand to its client, through
///    [`command_failed`](Self::command_failed).
///
/// The driver stops once every clone of the inbox is dropped.
#[derive(Clone)]
pub struct Inbox {
    pub(crate) tx: UnboundedSender<Inbound>,
    pub(crate) shared: Arc<Shared>,
}

impl fmt::Debug for Inbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inbox")
            .field("queued_bytes", &self.shared.bytes.load(Ordering::Relaxed))
            .field("cap", &self.shared.cap)
            .finish_non_exhaustive()
    }
}

impl Inbox {
    /// Queues a received message. Returns `false` when it was dropped: its
    /// topic and payload would take the queued bytes past
    /// [`AttachOptions::inbox_bytes`](crate::AttachOptions::inbox_bytes), or
    /// the driver has stopped. The controller resubscribes the device a
    /// dropped message addressed once the backlog drains, so the broker
    /// replays its retained messages. A message larger than the cap on its
    /// own is dropped with `reason=inboxMessageTooLarge` and resubscribes
    /// nothing, since a replay would be dropped again.
    ///
    /// An owned `String` topic moves in without a copy. `retain` is the MQTT
    /// retain bit, or `None` when the client does not report it.
    pub fn message(&self, topic: impl Into<String>, payload: Bytes, retain: Option<bool>) -> bool {
        let topic: String = topic.into();
        let size = topic.len().saturating_add(payload.len());
        let cap = self.shared.cap;
        if size > cap {
            log::warn!("reason=inboxMessageTooLarge,topic={topic},bytes={size},cap={cap}");
            return false;
        }
        let admitted = self
            .shared
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |b| {
                b.checked_add(size).filter(|n| *n <= cap)
            })
            .is_ok();
        if !admitted {
            self.note_dropped(&topic);
            return false;
        }
        let sent = self.tx.send(Inbound::Message {
            topic,
            payload,
            retain,
            size,
        });
        if sent.is_err() {
            self.shared.release(size);
            return false;
        }
        true
    }

    /// Reports a CONNACK. Never dropped. From a clean-session one on, with
    /// `link_up_discards_queued` set, [`Commands`](crate::Commands) skips
    /// every SUBSCRIBE the controller queued before handling it.
    pub fn connected(&self, session_present: bool) {
        if !session_present {
            self.shared.connacks.fetch_add(1, Ordering::AcqRel);
        }
        let _ = self.tx.send(Inbound::Link(Link::Up { session_present }));
    }

    /// Reports a lost connection. Never dropped.
    pub fn disconnected(&self) {
        let _ = self.tx.send(Inbound::Link(Link::Down));
    }

    /// Reports a command the host could not hand to its client. A Subscribe
    /// or Unsubscribe is retried with a 1-30 s backoff; a Publish is dropped,
    /// since QoS 0 is never held. Never dropped.
    pub fn command_failed(&self, cmd: Command) {
        let _ = self.tx.send(Inbound::CommandFailed(cmd));
    }

    /// Remembers the device `topic` addresses and wakes the driver.
    fn note_dropped(&self, topic: &str) {
        let mut levels = topic.match_indices('/');
        let Some((end, _)) = levels.nth(2) else {
            return;
        };
        let Some(prefix) = topic.get(..end) else {
            return;
        };
        let is_state = topic.get(end + 1..) == Some("$state");
        self.shared.lock_dropped().add(prefix, is_state);
        if !self.shared.has_dropped.swap(true, Ordering::AcqRel) {
            let _ = self.tx.send(Inbound::Wake);
        }
    }
}
