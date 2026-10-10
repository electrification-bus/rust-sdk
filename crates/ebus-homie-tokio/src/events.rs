//! The bounded event channel.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ebus_homie::controller::Event;
use tokio::sync::mpsc::{self, error::TryRecvError, error::TrySendError};

/// One delivery from an [`EventReceiver`].
#[derive(Clone, Debug, PartialEq)]
pub enum Delivered {
    /// An event, in the order the controller emitted it.
    Event(Event),
    /// The channel was full and `dropped` events were lost. Re-read the
    /// snapshots through the [`ControllerHandle`](crate::ControllerHandle).
    Overflow {
        /// Events lost since the last delivery.
        dropped: u64,
    },
}

/// The driver's end: never waits for the consumer.
#[derive(Debug)]
pub(crate) struct EventSender {
    tx: mpsc::Sender<Delivered>,
    dropped: Arc<AtomicU64>,
}

impl EventSender {
    pub(crate) fn deliver(&self, event: Event) {
        let pending = self.dropped.swap(0, Ordering::AcqRel);
        if pending > 0 {
            match self.tx.try_send(Delivered::Overflow { dropped: pending }) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    self.dropped
                        .fetch_add(pending.saturating_add(1), Ordering::AcqRel);
                    return;
                }
                Err(TrySendError::Closed(_)) => return,
            }
        }
        if let Err(TrySendError::Full(_)) = self.tx.try_send(Delivered::Event(event)) {
            self.dropped.fetch_add(1, Ordering::AcqRel);
        }
    }
}

/// The application's end of the event channel.
///
/// The channel is bounded and the driver never waits on it. When it is full,
/// events are counted and dropped, and a [`Delivered::Overflow`] follows.
/// Events are hints; snapshots are the truth.
#[derive(Debug)]
pub struct EventReceiver {
    rx: mpsc::Receiver<Delivered>,
    dropped: Arc<AtomicU64>,
}

/// A bounded event channel holding `capacity` deliveries (at least one).
pub(crate) fn channel(capacity: usize) -> (EventSender, EventReceiver) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    let dropped = Arc::new(AtomicU64::new(0));
    (
        EventSender {
            tx,
            dropped: Arc::clone(&dropped),
        },
        EventReceiver { rx, dropped },
    )
}

impl EventReceiver {
    /// The next delivery, or `None` once the driver has stopped and every
    /// delivery was read. Cancel-safe.
    pub async fn recv(&mut self) -> Option<Delivered> {
        match self.rx.try_recv() {
            Ok(d) => return Some(d),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => return self.overflow(),
        }
        if let Some(d) = self.overflow() {
            return Some(d);
        }
        match self.rx.recv().await {
            Some(d) => Some(d),
            None => self.overflow(),
        }
    }

    /// The next delivery, when one is ready.
    pub fn try_recv(&mut self) -> Option<Delivered> {
        match self.rx.try_recv() {
            Ok(d) => Some(d),
            Err(_) => self.overflow(),
        }
    }

    fn overflow(&self) -> Option<Delivered> {
        let dropped = self.dropped.swap(0, Ordering::AcqRel);
        (dropped > 0).then_some(Delivered::Overflow { dropped })
    }
}
