//! Re-sync windows: clearing what the broker no longer retains.
//!
//! A SUBSCRIBE makes the broker send the retained messages its filters match
//! with RETAIN=1, and an established subscription delivers with RETAIN=0
//! (MQTT 3.1.1 section 3.3.1.3). Every SUBSCRIBE carrying a device's
//! `$description` filter is numbered and recorded for that device; each
//! retained `$description` received is matched to the oldest record. The
//! broker processes SUBSCRIBEs in order (section 3.8.4), replays are assumed
//! to arrive in that order, and some may never arrive (a topic not retained,
//! a connection lost), so the number matched is never above the SUBSCRIBE that
//! was answered. Records survive a link-up, since a client may send after a
//! CONNACK a SUBSCRIBE queued before it, unless the host rules that out
//! ([`Config::link_up_discards_queued`](super::Config::link_up_discards_queued)).
//!
//! A window over some of a device's nodes opens with a SUBSCRIBE for their
//! property filters followed by a `$description` marker, and closes once a
//! matched number reaches the marker's: every replay of those filters has then
//! arrived, and what the window covers and did not hear since it opened is
//! cleared.
//!
//! No window opens after a link-up that may resume a session, since the
//! broker may send a message of the old session again (section 4.4); its
//! replays are still matched, so later sessions stay in step. Nothing is
//! cleared for a device whose `$description` a host filter matches. Once a
//! message arrives with `retain: None`, no window opens: a live
//! `$description` is then indistinguishable from a replay.

use alloc::{collections::VecDeque, sync::Arc, vec::Vec};

use super::inbound::{addr, put_slot};
use super::key::{DevKey, KeyRef};
use super::types::Slot;
use super::{Controller, Device, Event, Id, Window};
use crate::topic::{self, TopicKind};

/// Runs kept per device; past it the oldest two merge.
const MAX_RUNS: usize = 16;

/// Windows open per device; past it the oldest is dropped.
const MAX_WINDOWS: usize = 8;

/// `n` SUBSCRIBEs numbered `lo..=hi`.
#[derive(Clone, Copy, Debug)]
struct Run {
    lo: u64,
    hi: u64,
    n: u32,
}

/// One device's SUBSCRIBEs carrying its `$description` whose replay has not
/// been matched, oldest first.
#[derive(Clone, Debug, Default)]
pub(super) struct Requests {
    runs: VecDeque<Run>,
}

impl Requests {
    fn push(&mut self, seq: u64) {
        self.runs.push_back(Run {
            lo: seq,
            hi: seq,
            n: 1,
        });
        if self.runs.len() > MAX_RUNS {
            if let Some(a) = self.runs.pop_front() {
                if let Some(b) = self.runs.front_mut() {
                    b.lo = a.lo;
                    b.n = b.n.saturating_add(a.n);
                }
            }
        }
    }

    /// Matches a replay to the oldest record; the number returned is at most
    /// that of the SUBSCRIBE it answers.
    fn pop(&mut self) -> Option<u64> {
        let front = self.runs.front_mut()?;
        let lo = front.lo;
        front.n -= 1;
        if front.n == 0 {
            self.runs.pop_front();
        }
        Some(lo)
    }

    /// Forgets the newest record, for a SUBSCRIBE the host could not send.
    fn pop_newest(&mut self) {
        if let Some(back) = self.runs.back_mut() {
            back.n -= 1;
            if back.n == 0 {
                self.runs.pop_back();
            }
        }
    }

    /// Forgets the SUBSCRIBEs numbered below `floor`: the broker processed
    /// them before one whose replay arrived, so any replay of theirs arrived
    /// first.
    fn shed(&mut self, floor: u64) {
        while self.runs.front().is_some_and(|r| r.hi < floor) {
            self.runs.pop_front();
        }
    }

    fn newest(&self) -> Option<u64> {
        self.runs.back().map(|r| r.hi)
    }

    fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }
}

/// The device a `$description` filter names.
fn desc_key(filter: &str) -> Option<DevKey> {
    let t = topic::parse(filter).ok()?;
    if !matches!(t.kind, TopicKind::DeviceDescription) {
        return None;
    }
    Some(DevKey {
        id: Id::new(t.device?)?,
        domain: Arc::from(t.domain),
    })
}

impl Window {
    fn covers(&self, node: &str) -> bool {
        self.nodes
            .as_ref()
            .is_none_or(|ns| ns.iter().any(|n| n.as_str() == node))
    }
}

/// True when the device holds a retained value or `$target` in `nodes`
/// (`None`: every node), or with `alerts` an alert.
pub(super) fn holds_any(dev: &Device, nodes: Option<&[Id]>, alerts: bool) -> bool {
    if alerts && !dev.view.alerts.is_empty() {
        return true;
    }
    dev.view
        .props
        .iter()
        .filter(|(n, _)| nodes.is_none_or(|ns| ns.contains(n)))
        .flat_map(|(_, m)| m.values())
        .any(|s| s.retained && (s.view.value.is_some() || s.view.target.is_some()))
}

/// Drops every open window.
pub(super) fn abandon(dev: &mut Device) {
    dev.windows.clear();
    dev.seen_alerts.clear();
}

impl Controller {
    /// True when a host filter matches `topic`, so the broker may replay it
    /// for a subscription the controller did not number.
    fn host_matches(&self, topic: &str) -> bool {
        self.cfg
            .host_filters
            .iter()
            .any(|f| ebus_mqtt::filter::matches(f, topic))
    }

    /// Numbers a SUBSCRIBE about to be queued.
    pub(super) fn number_subscribe(&mut self, filters: &[(alloc::string::String, ebus_mqtt::QoS)]) {
        self.seq += 1;
        for (f, _) in filters {
            if !f.ends_with("/$description") || self.host_matches(f) {
                continue;
            }
            let Some(key) = desc_key(f) else {
                continue;
            };
            let seq = self.seq;
            self.requests.entry(key).or_default().push(seq);
        }
    }

    /// A SUBSCRIBE the host could not send: its records are forgotten and the
    /// windows of every device it names are dropped.
    pub(super) fn unnumber_subscribe(
        &mut self,
        filters: &[(alloc::string::String, ebus_mqtt::QoS)],
    ) {
        for (f, _) in filters {
            if let Some(key) = desc_key(f) {
                if let Some(r) = self.requests.get_mut(&key) {
                    r.pop_newest();
                }
            }
            for (k, d) in self.devices.iter_mut() {
                let prefix = Self::device_filter(k, "");
                if f.starts_with(prefix.as_str()) {
                    abandon(d);
                }
            }
        }
    }

    /// A link-up: windows are dropped, and only a clean session (not an
    /// implicit one) lets the next replays close new ones. With
    /// [`Config::link_up_discards_queued`](super::Config::link_up_discards_queued)
    /// no SUBSCRIBE queued before a clean link-up is answered after it, so
    /// every record is dropped, along with the SUBSCRIBEs still queued here.
    pub(super) fn replay_link_up(&mut self, resumed: bool) {
        self.resumed = resumed;
        for d in self.devices.values_mut() {
            abandon(d);
        }
        if resumed {
            return;
        }
        if self.cfg.link_up_discards_queued {
            self.requests.clear();
            self.commands
                .retain(|c| !matches!(c, ebus_mqtt::Command::Subscribe { .. }));
            return;
        }
        let floor = self.floor;
        for r in self.requests.values_mut() {
            r.shed(floor);
        }
        let (devices, holds) = (&self.devices, &self.holds);
        self.requests
            .retain(|k, r| !r.is_empty() || devices.contains_key(k) || holds.contains_key(k));
    }

    /// Queues the end-of-replay marker for `key` in a SUBSCRIBE of its own and
    /// opens a window over `nodes`.
    pub(super) fn mark_and_open(&mut self, key: &DevKey, nodes: Option<Vec<Id>>, alerts: bool) {
        if self.resumed {
            return;
        }
        let f = Self::device_filter(key, "$description");
        self.resubscribe(&[f]);
        self.open_window(key, nodes, alerts);
    }

    /// Opens a window over `nodes` whose marker is the `$description` in the
    /// SUBSCRIBE queued last; does nothing when that SUBSCRIBE carries none
    /// for `key`, or in a session that may be resumed.
    pub(super) fn open_window(&mut self, key: &DevKey, nodes: Option<Vec<Id>>, alerts: bool) {
        if self.resumed {
            return;
        }
        let seq = self.seq;
        let numbered = self
            .requests
            .get(key as &dyn KeyRef)
            .and_then(Requests::newest)
            == Some(seq);
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        if !numbered {
            return;
        }
        let w = Window {
            marker: seq,
            nodes,
            alerts,
        };
        for node in dev.view.props.keys() {
            if w.covers(node.as_str()) && !dev.windows.iter().any(|o| o.covers(node.as_str())) {
                for s in dev
                    .marks
                    .get_mut(node)
                    .into_iter()
                    .flat_map(|m| m.values_mut())
                {
                    s.seen = 0;
                }
            }
        }
        if alerts && !dev.windows.iter().any(|o| o.alerts) {
            dev.seen_alerts.clear();
        }
        dev.windows.push(w);
        if dev.windows.len() > MAX_WINDOWS {
            dev.windows.remove(0);
        }
    }

    /// A retained `$description` for `key` on `topic`. In a session that may
    /// be resumed no window is open, and the floor stays, since the broker
    /// may have sent it twice.
    pub(super) fn note_replay(&mut self, key: &DevKey, topic: &str) {
        if self.host_matches(topic) {
            return;
        }
        let floor = self.floor;
        let Some(r) = self.requests.get_mut(key) else {
            return;
        };
        r.shed(floor);
        let Some(seq) = r.pop() else {
            return;
        };
        if self.resumed {
            return;
        }
        self.floor = self.floor.max(seq);
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        let (closed, open): (Vec<Window>, Vec<Window>) = core::mem::take(&mut dev.windows)
            .into_iter()
            .partition(|w| w.marker <= seq);
        dev.windows = open;
        for w in &closed {
            self.clear_unreplayed(key, w);
        }
        if let Some(dev) = self.devices.get_mut(key) {
            if !dev.windows.iter().any(|w| w.alerts) {
                dev.seen_alerts.clear();
            }
        }
    }

    /// Clears the retained values, `$target`s and, with `w.alerts`, alerts
    /// that `w` covers and that were not heard since it opened: the broker
    /// no longer retains them.
    fn clear_unreplayed(&mut self, key: &DevKey, w: &Window) {
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        let mut present = dev.retained_present;
        let mut cleared: Vec<Slot> = Vec::new();
        for (node, m) in dev.view.props.iter() {
            if !w.covers(node.as_str()) {
                continue;
            }
            for slot in m.values().filter(|s| s.retained) {
                let seen = dev.marks(&slot.node, &slot.prop).seen;
                let value = seen & 1 == 0 && slot.view.value.is_some();
                let target = seen & 2 == 0 && slot.view.target.is_some();
                if !value && !target {
                    continue;
                }
                let mut slot = slot.clone();
                if value {
                    slot.view.value = None;
                    slot.view.last_valid = None;
                    present = present.saturating_sub(1);
                    crate::log::debug!(
                        "valueNotReplayed",
                        device = key.id,
                        node = slot.node,
                        property = slot.prop
                    );
                    self.events.push_back(Event::PropertyCleared {
                        addr: addr(key, &slot),
                    });
                }
                if target {
                    slot.view.target = None;
                    self.events.push_back(Event::TargetChanged {
                        addr: addr(key, &slot),
                        target: None,
                    });
                }
                cleared.push(slot);
            }
        }
        for slot in cleared {
            put_slot(&mut dev.view.props, slot);
        }
        dev.retained_present = present;
        if w.alerts {
            let gone: Vec<Id> = dev
                .view
                .alerts
                .keys()
                .filter(|a| !dev.seen_alerts.contains(*a))
                .cloned()
                .collect();
            for alert in gone {
                dev.view.alerts.remove_mut(alert.as_str());
                self.events.push_back(Event::Alert {
                    device: key.id.clone(),
                    alert,
                    message: None,
                });
            }
        }
    }
}
