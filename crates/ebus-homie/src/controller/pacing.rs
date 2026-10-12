//! Subscriptions: the link, stage one and paced stage two (python-sdk#97),
//! stuck healing, dropped messages, failed commands and timers.

use alloc::{collections::BTreeSet, string::String, vec::Vec};

use ebus_mqtt::{Command, Instant, Link};

use super::key::DevKey;
use super::replay::{abandon, holds_any};
use super::{
    ALERTS, Attrs, Burst, Controller, Coverage, Device, Discovery, Event, Id, Missing,
    PendingEntry, RETRY_MAX_MS, RETRY_MIN_MS,
};

/// Nodes without property filters, with their retained property counts, in
/// declared order. `None` when every node is covered or there is no model;
/// an empty list when nothing is subscribed and no node is declared.
fn uncovered(dev: &Device) -> Option<Vec<(Id, u64)>> {
    let desc = dev.view.description.as_ref()?;
    let covered = match &dev.coverage {
        Coverage::All => return None,
        Coverage::None => None,
        Coverage::Nodes(s) => Some(s),
    };
    let mut out = Vec::new();
    for (nid, _) in &desc.nodes {
        let Some((id, m)) = dev.view.props.get_key_value(nid.as_str()) else {
            continue;
        };
        if covered.is_some_and(|s| s.contains(id)) {
            continue;
        }
        out.push((id.clone(), m.values().filter(|s| s.retained).count() as u64));
    }
    (covered.is_none() || !out.is_empty()).then_some(out)
}

/// The next property filters to subscribe: `(None, n)` for the whole device
/// when nothing is subscribed and its `n` retained properties fit the budget,
/// else the next nodes in declared order that fit it (at least one).
fn next_chunk(dev: &Device, budget: u64) -> Option<(Option<Vec<Id>>, u64)> {
    let nodes = uncovered(dev)?;
    let total: u64 = nodes.iter().map(|(_, c)| c).sum();
    if dev.coverage == Coverage::None && 2 * total <= budget {
        return Some((None, total));
    }
    let mut chunk = Vec::new();
    let mut expected = 0u64;
    for (n, c) in nodes {
        if !chunk.is_empty() && 2 * (expected + c) > budget {
            break;
        }
        chunk.push(n);
        expected += c;
    }
    Some((Some(chunk), expected))
}

fn clear_burst(dev: &mut Device) {
    for m in dev.marks.values_mut() {
        for s in m.values_mut() {
            s.burst = 0;
        }
    }
}

/// Forgets property coverage for `nodes` (`None`: the whole device), so the
/// next chunk subscribes them again.
fn uncover(dev: &mut Device, nodes: Option<&[Id]>) {
    match nodes {
        None => dev.coverage = Coverage::None,
        Some(ns) => {
            if let Coverage::Nodes(s) = &mut dev.coverage {
                for n in ns {
                    s.remove(n);
                }
                if s.is_empty() {
                    dev.coverage = Coverage::None;
                }
            }
        }
    }
}

impl Controller {
    /// Handles a change in the broker link.
    ///
    /// `Up` after a clean session, or after `session_present: true` unless
    /// [`Config::trust_session`](super::Config::trust_session), queues the
    /// discovery filter, then stage one for every known device (in tree mode
    /// the root unpaced, then every tracked descendant), with a fresh
    /// `$description` required before property filters are paced again; it
    /// marks every device unconfirmed and arms the absence and sync timers.
    /// A trusted resumed session keeps everything. [`Event::Link`] follows the
    /// commands. `Down` while the link is down changes nothing and emits
    /// nothing.
    pub fn handle_link(&mut self, now: Instant, link: Link) {
        self.link(now, link, false);
    }

    /// [`handle_link`](Self::handle_link). An `implicit` link-up keeps the
    /// ledger's wire state, since the broker may still hold those filters.
    pub(super) fn link(&mut self, now: Instant, link: Link, implicit: bool) {
        if self.shut_down {
            return;
        }
        match link {
            Link::Down => {
                if !self.connected {
                    return;
                }
                crate::log::info!("controllerDisconnect");
                self.connected = false;
            }
            Link::Up {
                session_present, ..
            } => {
                let first = !self.ever_up;
                self.ever_up = true;
                self.connected = true;
                self.backoff_ms = RETRY_MIN_MS;
                self.ledger.link_up(session_present || implicit);
                self.replay_link_up(session_present || implicit);
                crate::log::info!("controllerOnConnect", session_present = session_present);
                if first {
                    self.start(now);
                } else if session_present && self.cfg.trust_session {
                    for a in self.awaiting.values_mut() {
                        a.subscribed_at = now;
                    }
                    for b in self.draining.values_mut() {
                        b.subscribed_at = now;
                    }
                } else {
                    self.repace(now);
                }
            }
            _ => return,
        }
        self.events.push_back(Event::Link(link));
    }

    /// The first link-up: subscribe discovery.
    fn start(&mut self, now: Instant) {
        match self.cfg.discovery.clone() {
            Discovery::Wildcard => {
                if let Some(f) = self.discovery_filter.clone() {
                    crate::log::info!("startDiscovery", topic = f);
                    let cmd = self
                        .ledger
                        .subscribe(alloc::vec![(f, self.cfg.subscribe_qos)]);
                    self.push(cmd);
                }
            }
            Discovery::Tree { root: id } | Discovery::Device(id) => {
                let Some(key) = self.named_key(&id) else {
                    return;
                };
                crate::log::info!("startDiscovery", device = id);
                self.devices
                    .entry(key.clone())
                    .or_insert_with(|| Device::new(&key));
                self.request_subscription(now, &key, false, false);
            }
        }
    }

    /// A new session the controller does not trust: re-pace everything.
    fn repace(&mut self, now: Instant) {
        self.awaiting.clear();
        self.pending.clear();
        self.pending_entries.clear();
        self.draining.clear();
        self.retries.clear();
        self.dropped.clear();
        self.dropped_discovery = false;
        self.dropped_due = None;
        let keys: Vec<DevKey> = self.holds.keys().cloned().collect();
        for k in &keys {
            self.release_props(k);
        }
        let sync_ms = self.cfg.sync_timeout_ms;
        for d in self.devices.values_mut() {
            d.coverage = Coverage::None;
            clear_burst(d);
            d.view.confirmed = false;
            d.absent = false;
            if d.view.description.is_some() {
                d.view.synced = false;
                d.sync_at = Some(now.add_ms(sync_ms));
                d.sync_expired = false;
            }
        }
        self.floor_sync(now.add_ms(sync_ms));
        if let Some(f) = self.discovery_filter.clone() {
            self.resubscribe(&[f]);
        }
        match self.cfg.discovery.clone() {
            Discovery::Wildcard => {
                let keys: Vec<DevKey> = self
                    .devices
                    .iter()
                    .filter(|(_, d)| d.view.state.is_some())
                    .map(|(k, _)| k.clone())
                    .collect();
                if !keys.is_empty() {
                    crate::log::info!("resyncRepaceDevices", devices = keys.len());
                }
                for k in &keys {
                    self.request_subscription(now, k, true, true);
                }
            }
            Discovery::Tree { root: id } => {
                let Some(key) = self.named_key(&id) else {
                    return;
                };
                self.devices
                    .entry(key.clone())
                    .or_insert_with(|| Device::new(&key));
                self.request_subscription(now, &key, false, true);
                // The clean session dropped the descendants' filters as well
                // (convention.md:259-260); a removed one keeps its attribute
                // filters so a return is heard.
                let tracked: Vec<DevKey> = self
                    .devices
                    .keys()
                    .filter(|k| **k != key)
                    .cloned()
                    .collect();
                for k in &tracked {
                    self.request_subscription(now, k, true, true);
                }
                let removed: Vec<String> = self
                    .holds
                    .iter()
                    .filter(|(k, _)| !self.devices.contains_key(*k))
                    .flat_map(|(_, h)| h.attrs.iter().cloned())
                    .collect();
                self.resubscribe(&removed);
            }
            Discovery::Device(id) => {
                let Some(key) = self.named_key(&id) else {
                    return;
                };
                self.devices
                    .entry(key.clone())
                    .or_insert_with(|| Device::new(&key));
                self.request_subscription(now, &key, false, true);
            }
        }
        self.absent_due = Some(now.add_ms(self.cfg.absent_after_ms));
    }

    /// Stage one: subscribe a device's `$state` (except in wildcard mode) and
    /// `$description` now; unpaced, its property filters too.
    pub(super) fn request_subscription(
        &mut self,
        now: Instant,
        key: &DevKey,
        paced: bool,
        refresh: bool,
    ) {
        if self.in_flight(key) {
            return;
        }
        let attrs = self.attr_filters(key);
        let unpaced = !paced || self.cfg.pacing.is_none();
        let window = unpaced
            && !self.no_retain_bit
            && self
                .devices
                .get(key)
                .is_some_and(|d| holds_any(d, None, true));
        let props = if unpaced {
            if let Some(d) = self.devices.get_mut(key) {
                d.coverage = Coverage::All;
            }
            Self::prop_filters(key, None)
        } else {
            Vec::new()
        };
        let mut filters = attrs.clone();
        filters.extend(props.iter().cloned());
        self.floor_stuck(now);
        self.awaiting.insert(
            key.clone(),
            Attrs {
                filters,
                subscribed_at: now,
                attempts: 0,
                refresh,
                props: unpaced,
            },
        );
        self.hold(key, attrs, props, false);
        if window {
            self.mark_and_open(key, None, true);
        }
    }

    /// Ends stage one once `$state` and `$description` are held and queues the
    /// device for its property filters.
    pub(super) fn note_progress(&mut self, now: Instant, key: &DevKey) {
        let Some(dev) = self.devices.get(key) else {
            return;
        };
        if dev.view.state.is_none() || dev.raw_hash.is_none() {
            return;
        }
        if self.awaiting.get(key).is_some_and(|a| a.refresh) {
            return;
        }
        self.awaiting.remove(key);
        if self.cfg.pacing.is_some() {
            self.requeue_uncovered(key, false);
            if self.pending_entries.contains_key(key) {
                self.take_pending(now);
            }
        }
        self.check_sync(now, key);
    }

    /// Queues a device for stage two when some node lacks property filters.
    fn requeue_uncovered(&mut self, key: &DevKey, front: bool) {
        let Some(dev) = self.devices.get(key) else {
            return;
        };
        if dev.view.state.is_none() || self.in_flight(key) || uncovered(dev).is_none() {
            return;
        }
        if front {
            self.pending.push_front(key.clone());
        } else {
            self.pending.push_back(key.clone());
        }
        self.pending_entries
            .insert(key.clone(), PendingEntry::default());
    }

    /// Moves pending devices into free batch slots and subscribes them.
    pub(super) fn take_pending(&mut self, now: Instant) {
        let Some(p) = self.cfg.pacing else {
            return;
        };
        let budget = p.batch_values as u64;
        let mut owed: u64 = self.draining.values().map(Burst::owed).sum();
        while self.draining.len() < p.batch_devices {
            let Some(key) = self.pending.front().cloned() else {
                break;
            };
            let Some((nodes, expected)) =
                self.devices.get(&key).and_then(|d| next_chunk(d, budget))
            else {
                self.pending.pop_front();
                self.pending_entries.remove(&key);
                continue;
            };
            if !self.draining.is_empty() && owed + 2 * expected > budget {
                break;
            }
            self.pending.pop_front();
            let entry = self.pending_entries.remove(&key).unwrap_or_default();
            owed += 2 * expected;
            let Some(dev) = self.devices.get_mut(&key) else {
                continue;
            };
            let first_chunk = nodes.is_some() && dev.coverage == Coverage::None;
            match (&nodes, &mut dev.coverage) {
                (None, c) => *c = Coverage::All,
                (Some(ns), Coverage::Nodes(s)) => s.extend(ns.iter().cloned()),
                (Some(ns), c) => *c = Coverage::Nodes(ns.iter().cloned().collect::<BTreeSet<_>>()),
            }
            let marked = expected > 0;
            let alerts = nodes.is_none() || first_chunk;
            let window = !self.no_retain_bit && holds_any(dev, nodes.as_deref(), alerts);
            if marked {
                clear_burst(dev);
                self.draining.insert(
                    key.clone(),
                    Burst {
                        nodes: nodes.clone(),
                        expected: u32::try_from(expected).unwrap_or(u32::MAX),
                        received: 0,
                        subscribed_at: now,
                        attempts: entry.attempts,
                        last_received: entry.last_received,
                    },
                );
                self.floor_stuck(now);
            }
            let mut filters = Self::prop_filters(&key, nodes.as_deref());
            if first_chunk {
                // `{n}/+` does not match `$alert/{id}`, which `+/+` carries.
                filters.push(Self::device_filter(&key, ALERTS));
            }
            crate::log::info!(
                "pacedSubscribe",
                device = key.id,
                filters = filters.len(),
                pending = self.pending.len()
            );
            self.hold(&key, Vec::new(), filters, marked || window);
            if window {
                self.open_window(&key, nodes.clone(), alerts);
            }
            if !marked {
                self.requeue_uncovered(&key, true);
            }
        }
    }

    /// A device's burst is over: free its slot and move on.
    pub(super) fn release_slot(&mut self, now: Instant, key: &DevKey) {
        if self.draining.remove(key).is_none() {
            return;
        }
        if let Some(d) = self.devices.get_mut(key) {
            clear_burst(d);
        }
        self.requeue_uncovered(key, true);
        self.take_pending(now);
        self.check_sync(now, key);
    }

    /// Drops a device from pacing, freeing any slot it held.
    pub(super) fn forget(&mut self, now: Instant, key: &DevKey) {
        self.awaiting.remove(key);
        let mut freed = self.draining.remove(key).is_some();
        if self.pending_entries.remove(key).is_some() {
            self.pending.retain(|k| k != key);
            freed = true;
        }
        if freed {
            self.take_pending(now);
        }
    }

    /// After a description change, a burst in flight owes what the new
    /// description declares for its nodes.
    pub(super) fn recount_burst(&mut self, now: Instant, key: &DevKey) {
        let (Some(b), Some(dev)) = (self.draining.get_mut(key), self.devices.get(key)) else {
            return;
        };
        let (mut expected, mut received) = (0u32, 0u32);
        for (node, m) in dev.view.props.iter() {
            if !b.covers(node.as_str()) {
                continue;
            }
            for s in m.values().filter(|s| s.retained) {
                let burst = dev.marks(&s.node, &s.prop).burst;
                expected += 1;
                received += u32::from(burst & 1) + u32::from(burst >> 1 & 1);
            }
        }
        b.expected = expected;
        b.received = received;
        if b.owed() == 0 {
            self.release_slot(now, key);
        }
    }

    /// Handles every deadline at or before `now`.
    pub fn handle_timeout(&mut self, now: Instant) {
        if self.shut_down {
            return;
        }
        if self.connected {
            self.run_retries(now);
            if self.dropped_due.is_some_and(|t| t <= now) {
                self.heal_dropped(now);
            }
            self.check_stuck(now);
            if self.absent_due.is_some_and(|t| t <= now) {
                self.absent_due = None;
                for (k, d) in self.devices.iter_mut() {
                    if d.view.state.is_some() && !d.view.confirmed && !d.absent {
                        d.absent = true;
                        crate::log::info!("deviceAbsent", device = k.id);
                        self.events.push_back(Event::DeviceAbsent {
                            device: k.id.clone(),
                        });
                    }
                }
            }
        }
        let due: Vec<DevKey> = self
            .devices
            .iter()
            .filter(|(_, d)| d.sync_at.is_some_and(|t| t <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for key in due {
            if self.in_flight(&key) {
                if let Some(d) = self.devices.get_mut(&key) {
                    d.sync_at = None;
                    d.sync_expired = true;
                }
            } else {
                self.check_sync(now, &key);
                if let Some(d) = self.devices.get_mut(&key) {
                    d.sync_at = None;
                }
            }
        }
        self.recompute_floors();
    }

    /// Stuck healing (homie.py:4317-4422).
    fn check_stuck(&mut self, now: Instant) {
        let Some(timeout) = self.cfg.stuck_timeout_ms else {
            return;
        };
        let max = self.cfg.max_resubscribe_attempts;
        let due: Vec<DevKey> = self
            .awaiting
            .iter()
            .filter(|(_, a)| now.saturating_since(a.subscribed_at) >= timeout)
            .map(|(k, _)| k.clone())
            .collect();
        for key in due {
            let refresh = self.awaiting.get(&key).is_some_and(|a| a.refresh);
            let missing = match self.devices.get(&key) {
                None => {
                    self.awaiting.remove(&key);
                    continue;
                }
                Some(d) if d.view.state.is_none() => Missing::State,
                Some(d) if d.raw_hash.is_none() || refresh => Missing::Description,
                Some(_) => {
                    self.awaiting.remove(&key);
                    self.note_progress(now, &key);
                    continue;
                }
            };
            let Some(a) = self.awaiting.get_mut(&key) else {
                continue;
            };
            if a.attempts >= max {
                let attempt = a.attempts;
                self.awaiting.remove(&key);
                crate::log::error!("stuckDeviceGaveUp", device = key.id, attempts = attempt);
                self.events.push_back(Event::DeviceStuck {
                    device: key.id.clone(),
                    missing,
                    attempt,
                    gave_up: true,
                });
                continue;
            }
            a.attempts += 1;
            a.subscribed_at = now;
            let attempt = a.attempts;
            let filters = a.filters.clone();
            let props = a.props;
            crate::log::warn!("stuckDeviceResubscribe", device = key.id, attempt = attempt);
            self.resubscribe(&filters);
            self.mark_if_held(&key, props);
            self.events.push_back(Event::DeviceStuck {
                device: key.id.clone(),
                missing,
                attempt,
                gave_up: false,
            });
        }

        let due: Vec<DevKey> = self
            .draining
            .iter()
            .filter(|(_, b)| now.saturating_since(b.subscribed_at) >= timeout)
            .map(|(k, _)| k.clone())
            .collect();
        let mut gave_up = Vec::new();
        for key in due {
            let Some(b) = self.draining.remove(&key) else {
                continue;
            };
            let Some(dev) = self.devices.get_mut(&key) else {
                continue;
            };
            clear_burst(dev);
            let stalled = b.last_received.is_some_and(|l| b.received <= l);
            if b.attempts >= max || stalled {
                crate::log::error!(
                    "stuckDeviceGaveUp",
                    device = key.id,
                    received = b.received,
                    attempts = b.attempts
                );
                self.events.push_back(Event::DeviceStuck {
                    device: key.id.clone(),
                    missing: Missing::Properties,
                    attempt: b.attempts,
                    gave_up: true,
                });
                self.requeue_uncovered(&key, false);
                gave_up.push(key);
                continue;
            }
            uncover(dev, b.nodes.as_deref());
            let attempt = b.attempts.saturating_add(1);
            self.pending.push_back(key.clone());
            self.pending_entries.insert(
                key.clone(),
                PendingEntry {
                    attempts: attempt,
                    last_received: Some(b.received),
                },
            );
            crate::log::warn!("stuckDeviceRequeue", device = key.id, attempt = attempt);
            self.events.push_back(Event::DeviceStuck {
                device: key.id.clone(),
                missing: Missing::Properties,
                attempt,
                gave_up: false,
            });
        }
        self.take_pending(now);
        for key in gave_up {
            self.check_sync(now, &key);
        }
    }

    /// Reports that the host dropped messages for the device `topic` addresses
    /// (for example on inbox overflow). The next
    /// [`handle_timeout`](Self::handle_timeout), which a driver calls once its
    /// backlog drains, resubscribes the device so the broker replays its
    /// retained messages. In wildcard mode a dropped `$state` also
    /// resubscribes the discovery filter, which carries every `$state`.
    ///
    /// A host that reports a drop only after it has handed later messages to
    /// the controller calls [`handle_lost`](Self::handle_lost) first, before
    /// those messages.
    pub fn handle_dropped(&mut self, now: Instant, topic: &str) {
        let Some((key, state)) = self.lost_key(topic) else {
            return;
        };
        let discovery = state && self.discovery_filter.is_some();
        match key {
            Some(k) => {
                if let Some(d) = self.devices.get_mut(&k) {
                    abandon(d);
                }
                self.dropped.insert(k);
            }
            None if discovery => {}
            None => return,
        }
        if discovery {
            self.dropped_discovery = true;
        }
        crate::log::warn!("messagesDropped", topic = topic);
        if self.dropped_due.is_none() {
            self.dropped_due = Some(now);
        }
    }

    /// Reports, at its place in the message order, that the host dropped a
    /// message for the device `topic` addresses: no message after it ends a
    /// re-sync window open for that device, so nothing the drop hid is
    /// cleared. It heals nothing; [`handle_dropped`](Self::handle_dropped)
    /// still follows once the backlog drains.
    pub fn handle_lost(&mut self, topic: &str) {
        if let Some((Some(k), _)) = self.lost_key(topic) {
            if let Some(d) = self.devices.get_mut(&k) {
                abandon(d);
            }
        }
    }

    /// The tracked device a dropped message's `topic` addresses, and whether
    /// it is a `$state`; `None` for a topic the controller does not want.
    fn lost_key(&self, topic: &str) -> Option<(Option<DevKey>, bool)> {
        if self.shut_down || !self.wants(topic) {
            return None;
        }
        let t = crate::topic::parse(topic).ok()?;
        let id = t.device?;
        let state = matches!(t.kind, crate::topic::TopicKind::DeviceState);
        Some((self.key_of(id, t.domain), state))
    }

    fn heal_dropped(&mut self, now: Instant) {
        self.dropped_due = None;
        let keys = core::mem::take(&mut self.dropped);
        for key in &keys {
            if !self.devices.contains_key(key) {
                continue;
            }
            crate::log::info!("droppedResubscribe", device = key.id);
            if let Some(a) = self.awaiting.get_mut(key) {
                a.subscribed_at = now;
                let filters = a.filters.clone();
                let props = a.props;
                self.resubscribe(&filters);
                self.mark_if_held(key, props);
                continue;
            }
            if self.pending_entries.contains_key(key) {
                continue;
            }
            if let Some(b) = self.draining.remove(key) {
                if let Some(dev) = self.devices.get_mut(key) {
                    clear_burst(dev);
                    uncover(dev, b.nodes.as_deref());
                }
                self.pending.push_back(key.clone());
                self.pending_entries.insert(
                    key.clone(),
                    PendingEntry {
                        attempts: b.attempts,
                        last_received: None,
                    },
                );
                continue;
            }
            let (attrs, props) = self
                .holds
                .get(key)
                .map(|h| (h.attrs.clone(), h.props.clone()))
                .unwrap_or_default();
            self.resubscribe(&attrs);
            let paced = self.cfg.pacing.is_some()
                && !self.is_unpaced_device(key)
                && self
                    .devices
                    .get(key)
                    .is_some_and(|d| d.view.description.is_some());
            if paced {
                if let Some(dev) = self.devices.get_mut(key) {
                    dev.coverage = Coverage::None;
                }
                self.requeue_uncovered(key, false);
            } else if !self.no_retain_bit
                && self
                    .devices
                    .get(key)
                    .is_some_and(|d| holds_any(d, None, true))
            {
                let mut filters = props;
                filters.push(Self::device_filter(key, "$description"));
                self.resubscribe(&filters);
                self.open_window(key, None, true);
            } else {
                self.resubscribe(&props);
            }
        }
        if core::mem::take(&mut self.dropped_discovery) {
            if let Some(f) = self.discovery_filter.clone() {
                self.resubscribe(&[f]);
            }
        }
        self.take_pending(now);
    }

    /// The tree root and the single device subscribe their property filters
    /// with stage one, unpaced.
    fn is_unpaced_device(&self, key: &DevKey) -> bool {
        match &self.cfg.discovery {
            Discovery::Wildcard => false,
            Discovery::Tree { root: id } | Discovery::Device(id) => *id == key.id,
        }
    }

    /// After stage one's filters were sent again: with the whole device's
    /// property filters among them, the marker and a window.
    fn mark_if_held(&mut self, key: &DevKey, props: bool) {
        if props
            && !self.no_retain_bit
            && self
                .devices
                .get(key)
                .is_some_and(|d| holds_any(d, None, true))
        {
            self.mark_and_open(key, None, true);
        }
    }

    /// Reports a command the host could not hand to its client. A Subscribe
    /// or Unsubscribe is retried with a 1-30 s backoff; a Publish is dropped
    /// (`reason=setDropped`), since QoS 0 is never held (convention.md:695).
    pub fn handle_command_failed(&mut self, now: Instant, cmd: Command) {
        if self.shut_down {
            return;
        }
        match cmd {
            Command::Publish { topic, .. } => {
                crate::log::warn!("setDropped", topic = topic);
            }
            c @ (Command::Subscribe { .. } | Command::Unsubscribe { .. }) => {
                if let Command::Subscribe { filters, .. } = &c {
                    self.unnumber_subscribe(filters);
                }
                crate::log::warn!("commandRetry", delay_ms = self.backoff_ms);
                self.retries.push((now.add_ms(self.backoff_ms), c));
                self.backoff_ms = (self.backoff_ms * 2).min(RETRY_MAX_MS);
            }
            _ => {
                crate::log::debug!("commandFailedIgnored");
            }
        }
    }

    fn run_retries(&mut self, now: Instant) {
        if !self.retries.iter().any(|(t, _)| *t <= now) {
            return;
        }
        let (due, later): (Vec<_>, Vec<_>) = core::mem::take(&mut self.retries)
            .into_iter()
            .partition(|(t, _)| *t <= now);
        self.retries = later;
        for (_, cmd) in due {
            match cmd {
                Command::Subscribe { filters, .. } => {
                    let cmd = self.ledger.resubscribe(filters.iter().map(|(f, _)| f));
                    self.push(cmd);
                }
                Command::Unsubscribe { filters, .. } => {
                    let filters: Vec<_> = filters
                        .into_iter()
                        .filter(|f| {
                            self.ledger.count(f) == 0 && !self.ledger.is_host_filter(f) && f != "#"
                        })
                        .collect();
                    if !filters.is_empty() {
                        self.commands.push_back(Command::unsubscribe(filters));
                    }
                }
                _ => {}
            }
        }
    }
}
