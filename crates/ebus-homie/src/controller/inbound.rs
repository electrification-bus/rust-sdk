//! Received messages: `$state`, `$description`, values, targets, alerts and
//! logs; removal, tree reconciliation, effective state and sync.

use alloc::{
    collections::{BTreeSet, VecDeque},
    string::String,
    sync::Arc,
    vec::Vec,
};

use ebus_mqtt::{Instant, Link, Message};
use sha2::{Digest, Sha256};

use super::key::{DevKey, KeyRef};
use super::replay;
use super::types::{Props, Slot};
use super::{Controller, Device, Discovery, Event, Handled, Id, PropertyAddr, Value, prop_decl};
use super::{Origin, PropertyView};
use crate::datatype::check_value;
use crate::description::{self, DescriptionError, DeviceDescription, Ignored};
use crate::payload::{self, Received};
use crate::state::{DeviceState, effective_state};
use crate::topic::{self, TopicKind};
use rpds::RedBlackTreeMapSync;

/// The event address of a slot.
pub(super) fn addr(key: &DevKey, slot: &Slot) -> PropertyAddr {
    PropertyAddr {
        device: key.id.clone(),
        node: slot.node.clone(),
        property: slot.prop.clone(),
    }
}

/// Stores `slot` at its node and property, copying only the path to it.
pub(super) fn put_slot(props: &mut Props, slot: Slot) {
    if let Some(s) = props
        .get_mut(slot.node.as_str())
        .and_then(|m| m.get_mut(slot.prop.as_str()))
    {
        *s = slot;
    }
}

/// Rebuilds a device's property slots for a new description: values of
/// properties still declared are kept and checked again, the rest dropped. A
/// slot that comes out unchanged keeps its entry.
fn rebuild_slots(dev: &mut Device, desc: &DeviceDescription) -> usize {
    let before: usize = dev.view.props.values().map(RedBlackTreeMapSync::size).sum();
    let mut props = dev.view.props.clone();
    let mut nodes: BTreeSet<&str> = BTreeSet::new();
    let (mut total, mut present, mut kept) = (0u32, 0u32, 0usize);
    for (ni, (nid, node)) in desc.nodes.iter().enumerate() {
        let Some(node_id) = Id::new(nid) else {
            continue;
        };
        nodes.insert(nid.as_str());
        let old_map = props.get(nid.as_str()).cloned();
        let mut map = old_map
            .clone()
            .unwrap_or_else(RedBlackTreeMapSync::new_sync);
        let declared: BTreeSet<&str> = node.properties.iter().map(|(p, _)| p.as_str()).collect();
        let gone: Vec<Id> = map
            .keys()
            .filter(|p| !declared.contains(p.as_str()))
            .cloned()
            .collect();
        for p in gone {
            map.remove_mut(p.as_str());
        }
        for (pi, (pid, p)) in node.properties.iter().enumerate() {
            let Some(prop_id) = Id::new(pid) else {
                continue;
            };
            let old = map.get(pid.as_str());
            kept += usize::from(old.is_some());
            let mut slot = old.cloned().unwrap_or_else(|| Slot {
                view: PropertyView::default(),
                node: node_id.clone(),
                prop: prop_id.clone(),
                node_idx: ni,
                prop_idx: pi,
                retained: p.retained,
            });
            slot.node_idx = ni;
            slot.prop_idx = pi;
            slot.retained = p.retained;
            if !p.retained {
                slot.view.value = None;
                slot.view.last_valid = None;
            }
            if let Some(v) = &mut slot.view.value {
                v.check = check_value(&v.raw, p);
            }
            if let Some(v) = &mut slot.view.target {
                v.check = check_value(&v.raw, p);
            }
            slot.view.last_valid = match &slot.view.value {
                Some(v) if v.check.is_ok() => Some(v.raw.clone()),
                _ => slot
                    .view
                    .last_valid
                    .take()
                    .filter(|lv| check_value(lv, p).is_ok()),
            };
            if p.retained {
                total += 1;
                if slot.view.value.is_some() {
                    present += 1;
                }
            }
            if old != Some(&slot) {
                map.insert_mut(prop_id, slot);
            }
        }
        if !old_map.is_some_and(|m| m.ptr_eq(&map)) {
            props.insert_mut(node_id, map);
        }
    }
    let gone: Vec<Id> = props
        .keys()
        .filter(|n| !nodes.contains(n.as_str()))
        .cloned()
        .collect();
    for n in gone {
        props.remove_mut(n.as_str());
    }
    dev.marks.retain(|n, m| match props.get(n.as_str()) {
        Some(pm) => {
            m.retain(|p, _| pm.contains_key(p.as_str()));
            !m.is_empty()
        }
        None => false,
    });
    dev.view.props = props;
    dev.retained_total = total;
    dev.retained_present = present;
    before.saturating_sub(kept)
}

impl Controller {
    /// Handles one received message. A message while the link is down is
    /// taken as evidence of a new session (`reason=implicitLinkUp`): devices
    /// are re-paced as after a clean session, while filters already sent stay
    /// counted as on the broker, so releasing one still unsubscribes it. The
    /// host should still forward every CONNACK.
    pub fn handle_message(&mut self, now: Instant, msg: Message<'_>) -> Handled {
        if self.shut_down || !self.wants(msg.topic) {
            return Handled::Ignored;
        }
        let Ok(t) = topic::parse(msg.topic) else {
            crate::log::debug!("invalidTopic", topic = msg.topic);
            return Handled::Ignored;
        };
        let Some(device) = t.device else {
            return Handled::Ignored;
        };
        if matches!(
            t.kind,
            TopicKind::PropertySet { .. } | TopicKind::Broadcast { .. }
        ) {
            return Handled::Ignored;
        }
        if msg.retain.is_none() && !self.no_retain_bit {
            self.no_retain_bit = true;
            for d in self.devices.values_mut() {
                replay::abandon(d);
            }
        }
        if !self.connected {
            crate::log::warn!("implicitLinkUp", topic = msg.topic);
            self.link(
                now,
                Link::Up {
                    session_present: false,
                },
                true,
            );
        }
        match t.kind {
            TopicKind::DeviceState => self.on_state(now, t.domain, device, msg.payload),
            TopicKind::DeviceDescription => self.on_description(now, t.domain, device, msg),
            TopicKind::PropertyValue { node, property } => {
                self.on_value(now, t.domain, device, node, property, msg, false)
            }
            TopicKind::PropertyTarget { node, property } => {
                self.on_value(now, t.domain, device, node, property, msg, true)
            }
            TopicKind::DeviceAlert { alert_id } => {
                self.on_alert(t.domain, device, alert_id, msg.payload)
            }
            TopicKind::DeviceLog { level } => {
                let Some(dev) = self.devices.get_mut(&(device, t.domain) as &dyn KeyRef) else {
                    return Handled::Ignored;
                };
                dev.touch();
                crate::log::debug!("deviceLog", device = device, level = level);
                Handled::Consumed
            }
            TopicKind::PropertySet { .. } | TopicKind::Broadcast { .. } => Handled::Ignored,
        }
    }

    fn at_capacity(&self) -> bool {
        self.cfg
            .max_devices
            .is_some_and(|m| self.devices.len() >= m)
    }

    // ----- $state -----

    fn on_state(&mut self, now: Instant, domain: &str, id: &str, raw: &[u8]) -> Handled {
        let received = match payload::classify(raw) {
            Ok(r) => r,
            Err(_) => {
                crate::log::warn!("invalidState", device = id);
                return Handled::Ignored;
            }
        };
        let key = match self.key_of(id, domain) {
            Some(k) => k,
            None => match self.adopt(now, domain, id, received) {
                Some(Ok(k)) => k,
                Some(Err(h)) => return h,
                None => return Handled::Ignored,
            },
        };
        match received {
            Received::Deleted => {
                self.remove_device(now, &key);
            }
            Received::Text(text) => match DeviceState::parse(text) {
                Some(state) => self.apply_state(now, &key, state),
                None => {
                    crate::log::warn!("invalidState", device = id, state = text);
                    if let Some(d) = self.devices.get_mut(&key) {
                        d.touch();
                    }
                }
            },
        }
        Handled::Consumed
    }

    /// A `$state` for a device not in the registry: start tracking it when the
    /// mode allows. `Some(Err(_))` is a message handled without a device.
    fn adopt(
        &mut self,
        now: Instant,
        domain: &str,
        id: &str,
        received: Received<'_>,
    ) -> Option<Result<DevKey, Handled>> {
        let Received::Text(text) = received else {
            return self.aux_state(domain, id, None);
        };
        let Some(state) = DeviceState::parse(text) else {
            crate::log::warn!("invalidState", device = id, state = text);
            return None;
        };
        let new_key = |c: &Controller| -> Option<DevKey> {
            let domain: Arc<str> = match &c.named {
                Some(d) => d.clone(),
                None => Arc::from(domain),
            };
            Some(DevKey {
                id: Id::new(id)?,
                domain,
            })
        };
        match &self.cfg.discovery {
            Discovery::Wildcard => {
                if self.at_capacity() {
                    crate::log::warn!("deviceLimit", device = id);
                    return None;
                }
                let key = new_key(self)?;
                crate::log::info!("deviceDiscovered", device = id, state = state);
                self.devices.insert(key.clone(), Device::new(&key));
                self.request_subscription(now, &key, true, false);
                Some(Ok(key))
            }
            Discovery::Tree { root } => {
                let is_root = root.as_str() == id;
                let declared = is_root || self.children.values().any(|s| s.contains(id));
                if !declared || self.at_capacity() {
                    return None;
                }
                let key = new_key(self)?;
                crate::log::info!("removedDeviceReturned", device = id);
                self.devices.insert(key.clone(), Device::new(&key));
                self.request_subscription(now, &key, !is_root, false);
                Some(Ok(key))
            }
            Discovery::Device(dev_id) => {
                if dev_id.as_str() != id {
                    return self.aux_state(domain, id, Some(state));
                }
                let key = new_key(self)?;
                crate::log::info!("removedDeviceReturned", device = id);
                self.devices.insert(key.clone(), Device::new(&key));
                self.request_subscription(now, &key, false, false);
                Some(Ok(key))
            }
        }
    }

    /// Device mode: the `$state` of the tracked device's root.
    fn aux_state(
        &mut self,
        domain: &str,
        id: &str,
        state: Option<DeviceState>,
    ) -> Option<Result<DevKey, Handled>> {
        let Discovery::Device(dev_id) = &self.cfg.discovery else {
            return None;
        };
        let dev_key = self.named_key(dev_id)?;
        let is_root = self
            .devices
            .get(&dev_key)
            .and_then(|d| d.root_filter.as_ref())
            .is_some_and(|(r, _)| r == id);
        if !is_root {
            return None;
        }
        let root = Id::new(id)?;
        match state {
            Some(s) => {
                self.aux_states.insert(root, s);
            }
            None => {
                self.aux_states.remove(id);
            }
        }
        let domain: Arc<str> = Arc::from(domain);
        self.propagate_root(&domain, id, state);
        Some(Err(Handled::Consumed))
    }

    fn apply_state(&mut self, now: Instant, key: &DevKey, new: DeviceState) {
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        dev.touch();
        let old = dev.view.state;
        let changed = old != Some(new);
        if changed {
            dev.view.state = Some(new);
            let effective = self.effective_of(key).unwrap_or(new);
            if let Some(dev) = self.devices.get_mut(key) {
                dev.view.effective = Some(effective);
            }
            match old {
                None => {
                    self.events.push_back(Event::DeviceDiscovered {
                        device: key.id.clone(),
                        state: new,
                    });
                }
                Some(o) => {
                    crate::log::info!("deviceStateChanged", device = key.id, old = o, new = new);
                }
            }
            self.events.push_back(Event::StateChanged {
                device: key.id.clone(),
                old,
                new,
                effective,
            });
            self.propagate_root(&key.domain, key.id.as_str(), Some(new));
            if new == DeviceState::Ready {
                self.announce(key, false);
            }
        }
        if changed
            && new == DeviceState::Ready
            && matches!(self.cfg.discovery, Discovery::Tree { .. })
        {
            self.reconcile(now, key);
        }
        self.note_progress(now, key);
    }

    /// Emits [`Event::DescriptionChanged`] when the applied description differs
    /// from the last one announced.
    fn announce(&mut self, key: &DevKey, while_ready: bool) {
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        let while_sleeping = core::mem::take(&mut dev.changed_while_sleeping);
        let (Some(desc), Some(hash)) = (&dev.view.description, dev.content_hash) else {
            return;
        };
        if dev.announced == Some(hash) {
            return;
        }
        let while_ready = (while_ready || while_sleeping) && dev.announced.is_some();
        if while_ready {
            crate::log::warn!("descriptionChangedWhileReady", device = key.id);
        }
        dev.announced = Some(hash);
        self.events.push_back(Event::DescriptionChanged {
            device: key.id.clone(),
            description: desc.clone(),
            ignored: dev.ignored.clone(),
            while_ready,
        });
    }

    /// Recomputes the effective state of every device whose declared root is
    /// `root` in `domain`, now in `root_state`.
    fn propagate_root(&mut self, domain: &Arc<str>, root: &str, root_state: Option<DeviceState>) {
        for (k, d) in self.devices.iter_mut() {
            if k.domain != *domain || d.root_id() != Some(root) {
                continue;
            }
            let Some(own) = d.view.state else {
                continue;
            };
            let new = effective_state(root_state, own);
            if d.view.effective == Some(new) {
                continue;
            }
            if let Some(old) = d.view.effective.replace(new) {
                self.events.push_back(Event::EffectiveStateChanged {
                    device: k.id.clone(),
                    old,
                    new,
                });
            }
        }
    }

    /// Recomputes one device's effective state after its root may have changed.
    fn refresh_effective(&mut self, key: &DevKey) {
        let new = self.effective_of(key);
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        if dev.view.effective == new {
            return;
        }
        if let (Some(old), Some(new)) = (core::mem::replace(&mut dev.view.effective, new), new) {
            self.events.push_back(Event::EffectiveStateChanged {
                device: key.id.clone(),
                old,
                new,
            });
        }
    }

    // ----- removal -----

    /// A zero-length `$state`: the device leaves the registry. Tree and device
    /// modes keep its `$state` and `$description` filters so a return is heard.
    fn remove_device(&mut self, now: Instant, key: &DevKey) {
        let Some(dev) = self.devices.remove(key) else {
            return;
        };
        self.forget(now, key);
        match self.cfg.discovery {
            Discovery::Wildcard => self.release_all(key),
            _ => self.release_props(key),
        }
        if let Some((root, filter)) = &dev.root_filter {
            if let Some(h) = self.holds.get_mut(key) {
                h.attrs.retain(|f| f != filter);
            }
            let cmd = self.ledger.unsubscribe(alloc::vec![filter.clone()]);
            self.push(cmd);
            self.aux_states.remove(root.as_str());
        }
        self.dropped.remove(key);
        if dev.view.state.is_some() {
            crate::log::info!("deviceRemoved", device = key.id);
            self.events.push_back(Event::DeviceRemoved {
                device: key.id.clone(),
            });
        }
        self.propagate_root(&key.domain, key.id.as_str(), None);
        self.reevaluate_trees(key, dev.root_id());
    }

    // ----- tree mode -----

    /// Diffs a device's declared children against those subscribed: adds the
    /// new ones (paced), drops the undeclared ones recursively.
    fn reconcile(&mut self, now: Instant, key: &DevKey) {
        let Some(desc) = self
            .devices
            .get(key)
            .and_then(|d| d.view.description.clone())
        else {
            return;
        };
        let mut declared: Vec<Id> = Vec::new();
        for c in &desc.children {
            if c.as_str() == key.id.as_str() || declared.iter().any(|d| d == c.as_str()) {
                continue;
            }
            if let Some(id) = Id::new(c) {
                declared.push(id);
            }
        }
        let current = self.children.get(key).cloned().unwrap_or_default();
        for c in &declared {
            let ckey = DevKey {
                id: c.clone(),
                domain: key.domain.clone(),
            };
            let known = self.devices.contains_key(&ckey);
            if known && current.contains(c) {
                continue;
            }
            if !known {
                if self.at_capacity() {
                    crate::log::warn!("deviceLimit", device = c);
                    continue;
                }
                self.devices.insert(ckey.clone(), Device::new(&ckey));
            }
            self.children
                .entry(key.clone())
                .or_default()
                .insert(c.clone());
            if known {
                // Another parent declares it, or it is the root: subscribed.
                continue;
            }
            crate::log::info!("treeRootedAddDescendant", parent = key.id, child = c);
            self.request_subscription(now, &ckey, true, false);
        }
        if let Some(set) = self.children.get_mut(key) {
            set.retain(|c| declared.contains(c));
        }
        let undeclared: Vec<&Id> = current.iter().filter(|c| !declared.contains(c)).collect();
        if !undeclared.is_empty() {
            let reachable = self.reachable(&key.domain);
            for c in undeclared {
                let ckey = DevKey {
                    id: c.clone(),
                    domain: key.domain.clone(),
                };
                if reachable.contains(c.as_str()) {
                    crate::log::info!("treeRootedKeepDescendant", parent = key.id, child = c);
                    continue;
                }
                crate::log::info!("treeRootedRemoveDescendant", parent = key.id, child = c);
                let mut visited = BTreeSet::new();
                self.drop_tree(now, &ckey, &reachable, &mut visited);
            }
        }
        self.reevaluate_trees(key, None);
    }

    /// Tree mode: the ids reachable from the configured root through the
    /// children subscribed for each parent.
    fn reachable(&self, domain: &Arc<str>) -> BTreeSet<Id> {
        let mut seen = BTreeSet::new();
        let Discovery::Tree { root } = &self.cfg.discovery else {
            return seen;
        };
        let mut queue: VecDeque<Id> = VecDeque::from([root.clone()]);
        while let Some(id) = queue.pop_front() {
            if !seen.insert(id.clone()) {
                continue;
            }
            let k = DevKey {
                id,
                domain: domain.clone(),
            };
            if let Some(set) = self.children.get(&k) {
                queue.extend(set.iter().cloned());
            }
        }
        seen
    }

    /// Drops a device and its subscribed descendants, leaves first, except
    /// those another parent still declares (`reachable`).
    fn drop_tree(
        &mut self,
        now: Instant,
        key: &DevKey,
        reachable: &BTreeSet<Id>,
        visited: &mut BTreeSet<DevKey>,
    ) {
        if reachable.contains(key.id.as_str()) {
            return;
        }
        if let Discovery::Tree { root } = &self.cfg.discovery {
            if *root == key.id {
                return;
            }
        }
        if !visited.insert(key.clone()) {
            return;
        }
        for c in self.children.remove(key).unwrap_or_default() {
            let ckey = DevKey {
                id: c,
                domain: key.domain.clone(),
            };
            self.drop_tree(now, &ckey, reachable, visited);
        }
        for set in self.children.values_mut() {
            set.remove(key.id.as_str());
        }
        let dev = self.devices.remove(key);
        self.forget(now, key);
        self.release_all(key);
        self.dropped.remove(key);
        if dev.is_some_and(|d| d.view.state.is_some()) {
            crate::log::info!("deviceRemoved", device = key.id);
            self.events.push_back(Event::DeviceRemoved {
                device: key.id.clone(),
            });
        }
    }

    /// True when `root` and every device declared under it have a description.
    pub(super) fn tree_complete(&self, root: &DevKey) -> bool {
        let Some(desc) = self
            .devices
            .get(root)
            .and_then(|r| r.view.description.as_ref())
        else {
            return false;
        };
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        seen.insert(root.id.as_str());
        let mut queue: VecDeque<&str> = desc.children.iter().map(String::as_str).collect();
        while let Some(c) = queue.pop_front() {
            if !seen.insert(c) {
                continue;
            }
            let Some(cd) = self
                .devices
                .get(&(c, &*root.domain) as &dyn KeyRef)
                .and_then(|d| d.view.description.as_ref())
            else {
                return false;
            };
            queue.extend(cd.children.iter().map(String::as_str));
        }
        true
    }

    fn reevaluate_tree(&mut self, root: &DevKey) {
        let complete = self.tree_complete(root);
        let Some(d) = self.devices.get_mut(root) else {
            return;
        };
        let was = core::mem::replace(&mut d.tree_complete, complete);
        if complete && !was {
            crate::log::info!("treeComplete", root = root.id);
            self.events.push_back(Event::TreeComplete {
                root: root.id.clone(),
            });
        }
    }

    /// Re-evaluates the tree `key` belongs to (its declared root, or itself),
    /// and in tree mode the configured root.
    fn reevaluate_trees(&mut self, key: &DevKey, declared_root: Option<&str>) {
        let root = match declared_root.or_else(|| self.devices.get(key).and_then(Device::root_id)) {
            Some(r) => Id::new(r).map(|id| DevKey {
                id,
                domain: key.domain.clone(),
            }),
            None => Some(key.clone()),
        };
        if let Some(r) = &root {
            self.reevaluate_tree(r);
        }
        if let Discovery::Tree { root: tree_root } = &self.cfg.discovery {
            if let Some(tk) = self.named_key(tree_root) {
                if root.as_ref() != Some(&tk) {
                    self.reevaluate_tree(&tk);
                }
            }
        }
    }

    // ----- $description -----

    fn on_description(
        &mut self,
        now: Instant,
        domain: &str,
        id: &str,
        msg: Message<'_>,
    ) -> Handled {
        let Some(key) = self.key_of(id, domain) else {
            return Handled::Ignored;
        };
        let marker = msg.retain != Some(false) && self.draining.contains_key(&key);
        if msg.payload.is_empty() {
            if let Some(d) = self.devices.get_mut(&key) {
                d.touch();
            }
            crate::log::debug!("descriptionCleared", device = id);
            if marker {
                self.release_slot(now, &key);
            }
            return Handled::Consumed;
        }
        if let Some(a) = self.awaiting.get_mut(&key) {
            a.refresh = false;
        }
        let hash: [u8; 32] = Sha256::digest(msg.payload).into();
        let (same, was_rejected) = {
            let Some(dev) = self.devices.get_mut(&key) else {
                return Handled::Ignored;
            };
            dev.touch();
            let was_rejected = dev.raw_hash.is_some() && dev.view.description.is_none();
            (dev.raw_hash.replace(hash) == Some(hash), was_rejected)
        };
        if msg.retain == Some(true) {
            self.note_replay(&key, msg.topic);
        }
        if marker {
            self.release_slot(now, &key);
        }
        if !same {
            let parsed = match core::str::from_utf8(msg.payload) {
                Ok(text) => description::parse_with_ignored(text),
                Err(_) => Err(DescriptionError::InvalidJson),
            };
            match parsed {
                Ok((desc, ignored)) => {
                    self.apply_description(now, &key, desc, ignored);
                    if was_rejected {
                        self.replay_props(&key);
                    }
                }
                Err(e) => self.reject_description(now, &key, e),
            }
        }
        self.note_progress(now, &key);
        Handled::Consumed
    }

    /// Sends the SUBSCRIBE again for the property filters a device holds, so
    /// the broker replays the values discarded while it had no model.
    fn replay_props(&mut self, key: &DevKey) {
        if self.pending_entries.contains_key(key) || self.draining.contains_key(key) {
            return;
        }
        let props = self
            .holds
            .get(key)
            .map(|h| h.props.clone())
            .unwrap_or_default();
        if props.is_empty() {
            return;
        }
        crate::log::info!("modelRestoredResubscribe", device = key.id);
        let window = self
            .devices
            .get(key)
            .filter(|_| !self.no_retain_bit)
            .and_then(|d| {
                let nodes = match &d.coverage {
                    super::Coverage::Nodes(s) => Some(s.iter().cloned().collect::<Vec<_>>()),
                    _ => None,
                };
                let alerts =
                    nodes.is_none() || props.contains(&Self::device_filter(key, super::ALERTS));
                replay::holds_any(d, nodes.as_deref(), alerts).then_some((nodes, alerts))
            });
        match window {
            Some((nodes, alerts)) => {
                let mut filters = props;
                filters.push(Self::device_filter(key, "$description"));
                self.resubscribe(&filters);
                self.open_window(key, nodes, alerts);
            }
            None => self.resubscribe(&props),
        }
    }

    fn reject_description(&mut self, now: Instant, key: &DevKey, error: DescriptionError) {
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        crate::log::warn!("descriptionRejected", device = key.id, error = error);
        let declared_root = dev.root_id().map(String::from);
        dev.view.description = None;
        dev.content_hash = None;
        dev.announced = None;
        dev.ignored.clear();
        if !dev.view.props.is_empty() {
            dev.view.props = Props::new_sync();
        }
        dev.marks.clear();
        dev.retained_total = 0;
        dev.retained_present = 0;
        dev.view.synced = false;
        dev.sync_at = None;
        dev.sync_expired = false;
        dev.early.clear();
        dev.changed_while_sleeping = false;
        replay::abandon(dev);
        self.events.push_back(Event::DescriptionRejected {
            device: key.id.clone(),
            error,
        });
        self.forget(now, key);
        self.update_root_filter(key);
        self.refresh_effective(key);
        self.reevaluate_trees(key, declared_root.as_deref());
    }

    fn apply_description(
        &mut self,
        now: Instant,
        key: &DevKey,
        desc: DeviceDescription,
        ignored: Vec<Ignored>,
    ) {
        let content = desc.content_hash();
        let sync_ms = self.cfg.sync_timeout_ms;
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        if dev.content_hash == Some(content) {
            dev.ignored = ignored;
            return;
        }
        let desc = Arc::new(desc);
        let dropped = rebuild_slots(dev, &desc);
        if dropped > 0 {
            crate::log::debug!(
                "vanishedPropertiesDropped",
                device = key.id,
                count = dropped
            );
        }
        dev.view.description = Some(desc);
        dev.content_hash = Some(content);
        dev.ignored = ignored;
        dev.view.synced = false;
        dev.sync_at = Some(now.add_ms(sync_ms));
        dev.sync_expired = false;
        let early = core::mem::take(&mut dev.early);
        let ready = dev.view.state == Some(DeviceState::Ready);
        if dev.view.state == Some(DeviceState::Sleeping) && dev.announced.is_some() {
            crate::log::warn!("descriptionChangedWhileSleeping", device = key.id);
            dev.changed_while_sleeping = true;
        }
        crate::log::info!("descriptionReceived", device = key.id);
        self.floor_sync(now.add_ms(sync_ms));
        if ready {
            self.announce(key, true);
        }
        self.prune_coverage(key);
        self.update_root_filter(key);
        self.refresh_effective(key);
        self.recount_burst(now, key);
        for ((node, prop), e) in early {
            if let Some((text, retain)) = e.value {
                self.store_value(key, &node, &prop, Some(&text), retain, false);
            }
            if let Some((text, retain)) = e.target {
                self.store_value(key, &node, &prop, Some(&text), retain, true);
            }
        }
        if ready && matches!(self.cfg.discovery, Discovery::Tree { .. }) {
            self.reconcile(now, key);
        }
        self.reevaluate_trees(key, None);
        self.check_sync(now, key);
    }

    /// Node by node: releases the property filters of covered nodes the
    /// description no longer declares.
    fn prune_coverage(&mut self, key: &DevKey) {
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        let super::Coverage::Nodes(set) = &mut dev.coverage else {
            return;
        };
        let props = &dev.view.props;
        let gone: Vec<Id> = set
            .iter()
            .filter(|n| !props.contains_key(n.as_str()))
            .cloned()
            .collect();
        if gone.is_empty() {
            return;
        }
        for n in &gone {
            set.remove(n);
        }
        if set.is_empty() {
            dev.coverage = super::Coverage::None;
        }
        let filters = Self::prop_filters(key, Some(&gone));
        if let Some(h) = self.holds.get_mut(key) {
            h.props.retain(|f| !filters.contains(f));
        }
        crate::log::debug!("vanishedNodesReleased", device = key.id, nodes = gone.len());
        let cmd = self.ledger.unsubscribe(filters);
        self.push(cmd);
    }

    /// Device mode: holds `{root}/$state` for the root the description names.
    fn update_root_filter(&mut self, key: &DevKey) {
        if !matches!(self.cfg.discovery, Discovery::Device(_)) {
            return;
        }
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        let want = dev.root_id().map(|r| {
            (
                String::from(r),
                Self::topic_filter(&key.domain, r, "$state"),
            )
        });
        if dev.root_filter == want {
            return;
        }
        let old = core::mem::replace(&mut dev.root_filter, want.clone());
        if let Some((root, filter)) = old {
            if let Some(h) = self.holds.get_mut(key) {
                h.attrs.retain(|f| *f != filter);
            }
            let cmd = self.ledger.unsubscribe(alloc::vec![filter]);
            self.push(cmd);
            self.aux_states.remove(root.as_str());
        }
        if let Some((_, filter)) = want {
            self.hold(key, alloc::vec![filter], Vec::new(), false);
        }
    }

    // ----- values -----

    #[allow(clippy::too_many_arguments)]
    fn on_value(
        &mut self,
        now: Instant,
        domain: &str,
        id: &str,
        node: &str,
        prop: &str,
        msg: Message<'_>,
        target: bool,
    ) -> Handled {
        let max_early = self.cfg.max_early_values;
        let Some((key, dev)) = self.devices.get_key_value(&(id, domain) as &dyn KeyRef) else {
            return Handled::Ignored;
        };
        let key = key.clone();
        let has_model = dev.view.description.is_some();
        let rejected = dev.raw_hash.is_some() && !has_model;
        let text = match payload::classify(msg.payload) {
            Ok(Received::Deleted) => None,
            Ok(Received::Text(t)) => Some(t),
            Err(_) => {
                crate::log::warn!("invalidUtf8", topic = msg.topic);
                return Handled::Consumed;
            }
        };
        let Some(dev) = self.devices.get_mut(&key) else {
            return Handled::Ignored;
        };
        dev.touch();
        if !has_model {
            if rejected {
                crate::log::debug!("valueWithoutModel", topic = msg.topic);
                return Handled::Consumed;
            }
            let k = (String::from(node), String::from(prop));
            match text {
                None => {
                    if let Some(e) = dev.early.get_mut(&k) {
                        if target {
                            e.target = None;
                        } else {
                            e.value = None;
                        }
                        if e.value.is_none() && e.target.is_none() {
                            dev.early.remove(&k);
                        }
                    }
                }
                Some(t) => {
                    if !dev.early.contains_key(&k) && dev.early.len() >= max_early {
                        crate::log::debug!("earlyValueDropped", topic = msg.topic);
                        return Handled::Consumed;
                    }
                    let e = dev.early.entry(k).or_default();
                    let v = Some((String::from(t), msg.retain));
                    if target {
                        e.target = v;
                    } else {
                        e.value = v;
                    }
                }
            }
            return Handled::Consumed;
        }
        let counted = self.store_value(&key, node, prop, text, msg.retain, target);
        if counted {
            let done = self.draining.get(&key).is_some_and(|b| b.owed() == 0);
            if done {
                self.release_slot(now, &key);
            } else if !self.pending.is_empty() {
                self.take_pending(now);
            }
        }
        self.check_sync(now, &key);
        Handled::Consumed
    }

    /// Applies a value or `$target` (`text: None` clears it) to a device with a
    /// model, emitting its event. True when it counted toward the device's
    /// burst for the first time.
    fn store_value(
        &mut self,
        key: &DevKey,
        node: &str,
        prop: &str,
        text: Option<&str>,
        retain: Option<bool>,
        target: bool,
    ) -> bool {
        let Some(dev) = self.devices.get_mut(key) else {
            return false;
        };
        let Some(desc) = dev.view.description.as_deref() else {
            return false;
        };
        let Some(old) = dev.view.props.get(node).and_then(|m| m.get(prop)) else {
            crate::log::debug!(
                "undeclaredProperty",
                device = key.id,
                node = node,
                property = prop
            );
            return false;
        };
        let Some(decl) = prop_decl(desc, old) else {
            return false;
        };
        let bit = if target { 2 } else { 1 };
        let marks = dev
            .marks
            .entry(old.node.clone())
            .or_default()
            .entry(old.prop.clone())
            .or_default();
        marks.seen |= bit;
        let mut slot = old.clone();
        let events = &mut self.events;
        let mut counts = false;
        if target {
            match text {
                None => {
                    if slot.view.target.take().is_some() {
                        events.push_back(Event::TargetChanged {
                            addr: addr(key, &slot),
                            target: None,
                        });
                    }
                }
                Some(t) => {
                    counts = decl.retained;
                    if slot.view.target.as_ref().is_none_or(|v| &*v.raw != t) {
                        let v = Value {
                            raw: Arc::from(t),
                            check: check_value(t, decl),
                        };
                        slot.view.target = Some(v.clone());
                        events.push_back(Event::TargetChanged {
                            addr: addr(key, &slot),
                            target: Some(v),
                        });
                    }
                }
            }
        } else if !decl.retained {
            match text {
                None => {
                    crate::log::debug!(
                        "eventPropertyCleared",
                        device = key.id,
                        node = node,
                        property = prop
                    );
                }
                Some(t) => {
                    if retain == Some(true) {
                        crate::log::info!(
                            "retainedEventProperty",
                            device = key.id,
                            node = node,
                            property = prop
                        );
                    }
                    events.push_back(Event::PropertyEvent {
                        addr: addr(key, &slot),
                        value: Value {
                            raw: Arc::from(t),
                            check: check_value(t, decl),
                        },
                        origin: Origin::of(retain),
                    });
                }
            }
        } else {
            match text {
                None => {
                    if slot.view.value.take().is_some() {
                        slot.view.last_valid = None;
                        dev.retained_present = dev.retained_present.saturating_sub(1);
                        events.push_back(Event::PropertyCleared {
                            addr: addr(key, &slot),
                        });
                    }
                }
                Some(t) => {
                    counts = true;
                    if slot.view.value.as_ref().is_none_or(|v| &*v.raw != t) {
                        if slot.view.value.is_none() {
                            dev.retained_present += 1;
                        }
                        let v = Value {
                            raw: Arc::from(t),
                            check: check_value(t, decl),
                        };
                        if v.check.is_ok() {
                            slot.view.last_valid = Some(v.raw.clone());
                        }
                        slot.view.value = Some(v.clone());
                        events.push_back(Event::PropertyChanged {
                            addr: addr(key, &slot),
                            value: v,
                            origin: Origin::of(retain),
                        });
                    }
                }
            }
        }
        if slot != *old {
            put_slot(&mut dev.view.props, slot);
        }
        if !counts {
            return false;
        }
        let Some(b) = self.draining.get_mut(key) else {
            return false;
        };
        if !b.covers(node) || marks.burst & bit != 0 {
            return false;
        }
        marks.burst |= bit;
        b.received = b.received.saturating_add(1);
        true
    }

    // ----- alerts -----

    fn on_alert(&mut self, domain: &str, id: &str, alert: &str, raw: &[u8]) -> Handled {
        let max_alerts = self.cfg.max_alerts;
        let Some((key, _)) = self.devices.get_key_value(&(id, domain) as &dyn KeyRef) else {
            return Handled::Ignored;
        };
        let device = key.id.clone();
        let Some(dev) = self.devices.get_mut(&(id, domain) as &dyn KeyRef) else {
            return Handled::Ignored;
        };
        dev.touch();
        let Some(alert_id) = Id::new(alert) else {
            return Handled::Consumed;
        };
        match payload::classify(raw) {
            Ok(Received::Deleted) => {
                // `remove_mut` copies the path even for an absent key.
                if dev.view.alerts.contains_key(alert) {
                    dev.view.alerts.remove_mut(alert);
                    self.events.push_back(Event::Alert {
                        device,
                        alert: alert_id,
                        message: None,
                    });
                }
            }
            Ok(Received::Text(t)) => {
                let known = dev.view.alerts.contains_key(alert);
                if !known && dev.view.alerts.size() >= max_alerts {
                    crate::log::debug!("alertDropped", device = id, alert = alert);
                    return Handled::Consumed;
                }
                if dev.windows.iter().any(|w| w.alerts) {
                    dev.seen_alerts.insert(alert_id.clone());
                }
                if dev.view.alerts.get(alert).map(String::as_str) != Some(t) {
                    dev.view
                        .alerts
                        .insert_mut(alert_id.clone(), String::from(t));
                    self.events.push_back(Event::Alert {
                        device,
                        alert: alert_id,
                        message: Some(String::from(t)),
                    });
                }
            }
            Err(_) => {
                crate::log::warn!("invalidUtf8", device = id, alert = alert);
            }
        }
        Handled::Consumed
    }

    // ----- sync -----

    pub(super) fn in_flight(&self, key: &DevKey) -> bool {
        self.awaiting.contains_key(key)
            || self.pending_entries.contains_key(key)
            || self.draining.contains_key(key)
    }

    /// Emits [`Event::DeviceSynced`] when its conditions hold.
    pub(super) fn check_sync(&mut self, now: Instant, key: &DevKey) {
        match self.devices.get(key) {
            Some(d) if !d.view.synced && d.view.state.is_some() && d.view.description.is_some() => {
            }
            _ => return,
        }
        if self.in_flight(key) {
            return;
        }
        let Some(dev) = self.devices.get_mut(key) else {
            return;
        };
        let missing = dev.missing();
        let expired = dev.sync_expired || dev.sync_at.is_some_and(|t| t <= now);
        if (missing > 0 || !dev.windows.is_empty()) && !expired {
            return;
        }
        dev.view.synced = true;
        dev.sync_at = None;
        dev.sync_expired = false;
        crate::log::info!("deviceSynced", device = key.id, missing = missing);
        self.events.push_back(Event::DeviceSynced {
            device: key.id.clone(),
            missing,
        });
    }
}
