//! The Homie 5 controller as a sans-IO state machine.
//!
//! Input arrives through the `handle_*` methods and output leaves through the
//! `poll_*` methods; time comes only from the caller's [`Instant`]. One owner
//! drives a [`Controller`]:
//!
//! 1. call [`handle_link`](Controller::handle_link) on every CONNACK and every
//!    lost connection, including `Up { session_present: false }` when attaching
//!    to a client that is already connected;
//! 2. pass every received message to
//!    [`handle_message`](Controller::handle_message) (or only those for which
//!    [`wants`](Controller::wants) is true);
//! 3. call [`handle_timeout`](Controller::handle_timeout) once the instant
//!    [`poll_timeout`](Controller::poll_timeout) returns has passed;
//! 4. after each call, hand every [`poll_command`](Controller::poll_command)
//!    result to the MQTT client, in order, and report one it could not accept
//!    through [`handle_command_failed`](Controller::handle_command_failed);
//!    read events with [`poll_event`](Controller::poll_event).
//!
//! Events are hints; the views returned by [`Controller::device`] are the
//! truth.
//!
//! The host's own subscriptions are its business: declare in
//! [`Config::host_filters`] every filter string it also subscribes on the same
//! client, so the controller never unsubscribes it, and leave resubscribing the
//! controller's filters after a reconnect to the controller.

mod inbound;
mod key;
mod pacing;
mod replay;
#[cfg(test)]
mod tests;
mod types;

pub use types::{
    Config, ConfigError, DeviceView, Discovery, Domain, Event, Handled, Id, Missing, Origin,
    Pacing, PropertyAddr, PropertyView, SetError, SetOptions, Value,
};

use alloc::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    format,
    string::String,
    sync::Arc,
    vec::Vec,
};
use core::ops::Bound;

use ebus_mqtt::{Command, FilterLedger, Instant, QoS};

use crate::HOMIE_TOPIC_VERSION;
use crate::datatype::{self, Current, Datatype};
use crate::description::{Ignored, PropertyDescription};
use crate::id::is_valid_id;
use crate::payload::EMPTY_STRING_PAYLOAD;
use crate::state::{DeviceState, effective_state};
use key::{DevKey, KeyRef};
use types::Slot;

/// Retry backoff for a command the host could not hand to its client
/// (client.py:155).
const RETRY_MIN_MS: u64 = 1_000;
const RETRY_MAX_MS: u64 = 30_000;

/// The property filter suffixes for a whole device.
const WHOLE_DEVICE: [&str; 2] = ["+/+", "+/+/$target"];

/// The alert filter suffix a device subscribed node by node also holds.
const ALERTS: &str = "$alert/+";

/// Which nodes have property filters subscribed.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Coverage {
    None,
    All,
    Nodes(BTreeSet<Id>),
}

/// A value or `$target` received before the device's `$description`.
#[derive(Clone, Debug, Default)]
struct Early {
    value: Option<(String, Option<bool>)>,
    target: Option<(String, Option<bool>)>,
}

/// Per-property bits the controller keeps beside a device's view, so that
/// setting them never copies the view's maps. Bit 0 stands for a value, bit 1
/// for a `$target`.
#[derive(Clone, Copy, Debug, Default)]
struct Marks {
    /// Arrived in the current burst.
    burst: u8,
    /// Arrived since the oldest re-sync window covering the node opened.
    seen: u8,
}

/// One device, including those subscribed in tree or device mode whose
/// `$state` has not arrived yet (`view.state` is `None`).
#[derive(Clone, Debug)]
struct Device {
    view: DeviceView,
    /// SHA-256 of the last `$description` payload.
    raw_hash: Option<[u8; 32]>,
    /// `content_hash` of the applied description.
    content_hash: Option<[u8; 32]>,
    /// `content_hash` of the last description announced.
    announced: Option<[u8; 32]>,
    ignored: Vec<Ignored>,
    early: BTreeMap<(String, String), Early>,
    /// By node, then property; absent means zero.
    marks: BTreeMap<Id, BTreeMap<Id, Marks>>,
    coverage: Coverage,
    retained_total: u32,
    retained_present: u32,
    /// When missing values are reported, until [`Event::DeviceSynced`] fires.
    sync_at: Option<Instant>,
    sync_expired: bool,
    absent: bool,
    /// Re-sync windows open, oldest first.
    windows: Vec<Window>,
    /// Alerts heard since the oldest window carrying alerts opened.
    seen_alerts: BTreeSet<Id>,
    /// The description changed while the device was `sleeping`; reported on
    /// the next announcement.
    changed_while_sleeping: bool,
    tree_complete: bool,
    /// Device mode: the root this device's description names, and the
    /// `{root}/$state` filter held for it.
    root_filter: Option<(String, String)>,
}

impl Device {
    fn new(key: &DevKey) -> Self {
        Device {
            view: DeviceView::new(key.id.clone(), key.domain.clone()),
            raw_hash: None,
            content_hash: None,
            announced: None,
            ignored: Vec::new(),
            early: BTreeMap::new(),
            marks: BTreeMap::new(),
            coverage: Coverage::None,
            retained_total: 0,
            retained_present: 0,
            sync_at: None,
            sync_expired: false,
            absent: false,
            windows: Vec::new(),
            seen_alerts: BTreeSet::new(),
            changed_while_sleeping: false,
            tree_complete: false,
            root_filter: None,
        }
    }

    fn touch(&mut self) {
        self.view.confirmed = true;
        self.absent = false;
    }

    /// The declared root, when it is another device.
    fn root_id(&self) -> Option<&str> {
        let root = self.view.description.as_ref()?.root.as_deref()?;
        (root != self.view.id.as_str()).then_some(root)
    }

    fn missing(&self) -> u32 {
        self.retained_total.saturating_sub(self.retained_present)
    }

    fn marks(&self, node: &str, prop: &str) -> Marks {
        self.marks
            .get(node)
            .and_then(|m| m.get(prop))
            .copied()
            .unwrap_or_default()
    }
}

/// Stage one: a device waiting for its `$state` and `$description`.
#[derive(Clone, Debug)]
struct Attrs {
    filters: Vec<String>,
    subscribed_at: Instant,
    attempts: u8,
    /// After a reconnect: a fresh `$description` is required.
    refresh: bool,
    /// `filters` include the whole device's property filters.
    props: bool,
}

/// A re-sync of some of a device's nodes, from the SUBSCRIBE for their
/// property filters until the replay of the `$description` marker queued
/// after them.
#[derive(Clone, Debug)]
struct Window {
    /// Number of the SUBSCRIBE carrying the marker.
    marker: u64,
    /// `None`: every node.
    nodes: Option<Vec<Id>>,
    /// The filters carry `$alert/+`.
    alerts: bool,
}

/// Stage two: a device holding a batch slot while its retained property
/// messages arrive.
#[derive(Clone, Debug)]
struct Burst {
    /// `None` for the whole-device filters, else the nodes of one chunk.
    nodes: Option<Vec<Id>>,
    /// Retained properties the chunk declares.
    expected: u32,
    /// Values and targets of those properties received.
    received: u32,
    subscribed_at: Instant,
    attempts: u8,
    /// `received` of the attempt before, to stop a retry that made no progress.
    last_received: Option<u32>,
}

impl Burst {
    fn owed(&self) -> u64 {
        (2 * u64::from(self.expected)).saturating_sub(u64::from(self.received))
    }

    fn covers(&self, node: &str) -> bool {
        self.nodes
            .as_ref()
            .is_none_or(|ns| ns.iter().any(|n| n.as_str() == node))
    }
}

/// A device waiting for a batch slot.
#[derive(Clone, Copy, Debug, Default)]
struct PendingEntry {
    attempts: u8,
    last_received: Option<u32>,
}

/// The filters one device holds in the ledger.
#[derive(Clone, Debug, Default)]
struct Holds {
    /// `$state`, `$description` and, in device mode, the root's `$state`.
    attrs: Vec<String>,
    /// Property and `$target` filters.
    props: Vec<String>,
}

/// The Homie 5 controller; see the [module documentation](self).
#[derive(Debug)]
pub struct Controller {
    cfg: Config,
    /// The [`Domain::Named`] domain.
    named: Option<Arc<str>>,
    /// `{domain}/5/` for a named domain.
    prefix: String,
    devices: BTreeMap<DevKey, Device>,
    holds: BTreeMap<DevKey, Holds>,
    /// Tree mode: children subscribed for each parent.
    children: BTreeMap<DevKey, BTreeSet<Id>>,
    /// Device mode: `$state` of the device's root.
    aux_states: BTreeMap<Id, DeviceState>,
    ledger: FilterLedger,
    /// Wildcard mode: `{domain}/5/+/$state`.
    discovery_filter: Option<String>,
    awaiting: BTreeMap<DevKey, Attrs>,
    pending: VecDeque<DevKey>,
    pending_entries: BTreeMap<DevKey, PendingEntry>,
    draining: BTreeMap<DevKey, Burst>,
    commands: VecDeque<Command>,
    events: VecDeque<Event>,
    connected: bool,
    ever_up: bool,
    shut_down: bool,
    absent_due: Option<Instant>,
    retries: Vec<(Instant, Command)>,
    backoff_ms: u64,
    dropped: BTreeSet<DevKey>,
    dropped_discovery: bool,
    dropped_due: Option<Instant>,
    /// At or before the earliest `sync_at` of any device.
    sync_floor: Option<Instant>,
    /// At or before the earliest `subscribed_at` in `awaiting` and `draining`.
    stuck_floor: Option<Instant>,
    /// Per device, the SUBSCRIBEs carrying its `$description` whose replay
    /// has not arrived.
    requests: BTreeMap<DevKey, replay::Requests>,
    /// Number of the last SUBSCRIBE queued.
    seq: u64,
    /// SUBSCRIBEs numbered below it were processed by the broker.
    floor: u64,
    /// The last link-up may have resumed a session.
    resumed: bool,
    /// A message arrived with `retain: None`.
    no_retain_bit: bool,
}

impl Controller {
    /// A controller for `config`. Discovery is subscribed on the first
    /// [`handle_link`](Self::handle_link) `Up`, or on the first message, which
    /// counts as one.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        config.validate()?;
        let (named, prefix) = match &config.domain {
            Domain::Named(d) => (
                Some(Arc::<str>::from(d.as_str())),
                format!("{d}/{HOMIE_TOPIC_VERSION}/"),
            ),
            Domain::Any => (None, String::new()),
        };
        let discovery_filter = match (&config.discovery, &config.domain) {
            (Discovery::Wildcard, Domain::Named(d)) => {
                Some(format!("{d}/{HOMIE_TOPIC_VERSION}/+/$state"))
            }
            (Discovery::Wildcard, Domain::Any) => Some(format!("+/{HOMIE_TOPIC_VERSION}/+/$state")),
            _ => None,
        };
        Ok(Controller {
            ledger: FilterLedger::new(config.host_filters.iter().cloned()),
            cfg: config,
            named,
            prefix,
            devices: BTreeMap::new(),
            holds: BTreeMap::new(),
            children: BTreeMap::new(),
            aux_states: BTreeMap::new(),
            discovery_filter,
            awaiting: BTreeMap::new(),
            pending: VecDeque::new(),
            pending_entries: BTreeMap::new(),
            draining: BTreeMap::new(),
            commands: VecDeque::new(),
            events: VecDeque::new(),
            connected: false,
            ever_up: false,
            shut_down: false,
            absent_due: None,
            retries: Vec::new(),
            backoff_ms: RETRY_MIN_MS,
            dropped: BTreeSet::new(),
            dropped_discovery: false,
            dropped_due: None,
            sync_floor: None,
            stuck_floor: None,
            requests: BTreeMap::new(),
            seq: 0,
            floor: 0,
            resumed: false,
            no_retain_bit: false,
        })
    }

    /// The configuration in use.
    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// The next command for the MQTT client.
    pub fn poll_command(&mut self) -> Option<Command> {
        self.commands.pop_front()
    }

    /// The next event.
    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// When to call [`handle_timeout`](Self::handle_timeout) next, or `None`
    /// when no timer is armed. The instant is never later than the earliest
    /// deadline, and may be earlier when a deadline was disarmed since the
    /// last `handle_timeout`, which then finds nothing due and recomputes it.
    /// Constant time in the number of devices.
    pub fn poll_timeout(&self) -> Option<Instant> {
        if self.shut_down {
            return None;
        }
        let mut next: Option<Instant> = None;
        let mut consider = |t: Instant| next = Some(next.map_or(t, |n| n.min(t)));
        if self.connected {
            if let (Some(stuck), Some(t)) = (self.cfg.stuck_timeout_ms, self.stuck_floor) {
                consider(t.add_ms(stuck));
            }
            for (due, _) in &self.retries {
                consider(*due);
            }
            if let Some(t) = self.dropped_due {
                consider(t);
            }
            if let Some(t) = self.absent_due {
                consider(t);
            }
        }
        if let Some(t) = self.sync_floor {
            consider(t);
        }
        next
    }

    /// Lowers the sync floor to `t`.
    fn floor_sync(&mut self, t: Instant) {
        self.sync_floor = Some(self.sync_floor.map_or(t, |f| f.min(t)));
    }

    /// Lowers the stuck floor to `t`.
    fn floor_stuck(&mut self, t: Instant) {
        self.stuck_floor = Some(self.stuck_floor.map_or(t, |f| f.min(t)));
    }

    /// Sets both floors to the exact earliest deadlines.
    fn recompute_floors(&mut self) {
        self.sync_floor = self.devices.values().filter_map(|d| d.sync_at).min();
        self.stuck_floor = self
            .awaiting
            .values()
            .map(|a| a.subscribed_at)
            .chain(self.draining.values().map(|b| b.subscribed_at))
            .min();
    }

    /// True when `topic` is in the controller's domain (`{domain}/5/...`, or
    /// any domain at version `5` for [`Domain::Any`]).
    pub fn wants(&self, topic: &str) -> bool {
        match self.named {
            Some(_) => topic.starts_with(self.prefix.as_str()),
            None => {
                let mut levels = topic.splitn(3, '/');
                let _domain = levels.next();
                levels.next() == Some(HOMIE_TOPIC_VERSION) && levels.next().is_some()
            }
        }
    }

    /// True between a link-up and the next link-down, and before
    /// [`shutdown`](Self::shutdown).
    pub fn is_connected(&self) -> bool {
        self.connected && !self.shut_down
    }

    /// The device with this id, once its first valid `$state` has arrived
    /// (convention.md:272). With [`Domain::Any`], an id known in several domains
    /// resolves to the first domain in byte order.
    pub fn device(&self, id: &str) -> Option<&DeviceView> {
        self.find(id).map(|(_, d)| &d.view)
    }

    /// Every device whose first valid `$state` has arrived, in id order.
    pub fn devices(&self) -> impl Iterator<Item = &DeviceView> + '_ {
        self.devices
            .values()
            .filter(|d| d.view.state.is_some())
            .map(|d| &d.view)
    }

    /// Every device with this id whose first valid `$state` has arrived, one
    /// per domain, in domain order. At most one with a [`Domain::Named`]
    /// domain.
    pub fn devices_with_id<'a>(&'a self, id: &'a str) -> impl Iterator<Item = &'a DeviceView> + 'a {
        self.devices
            .range::<dyn KeyRef, _>((Bound::Included(&(id, "") as &dyn KeyRef), Bound::Unbounded))
            .take_while(move |(k, _)| k.id.as_str() == id)
            .filter(|(_, d)| d.view.state.is_some())
            .map(|(_, d)| &d.view)
    }

    /// The device's effective state (convention.md:262-267).
    pub fn effective_state(&self, id: &str) -> Option<DeviceState> {
        self.device(id).and_then(|v| v.effective)
    }

    /// True when the device `root` and every device its description declares
    /// under it, transitively, have published a description. A declared device
    /// counts whatever its `$state`.
    pub fn is_tree_complete(&self, root: &str) -> bool {
        let Some((key, _)) = self.find_any(root) else {
            return false;
        };
        let key = key.clone();
        self.tree_complete(&key)
    }

    /// Queues a `/set` for `addr` (QoS 0, not retained). `Ok` means validated
    /// and queued; nothing confirms delivery (convention.md:474-475).
    ///
    /// QoS 0 follows framework.md:87 for every property. convention.md:45-49
    /// recommends QoS 2 except for `/set` on non-retained properties; the eBus
    /// specification decides where the two differ.
    ///
    /// Checked in order: the link is up; the device and property are known;
    /// the property is settable; the device's effective state is `ready`
    /// unless `opts.allow_not_ready`; unless `opts.unchecked`, the payload
    /// passes [`datatype::parse_set_payload`] with the last valid value as the
    /// step base. The payload is sent as given, except that `""` for a
    /// `string` property is sent as a single `0x00` byte (convention.md:60-71).
    pub fn set(
        &mut self,
        addr: &PropertyAddr,
        payload: &str,
        opts: SetOptions,
    ) -> Result<(), SetError> {
        if !self.is_connected() {
            return Err(SetError::NotConnected);
        }
        let (key, dev) = self
            .find(addr.device.as_str())
            .ok_or(SetError::UnknownDevice)?;
        let desc = dev
            .view
            .description
            .as_deref()
            .ok_or(SetError::UnknownProperty)?;
        let slot = dev
            .view
            .props
            .get(addr.node.as_str())
            .and_then(|m| m.get(addr.property.as_str()))
            .ok_or(SetError::UnknownProperty)?;
        let decl = prop_decl(desc, slot).ok_or(SetError::UnknownProperty)?;
        if !decl.settable {
            return Err(SetError::NotSettable);
        }
        let effective = dev.view.effective.unwrap_or(DeviceState::Init);
        if effective != DeviceState::Ready && !opts.allow_not_ready {
            return Err(SetError::DeviceNotReady(effective));
        }
        if !opts.unchecked {
            let current = slot
                .view
                .last_valid
                .as_deref()
                .and_then(|t| current_of(decl.datatype, t));
            datatype::parse_set_payload(payload, decl.datatype, decl.format.as_deref(), current)
                .map_err(SetError::Invalid)?;
        }
        let topic = crate::topic::property_set(
            &key.domain,
            key.id.as_str(),
            addr.node.as_str(),
            addr.property.as_str(),
        );
        let payload = if payload.is_empty() && decl.datatype == Datatype::String {
            Vec::from(EMPTY_STRING_PAYLOAD)
        } else {
            Vec::from(payload.as_bytes())
        };
        crate::log::info!("settingProperty", topic = topic);
        self.commands.push_back(Command::Publish {
            topic,
            payload,
            qos: QoS::AtMostOnce,
            retain: false,
        });
        Ok(())
    }

    /// Queues `payload` to `{domain}/5/$broadcast/{subtopic}` (QoS 0, not
    /// retained), after checking that every subtopic level is a valid id
    /// (convention.md:531-535).
    pub fn broadcast(&mut self, subtopic: &str, payload: &str) -> Result<(), SetError> {
        if !self.is_connected() {
            return Err(SetError::NotConnected);
        }
        let Some(domain) = &self.named else {
            return Err(SetError::InvalidTopic);
        };
        if !subtopic.split('/').all(is_valid_id) {
            return Err(SetError::InvalidTopic);
        }
        let topic = crate::topic::broadcast(domain, subtopic);
        crate::log::info!("broadcasting", topic = topic);
        self.commands.push_back(Command::Publish {
            topic,
            payload: Vec::from(payload.as_bytes()),
            qos: QoS::AtMostOnce,
            retain: false,
        });
        Ok(())
    }

    /// Unsubscribes every filter the controller holds, except host filters,
    /// and stops: later input is ignored, no timer is armed, and
    /// [`set`](Self::set) returns [`SetError::NotConnected`]. The views stay
    /// readable.
    pub fn shutdown(&mut self) {
        if self.shut_down {
            return;
        }
        crate::log::info!("stoppingController", devices = self.devices.len());
        self.shut_down = true;
        let cmd = self.ledger.release_all();
        self.push(cmd);
        self.holds.clear();
        self.awaiting.clear();
        self.pending.clear();
        self.pending_entries.clear();
        self.draining.clear();
        self.retries.clear();
        self.dropped.clear();
        self.dropped_discovery = false;
        self.dropped_due = None;
        self.absent_due = None;
        self.sync_floor = None;
        self.stuck_floor = None;
        self.requests.clear();
        for d in self.devices.values_mut() {
            d.sync_at = None;
        }
    }

    // ----- lookups -----

    /// A device with a valid `$state`, by id alone.
    fn find(&self, id: &str) -> Option<(&DevKey, &Device)> {
        match &self.named {
            Some(dom) => self
                .devices
                .get_key_value(&(id, &**dom) as &dyn KeyRef)
                .filter(|(_, d)| d.view.state.is_some()),
            None => self
                .devices
                .range::<dyn KeyRef, _>((
                    Bound::Included(&(id, "") as &dyn KeyRef),
                    Bound::Unbounded,
                ))
                .take_while(|(k, _)| k.id.as_str() == id)
                .find(|(_, d)| d.view.state.is_some()),
        }
    }

    /// Any tracked device by id alone, heard or not.
    fn find_any(&self, id: &str) -> Option<(&DevKey, &Device)> {
        match &self.named {
            Some(dom) => self.devices.get_key_value(&(id, &**dom) as &dyn KeyRef),
            None => self
                .devices
                .range::<dyn KeyRef, _>((
                    Bound::Included(&(id, "") as &dyn KeyRef),
                    Bound::Unbounded,
                ))
                .next()
                .filter(|(k, _)| k.id.as_str() == id),
        }
    }

    fn key_of(&self, id: &str, domain: &str) -> Option<DevKey> {
        self.devices
            .get_key_value(&(id, domain) as &dyn KeyRef)
            .map(|(k, _)| k.clone())
    }

    /// The key for `id` in the configured named domain.
    fn named_key(&self, id: &Id) -> Option<DevKey> {
        self.named.as_ref().map(|d| DevKey {
            id: id.clone(),
            domain: d.clone(),
        })
    }

    // ----- filters and the ledger -----

    fn topic_filter(domain: &str, id: &str, suffix: &str) -> String {
        format!("{domain}/{HOMIE_TOPIC_VERSION}/{id}/{suffix}")
    }

    fn device_filter(key: &DevKey, suffix: &str) -> String {
        Self::topic_filter(&key.domain, key.id.as_str(), suffix)
    }

    /// `$state` (except in wildcard mode) and `$description`, plus the root's
    /// `$state` in device mode.
    fn attr_filters(&self, key: &DevKey) -> Vec<String> {
        let mut out = Vec::with_capacity(3);
        if !matches!(self.cfg.discovery, Discovery::Wildcard) {
            out.push(Self::device_filter(key, "$state"));
        }
        out.push(Self::device_filter(key, "$description"));
        if let Some((_, f)) = self.devices.get(key).and_then(|d| d.root_filter.clone()) {
            out.push(f);
        }
        out
    }

    /// The property filters for `nodes` (`None`: the whole device).
    fn prop_filters(key: &DevKey, nodes: Option<&[Id]>) -> Vec<String> {
        match nodes {
            None => WHOLE_DEVICE
                .iter()
                .map(|s| Self::device_filter(key, s))
                .collect(),
            Some(nodes) => nodes
                .iter()
                .flat_map(|n| {
                    [
                        Self::device_filter(key, &format!("{n}/+")),
                        Self::device_filter(key, &format!("{n}/+/$target")),
                    ]
                })
                .collect(),
        }
    }

    fn push(&mut self, cmd: Option<Command>) {
        let Some(cmd) = cmd else {
            return;
        };
        if let Command::Subscribe { filters } = &cmd {
            self.number_subscribe(filters);
        }
        self.commands.push_back(cmd);
    }

    /// Holds `attrs` and `props` for `key` and queues one SUBSCRIBE for them,
    /// in the order given, followed with `marker` by its `$description` (the
    /// end-of-burst marker). A filter the device already holds is sent again
    /// without a new holder, so the broker replays its retained messages.
    fn hold(&mut self, key: &DevKey, attrs: Vec<String>, props: Vec<String>, marker: bool) {
        let qos = self.cfg.subscribe_qos;
        let holds = self.holds.entry(key.clone()).or_default();
        let mut order: Vec<String> = Vec::with_capacity(attrs.len() + props.len() + 1);
        let mut fresh = Vec::new();
        let mut again = Vec::new();
        for (list, filters) in [(&mut holds.attrs, attrs), (&mut holds.props, props)] {
            for f in filters {
                order.push(f.clone());
                if list.contains(&f) {
                    again.push(f);
                } else {
                    list.push(f.clone());
                    fresh.push((f, qos));
                }
            }
        }
        if marker {
            let f = Self::device_filter(key, "$description");
            order.push(f.clone());
            again.push(f);
        }
        let mut filters = Vec::new();
        for cmd in [
            self.ledger.subscribe(fresh),
            self.ledger.resubscribe(again.iter()),
        ] {
            if let Some(Command::Subscribe { filters: more }) = cmd {
                for (f, q) in more {
                    match filters
                        .iter_mut()
                        .find(|(g, _): &&mut (String, QoS)| *g == f)
                    {
                        Some(e) => e.1 = e.1.max(q),
                        None => filters.push((f, q)),
                    }
                }
            }
        }
        if filters.is_empty() {
            return;
        }
        filters.sort_by_key(|(f, _)| order.iter().position(|o| o == f));
        self.push(Some(Command::Subscribe { filters }));
    }

    /// Sends the SUBSCRIBE again for filters the device holds, so the broker
    /// replays their retained messages.
    fn resubscribe(&mut self, filters: &[String]) {
        let cmd = self.ledger.resubscribe(filters.iter());
        self.push(cmd);
    }

    fn release_props(&mut self, key: &DevKey) {
        let Some(h) = self.holds.get_mut(key) else {
            return;
        };
        let props = core::mem::take(&mut h.props);
        let cmd = self.ledger.unsubscribe(props);
        self.push(cmd);
    }

    fn release_all(&mut self, key: &DevKey) {
        let Some(h) = self.holds.remove(key) else {
            return;
        };
        let mut all = h.props;
        all.extend(h.attrs);
        let cmd = self.ledger.unsubscribe(all);
        self.push(cmd);
    }

    // ----- state -----

    /// `$state` of the device's declared root.
    fn root_state(&self, key: &DevKey, dev: &Device) -> Option<DeviceState> {
        let root = dev.root_id()?;
        self.devices
            .get(&(root, &*key.domain) as &dyn KeyRef)
            .and_then(|r| r.view.state)
            .or_else(|| self.aux_states.get(root).copied())
    }

    fn effective_of(&self, key: &DevKey) -> Option<DeviceState> {
        let dev = self.devices.get(key)?;
        let own = dev.view.state?;
        Some(effective_state(self.root_state(key, dev), own))
    }
}

/// The declaration of the property a slot stands for.
fn prop_decl<'a>(
    desc: &'a crate::description::DeviceDescription,
    slot: &Slot,
) -> Option<&'a PropertyDescription> {
    desc.nodes
        .get(slot.node_idx)?
        .1
        .properties
        .get(slot.prop_idx)
        .map(|(_, p)| p)
}

/// The step base [`datatype::parse_set_payload`] takes, from a value's text.
fn current_of(datatype: Datatype, text: &str) -> Option<Current> {
    match datatype {
        Datatype::Integer => text.parse().ok().map(Current::Integer),
        Datatype::Float => text.parse().ok().map(Current::Float),
        _ => None,
    }
}
