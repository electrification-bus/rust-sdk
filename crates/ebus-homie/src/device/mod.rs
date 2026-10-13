//! The Homie 5 device role as a sans-IO state machine.
//!
//! One [`Tree`] owns one root device and its descendants, which share the
//! root's MQTT connection and will (convention.md:56-58). Input arrives
//! through the `handle_*` methods and output leaves through the `poll_*`
//! methods; time comes only from the caller's [`Instant`]. One owner drives a
//! [`Tree`]:
//!
//! 1. register [`Tree::last_will`] as the connection's only will;
//! 2. call [`handle_link`](Tree::handle_link) on every CONNACK and every lost
//!    connection: every link-up republishes the whole tree;
//! 3. pass every received message the tree [`wants`](Tree::wants) to
//!    [`handle_message`](Tree::handle_message);
//! 4. call [`handle_timeout`](Tree::handle_timeout) once the instant
//!    [`poll_timeout`](Tree::poll_timeout) returns has passed;
//! 5. after each call, hand every [`poll_command`](Tree::poll_command) result
//!    to the MQTT client, in order, and report one it could not accept
//!    through [`handle_command_failed`](Tree::handle_command_failed); read
//!    events with [`poll_event`](Tree::poll_event).
//!
//! A value equal to the one held publishes nothing. Nothing is published
//! before the first link-up; values set before it go out in its refresh.

mod model;
#[cfg(test)]
mod tests;
mod value;

pub use model::{BuildError, DevIdx, DeviceSpec, NodeIdx, NodeSpec, PropSpec, Reconfigure, SlotId};
pub use value::{Color, Datetime, HomieEnum, HomieValue, ValueError, ValueRef};

use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet, VecDeque},
    format,
    string::{String, ToString},
    vec::Vec,
};
use core::fmt;

use ebus_mqtt::{Command, FilterLedger, Handled, Instant, LastWill, Link, Message, QoS, Receipt};

use crate::datatype::{
    self, Current, Datatype, Decimal, Problem, SetValue, check_color, check_datetime,
};
use crate::id::is_valid_id;
use crate::payload::encode_str;
use crate::state::DeviceState;
use crate::topic::{self, TopicKind};
use crate::{EBUS_DOMAIN, HOMIE_TOPIC_VERSION};
use model::{Fp, Model, Slot};

/// Retry backoff for a refused or failed subscription (client.py:155).
const RETRY_MIN_MS: u64 = 1_000;
const RETRY_MAX_MS: u64 = 30_000;

/// Re-asserts of the root's `$state` allowed in one link-up.
const MAX_REASSERTS: u8 = 3;

/// Device role settings. Start from [`TreeConfig::default`] and set the
/// fields.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TreeConfig {
    /// Topic domain, `ebus` by default (framework.md:85).
    pub domain: String,
    /// QoS of retained publishes, the will and the `/set` subscriptions;
    /// QoS 2 by default (convention.md:45).
    pub qos: QoS,
    /// A larger `$description` is refused; 256 KiB by default.
    pub max_description_bytes: usize,
    /// Subscribe to the root's `$state` and re-assert it over a stale `lost`;
    /// off by default. Turn it on for a broker that publishes a replaced
    /// session's will after the new session's CONNACK; mosquitto publishes it
    /// before, so the device's own refresh already overwrites it.
    pub watch_own_state: bool,
    /// The host reports SUBACKs, `$description` writes and acknowledgments of
    /// zero-length publishes through [`Tree::handle_receipt`]; off by default.
    pub receipts: bool,
    /// Topics kept for clearing again after a reconnect; 4096 by default.
    pub max_tombstones: usize,
    /// Subscribe to `{domain}/5/$broadcast/#`; off by default.
    pub broadcast: bool,
    /// Filters the host also subscribes on the same client; never
    /// unsubscribed.
    pub host_filters: Vec<String>,
}

impl Default for TreeConfig {
    fn default() -> Self {
        TreeConfig {
            domain: String::from(EBUS_DOMAIN),
            qos: QoS::ExactlyOnce,
            max_description_bytes: 256 * 1024,
            watch_own_state: false,
            receipts: false,
            max_tombstones: 4096,
            broadcast: false,
            host_filters: Vec::new(),
        }
    }
}

/// A device's state as the application sets it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Presence {
    /// `ready`.
    Ready,
    /// `sleeping` (convention.md:282-283).
    Sleeping,
    /// `disconnected`; child devices only.
    Disconnected,
    /// `lost`; child devices only. The root's comes from
    /// [`Tree::declare_lost`].
    Lost,
}

impl Presence {
    fn state(self) -> DeviceState {
        match self {
            Presence::Ready => DeviceState::Ready,
            Presence::Sleeping => DeviceState::Sleeping,
            Presence::Disconnected => DeviceState::Disconnected,
            Presence::Lost => DeviceState::Lost,
        }
    }
}

/// What a value update did.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Change {
    /// Equal to what is held: nothing will be published.
    Unchanged,
    /// Queued for publishing.
    Queued,
    /// Stored while the link is down; the next refresh publishes it.
    Cached,
    /// An event while the link is down, or any update after shutdown:
    /// discarded.
    Dropped,
}

/// Why an alert was refused; nothing was published.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AlertError {
    /// Not a valid id (convention.md:519).
    InvalidId,
    /// An empty message, which would delete the alert (convention.md:519, 527).
    EmptyMessage,
    /// The device was removed.
    Gone,
}

impl fmt::Display for AlertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AlertError::InvalidId => "invalidId",
            AlertError::EmptyMessage => "emptyMessage",
            AlertError::Gone => "gone",
        })
    }
}

impl core::error::Error for AlertError {}

/// Something the application acts on.
///
/// Each struct variant is `#[non_exhaustive]`, so a pattern names the fields
/// it needs and ends with `..`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum DeviceEvent {
    /// An accepted `/set`.
    #[non_exhaustive]
    Set {
        /// The property.
        slot: SlotId,
        /// Validated and step-rounded.
        value: SetValue,
        /// The payload as received.
        raw: Box<[u8]>,
    },
    /// A `$broadcast` message, with [`TreeConfig::broadcast`].
    #[non_exhaustive]
    Broadcast {
        /// Everything after `$broadcast/`.
        subtopic: String,
        /// The payload as received.
        payload: Box<[u8]>,
    },
    /// The root's `$state` was re-published over a `lost` the device did not
    /// declare, with [`TreeConfig::watch_own_state`].
    #[non_exhaustive]
    StateReasserted {
        /// Re-asserts in this link-up.
        attempt: u8,
        /// The cap was reached; later `lost` messages are left alone until
        /// the next link-up (`reason=stateOverwritten`).
        gave_up: bool,
    },
    /// A link change, after the commands it causes are queued.
    Link(Link),
}

/// How [`Tree::shutdown`] ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Shutdown {
    /// `disconnected` for every device, root last; the host then sends
    /// DISCONNECT once they are acknowledged.
    Announce,
    /// Nothing; the host drops the connection so the broker fires the will.
    Silent,
    /// Every topic of the tree cleared, the root's `$state` first; the host
    /// then sends DISCONNECT once they are acknowledged.
    Remove,
}

/// When a `$state` step publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateMode {
    /// Always: the refresh and shutdown.
    Forced,
    /// Only to close an `init` this device published.
    IfInit,
    /// A presence change, while its ticket is current and the state differs
    /// from the one last published.
    Presence(u64),
}

/// One unit of output, turned into a command when polled so it carries the
/// latest value.
#[derive(Clone, Debug)]
enum Step {
    Cmd(Command),
    Subscribe {
        filters: Vec<(String, QoS)>,
        gate: Option<u64>,
    },
    Unsubscribe(Vec<String>),
    Clear(String),
    Init {
        dev: u16,
        reconfig: bool,
    },
    Desc(u16),
    Value {
        slot: u32,
        ticket: u64,
    },
    Target {
        slot: u32,
        ticket: u64,
    },
    Alert {
        dev: u16,
        id: String,
        ticket: u64,
    },
    State {
        dev: u16,
        mode: StateMode,
    },
    /// Holds every later step until the SUBACK of the SUBSCRIBE with this
    /// gate is reported.
    Gate(u64),
}

/// The Homie 5 device role for one tree; see the [module documentation](self).
#[derive(Debug)]
pub struct Tree {
    cfg: TreeConfig,
    m: Model,
    ledger: FilterLedger,
    root_state_filter: Option<String>,
    broadcast_filter: Option<String>,
    steps: VecDeque<Step>,
    urgent: VecDeque<Step>,
    events: VecDeque<DeviceEvent>,
    connected: bool,
    shut_down: bool,
    /// Tickets below it belong to steps of an earlier link.
    epoch: u64,
    next_ticket: u64,
    /// Gates whose SUBACK has not been reported.
    gates: BTreeSet<u64>,
    /// With receipts: device SUBSCRIBEs polled and not yet answered.
    pending_subs: VecDeque<(Vec<String>, Option<u64>)>,
    retries: Vec<(Instant, Step)>,
    backoff_ms: u64,
    /// Topic to insertion number, and the reverse, oldest first.
    tombs: BTreeMap<String, u64>,
    tomb_order: BTreeMap<u64, String>,
    tomb_seq: u64,
    /// With receipts: clears polled on this link and not yet acknowledged.
    unacked: BTreeMap<String, u32>,
    rejected_sets: u64,
    reasserts: u8,
    gave_up: bool,
    /// This link-up's closing root `$state` has been polled.
    root_closed: bool,
    scratch: String,
}

/// Bytes a slot's payload buffer starts with: the longest `integer` or
/// `float` payload is 24.
const PAYLOAD_ROOM: usize = 24;

/// What a value is checked against before it is published: the property's
/// datatype, `$format` range, enum values and color types. A copy, for
/// checking on another thread; it does not follow a later `set_format`.
#[derive(Clone, Debug)]
pub struct ValueCheck {
    datatype: Datatype,
    retained: bool,
    target: bool,
    settable: bool,
    imin: Option<i64>,
    imax: Option<i64>,
    fmin: Option<(f64, Decimal)>,
    fmax: Option<(f64, Decimal)>,
    variants: usize,
    format: Option<String>,
}

impl ValueCheck {
    pub(in crate::device) fn of(s: &Slot) -> Self {
        ValueCheck {
            datatype: s.datatype,
            retained: s.spec.retained,
            target: s.spec.target,
            settable: s.spec.settable,
            imin: s.imin,
            imax: s.imax,
            fmin: s.fmin.clone(),
            fmax: s.fmax.clone(),
            variants: s.variants.len(),
            format: s.spec.format.clone(),
        }
    }

    /// The property's datatype.
    pub fn datatype(&self) -> Datatype {
        self.datatype
    }

    /// The property is retained (not an event).
    pub fn is_retained(&self) -> bool {
        self.retained
    }

    /// The property publishes `$target`.
    pub fn is_target(&self) -> bool {
        self.target
    }

    /// The property accepts `/set`.
    pub fn is_settable(&self) -> bool {
        self.settable
    }

    /// The effective `$format`.
    pub fn format(&self) -> Option<&str> {
        self.format.as_deref()
    }

    /// The check [`Tree::set_value`] and [`Tree::emit`] run first: finite,
    /// within the `$format` range, enum and color membership.
    pub fn check(&self, v: ValueRef<'_>) -> Result<(), ValueError> {
        check_limits(&self.limits(), v).map(|_| ())
    }

    fn limits(&self) -> Limits<'_> {
        Limits {
            datatype: self.datatype,
            imin: self.imin,
            imax: self.imax,
            fmin: self.fmin.as_ref(),
            fmax: self.fmax.as_ref(),
            variants: self.variants,
            format: self.format.as_deref(),
        }
    }
}

/// A tree under construction. Each change applies at once and is checked
/// alone; the descriptions are built and checked by round trip once per
/// device in [`finish`](Self::finish), so building a large tree costs one
/// description per device instead of one per change.
#[derive(Debug)]
pub struct TreeBuilder {
    cfg: TreeConfig,
    m: Model,
}

impl TreeBuilder {
    /// A tree with one root device and no nodes.
    pub fn new(cfg: TreeConfig, root_id: &str, root: DeviceSpec) -> Result<Self, BuildError> {
        if !is_valid_id(&cfg.domain) {
            return Err(BuildError::InvalidId);
        }
        let prefix = format!("{}/{HOMIE_TOPIC_VERSION}/", cfg.domain);
        let m = Model::new(prefix, root_id, root)?;
        Ok(TreeBuilder { cfg, m })
    }

    /// The configuration in use.
    pub fn config(&self) -> &TreeConfig {
        &self.cfg
    }

    /// Sets [`TreeConfig::receipts`], for a host that decides it after the
    /// tree is built.
    pub fn set_receipts(&mut self, on: bool) {
        self.cfg.receipts = on;
    }

    /// The root device.
    pub fn root(&self) -> DevIdx {
        DevIdx(self.m.root)
    }

    /// The model, for adding children, nodes and properties. A refused change
    /// changes nothing; there is no rollback of earlier ones.
    pub fn edit(&mut self) -> Reconfigure<'_> {
        Reconfigure { m: &mut self.m }
    }

    /// The check a property's values pass.
    pub fn value_check(&self, slot: SlotId) -> Option<ValueCheck> {
        self.m.slot(slot).map(ValueCheck::of)
    }

    /// A property's value topic.
    pub fn topic(&self, slot: SlotId) -> Option<&str> {
        self.m.slot(slot).map(|s| s.topic.as_str())
    }

    /// Builds and checks every device's description and returns the tree.
    pub fn finish(self) -> Result<Tree, BuildError> {
        let TreeBuilder { cfg, mut m } = self;
        for d in m.preorder() {
            m.rebuild(d, cfg.max_description_bytes)?;
        }
        m.begin();
        Tree::from_model(cfg, m)
    }
}

impl Tree {
    /// A tree with one root device and no nodes. Nothing is published until
    /// the first [`handle_link`](Self::handle_link) `Up`.
    pub fn new(cfg: TreeConfig, root_id: &str, root: DeviceSpec) -> Result<Self, BuildError> {
        TreeBuilder::new(cfg, root_id, root)?.finish()
    }

    fn from_model(cfg: TreeConfig, m: Model) -> Result<Self, BuildError> {
        let prefix = m.prefix.clone();
        let root_id = m
            .dev(m.root)
            .map(|d| d.id.to_string())
            .ok_or(BuildError::Gone)?;
        let mut ledger = FilterLedger::new(cfg.host_filters.iter().cloned());
        let root_state_filter = cfg
            .watch_own_state
            .then(|| format!("{prefix}{root_id}/$state"));
        let broadcast_filter = cfg.broadcast.then(|| format!("{prefix}$broadcast/#"));
        let mut filters: Vec<(String, QoS)> = m
            .preorder()
            .into_iter()
            .filter_map(|d| m.dev(d))
            .map(|d| (d.set_filter.clone(), cfg.qos))
            .collect();
        filters.extend(root_state_filter.iter().map(|f| (f.clone(), cfg.qos)));
        filters.extend(broadcast_filter.iter().map(|f| (f.clone(), cfg.qos)));
        // Nothing is on the wire until a link-up resubscribes it.
        let _ = ledger.subscribe(filters);
        ledger.link_up(false);
        Ok(Tree {
            cfg,
            m,
            ledger,
            root_state_filter,
            broadcast_filter,
            steps: VecDeque::new(),
            urgent: VecDeque::new(),
            events: VecDeque::new(),
            connected: false,
            shut_down: false,
            epoch: 1,
            next_ticket: 1,
            gates: BTreeSet::new(),
            pending_subs: VecDeque::new(),
            retries: Vec::new(),
            backoff_ms: RETRY_MIN_MS,
            tombs: BTreeMap::new(),
            tomb_order: BTreeMap::new(),
            tomb_seq: 0,
            unacked: BTreeMap::new(),
            rejected_sets: 0,
            reasserts: 0,
            gave_up: false,
            root_closed: false,
            scratch: String::new(),
        })
    }

    /// The configuration in use.
    pub fn config(&self) -> &TreeConfig {
        &self.cfg
    }

    /// The root device.
    pub fn root(&self) -> DevIdx {
        DevIdx(self.m.root)
    }

    /// The will to register on the connection: the root's `$state` set to
    /// `lost`, retained, at the tree QoS (convention.md:56-58).
    pub fn last_will(&self) -> LastWill {
        LastWill::new(
            self.m
                .dev(self.m.root)
                .map(|d| d.state_topic.clone())
                .unwrap_or_default(),
            b"lost".to_vec(),
            self.cfg.qos,
            true,
        )
    }

    /// Every device, in pre-order, root first.
    pub fn devices(&self) -> impl Iterator<Item = DevIdx> + '_ {
        self.m.preorder().into_iter().map(DevIdx)
    }

    /// The check a property's values pass; `None` once it is removed.
    pub fn value_check(&self, slot: SlotId) -> Option<ValueCheck> {
        self.m.slot(slot).map(ValueCheck::of)
    }

    /// A property's value topic; `None` once it is removed.
    pub fn topic(&self, slot: SlotId) -> Option<&str> {
        self.m.slot(slot).map(|s| s.topic.as_str())
    }

    /// The device with this id.
    pub fn device(&self, id: &str) -> Option<DevIdx> {
        self.m.by_id.get(id).copied().map(DevIdx)
    }

    /// The `$description` last committed for `dev`.
    pub fn description(&self, dev: DevIdx) -> Option<&str> {
        self.m.dev(dev.0).map(|d| d.desc.as_str())
    }

    /// The state `dev` publishes.
    pub fn state(&self, dev: DevIdx) -> Option<DeviceState> {
        self.m.dev(dev.0).map(|d| d.state)
    }

    /// True between a link-up and the next link-down, before
    /// [`shutdown`](Self::shutdown).
    pub fn is_connected(&self) -> bool {
        self.connected && !self.shut_down
    }

    /// `/set` messages refused with `propertySetNotSettable` or
    /// `propertySetRejected`.
    pub fn rejected_sets(&self) -> u64 {
        self.rejected_sets
    }

    /// Topics held for clearing again after a reconnect.
    pub fn tombstones(&self) -> impl Iterator<Item = &str> + '_ {
        self.tomb_order.values().map(String::as_str)
    }

    /// The step base `/set` rounding uses: an `integer` or `float` value held.
    pub fn current(&self, slot: SlotId) -> Option<Current> {
        current_of(self.m.slot(slot)?)
    }

    /// The next event.
    pub fn poll_event(&mut self) -> Option<DeviceEvent> {
        self.events.pop_front()
    }

    fn ticket(&mut self) -> u64 {
        let t = self.next_ticket;
        self.next_ticket += 1;
        t
    }

    fn live(&self) -> bool {
        self.connected && !self.shut_down
    }

    // ----- link -----

    /// A CONNACK (`Up`) or a lost connection (`Down`). Every `Up` republishes
    /// the whole tree, whatever `session_present` says.
    pub fn handle_link(&mut self, _now: Instant, link: Link) {
        if self.shut_down {
            return;
        }
        match link {
            Link::Up { .. } => {
                if self.connected {
                    self.reset_link();
                }
                self.connected = true;
                self.refresh();
            }
            Link::Down => {
                if !self.connected {
                    return;
                }
                self.reset_link();
            }
            _ => return,
        }
        self.events.push_back(DeviceEvent::Link(link));
    }

    fn reset_link(&mut self) {
        self.connected = false;
        self.steps.clear();
        self.urgent.clear();
        self.gates.clear();
        self.pending_subs.clear();
        self.retries.clear();
        self.unacked.clear();
        self.root_closed = false;
        self.reasserts = 0;
        self.gave_up = false;
        for dev in self.m.devices.iter_mut().flatten() {
            if dev.awaiting_close {
                dev.memo = None;
                dev.awaiting_close = false;
            }
            dev.desc_inflight.clear();
            dev.init_open = false;
            dev.published_state = None;
        }
    }

    fn refresh(&mut self) {
        self.epoch = self.next_ticket;
        self.backoff_ms = RETRY_MIN_MS;
        self.ledger.link_up(false);
        let order = self.m.preorder();
        let mut filters: Vec<String> = order
            .iter()
            .filter_map(|d| self.m.dev(*d))
            .map(|d| d.set_filter.clone())
            .collect();
        filters.extend(self.root_state_filter.iter().cloned());
        filters.extend(self.broadcast_filter.iter().cloned());
        let cmd = self.ledger.resubscribe(&filters);
        let gate = self.push_subscribe_cmd(cmd);
        // A removed device's topics are cleared after its parent's new
        // description and state (convention.md:633-641).
        let (tombs, orphans): (Vec<String>, Vec<String>) = self
            .tomb_order
            .values()
            .cloned()
            .partition(|t| self.in_tree(t));
        self.steps.extend(tombs.into_iter().map(Step::Clear));
        for &d in &order {
            self.push_device(d, false);
        }
        if let Some(g) = gate {
            self.steps.push_back(Step::Gate(g));
        }
        for d in self.m.postorder() {
            self.steps.push_back(Step::State {
                dev: d,
                mode: StateMode::Forced,
            });
        }
        self.steps.extend(orphans.into_iter().map(Step::Clear));
    }

    /// True when `topic` is under a device in the tree.
    fn in_tree(&self, topic: &str) -> bool {
        topic
            .strip_prefix(self.m.prefix.as_str())
            .and_then(|rest| rest.split('/').next())
            .is_some_and(|id| self.m.by_id.contains_key(id))
    }

    /// Queues `cmd` (a SUBSCRIBE from the ledger) with a gate when receipts
    /// are on; returns the gate.
    fn push_subscribe_cmd(&mut self, cmd: Option<Command>) -> Option<u64> {
        let Some(Command::Subscribe { filters, .. }) = cmd else {
            return None;
        };
        let gate = self.cfg.receipts.then(|| self.ticket());
        if let Some(g) = gate {
            self.gates.insert(g);
        }
        self.steps.push_back(Step::Subscribe { filters, gate });
        gate
    }

    /// `init` when due, `$description`, then each `$target` before its value,
    /// and every alert.
    fn push_device(&mut self, d: u16, reconfig: bool) {
        self.steps.push_back(Step::Init { dev: d, reconfig });
        self.steps.push_back(Step::Desc(d));
        for s in self.m.dev_slots(d) {
            self.push_slot_refresh(s);
        }
        let ids: Vec<String> = self
            .m
            .dev(d)
            .map(|x| x.alerts.keys().cloned().collect())
            .unwrap_or_default();
        for id in ids {
            let ticket = self.ticket();
            if let Some(dev) = self.m.dev_mut(d) {
                dev.alert_tickets.insert(id.clone(), ticket);
            }
            self.steps.push_back(Step::Alert { dev: d, id, ticket });
        }
    }

    fn push_slot_refresh(&mut self, s: u32) {
        let Some(slot) = self.m.slot_at(s) else {
            return;
        };
        if !slot.spec.retained {
            return;
        }
        let (has_target, has_value) = (slot.target.is_some(), slot.fp.is_some());
        if has_target {
            let ticket = self.ticket();
            if let Some(slot) = self.m.slot_at_mut(s) {
                slot.target_ticket = ticket;
            }
            self.steps.push_back(Step::Target { slot: s, ticket });
        }
        if has_value {
            let ticket = self.ticket();
            if let Some(slot) = self.m.slot_at_mut(s) {
                slot.value_ticket = ticket;
            }
            self.steps.push_back(Step::Value { slot: s, ticket });
        }
    }

    // ----- output -----

    /// The next command for the MQTT client. Payloads are read when polled,
    /// so a value changed before its turn goes out once, at its latest value.
    pub fn poll_command(&mut self) -> Option<Command> {
        loop {
            let step = match self.urgent.pop_front() {
                Some(s) => s,
                None => {
                    if let Some(Step::Gate(g)) = self.steps.front() {
                        if self.gates.contains(g) {
                            return None;
                        }
                    }
                    self.steps.pop_front()?
                }
            };
            if let Some(cmd) = self.materialize(step) {
                return Some(cmd);
            }
        }
    }

    fn retained(&self, topic: String, payload: Vec<u8>) -> Command {
        Command::publish(topic, payload, self.cfg.qos, true)
    }

    fn untomb(&mut self, topic: &str) {
        if let Some(seq) = self.tombs.remove(topic) {
            self.tomb_order.remove(&seq);
        }
    }

    fn materialize(&mut self, step: Step) -> Option<Command> {
        match step {
            Step::Cmd(c) => Some(c),
            Step::Gate(_) => None,
            Step::Subscribe { filters, gate } => {
                if self.cfg.receipts {
                    let names = filters.iter().map(|(f, _)| f.clone()).collect();
                    self.pending_subs.push_back((names, gate));
                }
                Some(Command::subscribe(filters))
            }
            Step::Unsubscribe(filters) => Some(Command::unsubscribe(filters)),
            Step::Clear(topic) => {
                if !self.tombs.contains_key(&topic) {
                    return None;
                }
                if self.cfg.receipts {
                    *self.unacked.entry(topic.clone()).or_insert(0) += 1;
                }
                Some(self.retained(topic, Vec::new()))
            }
            Step::Init { dev, reconfig } => {
                let d = self.m.dev_mut(dev)?;
                let due = d.hash != d.memo
                    && (!reconfig || matches!(d.state, DeviceState::Ready | DeviceState::Sleeping));
                if !due {
                    return None;
                }
                d.init_open = true;
                d.awaiting_close = true;
                d.published_state = Some(DeviceState::Init);
                let topic = d.state_topic.clone();
                self.untomb(&topic);
                Some(self.retained(topic, b"init".to_vec()))
            }
            Step::Desc(dev) => {
                let receipts = self.cfg.receipts;
                let d = self.m.dev_mut(dev)?;
                let hash = d.hash?;
                if d.memo != Some(hash) {
                    d.awaiting_close = true;
                }
                if receipts {
                    d.desc_inflight.push_back(hash);
                } else {
                    d.memo = Some(hash);
                }
                let (topic, payload) = (d.desc_topic.clone(), d.desc.as_bytes().to_vec());
                self.untomb(&topic);
                Some(self.retained(topic, payload))
            }
            Step::Value { slot, ticket } => {
                let s = self.m.slot_at_mut(slot)?;
                if s.value_ticket != ticket || s.fp.is_none() {
                    return None;
                }
                s.value_ticket = 0;
                s.sent.clear();
                s.sent.push_str(&s.payload);
                s.sent_valid = true;
                let (topic, payload) = (s.topic.clone(), s.payload.as_bytes().to_vec());
                self.untomb(&topic);
                Some(self.retained(topic, payload))
            }
            Step::Target { slot, ticket } => {
                let s = self.m.slot_at_mut(slot)?;
                if s.target_ticket != ticket {
                    return None;
                }
                s.target_ticket = 0;
                let topic = s.target_topic.clone()?;
                let payload = s.target.clone()?;
                self.untomb(&topic);
                Some(self.retained(topic, payload))
            }
            Step::Alert { dev, id, ticket } => {
                let prefix = &self.m.prefix;
                let d = self.m.devices.get_mut(usize::from(dev))?.as_mut()?;
                if d.alert_tickets.get(&id) != Some(&ticket) {
                    return None;
                }
                d.alert_tickets.remove(&id);
                let msg = d.alerts.get(&id)?.as_bytes().to_vec();
                let topic = format!("{prefix}{}/$alert/{id}", d.id);
                self.untomb(&topic);
                Some(self.retained(topic, msg))
            }
            Step::State { dev, mode } => {
                let root = dev == self.m.root;
                let d = self.m.dev_mut(dev)?;
                let emit = match mode {
                    StateMode::Forced => true,
                    StateMode::IfInit => d.init_open,
                    StateMode::Presence(t) => {
                        if d.state_ticket != t {
                            return None;
                        }
                        d.state_ticket = 0;
                        d.published_state != Some(d.state)
                    }
                };
                if !matches!(mode, StateMode::Presence(_)) {
                    d.awaiting_close = false;
                }
                if !emit {
                    return None;
                }
                d.init_open = false;
                d.awaiting_close = false;
                d.published_state = Some(d.state);
                let (topic, payload) =
                    (d.state_topic.clone(), d.state.as_str().as_bytes().to_vec());
                if root {
                    self.root_closed = true;
                }
                self.untomb(&topic);
                Some(self.retained(topic, payload))
            }
        }
    }

    /// Enters `topic` in the tombstone set and, while up, queues its clear.
    fn issue_clear(&mut self, topic: String) {
        self.untomb(&topic);
        self.tomb_seq += 1;
        self.tombs.insert(topic.clone(), self.tomb_seq);
        self.tomb_order.insert(self.tomb_seq, topic.clone());
        while self.tombs.len() > self.cfg.max_tombstones {
            let Some((_, oldest)) = self.tomb_order.pop_first() else {
                break;
            };
            crate::log::warn!("tombstonesOverflow", topic = oldest);
            self.tombs.remove(&oldest);
        }
        if self.connected {
            self.steps.push_back(Step::Clear(topic));
        }
    }

    // ----- values -----

    fn status(&self) -> Change {
        if self.connected {
            Change::Queued
        } else {
            Change::Cached
        }
    }

    /// Checks `v` against the slot and encodes it into the scratch buffer.
    fn encode(&mut self, slot: SlotId, v: ValueRef<'_>) -> Result<Fp, ValueError> {
        let s = self.m.slot(slot).ok_or(ValueError::Gone)?;
        let fp = check(s, v)?;
        let mut out = core::mem::take(&mut self.scratch);
        out.clear();
        let r = write_value(&mut out, s, v);
        self.scratch = out;
        r.map(|()| fp)
    }

    /// Stores the scratch payload in the slot when it differs; true when it
    /// did.
    fn store(&mut self, slot: SlotId, fp: Fp) -> bool {
        let Some(s) = self.m.slot_mut(slot) else {
            return false;
        };
        if s.fp.is_some() && ((s.fp == Some(fp) && fp != Fp::Bytes) || s.payload == self.scratch) {
            s.fp = Some(fp);
            return false;
        }
        // A copy, not a swap: each buffer keeps the capacity it grew to, and
        // a number's first payload gets room for any later one.
        s.payload.clear();
        s.payload.reserve(self.scratch.len().max(PAYLOAD_ROOM));
        s.payload.push_str(&self.scratch);
        s.fp = Some(fp);
        true
    }

    /// Queues the slot's value; returns false when the step was cancelled
    /// because the value is back to the one last published.
    fn enqueue_value(&mut self, slot: SlotId) -> bool {
        if !self.connected {
            return true;
        }
        let epoch = self.epoch;
        let ticket = self.next_ticket;
        let Some(s) = self.m.slot_mut(slot) else {
            return false;
        };
        if s.value_ticket != 0 && s.value_ticket >= epoch {
            if s.sent_valid && s.payload == s.sent {
                s.value_ticket = 0;
                return false;
            }
            return true;
        }
        s.value_ticket = ticket;
        self.next_ticket += 1;
        self.steps.push_back(Step::Value {
            slot: slot.index,
            ticket,
        });
        true
    }

    /// Queues the slot's `$target`, moving a queued value step behind it.
    fn enqueue_target(&mut self, slot: SlotId) {
        if !self.connected {
            return;
        }
        let epoch = self.epoch;
        let Some(s) = self.m.slot(slot) else { return };
        let target_queued = s.target_ticket != 0 && s.target_ticket >= epoch;
        let value_queued = s.value_ticket != 0 && s.value_ticket >= epoch;
        if target_queued {
            return;
        }
        let t = self.ticket();
        self.steps.push_back(Step::Target {
            slot: slot.index,
            ticket: t,
        });
        let v = value_queued.then(|| self.ticket());
        if let Some(v) = v {
            self.steps.push_back(Step::Value {
                slot: slot.index,
                ticket: v,
            });
        }
        if let Some(s) = self.m.slot_mut(slot) {
            s.target_ticket = t;
            if let Some(v) = v {
                s.value_ticket = v;
            }
        }
    }

    /// Sets a retained property's value. On a `$target` property, `$target`
    /// is published first unless it already equals `v`, and the transition
    /// closes (convention.md:333, 466-468).
    pub fn set_value(&mut self, slot: SlotId, v: ValueRef<'_>) -> Result<Change, ValueError> {
        let s = self.m.slot(slot).ok_or(ValueError::Gone)?;
        if !s.spec.retained {
            return Err(ValueError::IsEvent);
        }
        let target_mode = s.spec.target;
        if !target_mode && !self.shut_down {
            // The gate before encoding: an equal fingerprint is an equal
            // payload, except for values compared by their bytes.
            let fp = check(s, v)?;
            if fp != Fp::Bytes && s.fp == Some(fp) {
                return Ok(Change::Unchanged);
            }
        }
        let fp = self.encode(slot, v)?;
        if self.shut_down {
            return Ok(Change::Dropped);
        }
        let mut published = false;
        if target_mode {
            let s = self.m.slot(slot).ok_or(ValueError::Gone)?;
            let matches = s
                .target
                .as_deref()
                .is_some_and(|t| target_equals(t, &self.scratch, v));
            if !matches {
                let bytes = self.scratch.as_bytes().to_vec();
                if let Some(s) = self.m.slot_mut(slot) {
                    s.target = Some(bytes);
                }
                self.enqueue_target(slot);
                published = true;
            }
            if let Some(s) = self.m.slot_mut(slot) {
                s.open = false;
            }
        }
        if self.store(slot, fp) {
            published |= self.enqueue_value(slot);
        }
        Ok(if published {
            self.status()
        } else {
            Change::Unchanged
        })
    }

    /// On a `$target` property, publishes the value only: the transition
    /// stays open. With no `$target` held (none set yet, or cleared), it is
    /// [`set_value`](Self::set_value), since `$target` accompanies every
    /// value (convention.md:333).
    pub fn set_progress(&mut self, slot: SlotId, v: ValueRef<'_>) -> Result<Change, ValueError> {
        let s = self.m.slot(slot).ok_or(ValueError::Gone)?;
        if !s.spec.target {
            return Err(ValueError::NotTarget);
        }
        if s.target.is_none() {
            return self.set_value(slot, v);
        }
        let fp = self.encode(slot, v)?;
        if self.shut_down {
            return Ok(Change::Dropped);
        }
        Ok(if self.store(slot, fp) && self.enqueue_value(slot) {
            self.status()
        } else {
            Change::Unchanged
        })
    }

    /// On a `$target` property, opens a transition to `v` from a local
    /// command and publishes `$target` when it differs.
    pub fn set_target(&mut self, slot: SlotId, v: ValueRef<'_>) -> Result<Change, ValueError> {
        let s = self.m.slot(slot).ok_or(ValueError::Gone)?;
        if !s.spec.target {
            return Err(ValueError::NotTarget);
        }
        self.encode(slot, v)?;
        if self.shut_down {
            return Ok(Change::Dropped);
        }
        let bytes = self.scratch.as_bytes().to_vec();
        self.open_target(slot, bytes)
    }

    /// On a `$target` property, accepts a `/set`: `$target` becomes the
    /// received bytes, byte for byte (convention.md:470).
    pub fn accept_set(&mut self, slot: SlotId, raw: &[u8]) -> Result<Change, ValueError> {
        let s = self.m.slot(slot).ok_or(ValueError::Gone)?;
        if !s.spec.target {
            return Err(ValueError::NotTarget);
        }
        if self.shut_down {
            return Ok(Change::Dropped);
        }
        self.open_target(slot, raw.to_vec())
    }

    fn open_target(&mut self, slot: SlotId, bytes: Vec<u8>) -> Result<Change, ValueError> {
        let s = self.m.slot_mut(slot).ok_or(ValueError::Gone)?;
        s.open = true;
        if s.target.as_deref() == Some(bytes.as_slice()) {
            return Ok(Change::Unchanged);
        }
        s.target = Some(bytes);
        self.enqueue_target(slot);
        Ok(self.status())
    }

    /// Deletes a retained value with a zero-length publish; a property with
    /// no value publishes nothing (homie.py:1334-1336). On a `$target`
    /// property, `$target` is cleared next.
    pub fn clear_value(&mut self, slot: SlotId) -> Result<Change, ValueError> {
        let s = self.m.slot_mut(slot).ok_or(ValueError::Gone)?;
        if !s.spec.retained {
            return Err(ValueError::IsEvent);
        }
        let mut clears = Vec::new();
        if s.fp.take().is_some() {
            s.payload.clear();
            s.value_ticket = 0;
            s.sent_valid = false;
            clears.push(s.topic.clone());
        }
        if s.target.take().is_some() {
            s.target_ticket = 0;
            clears.extend(s.target_topic.clone());
        }
        s.open = false;
        if self.shut_down {
            return Ok(if clears.is_empty() {
                Change::Unchanged
            } else {
                Change::Dropped
            });
        }
        if clears.is_empty() {
            return Ok(Change::Unchanged);
        }
        for t in clears {
            self.issue_clear(t);
        }
        Ok(self.status())
    }

    /// Publishes one value of a `retained: false` property, non-retained at
    /// QoS 0, every call (convention.md:50); dropped while down
    /// (convention.md:695).
    pub fn emit(&mut self, slot: SlotId, v: ValueRef<'_>) -> Result<Change, ValueError> {
        let s = self.m.slot(slot).ok_or(ValueError::Gone)?;
        if s.spec.retained {
            return Err(ValueError::NotEvent);
        }
        self.encode(slot, v)?;
        if !self.live() {
            return Ok(Change::Dropped);
        }
        let topic = self
            .m
            .slot(slot)
            .map(|s| s.topic.clone())
            .ok_or(ValueError::Gone)?;
        self.steps.push_back(Step::Cmd(Command::publish(
            topic,
            self.scratch.as_bytes().to_vec(),
            QoS::AtMostOnce,
            false,
        )));
        Ok(Change::Queued)
    }

    // ----- presence and alerts -----

    fn set_state(&mut self, d: u16, state: DeviceState) {
        let Some(dev) = self.m.dev_mut(d) else { return };
        if dev.state == state {
            return;
        }
        dev.state = state;
        if !self.live() {
            return;
        }
        let epoch = self.epoch;
        let ticket = self.next_ticket;
        let Some(dev) = self.m.dev_mut(d) else { return };
        if dev.state_ticket != 0 && dev.state_ticket >= epoch {
            return;
        }
        dev.state_ticket = ticket;
        self.next_ticket += 1;
        self.steps.push_back(Step::State {
            dev: d,
            mode: StateMode::Presence(ticket),
        });
    }

    /// Sets a device's state. The same state publishes nothing.
    /// `Disconnected` and `Lost` are for child devices; `Ready` on the root
    /// also ends [`declare_lost`](Self::declare_lost).
    pub fn set_presence(&mut self, dev: DevIdx, p: Presence) -> Result<(), ValueError> {
        self.m.dev(dev.0).ok_or(ValueError::Gone)?;
        if dev.0 == self.m.root && matches!(p, Presence::Disconnected | Presence::Lost) {
            return Err(ValueError::NotForRoot);
        }
        self.set_state(dev.0, p.state());
        Ok(())
    }

    /// The root's state becomes `lost`, connected or not; while down the next
    /// refresh publishes it (homie.py:2538-2541, 2563-2564).
    pub fn declare_lost(&mut self) {
        self.set_state(self.m.root, DeviceState::Lost);
    }

    /// Raises or updates an alert, retained (convention.md:519-527); the same
    /// message publishes nothing.
    pub fn raise_alert(&mut self, dev: DevIdx, id: &str, msg: &str) -> Result<Change, AlertError> {
        if !is_valid_id(id) {
            return Err(AlertError::InvalidId);
        }
        if msg.is_empty() {
            return Err(AlertError::EmptyMessage);
        }
        let live = self.live();
        let epoch = self.epoch;
        let ticket = self.next_ticket;
        let d = self.m.dev_mut(dev.0).ok_or(AlertError::Gone)?;
        if d.alerts.get(id).map(String::as_str) == Some(msg) {
            return Ok(Change::Unchanged);
        }
        d.alerts.insert(id.to_string(), msg.to_string());
        if self.shut_down {
            return Ok(Change::Dropped);
        }
        if !live {
            return Ok(Change::Cached);
        }
        if d.alert_tickets.get(id).is_some_and(|t| *t >= epoch) {
            return Ok(Change::Queued);
        }
        d.alert_tickets.insert(id.to_string(), ticket);
        self.next_ticket += 1;
        self.steps.push_back(Step::Alert {
            dev: dev.0,
            id: id.to_string(),
            ticket,
        });
        Ok(Change::Queued)
    }

    /// Deletes an alert with a zero-length publish; an alert not raised
    /// publishes nothing.
    pub fn clear_alert(&mut self, dev: DevIdx, id: &str) -> Result<Change, AlertError> {
        if !is_valid_id(id) {
            return Err(AlertError::InvalidId);
        }
        let prefix = self.m.prefix.clone();
        let d = self.m.dev_mut(dev.0).ok_or(AlertError::Gone)?;
        if d.alerts.remove(id).is_none() {
            return Ok(Change::Unchanged);
        }
        d.alert_tickets.remove(id);
        let topic = format!("{prefix}{}/$alert/{id}", d.id);
        if self.shut_down {
            return Ok(Change::Dropped);
        }
        self.issue_clear(topic);
        Ok(self.status())
    }

    // ----- reconfiguration -----

    /// Changes the tree in one commit. Each touched device's description is
    /// rebuilt and checked by round trip; an error from `f` or the check
    /// rolls everything back. While up, the commit publishes (convention.md:
    /// 207, 613-641): new children's `/set` SUBSCRIBE and their `init`,
    /// description and states; then each changed device's `init` (when
    /// `ready` or `sleeping`), description and the clears of removed
    /// properties, then its state; then each removed subtree's clears, leaves
    /// first, and one UNSUBSCRIBE. While down only the model changes; the
    /// next refresh publishes it.
    pub fn reconfigure<R>(
        &mut self,
        f: impl FnOnce(&mut Reconfigure<'_>) -> Result<R, BuildError>,
    ) -> Result<R, BuildError> {
        let backup = self.m.clone();
        self.m.begin();
        let r = f(&mut Reconfigure { m: &mut self.m });
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                self.m = backup;
                return Err(e);
            }
        };
        let touched: Vec<u16> = self.m.touched.iter().copied().collect();
        let mut changed = BTreeSet::new();
        for d in touched {
            match self.m.rebuild(d, self.cfg.max_description_bytes) {
                Ok(true) => {
                    changed.insert(d);
                }
                Ok(false) => {}
                Err(e) => {
                    self.m = backup;
                    return Err(e);
                }
            }
        }
        let added: BTreeSet<u16> = self.m.added.iter().copied().collect();
        let removed = core::mem::take(&mut self.m.removed);
        let removed_slots = core::mem::take(&mut self.m.removed_slots);
        self.m.begin();

        let order = self.m.preorder();
        let qos = self.cfg.qos;
        let new_filters: Vec<(String, QoS)> = order
            .iter()
            .filter(|d| added.contains(d))
            .filter_map(|d| self.m.dev(*d))
            .map(|d| (d.set_filter.clone(), qos))
            .collect();
        let sub = self.ledger.subscribe(new_filters);
        let unsub = self
            .ledger
            .unsubscribe(removed.iter().map(|r| r.set_filter.clone()).collect());
        if self.shut_down {
            return Ok(r);
        }
        if !self.connected {
            for t in removed_slots.into_iter().flat_map(|s| s.clears) {
                self.issue_clear(t);
            }
            for t in removed.into_iter().flat_map(|r| r.clears) {
                self.issue_clear(t);
            }
            return Ok(r);
        }
        let gate = self.push_subscribe_cmd(sub);
        for &d in order.iter().filter(|d| added.contains(d)) {
            self.push_device(d, false);
        }
        if let Some(g) = gate {
            self.steps.push_back(Step::Gate(g));
        }
        for d in self.m.postorder().into_iter().filter(|d| added.contains(d)) {
            self.steps.push_back(Step::State {
                dev: d,
                mode: StateMode::Forced,
            });
        }
        let existing: Vec<u16> = order
            .iter()
            .copied()
            .filter(|d| changed.contains(d) && !added.contains(d))
            .collect();
        for &d in &existing {
            self.steps.push_back(Step::Init {
                dev: d,
                reconfig: true,
            });
            self.steps.push_back(Step::Desc(d));
            let clears: Vec<String> = removed_slots
                .iter()
                .filter(|s| s.dev == d)
                .flat_map(|s| s.clears.iter().cloned())
                .collect();
            for t in clears {
                self.issue_clear(t);
            }
        }
        for d in self
            .m
            .postorder()
            .into_iter()
            .filter(|d| existing.contains(d))
        {
            self.steps.push_back(Step::State {
                dev: d,
                mode: StateMode::IfInit,
            });
        }
        for t in removed.into_iter().flat_map(|r| r.clears) {
            self.issue_clear(t);
        }
        if let Some(Command::Unsubscribe { filters, .. }) = unsub {
            self.steps.push_back(Step::Unsubscribe(filters));
        }
        Ok(r)
    }

    // ----- shutdown -----

    /// Stops the tree; later input is ignored and later updates return
    /// [`Change::Dropped`]. Steps already queued still go out, then the
    /// mode's own (none for [`Shutdown::Silent`], which also discards the
    /// queue). While down nothing is published.
    pub fn shutdown(&mut self, mode: Shutdown) {
        if self.shut_down {
            return;
        }
        crate::log::info!("stoppingDevice", mode = mode_str(mode));
        let post = self.m.postorder();
        match mode {
            Shutdown::Silent => {
                self.steps.clear();
                self.urgent.clear();
            }
            Shutdown::Announce => {
                for &d in &post {
                    if let Some(dev) = self.m.dev_mut(d) {
                        dev.state = DeviceState::Disconnected;
                    }
                }
                if self.connected {
                    // Queued presence steps would repeat it.
                    self.steps.retain(|s| {
                        !matches!(
                            s,
                            Step::State {
                                mode: StateMode::Presence(_),
                                ..
                            }
                        )
                    });
                    for d in post {
                        self.steps.push_back(Step::State {
                            dev: d,
                            mode: StateMode::Forced,
                        });
                    }
                }
            }
            Shutdown::Remove => {
                let mut clears: Vec<String> = Vec::new();
                if let Some(root) = self.m.dev(self.m.root) {
                    clears.push(root.state_topic.clone());
                }
                for &d in &post {
                    let slots = self.m.dev_slots(d);
                    let Some(dev) = self.m.dev(d) else { continue };
                    if d != self.m.root {
                        clears.push(dev.state_topic.clone());
                    }
                    let mut targets = Vec::new();
                    for s in slots.iter().filter_map(|s| self.m.slot_at(*s)) {
                        if s.fp.is_some() {
                            clears.push(s.topic.clone());
                        }
                        if let (Some(t), Some(_)) = (&s.target_topic, &s.target) {
                            targets.push(t.clone());
                        }
                    }
                    clears.extend(targets);
                    let p = &self.m.prefix;
                    clears.extend(
                        dev.alerts
                            .keys()
                            .map(|a| format!("{p}{}/$alert/{a}", dev.id)),
                    );
                    clears.push(dev.desc_topic.clone());
                }
                let older: Vec<String> = self.tomb_order.values().cloned().collect();
                for t in clears {
                    self.issue_clear(t);
                }
                if self.connected {
                    for t in older {
                        if !self
                            .steps
                            .iter()
                            .any(|s| matches!(s, Step::Clear(x) if *x == t))
                        {
                            self.steps.push_back(Step::Clear(t));
                        }
                    }
                }
            }
        }
        if !self.connected {
            self.steps.clear();
        }
        // No retry runs after shutdown, so nothing waits for a SUBACK.
        self.gates.clear();
        self.retries.clear();
        self.shut_down = true;
    }

    // ----- input -----

    /// True when the tree takes messages on `topic`: a `/set` for one of its
    /// devices, `$broadcast` with [`TreeConfig::broadcast`], and the root's
    /// `$state` with [`TreeConfig::watch_own_state`].
    pub fn wants(&self, topic: &str) -> bool {
        if self.shut_down {
            return false;
        }
        let Some(rest) = topic.strip_prefix(self.m.prefix.as_str()) else {
            return false;
        };
        if self.cfg.broadcast && rest.starts_with("$broadcast/") {
            return true;
        }
        if rest.ends_with("/set") {
            return rest
                .split('/')
                .next()
                .is_some_and(|id| self.m.by_id.contains_key(id));
        }
        self.root_state_filter.as_deref() == Some(topic)
    }

    /// Handles one received message. A wanted message while the link is down
    /// counts as `Up { session_present: false }` (`reason=implicitLinkUp`).
    pub fn handle_message(&mut self, now: Instant, msg: Message<'_>) -> Handled {
        if !self.wants(msg.topic) {
            if msg.topic.ends_with("/set") && msg.topic.starts_with(self.m.prefix.as_str()) {
                crate::log::debug!("propertySetUnknownDevice", topic = msg.topic);
            }
            return Handled::Ignored;
        }
        if !self.connected {
            crate::log::warn!("implicitLinkUp", topic = msg.topic);
            self.handle_link(now, Link::up(false));
        }
        let Ok(t) = topic::parse(msg.topic) else {
            crate::log::debug!("invalidTopic", topic = msg.topic);
            return Handled::Ignored;
        };
        match (t.device, t.kind) {
            (Some(device), TopicKind::PropertySet { node, property }) => {
                self.handle_set(device, node, property, msg);
            }
            (None, TopicKind::Broadcast { subtopic }) => {
                self.events.push_back(DeviceEvent::Broadcast {
                    subtopic: subtopic.to_string(),
                    payload: msg.payload.into(),
                });
            }
            (Some(_), TopicKind::DeviceState) => self.handle_own_state(msg),
            _ => return Handled::Ignored,
        }
        Handled::Consumed
    }

    fn handle_set(&mut self, device: &str, node: &str, property: &str, msg: Message<'_>) {
        if msg.retain == Some(true) {
            crate::log::warn!("propertySetRetainedIgnored", topic = msg.topic);
            return;
        }
        let Some(dev) = self.m.by_id.get(device).and_then(|d| self.m.dev(*d)) else {
            return;
        };
        let found = dev
            .routes
            .binary_search_by(|(n, p, _)| (n.as_str(), p.as_str()).cmp(&(node, property)));
        let Some(index) = found.ok().and_then(|i| dev.routes.get(i)).map(|r| r.2) else {
            crate::log::debug!("propertySetUnknownProperty", topic = msg.topic);
            return;
        };
        let Some(slot) = self.m.slot_at(index) else {
            return;
        };
        if !slot.spec.settable {
            self.rejected_sets += 1;
            crate::log::info!("propertySetNotSettable", topic = msg.topic);
            return;
        }
        let parsed = if msg.payload.is_empty() {
            Err(Problem::EnumEmpty)
        } else {
            match core::str::from_utf8(msg.payload) {
                Ok(text) => datatype::parse_set_payload_with(
                    text,
                    slot.datatype,
                    slot.spec.format.as_deref(),
                    &slot.numeric,
                    current_of(slot),
                ),
                Err(_) => Err(Problem::BooleanGrammar),
            }
        };
        let problem_text = |p: Problem, payload: &[u8]| -> &'static str {
            if payload.is_empty() {
                "emptyPayload"
            } else if core::str::from_utf8(payload).is_err() {
                "notUtf8"
            } else {
                p.as_str()
            }
        };
        match parsed {
            Ok(value) => {
                let id = self.m.slot_id(index);
                self.events.push_back(DeviceEvent::Set {
                    slot: id,
                    value,
                    raw: msg.payload.into(),
                });
            }
            Err(p) => {
                self.rejected_sets += 1;
                crate::log::warn!(
                    "propertySetRejected",
                    topic = msg.topic,
                    problem = problem_text(p, msg.payload)
                );
            }
        }
    }

    fn handle_own_state(&mut self, msg: Message<'_>) {
        let root_state = self.m.dev(self.m.root).map(|d| d.state);
        if msg.payload != b"lost"
            || msg.retain == Some(true)
            || !self.root_closed
            || root_state == Some(DeviceState::Lost)
            || self.gave_up
        {
            return;
        }
        if self.reasserts >= MAX_REASSERTS {
            self.gave_up = true;
            crate::log::warn!("stateOverwritten", attempts = self.reasserts);
            self.events.push_back(DeviceEvent::StateReasserted {
                attempt: self.reasserts,
                gave_up: true,
            });
            return;
        }
        self.reasserts += 1;
        self.steps.push_back(Step::State {
            dev: self.m.root,
            mode: StateMode::Forced,
        });
        self.events.push_back(DeviceEvent::StateReasserted {
            attempt: self.reasserts,
            gave_up: false,
        });
    }

    /// A delivery report, used only with [`TreeConfig::receipts`]: a SUBACK
    /// releases the states that wait for it, or schedules a retry when
    /// refused; a `$description` written advances that device's description
    /// memo; an acknowledged clear retires its tombstone once every clear of
    /// the topic polled on this link is acknowledged.
    pub fn handle_receipt(&mut self, now: Instant, r: &Receipt) {
        if !self.cfg.receipts || !self.live() {
            return;
        }
        match r {
            Receipt::Subscribed { filters, granted } => {
                let Some(i) = self.pending_subs.iter().position(|(f, _)| f == filters) else {
                    return;
                };
                let Some((names, gate)) = self.pending_subs.remove(i) else {
                    return;
                };
                if *granted {
                    if let Some(g) = gate {
                        self.gates.remove(&g);
                    }
                } else {
                    crate::log::warn!("subscribeRefused", delay_ms = self.backoff_ms);
                    self.schedule_subscribe(now, names, gate);
                }
            }
            Receipt::Written { topic } => {
                let Some(d) = self.device_of(topic) else {
                    return;
                };
                let Some(dev) = self.m.dev_mut(d) else { return };
                if *topic == dev.desc_topic {
                    if let Some(h) = dev.desc_inflight.pop_front() {
                        dev.memo = Some(h);
                    }
                }
            }
            Receipt::Acknowledged { topic, cleared } => {
                if !cleared {
                    return;
                }
                let Some(n) = self.unacked.get_mut(topic) else {
                    return;
                };
                *n = n.saturating_sub(1);
                if *n == 0 {
                    self.unacked.remove(topic);
                    self.untomb(topic);
                }
            }
            _ => {}
        }
    }

    fn device_of(&self, topic: &str) -> Option<u16> {
        let rest = topic.strip_prefix(self.m.prefix.as_str())?;
        self.m.by_id.get(rest.split('/').next()?).copied()
    }

    fn schedule_subscribe(&mut self, now: Instant, names: Vec<String>, gate: Option<u64>) {
        let filters = names.into_iter().map(|f| (f, self.cfg.qos)).collect();
        self.retries.push((
            now.add_ms(self.backoff_ms),
            Step::Subscribe { filters, gate },
        ));
        self.backoff_ms = (self.backoff_ms * 2).min(RETRY_MAX_MS);
    }

    /// Reports a command the host could not hand to its client. A SUBSCRIBE
    /// or UNSUBSCRIBE is retried with a 1-30 s backoff; a value publish is
    /// queued again at the slot's latest value; a `$state`, `$description`,
    /// `$target`, alert or clear is queued again from the model; an event is
    /// dropped.
    pub fn handle_command_failed(&mut self, now: Instant, cmd: Command) {
        if !self.live() {
            return;
        }
        match cmd {
            Command::Subscribe { filters, .. } => {
                let names: Vec<String> = filters.into_iter().map(|(f, _)| f).collect();
                let gate = match self.pending_subs.iter().position(|(f, _)| *f == names) {
                    Some(i) => self.pending_subs.remove(i).and_then(|(_, g)| g),
                    None => None,
                };
                crate::log::warn!("commandRetry", delay_ms = self.backoff_ms);
                self.schedule_subscribe(now, names, gate);
            }
            Command::Unsubscribe { filters, .. } => {
                crate::log::warn!("commandRetry", delay_ms = self.backoff_ms);
                self.retries
                    .push((now.add_ms(self.backoff_ms), Step::Unsubscribe(filters)));
                self.backoff_ms = (self.backoff_ms * 2).min(RETRY_MAX_MS);
            }
            Command::Publish {
                topic,
                payload,
                retain,
                ..
            } => self.requeue_publish(topic, &payload, retain),
            _ => {}
        }
    }

    fn requeue_publish(&mut self, topic: String, payload: &[u8], retain: bool) {
        if !retain {
            crate::log::warn!("eventDropped", topic = topic);
            return;
        }
        if payload.is_empty() {
            if let Some(n) = self.unacked.get_mut(&topic) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    self.unacked.remove(&topic);
                }
            }
            if self.tombs.contains_key(&topic) {
                self.steps.push_back(Step::Clear(topic));
            }
            return;
        }
        let Some(d) = self.device_of(&topic) else {
            return;
        };
        let Ok(t) = topic::parse(&topic) else { return };
        match t.kind {
            TopicKind::DeviceState => {
                let Some(dev) = self.m.dev_mut(d) else { return };
                dev.published_state = None;
                self.steps.push_back(Step::State {
                    dev: d,
                    mode: StateMode::Forced,
                });
            }
            TopicKind::DeviceDescription => {
                if let Some(dev) = self.m.dev_mut(d) {
                    dev.desc_inflight.pop_back();
                }
                self.steps.push_back(Step::Desc(d));
            }
            TopicKind::DeviceAlert { alert_id } => {
                let id = alert_id.to_string();
                let ticket = self.ticket();
                if let Some(dev) = self.m.dev_mut(d) {
                    dev.alert_tickets.insert(id.clone(), ticket);
                }
                self.steps.push_back(Step::Alert { dev: d, id, ticket });
            }
            TopicKind::PropertyValue { node, property }
            | TopicKind::PropertyTarget { node, property } => {
                let is_target = matches!(t.kind, TopicKind::PropertyTarget { .. });
                let Some(dev) = self.m.dev(d) else { return };
                let found = dev
                    .routes
                    .binary_search_by(|(n, p, _)| (n.as_str(), p.as_str()).cmp(&(node, property)));
                let Some(index) = found.ok().and_then(|i| dev.routes.get(i)).map(|r| r.2) else {
                    return;
                };
                let id = self.m.slot_id(index);
                if is_target {
                    if let Some(s) = self.m.slot_mut(id) {
                        s.target_ticket = 0;
                    }
                    self.enqueue_target(id);
                } else {
                    if let Some(s) = self.m.slot_mut(id) {
                        s.value_ticket = 0;
                        s.sent_valid = false;
                    }
                    self.enqueue_value(id);
                }
            }
            _ => {}
        }
    }

    /// Runs the retries that are due.
    pub fn handle_timeout(&mut self, now: Instant) {
        if !self.live() || !self.retries.iter().any(|(t, _)| *t <= now) {
            return;
        }
        let (due, later): (Vec<_>, Vec<_>) = core::mem::take(&mut self.retries)
            .into_iter()
            .partition(|(t, _)| *t <= now);
        self.retries = later;
        for (_, step) in due {
            match step {
                Step::Subscribe { filters, gate } => {
                    let held: Vec<(String, QoS)> = filters
                        .into_iter()
                        .filter(|(f, _)| self.ledger.count(f) > 0)
                        .collect();
                    if held.is_empty() {
                        if let Some(g) = gate {
                            self.gates.remove(&g);
                        }
                        continue;
                    }
                    self.urgent.push_back(Step::Subscribe {
                        filters: held,
                        gate,
                    });
                }
                Step::Unsubscribe(filters) => {
                    let filters: Vec<String> = filters
                        .into_iter()
                        .filter(|f| self.ledger.count(f) == 0 && !self.ledger.is_host_filter(f))
                        .collect();
                    if !filters.is_empty() {
                        self.urgent.push_back(Step::Unsubscribe(filters));
                    }
                }
                other => self.urgent.push_back(other),
            }
        }
    }

    /// When to call [`handle_timeout`](Self::handle_timeout) next.
    pub fn poll_timeout(&self) -> Option<Instant> {
        if !self.live() {
            return None;
        }
        self.retries.iter().map(|(t, _)| *t).min()
    }
}

fn mode_str(mode: Shutdown) -> &'static str {
    match mode {
        Shutdown::Announce => "announce",
        Shutdown::Silent => "silent",
        Shutdown::Remove => "remove",
    }
}

fn current_of(s: &Slot) -> Option<Current> {
    match s.fp? {
        Fp::Int(i) => Some(Current::Integer(i)),
        Fp::F64(b) => Some(Current::Float(f64::from_bits(b))),
        Fp::F32(b) => Some(Current::Float(f64::from(f32::from_bits(b)))),
        _ => None,
    }
}

/// `x` is at or above `lo`, exactly: the `f64` nearest a decimal bound
/// decides every value but itself.
fn at_least(x: f64, lo: &(f64, Decimal)) -> bool {
    if x != lo.0 {
        return x > lo.0;
    }
    Decimal::from_f64(x).is_some_and(|d| d >= lo.1)
}

fn at_most(x: f64, hi: &(f64, Decimal)) -> bool {
    if x != hi.0 {
        return x < hi.0;
    }
    Decimal::from_f64(x).is_some_and(|d| d <= hi.1)
}

/// What [`check_limits`] reads, borrowed from a slot or a [`ValueCheck`].
struct Limits<'a> {
    datatype: Datatype,
    imin: Option<i64>,
    imax: Option<i64>,
    fmin: Option<&'a (f64, Decimal)>,
    fmax: Option<&'a (f64, Decimal)>,
    variants: usize,
    format: Option<&'a str>,
}

/// Validates `v` for the slot (finite, `$format` range, enum and color
/// membership) and returns its fingerprint.
fn check(s: &Slot, v: ValueRef<'_>) -> Result<Fp, ValueError> {
    check_limits(
        &Limits {
            datatype: s.datatype,
            imin: s.imin,
            imax: s.imax,
            fmin: s.fmin.as_ref(),
            fmax: s.fmax.as_ref(),
            variants: s.variants.len(),
            format: s.spec.format.as_deref(),
        },
        v,
    )
}

fn check_limits(s: &Limits<'_>, v: ValueRef<'_>) -> Result<Fp, ValueError> {
    let float = |x: f64| -> Result<(), ValueError> {
        if !x.is_finite() {
            return Err(ValueError::NotFinite);
        }
        if s.fmin.is_some_and(|lo| !at_least(x, lo)) || s.fmax.is_some_and(|hi| !at_most(x, hi)) {
            return Err(ValueError::OutOfRange);
        }
        Ok(())
    };
    match (s.datatype, v) {
        (Datatype::Integer, ValueRef::Integer(i)) => {
            if s.imin.is_some_and(|lo| i < lo) || s.imax.is_some_and(|hi| i > hi) {
                return Err(ValueError::OutOfRange);
            }
            Ok(Fp::Int(i))
        }
        (Datatype::Float, ValueRef::Float(x)) => float(x).map(|()| Fp::F64(x.to_bits())),
        (Datatype::Float, ValueRef::Float32(x)) => {
            float(f64::from(x)).map(|()| Fp::F32(x.to_bits()))
        }
        (Datatype::Boolean, ValueRef::Boolean(b)) => Ok(Fp::Bool(b)),
        (Datatype::String, ValueRef::String(_)) => Ok(Fp::Bytes),
        (Datatype::Enum, ValueRef::Enum(i)) => {
            if usize::from(i) < s.variants {
                Ok(Fp::Enum(i))
            } else {
                Err(ValueError::NotInFormat)
            }
        }
        (Datatype::Color, ValueRef::Color(text)) => match check_color(text, s.format) {
            Ok(()) => Ok(Fp::Bytes),
            Err(Problem::ColorTypeNotInFormat) => Err(ValueError::NotInFormat),
            Err(Problem::ColorComponentRange) => Err(ValueError::OutOfRange),
            Err(_) => Err(ValueError::WrongDatatype),
        },
        (Datatype::Datetime, ValueRef::Datetime(text)) => check_datetime(text)
            .map(|()| Fp::Bytes)
            .map_err(|_| ValueError::WrongDatatype),
        (Datatype::Duration, ValueRef::Duration { secs, micros }) => Ok(Fp::Dur(secs, micros)),
        (Datatype::Json, ValueRef::Json(j)) => {
            if j.is_array() || j.is_object() {
                Ok(Fp::Bytes)
            } else {
                Err(ValueError::WrongDatatype)
            }
        }
        _ => Err(ValueError::WrongDatatype),
    }
}

/// Encodes an already checked `v` into `out`.
fn write_value(out: &mut String, s: &Slot, v: ValueRef<'_>) -> Result<(), ValueError> {
    match v {
        ValueRef::Integer(i) => datatype::write_integer(out, i),
        ValueRef::Float(x) => datatype::write_float(out, x).map_err(|_| ValueError::NotFinite)?,
        ValueRef::Float32(x) => {
            datatype::write_float32(out, x).map_err(|_| ValueError::NotFinite)?
        }
        ValueRef::Boolean(b) => datatype::write_boolean(out, b),
        ValueRef::String(text) => out.push_str(encode_str(text)),
        ValueRef::Enum(i) => out.push_str(
            s.variants
                .get(usize::from(i))
                .ok_or(ValueError::NotInFormat)?,
        ),
        ValueRef::Color(text) | ValueRef::Datetime(text) => out.push_str(text),
        ValueRef::Duration { secs, micros } => datatype::write_duration(out, secs, micros),
        ValueRef::Json(j) => datatype::write_json(out, j).map_err(|_| ValueError::WrongDatatype)?,
    }
    Ok(())
}

/// The open `$target` already equals `v`: the same bytes, or for a number
/// the same value (`21.50` and `21.5`).
fn target_equals(target: &[u8], encoded: &str, v: ValueRef<'_>) -> bool {
    if target == encoded.as_bytes() {
        return true;
    }
    let Ok(text) = core::str::from_utf8(target) else {
        return false;
    };
    match v {
        ValueRef::Integer(i) => text.parse::<i64>() == Ok(i),
        ValueRef::Float(x) => text.parse::<f64>() == Ok(x),
        ValueRef::Float32(x) => text.parse::<f32>() == Ok(x),
        _ => false,
    }
}

impl ebus_mqtt::Role for Tree {
    fn handle_link(&mut self, now: Instant, link: Link) {
        Tree::handle_link(self, now, link);
    }

    fn handle_message(&mut self, now: Instant, msg: Message<'_>) -> Handled {
        Tree::handle_message(self, now, msg)
    }

    fn handle_command_failed(&mut self, now: Instant, cmd: Command) {
        Tree::handle_command_failed(self, now, cmd);
    }

    fn handle_timeout(&mut self, now: Instant) {
        Tree::handle_timeout(self, now);
    }

    fn handle_receipt(&mut self, now: Instant, r: &Receipt) {
        Tree::handle_receipt(self, now, r);
    }

    fn poll_command(&mut self) -> Option<Command> {
        Tree::poll_command(self)
    }

    fn poll_timeout(&self) -> Option<Instant> {
        Tree::poll_timeout(self)
    }

    fn wants(&self, topic: &str) -> bool {
        Tree::wants(self, topic)
    }

    /// The tree's commands are retained publishes and clears under its
    /// domain (not `/set` or `$broadcast`), and subscriptions that carry a
    /// `/set` filter or its `$broadcast` filter.
    fn owns(&self, cmd: &Command) -> bool {
        let prefix = self.m.prefix.as_str();
        let mine = |f: &str| {
            f.starts_with(prefix) && f.ends_with("/+/+/set")
                || self.broadcast_filter.as_deref() == Some(f)
        };
        match cmd {
            Command::Publish { topic, .. } => topic
                .strip_prefix(prefix)
                .is_some_and(|rest| !rest.ends_with("/set") && !rest.starts_with("$broadcast/")),
            Command::Subscribe { filters, .. } => filters.iter().any(|(f, _)| mine(f)),
            Command::Unsubscribe { filters, .. } => filters.iter().any(|f| mine(f)),
            _ => false,
        }
    }

    fn holds(&self, filter: &str) -> bool {
        self.ledger.count(filter) > 0
    }
}
