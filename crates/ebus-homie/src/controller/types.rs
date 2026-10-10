//! Public value types of the controller: ids, configuration, events, views.

use alloc::{string::String, sync::Arc, vec::Vec};
use core::{borrow::Borrow, fmt, ops::Deref};

use ebus_mqtt::{Link, QoS, filter};
use rpds::RedBlackTreeMapSync;

use crate::EBUS_DOMAIN;
use crate::datatype::Problem;
use crate::description::{DescriptionError, DeviceDescription, Ignored};
use crate::id::is_valid_id;
use crate::state::DeviceState;

/// A Homie id (device, node, property or alert), validated with
/// [`is_valid_id`] and cheap to clone.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(Arc<str>);

impl Id {
    /// `s` as an id, or `None` when it is not a valid Homie id.
    pub fn new(s: &str) -> Option<Self> {
        is_valid_id(s).then(|| Id(Arc::from(s)))
    }

    /// The id text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for Id {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Id {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for Id {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for Id {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for Id {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One property of one device.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PropertyAddr {
    /// Device id.
    pub device: Id,
    /// Node id.
    pub node: Id,
    /// Property id.
    pub property: Id,
}

impl PropertyAddr {
    /// The address of `device/node/property`, or `None` when any level is not a
    /// valid id.
    pub fn new(device: &str, node: &str, property: &str) -> Option<Self> {
        Some(PropertyAddr {
            device: Id::new(device)?,
            node: Id::new(node)?,
            property: Id::new(property)?,
        })
    }
}

/// The topic domain (first topic level) the controller watches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Domain {
    /// One domain, `ebus` by default (framework.md:85).
    Named(String),
    /// Every domain (`+/5/+/$state`, convention.md:161-162); wildcard discovery
    /// only.
    Any,
}

/// Which devices the controller tracks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Discovery {
    /// Every device that publishes a `$state` in the domain.
    Wildcard,
    /// One device, plus the `$state` of the root its description names.
    Device(Id),
    /// A root device and every device its `children` declare, transitively.
    Tree {
        /// The root device.
        root: Id,
    },
}

/// Limits on how many devices replay their retained property messages at once
/// (python-sdk#97). Start from [`Pacing::default`] and set the fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Pacing {
    /// Devices whose property burst may be in flight at once.
    pub batch_devices: usize,
    /// Retained messages the devices in flight may owe in total, two per
    /// retained property (a value and a `$target`).
    pub batch_values: usize,
}

impl Default for Pacing {
    /// python-sdk's constants: 8 devices, 500 messages.
    fn default() -> Self {
        Pacing {
            batch_devices: 8,
            batch_values: 500,
        }
    }
}

/// Controller configuration. Start from [`Config::wildcard`],
/// [`Config::tree`] or [`Config::device`] and adjust the fields.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Config {
    /// Topic domain; [`Domain::Named`] `"ebus"` by default.
    pub domain: Domain,
    /// Discovery mode.
    pub discovery: Discovery,
    /// QoS of every subscription; `ExactlyOnce` by default (convention.md:45).
    pub subscribe_qos: QoS,
    /// Subscription pacing; `None` subscribes every device's property filters
    /// with its `$description`.
    pub pacing: Option<Pacing>,
    /// Milliseconds a device may wait for its retained messages before it is
    /// resubscribed; `None` disables stuck healing. 10 000 by default.
    pub stuck_timeout_ms: Option<u64>,
    /// Resubscribe attempts before a stuck device is given up; 3 by default.
    pub max_resubscribe_attempts: u8,
    /// Milliseconds after a link-up within which a known device must be heard
    /// again, or [`Event::DeviceAbsent`] fires; 30 000 by default.
    pub absent_after_ms: u64,
    /// Milliseconds after a description is applied, or after a link-up, before
    /// [`Event::DeviceSynced`] reports missing values; 10 000 by default.
    pub sync_timeout_ms: u64,
    /// Trust a CONNACK with `session_present: true` and keep every
    /// subscription; `false` (the default) re-paces as after a clean session.
    pub trust_session: bool,
    /// The host never sends, after a `Link::Up { session_present: false }`,
    /// a SUBSCRIBE the controller queued before handling it. Replay matching
    /// then restarts at each clean link-up, so a replay lost with a
    /// connection delays no later re-sync; `false` (the default) keeps the
    /// unmatched SUBSCRIBEs across it.
    pub link_up_discards_queued: bool,
    /// Values buffered per device before its `$description` arrives; 1024 by
    /// default.
    pub max_early_values: usize,
    /// Alerts held per device; a new alert id past it is dropped. 256 by
    /// default.
    pub max_alerts: usize,
    /// Most devices tracked at once; `None` (the default) is unbounded.
    pub max_devices: Option<usize>,
    /// Filters the host subscribes on the same client. The controller never
    /// unsubscribes them.
    pub host_filters: Vec<String>,
}

impl Config {
    /// Wildcard discovery in the `ebus` domain, with every default.
    pub fn wildcard() -> Self {
        Config {
            domain: Domain::Named(String::from(EBUS_DOMAIN)),
            discovery: Discovery::Wildcard,
            subscribe_qos: QoS::ExactlyOnce,
            pacing: Some(Pacing::default()),
            stuck_timeout_ms: Some(10_000),
            max_resubscribe_attempts: 3,
            absent_after_ms: 30_000,
            sync_timeout_ms: 10_000,
            trust_session: false,
            link_up_discards_queued: false,
            max_early_values: 1024,
            max_alerts: 256,
            max_devices: None,
            host_filters: Vec::new(),
        }
    }

    /// Tree discovery from `root`, otherwise as [`Config::wildcard`].
    pub fn tree(root: &str) -> Result<Self, ConfigError> {
        let root = Id::new(root).ok_or(ConfigError::InvalidDeviceId)?;
        Ok(Config {
            discovery: Discovery::Tree { root },
            ..Config::wildcard()
        })
    }

    /// Single-device discovery of `id`, otherwise as [`Config::wildcard`].
    pub fn device(id: &str) -> Result<Self, ConfigError> {
        let id = Id::new(id).ok_or(ConfigError::InvalidDeviceId)?;
        Ok(Config {
            discovery: Discovery::Device(id),
            ..Config::wildcard()
        })
    }

    pub(super) fn validate(&self) -> Result<(), ConfigError> {
        match (&self.domain, &self.discovery) {
            (Domain::Named(d), _) if !is_valid_id(d) => return Err(ConfigError::InvalidDomain),
            (Domain::Any, Discovery::Device(_) | Discovery::Tree { .. }) => {
                return Err(ConfigError::InvalidDomain);
            }
            _ => {}
        }
        if self
            .pacing
            .is_some_and(|p| p.batch_devices == 0 || p.batch_values == 0)
        {
            return Err(ConfigError::ZeroBatch);
        }
        if !self.host_filters.iter().all(|f| filter::is_valid_filter(f)) {
            return Err(ConfigError::InvalidHostFilter);
        }
        Ok(())
    }
}

impl Default for Config {
    /// [`Config::wildcard`].
    fn default() -> Self {
        Config::wildcard()
    }
}

/// Why a [`Config`] was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ConfigError {
    /// [`Domain::Named`] holds an invalid id, or [`Domain::Any`] is combined
    /// with device or tree discovery.
    InvalidDomain,
    /// The device or root id is not a valid id.
    InvalidDeviceId,
    /// A [`Pacing`] limit is zero.
    ZeroBatch,
    /// A host filter is not a valid MQTT topic filter.
    InvalidHostFilter,
}

impl ConfigError {
    /// camelCase reason code.
    pub const fn as_str(self) -> &'static str {
        match self {
            ConfigError::InvalidDomain => "invalidDomain",
            ConfigError::InvalidDeviceId => "invalidDeviceId",
            ConfigError::ZeroBatch => "zeroBatch",
            ConfigError::InvalidHostFilter => "invalidHostFilter",
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(feature = "std")]
impl std::error::Error for ConfigError {}

/// Where a property message came from, read from [`ebus_mqtt::Message::retain`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Origin {
    /// The broker's retained copy (retain bit set).
    Retained,
    /// A live publish (retain bit clear).
    Live,
    /// The client does not report the retain bit.
    Unknown,
}

impl Origin {
    pub(super) fn of(retain: Option<bool>) -> Self {
        match retain {
            Some(true) => Origin::Retained,
            Some(false) => Origin::Live,
            None => Origin::Unknown,
        }
    }
}

/// A received value or `$target`, delivered unmodified (convention.md:78).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Value {
    /// The payload text, with a single `0x00` byte decoded to `""`.
    pub raw: Arc<str>,
    /// [`check_value`](crate::datatype::check_value) against the property's
    /// declaration.
    pub check: Result<(), Problem>,
}

/// What a stuck device still lacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Missing {
    /// Its `$state`.
    State,
    /// Its `$description`.
    Description,
    /// Retained property values.
    Properties,
}

/// Something the controller observed. Events are hints; the views returned by
/// [`Controller::device`](super::Controller::device) are the truth.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Event {
    /// The broker link changed, emitted after the commands it caused are queued.
    Link(Link),
    /// A device's first valid `$state`.
    DeviceDiscovered {
        /// Device id.
        device: Id,
        /// Its state.
        state: DeviceState,
    },
    /// A device's `$state` changed; also emitted, with `old: None`, after
    /// [`Event::DeviceDiscovered`].
    StateChanged {
        /// Device id.
        device: Id,
        /// The previous state.
        old: Option<DeviceState>,
        /// The new state.
        new: DeviceState,
        /// The device's effective state after the change.
        effective: DeviceState,
    },
    /// A device's effective state changed while its own `$state` did not:
    /// its root changed state or went away.
    EffectiveStateChanged {
        /// Device id.
        device: Id,
        /// The previous effective state.
        old: DeviceState,
        /// The new effective state.
        new: DeviceState,
    },
    /// A device announced a description: on its ready edge, or at once when
    /// it changed it while `ready`. A change while `sleeping` is announced on
    /// the next ready edge.
    DescriptionChanged {
        /// Device id.
        device: Id,
        /// The parsed description.
        description: Arc<DeviceDescription>,
        /// Nodes and properties left out as illegal.
        ignored: Vec<Ignored>,
        /// The description changed while the device was `ready` or
        /// `sleeping`, which convention.md:207 forbids.
        while_ready: bool,
    },
    /// A `$description` with an illegal device-level field. The device keeps
    /// its `$state` and has no model.
    DescriptionRejected {
        /// Device id.
        device: Id,
        /// Why it was rejected.
        error: DescriptionError,
    },
    /// A retained property's value changed.
    PropertyChanged {
        /// The property.
        addr: PropertyAddr,
        /// The value.
        value: Value,
        /// Where it came from.
        origin: Origin,
    },
    /// A message for a property declared `retained: false`; never cached
    /// (convention.md:370-371). An event with [`Origin::Retained`] is a stale
    /// copy the broker replayed on a subscribe, which convention.md:50 forbids
    /// devices to publish.
    PropertyEvent {
        /// The property.
        addr: PropertyAddr,
        /// The value.
        value: Value,
        /// Where it came from.
        origin: Origin,
    },
    /// A property's value was cleared: by a zero-length payload, or because
    /// a re-sync found it no longer retained.
    PropertyCleared {
        /// The property.
        addr: PropertyAddr,
    },
    /// A property's `$target` changed; `None` when it was cleared.
    TargetChanged {
        /// The property.
        addr: PropertyAddr,
        /// The target.
        target: Option<Value>,
    },
    /// A device's `$alert` changed.
    Alert {
        /// Device id.
        device: Id,
        /// Alert id.
        alert: Id,
        /// Alert text; `None` when the alert was cleared.
        message: Option<String>,
    },
    /// A device's description is held, its property burst is over, and every
    /// retained property it declares has a value (`missing: 0`), or
    /// [`Config::sync_timeout_ms`] passed with `missing` still absent.
    /// Re-armed by a description change and by a link-up.
    DeviceSynced {
        /// Device id.
        device: Id,
        /// Declared retained properties without a value.
        missing: u32,
    },
    /// A device's retained messages did not arrive in time.
    DeviceStuck {
        /// Device id.
        device: Id,
        /// What it lacks.
        missing: Missing,
        /// The resubscribe attempt now made, or with `gave_up` the attempts made.
        attempt: u8,
        /// No more attempts follow; the subscriptions stay in place.
        gave_up: bool,
    },
    /// A known device was not heard within [`Config::absent_after_ms`] of a
    /// link-up. It stays known and heals on its next message. This is also
    /// the only signal for a device whose `$state` was deleted while the
    /// controller did not hear it, since nothing retained is replayed for it.
    DeviceAbsent {
        /// Device id.
        device: Id,
    },
    /// A device left the registry: a zero-length `$state`, or, in tree mode,
    /// its parent no longer declares it.
    DeviceRemoved {
        /// Device id.
        device: Id,
    },
    /// Every device declared under a root has published its description.
    /// Fires on each incomplete-to-complete edge.
    TreeComplete {
        /// The root.
        root: Id,
    },
}

/// Options for [`Controller::set`](super::Controller::set).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SetOptions {
    /// Send even when the device's effective state is not `ready`.
    pub allow_not_ready: bool,
    /// Skip the payload check.
    pub unchecked: bool,
}

/// Why a `/set` or `$broadcast` was not sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SetError {
    /// The broker link is down; QoS 0 is never held (convention.md:695).
    NotConnected,
    /// No device with that id is known.
    UnknownDevice,
    /// The device has no model, or declares no such property.
    UnknownProperty,
    /// The property is not settable.
    NotSettable,
    /// The device's effective state is not `ready` (convention.md:279).
    DeviceNotReady(DeviceState),
    /// The payload fails the check the device runs.
    Invalid(Problem),
    /// A `$broadcast` subtopic level is not a valid id, or the domain is
    /// [`Domain::Any`].
    InvalidTopic,
}

impl SetError {
    /// camelCase reason code.
    pub const fn as_str(self) -> &'static str {
        match self {
            SetError::NotConnected => "notConnected",
            SetError::UnknownDevice => "unknownDevice",
            SetError::UnknownProperty => "unknownProperty",
            SetError::NotSettable => "notSettable",
            SetError::DeviceNotReady(_) => "deviceNotReady",
            SetError::Invalid(_) => "invalid",
            SetError::InvalidTopic => "invalidTopic",
        }
    }
}

impl fmt::Display for SetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetError::DeviceNotReady(s) => write!(f, "deviceNotReady({s})"),
            SetError::Invalid(p) => write!(f, "invalid({p})"),
            other => f.write_str(other.as_str()),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for SetError {}

/// Whether a message was for the controller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Handled {
    /// The message addressed a tracked device or the discovery filter.
    Consumed,
    /// Not for the controller: another domain, an untracked device, a `/set`
    /// or `$broadcast`, or not a Homie 5 topic.
    Ignored,
}

/// What the controller knows about one property.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PropertyView {
    /// The last value, checked; always `None` for a `retained: false` property.
    pub value: Option<Value>,
    /// The last value text that passed the check.
    pub last_valid: Option<Arc<str>>,
    /// The last `$target`.
    pub target: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Slot {
    pub(super) view: PropertyView,
    pub(super) node: Id,
    pub(super) prop: Id,
    /// Position in `DeviceDescription::nodes` and the node's `properties`.
    pub(super) node_idx: usize,
    pub(super) prop_idx: usize,
    pub(super) retained: bool,
}

/// A device's slots by node, then property. Persistent: a clone shares every
/// entry, and a change copies only the path to it.
pub(super) type Props = RedBlackTreeMapSync<Id, RedBlackTreeMapSync<Id, Slot>>;

/// What the controller knows about one device.
#[derive(Clone, Debug)]
pub struct DeviceView {
    /// Device id.
    pub id: Id,
    /// Topic domain it publishes in.
    pub domain: Arc<str>,
    /// Last valid `$state`; always `Some` for a device the queries return.
    pub state: Option<DeviceState>,
    /// Effective state (convention.md:262-267): `lost` when its root is lost,
    /// else its own state.
    pub effective: Option<DeviceState>,
    /// The applied description, `None` before the first valid one.
    pub description: Option<Arc<DeviceDescription>>,
    /// Heard since the last link-up.
    pub confirmed: bool,
    /// [`Event::DeviceSynced`] fired since the last description change or
    /// link-up.
    pub synced: bool,
    pub(super) props: Props,
    pub(super) alerts: RedBlackTreeMapSync<Id, String>,
}

impl DeviceView {
    pub(super) fn new(id: Id, domain: Arc<str>) -> Self {
        DeviceView {
            id,
            domain,
            state: None,
            effective: None,
            description: None,
            confirmed: true,
            synced: false,
            props: Props::new_sync(),
            alerts: RedBlackTreeMapSync::new_sync(),
        }
    }

    /// True when `other` holds the very maps of properties and alerts this
    /// view holds, as a clone does until either is changed. Constant time;
    /// `false` does not prove the contents differ.
    pub fn shares_values(&self, other: &DeviceView) -> bool {
        self.props.ptr_eq(&other.props) && self.alerts.ptr_eq(&other.alerts)
    }

    /// The property `node/property`, when the description declares it.
    pub fn property(&self, node: &str, property: &str) -> Option<&PropertyView> {
        self.props.get(node)?.get(property).map(|s| &s.view)
    }

    /// Every declared property as `(node, property, view)`, in id order.
    pub fn properties(&self) -> impl Iterator<Item = (&Id, &Id, &PropertyView)> + '_ {
        self.props
            .values()
            .flat_map(|m| m.values())
            .map(|s| (&s.node, &s.prop, &s.view))
    }

    /// The text of alert `id`, when it is set.
    pub fn alert(&self, id: &str) -> Option<&str> {
        self.alerts.get(id).map(String::as_str)
    }

    /// Every set alert as `(id, text)`, in id order.
    pub fn alerts(&self) -> impl Iterator<Item = (&Id, &str)> + '_ {
        self.alerts.iter().map(|(k, v)| (k, v.as_str()))
    }
}
