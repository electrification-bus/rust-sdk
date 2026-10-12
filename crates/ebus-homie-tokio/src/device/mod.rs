//! The Homie 5 device role on tokio.
//!
//! A [`TreeBuilder`] describes one root device and its descendants and hands
//! out typed handles; [`attach_device`] (or [`attach_pair`], with a
//! controller on the same connection) returns the pieces:
//!
//! | Piece | Who uses it |
//! |---|---|
//! | [`DeviceDriver`] or [`PairDriver`] | The host spawns `run` on its runtime; the SDK spawns nothing. |
//! | [`Inbox`] | The host's MQTT reader forwards CONNACKs, lost connections, `/set` messages and, optionally, delivery receipts. |
//! | [`Commands`] | The host's executor hands each command to its client. |
//! | [`DeviceHandle`] | Presence, alerts, reconfiguration, `$broadcast` and shutdown. |
//! | [`Property`], [`Settable`], [`Event`] | Values, from any thread; `/set` requests. |
//! | [`LastWill`] | Installed on the connection before CONNECT, as its only will. |
//!
//! ```no_run
//! use ebus_homie_tokio::device::{
//!     DeviceOptions, DeviceSpec, NodeSpec, PropSpec, TreeBuilder, TreeConfig, attach_device,
//! };
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut b = TreeBuilder::new(TreeConfig::default(), "meter-1", DeviceSpec::typed("meter"))?;
//! let power = b.node(b.root(), "power", NodeSpec::default())?;
//! let watts = b.property::<f64>(power, "active", PropSpec::new().unit("W"))?;
//! let mut relay = b.settable::<bool>(power, "relay", PropSpec::new())?;
//! relay.set(false)?;
//! let dev = attach_device(b, DeviceOptions::default())?;
//! // The host installs dev.last_will on its connection, feeds dev.inbox and
//! // executes dev.commands.
//! tokio::spawn(dev.driver.run());
//!
//! watts.set(1234.5)?; // any thread; an unchanged value costs one compare
//! while let Some(req) = relay.next_set().await {
//!     let on = *req.value();
//!     req.accept();
//!     let _ = on;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Bring your own connection
//!
//! The host:
//!
//! 1. installs [`AttachedDevice::last_will`] on the connection before
//!    CONNECT, as the connection's only will;
//! 2. forwards every CONNACK, every PUBLISH matching the device's filters
//!    with its retain bit (or `None`), and every lost connection to the
//!    [`Inbox`];
//! 3. leaves subscribing `/set` and republishing after a reconnect to the
//!    SDK, and publishes nothing under the tree's topics;
//! 4. sends a client-held replay of an earlier session before anything
//!    handed over after the CONNACK;
//! 5. declares in [`TreeConfig::host_filters`] any filter it subscribes that
//!    matches the device's `/set` topics;
//! 6. sizes its client's outgoing packet limit to at least
//!    [`TreeConfig::max_description_bytes`] plus 1 KiB;
//! 7. reports executor failures through [`Inbox::command_failed`];
//! 8. optionally sets [`TreeConfig::receipts`] and reports, through
//!    [`Inbox::receipt`], every SUBACK of a device SUBSCRIBE, every device
//!    `$description` publish written, and every broker acknowledgment of a
//!    device zero-length publish;
//! 9. once [`Commands::recv`] returns `None`, ends the connection as
//!    [`Commands::ending`] says.
//!
//! The driver hands out device commands only while fewer than
//! [`DeviceOptions::max_outstanding`] are sent and not yet received from
//! [`Commands`]; past it, values stay dirty and coalesce.

mod dirty;
mod driver;
mod handles;
#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub use ebus_homie::device::{
    AlertError, BuildError, Color, Datetime, DevIdx, DeviceSpec, HomieEnum, HomieValue, NodeIdx,
    NodeSpec, Presence, PropSpec, Reconfigure, Shutdown, SlotId, TreeConfig, ValueError,
};

pub use driver::{DeviceDriver, PairDriver};
pub use handles::{
    DeviceHandle, EmitError, Event, Property, ReconfigureHandles, SetRequest, Settable,
};

use ebus_homie::controller::{Config, ConfigError, Controller};
use ebus_homie::device::{Tree, TreeBuilder as CoreBuilder};
use ebus_mqtt::{LastWill, Pair};
use tokio::sync::{mpsc, watch};

use crate::driver::{ControllerSide, Out};
use crate::handle::Snapshot;
use crate::inbox::Shared;
use crate::{Commands, ControllerHandle, EventReceiver, Inbox, events, handle};
use driver::Driver;
use handles::{DevShared, Registry, add_event, add_slot};

/// Sizes and limits for [`attach_device`] and [`attach_pair`]. Start from
/// [`DeviceOptions::default`] and set the fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct DeviceOptions {
    /// Most bytes of topic plus payload queued in the [`Inbox`]. Default
    /// 4 MiB.
    pub inbox_bytes: usize,
    /// Accepted `/set` requests each [`Settable`] holds; past it a `/set` is
    /// dropped with `reason=setDropped`. Default 16.
    pub set_capacity: usize,
    /// Events queued by [`Event::emit`], `$broadcast` messages held for
    /// [`DeviceHandle::next_broadcast`], and, with [`attach_pair`],
    /// controller event deliveries. Default 1024.
    pub event_capacity: usize,
    /// Most commands handed to [`Commands`] and not yet received. Default
    /// 1024.
    pub max_outstanding: usize,
    /// How long a bundled connection waits for the publishes of a
    /// `Shutdown::Announce` or `Shutdown::Remove` before DISCONNECT. Default
    /// 2000.
    pub flush_timeout_ms: u64,
    /// [`Commands`] skips every device command, and every controller
    /// SUBSCRIBE, queued before a clean-session link-up; see
    /// [`Config::link_up_discards_queued`] for when a host may set it. With
    /// [`attach_pair`] it also sets the controller's. Default false.
    pub link_up_discards_queued: bool,
}

impl Default for DeviceOptions {
    fn default() -> Self {
        DeviceOptions {
            inbox_bytes: 4 * 1024 * 1024,
            set_capacity: 16,
            event_capacity: 1024,
            max_outstanding: 1024,
            flush_timeout_ms: 2000,
            link_up_discards_queued: false,
        }
    }
}

/// Why [`attach_pair`] refused.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AttachError {
    /// The tree.
    Build(BuildError),
    /// The controller's configuration.
    Config(ConfigError),
}

impl fmt::Display for AttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttachError::Build(_) => f.write_str("device tree refused"),
            AttachError::Config(_) => f.write_str("controller configuration refused"),
        }
    }
}

impl std::error::Error for AttachError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AttachError::Build(e) => Some(e),
            AttachError::Config(e) => Some(e),
        }
    }
}

impl From<BuildError> for AttachError {
    fn from(e: BuildError) -> Self {
        AttachError::Build(e)
    }
}

impl From<ConfigError> for AttachError {
    fn from(e: ConfigError) -> Self {
        AttachError::Config(e)
    }
}

/// A device tree under construction, and the typed handles for it. Each
/// change is checked as it is made; the descriptions are built and checked
/// once per device by [`attach_device`] or [`attach_pair`]. Handles work at
/// once: a value set before the driver runs goes out in the first refresh.
pub struct TreeBuilder {
    core: CoreBuilder,
    reg: Registry,
    dev: Arc<DevShared>,
    requests: mpsc::UnboundedReceiver<handles::DevRequest>,
    emits: mpsc::UnboundedReceiver<handles::Emit>,
}

impl fmt::Debug for TreeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TreeBuilder")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl TreeBuilder {
    /// A tree with the root device `root_id` and no nodes.
    pub fn new(cfg: TreeConfig, root_id: &str, root: DeviceSpec) -> Result<Self, BuildError> {
        let core = CoreBuilder::new(cfg, root_id, root)?;
        let (req_tx, requests) = mpsc::unbounded_channel();
        let (emit_tx, emits) = mpsc::unbounded_channel();
        let dev = Arc::new(DevShared::new(req_tx, emit_tx));
        Ok(TreeBuilder {
            core,
            reg: Registry::new(Arc::clone(&dev)),
            dev,
            requests,
            emits,
        })
    }

    /// The configuration in use.
    pub fn config(&self) -> &TreeConfig {
        self.core.config()
    }

    /// Sets [`TreeConfig::receipts`]; the bundled connection sets it.
    pub fn set_receipts(&mut self, on: bool) {
        self.core.set_receipts(on);
    }

    /// The root device.
    pub fn root(&self) -> DevIdx {
        self.core.root()
    }

    /// Adds a child device under `parent`.
    pub fn child(
        &mut self,
        parent: DevIdx,
        id: &str,
        spec: DeviceSpec,
    ) -> Result<DevIdx, BuildError> {
        self.core.edit().add_child(parent, id, spec)
    }

    /// Adds a node to `dev`.
    pub fn node(&mut self, dev: DevIdx, id: &str, spec: NodeSpec) -> Result<NodeIdx, BuildError> {
        self.core.edit().add_node(dev, id, spec)
    }

    /// Adds a retained property of type `T`; `spec.settable` and
    /// `spec.retained` are set by this method.
    pub fn property<T: HomieValue + Send + Sync>(
        &mut self,
        node: NodeIdx,
        id: &str,
        spec: PropSpec,
    ) -> Result<Property<T>, BuildError> {
        let r = add_slot::<T>(&mut self.core.edit(), &mut self.reg, node, id, spec, false);
        self.reg.install();
        r.map(|(p, _)| p)
    }

    /// Adds a settable retained property of type `T`.
    pub fn settable<T: HomieValue + Send + Sync>(
        &mut self,
        node: NodeIdx,
        id: &str,
        spec: PropSpec,
    ) -> Result<Settable<T>, BuildError> {
        let r = add_slot::<T>(&mut self.core.edit(), &mut self.reg, node, id, spec, true);
        self.reg.install();
        r.and_then(|(_, s)| s.ok_or(BuildError::Gone))
    }

    /// Adds a `retained: false` property of type `T`, published on every
    /// [`Event::emit`].
    pub fn event<T: HomieValue + Send + Sync>(
        &mut self,
        node: NodeIdx,
        id: &str,
        spec: PropSpec,
    ) -> Result<Event<T>, BuildError> {
        let r = add_event::<T>(&mut self.core.edit(), &mut self.reg, node, id, spec);
        self.reg.install();
        r
    }

    /// Builds the tree and the driver's parts for `opts`.
    fn finish(self, opts: &DeviceOptions) -> Result<(Tree, Parts), BuildError> {
        let TreeBuilder {
            core,
            reg,
            dev,
            requests,
            emits,
        } = self;
        let tree = core.finish()?;
        dev.set_cap.store(opts.set_capacity, Ordering::Release);
        dev.emit_cap
            .store(opts.event_capacity.max(1), Ordering::Release);
        driver::sync_states(&dev, &tree);
        Ok((
            tree,
            Parts {
                reg,
                dev,
                requests,
                emits,
            },
        ))
    }
}

/// What a driver takes from its [`TreeBuilder`].
struct Parts {
    reg: Registry,
    dev: Arc<DevShared>,
    requests: mpsc::UnboundedReceiver<handles::DevRequest>,
    emits: mpsc::UnboundedReceiver<handles::Emit>,
}

/// The pieces of an attached device tree; see the [module documentation](self).
#[derive(Debug)]
#[non_exhaustive]
pub struct AttachedDevice {
    /// Presence, alerts, reconfiguration and shutdown.
    pub handle: DeviceHandle,
    /// The root's `$state` set to `lost`, retained: the connection's will.
    pub last_will: LastWill,
    /// Where the host's MQTT reader feeds the tree.
    pub inbox: Inbox,
    /// Commands for the host's MQTT client, in order.
    pub commands: Commands,
    /// The future the host spawns.
    pub driver: DeviceDriver,
}

/// The pieces of a device tree and a controller on one connection.
#[derive(Debug)]
#[non_exhaustive]
pub struct AttachedPair {
    /// The device's handle.
    pub device: DeviceHandle,
    /// The controller's handle.
    pub controller: ControllerHandle,
    /// The controller's events.
    pub events: EventReceiver,
    /// The root's `$state` set to `lost`, retained: the connection's will.
    pub last_will: LastWill,
    /// Where the host's MQTT reader feeds both roles.
    pub inbox: Inbox,
    /// Both roles' commands, the device's first after each link-up.
    pub commands: Commands,
    /// The future the host spawns.
    pub driver: PairDriver,
}

/// The host's pieces and the driver's ends of their channels.
struct Wiring {
    inbox: Inbox,
    commands: Commands,
    handle: DeviceHandle,
    ends: Ends,
}

/// The driver's ends of the host's channels.
struct Ends {
    inbound: mpsc::UnboundedReceiver<crate::inbox::Inbound>,
    shared: Arc<Shared>,
    out: Out,
    broadcasts: mpsc::Sender<(String, bytes::Bytes)>,
}

fn wiring(opts: &DeviceOptions, dev: &Arc<DevShared>) -> Wiring {
    let (in_tx, inbound) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared::new(opts.inbox_bytes));
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (b_tx, b_rx) = mpsc::channel(opts.event_capacity.max(1));
    Wiring {
        inbox: Inbox {
            tx: in_tx,
            shared: Arc::clone(&shared),
        },
        commands: Commands {
            rx: cmd_rx,
            shared: Arc::clone(&shared),
            skip_stale: opts.link_up_discards_queued,
            received: 0,
            last_device: false,
        },
        handle: DeviceHandle {
            dev: Arc::clone(dev),
            broadcasts: Arc::new(tokio::sync::Mutex::new(b_rx)),
        },
        ends: Ends {
            inbound,
            shared,
            out: Out::new(cmd_tx),
            broadcasts: b_tx,
        },
    }
}

fn driver<R: driver::Roles>(
    roles: R,
    parts: Parts,
    ends: Ends,
    opts: &DeviceOptions,
    ctl: Option<ControllerSide>,
) -> Driver<R> {
    let Ends {
        inbound,
        shared,
        out,
        broadcasts,
    } = ends;
    Driver {
        roles,
        inbound,
        shared,
        out,
        limit: u64::try_from(opts.max_outstanding.max(1)).unwrap_or(u64::MAX),
        dev: parts.dev,
        requests: parts.requests,
        requests_open: true,
        emits: parts.emits,
        reg: parts.reg,
        broadcasts,
        ctl,
        up: false,
        stopping: None,
        states_dirty: false,
    }
}

/// Builds the tree and the pieces that drive it. Nothing runs until the host
/// spawns [`DeviceDriver::run`].
pub fn attach_device(tree: TreeBuilder, opts: DeviceOptions) -> Result<AttachedDevice, BuildError> {
    let (tree, parts) = tree.finish(&opts)?;
    let last_will = tree.last_will();
    let w = wiring(&opts, &parts.dev);
    let driver = driver(tree, parts, w.ends, &opts, None);
    Ok(AttachedDevice {
        handle: w.handle,
        last_will,
        inbox: w.inbox,
        commands: w.commands,
        driver: DeviceDriver(driver),
    })
}

/// Builds the tree, a controller for `config`, and the pieces that drive
/// both on one connection. `config.link_up_discards_queued` is set from
/// `opts`. Nothing runs until the host spawns [`PairDriver::run`].
pub fn attach_pair(
    tree: TreeBuilder,
    mut config: Config,
    opts: DeviceOptions,
) -> Result<AttachedPair, AttachError> {
    config.link_up_discards_queued = opts.link_up_discards_queued;
    let core = Controller::new(config)?;
    let (tree, parts) = tree.finish(&opts)?;
    let last_will = tree.last_will();
    let w = wiring(&opts, &parts.dev);
    let (req_tx, req_rx) = mpsc::unbounded_channel();
    let (ev_tx, ev_rx) = events::channel(opts.event_capacity);
    let (snap_tx, snap_rx) = watch::channel(Arc::new(Snapshot::default()));
    let side = ControllerSide::new(req_rx, ev_tx, snap_tx);
    let driver = driver(Pair::new(tree, core), parts, w.ends, &opts, Some(side));
    Ok(AttachedPair {
        device: w.handle,
        controller: handle::ControllerHandle {
            requests: req_tx,
            snap: snap_rx,
        },
        events: ev_rx,
        last_will,
        inbox: w.inbox,
        commands: w.commands,
        driver: PairDriver(driver),
    })
}
