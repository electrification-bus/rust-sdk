//! Tokio runtime for the eBus Homie 5 controller.
//!
//! Drives the sans-IO [`Controller`] over an MQTT connection the host owns, or
//! one the `ebus-rumqttc` crate bundles. [`attach`] returns the pieces:
//!
//! | Piece | Who uses it |
//! |---|---|
//! | [`ControllerDriver`] | The host spawns [`ControllerDriver::run`] on its runtime; the SDK spawns nothing. |
//! | [`Inbox`] | The host's MQTT reader forwards CONNACKs, lost connections and messages; synchronous and never waits. |
//! | [`Attached::commands`] | The host's executor hands each [`Command`](ebus_mqtt::Command) to its client and reports failures through [`Inbox::command_failed`]. |
//! | [`ControllerHandle`] | The application reads snapshots and sends `/set` and `$broadcast`. |
//! | [`EventReceiver`] | The application reads events; a full channel yields [`Delivered::Overflow`]. |
//!
//! Events are hints; snapshots are the truth. The snapshot already reflects
//! an event by the time the event is delivered.
//!
//! # Bring your own connection
//!
//! A host keeps its own MQTT client, options, TLS, last will and reconnect
//! loop. Its contract:
//!
//! 1. Forward every CONNACK, PUBLISH and connection error to the [`Inbox`],
//!    including a CONNACK for a client that was already connected.
//! 2. Never await a controller call on the reader thread;
//!    [`ControllerHandle::try_set`] is the form for it.
//! 3. Declare in [`Config::host_filters`] every filter string the host also
//!    subscribes on the same client.
//! 4. Leave resubscribing the controller's filters to the controller.
//! 5. Report executor failures through [`Inbox::command_failed`].
//! 6. Set [`Config::link_up_discards_queued`] only when the client never
//!    writes, after a clean-session CONNACK, a request handed to it before
//!    that CONNACK, and every command handed to the client after a CONNACK
//!    was received from [`Commands`], or checked with
//!    [`Commands::last_is_stale`], after that CONNACK was reported.
//!
//! A client that does not surface the retain bit passes `retain: None`.
//!
//! The inbox is capped in bytes and the event channel in deliveries. The
//! command channel and the handle's request channel are unbounded: the
//! controller sends what pacing allows, plus one command per accepted `/set`
//! or `$broadcast`, so a host whose client stalls holds those commands until
//! its executor catches up.
//!
//! The example below stands in a hypothetical synchronous client whose
//! `try_*` calls never block, read by a `std::thread`:
//!
//! ```no_run
//! use std::sync::{Arc, Mutex, mpsc};
//!
//! use ebus_homie_tokio::ebus_homie::controller::{Config, PropertyAddr};
//! use ebus_homie_tokio::ebus_mqtt::{Command, QoS};
//! use ebus_homie_tokio::{AttachOptions, attach};
//!
//! /// What the host's MQTT reader yields.
//! enum HostEvent {
//!     ConnAck { session_present: bool },
//!     Publish { topic: String, payload: Vec<u8>, retain: bool },
//!     Error,
//! }
//!
//! /// The host's client: non-blocking `try_*` calls.
//! struct HostClient;
//! impl HostClient {
//!     fn try_subscribe_many(&mut self, _f: &[(String, QoS)]) -> Result<(), ()> { Ok(()) }
//!     fn try_unsubscribe(&mut self, _f: &str) -> Result<(), ()> { Ok(()) }
//!     fn try_publish(&mut self, _t: &str, _q: QoS, _r: bool, _p: &[u8]) -> Result<(), ()> { Ok(()) }
//! }
//!
//! # async fn host(reader: mpsc::Receiver<HostEvent>, client: Arc<Mutex<HostClient>>)
//! # -> Result<(), Box<dyn std::error::Error>> {
//! let mut cfg = Config::tree("panel-1")?;
//! cfg.host_filters = vec!["ebus/5/+/$description".into()];
//! let ebus = attach(cfg, AttachOptions::default())?;
//! tokio::spawn(ebus.driver.run());
//!
//! // The host's existing reader thread, with the eBus arms added.
//! let inbox = ebus.inbox.clone();
//! std::thread::spawn(move || {
//!     for ev in reader {
//!         match ev {
//!             HostEvent::ConnAck { session_present } => inbox.connected(session_present),
//!             HostEvent::Publish { topic, payload, retain } => {
//!                 if topic.starts_with("ebus/5/") {
//!                     inbox.message(&topic, payload.into(), Some(retain));
//!                 }
//!             }
//!             HostEvent::Error => inbox.disconnected(),
//!         }
//!     }
//! });
//!
//! // One executor task. try_* never blocks, so holding the std Mutex is safe.
//! let mut commands = ebus.commands;
//! let inbox = ebus.inbox;
//! tokio::spawn(async move {
//!     while let Some(cmd) = commands.recv().await {
//!         let res = {
//!             let mut c = client.lock().unwrap_or_else(|p| p.into_inner());
//!             match &cmd {
//!                 Command::Subscribe { filters } => c.try_subscribe_many(filters),
//!                 Command::Unsubscribe { filters } => {
//!                     filters.iter().try_for_each(|f| c.try_unsubscribe(f))
//!                 }
//!                 Command::Publish { topic, payload, qos, retain } => {
//!                     c.try_publish(topic, *qos, *retain, payload)
//!                 }
//!                 _ => Ok(()),
//!             }
//!         };
//!         if res.is_err() {
//!             inbox.command_failed(cmd);
//!         }
//!     }
//! });
//!
//! let addr = PropertyAddr::new("panel-1", "relay", "on").ok_or("bad address")?;
//! ebus.handle.set(addr, "true").await?;
//! # Ok(())
//! # }
//! ```

mod commands;
mod driver;
mod events;
mod handle;
mod inbox;
#[cfg(test)]
mod tests;

use std::sync::Arc;

pub use ebus_homie;
pub use ebus_mqtt;

use ebus_homie::controller::{Config, ConfigError, Controller};
use tokio::sync::{mpsc, watch};

pub use commands::Commands;
pub use driver::{ControllerDriver, DriverError};
pub use events::{Delivered, EventReceiver};
pub use handle::{ControllerHandle, DeviceSnapshot};
pub use inbox::Inbox;

/// Sizes for [`attach`]. Start from [`AttachOptions::default`] and set the
/// fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct AttachOptions {
    /// Most bytes of topic plus payload queued in the [`Inbox`]; a message
    /// past it is dropped and its device resubscribed, and a message larger
    /// than it on its own is dropped for good. Default 4 MiB.
    pub inbox_bytes: usize,
    /// Deliveries the event channel holds; at least one. Default 1024.
    pub event_capacity: usize,
}

impl Default for AttachOptions {
    fn default() -> Self {
        AttachOptions {
            inbox_bytes: 4 * 1024 * 1024,
            event_capacity: 1024,
        }
    }
}

/// The pieces of an attached controller; see the [crate documentation](crate).
#[derive(Debug)]
pub struct Attached {
    /// Snapshot reads, `/set` and `$broadcast`.
    pub handle: ControllerHandle,
    /// Where the host's MQTT reader feeds the controller.
    pub inbox: Inbox,
    /// Commands for the host's MQTT client, in order. Unbounded; see the
    /// [crate documentation](crate).
    pub commands: Commands,
    /// The controller's events.
    pub events: EventReceiver,
    /// The future the host spawns.
    pub driver: ControllerDriver,
}

/// Builds a controller for `config` and the pieces that drive it. Nothing
/// runs until the host spawns [`ControllerDriver::run`].
pub fn attach(config: Config, opts: AttachOptions) -> Result<Attached, ConfigError> {
    let skip_stale = config.link_up_discards_queued;
    let core = Controller::new(config)?;
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    let shared = Arc::new(inbox::Shared::new(opts.inbox_bytes));
    let (req_tx, req_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (ev_tx, ev_rx) = events::channel(opts.event_capacity);
    let (snap_tx, snap_rx) = watch::channel(Arc::new(handle::Snapshot::default()));
    Ok(Attached {
        handle: ControllerHandle {
            requests: req_tx,
            snap: snap_rx,
        },
        inbox: Inbox {
            tx: in_tx,
            shared: Arc::clone(&shared),
        },
        commands: Commands {
            rx: cmd_rx,
            shared: Arc::clone(&shared),
            skip_stale,
            received: 0,
        },
        events: ev_rx,
        driver: ControllerDriver {
            core,
            inbound: in_rx,
            shared,
            requests: req_rx,
            commands: cmd_tx,
            events: ev_tx,
            snap: snap_tx,
            lost: inbox::Dropped::default(),
            sent: 0,
            closed: false,
        },
    })
}
