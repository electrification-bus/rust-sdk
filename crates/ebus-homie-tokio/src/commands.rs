//! The roles' commands, as the host's executor reads them.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ebus_homie::device::Shutdown;
use ebus_mqtt::Command;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::inbox::Shared;

/// A command with the role it came from.
#[derive(Debug)]
pub(crate) struct Tagged {
    pub(crate) cmd: Command,
    /// From the device role, which regenerates its output at every link-up.
    pub(crate) device: bool,
}

/// The commands for the host's MQTT client, in order.
///
/// With [`Config::link_up_discards_queued`] (or
/// [`DeviceOptions::link_up_discards_queued`]) set, a command queued before
/// the driver handled the last clean-session CONNACK reported through
/// [`Inbox::connected`](crate::Inbox::connected) is skipped when it is a
/// controller's SUBSCRIBE or any device command: after that link-up the
/// controller subscribes again every filter it holds, and the device
/// republishes its tree. Without it, every command is delivered, since the
/// controller then still expects the replays of such a SUBSCRIBE.
///
/// [`Config::link_up_discards_queued`]: ebus_homie::controller::Config::link_up_discards_queued
/// [`DeviceOptions::link_up_discards_queued`]: crate::device::DeviceOptions::link_up_discards_queued
pub struct Commands {
    pub(crate) rx: UnboundedReceiver<Tagged>,
    pub(crate) shared: Arc<Shared>,
    /// The config's `link_up_discards_queued`.
    pub(crate) skip_stale: bool,
    /// Commands received, skipped ones included.
    pub(crate) received: u64,
    /// The last command received came from the device role.
    pub(crate) last_device: bool,
}

impl fmt::Debug for Commands {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Commands")
            .field("received", &self.received)
            .finish_non_exhaustive()
    }
}

impl Commands {
    /// The next command, or `None` once the driver has stopped and every
    /// command it sent was received.
    pub async fn recv(&mut self) -> Option<Command> {
        loop {
            let item = self.rx.recv().await?;
            if let Some(cmd) = self.admit(item) {
                return Some(cmd);
            }
        }
    }

    /// The next command, or `None` when none is queued now.
    pub fn try_recv(&mut self) -> Option<Command> {
        loop {
            let item = self.rx.try_recv().ok()?;
            if let Some(cmd) = self.admit(item) {
                return Some(cmd);
            }
        }
    }

    /// True when a clean-session CONNACK reported since the last command was
    /// received makes it one to skip: a controller's SUBSCRIBE, or any
    /// device command. Always false without `link_up_discards_queued`. An
    /// executor that holds a command across an await checks this before
    /// handing it to the client.
    pub fn last_is_stale(&self, last: &Command) -> bool {
        self.skip_stale
            && (self.last_device || matches!(last, Command::Subscribe { .. }))
            && self.shared.before_link_up(self.received)
    }

    /// How a device driver ended, once [`recv`](Self::recv) has returned
    /// `None`: the [`Shutdown`] mode of
    /// [`DeviceHandle::shutdown`](crate::device::DeviceHandle::shutdown), or
    /// `Silent` after [`DeviceHandle::abort`](crate::device::DeviceHandle::abort).
    /// `None` for a controller, or a driver that stopped for another reason.
    /// The host ends the connection accordingly: a clean DISCONNECT after
    /// `Announce` and `Remove`, once their publishes are acknowledged; a drop
    /// without DISCONNECT after `Silent`, so the broker publishes the will.
    pub fn ending(&self) -> Option<Shutdown> {
        self.shared.ending()
    }

    fn admit(&mut self, item: Tagged) -> Option<Command> {
        self.received += 1;
        self.last_device = item.device;
        self.shared.received.store(self.received, Ordering::Release);
        if self.shared.starved.swap(false, Ordering::AcqRel) {
            self.shared.credit.notify_one();
        }
        if self.last_is_stale(&item.cmd) {
            if item.device {
                log::debug!("reason=staleDeviceCommandSkipped");
            } else {
                log::debug!("reason=staleSubscribeSkipped");
            }
            return None;
        }
        Some(item.cmd)
    }
}
