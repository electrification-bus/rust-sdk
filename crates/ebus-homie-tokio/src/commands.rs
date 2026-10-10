//! The controller's commands, as the host's executor reads them.

use std::fmt;
use std::sync::Arc;

use ebus_mqtt::Command;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::inbox::Shared;

/// The controller's commands for the host's MQTT client, in order.
///
/// With [`Config::link_up_discards_queued`] set, a SUBSCRIBE the controller
/// queued before it handled the last clean-session CONNACK reported through
/// [`Inbox::connected`](crate::Inbox::connected) is skipped: after that
/// link-up the controller subscribes again every filter it holds. Without
/// it, every command is delivered, since the controller then still expects
/// the replays of such a SUBSCRIBE.
///
/// [`Config::link_up_discards_queued`]: ebus_homie::controller::Config::link_up_discards_queued
pub struct Commands {
    pub(crate) rx: UnboundedReceiver<Command>,
    pub(crate) shared: Arc<Shared>,
    /// The config's `link_up_discards_queued`.
    pub(crate) skip_stale: bool,
    /// Commands received, skipped ones included.
    pub(crate) received: u64,
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
            let cmd = self.rx.recv().await?;
            if let Some(cmd) = self.admit(cmd) {
                return Some(cmd);
            }
        }
    }

    /// The next command, or `None` when none is queued now.
    pub fn try_recv(&mut self) -> Option<Command> {
        loop {
            let cmd = self.rx.try_recv().ok()?;
            if let Some(cmd) = self.admit(cmd) {
                return Some(cmd);
            }
        }
    }

    /// True when the last command received is a SUBSCRIBE and a clean-session
    /// CONNACK reported since makes it one to skip; always false without
    /// `link_up_discards_queued`. An executor that holds a command across an
    /// await checks this before handing it to the client.
    pub fn last_is_stale(&self, last: &Command) -> bool {
        self.skip_stale
            && matches!(last, Command::Subscribe { .. })
            && self.shared.before_link_up(self.received)
    }

    fn admit(&mut self, cmd: Command) -> Option<Command> {
        self.received += 1;
        if self.last_is_stale(&cmd) {
            log::debug!("reason=staleSubscribeSkipped");
            return None;
        }
        Some(cmd)
    }
}
