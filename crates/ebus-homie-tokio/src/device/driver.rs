//! The task that owns a device tree, alone or paired with a controller.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use ebus_homie::controller::Controller;
use ebus_homie::device::{DeviceEvent, Shutdown, Tree};
use ebus_mqtt::{Command, Instant, Link, Message, Pair, Role};
use tokio::sync::mpsc::{self, UnboundedReceiver, error::TryRecvError};
use tokio::sync::oneshot;
use tokio::time;

use super::handles::{DevRequest, DevShared, Emit, Registry};
use crate::driver::{ControllerSide, DRAIN_BATCH, DriverError, Out, clock, deadline, sleep_until};
use crate::inbox::{Inbound, Shared};

/// The roles a [`Driver`] serves: a tree, or a tree and a controller.
pub(crate) trait Roles: Role + Send + 'static {
    fn tree(&mut self) -> &mut Tree;
    fn tree_ref(&self) -> &Tree;
    fn controller(&mut self) -> Option<&mut Controller>;
    /// True when `cmd` came from the tree.
    fn is_device_cmd(&self, cmd: &Command) -> bool;
}

impl Roles for Tree {
    fn tree(&mut self) -> &mut Tree {
        self
    }

    fn tree_ref(&self) -> &Tree {
        self
    }

    fn controller(&mut self) -> Option<&mut Controller> {
        None
    }

    fn is_device_cmd(&self, _cmd: &Command) -> bool {
        true
    }
}

impl Roles for Pair<Tree, Controller> {
    fn tree(&mut self) -> &mut Tree {
        &mut self.a
    }

    fn tree_ref(&self) -> &Tree {
        &self.a
    }

    fn controller(&mut self) -> Option<&mut Controller> {
        Some(&mut self.b)
    }

    fn is_device_cmd(&self, cmd: &Command) -> bool {
        self.a.owns(cmd)
    }
}

/// The driver loop shared by [`DeviceDriver`] and [`PairDriver`].
pub(crate) struct Driver<R: Roles> {
    pub(crate) roles: R,
    pub(crate) inbound: UnboundedReceiver<Inbound>,
    pub(crate) shared: Arc<Shared>,
    pub(crate) out: Out,
    /// Most commands sent and not yet received by the executor.
    pub(crate) limit: u64,
    pub(crate) dev: Arc<DevShared>,
    pub(crate) requests: UnboundedReceiver<DevRequest>,
    pub(crate) requests_open: bool,
    pub(crate) emits: UnboundedReceiver<Emit>,
    pub(crate) reg: Registry,
    pub(crate) broadcasts: mpsc::Sender<(String, Bytes)>,
    pub(crate) ctl: Option<ControllerSide>,
    /// The link as the driver last reported it to the roles.
    pub(crate) up: bool,
    /// A device shutdown in progress, and who waits for it.
    pub(crate) stopping: Option<(Shutdown, Vec<oneshot::Sender<()>>)>,
    /// A device's state may have changed since the watches were updated.
    pub(crate) states_dirty: bool,
}

impl<R: Roles> fmt::Debug for Driver<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Driver")
            .field("up", &self.up)
            .field("sent", &self.out.sent)
            .finish_non_exhaustive()
    }
}

/// What ended a [`Driver::wait`].
pub(crate) enum Woken {
    Inbound(Inbound),
    InboxClosed,
    Other,
}

/// What a pass through the loop left to do.
struct Pass {
    /// Inputs remain in the inbox.
    backlog: bool,
    /// Dirty values or events remain and credit allows them.
    more: bool,
}

impl<R: Roles> Driver<R> {
    pub(crate) async fn run(mut self) -> Result<(), DriverError> {
        let start = time::Instant::now();
        let mut inbox_open = true;
        let mut first: Option<Inbound> = None;
        loop {
            let mut pass = Pass {
                backlog: true,
                more: false,
            };
            if let Some(item) = first.take() {
                self.inbound(clock(start), item);
            }
            for _ in 0..DRAIN_BATCH {
                match self.inbound.try_recv() {
                    Ok(item) => self.inbound(clock(start), item),
                    Err(TryRecvError::Empty) => {
                        pass.backlog = false;
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        inbox_open = false;
                        pass.backlog = false;
                        break;
                    }
                }
            }
            self.drain_requests();
            if let (Some(ctl), Some(core)) = (self.ctl.as_mut(), self.roles.controller()) {
                ctl.drain_requests(core);
            }
            if self.dev.aborted.load(Ordering::Acquire) {
                self.roles.tree().shutdown(Shutdown::Silent);
                self.finish(Shutdown::Silent);
                return Ok(());
            }
            let now = clock(start);
            if !pass.backlog {
                if let (Some(ctl), Some(core)) = (self.ctl.as_mut(), self.roles.controller()) {
                    ctl.report_dropped(core, &self.shared, now);
                }
            }
            if self.roles.poll_timeout().is_some_and(|t| t <= now) {
                self.roles.handle_timeout(now);
                if let Some(ctl) = self.ctl.as_mut() {
                    ctl.touch_all();
                }
            }
            pass.more = self.feed();
            let empty = self.send();
            let flushed = self.out.result();
            if let (Some(ctl), Some(core)) = (self.ctl.as_mut(), self.roles.controller()) {
                ctl.publish(core, &self.shared);
                ctl.reply_stopping();
            }
            self.device_events();
            self.status();
            if let Some((mode, _)) = &self.stopping {
                if empty || !self.up {
                    let mode = *mode;
                    self.finish(mode);
                    return Ok(());
                }
            }
            flushed?;
            if !inbox_open {
                self.finish_quiet();
                return Ok(());
            }
            if pass.backlog || pass.more {
                tokio::task::yield_now().await;
                continue;
            }

            let deadline = deadline(start, now, self.roles.poll_timeout());
            match self.wait(deadline).await {
                Woken::Inbound(item) => first = Some(item),
                Woken::InboxClosed => inbox_open = false,
                Woken::Other => {}
            }
        }
    }

    /// Waits for an input, a request, a wake, credit or the deadline.
    pub(crate) async fn wait(&mut self, deadline: Option<time::Instant>) -> Woken {
        let ctl_open = self.ctl.as_ref().is_some_and(|c| c.requests_open);
        let requests_open = self.requests_open;
        let dev = Arc::clone(&self.dev);
        let shared = Arc::clone(&self.shared);
        tokio::select! {
            biased;
            item = self.inbound.recv() => match item {
                Some(item) => return Woken::Inbound(item),
                None => return Woken::InboxClosed,
            },
            req = self.requests.recv(), if requests_open => match req {
                Some(req) => self.request(req),
                None => self.requests_open = false,
            },
            req = recv_ctl(self.ctl.as_mut()), if ctl_open => {
                if let (Some(ctl), Some(core)) = (self.ctl.as_mut(), self.roles.controller()) {
                    match req {
                        Some(req) => ctl.request(core, req),
                        None => ctl.requests_open = false,
                    }
                }
            },
            () = dev.wake.notified() => {}
            // Always armed: the executor may clear `starved` and store this
            // permit between the driver's last credit check and here.
            () = shared.credit.notified() => {}
            () = sleep_until(deadline) => {}
        }
        Woken::Other
    }

    /// Ends after a device shutdown or an abort: records the mode for the
    /// host and answers every caller waiting on the shutdown.
    fn finish(&mut self, mode: Shutdown) {
        self.shared.set_ending(mode);
        self.finish_quiet();
    }

    fn finish_quiet(&mut self) {
        if let Some((_, replies)) = self.stopping.take() {
            for r in replies {
                let _ = r.send(());
            }
        }
        self.dev.connected.store(false, Ordering::Release);
        self.reg.retire_all();
    }

    fn inbound(&mut self, now: Instant, item: Inbound) {
        match item {
            Inbound::Message {
                topic,
                payload,
                retain,
                size,
            } => {
                self.shared.release(size);
                if !self.up && self.roles.wants(&topic) {
                    log::warn!("reason=implicitLinkUp,topic={topic}");
                    self.link_up(now, false, true);
                }
                if let (Some(ctl), Some(core)) = (self.ctl.as_mut(), self.roles.controller()) {
                    ctl.before_message(core, &self.shared);
                }
                self.roles
                    .handle_message(now, Message::new(&topic, &payload, retain));
                if let Some(ctl) = self.ctl.as_mut() {
                    ctl.touch_topic(&topic);
                }
            }
            Inbound::Link(Link::Up {
                session_present, ..
            }) => {
                self.link_up(now, session_present, false);
            }
            Inbound::Link(Link::Down) => {
                self.up = false;
                self.dev.connected.store(false, Ordering::Release);
                self.drop_emits();
                self.roles.handle_link(now, Link::Down);
                if let Some(ctl) = self.ctl.as_mut() {
                    ctl.touch_all();
                }
            }
            Inbound::Link(_) => {}
            Inbound::CommandFailed(cmd) => self.roles.handle_command_failed(now, cmd),
            Inbound::Receipt(r) => self.roles.handle_receipt(now, &r),
            Inbound::Wake => {}
        }
    }

    /// Every dirty value goes into the core while it is still down, so the
    /// refresh carries the latest values; then what the roles queued before
    /// the link-up falls below the stale-command boundary.
    fn link_up(&mut self, now: Instant, session_present: bool, implicit: bool) {
        // Changes the handles made before the link-up apply before it.
        self.drain_requests();
        self.reg.drain(self.roles.tree(), usize::MAX);
        // Queued while an earlier link was up: QoS 0 is now or never.
        self.drop_emits();
        self.send();
        if implicit {
            self.shared.implicit_link_up(self.out.sent);
        } else if !session_present {
            self.shared.link_up(self.out.sent);
        }
        self.up = true;
        self.roles.handle_link(now, Link::up(session_present));
        if let Some(ctl) = self.ctl.as_mut() {
            ctl.touch_all();
        }
    }

    /// Discards the queued events (convention.md:695).
    fn drop_emits(&mut self) {
        while self.emits.try_recv().is_ok() {
            self.dev.emit_queued.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Handles every request queued now.
    fn drain_requests(&mut self) {
        while self.requests_open {
            match self.requests.try_recv() {
                Ok(req) => self.request(req),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => self.requests_open = false,
            }
        }
    }

    fn request(&mut self, req: DevRequest) {
        match req {
            DevRequest::Apply(f) => {
                f(self.roles.tree(), &mut self.reg);
                self.states_dirty = true;
            }
            DevRequest::Shutdown { mode, reply } => {
                if let Some((_, replies)) = &mut self.stopping {
                    replies.push(reply);
                    return;
                }
                // Values set before the shutdown still go out.
                self.reg.drain(self.roles.tree(), usize::MAX);
                while let Ok(e) = self.emits.try_recv() {
                    self.dev.emit_queued.fetch_sub(1, Ordering::AcqRel);
                    e(self.roles.tree());
                }
                self.roles.tree().shutdown(mode);
                self.states_dirty = true;
                self.stopping = Some((mode, vec![reply]));
            }
        }
    }

    /// Commands that may still be sent before the credit runs out.
    fn credit(&self) -> u64 {
        let received = self.shared.received.load(Ordering::Acquire);
        self.limit
            .saturating_sub(self.out.sent.saturating_sub(received))
    }

    /// Hands queued events and dirty values to the core while the link is up
    /// and credit remains; true when more remain for another pass.
    fn feed(&mut self) -> bool {
        if !self.up || self.stopping.is_some() {
            return false;
        }
        let credit = usize::try_from(self.credit()).unwrap_or(usize::MAX);
        if credit == 0 {
            self.starve();
            return false;
        }
        let mut budget = credit.min(DRAIN_BATCH);
        while budget > 0 {
            let Ok(e) = self.emits.try_recv() else { break };
            self.dev.emit_queued.fetch_sub(1, Ordering::AcqRel);
            e(self.roles.tree());
            budget -= 1;
        }
        if budget == 0 {
            return true;
        }
        !self.reg.drain(self.roles.tree(), budget)
    }

    /// Marks the driver starved, so the next command received wakes it.
    fn starve(&self) {
        self.shared.starved.store(true, Ordering::Release);
    }

    /// Sends commands while credit remains; true when the roles have none
    /// left.
    fn send(&mut self) -> bool {
        loop {
            if self.credit() == 0 {
                self.starve();
                // A command received since the check would not wake us.
                if self.credit() == 0 {
                    return false;
                }
            }
            let Some(cmd) = self.roles.poll_command() else {
                return true;
            };
            let device = self.roles.is_device_cmd(&cmd);
            self.out.send(cmd, device);
        }
    }

    fn device_events(&mut self) {
        while let Some(e) = self.roles.tree().poll_event() {
            match e {
                DeviceEvent::Set {
                    slot, value, raw, ..
                } => self.reg.deliver(slot, &value, raw),
                DeviceEvent::Broadcast {
                    subtopic, payload, ..
                } => {
                    let payload = Bytes::from(payload.into_vec());
                    if self.broadcasts.try_send((subtopic, payload)).is_err() {
                        log::warn!("reason=broadcastDropped");
                    }
                }
                DeviceEvent::StateReasserted {
                    attempt, gave_up, ..
                } => {
                    log::info!("reason=stateReasserted,attempt={attempt},gave_up={gave_up}");
                }
                _ => {}
            }
        }
    }

    /// Updates what the handles read.
    fn status(&mut self) {
        let tree = self.roles.tree_ref();
        self.dev
            .connected
            .store(tree.is_connected(), Ordering::Release);
        self.dev
            .rejected
            .store(tree.rejected_sets(), Ordering::Release);
        if self.states_dirty {
            self.states_dirty = false;
            sync_states(&self.dev, tree);
        }
    }
}

/// Brings every device's state watch up to date; an unchanged state
/// notifies nothing.
pub(crate) fn sync_states(dev: &DevShared, tree: &Tree) {
    let mut states = dev.states();
    let live: Vec<u16> = tree.devices().map(|d| d.index()).collect();
    states.retain(|d, tx| {
        let keep = live.contains(d);
        if !keep {
            tx.send_if_modified(|s| s.take().is_some());
        }
        keep
    });
    for d in tree.devices() {
        let state = tree.state(d);
        match states.get(&d.index()) {
            Some(tx) => {
                tx.send_if_modified(|s| {
                    let changed = *s != state;
                    *s = state;
                    changed
                });
            }
            None => {
                states.insert(d.index(), tokio::sync::watch::channel(state).0);
            }
        }
    }
}

async fn recv_ctl(ctl: Option<&mut ControllerSide>) -> Option<crate::handle::Request> {
    match ctl {
        Some(c) => c.requests.recv().await,
        None => std::future::pending().await,
    }
}

/// The future that owns a device [`Tree`]. The host spawns
/// [`run`](Self::run) on its own runtime; the SDK spawns nothing.
#[derive(Debug)]
pub struct DeviceDriver(pub(crate) Driver<Tree>);

impl DeviceDriver {
    /// Runs the tree until [`DeviceHandle::shutdown`](super::DeviceHandle::shutdown)
    /// has handed out its commands, [`DeviceHandle::abort`](super::DeviceHandle::abort),
    /// or every [`Inbox`](crate::Inbox) clone is dropped; all end with `Ok`.
    /// The future is `Send`.
    pub async fn run(self) -> Result<(), DriverError> {
        self.0.run().await
    }
}

/// The future that owns a device tree and a controller on one connection.
/// The host spawns [`run`](Self::run) on its own runtime; the SDK spawns
/// nothing.
#[derive(Debug)]
pub struct PairDriver(pub(crate) Driver<Pair<Tree, Controller>>);

impl PairDriver {
    /// Runs both roles until the device stops, as [`DeviceDriver::run`]
    /// does. [`ControllerHandle::shutdown`](crate::ControllerHandle::shutdown)
    /// unsubscribes the controller's filters and leaves the device running.
    /// The future is `Send`.
    pub async fn run(self) -> Result<(), DriverError> {
        self.0.run().await
    }
}
