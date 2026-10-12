//! The task that owns the controller.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ebus_homie::controller::{Controller, DeviceView, Discovery, Event, Id};
use ebus_mqtt::{Command, Instant, Link, Message};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, error::TryRecvError};
use tokio::sync::{oneshot, watch};
use tokio::time;

use crate::commands::Tagged;
use crate::events::EventSender;
use crate::handle::{Request, Snapshot};
use crate::inbox::{Dropped, Inbound, Shared};

/// Inputs handled before commands, events and the snapshot are flushed.
pub(crate) const DRAIN_BATCH: usize = 256;

/// Why [`ControllerDriver::run`] stopped early.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DriverError {
    /// The command receiver was dropped while the controller still had
    /// commands for the MQTT client.
    CommandsClosed,
}

impl DriverError {
    /// camelCase reason code.
    pub const fn as_str(self) -> &'static str {
        match self {
            DriverError::CommandsClosed => "commandsClosed",
        }
    }
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::error::Error for DriverError {}

/// The command channel's sending end, with the count the stale-command
/// boundary and the credit use.
#[derive(Debug)]
pub(crate) struct Out {
    pub(crate) tx: UnboundedSender<Tagged>,
    /// Commands sent on `tx`.
    pub(crate) sent: u64,
    /// A send on `tx` failed.
    pub(crate) closed: bool,
}

impl Out {
    pub(crate) fn new(tx: UnboundedSender<Tagged>) -> Self {
        Out {
            tx,
            sent: 0,
            closed: false,
        }
    }

    pub(crate) fn send(&mut self, cmd: Command, device: bool) {
        self.sent += 1;
        if self.tx.send(Tagged { cmd, device }).is_err() {
            self.closed = true;
        }
    }

    pub(crate) fn result(&self) -> Result<(), DriverError> {
        if self.closed {
            Err(DriverError::CommandsClosed)
        } else {
            Ok(())
        }
    }
}

/// What the driver keeps for a controller: its handle's requests, its events
/// and its snapshot.
pub(crate) struct ControllerSide {
    pub(crate) requests: UnboundedReceiver<Request>,
    pub(crate) requests_open: bool,
    pub(crate) events: EventSender,
    pub(crate) snap: watch::Sender<Arc<Snapshot>>,
    /// Drops already reported to the core in message order, to heal once the
    /// backlog drains.
    pub(crate) lost: Dropped,
    /// What the inputs since the last flush touched.
    touched: Touched,
    /// Replies to send once a shutdown's commands are queued.
    pub(crate) stopping: Vec<oneshot::Sender<()>>,
}

/// The future that owns the [`Controller`]. The host spawns
/// [`run`](Self::run) on its own runtime; the SDK spawns nothing.
pub struct ControllerDriver {
    pub(crate) core: Controller,
    pub(crate) inbound: UnboundedReceiver<Inbound>,
    pub(crate) shared: Arc<Shared>,
    pub(crate) side: ControllerSide,
    pub(crate) out: Out,
}

impl fmt::Debug for ControllerDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControllerDriver")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

/// What a drain touched, for the snapshot refresh.
#[derive(Default)]
pub(crate) struct Touched {
    /// Compare every device, not only `ids`.
    all: bool,
    ids: BTreeSet<String>,
    /// A `$state`, a `$description` or a device-level event, which may change
    /// a tree's completeness.
    trees: bool,
}

impl Touched {
    fn id(&mut self, id: &str) {
        if !self.ids.contains(id) {
            self.ids.insert(id.to_owned());
        }
    }

    fn event(&mut self, event: &Event) {
        if !matches!(
            event,
            Event::PropertyChanged { .. }
                | Event::PropertyEvent { .. }
                | Event::PropertyCleared { .. }
                | Event::TargetChanged { .. }
                | Event::Alert { .. }
        ) {
            self.trees = true;
        }
        match event {
            Event::DeviceDiscovered { device, .. }
            | Event::StateChanged { device, .. }
            | Event::EffectiveStateChanged { device, .. }
            | Event::DescriptionChanged { device, .. }
            | Event::DescriptionRejected { device, .. }
            | Event::Alert { device, .. }
            | Event::DeviceSynced { device, .. }
            | Event::DeviceStuck { device, .. }
            | Event::DeviceAbsent { device, .. }
            | Event::DeviceRemoved { device, .. } => self.id(device.as_str()),
            Event::PropertyChanged { addr, .. }
            | Event::PropertyEvent { addr, .. }
            | Event::PropertyCleared { addr, .. }
            | Event::TargetChanged { addr, .. } => self.id(addr.device.as_str()),
            Event::TreeComplete { root, .. } => self.id(root.as_str()),
            _ => self.all = true,
        }
    }

    /// Tree completeness may have changed.
    fn trees(&self) -> bool {
        self.all || self.trees
    }
}

impl ControllerDriver {
    /// Runs the controller until [`ControllerHandle::shutdown`] has queued its
    /// commands, or every [`Inbox`](crate::Inbox) clone is dropped; both end
    /// with `Ok`. The future is `Send`. Dropping it stops the controller: the
    /// handles then read the last snapshot, [`EventReceiver::recv`] returns
    /// `None` after the queued deliveries, and commands fail with
    /// [`SetError::NotConnected`](ebus_homie::controller::SetError::NotConnected).
    ///
    /// [`ControllerHandle::shutdown`]: crate::ControllerHandle::shutdown
    /// [`EventReceiver::recv`]: crate::EventReceiver::recv
    pub async fn run(mut self) -> Result<(), DriverError> {
        let start = time::Instant::now();
        let mut inbox_open = true;
        let mut first: Option<Inbound> = None;
        loop {
            let mut drained = false;
            if let Some(item) = first.take() {
                self.inbound(clock(start), item);
            }
            for _ in 0..DRAIN_BATCH {
                match self.inbound.try_recv() {
                    Ok(item) => self.inbound(clock(start), item),
                    Err(TryRecvError::Empty) => {
                        drained = true;
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        inbox_open = false;
                        drained = true;
                        break;
                    }
                }
            }
            self.side.drain_requests(&mut self.core);
            if drained {
                self.side
                    .report_dropped(&mut self.core, &self.shared, clock(start));
            }
            let now = clock(start);
            if self.core.poll_timeout().is_some_and(|t| t <= now) {
                self.core.handle_timeout(now);
                self.side.touched.all = true;
            }
            send_all(&mut self.core, &mut self.out);
            let flushed = self.out.result();
            self.side.publish(&mut self.core, &self.shared);
            if !self.side.stopping.is_empty() {
                self.side.reply_stopping();
                return Ok(());
            }
            flushed?;
            if !inbox_open {
                return Ok(());
            }
            if !drained {
                // A backlog remains: let the host's other tasks, such as the
                // MQTT client's event loop, run between batches.
                tokio::task::yield_now().await;
                continue;
            }

            let deadline = deadline(start, now, self.core.poll_timeout());
            let requests_open = self.side.requests_open;
            tokio::select! {
                biased;
                item = self.inbound.recv() => match item {
                    Some(item) => first = Some(item),
                    None => inbox_open = false,
                },
                req = self.side.requests.recv(), if requests_open => match req {
                    Some(req) => self.side.request(&mut self.core, req),
                    None => self.side.requests_open = false,
                },
                () = sleep_until(deadline) => {}
            }
        }
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
                self.side.before_message(&mut self.core, &self.shared);
                self.core
                    .handle_message(now, Message::new(&topic, &payload, retain));
                self.side.touch_topic(&topic);
            }
            Inbound::Link(link) => {
                if link == Link::up(false) {
                    // What the core queued before the link-up falls below
                    // the boundary.
                    send_all(&mut self.core, &mut self.out);
                    self.shared.link_up(self.out.sent);
                }
                self.core.handle_link(now, link);
                self.side.touched.all = true;
            }
            Inbound::CommandFailed(cmd) => self.core.handle_command_failed(now, cmd),
            Inbound::Receipt(_) | Inbound::Wake => {}
        }
    }
}

/// Sends every command `core` has queued.
fn send_all(core: &mut Controller, out: &mut Out) {
    while let Some(cmd) = core.poll_command() {
        out.send(cmd, false);
    }
}

impl ControllerSide {
    pub(crate) fn new(
        requests: UnboundedReceiver<Request>,
        events: EventSender,
        snap: watch::Sender<Arc<Snapshot>>,
    ) -> Self {
        ControllerSide {
            requests,
            requests_open: true,
            events,
            snap,
            lost: Dropped::default(),
            touched: Touched {
                all: true,
                ..Touched::default()
            },
            stopping: Vec::new(),
        }
    }

    /// Handles every request queued now.
    pub(crate) fn drain_requests(&mut self, core: &mut Controller) {
        while self.requests_open {
            match self.requests.try_recv() {
                Ok(req) => self.request(core, req),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => self.requests_open = false,
            }
        }
    }

    /// Reports drops recorded before a message reaches the core.
    pub(crate) fn before_message(&mut self, core: &mut Controller, shared: &Shared) {
        if shared.has_dropped.load(Ordering::Acquire) {
            self.note_lost(core, shared);
        }
    }

    /// Marks what a message on `topic` may have changed.
    pub(crate) fn touch_topic(&mut self, topic: &str) {
        let mut levels = topic.splitn(4, '/').skip(2);
        if let Some(id) = levels.next() {
            self.touched.id(id);
        }
        if matches!(levels.next(), Some("$state" | "$description")) {
            self.touched.trees = true;
        }
    }

    /// Every device may have changed.
    pub(crate) fn touch_all(&mut self) {
        self.touched.all = true;
    }

    pub(crate) fn reply_stopping(&mut self) {
        for s in self.stopping.drain(..) {
            let _ = s.send(());
        }
    }

    pub(crate) fn request(&mut self, core: &mut Controller, req: Request) {
        match req {
            Request::Set {
                addr,
                payload,
                opts,
                reply,
            } => {
                let res = core.set(&addr, &payload, opts);
                match reply {
                    Some(reply) => {
                        let _ = reply.send(res);
                    }
                    None => {
                        if let Err(e) = res {
                            log::warn!(
                                "reason=setRejected,device={},node={},property={},error={}",
                                addr.device,
                                addr.node,
                                addr.property,
                                e
                            );
                        }
                    }
                }
            }
            Request::Broadcast {
                subtopic,
                payload,
                reply,
            } => {
                let _ = reply.send(core.broadcast(&subtopic, &payload));
            }
            Request::Shutdown { reply } => {
                core.shutdown();
                self.touched.all = true;
                self.stopping.push(reply);
            }
        }
    }

    /// Takes the drops the inbox recorded and reports them to the core before
    /// any later message, so no later marker ends a window over a gap.
    fn note_lost(&mut self, core: &mut Controller, shared: &Shared) {
        let Some(dropped) = shared.take_dropped() else {
            return;
        };
        for prefix in dropped.devices.keys() {
            core.handle_lost(&format!("{prefix}/$state"));
        }
        if dropped.all {
            let topics: Vec<String> = core
                .devices()
                .map(|d| format!("{}/5/{}/$state", d.domain, d.id))
                .collect();
            for t in topics {
                core.handle_lost(&t);
            }
        }
        self.lost.merge(dropped);
    }

    /// Reports dropped messages to the core once the backlog has drained.
    pub(crate) fn report_dropped(&mut self, core: &mut Controller, shared: &Shared, now: Instant) {
        self.note_lost(core, shared);
        let dropped = std::mem::take(&mut self.lost);
        for (prefix, is_state) in &dropped.devices {
            let suffix = if *is_state { "$state" } else { "$description" };
            core.handle_dropped(now, &format!("{prefix}/{suffix}"));
        }
        if dropped.all {
            let topics: Vec<String> = core
                .devices()
                .map(|d| format!("{}/5/{}/$state", d.domain, d.id))
                .collect();
            for t in topics {
                core.handle_dropped(now, &t);
            }
        }
    }

    /// Republishes the snapshot, then delivers events, so the snapshot
    /// already reflects an event when it is received. Call after the
    /// commands are sent.
    pub(crate) fn publish(&mut self, core: &mut Controller, shared: &Shared) {
        let events: Vec<Event> = std::iter::from_fn(|| core.poll_event()).collect();
        for event in &events {
            self.touched.event(event);
        }
        let touched = std::mem::take(&mut self.touched);
        self.refresh(core, shared, &touched);
        for event in events {
            self.events.deliver(event);
        }
    }

    /// Publishes a new snapshot when anything in it changed, sharing every
    /// unchanged device. Only the devices `touched` names are compared, unless
    /// it says all; each comparison and each copy takes constant time, and
    /// the device map copies only the path to each changed id. Tree
    /// completeness, which walks every tree, is recomputed only when
    /// `touched` says it may have changed.
    fn refresh(&mut self, core: &Controller, shared: &Shared, touched: &Touched) {
        let old = Arc::clone(&self.snap.borrow());
        let mut next: Option<Snapshot> = None;
        if touched.all {
            let mut ids: Vec<Id> = Vec::new();
            for v in core.devices() {
                if ids.last() != Some(&v.id) {
                    ids.push(v.id.clone());
                }
            }
            for id in old.ids() {
                if core.devices_with_id(id.as_str()).next().is_none() {
                    ids.push(id.clone());
                }
            }
            for id in &ids {
                sync_id(core, &old, id.as_str(), &mut next);
            }
        } else {
            for id in &touched.ids {
                sync_id(core, &old, id, &mut next);
            }
        }
        let connected = core.is_connected();
        if connected != old.connected {
            next.get_or_insert_with(|| Snapshot::clone(&old)).connected = connected;
        }
        if touched.trees() {
            let trees = trees(core, shared);
            if trees != *old.trees {
                next.get_or_insert_with(|| Snapshot::clone(&old)).trees = Arc::new(trees);
            }
        }
        drop(old);
        if let Some(next) = next {
            self.snap.send_replace(Arc::new(next));
        }
    }
}

/// Brings the devices with this id up to date in `next`, copying the old
/// snapshot on the first change.
fn sync_id(core: &Controller, old: &Arc<Snapshot>, id: &str, next: &mut Option<Snapshot>) {
    let olds = old.list(id);
    let mut list: Vec<Arc<DeviceView>> = Vec::with_capacity(olds.len());
    let mut changed = false;
    for v in core.devices_with_id(id) {
        match olds.iter().find(|o| o.domain == v.domain) {
            Some(o) if same(o, v) => list.push(Arc::clone(o)),
            _ => {
                changed = true;
                list.push(Arc::new(v.clone()));
            }
        }
    }
    if !changed && list.len() == olds.len() {
        return;
    }
    let key = match (list.first(), olds.first()) {
        (Some(v), _) | (None, Some(v)) => v.id.clone(),
        (None, None) => return,
    };
    next.get_or_insert_with(|| Snapshot::clone(old))
        .set(&key, list);
}

fn trees(core: &Controller, shared: &Shared) -> std::collections::BTreeMap<Id, bool> {
    #[cfg(test)]
    shared.tree_scans.fetch_add(1, Ordering::Relaxed);
    #[cfg(not(test))]
    let _ = shared;
    let mut out: std::collections::BTreeMap<Id, bool> = core
        .devices()
        .filter(|d| {
            d.description
                .as_ref()
                .is_some_and(|x| !x.children.is_empty())
        })
        .map(|d| (d.id.clone(), core.is_tree_complete(d.id.as_str())))
        .collect();
    if let Discovery::Tree { root } = &core.config().discovery {
        out.insert(root.clone(), core.is_tree_complete(root.as_str()));
    }
    out
}

/// Whether a published device snapshot still matches the controller's view.
/// The snapshot was cloned from the view, so their maps are shared until the
/// controller changes one.
fn same(a: &DeviceView, b: &DeviceView) -> bool {
    a.state == b.state
        && a.effective == b.effective
        && a.confirmed == b.confirmed
        && a.synced == b.synced
        && match (&a.description, &b.description) {
            (Some(x), Some(y)) => Arc::ptr_eq(x, y),
            (None, None) => true,
            _ => false,
        }
        && a.shares_values(b)
}

/// The tokio instant of the next deadline, at least 1 ms after `now`, so a
/// deadline `handle_timeout` just passed does not spin.
pub(crate) fn deadline(
    start: time::Instant,
    now: Instant,
    next: Option<Instant>,
) -> Option<time::Instant> {
    next.map(|t| {
        let t = t.max(now.add_ms(1));
        start
            .checked_add(Duration::from_millis(t.0))
            .unwrap_or_else(far_future)
    })
}

/// Milliseconds since `start` on the tokio clock.
pub(crate) fn clock(start: time::Instant) -> Instant {
    Instant(u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX))
}

fn far_future() -> time::Instant {
    time::Instant::now() + Duration::from_secs(86_400 * 365)
}

pub(crate) async fn sleep_until(deadline: Option<time::Instant>) {
    match deadline {
        Some(t) => time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}
