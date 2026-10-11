//! One rumqttc connection driven for the roles: the command executor, the
//! event-loop poller, and the delivery tracking the device role uses.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ebus_homie_tokio::device::Shutdown;
use ebus_homie_tokio::ebus_mqtt::{self, Command, LastWill, Receipt};
use ebus_homie_tokio::{Commands, DriverError, Inbox};
use rumqttc::{
    AsyncClient, ClientError, EventLoop, MqttOptions, Outgoing, Packet, Request, SubscribeFilter,
    SubscribeReasonCode,
};
use tokio::sync::{Notify, watch};

/// Requests rumqttc buffers between the command executor and the event loop.
pub(crate) const REQUEST_CAPACITY: usize = 64;

/// Reconnect backoff (client.py:155).
pub(crate) const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// The link as the executor sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wire {
    Down,
    Up,
    /// The event loop is gone.
    Ended,
}

/// What the executor asks of the poller once the commands have ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum End {
    Running,
    /// A DISCONNECT is queued, or the link is down: stop once it is written,
    /// or at once while down.
    Disconnect,
    /// Drop the connection without DISCONNECT, so the broker publishes the
    /// will.
    Drop,
}

/// A publish whose delivery the device role tracks.
#[derive(Debug, PartialEq, Eq)]
enum Mark {
    /// A `$description`: reported once written.
    Description(String),
    /// A zero-length retained publish: reported once acknowledged.
    Clear(String),
}

/// Looks the broker up again when connects to it fail: returns its options
/// at the current address, or `None` to keep the old ones. See
/// [`ControllerTask::reresolve`](crate::ControllerTask::reresolve).
pub type Reresolve =
    Box<dyn FnMut() -> Pin<Box<dyn Future<Output = Option<MqttOptions>> + Send>> + Send>;

/// Failed connects between [`Reresolve`] calls.
const RERESOLVE_EVERY: u32 = 3;

/// When to call [`Reresolve`]: after the first failed connect since the task
/// started or since the last link-up, then after every third.
#[derive(Debug, Default)]
pub(crate) struct ReresolveSchedule {
    /// Failed connects since the task started or the last link-up.
    failed: u32,
}

impl ReresolveSchedule {
    /// A CONNACK arrived.
    pub(crate) fn link_up(&mut self) {
        self.failed = 0;
    }

    /// A connect failed; true when [`Reresolve`] is due.
    pub(crate) fn connect_failed(&mut self) -> bool {
        self.failed = self.failed.saturating_add(1);
        self.failed % RERESOLVE_EVERY == 1
    }
}

/// The settings `device()` and `pair()` put on the caller's options, applied
/// again to options from [`Reresolve`].
#[derive(Clone, Debug)]
pub(crate) struct DeviceConf {
    pub(crate) will: LastWill,
    pub(crate) max_description: usize,
}

/// A [`Reresolve`] hook and what its options need before use.
pub(crate) struct Redirect {
    pub(crate) f: Reresolve,
    pub(crate) device: Option<DeviceConf>,
    pub(crate) schedule: ReresolveSchedule,
}

impl Redirect {
    pub(crate) fn new(f: Reresolve, device: Option<DeviceConf>) -> Self {
        Self {
            f,
            device,
            schedule: ReresolveSchedule::default(),
        }
    }

    /// `new` as the event loop can use it: the device's settings as at
    /// startup, and the inflight cap and manual acks the event loop's state
    /// was built for, since rumqttc sizes its packet-id range once.
    pub(crate) fn prepare(&self, new: &mut MqttOptions, current: &MqttOptions) {
        if let Some(d) = &self.device {
            crate::device::configure(new, &d.will, d.max_description);
        }
        new.set_inflight(current.inflight());
        new.set_manual_acks(current.manual_acks());
    }
}

/// Delivery tracking for one connection, matching rumqttc's packet ids in
/// write order: requests leave the channel in the order they were handed
/// over, and a clean-session reconnect discards every one not yet written.
#[derive(Debug, Default)]
pub(crate) struct Track {
    /// Publishes handed to rumqttc on this connection.
    handed: u64,
    /// `Outgoing::Publish` events seen on this connection.
    written: u64,
    /// Tracked publishes, by their number among the handed ones.
    marks: VecDeque<(u64, Mark)>,
    /// SUBSCRIBEs handed and not yet written, in order.
    subs: VecDeque<Vec<String>>,
    /// SUBSCRIBEs written, by packet id, awaiting their SUBACK.
    sub_ids: BTreeMap<u16, Vec<String>>,
    /// Clears written at QoS 1 or 2, by packet id, awaiting their ack.
    clears: BTreeMap<u16, String>,
    /// `EventLoop::state.inflight()` after the last event.
    inflight: u16,
}

impl Track {
    /// The mark a publish gets, if any; the topic is copied only for one.
    fn mark(topic: &str, empty: bool, retain: bool) -> Option<Mark> {
        if !retain {
            None
        } else if empty {
            Some(Mark::Clear(topic.to_owned()))
        } else if topic.ends_with("/$description") {
            Some(Mark::Description(topic.to_owned()))
        } else {
            None
        }
    }

    fn handed_publish(&mut self, mark: Option<Mark>) {
        self.handed += 1;
        if let Some(m) = mark {
            self.marks.push_back((self.handed, m));
        }
    }

    fn handed_subscribe(&mut self, filters: Vec<String>) {
        self.subs.push_back(filters);
    }

    /// An `Outgoing::Publish(pkid)`: the next handed publish was written.
    fn written_publish(&mut self, pkid: u16) -> Option<Receipt> {
        self.written += 1;
        if self.marks.front().map(|(n, _)| *n) != Some(self.written) {
            return None;
        }
        match self.marks.pop_front().map(|(_, m)| m)? {
            Mark::Description(topic) => Some(Receipt::Written { topic }),
            Mark::Clear(topic) => {
                if pkid != 0 {
                    self.clears.insert(pkid, topic);
                }
                None
            }
        }
    }

    fn written_subscribe(&mut self, pkid: u16) {
        if let Some(f) = self.subs.pop_front() {
            self.sub_ids.insert(pkid, f);
        }
    }

    fn suback(&mut self, pkid: u16, codes: &[SubscribeReasonCode]) -> Option<Receipt> {
        let filters = self.sub_ids.remove(&pkid)?;
        let granted = !codes
            .iter()
            .any(|c| matches!(c, SubscribeReasonCode::Failure));
        Some(Receipt::Subscribed { filters, granted })
    }

    /// A PUBACK or PUBCOMP.
    fn acked(&mut self, pkid: u16) -> Option<Receipt> {
        let topic = self.clears.remove(&pkid)?;
        Some(Receipt::Acknowledged {
            topic,
            cleared: true,
        })
    }

    /// Every handed publish was written and none awaits an ack.
    fn flushed(&self) -> bool {
        self.written >= self.handed && self.inflight == 0
    }

    /// The connection was lost: rumqttc discards what it had not written
    /// and, at the clean-session CONNACK, what it had.
    fn reset(&mut self) {
        *self = Track::default();
    }
}

fn lock(t: &Mutex<Track>) -> MutexGuard<'_, Track> {
    t.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What one connection runs.
pub(crate) struct Conn {
    pub(crate) inbox: Inbox,
    pub(crate) commands: Commands,
    pub(crate) client: AsyncClient,
    pub(crate) eventloop: EventLoop,
    /// Track deliveries and report them through the inbox.
    pub(crate) receipts: bool,
    /// How long `Announce` and `Remove` wait for their publishes.
    pub(crate) flush_timeout: Duration,
    /// Called as [`ReresolveSchedule`] says while the link is down.
    pub(crate) redirect: Option<Redirect>,
}

/// Runs `driver`, the poller and the executor in one task until all three
/// end, and returns the driver's result.
pub(crate) async fn run(
    driver: impl Future<Output = Result<(), DriverError>>,
    conn: Conn,
) -> Result<(), DriverError> {
    let Conn {
        inbox,
        commands,
        client,
        eventloop,
        receipts,
        flush_timeout,
        redirect,
    } = conn;
    let (end_tx, end_rx) = watch::channel(End::Running);
    let (done_tx, done_rx) = watch::channel(false);
    let (wire_tx, wire_rx) = watch::channel(Wire::Down);
    let progress = Notify::new();
    let track = receipts.then(|| Mutex::new(Track::default()));
    let driver = async move {
        let res = driver.await;
        let _ = done_tx.send(true);
        res
    };
    let poll_inbox = inbox.clone();
    let poller = async {
        poll(
            eventloop,
            poll_inbox,
            end_rx,
            done_rx,
            &wire_tx,
            &progress,
            track.as_ref(),
            redirect,
        )
        .await;
        wire_tx.send_replace(Wire::Ended);
    };
    let exec = Executor {
        client,
        inbox,
        track: track.as_ref(),
        progress: &progress,
        flush_timeout,
    };
    let (res, (), ()) = tokio::join!(driver, exec.run(commands, end_tx, wire_rx), poller);
    res
}

/// Hands commands to the client.
pub(crate) struct Executor<'a> {
    pub(crate) client: AsyncClient,
    pub(crate) inbox: Inbox,
    pub(crate) track: Option<&'a Mutex<Track>>,
    pub(crate) progress: &'a Notify,
    pub(crate) flush_timeout: Duration,
}

impl Executor<'_> {
    /// Hands each command to the client; the driver's end of the channel
    /// closing means it stopped, so the executor ends the connection as
    /// [`Commands::ending`] says.
    ///
    /// Nothing is handed over while the link is down: rumqttc would send it
    /// after the next CONNACK, clean or not. Handing over never waits, so a
    /// disconnect rumqttc notices finds each request either written or in its
    /// queue, which a clean-session CONNACK empties. A command that does not
    /// fit waits for the event loop to send something, and is checked again.
    pub(crate) async fn run(
        self,
        mut commands: Commands,
        end: watch::Sender<End>,
        mut wire: watch::Receiver<Wire>,
    ) {
        while let Some(mut cmd) = commands.recv().await {
            loop {
                let state = match wire.wait_for(|w| *w != Wire::Down).await {
                    Ok(w) => *w,
                    Err(_) => Wire::Ended,
                };
                if state == Wire::Ended {
                    self.inbox.command_failed(cmd);
                    break;
                }
                if commands.last_is_stale(&cmd) {
                    break;
                }
                match try_send(&self.client, cmd, self.track) {
                    Ok(()) => break,
                    Err(Unsent::Invalid(cmd)) => {
                        self.inbox.command_failed(cmd);
                        break;
                    }
                    Err(Unsent::Full(rest)) => {
                        cmd = rest;
                        tokio::select! {
                            () = self.progress.notified() => {}
                            _ = wire.changed() => {}
                        }
                    }
                }
            }
        }
        match commands.ending() {
            Some(Shutdown::Silent) => {
                end.send_replace(End::Drop);
                return;
            }
            Some(Shutdown::Announce | Shutdown::Remove) => self.flush(&mut wire).await,
            _ => {}
        }
        let _ = self.client.disconnect().await;
        end.send_replace(End::Disconnect);
    }

    /// Waits until every publish handed over was written and acknowledged,
    /// at most `flush_timeout`, or until the link goes down. rumqttc writes
    /// DISCONNECT without waiting for acks (state.rs:449-456), and its
    /// `inflight()` is zero while publishes still wait in the request
    /// channel (state.rs:333-334).
    async fn flush(&self, wire: &mut watch::Receiver<Wire>) {
        let Some(track) = self.track else {
            return;
        };
        let deadline = tokio::time::Instant::now() + self.flush_timeout;
        loop {
            if lock(track).flushed() || *wire.borrow() != Wire::Up {
                return;
            }
            tokio::select! {
                () = self.progress.notified() => {}
                _ = wire.changed() => {}
                () = tokio::time::sleep_until(deadline) => {
                    log::warn!("reason=flushTimeout");
                    return;
                }
            }
        }
    }
}

/// What [`try_send`] could not hand over.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Unsent {
    /// The request queue is full or gone; the part not handed over.
    Full(Command),
    /// rumqttc refuses the filters or topic.
    Invalid(Command),
}

/// One command through the client, without waiting. A publish's topic and
/// payload move into the request, and come back from it when it does not
/// fit.
pub(crate) fn try_send(
    client: &AsyncClient,
    cmd: Command,
    track: Option<&Mutex<Track>>,
) -> Result<(), Unsent> {
    match cmd {
        Command::Subscribe { filters } => {
            if filters.is_empty() || !filters.iter().all(|(f, _)| rumqttc::valid_filter(f)) {
                return Err(Unsent::Invalid(Command::Subscribe { filters }));
            }
            let requests = filters
                .iter()
                .map(|(f, q)| SubscribeFilter::new(f.clone(), qos(*q)));
            match client.try_subscribe_many(requests) {
                Ok(()) => {
                    if let Some(t) = track {
                        lock(t).handed_subscribe(filters.into_iter().map(|(f, _)| f).collect());
                    }
                    Ok(())
                }
                Err(_) => Err(Unsent::Full(Command::Subscribe { filters })),
            }
        }
        Command::Unsubscribe { filters } => {
            for (i, f) in filters.iter().enumerate() {
                if client.try_unsubscribe(f.clone()).is_err() {
                    return Err(Unsent::Full(Command::Unsubscribe {
                        filters: filters.iter().skip(i).cloned().collect(),
                    }));
                }
            }
            Ok(())
        }
        Command::Publish {
            topic,
            payload,
            qos: q,
            retain,
        } => {
            if !rumqttc::valid_topic(&topic) {
                return Err(Unsent::Invalid(Command::Publish {
                    topic,
                    payload,
                    qos: q,
                    retain,
                }));
            }
            let mark = track.and_then(|_| Track::mark(&topic, payload.is_empty(), retain));
            match client.try_publish(topic, qos(q), retain, payload) {
                Ok(()) => {
                    if let Some(t) = track {
                        lock(t).handed_publish(mark);
                    }
                    Ok(())
                }
                Err(ClientError::TryRequest(r) | ClientError::Request(r)) => match r {
                    Request::Publish(p) => Err(Unsent::Full(Command::Publish {
                        topic: p.topic,
                        payload: p.payload.to_vec(),
                        qos: q,
                        retain,
                    })),
                    // try_publish returns the request it was given.
                    _ => Ok(()),
                },
            }
        }
        _ => Ok(()),
    }
}

pub(crate) fn qos(q: ebus_mqtt::QoS) -> rumqttc::QoS {
    match q {
        ebus_mqtt::QoS::AtMostOnce => rumqttc::QoS::AtMostOnce,
        ebus_mqtt::QoS::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
        ebus_mqtt::QoS::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
    }
}

/// The backoff after `current`.
pub(crate) fn next_backoff(current: Duration) -> Duration {
    current.saturating_mul(2).min(BACKOFF_MAX)
}

/// Polls the event loop, feeding the inbox, until the executor's DISCONNECT
/// is written, the executor ended while the link was down, the executor
/// asks for a drop, or the driver stopped while the link is down. Returning
/// drops the event loop, so the executor's pending requests fail at once
/// instead of waiting for a reconnect. While the link is down, `redirect`
/// may replace the event loop's options before the next attempt.
#[allow(clippy::too_many_arguments)]
async fn poll(
    mut eventloop: EventLoop,
    inbox: Inbox,
    mut end: watch::Receiver<End>,
    mut done: watch::Receiver<bool>,
    wire: &watch::Sender<Wire>,
    progress: &Notify,
    track: Option<&Mutex<Track>>,
    mut redirect: Option<Redirect>,
) {
    let mut backoff = BACKOFF_MIN;
    let mut up = false;
    loop {
        if !up && *done.borrow() {
            log::debug!("reason=mqttStopWhileDown");
            return;
        }
        let event = if up {
            tokio::select! {
                e = eventloop.poll() => e,
                // The executor drops its sender once it has queued the
                // DISCONNECT; only an explicit `Drop` ends the poll here.
                Ok(_) = end.wait_for(|e| *e == End::Drop) => {
                    log::debug!("reason=mqttDropped");
                    return;
                }
            }
        } else {
            tokio::select! {
                e = eventloop.poll() => e,
                _ = done.wait_for(|d| *d) => continue,
            }
        };
        if let (Some(t), Ok(_)) = (track, &event) {
            lock(t).inflight = eventloop.state.inflight();
        }
        match event {
            Ok(rumqttc::Event::Incoming(Packet::ConnAck(ack))) => {
                log::info!(
                    "reason=mqttConnected,session_present={}",
                    ack.session_present
                );
                up = true;
                backoff = BACKOFF_MIN;
                if let Some(r) = redirect.as_mut() {
                    r.schedule.link_up();
                }
                inbox.connected(ack.session_present);
                wire.send_replace(Wire::Up);
            }
            Ok(rumqttc::Event::Incoming(Packet::Publish(p))) => {
                let bytes = p.topic.len() + p.payload.len();
                if !inbox.message(p.topic, p.payload, Some(p.retain)) {
                    log::debug!("reason=inboxDropped,bytes={bytes}");
                }
            }
            Ok(rumqttc::Event::Incoming(packet)) => {
                if let Some(t) = track {
                    let r = match &packet {
                        Packet::SubAck(a) => lock(t).suback(a.pkid, &a.return_codes),
                        Packet::PubAck(a) => lock(t).acked(a.pkid),
                        Packet::PubComp(a) => lock(t).acked(a.pkid),
                        _ => None,
                    };
                    if let Some(r) = r {
                        inbox.receipt(r);
                    }
                    progress.notify_one();
                }
            }
            Ok(rumqttc::Event::Outgoing(Outgoing::Disconnect)) => return,
            Ok(rumqttc::Event::Outgoing(out)) => {
                if let Some(t) = track {
                    let r = match out {
                        Outgoing::Publish(pkid) => lock(t).written_publish(pkid),
                        Outgoing::Subscribe(pkid) => {
                            lock(t).written_subscribe(pkid);
                            None
                        }
                        _ => None,
                    };
                    if let Some(r) = r {
                        inbox.receipt(r);
                    }
                }
                progress.notify_one();
            }
            Err(e) => {
                log::warn!(
                    "reason=mqttConnectionError,error={e},retry_ms={}",
                    backoff.as_millis()
                );
                // Before any await: the executor must not hand rumqttc a
                // request it would queue for the next connection.
                wire.send_replace(Wire::Down);
                if let Some(t) = track {
                    lock(t).reset();
                }
                discard_stale(&mut eventloop);
                let retry_at = tokio::time::Instant::now() + backoff;
                if up {
                    up = false;
                    inbox.disconnected();
                } else if let Some(r) = redirect.as_mut() {
                    if r.schedule.connect_failed() {
                        tokio::select! {
                            new = (r.f)() => {
                                if let Some(mut new) = new {
                                    r.prepare(&mut new, &eventloop.mqtt_options);
                                    let addr = new.broker_address();
                                    if addr != eventloop.mqtt_options.broker_address() {
                                        log::info!(
                                            "reason=mqttReresolved,host={},port={}",
                                            addr.0,
                                            addr.1
                                        );
                                    }
                                    eventloop.mqtt_options = new;
                                }
                            }
                            _ = end.wait_for(|e| *e != End::Running) => return,
                            _ = done.wait_for(|d| *d) => return,
                        }
                    }
                }
                tokio::select! {
                    () = tokio::time::sleep_until(retry_at) => {}
                    _ = end.wait_for(|e| *e != End::Running) => return,
                    _ = done.wait_for(|d| *d) => return,
                }
                backoff = next_backoff(backoff);
            }
        }
    }
}

/// Drops the events rumqttc buffered on the connection just lost, which it
/// would return after the next CONNACK (eventloop.rs:193): an
/// `Outgoing::Publish` queued before a failed write would count against the
/// new connection's tracking, and an ack's packet id may be reused there.
/// Received publishes are kept.
fn discard_stale(eventloop: &mut EventLoop) {
    eventloop
        .state
        .events
        .retain(|e| matches!(e, rumqttc::Event::Incoming(Packet::Publish(_))));
}

#[cfg(test)]
mod tests {
    use super::*;

    use rumqttc::{MqttOptions, Request};

    #[test]
    fn track_reports_suback_written_description_and_acked_clear() {
        let mut t = Track::default();
        t.handed_subscribe(vec!["ebus/5/r/+/+/set".into()]);
        for (topic, empty, retain) in [
            ("ebus/5/r/$state", false, true),
            ("ebus/5/r/$description", false, true),
            ("ebus/5/r/n/old", true, true),
            ("ebus/5/r/n/e", false, false),
        ] {
            t.handed_publish(Track::mark(topic, empty, retain));
        }
        t.written_subscribe(1);
        assert_eq!(t.written_publish(2), None, "a state is not tracked");
        assert_eq!(
            t.written_publish(3),
            Some(Receipt::Written {
                topic: "ebus/5/r/$description".into()
            })
        );
        assert_eq!(t.written_publish(4), None, "a clear waits for its ack");
        assert_eq!(t.written_publish(0), None);
        assert!(t.written >= t.handed);
        assert_eq!(
            t.suback(
                1,
                &[SubscribeReasonCode::Success(rumqttc::QoS::ExactlyOnce)]
            ),
            Some(Receipt::Subscribed {
                filters: vec!["ebus/5/r/+/+/set".into()],
                granted: true
            })
        );
        assert_eq!(t.acked(3), None, "only clears are reported");
        assert_eq!(
            t.acked(4),
            Some(Receipt::Acknowledged {
                topic: "ebus/5/r/n/old".into(),
                cleared: true
            })
        );
    }

    #[test]
    fn a_failed_suback_is_not_granted() {
        let mut t = Track::default();
        t.handed_subscribe(vec!["a".into(), "b".into()]);
        t.written_subscribe(7);
        assert_eq!(
            t.suback(
                7,
                &[
                    SubscribeReasonCode::Success(rumqttc::QoS::AtMostOnce),
                    SubscribeReasonCode::Failure
                ]
            ),
            Some(Receipt::Subscribed {
                filters: vec!["a".into(), "b".into()],
                granted: false
            })
        );
    }

    #[test]
    fn flushed_needs_every_publish_written_and_none_in_flight() {
        let mut t = Track::default();
        assert!(t.flushed());
        t.handed_publish(Track::mark("ebus/5/r/$state", false, true));
        assert!(!t.flushed(), "handed, not written");
        t.written_publish(1);
        t.inflight = 1;
        assert!(!t.flushed(), "written, not acknowledged");
        t.inflight = 0;
        assert!(t.flushed());
        t.handed_publish(Track::mark("ebus/5/r/$description", false, true));
        t.reset();
        assert!(t.flushed(), "a lost connection discards everything");
        assert!(t.marks.is_empty());
    }

    #[test]
    fn a_publish_that_does_not_fit_comes_back_whole() {
        let mqtt = MqttOptions::new("t", "localhost", 1883);
        let (client, mut el) = AsyncClient::new(mqtt, 1);
        let track = Mutex::new(Track::default());
        let p = |payload: &[u8]| Command::Publish {
            topic: "ebus/5/r/$description".into(),
            payload: payload.to_vec(),
            qos: ebus_mqtt::QoS::ExactlyOnce,
            retain: true,
        };
        try_send(&client, p(b"one"), Some(&track)).expect("fits");
        assert_eq!(
            try_send(&client, p(b"two"), Some(&track)),
            Err(Unsent::Full(p(b"two")))
        );
        assert_eq!(lock(&track).handed, 1, "only the handed one counts");
        el.clean();
        let got: Vec<Request> = el.pending.drain(..).collect();
        assert!(matches!(&got[..], [Request::Publish(x)] if &x.payload[..] == b"one"));
    }

    /// A broker that accepts one client and answers its CONNECT; true when
    /// a DISCONNECT arrived before the connection closed.
    async fn one_client_broker() -> (u16, tokio::task::JoinHandle<bool>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("address").port();
        let broker = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.expect("accept");
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = s.read(&mut chunk).await.unwrap_or(0);
                if n == 0 {
                    return false;
                }
                buf.extend_from_slice(&chunk[..n]);
                // Every packet here is shorter than 128 bytes: one length byte.
                while buf.len() >= 2 && buf.len() >= 2 + usize::from(buf[1]) {
                    match buf[0] >> 4 {
                        1 => s.write_all(&[0x20, 2, 0, 0]).await.expect("CONNACK"),
                        14 => return true,
                        _ => {}
                    }
                    buf.drain(..2 + usize::from(buf[1]));
                }
            }
        });
        (port, broker)
    }

    #[tokio::test]
    async fn the_executor_dropping_its_end_sender_does_not_drop_the_connection() {
        let (port, broker) = one_client_broker().await;
        let (client, eventloop) = AsyncClient::new(MqttOptions::new("r", "127.0.0.1", port), 8);
        let tree = ebus_homie_tokio::device::TreeBuilder::new(
            ebus_homie_tokio::device::TreeConfig::default(),
            "r",
            ebus_homie_tokio::device::DeviceSpec::default(),
        )
        .expect("tree");
        let dev = ebus_homie_tokio::device::attach_device(
            tree,
            ebus_homie_tokio::device::DeviceOptions::default(),
        )
        .expect("device");
        let (end_tx, end_rx) = watch::channel(End::Running);
        let (_done_tx, done_rx) = watch::channel(false);
        let wire: &'static watch::Sender<Wire> = Box::leak(Box::new(watch::channel(Wire::Down).0));
        let progress: &'static Notify = Box::leak(Box::new(Notify::new()));
        let mut up = wire.subscribe();
        let poller = tokio::spawn(poll(
            eventloop, dev.inbox, end_rx, done_rx, wire, progress, None, None,
        ));
        up.wait_for(|w| *w == Wire::Up)
            .await
            .expect("poller running");
        // As the executor leaves it after a clean shutdown; the poller runs
        // before the DISCONNECT is queued.
        end_tx.send_replace(End::Disconnect);
        drop(end_tx);
        tokio::task::yield_now().await;
        client.disconnect().await.expect("DISCONNECT queued");
        tokio::time::timeout(Duration::from_secs(5), poller)
            .await
            .expect("poller ends")
            .expect("poller did not panic");
        assert!(broker.await.expect("broker"), "no DISCONNECT written");
    }

    #[tokio::test]
    async fn a_write_lost_with_its_connection_is_not_counted_on_the_next() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("address").port();
        // The first connection stops reading after its CONNACK, so a large
        // publish fails its write; the second reads everything.
        tokio::spawn(async move {
            let mut buf = [0u8; 64];
            let (mut first, _) = listener.accept().await.expect("accept");
            let _ = first.read(&mut buf).await;
            first.write_all(&[0x20, 2, 0, 0]).await.expect("CONNACK");
            let (mut second, _) = listener.accept().await.expect("accept");
            let _ = second.read(&mut buf).await;
            second.write_all(&[0x20, 2, 0, 0]).await.expect("CONNACK");
            let mut sink = vec![0u8; 1 << 16];
            while second.read(&mut sink).await.unwrap_or(0) > 0 {}
            drop(first);
        });
        let mut mqtt = MqttOptions::new("r", "127.0.0.1", port);
        mqtt.set_max_packet_size(1 << 20, 64 << 20);
        let (client, mut eventloop) = AsyncClient::new(mqtt, 8);
        let mut net = eventloop.network_options();
        net.set_connection_timeout(1);
        eventloop.set_network_options(net);
        let tree = ebus_homie_tokio::device::TreeBuilder::new(
            ebus_homie_tokio::device::TreeConfig::default(),
            "r",
            ebus_homie_tokio::device::DeviceSpec::default(),
        )
        .expect("tree");
        let dev = ebus_homie_tokio::device::attach_device(
            tree,
            ebus_homie_tokio::device::DeviceOptions::default(),
        )
        .expect("device");
        let (_end_tx, end_rx) = watch::channel(End::Running);
        let (_done_tx, done_rx) = watch::channel(false);
        let wire: &'static watch::Sender<Wire> = Box::leak(Box::new(watch::channel(Wire::Down).0));
        let progress: &'static Notify = Box::leak(Box::new(Notify::new()));
        let track: &'static Mutex<Track> = Box::leak(Box::new(Mutex::new(Track::default())));
        let mut link = wire.subscribe();
        tokio::spawn(poll(
            eventloop,
            dev.inbox,
            end_rx,
            done_rx,
            wire,
            progress,
            Some(track),
            None,
        ));
        let publish = |topic: &str, payload: Vec<u8>| Command::Publish {
            topic: topic.into(),
            payload,
            qos: ebus_mqtt::QoS::AtLeastOnce,
            retain: true,
        };
        let within = Duration::from_secs(10);
        tokio::time::timeout(within, link.wait_for(|w| *w == Wire::Up))
            .await
            .expect("first CONNACK")
            .expect("poller running");
        try_send(
            &client,
            publish("ebus/5/r/n/big", vec![7; 32 << 20]),
            Some(track),
        )
        .expect("handed");
        tokio::time::timeout(within, link.wait_for(|w| *w == Wire::Down))
            .await
            .expect("the write fails")
            .expect("poller running");
        tokio::time::timeout(within, link.wait_for(|w| *w == Wire::Up))
            .await
            .expect("second CONNACK")
            .expect("poller running");
        try_send(
            &client,
            publish("ebus/5/r/$description", b"{}".to_vec()),
            Some(track),
        )
        .expect("handed");
        let tracked = async {
            loop {
                {
                    let t = lock(track);
                    if t.handed == 1 && t.written == 1 && t.marks.is_empty() {
                        return;
                    }
                }
                progress.notified().await;
            }
        };
        assert!(
            tokio::time::timeout(within, tracked).await.is_ok(),
            "{:?}",
            lock(track)
        );
    }

    #[test]
    fn reresolve_runs_after_the_first_failed_connect_then_every_third() {
        let mut s = ReresolveSchedule::default();
        let startup: Vec<u32> = (1..=7).filter(|_| s.connect_failed()).collect();
        assert_eq!(startup, [1, 4, 7], "before any link-up");
        s.link_up();
        let due: Vec<u32> = (1..=10).filter(|_| s.connect_failed()).collect();
        assert_eq!(due, [1, 4, 7, 10]);
        s.link_up();
        assert!(s.connect_failed(), "a new lost link starts the count again");
        assert!(!s.connect_failed());
    }

    #[test]
    fn reresolved_options_get_the_device_settings_and_the_event_loop_limits() {
        let will = LastWill {
            topic: "ebus/5/r/$state".into(),
            payload: b"lost".to_vec(),
            qos: ebus_mqtt::QoS::ExactlyOnce,
            retain: true,
        };
        let hook: Reresolve = Box::new(|| Box::pin(async { None }));
        let r = Redirect::new(
            hook,
            Some(DeviceConf {
                will,
                max_description: 10_000,
            }),
        );
        let mut current = MqttOptions::new("r", "127.0.0.1", 1883);
        current.set_inflight(7);
        current.set_manual_acks(true);
        let mut new = MqttOptions::new("r", "192.0.2.7", 1883);
        new.set_clean_session(false);
        new.set_inflight(100);
        r.prepare(&mut new, &current);
        let w = new.last_will().expect("will");
        assert_eq!(w.topic, "ebus/5/r/$state");
        assert_eq!(&w.message[..], b"lost");
        assert!(w.retain);
        assert!(new.clean_session());
        assert_eq!(new.inflight(), 7, "the event loop's state is sized for 7");
        assert!(new.manual_acks(), "the event loop's manual-ack setting");
        assert_eq!(new.broker_address(), ("192.0.2.7".to_owned(), 1883));

        let hook: Reresolve = Box::new(|| Box::pin(async { None }));
        let r = Redirect::new(hook, None);
        let mut new = MqttOptions::new("c", "192.0.2.7", 1883);
        new.set_clean_session(false);
        r.prepare(&mut new, &current);
        assert!(new.last_will().is_none(), "a controller has no will");
        assert!(
            !new.clean_session(),
            "a controller's options are the host's"
        );
        assert_eq!(new.inflight(), current.inflight());
        assert!(new.manual_acks());
    }

    /// A poll loop for a device whose options name `port`, with `hook`;
    /// returns its link state.
    fn spawn_poll(port: u16, hook: Reresolve) -> watch::Receiver<Wire> {
        let (_client, eventloop) = AsyncClient::new(MqttOptions::new("r", "127.0.0.1", port), 8);
        let tree = ebus_homie_tokio::device::TreeBuilder::new(
            ebus_homie_tokio::device::TreeConfig::default(),
            "r",
            ebus_homie_tokio::device::DeviceSpec::default(),
        )
        .expect("tree");
        let dev = ebus_homie_tokio::device::attach_device(
            tree,
            ebus_homie_tokio::device::DeviceOptions::default(),
        )
        .expect("device");
        let (end_tx, end_rx) = watch::channel(End::Running);
        let (done_tx, done_rx) = watch::channel(false);
        // Kept for the test's lifetime, so the poll loop keeps running.
        Box::leak(Box::new((end_tx, done_tx)));
        let wire: &'static watch::Sender<Wire> = Box::leak(Box::new(watch::channel(Wire::Down).0));
        let progress: &'static Notify = Box::leak(Box::new(Notify::new()));
        let link = wire.subscribe();
        tokio::spawn(poll(
            eventloop,
            dev.inbox,
            end_rx,
            done_rx,
            wire,
            progress,
            None,
            Some(Redirect::new(hook, None)),
        ));
        link
    }

    #[tokio::test]
    async fn a_broker_down_at_startup_is_replaced_by_the_reresolved_one() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Nothing listens on A's port; broker B answers.
        let a_port = {
            let a = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            a.local_addr().expect("address").port()
        };
        let b = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let b_port = b.local_addr().expect("address").port();
        tokio::spawn(async move {
            let mut buf = [0u8; 256];
            let (mut s, _) = b.accept().await.expect("accept");
            let _ = s.read(&mut buf).await;
            s.write_all(&[0x20, 2, 0, 0]).await.expect("CONNACK");
            while s.read(&mut buf).await.unwrap_or(0) > 0 {}
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let hook: Reresolve = Box::new(move || {
            c.fetch_add(1, Ordering::AcqRel);
            Box::pin(async move { Some(MqttOptions::new("r", "127.0.0.1", b_port)) })
        });
        let mut link = spawn_poll(a_port, hook);
        tokio::time::timeout(Duration::from_secs(15), link.wait_for(|w| *w == Wire::Up))
            .await
            .expect("up on B")
            .expect("poller running");
        assert_eq!(
            calls.load(Ordering::Acquire),
            1,
            "called after the first failed connect"
        );
    }

    #[tokio::test]
    async fn the_reresolved_broker_is_used_for_the_next_attempt() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Broker A answers one CONNECT, then closes the connection and stops
        // listening; broker B answers the next.
        let a = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let a_port = a.local_addr().expect("address").port();
        let b = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let b_port = b.local_addr().expect("address").port();
        tokio::spawn(async move {
            let mut buf = [0u8; 256];
            let (mut s, _) = a.accept().await.expect("accept");
            let _ = s.read(&mut buf).await;
            s.write_all(&[0x20, 2, 0, 0]).await.expect("CONNACK");
            drop(a);
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let (b_tx, b_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut buf = [0u8; 256];
            let (mut s, _) = b.accept().await.expect("accept");
            let _ = s.read(&mut buf).await;
            s.write_all(&[0x20, 2, 0, 0]).await.expect("CONNACK");
            let _ = b_tx.send(());
            while s.read(&mut buf).await.unwrap_or(0) > 0 {}
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let hook: Reresolve = Box::new(move || {
            c.fetch_add(1, Ordering::AcqRel);
            Box::pin(async move { Some(MqttOptions::new("r", "127.0.0.1", b_port)) })
        });
        let mut link = spawn_poll(a_port, hook);
        let within = Duration::from_secs(15);
        tokio::time::timeout(within, link.wait_for(|w| *w == Wire::Up))
            .await
            .expect("connected to A")
            .expect("poller running");
        assert_eq!(calls.load(Ordering::Acquire), 0, "not called while up");
        tokio::time::timeout(within, link.wait_for(|w| *w == Wire::Down))
            .await
            .expect("A closed the link")
            .expect("poller running");
        assert_eq!(
            calls.load(Ordering::Acquire),
            0,
            "a lost link is not a failed connect"
        );
        tokio::time::timeout(within, b_rx)
            .await
            .expect("connected to B")
            .expect("B answered");
        tokio::time::timeout(within, link.wait_for(|w| *w == Wire::Up))
            .await
            .expect("up on B")
            .expect("poller running");
        assert_eq!(calls.load(Ordering::Acquire), 1);
    }
}
