//! Bundled rumqttc 0.25 connection for the eBus Homie 5 controller on tokio.
//!
//! [`controller`] builds an [`ebus_homie_tokio`] controller with its own
//! rumqttc client. The host spawns [`ControllerTask::run`]; the SDK spawns
//! nothing.
//!
//! ```no_run
//! use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event};
//! use ebus_rumqttc::ebus_homie_tokio::{AttachOptions, Delivered};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mqtt = rumqttc::MqttOptions::new(ebus_rumqttc::client_id(), "localhost", 1883);
//! let run = ebus_rumqttc::controller(mqtt, Config::wildcard(), AttachOptions::default())?;
//! tokio::spawn(run.task.run());
//!
//! let mut events = run.events;
//! while let Some(d) = events.recv().await {
//!     match d {
//!         Delivered::Event(Event::DeviceSynced { device, missing: 0 }) => {
//!             let _values = run.handle.device(&device);
//!         }
//!         Delivered::Overflow { .. } => {
//!             let _all = run.handle.devices();
//!         }
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The transport is MQTT over TCP, or TLS with the `rustls` or `native-tls`
//! feature. rumqttc's default maximum incoming packet size is 10 KiB, and a
//! larger `$description` fails the connection; raise it with
//! [`MqttOptions::set_max_packet_size`](rumqttc::MqttOptions::set_max_packet_size)
//! for devices that publish one.
//!
//! On a clean-session reconnect rumqttc discards the requests it had not sent
//! and restores no subscriptions; the controller resubscribes its filters on
//! every CONNACK. The executor hands rumqttc nothing while the link is down
//! and skips every SUBSCRIBE queued before a clean-session link-up, so none is
//! sent after it, and the controller runs with
//! [`Config::link_up_discards_queued`] set.

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

pub use ebus_homie_tokio;
pub use rumqttc;

use ebus_homie_tokio::ebus_homie::controller::{Config, ConfigError};
use ebus_homie_tokio::ebus_mqtt::{self, Command};
use ebus_homie_tokio::{
    AttachOptions, Commands, ControllerDriver, ControllerHandle, DriverError, EventReceiver, Inbox,
    attach,
};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, Outgoing, Packet, SubscribeFilter};
use tokio::sync::{Notify, oneshot, watch};

/// Requests rumqttc buffers between the command executor and the event loop.
const REQUEST_CAPACITY: usize = 64;

/// Reconnect backoff (client.py:155).
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Why [`ControllerTask::run`] stopped early.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Error {
    /// The driver stopped with an error.
    Driver(DriverError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Driver(e) => write!(f, "driver: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Driver(e) => Some(e),
        }
    }
}

impl From<DriverError> for Error {
    fn from(e: DriverError) -> Self {
        Error::Driver(e)
    }
}

/// A controller with its own connection.
#[derive(Debug)]
pub struct Running {
    /// Snapshot reads, `/set` and `$broadcast`.
    pub handle: ControllerHandle,
    /// The controller's events.
    pub events: EventReceiver,
    /// The future the host spawns.
    pub task: ControllerTask,
}

/// The driver, the rumqttc event loop and the command executor, as one
/// future.
pub struct ControllerTask {
    driver: ControllerDriver,
    inbox: Inbox,
    commands: Commands,
    client: AsyncClient,
    eventloop: EventLoop,
}

impl fmt::Debug for ControllerTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControllerTask")
            .field("driver", &self.driver)
            .field("broker", &self.eventloop.mqtt_options.broker_address())
            .finish_non_exhaustive()
    }
}

/// A controller for `config` that connects with `mqtt`, with
/// [`Config::link_up_discards_queued`] set. Nothing runs until the host spawns
/// [`ControllerTask::run`].
pub fn controller(
    mqtt: MqttOptions,
    mut config: Config,
    opts: AttachOptions,
) -> Result<Running, ConfigError> {
    config.link_up_discards_queued = true;
    let a = attach(config, opts)?;
    let (client, eventloop) = AsyncClient::new(mqtt, REQUEST_CAPACITY);
    Ok(Running {
        handle: a.handle,
        events: a.events,
        task: ControllerTask {
            driver: a.driver,
            inbox: a.inbox,
            commands: a.commands,
            client,
            eventloop,
        },
    })
}

/// `ebus-controller-` and 16 random hex digits, after python-sdk's
/// `homie-controller-{uuid}` (homie.py:3870).
pub fn client_id() -> String {
    let mut h = RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.write_u32(std::process::id());
    format!("ebus-controller-{:016x}", h.finish())
}

/// The link as the executor sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wire {
    Down,
    Up,
    /// The event loop is gone.
    Ended,
}

impl ControllerTask {
    /// Runs until [`ControllerHandle::shutdown`]: the driver; an
    /// `EventLoop::poll` loop that feeds the inbox and reconnects with a
    /// 1-30 s backoff; and a command executor, separate so a full request
    /// channel cannot deadlock against `poll`. After the shutdown's
    /// UNSUBSCRIBE the executor sends a DISCONNECT, and the task ends once it
    /// is written, or at once when the link is down, discarding the commands
    /// not yet written. The future is `Send`.
    ///
    /// The three run in one task. The driver yields between batches of
    /// received messages, so a backlog does not starve the event loop.
    pub async fn run(self) -> Result<(), Error> {
        let ControllerTask {
            driver,
            inbox,
            commands,
            client,
            eventloop,
        } = self;
        let (stop_tx, stop_rx) = oneshot::channel();
        let (done_tx, done_rx) = watch::channel(false);
        let (wire_tx, wire_rx) = watch::channel(Wire::Down);
        let progress = Notify::new();
        let driver = async move {
            let res = driver.run().await;
            let _ = done_tx.send(true);
            res
        };
        let poll_inbox = inbox.clone();
        let poll = async {
            poll(eventloop, poll_inbox, stop_rx, done_rx, &wire_tx, &progress).await;
            wire_tx.send_replace(Wire::Ended);
        };
        let (res, (), ()) = tokio::join!(
            driver,
            execute(commands, client, inbox, stop_tx, wire_rx, &progress),
            poll,
        );
        res.map_err(Error::Driver)
    }
}

/// Hands each command to the client; the driver's end of the channel closing
/// means it stopped, so the executor disconnects.
///
/// Nothing is handed over while the link is down: rumqttc would send it after
/// the next CONNACK, clean or not. Handing over never waits, so a disconnect
/// rumqttc notices finds each request either written or in its queue, which
/// a clean-session CONNACK empties. A command that does not fit waits for the
/// event loop to send something, and is checked again.
async fn execute(
    mut commands: Commands,
    client: AsyncClient,
    inbox: Inbox,
    stop: oneshot::Sender<()>,
    mut wire: watch::Receiver<Wire>,
    progress: &Notify,
) {
    while let Some(mut cmd) = commands.recv().await {
        loop {
            let state = match wire.wait_for(|w| *w != Wire::Down).await {
                Ok(w) => *w,
                Err(_) => Wire::Ended,
            };
            if state == Wire::Ended {
                inbox.command_failed(cmd);
                break;
            }
            if commands.last_is_stale(&cmd) {
                break;
            }
            match try_send(&client, cmd) {
                Ok(()) => break,
                Err(Unsent::Invalid(cmd)) => {
                    inbox.command_failed(cmd);
                    break;
                }
                Err(Unsent::Full(rest)) => {
                    cmd = rest;
                    tokio::select! {
                        () = progress.notified() => {}
                        _ = wire.changed() => {}
                    }
                }
            }
        }
    }
    let _ = client.disconnect().await;
    let _ = stop.send(());
}

/// What [`try_send`] could not hand over.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Unsent {
    /// The request queue is full or gone; the part not handed over.
    Full(Command),
    /// rumqttc refuses the filters or topic.
    Invalid(Command),
}

/// One command through the client, without waiting.
pub(crate) fn try_send(client: &AsyncClient, cmd: Command) -> Result<(), Unsent> {
    match &cmd {
        Command::Subscribe { filters } => {
            if filters.is_empty() || !filters.iter().all(|(f, _)| rumqttc::valid_filter(f)) {
                return Err(Unsent::Invalid(cmd));
            }
            let filters = filters
                .iter()
                .map(|(f, q)| SubscribeFilter::new(f.clone(), qos(*q)));
            client
                .try_subscribe_many(filters)
                .map_err(|_| Unsent::Full(cmd))
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
            if !rumqttc::valid_topic(topic) {
                return Err(Unsent::Invalid(cmd));
            }
            client
                .try_publish(topic.clone(), qos(*q), *retain, payload.clone())
                .map_err(|_| Unsent::Full(cmd))
        }
        _ => Ok(()),
    }
}

fn qos(q: ebus_mqtt::QoS) -> rumqttc::QoS {
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
/// is written, the executor stopped while the link was down, or the driver
/// stopped while the link is down. Returning drops the event loop, so the
/// executor's pending requests fail at once instead of waiting for a
/// reconnect.
async fn poll(
    mut eventloop: EventLoop,
    inbox: Inbox,
    mut stop: oneshot::Receiver<()>,
    mut done: watch::Receiver<bool>,
    wire: &watch::Sender<Wire>,
    progress: &Notify,
) {
    let mut backoff = BACKOFF_MIN;
    let mut up = false;
    loop {
        if !up && *done.borrow() {
            log::debug!("reason=mqttStopWhileDown");
            return;
        }
        let event = if up {
            eventloop.poll().await
        } else {
            tokio::select! {
                e = eventloop.poll() => e,
                _ = done.wait_for(|d| *d) => continue,
            }
        };
        match event {
            Ok(rumqttc::Event::Incoming(Packet::ConnAck(ack))) => {
                log::info!(
                    "reason=mqttConnected,session_present={}",
                    ack.session_present
                );
                up = true;
                backoff = BACKOFF_MIN;
                inbox.connected(ack.session_present);
                wire.send_replace(Wire::Up);
            }
            Ok(rumqttc::Event::Incoming(Packet::Publish(p))) => {
                let bytes = p.topic.len() + p.payload.len();
                if !inbox.message(p.topic, p.payload, Some(p.retain)) {
                    log::debug!("reason=inboxDropped,bytes={bytes}");
                }
            }
            Ok(rumqttc::Event::Outgoing(Outgoing::Disconnect)) => return,
            Ok(rumqttc::Event::Outgoing(_)) => progress.notify_one(),
            Ok(_) => {}
            Err(e) => {
                log::warn!(
                    "reason=mqttConnectionError,error={e},retry_ms={}",
                    backoff.as_millis()
                );
                // Before any await: the executor must not hand rumqttc a
                // request it would queue for the next connection.
                wire.send_replace(Wire::Down);
                if up {
                    up = false;
                    inbox.disconnected();
                }
                tokio::select! {
                    () = tokio::time::sleep(backoff) => {}
                    _ = &mut stop => return,
                    _ = done.wait_for(|d| *d) => return,
                }
                backoff = next_backoff(backoff);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rumqttc::Request;

    fn send_check<T: Send>(_: &T) {}

    #[test]
    fn task_future_is_send() {
        let mqtt = MqttOptions::new(client_id(), "localhost", 1883);
        let run =
            controller(mqtt, Config::wildcard(), AttachOptions::default()).expect("valid config");
        send_check(&run.task.run());
    }

    #[test]
    fn client_ids_are_distinct_and_prefixed() {
        let a = client_id();
        let b = client_id();
        assert!(a.starts_with("ebus-controller-"));
        assert_eq!(a.len(), "ebus-controller-".len() + 16);
        assert_ne!(a, b);
    }

    #[test]
    fn backoff_doubles_to_thirty_seconds() {
        let mut b = BACKOFF_MIN;
        let mut seen = vec![b.as_secs()];
        for _ in 0..7 {
            b = next_backoff(b);
            seen.push(b.as_secs());
        }
        assert_eq!(seen, [1, 2, 4, 8, 16, 30, 30, 30]);
    }

    #[tokio::test]
    async fn commands_become_rumqttc_requests() {
        let mqtt = MqttOptions::new("t", "localhost", 1883);
        let (client, mut el) = AsyncClient::new(mqtt, 16);
        let cmds = [
            Command::Subscribe {
                filters: vec![
                    ("ebus/5/+/$state".into(), ebus_mqtt::QoS::ExactlyOnce),
                    ("ebus/5/d/+/+".into(), ebus_mqtt::QoS::AtLeastOnce),
                ],
            },
            Command::Unsubscribe {
                filters: vec!["a/b".into(), "c/d".into()],
            },
            Command::Publish {
                topic: "ebus/5/d/n/p/set".into(),
                payload: b"true".to_vec(),
                qos: ebus_mqtt::QoS::AtMostOnce,
                retain: false,
            },
        ];
        for c in cmds {
            try_send(&client, c).expect("queued");
        }
        // clean() moves the queued requests into `pending`.
        el.clean();
        let got: Vec<Request> = el.pending.drain(..).collect();
        assert_eq!(got.len(), 4, "{got:?}");
        let Request::Subscribe(s) = &got[0] else {
            panic!("{got:?}");
        };
        assert_eq!(
            s.filters,
            vec![
                SubscribeFilter::new("ebus/5/+/$state".into(), rumqttc::QoS::ExactlyOnce),
                SubscribeFilter::new("ebus/5/d/+/+".into(), rumqttc::QoS::AtLeastOnce),
            ]
        );
        assert!(matches!(&got[1], Request::Unsubscribe(u) if u.topics == ["a/b"]));
        assert!(matches!(&got[2], Request::Unsubscribe(u) if u.topics == ["c/d"]));
        let Request::Publish(p) = &got[3] else {
            panic!("{got:?}");
        };
        assert_eq!(p.topic, "ebus/5/d/n/p/set");
        assert_eq!(&p.payload[..], b"true");
        assert_eq!(p.qos, rumqttc::QoS::AtMostOnce);
        assert!(!p.retain);
    }

    #[tokio::test]
    async fn shutdown_while_the_broker_is_down_ends_the_task() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port")
            .port();
        let mqtt = MqttOptions::new(client_id(), "127.0.0.1", port);
        let run = controller(mqtt, Config::wildcard(), AttachOptions::default()).expect("valid");
        // The controller believes it is connected and holds more filters than
        // rumqttc's request channel buffers, while no broker listens.
        let inbox = run.task.inbox.clone();
        inbox.connected(false);
        for i in 0..2 * REQUEST_CAPACITY {
            let topic = format!("ebus/5/d{i}/$state");
            assert!(inbox.message(topic, b"ready".to_vec().into(), Some(true)));
        }
        drop(inbox);
        let task = tokio::spawn(run.task.run());
        tokio::time::sleep(Duration::from_millis(200)).await;
        run.handle.shutdown().await;
        let ended = tokio::time::timeout(Duration::from_secs(10), task).await;
        assert!(
            matches!(ended, Ok(Ok(Ok(())))),
            "the task did not end: {ended:?}"
        );
    }

    /// Runs `fut` for `ms` milliseconds; it must not finish.
    async fn run_for(fut: &mut std::pin::Pin<&mut impl std::future::Future<Output = ()>>, ms: u64) {
        tokio::select! {
            () = fut.as_mut() => panic!("the executor stopped"),
            () = tokio::time::sleep(Duration::from_millis(ms)) => {}
        }
    }

    #[tokio::test]
    async fn the_executor_holds_commands_while_down_and_skips_stale_subscribes() {
        // As `controller` configures it.
        let mut cfg = Config::wildcard();
        cfg.link_up_discards_queued = true;
        let a = attach(cfg, AttachOptions::default()).expect("valid");
        let inbox = a.inbox.clone();
        tokio::spawn(a.driver.run());
        let (client, mut el) = AsyncClient::new(MqttOptions::new("t", "localhost", 1883), 16);
        let (stop_tx, _stop_rx) = oneshot::channel();
        let (wire_tx, wire_rx) = watch::channel(Wire::Down);
        let progress = Notify::new();
        let exec = execute(a.commands, client, a.inbox, stop_tx, wire_rx, &progress);
        tokio::pin!(exec);

        // The discovery SUBSCRIBE waits while the link is down.
        inbox.connected(false);
        run_for(&mut exec, 50).await;
        el.clean();
        assert!(el.pending.is_empty(), "{:?}", el.pending);

        // A clean link-up while it waits makes it stale; the link-up's own
        // SUBSCRIBE goes out.
        inbox.disconnected();
        inbox.connected(false);
        run_for(&mut exec, 50).await;
        wire_tx.send_replace(Wire::Up);
        run_for(&mut exec, 50).await;
        el.clean();
        let subs: Vec<&Request> = el
            .pending
            .iter()
            .filter(|r| matches!(r, Request::Subscribe(_)))
            .collect();
        assert_eq!(subs.len(), 1, "{:?}", el.pending);
    }

    #[tokio::test]
    async fn a_closed_client_returns_the_command() {
        let mqtt = MqttOptions::new("t", "localhost", 1883);
        let (client, el) = AsyncClient::new(mqtt, 16);
        drop(el);
        let cmd = Command::Unsubscribe {
            filters: vec!["a".into(), "b".into()],
        };
        assert_eq!(try_send(&client, cmd.clone()), Err(Unsent::Full(cmd)));
        let cmd = Command::Subscribe {
            filters: vec![("a".into(), ebus_mqtt::QoS::AtMostOnce)],
        };
        assert_eq!(try_send(&client, cmd.clone()), Err(Unsent::Full(cmd)));
        let cmd = Command::Subscribe {
            filters: vec![("a/#/b".into(), ebus_mqtt::QoS::AtMostOnce)],
        };
        assert_eq!(try_send(&client, cmd.clone()), Err(Unsent::Invalid(cmd)));
    }
}
