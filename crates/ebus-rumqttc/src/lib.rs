//! Bundled rumqttc 0.25 connection for the eBus Homie 5 controller and
//! device on tokio.
//!
//! [`controller`] builds an [`ebus_homie_tokio`] controller with its own
//! rumqttc client, [`device`] a device tree, and [`pair`] both on one
//! connection. The host spawns the task's `run`; the SDK spawns nothing. The
//! broker coordinates, client id, credentials and TLS come from the caller's
//! [`MqttOptions`]. A host that found its broker over mDNS gives the task a
//! [`Reresolve`] hook ([`ControllerTask::reresolve`]), which finds the same
//! broker again when connecting fails, at startup or after a lost link, so a
//! broker whose address changed is reached at its new one.
//!
//! ```no_run
//! use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event};
//! use ebus_rumqttc::ebus_homie_tokio::{ControllerOptions, Delivered};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mqtt = rumqttc::MqttOptions::new(ebus_rumqttc::controller_client_id(), "localhost", 1883);
//! let run = ebus_rumqttc::controller(mqtt, Config::wildcard(), ControllerOptions::default())?;
//! tokio::spawn(run.task.run());
//!
//! let mut events = run.events;
//! while let Some(d) = events.recv().await {
//!     match d {
//!         Delivered::Event(Event::DeviceSynced { device, missing: 0, .. }) => {
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

#![cfg_attr(docsrs, feature(doc_cfg))]

mod conn;
mod device;

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

pub use ebus_homie_tokio;
pub use rumqttc;

pub use conn::Reresolve;
pub use device::{DeviceTask, PairTask, RunningDevice, RunningPair, device, pair};

use ebus_homie_tokio::ebus_homie::controller::{Config, ConfigError};
use ebus_homie_tokio::{
    Commands, ControllerDriver, ControllerHandle, ControllerOptions, DriverError, EventReceiver,
    Inbox, attach_controller,
};
use rumqttc::{AsyncClient, EventLoop, MqttOptions};

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
            Error::Driver(_) => f.write_str("driver stopped"),
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
#[non_exhaustive]
pub struct RunningController {
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
    reresolve: Option<Reresolve>,
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
    opts: ControllerOptions,
) -> Result<RunningController, ConfigError> {
    config.link_up_discards_queued = true;
    let a = attach_controller(config, opts)?;
    let (client, eventloop) = AsyncClient::new(mqtt, conn::REQUEST_CAPACITY);
    Ok(RunningController {
        handle: a.handle,
        events: a.events,
        task: ControllerTask {
            driver: a.driver,
            inbox: a.inbox,
            commands: a.commands,
            client,
            eventloop,
            reresolve: None,
        },
    })
}

/// `ebus-controller-` and 16 random hex digits, after python-sdk's
/// `homie-controller-{uuid}` (homie.py:3870).
pub fn controller_client_id() -> String {
    let mut h = RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.write_u32(std::process::id());
    format!("ebus-controller-{:016x}", h.finish())
}

impl ControllerTask {
    /// Looks the broker up again while its address may have changed: after
    /// the first failed connect since the task started or since the last
    /// successful connect, and after every third failed connect after that,
    /// the connection calls `f`, and options it returns replace the event
    /// loop's before the next attempt. They keep the inflight cap and
    /// manual-ack setting of the options the task was built with. `f`
    /// returning `None` keeps the current options. Calls start with the
    /// first connect, so a broker that got a new address while the host was
    /// off is found at it. A host that found its broker over mDNS returns
    /// the same broker at its new address; one connected by a host name, or
    /// to a configured broker, needs no hook.
    pub fn reresolve(mut self, f: Reresolve) -> Self {
        self.reresolve = Some(f);
        self
    }

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
            reresolve,
        } = self;
        let conn = conn::Conn {
            inbox,
            commands,
            client,
            eventloop,
            receipts: false,
            flush_timeout: Duration::ZERO,
            redirect: reresolve.map(|f| conn::Redirect::new(f, None)),
        };
        conn::run(driver.run(), conn).await.map_err(Error::Driver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use conn::{BACKOFF_MIN, Executor, REQUEST_CAPACITY, Unsent, Wire, next_backoff};
    use ebus_homie_tokio::ebus_mqtt::{self, Command};
    use rumqttc::{Request, SubscribeFilter};
    use tokio::sync::{Notify, watch};

    fn try_send(client: &AsyncClient, cmd: Command) -> Result<(), Unsent> {
        conn::try_send(client, cmd, None)
    }

    fn send_check<T: Send>(_: &T) {}

    #[test]
    fn task_future_is_send() {
        let mqtt = MqttOptions::new(controller_client_id(), "localhost", 1883);
        let run = controller(mqtt, Config::wildcard(), ControllerOptions::default())
            .expect("valid config");
        send_check(
            &run.task
                .reresolve(Box::new(|| Box::pin(async { None })))
                .run(),
        );
    }

    #[test]
    fn driver_error_is_reported_once() {
        use std::error::Error as _;
        let inner = DriverError::CommandsClosed;
        let e = Error::from(inner);
        assert!(!e.to_string().contains(&inner.to_string()), "{e}");
        assert_eq!(e.source().map(ToString::to_string), Some(inner.to_string()));
    }

    #[test]
    fn client_ids_are_distinct_and_prefixed() {
        let a = controller_client_id();
        let b = controller_client_id();
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
            Command::subscribe(vec![
                ("ebus/5/+/$state".into(), ebus_mqtt::QoS::ExactlyOnce),
                ("ebus/5/d/+/+".into(), ebus_mqtt::QoS::AtLeastOnce),
            ]),
            Command::unsubscribe(vec!["a/b".into(), "c/d".into()]),
            Command::publish(
                "ebus/5/d/n/p/set",
                b"true".to_vec(),
                ebus_mqtt::QoS::AtMostOnce,
                false,
            ),
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
        let mqtt = MqttOptions::new(controller_client_id(), "127.0.0.1", port);
        let run =
            controller(mqtt, Config::wildcard(), ControllerOptions::default()).expect("valid");
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
        let a = attach_controller(cfg, ControllerOptions::default()).expect("valid");
        let inbox = a.inbox.clone();
        tokio::spawn(a.driver.run());
        let (client, mut el) = AsyncClient::new(MqttOptions::new("t", "localhost", 1883), 16);
        let (end_tx, _end_rx) = watch::channel(conn::End::Running);
        let (wire_tx, wire_rx) = watch::channel(Wire::Down);
        let progress = Notify::new();
        let exec = Executor {
            client,
            inbox: a.inbox,
            track: None,
            progress: &progress,
            flush_timeout: Duration::ZERO,
        }
        .run(a.commands, end_tx, wire_rx);
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
        let cmd = Command::unsubscribe(vec!["a".into(), "b".into()]);
        assert_eq!(try_send(&client, cmd.clone()), Err(Unsent::Full(cmd)));
        let cmd = Command::subscribe(vec![("a".into(), ebus_mqtt::QoS::AtMostOnce)]);
        assert_eq!(try_send(&client, cmd.clone()), Err(Unsent::Full(cmd)));
        let cmd = Command::subscribe(vec![("a/#/b".into(), ebus_mqtt::QoS::AtMostOnce)]);
        assert_eq!(try_send(&client, cmd.clone()), Err(Unsent::Invalid(cmd)));
    }
}

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
