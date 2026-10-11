//! The device role, alone or with a controller, on a bundled connection.

use std::fmt;
use std::time::Duration;

use ebus_homie_tokio::device::{
    AttachError, BuildError, DeviceDriver, DeviceHandle, DeviceOptions, PairDriver, TreeBuilder,
    attach_device, attach_pair,
};
use ebus_homie_tokio::ebus_homie::controller::Config;
use ebus_homie_tokio::ebus_mqtt::LastWill;
use ebus_homie_tokio::{ControllerHandle, EventReceiver};
use rumqttc::{AsyncClient, MqttOptions};

use crate::Error;
use crate::conn::{self, Conn, REQUEST_CAPACITY};

/// mosquitto's default receive quota: it discards QoS 2 messages past 20
/// in flight from an MQTT 3.1.1 client (client.py:549-552).
const MAX_INFLIGHT: u16 = 20;

/// Room for a publish's fixed header, topic and packet id beyond its payload.
const PACKET_OVERHEAD: usize = 1024;

/// A device tree with its own connection.
#[derive(Debug)]
pub struct RunningDevice {
    /// Presence, alerts, reconfiguration and shutdown.
    pub handle: DeviceHandle,
    /// The future the host spawns.
    pub task: DeviceTask,
}

/// A device tree and a controller sharing one connection.
#[derive(Debug)]
pub struct RunningPair {
    /// The device's handle.
    pub device: DeviceHandle,
    /// The controller's handle.
    pub controller: ControllerHandle,
    /// The controller's events.
    pub events: EventReceiver,
    /// The future the host spawns.
    pub task: PairTask,
}

/// The device driver, the rumqttc event loop and the command executor, as
/// one future.
pub struct DeviceTask {
    driver: DeviceDriver,
    conn: Conn,
}

impl fmt::Debug for DeviceTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceTask")
            .field("driver", &self.driver)
            .field("broker", &self.conn.eventloop.mqtt_options.broker_address())
            .finish_non_exhaustive()
    }
}

/// The pair driver, the rumqttc event loop and the command executor, as one
/// future.
pub struct PairTask {
    driver: PairDriver,
    conn: Conn,
}

impl fmt::Debug for PairTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PairTask")
            .field("driver", &self.driver)
            .field("broker", &self.conn.eventloop.mqtt_options.broker_address())
            .finish_non_exhaustive()
    }
}

impl DeviceTask {
    /// Runs until the device stops; see [`DeviceDriver::run`]. After
    /// `Shutdown::Announce` or `Shutdown::Remove`, the connection waits until
    /// every publish handed to rumqttc was written and acknowledged, at most
    /// [`DeviceOptions::flush_timeout_ms`], then sends DISCONNECT. After
    /// `Shutdown::Silent` or [`DeviceHandle::abort`] it drops the connection
    /// without DISCONNECT, so the broker publishes the will. The future is
    /// `Send`.
    pub async fn run(self) -> Result<(), Error> {
        conn::run(self.driver.run(), self.conn)
            .await
            .map_err(Error::Driver)
    }
}

impl PairTask {
    /// Runs until the device stops, as [`DeviceTask::run`] does. The future
    /// is `Send`.
    pub async fn run(self) -> Result<(), Error> {
        conn::run(self.driver.run(), self.conn)
            .await
            .map_err(Error::Driver)
    }
}

/// Sets what a device's connection needs on the caller's options: `will`, a
/// clean session, at most 20 messages in flight, and an outgoing packet
/// limit that fits the largest `$description`.
pub(crate) fn configure(mqtt: &mut MqttOptions, will: &LastWill, max_description: usize) {
    mqtt.set_last_will(rumqttc::LastWill::new(
        will.topic.clone(),
        will.payload.clone(),
        conn::qos(will.qos),
        will.retain,
    ));
    mqtt.set_clean_session(true);
    let inflight = mqtt.inflight().min(MAX_INFLIGHT);
    mqtt.set_inflight(inflight);
    let incoming = mqtt.max_packet_size();
    mqtt.set_max_packet_size(incoming, max_description.saturating_add(PACKET_OVERHEAD));
}

fn options(tree: &mut TreeBuilder, mut opts: DeviceOptions) -> DeviceOptions {
    tree.set_receipts(true);
    opts.link_up_discards_queued = true;
    opts
}

/// A device tree that connects with `mqtt`. The broker coordinates, the
/// client id (by convention the root's id, homie.py:3379), credentials and
/// TLS are the caller's; the root's will, a clean session, the inflight cap
/// and the outgoing packet limit are set here, along with
/// [`TreeConfig::receipts`](ebus_homie_tokio::device::TreeConfig::receipts)
/// and [`DeviceOptions::link_up_discards_queued`]. Nothing runs until the
/// host spawns [`DeviceTask::run`].
pub fn device(
    mut mqtt: MqttOptions,
    mut tree: TreeBuilder,
    opts: DeviceOptions,
) -> Result<RunningDevice, BuildError> {
    let opts = options(&mut tree, opts);
    let max = tree.config().max_description_bytes;
    let a = attach_device(tree, opts)?;
    configure(&mut mqtt, &a.last_will, max);
    let (client, eventloop) = AsyncClient::new(mqtt, REQUEST_CAPACITY);
    Ok(RunningDevice {
        handle: a.handle,
        task: DeviceTask {
            driver: a.driver,
            conn: Conn {
                inbox: a.inbox,
                commands: a.commands,
                client,
                eventloop,
                receipts: true,
                flush_timeout: Duration::from_millis(opts.flush_timeout_ms),
            },
        },
    })
}

/// A device tree and a controller for `config` on one connection, configured
/// as [`device`] configures it. Nothing runs until the host spawns
/// [`PairTask::run`].
pub fn pair(
    mut mqtt: MqttOptions,
    mut tree: TreeBuilder,
    config: Config,
    opts: DeviceOptions,
) -> Result<RunningPair, AttachError> {
    let opts = options(&mut tree, opts);
    let max = tree.config().max_description_bytes;
    let a = attach_pair(tree, config, opts)?;
    configure(&mut mqtt, &a.last_will, max);
    let (client, eventloop) = AsyncClient::new(mqtt, REQUEST_CAPACITY);
    Ok(RunningPair {
        device: a.device,
        controller: a.controller,
        events: a.events,
        task: PairTask {
            driver: a.driver,
            conn: Conn {
                inbox: a.inbox,
                commands: a.commands,
                client,
                eventloop,
                receipts: true,
                flush_timeout: Duration::from_millis(opts.flush_timeout_ms),
            },
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use ebus_homie_tokio::device::{DeviceSpec, TreeConfig};

    fn send_check<T: Send>(_: &T) {}

    fn tree() -> TreeBuilder {
        TreeBuilder::new(TreeConfig::default(), "r", DeviceSpec::default()).expect("tree")
    }

    #[test]
    fn device_connections_carry_the_will_and_the_limits() {
        let mut mqtt = MqttOptions::new("r", "localhost", 1883);
        mqtt.set_clean_session(false);
        let will = LastWill {
            topic: "ebus/5/r/$state".into(),
            payload: b"lost".to_vec(),
            qos: ebus_homie_tokio::ebus_mqtt::QoS::ExactlyOnce,
            retain: true,
        };
        configure(&mut mqtt, &will, 10_000);
        let w = mqtt.last_will().expect("will");
        assert_eq!(w.topic, "ebus/5/r/$state");
        assert_eq!(&w.message[..], b"lost");
        assert_eq!(w.qos, rumqttc::QoS::ExactlyOnce);
        assert!(w.retain);
        assert!(mqtt.clean_session());
        assert_eq!(mqtt.inflight(), 20);
        mqtt.set_inflight(5);
        configure(&mut mqtt, &will, 10_000);
        assert_eq!(mqtt.inflight(), 5, "a lower cap is kept");
    }

    #[test]
    fn tasks_are_send() {
        let mqtt = MqttOptions::new("r", "localhost", 1883);
        let run = device(mqtt.clone(), tree(), DeviceOptions::default()).expect("device");
        send_check(&run.task.run());
        let run = pair(mqtt, tree(), Config::wildcard(), DeviceOptions::default()).expect("pair");
        send_check(&run.task.run());
    }

    #[tokio::test]
    async fn abort_while_the_broker_is_down_ends_the_task() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port")
            .port();
        let mqtt = MqttOptions::new("r", "127.0.0.1", port);
        let run = device(mqtt, tree(), DeviceOptions::default()).expect("device");
        let task = tokio::spawn(run.task.run());
        tokio::time::sleep(Duration::from_millis(100)).await;
        run.handle.abort();
        let ended = tokio::time::timeout(Duration::from_secs(10), task).await;
        assert!(matches!(ended, Ok(Ok(Ok(())))), "{ended:?}");
    }

    #[tokio::test]
    async fn shutdown_while_the_broker_is_down_ends_the_task() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port")
            .port();
        let mqtt = MqttOptions::new("r", "127.0.0.1", port);
        let run = device(mqtt, tree(), DeviceOptions::default()).expect("device");
        let task = tokio::spawn(run.task.run());
        tokio::time::sleep(Duration::from_millis(100)).await;
        run.handle
            .shutdown(ebus_homie_tokio::device::Shutdown::Announce)
            .await;
        let ended = tokio::time::timeout(Duration::from_secs(10), task).await;
        assert!(matches!(ended, Ok(Ok(Ok(())))), "{ended:?}");
    }
}
