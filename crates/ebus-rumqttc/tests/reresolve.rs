//! The `reresolve` hook against local mosquitto brokers: a device and a
//! controller whose broker stops, or whose broker is down when they start,
//! find another through their hooks and connect there. Skipped when no
//! `mosquitto` binary is found (`MOSQUITTO` names one).

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ebus_rumqttc::Reresolve;
use ebus_rumqttc::ebus_homie_tokio::ControllerOptions;
use ebus_rumqttc::ebus_homie_tokio::device::{
    DeviceOptions, DeviceSpec, NodeSpec, PropSpec, TreeBuilder, TreeConfig,
};
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event};
use ebus_rumqttc::rumqttc::MqttOptions;
use support::{Broker, Ctl, observe, until_message};
use tokio::time::timeout;

const ROOT: &str = "rs-moved";
const ROOT_STATE: &str = "ebus/5/rs-moved/$state";

/// A hook that counts its calls and returns options for `client_id` at the
/// broker on `port`.
fn hook(client_id: String, port: u16, calls: &Arc<AtomicUsize>) -> Reresolve {
    let calls = Arc::clone(calls);
    Box::new(move || {
        calls.fetch_add(1, Ordering::AcqRel);
        let mut mqtt = MqttOptions::new(client_id.clone(), "127.0.0.1", port);
        mqtt.set_max_packet_size(1 << 20, 1 << 20);
        Box::pin(async move { Some(mqtt) })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_and_controller_follow_the_reresolved_broker() {
    let test = "device_and_controller_follow_the_reresolved_broker";
    let Some(mut a) = Broker::start(test, "") else {
        return;
    };
    let Some(b) = Broker::start(test, "") else {
        return;
    };

    let mut tree =
        TreeBuilder::new(TreeConfig::default(), ROOT, DeviceSpec::default()).expect("tree");
    let node = tree
        .node(
            tree.root(),
            "n",
            NodeSpec::typed("energy.ebus.capability.test"),
        )
        .expect("node");
    let level = tree
        .property::<i64>(node, "level", PropSpec::new())
        .expect("level");
    level.set(1).expect("level");
    let dev_calls = Arc::new(AtomicUsize::new(0));
    let dev = ebus_rumqttc::device(
        MqttOptions::new(ROOT, "127.0.0.1", a.port),
        tree,
        DeviceOptions::default(),
    )
    .expect("device");
    // The hook's options carry no will; the device's is applied to them.
    let dev_task = tokio::spawn(
        dev.task
            .reresolve(hook(ROOT.to_owned(), b.port, &dev_calls))
            .run(),
    );

    let ctl_calls = Arc::new(AtomicUsize::new(0));
    let id = ebus_rumqttc::controller_client_id();
    let mut mqtt = MqttOptions::new(id.clone(), "127.0.0.1", a.port);
    mqtt.set_max_packet_size(1 << 20, 1 << 20);
    let run = ebus_rumqttc::controller(
        mqtt,
        Config::tree(ROOT).expect("valid root"),
        ControllerOptions::default(),
    )
    .expect("controller");
    let ctl_task = tokio::spawn(run.task.reresolve(hook(id, b.port, &ctl_calls)).run());
    let mut ctl = Ctl::attached(run.handle, run.events, ctl_task);
    ctl.synced(15, &[ROOT]).await;
    assert_eq!(dev_calls.load(Ordering::Acquire), 0, "called while up");
    assert_eq!(ctl_calls.load(Ordering::Acquire), 0, "called while up");

    let mut on_b = observe(
        b.port,
        "observer-b",
        &[ROOT_STATE, "ebus/5/rs-moved/n/level"],
    )
    .await;
    a.kill();
    until_message(&mut on_b, 20, ROOT_STATE, b"ready").await;
    ctl.synced(20, &[ROOT]).await;
    assert_eq!(ctl.value(ROOT, "n", "level").as_deref(), Some("1"));

    level.set(2).expect("level");
    ctl.until(10, "level 2 on the new broker", |e| {
        matches!(e, Event::PropertyChanged { addr, value, .. }
            if addr.property.as_str() == "level" && &*value.raw == "2")
    })
    .await;
    assert_eq!(dev_calls.load(Ordering::Acquire), 1);
    assert_eq!(ctl_calls.load(Ordering::Acquire), 1);

    // The will was applied to the new options: dropping the connection
    // makes broker B publish it.
    dev.handle.abort();
    until_message(&mut on_b, 20, ROOT_STATE, b"lost").await;
    let res = timeout(Duration::from_secs(10), dev_task)
        .await
        .expect("device task ends")
        .expect("device task did not panic");
    assert!(res.is_ok(), "{res:?}");
    ctl.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_and_controller_reresolve_a_broker_down_at_startup() {
    let test = "device_and_controller_reresolve_a_broker_down_at_startup";
    let Some(b) = Broker::start(test, "") else {
        return;
    };
    // A port nothing listens on.
    let down = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("bind")
        .port();

    let tree = TreeBuilder::new(TreeConfig::default(), ROOT, DeviceSpec::default()).expect("tree");
    let dev_calls = Arc::new(AtomicUsize::new(0));
    let dev = ebus_rumqttc::device(
        MqttOptions::new(ROOT, "127.0.0.1", down),
        tree,
        DeviceOptions::default(),
    )
    .expect("device");
    let dev_task = tokio::spawn(
        dev.task
            .reresolve(hook(ROOT.to_owned(), b.port, &dev_calls))
            .run(),
    );

    let ctl_calls = Arc::new(AtomicUsize::new(0));
    let id = ebus_rumqttc::controller_client_id();
    let run = ebus_rumqttc::controller(
        MqttOptions::new(id.clone(), "127.0.0.1", down),
        Config::tree(ROOT).expect("valid root"),
        ControllerOptions::default(),
    )
    .expect("controller");
    let ctl_task = tokio::spawn(run.task.reresolve(hook(id, b.port, &ctl_calls)).run());
    let mut ctl = Ctl::attached(run.handle, run.events, ctl_task);
    ctl.synced(15, &[ROOT]).await;
    assert_eq!(dev_calls.load(Ordering::Acquire), 1);
    assert_eq!(ctl_calls.load(Ordering::Acquire), 1);

    dev.handle.abort();
    let res = timeout(Duration::from_secs(10), dev_task)
        .await
        .expect("device task ends")
        .expect("device task did not panic");
    assert!(res.is_ok(), "{res:?}");
    ctl.shutdown().await;
}
