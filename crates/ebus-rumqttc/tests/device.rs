//! The bundled device, alone and paired with a controller, against a local
//! mosquitto, one broker per test. Every test is skipped when no `mosquitto`
//! binary is found (`MOSQUITTO` names one), and the python-sdk cases also
//! when `ebus_sdk` cannot be imported (`EBUS_PYTHON` names the interpreter).

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use ebus_rumqttc::ebus_homie_tokio::device::{
    DevIdx, DeviceHandle, DeviceOptions, DeviceSpec, HomieEnum, NodeSpec, PropSpec, Property,
    Settable, Shutdown, TreeBuilder, TreeConfig,
};
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event, PropertyAddr};
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::state::DeviceState;
use ebus_rumqttc::rumqttc::MqttOptions;
use serde_json::Value;
use support::{
    Broker, Ctl, Proxy, PyController, Seeder, Session, controller, observe, retained, until_message,
};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

const ROOT: &str = "rs-root";
const CHILD: &str = "rs-child";
const ROOT_STATE: &str = "ebus/5/rs-root/$state";
const CHILD_STATE: &str = "ebus/5/rs-child/$state";
const METER: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Auto,
    Off,
}

impl HomieEnum for Mode {
    const VARIANTS: &'static [&'static str] = &["auto", "off"];
    fn index(self) -> u16 {
        self as u16
    }
    fn from_index(i: u16) -> Option<Self> {
        match i {
            0 => Some(Mode::Auto),
            1 => Some(Mode::Off),
            _ => None,
        }
    }
}

/// The test tree's handles: the root's node `ctl` and the child's node
/// `meter`.
struct Handles {
    level: Settable<i64>,
    on: Settable<bool>,
    text: Settable<String>,
    temp: Property<f64>,
    mode: Property<Mode>,
    meter: Vec<Property<i64>>,
}

impl Handles {
    /// Sets every value to its initial value again.
    fn set_initial(&self) {
        self.level.set(50).expect("level");
        self.on.set(false).expect("on");
        self.text.set("initial".to_owned()).expect("text");
        self.temp.set(21.5).expect("temp");
        self.mode.set(Mode::Auto).expect("mode");
        for (i, p) in self.meter.iter().enumerate() {
            p.set(100 + i as i64).expect("meter");
        }
    }
}

/// `rs-root` with a settable `level` (`0:100:5`), `on` and `text`, a `temp`
/// and a `mode`, and the child `rs-child` with `METER` integers, every value
/// set.
fn tree() -> (TreeBuilder, Handles, DevIdx) {
    tree_with(TreeConfig::default())
}

/// [`tree`] configured with `cfg`.
fn tree_with(cfg: TreeConfig) -> (TreeBuilder, Handles, DevIdx) {
    let mut b = TreeBuilder::new(
        cfg,
        ROOT,
        DeviceSpec::typed("energy.ebus.device.test").name("Rust root"),
    )
    .expect("tree");
    let ctl = b
        .node(
            b.root(),
            "ctl",
            NodeSpec::typed("energy.ebus.capability.test"),
        )
        .expect("node");
    let level = b
        .settable::<i64>(ctl, "level", PropSpec::new().format("0:100:5"))
        .expect("level");
    let on = b.settable::<bool>(ctl, "on", PropSpec::new()).expect("on");
    let text = b
        .settable::<String>(ctl, "text", PropSpec::new())
        .expect("text");
    let temp = b
        .property::<f64>(ctl, "temp", PropSpec::new().unit("°C"))
        .expect("temp");
    let mode = b
        .property::<Mode>(ctl, "mode", PropSpec::new())
        .expect("mode");
    let child = b
        .child(
            b.root(),
            CHILD,
            DeviceSpec::typed("energy.ebus.device.test"),
        )
        .expect("child");
    let meter_node = b
        .node(
            child,
            "meter",
            NodeSpec::typed("energy.ebus.capability.meter"),
        )
        .expect("node");
    let meter = (0..METER)
        .map(|i| {
            b.property::<i64>(meter_node, &format!("p{i}"), PropSpec::new())
                .expect("meter")
        })
        .collect();
    let h = Handles {
        level,
        on,
        text,
        temp,
        mode,
        meter,
    };
    h.set_initial();
    (b, h, child)
}

/// The value topics the tree retains once ready, with their payloads.
fn values() -> BTreeMap<String, Vec<u8>> {
    let mut v: BTreeMap<String, Vec<u8>> = [
        ("ctl/level", "50"),
        ("ctl/on", "false"),
        ("ctl/text", "initial"),
        ("ctl/temp", "21.5"),
        ("ctl/mode", "auto"),
    ]
    .into_iter()
    .map(|(t, p)| (format!("ebus/5/{ROOT}/{t}"), p.as_bytes().to_vec()))
    .collect();
    for i in 0..METER {
        v.insert(
            format!("ebus/5/{CHILD}/meter/p{i}"),
            (100 + i).to_string().into_bytes(),
        );
    }
    v
}

/// Checks that `snap` is exactly the ready tree: both descriptions, both
/// states `ready`, and every value.
fn assert_ready_tree(snap: &BTreeMap<String, Vec<u8>>) {
    let mut want = values();
    want.insert(ROOT_STATE.into(), b"ready".to_vec());
    want.insert(CHILD_STATE.into(), b"ready".to_vec());
    for d in [ROOT, CHILD] {
        let topic = format!("ebus/5/{d}/$description");
        let desc = snap.get(&topic).unwrap_or_else(|| panic!("no {topic}"));
        want.insert(topic, desc.clone());
    }
    assert_eq!(snap, &want);
}

fn description(snap: &BTreeMap<String, Vec<u8>>, d: &str) -> Value {
    serde_json::from_slice(&snap[&format!("ebus/5/{d}/$description")]).expect("JSON description")
}

/// A spawned bundled device.
struct Dev {
    handle: DeviceHandle,
    task: JoinHandle<Result<(), ebus_rumqttc::Error>>,
    h: Handles,
}

impl Dev {
    /// Shuts down in `mode` and waits for the task.
    async fn shutdown(self, mode: Shutdown) {
        self.handle.shutdown(mode).await;
        let res = timeout(Duration::from_secs(10), self.task)
            .await
            .expect("task ends after shutdown")
            .expect("task did not panic");
        assert!(res.is_ok(), "{res:?}");
    }
}

/// The device on the broker (or proxy) at `port`, connected as its root id.
fn start(port: u16) -> Dev {
    start_with(port, TreeConfig::default())
}

/// [`start`] with the tree configured with `cfg`.
fn start_with(port: u16, cfg: TreeConfig) -> Dev {
    let (b, h, _) = tree_with(cfg);
    let run = ebus_rumqttc::device(
        MqttOptions::new(ROOT, "127.0.0.1", port),
        b,
        DeviceOptions::default(),
    )
    .expect("device");
    Dev {
        handle: run.handle,
        task: tokio::spawn(run.task.run()),
        h,
    }
}

/// Starts the device on `port` and waits until the root's `ready` reaches
/// an observer on the broker at `broker`.
async fn start_ready(broker: u16, port: u16) -> Dev {
    let mut obs = observe(broker, "ready-observer", &[ROOT_STATE]).await;
    let dev = start(port);
    until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    dev
}

async fn next_set<T>(s: &mut Settable<T>) -> ebus_rumqttc::ebus_homie_tokio::device::SetRequest<T>
where
    T: ebus_rumqttc::ebus_homie_tokio::device::HomieValue + Send + Sync,
{
    timeout(Duration::from_secs(5), s.next_set())
        .await
        .expect("a /set in time")
        .expect("driver running")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_controller_syncs_a_rust_device_tree() {
    let Some(broker) = Broker::start("rust_controller_syncs_a_rust_device_tree", "") else {
        return;
    };
    let mut dev = start_ready(broker.port, broker.port).await;
    let mut ctl = controller(broker.port, Config::tree(ROOT).expect("valid root"));
    ctl.synced(15, &[ROOT, CHILD]).await;
    for (topic, payload) in values() {
        let mut parts = topic.split('/').skip(2);
        let (d, n, p) = (parts.next(), parts.next(), parts.next());
        let (d, n, p) = (d.expect("device"), n.expect("node"), p.expect("property"));
        assert_eq!(
            ctl.value(d, n, p).map(String::into_bytes),
            Some(payload),
            "{topic}"
        );
    }
    for d in [ROOT, CHILD] {
        assert_eq!(
            ctl.handle.effective_state(d),
            Some(DeviceState::Ready),
            "{d}"
        );
    }

    dev.h.temp.set(22.25).expect("temp");
    ctl.until(5, "temp 22.25", |e| {
        matches!(e, Event::PropertyChanged { addr, value, .. }
            if addr.property.as_str() == "temp" && &*value.raw == "22.25")
    })
    .await;

    let addr = PropertyAddr::new(ROOT, "ctl", "level").expect("valid address");
    ctl.handle.set(addr, "35").await.expect("set accepted");
    let req = next_set(&mut dev.h.level).await;
    assert_eq!(*req.value(), 35);
    req.accept();
    ctl.until(5, "level 35", |e| {
        matches!(e, Event::PropertyChanged { addr, value, .. }
            if addr.property.as_str() == "level" && &*value.raw == "35")
    })
    .await;
    ctl.shutdown().await;
    dev.shutdown(Shutdown::Announce).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn python_controller_reads_a_rust_device_tree() {
    let test = "python_controller_reads_a_rust_device_tree";
    let Some(broker) = Broker::start(test, "") else {
        return;
    };
    let dev = start_ready(broker.port, broker.port).await;
    let Some(mut py) = PyController::start(test, broker.port, ROOT) else {
        return;
    };
    let snap = retained(broker.port, "ebus/5/#").await;
    assert_ready_tree(&snap);
    let want = values();
    for (d, n) in [(ROOT, 5), (CHILD, METER)] {
        let seen = py.await_device(d, n);
        assert_eq!(seen["state"], "ready", "{d}");
        assert_eq!(seen["description"], description(&snap, d), "{d}");
        let mut got = BTreeMap::new();
        for (node, props) in seen["properties"].as_object().expect("properties") {
            for (prop, v) in props.as_object().expect("node properties") {
                let text = v.as_str().expect("payload text");
                got.insert(
                    format!("ebus/5/{d}/{node}/{prop}"),
                    text.as_bytes().to_vec(),
                );
            }
        }
        let mine: BTreeMap<String, Vec<u8>> = want
            .iter()
            .filter(|(t, _)| t.starts_with(&format!("ebus/5/{d}/")))
            .map(|(t, p)| (t.clone(), p.clone()))
            .collect();
        assert_eq!(got, mine, "{d}");
    }
    drop(py);
    dev.shutdown(Shutdown::Announce).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn python_controller_sets_a_rust_device() {
    let test = "python_controller_sets_a_rust_device";
    let Some(broker) = Broker::start(test, "") else {
        return;
    };
    let mut dev = start_ready(broker.port, broker.port).await;
    let Some(mut py) = PyController::start(test, broker.port, ROOT) else {
        return;
    };
    py.await_device(ROOT, 5);

    py.set(ROOT, "ctl", "on", "true");
    let req = next_set(&mut dev.h.on).await;
    assert!(*req.value());
    assert_eq!(req.raw(), b"true");
    req.accept();
    py.await_value(ROOT, "ctl", "on", "true");

    // Step rounding from the minimum (convention.md:399-413).
    py.set(ROOT, "ctl", "level", "12");
    let req = next_set(&mut dev.h.level).await;
    assert_eq!(*req.value(), 10);
    assert_eq!(req.raw(), b"12");
    req.accept();
    py.await_value(ROOT, "ctl", "level", "10");

    // Two refused payloads and a property that is not settable, then a
    // valid /set as the marker that the device has read them.
    py.set(ROOT, "ctl", "level", "abc");
    py.set(ROOT, "ctl", "level", "150");
    py.set(ROOT, "ctl", "temp", "1.0");
    py.set(ROOT, "ctl", "text", "marker");
    let req = next_set(&mut dev.h.text).await;
    assert_eq!(req.value(), "marker");
    let deadline = Instant::now() + Duration::from_secs(5);
    while dev.handle.rejected_sets() < 3 {
        assert!(
            Instant::now() < deadline,
            "{} refused",
            dev.handle.rejected_sets()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(dev.handle.rejected_sets(), 3);
    assert!(dev.h.level.try_next_set().is_none());
    req.accept();
    py.await_value(ROOT, "ctl", "text", "marker");
    assert_eq!(py.value(ROOT, "ctl", "level"), "10");
    assert_eq!(py.value(ROOT, "ctl", "temp"), "21.5");
    drop(py);
    dev.shutdown(Shutdown::Announce).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_restart_republishes_the_whole_tree() {
    let Some(mut broker) = Broker::start("broker_restart_republishes_the_whole_tree", "") else {
        return;
    };
    let dev = start_ready(broker.port, broker.port).await;
    let before = retained(broker.port, "ebus/5/#").await;
    assert_ready_tree(&before);

    broker.restart();
    let mut obs = observe(broker.port, "restart-observer", &[ROOT_STATE]).await;
    until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    assert_eq!(retained(broker.port, "ebus/5/#").await, before);
    dev.shutdown(Shutdown::Announce).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_values_publish_nothing() {
    let Some(broker) = Broker::start("unchanged_values_publish_nothing", "") else {
        return;
    };
    let proxy = Proxy::start(broker.port).await;
    let temp = format!("ebus/5/{ROOT}/ctl/temp");
    let mut obs = observe(broker.port, "temp-observer", &[ROOT_STATE, &temp]).await;
    let dev = start(proxy.port);
    until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    let refresh = proxy.publishes();

    dev.h.set_initial();
    dev.h.temp.set(23.0).expect("temp");
    let before = until_message(&mut obs, 5, &temp, b"23.0").await;
    assert!(before.is_empty(), "{before:?}");
    assert_eq!(proxy.publishes(), refresh + 1);
    dev.shutdown(Shutdown::Announce).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_leaves_no_retained_topic_under_the_root() {
    let Some(broker) = Broker::start("remove_leaves_no_retained_topic_under_the_root", "") else {
        return;
    };
    let dev = start_ready(broker.port, broker.port).await;
    assert_ready_tree(&retained(broker.port, "ebus/5/#").await);
    dev.shutdown(Shutdown::Remove).await;
    let left = retained(broker.port, "ebus/5/#").await;
    assert!(left.is_empty(), "{left:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn announce_publishes_disconnected() {
    let Some(broker) = Broker::start("announce_publishes_disconnected", "") else {
        return;
    };
    let mut obs = observe(broker.port, "state-observer", &["ebus/5/+/$state"]).await;
    let dev = start(broker.port);
    until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    dev.shutdown(Shutdown::Announce).await;
    let before = until_message(&mut obs, 5, ROOT_STATE, b"disconnected").await;
    assert_eq!(
        before,
        vec![(CHILD_STATE.to_owned(), b"disconnected".to_vec(), false)]
    );
    let snap = retained(broker.port, "ebus/5/#").await;
    assert_eq!(snap[ROOT_STATE], b"disconnected");
    assert_eq!(snap[CHILD_STATE], b"disconnected");
    for (topic, payload) in values() {
        assert_eq!(snap.get(&topic), Some(&payload), "{topic}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_without_shutdown_fires_the_will() {
    let Some(broker) = Broker::start("dropping_without_shutdown_fires_the_will", "") else {
        return;
    };
    let mut obs = observe(broker.port, "will-observer", &[ROOT_STATE]).await;
    let dev = start(broker.port);
    until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    dev.task.abort();
    drop(dev.handle);
    drop(dev.h);
    until_message(&mut obs, 15, ROOT_STATE, b"lost").await;
    let snap = retained(broker.port, "ebus/5/#").await;
    assert_eq!(snap[ROOT_STATE], b"lost");
    assert_eq!(snap[CHILD_STATE], b"ready");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pair_shares_one_connection_and_outlives_its_controller() {
    let Some(broker) = Broker::start("pair_shares_one_connection_and_outlives_its_controller", "")
    else {
        return;
    };
    let proxy = Proxy::start(broker.port).await;
    let mut seed = Seeder::new(broker.port, "seeder");
    seed.device(
        "seed-1",
        r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"q":{"datatype":"integer"}}}}}"#,
        &[("n/q".into(), "7".into())],
    )
    .await;
    seed.flush().await;

    let (b, mut h, _) = tree();
    let run = ebus_rumqttc::pair(
        MqttOptions::new(ROOT, "127.0.0.1", proxy.port),
        b,
        Config::wildcard(),
        DeviceOptions::default(),
    )
    .expect("pair");
    let device = run.device;
    let task = tokio::spawn(run.task.run());
    let mut ctl = Ctl::attached(run.controller, run.events, task);
    ctl.synced(15, &[ROOT, CHILD, "seed-1"]).await;
    assert_eq!(ctl.value("seed-1", "n", "q").as_deref(), Some("7"));
    assert_eq!(ctl.value(ROOT, "ctl", "level").as_deref(), Some("50"));
    assert_eq!(ctl.value(CHILD, "meter", "p2").as_deref(), Some("102"));
    assert_eq!(proxy.connections(), 1);

    // Once the broker has acknowledged the controller's UNSUBSCRIBE, the
    // device's /set filter still delivers.
    let unsubacks = proxy.unsubacks();
    ctl.handle.shutdown().await;
    proxy.wait_unsubacks(unsubacks + 1, 10).await;
    seed.publish(&format!("ebus/5/{ROOT}/ctl/on/set"), b"true", false)
        .await;
    seed.flush().await;
    let req = next_set(&mut h.on).await;
    assert!(*req.value());
    req.accept();
    assert!(device.is_connected());
    assert_eq!(proxy.connections(), 1);
    device.shutdown(Shutdown::Announce).await;
    let res = timeout(Duration::from_secs(10), ctl.join())
        .await
        .expect("task ends after shutdown");
    assert!(res.is_ok(), "{res:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_killed_socket_shows_lost_then_ready_after_the_reconnect() {
    let Some(broker) = Broker::start(
        "a_killed_socket_shows_lost_then_ready_after_the_reconnect",
        "",
    ) else {
        return;
    };
    let proxy = Proxy::start(broker.port).await;
    let mut obs = observe(broker.port, "reconnect-observer", &[ROOT_STATE]).await;
    let dev = start(proxy.port);
    until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    let before = retained(broker.port, "ebus/5/#").await;
    assert_ready_tree(&before);

    proxy.cut();
    let early = until_message(&mut obs, 15, ROOT_STATE, b"lost").await;
    assert!(early.is_empty(), "{early:?}");
    // An unchanged description is not wrapped in `init` again.
    let between = until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    assert!(between.is_empty(), "{between:?}");
    assert_eq!(proxy.connections(), 2);
    assert_eq!(retained(broker.port, "ebus/5/#").await, before);
    dev.shutdown(Shutdown::Announce).await;
}

const WIDE: &str = "rs-wide";
const WIDE_PROPS: usize = 200;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_description_over_10_kib_reaches_the_controller_intact() {
    let Some(broker) = Broker::start(
        "a_description_over_10_kib_reaches_the_controller_intact",
        "",
    ) else {
        return;
    };
    let mut b = TreeBuilder::new(
        TreeConfig::default(),
        WIDE,
        DeviceSpec::typed("energy.ebus.device.test").name("Wide root"),
    )
    .expect("tree");
    let node = b
        .node(
            b.root(),
            "wide",
            NodeSpec::typed("energy.ebus.capability.test"),
        )
        .expect("node");
    let props: Vec<Property<i64>> = (0..WIDE_PROPS)
        .map(|i| {
            let spec = PropSpec::new()
                .name(&format!("Wide property number {i:03}"))
                .unit("W");
            b.property::<i64>(node, &format!("p{i:03}"), spec)
                .expect("property")
        })
        .collect();
    for (i, p) in props.iter().enumerate() {
        p.set(i as i64).expect("value");
    }
    let state = format!("ebus/5/{WIDE}/$state");
    let mut obs = observe(broker.port, "wide-observer", &[&state]).await;
    let run = ebus_rumqttc::device(
        MqttOptions::new(WIDE, "127.0.0.1", broker.port),
        b,
        DeviceOptions::default(),
    )
    .expect("device");
    let task = tokio::spawn(run.task.run());
    until_message(&mut obs, 15, &state, b"ready").await;

    let snap = retained(broker.port, "ebus/5/#").await;
    let raw = &snap[&format!("ebus/5/{WIDE}/$description")];
    assert!(raw.len() > 10 * 1024, "{} bytes", raw.len());
    let mut ctl = controller(broker.port, Config::tree(WIDE).expect("valid root"));
    ctl.synced(15, &[WIDE]).await;
    let view = ctl.handle.device(WIDE).expect("device known");
    let got = view.description.as_deref().expect("description applied");
    assert_eq!(got.to_json().as_bytes(), &raw[..]);
    for i in 0..WIDE_PROPS {
        let want = i.to_string();
        assert_eq!(
            ctl.value(WIDE, "wide", &format!("p{i:03}")).as_deref(),
            Some(want.as_str()),
            "p{i:03}"
        );
    }
    ctl.shutdown().await;
    run.handle.shutdown(Shutdown::Announce).await;
    let res = timeout(Duration::from_secs(10), task)
        .await
        .expect("task ends after shutdown")
        .expect("task did not panic");
    assert!(res.is_ok(), "{res:?}");
}

/// Sends `/set` `payload` to the root's `level` from `seed`, accepts it, and
/// returns what `obs` saw before the value it republished: every message the
/// device published after it handled the messages the broker sent it first.
async fn level_marker(
    seed: &mut Seeder,
    dev: &mut Dev,
    obs: &mut tokio::sync::mpsc::UnboundedReceiver<(String, Vec<u8>, bool)>,
    payload: &str,
) -> Vec<(String, Vec<u8>, bool)> {
    seed.publish(&format!("{LEVEL}/set"), payload.as_bytes(), false)
        .await;
    seed.flush().await;
    let req = next_set(&mut dev.h.level).await;
    req.accept();
    until_message(obs, 5, LEVEL, payload.as_bytes()).await
}

const LEVEL: &str = "ebus/5/rs-root/ctl/level";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_takeover_replaces_the_stale_lost_and_reasserts_ready() {
    let Some(broker) = Broker::start(
        "a_session_takeover_replaces_the_stale_lost_and_reasserts_ready",
        "",
    ) else {
        return;
    };
    let mut obs = observe(broker.port, "takeover-observer", &[ROOT_STATE, LEVEL]).await;
    let mut seed = Seeder::new(broker.port, "seeder");

    // An earlier session of the root, under its client id, left `ready`.
    let mut old = Session::start(broker.port, ROOT, ROOT_STATE).await;
    old.publish(ROOT_STATE, b"ready").await;
    until_message(&mut obs, 5, ROOT_STATE, b"ready").await;
    // Earlier sessions under other client ids, with the root's will, whose
    // wills land once the device is ready.
    let mut stale = Vec::new();
    for i in 0..4 {
        let proxy = Proxy::start(broker.port).await;
        let s = Session::start(proxy.port, &format!("stale-{i}"), ROOT_STATE).await;
        stale.push((proxy, s));
    }

    let mut cfg = TreeConfig::default();
    cfg.watch_own_state = true;
    let front = Proxy::start(broker.port).await;
    front.hold_retained(ROOT_STATE);
    let mut dev = start_with(front.port, cfg);
    // mosquitto publishes the replaced session's will before the CONNACK,
    // so the device receives it retained at its SUBSCRIBE.
    let early = until_message(&mut obs, 15, ROOT_STATE, b"lost").await;
    assert!(early.is_empty(), "{early:?}");
    old.ended(5).await;
    let refresh = until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    let states: Vec<&[u8]> = refresh
        .iter()
        .filter(|m| m.0 == ROOT_STATE)
        .map(|m| &m.1[..])
        .collect();
    assert_eq!(states, [&b"init"[..]]);
    assert_ready_tree(&retained(broker.port, "ebus/5/#").await);
    // The retained `lost`, held at the proxy, reaches the device after its
    // refresh, so only its retain flag keeps it from drawing a re-assert.
    front.wait_held(1, 5).await;
    front.release().await;
    let after = level_marker(&mut seed, &mut dev, &mut obs, "35").await;
    assert!(after.is_empty(), "{after:?}");

    // A live `lost` once ready is answered with one `ready`, 3 times.
    let mut stale = stale.into_iter();
    for (i, (proxy, s)) in stale.by_ref().take(3).enumerate() {
        proxy.cut();
        s.ended(5).await;
        let early = until_message(&mut obs, 5, ROOT_STATE, b"lost").await;
        assert!(early.is_empty(), "will {i}: {early:?}");
        let between = until_message(&mut obs, 5, ROOT_STATE, b"ready").await;
        assert!(between.is_empty(), "will {i}: {between:?}");
    }
    assert_eq!(
        retained(broker.port, ROOT_STATE).await[ROOT_STATE],
        b"ready"
    );

    // The fourth is left standing.
    let (proxy, s) = stale.next().expect("a fourth session");
    proxy.cut();
    s.ended(5).await;
    until_message(&mut obs, 5, ROOT_STATE, b"lost").await;
    let after = level_marker(&mut seed, &mut dev, &mut obs, "40").await;
    assert!(after.is_empty(), "{after:?}");
    assert_eq!(retained(broker.port, ROOT_STATE).await[ROOT_STATE], b"lost");
    dev.shutdown(Shutdown::Announce).await;
}

const BIG: &str = "rs-big";
const BIG_CHILDREN: usize = 19;
const BIG_PROPS: usize = 300;
/// The bound on one refresh of the big tree, generous for a slow runner.
const BIG_REFRESH_SECS: u64 = 60;

/// The big tree's device ids, root first, in pre-order.
fn big_ids() -> Vec<String> {
    let mut ids = vec![BIG.to_owned()];
    ids.extend((1..=BIG_CHILDREN).map(|c| format!("{BIG}-{c:02}")));
    ids
}

/// The value topics of device `k` of [`big_ids`], with their payloads.
fn big_values(k: usize, id: &str) -> BTreeMap<String, Vec<u8>> {
    (0..BIG_PROPS)
        .map(|i| {
            (
                format!("ebus/5/{id}/n/p{i:03}"),
                (k * 1000 + i).to_string().into_bytes(),
            )
        })
        .collect()
}

/// `rs-big` and its children, each with one node of `BIG_PROPS` integers,
/// every value set.
fn big_tree() -> (TreeBuilder, Vec<Property<i64>>) {
    let mut b = TreeBuilder::new(
        TreeConfig::default(),
        BIG,
        DeviceSpec::typed("energy.ebus.device.test"),
    )
    .expect("tree");
    let mut props = Vec::new();
    for (k, id) in big_ids().iter().enumerate() {
        let dev = if k == 0 {
            b.root()
        } else {
            b.child(b.root(), id, DeviceSpec::typed("energy.ebus.device.test"))
                .expect("child")
        };
        let node = b
            .node(dev, "n", NodeSpec::typed("energy.ebus.capability.test"))
            .expect("node");
        for i in 0..BIG_PROPS {
            let p = b
                .property::<i64>(node, &format!("p{i:03}"), PropSpec::new())
                .expect("property");
            p.set((k * 1000 + i) as i64).expect("value");
            props.push(p);
        }
    }
    (b, props)
}

/// Receives one refresh of the big tree from `obs`, up to the root's
/// `ready`, and checks its order and content: per device in pre-order,
/// `init` when `init`, `$description`, then every value once; then each
/// child's `ready`, the root's last. Returns the descriptions.
async fn big_refresh(
    obs: &mut tokio::sync::mpsc::UnboundedReceiver<(String, Vec<u8>, bool)>,
    init: bool,
) -> BTreeMap<String, Vec<u8>> {
    let root_state = format!("ebus/5/{BIG}/$state");
    let msgs = until_message(obs, BIG_REFRESH_SECS, &root_state, b"ready").await;
    let mut it = msgs.into_iter();
    let mut descriptions = BTreeMap::new();
    for (k, id) in big_ids().iter().enumerate() {
        if init {
            let m = it.next().expect("init");
            assert_eq!(
                (m.0.as_str(), &m.1[..]),
                (&*format!("ebus/5/{id}/$state"), &b"init"[..])
            );
        }
        let m = it.next().expect("$description");
        let topic = format!("ebus/5/{id}/$description");
        assert_eq!(m.0, topic);
        descriptions.insert(topic, m.1);
        let mut want = big_values(k, id);
        for _ in 0..BIG_PROPS {
            let m = it.next().expect("a value");
            assert!(
                want.remove(&m.0).is_some_and(|p| p == m.1),
                "{} is not one of the {} values {id} still owes",
                m.0,
                want.len()
            );
        }
    }
    for id in &big_ids()[1..] {
        let m = it.next().expect("a child's ready");
        assert_eq!(
            (m.0.as_str(), &m.1[..]),
            (&*format!("ebus/5/{id}/$state"), &b"ready"[..])
        );
    }
    let rest: Vec<_> = it.collect();
    assert!(rest.is_empty(), "{rest:?}");
    descriptions
}

/// The big tree as the broker retains it once ready.
fn big_retained(descriptions: &BTreeMap<String, Vec<u8>>) -> BTreeMap<String, Vec<u8>> {
    let mut want = descriptions.clone();
    for (k, id) in big_ids().iter().enumerate() {
        want.extend(big_values(k, id));
        want.insert(format!("ebus/5/{id}/$state"), b"ready".to_vec());
    }
    want
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_qos_2_refresh_of_20_devices_by_300_properties_completes_in_order() {
    // mosquitto drops (2.0.22 and later) or disconnects on (earlier 2.0.x) a
    // 3.1.1 client's QoS 2 publishes past its receive quota of
    // `max_inflight_messages`. The queue limit is lifted so the observers
    // here lose nothing.
    let Some(mut broker) = Broker::start(
        "a_qos_2_refresh_of_20_devices_by_300_properties_completes_in_order",
        "max_inflight_messages 20\nmax_queued_messages 0",
    ) else {
        return;
    };
    let proxy = Proxy::start(broker.port).await;
    let mut obs = observe(broker.port, "big-observer", &["ebus/5/#"]).await;
    let (b, _props) = big_tree();
    let run = ebus_rumqttc::device(
        MqttOptions::new(BIG, "127.0.0.1", proxy.port),
        b,
        DeviceOptions::default(),
    )
    .expect("device");
    let started = Instant::now();
    let task = tokio::spawn(run.task.run());
    let descriptions = big_refresh(&mut obs, true).await;
    eprintln!("first refresh: {:?}", started.elapsed());
    for d in descriptions.values() {
        let v: Value = serde_json::from_slice(d).expect("JSON description");
        let props = v["nodes"]["n"]["properties"]
            .as_object()
            .expect("properties");
        assert_eq!(props.len(), BIG_PROPS);
    }
    let want = big_retained(&descriptions);
    assert_eq!(want.len(), (BIG_CHILDREN + 1) * (BIG_PROPS + 2));
    assert_eq!(retained(broker.port, "ebus/5/#").await, want);

    // The refresh after a reconnect republishes everything, without `init`.
    let root_state = format!("ebus/5/{BIG}/$state");
    proxy.cut();
    let early = until_message(&mut obs, 15, &root_state, b"lost").await;
    assert!(early.is_empty(), "{early:?}");
    let cut = Instant::now();
    assert_eq!(big_refresh(&mut obs, false).await, descriptions);
    eprintln!("refresh after a cut: {:?}", cut.elapsed());
    assert_eq!(retained(broker.port, "ebus/5/#").await, want);

    // A broker that lost every retained message gets all of them back.
    drop(obs);
    let restart = Instant::now();
    broker.restart();
    let mut obs = observe(broker.port, "big-restart-observer", &[&root_state]).await;
    until_message(&mut obs, BIG_REFRESH_SECS, &root_state, b"ready").await;
    eprintln!("refresh after a broker restart: {:?}", restart.elapsed());
    assert_eq!(retained(broker.port, "ebus/5/#").await, want);

    run.handle.shutdown(Shutdown::Announce).await;
    let res = timeout(Duration::from_secs(10), task)
        .await
        .expect("task ends after shutdown")
        .expect("task did not panic");
    assert!(res.is_ok(), "{res:?}");
}

const TGT: &str = "rs-target";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn target_keeps_the_set_bytes_through_python_sdk() {
    let test = "target_keeps_the_set_bytes_through_python_sdk";
    let Some(broker) = Broker::start(test, "") else {
        return;
    };
    let mut b = TreeBuilder::new(
        TreeConfig::default(),
        TGT,
        DeviceSpec::typed("energy.ebus.device.test"),
    )
    .expect("tree");
    let node = b
        .node(
            b.root(),
            "ctl",
            NodeSpec::typed("energy.ebus.capability.test"),
        )
        .expect("node");
    let mut point = b
        .settable::<f64>(node, "point", PropSpec::new().target(true))
        .expect("point");
    point.set(1.0).expect("point");
    let value = format!("ebus/5/{TGT}/ctl/point");
    let target = format!("{value}/$target");
    let state = format!("ebus/5/{TGT}/$state");
    let mut obs = observe(broker.port, "target-observer", &[&state, &value, &target]).await;
    let run = ebus_rumqttc::device(
        MqttOptions::new(TGT, "127.0.0.1", broker.port),
        b,
        DeviceOptions::default(),
    )
    .expect("device");
    let task = tokio::spawn(run.task.run());
    until_message(&mut obs, 15, &state, b"ready").await;
    let Some(mut py) = PyController::start(test, broker.port, TGT) else {
        return;
    };
    py.await_device(TGT, 1);
    py.await_target(TGT, "ctl", "point", "1.0");

    // "1.50" re-encodes as "1.5".
    py.set(TGT, "ctl", "point", "1.50");
    let req = next_set(&mut point).await;
    assert_eq!(req.raw(), b"1.50");
    assert_eq!(*req.value(), 1.5);
    req.accept();
    let before = until_message(&mut obs, 5, &target, b"1.50").await;
    assert!(before.is_empty(), "{before:?}");
    py.await_target(TGT, "ctl", "point", "1.50");
    assert_eq!(py.value(TGT, "ctl", "point"), "1.0");

    // The value reached closes the transition; `$target` already names it.
    point.set(1.5).expect("point");
    let before = until_message(&mut obs, 5, &value, b"1.5").await;
    assert!(before.is_empty(), "{before:?}");
    py.await_value(TGT, "ctl", "point", "1.5");
    py.await_target(TGT, "ctl", "point", "1.50");
    let snap = retained(broker.port, &format!("ebus/5/{TGT}/ctl/#")).await;
    assert_eq!(
        snap,
        BTreeMap::from([(value, b"1.5".to_vec()), (target, b"1.50".to_vec())])
    );

    drop(py);
    run.handle.shutdown(Shutdown::Announce).await;
    let res = timeout(Duration::from_secs(10), task)
        .await
        .expect("task ends after shutdown")
        .expect("task did not panic");
    assert!(res.is_ok(), "{res:?}");
}
