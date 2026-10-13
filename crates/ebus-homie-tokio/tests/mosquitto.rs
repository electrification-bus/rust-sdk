//! Bring-your-own connection against a local mosquitto, with rumqttc 0.10 as
//! the host's client: a synchronous `Client`, a `std::thread` reader over
//! `Connection::iter`, and an executor task using the non-blocking `try_*`
//! calls, for the controller and for a device tree. Skipped when no
//! `mosquitto` binary is found (`MOSQUITTO` names one).

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, ChildStderr, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ebus_homie_tokio::device::{
    DeviceHandle, DeviceOptions, DeviceSpec, NodeSpec, PropSpec, Property, Settable, TreeBuilder,
    TreeConfig, attach_device,
};
use ebus_homie_tokio::ebus_homie::controller::{Config, Event};
use ebus_homie_tokio::ebus_mqtt::{Command, QoS};
use ebus_homie_tokio::{
    ControllerDriver, ControllerHandle, ControllerOptions, Delivered, DriverError, EventReceiver,
    attach_controller,
};
use rumqttc::{AsyncClient, Client, Connection, MqttOptions, Packet, SubscribeFilter};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

fn mosquitto() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("MOSQUITTO") {
        return Some(PathBuf::from(p));
    }
    let mut dirs: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).collect())
        .unwrap_or_default();
    dirs.extend(
        ["/opt/homebrew/sbin", "/usr/local/sbin", "/usr/sbin"]
            .iter()
            .map(PathBuf::from),
    );
    dirs.into_iter()
        .map(|d| d.join("mosquitto"))
        .find(|p| p.is_file())
}

/// A mosquitto on a free 127.0.0.1 port, without persistence; killed on drop.
struct Broker {
    child: Option<Child>,
    bin: PathBuf,
    conf: PathBuf,
    port: u16,
}

impl Broker {
    fn start(test: &str) -> Option<Broker> {
        let Some(bin) = mosquitto() else {
            eprintln!("skipped {test}: no mosquitto binary (set MOSQUITTO to name one)");
            return None;
        };
        // A free port can be taken by another socket before mosquitto binds
        // it; mosquitto then exits, and a fresh port is tried.
        let mut failure = String::new();
        for _ in 0..10 {
            let port = TcpListener::bind("127.0.0.1:0")
                .and_then(|l| l.local_addr())
                .expect("free port")
                .port();
            let conf = std::env::temp_dir().join(format!(
                "ebus-homie-tokio-{}-{port}.conf",
                std::process::id()
            ));
            std::fs::write(
                &conf,
                format!(
                    "listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\nlog_dest stderr\n"
                ),
            )
            .expect("write config");
            let mut broker = Broker {
                child: None,
                bin: bin.clone(),
                conf,
                port,
            };
            match broker.spawn() {
                Ok(()) => return Some(broker),
                Err(e) => failure = format!("port {port}: {e}"),
            }
        }
        panic!("mosquitto did not start: {failure}");
    }

    /// Starts mosquitto on this broker's port, with nothing retained.
    fn spawn(&mut self) -> Result<(), String> {
        let mut child = std::process::Command::new(&self.bin)
            .arg("-c")
            .arg(&self.conf)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start mosquitto");
        let ready = mosquitto_running(child.stderr.take().expect("piped stderr"));
        self.child = Some(child);
        ready
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Reads mosquitto's log from `stderr` until its `running` line, which it
/// logs once its listener is bound, within 10 s; else returns what it logged.
/// The rest of the log is drained in the background.
fn mosquitto_running(stderr: ChildStderr) -> Result<(), String> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut log = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(left) {
            Ok(line) if line.ends_with(" running") => return Ok(()),
            Ok(line) => log.push(line),
            Err(_) => return Err(log.join("\n")),
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.kill();
        let _ = std::fs::remove_file(&self.conf);
    }
}

fn qos(q: QoS) -> rumqttc::QoS {
    match q {
        QoS::AtMostOnce => rumqttc::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
    }
}

fn options(id: &str, port: u16) -> MqttOptions {
    let mut o = MqttOptions::new(id, "127.0.0.1", port);
    o.set_max_packet_size(1 << 20, 1 << 20);
    o
}

/// A rumqttc 0.10 publisher that waits for the broker's acknowledgements.
struct Seeder {
    client: Client,
    sent: usize,
    acked: Arc<AtomicUsize>,
}

impl Seeder {
    fn new(port: u16) -> Seeder {
        let mut o = options("seeder", port);
        o.set_inflight(1000);
        let (client, mut conn) = Client::new(o, 4096);
        let acked = Arc::new(AtomicUsize::new(0));
        let a = Arc::clone(&acked);
        std::thread::spawn(move || {
            for ev in conn.iter() {
                match ev {
                    Ok(rumqttc::Event::Incoming(Packet::PubAck(_))) => {
                        a.fetch_add(1, Ordering::AcqRel);
                    }
                    Ok(_) => {}
                    Err(_) => std::thread::sleep(Duration::from_millis(100)),
                }
            }
        });
        Seeder {
            client,
            sent: 0,
            acked,
        }
    }

    fn publish(&mut self, topic: &str, payload: &str, retain: bool) {
        self.client
            .try_publish(topic, rumqttc::QoS::AtLeastOnce, retain, payload.as_bytes())
            .expect("seeder publish");
        self.sent += 1;
    }

    /// A retained device with one node `n` of `props` integer properties set
    /// to `base + i`.
    fn device(&mut self, id: &str, props: usize, base: usize) {
        let p: Vec<String> = (0..props)
            .map(|i| format!(r#""p{i:03}":{{"datatype":"integer"}}"#))
            .collect();
        let desc = format!(
            r#"{{"homie":"5.0","version":1,"nodes":{{"n":{{"properties":{{{}}}}}}}}}"#,
            p.join(",")
        );
        self.publish(&format!("ebus/5/{id}/$description"), &desc, true);
        self.values(id, props, base);
        self.publish(&format!("ebus/5/{id}/$state"), "ready", true);
    }

    fn values(&mut self, id: &str, props: usize, base: usize) {
        for i in 0..props {
            self.publish(
                &format!("ebus/5/{id}/n/p{i:03}"),
                &(base + i).to_string(),
                true,
            );
        }
    }

    async fn flush(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.acked.load(Ordering::Acquire) < self.sent {
            assert!(
                Instant::now() < deadline,
                "seeder publishes not acknowledged"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Runs the driver only while `gate` holds `true`.
async fn gated(
    driver: ControllerDriver,
    mut gate: watch::Receiver<bool>,
) -> Result<(), DriverError> {
    let run = driver.run();
    tokio::pin!(run);
    loop {
        let open = *gate.borrow_and_update();
        tokio::select! {
            r = &mut run, if open => return r,
            c = gate.changed() => if c.is_err() {
                return run.await;
            },
        }
    }
}

/// A host service: its own rumqttc 0.10 client and filters, with the
/// controller attached.
struct Host {
    handle: ControllerHandle,
    events: EventReceiver,
    driver: JoinHandle<Result<(), DriverError>>,
    executor: JoinHandle<()>,
    gate: watch::Sender<bool>,
    /// Every message the host's reader received, for the host's own use.
    own: std::sync::mpsc::Receiver<(String, Vec<u8>)>,
    /// Every command the executor handed to the client.
    sent: Arc<Mutex<Vec<Command>>>,
    /// Messages the inbox refused.
    dropped: Arc<AtomicUsize>,
    /// UNSUBSCRIBE packets the executor handed to the client.
    unsubscribes: Arc<AtomicUsize>,
    /// UNSUBACKs the reader received.
    unsubacks: Arc<AtomicUsize>,
    seen: Vec<Event>,
}

fn start_host(port: u16, host_filters: &[&str], inbox_bytes: usize) -> Host {
    let mut cfg = Config::wildcard();
    cfg.host_filters = host_filters.iter().map(|f| (*f).to_owned()).collect();
    let mut opts = ControllerOptions::default();
    opts.inbox_bytes = inbox_bytes;
    let ebus = attach_controller(cfg, opts).expect("valid config");
    let (gate, gate_rx) = watch::channel(true);
    let driver = tokio::spawn(gated(ebus.driver, gate_rx));

    let (client, conn): (Client, Connection) = Client::new(options("host-service", port), 256);
    let client = Arc::new(Mutex::new(client));

    // The host's reader thread, with the eBus arms added. The host subscribes
    // its own filters on every CONNACK.
    let (own_tx, own) = std::sync::mpsc::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let unsubacks = Arc::new(AtomicUsize::new(0));
    {
        let inbox = ebus.inbox.clone();
        let client = Arc::clone(&client);
        let dropped = Arc::clone(&dropped);
        let unsubacks = Arc::clone(&unsubacks);
        let filters: Vec<String> = host_filters.iter().map(|f| (*f).to_owned()).collect();
        let mut conn = conn;
        std::thread::spawn(move || {
            for ev in conn.iter() {
                match ev {
                    Ok(rumqttc::Event::Incoming(Packet::ConnAck(a))) => {
                        inbox.connected(a.session_present);
                        if !filters.is_empty() {
                            let mut c = client.lock().unwrap_or_else(|p| p.into_inner());
                            let subs = filters.iter().map(|f| {
                                SubscribeFilter::new(f.clone(), rumqttc::QoS::AtLeastOnce)
                            });
                            c.try_subscribe_many(subs).expect("host subscribe");
                        }
                    }
                    Ok(rumqttc::Event::Incoming(Packet::Publish(p))) => {
                        if p.topic.starts_with("ebus/5/")
                            && !inbox.message(&p.topic, p.payload.clone(), Some(p.retain))
                        {
                            dropped.fetch_add(1, Ordering::AcqRel);
                        }
                        let _ = own_tx.send((p.topic, p.payload.to_vec()));
                    }
                    Ok(rumqttc::Event::Incoming(Packet::UnsubAck(_))) => {
                        unsubacks.fetch_add(1, Ordering::AcqRel);
                    }
                    Ok(_) => {}
                    Err(_) => {
                        inbox.disconnected();
                        std::thread::sleep(Duration::from_millis(200));
                    }
                }
            }
        });
    }

    // One executor task; try_* never blocks, so holding the std Mutex is safe.
    let sent = Arc::new(Mutex::new(Vec::new()));
    let unsubscribes = Arc::new(AtomicUsize::new(0));
    let executor = {
        let mut commands = ebus.commands;
        let inbox = ebus.inbox;
        let sent = Arc::clone(&sent);
        let unsubscribes = Arc::clone(&unsubscribes);
        tokio::spawn(async move {
            while let Some(cmd) = commands.recv().await {
                sent.lock().expect("log").push(cmd.clone());
                let res = {
                    let mut c = client.lock().unwrap_or_else(|p| p.into_inner());
                    match &cmd {
                        Command::Subscribe { filters, .. } => c.try_subscribe_many(
                            filters
                                .iter()
                                .map(|(f, q)| SubscribeFilter::new(f.clone(), qos(*q))),
                        ),
                        Command::Unsubscribe { filters, .. } => filters.iter().try_for_each(|f| {
                            c.try_unsubscribe(f.clone())?;
                            unsubscribes.fetch_add(1, Ordering::AcqRel);
                            Ok(())
                        }),
                        Command::Publish {
                            topic,
                            payload,
                            qos: q,
                            retain,
                            ..
                        } => c.try_publish(topic.clone(), qos(*q), *retain, payload.clone()),
                        _ => Ok(()),
                    }
                };
                if res.is_err() {
                    inbox.command_failed(cmd);
                }
            }
        })
    };

    Host {
        handle: ebus.handle,
        events: ebus.events,
        driver,
        executor,
        gate,
        own,
        sent,
        dropped,
        unsubscribes,
        unsubacks,
        seen: Vec::new(),
    }
}

impl Host {
    async fn until(
        &mut self,
        secs: u64,
        what: &str,
        mut done: impl FnMut(&Event) -> bool,
    ) -> Event {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let d = timeout_at(deadline, self.events.recv())
                .await
                .unwrap_or_else(|_| {
                    let tail = &self.seen[self.seen.len().saturating_sub(20)..];
                    panic!("timed out waiting for {what}; seen {tail:#?}")
                })
                .expect("driver running");
            match d {
                Delivered::Event(e) => {
                    self.seen.push(e.clone());
                    if done(&e) {
                        return e;
                    }
                }
                Delivered::Overflow { dropped } => panic!("event overflow: {dropped}"),
                other => panic!("unexpected delivery: {other:?}"),
            }
        }
    }

    async fn synced(&mut self, secs: u64, devices: &[&str]) {
        let mut left: Vec<String> = devices.iter().map(|d| (*d).to_owned()).collect();
        while !left.is_empty() {
            let e = self
                .until(secs, &format!("DeviceSynced for {left:?}"), |e| {
                    matches!(e, Event::DeviceSynced { device, .. } if left.iter().any(|l| l == device.as_str()))
                })
                .await;
            if let Event::DeviceSynced {
                device, missing, ..
            } = e
            {
                assert_eq!(missing, 0, "{device:?} synced with values missing");
                left.retain(|l| l != device.as_str());
            }
        }
    }

    fn value(&self, device: &str, property: &str) -> Option<String> {
        self.handle
            .device(device)?
            .property("n", property)?
            .value
            .as_ref()
            .map(|v| v.raw.to_string())
    }

    /// The host's own messages until one arrives on `topic`, within 5 s.
    fn own_until(&self, topic: &str) -> Vec<String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut got = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            let (t, _) = tokio::task::block_in_place(|| self.own.recv_timeout(left))
                .unwrap_or_else(|_| panic!("host did not receive {topic}; got {got:?}"));
            let done = t == topic;
            got.push(t);
            if done {
                return got;
            }
        }
    }

    /// Waits until the broker acknowledged every UNSUBSCRIBE handed to the
    /// client, so a publish after it is routed without the dropped filters.
    async fn unsubscribed(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let want = self.unsubscribes.load(Ordering::Acquire);
            let got = self.unsubacks.load(Ordering::Acquire);
            if got >= want {
                return;
            }
            assert!(Instant::now() < deadline, "{got} of {want} UNSUBACKs");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Shuts the controller down and waits for the driver and the executor.
    async fn shutdown(&mut self) -> Vec<Command> {
        self.handle.shutdown().await;
        let res = tokio::time::timeout(Duration::from_secs(5), &mut self.driver)
            .await
            .expect("driver ends after shutdown")
            .expect("driver did not panic");
        assert_eq!(res, Ok(()));
        tokio::time::timeout(Duration::from_secs(5), &mut self.executor)
            .await
            .expect("executor ends with the driver")
            .expect("executor did not panic");
        self.sent.lock().expect("log").clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_filters_survive_controller_unsubscribes() {
    let Some(broker) = Broker::start("host_filters_survive_controller_unsubscribes") else {
        return;
    };
    // Identical to the controller's discovery filter, identical to h1's
    // property filter, and overlapping every filter of h2.
    let host_filters = ["ebus/5/+/$state", "ebus/5/h1/+/+", "ebus/5/h2/#"];
    let mut seed = Seeder::new(broker.port);
    seed.device("h1", 3, 10);
    seed.device("h2", 3, 20);
    seed.flush().await;

    let mut host = start_host(broker.port, &host_filters, 4 << 20);
    host.synced(15, &["h1", "h2"]).await;
    assert_eq!(host.value("h1", "p002").as_deref(), Some("12"));
    assert_eq!(host.value("h2", "p000").as_deref(), Some("20"));

    // A live update arrives through overlapping subscriptions; the
    // controller reports it once.
    seed.publish("ebus/5/h2/n/p001", "99", true);
    seed.flush().await;
    host.until(5, "h2/n/p001 = 99", |e| {
        matches!(e, Event::PropertyChanged { addr, value, .. }
            if addr.device.as_str() == "h2" && &*value.raw == "99")
    })
    .await;

    seed.publish("ebus/5/h1/$state", "", true);
    seed.flush().await;
    host.until(
        5,
        "DeviceRemoved h1",
        |e| matches!(e, Event::DeviceRemoved { device, .. } if device.as_str() == "h1"),
    )
    .await;
    while host.own.try_recv().is_ok() {}
    let sent = host.shutdown().await;

    let subscribed: Vec<&str> = sent
        .iter()
        .filter_map(|c| match c {
            Command::Subscribe { filters, .. } => Some(filters),
            _ => None,
        })
        .flatten()
        .map(|(f, _)| f.as_str())
        .collect();
    let unsubscribed: Vec<&str> = sent
        .iter()
        .filter_map(|c| match c {
            Command::Unsubscribe { filters, .. } => Some(filters),
            _ => None,
        })
        .flatten()
        .map(String::as_str)
        .collect();
    for f in ["ebus/5/+/$state", "ebus/5/h1/+/+", "ebus/5/h1/$description"] {
        assert!(
            subscribed.contains(&f),
            "{f} not subscribed: {subscribed:?}"
        );
    }
    for f in host_filters {
        assert!(
            !unsubscribed.contains(&f),
            "{f} unsubscribed: {unsubscribed:?}"
        );
    }
    for f in [
        "ebus/5/h1/$description",
        "ebus/5/h2/+/+",
        "ebus/5/h2/$description",
    ] {
        assert!(unsubscribed.contains(&f), "{f} kept: {unsubscribed:?}");
    }

    // The broker still delivers on every host filter, and no longer on a
    // filter only the controller held.
    host.unsubscribed().await;
    seed.publish("ebus/5/h1/$description", "{}", false);
    seed.publish("ebus/5/h1/n/p000", "1", false);
    seed.publish("ebus/5/h2/n/p000", "2", false);
    seed.publish("ebus/5/zz/$state", "init", false);
    seed.flush().await;
    let got = host.own_until("ebus/5/zz/$state");
    for t in ["ebus/5/h1/n/p000", "ebus/5/h2/n/p000"] {
        assert!(got.iter().any(|g| g == t), "{t} not received: {got:?}");
    }
    assert!(
        !got.iter().any(|g| g == "ebus/5/h1/$description"),
        "{got:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbox_overflow_drops_heal_by_resubscribing() {
    let Some(broker) = Broker::start("inbox_overflow_drops_heal_by_resubscribing") else {
        return;
    };
    const PROPS: usize = 50;
    const ROUNDS: usize = 10;
    let mut seed = Seeder::new(broker.port);
    seed.device("d1", PROPS, 0);
    seed.flush().await;

    let mut host = start_host(broker.port, &[], 4096);
    host.synced(15, &["d1"]).await;
    assert_eq!(host.value("d1", "p049").as_deref(), Some("49"));
    while host.own.try_recv().is_ok() {}

    // Stall the driver while the broker delivers more than the inbox holds.
    host.gate.send_replace(false);
    for r in 1..=ROUNDS {
        seed.values("d1", PROPS, r * 1000);
    }
    seed.flush().await;
    let last = format!("ebus/5/d1/n/p{:03}", PROPS - 1);
    let mut seen = 0;
    while seen < ROUNDS {
        if host.own_until(&last).last().is_some() {
            seen += 1;
        }
    }
    let dropped = host.dropped.load(Ordering::Acquire);
    // Once full, a stalled inbox refuses everything after: at least the last
    // round, which only a resubscribe can recover.
    assert!(dropped >= PROPS, "the inbox dropped only {dropped}");

    host.gate.send_replace(true);
    let deadline = Instant::now() + Duration::from_secs(10);
    let want = |i: usize| Some((ROUNDS * 1000 + i).to_string());
    while (0..PROPS).any(|i| host.value("d1", &format!("p{i:03}")) != want(i)) {
        assert!(
            Instant::now() < deadline,
            "values not healed after {dropped} drops"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    host.shutdown().await;
}

const ROOT: &str = "byo-root";
const CHILD: &str = "byo-child";
const ROOT_STATE: &str = "ebus/5/byo-root/$state";
const CHILD_STATE: &str = "ebus/5/byo-child/$state";
const LEVEL: &str = "ebus/5/byo-root/ctl/level";

/// A host service that owns a rumqttc 0.10 connection with a device tree
/// attached: `byo-root` with a settable `level` and a `temp`, and the child
/// `byo-child` with a `p0`.
struct DevHost {
    handle: DeviceHandle,
    level: Settable<i64>,
    temp: Property<f64>,
    _p0: Property<i64>,
    driver: JoinHandle<Result<(), DriverError>>,
    executor: JoinHandle<()>,
    client: Arc<Mutex<Client>>,
    reader: std::thread::JoinHandle<()>,
}

fn start_device(port: u16) -> DevHost {
    let mut b = TreeBuilder::new(
        TreeConfig::default(),
        ROOT,
        DeviceSpec::typed("energy.ebus.device.test"),
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
        .settable::<i64>(ctl, "level", PropSpec::new().format("0:100"))
        .expect("level");
    let temp = b
        .property::<f64>(ctl, "temp", PropSpec::new().unit("°C"))
        .expect("temp");
    let child = b
        .child(
            b.root(),
            CHILD,
            DeviceSpec::typed("energy.ebus.device.test"),
        )
        .expect("child");
    let meter = b
        .node(
            child,
            "meter",
            NodeSpec::typed("energy.ebus.capability.meter"),
        )
        .expect("node");
    let p0 = b.property::<i64>(meter, "p0", PropSpec::new()).expect("p0");
    level.set(50).expect("level");
    temp.set(21.5).expect("temp");
    p0.set(100).expect("p0");
    let dev = attach_device(b, DeviceOptions::default()).expect("valid tree");
    let driver = tokio::spawn(dev.driver.run());

    // The device's will, the root's client id, and the bundled path's
    // inflight cap, since mosquitto discards QoS 2 publishes past its
    // receive quota of 20.
    let w = &dev.last_will;
    let mut o = options(ROOT, port);
    o.set_last_will(rumqttc::LastWill::new(
        w.topic.clone(),
        w.payload.clone(),
        qos(w.qos),
        w.retain,
    ));
    o.set_inflight(20);
    let (client, mut conn): (Client, Connection) = Client::new(o, 256);
    let client = Arc::new(Mutex::new(client));

    // The reader forwards every CONNACK, every eBus message with its retain
    // bit, and every connection error; the device subscribes and republishes.
    let reader = {
        let inbox = dev.inbox.clone();
        std::thread::spawn(move || {
            for ev in conn.iter() {
                match ev {
                    Ok(rumqttc::Event::Incoming(Packet::ConnAck(a))) => {
                        inbox.connected(a.session_present);
                    }
                    Ok(rumqttc::Event::Incoming(Packet::Publish(p))) => {
                        if p.topic.starts_with("ebus/5/") {
                            inbox.message(p.topic, p.payload, Some(p.retain));
                        }
                    }
                    Ok(_) => {}
                    Err(_) => {
                        inbox.disconnected();
                        std::thread::sleep(Duration::from_millis(200));
                    }
                }
            }
        })
    };

    let executor = {
        let mut commands = dev.commands;
        let inbox = dev.inbox;
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            while let Some(cmd) = commands.recv().await {
                let res = {
                    let mut c = client.lock().unwrap_or_else(|p| p.into_inner());
                    match &cmd {
                        Command::Subscribe { filters, .. } => c.try_subscribe_many(
                            filters
                                .iter()
                                .map(|(f, q)| SubscribeFilter::new(f.clone(), qos(*q))),
                        ),
                        Command::Unsubscribe { filters, .. } => filters
                            .iter()
                            .try_for_each(|f| c.try_unsubscribe(f.clone())),
                        Command::Publish {
                            topic,
                            payload,
                            qos: q,
                            retain,
                            ..
                        } => c.try_publish(topic.clone(), qos(*q), *retain, payload.clone()),
                        _ => Ok(()),
                    }
                };
                if res.is_err() {
                    inbox.command_failed(cmd);
                }
            }
        })
    };

    DevHost {
        handle: dev.handle,
        level,
        temp,
        _p0: p0,
        driver,
        executor,
        client,
        reader,
    }
}

impl DevHost {
    /// The host goes away without shutting the device down: the driver and
    /// executor stop, and the connection closes without DISCONNECT.
    fn drop_without_shutdown(self) {
        self.driver.abort();
        self.executor.abort();
        tokio::task::block_in_place(|| {
            self.client
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .cancel()
                .expect("cancel the connection");
            self.reader.join().expect("reader did not panic");
        });
    }

    /// Waits until the device reports `connected`.
    async fn link(&self, connected: bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.handle.is_connected() != connected {
            assert!(
                Instant::now() < deadline,
                "device never reported connected = {connected}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The value topics the tree retains once ready, with their payloads.
fn device_values(level: &str) -> BTreeMap<String, String> {
    [
        (LEVEL.to_owned(), level),
        (format!("ebus/5/{ROOT}/ctl/temp"), "21.5"),
        (format!("ebus/5/{CHILD}/meter/p0"), "100"),
    ]
    .into_iter()
    .map(|(t, p)| (t, p.to_owned()))
    .collect()
}

/// Checks that `snap` is exactly the ready tree with `level`: a description
/// of each device, both states `ready`, and every value.
fn assert_ready_tree(snap: &BTreeMap<String, String>, level: &str) {
    let mut want = device_values(level);
    want.insert(ROOT_STATE.into(), "ready".into());
    want.insert(CHILD_STATE.into(), "ready".into());
    for d in [ROOT, CHILD] {
        let topic = format!("ebus/5/{d}/$description");
        let desc = snap.get(&topic).unwrap_or_else(|| panic!("no {topic}"));
        assert!(desc.starts_with(r#"{"homie": "5.0""#), "{topic}: {desc}");
        want.insert(topic, desc.clone());
    }
    assert_eq!(snap, &want);
}

type Received = tokio::sync::mpsc::UnboundedReceiver<(String, Vec<u8>, bool)>;

/// A rumqttc 0.10 client subscribed to `filters`, once the broker has
/// acknowledged every SUBSCRIBE; it yields each message with its retain bit.
async fn observe(port: u16, id: &str, filters: &[&str]) -> Received {
    observer(port, id, filters).await.1
}

/// [`observe`], with the client.
async fn observer(port: u16, id: &str, filters: &[&str]) -> (AsyncClient, Received) {
    let (client, mut el) = AsyncClient::new(options(id, port), 64);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (sub_tx, sub_rx) = tokio::sync::oneshot::channel();
    let n = filters.len();
    let keep = client.clone();
    tokio::spawn(async move {
        let _keep = keep;
        let mut sub_tx = Some(sub_tx);
        let mut subacks = 0;
        loop {
            match el.poll().await {
                Ok(rumqttc::Event::Incoming(Packet::Publish(p))) => {
                    let _ = tx.send((p.topic, p.payload.to_vec(), p.retain));
                }
                Ok(rumqttc::Event::Incoming(Packet::SubAck(_))) => {
                    subacks += 1;
                    if subacks == n {
                        if let Some(s) = sub_tx.take() {
                            let _ = s.send(());
                        }
                    }
                }
                Ok(_) => {}
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    });
    for f in filters {
        client
            .subscribe(*f, rumqttc::QoS::AtLeastOnce)
            .await
            .expect("observer subscribe");
    }
    tokio::time::timeout(Duration::from_secs(5), sub_rx)
        .await
        .expect("observer subscribed in time")
        .expect("observer running");
    (client, rx)
}

/// Receives until `topic` = `payload` arrives within `secs`; returns what
/// came before it.
async fn until_message(
    rx: &mut Received,
    secs: u64,
    topic: &str,
    payload: &[u8],
) -> Vec<(String, Vec<u8>, bool)> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut before = Vec::new();
    loop {
        let m = timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out waiting for {topic} = {:?}; got {before:?}",
                    String::from_utf8_lossy(payload)
                )
            })
            .expect("observer running");
        if m.0 == topic && m.1 == payload {
            return before;
        }
        before.push(m);
    }
}

/// Every retained message under `ebus/5/#` the broker holds now: a fresh
/// client subscribes, publishes a marker to itself, and collects until the
/// marker arrives, which the broker queues after the retained messages.
async fn retained(port: u16) -> BTreeMap<String, String> {
    static N: AtomicUsize = AtomicUsize::new(0);
    let id = format!("snapshot-{}", N.fetch_add(1, Ordering::AcqRel));
    let marker = format!("ebus-test/{id}");
    let (client, mut rx) = observer(port, &id, &["ebus/5/#", &marker]).await;
    client
        .publish(&marker, rumqttc::QoS::AtMostOnce, false, "end")
        .await
        .expect("snapshot marker");
    let mut out = BTreeMap::new();
    for (topic, payload, retain) in until_message(&mut rx, 15, &marker, b"end").await {
        assert!(retain, "a live message during the snapshot: {topic}");
        out.insert(topic, String::from_utf8(payload).expect("UTF-8 payload"));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn device_on_a_host_connection_publishes_serves_set_and_republishes() {
    let Some(mut broker) =
        Broker::start("device_on_a_host_connection_publishes_serves_set_and_republishes")
    else {
        return;
    };
    let mut obs = observe(broker.port, "observer", &[ROOT_STATE, LEVEL]).await;
    let mut dev = start_device(broker.port);
    until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    assert_ready_tree(&retained(broker.port).await, "50");

    // A /set from another client reaches the device; the accepted value is
    // republished.
    let mut setter = Seeder::new(broker.port);
    setter.publish(&format!("{LEVEL}/set"), "35", false);
    let req = timeout_at(
        Instant::now() + Duration::from_secs(5),
        dev.level.next_set(),
    )
    .await
    .expect("a /set in time")
    .expect("driver running");
    assert_eq!(*req.value(), 35);
    req.accept();
    until_message(&mut obs, 5, LEVEL, b"35").await;
    let before = retained(broker.port).await;
    assert_ready_tree(&before, "35");

    // The broker restarts without persistence: the host reports the lost
    // connection and the new CONNACK, and the SDK republishes the whole tree.
    broker.kill();
    dev.link(false).await;
    broker.spawn().expect("mosquitto restarts");
    dev.link(true).await;
    let mut obs = observe(broker.port, "restart-observer", &[ROOT_STATE]).await;
    until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    assert_eq!(retained(broker.port).await, before);

    // The SDK subscribes `/set` again on the new session.
    let mut obs = observe(broker.port, "set-observer", &[LEVEL]).await;
    setter.publish(&format!("{LEVEL}/set"), "60", false);
    let req = timeout_at(
        Instant::now() + Duration::from_secs(5),
        dev.level.next_set(),
    )
    .await
    .expect("a /set in time after the restart")
    .expect("driver running");
    assert_eq!(*req.value(), 60);
    req.accept();
    until_message(&mut obs, 5, LEVEL, b"60").await;

    // Values set while connected after the reconnect still go out.
    dev.temp.set(22.25).expect("temp");
    let mut obs = observe(broker.port, "temp-observer", &["ebus/5/byo-root/ctl/temp"]).await;
    until_message(&mut obs, 5, "ebus/5/byo-root/ctl/temp", b"22.25").await;
    dev.drop_without_shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn device_on_a_host_connection_dropped_without_shutdown_leaves_the_will() {
    let Some(broker) =
        Broker::start("device_on_a_host_connection_dropped_without_shutdown_leaves_the_will")
    else {
        return;
    };
    let mut obs = observe(broker.port, "will-observer", &[ROOT_STATE]).await;
    let dev = start_device(broker.port);
    until_message(&mut obs, 15, ROOT_STATE, b"ready").await;
    dev.drop_without_shutdown();
    until_message(&mut obs, 15, ROOT_STATE, b"lost").await;
    let snap = retained(broker.port).await;
    assert_eq!(snap[ROOT_STATE], "lost");
    assert_eq!(snap[CHILD_STATE], "ready");
}
