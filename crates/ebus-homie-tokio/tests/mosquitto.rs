//! Bring-your-own connection against a local mosquitto, with rumqttc 0.10 as
//! the host's client: a synchronous `Client`, a `std::thread` reader over
//! `Connection::iter`, and an executor task using the non-blocking `try_*`
//! calls. Skipped when no `mosquitto` binary is found (`MOSQUITTO` names one).

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ebus_homie_tokio::ebus_homie::controller::{Config, Event};
use ebus_homie_tokio::ebus_mqtt::{Command, QoS};
use ebus_homie_tokio::{
    AttachOptions, ControllerDriver, ControllerHandle, Delivered, DriverError, EventReceiver,
    attach,
};
use rumqttc::{Client, Connection, MqttOptions, Packet, SubscribeFilter};
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
    child: Child,
    conf: PathBuf,
    port: u16,
}

impl Broker {
    fn start(test: &str) -> Option<Broker> {
        let Some(bin) = mosquitto() else {
            eprintln!("skipped {test}: no mosquitto binary (set MOSQUITTO to name one)");
            return None;
        };
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
            format!("listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n"),
        )
        .expect("write config");
        let child = std::process::Command::new(bin)
            .arg("-c")
            .arg(&conf)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start mosquitto");
        let broker = Broker { child, conf, port };
        for _ in 0..200 {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Some(broker);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("mosquitto did not listen on {port}");
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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
    seen: Vec<Event>,
}

fn start_host(port: u16, host_filters: &[&str], inbox_bytes: usize) -> Host {
    let mut cfg = Config::wildcard();
    cfg.host_filters = host_filters.iter().map(|f| (*f).to_owned()).collect();
    let mut opts = AttachOptions::default();
    opts.inbox_bytes = inbox_bytes;
    let ebus = attach(cfg, opts).expect("valid config");
    let (gate, gate_rx) = watch::channel(true);
    let driver = tokio::spawn(gated(ebus.driver, gate_rx));

    let (client, conn): (Client, Connection) = Client::new(options("host-service", port), 256);
    let client = Arc::new(Mutex::new(client));

    // The host's reader thread, with the eBus arms added. The host subscribes
    // its own filters on every CONNACK.
    let (own_tx, own) = std::sync::mpsc::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    {
        let inbox = ebus.inbox.clone();
        let client = Arc::clone(&client);
        let dropped = Arc::clone(&dropped);
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
    let executor = {
        let mut commands = ebus.commands;
        let inbox = ebus.inbox;
        let sent = Arc::clone(&sent);
        tokio::spawn(async move {
            while let Some(cmd) = commands.recv().await {
                sent.lock().expect("log").push(cmd.clone());
                let res = {
                    let mut c = client.lock().unwrap_or_else(|p| p.into_inner());
                    match &cmd {
                        Command::Subscribe { filters } => c.try_subscribe_many(
                            filters
                                .iter()
                                .map(|(f, q)| SubscribeFilter::new(f.clone(), qos(*q))),
                        ),
                        Command::Unsubscribe { filters } => filters
                            .iter()
                            .try_for_each(|f| c.try_unsubscribe(f.clone())),
                        Command::Publish {
                            topic,
                            payload,
                            qos: q,
                            retain,
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
            if let Event::DeviceSynced { device, missing } = e {
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
        |e| matches!(e, Event::DeviceRemoved { device } if device.as_str() == "h1"),
    )
    .await;
    while host.own.try_recv().is_ok() {}
    let sent = host.shutdown().await;

    let subscribed: Vec<&str> = sent
        .iter()
        .filter_map(|c| match c {
            Command::Subscribe { filters } => Some(filters),
            _ => None,
        })
        .flatten()
        .map(|(f, _)| f.as_str())
        .collect();
    let unsubscribed: Vec<&str> = sent
        .iter()
        .filter_map(|c| match c {
            Command::Unsubscribe { filters } => Some(filters),
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
