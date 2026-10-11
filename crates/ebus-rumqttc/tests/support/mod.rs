//! A throwaway mosquitto per test, python-sdk device and controller
//! processes, a proxy that can cut and mute connections and counts what the
//! client sends, and rumqttc helpers that seed and observe the broker.

// Each test binary uses part of this module.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event};
use ebus_rumqttc::ebus_homie_tokio::{AttachOptions, ControllerHandle, Delivered, EventReceiver};
use rumqttc::{AsyncClient, MqttOptions, Packet, QoS};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

/// The mosquitto binary: `MOSQUITTO`, else the first on `PATH` or in the
/// usual sbin directories.
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
pub struct Broker {
    bin: PathBuf,
    child: Option<Child>,
    conf: PathBuf,
    /// The listener port.
    pub port: u16,
}

impl Broker {
    /// Starts a broker with `extra` appended to its configuration, or `None`
    /// (with a message) when no mosquitto binary is found.
    pub fn start(test: &str, extra: &str) -> Option<Broker> {
        let Some(bin) = mosquitto() else {
            eprintln!("skipped {test}: no mosquitto binary (set MOSQUITTO to name one)");
            return None;
        };
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port")
            .port();
        let conf =
            std::env::temp_dir().join(format!("ebus-rumqttc-{}-{port}.conf", std::process::id()));
        std::fs::write(
            &conf,
            format!(
                "listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n{extra}\n"
            ),
        )
        .expect("write config");
        let mut b = Broker {
            bin,
            child: None,
            conf,
            port,
        };
        b.spawn();
        Some(b)
    }

    fn spawn(&mut self) {
        let child = Command::new(&self.bin)
            .arg("-c")
            .arg(&self.conf)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start mosquitto");
        self.child = Some(child);
        for _ in 0..200 {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("mosquitto did not listen on {}", self.port);
    }

    /// Kills the broker, losing every retained message and session.
    pub fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Kills the broker and starts a fresh one on the same port.
    pub fn restart(&mut self) {
        self.kill();
        self.spawn();
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.kill();
        let _ = std::fs::remove_file(&self.conf);
    }
}

/// A TCP proxy in front of a broker. A muted connection passes the client's
/// bytes and only the broker's first four, its CONNACK.
pub struct Proxy {
    /// The port clients connect to.
    pub port: u16,
    state: Arc<Mutex<ProxyState>>,
}

#[derive(Default)]
struct ProxyState {
    mute: bool,
    tasks: Vec<tokio::task::AbortHandle>,
    /// SUBSCRIBE packets the client sent, per connection since the last cut.
    subscribes: Vec<Arc<AtomicUsize>>,
    /// PUBLISH packets the clients sent, over every connection.
    publishes: Arc<AtomicUsize>,
    /// Connections accepted.
    connections: usize,
    /// UNSUBACK packets the broker sent, over every connection.
    unsubacks: Arc<AtomicUsize>,
}

impl Proxy {
    /// A proxy to the broker on `broker_port`.
    pub async fn start(broker_port: u16) -> Proxy {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind proxy");
        let port = listener.local_addr().expect("proxy address").port();
        let state = Arc::new(Mutex::new(ProxyState::default()));
        let st = Arc::clone(&state);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(broker) = tokio::net::TcpStream::connect(("127.0.0.1", broker_port)).await
                else {
                    continue;
                };
                let subs = Arc::new(AtomicUsize::new(0));
                let mut s = st.lock().expect("proxy state");
                s.subscribes.push(Arc::clone(&subs));
                s.connections += 1;
                let pubs = Arc::clone(&s.publishes);
                let unsubacks = Arc::clone(&s.unsubacks);
                let (cr, cw) = client.into_split();
                let (br, bw) = broker.into_split();
                let up = tokio::spawn(upstream(cr, bw, subs, pubs));
                let down = tokio::spawn(downstream(br, cw, s.mute, unsubacks));
                s.tasks.push(up.abort_handle());
                s.tasks.push(down.abort_handle());
            }
        });
        Proxy { port, state }
    }

    /// Mutes the connections accepted from now on, or not.
    pub fn mute(&self, on: bool) {
        self.state.lock().expect("proxy state").mute = on;
    }

    /// Closes every open connection.
    pub fn cut(&self) {
        let mut s = self.state.lock().expect("proxy state");
        for t in s.tasks.drain(..) {
            t.abort();
        }
        s.subscribes.clear();
    }

    /// PUBLISH packets the clients sent so far, over every connection.
    pub fn publishes(&self) -> usize {
        self.state
            .lock()
            .expect("proxy state")
            .publishes
            .load(Ordering::Acquire)
    }

    /// Connections accepted so far.
    pub fn connections(&self) -> usize {
        self.state.lock().expect("proxy state").connections
    }

    /// UNSUBACK packets the broker sent so far, over every connection.
    pub fn unsubacks(&self) -> usize {
        self.state
            .lock()
            .expect("proxy state")
            .unsubacks
            .load(Ordering::Acquire)
    }

    /// Waits until the broker sent `n` UNSUBACK packets, over every
    /// connection: it has then removed those subscriptions.
    pub async fn wait_unsubacks(&self, n: usize, secs: u64) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        let unsubacks = Arc::clone(&self.state.lock().expect("proxy state").unsubacks);
        loop {
            let seen = unsubacks.load(Ordering::Acquire);
            if seen >= n {
                return;
            }
            assert!(Instant::now() < deadline, "{seen} of {n} UNSUBACKs sent");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits until the client sent `n` SUBSCRIBE packets on one connection
    /// opened since the last cut.
    pub async fn wait_subscribes(&self, n: usize, secs: u64) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let seen = {
                let s = self.state.lock().expect("proxy state");
                s.subscribes
                    .iter()
                    .map(|c| c.load(Ordering::Acquire))
                    .max()
                    .unwrap_or(0)
            };
            if seen >= n {
                return;
            }
            assert!(Instant::now() < deadline, "{seen} of {n} SUBSCRIBEs sent");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The remaining length and the fixed header length of the MQTT packet
/// starting `b`, once all of it is in `b`.
fn frame(b: &[u8]) -> Option<(usize, usize)> {
    let mut len = 0usize;
    for i in 1..5 {
        let byte = *b.get(i)?;
        len += usize::from(byte & 0x7f) << (7 * (i - 1));
        if byte & 0x80 == 0 {
            return (b.len() >= i + 1 + len).then_some((len, i + 1));
        }
    }
    None
}

/// Client to broker, counting PUBLISH packets before they are forwarded and
/// SUBSCRIBE packets after.
async fn upstream(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    subs: Arc<AtomicUsize>,
    pubs: Arc<AtomicUsize>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 8192];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        pending.extend_from_slice(&buf[..n]);
        let mut subscribes = 0;
        while let Some((len, hdr)) = frame(&pending) {
            match pending[0] >> 4 {
                3 => {
                    pubs.fetch_add(1, Ordering::AcqRel);
                }
                8 => subscribes += 1,
                _ => {}
            }
            pending.drain(..hdr + len);
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
        subs.fetch_add(subscribes, Ordering::AcqRel);
    }
}

/// Broker to client, counting UNSUBACK packets; muted, only the first four
/// bytes pass.
async fn downstream(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mute: bool,
    unsubacks: Arc<AtomicUsize>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 8192];
    let mut pending: Vec<u8> = Vec::new();
    let mut passed = 0usize;
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        pending.extend_from_slice(&buf[..n]);
        while let Some((len, hdr)) = frame(&pending) {
            if pending[0] >> 4 == 11 {
                unsubacks.fetch_add(1, Ordering::AcqRel);
            }
            pending.drain(..hdr + len);
        }
        let pass = if mute { n.min(4 - passed) } else { n };
        if to.write_all(&buf[..pass]).await.is_err() {
            return;
        }
        passed += pass;
    }
}

/// The interpreter (`EBUS_PYTHON`, else `python3`) and a `PYTHONPATH` that
/// adds sibling `python-sdk` and `ebus-mqtt-client` checkouts when present,
/// or `None` (with a message) when python-sdk cannot be imported.
fn python_env(test: &str) -> Option<(std::ffi::OsString, std::ffi::OsString)> {
    let python = std::env::var_os("EBUS_PYTHON").unwrap_or_else(|| "python3".into());
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut path: Vec<PathBuf> = std::env::var_os("PYTHONPATH")
        .map(|p| std::env::split_paths(&p).collect())
        .unwrap_or_default();
    for sibling in ["../python-sdk/src", "../ebus-mqtt-client/src"] {
        let dir = workspace.join(sibling);
        if dir.is_dir() {
            path.push(dir);
        }
    }
    let path = std::env::join_paths(path).expect("PYTHONPATH");
    let probe = Command::new(&python)
        .args(["-c", "import ebus_sdk.homie"])
        .env("PYTHONPATH", &path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if !probe.is_ok_and(|s| s.success()) {
        eprintln!("skipped {test}: python-sdk not importable (set EBUS_PYTHON and PYTHONPATH)");
        return None;
    }
    Some((python, path))
}

/// A python-sdk device tree (`support/py_device.py`); killed on drop.
pub struct PyDevice {
    child: Child,
    stdin: Option<ChildStdin>,
    /// Lines the device printed: `ready`, then `set <property> <json value>`.
    pub lines: std::sync::mpsc::Receiver<String>,
}

impl PyDevice {
    /// Starts `py-root` with `children` children of `props` integer
    /// properties each, or `None` (with a message) when python-sdk cannot be
    /// imported (see [`python_env`]).
    pub fn start(test: &str, port: u16, children: usize, props: usize) -> Option<PyDevice> {
        let (python, path) = python_env(test)?;
        let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/support/py_device.py");
        let mut child = Command::new(&python)
            .arg("-u")
            .arg(script)
            .args(["--port", &port.to_string()])
            .args(["--children", &children.to_string()])
            .args(["--props", &props.to_string()])
            .env("PYTHONPATH", &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("start python device");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Some(PyDevice {
            child,
            stdin,
            lines,
        })
    }

    /// The next printed line, within `secs` seconds. Needs the multi-thread
    /// runtime.
    pub fn line(&self, secs: u64) -> String {
        tokio::task::block_in_place(|| {
            self.lines
                .recv_timeout(Duration::from_secs(secs))
                .expect("a line from the python device")
        })
    }
}

impl Drop for PyDevice {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A python-sdk controller in tree mode (`support/py_controller.py`); killed
/// on drop.
pub struct PyController {
    child: Child,
    stdin: ChildStdin,
    lines: std::sync::mpsc::Receiver<String>,
}

impl PyController {
    /// Starts a controller for the tree under `root`, or `None` (with a
    /// message) when python-sdk cannot be imported.
    pub fn start(test: &str, port: u16, root: &str) -> Option<PyController> {
        let (python, path) = python_env(test)?;
        let script =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/support/py_controller.py");
        let mut child = Command::new(&python)
            .arg("-u")
            .arg(script)
            .args(["--port", &port.to_string()])
            .args(["--root", root])
            .env("PYTHONPATH", &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("start python controller");
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let ctl = PyController {
            child,
            stdin,
            lines,
        };
        assert_eq!(ctl.line(10), "ready");
        Some(ctl)
    }

    fn line(&self, secs: u64) -> String {
        tokio::task::block_in_place(|| {
            self.lines
                .recv_timeout(Duration::from_secs(secs))
                .expect("a line from the python controller")
        })
    }

    /// Sends one command and returns its JSON answer. Needs the
    /// multi-thread runtime.
    pub fn call(&mut self, cmd: serde_json::Value, secs: u64) -> serde_json::Value {
        writeln!(self.stdin, "{cmd}").expect("write to the python controller");
        self.stdin.flush().expect("flush to the python controller");
        serde_json::from_str(&self.line(secs)).expect("a JSON answer")
    }

    /// Waits until `device` is ready with its description and at least
    /// `values` values; returns its state, description and values.
    pub fn await_device(&mut self, device: &str, values: usize) -> serde_json::Value {
        let r = self.call(serde_json::json!(["await", device, values, 15]), 20);
        assert_eq!(r["ok"], true, "{device} not complete: {r}");
        r["device"].clone()
    }

    /// Waits until `device/node/property` holds `payload`.
    pub fn await_value(&mut self, device: &str, node: &str, property: &str, payload: &str) {
        let r = self.call(
            serde_json::json!(["value", device, node, property, payload, 10]),
            15,
        );
        assert_eq!(
            r["ok"], true,
            "{device}/{node}/{property} never {payload:?}: {r}"
        );
    }

    /// The current value of `device/node/property`, or JSON `null`.
    pub fn value(&mut self, device: &str, node: &str, property: &str) -> serde_json::Value {
        self.call(
            serde_json::json!(["value", device, node, property, "", 0]),
            5,
        )["value"]
            .clone()
    }

    /// Publishes `payload` to the property's `/set` topic.
    pub fn set(&mut self, device: &str, node: &str, property: &str, payload: &str) {
        let r = self.call(
            serde_json::json!(["set", device, node, property, payload]),
            5,
        );
        assert_eq!(r["ok"], true, "set not sent: {r}");
    }
}

impl Drop for PyController {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A rumqttc client that publishes and waits until the broker acknowledged
/// every QoS 1 publish.
pub struct Seeder {
    client: AsyncClient,
    sent: usize,
    acked: Arc<AtomicUsize>,
}

impl Seeder {
    /// Connects a seeder to the broker on `port`.
    pub fn new(port: u16, id: &str) -> Seeder {
        let mut mqtt = MqttOptions::new(id, "127.0.0.1", port);
        mqtt.set_max_packet_size(1 << 20, 1 << 20);
        mqtt.set_inflight(1000);
        let (client, mut el) = AsyncClient::new(mqtt, 4096);
        let acked = Arc::new(AtomicUsize::new(0));
        let a = Arc::clone(&acked);
        tokio::spawn(async move {
            loop {
                match el.poll().await {
                    Ok(rumqttc::Event::Incoming(Packet::PubAck(_))) => {
                        a.fetch_add(1, Ordering::AcqRel);
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        });
        Seeder {
            client,
            sent: 0,
            acked,
        }
    }

    /// Publishes `payload` on `topic` at QoS 1.
    pub async fn publish(&mut self, topic: &str, payload: &[u8], retain: bool) {
        self.client
            .publish(topic, QoS::AtLeastOnce, retain, payload.to_vec())
            .await
            .expect("seeder publish");
        self.sent += 1;
    }

    /// A retained device: `$description` from `desc`, every `values` entry as
    /// `node/property`, then `$state` `ready`.
    pub async fn device(&mut self, id: &str, desc: &str, values: &[(String, String)]) {
        self.publish(&format!("ebus/5/{id}/$description"), desc.as_bytes(), true)
            .await;
        for (np, v) in values {
            self.publish(&format!("ebus/5/{id}/{np}"), v.as_bytes(), true)
                .await;
        }
        self.publish(&format!("ebus/5/{id}/$state"), b"ready", true)
            .await;
    }

    /// Waits until every publish so far is acknowledged.
    pub async fn flush(&self) {
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

/// A `$description` with one node `n` of `props` integer properties
/// `p000`..; `extra` is spliced into the top-level object.
pub fn int_description(props: usize, extra: &str) -> String {
    let p: Vec<String> = (0..props)
        .map(|i| format!(r#""p{i:03}":{{"datatype":"integer"}}"#))
        .collect();
    format!(
        r#"{{"homie":"5.0","version":1{extra},"nodes":{{"n":{{"properties":{{{}}}}}}}}}"#,
        p.join(",")
    )
}

/// The values for [`int_description`]: `n/p000`.. set to `base + i`.
pub fn int_values(props: usize, base: usize) -> Vec<(String, String)> {
    (0..props)
        .map(|i| (format!("n/p{i:03}"), (base + i).to_string()))
        .collect()
}

/// A rumqttc client that forwards every message on `filters` as
/// `(topic, payload, retain)`.
pub async fn observe(
    port: u16,
    id: &str,
    filters: &[&str],
) -> mpsc::UnboundedReceiver<(String, Vec<u8>, bool)> {
    let (client, mut el) = AsyncClient::new(MqttOptions::new(id, "127.0.0.1", port), 64);
    let (tx, rx) = mpsc::unbounded_channel();
    let (sub_tx, sub_rx) = tokio::sync::oneshot::channel();
    let n = filters.len();
    let keep = client.clone();
    tokio::spawn(async move {
        // The event loop stops once every client handle is dropped.
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
            .subscribe(*f, QoS::AtLeastOnce)
            .await
            .expect("observer subscribe");
    }
    tokio::time::timeout(Duration::from_secs(5), sub_rx)
        .await
        .expect("observer subscribed in time")
        .expect("observer running");
    rx
}

/// Receives from an [`observe`] receiver until a message on `topic` with
/// `payload` arrives, within `secs` seconds; returns every message before it.
pub async fn until_message(
    rx: &mut mpsc::UnboundedReceiver<(String, Vec<u8>, bool)>,
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
                    "timed out waiting for {topic} = {:?}",
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

/// Every retained message under `filter`, as the broker holds it now: a
/// fresh client subscribes, then publishes a marker to itself, and collects
/// until the marker arrives, which the broker queues after the retained
/// messages.
pub async fn retained(port: u16, filter: &str) -> BTreeMap<String, Vec<u8>> {
    static N: AtomicUsize = AtomicUsize::new(0);
    let n = N.fetch_add(1, Ordering::AcqRel);
    let id = format!("snapshot-{}-{n}", std::process::id());
    let marker = format!("ebus-test/{id}");
    let mut mqtt = MqttOptions::new(&id, "127.0.0.1", port);
    mqtt.set_max_packet_size(1 << 20, 1 << 20);
    let (client, mut el) = AsyncClient::new(mqtt, 64);
    client
        .subscribe(filter, QoS::AtMostOnce)
        .await
        .expect("snapshot subscribe");
    client
        .subscribe(&marker, QoS::AtMostOnce)
        .await
        .expect("snapshot subscribe");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut subacks = 0;
    let mut out = BTreeMap::new();
    loop {
        let ev = timeout_at(deadline, el.poll())
            .await
            .expect("snapshot complete in time")
            .expect("snapshot connection");
        match ev {
            rumqttc::Event::Incoming(Packet::SubAck(_)) => {
                subacks += 1;
                if subacks == 2 {
                    client
                        .publish(&marker, QoS::AtMostOnce, false, "end")
                        .await
                        .expect("snapshot marker");
                }
            }
            rumqttc::Event::Incoming(Packet::Publish(p)) => {
                if p.topic == marker {
                    break;
                }
                assert!(p.retain, "a live message during the snapshot: {}", p.topic);
                out.insert(p.topic, p.payload.to_vec());
            }
            _ => {}
        }
    }
    let _ = client.disconnect().await;
    out
}

/// A bundled controller on the broker at `port`, spawned.
pub fn controller(port: u16, config: Config) -> Ctl {
    let mut mqtt = MqttOptions::new(ebus_rumqttc::client_id(), "127.0.0.1", port);
    mqtt.set_max_packet_size(1 << 20, 1 << 20);
    let run =
        ebus_rumqttc::controller(mqtt, config, AttachOptions::default()).expect("valid config");
    let task = tokio::spawn(run.task.run());
    Ctl {
        handle: run.handle,
        events: run.events,
        task,
        seen: Vec::new(),
    }
}

/// A running bundled controller and every event it delivered so far.
pub struct Ctl {
    /// Snapshot reads and `/set`.
    pub handle: ControllerHandle,
    events: EventReceiver,
    task: JoinHandle<Result<(), ebus_rumqttc::Error>>,
    /// Events received, in order.
    pub seen: Vec<Event>,
}

impl Ctl {
    /// A controller whose task the caller already spawned, such as a pair's.
    pub fn attached(
        handle: ControllerHandle,
        events: EventReceiver,
        task: JoinHandle<Result<(), ebus_rumqttc::Error>>,
    ) -> Ctl {
        Ctl {
            handle,
            events,
            task,
            seen: Vec::new(),
        }
    }

    /// Receives events until `done` holds for one, within `secs` seconds;
    /// returns that event. An `Overflow` fails the test.
    pub async fn until(
        &mut self,
        secs: u64,
        what: &str,
        mut done: impl FnMut(&Event) -> bool,
    ) -> Event {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let d = timeout_at(deadline, self.events.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for {what}; seen {:#?}", self.tail()))
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

    /// Waits until each of `devices` delivered `DeviceSynced { missing: 0 }`,
    /// counting only events after this call.
    pub async fn synced(&mut self, secs: u64, devices: &[&str]) {
        let mut left: Vec<String> = devices.iter().map(|d| (*d).to_owned()).collect();
        while !left.is_empty() {
            let e = self
                .until(secs, &format!("DeviceSynced for {left:?}"), |e| {
                    matches!(e, Event::DeviceSynced { device, .. } if left.iter().any(|l| l == device.as_str()))
                })
                .await;
            if let Event::DeviceSynced { device, missing } = e {
                assert_eq!(
                    missing,
                    0,
                    "{device} synced with values missing; its events {:#?}",
                    self.events_of(device.as_str())
                );
                left.retain(|l| l != device.as_str());
            }
        }
    }

    /// The raw value of `device/node/property` in the snapshot.
    pub fn value(&self, device: &str, node: &str, property: &str) -> Option<String> {
        self.handle
            .device(device)?
            .property(node, property)?
            .value
            .as_ref()
            .map(|v| v.raw.to_string())
    }

    /// True when an event already received matches `pred`.
    pub fn saw(&self, pred: impl FnMut(&Event) -> bool) -> bool {
        self.seen.iter().any(pred)
    }

    /// Waits until `cond` holds for the snapshot, within `secs` seconds.
    pub async fn eventually(
        &self,
        secs: u64,
        what: &str,
        cond: impl Fn(&ControllerHandle) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while !cond(&self.handle) {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn events_of(&self, id: &str) -> Vec<&Event> {
        self.seen
            .iter()
            .filter(|e| format!("{e:?}").contains(&format!("{id:?}")))
            .collect()
    }

    fn tail(&self) -> &[Event] {
        &self.seen[self.seen.len().saturating_sub(20)..]
    }

    /// Waits for the task without shutting the controller down.
    pub async fn join(self) -> Result<(), ebus_rumqttc::Error> {
        self.task.await.expect("task did not panic")
    }

    /// Shuts the controller down and waits for its task.
    pub async fn shutdown(self) {
        self.handle.shutdown().await;
        let res = tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("task ends after shutdown")
            .expect("task did not panic");
        assert!(res.is_ok(), "{res:?}");
    }
}
