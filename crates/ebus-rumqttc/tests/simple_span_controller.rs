//! The `simple-span-controller` example's SPAN helpers, whose unit tests in
//! `examples/common/span.rs` run in this binary, and its monitor end to end:
//! a masked panel-sim capture replayed into a local mosquitto over plain
//! TCP. With the `rustls` feature, the panel TLS configuration against a
//! mosquitto TLS listener on 127.0.0.1 whose certificate names the panel's
//! host. Skipped when no `mosquitto` binary is found (`MOSQUITTO` names one),
//! and the TLS test also without an `openssl` binary.

mod support;
// The example uses the parts these tests do not.
#[allow(dead_code)]
#[path = "../examples/common/span.rs"]
mod span;

use std::path::Path;
use std::time::Duration;

use ebus_rumqttc::ebus_homie_tokio::AttachOptions;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::Config;
use rumqttc::MqttOptions;
use serde_json::{Map, Value};
use span::Line;
use support::{Broker, Seeder};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, timeout_at};

/// A panel with a MID child.
const CAPTURE: &str = "main32_r202639-tree-v1.json";

fn capture() -> Map<String, Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/panel-sim")
        .join(CAPTURE);
    let text = std::fs::read_to_string(&path).expect("capture");
    let v: Value = serde_json::from_str(&text).expect("capture JSON");
    v["devices"].as_object().expect("devices").clone()
}

/// Every captured value of `device` as `node/property` and payload.
fn values(device: &Value) -> Vec<(String, String)> {
    ["properties", "numeric_properties"]
        .iter()
        .flat_map(|k| device[*k].as_object().into_iter().flatten())
        .map(|(np, v)| (np.clone(), v.as_str().expect("payload text").to_owned()))
        .collect()
}

/// The lines the monitor reports.
struct Lines {
    rx: mpsc::UnboundedReceiver<Line>,
    seen: Vec<Line>,
}

impl Lines {
    /// Receives lines until `done` holds for one, within `secs` seconds;
    /// returns the lines received meanwhile, that one last.
    async fn until(&mut self, secs: u64, what: &str, done: impl Fn(&Line) -> bool) -> Vec<Line> {
        let deadline = Instant::now() + Duration::from_secs(secs);
        let mut got = Vec::new();
        loop {
            let l = timeout_at(deadline, self.rx.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for {what}; seen {:#?}", self.seen))
                .expect("monitor running");
            self.seen.push(l.clone());
            let stop = done(&l);
            got.push(l);
            if stop {
                return got;
            }
        }
    }
}

fn info(s: &str) -> Line {
    Line::Info(s.to_owned())
}

fn is_info(l: &Line, s: &str) -> bool {
    *l == info(s)
}

#[tokio::test]
async fn monitor_reports_a_replayed_panel() {
    let test = "monitor_reports_a_replayed_panel";
    let Some(broker) = Broker::start(test, "") else {
        return;
    };
    let devices = capture();
    let root = devices
        .iter()
        .find(|(_, d)| d["description"].get("parent").is_none())
        .map(|(id, _)| id.clone())
        .expect("a root");
    let mid = devices
        .iter()
        .find(|(_, d)| d["description"]["type"] == span::MID_TYPE)
        .map(|(id, _)| id.clone())
        .expect("a MID");

    let mut mqtt = MqttOptions::new(ebus_rumqttc::client_id(), "127.0.0.1", broker.port);
    mqtt.set_max_packet_size(span::MAX_PACKET, span::MAX_PACKET);
    let run = ebus_rumqttc::controller(mqtt, Config::wildcard(), AttachOptions::default())
        .expect("valid config");
    let task = tokio::spawn(run.task.run());
    let (tx, rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let (handle, mut events) = (run.handle, run.events);
    let enclosure = root.clone();
    let monitor = tokio::spawn(async move {
        let stop = async {
            let _ = stop_rx.await;
        };
        let mut out = |l: Line| {
            let _ = tx.send(l);
        };
        span::monitor(
            &handle,
            &mut events,
            &enclosure,
            Duration::from_millis(200),
            stop,
            &mut out,
        )
        .await;
    });
    let mut lines = Lines {
        rx,
        seen: Vec::new(),
    };
    let waiting = "still waiting to discover the MID (islanding source)";
    lines
        .until(5, "the waiting line", |l| is_info(l, waiting))
        .await;

    let mut seeder = Seeder::new(broker.port, "span-capture");
    for (id, d) in &devices {
        let desc = serde_json::to_string(&d["description"]).expect("description");
        seeder.device(id, &desc, &values(d)).await;
    }
    seeder.flush().await;

    // Both watched nodes reported, each property with its datatype, and
    // each watched value shown in the report or, when the report came
    // first, as its initial value.
    let mut headers = Vec::new();
    // (the report line with the value, the one without, the initial value)
    let mut props: Vec<(String, String, String)> = Vec::new();
    for (device, node) in [(&root, span::ENCLOSURE_NODE), (&mid, span::MID_NODE)] {
        let declared = devices[device]["description"]["nodes"][node]["properties"]
            .as_object()
            .expect("watched node");
        headers.push(format!("{device}/{node} ({} properties):", declared.len()));
        let captured = values(&devices[device]);
        for (prop, p) in declared {
            let bare = format!(
                "  {node}/{prop} ({})",
                p["datatype"].as_str().expect("datatype")
            );
            let unit = p["unit"].as_str().unwrap_or("");
            let value = captured
                .iter()
                .find(|(np, _)| *np == format!("{node}/{prop}"))
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("{device}/{node}/{prop} has no captured value"));
            props.push((
                format!("{bare} = {value}{unit}"),
                bare,
                format!("initial value: {device}/{node}/{prop} = {value}"),
            ));
        }
    }
    let saw = |seen: &[Line], s: &str| seen.iter().any(|l| is_info(l, s));
    let complete = |seen: &[Line]| {
        headers.iter().all(|h| saw(seen, h))
            && props.iter().all(|(with_value, bare, initial)| {
                saw(seen, with_value) || (saw(seen, bare) && saw(seen, initial))
            })
    };
    let mid_line = format!("MID (islanding source) discovered: {mid}");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !(complete(&lines.seen) && saw(&lines.seen, &mid_line)) {
        let left = deadline
            .saturating_duration_since(Instant::now())
            .as_secs()
            .max(1);
        lines
            .until(left, "both watched nodes reported", |_| true)
            .await;
        assert!(Instant::now() < deadline, "seen {:#?}", lines.seen);
    }
    let after_mid = lines
        .seen
        .iter()
        .skip_while(|l| !is_info(l, &mid_line))
        .skip(1);
    assert!(
        !after_mid.clone().any(|l| is_info(l, waiting)),
        "{:#?}",
        lines.seen
    );
    assert!(
        !lines.seen.iter().any(|l| matches!(l, Line::Warn(_))),
        "{:#?}",
        lines.seen
    );
    // Discovering a device is not a state change.
    let root_state = format!("{root}: ");
    assert!(
        !lines
            .seen
            .iter()
            .any(|l| matches!(l, Line::Info(s) if s.starts_with(&root_state))),
        "{:#?}",
        lines.seen
    );

    // A changed watched value is reported with its old value.
    let islanding = format!("ebus/5/{mid}/grid/islanding-state");
    seeder.publish(&islanding, b"OFF_GRID", true).await;
    let changed = format!("{mid}/grid/islanding-state changed: ON_GRID -> OFF_GRID");
    let got = lines.until(5, &changed, |l| is_info(l, &changed)).await;
    assert_eq!(got, [info(&changed)]);

    // An unchanged watched value and an unwatched change report nothing:
    // the next line is the change published after them.
    let grid = format!("{root}/power-flows/grid");
    let grid_value = values(&devices[&root])
        .into_iter()
        .find(|(np, _)| np == "power-flows/grid")
        .map(|(_, v)| v)
        .expect("captured power-flows/grid");
    seeder
        .publish(&format!("ebus/5/{grid}"), grid_value.as_bytes(), true)
        .await;
    seeder
        .publish(&format!("ebus/5/{root}/door/state"), b"OPEN", true)
        .await;
    seeder.publish(&islanding, b"OFF_GRID", true).await;
    seeder
        .publish(&format!("ebus/5/{mid}/grid/grid-state"), b"UP", true)
        .await;
    let sentinel = format!("{mid}/grid/grid-state changed: UNKNOWN -> UP");
    let got = lines.until(5, &sentinel, |l| is_info(l, &sentinel)).await;
    assert_eq!(got, [info(&sentinel)]);

    // A watched device's state change.
    seeder
        .publish(&format!("ebus/5/{mid}/$state"), b"init", true)
        .await;
    let state = format!("{mid}: ready -> init");
    let got = lines.until(5, &state, |l| is_info(l, &state)).await;
    assert_eq!(got, [info(&state)]);
    seeder.flush().await;

    stop_tx.send(()).expect("monitor running");
    lines
        .until(5, "shutting down", |l| is_info(l, "shutting down"))
        .await;
    monitor.await.expect("monitor did not panic");
    let res = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("task ends after shutdown")
        .expect("task did not panic");
    assert!(res.is_ok(), "{res:?}");
}

/// Runs `openssl` with the space-separated `args` in `dir`; false when it
/// cannot be run or fails.
#[cfg(feature = "rustls")]
fn openssl(dir: &Path, args: &str) -> bool {
    std::process::Command::new("openssl")
        .args(args.split_whitespace())
        .current_dir(dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// A CA named `ca` in `dir` (`<ca>.pem`, `<ca>.key`).
#[cfg(feature = "rustls")]
fn make_ca(dir: &Path, ca: &str) -> bool {
    openssl(
        dir,
        &format!(
            "req -x509 -newkey rsa:2048 -nodes -keyout {ca}.key \
             -out {ca}.pem -days 2 -subj /CN={ca} -addext basicConstraints=critical,CA:TRUE \
             -addext keyUsage=critical,keyCertSign"
        ),
    )
}

/// Connects to the TLS listener at `port` with `tls`; true on a CONNACK.
#[cfg(feature = "rustls")]
async fn connects(
    port: u16,
    tls: std::sync::Arc<rumqttc::tokio_rustls::rustls::ClientConfig>,
) -> bool {
    let mut mqtt = MqttOptions::new(ebus_rumqttc::client_id(), "127.0.0.1", port);
    mqtt.set_transport(rumqttc::Transport::tls_with_config(
        rumqttc::TlsConfiguration::Rustls(tls),
    ));
    let (_client, mut el) = rumqttc::AsyncClient::new(mqtt, 10);
    let poll = async {
        loop {
            match el.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => return true,
                Ok(_) => {}
                Err(_) => return false,
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("connect settles")
}

/// The panel's certificate is verified under its host name while the
/// connection goes to an address: a broker on 127.0.0.1 whose certificate
/// names only `span-<serial>.local` is accepted under that name and refused
/// under another or with another CA; without a CA any certificate is
/// accepted.
#[cfg(feature = "rustls")]
#[tokio::test]
async fn tls_verifies_the_panel_name_at_an_address() {
    let test = "tls_verifies_the_panel_name_at_an_address";
    let host = span::panel_host("nt-0000-xxxxx");
    let dir = std::env::temp_dir().join(format!("ebus-span-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    std::fs::write(
        dir.join("server.ext"),
        format!(
            "subjectAltName=DNS:{host}\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n"
        ),
    )
    .expect("extensions");
    let made = make_ca(&dir, "panel-ca")
        && make_ca(&dir, "other-ca")
        && openssl(
            &dir,
            &format!(
                "req -newkey rsa:2048 -nodes -keyout server.key \
                 -out server.csr -subj /CN={host}"
            ),
        )
        && openssl(
            &dir,
            "x509 -req -in server.csr -CA panel-ca.pem -CAkey panel-ca.key -CAcreateserial \
             -out server.pem -days 2 -extfile server.ext",
        );
    if !made {
        let _ = std::fs::remove_dir_all(&dir);
        support::skip(test, "no openssl binary that makes the test certificates");
        return;
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("free port")
        .port();
    let file = |f: &str| dir.join(f).display().to_string();
    let extra = format!(
        "listener {port} 127.0.0.1\ncafile {}\ncertfile {}\nkeyfile {}\n",
        file("panel-ca.pem"),
        file("server.pem"),
        file("server.key")
    );
    let Some(_broker) = Broker::start(test, &extra) else {
        let _ = std::fs::remove_dir_all(&dir);
        return;
    };
    let panel_ca = std::fs::read(dir.join("panel-ca.pem")).expect("CA");
    let other_ca = std::fs::read(dir.join("other-ca.pem")).expect("CA");
    let config =
        |ca: Option<&[u8]>, name: &str| span::tls::client_config(ca, name).expect("config");

    // The plain listener is up; give the TLS one the same chance.
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "no TLS listener");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(connects(port, config(Some(&panel_ca), &host)).await);
    assert!(!connects(port, config(Some(&panel_ca), "span-nt-0000-yyyyy.local")).await);
    assert!(!connects(port, config(Some(&panel_ca), "127.0.0.1")).await);
    assert!(!connects(port, config(Some(&other_ca), &host)).await);
    assert!(connects(port, config(None, "span-nt-0000-yyyyy.local")).await);
    assert!(span::tls::client_config(Some(b"not a certificate"), &host).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
