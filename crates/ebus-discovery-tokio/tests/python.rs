//! Interop with python-service-discovery (`support/py_discovery.py`): each
//! backend finds a python advertisement, and python's `find_broker` finds
//! each backend's advertisement as the endpoint the backend itself resolves.
//!
//! Skipped unless `EBUS_PYTHON` (else `python3`) imports
//! `ebus_service_discovery`, from `PYTHONPATH` or a sibling
//! `python-service-discovery` checkout. mdns-sd and python use the loopback
//! interface and are skipped when two mdns-sd daemons cannot hear each other
//! there; the Avahi tests use the interfaces avahi-daemon uses and are
//! skipped without it. With `EBUS_REQUIRE_E2E` set, each of those fails the
//! test instead.
#![cfg(any(feature = "mdns-sd", all(feature = "avahi", target_os = "linux")))]

mod support;

use std::ffi::OsString;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::OnceLock;
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ebus_discovery_tokio::ebus_discovery::{
    Advertisement, BrokerConfig, BrokerEndpoint, BrokerService, BrokerType, Identity, Wanted,
};
use ebus_discovery_tokio::{Backend, find_broker};
use tokio::sync::Mutex;
use tokio::time::timeout;

static SERIAL: Mutex<()> = Mutex::const_new(());

fn run_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        format!("{:x}{:x}", std::process::id(), nanos)
    })
}

/// The interpreter and a `PYTHONPATH` that imports python-service-discovery,
/// or `None` (with a message).
fn python_env() -> Option<&'static (OsString, OsString)> {
    static ENV: OnceLock<Option<(OsString, OsString)>> = OnceLock::new();
    ENV.get_or_init(|| {
        let python = std::env::var_os("EBUS_PYTHON").unwrap_or_else(|| "python3".into());
        let mut path: Vec<PathBuf> = std::env::var_os("PYTHONPATH")
            .map(|p| std::env::split_paths(&p).collect())
            .unwrap_or_default();
        let sibling =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../python-service-discovery/src");
        if sibling.is_dir() {
            path.push(sibling);
        }
        let path = std::env::join_paths(path).expect("PYTHONPATH");
        let probe = Command::new(&python)
            .args(["-c", "import ebus_service_discovery.mdns"])
            .env("PYTHONPATH", &path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if !probe.is_ok_and(|s| s.success()) {
            support::skip(
                "python-service-discovery not importable (set EBUS_PYTHON and PYTHONPATH)",
            );
            return None;
        }
        Some((python, path))
    })
    .as_ref()
}

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/support/py_discovery.py")
}

/// A python `Advertiser`; withdrawn when dropped.
struct PyAdvertiser {
    child: Child,
    stdin: Option<ChildStdin>,
    instance: String,
    server: String,
}

impl PyAdvertiser {
    /// Advertises `device`'s broker of type `ty` on `port` through `iface`,
    /// under `server` with `address` (`-` for python's choice).
    fn start(
        iface: &str,
        server: &str,
        address: &str,
        ty: BrokerType,
        port: u16,
        device: &str,
    ) -> Self {
        let (python, path) = python_env().expect("checked");
        let mut child = Command::new(python)
            .arg("-u")
            .arg(script())
            .args(["advertise", iface, server, address, ty.as_str()])
            .arg(port.to_string())
            .arg(device)
            .env("PYTHONPATH", path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start python advertiser");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let line = tokio::task::block_in_place(|| rx.recv_timeout(Duration::from_secs(20)))
            .expect("python advertiser ready");
        let mut words = line.split(' ');
        assert_eq!(words.next(), Some("ready"), "{line}");
        let instance = words.next().expect("instance").to_owned();
        let server = words.next().expect("server").to_owned();
        PyAdvertiser {
            child,
            stdin,
            instance,
            server,
        }
    }
}

impl Drop for PyAdvertiser {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let done = (0..100).any(|_| {
            std::thread::sleep(Duration::from_millis(50));
            matches!(self.child.try_wait(), Ok(Some(_)))
        });
        if !done {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// python's `find_broker` in `discovery-with-fallback` with `url` configured,
/// through `iface`: the endpoint's fields as JSON.
async fn py_find(iface: &str, url: &str) -> serde_json::Value {
    let (python, path) = python_env().expect("checked");
    let out = tokio::process::Command::new(python)
        .arg(script())
        .args(["find", iface, url])
        .env("PYTHONPATH", path)
        .stderr(Stdio::inherit())
        .output();
    let out = timeout(Duration::from_secs(40), out)
        .await
        .expect("python find_broker returns")
        .expect("run python");
    assert!(out.status.success(), "python find_broker: {:?}", out.status);
    serde_json::from_slice(&out.stdout).expect("JSON endpoint")
}

/// The broker advertised as `instance`, found through `b`.
async fn found<B: Backend>(b: &B, instance: &str) -> BrokerEndpoint {
    let mut cfg = BrokerConfig::default();
    cfg.wanted = Some(Wanted::Instance(instance.into()));
    timeout(Duration::from_secs(20), find_broker(b, cfg))
        .await
        .unwrap_or_else(|_| panic!("{instance} found"))
        .unwrap()
}

/// Asserts that python's endpoint is `e`. python's IPv4 candidates, from
/// the interfaces it browsed, are among `e`'s, and with `addressed` there is
/// at least one.
fn assert_same(py: &serde_json::Value, e: &BrokerEndpoint, addressed: bool) {
    assert_eq!(py["service_type"], e.broker_type.as_str(), "{py}");
    assert_eq!(py["host"], e.host.as_str(), "{py}");
    assert_eq!(py["port"], e.port, "{py}");
    assert_eq!(py["server"].as_str(), e.server.as_deref(), "{py}");
    assert_eq!(py["instance_name"].as_str(), e.instance.as_deref(), "{py}");
    assert_eq!(
        py["device_id"].as_str().map(str::to_owned),
        e.device_ids().first().cloned(),
        "{py}"
    );
    assert_eq!(py["url"], e.url().as_str(), "{py}");
    let ours: Vec<String> = e
        .addrs
        .iter()
        .filter(|a| a.ip.is_ipv4() && a.is_candidate())
        .map(|a| a.ip.to_string())
        .collect();
    let addrs = py["addresses"].as_array().expect("addresses");
    assert_eq!(!addrs.is_empty(), addressed, "{py}; ours {ours:?}");
    for a in addrs {
        let a = a.as_str().expect("address");
        assert!(ours.iter().any(|o| o == a), "{py}; ours {ours:?}");
    }
}

/// A `_secure-mqtt._tcp` broker advertisement for `device`.
fn broker_ad(device: &str, port: u16) -> Advertisement {
    Advertisement::new(Identity::new(
        [device],
        ["broker-host"],
        "Acme",
        "P1",
        "SN1",
    ))
    .broker(BrokerService::new(BrokerType::SecureMqtt, port))
}

#[cfg(feature = "mdns-sd")]
mod with_mdns_sd {
    use std::net::IpAddr;

    use ebus_discovery_tokio::{AdState, HostName, Mdns, MdnsSdOptions, advertise};
    use mdns_sd::{IfKind, IfPredicate, ServiceDaemon, ServiceEvent, ServiceInfo};

    use super::*;

    /// The interface python uses: loopback's IPv4 address.
    const PY_IFACE: &str = "127.0.0.1";

    fn mdns() -> Mdns {
        let mut opts = MdnsSdOptions::default();
        opts.loopback = true;
        Mdns::mdns_sd(opts).unwrap()
    }

    fn raw_daemon() -> ServiceDaemon {
        let d = ServiceDaemon::new().unwrap();
        d.disable_interface(IfKind::All).unwrap();
        d.enable_interface(IfKind::Predicate(IfPredicate::new(|i| {
            i.ip().is_loopback() || i.name == "lo" || i.name == "lo0"
        })))
        .unwrap();
        d
    }

    /// True when one daemon resolves another's service over loopback.
    fn loopback_multicast() -> bool {
        static OK: OnceLock<bool> = OnceLock::new();
        *OK.get_or_init(|| {
            let ty = "_ebus-probe._tcp.local.";
            let (a, b) = (raw_daemon(), raw_daemon());
            let host = format!("ebus-probe-{}.local.", run_id());
            let info = ServiceInfo::new(ty, &format!("probe-{}", run_id()), &host, (), 9, None)
                .unwrap()
                .enable_addr_auto();
            a.register(info).unwrap();
            let rx = b.browse(ty).unwrap();
            let mut ok = false;
            while let Ok(ev) = rx.recv_timeout(Duration::from_secs(4)) {
                if matches!(ev, ServiceEvent::ServiceResolved(_)) {
                    ok = true;
                    break;
                }
            }
            let _ = a.shutdown();
            let _ = b.shutdown();
            if !ok {
                support::skip("two mdns-sd daemons do not hear each other over loopback");
            }
            ok
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mdns_sd_finds_a_python_advertisement() {
        let _serial = SERIAL.lock().await;
        if python_env().is_none() || !loopback_multicast() {
            return;
        }
        let id = run_id();
        let device = format!("py-ms-{id}");
        let server = format!("ebus-test-py-{id}.local.");
        let py = PyAdvertiser::start(
            PY_IFACE,
            &server,
            "127.0.0.1",
            BrokerType::SecureMqtt,
            18911,
            &device,
        );
        assert_eq!(py.instance, device);
        assert_eq!(
            py.server.trim_end_matches('.'),
            server.trim_end_matches('.')
        );
        let e = found(&mdns(), &py.instance).await;
        assert_eq!(e.broker_type, BrokerType::SecureMqtt);
        assert_eq!(e.host, server.trim_end_matches('.'));
        assert_eq!(e.server.as_deref(), Some(server.trim_end_matches('.')));
        assert_eq!(e.port, 18911);
        assert_eq!(e.device_ids(), std::slice::from_ref(&device));
        let v4: Vec<IpAddr> = e
            .addrs
            .iter()
            .map(|a| a.ip)
            .filter(IpAddr::is_ipv4)
            .collect();
        assert!(v4.is_empty(), "loopback is never a candidate: {v4:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn python_finds_an_mdns_sd_advertisement() {
        let _serial = SERIAL.lock().await;
        if python_env().is_none() || !loopback_multicast() {
            return;
        }
        let id = run_id();
        let device = format!("rs-ms-{id}");
        let host = format!("ebus-test-rs-{id}");
        let adv = advertise(
            &mdns(),
            broker_ad(&device, 18912),
            HostName::Own(format!("{host}.local.")),
        )
        .await
        .unwrap();
        let mut state = adv.state();
        timeout(
            Duration::from_secs(10),
            state.wait_for(|s| matches!(s, AdState::Established { .. })),
        )
        .await
        .expect("established")
        .unwrap();
        let ours = found(&mdns(), &device).await;
        let py = py_find(PY_IFACE, &format!("mqtts://{host}.local:1")).await;
        assert_same(&py, &ours, false);
        adv.withdraw().await.unwrap();
    }
}

#[cfg(all(feature = "avahi", target_os = "linux"))]
mod with_avahi {
    use ebus_discovery_tokio::{AdState, Error, HostName, Mdns, advertise};

    use super::*;

    /// The interfaces python uses: one per IPv4 subnet, loopback left out,
    /// where avahi-daemon also answers.
    const PY_IFACE: &str = "one-per-subnet";

    async fn avahi() -> Option<Mdns> {
        match Mdns::avahi().await {
            Ok(m) => Some(m),
            Err(e @ (Error::NoSystemBus | Error::AvahiUnavailable)) => {
                support::skip(&e.to_string());
                None
            }
            Err(e) => panic!("{e}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn avahi_finds_a_python_advertisement() {
        let _serial = SERIAL.lock().await;
        if python_env().is_none() {
            return;
        }
        let Some(mdns) = avahi().await else { return };
        let id = run_id();
        let device = format!("py-av-{id}");
        // Under avahi-daemon's host name, without address records, as python
        // advertises on a host with an OS responder.
        let py = PyAdvertiser::start(PY_IFACE, "-", "-", BrokerType::SecureMqtt, 18913, &device);
        let avahi_server = mdns.server_name(&HostName::Auto).await.unwrap();
        assert_eq!(
            py.server.trim_end_matches('.'),
            avahi_server.trim_end_matches('.')
        );
        let e = found(&mdns, &py.instance).await;
        assert_eq!(e.broker_type, BrokerType::SecureMqtt);
        let server = py.server.trim_end_matches('.');
        assert_eq!(e.host, server);
        assert_eq!(e.server.as_deref(), Some(server));
        assert_eq!(e.port, 18913);
        assert_eq!(e.device_ids(), std::slice::from_ref(&device));
        assert!(
            e.addrs.iter().any(|a| a.ip.is_ipv4() && a.is_candidate()),
            "avahi-daemon resolves its own name: {:?}",
            e.addrs
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn python_finds_an_avahi_advertisement() {
        let _serial = SERIAL.lock().await;
        if python_env().is_none() {
            return;
        }
        let Some(mdns) = avahi().await else { return };
        let device = format!("rs-av-{}", run_id());
        let adv = advertise(&mdns, broker_ad(&device, 18914), HostName::Auto)
            .await
            .unwrap();
        let mut state = adv.state();
        timeout(
            Duration::from_secs(20),
            state.wait_for(|s| matches!(s, AdState::Established { .. })),
        )
        .await
        .expect("established")
        .unwrap();
        let ours = found(&mdns, &device).await;
        let py = py_find(PY_IFACE, &format!("mqtts://{}:1", ours.host)).await;
        assert_same(&py, &ours, true);
        adv.withdraw().await.unwrap();
    }
}
