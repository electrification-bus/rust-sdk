//! mdns-sd to mdns-sd, two or more daemons in one process.
//!
//! The daemons use port 5353 on the loopback interfaces alone: on any other
//! port mdns-sd answers queries and probes by unicast (service_daemon.rs:3672),
//! which reaches only one of the daemons sharing the port. Every name carries
//! a per-run suffix, and the tests run one at a time. They are skipped when
//! two daemons cannot hear each other over loopback multicast, and fail then
//! with `EBUS_REQUIRE_E2E` set.
#![cfg(feature = "mdns-sd")]

mod support;

use std::net::IpAddr;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ebus_discovery_tokio::ebus_discovery::{
    Advertisement, BrokerConfig, BrokerService, BrokerType, Identity, ScopedAddr, Sighting, Wanted,
};
use ebus_discovery_tokio::{
    AdState, Advertiser, Backend, Browse, HostName, Mdns, MdnsSdOptions, advertise, find_broker,
    list_brokers, reresolve,
};
use mdns_sd::{IfKind, IfPredicate, ServiceDaemon, ServiceEvent, ServiceInfo};
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

fn is_loopback_interface(name: &str, ip: IpAddr) -> bool {
    ip.is_loopback() || name == "lo" || name == "lo0"
}

/// A bare mdns-sd daemon on the loopback interfaces, standing in for
/// another host's responder.
fn raw_daemon() -> ServiceDaemon {
    let d = ServiceDaemon::new().unwrap();
    d.disable_interface(IfKind::All).unwrap();
    d.enable_interface(IfKind::Predicate(IfPredicate::new(|i| {
        is_loopback_interface(&i.name, i.ip())
    })))
    .unwrap();
    d
}

fn mdns() -> Mdns {
    let mut opts = MdnsSdOptions::default();
    opts.loopback = true;
    Mdns::mdns_sd(opts).unwrap()
}

/// True when one daemon resolves another's service over loopback.
fn loopback_multicast() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| {
        let ty = "_ebus-probe._tcp.local.";
        let a = raw_daemon();
        let b = raw_daemon();
        let info = ServiceInfo::new(
            ty,
            &format!("probe-{}", run_id()),
            &format!("ebus-probe-{}.local.", run_id()),
            (),
            9,
            None,
        )
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

fn identity(device: &str) -> Identity {
    Identity::new([device], ["device", "broker-host"], "Acme", "P1", "SN1")
}

fn broker_ad(device: &str, ty: BrokerType, port: u16) -> Advertisement {
    Advertisement::new(identity(device)).broker(BrokerService::new(ty, port))
}

fn own(host: &str) -> HostName {
    HostName::Own(format!("{host}.local."))
}

async fn established(adv: &Advertiser, instance: &str) -> String {
    let mut state = adv.state();
    let s = timeout(
        Duration::from_secs(10),
        state.wait_for(|s| matches!(s, AdState::Established { instance: i, .. } if i == instance)),
    )
    .await
    .unwrap_or_else(|_| panic!("{instance} established; state {:?}", *adv.state().borrow()))
    .unwrap();
    match &*s {
        AdState::Established { server, .. } => server.clone(),
        other => unreachable!("{other:?}"),
    }
}

/// The next sighting of `name` on `browse` satisfying `want`.
async fn sighting<B: Browse>(
    browse: &mut B,
    name: &str,
    want: impl Fn(&Sighting) -> bool,
) -> Sighting {
    timeout(Duration::from_secs(10), async {
        loop {
            let s = browse.next().await.expect("browse running");
            let key = match &s {
                Sighting::Seen { key, .. } | Sighting::Gone { key, .. } => key,
            };
            if key.name == name && want(&s) {
                return s;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("sighting of {name}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn list_and_find_brokers() {
    let _serial = SERIAL.lock().await;
    if !loopback_multicast() {
        return;
    }
    let id = run_id();
    let (a, b) = (format!("lf-a-{id}"), format!("lf-b-{id}"));
    let host_a = format!("ebus-test-lfa-{id}");
    let host_b = format!("ebus-test-lfb-{id}");
    let adv_a = advertise(
        &mdns(),
        broker_ad(&a, BrokerType::Mqtt, 18831),
        own(&host_a),
    )
    .await
    .unwrap();
    // b serves TLS and plain MQTT; only its _secure-mqtt._tcp TXT carries
    // device_id.
    let adv_b = advertise(
        &mdns(),
        broker_ad(&b, BrokerType::SecureMqtt, 18832)
            .broker(BrokerService::new(BrokerType::Mqtt, 18830)),
        own(&host_b),
    )
    .await
    .unwrap();
    established(&adv_a, &a).await;
    established(&adv_b, &b).await;

    let client = mdns();
    let all = list_brokers(&client, &BrokerConfig::default(), Duration::from_secs(4))
        .await
        .unwrap();
    let ours: Vec<(BrokerType, String, u16)> = all
        .iter()
        .filter(|e| e.instance.as_deref().is_some_and(|i| i.ends_with(id)))
        .map(|e| (e.broker_type, e.host.clone(), e.port))
        .collect();
    assert_eq!(
        ours,
        [
            (BrokerType::SecureMqtt, format!("{host_b}.local"), 18832),
            (BrokerType::Mqtt, format!("{host_a}.local"), 18831),
        ]
    );

    let mut cfg = BrokerConfig::default();
    cfg.wanted = Some(Wanted::Instance(a.clone()));
    let found = timeout(Duration::from_secs(15), find_broker(&client, cfg))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.instance.as_deref(), Some(a.as_str()));
    assert_eq!(
        found.server.as_deref(),
        Some(format!("{host_a}.local").as_str())
    );
    assert_eq!(found.port, 18831);

    let mut cfg = BrokerConfig::default();
    cfg.wanted = Some(Wanted::DeviceId(b.clone()));
    let found = timeout(Duration::from_secs(15), find_broker(&client, cfg))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.instance.as_deref(), Some(b.as_str()));
    assert_eq!(found.broker_type, BrokerType::SecureMqtt);
    assert_eq!(found.device_ids(), std::slice::from_ref(&b));

    let mut cfg = BrokerConfig::default();
    cfg.wanted = Some(Wanted::DeviceId(b.clone()));
    cfg.accept = Some(vec![BrokerType::Mqtt]);
    let found = timeout(Duration::from_secs(15), find_broker(&client, cfg))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (found.broker_type, found.instance.as_deref(), found.port),
        (BrokerType::Mqtt, Some(b.as_str()), 18830)
    );

    adv_a.withdraw().await.unwrap();
    adv_b.withdraw().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn txt_update_and_withdraw() {
    let _serial = SERIAL.lock().await;
    if !loopback_multicast() {
        return;
    }
    let id = run_id();
    let name = format!("tu-{id}");
    let host = format!("ebus-test-tu-{id}");
    let ad = broker_ad(&name, BrokerType::Mqtt, 18833);
    let adv = advertise(&mdns(), ad.clone(), own(&host)).await.unwrap();
    established(&adv, &name).await;

    let client = mdns();
    let mut browse = client.browse("_mqtt._tcp").await.unwrap();
    let first = sighting(&mut browse, &name, |s| matches!(s, Sighting::Seen { .. })).await;
    let Sighting::Seen { txt, server, .. } = first else {
        unreachable!()
    };
    assert_eq!(server, format!("{host}.local."));
    assert_eq!(txt.get("k"), None);

    let mut changed = ad.clone();
    changed.brokers[0].extra_txt.push(("k".into(), "v".into()));
    adv.update(changed).unwrap();
    let updated = sighting(
        &mut browse,
        &name,
        |s| matches!(s, Sighting::Seen { txt, .. } if txt.get("k").as_deref() == Some("v")),
    )
    .await;
    let Sighting::Seen { port, .. } = updated else {
        unreachable!()
    };
    assert_eq!(port, 18833);

    adv.withdraw().await.unwrap();
    sighting(&mut browse, &name, |s| matches!(s, Sighting::Gone { .. })).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn instance_name_collision_renames_every_record() {
    let _serial = SERIAL.lock().await;
    if !loopback_multicast() {
        return;
    }
    let id = run_id();
    let name = format!("col-{id}");
    let first = advertise(
        &mdns(),
        broker_ad("dev-1", BrokerType::Mqtt, 18834).instance(&name),
        own(&format!("ebus-test-c1-{id}")),
    )
    .await
    .unwrap();
    established(&first, &name).await;
    // Let its announcements finish so the second daemon's probe is answered.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let second_host = format!("ebus-test-c2-{id}");
    let second = advertise(
        &mdns(),
        broker_ad("dev-2", BrokerType::Mqtt, 18835).instance(&name),
        own(&second_host),
    )
    .await
    .unwrap();
    // Every state the second advertiser passes through: none is
    // `Established` under the name the first holds.
    let mut states = second.state();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(vec![
        states.borrow_and_update().clone(),
    ]));
    let log = seen.clone();
    let watcher = tokio::spawn(async move {
        while states.changed().await.is_ok() {
            let s = states.borrow_and_update().clone();
            log.lock().unwrap().push(s);
        }
    });
    let renamed = format!("{name}-2");
    established(&second, &renamed).await;
    watcher.abort();
    let seen = seen.lock().unwrap().clone();
    assert!(
        !seen
            .iter()
            .any(|s| matches!(s, AdState::Established { instance, .. } if *instance == name)),
        "{seen:?}"
    );
    // A browse started while the renamed records are still being announced
    // can see one only as found, not resolved, for many seconds (seen with
    // bare mdns-sd daemons too); let the announcements finish first.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let client = mdns();
    for ty in ["_ebus._tcp", "_device-info._tcp", "_mqtt._tcp"] {
        let mut browse = client.browse(ty).await.unwrap();
        let s = sighting(&mut browse, &renamed, |s| {
            matches!(s, Sighting::Seen { .. })
        })
        .await;
        let Sighting::Seen { server, .. } = s else {
            unreachable!()
        };
        assert_eq!(server, format!("{second_host}.local."), "{ty}");
    }
    second.withdraw().await.unwrap();
    first.withdraw().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn host_name_collision_rebuilds_the_broker_txt() {
    let _serial = SERIAL.lock().await;
    if !loopback_multicast() {
        return;
    }
    let id = run_id();
    let host = format!("ebus-test-hc-{id}");
    // Another host already answers for the name, at another address.
    let other = raw_daemon();
    let holder = ServiceInfo::new(
        "_ebus-hold._tcp.local.",
        &format!("hold-{id}"),
        &format!("{host}.local."),
        "127.0.0.2",
        9,
        None,
    )
    .unwrap();
    other.register(holder).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let name = format!("hc-{id}");
    let adv = advertise(
        &mdns(),
        broker_ad(&name, BrokerType::SecureMqtt, 18836),
        own(&host),
    )
    .await
    .unwrap();
    let mut state = adv.state();
    let renamed = format!("{host}-2.local.");
    timeout(
        Duration::from_secs(10),
        state.wait_for(|s| matches!(s, AdState::Established { server, .. } if *server == renamed)),
    )
    .await
    .unwrap_or_else(|_| panic!("renamed host; state {:?}", *adv.state().borrow()))
    .unwrap();

    let client = mdns();
    let mut browse = client.browse("_secure-mqtt._tcp").await.unwrap();
    // A resolve racing the in-place registration can lack the TXT for a
    // moment, so wait for the final record.
    let broker = renamed.trim_end_matches('.');
    sighting(&mut browse, &name, |s| {
        matches!(s, Sighting::Seen { server, txt, .. }
            if *server == renamed && txt.get("broker").as_deref() == Some(broker))
    })
    .await;
    // Once the probes have ended, the record still carries its own name:
    // registering it again while it probed made the daemon rename it
    // `<name> (N)`.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut browse = client.browse("_secure-mqtt._tcp").await.unwrap();
    let mut names = Vec::new();
    let _ = timeout(Duration::from_secs(3), async {
        while let Some(Sighting::Seen { key, .. }) = browse.next().await {
            names.push(key.name);
        }
    })
    .await;
    assert!(
        !names.iter().any(|n| n.starts_with(&format!("{name} ("))),
        "{names:?}"
    );
    assert!(names.contains(&name), "{names:?}");
    adv.withdraw().await.unwrap();
    let _ = other.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn reresolve_after_an_address_change() {
    let _serial = SERIAL.lock().await;
    if !loopback_multicast() {
        return;
    }
    // A candidate address on loopback: the IPv6 link-local subnet macOS
    // gives lo0. Elsewhere loopback carries only loopback addresses, which
    // are never candidates.
    if !cfg!(target_os = "macos") {
        eprintln!("skipping: no link-local subnet on the loopback interface");
        return;
    }
    let id = run_id();
    let name = format!("rr-{id}");
    let host = format!("ebus-test-rr-{id}.local.");
    let broker_host = raw_daemon();
    let at = |addr: &str| {
        ServiceInfo::new("_mqtt._tcp.local.", &name, &host, addr, 18837, None).unwrap()
    };
    broker_host.register(at("fe80::2")).unwrap();

    let client = mdns();
    let mut cfg = BrokerConfig::default();
    cfg.wanted = Some(Wanted::Instance(name.clone()));
    let found = timeout(Duration::from_secs(15), find_broker(&client, cfg.clone()))
        .await
        .unwrap()
        .unwrap();
    let v6 = |s: &str| s.parse::<IpAddr>().unwrap();
    assert_eq!(found.addrs.len(), 1);
    assert_eq!(found.addrs[0].ip, v6("fe80::2"));
    assert!(found.addrs[0].ifindex.is_some());

    broker_host.register(at("fe80::3")).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let again = reresolve(&client, &found, &cfg, Duration::from_secs(3))
        .await
        .unwrap()
        .expect("broker seen again");
    assert_eq!(
        again.addrs,
        [ScopedAddr {
            ip: v6("fe80::3"),
            ifindex: found.addrs[0].ifindex,
        }]
    );
    assert_eq!(again.instance, found.instance);
    let _ = broker_host.shutdown();
}
