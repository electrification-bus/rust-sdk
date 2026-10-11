//! The Avahi backend against a running avahi-daemon, and against mdns-sd on
//! the same host.
//!
//! Skipped when there is no system bus or no avahi-daemon on it. The
//! cross-backend tests run mdns-sd on the non-loopback interfaces, which
//! avahi-daemon also uses. `EBUS_AVAHI_STOP` and `EBUS_AVAHI_START` name
//! shell commands that stop avahi-daemon and start it again, each returning
//! once done; the restart test runs only with both. The host-rename
//! test runs only where `avahi-set-host-name` is allowed (root, or the
//! `netdev` group). With `EBUS_REQUIRE_E2E` set, a missing avahi-daemon or
//! restart command fails the test instead of skipping it.
#![cfg(all(feature = "avahi", target_os = "linux"))]

mod support;

use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ebus_discovery_tokio::ebus_discovery::{
    Advertisement, BrokerConfig, BrokerEndpoint, BrokerService, BrokerType, Identity, Sighting,
    Slot, TxtRecord, Wanted,
};
use ebus_discovery_tokio::{
    AdState, Advertiser, Backend, Browse, Error, HostName, Mdns, advertise, list_brokers,
};
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

/// The Avahi backend, or `None` (and a skip message) without avahi-daemon.
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

/// mdns-sd on the interfaces avahi-daemon uses.
#[cfg(feature = "mdns-sd")]
fn mdns_sd() -> Mdns {
    Mdns::mdns_sd(ebus_discovery_tokio::MdnsSdOptions::default()).unwrap()
}

fn broker_ad(device: &str, ty: BrokerType, port: u16) -> Advertisement {
    Advertisement::new(Identity::new(
        [device],
        ["device", "broker-host"],
        "Acme",
        "P1",
        "SN1",
    ))
    .broker(BrokerService::new(ty, port))
}

async fn wait_state(adv: &Advertiser, what: &str, want: impl Fn(&AdState) -> bool) -> AdState {
    let mut state = adv.state();
    timeout(Duration::from_secs(20), state.wait_for(|s| want(s)))
        .await
        .unwrap_or_else(|_| panic!("{what}; state {:?}", *adv.state().borrow()))
        .unwrap()
        .clone()
}

/// The server once `instance` is established.
async fn established(adv: &Advertiser, instance: &str) -> String {
    match wait_state(
        adv,
        instance,
        |s| matches!(s, AdState::Established { instance: i, .. } if i == instance),
    )
    .await
    {
        AdState::Established { server, .. } => server,
        other => unreachable!("{other:?}"),
    }
}

/// The next sighting of `name` on `browse` satisfying `want`.
async fn sighting<B: Browse>(
    browse: &mut B,
    name: &str,
    want: impl Fn(&Sighting) -> bool,
) -> Sighting {
    timeout(Duration::from_secs(20), async {
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

/// The TXT `ad` builds for its record of `service_type` under `server`.
fn built_txt(ad: &Advertisement, server: &str, service_type: &str) -> TxtRecord {
    let records = ad.records(server).unwrap();
    let r = records.iter().find(|r| r.service_type == service_type);
    r.expect("record").txt.clone()
}

/// Undoes a change to the host when dropped, so a failing test leaves
/// avahi-daemon as it found it; [`Restore::now`] undoes it at once.
struct Restore<F: FnOnce() -> bool>(Option<F>);

impl<F: FnOnce() -> bool> Restore<F> {
    /// Undo the change now; true when that succeeded.
    fn now(mut self) -> bool {
        self.0.take().is_some_and(|f| f())
    }
}

impl<F: FnOnce() -> bool> Drop for Restore<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

fn sh(cmd: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .status()
        .is_ok_and(|s| s.success())
}

fn seen(s: &Sighting) -> bool {
    matches!(s, Sighting::Seen { .. })
}

/// The broker advertised as `instance`, listed by `b` within a few seconds.
async fn listed<B: Backend>(b: &B, instance: &str) -> BrokerEndpoint {
    let mut cfg = BrokerConfig::default();
    cfg.wanted = Some(Wanted::Instance(instance.into()));
    for _ in 0..4 {
        let found = list_brokers(b, &cfg, Duration::from_secs(4)).await.unwrap();
        if let Some(e) = found.into_iter().next() {
            return e;
        }
    }
    panic!("{instance} not listed");
}

#[tokio::test(flavor = "multi_thread")]
async fn system_prefers_avahi() {
    let _serial = SERIAL.lock().await;
    let Some(_) = avahi().await else { return };
    let m = Mdns::system().await.unwrap();
    assert!(format!("{m:?}").contains("avahi"), "{m:?}");
}

/// Without avahi-daemon, `system` falls back to mdns-sd.
#[cfg(feature = "mdns-sd")]
#[tokio::test(flavor = "multi_thread")]
async fn system_falls_back_to_mdns_sd() {
    let _serial = SERIAL.lock().await;
    match Mdns::avahi().await {
        Err(Error::NoSystemBus | Error::AvahiUnavailable) => {}
        Ok(_) => {
            eprintln!("skipping: avahi-daemon is running");
            return;
        }
        Err(e) => panic!("{e}"),
    }
    let m = Mdns::system().await.unwrap();
    assert!(format!("{m:?}").contains("mdns-sd"), "{m:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn avahi_browses_an_avahi_advertisement() {
    let _serial = SERIAL.lock().await;
    let Some(publisher) = avahi().await else {
        return;
    };
    let client = avahi().await.unwrap();
    assert!(matches!(
        publisher
            .server_name(&HostName::Own("x.local.".into()))
            .await,
        Err(Error::HostName(_))
    ));
    let name = format!("aa-{}", run_id());
    let ad = broker_ad(&name, BrokerType::SecureMqtt, 18841);
    let adv = advertise(&publisher, ad.clone(), HostName::Auto)
        .await
        .unwrap();
    let server = established(&adv, &name).await;
    assert_eq!(
        server,
        publisher.server_name(&HostName::Auto).await.unwrap()
    );

    let found = listed(&client, &name).await;
    assert_eq!(found.broker_type, BrokerType::SecureMqtt);
    assert_eq!(found.port, 18841);
    assert_eq!(found.host, server.trim_end_matches('.'));
    assert!(!found.addrs.is_empty(), "{found:?}");

    let mut browse = client.browse("_secure-mqtt._tcp").await.unwrap();
    let Sighting::Seen { txt, .. } = sighting(&mut browse, &name, seen).await else {
        unreachable!()
    };
    // AVAHI_PUBLISH_NO_COOKIE: the TXT is exactly what was built.
    assert_eq!(txt, built_txt(&ad, &server, "_secure-mqtt._tcp"));

    let mut changed = ad.clone();
    changed.brokers[0].extra_txt.push(("k".into(), "v".into()));
    adv.update(changed).unwrap();
    let s = sighting(
        &mut browse,
        &name,
        |s| matches!(s, Sighting::Seen { txt, .. } if txt.get("k").as_deref() == Some("v")),
    )
    .await;
    let Sighting::Seen { port, .. } = s else {
        unreachable!()
    };
    assert_eq!(port, 18841);

    adv.withdraw().await.unwrap();
    sighting(&mut browse, &name, |s| matches!(s, Sighting::Gone { .. })).await;
}

/// The sightings of `name` Avahi reports over a few seconds, checking that
/// each item's addresses are of its own protocol; (IPv4 items, IPv6 items).
async fn items_by_family<B: Browse>(browse: &mut B, name: &str) -> (usize, usize) {
    let (mut v4_items, mut v6_items) = (0, 0);
    let _ = timeout(Duration::from_secs(6), async {
        loop {
            let Sighting::Seen {
                slot: Slot::Link { v6, ifindex },
                addrs,
                ..
            } = sighting(browse, name, seen).await
            else {
                panic!("an Avahi sighting without its link");
            };
            for a in &addrs {
                assert_eq!(a.ip.is_ipv6(), v6, "item on {ifindex}: {addrs:?}");
            }
            if v6 {
                v6_items += 1;
            } else {
                v4_items += 1;
            }
        }
    })
    .await;
    (v4_items, v6_items)
}

/// On a dual-stack link, each item resolves the addresses of its own
/// protocol, so the IPv4 item of a `.local` broker carries the IPv4 address
/// `connect_host` uses. Needs an IPv6 address on an interface avahi-daemon
/// uses.
#[tokio::test(flavor = "multi_thread")]
async fn each_item_resolves_its_own_address_family() {
    let _serial = SERIAL.lock().await;
    let Some(publisher) = avahi().await else {
        return;
    };
    let client = avahi().await.unwrap();
    let name = format!("a6-{}", run_id());
    let adv = advertise(
        &publisher,
        broker_ad(&name, BrokerType::Mqtt, 18851),
        HostName::Auto,
    )
    .await
    .unwrap();
    established(&adv, &name).await;
    let mut browse = client.browse("_mqtt._tcp").await.unwrap();
    let (v4_items, v6_items) = items_by_family(&mut browse, &name).await;
    let found = listed(&client, &name).await;
    adv.withdraw().await.unwrap();
    assert!(v4_items > 0, "no IPv4 item");
    if v6_items == 0 {
        support::skip("no IPv6 item: avahi-daemon uses no interface with an IPv6 address");
        return;
    }
    assert!(
        found.connect_host().parse::<std::net::Ipv4Addr>().is_ok(),
        "{found:?}"
    );
}

/// [`each_item_resolves_its_own_address_family`] for a broker another
/// responder (mdns-sd) advertises with A and AAAA records.
#[cfg(feature = "mdns-sd")]
#[tokio::test(flavor = "multi_thread")]
async fn each_item_of_another_responder_resolves_its_own_address_family() {
    let _serial = SERIAL.lock().await;
    let Some(client) = avahi().await else { return };
    let id = run_id();
    let name = format!("m6-{id}");
    let host = format!("ebus-test-m6-{id}");
    let adv = advertise(
        &mdns_sd(),
        broker_ad(&name, BrokerType::Mqtt, 18852),
        HostName::Own(format!("{host}.local.")),
    )
    .await
    .unwrap();
    established(&adv, &name).await;
    let mut browse = client.browse("_mqtt._tcp").await.unwrap();
    let (v4_items, v6_items) = items_by_family(&mut browse, &name).await;
    let found = listed(&client, &name).await;
    adv.withdraw().await.unwrap();
    assert!(v4_items > 0, "no IPv4 item");
    if v6_items == 0 {
        support::skip("no IPv6 item: no interface carries an IPv6 address");
        return;
    }
    assert!(
        found.connect_host().parse::<std::net::Ipv4Addr>().is_ok(),
        "{found:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn avahi_instance_collision_renames() {
    let _serial = SERIAL.lock().await;
    let Some(a) = avahi().await else { return };
    let b = avahi().await.unwrap();
    let name = format!("ac-{}", run_id());
    let first = advertise(
        &a,
        broker_ad("dev-1", BrokerType::Mqtt, 18842).instance(&name),
        HostName::Auto,
    )
    .await
    .unwrap();
    established(&first, &name).await;
    let second = advertise(
        &b,
        broker_ad("dev-2", BrokerType::Mqtt, 18843).instance(&name),
        HostName::Auto,
    )
    .await
    .unwrap();
    let renamed = format!("{name}-2");
    established(&second, &renamed).await;

    let found = listed(&a, &renamed).await;
    assert_eq!(found.port, 18843);
    second.withdraw().await.unwrap();
    first.withdraw().await.unwrap();
}

#[cfg(feature = "mdns-sd")]
#[tokio::test(flavor = "multi_thread")]
async fn avahi_browses_an_mdns_sd_advertisement() {
    let _serial = SERIAL.lock().await;
    let Some(client) = avahi().await else { return };
    let id = run_id();
    let name = format!("ma-{id}");
    let host = format!("ebus-test-ma-{id}");
    let ad = broker_ad(&name, BrokerType::Mqtt, 18844);
    let adv = advertise(
        &mdns_sd(),
        ad.clone(),
        HostName::Own(format!("{host}.local.")),
    )
    .await
    .unwrap();
    established(&adv, &name).await;

    let found = listed(&client, &name).await;
    assert_eq!(found.host, format!("{host}.local"));
    assert_eq!(found.port, 18844);
    let mut browse = client.browse("_mqtt._tcp").await.unwrap();
    let Sighting::Seen { txt, .. } = sighting(&mut browse, &name, seen).await else {
        unreachable!()
    };
    assert_eq!(txt, built_txt(&ad, &format!("{host}.local."), "_mqtt._tcp"));
    assert!(
        found.addrs.iter().any(|a| a.ip.is_ipv4()),
        "an IPv4 address: {found:?}"
    );
    adv.withdraw().await.unwrap();
}

#[cfg(feature = "mdns-sd")]
#[tokio::test(flavor = "multi_thread")]
async fn mdns_sd_browses_an_avahi_advertisement() {
    let _serial = SERIAL.lock().await;
    let Some(publisher) = avahi().await else {
        return;
    };
    let name = format!("am-{}", run_id());
    let ad = broker_ad(&name, BrokerType::SecureMqtt, 18845);
    let adv = advertise(&publisher, ad.clone(), HostName::Auto)
        .await
        .unwrap();
    let server = established(&adv, &name).await;

    let client = mdns_sd();
    let found = listed(&client, &name).await;
    let mut browse = client.browse("_secure-mqtt._tcp").await.unwrap();
    let Sighting::Seen { txt, .. } = sighting(&mut browse, &name, seen).await else {
        unreachable!()
    };
    // The order avahi-daemon put on the wire, and no cookie.
    assert_eq!(txt, built_txt(&ad, &server, "_secure-mqtt._tcp"));
    assert_eq!(found.host, server.trim_end_matches('.'));
    assert_eq!(found.port, 18845);
    assert!(
        found.addrs.iter().any(|a| a.ip.is_ipv4()),
        "an IPv4 address: {found:?}"
    );
    adv.withdraw().await.unwrap();
}

/// avahi-daemon restarted under a running advertisement and a running
/// browse: the advertisement is committed again, and the browse keeps
/// working and drops an instance withdrawn while the daemon was down.
#[tokio::test(flavor = "multi_thread")]
async fn restart_commits_again() {
    let _serial = SERIAL.lock().await;
    let (Ok(stop), Ok(start)) = (
        std::env::var("EBUS_AVAHI_STOP"),
        std::env::var("EBUS_AVAHI_START"),
    ) else {
        support::skip("EBUS_AVAHI_STOP or EBUS_AVAHI_START is not set");
        return;
    };
    let Some(publisher) = avahi().await else {
        return;
    };
    let client = avahi().await.unwrap();
    let id = run_id();
    let name = format!("ar-{id}");
    let adv = advertise(
        &publisher,
        broker_ad(&name, BrokerType::Mqtt, 18846),
        HostName::Auto,
    )
    .await
    .unwrap();
    established(&adv, &name).await;
    let mut browse = client.browse("_mqtt._tcp").await.unwrap();
    sighting(&mut browse, &name, seen).await;

    // An instance of another stack, withdrawn while avahi-daemon is down.
    #[cfg(feature = "mdns-sd")]
    let (other, other_name) = {
        let other_name = format!("ar-other-{id}");
        let other = advertise(
            &mdns_sd(),
            broker_ad(&other_name, BrokerType::Mqtt, 18847),
            HostName::Own(format!("ebus-test-ar-{id}.local.")),
        )
        .await
        .unwrap();
        established(&other, &other_name).await;
        sighting(&mut browse, &other_name, seen).await;
        (other, other_name)
    };

    let mut state = adv.state();
    state.mark_unchanged();
    assert!(sh(&stop), "{stop}");
    let restart = Restore(Some(|| sh(&start)));
    timeout(
        Duration::from_secs(10),
        state.wait_for(|s| *s == AdState::Registering),
    )
    .await
    .expect("registering again")
    .unwrap();
    // Its goodbyes reach nobody that remembers them.
    #[cfg(feature = "mdns-sd")]
    other.withdraw().await.unwrap();
    assert!(restart.now(), "{start}");
    established(&adv, &name).await;

    // A client connected after the restart sees the advertisement.
    let fresh = avahi().await.expect("avahi-daemon back");
    listed(&fresh, &name).await;

    #[cfg(feature = "mdns-sd")]
    sighting(&mut browse, &other_name, |s| {
        matches!(s, Sighting::Gone { .. })
    })
    .await;

    // The browse from before the restart sees a new advertisement.
    let later = format!("ar-later-{id}");
    let adv2 = advertise(
        &publisher,
        broker_ad(&later, BrokerType::Mqtt, 18848),
        HostName::Auto,
    )
    .await
    .unwrap();
    established(&adv2, &later).await;
    sighting(&mut browse, &later, seen).await;
    adv2.withdraw().await.unwrap();
    adv.withdraw().await.unwrap();
}

/// avahi-daemon renamed: the advertisement follows the new SRV target and
/// rebuilds its TXT `broker`.
#[tokio::test(flavor = "multi_thread")]
async fn host_rename_rebuilds_the_broker_txt() {
    let _serial = SERIAL.lock().await;
    let Some(publisher) = avahi().await else {
        return;
    };
    let id = run_id();
    let name = format!("ah-{id}");
    let adv = advertise(
        &publisher,
        broker_ad(&name, BrokerType::SecureMqtt, 18849),
        HostName::Auto,
    )
    .await
    .unwrap();
    let before = established(&adv, &name).await;
    let label = before.split('.').next().unwrap().to_string();
    let renamed_label = format!("ebus-rn-{id}");
    let set = |l: &str| Command::new("avahi-set-host-name").arg(l).status();
    match set(&renamed_label) {
        Ok(s) if s.success() => {}
        other => {
            eprintln!("skipping: avahi-set-host-name failed: {other:?}");
            adv.withdraw().await.unwrap();
            return;
        }
    }
    let restore = Restore(Some(|| set(&label).is_ok_and(|s| s.success())));
    let renamed = format!("{renamed_label}.local.");
    let result = async {
        wait_state(
            &adv,
            "renamed host",
            |s| matches!(s, AdState::Established { server, .. } if *server == renamed),
        )
        .await;
        let client = avahi().await.unwrap();
        let mut browse = client.browse("_secure-mqtt._tcp").await.unwrap();
        let s = sighting(
            &mut browse,
            &name,
            |s| matches!(s, Sighting::Seen { server, .. } if *server == renamed),
        )
        .await;
        let Sighting::Seen { txt, .. } = s else {
            unreachable!()
        };
        txt.get("broker").map(|b| b.into_owned())
    }
    .await;
    let restored = restore.now();
    assert_eq!(result.as_deref(), Some(renamed.trim_end_matches('.')));
    assert!(restored, "host name restored");
    adv.withdraw().await.unwrap();
}
