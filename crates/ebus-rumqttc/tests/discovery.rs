//! Brokers found over mDNS (mdns-sd) and connected to: local mosquitto
//! brokers advertised as `_mqtt._tcp` on the interface `EBUS_TEST_MDNS_IFACE`
//! names, which must carry an IPv4 address that is not loopback (an address
//! a client connects to) and no other eBus broker. Skipped unless that
//! variable is set and a `mosquitto` binary is found. The test of a broker
//! that moves needs two IPv4 addresses on the interface. With
//! `EBUS_REQUIRE_E2E` set, a missing interface or mosquitto fails the test
//! instead of skipping it.

mod support;

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ebus_discovery_tokio::ebus_discovery::{
    Advertisement, BrokerConfig, BrokerEndpoint, BrokerMode, BrokerService, BrokerType, Identity,
    Wanted,
};
use ebus_discovery_tokio::{
    AdState, Advertiser, HostName, Mdns, MdnsSdOptions, advertise, find_broker, list_brokers,
    reresolve,
};
use ebus_rumqttc::Reresolve;
use ebus_rumqttc::RunningDevice;
use ebus_rumqttc::ebus_homie_tokio::ControllerOptions;
use ebus_rumqttc::ebus_homie_tokio::device::{
    DeviceOptions, DeviceSpec, NodeSpec, PropSpec, Property, TreeBuilder, TreeConfig,
};
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event};
use ebus_rumqttc::rumqttc::MqttOptions;
use mdns_sd::{IfKind, IfPredicate, ServiceDaemon, ServiceInfo};
use support::{Broker, Ctl, observe_at, until_message};
use tokio::sync::Mutex;
use tokio::time::timeout;

static SERIAL: Mutex<()> = Mutex::const_new(());

/// The interface the tests advertise and browse on, and its IPv4 addresses.
struct Net {
    iface: String,
    v4: Vec<Ipv4Addr>,
}

fn net(test: &str) -> Option<Net> {
    let Ok(iface) = std::env::var("EBUS_TEST_MDNS_IFACE") else {
        support::skip(test, "EBUS_TEST_MDNS_IFACE names no interface");
        return None;
    };
    let mut v4: Vec<Ipv4Addr> = if_addrs::get_if_addrs()
        .expect("interfaces")
        .into_iter()
        .filter(|i| i.name == iface)
        .filter_map(|i| match i.ip() {
            IpAddr::V4(a) if !a.is_loopback() => Some(a),
            _ => None,
        })
        .collect();
    v4.sort();
    assert!(!v4.is_empty(), "{iface} has no IPv4 address");
    Some(Net { iface, v4 })
}

fn run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{:x}{:x}", std::process::id(), nanos)
}

impl Net {
    fn mdns(&self) -> Mdns {
        let mut opts = MdnsSdOptions::default();
        opts.interfaces = vec![self.iface.clone()];
        Mdns::mdns_sd(opts).unwrap()
    }

    /// A bare mdns-sd daemon on the interface, standing in for a broker
    /// host's responder.
    fn raw_daemon(&self) -> ServiceDaemon {
        let d = ServiceDaemon::new().unwrap();
        d.disable_interface(IfKind::All).unwrap();
        let iface = self.iface.clone();
        d.enable_interface(IfKind::Predicate(IfPredicate::new(move |i| {
            i.name == iface
        })))
        .unwrap();
        d
    }

    /// A mosquitto on the interface's first address.
    fn broker(&self, test: &str) -> Option<Broker> {
        Broker::start_at(test, IpAddr::V4(self.v4[0]), 0, "")
    }

    /// Advertises `broker` as `_mqtt._tcp` for device `device`, under host
    /// `host`.
    async fn advertise(&self, device: &str, host: &str, broker: &Broker) -> Advertiser {
        let ad = Advertisement::new(Identity::new(
            [device],
            ["broker-host"],
            "Acme",
            "P1",
            "SN1",
        ))
        .broker(BrokerService::new(BrokerType::Mqtt, broker.port));
        let adv = advertise(&self.mdns(), ad, HostName::Own(format!("{host}.local.")))
            .await
            .unwrap();
        let mut state = adv.state();
        timeout(
            Duration::from_secs(10),
            state.wait_for(|s| matches!(s, AdState::Established { .. })),
        )
        .await
        .expect("advertised")
        .unwrap();
        adv
    }
}

/// The options a host builds for `broker`.
fn options_for(broker: &BrokerEndpoint, client_id: &str) -> MqttOptions {
    let mut mqtt = MqttOptions::new(client_id, broker.connect_host(), broker.port);
    mqtt.set_max_packet_size(1 << 20, 1 << 20);
    mqtt
}

/// A device `root` with integer property `n/level` set to 1, connecting
/// with `mqtt`.
fn device(root: &str, mqtt: MqttOptions) -> (RunningDevice, Property<i64>) {
    let mut tree =
        TreeBuilder::new(TreeConfig::default(), root, DeviceSpec::default()).expect("tree");
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
    let dev = ebus_rumqttc::device(mqtt, tree, DeviceOptions::default()).expect("device");
    (dev, level)
}

/// A controller of `root` connecting with `mqtt`, and its task with `hook`.
fn controller(root: &str, mqtt: MqttOptions, hook: Option<Reresolve>) -> Ctl {
    let run = ebus_rumqttc::controller(
        mqtt,
        Config::tree(root).expect("valid root"),
        ControllerOptions::default(),
    )
    .expect("controller");
    let task = match hook {
        Some(h) => run.task.reresolve(h),
        None => run.task,
    };
    Ctl::attached(run.handle, run.events, tokio::spawn(task.run()))
}

/// Our brokers among those `list_brokers` returns, in its order.
async fn listed(mdns: &Mdns, cfg: &BrokerConfig, id: &str) -> Vec<BrokerEndpoint> {
    list_brokers(mdns, cfg, Duration::from_secs(4))
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.instance.as_deref().is_some_and(|i| i.ends_with(id)))
        .collect()
}

async fn find(mdns: &Mdns, cfg: BrokerConfig) -> BrokerEndpoint {
    timeout(Duration::from_secs(30), find_broker(mdns, cfg))
        .await
        .expect("find_broker returns")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_advertised_brokers() {
    let test = "two_advertised_brokers";
    let _serial = SERIAL.lock().await;
    let Some(net) = net(test) else { return };
    let (Some(a), Some(b)) = (net.broker(test), net.broker(test)) else {
        return;
    };
    let id = run_id();
    let (dev_a, dev_b) = (format!("bk-a-{id}"), format!("bk-b-{id}"));
    let adv_a = net
        .advertise(&dev_a, &format!("ebus-test-a-{id}"), &a)
        .await;
    let adv_b = net
        .advertise(&dev_b, &format!("ebus-test-b-{id}"), &b)
        .await;

    let mdns = net.mdns();
    let ours = listed(&mdns, &BrokerConfig::default(), &id).await;
    let seen: Vec<(&str, u16)> = ours
        .iter()
        .map(|e| (e.instance.as_deref().unwrap(), e.port))
        .collect();
    assert_eq!(seen, [(dev_a.as_str(), a.port), (dev_b.as_str(), b.port)]);
    for e in &ours {
        assert_eq!(e.broker_type, BrokerType::Mqtt);
        let ip: Ipv4Addr = e.connect_host().parse().expect("connects by address");
        assert!(net.v4.contains(&ip), "{ip} on {}", net.iface);
        // Advertised and browsed on that interface alone.
        for a in e.addrs.iter().filter(|a| a.ip.is_ipv4()) {
            assert!(net.v4.iter().any(|v| IpAddr::V4(*v) == a.ip), "{e:?}");
        }
    }

    // discovery-only picks the first; a device and a controller connect
    // through it.
    let first = find(&mdns, BrokerConfig::default()).await;
    assert_eq!(first.instance.as_deref(), Some(dev_a.as_str()));
    let root = format!("rs-disc-{id}");
    let (dev, level) = device(&root, options_for(&first, &root));
    let dev_task = tokio::spawn(dev.task.run());
    let mut ctl = controller(
        &root,
        options_for(&first, &ebus_rumqttc::controller_client_id()),
        None,
    );
    ctl.synced(15, &[&root]).await;
    assert_eq!(ctl.value(&root, "n", "level").as_deref(), Some("1"));
    level.set(2).expect("level");
    ctl.until(10, "level 2", |e| {
        matches!(e, Event::PropertyChanged { addr, value, .. }
            if addr.property.as_str() == "level" && &*value.raw == "2")
    })
    .await;
    ctl.shutdown().await;
    dev.handle.abort();
    let _ = timeout(Duration::from_secs(10), dev_task).await;

    // `wanted` picks the other, and a controller there syncs a device
    // published to it.
    let mut cfg = BrokerConfig::default();
    cfg.wanted = Some(Wanted::Instance(dev_b.clone()));
    let other = find(&mdns, cfg).await;
    assert_eq!(other.instance.as_deref(), Some(dev_b.as_str()));
    assert_eq!(other.port, b.port);
    let root_b = format!("rs-disc-b-{id}");
    let (dev, _level) = device(&root_b, options_for(&other, &root_b));
    let dev_task = tokio::spawn(dev.task.run());
    let mut ctl = controller(
        &root_b,
        options_for(&other, &ebus_rumqttc::controller_client_id()),
        None,
    );
    ctl.synced(15, &[&root_b]).await;
    ctl.shutdown().await;
    dev.handle.abort();
    let _ = timeout(Duration::from_secs(10), dev_task).await;

    adv_a.withdraw().await.unwrap();
    adv_b.withdraw().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fallback_to_an_unadvertised_configured_broker() {
    let test = "fallback_to_an_unadvertised_configured_broker";
    let _serial = SERIAL.lock().await;
    let Some(net) = net(test) else { return };
    let (Some(advertised), Some(configured)) = (net.broker(test), Broker::start(test, "")) else {
        return;
    };
    let id = run_id();
    let dev_a = format!("fb-{id}");
    let adv = net
        .advertise(&dev_a, &format!("ebus-test-fb-{id}"), &advertised)
        .await;
    let mdns = net.mdns();
    assert_eq!(listed(&mdns, &BrokerConfig::default(), &id).await.len(), 1);

    let mut cfg = BrokerConfig::default();
    cfg.mode = Some(BrokerMode::DiscoveryWithFallback);
    let url = format!("mqtt://127.0.0.1:{}", configured.port);
    cfg.url = Some(BrokerEndpoint::from_url(&url).unwrap());
    cfg.fallback_after_ms = 5_000;
    let start = tokio::time::Instant::now();
    let chosen = find(&mdns, cfg).await;
    assert!(start.elapsed() >= Duration::from_secs(5));
    assert_eq!(chosen.instance, None, "{chosen:?}");
    assert_eq!(chosen.url(), url);

    let root = format!("rs-fb-{id}");
    let (dev, _level) = device(&root, options_for(&chosen, &root));
    let dev_task = tokio::spawn(dev.task.run());
    let mut ctl = controller(
        &root,
        options_for(&chosen, &ebus_rumqttc::controller_client_id()),
        None,
    );
    ctl.synced(15, &[&root]).await;
    ctl.shutdown().await;
    dev.handle.abort();
    let _ = timeout(Duration::from_secs(10), dev_task).await;
    adv.withdraw().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_ignores_a_plaintext_advertisement() {
    let test = "tls_ignores_a_plaintext_advertisement";
    let _serial = SERIAL.lock().await;
    let Some(net) = net(test) else { return };
    let Some(plain) = net.broker(test) else {
        return;
    };
    let id = run_id();
    let dev = format!("tls-{id}");
    let adv = net
        .advertise(&dev, &format!("ebus-test-tls-{id}"), &plain)
        .await;
    let mdns = net.mdns();

    // Any discovered broker would do, and without TLS the plaintext one is
    // taken.
    let config = |url: &str| {
        let mut cfg = BrokerConfig::default();
        cfg.mode = Some(BrokerMode::DiscoveryWithFallback);
        cfg.url = Some(BrokerEndpoint::from_url(url).unwrap());
        cfg.allow_unmatched = true;
        cfg.fallback_after_ms = 5_000;
        cfg
    };
    let without = find(&mdns, config("mqtt://192.0.2.1:1883")).await;
    assert_eq!(without.instance.as_deref(), Some(dev.as_str()));

    let tls = config("mqtts://192.0.2.1:8883");
    assert!(listed(&mdns, &tls, &id).await.is_empty());
    let chosen = find(&mdns, tls).await;
    assert_eq!(chosen.instance, None, "{chosen:?}");
    assert_eq!(chosen.broker_type, BrokerType::SecureMqtt);
    assert_eq!(chosen.url(), "mqtts://192.0.2.1:8883");
    adv.withdraw().await.unwrap();
}

/// A hook that counts its calls and finds `broker` again through `mdns`.
fn hook(
    mdns: &Mdns,
    broker: &BrokerEndpoint,
    cfg: &BrokerConfig,
    client_id: &str,
    calls: &Arc<AtomicUsize>,
) -> Reresolve {
    let (mdns, broker, cfg, id) = (
        mdns.clone(),
        broker.clone(),
        cfg.clone(),
        client_id.to_owned(),
    );
    let calls = Arc::clone(calls);
    Box::new(move || {
        calls.fetch_add(1, Ordering::AcqRel);
        let (mdns, broker, cfg, id) = (mdns.clone(), broker.clone(), cfg.clone(), id.clone());
        Box::pin(async move {
            let found = reresolve(&mdns, &broker, &cfg, Duration::from_secs(3))
                .await
                .ok()
                .flatten()?;
            Some(options_for(&found, &id))
        })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reresolve_follows_a_broker_to_its_new_address() {
    let test = "reresolve_follows_a_broker_to_its_new_address";
    let _serial = SERIAL.lock().await;
    let Some(net) = net(test) else { return };
    if net.v4.len() < 2 {
        support::skip(test, &format!("{} has one IPv4 address", net.iface));
        return;
    }
    let (old_ip, new_ip) = (IpAddr::V4(net.v4[0]), IpAddr::V4(net.v4[1]));
    let Some(mut old) = Broker::start_at(test, old_ip, 0, "") else {
        return;
    };
    let id = run_id();
    let name = format!("mv-{id}");
    let host = format!("ebus-test-mv-{id}.local.");
    let responder = net.raw_daemon();
    let at = |ip: IpAddr, port: u16| {
        ServiceInfo::new(
            "_mqtt._tcp.local.",
            &name,
            &host,
            &*ip.to_string(),
            port,
            None,
        )
        .unwrap()
    };
    responder.register(at(old_ip, old.port)).unwrap();

    let mdns = net.mdns();
    let mut cfg = BrokerConfig::default();
    cfg.wanted = Some(Wanted::Instance(name.clone()));
    let broker = find(&mdns, cfg.clone()).await;
    assert_eq!(broker.connect_host(), old_ip.to_string());

    let root = format!("rs-mv-{id}");
    let (dev, level) = device(&root, options_for(&broker, &root));
    let dev_calls = Arc::new(AtomicUsize::new(0));
    let dev_task = tokio::spawn(
        dev.task
            .reresolve(hook(&mdns, &broker, &cfg, &root, &dev_calls))
            .run(),
    );
    let ctl_calls = Arc::new(AtomicUsize::new(0));
    let ctl_id = ebus_rumqttc::controller_client_id();
    let mut ctl = controller(
        &root,
        options_for(&broker, &ctl_id),
        Some(hook(&mdns, &broker, &cfg, &ctl_id, &ctl_calls)),
    );
    ctl.synced(15, &[&root]).await;

    // The broker host moves: the same broker, re-advertised at a new address.
    let Some(new) = Broker::start_at(test, new_ip, old.port, "") else {
        return;
    };
    let state = format!("ebus/5/{root}/$state");
    let mut on_new = observe_at(new_ip, new.port, "observer-new", &[&state]).await;
    responder.register(at(new_ip, new.port)).unwrap();
    old.kill();

    until_message(&mut on_new, 30, &state, b"ready").await;
    ctl.synced(30, &[&root]).await;
    level.set(2).expect("level");
    ctl.until(10, "level 2 at the new address", |e| {
        matches!(e, Event::PropertyChanged { addr, value, .. }
            if addr.property.as_str() == "level" && &*value.raw == "2")
    })
    .await;
    assert!(dev_calls.load(Ordering::Acquire) >= 1);
    assert!(ctl_calls.load(Ordering::Acquire) >= 1);

    ctl.shutdown().await;
    dev.handle.abort();
    let _ = timeout(Duration::from_secs(10), dev_task).await;
    let _ = responder.shutdown();
}
