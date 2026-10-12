//! The bundled controller against a local mosquitto, one broker per test.
//! Every test is skipped when no `mosquitto` binary is found (`MOSQUITTO`
//! names one), and the python-sdk cases also when `ebus_sdk` cannot be
//! imported (`EBUS_PYTHON` names the interpreter).

mod support;

use std::time::Duration;

use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event, Pacing, PropertyAddr};
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::state::DeviceState;
use ebus_rumqttc::ebus_homie_tokio::ebus_mqtt::Link;
use support::{Broker, Proxy, PyDevice, Seeder, controller, int_description, int_values, observe};
use tokio::time::timeout;

const DESC: &str = r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{"datatype":"boolean","settable":true},"q":{"datatype":"integer"}}}}}"#;

fn synced_for(e: &Event, id: &str) -> bool {
    matches!(e, Event::DeviceSynced { device, .. } if device.as_str() == id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_syncs_sets_and_shuts_down() {
    let Some(broker) = Broker::start("controller_syncs_sets_and_shuts_down", "") else {
        return;
    };
    let mut set_rx = observe(broker.port, "device-set", &["ebus/5/d1/+/+/set"]).await;
    let mut seed = Seeder::new(broker.port, "seeder");
    seed.device(
        "d1",
        DESC,
        &[("n/p".into(), "true".into()), ("n/q".into(), "3".into())],
    )
    .await;
    seed.flush().await;

    let mut ctl = controller(broker.port, Config::wildcard());
    ctl.synced(15, &["d1"]).await;
    assert_eq!(ctl.value("d1", "n", "q").as_deref(), Some("3"));

    let addr = PropertyAddr::new("d1", "n", "p").expect("valid address");
    ctl.handle.set(addr, "false").await.expect("set accepted");
    let (topic, payload, retain) = timeout(Duration::from_secs(5), set_rx.recv())
        .await
        .expect("set delivered in time")
        .expect("observer running");
    assert_eq!(topic, "ebus/5/d1/n/p/set");
    assert_eq!(payload, b"false");
    assert!(!retain);

    let handle = ctl.handle.clone();
    ctl.shutdown().await;
    assert!(!handle.is_connected());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn python_device_tree_is_discovered_and_synced() {
    let test = "python_device_tree_is_discovered_and_synced";
    let Some(broker) = Broker::start(test, "") else {
        return;
    };
    let Some(py) = PyDevice::start(test, broker.port, 3, 4) else {
        return;
    };
    assert_eq!(py.line(10), "ready");

    let all = ["py-root", "py-child-00", "py-child-01", "py-child-02"];
    let mut tree = controller(broker.port, Config::tree("py-root").expect("valid root"));
    let mut wild = controller(broker.port, Config::wildcard());
    tree.synced(15, &all).await;
    wild.synced(15, &all).await;

    for ctl in [&tree, &wild] {
        for (i, child) in all[1..].iter().enumerate() {
            for p in 0..4 {
                assert_eq!(
                    ctl.value(child, "meter", &format!("p{p}")),
                    Some((i * 100 + p).to_string()),
                    "{child}/meter/p{p}"
                );
            }
        }
        assert_eq!(
            ctl.value("py-root", "ctl", "text").as_deref(),
            Some("initial")
        );
        assert_eq!(ctl.value("py-root", "ctl", "level").as_deref(), Some("1"));
        assert_eq!(ctl.value("py-root", "ctl", "on").as_deref(), Some("false"));
        for id in all {
            assert_eq!(
                ctl.handle.effective_state(id),
                Some(DeviceState::Ready),
                "{id}"
            );
        }
    }
    tree.eventually(5, "tree complete", |h| h.is_tree_complete("py-root"))
        .await;
    assert!(
        tree.saw(|e| matches!(e, Event::TreeComplete { root, .. } if root.as_str() == "py-root")),
        "no TreeComplete"
    );
    tree.shutdown().await;
    wild.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_round_trips_through_a_python_device() {
    let test = "set_round_trips_through_a_python_device";
    let Some(broker) = Broker::start(test, "") else {
        return;
    };
    let Some(py) = PyDevice::start(test, broker.port, 0, 0) else {
        return;
    };
    assert_eq!(py.line(10), "ready");
    let mut ctl = controller(broker.port, Config::wildcard());
    ctl.synced(15, &["py-root"]).await;
    let mut wire = observe(broker.port, "set-observer", &["ebus/5/py-root/ctl/+/set"]).await;

    for (prop, payload) in [
        ("text", "h\u{e9}llo, w\u{f6}rld \u{2713} \"q\""),
        ("level", "-42"),
        ("on", "true"),
    ] {
        let addr = PropertyAddr::new("py-root", "ctl", prop).expect("valid address");
        ctl.handle.set(addr, payload).await.expect("set accepted");

        let (topic, bytes, retain) = timeout(Duration::from_secs(5), wire.recv())
            .await
            .expect("set on the wire in time")
            .expect("observer running");
        assert_eq!(topic, format!("ebus/5/py-root/ctl/{prop}/set"));
        assert_eq!(bytes, payload.as_bytes(), "{prop}: /set bytes");
        assert!(!retain);

        let line = py.line(5);
        assert!(line.starts_with(&format!("set {prop} ")), "{line}");

        ctl.until(5, &format!("{prop} republished"), |e| {
            matches!(e, Event::PropertyChanged { addr, value, .. }
                if addr.property.as_str() == prop && &*value.raw == payload)
        })
        .await;
        assert_eq!(ctl.value("py-root", "ctl", prop).as_deref(), Some(payload));
    }
    ctl.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_restart_without_persistence_marks_absent_then_heals() {
    let test = "broker_restart_without_persistence_marks_absent_then_heals";
    let Some(mut broker) = Broker::start(test, "") else {
        return;
    };
    let py = PyDevice::start(test, broker.port, 1, 2);
    let mut devices = vec!["static-1"];
    if let Some(py) = &py {
        assert_eq!(py.line(10), "ready");
        devices.extend(["py-root", "py-child-00"]);
    }
    let desc = int_description(3, "");
    let mut seed = Seeder::new(broker.port, "seeder-1");
    seed.device("static-1", &desc, &int_values(3, 10)).await;
    seed.flush().await;

    let mut cfg = Config::wildcard();
    cfg.absent_after_ms = 2_000;
    let mut ctl = controller(broker.port, cfg);
    ctl.synced(15, &devices).await;

    broker.restart();
    ctl.until(5, "Link(Down)", |e| matches!(e, Event::Link(Link::Down)))
        .await;
    ctl.until(5, "Link(Up)", |e| matches!(e, Event::Link(Link::Up { .. })))
        .await;
    ctl.until(
        5,
        "DeviceAbsent static-1",
        |e| matches!(e, Event::DeviceAbsent { device, .. } if device.as_str() == "static-1"),
    )
    .await;
    let d = ctl
        .handle
        .device("static-1")
        .expect("absent device stays known");
    assert!(!d.confirmed);
    assert_eq!(ctl.value("static-1", "n", "p000").as_deref(), Some("10"));

    // The static device republishes; the python device did so on reconnect.
    let mut seed = Seeder::new(broker.port, "seeder-2");
    seed.device("static-1", &desc, &int_values(3, 20)).await;
    seed.flush().await;
    ctl.until(10, "static-1 synced again", |e| synced_for(e, "static-1"))
        .await;
    assert_eq!(ctl.value("static-1", "n", "p000").as_deref(), Some("20"));
    for id in devices {
        ctl.eventually(10, &format!("{id} confirmed and synced"), |h| {
            h.device(id).is_some_and(|d| d.confirmed && d.synced)
        })
        .await;
    }
    ctl.shutdown().await;
}

async fn removal_and_return(test: &str, config: Config) {
    let Some(broker) = Broker::start(test, "") else {
        return;
    };
    let mut seed = Seeder::new(broker.port, "seeder");
    seed.device(
        "rm-root",
        &int_description(1, r#","children":["rm-1"]"#),
        &int_values(1, 0),
    )
    .await;
    seed.device(
        "rm-1",
        &int_description(2, r#","root":"rm-root","parent":"rm-root""#),
        &int_values(2, 10),
    )
    .await;
    seed.flush().await;

    let mut ctl = controller(broker.port, config);
    ctl.synced(15, &["rm-root", "rm-1"]).await;

    seed.publish("ebus/5/rm-1/$state", b"", true).await;
    seed.flush().await;
    ctl.until(
        5,
        "DeviceRemoved rm-1",
        |e| matches!(e, Event::DeviceRemoved { device, .. } if device.as_str() == "rm-1"),
    )
    .await;
    assert!(ctl.handle.device("rm-1").is_none());

    // A later zero-length value for the removed device changes nothing.
    let mark = ctl.seen.len();
    seed.publish("ebus/5/rm-1/n/p000", b"", true).await;
    // A marker on a device the controller holds, delivered after it.
    seed.publish("ebus/5/rm-root/n/p000", b"7", true).await;
    seed.flush().await;
    ctl.until(5, "rm-root/n/p000 = 7", |e| {
        matches!(e, Event::PropertyChanged { addr, value, .. }
            if addr.device.as_str() == "rm-root" && &*value.raw == "7")
    })
    .await;
    assert!(ctl.handle.device("rm-1").is_none());

    seed.device(
        "rm-1",
        &int_description(2, r#","root":"rm-root","parent":"rm-root""#),
        &int_values(2, 50),
    )
    .await;
    seed.flush().await;
    ctl.until(
        5,
        "DeviceDiscovered rm-1",
        |e| matches!(e, Event::DeviceDiscovered { device, .. } if device.as_str() == "rm-1"),
    )
    .await;
    assert!(
        !ctl.seen[mark..ctl.seen.len() - 1].iter().any(|e| matches!(
            e,
            Event::PropertyCleared { addr, .. } if addr.device.as_str() == "rm-1"
        )),
        "a removed device's value was cleared: {:?}",
        &ctl.seen[mark..]
    );
    ctl.synced(10, &["rm-1"]).await;
    assert_eq!(ctl.value("rm-1", "n", "p000").as_deref(), Some("50"));
    assert_eq!(ctl.value("rm-1", "n", "p001").as_deref(), Some("51"));
    ctl.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removal_and_return_wildcard() {
    removal_and_return("removal_and_return_wildcard", Config::wildcard()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removal_and_return_tree() {
    removal_and_return(
        "removal_and_return_tree",
        Config::tree("rm-root").expect("valid root"),
    )
    .await;
}

const PACE_CHILDREN: usize = 60;
const PACE_PROPS: usize = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pacing_heals_broker_queue_drops() {
    let Some(broker) = Broker::start(
        "pacing_heals_broker_queue_drops",
        "max_inflight_messages 10\nmax_queued_messages 30",
    ) else {
        return;
    };
    let children: Vec<String> = (0..PACE_CHILDREN).map(|i| format!("pace-{i:02}")).collect();
    let mut seed = Seeder::new(broker.port, "seeder");
    for (i, c) in children.iter().enumerate() {
        seed.device(
            c,
            &int_description(PACE_PROPS, r#","root":"pace-root","parent":"pace-root""#),
            &int_values(PACE_PROPS, i * 100),
        )
        .await;
    }
    let list: Vec<String> = children.iter().map(|c| format!("\"{c}\"")).collect();
    seed.device(
        "pace-root",
        &int_description(1, &format!(r#","children":[{}]"#, list.join(","))),
        &int_values(1, 0),
    )
    .await;
    seed.flush().await;
    let retained = PACE_CHILDREN * (PACE_PROPS + 2) + 3;

    // An unpaced subscriber loses retained messages to the queue limit.
    let mut raw = observe(broker.port, "unpaced", &["ebus/5/#"]).await;
    let mut got = 0;
    while timeout(Duration::from_millis(500), raw.recv())
        .await
        .is_ok()
    {
        got += 1;
    }
    assert!(got < retained, "no drops: {got} of {retained}");

    let mut cfg = Config::tree("pace-root").expect("valid root");
    let mut pacing = Pacing::default();
    pacing.batch_devices = 1;
    pacing.batch_values = 500;
    cfg.pacing = Some(pacing);
    cfg.stuck_timeout_ms = Some(1_000);
    cfg.max_resubscribe_attempts = 8;
    // Whether the controller's own bursts overflow the queue depends on how
    // fast it acknowledges; it must end with every value either way.
    let mut ctl = controller(broker.port, cfg);
    let mut all: Vec<&str> = children.iter().map(String::as_str).collect();
    all.push("pace-root");
    ctl.synced(18, &all).await;
    for (i, c) in children.iter().enumerate() {
        for p in 0..PACE_PROPS {
            assert_eq!(
                ctl.value(c, "n", &format!("p{p:03}")),
                Some((i * 100 + p).to_string()),
                "{c}"
            );
        }
    }
    ctl.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resync_lost_with_its_connection_does_not_stall_the_next() {
    let test = "a_resync_lost_with_its_connection_does_not_stall_the_next";
    let Some(broker) = Broker::start(test, "") else {
        return;
    };
    let proxy = Proxy::start(broker.port).await;
    let mut seed = Seeder::new(broker.port, "seeder");
    seed.device("d1", &int_description(2, ""), &int_values(2, 10))
        .await;
    seed.flush().await;
    let mut ctl = controller(proxy.port, Config::device("d1").expect("valid"));
    ctl.synced(15, &["d1"]).await;

    // A re-sync whose SUBSCRIBEs reach the broker and whose replays are lost
    // with the connection: stage one with the property filters, then the
    // marker.
    proxy.mute(true);
    proxy.cut();
    ctl.until(10, "Link(Up)", |e| {
        matches!(e, Event::Link(Link::Up { .. }))
    })
    .await;
    proxy.wait_subscribes(2, 10).await;
    // Deleted while the controller hears nothing.
    seed.publish("ebus/5/d1/n/p001", b"", true).await;
    seed.flush().await;

    proxy.mute(false);
    proxy.cut();
    ctl.until(
        10,
        "p001 cleared",
        |e| matches!(e, Event::PropertyCleared { addr, .. } if addr.property.as_str() == "p001"),
    )
    .await;
    assert_eq!(ctl.value("d1", "n", "p000").as_deref(), Some("10"));
    assert_eq!(ctl.value("d1", "n", "p001"), None);
    ctl.shutdown().await;
}
