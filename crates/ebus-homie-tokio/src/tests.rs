//! The driver against a fake host: input through the inbox, commands read
//! from the command channel, on tokio's paused clock.

use std::iter;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use ebus_homie::controller::{Config, Event, PropertyAddr, PropertyView, SetError};
use ebus_homie::state::DeviceState;
use ebus_mqtt::{Command, Link, QoS};
use tokio::task::JoinHandle;

use crate::{
    AttachOptions, Attached, Commands, ControllerHandle, Delivered, DeviceSnapshot, DriverError,
    EventReceiver, Inbox, attach,
};

fn t(id: &str, rest: &str) -> String {
    format!("ebus/5/{id}/{rest}")
}

const DESC: &str = r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{"datatype":"boolean","settable":true},"q":{"datatype":"integer"}}}}}"#;

struct Fake {
    handle: ControllerHandle,
    inbox: Inbox,
    commands: Commands,
    events: EventReceiver,
    join: JoinHandle<Result<(), DriverError>>,
}

impl Fake {
    fn start(cfg: Config, opts: AttachOptions) -> Self {
        let a = attach(cfg, opts).expect("valid config");
        Fake::spawn(a)
    }

    fn spawn(a: Attached) -> Self {
        Fake {
            handle: a.handle,
            inbox: a.inbox,
            commands: a.commands,
            events: a.events,
            join: tokio::spawn(a.driver.run()),
        }
    }

    /// Wildcard, connected, with the link-up output read.
    async fn connected() -> Self {
        let mut f = Fake::start(Config::wildcard(), AttachOptions::default());
        f.inbox.connected(false);
        settle().await;
        f.cmds();
        f.events();
        f
    }

    fn r(&self, id: &str, rest: &str, payload: &str) -> bool {
        self.inbox.message(
            t(id, rest),
            Bytes::copy_from_slice(payload.as_bytes()),
            Some(true),
        )
    }

    /// A device announced ready, its burst ended by the marker.
    async fn add(&mut self, id: &str) {
        self.r(id, "$state", "ready");
        self.r(id, "$description", DESC);
        self.r(id, "$description", DESC);
        settle().await;
    }

    fn cmds(&mut self) -> Vec<Command> {
        iter::from_fn(|| self.commands.try_recv()).collect()
    }

    fn events(&mut self) -> Vec<Delivered> {
        iter::from_fn(|| self.events.try_recv()).collect()
    }
}

/// Lets the driver run; on the paused clock this advances time by 3 ms.
async fn settle() {
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Reads snapshots until `changed` reports the driver gone.
async fn seen_last(handle: &mut ControllerHandle) {
    for _ in 0..2 {
        if !handle.changed().await {
            return;
        }
    }
    panic!("changed() kept returning true after the driver stopped");
}

fn subscribed(cmds: &[Command]) -> Vec<String> {
    cmds.iter()
        .filter_map(|c| match c {
            Command::Subscribe { filters } => Some(filters.iter().map(|(f, _)| f.clone())),
            _ => None,
        })
        .flatten()
        .collect()
}

fn addr(id: &str, node: &str, prop: &str) -> PropertyAddr {
    PropertyAddr::new(id, node, prop).expect("valid address")
}

#[test]
fn handle_and_driver_are_send_and_sync() {
    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>(_: &T) {}
    send_sync::<ControllerHandle>();
    send_sync::<Inbox>();
    let a = attach(Config::wildcard(), AttachOptions::default()).expect("valid config");
    send(&a.driver.run());
}

#[test]
fn invalid_config_is_refused() {
    let mut cfg = Config::wildcard();
    cfg.host_filters = vec!["a/#/b".into()];
    assert!(attach(cfg, AttachOptions::default()).is_err());
}

#[tokio::test(start_paused = true)]
async fn link_up_subscribes_discovery_and_reports_connected() {
    let mut f = Fake::start(Config::wildcard(), AttachOptions::default());
    assert!(!f.handle.is_connected());
    f.inbox.connected(false);
    assert!(f.handle.changed().await);
    assert!(f.handle.is_connected());
    let cmds = f.cmds();
    assert_eq!(
        cmds,
        vec![Command::Subscribe {
            filters: vec![("ebus/5/+/$state".into(), QoS::ExactlyOnce)]
        }]
    );
    assert_eq!(
        f.events(),
        vec![Delivered::Event(Event::Link(Link::Up {
            session_present: false
        }))]
    );
    f.inbox.disconnected();
    settle().await;
    assert!(!f.handle.is_connected());
}

#[tokio::test(start_paused = true)]
async fn device_appears_in_snapshot_with_values_and_events() {
    let mut f = Fake::connected().await;
    f.add("d1").await;
    f.r("d1", "n/p", "true");
    f.r("d1", "n/q", "7");
    settle().await;

    let d = f.handle.device("d1").expect("device known");
    assert_eq!(d.state, Some(DeviceState::Ready));
    assert_eq!(f.handle.effective_state("d1"), Some(DeviceState::Ready));
    assert!(d.description.is_some());
    assert_eq!(
        d.property("n", "p")
            .and_then(|p| p.value.as_ref())
            .map(|v| &*v.raw),
        Some("true")
    );
    assert!(f.handle.is_tree_complete("d1"));
    assert_eq!(f.handle.devices().len(), 1);

    let ev: Vec<Event> = f
        .events()
        .into_iter()
        .filter_map(|d| match d {
            Delivered::Event(e) => Some(e),
            Delivered::Overflow { .. } => None,
        })
        .collect();
    assert!(
        ev.iter()
            .any(|e| matches!(e, Event::DeviceDiscovered { .. }))
    );
    assert!(
        ev.iter()
            .any(|e| matches!(e, Event::DeviceSynced { missing: 0, .. }))
    );
    let subs = subscribed(&f.cmds());
    assert!(subs.contains(&t("d1", "$description")));
    assert!(subs.contains(&t("d1", "+/+")));
}

#[tokio::test(start_paused = true)]
async fn snapshots_share_unchanged_devices() {
    let mut f = Fake::connected().await;
    f.add("d1").await;
    f.add("d2").await;
    let d1 = f.handle.device("d1").expect("d1");
    let d2 = f.handle.device("d2").expect("d2");

    f.r("d1", "n/q", "5");
    assert!(f.handle.changed().await);
    settle().await;
    let d1b = f.handle.device("d1").expect("d1");
    let d2b = f.handle.device("d2").expect("d2");
    assert!(!Arc::ptr_eq(&d1, &d1b));
    assert!(Arc::ptr_eq(&d2, &d2b));

    // An identical retained repeat changes nothing.
    f.r("d1", "n/q", "5");
    settle().await;
    assert!(Arc::ptr_eq(&d1b, &f.handle.device("d1").expect("d1")));

    // Removal leaves the snapshot.
    f.inbox.message(t("d2", "$state"), Bytes::new(), Some(true));
    settle().await;
    assert!(f.handle.device("d2").is_none());
    assert!(f.handle.device("d1").is_some());
}

#[tokio::test(start_paused = true)]
async fn set_and_try_set_queue_publishes() {
    let mut f = Fake::connected().await;
    f.add("d1").await;
    f.cmds();

    f.handle
        .set(addr("d1", "n", "p"), "false")
        .await
        .expect("set accepted");
    f.handle
        .try_set(addr("d1", "n", "p"), "true")
        .expect("try_set accepted");
    settle().await;
    let publishes: Vec<(String, Vec<u8>)> = f
        .cmds()
        .into_iter()
        .filter_map(|c| match c {
            Command::Publish {
                topic,
                payload,
                qos: QoS::AtMostOnce,
                retain: false,
            } => Some((topic, payload)),
            _ => None,
        })
        .collect();
    assert_eq!(
        publishes,
        vec![
            (t("d1", "n/p/set"), b"false".to_vec()),
            (t("d1", "n/p/set"), b"true".to_vec()),
        ]
    );

    for (a, payload, want) in [
        (addr("d1", "n", "p"), "maybe", "invalid"),
        (addr("d1", "n", "q"), "1", "notSettable"),
        (addr("d1", "n", "x"), "1", "unknownProperty"),
        (addr("dx", "n", "p"), "1", "unknownDevice"),
    ] {
        let got = f.handle.set(a.clone(), payload).await.expect_err("refused");
        assert_eq!(got.as_str(), want);
        let got = f.handle.try_set(a, payload).expect_err("refused");
        assert_eq!(got.as_str(), want);
    }

    f.handle
        .broadcast("alarm", "on")
        .await
        .expect("broadcast accepted");
    assert_eq!(
        f.handle.broadcast("a/$x", "on").await,
        Err(SetError::InvalidTopic)
    );
    settle().await;
    assert!(f.cmds().iter().any(|c| matches!(
        c,
        Command::Publish { topic, .. } if topic == "ebus/5/$broadcast/alarm"
    )));

    f.inbox.disconnected();
    settle().await;
    assert_eq!(
        f.handle.try_set(addr("d1", "n", "p"), "true"),
        Err(SetError::NotConnected)
    );
    assert_eq!(
        f.handle.set(addr("d1", "n", "p"), "true").await,
        Err(SetError::NotConnected)
    );
}

#[tokio::test(start_paused = true)]
async fn inbox_over_its_byte_cap_drops_and_resubscribes() {
    let opts = AttachOptions {
        inbox_bytes: 64,
        ..AttachOptions::default()
    };
    let a = attach(Config::wildcard(), opts).expect("valid config");
    let inbox = a.inbox.clone();
    inbox.connected(false);
    assert!(inbox.message(t("d1", "$state"), Bytes::from_static(b"ready"), Some(true)));
    // Past the cap while the driver has not run yet: fits alone, not beside
    // the queued message.
    assert!(!inbox.message(
        t("d1", "$description"),
        Bytes::from(vec![b'{'; 40]),
        Some(true)
    ));
    // Larger than the cap on its own.
    assert!(!inbox.message(t("d1", "$state"), Bytes::from(vec![b'x'; 64]), Some(true)));
    // Link changes are never dropped.
    inbox.disconnected();
    inbox.connected(false);

    let mut f = Fake::spawn(a);
    settle().await;
    let cmds = f.cmds();
    let subs = subscribed(&cmds);
    // Stage one for d1 after each link-up, then the resubscribe for the drop.
    let desc_subs = subs
        .iter()
        .filter(|s| **s == t("d1", "$description"))
        .count();
    assert!(desc_subs >= 2, "{cmds:?}");
    let links: Vec<_> = f
        .events()
        .into_iter()
        .filter(|d| matches!(d, Delivered::Event(Event::Link(_))))
        .collect();
    assert_eq!(links.len(), 3);
    assert!(f.handle.is_connected());

    // The queue drained, so there is room again.
    assert!(f.r("d1", "$state", "ready"));
}

#[tokio::test(start_paused = true)]
async fn dropped_state_of_unknown_device_resubscribes_discovery() {
    let opts = AttachOptions {
        inbox_bytes: 32,
        ..AttachOptions::default()
    };
    let mut f = Fake::start(Config::wildcard(), opts);
    f.inbox.connected(false);
    settle().await;
    f.cmds();
    assert!(f.r("d8", "$state", "ready"));
    assert!(!f.r("d9", "$state", "ready"));
    settle().await;
    assert_eq!(
        subscribed(&f.cmds()),
        [t("d8", "$description"), "ebus/5/+/$state".to_string()]
    );
}

#[tokio::test(start_paused = true)]
async fn a_drop_reported_late_does_not_let_a_later_marker_clear() {
    let mut cfg = Config::wildcard();
    cfg.pacing = None;
    let opts = AttachOptions {
        inbox_bytes: 512,
        ..AttachOptions::default()
    };
    let mut f = Fake::start(cfg, opts);
    f.inbox.connected(false);
    settle().await;
    f.add("d1").await;
    f.r("d1", "n/p", "true");
    f.r("d1", "n/q", "2");
    settle().await;
    f.inbox.disconnected();
    f.inbox.connected(false);
    settle().await;
    f.cmds();
    f.events();
    // One batch: the replay of q is dropped, and the marker after it fits.
    assert!(f.r("d1", "$description", DESC));
    assert!(!f.r("d1", "n/q", &"9".repeat(400)));
    assert!(f.r("d1", "n/p", "true"));
    assert!(f.r("d1", "$description", DESC));
    settle().await;
    let ev = f.events();
    assert!(
        !ev.iter()
            .any(|d| matches!(d, Delivered::Event(Event::PropertyCleared { .. }))),
        "{ev:?}"
    );
    let d = f.handle.device("d1").expect("device known");
    assert!(
        d.property("n", "q")
            .and_then(|p| p.value.as_ref())
            .is_some()
    );
}

#[tokio::test(start_paused = true)]
async fn a_message_larger_than_the_inbox_is_not_healed() {
    let opts = AttachOptions {
        inbox_bytes: 1024,
        ..AttachOptions::default()
    };
    let mut f = Fake::start(Config::wildcard(), opts);
    f.inbox.connected(false);
    settle().await;
    f.add("d1").await;
    f.cmds();
    f.events();
    let big = "x".repeat(2048);
    for _ in 0..3 {
        assert!(!f.r("d1", "n/q", &big));
        settle().await;
        assert!(
            f.cmds().is_empty(),
            "no resubscribe for a message that can never fit"
        );
    }
}

#[test]
fn the_driver_yields_between_batches() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let a = attach(Config::wildcard(), AttachOptions::default()).expect("valid config");
        let inbox = a.inbox.clone();
        inbox.connected(false);
        for i in 0..5_000 {
            inbox.message(
                t(&format!("d{i}"), "$state"),
                Bytes::from_static(b"ready"),
                Some(true),
            );
        }
        let shared = Arc::clone(&inbox.shared);
        let driver = tokio::spawn(a.driver.run());
        // Spawned after the driver, so it runs once the driver first yields.
        let probe =
            tokio::spawn(async move { shared.bytes.load(std::sync::atomic::Ordering::Acquire) });
        let queued = probe.await.expect("probe");
        assert!(
            queued > 0,
            "the driver ran the whole backlog without yielding"
        );
        drop(inbox);
        driver.abort();
    });
}

#[tokio::test(start_paused = true)]
async fn full_event_channel_reports_overflow() {
    let opts = AttachOptions {
        event_capacity: 2,
        ..AttachOptions::default()
    };
    let mut f = Fake::start(Config::wildcard(), opts);
    f.inbox.connected(false);
    for i in 0..5 {
        f.r(&format!("d{i}"), "$state", "ready");
    }
    settle().await;
    // Link(Up) and the first device's first event fit; the rest were counted.
    let first = f.events.recv().await;
    assert!(matches!(first, Some(Delivered::Event(Event::Link(_)))));
    let second = f.events.recv().await;
    assert!(matches!(second, Some(Delivered::Event(_))));
    let third = f.events.recv().await;
    let Some(Delivered::Overflow { dropped }) = third else {
        panic!("expected overflow, got {third:?}");
    };
    assert!(dropped > 0);
    // Events flow again afterwards.
    f.r("d0", "$state", "init");
    settle().await;
    let next = f.events.recv().await;
    assert!(matches!(
        next,
        Some(Delivered::Event(Event::StateChanged { .. }))
    ));
    // The snapshot holds every device regardless.
    assert_eq!(f.handle.devices().len(), 5);
}

#[tokio::test(start_paused = true)]
async fn deadlines_fire_on_the_tokio_clock() {
    let mut f = Fake::start(
        Config::tree("root").expect("valid"),
        AttachOptions::default(),
    );
    f.inbox.connected(false);
    settle().await;
    let first = subscribed(&f.cmds());
    assert!(first.contains(&t("root", "$state")));
    f.events();
    // Nothing arrives; stuck healing resubscribes after 10 s.
    tokio::time::sleep(Duration::from_millis(10_050)).await;
    let again = subscribed(&f.cmds());
    assert!(again.contains(&t("root", "$state")), "{again:?}");
    assert!(
        f.events()
            .iter()
            .any(|d| matches!(d, Delivered::Event(Event::DeviceStuck { attempt: 1, .. })))
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_unsubscribes_and_stops_the_driver() {
    let mut f = Fake::connected().await;
    f.add("d1").await;
    f.cmds();
    f.handle.shutdown().await;
    let res = (&mut f.join).await.expect("driver task");
    assert_eq!(res, Ok(()));
    let cmds = f.cmds();
    let unsubs: Vec<&String> = cmds
        .iter()
        .filter_map(|c| match c {
            Command::Unsubscribe { filters } => Some(filters.iter()),
            _ => None,
        })
        .flatten()
        .collect();
    assert!(unsubs.contains(&&"ebus/5/+/$state".to_string()), "{cmds:?}");
    assert!(!f.handle.is_connected());
    assert!(f.handle.device("d1").is_some());
    seen_last(&mut f.handle).await;
    // Calls after the stop return at once.
    f.handle.shutdown().await;
    assert_eq!(
        f.handle.set(addr("d1", "n", "p"), "true").await,
        Err(SetError::NotConnected)
    );
    assert!(!f.r("d1", "$state", "ready"));
}

#[tokio::test(start_paused = true)]
async fn dropping_every_inbox_stops_the_driver() {
    let f = Fake::start(Config::wildcard(), AttachOptions::default());
    let Fake { inbox, join, .. } = f;
    drop(inbox);
    assert_eq!(join.await.expect("driver task"), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn dropped_command_receiver_is_an_error() {
    let f = Fake::start(Config::wildcard(), AttachOptions::default());
    let Fake {
        inbox,
        commands,
        join,
        ..
    } = f;
    drop(commands);
    inbox.connected(false);
    assert_eq!(
        join.await.expect("driver task"),
        Err(DriverError::CommandsClosed)
    );
}

#[tokio::test(start_paused = true)]
async fn cancelled_driver_releases_the_handles() {
    let a = attach(Config::wildcard(), AttachOptions::default()).expect("valid config");
    let mut handle = a.handle.clone();
    let mut events = a.events;
    let inbox = a.inbox.clone();
    inbox.connected(false);
    inbox.message(t("d1", "$state"), Bytes::from_static(b"ready"), Some(true));
    // Run the driver until it parks on its select, then drop it there.
    let run = a.driver.run();
    tokio::select! {
        _ = run => panic!("driver stopped on its own"),
        () = tokio::time::sleep(Duration::from_millis(5)) => {}
    }
    assert!(handle.is_connected());
    assert!(handle.device("d1").is_some());
    seen_last(&mut handle).await;
    assert_eq!(
        handle.set(addr("d1", "n", "p"), "true").await,
        Err(SetError::NotConnected)
    );
    assert_eq!(
        handle.try_set(addr("d1", "n", "p"), "true"),
        Err(SetError::UnknownProperty)
    );
    // Queued deliveries are still readable, then the channel ends.
    let mut n = 0;
    while events.recv().await.is_some() {
        n += 1;
    }
    assert!(n >= 2);
    assert!(!inbox.message(t("d1", "$state"), Bytes::from_static(b"init"), Some(true)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbox_works_from_a_std_thread() {
    let mut f = Fake::start(Config::wildcard(), AttachOptions::default());
    let inbox = f.inbox.clone();
    let reader = std::thread::spawn(move || {
        inbox.connected(false);
        for i in 0..200 {
            assert!(inbox.message(
                t(&format!("d{i}"), "$state"),
                Bytes::from_static(b"ready"),
                Some(true)
            ));
        }
    });
    reader.join().expect("reader thread");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while f.handle.devices().len() < 200 {
        assert!(tokio::time::Instant::now() < deadline, "devices missing");
        tokio::time::timeout(Duration::from_millis(100), f.handle.changed())
            .await
            .ok();
    }
    assert!(f.handle.is_connected());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_reflects_an_event_when_it_is_delivered() {
    let mut f = Fake::start(Config::wildcard(), AttachOptions::default());
    f.inbox.connected(false);
    for i in 0..50 {
        f.r(&format!("d{i}"), "$state", "ready");
    }
    let mut seen = 0;
    while seen < 50 {
        let d = tokio::time::timeout(Duration::from_secs(10), f.events.recv())
            .await
            .expect("event in time")
            .expect("driver running");
        if let Delivered::Event(Event::DeviceDiscovered { device, .. }) = d {
            assert!(f.handle.device(device.as_str()).is_some(), "{device}");
            seen += 1;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_changed_device_shares_its_unchanged_values() {
    let mut f = Fake::connected().await;
    f.add("d1").await;
    f.r("d1", "n/p", "true");
    f.r("d1", "n/q", "5");
    settle().await;
    let before = f.handle.device("d1").expect("d1");
    f.r("d1", "n/q", "6");
    settle().await;
    let after = f.handle.device("d1").expect("d1");
    assert!(!Arc::ptr_eq(&before, &after));
    let p = |d: &DeviceSnapshot| d.property("n", "p").expect("declared") as *const PropertyView;
    assert_eq!(p(&before), p(&after), "the unchanged slot was copied");
    let raw = |d: &DeviceSnapshot| {
        d.property("n", "p")
            .and_then(|v| v.value.as_ref())
            .map(|v| Arc::clone(&v.raw))
            .expect("value")
    };
    assert!(Arc::ptr_eq(&raw(&before), &raw(&after)));
    assert_eq!(
        after
            .property("n", "q")
            .and_then(|v| v.value.as_ref())
            .map(|v| &*v.raw),
        Some("6")
    );
    f.events();
    // A live repeat publishes nothing.
    f.inbox
        .message(t("d1", "n/q"), Bytes::from_static(b"6"), Some(false));
    settle().await;
    assert!(Arc::ptr_eq(&after, &f.handle.device("d1").expect("d1")));
    assert!(
        !f.events()
            .iter()
            .any(|d| matches!(d, Delivered::Event(Event::PropertyChanged { .. })))
    );
}

#[tokio::test(start_paused = true)]
async fn subscribes_queued_before_a_clean_link_up_are_skipped() {
    let mut cfg = Config::wildcard();
    cfg.link_up_discards_queued = true;
    let mut f = Fake::start(cfg, AttachOptions::default());
    f.inbox.connected(false);
    settle().await;
    f.cmds();
    f.add("d1").await;
    f.cmds();
    // Stage one for d2, queued but not read by the executor.
    f.r("d2", "$state", "ready");
    settle().await;
    // Stage one for d3, queued after the CONNACK is reported and before the
    // driver handles it.
    f.r("d3", "$state", "ready");
    f.inbox.disconnected();
    f.inbox.connected(false);
    assert_eq!(subscribed(&f.cmds()), Vec::<String>::new());
    settle().await;
    let subs = subscribed(&f.cmds());
    assert_eq!(subs.first().map(String::as_str), Some("ebus/5/+/$state"));
    for d in ["d2", "d3"] {
        let n = subs.iter().filter(|s| **s == t(d, "$description")).count();
        assert_eq!(n, 1, "{subs:?}");
    }
}

/// Answers every SUBSCRIBE the executor reads, in order, with the retained
/// messages its filters match, until no command is left.
async fn answer(f: &mut Fake, retained: &[(String, &str)]) {
    loop {
        let cmds = f.cmds();
        if cmds.is_empty() {
            return;
        }
        for filter in subscribed(&cmds) {
            for (topic, payload) in retained {
                if ebus_mqtt::filter::matches(&filter, topic) {
                    f.inbox.message(
                        topic.as_str(),
                        Bytes::copy_from_slice(payload.as_bytes()),
                        Some(true),
                    );
                }
            }
        }
        settle().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_queued_subscribe_is_skipped_only_when_the_host_discards() {
    for discards in [false, true] {
        let mut cfg = Config::device("d1").expect("valid");
        cfg.link_up_discards_queued = discards;
        let mut f = Fake::start(cfg, AttachOptions::default());
        f.inbox.connected(false);
        settle().await;
        let mut retained = vec![
            (t("d1", "$state"), "ready"),
            (t("d1", "$description"), DESC),
            (t("d1", "n/p"), "true"),
            (t("d1", "n/q"), "5"),
        ];
        answer(&mut f, &retained).await;
        assert!(
            f.handle
                .device("d1")
                .is_some_and(|d| d.property("n", "p").is_some_and(|p| p.value.is_some()))
        );
        f.events();
        // p is deleted at the broker. Two clean reconnects follow before the
        // executor reads the first one's re-sync.
        retained.retain(|(topic, _)| *topic != t("d1", "n/p"));
        for _ in 0..2 {
            f.inbox.disconnected();
            f.inbox.connected(false);
            settle().await;
        }
        answer(&mut f, &retained).await;
        let cleared = f.events().iter().any(|d| {
            matches!(d, Delivered::Event(Event::PropertyCleared { addr }) if addr.property.as_str() == "p")
        });
        assert!(cleared, "discards={discards}");
        let d = f.handle.device("d1").expect("d1");
        assert_eq!(d.property("n", "p").and_then(|p| p.value.as_ref()), None);
    }
}

#[tokio::test(start_paused = true)]
async fn a_description_change_shares_the_unchanged_slots() {
    let mut f = Fake::connected().await;
    f.add("d1").await;
    f.r("d1", "n/p", "true");
    f.r("d1", "n/q", "5");
    settle().await;
    let before = f.handle.device("d1").expect("d1");
    let wider = DESC.replace(
        r#""q":{"datatype":"integer"}"#,
        r#""q":{"datatype":"integer"},"r":{"datatype":"integer"}"#,
    );
    f.inbox
        .message(t("d1", "$description"), Bytes::from(wider), Some(false));
    settle().await;
    let after = f.handle.device("d1").expect("d1");
    assert!(
        after.property("n", "r").is_some(),
        "the new description applied"
    );
    let p = |d: &DeviceSnapshot, prop: &str| {
        d.property("n", prop).expect("declared") as *const PropertyView
    };
    assert_eq!(p(&before, "p"), p(&after, "p"));
    assert_eq!(p(&before, "q"), p(&after, "q"));
}

#[tokio::test(start_paused = true)]
async fn value_traffic_does_not_rescan_the_trees() {
    let mut f = Fake::connected().await;
    f.add("d1").await;
    let scans = |f: &Fake| f.inbox.shared.tree_scans.load(Ordering::Relaxed);
    let base = scans(&f);
    f.r("d1", "n/q", "5");
    settle().await;
    f.inbox
        .message(t("d1", "n/q"), Bytes::from_static(b"6"), Some(false));
    settle().await;
    f.inbox
        .message(t("d1", "n/q"), Bytes::from_static(b"6"), Some(false));
    settle().await;
    assert_eq!(scans(&f), base, "a value changes no tree");
    assert_eq!(
        f.handle.device("d1").and_then(|d| d
            .property("n", "q")
            .and_then(|v| v.value.as_ref())
            .map(|v| v.raw.to_string())),
        Some("6".to_owned())
    );
    // A description is.
    f.add("d2").await;
    assert!(scans(&f) > base);
}
