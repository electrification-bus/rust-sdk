//! The device driver and its handles against a fake host: input through the
//! inbox, commands read from the command channel, on tokio's paused clock.

use std::iter;
use std::time::Duration;

use bytes::Bytes;
use ebus_homie::controller::Config;
use ebus_homie::device::{DeviceSpec, NodeIdx, NodeSpec, PropSpec, TreeConfig};
use ebus_homie::state::DeviceState;
use ebus_mqtt::{Command, QoS, Receipt};
use tokio::task::JoinHandle;

use super::*;
use crate::{Delivered, DriverError};

/// One command as text: `topic=payload` for a retained publish (`desc` for
/// a `$description`, `<clear>` for a zero-length one), `topic~payload` for a
/// non-retained one, `SUB a,b` and `UNSUB a,b`.
fn show(c: &Command) -> String {
    match c {
        Command::Publish {
            topic,
            payload,
            qos,
            retain,
        } => {
            let text = String::from_utf8_lossy(payload);
            if !retain {
                format!("{topic}~{text} q{}", *qos as u8)
            } else if payload.is_empty() {
                format!("{topic}=<clear>")
            } else if topic.ends_with("/$description") {
                format!("{topic}=desc")
            } else {
                format!("{topic}={text}")
            }
        }
        Command::Subscribe { filters } => {
            let f: Vec<&str> = filters.iter().map(|(f, _)| f.as_str()).collect();
            format!("SUB {}", f.join(","))
        }
        Command::Unsubscribe { filters } => format!("UNSUB {}", filters.join(",")),
        _ => String::from("?"),
    }
}

/// Lets the driver run; on the paused clock this advances time by 3 ms.
async fn settle() {
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Root `r` with node `n`: `p: f64`, settable `s: bool`, event `e: i64`.
struct Built {
    b: TreeBuilder,
    n: NodeIdx,
    p: Property<f64>,
    s: Settable<bool>,
    e: Event<i64>,
}

fn build(cfg: TreeConfig) -> Built {
    let mut b = TreeBuilder::new(cfg, "r", DeviceSpec::default()).expect("builder");
    let n = b.node(b.root(), "n", NodeSpec::default()).expect("n");
    let p = b.property::<f64>(n, "p", PropSpec::new()).expect("p");
    let s = b.settable::<bool>(n, "s", PropSpec::new()).expect("s");
    let e = b.event::<i64>(n, "e", PropSpec::new()).expect("e");
    Built { b, n, p, s, e }
}

struct Fake {
    handle: DeviceHandle,
    inbox: Inbox,
    commands: Commands,
    join: JoinHandle<Result<(), DriverError>>,
}

impl Fake {
    fn start(b: TreeBuilder, opts: DeviceOptions) -> Self {
        let a = attach_device(b, opts).expect("attach");
        Fake {
            handle: a.handle,
            inbox: a.inbox,
            commands: a.commands,
            join: tokio::spawn(a.driver.run()),
        }
    }

    fn cmds(&mut self) -> Vec<String> {
        iter::from_fn(|| self.commands.try_recv())
            .map(|c| show(&c))
            .collect()
    }

    fn set(&self, topic: &str, payload: &str) {
        self.inbox.message(
            topic,
            Bytes::copy_from_slice(payload.as_bytes()),
            Some(false),
        );
    }

    /// Connected, with the first refresh read.
    async fn up(&mut self) -> Vec<String> {
        self.inbox.connected(false);
        settle().await;
        self.cmds()
    }
}

const BOOT: [&str; 5] = [
    "SUB ebus/5/r/+/+/set",
    "ebus/5/r/$state=init",
    "ebus/5/r/$description=desc",
    "ebus/5/r/n/p=1.5",
    "ebus/5/r/$state=ready",
];

#[test]
fn handles_and_drivers_are_send_and_sync() {
    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>(_: &T) {}
    send_sync::<DeviceHandle>();
    send_sync::<Property<f64>>();
    send_sync::<Event<String>>();
    send::<Settable<bool>>(&build(TreeConfig::default()).s);
    let a =
        attach_device(build(TreeConfig::default()).b, DeviceOptions::default()).expect("attach");
    send(&a.driver.run());
    let p = attach_pair(
        build(TreeConfig::default()).b,
        Config::wildcard(),
        DeviceOptions::default(),
    )
    .expect("attach");
    send(&p.driver.run());
}

#[tokio::test(start_paused = true)]
async fn a_value_set_before_the_driver_runs_goes_out_in_the_first_refresh() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("set before attach");
    assert_eq!(t.p.get(), Some(1.5));
    let mut f = Fake::start(t.b, DeviceOptions::default());
    settle().await;
    assert!(f.cmds().is_empty(), "nothing before the first link-up");
    assert!(!f.handle.is_connected());
    assert_eq!(f.up().await, BOOT);
    assert!(f.handle.is_connected());
}

#[tokio::test(start_paused = true)]
async fn an_unchanged_value_publishes_and_notifies_nothing() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let p = t.p.clone();
    let mut watch = p.subscribe();
    watch.mark_unchanged();
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    p.set(1.5).expect("same");
    settle().await;
    assert!(f.cmds().is_empty());
    assert!(!watch.has_changed().expect("open"), "no observer notified");
    p.set(2.0).expect("changed");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/p=2.0"]);
    assert!(watch.has_changed().expect("open"));
}

#[tokio::test(start_paused = true)]
async fn values_that_come_back_before_a_drain_publish_nothing() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let p = t.p.clone();
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    // No await between the sets: the driver drains once, after them.
    p.set(2.0).expect("a");
    p.set(3.0).expect("b");
    p.set(1.5).expect("back");
    settle().await;
    assert!(f.cmds().is_empty());
    p.set(4.0).expect("a");
    p.set(5.0).expect("b");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/p=5.0"], "coalesced to the latest");
}

#[tokio::test(start_paused = true)]
async fn values_set_while_down_go_out_once_in_the_refresh() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let p = t.p.clone();
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    f.inbox.disconnected();
    settle().await;
    p.set(2.0).expect("a");
    settle().await;
    p.set(3.0).expect("b");
    settle().await;
    assert!(f.cmds().is_empty(), "nothing while down");
    f.inbox.connected(false);
    settle().await;
    assert_eq!(
        f.cmds(),
        [
            "SUB ebus/5/r/+/+/set",
            "ebus/5/r/$description=desc",
            "ebus/5/r/n/p=3.0",
            "ebus/5/r/$state=ready",
        ],
        "no init: the description did not change"
    );
}

#[tokio::test(start_paused = true)]
async fn invalid_values_are_refused_on_the_callers_thread() {
    let mut b = TreeBuilder::new(TreeConfig::default(), "r", DeviceSpec::default()).expect("b");
    let n = b.node(b.root(), "n", NodeSpec::default()).expect("n");
    let pct = b
        .property::<f64>(n, "pct", PropSpec::new().format("0:100"))
        .expect("pct");
    let small = b.property::<u8>(n, "small", PropSpec::new()).expect("u8");
    let e = b.event::<f64>(n, "e", PropSpec::new()).expect("e");
    assert_eq!(pct.set(100.5), Err(ValueError::OutOfRange));
    assert_eq!(pct.set(f64::NAN), Err(ValueError::NotFinite));
    assert_eq!(pct.get(), None, "a refused value is not stored");
    assert_eq!(small.set(255), Ok(()));
    assert_eq!(
        e.emit(f64::INFINITY),
        Err(EmitError::Invalid(ValueError::NotFinite))
    );
    assert_eq!(e.emit(1.0), Err(EmitError::Down), "not connected yet");
}

#[tokio::test(start_paused = true)]
async fn events_publish_every_call_and_are_refused_while_down_or_full() {
    let t = build(TreeConfig::default());
    let e = t.e.clone();
    let opts = DeviceOptions {
        event_capacity: 2,
        ..DeviceOptions::default()
    };
    let mut f = Fake::start(t.b, opts);
    f.up().await;
    e.emit(7).expect("one");
    e.emit(7).expect("same value again");
    assert_eq!(e.emit(7), Err(EmitError::Full), "two already queued");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/e~7 q0", "ebus/5/r/n/e~7 q0"]);
    f.inbox.disconnected();
    settle().await;
    assert_eq!(e.emit(8), Err(EmitError::Down));
}

#[tokio::test(start_paused = true)]
async fn a_set_is_delivered_and_accept_reflects_it() {
    let mut t = build(TreeConfig::default());
    t.s.set(false).expect("s");
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    f.set("ebus/5/r/n/s/set", "true");
    settle().await;
    let req = t.s.try_next_set().expect("request");
    assert!(*req.value());
    assert_eq!(req.raw(), b"true");
    req.accept();
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/s=true"]);
    // A /set equal to the value held is still delivered; accepting it
    // publishes nothing.
    f.set("ebus/5/r/n/s/set", "true");
    settle().await;
    t.s.try_next_set().expect("delivered").accept();
    settle().await;
    assert!(f.cmds().is_empty());
    // accept_as reflects the value reached; reject publishes nothing.
    f.set("ebus/5/r/n/s/set", "false");
    settle().await;
    t.s.try_next_set().expect("request").accept_as(true);
    f.set("ebus/5/r/n/s/set", "false");
    settle().await;
    t.s.try_next_set().expect("request").reject("relayFault");
    settle().await;
    assert!(f.cmds().is_empty(), "the value is already true");
    assert_eq!(f.handle.rejected_sets(), 0);
}

#[tokio::test(start_paused = true)]
async fn refused_sets_are_counted_and_deliver_nothing() {
    let mut t = build(TreeConfig::default());
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    f.set("ebus/5/r/n/s/set", "maybe");
    f.set("ebus/5/r/n/p/set", "1.0");
    settle().await;
    assert_eq!(f.handle.rejected_sets(), 2);
    assert!(t.s.try_next_set().is_none());
    assert!(f.cmds().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_full_set_queue_drops_the_set() {
    let mut t = build(TreeConfig::default());
    let opts = DeviceOptions {
        set_capacity: 1,
        ..DeviceOptions::default()
    };
    let mut f = Fake::start(t.b, opts);
    f.up().await;
    f.set("ebus/5/r/n/s/set", "true");
    f.set("ebus/5/r/n/s/set", "false");
    settle().await;
    assert!(*t.s.try_next_set().expect("first").value());
    assert!(t.s.try_next_set().is_none(), "the second was dropped");
    f.set("ebus/5/r/n/s/set", "false");
    settle().await;
    assert!(
        !*t.s.try_next_set().expect("room again").value(),
        "receiving frees the slot"
    );
}

#[tokio::test(start_paused = true)]
async fn target_mode_echoes_the_raw_set_and_closes_on_set() {
    let mut b = TreeBuilder::new(TreeConfig::default(), "r", DeviceSpec::default()).expect("b");
    let n = b.node(b.root(), "n", NodeSpec::default()).expect("n");
    let mut sp = b
        .settable::<f64>(n, "sp", PropSpec::new().target(true))
        .expect("sp");
    sp.set(20.0).expect("initial");
    let mut f = Fake::start(b, DeviceOptions::default());
    let boot = f.up().await;
    assert_eq!(
        &boot[3..5],
        ["ebus/5/r/n/sp/$target=20.0", "ebus/5/r/n/sp=20.0"],
        "{boot:?}"
    );
    f.set("ebus/5/r/n/sp/set", "21.50");
    settle().await;
    let req = sp.try_next_set().expect("request");
    assert_eq!(*req.value(), 21.5);
    req.accept();
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/sp/$target=21.50"]);
    sp.progress(21.0).expect("progress");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/sp=21.0"]);
    sp.set(21.5).expect("reached");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/sp=21.5"], "target already equal");
    sp.set(22.0).expect("other");
    settle().await;
    assert_eq!(
        f.cmds(),
        ["ebus/5/r/n/sp/$target=22.0", "ebus/5/r/n/sp=22.0"]
    );
    sp.set_target(23.0).expect("local command");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/sp/$target=23.0"]);
    sp.clear().expect("clear");
    settle().await;
    assert_eq!(
        f.cmds(),
        ["ebus/5/r/n/sp=<clear>", "ebus/5/r/n/sp/$target=<clear>"]
    );
    assert_eq!(t_progress_not_target(), Err(ValueError::NotTarget));
}

fn t_progress_not_target() -> Result<(), ValueError> {
    build(TreeConfig::default()).s.progress(true)
}

#[tokio::test(start_paused = true)]
async fn credit_limits_the_commands_outstanding() {
    let mut b = TreeBuilder::new(TreeConfig::default(), "r", DeviceSpec::default()).expect("b");
    let n = b.node(b.root(), "n", NodeSpec::default()).expect("n");
    let props: Vec<Property<i64>> = (0..10)
        .map(|i| {
            b.property::<i64>(n, &format!("p{i}"), PropSpec::new())
                .expect("p")
        })
        .collect();
    for (i, p) in props.iter().enumerate() {
        p.set(i as i64).expect("set");
    }
    let opts = DeviceOptions {
        max_outstanding: 3,
        ..DeviceOptions::default()
    };
    let mut f = Fake::start(b, opts);
    f.inbox.connected(false);
    settle().await;
    let mut all = Vec::new();
    loop {
        // Nothing more than the limit is handed out before it is received.
        let queued = f.commands.rx.len();
        assert!(queued <= 3, "{queued} outstanding");
        let Some(c) = f.commands.try_recv() else {
            break;
        };
        all.push(show(&c));
        settle().await;
    }
    assert_eq!(all.len(), 1 + 2 + 10 + 1, "{all:?}");
    // A value changed while out of credit waits, then goes out at its latest.
    for p in &props {
        p.set(100).expect("set");
    }
    settle().await;
    let mut more = Vec::new();
    while let Some(c) = f.commands.try_recv() {
        more.push(show(&c));
        settle().await;
    }
    assert_eq!(more.len(), 10);
    assert!(more.iter().all(|c| c.ends_with("=100")), "{more:?}");
}

#[tokio::test(start_paused = true)]
async fn many_slots_grow_the_dirty_sets() {
    let mut b = TreeBuilder::new(TreeConfig::default(), "r", DeviceSpec::default()).expect("b");
    let n = b.node(b.root(), "n", NodeSpec::default()).expect("n");
    let props: Vec<Property<i64>> = (0..600)
        .map(|i| {
            b.property::<i64>(n, &format!("p{i}"), PropSpec::new())
                .expect("p")
        })
        .collect();
    let mut f = Fake::start(b, DeviceOptions::default());
    f.up().await;
    for p in &props {
        p.set(1).expect("set");
    }
    settle().await;
    assert_eq!(f.cmds().len(), 600);
}

#[tokio::test(start_paused = true)]
async fn presence_alerts_and_the_state_watch() {
    let t = build(TreeConfig::default());
    let root = t.b.root();
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    let mut state = f.handle.state(root);
    assert_eq!(*state.borrow_and_update(), Some(DeviceState::Ready));
    f.handle
        .set_presence(root, Presence::Ready)
        .await
        .expect("same");
    settle().await;
    assert!(f.cmds().is_empty());
    assert!(
        !state.has_changed().expect("open"),
        "an unchanged state notifies nothing"
    );
    f.handle
        .set_presence(root, Presence::Sleeping)
        .await
        .expect("sleep");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/$state=sleeping"]);
    assert_eq!(*state.borrow_and_update(), Some(DeviceState::Sleeping));
    assert_eq!(
        f.handle.set_presence(root, Presence::Lost).await,
        Err(ValueError::NotForRoot)
    );
    f.handle
        .raise_alert(root, "hot", "too hot")
        .await
        .expect("alert");
    assert_eq!(
        f.handle.raise_alert(root, "Bad Id", "x").await,
        Err(AlertError::InvalidId)
    );
    f.handle.clear_alert(root, "hot").await.expect("clear");
    settle().await;
    assert_eq!(
        f.cmds(),
        ["ebus/5/r/$alert/hot=too hot", "ebus/5/r/$alert/hot=<clear>"]
    );
    f.handle.declare_lost().await;
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/$state=lost"]);
}

#[tokio::test(start_paused = true)]
async fn reconfigure_returns_live_handles_and_retires_removed_ones() {
    let t = build(TreeConfig::default());
    let (n, p, root) = (t.n, t.p.clone(), t.b.root());
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    let q = f
        .handle
        .reconfigure(move |r| {
            let slot = r.core().add_property(
                n,
                "x",
                ebus_homie::datatype::Datatype::Integer,
                None,
                PropSpec::new(),
            )?;
            r.core().remove_property(slot)?;
            r.property::<i64>(n, "q", PropSpec::new())
        })
        .await
        .expect("commit");
    settle().await;
    assert_eq!(
        f.cmds(),
        [
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/r/$state=ready"
        ]
    );
    q.set(5).expect("live");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/q=5"]);
    // A refused commit changes nothing and its handles are dead.
    let refused = f
        .handle
        .reconfigure(move |r| {
            let h = r.property::<i64>(n, "z", PropSpec::new())?;
            r.core().add_node(root, "n", NodeSpec::default())?;
            Ok(h)
        })
        .await;
    assert_eq!(refused.err(), Some(BuildError::DuplicateId));
    // Removing a property retires its handle.
    let slot_of_p = f.handle.reconfigure(move |r| r.core().remove_node(n)).await;
    assert_eq!(slot_of_p, Ok(()));
    settle().await;
    assert_eq!(p.set(9.0), Err(ValueError::Gone));
    assert_eq!(q.set(6), Err(ValueError::Gone));
}

#[tokio::test(start_paused = true)]
async fn shutdown_announce_flushes_then_ends_with_its_mode() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let p = t.p.clone();
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    p.set(2.0).expect("pending");
    f.handle.shutdown(Shutdown::Announce).await;
    let mut out = Vec::new();
    while let Some(c) = f.commands.recv().await {
        out.push(show(&c));
    }
    assert_eq!(out, ["ebus/5/r/n/p=2.0", "ebus/5/r/$state=disconnected"]);
    assert_eq!(f.commands.ending(), Some(Shutdown::Announce));
    assert!(matches!(f.join.await, Ok(Ok(()))));
    assert_eq!(p.set(3.0), Err(ValueError::Gone), "the driver stopped");
}

#[tokio::test(start_paused = true)]
async fn abort_sends_nothing_and_ends_silent() {
    let t = build(TreeConfig::default());
    let p = t.p.clone();
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    p.set(2.0).expect("pending");
    f.handle.abort();
    let mut out = Vec::new();
    while let Some(c) = f.commands.recv().await {
        out.push(show(&c));
    }
    assert!(out.is_empty(), "{out:?}");
    assert_eq!(f.commands.ending(), Some(Shutdown::Silent));
}

#[tokio::test(start_paused = true)]
async fn shutdown_while_down_ends_at_once() {
    let t = build(TreeConfig::default());
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.handle.shutdown(Shutdown::Remove).await;
    assert!(f.commands.recv().await.is_none());
    assert_eq!(f.commands.ending(), Some(Shutdown::Remove));
}

#[tokio::test(start_paused = true)]
async fn a_message_while_down_is_an_implicit_link_up() {
    let mut t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.set("ebus/5/r/n/s/set", "true");
    settle().await;
    assert_eq!(f.cmds(), BOOT);
    assert!(t.s.try_next_set().is_some(), "the message is still handled");
}

#[tokio::test(start_paused = true)]
async fn device_commands_queued_before_a_clean_link_up_are_skipped() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let opts = DeviceOptions {
        link_up_discards_queued: true,
        ..DeviceOptions::default()
    };
    let mut f = Fake::start(t.b, opts);
    f.inbox.connected(false);
    settle().await;
    // The refresh is queued; a clean link-up arrives before it is read.
    f.inbox.disconnected();
    f.inbox.connected(false);
    settle().await;
    // Polled counts as delivered without receipts, so the skipped refresh
    // left the description memo advanced: no `init`.
    assert_eq!(
        f.cmds(),
        [
            "SUB ebus/5/r/+/+/set",
            "ebus/5/r/$description=desc",
            "ebus/5/r/n/p=1.5",
            "ebus/5/r/$state=ready",
        ],
        "one refresh, not two"
    );
}

#[tokio::test(start_paused = true)]
async fn with_receipts_ready_waits_for_the_suback() {
    let t = build(TreeConfig::default());
    let mut b = t.b;
    b.set_receipts(true);
    let mut f = Fake::start(b, DeviceOptions::default());
    let out = f.up().await;
    assert!(out.iter().all(|c| !c.ends_with("=ready")), "{out:?}");
    f.inbox.receipt(Receipt::Subscribed {
        filters: vec!["ebus/5/r/+/+/set".into()],
        granted: true,
    });
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/$state=ready"]);
}

#[tokio::test(start_paused = true)]
async fn a_failed_value_publish_is_retried_at_the_latest_value() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let p = t.p.clone();
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    p.set(2.0).expect("p");
    settle().await;
    let failed = f.commands.try_recv().expect("publish");
    p.set(3.0).expect("newer");
    f.inbox.command_failed(failed);
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/p=3.0"]);
}

#[tokio::test(start_paused = true)]
async fn broadcasts_reach_the_handle() {
    let mut cfg = TreeConfig::default();
    cfg.broadcast = true;
    let t = build(cfg);
    let mut f = Fake::start(t.b, DeviceOptions::default());
    let boot = f.up().await;
    assert_eq!(
        boot.first().map(String::as_str),
        Some("SUB ebus/5/r/+/+/set,ebus/5/$broadcast/#")
    );
    f.set("ebus/5/$broadcast/alarm", "fire");
    let (sub, payload) = f.handle.next_broadcast().await.expect("broadcast");
    assert_eq!(sub, "alarm");
    assert_eq!(&payload[..], b"fire");
}

#[tokio::test(start_paused = true)]
async fn dropping_every_inbox_stops_the_driver() {
    let t = build(TreeConfig::default());
    let f = Fake::start(t.b, DeviceOptions::default());
    drop(f.inbox);
    assert!(matches!(f.join.await, Ok(Ok(()))));
    assert_eq!(f.commands.ending(), None);
}

// ----- pair -----

struct FakePair {
    device: DeviceHandle,
    controller: ControllerHandle,
    events: EventReceiver,
    inbox: Inbox,
    commands: Commands,
}

impl FakePair {
    fn start(b: TreeBuilder, cfg: Config) -> Self {
        let a = attach_pair(b, cfg, DeviceOptions::default()).expect("attach");
        tokio::spawn(a.driver.run());
        FakePair {
            device: a.device,
            controller: a.controller,
            events: a.events,
            inbox: a.inbox,
            commands: a.commands,
        }
    }

    fn cmds(&mut self) -> Vec<String> {
        iter::from_fn(|| self.commands.try_recv())
            .map(|c| show(&c))
            .collect()
    }
}

#[tokio::test(start_paused = true)]
async fn pair_sends_the_device_refresh_before_the_controller_subscribes() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let mut f = FakePair::start(t.b, Config::wildcard());
    f.inbox.connected(false);
    settle().await;
    let mut want: Vec<String> = BOOT.iter().map(|s| (*s).to_owned()).collect();
    want.push("SUB ebus/5/+/$state".into());
    assert_eq!(f.cmds(), want);
    assert!(f.device.is_connected() && f.controller.is_connected());
    // The controller sees devices through the same inbox.
    f.inbox.message(
        "ebus/5/other/$state",
        Bytes::from_static(b"ready"),
        Some(true),
    );
    settle().await;
    assert!(f.controller.device("other").is_some());
    let events: Vec<Delivered> = iter::from_fn(|| f.events.try_recv()).collect();
    assert!(!events.is_empty());
}

#[tokio::test(start_paused = true)]
async fn pair_controller_shutdown_keeps_the_device_and_its_filters() {
    let mut cfg = TreeConfig::default();
    cfg.watch_own_state = true;
    let t = build(cfg);
    let mut f = FakePair::start(t.b, Config::device("r").expect("config"));
    f.inbox.connected(false);
    settle().await;
    f.cmds();
    // The controller follows its own device: same $state filter.
    f.inbox
        .message("ebus/5/r/$state", Bytes::from_static(b"ready"), Some(true));
    settle().await;
    f.cmds();
    f.controller.shutdown().await;
    settle().await;
    let out = f.cmds();
    assert!(
        out.iter()
            .all(|c| !c.contains("ebus/5/r/$state,") && !c.ends_with("ebus/5/r/$state")),
        "{out:?}"
    );
    assert!(f.device.is_connected(), "the device keeps running");
    let p = t.p;
    p.set(4.0).expect("p");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/p=4.0"]);
}

#[tokio::test(start_paused = true)]
async fn pair_message_while_down_brings_both_roles_up() {
    let t = build(TreeConfig::default());
    let mut f = FakePair::start(t.b, Config::wildcard());
    f.inbox
        .message("ebus/5/x/$state", Bytes::from_static(b"ready"), Some(true));
    settle().await;
    let out = f.cmds();
    assert_eq!(
        out.first().map(String::as_str),
        Some("SUB ebus/5/r/+/+/set")
    );
    assert!(out.iter().any(|c| c == "SUB ebus/5/+/$state"), "{out:?}");
    assert!(f.device.is_connected() && f.controller.is_connected());
}

#[tokio::test(start_paused = true)]
async fn pair_failures_go_to_the_role_that_owns_the_command() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let mut f = FakePair::start(t.b, Config::wildcard());
    f.inbox.connected(false);
    settle().await;
    f.cmds();
    f.inbox.command_failed(Command::Publish {
        topic: "ebus/5/r/n/p".into(),
        payload: b"1.5".to_vec(),
        qos: QoS::ExactlyOnce,
        retain: true,
    });
    f.inbox.command_failed(Command::Subscribe {
        filters: vec![("ebus/5/+/$state".into(), QoS::ExactlyOnce)],
    });
    settle().await;
    assert_eq!(
        f.cmds(),
        ["ebus/5/r/n/p=1.5"],
        "the device republishes at once"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        f.cmds(),
        ["SUB ebus/5/+/$state"],
        "the controller retries after its backoff"
    );
}

#[tokio::test(start_paused = true)]
async fn a_dropped_set_is_not_routed_to_the_controller() {
    let t = build(TreeConfig::default());
    let a = attach_pair(
        t.b,
        Config::wildcard(),
        DeviceOptions {
            inbox_bytes: 64,
            ..DeviceOptions::default()
        },
    )
    .expect("attach");
    let (inbox, mut commands) = (a.inbox, a.commands);
    tokio::spawn(a.driver.run());
    inbox.connected(false);
    settle().await;
    // The controller tracks the device's own root.
    inbox.message("ebus/5/r/$state", Bytes::from_static(b"ready"), Some(true));
    settle().await;
    while commands.try_recv().is_some() {}
    // Fill the inbox, then overflow it with a /set.
    let big = Bytes::from(vec![b'x'; 40]);
    assert!(inbox.message("ebus/5/a/$state", big, Some(true)));
    assert!(!inbox.message("ebus/5/r/n/s/set", Bytes::from_static(b"true"), Some(false)));
    settle().await;
    let out: Vec<String> = iter::from_fn(|| commands.try_recv())
        .map(|c| show(&c))
        .collect();
    assert!(
        out.iter().all(|c| !c.starts_with("SUB ebus/5/r/")),
        "no resubscribe for a dropped /set: {out:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stored_credit_permit_wakes_the_driver_whatever_starved_says() {
    let a =
        attach_device(build(TreeConfig::default()).b, DeviceOptions::default()).expect("attach");
    let mut d = a.driver.0;
    // As the executor leaves it when it receives a command between the
    // driver's last credit check and its wait: `starved` cleared, one permit.
    d.shared
        .starved
        .store(false, std::sync::atomic::Ordering::Release);
    d.shared.credit.notify_one();
    let woken = tokio::time::timeout(Duration::from_secs(1), d.wait(None)).await;
    assert!(
        matches!(woken, Ok(driver::Woken::Other)),
        "the wait ignored the permit"
    );
    drop(a.inbox);
}

#[tokio::test(start_paused = true)]
async fn an_event_queued_before_a_link_drop_is_not_sent_after_it() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let e = t.e.clone();
    let opts = DeviceOptions {
        max_outstanding: 1,
        ..DeviceOptions::default()
    };
    let mut f = Fake::start(t.b, opts);
    f.inbox.connected(false);
    settle().await;
    // Out of credit: the event waits in the queue.
    e.emit(42).expect("queued while up");
    f.inbox.disconnected();
    f.inbox.connected(false);
    settle().await;
    let mut all = Vec::new();
    while let Some(c) = f.commands.try_recv() {
        all.push(show(&c));
        settle().await;
    }
    assert!(all.iter().all(|c| !c.contains("/e~")), "{all:?}");
    assert_eq!(
        all.last().map(String::as_str),
        Some("ebus/5/r/$state=ready")
    );
    assert_eq!(
        f.handle
            .dev
            .emit_queued
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "the dropped event's capacity is released"
    );
    e.emit(43).expect("up again");
    settle().await;
    assert_eq!(f.cmds(), ["ebus/5/r/n/e~43 q0"]);
}

#[test]
fn an_unchanged_set_or_clear_on_a_target_property_sends_nothing() {
    let mut b = TreeBuilder::new(TreeConfig::default(), "r", DeviceSpec::default()).expect("b");
    let n = b.node(b.root(), "n", NodeSpec::default()).expect("n");
    let sp = b
        .settable::<f64>(n, "sp", PropSpec::new().target(true))
        .expect("sp");
    let mut sent = || iter::from_fn(|| b.requests.try_recv().ok()).count();
    sp.clear().expect("nothing held");
    assert_eq!(sent(), 0);
    sp.set(1.0).expect("set");
    assert_eq!(sent(), 1);
    sp.set(1.0).expect("same");
    sp.property().set(1.0).expect("same, through the property");
    assert_eq!(sent(), 0);
    // An open transition is closed even by an equal value.
    sp.set_target(2.0).expect("target");
    sp.set(1.0).expect("close");
    assert_eq!(sent(), 2);
    sp.progress(1.0).expect("progress");
    sp.set(1.0).expect("close");
    assert_eq!(sent(), 2);
    sp.clear().expect("clear");
    sp.clear().expect("again");
    assert_eq!(sent(), 1);
}

#[tokio::test(start_paused = true)]
async fn typed_handles_name_their_property_for_core_changes() {
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let (p, s, e) = (t.p.clone(), t.s.property(), t.e.clone());
    let (ps, ss, es) = (t.p.slot(), t.s.slot(), t.e.slot());
    assert_eq!(ss, s.slot());
    let mut f = Fake::start(t.b, DeviceOptions::default());
    f.up().await;
    f.handle
        .reconfigure(move |r| {
            r.core().set_unit(ss, Some("W"))?;
            r.core().remove_property(ps)?;
            r.core().remove_property(es)
        })
        .await
        .expect("commit");
    settle().await;
    assert_eq!(
        f.cmds(),
        [
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/r/n/p=<clear>",
            "ebus/5/r/$state=ready"
        ]
    );
    assert_eq!(p.set(2.0), Err(ValueError::Gone));
    assert_eq!(e.emit(1), Err(EmitError::Invalid(ValueError::Gone)));
    s.set(true).expect("still live");
}

#[tokio::test(start_paused = true)]
async fn handles_are_gone_when_the_tree_never_runs() {
    let mut cfg = TreeConfig::default();
    cfg.max_description_bytes = 10;
    let t = build(cfg);
    let (p, e) = (t.p.clone(), t.e.clone());
    assert!(attach_device(t.b, DeviceOptions::default()).is_err());
    assert_eq!(p.set(3.0), Err(ValueError::Gone));
    assert_eq!(e.emit(1), Err(EmitError::Invalid(ValueError::Gone)));
    // A driver dropped without running.
    let t = build(TreeConfig::default());
    let p = t.p.clone();
    let a = attach_device(t.b, DeviceOptions::default()).expect("attach");
    drop(a.driver);
    assert_eq!(p.set(3.0), Err(ValueError::Gone));
    assert!(!a.handle.is_connected());
}

#[tokio::test(start_paused = true)]
async fn an_enum_value_is_published_again_when_its_format_changes() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Mode {
        A,
        B,
    }
    impl HomieEnum for Mode {
        const VARIANTS: &'static [&'static str] = &["a", "b"];
        fn index(self) -> u16 {
            self as u16
        }
        fn from_index(i: u16) -> Option<Self> {
            [Mode::A, Mode::B].get(usize::from(i)).copied()
        }
    }
    let t = build(TreeConfig::default());
    t.p.set(1.5).expect("p");
    let n = t.n;
    let mut b = t.b;
    let m = b.property::<Mode>(n, "m", PropSpec::new()).expect("m");
    m.set(Mode::B).expect("b");
    let mut f = Fake::start(b, DeviceOptions::default());
    assert!(f.up().await.contains(&"ebus/5/r/n/m=b".to_string()));
    let slot = m.slot();
    f.handle
        .reconfigure(move |r| r.core().set_format(slot, Some("a,c")))
        .await
        .expect("commit");
    settle().await;
    let out = f.cmds();
    assert!(out.contains(&"ebus/5/r/n/m=c".to_string()), "{out:?}");
    // A commit that leaves the list alone publishes nothing for it.
    f.handle
        .reconfigure(move |r| r.core().set_unit(slot, Some("x")))
        .await
        .expect("commit");
    settle().await;
    assert!(f.cmds().iter().all(|c| !c.starts_with("ebus/5/r/n/m=")));
}
