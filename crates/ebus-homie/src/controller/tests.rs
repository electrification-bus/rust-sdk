//! Controller rules, driven with a virtual clock.

extern crate std;

use alloc::{format, string::ToString, vec};
use std::iter;

use ebus_mqtt::{Link, Message};

use super::*;
use crate::datatype::Problem;
use crate::description::DescriptionError;

const Q0: QoS = QoS::AtMostOnce;
const Q2: QoS = QoS::ExactlyOnce;

struct H {
    c: Controller,
    now: Instant,
}

impl H {
    fn new(cfg: Config) -> Self {
        H {
            c: Controller::new(cfg).expect("valid config"),
            now: Instant(1_000),
        }
    }

    /// Connected, with the first link-up's output drained.
    fn connected(cfg: Config) -> Self {
        let mut h = H::new(cfg);
        h.up(false);
        h.drain();
        h
    }

    fn wildcard() -> Self {
        H::connected(Config::wildcard())
    }

    fn paced(batch_devices: usize, batch_values: usize) -> Self {
        let mut cfg = Config::wildcard();
        cfg.pacing = Some(Pacing {
            batch_devices,
            batch_values,
        });
        H::connected(cfg)
    }

    fn up(&mut self, session_present: bool) {
        self.c.handle_link(self.now, Link::Up { session_present });
    }

    fn down(&mut self) {
        self.c.handle_link(self.now, Link::Down);
    }

    fn send(&mut self, topic: &str, payload: &[u8], retain: Option<bool>) -> Handled {
        self.c.handle_message(
            self.now,
            Message {
                topic,
                payload,
                retain,
            },
        )
    }

    /// A retained message.
    fn r(&mut self, topic: &str, payload: &str) -> Handled {
        self.send(topic, payload.as_bytes(), Some(true))
    }

    /// A live message.
    fn live(&mut self, topic: &str, payload: &str) -> Handled {
        self.send(topic, payload.as_bytes(), Some(false))
    }

    /// Advances the clock by `ms`, handling every deadline on the way.
    fn advance(&mut self, ms: u64) {
        let end = self.now.add_ms(ms);
        let mut guard = 0;
        while let Some(t) = self.c.poll_timeout() {
            if t > end {
                break;
            }
            self.now = self.now.max(t);
            self.c.handle_timeout(self.now);
            guard += 1;
            assert!(guard < 10_000, "timer spins at {t:?}");
        }
        self.now = end;
    }

    fn cmds(&mut self) -> Vec<Command> {
        iter::from_fn(|| self.c.poll_command()).collect()
    }

    /// Events, without [`Event::TreeComplete`], which every standalone
    /// device fires once.
    fn events(&mut self) -> Vec<Event> {
        let mut ev = self.all_events();
        ev.retain(|e| !matches!(e, Event::TreeComplete { .. }));
        ev
    }

    fn all_events(&mut self) -> Vec<Event> {
        iter::from_fn(|| self.c.poll_event()).collect()
    }

    fn drain(&mut self) {
        self.cmds();
        self.all_events();
    }

    /// A device announced ready with `desc`, its burst ended by the marker,
    /// output drained.
    fn add(&mut self, id: &str, desc: &str) {
        self.r(&t(id, "$state"), "ready");
        self.r(&t(id, "$description"), desc);
        self.r(&t(id, "$description"), desc);
        self.drain();
    }

    fn prop(&self, id: &str, node: &str, prop: &str) -> Option<PropertyView> {
        self.c.device(id)?.property(node, prop).cloned()
    }
}

fn t(id: &str, rest: &str) -> String {
    format!("ebus/5/{id}/{rest}")
}

fn desc(body: &str) -> String {
    if body.is_empty() {
        r#"{"homie":"5.0","version":1}"#.to_string()
    } else {
        format!(r#"{{"homie":"5.0","version":1,{body}}}"#)
    }
}

/// One node `n` with properties `(id, object body)`.
fn node(props: &[(&str, &str)]) -> String {
    let props: Vec<String> = props
        .iter()
        .map(|(id, body)| format!(r#""{id}":{{{body}}}"#))
        .collect();
    desc(&format!(
        r#""nodes":{{"n":{{"properties":{{{}}}}}}}"#,
        props.join(",")
    ))
}

/// Node `n` with `count` retained integer properties `p0`, `p1`, ...
fn ints(count: usize) -> String {
    let props: Vec<(String, &str)> = (0..count)
        .map(|i| (format!("p{i}"), r#""datatype":"integer""#))
        .collect();
    let refs: Vec<(&str, &str)> = props.iter().map(|(a, b)| (a.as_str(), *b)).collect();
    node(&refs)
}

fn sub(filters: &[&str]) -> Command {
    Command::Subscribe {
        filters: filters.iter().map(|f| (f.to_string(), Q2)).collect(),
    }
}

fn unsub(filters: &[&str]) -> Command {
    Command::Unsubscribe {
        filters: filters.iter().map(|f| f.to_string()).collect(),
    }
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

fn unsubscribed(cmds: &[Command]) -> Vec<String> {
    cmds.iter()
        .filter_map(|c| match c {
            Command::Unsubscribe { filters } => Some(filters.iter().cloned()),
            _ => None,
        })
        .flatten()
        .collect()
}

fn id(s: &str) -> Id {
    Id::new(s).expect("valid id")
}

fn addr(d: &str, n: &str, p: &str) -> PropertyAddr {
    PropertyAddr::new(d, n, p).expect("valid address")
}

fn val(raw: &str, check: Result<(), Problem>) -> Value {
    Value {
        raw: Arc::from(raw),
        check,
    }
}

fn whole(id: &str) -> [String; 3] {
    [t(id, "+/+"), t(id, "+/+/$target"), t(id, "$description")]
}

// ===== configuration =====

#[test]
fn config_defaults_and_constructors() {
    let c = Config::wildcard();
    assert_eq!(c.domain, Domain::Named("ebus".to_string()));
    assert_eq!(c.discovery, Discovery::Wildcard);
    assert_eq!(c.subscribe_qos, Q2);
    assert_eq!(
        c.pacing,
        Some(Pacing {
            batch_devices: 8,
            batch_values: 500
        })
    );
    assert_eq!(c.stuck_timeout_ms, Some(10_000));
    assert_eq!(c.max_resubscribe_attempts, 3);
    assert_eq!(c.absent_after_ms, 30_000);
    assert_eq!(c.sync_timeout_ms, 10_000);
    assert!(!c.trust_session);
    assert_eq!(c.max_early_values, 1024);
    assert_eq!(c.max_devices, None);
    assert!(c.host_filters.is_empty());
    assert_eq!(Config::default(), c);
    assert_eq!(
        Config::tree("r").map(|c| c.discovery),
        Ok(Discovery::Tree { root: id("r") })
    );
    assert_eq!(
        Config::device("d").map(|c| c.discovery),
        Ok(Discovery::Device(id("d")))
    );
    assert_eq!(Config::tree("R").err(), Some(ConfigError::InvalidDeviceId));
    assert_eq!(Config::device("").err(), Some(ConfigError::InvalidDeviceId));
}

#[test]
fn config_errors() {
    let err = |f: &dyn Fn(&mut Config)| {
        let mut c = Config::wildcard();
        f(&mut c);
        Controller::new(c).err()
    };
    assert_eq!(
        err(&|c| c.domain = Domain::Named("Bad".into())),
        Some(ConfigError::InvalidDomain)
    );
    assert_eq!(
        err(&|c| c.domain = Domain::Named(String::new())),
        Some(ConfigError::InvalidDomain)
    );
    assert_eq!(
        err(&|c| {
            c.domain = Domain::Any;
            c.discovery = Discovery::Tree { root: id("r") };
        }),
        Some(ConfigError::InvalidDomain)
    );
    assert_eq!(
        err(&|c| c.pacing = Some(Pacing {
            batch_devices: 0,
            batch_values: 1
        })),
        Some(ConfigError::ZeroBatch)
    );
    assert_eq!(
        err(&|c| c.pacing = Some(Pacing {
            batch_devices: 1,
            batch_values: 0
        })),
        Some(ConfigError::ZeroBatch)
    );
    assert_eq!(
        err(&|c| c.host_filters = vec!["a/#/b".into()]),
        Some(ConfigError::InvalidHostFilter)
    );
    assert_eq!(err(&|c| c.domain = Domain::Any), None);
    assert_eq!(err(&|c| c.pacing = None), None);
    assert_eq!(ConfigError::ZeroBatch.to_string(), "zeroBatch");
}

#[test]
fn ids_and_addresses() {
    assert!(Id::new("a-1").is_some());
    assert!(Id::new("A").is_none());
    assert!(Id::new("").is_none());
    assert_eq!(id("x").as_str(), "x");
    assert_eq!(format!("{}/{:?}", id("x"), id("y")), "x/\"y\"");
    assert!(PropertyAddr::new("d", "n", "p").is_some());
    assert!(PropertyAddr::new("d", "n", "$p").is_none());
}

#[test]
fn wants_checks_domain_and_version() {
    let h = H::new(Config::wildcard());
    assert!(h.c.wants("ebus/5/d/$state"));
    assert!(!h.c.wants("ebus/50/d/$state"));
    assert!(!h.c.wants("homie/5/d/$state"));
    assert!(!h.c.wants("ebusx/5/d"));
    let mut cfg = Config::wildcard();
    cfg.domain = Domain::Any;
    let h = H::new(cfg);
    assert!(h.c.wants("homie/5/d/$state"));
    assert!(h.c.wants("ebus/5/d"));
    assert!(!h.c.wants("homie/4/d/$state"));
    assert!(!h.c.wants("homie/5"));
}

// ===== discovery =====

#[test]
fn nothing_is_subscribed_before_the_first_link_up() {
    let mut h = H::new(Config::wildcard());
    assert!(h.cmds().is_empty());
    assert!(!h.c.is_connected());
    assert_eq!(h.c.poll_timeout(), None);
    h.up(false);
    assert_eq!(h.cmds(), [sub(&["ebus/5/+/$state"])]);
    assert_eq!(
        h.events(),
        [Event::Link(Link::Up {
            session_present: false
        })]
    );
    assert!(h.c.is_connected());
}

#[test]
fn any_domain_discovers_every_domain() {
    let mut cfg = Config::wildcard();
    cfg.domain = Domain::Any;
    let mut h = H::new(cfg);
    h.up(false);
    assert_eq!(h.cmds(), [sub(&["+/5/+/$state"])]);
    assert_eq!(h.r("homie/5/d/$state", "ready"), Handled::Consumed);
    assert_eq!(h.cmds(), [sub(&["homie/5/d/$description"])]);
    let v = h.c.device("d").expect("known");
    assert_eq!(&*v.domain, "homie");
    h.r("other/5/d/$state", "init");
    assert_eq!(h.cmds(), [sub(&["other/5/d/$description"])]);
    assert_eq!(h.c.devices().count(), 2);
    assert_eq!(h.c.device("d").map(|v| &*v.domain), Some("homie"));
}

#[test]
fn wildcard_discovery_then_paced_properties() {
    let mut h = H::wildcard();
    assert_eq!(h.r(&t("d1", "$state"), "ready"), Handled::Consumed);
    assert_eq!(h.cmds(), [sub(&["ebus/5/d1/$description"])]);
    assert_eq!(
        h.events(),
        [
            Event::DeviceDiscovered {
                device: id("d1"),
                state: DeviceState::Ready
            },
            Event::StateChanged {
                device: id("d1"),
                old: None,
                new: DeviceState::Ready,
                effective: DeviceState::Ready
            },
        ]
    );
    let v = h.c.device("d1").expect("known");
    assert_eq!(v.state, Some(DeviceState::Ready));
    assert!(v.description.is_none());

    let d = ints(1);
    h.r(&t("d1", "$description"), &d);
    let ev = h.events();
    assert!(
        matches!(&ev[..], [Event::DescriptionChanged { device, while_ready: false, ignored, .. }]
            if *device == "d1" && ignored.is_empty()),
        "{ev:?}"
    );
    let w = whole("d1");
    assert_eq!(h.cmds(), [sub(&[&w[0], &w[1], &w[2]])]);

    h.r(&t("d1", "n/p0"), "5");
    assert_eq!(
        h.events(),
        [Event::PropertyChanged {
            addr: addr("d1", "n", "p0"),
            value: val("5", Ok(())),
            origin: Origin::Retained
        }]
    );
    // The marker ends the burst; every value is present.
    h.r(&t("d1", "$description"), &d);
    assert_eq!(
        h.events(),
        [Event::DeviceSynced {
            device: id("d1"),
            missing: 0
        }]
    );
    assert!(h.c.device("d1").is_some_and(|v| v.synced));
    assert!(h.cmds().is_empty());
}

#[test]
fn an_unparseable_state_creates_nothing() {
    let mut h = H::wildcard();
    assert_eq!(h.r(&t("d1", "$state"), "Ready"), Handled::Ignored);
    assert!(h.cmds().is_empty());
    assert!(h.events().is_empty());
    assert!(h.c.device("d1").is_none());
    h.add("d2", &ints(1));
    assert_eq!(h.r(&t("d2", "$state"), "bogus"), Handled::Consumed);
    assert_eq!(
        h.c.device("d2").and_then(|v| v.state),
        Some(DeviceState::Ready)
    );
    assert!(h.events().is_empty());
}

#[test]
fn untracked_and_foreign_messages_are_ignored() {
    let mut h = H::wildcard();
    assert_eq!(h.r(&t("x", "n/p"), "1"), Handled::Ignored);
    assert_eq!(h.r(&t("x", "$description"), &ints(1)), Handled::Ignored);
    assert_eq!(h.r(&t("x", "$alert/a"), "hi"), Handled::Ignored);
    assert_eq!(h.r("homie/5/x/$state", "ready"), Handled::Ignored);
    assert_eq!(h.r("ebus/5/$broadcast/a", "x"), Handled::Ignored);
    assert_eq!(h.r("ebus/5/X/$state", "ready"), Handled::Ignored);
    h.add("d1", &ints(1));
    assert_eq!(h.live(&t("d1", "n/p0/set"), "1"), Handled::Ignored);
    assert!(
        h.cmds().is_empty(),
        "untracked messages trigger no unsubscribe"
    );
    assert!(h.events().is_empty());
}

#[test]
fn max_devices_caps_the_registry() {
    let mut cfg = Config::wildcard();
    cfg.max_devices = Some(1);
    let mut h = H::connected(cfg);
    h.r(&t("d1", "$state"), "ready");
    assert_eq!(h.r(&t("d2", "$state"), "ready"), Handled::Ignored);
    assert_eq!(h.c.devices().count(), 1);
}

#[test]
fn device_mode_subscribes_the_device_and_its_root_state() {
    let mut h = H::new(Config::device("c").expect("valid"));
    h.up(false);
    assert_eq!(
        h.cmds(),
        [sub(&[
            &t("c", "$state"),
            &t("c", "$description"),
            &t("c", "+/+"),
            &t("c", "+/+/$target")
        ])]
    );
    h.drain();
    assert_eq!(h.r(&t("x", "$state"), "ready"), Handled::Ignored);
    assert_eq!(h.r(&t("r", "$state"), "lost"), Handled::Ignored);
    h.r(&t("c", "$state"), "ready");
    h.r(&t("c", "$description"), &desc(r#""root":"r","parent":"r""#));
    assert_eq!(h.cmds(), [sub(&[&t("r", "$state")])]);
    h.drain();
    assert_eq!(h.r(&t("r", "$state"), "lost"), Handled::Consumed);
    assert_eq!(
        h.events(),
        [Event::EffectiveStateChanged {
            device: id("c"),
            old: DeviceState::Ready,
            new: DeviceState::Lost
        }]
    );
    assert_eq!(h.c.effective_state("c"), Some(DeviceState::Lost));
    assert!(
        h.c.device("r").is_none(),
        "the root is not tracked as a device"
    );
    h.r(&t("r", "$state"), "ready");
    assert_eq!(h.c.effective_state("c"), Some(DeviceState::Ready));
    // A description naming another root moves the filter.
    h.drain();
    h.r(&t("c", "$description"), &desc(r#""root":"q","parent":"q""#));
    assert_eq!(
        h.cmds(),
        [unsub(&[&t("r", "$state")]), sub(&[&t("q", "$state")])]
    );
    assert_eq!(h.r(&t("r", "$state"), "lost"), Handled::Ignored);
}

// ===== pacing =====

#[test]
fn pacing_limits_devices_in_flight() {
    let mut h = H::paced(2, 500);
    let d = ints(1);
    for dev in ["d1", "d2", "d3", "d4"] {
        h.r(&t(dev, "$state"), "ready");
        h.r(&t(dev, "$description"), &d);
    }
    let s = subscribed(&h.cmds());
    assert!(s.contains(&t("d1", "+/+")) && s.contains(&t("d2", "+/+")));
    assert!(!s.contains(&t("d3", "+/+")) && !s.contains(&t("d4", "+/+")));
    // The retained marker ends d1's burst; d3 starts.
    h.r(&t("d1", "$description"), &d);
    let s = subscribed(&h.cmds());
    assert_eq!(s, whole("d3"));
    // A live republish is not the marker.
    h.live(&t("d2", "$description"), &d);
    assert!(h.cmds().is_empty());
    h.r(&t("d2", "$description"), &d);
    assert_eq!(subscribed(&h.cmds()), whole("d4"));
}

#[test]
fn retain_none_counts_as_the_marker() {
    let mut h = H::paced(1, 500);
    let d = ints(1);
    for dev in ["d1", "d2"] {
        h.r(&t(dev, "$state"), "ready");
        h.r(&t(dev, "$description"), &d);
    }
    h.drain();
    h.send(&t("d1", "$description"), d.as_bytes(), None);
    assert_eq!(subscribed(&h.cmds()), whole("d2"));
}

#[test]
fn a_burst_ends_when_every_owed_message_arrives() {
    let mut h = H::paced(1, 500);
    let d = ints(2);
    for dev in ["d1", "d2"] {
        h.r(&t(dev, "$state"), "ready");
        h.r(&t(dev, "$description"), &d);
    }
    h.drain();
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "n/p0"), "2");
    h.r(&t("d1", "n/p0/$target"), "2");
    h.r(&t("d1", "n/p1"), "1");
    assert!(h.cmds().is_empty(), "repeats count once");
    h.r(&t("d1", "n/p1/$target"), "1");
    assert_eq!(subscribed(&h.cmds()), whole("d2"));
}

#[test]
fn the_value_budget_splits_a_device_by_node() {
    let mut h = H::paced(8, 4);
    let d = desc(
        r#""nodes":{"a":{"properties":{"p":{"datatype":"integer"}}},"b":{"properties":{"p":{"datatype":"integer"}}},"c":{"properties":{"p":{"datatype":"integer"}}}}"#,
    );
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &d);
    assert_eq!(
        h.cmds(),
        [
            sub(&[&t("d1", "$description")]),
            sub(&[
                &t("d1", "a/+"),
                &t("d1", "a/+/$target"),
                &t("d1", "b/+"),
                &t("d1", "b/+/$target"),
                &t("d1", "$alert/+"),
                &t("d1", "$description")
            ])
        ]
    );
    // A second device waits while the first owes the whole budget.
    h.r(&t("d2", "$state"), "ready");
    h.r(&t("d2", "$description"), &ints(1));
    assert_eq!(subscribed(&h.cmds()), [t("d2", "$description")]);
    // d1's marker: its next chunk goes first (front of the queue), and d2
    // fits beside it.
    h.r(&t("d1", "$description"), &d);
    assert_eq!(
        h.cmds(),
        [
            sub(&[
                &t("d1", "c/+"),
                &t("d1", "c/+/$target"),
                &t("d1", "$description")
            ]),
            sub(&whole("d2").each_ref().map(String::as_str)),
        ]
    );
    h.r(&t("d1", "$description"), &d);
    assert!(h.cmds().is_empty(), "every node is covered");
}

#[test]
fn unpaced_subscribes_properties_with_stage_one() {
    let mut cfg = Config::wildcard();
    cfg.pacing = None;
    let mut h = H::connected(cfg);
    h.r(&t("d1", "$state"), "ready");
    let w = whole("d1");
    assert_eq!(h.cmds(), [sub(&[&w[2], &w[0], &w[1]])]);
    h.r(&t("d1", "$description"), &ints(1));
    assert!(h.cmds().is_empty());
    h.r(&t("d1", "n/p0"), "1");
    assert!(h.events().contains(&Event::DeviceSynced {
        device: id("d1"),
        missing: 0
    }));
}

// ===== messages =====

#[test]
fn description_dedup_and_the_ready_edge() {
    let mut h = H::wildcard();
    let a = ints(1);
    let b = ints(2);
    h.r(&t("d1", "$state"), "init");
    h.r(&t("d1", "$description"), &a);
    h.drain();
    // Same bytes, then the same content formatted differently: nothing.
    h.r(&t("d1", "$description"), &a);
    h.r(&t("d1", "$description"), &a.replace(':', ": "));
    assert!(h.events().is_empty());
    // A change while init is applied silently.
    h.r(&t("d1", "$description"), &b);
    assert!(h.events().is_empty());
    assert!(
        h.c.device("d1")
            .and_then(|v| v.description.as_ref())
            .is_some_and(|d| d.nodes[0].1.properties.len() == 2)
    );
    h.r(&t("d1", "$state"), "ready");
    let ev = h.events();
    assert!(
        matches!(&ev[..], [Event::StateChanged { new: DeviceState::Ready, .. }, Event::DescriptionChanged { while_ready: false, description, .. }]
            if description.nodes[0].1.properties.len() == 2),
        "{ev:?}"
    );
    // A change while ready is announced at once.
    let c = ints(3);
    h.live(&t("d1", "$description"), &c);
    assert!(matches!(
        &h.events()[..],
        [Event::DescriptionChanged {
            while_ready: true,
            ..
        }]
    ));
    // Back to init, a detour, and back to the announced content: the ready
    // edge announces nothing.
    h.live(&t("d1", "$state"), "init");
    h.live(&t("d1", "$description"), &a);
    h.live(&t("d1", "$description"), &c);
    h.live(&t("d1", "$state"), "ready");
    let ev = h.events();
    assert!(
        ev.iter()
            .all(|e| !matches!(e, Event::DescriptionChanged { .. })),
        "{ev:?}"
    );
    // A repeated identical $state emits nothing.
    h.live(&t("d1", "$state"), "ready");
    assert!(h.events().is_empty());
}

#[test]
fn a_rejected_description_leaves_the_device_without_a_model() {
    let mut h = H::wildcard();
    h.r(&t("d1", "$state"), "ready");
    h.drain();
    h.r(&t("d1", "$description"), r#"{"homie":"4.0","version":1}"#);
    assert_eq!(
        h.events(),
        [Event::DescriptionRejected {
            device: id("d1"),
            error: DescriptionError::HomieInvalid
        }]
    );
    h.r(&t("d1", "$description"), r#"{"homie":"4.0","version":1}"#);
    assert!(h.events().is_empty(), "deduplicated by bytes");
    assert_eq!(
        h.c.device("d1").map(|v| v.state),
        Some(Some(DeviceState::Ready))
    );
    assert!(h.c.device("d1").is_some_and(|v| v.description.is_none()));
    h.send(&t("d1", "$description"), b"\xff", Some(true));
    assert_eq!(
        h.events(),
        [Event::DescriptionRejected {
            device: id("d1"),
            error: DescriptionError::InvalidJson
        }]
    );
    h.r(&t("d1", "$description"), &ints(1));
    assert!(matches!(
        &h.events()[..],
        [Event::DescriptionChanged {
            while_ready: false,
            ..
        }]
    ));
}

#[test]
fn illegal_properties_are_pruned_and_reported() {
    let mut h = H::wildcard();
    h.r(&t("d1", "$state"), "ready");
    h.drain();
    h.r(
        &t("d1", "$description"),
        &node(&[
            ("p", r#""datatype":"integer""#),
            ("q", r#""datatype":"nope""#),
        ]),
    );
    let ev = h.events();
    assert!(
        matches!(&ev[..], [Event::DescriptionChanged { ignored, .. }]
            if ignored.len() == 1 && ignored[0].property.as_deref() == Some("q")),
        "{ev:?}"
    );
    h.r(
        &t("d1", "$description"),
        &node(&[
            ("p", r#""datatype":"integer""#),
            ("q", r#""datatype":"nope""#),
        ]),
    );
    h.r(&t("d1", "n/q"), "1");
    assert!(
        h.events().is_empty(),
        "a pruned property's value is dropped"
    );
    assert!(h.prop("d1", "n", "q").is_none());
}

#[test]
fn early_values_wait_for_the_description() {
    let mut cfg = Config::device("d1").expect("valid");
    cfg.max_early_values = 2;
    let mut h = H::connected(cfg);
    h.r(&t("d1", "n/p"), "1");
    h.r(&t("d1", "n/p"), "2");
    h.r(&t("d1", "n/p/$target"), "3");
    h.r(&t("d1", "n/q"), "x");
    h.r(&t("d1", "n/z"), "dropped over the cap");
    h.r(&t("d1", "n/gone"), "1");
    assert!(h.events().is_empty());
    h.r(&t("d1", "$state"), "ready");
    h.drain();
    h.r(
        &t("d1", "$description"),
        &node(&[
            ("p", r#""datatype":"integer""#),
            ("q", r#""datatype":"integer""#),
            ("z", r#""datatype":"string""#),
        ]),
    );
    let ev = h.events();
    assert!(matches!(ev[0], Event::DescriptionChanged { .. }), "{ev:?}");
    assert_eq!(
        ev[1..],
        [
            Event::PropertyChanged {
                addr: addr("d1", "n", "p"),
                value: val("2", Ok(())),
                origin: Origin::Retained
            },
            Event::TargetChanged {
                addr: addr("d1", "n", "p"),
                target: Some(val("3", Ok(())))
            },
            Event::PropertyChanged {
                addr: addr("d1", "n", "q"),
                value: val("x", Err(Problem::IntegerGrammar)),
                origin: Origin::Retained
            },
        ]
    );
    assert!(h.prop("d1", "n", "z").is_some_and(|p| p.value.is_none()));
}

#[test]
fn a_cleared_early_value_is_forgotten() {
    let mut h = H::connected(Config::device("d1").expect("valid"));
    h.r(&t("d1", "n/p"), "1");
    h.send(&t("d1", "n/p"), b"", Some(true));
    h.r(&t("d1", "$state"), "ready");
    h.r(
        &t("d1", "$description"),
        &ints(0).replace(
            r#""properties":{}"#,
            r#""properties":{"p":{"datatype":"integer"}}"#,
        ),
    );
    let ev = h.events();
    assert!(
        ev.iter()
            .all(|e| !matches!(e, Event::PropertyChanged { .. })),
        "{ev:?}"
    );
}

#[test]
fn value_events_follow_origin_and_bytes() {
    let mut h = H::wildcard();
    h.add(
        "d1",
        &node(&[
            ("p", r#""datatype":"integer""#),
            ("s", r#""datatype":"string""#),
        ]),
    );
    let a = addr("d1", "n", "p");
    h.r(&t("d1", "n/p"), "1");
    assert_eq!(
        h.events(),
        [Event::PropertyChanged {
            addr: a.clone(),
            value: val("1", Ok(())),
            origin: Origin::Retained
        }]
    );
    h.r(&t("d1", "n/p"), "1");
    h.send(&t("d1", "n/p"), b"1", None);
    h.live(&t("d1", "n/p"), "1");
    assert!(
        h.events().is_empty(),
        "a value repeated retained, live or of unknown origin is quiet"
    );
    h.send(&t("d1", "n/p"), b"2", None);
    assert_eq!(
        h.events(),
        [Event::PropertyChanged {
            addr: a.clone(),
            value: val("2", Ok(())),
            origin: Origin::Unknown
        }]
    );
    // An invalid value is delivered unmodified; last_valid keeps the last good one.
    h.live(&t("d1", "n/p"), "2.5");
    assert_eq!(
        h.events(),
        [Event::PropertyChanged {
            addr: a.clone(),
            value: val("2.5", Err(Problem::IntegerGrammar)),
            origin: Origin::Live
        }]
    );
    let p = h.prop("d1", "n", "p").expect("declared");
    assert_eq!(p.value, Some(val("2.5", Err(Problem::IntegerGrammar))));
    assert_eq!(p.last_valid.as_deref(), Some("2"));
    // 0x00 is "" and valid only for string.
    h.send(&t("d1", "n/p"), b"\x00", Some(false));
    assert!(
        matches!(&h.events()[..], [Event::PropertyChanged { value, .. }] if value.raw.is_empty() && value.check.is_err())
    );
    h.send(&t("d1", "n/s"), b"\x00", Some(false));
    assert_eq!(
        h.events()[..1],
        [Event::PropertyChanged {
            addr: addr("d1", "n", "s"),
            value: val("", Ok(())),
            origin: Origin::Live
        }]
    );
    // Invalid UTF-8 is dropped.
    assert_eq!(
        h.send(&t("d1", "n/s"), b"\xff", Some(false)),
        Handled::Consumed
    );
    assert!(h.events().is_empty());
}

#[test]
fn retained_false_properties_are_events_only() {
    let mut h = H::wildcard();
    h.add(
        "d1",
        &node(&[("e", r#""datatype":"string","retained":false"#)]),
    );
    for _ in 0..2 {
        h.live(&t("d1", "n/e"), "pressed");
        assert_eq!(
            h.events(),
            [Event::PropertyEvent {
                addr: addr("d1", "n", "e"),
                value: val("pressed", Ok(())),
                origin: Origin::Live,
            }]
        );
    }
    h.r(&t("d1", "n/e"), "retained anyway");
    assert_eq!(
        h.events(),
        [Event::PropertyEvent {
            addr: addr("d1", "n", "e"),
            value: val("retained anyway", Ok(())),
            origin: Origin::Retained,
        }],
        "accepted with a diagnostic, marked as a replay"
    );
    assert_eq!(h.prop("d1", "n", "e"), Some(PropertyView::default()));
}

#[test]
fn zero_length_payloads_clear() {
    let mut h = H::wildcard();
    h.add("d1", &ints(1));
    let a = addr("d1", "n", "p0");
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "n/p0/$target"), "2");
    h.r(&t("d1", "$alert/low"), "battery low");
    let ev = h.events();
    assert_eq!(ev.len(), 4, "{ev:?}");
    assert_eq!(
        ev[1],
        Event::DeviceSynced {
            device: id("d1"),
            missing: 0
        }
    );
    assert_eq!(
        ev[3],
        Event::Alert {
            device: id("d1"),
            alert: id("low"),
            message: Some("battery low".into())
        }
    );
    assert_eq!(
        h.c.device("d1").and_then(|v| v.alert("low")),
        Some("battery low")
    );
    h.r(&t("d1", "$alert/low"), "battery low");
    assert!(h.events().is_empty());
    h.send(&t("d1", "n/p0"), b"", Some(true));
    h.send(&t("d1", "n/p0/$target"), b"", Some(true));
    h.send(&t("d1", "$alert/low"), b"", Some(true));
    assert_eq!(
        h.events(),
        [
            Event::PropertyCleared { addr: a.clone() },
            Event::TargetChanged {
                addr: a.clone(),
                target: None
            },
            Event::Alert {
                device: id("d1"),
                alert: id("low"),
                message: None
            },
        ]
    );
    assert_eq!(h.prop("d1", "n", "p0"), Some(PropertyView::default()));
    h.send(&t("d1", "n/p0"), b"", Some(true));
    assert!(h.events().is_empty(), "clearing an empty property is quiet");
    assert_eq!(h.c.device("d1").map(|v| v.alerts().count()), Some(0));
}

#[test]
fn logs_are_consumed_without_events() {
    let mut h = H::wildcard();
    h.add("d1", &ints(1));
    assert_eq!(h.live(&t("d1", "$log/warn"), "hello"), Handled::Consumed);
    assert_eq!(h.live(&t("x", "$log/warn"), "hello"), Handled::Ignored);
    assert!(h.events().is_empty());
}

#[test]
fn vanished_properties_are_dropped_and_new_values_rechecked() {
    let mut h = H::wildcard();
    h.add(
        "d1",
        &node(&[
            ("p", r#""datatype":"integer""#),
            ("q", r#""datatype":"integer""#),
        ]),
    );
    h.r(&t("d1", "n/p"), "5");
    h.r(&t("d1", "n/q"), "1");
    h.drain();
    h.live(
        &t("d1", "$description"),
        &node(&[("p", r#""datatype":"integer","format":"0:3""#)]),
    );
    assert!(h.prop("d1", "n", "q").is_none());
    let p = h.prop("d1", "n", "p").expect("kept");
    assert_eq!(p.value, Some(val("5", Err(Problem::AboveMax))));
    assert_eq!(p.last_valid, None);
}

// ===== sync =====

#[test]
fn sync_reports_missing_values_after_the_timeout() {
    let mut h = H::wildcard();
    let d = ints(2);
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &d);
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "$description"), &d);
    h.drain();
    h.advance(9_999);
    assert!(h.events().is_empty());
    h.advance(1);
    assert_eq!(
        h.events(),
        [Event::DeviceSynced {
            device: id("d1"),
            missing: 1
        }]
    );
    // Synced fires once; a late value does not re-fire it.
    h.r(&t("d1", "n/p1"), "1");
    assert_eq!(h.events().len(), 1);
    // A description change re-arms it.
    h.live(&t("d1", "$description"), &ints(3));
    let ev = h.events();
    assert!(
        ev.iter()
            .any(|e| matches!(e, Event::DescriptionChanged { .. })),
        "{ev:?}"
    );
    assert!(!h.c.device("d1").is_some_and(|v| v.synced));
}

#[test]
fn sync_waits_for_a_burst_past_its_deadline() {
    let mut cfg = Config::wildcard();
    cfg.pacing = Some(Pacing {
        batch_devices: 1,
        batch_values: 500,
    });
    cfg.stuck_timeout_ms = None;
    let mut h = H::connected(cfg);
    let d = ints(1);
    for dev in ["d1", "d2"] {
        h.r(&t(dev, "$state"), "ready");
        h.r(&t(dev, "$description"), &d);
    }
    h.drain();
    h.advance(10_000);
    assert!(h.events().is_empty(), "d1 is draining and d2 is queued");
    h.r(&t("d1", "$description"), &d);
    assert_eq!(
        h.events(),
        [Event::DeviceSynced {
            device: id("d1"),
            missing: 1
        }]
    );
    assert_eq!(subscribed(&h.cmds()), whole("d2"));
    h.r(&t("d2", "n/p0"), "1");
    h.r(&t("d2", "$description"), &d);
    assert_eq!(
        h.events()[1..],
        [Event::DeviceSynced {
            device: id("d2"),
            missing: 0
        }]
    );
}

// ===== stuck healing =====

#[test]
fn stage_one_resubscribes_then_gives_up() {
    let mut h = H::wildcard();
    h.r(&t("d1", "$state"), "ready");
    h.drain();
    for attempt in 1..=3u8 {
        h.advance(10_000);
        assert_eq!(h.cmds(), [sub(&[&t("d1", "$description")])]);
        assert_eq!(
            h.events(),
            [Event::DeviceStuck {
                device: id("d1"),
                missing: Missing::Description,
                attempt,
                gave_up: false
            }]
        );
    }
    h.advance(10_000);
    assert!(h.cmds().is_empty());
    assert_eq!(
        h.events(),
        [Event::DeviceStuck {
            device: id("d1"),
            missing: Missing::Description,
            attempt: 3,
            gave_up: true
        }]
    );
    assert_eq!(h.c.poll_timeout(), None);
    // A description that arrives later still gets its properties.
    h.r(&t("d1", "$description"), &ints(1));
    let w = whole("d1");
    assert_eq!(h.cmds(), [sub(&[&w[0], &w[1], &w[2]])]);
}

#[test]
fn a_tree_root_without_state_is_stuck_on_state() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    h.advance(10_000);
    assert_eq!(
        h.events(),
        [Event::DeviceStuck {
            device: id("r"),
            missing: Missing::State,
            attempt: 1,
            gave_up: false
        }]
    );
    assert_eq!(
        h.cmds(),
        [sub(&[
            &t("r", "$state"),
            &t("r", "$description"),
            &t("r", "+/+"),
            &t("r", "+/+/$target")
        ])]
    );
}

#[test]
fn stage_two_requeues_and_stops_without_progress() {
    let mut h = H::wildcard();
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &ints(2));
    h.drain();
    h.advance(10_000);
    let w = whole("d1");
    assert_eq!(
        h.cmds(),
        [sub(&[&w[0], &w[1], &w[2]])],
        "resubscribed so the broker replays"
    );
    assert_eq!(
        h.events(),
        [Event::DeviceStuck {
            device: id("d1"),
            missing: Missing::Properties,
            attempt: 1,
            gave_up: false
        }]
    );
    h.advance(10_000);
    assert!(h.cmds().is_empty());
    assert_eq!(
        h.events(),
        [
            Event::DeviceStuck {
                device: id("d1"),
                missing: Missing::Properties,
                attempt: 1,
                gave_up: true
            },
            Event::DeviceSynced {
                device: id("d1"),
                missing: 2
            },
        ]
    );
}

#[test]
fn stage_two_gives_up_after_the_attempt_limit() {
    let mut h = H::wildcard();
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &ints(2));
    h.drain();
    for (sent, attempt) in (1..=3u8).enumerate() {
        h.advance(10_000);
        assert_eq!(
            h.events(),
            [Event::DeviceStuck {
                device: id("d1"),
                missing: Missing::Properties,
                attempt,
                gave_up: false
            }]
        );
        h.drain();
        // Each retry brings one more message than the one before.
        let msgs = [("n/p0", "1"), ("n/p0/$target", "1"), ("n/p1", "1")];
        for (topic, payload) in &msgs[..=sent] {
            h.r(&t("d1", topic), payload);
        }
        h.drain();
    }
    h.advance(10_000);
    assert_eq!(
        h.events()[0],
        Event::DeviceStuck {
            device: id("d1"),
            missing: Missing::Properties,
            attempt: 3,
            gave_up: true
        }
    );
}

#[test]
fn no_stuck_timer_without_a_stuck_timeout() {
    let mut cfg = Config::wildcard();
    cfg.stuck_timeout_ms = None;
    let mut h = H::connected(cfg);
    h.r(&t("d1", "$state"), "ready");
    assert_eq!(h.c.poll_timeout(), None);
}

#[test]
fn dropped_messages_resubscribe_once_the_backlog_drains() {
    let mut h = H::wildcard();
    h.add("d1", &ints(1));
    h.r(&t("d1", "n/p0"), "1");
    h.drain();
    h.c.handle_dropped(h.now, &t("d1", "n/p0"));
    h.c.handle_dropped(h.now, &t("d1", "n/p0/$target"));
    assert!(h.cmds().is_empty(), "nothing until the backlog drains");
    assert_eq!(h.c.poll_timeout(), Some(h.now));
    h.advance(0);
    let w = whole("d1");
    assert_eq!(
        h.cmds(),
        [
            sub(&[&t("d1", "$description")]),
            sub(&[&w[0], &w[1], &w[2]])
        ]
    );
    // A dropped $state of an unknown device replays the discovery filter.
    h.c.handle_dropped(h.now, &t("new", "$state"));
    h.advance(0);
    assert_eq!(h.cmds(), [sub(&["ebus/5/+/$state"])]);
    // Untracked non-state topics and foreign topics are ignored.
    let before = h.c.poll_timeout();
    h.c.handle_dropped(h.now, &t("new", "n/p"));
    h.c.handle_dropped(h.now, "other/5/x/$state");
    assert_eq!(h.c.poll_timeout(), before);
}

#[test]
fn failed_commands_are_retried_with_backoff() {
    let mut h = H::wildcard();
    h.r(&t("d1", "$state"), "ready");
    let cmds = h.cmds();
    assert_eq!(cmds.len(), 1);
    let failed = cmds[0].clone();
    h.c.handle_command_failed(h.now, failed.clone());
    h.advance(999);
    assert!(h.cmds().is_empty());
    h.advance(1);
    assert_eq!(h.cmds(), core::slice::from_ref(&failed));
    h.c.handle_command_failed(h.now, failed.clone());
    h.advance(1_999);
    assert!(h.cmds().is_empty());
    h.advance(1);
    assert_eq!(h.cmds(), core::slice::from_ref(&failed));
    // A retried Unsubscribe is sent only for filters still released.
    h.c.handle_command_failed(
        h.now,
        unsub(&[&t("d1", "$description"), &t("gone", "$description")]),
    );
    h.advance(4_000);
    assert_eq!(h.cmds(), [unsub(&[&t("gone", "$description")])]);
    // A /set is never retried.
    h.c.handle_command_failed(
        h.now,
        Command::Publish {
            topic: t("d1", "n/p/set"),
            payload: b"1".to_vec(),
            qos: Q0,
            retain: false,
        },
    );
    h.advance(60_000);
    assert!(
        !h.cmds()
            .iter()
            .any(|c| matches!(c, Command::Publish { .. }))
    );
}

// ===== link =====

#[test]
fn a_clean_reconnect_repaces_every_device() {
    let mut h = H::wildcard();
    h.add("d1", &ints(1));
    h.add("d2", &ints(1));
    h.r(&t("d1", "n/p0"), "1");
    h.drain();
    h.down();
    assert_eq!(h.events(), [Event::Link(Link::Down)]);
    assert!(!h.c.is_connected());
    h.advance(5_000);
    h.up(false);
    assert_eq!(
        h.cmds(),
        [
            sub(&["ebus/5/+/$state"]),
            sub(&[&t("d1", "$description")]),
            sub(&[&t("d2", "$description")])
        ]
    );
    assert_eq!(
        h.events(),
        [Event::Link(Link::Up {
            session_present: false
        })]
    );
    assert!(h.c.devices().all(|v| !v.confirmed && !v.synced));
    // The replayed $description is required before properties are paced.
    h.r(&t("d1", "$state"), "ready");
    assert!(h.cmds().is_empty());
    h.r(&t("d1", "$description"), &ints(1));
    let w = whole("d1");
    assert_eq!(h.cmds(), [sub(&[&w[0], &w[1], &w[2]])]);
    assert!(h.c.device("d1").is_some_and(|v| v.confirmed));
    // d2 is never heard again.
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "$description"), &ints(1));
    h.drain();
    h.advance(30_000);
    let ev = h.events();
    assert!(
        ev.contains(&Event::DeviceAbsent { device: id("d2") }),
        "{ev:?}"
    );
    assert!(!ev.contains(&Event::DeviceAbsent { device: id("d1") }));
    assert!(h.c.device("d2").is_some(), "absence is not removal");
    h.live(&t("d2", "$state"), "ready");
    assert!(h.c.device("d2").is_some_and(|v| v.confirmed));
}

#[test]
fn an_untrusted_session_present_unsubscribes_before_repacing() {
    let mut h = H::wildcard();
    h.add("d1", &ints(1));
    h.down();
    h.up(true);
    let w = whole("d1");
    assert_eq!(
        h.cmds(),
        [
            unsub(&[&w[0], &w[1]]),
            sub(&["ebus/5/+/$state"]),
            sub(&[&t("d1", "$description")])
        ]
    );
}

#[test]
fn a_trusted_session_keeps_everything() {
    let mut cfg = Config::wildcard();
    cfg.trust_session = true;
    let mut h = H::connected(cfg);
    h.add("d1", &ints(1));
    h.down();
    h.up(true);
    assert!(h.cmds().is_empty());
    assert_eq!(
        h.events(),
        [
            Event::Link(Link::Down),
            Event::Link(Link::Up {
                session_present: true
            })
        ]
    );
    assert!(h.c.device("d1").is_some_and(|v| v.confirmed));
    // A clean session is still re-paced.
    h.down();
    h.up(false);
    assert_eq!(h.cmds().len(), 2);
}

#[test]
fn a_message_while_down_is_an_implicit_link_up() {
    let mut h = H::wildcard();
    h.add("d1", &ints(1));
    h.down();
    h.drain();
    assert_eq!(h.r(&t("d1", "$state"), "ready"), Handled::Consumed);
    assert_eq!(
        h.events(),
        [Event::Link(Link::Up {
            session_present: false
        })]
    );
    // The broker may still hold the property filters, so releasing them for
    // the re-pace unsubscribes them.
    assert_eq!(
        h.cmds(),
        [
            unsub(&[&t("d1", "+/+"), &t("d1", "+/+/$target")]),
            sub(&["ebus/5/+/$state"]),
            sub(&[&t("d1", "$description")])
        ]
    );
    assert!(h.c.is_connected());

    // Before the first link-up too.
    let mut h = H::new(Config::wildcard());
    h.r(&t("d1", "$state"), "ready");
    assert_eq!(
        h.cmds(),
        [sub(&["ebus/5/+/$state"]), sub(&[&t("d1", "$description")])]
    );
}

// ===== removal and trees =====

#[test]
fn removal_in_wildcard_mode() {
    let mut h = H::wildcard();
    h.add("d1", &ints(1));
    h.send(&t("d1", "$state"), b"", Some(true));
    let w = whole("d1");
    assert_eq!(h.cmds(), [unsub(&[&w[0], &w[1], &w[2]])]);
    assert_eq!(h.events(), [Event::DeviceRemoved { device: id("d1") }]);
    assert!(h.c.device("d1").is_none());
    assert_eq!(h.send(&t("d1", "n/p0"), b"", Some(true)), Handled::Ignored);
    assert_eq!(
        h.send(&t("d1", "$state"), b"", Some(true)),
        Handled::Ignored
    );
    assert!(h.events().is_empty());
    // It can come back.
    h.r(&t("d1", "$state"), "init");
    assert!(matches!(h.events()[0], Event::DeviceDiscovered { .. }));
}

#[test]
fn removal_in_tree_mode_keeps_the_attribute_filters() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    h.r(&t("r", "$description"), &desc(r#""children":["c"]"#));
    h.r(&t("r", "$state"), "ready");
    assert_eq!(
        h.cmds(),
        [sub(&[&t("c", "$state"), &t("c", "$description")])]
    );
    h.r(&t("c", "$state"), "ready");
    h.r(
        &t("c", "$description"),
        &desc(
            r#""root":"r","parent":"r","nodes":{"n":{"properties":{"p":{"datatype":"integer"}}}}"#,
        ),
    );
    h.r(
        &t("c", "$description"),
        &desc(
            r#""root":"r","parent":"r","nodes":{"n":{"properties":{"p":{"datatype":"integer"}}}}"#,
        ),
    );
    h.drain();
    h.send(&t("c", "$state"), b"", Some(true));
    assert_eq!(h.cmds(), [unsub(&[&t("c", "+/+"), &t("c", "+/+/$target")])]);
    assert_eq!(h.events(), [Event::DeviceRemoved { device: id("c") }]);
    // A declared child that returns is re-adopted.
    h.r(&t("c", "$state"), "ready");
    assert_eq!(
        h.cmds(),
        [sub(&[&t("c", "$state"), &t("c", "$description")])]
    );
    assert!(matches!(h.events()[0], Event::DeviceDiscovered { .. }));
    // An undeclared device is not.
    assert_eq!(h.r(&t("x", "$state"), "ready"), Handled::Ignored);
}

#[test]
fn tree_reconciles_children_on_the_ready_edge() {
    let mut h = H::new(Config::tree("r").expect("valid"));
    h.up(false);
    assert_eq!(
        h.cmds(),
        [sub(&[
            &t("r", "$state"),
            &t("r", "$description"),
            &t("r", "+/+"),
            &t("r", "+/+/$target")
        ])]
    );
    h.drain();
    let child = |parent: &str, children: &str| {
        desc(&format!(
            r#""root":"r","parent":"{parent}","children":[{children}]"#
        ))
    };
    h.r(&t("r", "$state"), "init");
    h.r(&t("r", "$description"), &desc(r#""children":["a","b"]"#));
    assert!(h.cmds().is_empty(), "children wait for the ready edge");
    h.r(&t("r", "$state"), "ready");
    assert_eq!(
        h.cmds(),
        [
            sub(&[&t("a", "$state"), &t("a", "$description")]),
            sub(&[&t("b", "$state"), &t("b", "$description")])
        ]
    );
    h.r(&t("a", "$state"), "ready");
    h.r(&t("a", "$description"), &child("r", r#""a1""#));
    // a declares no property: its property filters carry no marker.
    assert_eq!(
        subscribed(&h.cmds()),
        [
            t("a1", "$state"),
            t("a1", "$description"),
            t("a", "+/+"),
            t("a", "+/+/$target")
        ]
    );
    h.r(&t("a1", "$state"), "ready");
    h.r(&t("a1", "$description"), &child("a", ""));
    assert!(!h.c.is_tree_complete("r"));
    h.drain();
    h.r(&t("b", "$description"), &child("r", ""));
    assert!(
        h.all_events()
            .contains(&Event::TreeComplete { root: id("r") })
    );
    assert!(h.c.is_tree_complete("r"));
    h.drain();
    // The root drops a while ready: a1, then a, leaves first.
    h.live(&t("r", "$description"), &desc(r#""children":["b"]"#));
    let ev = h.events();
    let removed: Vec<&Event> = ev
        .iter()
        .filter(|e| matches!(e, Event::DeviceRemoved { .. }))
        .collect();
    assert_eq!(
        removed,
        [
            &Event::DeviceRemoved { device: id("a1") },
            &Event::DeviceRemoved { device: id("a") }
        ]
    );
    assert!(ev.iter().any(|e| matches!(
        e,
        Event::DescriptionChanged {
            while_ready: true,
            ..
        }
    )));
    let gone = unsubscribed(&h.cmds());
    for f in [
        t("a1", "$state"),
        t("a1", "$description"),
        t("a", "$state"),
        t("a", "$description"),
    ] {
        assert!(gone.contains(&f), "{f} in {gone:?}");
    }
    assert!(h.c.device("a").is_none() && h.c.device("a1").is_none());
    // A new child makes the tree incomplete; its description re-arms the event.
    h.live(&t("r", "$description"), &desc(r#""children":["b","c"]"#));
    assert!(!h.c.is_tree_complete("r"));
    h.drain();
    h.r(&t("c", "$description"), &child("r", ""));
    assert!(
        h.all_events()
            .contains(&Event::TreeComplete { root: id("r") })
    );
}

#[test]
fn tree_reconciles_when_the_description_follows_ready() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    h.r(&t("r", "$state"), "ready");
    h.drain();
    h.r(&t("r", "$description"), &desc(r#""children":["a"]"#));
    assert_eq!(
        subscribed(&h.cmds()),
        [t("a", "$state"), t("a", "$description")]
    );
}

#[test]
fn a_clean_reconnect_in_tree_mode_holds_every_child_until_it_is_heard() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    let child = desc(
        r#""root":"r","parent":"r","nodes":{"n":{"properties":{"p":{"datatype":"integer"}}}}"#,
    );
    h.r(&t("r", "$description"), &desc(r#""children":["a","b"]"#));
    h.r(&t("r", "$state"), "ready");
    h.r(&t("a", "$state"), "ready");
    h.r(&t("a", "$description"), &child);
    h.r(&t("a", "n/p"), "1");
    h.r(&t("a", "$description"), &child);
    h.r(&t("b", "$state"), "ready");
    h.r(&t("b", "$description"), &desc(r#""root":"r","parent":"r""#));
    h.send(&t("b", "$state"), b"", Some(true));
    assert!(h.c.device("a").is_some_and(|v| v.synced));
    h.drain();
    h.down();
    h.up(false);
    // The removed b keeps its attribute filters so a return is heard.
    assert_eq!(
        subscribed(&h.cmds()),
        [
            t("r", "$state"),
            t("r", "$description"),
            t("r", "+/+"),
            t("r", "+/+/$target"),
            t("a", "$state"),
            t("a", "$description"),
            t("b", "$state"),
            t("b", "$description"),
        ]
    );
    h.advance(15_000);
    let ev = h.events();
    assert!(
        !ev.iter().any(|e| matches!(e, Event::DeviceSynced { .. })),
        "{ev:?}"
    );
    assert!(h.c.device("a").is_some_and(|v| !v.synced && !v.confirmed));
    h.drain();
    // The root's replayed ready does not request a again.
    h.r(&t("r", "$state"), "ready");
    h.r(&t("r", "$description"), &desc(r#""children":["a","b"]"#));
    assert!(!subscribed(&h.cmds()).contains(&t("a", "$state")));
    h.drain();
    h.r(&t("a", "$state"), "ready");
    h.r(&t("a", "$description"), &child);
    h.r(&t("a", "n/p"), "1");
    assert!(h.events().is_empty());
    h.r(&t("a", "$description"), &child);
    assert!(h.events().contains(&Event::DeviceSynced {
        device: id("a"),
        missing: 0
    }));
    assert!(h.c.device("a").is_some_and(|v| v.synced && v.confirmed));
}

#[test]
fn effective_state_follows_a_lost_root() {
    let mut h = H::wildcard();
    h.add("r", &desc(r#""children":["c"]"#));
    h.add("c", &desc(r#""root":"r","parent":"r""#));
    h.live(&t("r", "$state"), "lost");
    assert_eq!(
        h.events(),
        [
            Event::StateChanged {
                device: id("r"),
                old: Some(DeviceState::Ready),
                new: DeviceState::Lost,
                effective: DeviceState::Lost
            },
            Event::EffectiveStateChanged {
                device: id("c"),
                old: DeviceState::Ready,
                new: DeviceState::Lost
            },
        ]
    );
    assert_eq!(h.c.effective_state("c"), Some(DeviceState::Lost));
    assert_eq!(
        h.c.device("c").and_then(|v| v.state),
        Some(DeviceState::Ready)
    );
    h.live(&t("r", "$state"), "disconnected");
    assert_eq!(
        h.events()[1],
        Event::EffectiveStateChanged {
            device: id("c"),
            old: DeviceState::Lost,
            new: DeviceState::Ready
        }
    );
    h.live(&t("r", "$state"), "sleeping");
    assert_eq!(h.events().len(), 1, "only lost overrides the tree");
    h.live(&t("r", "$state"), "lost");
    h.drain();
    h.send(&t("r", "$state"), b"", Some(true));
    assert!(h.events().contains(&Event::EffectiveStateChanged {
        device: id("c"),
        old: DeviceState::Lost,
        new: DeviceState::Ready
    }));
    // A child that changes state while its root is lost stays lost.
    let mut h = H::wildcard();
    h.add("r", &desc(r#""children":["c"]"#));
    h.add("c", &desc(r#""root":"r","parent":"r""#));
    h.live(&t("r", "$state"), "lost");
    h.drain();
    h.live(&t("c", "$state"), "init");
    assert_eq!(
        h.events(),
        [Event::StateChanged {
            device: id("c"),
            old: Some(DeviceState::Ready),
            new: DeviceState::Init,
            effective: DeviceState::Lost
        }]
    );
}

#[test]
fn a_standalone_device_is_its_own_complete_tree() {
    let mut h = H::wildcard();
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &ints(1));
    assert!(
        h.all_events()
            .contains(&Event::TreeComplete { root: id("d1") })
    );
    assert!(h.c.is_tree_complete("d1"));
    assert!(!h.c.is_tree_complete("nope"));
}

#[test]
fn a_cycle_in_the_declared_tree_terminates() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    h.r(&t("r", "$description"), &desc(r#""children":["a"]"#));
    h.r(&t("r", "$state"), "ready");
    h.r(
        &t("a", "$description"),
        &desc(r#""root":"r","parent":"r","children":["b"]"#),
    );
    h.r(&t("a", "$state"), "ready");
    h.r(
        &t("b", "$description"),
        &desc(r#""root":"r","parent":"a","children":["a","r"]"#),
    );
    h.r(&t("b", "$state"), "ready");
    assert!(h.c.is_tree_complete("r"));
    h.drain();
    h.live(&t("r", "$description"), &desc(""));
    let removed: Vec<Event> = h
        .events()
        .into_iter()
        .filter(|e| matches!(e, Event::DeviceRemoved { .. }))
        .collect();
    assert_eq!(
        removed,
        [
            Event::DeviceRemoved { device: id("b") },
            Event::DeviceRemoved { device: id("a") }
        ]
    );
    assert!(h.c.device("r").is_some(), "the root is never pruned");
}

#[test]
fn new_nodes_of_a_changed_description_are_paced_in() {
    let mut h = H::paced(8, 4);
    let nodes = |ids: &[&str]| {
        let n: Vec<String> = ids
            .iter()
            .map(|i| format!(r#""{i}":{{"properties":{{"p":{{"datatype":"integer"}}}}}}"#))
            .collect();
        desc(&format!(r#""nodes":{{{}}}"#, n.join(",")))
    };
    let three = nodes(&["a", "b", "c"]);
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &three);
    h.r(&t("d1", "$description"), &three);
    h.r(&t("d1", "$description"), &three);
    h.drain();
    h.live(&t("d1", "$description"), &nodes(&["a", "b", "c", "d"]));
    assert_eq!(
        h.cmds(),
        [sub(&[
            &t("d1", "d/+"),
            &t("d1", "d/+/$target"),
            &t("d1", "$description")
        ])]
    );
    // A device covered by its whole-device filters needs nothing more.
    let mut h = H::wildcard();
    h.add("d2", &nodes(&["a"]));
    h.live(&t("d2", "$description"), &nodes(&["a", "b"]));
    assert!(h.cmds().is_empty());
}

#[test]
fn device_mode_removal_releases_the_root_filter() {
    let mut h = H::connected(Config::device("c").expect("valid"));
    h.r(&t("c", "$state"), "ready");
    h.r(&t("c", "$description"), &desc(r#""root":"r","parent":"r""#));
    h.drain();
    h.send(&t("c", "$state"), b"", Some(true));
    let gone = unsubscribed(&h.cmds());
    assert_eq!(
        gone,
        [t("c", "+/+"), t("c", "+/+/$target"), t("r", "$state")]
    );
    assert_eq!(h.events(), [Event::DeviceRemoved { device: id("c") }]);
    // It returns naming another root.
    h.r(&t("c", "$state"), "ready");
    h.drain();
    h.r(&t("c", "$description"), &desc(r#""root":"q","parent":"q""#));
    assert_eq!(h.cmds(), [sub(&[&t("q", "$state")])]);
}

#[test]
fn any_domain_sets_in_the_devices_own_domain() {
    let mut cfg = Config::wildcard();
    cfg.domain = Domain::Any;
    let mut h = H::connected(cfg);
    h.r("homie/5/d1/$state", "ready");
    h.r(
        "homie/5/d1/$description",
        &node(&[("p", r#""datatype":"boolean","settable":true"#)]),
    );
    h.drain();
    assert_eq!(
        h.c.set(&addr("d1", "n", "p"), "true", SetOptions::default()),
        Ok(())
    );
    assert_eq!(
        h.cmds(),
        [Command::Publish {
            topic: "homie/5/d1/n/p/set".into(),
            payload: b"true".to_vec(),
            qos: Q0,
            retain: false
        }]
    );
}

#[test]
fn a_tree_child_never_heard_is_not_synced() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    h.r(&t("r", "$description"), &desc(r#""children":["a"]"#));
    h.r(&t("r", "$state"), "ready");
    h.r(&t("a", "$description"), &desc(r#""root":"r","parent":"r""#));
    h.drain();
    h.advance(120_000);
    let ev = h.events();
    assert!(
        !ev.iter()
            .any(|e| matches!(e, Event::DeviceSynced { device, .. } if *device == "a")),
        "{ev:?}"
    );
    assert!(ev.contains(&Event::DeviceStuck {
        device: id("a"),
        missing: Missing::State,
        attempt: 3,
        gave_up: true
    }));
    assert!(h.c.device("a").is_none());
}

// ===== /set and $broadcast =====

fn settable() -> String {
    node(&[
        (
            "i",
            r#""datatype":"integer","settable":true,"format":"0:10:2""#,
        ),
        ("s", r#""datatype":"string","settable":true"#),
        ("ro", r#""datatype":"integer""#),
    ])
}

#[test]
fn set_rejections_in_order() {
    let mut h = H::new(Config::wildcard());
    let o = SetOptions::default();
    assert_eq!(
        h.c.set(&addr("d1", "n", "i"), "2", o),
        Err(SetError::NotConnected)
    );
    h.up(false);
    assert_eq!(
        h.c.set(&addr("d1", "n", "i"), "2", o),
        Err(SetError::UnknownDevice)
    );
    h.r(&t("d1", "$state"), "init");
    assert_eq!(
        h.c.set(&addr("d1", "n", "i"), "2", o),
        Err(SetError::UnknownProperty)
    );
    h.r(&t("d1", "$description"), &settable());
    assert_eq!(
        h.c.set(&addr("d1", "n", "x"), "2", o),
        Err(SetError::UnknownProperty)
    );
    assert_eq!(
        h.c.set(&addr("d1", "m", "i"), "2", o),
        Err(SetError::UnknownProperty)
    );
    assert_eq!(
        h.c.set(&addr("d1", "n", "ro"), "2", o),
        Err(SetError::NotSettable)
    );
    assert_eq!(
        h.c.set(&addr("d1", "n", "i"), "bad", o),
        Err(SetError::DeviceNotReady(DeviceState::Init)),
        "readiness is checked before the payload"
    );
    let anyway = SetOptions {
        allow_not_ready: true,
        unchecked: false,
    };
    assert_eq!(
        h.c.set(&addr("d1", "n", "i"), "bad", anyway),
        Err(SetError::Invalid(Problem::IntegerGrammar))
    );
    assert_eq!(
        h.c.set(&addr("d1", "n", "i"), "11", anyway),
        Err(SetError::Invalid(Problem::AboveMax))
    );
    h.drain();
    assert_eq!(
        h.c.set(
            &addr("d1", "n", "i"),
            "bad",
            SetOptions {
                allow_not_ready: true,
                unchecked: true
            }
        ),
        Ok(())
    );
    assert_eq!(
        h.cmds(),
        [Command::Publish {
            topic: t("d1", "n/i/set"),
            payload: b"bad".to_vec(),
            qos: Q0,
            retain: false
        }]
    );
}

#[test]
fn set_sends_the_callers_bytes() {
    let mut h = H::wildcard();
    h.add("d1", &settable());
    let o = SetOptions::default();
    // Step rounding is checked (3 rounds to 4) but the bytes go out unchanged.
    assert_eq!(h.c.set(&addr("d1", "n", "i"), "3", o), Ok(()));
    assert_eq!(h.c.set(&addr("d1", "n", "s"), "", o), Ok(()));
    assert_eq!(
        h.cmds(),
        [
            Command::Publish {
                topic: t("d1", "n/i/set"),
                payload: b"3".to_vec(),
                qos: Q0,
                retain: false
            },
            Command::Publish {
                topic: t("d1", "n/s/set"),
                payload: vec![0],
                qos: Q0,
                retain: false
            },
        ]
    );
    // A lost root makes the device not ready.
    h.add("r", &desc(r#""children":["c"]"#));
    h.add("c", &desc(r#""root":"r","parent":"r","nodes":{"n":{"properties":{"i":{"datatype":"integer","settable":true}}}}"#));
    h.live(&t("r", "$state"), "lost");
    assert_eq!(
        h.c.set(&addr("c", "n", "i"), "1", o),
        Err(SetError::DeviceNotReady(DeviceState::Lost))
    );
    h.down();
    assert_eq!(
        h.c.set(&addr("d1", "n", "i"), "2", o),
        Err(SetError::NotConnected)
    );
}

#[test]
fn set_uses_the_last_valid_value_as_the_step_base() {
    let mut h = H::wildcard();
    h.add(
        "d1",
        &node(&[(
            "f",
            r#""datatype":"float","settable":true,"format":"::0.5""#,
        )]),
    );
    h.r(&t("d1", "n/f"), "0.25");
    assert_eq!(
        h.c.set(&addr("d1", "n", "f"), "1.1", SetOptions::default()),
        Ok(())
    );
}

#[test]
fn broadcast_checks_the_subtopic() {
    let mut h = H::new(Config::wildcard());
    assert_eq!(h.c.broadcast("a", "x"), Err(SetError::NotConnected));
    h.up(false);
    h.drain();
    assert_eq!(h.c.broadcast("a/B", "x"), Err(SetError::InvalidTopic));
    assert_eq!(h.c.broadcast("", "x"), Err(SetError::InvalidTopic));
    assert_eq!(h.c.broadcast("a//b", "x"), Err(SetError::InvalidTopic));
    assert_eq!(h.c.broadcast("alert/all", "hi"), Ok(()));
    assert_eq!(
        h.cmds(),
        [Command::Publish {
            topic: "ebus/5/$broadcast/alert/all".into(),
            payload: b"hi".to_vec(),
            qos: Q0,
            retain: false
        }]
    );
    let mut cfg = Config::wildcard();
    cfg.domain = Domain::Any;
    let mut h = H::connected(cfg);
    assert_eq!(h.c.broadcast("a", "x"), Err(SetError::InvalidTopic));
}

// ===== shutdown and host filters =====

#[test]
fn shutdown_releases_everything_but_host_filters() {
    let mut cfg = Config::wildcard();
    cfg.host_filters = vec!["ebus/5/+/$state".into(), t("d1", "+/+")];
    let mut h = H::connected(cfg);
    h.add("d1", &ints(1));
    h.c.shutdown();
    // In byte order, without the two host filters.
    assert_eq!(
        h.cmds(),
        [unsub(&[&t("d1", "$description"), &t("d1", "+/+/$target")])]
    );
    assert_eq!(h.c.poll_timeout(), None);
    assert!(!h.c.is_connected());
    assert_eq!(h.r(&t("d2", "$state"), "ready"), Handled::Ignored);
    assert_eq!(
        h.c.set(&addr("d1", "n", "p0"), "1", SetOptions::default()),
        Err(SetError::NotConnected)
    );
    assert!(h.c.device("d1").is_some(), "views stay readable");
    h.c.shutdown();
    assert!(h.cmds().is_empty());
}

#[test]
fn host_filters_are_never_unsubscribed() {
    let mut cfg = Config::wildcard();
    cfg.host_filters = vec![t("d1", "$description")];
    let mut h = H::connected(cfg);
    h.add("d1", &ints(1));
    h.send(&t("d1", "$state"), b"", Some(true));
    let gone = unsubscribed(&h.cmds());
    assert_eq!(gone, [t("d1", "+/+"), t("d1", "+/+/$target")]);
}

#[test]
fn every_timer_is_consumed() {
    // Drive a busy scenario through every timer kind; `advance` asserts that
    // no deadline is returned again after it was handled.
    let mut h = H::paced(2, 6);
    for i in 0..6 {
        let d = format!("d{i}");
        h.r(&t(&d, "$state"), "ready");
        h.r(&t(&d, "$description"), &ints(i));
    }
    h.c.handle_command_failed(h.now, sub(&["x/y"]));
    h.c.handle_dropped(h.now, &t("d1", "n/p0"));
    h.advance(120_000);
    h.down();
    h.up(false);
    h.advance(120_000);
    assert!(h.c.devices().all(|v| v.state.is_some()));
}

// ===== robustness =====

/// Random input in every mode: no panic, no timer spin, and the views stay
/// consistent with the events.
#[test]
fn random_input_never_panics() {
    for seed in [
        0x2545_f491_4f6c_dd1d,
        0x9e37_79b9_7f4a_7c15,
        0xd1b5_4a32_d192_ed03,
    ] {
        random_run(seed);
    }
}

fn random_run(mut seed: u64) {
    let mut next = move |n: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % n as u64) as usize
    };
    let devices = ["r", "a", "b", "c"];
    let attrs = [
        "$state",
        "$description",
        "n/p",
        "n/q",
        "n/p/$target",
        "m/x",
        "$alert/a",
        "$log/info",
        "n/p/set",
        "$bogus",
    ];
    let states = [
        "init",
        "ready",
        "lost",
        "sleeping",
        "disconnected",
        "",
        "Ready",
        "\u{0}",
    ];
    let descs = [
        desc(
            r#""children":["a","b"],"nodes":{"n":{"properties":{"p":{"datatype":"integer","settable":true},"q":{"datatype":"string","retained":false}}}}"#,
        ),
        desc(
            r#""root":"r","parent":"r","children":["c"],"nodes":{"n":{"properties":{"p":{"datatype":"float","format":"0:1"}}}}"#,
        ),
        desc(
            r#""root":"r","parent":"a","nodes":{"m":{"properties":{"x":{"datatype":"enum","format":"a,b"}}}}"#,
        ),
        desc(r#""root":"r","parent":"r","children":["a","r"]"#),
        r#"{"homie":"4.0","version":1}"#.to_string(),
        "not json".to_string(),
        String::new(),
    ];
    let values = [
        "1",
        "0.5",
        "a",
        "",
        "\u{0}",
        "x",
        "true",
        "-9223372036854775809",
    ];
    let configs = [
        Config::wildcard(),
        Config::tree("r").expect("valid"),
        Config::device("a").expect("valid"),
        {
            let mut c = Config::wildcard();
            c.pacing = Some(Pacing {
                batch_devices: 1,
                batch_values: 2,
            });
            c.max_early_values = 1;
            c.max_devices = Some(3);
            c
        },
        {
            let mut c = Config::wildcard();
            c.domain = Domain::Any;
            c.pacing = None;
            c.trust_session = true;
            c
        },
    ];
    for (ci, cfg) in configs.into_iter().enumerate() {
        let mut h = H::new(cfg);
        let mut log: Vec<String> = Vec::new();
        for step in 0..10_000 {
            match next(20) {
                0 => h.down(),
                1 => h.up(next(2) == 0),
                2 => h.advance(next(15_000) as u64),
                3 => {
                    let topic = t(devices[next(devices.len())], attrs[next(attrs.len())]);
                    h.c.handle_dropped(h.now, &topic);
                }
                4 => {
                    let cmd = h.cmds().into_iter().next();
                    if let Some(cmd) = cmd {
                        h.c.handle_command_failed(h.now, cmd);
                    }
                }
                5 => {
                    let a = addr(devices[next(devices.len())], "n", "p");
                    let opts = SetOptions {
                        allow_not_ready: next(2) == 0,
                        unchecked: next(4) == 0,
                    };
                    let _ = h.c.set(&a, values[next(values.len())], opts);
                    let _ = h.c.broadcast("x/y", "1");
                }
                6 if next(50) == 0 => h.c.shutdown(),
                _ => {
                    let d = devices[next(devices.len())];
                    let attr = attrs[next(attrs.len())];
                    let payload: String = match attr {
                        "$state" => states[next(states.len())].to_string(),
                        "$description" => descs[next(descs.len())].clone(),
                        _ => values[next(values.len())].to_string(),
                    };
                    let domain = if next(10) == 0 { "homie" } else { "ebus" };
                    let topic = format!("{domain}/5/{d}/{attr}");
                    let retain = [Some(true), Some(false), None][next(3)];
                    log.push(format!("{topic} {payload:?}"));
                    h.send(&topic, payload.as_bytes(), retain);
                }
            }
            for v in h.c.devices() {
                assert!(v.state.is_some());
                assert_eq!(
                    v.effective,
                    v.state.map(|s| effective_state(
                        v.description
                            .as_ref()
                            .and_then(|d| d.root.as_deref())
                            .filter(|r| *r != v.id.as_str())
                            .and_then(|r| {
                                h.c.devices()
                                    .find(|rv| rv.id == r && rv.domain == v.domain)
                                    .and_then(|rv| rv.state)
                                    .or_else(|| h.c.aux_states.get(r).copied())
                            }),
                        s
                    )),
                    "config {ci} step {step} device {} domain {}; last: {:?}",
                    v.id,
                    v.domain,
                    &log[log.len().saturating_sub(12)..]
                );
            }
            h.drain();
        }
    }
}

// ===== review regressions =====

#[test]
fn a_resync_clears_what_the_broker_no_longer_retains() {
    let mut h = H::wildcard();
    let d = ints(2);
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &d);
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "n/p1"), "2");
    h.r(&t("d1", "n/p1/$target"), "3");
    h.r(&t("d1", "$alert/hot"), "too hot");
    h.r(&t("d1", "$description"), &d);
    h.drain();
    // p1, its target and the alert are deleted while the link is down.
    h.down();
    h.up(false);
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &d);
    h.drain();
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "$description"), &d);
    let ev = h.events();
    assert!(
        ev.contains(&Event::PropertyCleared {
            addr: addr("d1", "n", "p1")
        }),
        "{ev:?}"
    );
    assert!(ev.contains(&Event::TargetChanged {
        addr: addr("d1", "n", "p1"),
        target: None
    }));
    assert!(ev.contains(&Event::Alert {
        device: id("d1"),
        alert: id("hot"),
        message: None
    }));
    assert_eq!(h.prop("d1", "n", "p1"), Some(PropertyView::default()));
    assert_eq!(
        h.prop("d1", "n", "p0").and_then(|p| p.value),
        Some(val("1", Ok(())))
    );
    assert_eq!(h.c.device("d1").map(|d| d.alerts().count()), Some(0));
    h.advance(20_000);
    assert!(h.events().contains(&Event::DeviceSynced {
        device: id("d1"),
        missing: 1
    }));
}

#[test]
fn a_marker_without_the_retain_bit_clears_nothing() {
    let mut h = H::wildcard();
    let d = ints(2);
    h.send(&t("d1", "$state"), b"ready", None);
    h.send(&t("d1", "$description"), d.as_bytes(), None);
    h.send(&t("d1", "n/p0"), b"1", None);
    h.send(&t("d1", "n/p1"), b"2", None);
    h.send(&t("d1", "$description"), d.as_bytes(), None);
    h.drain();
    h.down();
    h.up(false);
    h.send(&t("d1", "$state"), b"ready", None);
    h.send(&t("d1", "$description"), d.as_bytes(), None);
    h.send(&t("d1", "n/p0"), b"1", None);
    h.send(&t("d1", "$description"), d.as_bytes(), None);
    assert!(
        !h.events()
            .iter()
            .any(|e| matches!(e, Event::PropertyCleared { .. }))
    );
    assert!(h.prop("d1", "n", "p1").and_then(|p| p.value).is_some());
}

/// `dev` with `p0`, `p1`, a `$target` on `p1` and an alert, all retained.
fn held(h: &mut H, dev: &str) {
    h.r(&t(dev, "$state"), "ready");
    h.r(&t(dev, "$description"), &ints(2));
    h.r(&t(dev, "n/p0"), "1");
    h.r(&t(dev, "n/p1"), "2");
    h.r(&t(dev, "n/p1/$target"), "3");
    h.r(&t(dev, "$alert/hot"), "too hot");
    h.r(&t(dev, "$description"), &ints(2));
    h.drain();
}

fn cleared(ev: &[Event]) -> usize {
    ev.iter()
        .filter(|e| {
            matches!(
                e,
                Event::PropertyCleared { .. }
                    | Event::TargetChanged { target: None, .. }
                    | Event::Alert { message: None, .. }
            )
        })
        .count()
}

/// p1, its `$target` and the alert are cleared, and p0 is kept.
fn assert_cleared_p1(h: &mut H, dev: &str) {
    let ev = h.events();
    assert!(
        ev.contains(&Event::PropertyCleared {
            addr: addr(dev, "n", "p1")
        }),
        "{ev:?}"
    );
    assert!(ev.contains(&Event::TargetChanged {
        addr: addr(dev, "n", "p1"),
        target: None
    }));
    assert!(ev.contains(&Event::Alert {
        device: id(dev),
        alert: id("hot"),
        message: None
    }));
    assert_eq!(cleared(&ev), 3, "{ev:?}");
    assert_eq!(h.prop(dev, "n", "p1"), Some(PropertyView::default()));
    assert_eq!(
        h.prop(dev, "n", "p0").and_then(|p| p.value),
        Some(val("1", Ok(())))
    );
}

fn unpaced_configs() -> Vec<Config> {
    let mut wildcard = Config::wildcard();
    wildcard.pacing = None;
    vec![
        Config::tree("r").expect("valid"),
        Config::device("r").expect("valid"),
        wildcard,
    ]
}

#[test]
fn an_unpaced_resync_clears_what_the_broker_no_longer_retains() {
    for cfg in unpaced_configs() {
        let mut h = H::connected(cfg);
        held(&mut h, "r");
        h.down();
        h.up(false);
        let cmds = h.cmds();
        let props = subscribed(&cmds[cmds.len() - 2..cmds.len() - 1]);
        assert!(props.contains(&t("r", "+/+")), "{cmds:?}");
        assert_eq!(cmds.last(), Some(&sub(&[&t("r", "$description")])));
        h.r(&t("r", "$state"), "ready");
        h.r(&t("r", "$description"), &ints(2));
        h.r(&t("r", "n/p0"), "1");
        // The stage-one `$description` comes before the property replays.
        let ev = h.events();
        assert_eq!(cleared(&ev), 0);
        assert!(
            !ev.iter().any(|e| matches!(e, Event::DeviceSynced { .. })),
            "{ev:?}"
        );
        h.r(&t("r", "$description"), &ints(2));
        assert_cleared_p1(&mut h, "r");
        h.advance(10_000);
        assert_eq!(
            h.events(),
            [Event::DeviceSynced {
                device: id("r"),
                missing: 1
            }]
        );
    }
}

#[test]
fn an_unpaced_resync_without_the_retain_bit_clears_nothing() {
    for cfg in unpaced_configs() {
        let mut h = H::connected(cfg);
        held(&mut h, "r");
        h.down();
        h.up(false);
        h.drain();
        h.send(&t("r", "$state"), b"ready", None);
        h.send(&t("r", "$description"), ints(2).as_bytes(), None);
        h.send(&t("r", "n/p0"), b"1", None);
        h.send(&t("r", "$description"), ints(2).as_bytes(), None);
        let ev = h.events();
        assert_eq!(cleared(&ev), 0);
        assert!(
            ev.contains(&Event::DeviceSynced {
                device: id("r"),
                missing: 0
            }),
            "{ev:?}"
        );
        assert!(h.prop("r", "n", "p1").and_then(|p| p.value).is_some());
    }
}

#[test]
fn dropped_healing_clears_what_the_broker_no_longer_retains() {
    let mut unpaced = Config::wildcard();
    unpaced.pacing = None;
    for cfg in [Config::wildcard(), unpaced] {
        let mut h = H::connected(cfg);
        held(&mut h, "d1");
        // The host dropped the deletions of p1, its target and the alert.
        h.c.handle_dropped(h.now, &t("d1", "n/p1"));
        h.advance(1);
        let cmds = h.cmds();
        assert_eq!(
            subscribed(&cmds).last(),
            Some(&t("d1", "$description")),
            "{cmds:?}"
        );
        h.r(&t("d1", "$description"), &ints(2));
        h.r(&t("d1", "n/p0"), "1");
        assert_eq!(cleared(&h.events()), 0);
        h.r(&t("d1", "$description"), &ints(2));
        assert_cleared_p1(&mut h, "d1");
    }
}

#[test]
fn a_lost_message_keeps_a_later_marker_from_clearing() {
    for cfg in unpaced_configs() {
        let mut h = H::connected(cfg);
        held(&mut h, "r");
        h.down();
        h.up(false);
        h.r(&t("r", "$state"), "ready");
        h.r(&t("r", "$description"), &ints(2));
        // The host dropped the replay of p1 and reports it before the marker.
        h.c.handle_lost(&t("r", "n/p1"));
        h.r(&t("r", "n/p0"), "1");
        h.r(&t("r", "$description"), &ints(2));
        assert_eq!(cleared(&h.events()), 0);
        assert!(h.prop("r", "n", "p1").and_then(|p| p.value).is_some());
    }
}

#[test]
fn stuck_healing_clears_once_a_later_replay_arrives() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    held(&mut h, "r");
    h.down();
    h.up(false);
    h.drain();
    // The `$description` replays are lost; stage one heals.
    h.r(&t("r", "$state"), "ready");
    h.advance(10_000);
    let cmds = h.cmds();
    assert_eq!(
        cmds.last(),
        Some(&sub(&[&t("r", "$description")])),
        "{cmds:?}"
    );
    assert!(h.events().iter().any(|e| matches!(
        e,
        Event::DeviceStuck {
            missing: Missing::Description,
            ..
        }
    )));
    h.r(&t("r", "$state"), "ready");
    h.r(&t("r", "$description"), &ints(2));
    h.r(&t("r", "n/p0"), "1");
    assert_eq!(cleared(&h.events()), 0);
    h.r(&t("r", "$description"), &ints(2));
    assert_cleared_p1(&mut h, "r");
}

#[test]
fn a_replay_requested_before_a_link_up_does_not_end_a_later_window() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    held(&mut h, "r");
    // Two SUBSCRIBEs carrying r/$description, sent after the reconnect.
    h.c.handle_dropped(h.now, &t("r", "n/p0"));
    h.advance(1);
    assert_eq!(
        subscribed(&h.cmds())
            .iter()
            .filter(|f| **f == t("r", "$description"))
            .count(),
        2
    );
    h.down();
    h.up(false);
    h.drain();
    h.r(&t("r", "$description"), &ints(2));
    h.r(&t("r", "$description"), &ints(2));
    h.r(&t("r", "$state"), "ready");
    h.r(&t("r", "$description"), &ints(2));
    assert_eq!(cleared(&h.events()), 0);
    h.r(&t("r", "n/p0"), "1");
    h.r(&t("r", "n/p1"), "2");
    h.r(&t("r", "n/p1/$target"), "3");
    h.r(&t("r", "$alert/hot"), "too hot");
    h.r(&t("r", "$description"), &ints(2));
    assert_eq!(cleared(&h.events()), 0);
}

#[test]
fn a_resync_whose_property_subscribe_failed_clears_nothing() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    held(&mut h, "r");
    h.down();
    h.up(false);
    let cmds = h.cmds();
    assert_eq!(cmds.len(), 2, "{cmds:?}");
    h.c.handle_command_failed(h.now, cmds[0].clone());
    h.r(&t("r", "$description"), &ints(2));
    h.advance(1_000);
    assert_eq!(h.cmds(), cmds[..1]);
    h.r(&t("r", "$state"), "ready");
    h.r(&t("r", "$description"), &ints(2));
    assert_eq!(cleared(&h.events()), 0);
    h.r(&t("r", "n/p0"), "1");
    h.r(&t("r", "n/p1"), "2");
    assert_eq!(cleared(&h.events()), 0);
}

#[test]
fn a_host_filter_on_the_description_disables_clearing() {
    let mut cfg = Config::wildcard();
    cfg.host_filters = vec!["ebus/5/#".into()];
    let mut h = H::connected(cfg);
    held(&mut h, "d1");
    h.down();
    h.up(false);
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &ints(2));
    h.r(&t("d1", "$description"), &ints(2));
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "$description"), &ints(2));
    assert_eq!(cleared(&h.events()), 0);
}

#[test]
fn a_resumed_session_clears_nothing() {
    let mut h = H::wildcard();
    held(&mut h, "d1");
    h.down();
    h.up(true);
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &ints(2));
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "$description"), &ints(2));
    assert_eq!(cleared(&h.events()), 0);
    assert!(h.prop("d1", "n", "p1").and_then(|p| p.value).is_some());
}

/// Configs whose re-sync opens a window: the unpaced ones and paced wildcard.
fn window_configs() -> Vec<Config> {
    let mut v = unpaced_configs();
    v.push(Config::wildcard());
    v
}

/// A link-up that may resume a session: `session_present: true`, or a
/// message while down.
fn resume(h: &mut H, implicit: bool) {
    h.down();
    if !implicit {
        h.up(true);
    }
}

/// The replays of a re-sync of `held`, without p1, its `$target` and the
/// alert.
fn replay_without_p1(h: &mut H, dev: &str) {
    h.r(&t(dev, "$state"), "ready");
    h.r(&t(dev, "$description"), &ints(2));
    h.r(&t(dev, "n/p0"), "1");
    h.r(&t(dev, "$description"), &ints(2));
}

#[test]
fn a_resumed_session_syncs_without_waiting_for_the_timeout() {
    for implicit in [false, true] {
        for cfg in window_configs() {
            let mut h = H::connected(cfg);
            held(&mut h, "r");
            resume(&mut h, implicit);
            h.r(&t("r", "$state"), "ready");
            h.r(&t("r", "$description"), &ints(2));
            h.r(&t("r", "n/p0"), "1");
            h.r(&t("r", "n/p1"), "2");
            h.r(&t("r", "n/p1/$target"), "3");
            h.r(&t("r", "$alert/hot"), "too hot");
            h.r(&t("r", "$description"), &ints(2));
            let ev = h.events();
            assert!(
                ev.contains(&Event::DeviceSynced {
                    device: id("r"),
                    missing: 0
                }),
                "implicit={implicit} {ev:?}"
            );
            assert_eq!(cleared(&ev), 0);
        }
    }
}

#[test]
fn a_clean_resync_after_a_resumed_session_clears() {
    for implicit in [false, true] {
        for cfg in window_configs() {
            let mut h = H::connected(cfg);
            held(&mut h, "r");
            resume(&mut h, implicit);
            h.r(&t("r", "$state"), "ready");
            h.r(&t("r", "$description"), &ints(2));
            h.r(&t("r", "n/p0"), "1");
            h.r(&t("r", "n/p1"), "2");
            h.r(&t("r", "n/p1/$target"), "3");
            h.r(&t("r", "$alert/hot"), "too hot");
            h.r(&t("r", "$description"), &ints(2));
            h.drain();
            for _ in 0..3 {
                h.down();
                h.up(false);
                replay_without_p1(&mut h, "r");
                assert_cleared_p1(&mut h, "r");
                // The broker retains p1 again before the next round.
                h.r(&t("r", "n/p1"), "2");
                h.r(&t("r", "n/p1/$target"), "3");
                h.r(&t("r", "$alert/hot"), "too hot");
                h.drain();
            }
        }
    }
}

#[test]
fn a_valid_description_after_a_rejected_one_replays_the_values() {
    let mut cfg = Config::wildcard();
    cfg.pacing = None;
    let mut h = H::connected(cfg);
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), "not json");
    h.r(&t("d1", "n/p0"), "7");
    h.drain();
    h.live(&t("d1", "$description"), &ints(1));
    let cmds = h.cmds();
    let w = whole("d1");
    assert_eq!(cmds, [sub(&[&w[0], &w[1]])], "the property filters again");
    h.r(&t("d1", "n/p0"), "7");
    assert_eq!(
        h.prop("d1", "n", "p0").and_then(|p| p.value),
        Some(val("7", Ok(())))
    );
}

#[test]
fn a_description_change_while_sleeping_is_flagged() {
    let mut h = H::wildcard();
    h.add("d1", &ints(1));
    h.live(&t("d1", "$state"), "sleeping");
    h.drain();
    h.live(&t("d1", "$description"), &ints(2));
    assert!(
        !h.events()
            .iter()
            .any(|e| matches!(e, Event::DescriptionChanged { .. }))
    );
    h.live(&t("d1", "$state"), "ready");
    let flagged: Vec<bool> = h
        .events()
        .iter()
        .filter_map(|e| match e {
            Event::DescriptionChanged { while_ready, .. } => Some(*while_ready),
            _ => None,
        })
        .collect();
    assert_eq!(flagged, [true]);
    // A change while init is allowed.
    h.live(&t("d1", "$state"), "init");
    h.live(&t("d1", "$description"), &ints(3));
    h.live(&t("d1", "$state"), "ready");
    let flagged: Vec<bool> = h
        .events()
        .iter()
        .filter_map(|e| match e {
            Event::DescriptionChanged { while_ready, .. } => Some(*while_ready),
            _ => None,
        })
        .collect();
    assert_eq!(flagged, [false]);
}

#[test]
fn an_empty_set_payload_is_0x00_only_for_strings() {
    let mut h = H::wildcard();
    h.add("d1", &settable());
    let unchecked = SetOptions {
        unchecked: true,
        ..SetOptions::default()
    };
    assert_eq!(h.c.set(&addr("d1", "n", "i"), "", unchecked), Ok(()));
    assert_eq!(
        h.cmds(),
        [Command::Publish {
            topic: t("d1", "n/i/set"),
            payload: Vec::new(),
            qos: Q0,
            retain: false
        }]
    );
}

#[test]
fn poll_timeout_is_a_floor_that_handle_timeout_tightens() {
    let mut h = H::wildcard();
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &ints(1));
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "$description"), &ints(1));
    assert!(h.events().contains(&Event::DeviceSynced {
        device: id("d1"),
        missing: 0
    }));
    // The sync deadline was disarmed; the floor may still report it.
    let t0 = h.c.poll_timeout().expect("the disarmed deadline's floor");
    h.now = h.now.max(t0);
    h.c.handle_timeout(h.now);
    assert!(h.events().is_empty());
    assert!(h.c.poll_timeout().is_none_or(|t| t > t0));
}

#[test]
fn renamed_nodes_release_their_filters() {
    let mut h = H::paced(8, 2);
    let two = |a: &str, b: &str| {
        desc(&format!(
            r#""nodes":{{"{a}":{{"properties":{{"p":{{"datatype":"integer"}}}}}},"{b}":{{"properties":{{"p":{{"datatype":"integer"}}}}}}}}"#
        ))
    };
    let mut d = two("a", "b");
    h.r(&t("d1", "$state"), "ready");
    h.r(&t("d1", "$description"), &d);
    h.r(&t("d1", "$description"), &d);
    h.r(&t("d1", "$description"), &d);
    h.drain();
    let held = |h: &H| h.c.ledger.held().count();
    let before = held(&h);
    for round in 0..5 {
        let next = two(&format!("x{round}"), "b");
        h.r(&t("d1", "$description"), &next);
        let old = if round == 0 {
            String::from("a")
        } else {
            format!("x{}", round - 1)
        };
        assert_eq!(
            unsubscribed(&h.cmds()),
            [
                t("d1", &format!("{old}/+")),
                t("d1", &format!("{old}/+/$target"))
            ]
        );
        h.r(&t("d1", "$description"), &next);
        h.drain();
        d = next;
        assert_eq!(held(&h), before, "round {round}");
    }
    assert!(h.c.ledger.held().all(|f| !f.contains("/a/")));
    let _ = d;
}

#[test]
fn alerts_per_device_are_capped() {
    let mut cfg = Config::wildcard();
    cfg.max_alerts = 2;
    let mut h = H::connected(cfg);
    h.add("d1", &ints(0));
    for i in 0..5 {
        h.live(&t("d1", &format!("$alert/a{i}")), "on");
    }
    assert_eq!(h.events().len(), 2);
    assert_eq!(h.c.device("d1").map(|d| d.alerts().count()), Some(2));
    // A known alert still updates and clears.
    h.live(&t("d1", "$alert/a0"), "off");
    h.send(&t("d1", "$alert/a1"), b"", Some(false));
    assert_eq!(h.events().len(), 2);
}

#[test]
fn a_child_another_parent_declares_stays() {
    let mut h = H::new(Config::tree("r").expect("valid"));
    h.up(false);
    h.drain();
    let child = |children: &str| {
        desc(&format!(
            r#""root":"r","parent":"r","children":[{children}]"#
        ))
    };
    h.r(&t("r", "$description"), &desc(r#""children":["a","b"]"#));
    h.r(&t("r", "$state"), "ready");
    for p in ["a", "b"] {
        h.r(&t(p, "$state"), "ready");
        h.r(&t(p, "$description"), &child(r#""x""#));
    }
    h.r(&t("x", "$state"), "ready");
    h.r(&t("x", "$description"), &desc(r#""root":"r","parent":"b""#));
    h.drain();
    // a lets go of x; b still declares it.
    h.live(&t("a", "$state"), "init");
    h.live(&t("a", "$description"), &child(""));
    h.live(&t("a", "$state"), "ready");
    assert!(
        !h.events()
            .iter()
            .any(|e| matches!(e, Event::DeviceRemoved { .. }))
    );
    assert!(h.c.device("x").is_some());
    assert!(h.c.is_tree_complete("r"));
    assert!(unsubscribed(&h.cmds()).is_empty());
    // b lets go too: x goes.
    h.live(&t("b", "$state"), "init");
    h.live(&t("b", "$description"), &child(""));
    h.live(&t("b", "$state"), "ready");
    assert!(
        h.events()
            .contains(&Event::DeviceRemoved { device: id("x") })
    );
    assert!(h.c.device("x").is_none());
}

#[test]
fn a_dropped_state_resubscribes_discovery_in_wildcard_mode() {
    let mut h = H::wildcard();
    h.add("d1", &ints(0));
    h.c.handle_dropped(h.now, &t("d1", "$state"));
    h.advance(0);
    assert!(subscribed(&h.cmds()).contains(&String::from("ebus/5/+/$state")));
    // A dropped value does not.
    h.c.handle_dropped(h.now, &t("d1", "n/p0"));
    h.advance(0);
    assert!(!subscribed(&h.cmds()).contains(&String::from("ebus/5/+/$state")));
}

#[test]
fn a_repeated_value_or_target_is_quiet_whatever_its_origin() {
    let mut h = H::wildcard();
    h.add(
        "d1",
        &node(&[
            ("p", r#""datatype":"integer""#),
            ("e", r#""datatype":"integer","retained":false"#),
        ]),
    );
    h.live(&t("d1", "n/p"), "1");
    h.live(&t("d1", "n/p/$target"), "2");
    h.drain();
    for retain in [Some(false), None, Some(true)] {
        h.send(&t("d1", "n/p"), b"1", retain);
        h.send(&t("d1", "n/p/$target"), b"2", retain);
    }
    assert_eq!(h.events(), []);
    // A `retained: false` property is a stream of events, repeats included.
    h.live(&t("d1", "n/e"), "5");
    h.live(&t("d1", "n/e"), "5");
    let ev = h.events();
    assert_eq!(ev.len(), 2, "{ev:?}");
    assert!(ev.iter().all(|e| matches!(e, Event::PropertyEvent { .. })));
}

#[test]
fn input_that_changes_nothing_emits_nothing_and_keeps_the_view() {
    let mut h = H::wildcard();
    let d = ints(2);
    h.add("d1", &d);
    h.r(&t("d1", "n/p0"), "1");
    h.r(&t("d1", "n/p0/$target"), "2");
    h.r(&t("d1", "$alert/hot"), "too hot");
    h.drain();
    let before = h.c.device("d1").expect("d1").clone();
    let respaced = d.replacen('{', "{ ", 1);
    for retain in [Some(true), Some(false), None] {
        let r = |s: &str| s.as_bytes().to_vec();
        for (topic, payload) in [
            (t("d1", "$state"), r("ready")),
            (t("d1", "$description"), r(&d)),
            (t("d1", "$description"), r(&respaced)),
            (t("d1", "n/p0"), r("1")),
            (t("d1", "n/p0/$target"), r("2")),
            (t("d1", "$alert/hot"), r("too hot")),
            (t("d1", "$log/info"), r("hello")),
            (t("d1", "n/p1"), Vec::new()),
            (t("d1", "n/p1/$target"), Vec::new()),
            (t("d1", "$alert/cold"), Vec::new()),
        ] {
            h.send(&topic, &payload, retain);
            assert_eq!(h.all_events(), [], "{topic} {retain:?}");
            assert_eq!(h.cmds(), [], "{topic} {retain:?}");
        }
    }
    let after = h.c.device("d1").expect("d1");
    assert!(before.shares_values(after));
    // A second Down changes nothing either.
    h.down();
    h.drain();
    h.down();
    assert_eq!(h.all_events(), []);
    assert_eq!(h.cmds(), []);
}

#[test]
fn a_value_change_copies_only_its_own_entry() {
    let mut h = H::wildcard();
    h.add("d1", &ints(3));
    for (p, v) in [("p0", "1"), ("p1", "2"), ("p2", "3")] {
        h.r(&t("d1", &format!("n/{p}")), v);
    }
    h.r(&t("d1", "$alert/hot"), "too hot");
    h.drain();
    let before = h.c.device("d1").expect("d1").clone();
    h.live(&t("d1", "n/p1"), "9");
    let after = h.c.device("d1").expect("d1");
    assert!(!before.shares_values(after));
    let entry =
        |v: &DeviceView, p: &str| -> *const PropertyView { v.property("n", p).expect("declared") };
    assert_eq!(entry(&before, "p0"), entry(after, "p0"));
    assert_eq!(entry(&before, "p2"), entry(after, "p2"));
    assert_ne!(entry(&before, "p1"), entry(after, "p1"));
    assert!(before.alerts.ptr_eq(&after.alerts));
    assert_eq!(
        before.property("n", "p1").and_then(|p| p.value.clone()),
        Some(val("2", Ok(())))
    );
}

#[test]
fn a_child_a_second_parent_declares_is_not_resubscribed() {
    let mut h = H::connected(Config::tree("r").expect("valid"));
    let child = |children: &str| {
        desc(&format!(
            r#""root":"r","parent":"r","children":[{children}]"#
        ))
    };
    h.r(&t("r", "$description"), &desc(r#""children":["a","b"]"#));
    h.r(&t("r", "$state"), "ready");
    h.r(&t("a", "$state"), "ready");
    h.r(&t("a", "$description"), &child(r#""x""#));
    h.r(&t("x", "$state"), "ready");
    h.r(&t("x", "$description"), &desc(r#""root":"r","parent":"a""#));
    h.r(&t("b", "$state"), "ready");
    h.drain();
    h.r(&t("b", "$description"), &child(r#""x""#));
    let subs = subscribed(&h.cmds());
    assert!(!subs.iter().any(|f| f.starts_with(&t("x", ""))), "{subs:?}");
    // x stays while either parent declares it.
    h.live(&t("a", "$state"), "init");
    h.live(&t("a", "$description"), &child(""));
    h.live(&t("a", "$state"), "ready");
    assert!(h.c.device("x").is_some());
    assert!(unsubscribed(&h.cmds()).is_empty());
}

#[test]
fn a_replay_lost_with_the_connection_delays_no_later_resync() {
    for discards in [false, true] {
        for mut cfg in window_configs() {
            cfg.link_up_discards_queued = discards;
            let paced = cfg.pacing.is_some();
            let mut h = H::connected(cfg);
            held(&mut h, "r");
            // A re-sync whose replays the connection loses; paced, its
            // `$description` SUBSCRIBE follows the `$state` replay.
            h.down();
            h.up(false);
            if paced {
                h.r(&t("r", "$state"), "ready");
            }
            assert!(subscribed(&h.cmds()).contains(&t("r", "$description")));
            h.down();
            h.up(false);
            h.drain();
            replay_without_p1(&mut h, "r");
            if discards {
                assert_cleared_p1(&mut h, "r");
            } else {
                assert_eq!(cleared(&h.events()), 0, "the conservative default");
            }
        }
    }
}

#[test]
fn a_clean_link_up_drops_subscribes_still_queued_when_the_host_discards() {
    let mut cfg = Config::wildcard();
    cfg.link_up_discards_queued = true;
    let mut h = H::connected(cfg);
    h.add("d1", &ints(1));
    h.c.handle_dropped(h.now, &t("d1", "n/p0"));
    h.advance(0);
    // The heal's SUBSCRIBE is still queued when the link comes back clean.
    h.down();
    h.up(false);
    let cmds = h.cmds();
    assert_eq!(cmds.first(), Some(&sub(&["ebus/5/+/$state"])), "{cmds:?}");
}
