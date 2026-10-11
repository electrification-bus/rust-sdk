//! Device rules, as exact command lists with a virtual clock.

extern crate std;

use alloc::{string::ToString, vec, vec::Vec};
use std::string::String;

use ebus_mqtt::{Pair, Role};

use super::*;
use crate::controller::{Config, Controller};

const Q2: QoS = QoS::ExactlyOnce;

struct H {
    t: Tree,
    now: Instant,
    n: NodeIdx,
    p: SlotId,
    c: DevIdx,
    m: NodeIdx,
    s: SlotId,
}

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
                std::format!("{topic}~{text} q{}", *qos as u8)
            } else if payload.is_empty() {
                std::format!("{topic}=<clear>")
            } else if topic.ends_with("/$description") {
                std::format!("{topic}=desc")
            } else {
                std::format!("{topic}={text}")
            }
        }
        Command::Subscribe { filters } => {
            let f: Vec<&str> = filters.iter().map(|(f, _)| f.as_str()).collect();
            std::format!("SUB {}", f.join(","))
        }
        Command::Unsubscribe { filters } => std::format!("UNSUB {}", filters.join(",")),
        _ => String::from("?"),
    }
}

fn cfg() -> TreeConfig {
    TreeConfig::default()
}

impl H {
    /// Root `r` with node `n` and `p: f64`; child `c` with node `m` and a
    /// settable `s: bool`. Not connected.
    fn new(cfg: TreeConfig) -> Self {
        let mut t = Tree::new(cfg, "r", DeviceSpec::default()).expect("tree");
        let (n, p, c, m, s) = t
            .reconfigure(|r| {
                let n = r.add_node(r.root(), "n", NodeSpec::default())?;
                let p = r.add_typed::<f64>(n, "p", PropSpec::new())?;
                let c = r.add_child(r.root(), "c", DeviceSpec::default())?;
                let m = r.add_node(c, "m", NodeSpec::default())?;
                let s = r.add_typed::<bool>(m, "s", PropSpec::new().settable(true))?;
                Ok((n, p, c, m, s))
            })
            .expect("build");
        H {
            t,
            now: Instant(1_000),
            n,
            p,
            c,
            m,
            s,
        }
    }

    /// Values set, connected, first refresh drained.
    fn up_drained(cfg: TreeConfig) -> Self {
        let mut h = H::new(cfg);
        h.t.set_value(h.p, ValueRef::Float(1.5)).expect("p");
        h.t.set_value(h.s, ValueRef::Boolean(false)).expect("s");
        h.up();
        h.drain();
        h.events();
        h
    }

    fn up(&mut self) {
        self.t.handle_link(
            self.now,
            Link::Up {
                session_present: false,
            },
        );
    }

    fn down(&mut self) {
        self.t.handle_link(self.now, Link::Down);
    }

    fn drain(&mut self) -> Vec<String> {
        core::iter::from_fn(|| self.t.poll_command())
            .map(|c| show(&c))
            .collect()
    }

    fn take(&mut self, n: usize) -> Vec<String> {
        (0..n)
            .filter_map(|_| self.t.poll_command())
            .map(|c| show(&c))
            .collect()
    }

    fn events(&mut self) -> Vec<DeviceEvent> {
        core::iter::from_fn(|| self.t.poll_event()).collect()
    }

    fn send(&mut self, topic: &str, payload: &[u8], retain: Option<bool>) -> Handled {
        self.t.handle_message(
            self.now,
            Message {
                topic,
                payload,
                retain,
            },
        )
    }

    fn receipt(&mut self, r: Receipt) {
        self.t.handle_receipt(self.now, &r);
    }

    fn advance(&mut self, ms: u64) {
        let end = self.now.add_ms(ms);
        while let Some(t) = self.t.poll_timeout() {
            if t > end {
                break;
            }
            self.now = self.now.max(t);
            self.t.handle_timeout(self.now);
        }
        self.now = end;
    }
}

fn boot_sequence() -> Vec<&'static str> {
    vec![
        "SUB ebus/5/r/+/+/set,ebus/5/c/+/+/set",
        "ebus/5/r/$state=init",
        "ebus/5/r/$description=desc",
        "ebus/5/r/n/p=1.5",
        "ebus/5/c/$state=init",
        "ebus/5/c/$description=desc",
        "ebus/5/c/m/s=false",
        "ebus/5/c/$state=ready",
        "ebus/5/r/$state=ready",
    ]
}

fn reconnect_sequence() -> Vec<&'static str> {
    vec![
        "SUB ebus/5/r/+/+/set,ebus/5/c/+/+/set",
        "ebus/5/r/$description=desc",
        "ebus/5/r/n/p=1.5",
        "ebus/5/c/$description=desc",
        "ebus/5/c/m/s=false",
        "ebus/5/c/$state=ready",
        "ebus/5/r/$state=ready",
    ]
}

fn subscribed(filters: &[&str], granted: bool) -> Receipt {
    Receipt::Subscribed {
        filters: filters.iter().map(|f| f.to_string()).collect(),
        granted,
    }
}

fn acked(topic: &str) -> Receipt {
    Receipt::Acknowledged {
        topic: topic.to_string(),
        cleared: true,
    }
}

// ----- lifecycle -----

#[test]
fn nothing_before_the_first_link_up() {
    let mut h = H::new(cfg());
    assert_eq!(h.t.set_value(h.p, ValueRef::Float(1.5)), Ok(Change::Cached));
    assert!(h.drain().is_empty());
    assert!(h.events().is_empty());
    assert!(!h.t.is_connected());
}

#[test]
fn boot_publishes_the_tree_in_order() {
    let mut h = H::new(cfg());
    h.t.set_value(h.p, ValueRef::Float(1.5)).expect("p");
    h.t.set_value(h.s, ValueRef::Boolean(false)).expect("s");
    h.up();
    assert_eq!(h.drain(), boot_sequence());
    assert_eq!(
        h.events(),
        [DeviceEvent::Link(Link::Up {
            session_present: false
        })]
    );
    assert!(h.t.is_connected());
}

#[test]
fn description_matches_python_layout() {
    let h = H::new(cfg());
    let r = h.t.description(h.t.root()).expect("root");
    assert!(r.starts_with("{\"homie\": \"5.0\", \"version\": "), "{r}");
    assert!(r.contains("\"name\": \"r\", \"nodes\": {\"n\": {\"name\": \"n\", \"properties\": {\"p\": {\"name\": \"p\", \"datatype\": \"float\"}}}}, \"children\": [\"c\"], \"extensions\": []}"), "{r}");
    let c = h.t.description(h.c).expect("child");
    assert!(
        c.contains("\"children\": [], \"root\": \"r\", \"parent\": \"r\", \"extensions\": []"),
        "{c}"
    );
    assert!(
        c.contains("\"datatype\": \"boolean\", \"settable\": true"),
        "{c}"
    );
    let parsed = crate::description::parse(c).expect("parses");
    assert_eq!(i128::from(parsed.compute_version()), parsed.version);
}

#[test]
fn reconnect_without_a_description_change_sends_no_init() {
    let mut h = H::up_drained(cfg());
    h.down();
    assert_eq!(h.events(), [DeviceEvent::Link(Link::Down)]);
    h.down();
    assert!(h.events().is_empty(), "a second Down changes nothing");
    h.t.handle_link(
        h.now,
        Link::Up {
            session_present: true,
        },
    );
    assert_eq!(h.drain(), reconnect_sequence());
}

#[test]
fn reconnect_after_a_change_while_down_sends_init_for_that_device() {
    let mut h = H::up_drained(cfg());
    h.down();
    let m = h.m;
    h.t.reconfigure(|r| r.add_typed::<i64>(m, "q", PropSpec::new()))
        .expect("add");
    assert!(h.drain().is_empty(), "down: the model only");
    h.up();
    assert_eq!(
        h.drain(),
        [
            "SUB ebus/5/r/+/+/set,ebus/5/c/+/+/set",
            "ebus/5/r/$description=desc",
            "ebus/5/r/n/p=1.5",
            "ebus/5/c/$state=init",
            "ebus/5/c/$description=desc",
            "ebus/5/c/m/s=false",
            "ebus/5/c/$state=ready",
            "ebus/5/r/$state=ready",
        ]
    );
}

#[test]
fn down_mid_refresh_resets_the_memo_of_open_devices() {
    let mut h = H::new(cfg());
    h.t.set_value(h.p, ValueRef::Float(1.5)).expect("p");
    h.up();
    // Through the root's description, before any closing state.
    assert_eq!(h.take(3).len(), 3);
    h.down();
    h.up();
    let out = h.drain();
    assert_eq!(out[1], "ebus/5/r/$state=init", "the root is wrapped again");
    assert!(out.contains(&"ebus/5/c/$state=init".to_string()));
    // A completed refresh leaves no init for the next one.
    h.down();
    h.up();
    assert!(!h.drain().iter().any(|c| c.ends_with("=init")));
}

#[test]
fn down_mid_reconfiguration_resets_the_memo_and_keeps_tombstones() {
    let mut h = H::up_drained(cfg());
    let p = h.p;
    h.t.reconfigure(|r| r.remove_property(p)).expect("remove");
    assert_eq!(
        h.take(2),
        ["ebus/5/r/$state=init", "ebus/5/r/$description=desc"]
    );
    h.down();
    assert_eq!(h.t.tombstones().collect::<Vec<_>>(), ["ebus/5/r/n/p"]);
    h.up();
    assert_eq!(
        h.drain(),
        [
            "SUB ebus/5/r/+/+/set,ebus/5/c/+/+/set",
            "ebus/5/r/n/p=<clear>",
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/c/$description=desc",
            "ebus/5/c/m/s=false",
            "ebus/5/c/$state=ready",
            "ebus/5/r/$state=ready",
        ]
    );
}

#[test]
fn up_while_up_runs_a_new_refresh() {
    let mut h = H::up_drained(cfg());
    h.t.set_value(h.p, ValueRef::Float(2.0)).expect("p");
    h.up();
    let out = h.drain();
    assert_eq!(
        out.iter().filter(|c| c.starts_with("ebus/5/r/n/p")).count(),
        1
    );
    assert!(out.contains(&"ebus/5/r/n/p=2.0".to_string()));
}

#[test]
fn implicit_link_up_on_a_message_while_down() {
    let mut h = H::up_drained(cfg());
    h.down();
    h.events();
    assert_eq!(
        h.send("ebus/5/c/m/s/set", b"true", Some(false)),
        Handled::Consumed
    );
    let ev = h.events();
    assert_eq!(
        ev[0],
        DeviceEvent::Link(Link::Up {
            session_present: false
        })
    );
    assert!(matches!(ev[1], DeviceEvent::Set { .. }));
    assert_eq!(h.drain(), reconnect_sequence());
}

#[test]
fn last_will_is_the_roots_lost() {
    let h = H::new(cfg());
    assert_eq!(
        h.t.last_will(),
        LastWill {
            topic: "ebus/5/r/$state".to_string(),
            payload: b"lost".to_vec(),
            qos: Q2,
            retain: true,
        }
    );
}

// ----- the unchanged-value gate -----

#[test]
fn equal_values_publish_nothing() {
    let mut h = H::up_drained(cfg());
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Float(1.5)),
        Ok(Change::Unchanged)
    );
    assert_eq!(
        h.t.set_value(h.s, ValueRef::Boolean(false)),
        Ok(Change::Unchanged)
    );
    assert!(h.drain().is_empty());
    assert!(h.events().is_empty());
    assert_eq!(h.t.set_value(h.p, ValueRef::Float(2.5)), Ok(Change::Queued));
    assert_eq!(h.drain(), ["ebus/5/r/n/p=2.5"]);
}

#[test]
fn an_equal_value_is_not_encoded() {
    let mut h = H::up_drained(cfg());
    // The scratch buffer now holds the last encoding.
    h.t.set_value(h.s, ValueRef::Boolean(true)).expect("s");
    assert_eq!(h.t.scratch, "true");
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Float(1.5)),
        Ok(Change::Unchanged)
    );
    assert_eq!(h.t.scratch, "true", "1.5 was encoded");
    // A value compared by its bytes is still encoded.
    let mut h = H::new(cfg());
    let q =
        h.t.reconfigure(|r| r.add_typed::<String>(h.n, "q", PropSpec::new()))
            .expect("q");
    h.t.set_value(q, ValueRef::String("x")).expect("q");
    h.t.set_value(h.p, ValueRef::Float(1.5)).expect("p");
    assert_eq!(
        h.t.set_value(q, ValueRef::String("x")),
        Ok(Change::Unchanged)
    );
    assert_eq!(h.t.scratch, "x");
}

#[test]
fn negative_zero_differs_from_zero() {
    let mut h = H::up_drained(cfg());
    h.t.set_value(h.p, ValueRef::Float(0.0)).expect("0");
    assert_eq!(h.drain(), ["ebus/5/r/n/p=0.0"]);
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Float(-0.0)),
        Ok(Change::Queued)
    );
    assert_eq!(h.drain(), ["ebus/5/r/n/p=-0.0"]);
}

#[test]
fn f32_and_f64_with_the_same_payload_publish_once() {
    let mut h = H::up_drained(cfg());
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Float32(21.4)),
        Ok(Change::Queued)
    );
    assert_eq!(h.drain(), ["ebus/5/r/n/p=21.4"]);
    // f32 21.4 widened is 21.399999618530273, but the f64 21.4 prints the
    // same payload, so nothing goes out.
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Float(21.4)),
        Ok(Change::Unchanged)
    );
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Float(f64::from(21.4f32))),
        Ok(Change::Queued)
    );
    assert_eq!(h.drain(), ["ebus/5/r/n/p=21.399999618530273"]);
}

#[test]
fn a_b_a_before_poll_publishes_nothing() {
    let mut h = H::up_drained(cfg());
    assert_eq!(h.t.set_value(h.p, ValueRef::Float(2.0)), Ok(Change::Queued));
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Float(1.5)),
        Ok(Change::Unchanged)
    );
    assert!(h.drain().is_empty());
    // Polled in between, A goes out again.
    h.t.set_value(h.p, ValueRef::Float(2.0)).expect("b");
    assert_eq!(h.drain(), ["ebus/5/r/n/p=2.0"]);
    h.t.set_value(h.p, ValueRef::Float(1.5)).expect("a");
    assert_eq!(h.drain(), ["ebus/5/r/n/p=1.5"]);
}

#[test]
fn enum_compares_by_index() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Mode {
        Off,
        On,
    }
    impl HomieEnum for Mode {
        const VARIANTS: &'static [&'static str] = &["off", "on"];
        fn index(self) -> u16 {
            self as u16
        }
        fn from_index(i: u16) -> Option<Self> {
            [Mode::Off, Mode::On].get(usize::from(i)).copied()
        }
    }
    let mut h = H::up_drained(cfg());
    let n = h.n;
    let e =
        h.t.reconfigure(|r| r.add_typed::<Mode>(n, "mode", PropSpec::new()))
            .expect("enum");
    h.drain();
    assert!(
        h.t.description(h.t.root())
            .expect("d")
            .contains("\"format\": \"off,on\"")
    );
    assert_eq!(h.t.set_value(e, Mode::On.as_value()), Ok(Change::Queued));
    assert_eq!(h.t.set_value(e, ValueRef::Enum(1)), Ok(Change::Unchanged));
    assert_eq!(h.drain(), ["ebus/5/r/n/mode=on"]);
    assert_eq!(h.t.set_value(e, ValueRef::Enum(1)), Ok(Change::Unchanged));
    assert_eq!(
        h.t.set_value(e, ValueRef::Enum(2)),
        Err(ValueError::NotInFormat)
    );
    assert_eq!(
        Mode::from_set(&SetValue::Text("off".to_string())),
        Some(Mode::Off)
    );
}

#[test]
fn enum_value_is_compared_by_payload_after_its_format_changes() {
    let mut h = H::up_drained(cfg());
    let n = h.n;
    let e =
        h.t.reconfigure(|r| {
            r.add_property(
                n,
                "mode",
                Datatype::Enum,
                None,
                PropSpec::new().format("a,b"),
            )
        })
        .expect("enum");
    h.t.set_value(e, ValueRef::Enum(1)).expect("b");
    h.drain();
    h.t.reconfigure(|r| r.set_format(e, Some("b,c")))
        .expect("format");
    h.drain();
    assert_eq!(h.t.set_value(e, ValueRef::Enum(1)), Ok(Change::Queued));
    assert_eq!(h.drain(), ["ebus/5/r/n/mode=c"]);
    // The same name at a new index publishes nothing.
    h.t.reconfigure(|r| r.set_format(e, Some("c,x")))
        .expect("format");
    h.drain();
    assert_eq!(h.t.set_value(e, ValueRef::Enum(0)), Ok(Change::Unchanged));
    assert!(h.drain().is_empty());
}

#[test]
fn values_changed_mid_refresh_go_out_once_at_their_latest() {
    let mut h = H::new(cfg());
    h.t.set_value(h.p, ValueRef::Float(1.5)).expect("p");
    h.t.set_value(h.s, ValueRef::Boolean(false)).expect("s");
    h.up();
    // Credit for three commands: through the root's value.
    assert_eq!(
        h.take(4).last().map(String::as_str),
        Some("ebus/5/r/n/p=1.5")
    );
    assert_eq!(h.t.set_value(h.p, ValueRef::Float(3.0)), Ok(Change::Queued));
    assert_eq!(
        h.t.set_value(h.s, ValueRef::Boolean(true)),
        Ok(Change::Queued)
    );
    let rest = h.drain();
    assert_eq!(
        rest,
        [
            "ebus/5/c/$state=init",
            "ebus/5/c/$description=desc",
            "ebus/5/c/m/s=true",
            "ebus/5/c/$state=ready",
            "ebus/5/r/$state=ready",
            "ebus/5/r/n/p=3.0",
        ]
    );
}

#[test]
fn values_are_validated() {
    let mut h = H::up_drained(cfg());
    let n = h.n;
    let (b, i, u) =
        h.t.reconfigure(|r| {
            Ok((
                r.add_typed::<f64>(n, "b", PropSpec::new().format("0:0.1"))?,
                r.add_typed::<i64>(n, "i", PropSpec::new().format("-5:5"))?,
                r.add_typed::<u8>(n, "u", PropSpec::new())?,
            ))
        })
        .expect("add");
    h.drain();
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Float(f64::NAN)),
        Err(ValueError::NotFinite)
    );
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Integer(1)),
        Err(ValueError::WrongDatatype)
    );
    // The f64 nearest 0.1 is above 0.1 exactly.
    assert_eq!(
        h.t.set_value(b, ValueRef::Float(0.1)),
        Err(ValueError::OutOfRange)
    );
    assert_eq!(
        h.t.set_value(b, ValueRef::Float(0.09999999999999999)),
        Ok(Change::Queued)
    );
    assert_eq!(
        h.t.set_value(i, ValueRef::Integer(6)),
        Err(ValueError::OutOfRange)
    );
    assert_eq!(h.t.set_value(i, ValueRef::Integer(-5)), Ok(Change::Queued));
    assert!(
        h.t.description(h.t.root())
            .expect("d")
            .contains("\"format\": \"0:255\"")
    );
    assert_eq!(
        h.t.set_value(u, ValueRef::Integer(256)),
        Err(ValueError::OutOfRange)
    );
    assert_eq!(h.t.set_value(u, 255u8.as_value()), Ok(Change::Queued));
    assert_eq!(h.t.clear_value(h.p).map(|_| ()), Ok(()));
    assert_eq!(
        h.t.emit(h.p, ValueRef::Float(1.0)),
        Err(ValueError::NotEvent)
    );
}

#[test]
fn other_datatypes_encode_and_gate_by_payload() {
    let mut h = H::up_drained(cfg());
    let n = h.n;
    let ids =
        h.t.reconfigure(|r| {
            Ok([
                r.add_typed::<String>(n, "t", PropSpec::new())?,
                r.add_typed::<Color>(n, "col", PropSpec::new().format("rgb"))?,
                r.add_typed::<Datetime>(n, "dt", PropSpec::new())?,
                r.add_typed::<core::time::Duration>(n, "du", PropSpec::new())?,
                r.add_typed::<serde_json::Value>(n, "j", PropSpec::new())?,
            ])
        })
        .expect("add");
    h.drain();
    let json = serde_json::json!({"a": [1, 2.5]});
    let dur = core::time::Duration::from_millis(61_250);
    let values = [
        ValueRef::String(""),
        ValueRef::Color("rgb,1,2,3"),
        ValueRef::Datetime("2026-10-10T12:00:00Z"),
        dur.as_value(),
        ValueRef::Json(&json),
    ];
    for (id, v) in ids.iter().zip(values) {
        assert_eq!(h.t.set_value(*id, v), Ok(Change::Queued));
        assert_eq!(h.t.set_value(*id, v), Ok(Change::Unchanged));
    }
    assert_eq!(
        h.drain(),
        [
            "ebus/5/r/n/t=\0",
            "ebus/5/r/n/col=rgb,1,2,3",
            "ebus/5/r/n/dt=2026-10-10T12:00:00Z",
            "ebus/5/r/n/du=PT1M1.25S",
            "ebus/5/r/n/j={\"a\": [1, 2.5]}",
        ]
    );
    assert_eq!(
        h.t.set_value(ids[1], ValueRef::Color("hsv,1,2,3")),
        Err(ValueError::NotInFormat)
    );
    assert_eq!(
        h.t.set_value(ids[1], ValueRef::Color("rgb,1,2,300")),
        Err(ValueError::OutOfRange)
    );
    assert_eq!(
        h.t.set_value(ids[2], ValueRef::Datetime("nope")),
        Err(ValueError::WrongDatatype)
    );
    let scalar = serde_json::json!(1);
    assert_eq!(
        h.t.set_value(ids[4], ValueRef::Json(&scalar)),
        Err(ValueError::WrongDatatype)
    );
}

#[test]
fn clear_publishes_once_and_only_a_held_value() {
    let mut h = H::up_drained(cfg());
    let n = h.n;
    let fresh =
        h.t.reconfigure(|r| r.add_typed::<f64>(n, "x", PropSpec::new()))
            .expect("add");
    h.drain();
    assert_eq!(h.t.clear_value(fresh), Ok(Change::Unchanged));
    assert_eq!(h.t.clear_value(h.p), Ok(Change::Queued));
    assert_eq!(h.t.clear_value(h.p), Ok(Change::Unchanged));
    assert_eq!(h.drain(), ["ebus/5/r/n/p=<clear>"]);
    assert_eq!(h.t.current(h.p), None);
}

#[test]
fn set_then_clear_before_poll_ends_cleared() {
    let mut h = H::up_drained(cfg());
    h.t.set_value(h.p, ValueRef::Float(9.0)).expect("set");
    h.t.clear_value(h.p).expect("clear");
    h.t.set_value(h.p, ValueRef::Float(7.0)).expect("set again");
    assert_eq!(h.drain(), ["ebus/5/r/n/p=<clear>", "ebus/5/r/n/p=7.0"]);
}

// ----- events -----

#[test]
fn events_publish_every_call_and_drop_while_down() {
    let mut h = H::up_drained(cfg());
    let n = h.n;
    let e =
        h.t.reconfigure(|r| r.add_typed::<i64>(n, "ev", PropSpec::new().retained(false)))
            .expect("add");
    h.drain();
    assert_eq!(h.t.emit(e, ValueRef::Integer(1)), Ok(Change::Queued));
    assert_eq!(h.t.emit(e, ValueRef::Integer(1)), Ok(Change::Queued));
    assert_eq!(h.drain(), ["ebus/5/r/n/ev~1 q0", "ebus/5/r/n/ev~1 q0"]);
    assert_eq!(
        h.t.set_value(e, ValueRef::Integer(1)),
        Err(ValueError::IsEvent)
    );
    h.down();
    assert_eq!(h.t.emit(e, ValueRef::Integer(2)), Ok(Change::Dropped));
    h.up();
    assert!(!h.drain().iter().any(|c| c.contains("/ev")));
}

// ----- /set -----

fn set_events(h: &mut H) -> Vec<(SlotId, SetValue, Vec<u8>)> {
    h.events()
        .into_iter()
        .filter_map(|e| match e {
            DeviceEvent::Set { slot, value, raw } => Some((slot, value, raw.to_vec())),
            _ => None,
        })
        .collect()
}

#[test]
fn set_verdicts() {
    let mut h = H::up_drained(cfg());
    let t = "ebus/5/c/m/s/set";
    // Retained: ignored, not counted.
    assert_eq!(h.send(t, b"true", Some(true)), Handled::Consumed);
    assert!(set_events(&mut h).is_empty());
    assert_eq!(h.t.rejected_sets(), 0);
    // retain None is processed.
    h.send(t, b"true", None);
    assert_eq!(
        set_events(&mut h),
        [(h.s, SetValue::Text("true".to_string()), b"true".to_vec())]
    );
    // Equal to the current value: still delivered.
    h.send(t, b"false", Some(false));
    assert_eq!(set_events(&mut h).len(), 1);
    // Unknown property: ignored at debug, not counted.
    h.send("ebus/5/c/m/zz/set", b"1", Some(false));
    // Unknown device: not wanted.
    assert!(!h.t.wants("ebus/5/zz/m/s/set"));
    assert_eq!(
        h.send("ebus/5/zz/m/s/set", b"1", Some(false)),
        Handled::Ignored
    );
    assert!(set_events(&mut h).is_empty());
    assert_eq!(h.t.rejected_sets(), 0);
    // Not settable, zero-length, not UTF-8, grammar: counted, nothing else.
    h.send("ebus/5/r/n/p/set", b"1.0", Some(false));
    h.send(t, b"", Some(false));
    h.send(t, b"\xff", Some(false));
    h.send(t, b"yes", Some(false));
    assert!(set_events(&mut h).is_empty());
    assert!(h.drain().is_empty(), "a refused /set publishes nothing");
    assert_eq!(h.t.rejected_sets(), 4);
    // Not a /set topic for the device: not wanted.
    assert!(!h.t.wants("ebus/5/c/m/s"));
    assert!(!h.t.wants("homie/5/c/m/s/set"));
}

#[test]
fn set_is_step_rounded_from_the_current_value() {
    let mut h = H::up_drained(cfg());
    let m = h.m;
    let (i, f) =
        h.t.reconfigure(|r| {
            Ok((
                r.add_typed::<i64>(m, "i", PropSpec::new().settable(true).format("0:10:2"))?,
                r.add_typed::<f64>(m, "f", PropSpec::new().settable(true).format("::0.5"))?,
            ))
        })
        .expect("add");
    h.drain();
    h.send("ebus/5/c/m/i/set", b"5", Some(false));
    h.send("ebus/5/c/m/i/set", b"11", Some(false));
    h.t.set_value(f, ValueRef::Float(0.25)).expect("current");
    h.send("ebus/5/c/m/f/set", b"2.26", Some(false));
    assert_eq!(
        set_events(&mut h),
        [
            (i, SetValue::Text("6".to_string()), b"5".to_vec()),
            (f, SetValue::Text("2.25".to_string()), b"2.26".to_vec()),
        ]
    );
    assert_eq!(h.t.rejected_sets(), 1, "11 is above max");
    assert_eq!(h.t.current(f), Some(Current::Float(0.25)));
}

#[test]
fn reflection_equal_to_the_current_value_publishes_nothing() {
    let mut h = H::up_drained(cfg());
    h.send("ebus/5/c/m/s/set", b"false", Some(false));
    let (slot, value, _) = set_events(&mut h).pop().expect("set");
    let v = bool::from_set(&value).expect("bool");
    assert_eq!(h.t.set_value(slot, v.as_value()), Ok(Change::Unchanged));
    assert!(h.drain().is_empty());
}

// ----- $target -----

fn with_target(settable: bool) -> (H, SlotId) {
    let mut h = H::up_drained(cfg());
    let m = h.m;
    let t =
        h.t.reconfigure(|r| {
            r.add_typed::<f64>(m, "t", PropSpec::new().settable(settable).target(true))
        })
        .expect("add");
    h.drain();
    (h, t)
}

#[test]
fn accepted_set_echoes_raw_bytes_and_set_closes_without_republishing_target() {
    let (mut h, t) = with_target(true);
    assert_eq!(h.t.accept_set(t, b"21.50"), Ok(Change::Queued));
    assert_eq!(h.drain(), ["ebus/5/c/m/t/$target=21.50"]);
    // The device reaches the target: value only.
    assert_eq!(h.t.set_value(t, ValueRef::Float(21.5)), Ok(Change::Queued));
    assert_eq!(h.drain(), ["ebus/5/c/m/t=21.5"]);
    // No transition open: $target, then the value.
    h.t.set_value(t, ValueRef::Float(22.0)).expect("set");
    assert_eq!(
        h.drain(),
        ["ebus/5/c/m/t/$target=22.0", "ebus/5/c/m/t=22.0"]
    );
}

#[test]
fn set_that_differs_from_the_open_target_republishes_it() {
    let (mut h, t) = with_target(true);
    h.t.accept_set(t, b"30").expect("accept");
    h.t.set_progress(t, ValueRef::Float(25.0))
        .expect("progress");
    assert_eq!(h.drain(), ["ebus/5/c/m/t/$target=30", "ebus/5/c/m/t=25.0"]);
    h.t.set_value(t, ValueRef::Float(28.0))
        .expect("settle elsewhere");
    assert_eq!(
        h.drain(),
        ["ebus/5/c/m/t/$target=28.0", "ebus/5/c/m/t=28.0"]
    );
}

#[test]
fn target_goes_before_a_value_already_queued() {
    let (mut h, t) = with_target(true);
    h.t.set_target(t, ValueRef::Float(1.0)).expect("target");
    h.drain();
    h.t.set_progress(t, ValueRef::Float(0.5)).expect("progress");
    h.t.set_value(t, ValueRef::Float(2.0)).expect("close");
    assert_eq!(h.drain(), ["ebus/5/c/m/t/$target=2.0", "ebus/5/c/m/t=2.0"]);
}

#[test]
fn local_target_and_clear_on_a_target_property() {
    let (mut h, t) = with_target(false);
    assert_eq!(h.t.set_target(t, ValueRef::Float(5.0)), Ok(Change::Queued));
    assert_eq!(
        h.t.set_target(t, ValueRef::Float(5.0)),
        Ok(Change::Unchanged)
    );
    assert_eq!(h.drain(), ["ebus/5/c/m/t/$target=5.0"]);
    h.t.set_value(t, ValueRef::Float(5.0)).expect("reach");
    assert_eq!(h.drain(), ["ebus/5/c/m/t=5.0"]);
    // Refresh: $target before its value.
    h.down();
    h.up();
    let out = h.drain();
    let ti = out.iter().position(|c| c == "ebus/5/c/m/t/$target=5.0");
    let vi = out.iter().position(|c| c == "ebus/5/c/m/t=5.0");
    assert!(ti.is_some() && ti < vi, "{out:?}");
    assert_eq!(h.t.clear_value(t), Ok(Change::Queued));
    assert_eq!(
        h.drain(),
        ["ebus/5/c/m/t=<clear>", "ebus/5/c/m/t/$target=<clear>"]
    );
    assert_eq!(
        h.t.set_progress(h.p, ValueRef::Float(1.0)),
        Err(ValueError::NotTarget)
    );
    assert_eq!(
        h.t.set_target(h.p, ValueRef::Float(1.0)),
        Err(ValueError::NotTarget)
    );
    assert_eq!(h.t.accept_set(h.p, b"1"), Err(ValueError::NotTarget));
}

#[test]
fn progress_with_no_target_held_publishes_target_first() {
    let (mut h, t) = with_target(false);
    assert_eq!(
        h.t.set_progress(t, ValueRef::Float(5.0)),
        Ok(Change::Queued)
    );
    assert_eq!(h.drain(), ["ebus/5/c/m/t/$target=5.0", "ebus/5/c/m/t=5.0"]);
    h.down();
    h.up();
    let out = h.drain();
    let ti = out.iter().position(|c| c == "ebus/5/c/m/t/$target=5.0");
    let vi = out.iter().position(|c| c == "ebus/5/c/m/t=5.0");
    assert!(ti.is_some() && ti < vi, "{out:?}");
    // A held $target leaves progress value-only; a cleared one does not.
    h.t.set_progress(t, ValueRef::Float(6.0)).expect("progress");
    assert_eq!(h.drain(), ["ebus/5/c/m/t=6.0"]);
    h.t.clear_value(t).expect("clear");
    h.drain();
    h.t.set_progress(t, ValueRef::Float(7.0)).expect("progress");
    assert_eq!(h.drain(), ["ebus/5/c/m/t/$target=7.0", "ebus/5/c/m/t=7.0"]);
}

// ----- alerts and presence -----

#[test]
fn alerts() {
    let mut h = H::up_drained(cfg());
    let c = h.c;
    assert_eq!(h.t.raise_alert(c, "Bad", "x"), Err(AlertError::InvalidId));
    assert_eq!(h.t.raise_alert(c, "hot", ""), Err(AlertError::EmptyMessage));
    assert!(h.drain().is_empty());
    assert_eq!(h.t.raise_alert(c, "hot", "too hot"), Ok(Change::Queued));
    assert_eq!(h.t.raise_alert(c, "hot", "too hot"), Ok(Change::Unchanged));
    assert_eq!(h.drain(), ["ebus/5/c/$alert/hot=too hot"]);
    h.down();
    h.up();
    let out = h.drain();
    let ai = out.iter().position(|x| x == "ebus/5/c/$alert/hot=too hot");
    let si = out.iter().position(|x| x == "ebus/5/c/$state=ready");
    assert!(ai.is_some() && ai < si, "{out:?}");
    assert_eq!(h.t.clear_alert(c, "cold"), Ok(Change::Unchanged));
    assert_eq!(h.t.clear_alert(c, "hot"), Ok(Change::Queued));
    assert_eq!(h.drain(), ["ebus/5/c/$alert/hot=<clear>"]);
    // Raise, clear, raise before polling ends raised.
    h.t.raise_alert(c, "x", "1").expect("raise");
    h.t.clear_alert(c, "x").expect("clear");
    h.t.raise_alert(c, "x", "2").expect("raise");
    assert_eq!(
        h.drain(),
        ["ebus/5/c/$alert/x=<clear>", "ebus/5/c/$alert/x=2"]
    );
}

#[test]
fn presence_changes() {
    let mut h = H::up_drained(cfg());
    let (r, c) = (h.t.root(), h.c);
    h.t.set_presence(c, Presence::Ready).expect("same");
    assert!(h.drain().is_empty());
    h.t.set_presence(c, Presence::Sleeping).expect("sleep");
    assert_eq!(h.drain(), ["ebus/5/c/$state=sleeping"]);
    // A-B-A before polling: nothing.
    h.t.set_presence(c, Presence::Lost).expect("lost");
    h.t.set_presence(c, Presence::Sleeping).expect("back");
    assert!(h.drain().is_empty());
    assert_eq!(
        h.t.set_presence(r, Presence::Disconnected),
        Err(ValueError::NotForRoot)
    );
    assert_eq!(
        h.t.set_presence(r, Presence::Lost),
        Err(ValueError::NotForRoot)
    );
}

#[test]
fn declare_lost_while_down_then_up() {
    let mut h = H::up_drained(cfg());
    h.down();
    h.t.declare_lost();
    h.up();
    let out = h.drain();
    assert_eq!(out.last().map(String::as_str), Some("ebus/5/r/$state=lost"));
    let r = h.t.root();
    h.t.set_presence(r, Presence::Ready).expect("ready");
    assert_eq!(h.drain(), ["ebus/5/r/$state=ready"]);
    h.t.declare_lost();
    assert_eq!(h.drain(), ["ebus/5/r/$state=lost"]);
}

// ----- reconfiguration -----

#[test]
fn property_added_while_ready() {
    let mut h = H::up_drained(cfg());
    let n = h.n;
    h.t.reconfigure(|r| r.add_typed::<i64>(n, "q", PropSpec::new()))
        .expect("add");
    assert_eq!(
        h.drain(),
        [
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/r/$state=ready"
        ]
    );
}

#[test]
fn property_removed_while_sleeping_clears_it() {
    let mut h = H::up_drained(cfg());
    let (r, p) = (h.t.root(), h.p);
    h.t.set_presence(r, Presence::Sleeping).expect("sleep");
    h.drain();
    h.t.reconfigure(|x| x.remove_property(p)).expect("remove");
    assert_eq!(
        h.drain(),
        [
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/r/n/p=<clear>",
            "ebus/5/r/$state=sleeping"
        ]
    );
    assert_eq!(
        h.t.set_value(p, ValueRef::Float(1.0)),
        Err(ValueError::Gone)
    );
}

#[test]
fn change_on_a_lost_child_sends_no_init() {
    let mut h = H::up_drained(cfg());
    let (c, m) = (h.c, h.m);
    h.t.set_presence(c, Presence::Lost).expect("lost");
    h.drain();
    h.t.reconfigure(|r| r.add_typed::<i64>(m, "q", PropSpec::new()))
        .expect("add");
    assert_eq!(h.drain(), ["ebus/5/c/$description=desc"]);
}

#[test]
fn unchanged_description_publishes_nothing() {
    let mut h = H::up_drained(cfg());
    let p = h.p;
    h.t.reconfigure(|r| r.set_unit(p, None)).expect("same");
    h.t.reconfigure(|_| Ok(())).expect("empty");
    assert!(h.drain().is_empty());
    h.t.reconfigure(|r| r.set_unit(p, Some("W"))).expect("unit");
    assert_eq!(h.drain().len(), 3);
}

#[test]
fn child_added_while_up() {
    let mut h = H::up_drained(cfg());
    h.t.reconfigure(|r| {
        let d = r.add_child(r.root(), "d", DeviceSpec::default())?;
        let k = r.add_node(d, "k", NodeSpec::default())?;
        r.add_typed::<bool>(k, "on", PropSpec::new().settable(true))
    })
    .expect("add");
    assert_eq!(
        h.drain(),
        [
            "SUB ebus/5/d/+/+/set",
            "ebus/5/d/$state=init",
            "ebus/5/d/$description=desc",
            "ebus/5/d/$state=ready",
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/r/$state=ready",
        ]
    );
    assert!(h.t.wants("ebus/5/d/k/on/set"));
}

#[test]
fn child_added_with_receipts_waits_for_its_suback() {
    let mut c = cfg();
    c.receipts = true;
    let mut h = H::new(c);
    h.up();
    h.drain();
    h.receipt(subscribed(&["ebus/5/r/+/+/set", "ebus/5/c/+/+/set"], true));
    h.drain();
    h.t.reconfigure(|r| r.add_child(r.root(), "d", DeviceSpec::default()))
        .expect("add");
    assert_eq!(
        h.drain(),
        [
            "SUB ebus/5/d/+/+/set",
            "ebus/5/d/$state=init",
            "ebus/5/d/$description=desc",
        ]
    );
    h.receipt(subscribed(&["ebus/5/d/+/+/set"], true));
    assert_eq!(
        h.drain(),
        [
            "ebus/5/d/$state=ready",
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/r/$state=ready",
        ]
    );
}

#[test]
fn child_removed_while_up() {
    let mut h = H::up_drained(cfg());
    let c = h.c;
    h.t.raise_alert(c, "a", "m").expect("alert");
    h.drain();
    h.t.reconfigure(|r| r.remove_child(c)).expect("remove");
    assert_eq!(
        h.drain(),
        [
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/r/$state=ready",
            "ebus/5/c/$state=<clear>",
            "ebus/5/c/m/s=<clear>",
            "ebus/5/c/$alert/a=<clear>",
            "ebus/5/c/$description=<clear>",
            "UNSUB ebus/5/c/+/+/set",
        ]
    );
    assert!(!h.t.wants("ebus/5/c/m/s/set"));
    assert_eq!(
        h.t.set_value(h.s, ValueRef::Boolean(true)),
        Err(ValueError::Gone)
    );
    assert_eq!(h.t.set_presence(c, Presence::Ready), Err(ValueError::Gone));
    assert!(!h.t.description(h.t.root()).expect("d").contains("\"c\""));
}

#[test]
fn subtree_removed_leaves_first() {
    let mut h = H::up_drained(cfg());
    let c = h.c;
    h.t.reconfigure(|r| r.add_child(c, "g", DeviceSpec::default()))
        .expect("grandchild");
    h.drain();
    h.t.reconfigure(|r| r.remove_child(c)).expect("remove");
    let out = h.drain();
    let g = out.iter().position(|x| x == "ebus/5/g/$state=<clear>");
    let cc = out.iter().position(|x| x == "ebus/5/c/$state=<clear>");
    assert!(g.is_some() && g < cc, "{out:?}");
    assert_eq!(
        out.last().map(String::as_str),
        Some("UNSUB ebus/5/g/+/+/set,ebus/5/c/+/+/set")
    );
}

#[test]
fn first_settable_added_needs_no_subscribe() {
    let mut h = H::up_drained(cfg());
    let n = h.n;
    let x =
        h.t.reconfigure(|r| r.add_typed::<bool>(n, "x", PropSpec::new().settable(true)))
            .expect("add");
    assert!(!h.drain().iter().any(|c| c.starts_with("SUB")));
    h.send("ebus/5/r/n/x/set", b"true", Some(false));
    assert_eq!(set_events(&mut h).len(), 1);
    let _ = x;
}

#[test]
fn reconfiguration_while_down_publishes_at_the_next_refresh() {
    let mut h = H::up_drained(cfg());
    let c = h.c;
    h.down();
    h.t.reconfigure(|r| {
        r.remove_child(c)?;
        r.add_child(r.root(), "d", DeviceSpec::default())
    })
    .expect("swap");
    assert!(h.drain().is_empty());
    h.up();
    assert_eq!(
        h.drain(),
        [
            "SUB ebus/5/r/+/+/set,ebus/5/d/+/+/set",
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/r/n/p=1.5",
            "ebus/5/d/$state=init",
            "ebus/5/d/$description=desc",
            "ebus/5/d/$state=ready",
            "ebus/5/r/$state=ready",
            // The removed child after its parent (convention.md:633-641).
            "ebus/5/c/$state=<clear>",
            "ebus/5/c/m/s=<clear>",
            "ebus/5/c/$description=<clear>",
        ]
    );
}

#[test]
fn build_errors_roll_back() {
    let mut h = H::up_drained(cfg());
    let (n, c) = (h.n, h.c);
    let before = h.t.description(h.t.root()).expect("d").to_string();
    let r = h.t.reconfigure(|r| {
        r.add_typed::<i64>(n, "new", PropSpec::new())?;
        r.add_node(c, "a.b", NodeSpec::default())
    });
    assert_eq!(r, Err(BuildError::NodeIdHasDot));
    assert_eq!(h.t.description(h.t.root()), Some(before.as_str()));
    assert!(h.drain().is_empty());
    let cases: Vec<Result<(), BuildError>> = vec![
        h.t.reconfigure(|r| r.add_node(c, "Bad", NodeSpec::default()).map(|_| ())),
        h.t.reconfigure(|r| r.add_node(c, "m", NodeSpec::default()).map(|_| ())),
        h.t.reconfigure(|r| r.add_typed::<f64>(n, "p", PropSpec::new()).map(|_| ())),
        h.t.reconfigure(|r| {
            r.add_child(r.root(), "c", DeviceSpec::default())
                .map(|_| ())
        }),
        h.t.reconfigure(|r| {
            r.add_property(n, "e", Datatype::Enum, None, PropSpec::new())
                .map(|_| ())
        }),
        h.t.reconfigure(|r| {
            r.add_typed::<u8>(n, "u", PropSpec::new().format("300:"))
                .map(|_| ())
        }),
        h.t.reconfigure(|r| {
            let mut s = DeviceSpec::default();
            s.extras.push(("nodes".to_string(), serde_json::json!(1)));
            r.add_child(r.root(), "x", s).map(|_| ())
        }),
        h.t.reconfigure(|r| {
            let mut s = DeviceSpec::default();
            s.extensions.push("plain".to_string());
            r.add_child(r.root(), "x", s).map(|_| ())
        }),
        h.t.reconfigure(|r| {
            let root = r.root();
            r.remove_child(root)
        }),
    ];
    assert_eq!(
        cases,
        [
            Err(BuildError::InvalidId),
            Err(BuildError::DuplicateId),
            Err(BuildError::DuplicateId),
            Err(BuildError::DuplicateId),
            Err(BuildError::FormatRequired),
            Err(BuildError::InvalidFormat(Problem::AboveMax)),
            Err(BuildError::CoreKeyInExtras),
            Err(BuildError::InvalidExtension),
            Err(BuildError::InvalidId),
        ]
    );
    let ignored = h.t.reconfigure(|r| {
        r.add_property(
            n,
            "bad",
            Datatype::Integer,
            None,
            PropSpec::new().format("a:b"),
        )
    });
    assert!(
        matches!(&ignored, Err(BuildError::Ignored(i)) if i.property.as_deref() == Some("bad")),
        "{ignored:?}"
    );
    assert_eq!(h.t.description(h.t.root()), Some(before.as_str()));
    assert!(h.drain().is_empty());
}

#[test]
fn oversized_description_is_refused() {
    let mut c = cfg();
    c.max_description_bytes = 400;
    let mut h = H::up_drained(c);
    let n = h.n;
    let r = h.t.reconfigure(|r| {
        for i in 0..20 {
            r.add_typed::<f64>(n, &std::format!("v{i}"), PropSpec::new())?;
        }
        Ok(())
    });
    assert!(matches!(r, Err(BuildError::DescriptionTooLarge { bytes }) if bytes > 400));
    assert!(h.drain().is_empty());
    assert!(matches!(
        Tree::new(c_small(), "r", DeviceSpec::typed(&"x".repeat(100))),
        Err(BuildError::DescriptionTooLarge { bytes }) if bytes > 100
    ));
}

fn c_small() -> TreeConfig {
    let mut c = cfg();
    c.max_description_bytes = 100;
    c
}

#[test]
fn removed_slot_index_is_reused_with_a_new_generation() {
    let mut h = H::up_drained(cfg());
    let (n, p) = (h.n, h.p);
    h.t.reconfigure(|r| r.remove_property(p)).expect("remove");
    let q =
        h.t.reconfigure(|r| r.add_typed::<f64>(n, "q", PropSpec::new()))
            .expect("add");
    assert_eq!(q.index(), p.index());
    assert_ne!(q, p);
    assert_eq!(
        h.t.set_value(p, ValueRef::Float(1.0)),
        Err(ValueError::Gone)
    );
    assert_eq!(h.t.set_value(q, ValueRef::Float(1.0)).map(|_| ()), Ok(()));
}

// ----- receipts: SUBACK before ready -----

#[test]
fn without_receipts_states_follow_the_subscribe() {
    let mut h = H::new(cfg());
    h.up();
    let out = h.drain();
    assert_eq!(
        out.last().map(String::as_str),
        Some("ebus/5/r/$state=ready")
    );
}

#[test]
fn with_receipts_no_state_before_the_suback() {
    let mut c = cfg();
    c.receipts = true;
    let mut h = H::new(c);
    h.t.set_value(h.p, ValueRef::Float(1.5)).expect("p");
    h.up();
    let out = h.drain();
    assert!(out.iter().all(|c| !c.ends_with("=ready")), "{out:?}");
    assert_eq!(out.len(), 6);
    // Another device's SUBACK does not count.
    h.receipt(subscribed(&["ebus/5/x/+/+/set"], true));
    assert!(h.drain().is_empty());
    h.receipt(subscribed(&["ebus/5/r/+/+/set", "ebus/5/c/+/+/set"], true));
    assert_eq!(
        h.drain(),
        ["ebus/5/c/$state=ready", "ebus/5/r/$state=ready"]
    );
}

#[test]
fn refused_suback_is_retried_with_the_states_held() {
    let mut c = cfg();
    c.receipts = true;
    let mut h = H::new(c);
    h.up();
    h.drain();
    let f = ["ebus/5/r/+/+/set", "ebus/5/c/+/+/set"];
    h.receipt(subscribed(&f, false));
    assert!(h.drain().is_empty());
    assert_eq!(h.t.poll_timeout(), Some(h.now.add_ms(1_000)));
    h.advance(999);
    assert!(h.drain().is_empty());
    h.advance(1);
    assert_eq!(h.drain(), ["SUB ebus/5/r/+/+/set,ebus/5/c/+/+/set"]);
    h.receipt(subscribed(&f, false));
    assert_eq!(
        h.t.poll_timeout(),
        Some(h.now.add_ms(2_000)),
        "backoff doubles"
    );
    h.advance(2_000);
    h.drain();
    h.receipt(subscribed(&f, true));
    assert_eq!(
        h.drain(),
        ["ebus/5/c/$state=ready", "ebus/5/r/$state=ready"]
    );
}

#[test]
fn receipts_are_ignored_without_the_flag() {
    let mut h = H::up_drained(cfg());
    let p = h.p;
    h.t.clear_value(p).expect("clear");
    h.drain();
    h.receipt(acked("ebus/5/r/n/p"));
    assert_eq!(h.t.tombstones().collect::<Vec<_>>(), ["ebus/5/r/n/p"]);
}

// ----- tombstones -----

fn receipts_up() -> H {
    let mut c = cfg();
    c.receipts = true;
    let mut h = H::new(c);
    h.t.set_value(h.p, ValueRef::Float(1.5)).expect("p");
    h.t.set_value(h.s, ValueRef::Boolean(false)).expect("s");
    h.up();
    h.drain();
    h.receipt(subscribed(&["ebus/5/r/+/+/set", "ebus/5/c/+/+/set"], true));
    h.drain();
    h.events();
    h
}

#[test]
fn tombstone_leaves_on_acknowledgment() {
    let mut h = receipts_up();
    h.t.clear_value(h.p).expect("clear");
    assert_eq!(h.t.tombstones().count(), 1);
    // Not yet polled: an acknowledgment of nothing changes nothing.
    h.receipt(acked("ebus/5/r/n/p"));
    assert_eq!(h.t.tombstones().count(), 1);
    h.drain();
    h.receipt(Receipt::Acknowledged {
        topic: "ebus/5/r/n/p".to_string(),
        cleared: false,
    });
    assert_eq!(h.t.tombstones().count(), 1);
    h.receipt(acked("ebus/5/r/n/p"));
    assert_eq!(h.t.tombstones().count(), 0);
    h.down();
    h.up();
    assert!(!h.drain().contains(&"ebus/5/r/n/p=<clear>".to_string()));
}

#[test]
fn clear_publish_clear_is_kept_until_the_second_acknowledgment() {
    let mut h = receipts_up();
    let t = "ebus/5/r/n/p";
    h.t.clear_value(h.p).expect("clear");
    h.drain();
    h.t.set_value(h.p, ValueRef::Float(2.0)).expect("publish");
    h.drain();
    assert_eq!(h.t.tombstones().count(), 0, "published again");
    h.t.clear_value(h.p).expect("clear again");
    h.drain();
    h.receipt(acked(t));
    assert_eq!(h.t.tombstones().collect::<Vec<_>>(), [t]);
    h.receipt(acked(t));
    assert_eq!(h.t.tombstones().count(), 0);
}

#[test]
fn unacknowledged_clear_is_resent_after_a_drop() {
    let mut h = receipts_up();
    h.t.clear_value(h.p).expect("clear");
    h.drain();
    h.down();
    h.up();
    let out = h.drain();
    assert_eq!(out[1], "ebus/5/r/n/p=<clear>");
    // The acknowledgment for the resent clear retires it.
    h.receipt(acked("ebus/5/r/n/p"));
    assert_eq!(h.t.tombstones().count(), 0);
}

#[test]
fn tombstone_set_is_capped_oldest_first() {
    let mut c = cfg();
    c.max_tombstones = 2;
    let mut h = H::up_drained(c);
    let n = h.n;
    let ids =
        h.t.reconfigure(|r| {
            (0..3)
                .map(|i| r.add_typed::<i64>(n, &std::format!("v{i}"), PropSpec::new()))
                .collect::<Result<Vec<_>, _>>()
        })
        .expect("add");
    for id in &ids {
        h.t.set_value(*id, ValueRef::Integer(1)).expect("set");
    }
    for id in &ids {
        h.t.clear_value(*id).expect("clear");
    }
    assert_eq!(
        h.t.tombstones().collect::<Vec<_>>(),
        ["ebus/5/r/n/v1", "ebus/5/r/n/v2"]
    );
    let out = h.drain();
    assert!(
        !out.contains(&"ebus/5/r/n/v0=<clear>".to_string()),
        "dropped before its turn"
    );
}

// ----- description memo -----

#[test]
fn memo_advances_on_written_with_receipts() {
    let mut h = receipts_up();
    // Only the root's description is reported written.
    h.receipt(Receipt::Written {
        topic: "ebus/5/r/$description".to_string(),
    });
    h.down();
    h.up();
    let out = h.drain();
    assert!(
        !out.contains(&"ebus/5/r/$state=init".to_string()),
        "{out:?}"
    );
    assert!(out.contains(&"ebus/5/c/$state=init".to_string()), "{out:?}");
}

#[test]
fn memo_advances_on_poll_without_receipts() {
    let mut h = H::up_drained(cfg());
    h.down();
    h.up();
    assert!(!h.drain().iter().any(|c| c.ends_with("=init")));
}

// ----- own $state -----

#[test]
fn watch_own_state_is_off_by_default() {
    let mut h = H::up_drained(cfg());
    assert!(!h.t.wants("ebus/5/r/$state"));
    assert_eq!(
        h.send("ebus/5/r/$state", b"lost", Some(false)),
        Handled::Ignored
    );
    assert!(h.drain().is_empty());
    assert!(h.events().is_empty());
}

#[test]
fn own_state_reasserted_over_a_stale_lost_and_capped() {
    let mut c = cfg();
    c.watch_own_state = true;
    let mut h = H::new(c);
    h.up();
    let out = h.take(1);
    assert_eq!(
        out,
        ["SUB ebus/5/r/+/+/set,ebus/5/c/+/+/set,ebus/5/r/$state"]
    );
    // Before the closing root state: ignored.
    h.send("ebus/5/r/$state", b"lost", Some(false));
    h.drain();
    h.events();
    // Echoes and retained copies: ignored.
    h.send("ebus/5/r/$state", b"init", Some(false));
    h.send("ebus/5/r/$state", b"ready", Some(false));
    h.send("ebus/5/r/$state", b"lost", Some(true));
    assert!(h.drain().is_empty());
    assert!(h.events().is_empty());
    for attempt in 1..=3 {
        h.send("ebus/5/r/$state", b"lost", None);
        assert_eq!(h.drain(), ["ebus/5/r/$state=ready"]);
        assert_eq!(
            h.events(),
            [DeviceEvent::StateReasserted {
                attempt,
                gave_up: false
            }]
        );
    }
    h.send("ebus/5/r/$state", b"lost", Some(false));
    assert!(h.drain().is_empty());
    assert_eq!(
        h.events(),
        [DeviceEvent::StateReasserted {
            attempt: 3,
            gave_up: true
        }]
    );
    h.send("ebus/5/r/$state", b"lost", Some(false));
    assert!(h.events().is_empty());
    // A new link-up resets the count.
    h.down();
    h.up();
    h.drain();
    h.events();
    h.send("ebus/5/r/$state", b"lost", Some(false));
    assert_eq!(h.drain(), ["ebus/5/r/$state=ready"]);
    // After declare_lost, `lost` is ours.
    h.t.declare_lost();
    h.drain();
    h.events();
    h.send("ebus/5/r/$state", b"lost", Some(false));
    assert!(h.drain().is_empty());
    assert!(h.events().is_empty());
}

// ----- shutdown -----

#[test]
fn shutdown_announce() {
    let mut h = H::up_drained(cfg());
    h.t.set_value(h.p, ValueRef::Float(2.0)).expect("pending");
    h.t.shutdown(Shutdown::Announce);
    assert_eq!(
        h.drain(),
        [
            "ebus/5/r/n/p=2.0",
            "ebus/5/c/$state=disconnected",
            "ebus/5/r/$state=disconnected"
        ]
    );
    assert!(!h.t.is_connected());
    assert_eq!(
        h.t.set_value(h.p, ValueRef::Float(3.0)),
        Ok(Change::Dropped)
    );
    assert!(!h.t.wants("ebus/5/c/m/s/set"));
    h.up();
    assert!(h.drain().is_empty(), "input after shutdown is ignored");
}

#[test]
fn shutdown_silent() {
    let mut h = H::up_drained(cfg());
    h.t.set_value(h.p, ValueRef::Float(2.0)).expect("pending");
    h.t.shutdown(Shutdown::Silent);
    assert!(h.drain().is_empty());
}

#[test]
fn shutdown_remove() {
    let mut h = H::up_drained(cfg());
    let c = h.c;
    h.t.raise_alert(c, "a", "m").expect("alert");
    h.drain();
    h.t.shutdown(Shutdown::Remove);
    assert_eq!(
        h.drain(),
        [
            "ebus/5/r/$state=<clear>",
            "ebus/5/c/$state=<clear>",
            "ebus/5/c/m/s=<clear>",
            "ebus/5/c/$alert/a=<clear>",
            "ebus/5/c/$description=<clear>",
            "ebus/5/r/n/p=<clear>",
            "ebus/5/r/$description=<clear>",
        ]
    );
}

#[test]
fn shutdown_while_down_publishes_nothing() {
    let mut h = H::up_drained(cfg());
    h.down();
    h.t.shutdown(Shutdown::Remove);
    assert!(h.drain().is_empty());
}

// ----- failed commands -----

#[test]
fn failed_commands_are_requeued_by_kind() {
    let mut h = H::up_drained(cfg());
    let publish = |topic: &str, payload: &[u8], retain: bool| Command::Publish {
        topic: topic.to_string(),
        payload: payload.to_vec(),
        qos: Q2,
        retain,
    };
    h.t.set_value(h.p, ValueRef::Float(5.0)).expect("p");
    h.t.handle_command_failed(h.now, publish("ebus/5/r/n/p", b"4.0", true));
    h.t.handle_command_failed(h.now, publish("ebus/5/c/$state", b"ready", true));
    h.t.handle_command_failed(h.now, publish("ebus/5/c/$description", b"{}", true));
    h.t.handle_command_failed(h.now, publish("ebus/5/r/n/ev", b"1", false));
    assert_eq!(
        h.drain(),
        [
            "ebus/5/r/n/p=5.0",
            "ebus/5/c/$state=ready",
            "ebus/5/c/$description=desc"
        ]
    );
    h.t.handle_command_failed(
        h.now,
        Command::Subscribe {
            filters: vec![("ebus/5/c/+/+/set".to_string(), Q2)],
        },
    );
    assert!(h.drain().is_empty());
    h.advance(1_000);
    assert_eq!(h.drain(), ["SUB ebus/5/c/+/+/set"]);
}

#[test]
fn failed_clear_is_resent_while_it_is_a_tombstone() {
    let mut h = H::up_drained(cfg());
    h.t.clear_value(h.p).expect("clear");
    h.drain();
    h.t.handle_command_failed(
        h.now,
        Command::Publish {
            topic: "ebus/5/r/n/p".to_string(),
            payload: vec![],
            qos: Q2,
            retain: true,
        },
    );
    assert_eq!(h.drain(), ["ebus/5/r/n/p=<clear>"]);
}

// ----- broadcast -----

#[test]
fn broadcast_with_the_flag() {
    let mut c = cfg();
    c.broadcast = true;
    let mut h = H::new(c);
    h.up();
    assert_eq!(
        h.take(1),
        ["SUB ebus/5/r/+/+/set,ebus/5/c/+/+/set,ebus/5/$broadcast/#"]
    );
    h.events();
    assert_eq!(
        h.send("ebus/5/$broadcast/alarm/fire", b"1", Some(false)),
        Handled::Consumed
    );
    assert_eq!(
        h.events(),
        [DeviceEvent::Broadcast {
            subtopic: "alarm/fire".to_string(),
            payload: b"1".to_vec().into_boxed_slice()
        }]
    );
    let off = H::new(cfg());
    assert!(!off.t.wants("ebus/5/$broadcast/alarm"));
}

#[test]
fn broadcast_subtopic_ending_in_set() {
    let mut c = cfg();
    c.broadcast = true;
    let mut h = H::up_drained(c);
    assert!(h.t.wants("ebus/5/$broadcast/set"));
    assert!(h.t.wants("ebus/5/$broadcast/a/set"));
    assert_eq!(
        h.send("ebus/5/$broadcast/a/set", b"1", Some(false)),
        Handled::Consumed
    );
    assert_eq!(
        h.events(),
        [DeviceEvent::Broadcast {
            subtopic: "a/set".to_string(),
            payload: b"1".to_vec().into_boxed_slice()
        }]
    );
    assert!(!H::new(cfg()).t.wants("ebus/5/$broadcast/a/set"));
}

// ----- Role and Pair -----

#[test]
fn tree_owns_its_commands_only() {
    let h = H::new(cfg());
    let publish = |topic: &str| Command::Publish {
        topic: topic.to_string(),
        payload: vec![],
        qos: Q2,
        retain: true,
    };
    assert!(Role::owns(&h.t, &publish("ebus/5/r/$state")));
    assert!(!Role::owns(&h.t, &publish("ebus/5/x/n/p/set")));
    assert!(!Role::owns(&h.t, &publish("ebus/5/$broadcast/a")));
    assert!(!Role::owns(&h.t, &publish("homie/5/r/$state")));
    let sub = |f: &str| Command::Subscribe {
        filters: vec![(f.to_string(), Q2)],
    };
    assert!(Role::owns(&h.t, &sub("ebus/5/r/+/+/set")));
    assert!(!Role::owns(&h.t, &sub("ebus/5/r/$description")));
    assert!(Role::holds(&h.t, "ebus/5/c/+/+/set"));
}

fn pair(watch: bool) -> Pair<Tree, Controller> {
    let mut c = cfg();
    c.watch_own_state = watch;
    let t = Tree::new(c, "r", DeviceSpec::default()).expect("tree");
    let ctl = Controller::new(Config::tree("r").expect("cfg")).expect("ctl");
    Pair::new(t, ctl)
}

fn drain_pair(p: &mut Pair<Tree, Controller>) -> Vec<String> {
    core::iter::from_fn(|| p.poll_command())
        .map(|c| show(&c))
        .collect()
}

#[test]
fn pair_up_sends_the_device_refresh_first() {
    let mut p = pair(false);
    p.handle_link(
        Instant(0),
        Link::Up {
            session_present: false,
        },
    );
    assert_eq!(
        drain_pair(&mut p),
        [
            "SUB ebus/5/r/+/+/set",
            "ebus/5/r/$state=init",
            "ebus/5/r/$description=desc",
            "ebus/5/r/$state=ready",
            "SUB ebus/5/r/$state,ebus/5/r/$description,ebus/5/r/+/+,ebus/5/r/+/+/$target",
        ]
    );
}

#[test]
fn pair_controller_shutdown_keeps_the_devices_state_filter() {
    let mut p = pair(true);
    p.handle_link(
        Instant(0),
        Link::Up {
            session_present: false,
        },
    );
    drain_pair(&mut p);
    p.b.shutdown();
    assert_eq!(
        drain_pair(&mut p),
        ["UNSUB ebus/5/r/$description,ebus/5/r/+/+,ebus/5/r/+/+/$target"]
    );
    assert!(p.holds("ebus/5/r/$state"));
}

#[test]
fn pair_routes_failures_receipts_and_implicit_link_up() {
    let mut c = cfg();
    c.receipts = true;
    let t = Tree::new(c, "r", DeviceSpec::default()).expect("tree");
    let ctl = Controller::new(Config::tree("r").expect("cfg")).expect("ctl");
    let mut p = Pair::new(t, ctl);
    // A /set while down brings both roles up before it is routed.
    p.handle_message(
        Instant(0),
        Message {
            topic: "ebus/5/r/n/p/set",
            payload: b"1",
            retain: Some(false),
        },
    );
    assert!(p.a.is_connected() && p.b.is_connected());
    let out = drain_pair(&mut p);
    assert_eq!(out[0], "SUB ebus/5/r/+/+/set");
    assert!(
        !out.iter().any(|c| c.ends_with("=ready")),
        "held for the SUBACK"
    );
    // The SUBACK reaches the device through the pair.
    p.handle_receipt(
        Instant(0),
        &Receipt::Subscribed {
            filters: vec!["ebus/5/r/+/+/set".to_string()],
            granted: true,
        },
    );
    assert_eq!(drain_pair(&mut p), ["ebus/5/r/$state=ready"]);
    // A failed device publish goes back to the device.
    p.handle_command_failed(
        Instant(0),
        Command::Publish {
            topic: "ebus/5/r/$state".to_string(),
            payload: b"ready".to_vec(),
            qos: Q2,
            retain: true,
        },
    );
    assert_eq!(drain_pair(&mut p), ["ebus/5/r/$state=ready"]);
    // A failed controller SUBSCRIBE goes to the controller, which retries.
    p.handle_command_failed(
        Instant(0),
        Command::Subscribe {
            filters: vec![("ebus/5/r/$description".to_string(), Q2)],
        },
    );
    assert!(drain_pair(&mut p).is_empty());
    let due = p.poll_timeout().expect("retry armed");
    p.handle_timeout(due);
    assert!(
        drain_pair(&mut p)
            .iter()
            .any(|c| c.contains("ebus/5/r/$description"))
    );
}

// ----- typed values -----

#[test]
fn homie_value_conversions() {
    assert_eq!(u8::default_format().as_deref(), Some("0:255"));
    assert_eq!(i16::default_format().as_deref(), Some("-32768:32767"));
    assert_eq!(u32::default_format().as_deref(), Some("0:4294967295"));
    assert_eq!(i64::default_format(), None);
    assert_eq!(f64::default_format(), None);
    assert_eq!(u8::from_set(&SetValue::Text("255".to_string())), Some(255));
    assert_eq!(u8::from_set(&SetValue::Text("256".to_string())), None);
    assert_eq!(f32::from_set(&SetValue::Text("1e39".to_string())), None);
    assert_eq!(
        String::from_set(&SetValue::String(String::new())),
        Some(String::new())
    );
    assert!(f64::same(&1.0, &1.0) && !f64::same(&0.0, &-0.0));
    assert_eq!(
        core::time::Duration::from_set(&SetValue::Text("PT1H2M3.25S".to_string())),
        Some(core::time::Duration::from_millis(3_723_250))
    );
    assert_eq!(
        value::parse_duration("PT0.0000019S"),
        Some(core::time::Duration::from_micros(1))
    );
    assert_eq!(value::parse_duration("P1D"), None);
    assert_eq!(value::parse_duration("PT99999999999999999999H"), None);
    assert!(Color::new("rgb,1,2,3").is_ok() && Color::new("x").is_err());
    assert!(Datetime::new("2026-10-10").is_ok() && Datetime::new("x").is_err());
}

#[test]
fn moved_types_keep_their_controller_paths() {
    let h: crate::controller::Handled = ebus_mqtt::Handled::Consumed;
    assert_eq!(h, Handled::Consumed);
    let a: crate::controller::PropertyAddr = crate::PropertyAddr::new("a", "b", "c").expect("addr");
    let id: crate::controller::Id = a.device;
    assert_eq!(id, crate::Id::new("a").expect("id"));
}

#[test]
fn controller_owns_set_publishes_and_its_own_filters() {
    let ctl = Controller::new(Config::wildcard()).expect("ctl");
    let publish = |topic: &str| Command::Publish {
        topic: topic.to_string(),
        payload: vec![],
        qos: QoS::AtMostOnce,
        retain: false,
    };
    assert!(Role::owns(&ctl, &publish("ebus/5/d/n/p/set")));
    assert!(Role::owns(&ctl, &publish("ebus/5/$broadcast/x")));
    assert!(!Role::owns(&ctl, &publish("ebus/5/d/$state")));
    assert!(!Role::owns(&ctl, &publish("homie/5/d/n/p/set")));
    let sub = |f: &str| Command::Subscribe {
        filters: vec![(f.to_string(), Q2)],
    };
    assert!(Role::owns(&ctl, &sub("ebus/5/+/$state")));
    assert!(!Role::owns(&ctl, &sub("ebus/5/d/+/+/set")));
    assert!(
        !Role::holds(&ctl, "ebus/5/+/$state"),
        "nothing held before a link-up"
    );
}

// ----- builder, value checks, device list -----

#[test]
fn builder_tree_matches_a_reconfigured_tree() {
    let built = H::new(cfg()).t;
    let mut b = Builder::new(cfg(), "r", DeviceSpec::default()).expect("builder");
    let mut e = b.edit();
    let n = e.add_node(e.root(), "n", NodeSpec::default()).expect("n");
    e.add_typed::<f64>(n, "p", PropSpec::new()).expect("p");
    let c = e
        .add_child(e.root(), "c", DeviceSpec::default())
        .expect("c");
    let m = e.add_node(c, "m", NodeSpec::default()).expect("m");
    e.add_typed::<bool>(m, "s", PropSpec::new().settable(true))
        .expect("s");
    assert_eq!(
        e.add_node(c, "m", NodeSpec::default()),
        Err(BuildError::DuplicateId),
        "each change is checked alone"
    );
    let mut t = b.finish().expect("finish");
    for id in ["r", "c"] {
        let (x, y) = (built.device(id).expect("x"), t.device(id).expect("y"));
        assert_eq!(built.description(x), t.description(y), "{id}");
    }
    let order: Vec<u16> = t.devices().map(DevIdx::index).collect();
    assert_eq!(order, [t.root().index(), c.index()]);
    t.handle_link(
        Instant(0),
        Link::Up {
            session_present: false,
        },
    );
    let first = t.poll_command().map(|c| show(&c));
    assert_eq!(
        first.as_deref(),
        Some("SUB ebus/5/r/+/+/set,ebus/5/c/+/+/set"),
        "every device's /set filter is held"
    );
}

#[test]
fn builder_checks_descriptions_at_finish() {
    let mut c = cfg();
    c.max_description_bytes = 200;
    let mut b = Builder::new(c, "r", DeviceSpec::default()).expect("builder");
    let mut e = b.edit();
    let n = e.add_node(e.root(), "n", NodeSpec::default()).expect("n");
    for i in 0..10 {
        e.add_typed::<f64>(n, &std::format!("p{i}"), PropSpec::new())
            .expect("added unchecked");
    }
    assert!(matches!(
        b.finish(),
        Err(BuildError::DescriptionTooLarge { .. })
    ));
}

#[test]
fn builder_sets_receipts() {
    let mut b = Builder::new(cfg(), "r", DeviceSpec::default()).expect("builder");
    assert!(!b.config().receipts);
    b.set_receipts(true);
    assert!(b.finish().expect("tree").config().receipts);
}

#[test]
fn value_check_matches_set_value() {
    let mut t = Tree::new(cfg(), "r", DeviceSpec::default()).expect("tree");
    let (p, e) = t
        .reconfigure(|r| {
            let n = r.add_node(r.root(), "n", NodeSpec::default())?;
            let p = r.add_typed::<f64>(n, "p", PropSpec::new().format("0:0.1"))?;
            let e = r.add_typed::<i64>(n, "e", PropSpec::new().retained(false))?;
            Ok((p, e))
        })
        .expect("build");
    let check = t.value_check(p).expect("check");
    assert_eq!(check.datatype(), Datatype::Float);
    assert!(check.is_retained() && !check.is_target() && !check.is_settable());
    assert!(!t.value_check(e).expect("e").is_retained());
    for v in [0.05, 0.1, 0.1000001, -0.0, f64::NAN, f64::INFINITY] {
        let core = t.set_value(p, ValueRef::Float(v)).map(|_| ());
        assert_eq!(check.check(ValueRef::Float(v)), core, "{v}");
    }
    assert_eq!(
        check.check(ValueRef::Boolean(true)),
        Err(ValueError::WrongDatatype)
    );
    t.reconfigure(|r| r.remove_property(p)).expect("remove");
    assert!(t.value_check(p).is_none(), "a removed slot has no check");
}

#[test]
fn shutdown_releases_a_pending_suback_gate() {
    let mut c = cfg();
    c.receipts = true;
    let mut h = H::new(c);
    h.up();
    let out = h.drain();
    assert!(out.iter().all(|c| !c.ends_with("=ready")), "{out:?}");
    h.t.shutdown(Shutdown::Announce);
    assert_eq!(
        h.drain(),
        [
            "ebus/5/c/$state=disconnected",
            "ebus/5/r/$state=disconnected",
            "ebus/5/c/$state=disconnected",
            "ebus/5/r/$state=disconnected"
        ],
        "the held refresh states and the announce go out"
    );
}
