//! Roles that share one MQTT link, and the combinator that runs two of them.

use alloc::{string::String, vec::Vec};

use crate::{Command, Handled, Instant, Link, Message};

/// A broker or client report on a command already sent, for a host that
/// tracks delivery (a device built with `TreeConfig::receipts`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Receipt {
    /// The SUBACK of one SUBSCRIBE.
    Subscribed {
        /// The SUBSCRIBE's filters, in packet order.
        filters: Vec<String>,
        /// False when any return code is `0x80` (failure).
        granted: bool,
    },
    /// A publish was written to the connection.
    Written {
        /// The publish's topic.
        topic: String,
    },
    /// The broker acknowledged a publish (PUBACK at QoS 1, PUBCOMP at QoS 2).
    Acknowledged {
        /// The publish's topic.
        topic: String,
        /// The publish had a zero-length payload.
        cleared: bool,
    },
}

/// One protocol role on an MQTT link: a sans-IO state machine fed with link
/// changes, messages and the caller's time, and drained of commands.
///
/// The methods mirror the controller's and the device's own; see either for
/// the calling contract.
pub trait Role {
    /// A CONNACK, or a lost connection.
    fn handle_link(&mut self, now: Instant, link: Link);
    /// One received message.
    fn handle_message(&mut self, now: Instant, msg: Message<'_>) -> Handled;
    /// A command the host could not hand to its client.
    fn handle_command_failed(&mut self, now: Instant, cmd: Command);
    /// The instant [`poll_timeout`](Self::poll_timeout) returned has passed.
    fn handle_timeout(&mut self, now: Instant);
    /// The host dropped messages for `topic`'s device; ignored by default.
    fn handle_dropped(&mut self, _now: Instant, _topic: &str) {}
    /// [`handle_dropped`](Self::handle_dropped) at its place in the message
    /// order; ignored by default.
    fn handle_lost(&mut self, _topic: &str) {}
    /// A delivery report; ignored by default.
    fn handle_receipt(&mut self, _now: Instant, _receipt: &Receipt) {}
    /// The next command for the MQTT client.
    fn poll_command(&mut self) -> Option<Command>;
    /// When to call [`handle_timeout`](Self::handle_timeout) next.
    fn poll_timeout(&self) -> Option<Instant>;
    /// True when the role takes messages on `topic`.
    fn wants(&self, topic: &str) -> bool;
    /// True when `cmd` came from this role, so a failure goes back to it.
    fn owns(&self, cmd: &Command) -> bool;
    /// True when this role holds a subscription to `filter`.
    fn holds(&self, filter: &str) -> bool;
}

/// Two roles on one link.
///
/// Link changes and commands go to `a` first, then `b`. Messages go to every
/// role that [`wants`](Role::wants) them, receipts and drop notices to both,
/// and a failed command to the first role that [`owns`](Role::owns) it. Each
/// role keeps its own subscriptions: a filter one role unsubscribes while the
/// other still [`holds`](Role::holds) it is removed from the UNSUBSCRIBE, so
/// the role that releases a shared filter last unsubscribes it.
///
/// A wanted message while the link is down (or before the first link-up)
/// counts as `Link::Up { session_present: false }`, delivered to both roles
/// before the message, so neither comes up alone.
#[derive(Debug)]
pub struct Pair<A: Role, B: Role> {
    /// The role served first; the device in a device and controller pair.
    pub a: A,
    /// The role served second.
    pub b: B,
    up: bool,
}

impl<A: Role, B: Role> Pair<A, B> {
    /// Both roles on one link that is not up yet.
    pub fn new(a: A, b: B) -> Self {
        Pair { a, b, up: false }
    }

    /// True between a link-up (explicit or implicit) and the next link-down.
    pub fn is_up(&self) -> bool {
        self.up
    }
}

/// `cmd` without the filters `other` holds; `None` when nothing is left.
fn strip(cmd: Command, other: &impl Role) -> Option<Command> {
    match cmd {
        Command::Unsubscribe { filters } => {
            let filters: Vec<String> = filters.into_iter().filter(|f| !other.holds(f)).collect();
            (!filters.is_empty()).then_some(Command::Unsubscribe { filters })
        }
        cmd => Some(cmd),
    }
}

impl<A: Role, B: Role> Role for Pair<A, B> {
    fn handle_link(&mut self, now: Instant, link: Link) {
        self.up = matches!(link, Link::Up { .. });
        self.a.handle_link(now, link);
        self.b.handle_link(now, link);
    }

    fn handle_message(&mut self, now: Instant, msg: Message<'_>) -> Handled {
        let (wa, wb) = (self.a.wants(msg.topic), self.b.wants(msg.topic));
        if !wa && !wb {
            return Handled::Ignored;
        }
        if !self.up {
            self.handle_link(
                now,
                Link::Up {
                    session_present: false,
                },
            );
        }
        let mut handled = Handled::Ignored;
        if wa && self.a.handle_message(now, msg) == Handled::Consumed {
            handled = Handled::Consumed;
        }
        if wb && self.b.handle_message(now, msg) == Handled::Consumed {
            handled = Handled::Consumed;
        }
        handled
    }

    fn handle_command_failed(&mut self, now: Instant, cmd: Command) {
        if self.a.owns(&cmd) {
            self.a.handle_command_failed(now, cmd);
        } else if self.b.owns(&cmd) {
            self.b.handle_command_failed(now, cmd);
        }
    }

    fn handle_timeout(&mut self, now: Instant) {
        self.a.handle_timeout(now);
        self.b.handle_timeout(now);
    }

    fn handle_dropped(&mut self, now: Instant, topic: &str) {
        self.a.handle_dropped(now, topic);
        self.b.handle_dropped(now, topic);
    }

    fn handle_lost(&mut self, topic: &str) {
        self.a.handle_lost(topic);
        self.b.handle_lost(topic);
    }

    fn handle_receipt(&mut self, now: Instant, receipt: &Receipt) {
        self.a.handle_receipt(now, receipt);
        self.b.handle_receipt(now, receipt);
    }

    fn poll_command(&mut self) -> Option<Command> {
        while let Some(cmd) = self.a.poll_command() {
            if let Some(cmd) = strip(cmd, &self.b) {
                return Some(cmd);
            }
        }
        while let Some(cmd) = self.b.poll_command() {
            if let Some(cmd) = strip(cmd, &self.a) {
                return Some(cmd);
            }
        }
        None
    }

    fn poll_timeout(&self) -> Option<Instant> {
        match (self.a.poll_timeout(), self.b.poll_timeout()) {
            (Some(x), Some(y)) => Some(x.min(y)),
            (x, y) => x.or(y),
        }
    }

    fn wants(&self, topic: &str) -> bool {
        self.a.wants(topic) || self.b.wants(topic)
    }

    fn owns(&self, cmd: &Command) -> bool {
        self.a.owns(cmd) || self.b.owns(cmd)
    }

    fn holds(&self, filter: &str) -> bool {
        self.a.holds(filter) || self.b.holds(filter)
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use crate::{FilterLedger, QoS};
    use alloc::{collections::VecDeque, string::ToString, vec, vec::Vec};

    /// A role that records its input and replays scripted commands.
    #[derive(Default)]
    struct Fake {
        name: &'static str,
        prefix: &'static str,
        ledger: FilterLedger,
        queue: VecDeque<Command>,
        log: Vec<std::string::String>,
        timeout: Option<Instant>,
    }

    impl Fake {
        fn new(name: &'static str, prefix: &'static str) -> Self {
            Fake {
                name,
                prefix,
                ..Fake::default()
            }
        }

        fn sub(&mut self, filters: &[&str]) {
            let cmd = self.ledger.subscribe(
                filters
                    .iter()
                    .map(|f| (f.to_string(), QoS::ExactlyOnce))
                    .collect(),
            );
            self.queue.extend(cmd);
        }

        fn unsub(&mut self, filters: &[&str]) {
            let cmd = self
                .ledger
                .unsubscribe(filters.iter().map(|f| f.to_string()).collect());
            self.queue.extend(cmd);
        }

        fn publish(&mut self, topic: &str) {
            self.queue.push_back(Command::Publish {
                topic: topic.to_string(),
                payload: Vec::new(),
                qos: QoS::AtMostOnce,
                retain: false,
            });
        }
    }

    impl Role for Fake {
        fn handle_link(&mut self, _now: Instant, link: Link) {
            self.log.push(std::format!("{}:{link:?}", self.name));
        }
        fn handle_message(&mut self, _now: Instant, msg: Message<'_>) -> Handled {
            self.log
                .push(std::format!("{}:msg {}", self.name, msg.topic));
            Handled::Consumed
        }
        fn handle_command_failed(&mut self, _now: Instant, cmd: Command) {
            self.log.push(std::format!("{}:failed {cmd:?}", self.name));
        }
        fn handle_timeout(&mut self, _now: Instant) {
            self.log.push(std::format!("{}:timeout", self.name));
        }
        fn handle_dropped(&mut self, _now: Instant, topic: &str) {
            self.log.push(std::format!("{}:dropped {topic}", self.name));
        }
        fn handle_receipt(&mut self, _now: Instant, receipt: &Receipt) {
            self.log
                .push(std::format!("{}:receipt {receipt:?}", self.name));
        }
        fn poll_command(&mut self) -> Option<Command> {
            self.queue.pop_front()
        }
        fn poll_timeout(&self) -> Option<Instant> {
            self.timeout
        }
        fn wants(&self, topic: &str) -> bool {
            topic.starts_with(self.prefix)
        }
        fn owns(&self, cmd: &Command) -> bool {
            match cmd {
                Command::Publish { topic, .. } => topic.starts_with(self.prefix),
                _ => false,
            }
        }
        fn holds(&self, filter: &str) -> bool {
            self.ledger.count(filter) > 0
        }
    }

    fn pair() -> Pair<Fake, Fake> {
        Pair::new(Fake::new("dev", "d/"), Fake::new("ctl", "c/"))
    }

    fn drain(p: &mut Pair<Fake, Fake>) -> Vec<Command> {
        core::iter::from_fn(|| p.poll_command()).collect()
    }

    fn unsub_cmd(filters: &[&str]) -> Command {
        Command::Unsubscribe {
            filters: filters.iter().map(|f| f.to_string()).collect(),
        }
    }

    #[test]
    fn link_and_commands_go_to_a_first() {
        let mut p = pair();
        p.b.publish("c/1");
        p.a.publish("d/1");
        p.a.publish("d/2");
        p.handle_link(
            Instant(0),
            Link::Up {
                session_present: true,
            },
        );
        assert_eq!(
            p.a.log,
            ["dev:Up { session_present: true }"],
            "a sees the link"
        );
        assert_eq!(p.b.log, ["ctl:Up { session_present: true }"]);
        let topics: Vec<_> = drain(&mut p)
            .into_iter()
            .map(|c| match c {
                Command::Publish { topic, .. } => topic,
                other => std::format!("{other:?}"),
            })
            .collect();
        assert_eq!(topics, ["d/1", "d/2", "c/1"]);
    }

    #[test]
    fn unsubscribe_keeps_filters_the_other_role_holds() {
        let mut p = pair();
        p.a.sub(&["x/$state", "d/+/+/set"]);
        p.b.sub(&["x/$state", "x/$description"]);
        drain(&mut p);
        // The controller releases everything; the device still holds x/$state.
        p.b.unsub(&["x/$state", "x/$description"]);
        assert_eq!(drain(&mut p), [unsub_cmd(&["x/$description"])]);
        // The device releases the shared filter last, so it unsubscribes it.
        p.a.unsub(&["x/$state"]);
        assert_eq!(drain(&mut p), [unsub_cmd(&["x/$state"])]);
    }

    #[test]
    fn unsubscribe_entirely_held_by_the_other_role_is_dropped() {
        let mut p = pair();
        p.a.sub(&["x/$state"]);
        p.b.sub(&["x/$state"]);
        drain(&mut p);
        p.a.unsub(&["x/$state"]);
        p.a.publish("d/after");
        let cmds = drain(&mut p);
        assert_eq!(cmds.len(), 1);
        assert!(matches!(&cmds[0], Command::Publish { topic, .. } if topic == "d/after"));
        assert!(p.holds("x/$state"));
    }

    #[test]
    fn messages_go_to_every_role_that_wants_them() {
        let mut p = pair();
        p.handle_link(
            Instant(0),
            Link::Up {
                session_present: false,
            },
        );
        let msg = |topic| Message {
            topic,
            payload: b"",
            retain: None,
        };
        assert_eq!(p.handle_message(Instant(0), msg("d/x")), Handled::Consumed);
        assert_eq!(p.handle_message(Instant(0), msg("c/x")), Handled::Consumed);
        assert_eq!(p.handle_message(Instant(0), msg("z/x")), Handled::Ignored);
        assert_eq!(p.a.log.last().map(|s| s.as_str()), Some("dev:msg d/x"));
        assert_eq!(p.b.log.last().map(|s| s.as_str()), Some("ctl:msg c/x"));
        assert_eq!(p.a.log.len(), 2);
        assert_eq!(p.b.log.len(), 2);
    }

    #[test]
    fn a_message_while_down_brings_both_roles_up_first() {
        let mut p = pair();
        let msg = Message {
            topic: "c/x",
            payload: b"1",
            retain: Some(false),
        };
        p.handle_message(Instant(5), msg);
        assert!(p.is_up());
        assert_eq!(p.a.log, ["dev:Up { session_present: false }"]);
        assert_eq!(
            p.b.log,
            ["ctl:Up { session_present: false }", "ctl:msg c/x"]
        );
        // Once up, no second link-up.
        p.handle_message(Instant(6), msg);
        assert_eq!(p.a.log.len(), 1);
        // Down, then a message unwanted by both: still down.
        p.handle_link(Instant(7), Link::Down);
        p.handle_message(
            Instant(8),
            Message {
                topic: "z/x",
                ..msg
            },
        );
        assert!(!p.is_up());
    }

    #[test]
    fn failed_command_goes_to_its_owner() {
        let mut p = pair();
        let publish = |topic: &str| Command::Publish {
            topic: topic.to_string(),
            payload: vec![],
            qos: QoS::AtMostOnce,
            retain: false,
        };
        p.handle_command_failed(Instant(0), publish("c/1"));
        p.handle_command_failed(Instant(0), publish("d/1"));
        p.handle_command_failed(Instant(0), publish("z/1"));
        assert_eq!(p.a.log.len(), 1);
        assert!(p.a.log[0].contains("d/1"));
        assert_eq!(p.b.log.len(), 1);
        assert!(p.b.log[0].contains("c/1"));
    }

    #[test]
    fn receipts_drops_and_timeouts_reach_both_roles() {
        let mut p = pair();
        let r = Receipt::Written {
            topic: "d/1".to_string(),
        };
        p.handle_receipt(Instant(0), &r);
        p.handle_dropped(Instant(0), "c/x");
        p.handle_timeout(Instant(0));
        for log in [&p.a.log, &p.b.log] {
            assert_eq!(log.len(), 3);
            assert!(log[0].contains("Written"));
            assert!(log[1].contains("dropped c/x"));
            assert!(log[2].ends_with("timeout"));
        }
    }

    #[test]
    fn poll_timeout_is_the_earlier_one() {
        let mut p = pair();
        assert_eq!(p.poll_timeout(), None);
        p.b.timeout = Some(Instant(9));
        assert_eq!(p.poll_timeout(), Some(Instant(9)));
        p.a.timeout = Some(Instant(4));
        assert_eq!(p.poll_timeout(), Some(Instant(4)));
    }
}
