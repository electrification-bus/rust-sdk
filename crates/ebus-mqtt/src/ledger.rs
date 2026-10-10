//! Which subscriptions this client holds, and who may remove them.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::String,
    vec::Vec,
};

use crate::{Command, QoS};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    /// Holders of this filter string; always at least 1 while the entry exists.
    count: u32,
    /// Highest QoS any holder requested since the filter was first held.
    qos: QoS,
    /// A SUBSCRIBE for this filter at `qos` was issued since the last clean
    /// session began.
    on_wire: bool,
}

/// Refcounts per filter string, so that the subscriptions several holders
/// share are subscribed once and unsubscribed only when the last holder lets go.
///
/// Filters are compared as exact strings; overlapping but different filters
/// are separate subscriptions, as they are on the broker. Host filters are
/// filter strings another part of the same client also subscribes: the ledger
/// subscribes them when a holder asks, but never unsubscribes them, and it
/// never unsubscribes `#`.
///
/// The ledger also tracks wire state: which filters it has issued a SUBSCRIBE
/// for in the current session. A clean session ([`link_up`](Self::link_up)
/// with `session_present: false`) clears it and keeps the counts, so the next
/// [`subscribe`](Self::subscribe) of a held filter goes back on the wire, and
/// [`resubscribe`](Self::resubscribe) restores held filters without
/// counting a new holder.
///
/// Filter strings are not validated here.
#[derive(Clone, Debug, Default)]
pub struct FilterLedger {
    held: BTreeMap<String, Entry>,
    host: BTreeSet<String>,
}

/// Adds `(filter, qos)` to a pending SUBSCRIBE, raising the QoS of a filter
/// already in it.
fn push_subscribe(out: &mut Vec<(String, QoS)>, filter: &str, qos: QoS) {
    match out.iter_mut().find(|(f, _)| f == filter) {
        Some((_, q)) => *q = (*q).max(qos),
        None => out.push((String::from(filter), qos)),
    }
}

fn subscribe_command(filters: Vec<(String, QoS)>) -> Option<Command> {
    (!filters.is_empty()).then_some(Command::Subscribe { filters })
}

fn unsubscribe_command(filters: Vec<String>) -> Option<Command> {
    (!filters.is_empty()).then_some(Command::Unsubscribe { filters })
}

impl FilterLedger {
    /// An empty ledger that will never unsubscribe any of `host_filters`.
    pub fn new(host_filters: impl IntoIterator<Item = String>) -> Self {
        Self {
            held: BTreeMap::new(),
            host: host_filters.into_iter().collect(),
        }
    }

    /// Adds one holder for each filter and returns the SUBSCRIBE for the
    /// filters that must go on the wire, in first-appearance order.
    ///
    /// A filter goes on the wire when it was not held, when it has not been
    /// subscribed since a clean session began, or when the requested QoS is
    /// higher than the QoS it is held at. A filter repeated in `filters` counts
    /// once per appearance and is sent once, at the highest QoS requested.
    pub fn subscribe(&mut self, filters: Vec<(String, QoS)>) -> Option<Command> {
        let mut out = Vec::new();
        for (filter, qos) in filters {
            match self.held.get_mut(&filter) {
                Some(e) => {
                    e.count = e.count.saturating_add(1);
                    if !e.on_wire || qos > e.qos {
                        e.qos = e.qos.max(qos);
                        e.on_wire = true;
                        push_subscribe(&mut out, &filter, e.qos);
                    }
                }
                None => {
                    push_subscribe(&mut out, &filter, qos);
                    self.held.insert(
                        filter,
                        Entry {
                            count: 1,
                            qos,
                            on_wire: true,
                        },
                    );
                }
            }
        }
        subscribe_command(out)
    }

    /// Removes one holder for each filter and returns the UNSUBSCRIBE for the
    /// filters whose count reached zero, that are on the wire, and that are
    /// neither host filters nor `#`.
    ///
    /// A filter that is not held is ignored.
    pub fn unsubscribe(&mut self, filters: Vec<String>) -> Option<Command> {
        let mut out: Vec<String> = Vec::new();
        for filter in filters {
            let Some(e) = self.held.get_mut(&filter) else {
                continue;
            };
            e.count = e.count.saturating_sub(1);
            if e.count > 0 {
                continue;
            }
            let on_wire = e.on_wire;
            self.held.remove(&filter);
            if on_wire && self.may_unsubscribe(&filter) && !out.contains(&filter) {
                out.push(filter);
            }
        }
        unsubscribe_command(out)
    }

    /// Returns a SUBSCRIBE for each of `filters` that is held, at its held QoS,
    /// whether or not it is on the wire, without changing any count.
    ///
    /// A broker re-sends the retained messages matching a filter that is
    /// subscribed again (MQTT 3.1.1 section 3.8.4), so this both restores
    /// subscriptions after a clean session and replays retained state.
    /// Filters that are not held are ignored.
    pub fn resubscribe<S: AsRef<str>>(
        &mut self,
        filters: impl IntoIterator<Item = S>,
    ) -> Option<Command> {
        let mut out = Vec::new();
        for filter in filters {
            let filter = filter.as_ref();
            if let Some(e) = self.held.get_mut(filter) {
                e.on_wire = true;
                push_subscribe(&mut out, filter, e.qos);
            }
        }
        subscribe_command(out)
    }

    /// Drops every holder of every filter and returns the UNSUBSCRIBE for the
    /// ones on the wire, except host filters and `#`, in filter order.
    pub fn release_all(&mut self) -> Option<Command> {
        let held = core::mem::take(&mut self.held);
        let out = held
            .into_iter()
            .filter(|(f, e)| e.on_wire && self.may_unsubscribe(f))
            .map(|(f, _)| f)
            .collect();
        unsubscribe_command(out)
    }

    /// Records a CONNACK. With `session_present: false` the broker holds no
    /// subscriptions, so wire state is cleared; counts are kept either way.
    pub fn link_up(&mut self, session_present: bool) {
        if !session_present {
            for e in self.held.values_mut() {
                e.on_wire = false;
            }
        }
    }

    /// Filters with at least one holder, in byte order.
    pub fn held(&self) -> impl Iterator<Item = &str> {
        self.held.keys().map(String::as_str)
    }

    /// Number of holders of `filter`; 0 when it is not held.
    pub fn count(&self, filter: &str) -> u32 {
        self.held.get(filter).map_or(0, |e| e.count)
    }

    /// True when `filter` is one of the host filters given to [`new`](Self::new).
    pub fn is_host_filter(&self, filter: &str) -> bool {
        self.host.contains(filter)
    }

    fn may_unsubscribe(&self, filter: &str) -> bool {
        filter != "#" && !self.host.contains(filter)
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use alloc::{format, string::ToString, vec};

    fn s(f: &str) -> String {
        f.to_string()
    }

    fn sub(filters: &[(&str, QoS)]) -> Vec<(String, QoS)> {
        filters.iter().map(|(f, q)| (s(f), *q)).collect()
    }

    fn unsub(filters: &[&str]) -> Vec<String> {
        filters.iter().map(|f| s(f)).collect()
    }

    fn subscribe_cmd(filters: &[(&str, QoS)]) -> Option<Command> {
        Some(Command::Subscribe {
            filters: sub(filters),
        })
    }

    fn unsubscribe_cmd(filters: &[&str]) -> Option<Command> {
        Some(Command::Unsubscribe {
            filters: unsub(filters),
        })
    }

    const Q0: QoS = QoS::AtMostOnce;
    const Q1: QoS = QoS::AtLeastOnce;
    const Q2: QoS = QoS::ExactlyOnce;

    #[test]
    fn first_holder_subscribes_last_holder_unsubscribes() {
        let mut l = FilterLedger::default();
        assert_eq!(
            l.subscribe(sub(&[("a/b", Q2)])),
            subscribe_cmd(&[("a/b", Q2)])
        );
        assert_eq!(l.subscribe(sub(&[("a/b", Q2)])), None);
        assert_eq!(l.count("a/b"), 2);
        assert_eq!(l.unsubscribe(unsub(&["a/b"])), None);
        assert_eq!(l.count("a/b"), 1);
        assert_eq!(l.unsubscribe(unsub(&["a/b"])), unsubscribe_cmd(&["a/b"]));
        assert_eq!(l.count("a/b"), 0);
        assert_eq!(l.held().count(), 0);
        assert_eq!(l.unsubscribe(unsub(&["a/b"])), None);
        assert_eq!(l.count("a/b"), 0);
    }

    #[test]
    fn one_command_for_newly_held_filters_only() {
        let mut l = FilterLedger::default();
        l.subscribe(sub(&[("x", Q2)]));
        assert_eq!(
            l.subscribe(sub(&[("y", Q2), ("x", Q2), ("z", Q1)])),
            subscribe_cmd(&[("y", Q2), ("z", Q1)])
        );
        assert_eq!(l.held().collect::<Vec<_>>(), ["x", "y", "z"]);
    }

    #[test]
    fn empty_requests_produce_no_command() {
        let mut l = FilterLedger::default();
        assert_eq!(l.subscribe(Vec::new()), None);
        assert_eq!(l.unsubscribe(Vec::new()), None);
        assert_eq!(l.resubscribe(Vec::<String>::new()), None);
        assert_eq!(l.release_all(), None);
    }

    #[test]
    fn duplicates_in_one_call_count_each_and_send_once() {
        let mut l = FilterLedger::default();
        assert_eq!(
            l.subscribe(sub(&[("a", Q1), ("a", Q2), ("a", Q0)])),
            subscribe_cmd(&[("a", Q2)])
        );
        assert_eq!(l.count("a"), 3);
        assert_eq!(l.unsubscribe(unsub(&["a", "a"])), None);
        assert_eq!(l.unsubscribe(unsub(&["a", "a"])), unsubscribe_cmd(&["a"]));
        assert_eq!(l.count("a"), 0);
    }

    #[test]
    fn unsubscribe_then_subscribe_in_one_sequence() {
        let mut l = FilterLedger::default();
        l.subscribe(sub(&[("a", Q2)]));
        assert_eq!(l.unsubscribe(unsub(&["a"])), unsubscribe_cmd(&["a"]));
        assert_eq!(l.subscribe(sub(&[("a", Q2)])), subscribe_cmd(&[("a", Q2)]));
    }

    #[test]
    fn higher_qos_goes_back_on_the_wire_lower_does_not() {
        let mut l = FilterLedger::default();
        l.subscribe(sub(&[("a", Q1)]));
        assert_eq!(l.subscribe(sub(&[("a", Q0)])), None);
        assert_eq!(l.subscribe(sub(&[("a", Q2)])), subscribe_cmd(&[("a", Q2)]));
        // The held QoS never drops while the filter is held.
        assert_eq!(l.unsubscribe(unsub(&["a"])), None);
        assert_eq!(l.resubscribe(["a"]), subscribe_cmd(&[("a", Q2)]));
    }

    #[test]
    fn host_filters_and_hash_are_never_unsubscribed() {
        let mut l = FilterLedger::new([s("ebus/5/+/$description")]);
        assert!(l.is_host_filter("ebus/5/+/$description"));
        assert!(!l.is_host_filter("#"));
        assert_eq!(
            l.subscribe(sub(&[("ebus/5/+/$description", Q2), ("#", Q0), ("a", Q2)])),
            subscribe_cmd(&[("ebus/5/+/$description", Q2), ("#", Q0), ("a", Q2)])
        );
        assert_eq!(
            l.unsubscribe(unsub(&["ebus/5/+/$description", "#", "a"])),
            unsubscribe_cmd(&["a"])
        );
        assert_eq!(l.held().count(), 0);

        l.subscribe(sub(&[("ebus/5/+/$description", Q2), ("#", Q0), ("b", Q1)]));
        assert_eq!(l.release_all(), unsubscribe_cmd(&["b"]));
        assert_eq!(l.held().count(), 0);
    }

    #[test]
    fn host_filter_overlap_is_not_ownership() {
        // Only the exact host string is protected.
        let mut l = FilterLedger::new([s("ebus/5/+/$description")]);
        l.subscribe(sub(&[("ebus/5/dev/$description", Q2)]));
        assert_eq!(
            l.unsubscribe(unsub(&["ebus/5/dev/$description"])),
            unsubscribe_cmd(&["ebus/5/dev/$description"])
        );
    }

    #[test]
    fn clean_session_clears_wire_state_and_keeps_counts() {
        let mut l = FilterLedger::default();
        l.subscribe(sub(&[("a", Q2), ("a", Q2), ("b", Q1)]));
        l.link_up(false);
        assert_eq!(l.count("a"), 2);
        assert_eq!(l.count("b"), 1);
        // Not on the wire: removing the last holder sends nothing.
        assert_eq!(l.unsubscribe(unsub(&["b"])), None);
        // The next holder of a held filter puts it back on the wire.
        assert_eq!(l.subscribe(sub(&[("a", Q1)])), subscribe_cmd(&[("a", Q2)]));
        assert_eq!(l.count("a"), 3);
        assert_eq!(l.subscribe(sub(&[("a", Q1)])), None);
    }

    #[test]
    fn session_present_keeps_wire_state() {
        let mut l = FilterLedger::default();
        l.subscribe(sub(&[("a", Q2)]));
        l.link_up(true);
        assert_eq!(l.subscribe(sub(&[("a", Q2)])), None);
        assert_eq!(l.unsubscribe(unsub(&["a", "a"])), unsubscribe_cmd(&["a"]));
    }

    #[test]
    fn resubscribe_restores_without_counting() {
        let mut l = FilterLedger::default();
        l.subscribe(sub(&[("a", Q2), ("b", Q1)]));
        l.link_up(false);
        assert_eq!(
            l.resubscribe(["b", "missing", "a", "b"]),
            subscribe_cmd(&[("b", Q1), ("a", Q2)])
        );
        assert_eq!(l.count("a"), 1);
        assert_eq!(l.count("b"), 1);
        assert_eq!(l.count("missing"), 0);
        // Back on the wire, so the last holder unsubscribes.
        assert_eq!(l.unsubscribe(unsub(&["a"])), unsubscribe_cmd(&["a"]));
        // Resubscribing a filter already on the wire still sends it, which
        // replays its retained messages.
        assert_eq!(l.resubscribe([s("b")]), subscribe_cmd(&[("b", Q1)]));
    }

    #[test]
    fn release_all_unsubscribes_only_what_is_on_the_wire() {
        let mut l = FilterLedger::default();
        l.subscribe(sub(&[("a", Q2), ("b", Q2)]));
        l.link_up(false);
        l.resubscribe(["b"]);
        l.subscribe(sub(&[("c", Q0), ("c", Q0)]));
        assert_eq!(l.release_all(), unsubscribe_cmd(&["b", "c"]));
        assert_eq!(l.held().count(), 0);
        assert_eq!(l.count("c"), 0);
    }

    /// A random walk over operations, checking the ledger against a model in
    /// which counts never go negative and every filter's UNSUBSCRIBE follows
    /// its SUBSCRIBE.
    #[test]
    fn random_operations_keep_invariants() {
        let filters = ["a", "b", "#", "host", "a/+"];
        let mut l = FilterLedger::new([s("host")]);
        let mut model: BTreeMap<&str, u32> = BTreeMap::new();
        // Whether the broker would hold each filter, from the commands sent.
        let mut broker: BTreeMap<String, bool> = BTreeMap::new();
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for step in 0..20_000 {
            let r = next();
            let f = filters[(r % filters.len() as u64) as usize];
            let ctx = format!("step {step} op {} filter {f}", (r >> 8) % 6);
            let apply = |cmd: Option<Command>, broker: &mut BTreeMap<String, bool>| match cmd {
                Some(Command::Subscribe { filters }) => {
                    assert!(!filters.is_empty());
                    for (f, _) in filters {
                        broker.insert(f, true);
                    }
                }
                Some(Command::Unsubscribe { filters }) => {
                    assert!(!filters.is_empty());
                    for f in filters {
                        assert!(f != "#" && f != "host", "{f}");
                        assert_eq!(broker.get(&f), Some(&true), "unsubscribe of {f}");
                        broker.insert(f, false);
                    }
                }
                None => {}
                other => panic!("{other:?}"),
            };
            match (r >> 8) % 6 {
                0 | 1 => {
                    let cmd = l.subscribe(vec![(s(f), Q2)]);
                    *model.entry(f).or_default() += 1;
                    if model[f] == 1 {
                        assert!(cmd.is_some(), "{ctx}");
                    }
                    apply(cmd, &mut broker);
                }
                2 | 3 => {
                    let cmd = l.unsubscribe(vec![s(f)]);
                    let c = model.entry(f).or_default();
                    let was = *c;
                    *c = c.saturating_sub(1);
                    if was != 1 || f == "#" || f == "host" {
                        assert_eq!(cmd, None, "{ctx}");
                    }
                    apply(cmd, &mut broker);
                }
                4 => {
                    let session_present = r & 1 == 0;
                    l.link_up(session_present);
                    if !session_present {
                        broker.clear();
                    }
                }
                _ => {
                    let cmd = l.resubscribe(filters);
                    apply(cmd, &mut broker);
                }
            }
            for g in filters {
                assert_eq!(l.count(g), model.get(g).copied().unwrap_or(0), "{ctx}");
            }
            let held: Vec<&str> = l.held().collect();
            let want: Vec<&str> = model
                .iter()
                .filter(|(_, c)| **c > 0)
                .map(|(f, _)| *f)
                .collect();
            assert_eq!(held, want, "{ctx}");
        }
    }
}
