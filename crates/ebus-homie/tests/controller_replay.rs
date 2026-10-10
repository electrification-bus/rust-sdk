//! Replays retained Homie trees through the sans-IO controller.
//!
//! A simulated broker holds a retained store built from each vendored
//! `fixtures/panel-sim` capture (every device's `$state` `ready`, its
//! `$description` and every captured value) and answers the controller's
//! SUBSCRIBE commands as a broker would: each new filter gets the retained
//! messages it matches, in topic order, queued behind what is already in
//! flight. Time is virtual: one millisecond per delivered message, and the
//! clock jumps to the controller's next deadline when the queue is empty.
//!
//! Set `EBUS_CAPTURE_CORPUS` to a directory to also replay every `*.tree.json`
//! below it; that corpus is never copied into this repository, and only counts
//! are printed for it.

#![cfg(feature = "std")]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

use ebus_homie::controller::{Config, Controller, Event, Pacing};
use ebus_homie::datatype::{self, Datatype};
use ebus_homie::description::{self, DeviceDescription, PropertyDescription};
use ebus_homie::state::DeviceState;
use ebus_mqtt::{Command, Instant, Link, Message, filter};
use serde_json::{Value as Json, json};

const DOMAIN_PREFIX: &str = "ebus/5/";
const SETTLE_MS: u64 = 60_000;

fn topic(dev: &str, rest: &str) -> String {
    format!("{DOMAIN_PREFIX}{dev}/{rest}")
}

/// Wire bytes of a value: `""` is sent as `0x00`.
fn wire(text: &str) -> Vec<u8> {
    if text.is_empty() {
        vec![0]
    } else {
        text.as_bytes().to_vec()
    }
}

// ===== the tree a broker holds =====

#[derive(Clone, Debug)]
struct Dev {
    desc_json: String,
    desc: DeviceDescription,
    /// (node, property) to value text, retained properties only.
    values: BTreeMap<(String, String), String>,
    /// (node, property) to `$target` text.
    targets: BTreeMap<(String, String), String>,
}

impl Dev {
    fn new(desc_json: String) -> Result<Self, String> {
        let desc = description::parse(&desc_json).map_err(|e| e.as_str().to_string())?;
        Ok(Dev {
            desc_json,
            desc,
            values: BTreeMap::new(),
            targets: BTreeMap::new(),
        })
    }

    fn retained_props(&self) -> impl Iterator<Item = (&str, &str, &PropertyDescription)> {
        self.desc.nodes.iter().flat_map(|(n, node)| {
            node.properties
                .iter()
                .filter(|(_, p)| p.retained)
                .map(move |(p, d)| (n.as_str(), p.as_str(), d))
        })
    }

    fn decl(&self, node: &str, prop: &str) -> Option<&PropertyDescription> {
        let (_, n) = self.desc.nodes.iter().find(|(id, _)| id == node)?;
        n.properties
            .iter()
            .find(|(id, _)| id == prop)
            .map(|(_, p)| p)
    }

    /// Declared retained properties without a value.
    fn gaps(&self) -> u32 {
        let n = self
            .retained_props()
            .filter(|(n, p, _)| !self.values.contains_key(&(n.to_string(), p.to_string())))
            .count();
        u32::try_from(n).unwrap_or(u32::MAX)
    }

    /// Retained properties declared in `nodes` (`None`: every node).
    fn retained_in(&self, nodes: Option<&[String]>) -> u64 {
        self.retained_props()
            .filter(|(n, _, _)| nodes.is_none_or(|ns| ns.iter().any(|x| x == n)))
            .count() as u64
    }
}

#[derive(Clone, Debug, Default)]
struct Tree {
    devices: BTreeMap<String, Dev>,
    /// Values dropped while loading: undeclared or `retained: false`.
    skipped_values: usize,
    /// Devices without a `$description`, left out.
    skipped_devices: usize,
}

impl Tree {
    fn load(path: &Path) -> Result<Tree, String> {
        let text = fs::read_to_string(path).map_err(|e| e.to_string())?;
        let json: Json = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        let devices = json
            .get("devices")
            .and_then(Json::as_object)
            .ok_or("no devices")?;
        let mut tree = Tree::default();
        for (id, device) in devices {
            let Some(raw) = device.get("description") else {
                tree.skipped_devices += 1;
                continue;
            };
            let mut dev = Dev::new(raw.to_string()).map_err(|e| format!("{id}: {e}"))?;
            for field in ["properties", "numeric_properties"] {
                let Some(values) = device.get(field).and_then(Json::as_object) else {
                    continue;
                };
                for (key, value) in values {
                    let text = match value.as_str() {
                        Some(s) => s.to_string(),
                        None => value.to_string(),
                    };
                    let Some((n, p)) = key.split_once('/') else {
                        tree.skipped_values += 1;
                        continue;
                    };
                    if dev.decl(n, p).is_some_and(|d| d.retained) {
                        dev.values.insert((n.to_string(), p.to_string()), text);
                    } else {
                        tree.skipped_values += 1;
                    }
                }
            }
            tree.devices.insert(id.clone(), dev);
        }
        Ok(tree)
    }

    /// Gives every declared retained property without a value one that passes
    /// [`datatype::check_value`].
    fn filled(&self) -> Result<Tree, String> {
        let mut out = self.clone();
        for (id, dev) in out.devices.iter_mut() {
            let gaps: Vec<(String, String, PropertyDescription)> = dev
                .retained_props()
                .filter(|(n, p, _)| !dev.values.contains_key(&(n.to_string(), p.to_string())))
                .map(|(n, p, d)| (n.to_string(), p.to_string(), d.clone()))
                .collect();
            for (n, p, d) in gaps {
                let v = filler(&d).ok_or_else(|| format!("{id} {n}/{p}: no filler value"))?;
                dev.values.insert((n, p), v);
            }
        }
        Ok(out)
    }

    /// Devices without a parent in the tree.
    fn roots(&self) -> Vec<String> {
        self.devices
            .iter()
            .filter(|(_, d)| {
                d.desc
                    .parent
                    .as_ref()
                    .is_none_or(|p| !self.devices.contains_key(p))
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// `root` and every device its descriptions declare under it.
    fn subtree(&self, root: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let mut todo = vec![root.to_string()];
        while let Some(id) = todo.pop() {
            let Some(dev) = self.devices.get(&id) else {
                continue;
            };
            if !out.insert(id) {
                continue;
            }
            todo.extend(dev.desc.children.iter().cloned());
        }
        out
    }

    /// Ids in publish order: roots first, then the rest in id order.
    fn publish_order(&self) -> Vec<String> {
        let roots = self.roots();
        let mut out = roots.clone();
        out.extend(self.devices.keys().filter(|k| !roots.contains(k)).cloned());
        out
    }
}

/// A value for `prop` that passes [`datatype::check_value`].
fn filler(prop: &PropertyDescription) -> Option<String> {
    let format = prop.format.as_deref().unwrap_or("");
    let mut candidates: Vec<String> = Vec::new();
    match prop.datatype {
        Datatype::Enum => candidates.extend(format.split(',').map(str::to_string)),
        Datatype::Integer | Datatype::Float => {
            if let Some(min) = format.split(':').next().filter(|s| !s.is_empty()) {
                candidates.push(min.to_string());
            }
            candidates.extend(["0", "0.0", "1"].map(String::from));
        }
        Datatype::Boolean => candidates.push("false".into()),
        Datatype::String => candidates.push("replay".into()),
        _ => {}
    }
    candidates.extend(
        [
            "rgb,0,0,0",
            "hsv,0,0,0",
            "xyz,0.3,0.3",
            "2026-01-01T00:00:00Z",
            "PT0S",
            "[]",
            "{}",
        ]
        .map(String::from),
    );
    candidates
        .into_iter()
        .find(|c| datatype::check_value(c, prop).is_ok())
}

// ===== the broker and the host loop =====

#[derive(Default)]
struct Broker {
    retained: BTreeMap<String, Vec<u8>>,
    subs: BTreeSet<String>,
    queue: VecDeque<(String, Vec<u8>, bool)>,
    online: bool,
}

/// A paced burst as the broker side sees it.
#[derive(Debug)]
struct Burst {
    /// `None`: every node.
    nodes: Option<Vec<String>>,
    /// 2 per retained property in `nodes`.
    expected: u64,
    /// Distinct values and targets of those properties delivered.
    received: BTreeSet<String>,
}

impl Burst {
    fn owed(&self) -> u64 {
        self.expected.saturating_sub(self.received.len() as u64)
    }
}

#[derive(Default, Clone, Copy, Debug)]
struct PacingSeen {
    max_devices: usize,
    max_owed: u64,
    bursts: usize,
    chunked: usize,
}

struct World {
    label: String,
    ctl: Controller,
    pacing: Option<Pacing>,
    broker: Broker,
    now: u64,
    retain_none: bool,
    events: Vec<Event>,
    /// The last `$description` each device published, for the pacing budget.
    descs: BTreeMap<String, Dev>,
    /// Paced bursts in flight, from the SUBSCRIBE until the device's
    /// end-of-burst `$description` is delivered.
    bursts: BTreeMap<String, Burst>,
    seen: PacingSeen,
    /// Values and targets delivered before the device's first `$description`.
    early_values: usize,
    described: BTreeSet<String>,
    faults: Vec<String>,
}

impl World {
    fn new(label: String, cfg: Config, retain_none: bool) -> World {
        let pacing = cfg.pacing;
        World {
            label,
            ctl: Controller::new(cfg).expect("valid config"),
            pacing,
            broker: Broker::default(),
            now: 0,
            retain_none,
            events: Vec::new(),
            descs: BTreeMap::new(),
            bursts: BTreeMap::new(),
            seen: PacingSeen::default(),
            early_values: 0,
            described: BTreeSet::new(),
            faults: Vec::new(),
        }
    }

    fn fault(&mut self, msg: String) {
        self.faults.push(format!("{}: {msg}", self.label));
    }

    /// Puts a device's `$state` `ready`, `$description`, values and targets in
    /// the retained store without delivering anything.
    fn seed(&mut self, id: &str, dev: &Dev, state: bool, desc: bool, values: bool) {
        let store = &mut self.broker.retained;
        if state {
            store.insert(topic(id, "$state"), b"ready".to_vec());
        }
        if desc {
            store.insert(topic(id, "$description"), dev.desc_json.as_bytes().to_vec());
            self.descs.insert(id.to_string(), dev.clone());
        }
        if values {
            for ((n, p), v) in &dev.values {
                store.insert(topic(id, &format!("{n}/{p}")), wire(v));
            }
            for ((n, p), v) in &dev.targets {
                store.insert(topic(id, &format!("{n}/{p}/$target")), wire(v));
            }
        }
    }

    fn seed_tree(&mut self, tree: &Tree) {
        for (id, dev) in &tree.devices {
            self.seed(id, dev, true, true, true);
        }
    }

    /// A device publish: updates the retained store and forwards to matching
    /// subscriptions with the retain bit cleared (MQTT 3.1.1 section 3.3.1.3).
    fn publish(&mut self, t: String, payload: Vec<u8>, retain: bool) {
        if retain {
            if payload.is_empty() {
                self.broker.retained.remove(&t);
            } else {
                self.broker.retained.insert(t.clone(), payload.clone());
            }
        }
        if self.broker.online && self.broker.subs.iter().any(|f| filter::matches(f, &t)) {
            self.broker.queue.push_back((t, payload, false));
        }
    }

    fn publish_desc(&mut self, id: &str, dev: &Dev) {
        self.descs.insert(id.to_string(), dev.clone());
        self.publish(
            topic(id, "$description"),
            dev.desc_json.as_bytes().to_vec(),
            true,
        );
    }

    fn publish_values(&mut self, id: &str, dev: &Dev) {
        for ((n, p), v) in &dev.values {
            self.publish(topic(id, &format!("{n}/{p}")), wire(v), true);
        }
        for ((n, p), v) in &dev.targets {
            self.publish(topic(id, &format!("{n}/{p}/$target")), wire(v), true);
        }
    }

    /// A device coming online: `init`, `$description`, values, `ready`.
    fn device_online(&mut self, id: &str, dev: &Dev) {
        self.publish(topic(id, "$state"), b"init".to_vec(), true);
        self.publish_desc(id, dev);
        self.publish_values(id, dev);
        self.publish(topic(id, "$state"), b"ready".to_vec(), true);
    }

    /// Clean session up.
    fn connect(&mut self) {
        self.broker.online = true;
        self.broker.subs.clear();
        self.broker.queue.clear();
        self.bursts.clear();
        self.ctl.handle_link(
            Instant(self.now),
            Link::Up {
                session_present: false,
            },
        );
        self.pump();
    }

    fn disconnect(&mut self) {
        self.broker.online = false;
        self.broker.subs.clear();
        self.broker.queue.clear();
        self.bursts.clear();
        self.ctl.handle_link(Instant(self.now), Link::Down);
        self.pump();
    }

    /// Hands every queued command to the broker and collects events.
    fn pump(&mut self) {
        while let Some(cmd) = self.ctl.poll_command() {
            if !self.broker.online {
                continue;
            }
            match cmd {
                Command::Subscribe { filters } => {
                    let filters: Vec<String> = filters.into_iter().map(|(f, _)| f).collect();
                    self.track_bursts(&filters);
                    for f in filters {
                        if !filter::is_valid_filter(&f) {
                            self.fault(format!("invalid filter {f:?}"));
                            continue;
                        }
                        let matching: Vec<(String, Vec<u8>)> = self
                            .broker
                            .retained
                            .iter()
                            .filter(|(t, _)| filter::matches(&f, t))
                            .map(|(t, p)| (t.clone(), p.clone()))
                            .collect();
                        self.broker.subs.insert(f);
                        for (t, p) in matching {
                            self.broker.queue.push_back((t, p, true));
                        }
                    }
                }
                Command::Unsubscribe { filters } => {
                    for f in filters {
                        self.broker.subs.remove(&f);
                    }
                }
                Command::Publish { topic, .. } => {
                    self.fault(format!("unexpected publish to {topic}"));
                }
                _ => {}
            }
        }
        while let Some(e) = self.ctl.poll_event() {
            self.events.push(e);
        }
    }

    /// A SUBSCRIBE with a device's property filters followed by its
    /// `$description` starts a paced burst.
    fn track_bursts(&mut self, filters: &[String]) {
        let mut nodes: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        let mut started = Vec::new();
        for f in filters {
            let Some(rest) = f.strip_prefix(DOMAIN_PREFIX) else {
                continue;
            };
            let parts: Vec<&str> = rest.split('/').collect();
            match parts.as_slice() {
                [d, "$description"] if nodes.contains_key(d) => started.push(*d),
                [d, n, "+"] => nodes.entry(d).or_default().push(n.to_string()),
                _ => {}
            }
        }
        for d in started {
            let ns = &nodes[d];
            let whole = ns.iter().any(|n| n == "+");
            let nodes = (!whole).then(|| ns.clone());
            let expected = self
                .descs
                .get(d)
                .map_or(0, |dev| 2 * dev.retained_in(nodes.as_deref()));
            self.seen.bursts += 1;
            if !whole {
                self.seen.chunked += 1;
            }
            self.bursts.insert(
                d.to_string(),
                Burst {
                    nodes,
                    expected,
                    received: BTreeSet::new(),
                },
            );
        }
        let Some(p) = self.pacing else {
            return;
        };
        let devices = self.bursts.len();
        let owed: u64 = self.bursts.values().map(Burst::owed).sum();
        self.seen.max_devices = self.seen.max_devices.max(devices);
        if devices > 1 {
            self.seen.max_owed = self.seen.max_owed.max(owed);
        }
        if devices > p.batch_devices {
            self.fault(format!(
                "{devices} devices in flight, limit {}",
                p.batch_devices
            ));
        }
        if devices > 1 && owed > p.batch_values as u64 {
            self.fault(format!(
                "{owed} values owed by {devices} devices, limit {}",
                p.batch_values
            ));
        }
    }

    fn deliver_one(&mut self) -> bool {
        let Some((t, payload, retain)) = self.broker.queue.pop_front() else {
            return false;
        };
        self.now += 1;
        if let Some(rest) = t.strip_prefix(DOMAIN_PREFIX) {
            let mut parts = rest.split('/');
            let dev = parts.next().unwrap_or("");
            let second = parts.next().unwrap_or("");
            let third = parts.next().unwrap_or("");
            if second == "$description" {
                self.bursts.remove(dev);
                self.described.insert(dev.to_string());
            } else if !second.starts_with('$') {
                if !self.described.contains(dev) {
                    self.early_values += 1;
                }
                let retained = self
                    .descs
                    .get(dev)
                    .and_then(|d| d.decl(second, third))
                    .is_some_and(|p| p.retained);
                if let Some(b) = self.bursts.get_mut(dev) {
                    let covered = b
                        .nodes
                        .as_ref()
                        .is_none_or(|ns| ns.iter().any(|n| n == second));
                    if retained && covered && !payload.is_empty() {
                        b.received.insert(t.clone());
                    }
                }
            }
        }
        let retain = if self.retain_none { None } else { Some(retain) };
        self.ctl.handle_message(
            Instant(self.now),
            Message {
                topic: &t,
                payload: &payload,
                retain,
            },
        );
        self.pump();
        true
    }

    fn fire_timers(&mut self) {
        let mut guard = 0u32;
        while let Some(t) = self.ctl.poll_timeout() {
            if t.0 > self.now {
                return;
            }
            self.ctl.handle_timeout(Instant(self.now));
            self.pump();
            guard += 1;
            assert!(
                guard < 10_000,
                "{}: timer livelock at {}",
                self.label,
                self.now
            );
        }
    }

    /// Delivers and fires timers until `ms` from now.
    fn run_for(&mut self, ms: u64) {
        let end = self.now + ms;
        let mut guard = 0u64;
        loop {
            guard += 1;
            assert!(guard < 5_000_000, "{}: no progress", self.label);
            self.fire_timers();
            if self.deliver_one() {
                continue;
            }
            match self.ctl.poll_timeout() {
                Some(t) if t.0 <= end => self.now = self.now.max(t.0),
                _ => break,
            }
        }
        self.now = self.now.max(end);
    }

    fn count(&self, since: usize, f: impl Fn(&Event) -> bool) -> usize {
        self.events[since..].iter().filter(|e| f(e)).count()
    }

    fn last_synced(&self, since: usize, id: &str) -> Option<u32> {
        self.events[since..].iter().rev().find_map(|e| match e {
            Event::DeviceSynced { device, missing } if device == id => Some(*missing),
            _ => None,
        })
    }

    /// Every device in `ids` is ready, described as `tree` says, holds exactly
    /// the tree's values and targets, and reported `DeviceSynced` since
    /// `since` with the tree's gaps as `missing`.
    fn check_synced(&mut self, tree: &Tree, ids: &BTreeSet<String>, since: usize) {
        let mut faults = Vec::new();
        for id in ids {
            let dev = &tree.devices[id];
            let Some(view) = self.ctl.device(id) else {
                faults.push(format!("{id}: not in the registry"));
                continue;
            };
            if view.state != Some(DeviceState::Ready) {
                faults.push(format!("{id}: state {:?}", view.state));
            }
            if !view.synced || !view.confirmed {
                faults.push(format!(
                    "{id}: synced {} confirmed {}",
                    view.synced, view.confirmed
                ));
            }
            if view.description.as_deref() != Some(&dev.desc) {
                faults.push(format!("{id}: description differs"));
            }
            let mut held = 0usize;
            for (n, p, pv) in view.properties() {
                let key = (n.to_string(), p.to_string());
                let got = pv.value.as_ref().map(|v| &*v.raw);
                if got.is_some() {
                    held += 1;
                }
                if got != dev.values.get(&key).map(String::as_str) {
                    faults.push(format!("{id} {n}/{p}: value {got:?}"));
                }
                let target = pv.target.as_ref().map(|v| &*v.raw);
                if target != dev.targets.get(&key).map(String::as_str) {
                    faults.push(format!("{id} {n}/{p}: target {target:?}"));
                }
            }
            if held != dev.values.len() {
                faults.push(format!(
                    "{id}: {held} values held, {} published",
                    dev.values.len()
                ));
            }
            match self.last_synced(since, id) {
                Some(m) if m == dev.gaps() => {}
                other => faults.push(format!(
                    "{id}: DeviceSynced {other:?}, expected missing {}",
                    dev.gaps()
                )),
            }
        }
        let rejected = self.count(0, |e| matches!(e, Event::DescriptionRejected { .. }));
        if rejected > 0 {
            faults.push(format!("{rejected} DescriptionRejected"));
        }
        for f in faults {
            self.fault(f);
        }
    }

    fn check_quiet(&mut self, since: usize) {
        for (what, n) in [
            (
                "DeviceStuck",
                self.count(since, |e| matches!(e, Event::DeviceStuck { .. })),
            ),
            (
                "DeviceAbsent",
                self.count(since, |e| matches!(e, Event::DeviceAbsent { .. })),
            ),
            (
                "DeviceRemoved",
                self.count(since, |e| matches!(e, Event::DeviceRemoved { .. })),
            ),
        ] {
            if n > 0 {
                self.fault(format!("{n} {what}"));
            }
        }
    }
}

// ===== modes =====

#[derive(Clone, Debug)]
enum Mode {
    Wildcard,
    Unpaced,
    Tree(String),
}

impl Mode {
    fn config(&self) -> Config {
        match self {
            Mode::Wildcard => Config::wildcard(),
            Mode::Unpaced => {
                let mut c = Config::wildcard();
                c.pacing = None;
                c
            }
            Mode::Tree(root) => Config::tree(root).expect("valid root"),
        }
    }

    fn scope(&self, tree: &Tree) -> BTreeSet<String> {
        match self {
            Mode::Tree(root) => tree.subtree(root),
            _ => tree.devices.keys().cloned().collect(),
        }
    }

    fn name(&self) -> String {
        match self {
            Mode::Wildcard => "wildcard".into(),
            Mode::Unpaced => "unpaced".into(),
            Mode::Tree(r) => format!("tree({r})"),
        }
    }
}

fn modes(tree: &Tree) -> Vec<Mode> {
    let mut out = vec![Mode::Wildcard];
    out.extend(tree.roots().into_iter().map(Mode::Tree));
    out
}

fn world(label: &str, mode: &Mode, cfg: Config, retain_none: bool) -> World {
    let retain = if retain_none { "none" } else { "some" };
    World::new(
        format!("{label} {} retain={retain}", mode.name()),
        cfg,
        retain_none,
    )
}

/// A leaf that is not a root, in scope.
fn leaf(tree: &Tree, scope: &BTreeSet<String>) -> Option<String> {
    let roots = tree.roots();
    scope
        .iter()
        .find(|id| !roots.contains(id) && tree.devices[*id].desc.children.is_empty())
        .cloned()
}

// ===== replays =====

/// The retained tree replayed into a fresh controller.
fn replay_retained(label: &str, tree: &Tree, mode: &Mode, cfg: Config, retain_none: bool) -> World {
    let mut w = world(label, mode, cfg, retain_none);
    w.seed_tree(tree);
    w.connect();
    w.run_for(SETTLE_MS);
    let scope = mode.scope(tree);
    w.check_synced(tree, &scope, 0);
    w.check_quiet(0);
    for id in &scope {
        let n = w.count(
            0,
            |e| matches!(e, Event::DeviceSynced { device, .. } if device.as_str() == id.as_str()),
        );
        if n != 1 {
            w.fault(format!("{id}: {n} DeviceSynced"));
        }
    }
    if let Mode::Tree(root) = mode {
        let fired = w.count(
            0,
            |e| matches!(e, Event::TreeComplete { root: r } if r.as_str() == root.as_str()),
        );
        if !w.ctl.is_tree_complete(root) || fired == 0 {
            w.fault(format!("{root}: tree not complete ({fired} events)"));
        }
    }
    if w.ctl.devices().count() != scope.len() {
        let n = w.ctl.devices().count();
        w.fault(format!("{n} devices registered, {} in scope", scope.len()));
    }
    w
}

/// The broker restarts without persistence; devices republish later.
fn broker_restart(label: &str, tree: &Tree, mode: &Mode, retain_none: bool) -> Vec<String> {
    let mut w = replay_retained(label, tree, mode, mode.config(), retain_none);
    let scope = mode.scope(tree);
    let mark = w.events.len();
    w.disconnect();
    w.broker.retained.clear();
    w.now += 1_000;
    w.connect();
    w.run_for(45_000);
    for id in &scope {
        let absent = w.count(
            mark,
            |e| matches!(e, Event::DeviceAbsent { device } if device.as_str() == id.as_str()),
        );
        if absent != 1 {
            w.fault(format!("{id}: {absent} DeviceAbsent"));
        }
        let kept = w
            .ctl
            .device(id)
            .is_some_and(|v| !v.confirmed && v.properties().count() > 0);
        if !kept {
            w.fault(format!("{id}: not kept unconfirmed while absent"));
        }
        let early = w.count(
            mark,
            |e| matches!(e, Event::DeviceSynced { device, .. } if device.as_str() == id.as_str()),
        );
        if early > 0 {
            w.fault(format!("{id}: DeviceSynced before it was heard again"));
        }
    }
    let mark = w.events.len();
    for id in tree.publish_order() {
        w.device_online(&id, &tree.devices[&id]);
    }
    w.run_for(SETTLE_MS);
    w.check_synced(tree, &scope, mark);
    w.faults
}

/// Devices come up after the controller: `ready` and `$description` first,
/// values two seconds later.
fn ready_before_values(label: &str, tree: &Tree, mode: &Mode, retain_none: bool) -> Vec<String> {
    let mut w = world(label, mode, mode.config(), retain_none);
    w.connect();
    w.run_for(1_000);
    let order = tree.publish_order();
    for id in &order {
        w.publish(topic(id, "$state"), b"ready".to_vec(), true);
        w.publish_desc(id, &tree.devices[id]);
    }
    w.run_for(2_000);
    for id in &order {
        w.publish_values(id, &tree.devices[id]);
    }
    w.run_for(SETTLE_MS);
    let scope = mode.scope(tree);
    w.check_synced(tree, &scope, 0);
    w.check_quiet(0);
    w.faults
}

/// The retained store holds `$state` and values; every `$description` is
/// published three seconds after the controller connects.
fn values_before_description(
    label: &str,
    tree: &Tree,
    mode: &Mode,
    retain_none: bool,
) -> Vec<String> {
    let mut w = world(label, mode, mode.config(), retain_none);
    for (id, dev) in &tree.devices {
        w.seed(id, dev, true, false, true);
    }
    w.connect();
    w.run_for(3_000);
    for id in tree.publish_order() {
        w.publish_desc(&id, &tree.devices[&id]);
    }
    w.run_for(SETTLE_MS);
    let scope = mode.scope(tree);
    w.check_synced(tree, &scope, 0);
    w.check_quiet(0);
    if !matches!(mode, Mode::Wildcard) && w.early_values == 0 {
        w.fault("no value arrived before its $description".into());
    }
    w.faults
}

/// A copy of `dev` without its first node and with a node `replay-extra`.
fn changed(dev: &Dev) -> Dev {
    let mut json: Json = serde_json::from_str(&dev.desc_json).expect("description JSON");
    let nodes = json["nodes"].as_object_mut().expect("nodes");
    let dropped = nodes.keys().next().cloned();
    if let Some(k) = &dropped {
        nodes.remove(k);
    }
    nodes.insert(
        "replay-extra".into(),
        json!({
            "name": "Replay extra",
            "properties": {
                "count": { "datatype": "integer", "format": "0:100" },
                "label": { "datatype": "string" }
            }
        }),
    );
    if let Some(v) = json.get("version").and_then(Json::as_i64) {
        json["version"] = json!(v + 1);
    }
    let mut out = Dev::new(json.to_string()).expect("changed description parses");
    out.values = dev
        .values
        .iter()
        .filter(|((n, _), _)| Some(n) != dropped.as_ref())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    out.values
        .insert(("replay-extra".into(), "count".into()), "7".into());
    out.values
        .insert(("replay-extra".into(), "label".into()), "added".into());
    out
}

/// A repeated identical `$description` changes nothing; a changed one while
/// `ready` is announced once and its new node is paced in.
fn description_repeat(label: &str, tree: &Tree, mode: &Mode, retain_none: bool) -> Vec<String> {
    let mut w = replay_retained(label, tree, mode, mode.config(), retain_none);
    let scope = mode.scope(tree);
    let Some(id) = leaf(tree, &scope) else {
        return w.faults;
    };
    let mark = w.events.len();
    let dev = tree.devices[&id].clone();
    w.publish_desc(&id, &dev);
    w.run_for(5_000);
    let noise = w.count(mark, |e| match e {
        Event::DescriptionChanged { device, .. } | Event::DeviceSynced { device, .. } => {
            device.as_str() == id.as_str()
        }
        _ => false,
    });
    if noise > 0 {
        w.fault(format!(
            "{id}: {noise} events after an identical $description"
        ));
    }
    let mark = w.events.len();
    let new = changed(&dev);
    w.publish_desc(&id, &new);
    for ((n, p), v) in &new.values {
        if n == "replay-extra" {
            w.publish(topic(&id, &format!("{n}/{p}")), wire(v), true);
        }
    }
    w.run_for(SETTLE_MS);
    let announced: Vec<bool> = w.events[mark..]
        .iter()
        .filter_map(|e| match e {
            Event::DescriptionChanged {
                device,
                while_ready,
                ..
            } if device.as_str() == id.as_str() => Some(*while_ready),
            _ => None,
        })
        .collect();
    if announced != [true] {
        w.fault(format!(
            "{id}: DescriptionChanged while_ready {announced:?}"
        ));
    }
    let mut after = tree.clone();
    after.devices.insert(id.clone(), new);
    w.check_synced(&after, &BTreeSet::from([id.clone()]), mark);
    let others: BTreeSet<String> = scope.into_iter().filter(|d| *d != id).collect();
    w.check_synced(tree, &others, 0);
    w.check_quiet(0);
    w.faults
}

/// A leaf publishes a zero-length `$state`, clears its topics, and returns.
fn removal_and_return(label: &str, tree: &Tree, mode: &Mode, retain_none: bool) -> Vec<String> {
    let mut w = replay_retained(label, tree, mode, mode.config(), retain_none);
    let scope = mode.scope(tree);
    let Some(id) = leaf(tree, &scope) else {
        return w.faults;
    };
    let dev = tree.devices[&id].clone();
    let mark = w.events.len();
    w.publish(topic(&id, "$state"), Vec::new(), true);
    w.publish(topic(&id, "$description"), Vec::new(), true);
    for (n, p) in dev.values.keys() {
        w.publish(topic(&id, &format!("{n}/{p}")), Vec::new(), true);
    }
    w.run_for(5_000);
    let removed: Vec<String> = w.events[mark..]
        .iter()
        .filter_map(|e| match e {
            Event::DeviceRemoved { device } => Some(device.to_string()),
            _ => None,
        })
        .collect();
    if removed != [id.clone()] {
        w.fault(format!("removed {removed:?}, expected [{id}]"));
    }
    if w.ctl.device(&id).is_some() {
        w.fault(format!("{id}: still in the registry after removal"));
    }
    let mark = w.events.len();
    w.device_online(&id, &dev);
    w.run_for(SETTLE_MS);
    let found = w.count(
        mark,
        |e| matches!(e, Event::DeviceDiscovered { device, .. } if device.as_str() == id.as_str()),
    );
    if found != 1 {
        w.fault(format!("{id}: {found} DeviceDiscovered on return"));
    }
    w.check_synced(tree, &BTreeSet::from([id.clone()]), mark);
    w.check_synced(tree, &scope, 0);
    w.faults
}

/// While the controller is disconnected, the first value of every device in
/// scope is deleted with its `$target`. With the retain bit the reconnect
/// clears exactly those and each device syncs with them missing; without it
/// nothing is cleared.
fn deleted_while_disconnected(
    label: &str,
    tree: &Tree,
    mode: &Mode,
    retain_none: bool,
) -> Vec<String> {
    let mut w = replay_retained(label, tree, mode, mode.config(), retain_none);
    let scope = mode.scope(tree);
    w.disconnect();
    let mut after = tree.clone();
    let mut deleted = BTreeSet::new();
    for id in &scope {
        let Some(dev) = after.devices.get_mut(id) else {
            continue;
        };
        let Some((n, p)) = dev.values.keys().next().cloned() else {
            continue;
        };
        dev.values.remove(&(n.clone(), p.clone()));
        dev.targets.remove(&(n.clone(), p.clone()));
        w.broker.retained.remove(&topic(id, &format!("{n}/{p}")));
        w.broker
            .retained
            .remove(&topic(id, &format!("{n}/{p}/$target")));
        deleted.insert((id.clone(), n, p));
    }
    let mark = w.events.len();
    w.now += 1_000;
    w.connect();
    w.run_for(SETTLE_MS);
    let mut cleared: Vec<(String, String, String)> = w.events[mark..]
        .iter()
        .filter_map(|e| match e {
            Event::PropertyCleared { addr } => Some((
                addr.device.to_string(),
                addr.node.to_string(),
                addr.property.to_string(),
            )),
            _ => None,
        })
        .collect();
    cleared.sort();
    let expected: Vec<(String, String, String)> = if retain_none {
        Vec::new()
    } else {
        deleted.into_iter().collect()
    };
    if cleared != expected {
        w.fault(format!("cleared {cleared:?}, expected {expected:?}"));
    }
    if retain_none {
        w.check_synced(tree, &scope, mark);
    } else {
        w.check_synced(&after, &scope, mark);
    }
    w.check_quiet(mark);
    w.faults
}

fn run_baselines(label: &str, tree: &Tree, faults: &mut Vec<String>) -> PacingSeen {
    let mut seen = PacingSeen::default();
    for mode in modes(tree) {
        for retain_none in [false, true] {
            let w = replay_retained(label, tree, &mode, mode.config(), retain_none);
            seen.max_devices = seen.max_devices.max(w.seen.max_devices);
            seen.max_owed = seen.max_owed.max(w.seen.max_owed);
            seen.bursts += w.seen.bursts;
            seen.chunked += w.seen.chunked;
            faults.extend(w.faults);
        }
    }
    seen
}

fn vendored() -> Vec<(String, Tree)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/panel-sim");
    let mut files = Vec::new();
    tree_files(&dir, "-tree-v1.json", &mut files);
    files.sort();
    assert!(!files.is_empty(), "no captures in {}", dir.display());
    files
        .iter()
        .map(|p| {
            let name = p
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let tree = Tree::load(p).unwrap_or_else(|e| panic!("{name}: {e}"));
            (name, tree)
        })
        .collect()
}

fn tree_files(dir: &Path, suffix: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            tree_files(&path, suffix, out);
        } else if path.to_string_lossy().ends_with(suffix) {
            out.push(path);
        }
    }
}

fn assert_none(what: &str, faults: &[String]) {
    assert!(
        faults.is_empty(),
        "{what}: {} faults:\n{}",
        faults.len(),
        faults.join("\n")
    );
}

// ===== tests =====

/// Every vendored capture with every declared retained property given a value:
/// each device syncs with `missing: 0`.
#[test]
fn captures_replay_complete() {
    let mut faults = Vec::new();
    for (name, tree) in vendored() {
        let tree = tree.filled().unwrap_or_else(|e| panic!("{name}: {e}"));
        let seen = run_baselines(&name, &tree, &mut faults);
        let n = tree.devices.len();
        if n > 8 && seen.max_devices != 8 {
            faults.push(format!(
                "{name}: at most {} devices in flight",
                seen.max_devices
            ));
        }
        eprintln!("{name}: {n} devices, pacing {seen:?}");
    }
    assert_none("complete captures", &faults);
}

/// Every vendored capture as published: devices sync once the sync timeout
/// passes, with `missing` equal to the properties the capture has no value for.
#[test]
fn captures_replay_as_captured() {
    let mut faults = Vec::new();
    for (name, tree) in vendored() {
        let gaps: u32 = tree.devices.values().map(Dev::gaps).sum();
        run_baselines(&name, &tree, &mut faults);
        eprintln!(
            "{name}: {} devices, {gaps} properties without a value",
            tree.devices.len()
        );
    }
    assert_none("captures as published", &faults);
}

type Scenario = fn(&str, &Tree, &Mode, bool) -> Vec<String>;

fn run_scenario(scenario: Scenario, extra: &[Mode]) {
    run_scenario_in(scenario, extra, true);
}

fn run_scenario_in(scenario: Scenario, extra: &[Mode], tree_modes: bool) {
    let mut faults = Vec::new();
    for (name, tree) in vendored() {
        let tree = tree.filled().unwrap_or_else(|e| panic!("{name}: {e}"));
        let mut all = modes(&tree);
        if !tree_modes {
            all.retain(|m| !matches!(m, Mode::Tree(_)));
        }
        all.extend(extra.iter().cloned());
        for mode in &all {
            for retain_none in [false, true] {
                faults.extend(scenario(&name, &tree, mode, retain_none));
            }
        }
    }
    assert_none("scenario", &faults);
}

#[test]
fn broker_restart_with_empty_retained_store() {
    run_scenario_in(broker_restart, &[Mode::Unpaced], false);
}

/// The children too stay unconfirmed and unsynced until each is heard again.
#[test]
fn broker_restart_with_empty_retained_store_tree_mode() {
    let mut faults = Vec::new();
    for (name, tree) in vendored() {
        let tree = tree.filled().unwrap_or_else(|e| panic!("{name}: {e}"));
        for root in tree.roots() {
            for retain_none in [false, true] {
                faults.extend(broker_restart(
                    &name,
                    &tree,
                    &Mode::Tree(root.clone()),
                    retain_none,
                ));
            }
        }
    }
    assert_none("tree-mode broker restart", &faults);
}

#[test]
fn ready_before_values_arrive() {
    run_scenario(ready_before_values, &[]);
}

#[test]
fn description_repeated_and_changed() {
    run_scenario(description_repeat, &[]);
}

#[test]
fn values_before_description_arrive() {
    run_scenario(values_before_description, &[Mode::Unpaced]);
}

#[test]
fn device_removal_and_return() {
    run_scenario(removal_and_return, &[]);
}

#[test]
fn values_deleted_while_disconnected_are_cleared() {
    run_scenario(deleted_while_disconnected, &[Mode::Unpaced]);
}

/// A root with 49 children, 30 properties each in 3 nodes, some with a
/// `$target`.
fn synthetic(devices: usize, props: usize) -> Tree {
    let root = "replay-root".to_string();
    let ids: Vec<String> = (1..devices).map(|i| format!("replay-dev-{i:02}")).collect();
    let mut tree = Tree::default();
    let kinds = [
        (json!({ "datatype": "integer", "format": "0:1000" }), "42"),
        (json!({ "datatype": "float", "unit": "W" }), "12.5"),
        (json!({ "datatype": "boolean" }), "true"),
        (
            json!({ "datatype": "enum", "format": "low,mid,high" }),
            "mid",
        ),
        (json!({ "datatype": "string" }), "text"),
    ];
    let mut all = vec![root.clone()];
    all.extend(ids.iter().cloned());
    for id in &all {
        let mut nodes = serde_json::Map::new();
        let mut values = BTreeMap::new();
        let mut targets = BTreeMap::new();
        for n in 0..3 {
            let node = format!("node-{n}");
            let mut properties = serde_json::Map::new();
            for p in (0..props).filter(|p| p % 3 == n) {
                let (decl, value) = &kinds[p % kinds.len()];
                let mut decl = decl.clone();
                let prop = format!("prop-{p:02}");
                if p % 7 == 0 {
                    decl["settable"] = json!(true);
                    targets.insert((node.clone(), prop.clone()), value.to_string());
                }
                properties.insert(prop.clone(), decl);
                values.insert((node.clone(), prop), value.to_string());
            }
            nodes.insert(node, json!({ "properties": properties }));
        }
        let mut desc = json!({ "homie": "5.0", "version": 1, "nodes": nodes });
        if *id == root {
            desc["children"] = json!(ids);
        } else {
            desc["parent"] = json!(root);
            desc["root"] = json!(root);
        }
        let mut dev = Dev::new(desc.to_string()).expect("synthetic description");
        dev.values = values;
        dev.targets = targets;
        tree.devices.insert(id.clone(), dev);
    }
    tree
}

#[test]
fn synthetic_50_by_30_tree() {
    let tree = synthetic(50, 30);
    assert_eq!(tree.devices.len(), 50);
    assert!(tree.devices.values().all(|d| d.retained_in(None) == 30));
    let mut faults = Vec::new();
    let seen = run_baselines("synthetic", &tree, &mut faults);
    if seen.max_devices != 8 {
        faults.push(format!("at most {} devices in flight", seen.max_devices));
    }
    let mut tight = Pacing::default();
    tight.batch_devices = 8;
    tight.batch_values = 50;
    for mode in modes(&tree) {
        for retain_none in [false, true] {
            let mut cfg = mode.config();
            cfg.pacing = Some(tight);
            let w = replay_retained("synthetic tight", &tree, &mode, cfg, retain_none);
            if w.seen.chunked == 0 {
                faults.push(format!("{}: no node-by-node subscribe", w.label));
            }
            faults.extend(w.faults);
        }
    }
    eprintln!("synthetic: pacing {seen:?}");
    assert_none("synthetic", &faults);
}

/// `EBUS_CAPTURE_CORPUS`: the complete replay over every `*.tree.json` below
/// it. Only counts are printed.
#[test]
fn local_capture_corpus_replay() {
    let Some(dir) = std::env::var_os("EBUS_CAPTURE_CORPUS") else {
        eprintln!("EBUS_CAPTURE_CORPUS not set; skipped");
        return;
    };
    let mut files = Vec::new();
    tree_files(Path::new(&dir), ".tree.json", &mut files);
    files.sort();
    let (mut devices, mut unreadable, mut faults) = (0usize, 0usize, 0usize);
    let (mut skipped_devices, mut skipped_values) = (0usize, 0usize);
    let mut failed_files = 0usize;
    for (i, path) in files.iter().enumerate() {
        let tree = match Tree::load(path).and_then(|t| t.filled()) {
            Ok(t) => t,
            Err(_) => {
                unreadable += 1;
                continue;
            }
        };
        devices += tree.devices.len();
        skipped_devices += tree.skipped_devices;
        skipped_values += tree.skipped_values;
        let mut f = Vec::new();
        run_baselines(&format!("corpus file {i}"), &tree, &mut f);
        if !f.is_empty() {
            failed_files += 1;
            faults += f.len();
        }
    }
    eprintln!(
        "local corpus: {} files, {unreadable} unreadable, {devices} devices, \
         {skipped_devices} devices without a description, {skipped_values} values skipped, \
         {failed_files} files with {faults} faults",
        files.len()
    );
    assert!(!files.is_empty(), "local corpus: no *.tree.json files");
    assert_eq!(failed_files, 0, "local corpus: {faults} faults");
}
