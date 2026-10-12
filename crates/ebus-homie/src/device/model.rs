//! The tree model: devices, nodes and property slots, their specs, and the
//! one-commit [`Reconfigure`] that changes them.

use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet, VecDeque},
    format,
    string::{String, ToString},
    vec::Vec,
};
use core::fmt;

use serde_json::Value;

use crate::Id;
use crate::datatype::{Datatype, Decimal, NumericFormat, Problem};
use crate::description::{
    self, CORE_FIELDS, DeviceDescription, Ignored, NodeDescription, PropertyDescription,
};
use crate::state::DeviceState;

use super::value::HomieValue;

/// A device in a [`Tree`](super::Tree). Indices are never reused, so a
/// removed device's index stays dead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DevIdx(pub(super) u16);

impl DevIdx {
    /// The index.
    pub fn index(self) -> u16 {
        self.0
    }
}

/// A node in a [`Tree`](super::Tree). Indices are never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeIdx(pub(super) u32);

impl NodeIdx {
    /// The index.
    pub fn index(self) -> u32 {
        self.0
    }
}

/// A property slot. A removed slot's index is reused with a new generation,
/// so the old id returns `Gone`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlotId {
    pub(super) index: u32,
    pub(super) generation: u32,
}

impl SlotId {
    /// The slot's index, dense from 0, for a runtime's per-slot bitset.
    pub fn index(self) -> u32 {
        self.index
    }
}

/// A device's own `$description` fields.
///
/// Built from [`DeviceSpec::typed`] or `DeviceSpec::default()` and the
/// chained setters outside this crate.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct DeviceSpec {
    /// Friendly name; the id when `None`, as python-sdk writes it.
    pub name: Option<String>,
    /// Device type.
    pub r#type: Option<String>,
    /// Extension ids (convention.md: Extensions), such as `org.example.feature`.
    pub extensions: Vec<String>,
    /// Extension-defined top-level fields; a core field name is refused.
    pub extras: Vec<(String, Value)>,
}

impl DeviceSpec {
    /// A spec with only a type.
    pub fn typed(r#type: &str) -> Self {
        DeviceSpec {
            r#type: Some(r#type.to_string()),
            ..DeviceSpec::default()
        }
    }

    /// With a friendly name.
    #[must_use]
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// With one more extension id.
    #[must_use]
    pub fn extension(mut self, id: &str) -> Self {
        self.extensions.push(id.to_string());
        self
    }

    /// With one more extension-defined top-level field.
    #[must_use]
    pub fn extra(mut self, key: &str, value: Value) -> Self {
        self.extras.push((key.to_string(), value));
        self
    }
}

/// A node's own `$description` fields.
///
/// Built from [`NodeSpec::typed`] or `NodeSpec::default()` and the chained
/// setters outside this crate.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeSpec {
    /// Friendly name; the id when `None`.
    pub name: Option<String>,
    /// Node type.
    pub r#type: Option<String>,
}

impl NodeSpec {
    /// A spec with only a type.
    pub fn typed(r#type: &str) -> Self {
        NodeSpec {
            name: None,
            r#type: Some(r#type.to_string()),
        }
    }

    /// With a friendly name.
    #[must_use]
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }
}

/// A property's `$description` fields and publishing options.
///
/// Built from [`PropSpec::new`] and the chained setters outside this crate.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PropSpec {
    /// Friendly name; the id when `None`.
    pub name: Option<String>,
    /// `$format`, intersected with the range a narrow integer type implies.
    pub format: Option<String>,
    /// Unit.
    pub unit: Option<String>,
    /// Accepts `/set`.
    pub settable: bool,
    /// Values are retained (default); `false` makes the property an event.
    pub retained: bool,
    /// Publishes `$target` on every update (convention.md:333).
    pub target: bool,
}

impl Default for PropSpec {
    fn default() -> Self {
        PropSpec::new()
    }
}

impl PropSpec {
    /// Retained, not settable, no `$target`, no name, format or unit.
    pub fn new() -> Self {
        PropSpec {
            name: None,
            format: None,
            unit: None,
            settable: false,
            retained: true,
            target: false,
        }
    }

    /// With a friendly name.
    #[must_use]
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// With a `$format`.
    #[must_use]
    pub fn format(mut self, format: &str) -> Self {
        self.format = Some(format.to_string());
        self
    }

    /// With a unit.
    #[must_use]
    pub fn unit(mut self, unit: &str) -> Self {
        self.unit = Some(unit.to_string());
        self
    }

    /// Settable or not.
    #[must_use]
    pub fn settable(mut self, settable: bool) -> Self {
        self.settable = settable;
        self
    }

    /// Retained or an event.
    #[must_use]
    pub fn retained(mut self, retained: bool) -> Self {
        self.retained = retained;
        self
    }

    /// With or without `$target`.
    #[must_use]
    pub fn target(mut self, target: bool) -> Self {
        self.target = target;
        self
    }
}

/// Why a tree or a reconfiguration was refused; a refused commit changes
/// nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BuildError {
    /// Not a valid Homie id, or the root given where it cannot be.
    InvalidId,
    /// The id is already in use: a device in the tree, a node in the device,
    /// a property in the node.
    DuplicateId,
    /// A node id with a `.`.
    NodeIdHasDot,
    /// An `enum` or `color` property needs a `$format`.
    FormatRequired,
    /// The `$format` cannot hold the type's values.
    InvalidFormat(Problem),
    /// The description's round trip left this node or property out.
    Ignored(Ignored),
    /// A device extra uses a core field name.
    CoreKeyInExtras,
    /// An extension id that is not `reverse.domain.suffix`.
    InvalidExtension,
    /// The `$description` is over `TreeConfig::max_description_bytes`.
    DescriptionTooLarge {
        /// Its size.
        bytes: usize,
    },
    /// The device, node or property was removed.
    Gone,
    /// No index is left for another device or node.
    Capacity,
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::InvalidId => f.write_str("invalidId"),
            BuildError::DuplicateId => f.write_str("duplicateId"),
            BuildError::NodeIdHasDot => f.write_str("nodeIdHasDot"),
            BuildError::FormatRequired => f.write_str("formatRequired"),
            BuildError::InvalidFormat(p) => write!(f, "invalidFormat:{p}"),
            BuildError::Ignored(i) => write!(f, "ignored:{}", i.error),
            BuildError::CoreKeyInExtras => f.write_str("coreKeyInExtras"),
            BuildError::InvalidExtension => f.write_str("invalidExtension"),
            BuildError::DescriptionTooLarge { bytes } => write!(f, "descriptionTooLarge:{bytes}"),
            BuildError::Gone => f.write_str("gone"),
            BuildError::Capacity => f.write_str("capacity"),
        }
    }
}

impl core::error::Error for BuildError {}

/// What a slot's value compares by before it is encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fp {
    Int(i64),
    F64(u64),
    F32(u32),
    Bool(bool),
    Enum(u16),
    Dur(u64, u32),
    /// Compared by the encoded payload.
    Bytes,
}

#[derive(Clone, Debug)]
pub(super) struct Slot {
    pub dev: u16,
    pub node: u32,
    pub id: Id,
    pub datatype: Datatype,
    /// The format a typed slot's Rust type implies.
    pub implied: Option<String>,
    pub spec: PropSpec,
    pub numeric: NumericFormat,
    pub imin: Option<i64>,
    pub imax: Option<i64>,
    pub fmin: Option<(f64, Decimal)>,
    pub fmax: Option<(f64, Decimal)>,
    pub variants: Vec<String>,
    pub topic: String,
    pub target_topic: Option<String>,
    /// Fingerprint of the value held; `None` when there is none.
    pub fp: Option<Fp>,
    pub payload: String,
    /// The payload last polled for publishing, when `sent_valid`.
    pub sent: String,
    pub sent_valid: bool,
    pub target: Option<Vec<u8>>,
    /// A `$target` transition is open.
    pub open: bool,
    pub value_ticket: u64,
    pub target_ticket: u64,
}

#[derive(Clone, Debug, Default)]
pub(super) struct Cell {
    pub generation: u32,
    pub slot: Option<Box<Slot>>,
}

#[derive(Clone, Debug)]
pub(super) struct Node {
    pub dev: u16,
    pub id: Id,
    pub spec: NodeSpec,
    pub slots: Vec<u32>,
}

#[derive(Clone, Debug)]
pub(super) struct Dev {
    pub id: Id,
    pub parent: Option<u16>,
    pub children: Vec<u16>,
    pub spec: DeviceSpec,
    pub nodes: Vec<u32>,
    pub state: DeviceState,
    pub alerts: BTreeMap<String, String>,
    pub alert_tickets: BTreeMap<String, u64>,
    pub desc: String,
    pub hash: Option<[u8; 32]>,
    /// (node, property) to slot index, sorted.
    pub routes: Vec<(Id, Id, u32)>,
    pub state_topic: String,
    pub desc_topic: String,
    pub set_filter: String,
    /// Content hash of the description last published, as far as known.
    pub memo: Option<[u8; 32]>,
    /// Hashes of `$description` publishes polled and not yet reported written.
    pub desc_inflight: VecDeque<[u8; 32]>,
    /// An `init` or a changed `$description` was polled and the closing
    /// `$state` was not.
    pub awaiting_close: bool,
    pub init_open: bool,
    pub published_state: Option<DeviceState>,
    pub state_ticket: u64,
}

/// What a removed slot leaves on the broker.
#[derive(Clone, Debug)]
pub(super) struct RemovedSlot {
    pub dev: u16,
    pub clears: Vec<String>,
}

/// A removed device and the topics it leaves, in clearing order.
#[derive(Clone, Debug)]
pub(super) struct RemovedDev {
    pub set_filter: String,
    pub clears: Vec<String>,
}

#[derive(Clone, Debug)]
pub(super) struct Model {
    pub prefix: String,
    pub root: u16,
    pub devices: Vec<Option<Dev>>,
    pub nodes: Vec<Option<Node>>,
    pub cells: Vec<Cell>,
    pub free: Vec<u32>,
    pub by_id: BTreeMap<Id, u16>,
    // One commit's changes.
    pub touched: BTreeSet<u16>,
    pub added: Vec<u16>,
    pub removed: Vec<RemovedDev>,
    pub removed_slots: Vec<RemovedSlot>,
}

/// A valid extension id: dot-separated segments of `a-z`, `0-9` and `-`,
/// at least two (convention.md: Extensions).
fn is_extension_id(s: &str) -> bool {
    s.split('.').count() >= 2 && s.split('.').all(crate::id::is_valid_id)
}

fn check_spec(spec: &DeviceSpec) -> Result<(), BuildError> {
    if spec
        .extras
        .iter()
        .any(|(k, _)| CORE_FIELDS.contains(&k.as_str()))
    {
        return Err(BuildError::CoreKeyInExtras);
    }
    if !spec.extensions.iter().all(|e| is_extension_id(e)) {
        return Err(BuildError::InvalidExtension);
    }
    Ok(())
}

fn valid_id(id: &str) -> Result<Id, BuildError> {
    Id::new(id).ok_or(BuildError::InvalidId)
}

/// The intersection of a declared integer `$format` with the range a narrow
/// integer type implies. A declared format that is not `[min]:[max][:step]`
/// with integer bounds is kept as given, for the round trip to judge.
fn intersect(declared: Option<&str>, implied: Option<&str>) -> Result<Option<String>, BuildError> {
    let (declared, implied) = match (declared.filter(|d| !d.is_empty()), implied) {
        (d, None) => return Ok(d.map(String::from)),
        (None, Some(i)) => return Ok(Some(i.to_string())),
        (Some(d), Some(i)) => (d, i),
    };
    let bounds = |f: &str| -> Option<(Option<i64>, Option<i64>, Option<String>)> {
        let mut parts = f.split(':');
        let lo = parts.next()?;
        let hi = parts.next()?;
        let step = parts.next().map(String::from);
        if parts.next().is_some() {
            return None;
        }
        let parse = |s: &str| -> Option<Option<i64>> {
            if s.is_empty() {
                Some(None)
            } else {
                s.parse().ok().map(Some)
            }
        };
        Some((parse(lo)?, parse(hi)?, step))
    };
    let (Some((dlo, dhi, step)), Some((Some(ilo), Some(ihi), _))) =
        (bounds(declared), bounds(implied))
    else {
        return Ok(Some(declared.to_string()));
    };
    let lo = dlo.map_or(ilo, |d| d.max(ilo));
    let hi = dhi.map_or(ihi, |d| d.min(ihi));
    if lo > hi {
        return Err(BuildError::InvalidFormat(if dlo.is_some_and(|d| d > ihi) {
            Problem::AboveMax
        } else {
            Problem::BelowMin
        }));
    }
    Ok(Some(match step {
        Some(step) => format!("{lo}:{hi}:{step}"),
        None => format!("{lo}:{hi}"),
    }))
}

impl Slot {
    /// Recomputes everything derived from the effective `$format`.
    pub fn apply_format(&mut self) {
        let format = self.spec.format.as_deref();
        self.numeric = match self.datatype {
            Datatype::Integer | Datatype::Float => NumericFormat::parse(format),
            _ => NumericFormat::default(),
        };
        self.imin = None;
        self.imax = None;
        self.fmin = None;
        self.fmax = None;
        match self.datatype {
            Datatype::Integer => {
                // Integer bounds are integer payloads (convention.md: Formats),
                // read exactly; a format that does not parse imposes none.
                if self.numeric != NumericFormat::default() {
                    let mut parts = format.unwrap_or("").split(':');
                    let exact = |s: Option<&str>| s.and_then(|s| s.parse::<i64>().ok());
                    self.imin = exact(parts.next());
                    self.imax = exact(parts.next());
                }
            }
            Datatype::Float => {
                self.fmin = self.numeric.min.clone().map(|d| (d.to_f64(), d));
                self.fmax = self.numeric.max.clone().map(|d| (d.to_f64(), d));
            }
            _ => {}
        }
        self.variants = match self.datatype {
            Datatype::Enum => format
                .unwrap_or("")
                .split(',')
                .filter(|v| !v.is_empty())
                .map(String::from)
                .collect(),
            _ => Vec::new(),
        };
    }

    fn describe(&self) -> PropertyDescription {
        PropertyDescription {
            name: Some(
                self.spec
                    .name
                    .clone()
                    .unwrap_or_else(|| self.id.to_string()),
            ),
            datatype: self.datatype,
            format: self.spec.format.clone(),
            settable: self.spec.settable,
            retained: self.spec.retained,
            unit: self.spec.unit.clone(),
        }
    }

    /// The topics this slot leaves on the broker.
    pub fn clears(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.fp.is_some() {
            out.push(self.topic.clone());
        }
        if let (Some(t), Some(_)) = (&self.target_topic, &self.target) {
            out.push(t.clone());
        }
        out
    }
}

impl Model {
    pub fn new(prefix: String, root_id: &str, spec: DeviceSpec) -> Result<Self, BuildError> {
        let mut m = Model {
            prefix,
            root: 0,
            devices: Vec::new(),
            nodes: Vec::new(),
            cells: Vec::new(),
            free: Vec::new(),
            by_id: BTreeMap::new(),
            touched: BTreeSet::new(),
            added: Vec::new(),
            removed: Vec::new(),
            removed_slots: Vec::new(),
        };
        m.insert_device(None, root_id, spec)?;
        Ok(m)
    }

    pub fn dev(&self, d: u16) -> Option<&Dev> {
        self.devices.get(usize::from(d))?.as_ref()
    }

    pub fn dev_mut(&mut self, d: u16) -> Option<&mut Dev> {
        self.devices.get_mut(usize::from(d))?.as_mut()
    }

    pub fn slot(&self, id: SlotId) -> Option<&Slot> {
        let cell = self.cells.get(id.index as usize)?;
        (cell.generation == id.generation).then_some(())?;
        cell.slot.as_deref()
    }

    pub fn slot_mut(&mut self, id: SlotId) -> Option<&mut Slot> {
        let cell = self.cells.get_mut(id.index as usize)?;
        (cell.generation == id.generation).then_some(())?;
        cell.slot.as_deref_mut()
    }

    pub fn slot_at(&self, index: u32) -> Option<&Slot> {
        self.cells.get(index as usize)?.slot.as_deref()
    }

    pub fn slot_at_mut(&mut self, index: u32) -> Option<&mut Slot> {
        self.cells.get_mut(index as usize)?.slot.as_deref_mut()
    }

    pub fn slot_id(&self, index: u32) -> SlotId {
        SlotId {
            index,
            generation: self.cells.get(index as usize).map_or(0, |c| c.generation),
        }
    }

    /// Devices in pre-order, root first.
    pub fn preorder(&self) -> Vec<u16> {
        let mut out = Vec::new();
        let mut stack = alloc::vec![self.root];
        while let Some(d) = stack.pop() {
            if let Some(dev) = self.dev(d) {
                out.push(d);
                stack.extend(dev.children.iter().rev());
            }
        }
        out
    }

    /// Devices in post-order, root last.
    pub fn postorder(&self) -> Vec<u16> {
        self.subtree_postorder(self.root)
    }

    pub fn subtree_postorder(&self, top: u16) -> Vec<u16> {
        let mut out = Vec::new();
        let mut stack = alloc::vec![(top, false)];
        while let Some((d, expanded)) = stack.pop() {
            let Some(dev) = self.dev(d) else { continue };
            if expanded {
                out.push(d);
            } else {
                stack.push((d, true));
                stack.extend(dev.children.iter().rev().map(|c| (*c, false)));
            }
        }
        out
    }

    /// Slot indices of a device, in node then property order.
    pub fn dev_slots(&self, d: u16) -> Vec<u32> {
        let Some(dev) = self.dev(d) else {
            return Vec::new();
        };
        dev.nodes
            .iter()
            .filter_map(|n| self.nodes.get(*n as usize)?.as_ref())
            .flat_map(|n| n.slots.iter().copied())
            .collect()
    }

    fn insert_device(
        &mut self,
        parent: Option<u16>,
        id: &str,
        spec: DeviceSpec,
    ) -> Result<u16, BuildError> {
        let id = valid_id(id)?;
        if self.by_id.contains_key(id.as_str()) {
            return Err(BuildError::DuplicateId);
        }
        check_spec(&spec)?;
        let d = u16::try_from(self.devices.len()).map_err(|_| BuildError::Capacity)?;
        let p = &self.prefix;
        let dev = Dev {
            state_topic: format!("{p}{id}/$state"),
            desc_topic: format!("{p}{id}/$description"),
            set_filter: format!("{p}{id}/+/+/set"),
            id: id.clone(),
            parent,
            children: Vec::new(),
            spec,
            nodes: Vec::new(),
            state: DeviceState::Ready,
            alerts: BTreeMap::new(),
            alert_tickets: BTreeMap::new(),
            desc: String::new(),
            hash: None,
            routes: Vec::new(),
            memo: None,
            desc_inflight: VecDeque::new(),
            awaiting_close: false,
            init_open: false,
            published_state: None,
            state_ticket: 0,
        };
        self.devices.push(Some(dev));
        self.by_id.insert(id, d);
        if let Some(parent) = parent.and_then(|p| self.dev_mut(p)) {
            parent.children.push(d);
        }
        self.touched.insert(d);
        if let Some(p) = parent {
            self.touched.insert(p);
        }
        self.added.push(d);
        Ok(d)
    }

    pub fn begin(&mut self) {
        self.touched.clear();
        self.added.clear();
        self.removed.clear();
        self.removed_slots.clear();
    }

    /// Rebuilds `d`'s description; true when its content changed.
    pub fn rebuild(&mut self, d: u16, max_bytes: usize) -> Result<bool, BuildError> {
        let Some(dev) = self.dev(d) else {
            return Ok(false);
        };
        let root_id = self.dev(self.root).map(|r| r.id.to_string());
        let nodes = dev
            .nodes
            .iter()
            .filter_map(|n| self.nodes.get(*n as usize)?.as_ref())
            .map(|n| {
                let properties = n
                    .slots
                    .iter()
                    .filter_map(|s| self.slot_at(*s))
                    .map(|s| (s.id.to_string(), s.describe()))
                    .collect();
                let node = NodeDescription {
                    name: Some(n.spec.name.clone().unwrap_or_else(|| n.id.to_string())),
                    r#type: n.spec.r#type.clone(),
                    properties,
                };
                (n.id.to_string(), node)
            })
            .collect();
        let children = dev
            .children
            .iter()
            .filter_map(|c| self.dev(*c))
            .map(|c| c.id.to_string())
            .collect();
        let parent = dev
            .parent
            .and_then(|p| self.dev(p))
            .map(|p| p.id.to_string());
        let doc = DeviceDescription {
            version: 0,
            r#type: dev.spec.r#type.clone(),
            name: Some(dev.spec.name.clone().unwrap_or_else(|| dev.id.to_string())),
            nodes,
            children,
            root: parent.as_ref().and(root_id),
            parent,
            extensions: dev.spec.extensions.clone(),
            extras: dev.spec.extras.clone(),
        }
        .with_computed_version();
        let json = doc.to_json();
        if json.len() > max_bytes {
            return Err(BuildError::DescriptionTooLarge { bytes: json.len() });
        }
        match description::parse_with_ignored(&json) {
            Ok((_, ignored)) => {
                if let Some(first) = ignored.into_iter().next() {
                    return Err(BuildError::Ignored(first));
                }
            }
            Err(error) => {
                return Err(BuildError::Ignored(Ignored {
                    node: String::new(),
                    property: None,
                    error,
                }));
            }
        }
        let hash = doc.content_hash();
        let mut routes: Vec<(Id, Id, u32)> = Vec::new();
        for n in dev
            .nodes
            .iter()
            .filter_map(|n| self.nodes.get(*n as usize)?.as_ref())
        {
            for s in &n.slots {
                if let Some(slot) = self.slot_at(*s) {
                    routes.push((n.id.clone(), slot.id.clone(), *s));
                }
            }
        }
        routes.sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));
        let Some(dev) = self.dev_mut(d) else {
            return Ok(false);
        };
        dev.routes = routes;
        let changed = dev.hash != Some(hash);
        dev.hash = Some(hash);
        dev.desc = json;
        Ok(changed)
    }

    /// Removes `d` and its descendants, recording what each leaves on the
    /// broker, leaves first.
    fn remove_device(&mut self, d: u16) {
        let order = self.subtree_postorder(d);
        if let Some(parent) = self.dev(d).and_then(|x| x.parent) {
            if let Some(p) = self.dev_mut(parent) {
                p.children.retain(|c| *c != d);
            }
            self.touched.insert(parent);
        }
        for x in order {
            let slots = self.dev_slots(x);
            let mut clears = Vec::new();
            let Some(dev) = self.dev(x) else { continue };
            clears.push(dev.state_topic.clone());
            let mut targets = Vec::new();
            for s in &slots {
                if let Some(slot) = self.slot_at(*s) {
                    if slot.fp.is_some() {
                        clears.push(slot.topic.clone());
                    }
                    if let (Some(t), Some(_)) = (&slot.target_topic, &slot.target) {
                        targets.push(t.clone());
                    }
                }
            }
            clears.extend(targets);
            let p = &self.prefix;
            clears.extend(
                dev.alerts
                    .keys()
                    .map(|a| format!("{p}{}/$alert/{a}", dev.id)),
            );
            clears.push(dev.desc_topic.clone());
            let set_filter = dev.set_filter.clone();
            let id = dev.id.clone();
            let node_ids = dev.nodes.clone();
            for s in slots {
                self.free_slot(s);
            }
            for n in node_ids {
                if let Some(node) = self.nodes.get_mut(n as usize) {
                    *node = None;
                }
            }
            self.by_id.remove(id.as_str());
            if let Some(slot) = self.devices.get_mut(usize::from(x)) {
                *slot = None;
            }
            self.touched.remove(&x);
            if let Some(i) = self.added.iter().position(|a| *a == x) {
                // Added and removed in one commit: nothing was published.
                self.added.remove(i);
                self.removed.push(RemovedDev {
                    set_filter,
                    clears: Vec::new(),
                });
            } else {
                self.removed.push(RemovedDev { set_filter, clears });
            }
        }
    }

    fn free_slot(&mut self, index: u32) {
        if let Some(cell) = self.cells.get_mut(index as usize) {
            if cell.slot.take().is_some() {
                cell.generation = cell.generation.wrapping_add(1);
                self.free.push(index);
            }
        }
    }
}

/// One reconfiguration commit in progress; see
/// [`Tree::reconfigure`](super::Tree::reconfigure).
pub struct Reconfigure<'a> {
    pub(super) m: &'a mut Model,
}

impl fmt::Debug for Reconfigure<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reconfigure").finish_non_exhaustive()
    }
}

impl Reconfigure<'_> {
    /// The root device.
    pub fn root(&self) -> DevIdx {
        DevIdx(self.m.root)
    }

    /// The check a property's values pass, as this commit leaves it so far.
    pub fn value_check(&self, slot: SlotId) -> Option<super::ValueCheck> {
        self.m.slot(slot).map(super::ValueCheck::of)
    }

    /// A property's value topic.
    pub fn topic(&self, slot: SlotId) -> Option<&str> {
        self.m.slot(slot).map(|s| s.topic.as_str())
    }

    /// Adds a child device under `parent`.
    pub fn add_child(
        &mut self,
        parent: DevIdx,
        id: &str,
        spec: DeviceSpec,
    ) -> Result<DevIdx, BuildError> {
        self.m.dev(parent.0).ok_or(BuildError::Gone)?;
        self.m.insert_device(Some(parent.0), id, spec).map(DevIdx)
    }

    /// Adds a node to `dev`.
    pub fn add_node(
        &mut self,
        dev: DevIdx,
        id: &str,
        spec: NodeSpec,
    ) -> Result<NodeIdx, BuildError> {
        if id.contains('.') {
            return Err(BuildError::NodeIdHasDot);
        }
        let id = valid_id(id)?;
        let d = self.m.dev(dev.0).ok_or(BuildError::Gone)?;
        let taken = d
            .nodes
            .iter()
            .filter_map(|n| self.m.nodes.get(*n as usize)?.as_ref())
            .any(|n| n.id == id);
        if taken {
            return Err(BuildError::DuplicateId);
        }
        let n = u32::try_from(self.m.nodes.len()).map_err(|_| BuildError::Capacity)?;
        self.m.nodes.push(Some(Node {
            dev: dev.0,
            id,
            spec,
            slots: Vec::new(),
        }));
        if let Some(d) = self.m.dev_mut(dev.0) {
            d.nodes.push(n);
        }
        self.m.touched.insert(dev.0);
        Ok(NodeIdx(n))
    }

    /// Adds a property of `datatype` to `node`. `implied` is the `$format`
    /// the caller's Rust type implies, intersected with `spec.format` for an
    /// integer and used when `spec.format` is absent otherwise.
    pub fn add_property(
        &mut self,
        node: NodeIdx,
        id: &str,
        datatype: Datatype,
        implied: Option<&str>,
        spec: PropSpec,
    ) -> Result<SlotId, BuildError> {
        let id = valid_id(id)?;
        let n = self
            .m
            .nodes
            .get(node.0 as usize)
            .and_then(Option::as_ref)
            .ok_or(BuildError::Gone)?;
        let taken = n
            .slots
            .iter()
            .filter_map(|s| self.m.slot_at(*s))
            .any(|s| s.id == id);
        if taken {
            return Err(BuildError::DuplicateId);
        }
        let dev = n.dev;
        let d = self.m.dev(dev).ok_or(BuildError::Gone)?;
        let mut spec = spec;
        spec.format = effective_format(datatype, spec.format.as_deref(), implied)?;
        if matches!(datatype, Datatype::Enum | Datatype::Color)
            && spec.format.as_deref().is_none_or(str::is_empty)
        {
            return Err(BuildError::FormatRequired);
        }
        let p = &self.m.prefix;
        let topic = format!("{p}{}/{}/{id}", d.id, n.id);
        let target_topic = spec.target.then(|| format!("{topic}/$target"));
        let mut slot = Slot {
            dev,
            node: node.0,
            id,
            datatype,
            implied: implied.map(String::from),
            spec,
            numeric: NumericFormat::default(),
            imin: None,
            imax: None,
            fmin: None,
            fmax: None,
            variants: Vec::new(),
            topic,
            target_topic,
            fp: None,
            payload: String::new(),
            sent: String::new(),
            sent_valid: false,
            target: None,
            open: false,
            value_ticket: 0,
            target_ticket: 0,
        };
        slot.apply_format();
        let index = match self.m.free.pop() {
            Some(i) => i,
            None => {
                let i = u32::try_from(self.m.cells.len()).map_err(|_| BuildError::Capacity)?;
                self.m.cells.push(Cell::default());
                i
            }
        };
        let cell = self
            .m
            .cells
            .get_mut(index as usize)
            .ok_or(BuildError::Capacity)?;
        cell.slot = Some(Box::new(slot));
        let id = SlotId {
            index,
            generation: cell.generation,
        };
        if let Some(n) = self
            .m
            .nodes
            .get_mut(node.0 as usize)
            .and_then(Option::as_mut)
        {
            n.slots.push(index);
        }
        self.m.touched.insert(dev);
        Ok(id)
    }

    /// [`add_property`](Self::add_property) for the Rust type `T`.
    pub fn add_typed<T: HomieValue>(
        &mut self,
        node: NodeIdx,
        id: &str,
        spec: PropSpec,
    ) -> Result<SlotId, BuildError> {
        let implied = T::default_format();
        if T::DATATYPE == Datatype::Enum {
            if let (Some(declared), Some(implied)) = (spec.format.as_deref(), implied.as_deref()) {
                if declared != implied {
                    return Err(BuildError::InvalidFormat(Problem::EnumNotInFormat));
                }
            }
        }
        self.add_property(node, id, T::DATATYPE, implied.as_deref(), spec)
    }

    /// Removes a child device and its descendants. The root cannot be
    /// removed (`InvalidId`).
    pub fn remove_child(&mut self, dev: DevIdx) -> Result<(), BuildError> {
        if dev.0 == self.m.root {
            return Err(BuildError::InvalidId);
        }
        self.m.dev(dev.0).ok_or(BuildError::Gone)?;
        self.m.remove_device(dev.0);
        Ok(())
    }

    /// Removes a node and its properties.
    pub fn remove_node(&mut self, node: NodeIdx) -> Result<(), BuildError> {
        let n = self
            .m
            .nodes
            .get(node.0 as usize)
            .and_then(Option::as_ref)
            .ok_or(BuildError::Gone)?;
        let (dev, slots) = (n.dev, n.slots.clone());
        for s in slots {
            let id = self.m.slot_id(s);
            self.remove_property(id)?;
        }
        if let Some(n) = self.m.nodes.get_mut(node.0 as usize) {
            *n = None;
        }
        if let Some(d) = self.m.dev_mut(dev) {
            d.nodes.retain(|x| *x != node.0);
        }
        self.m.touched.insert(dev);
        Ok(())
    }

    /// Removes a property; its retained value and `$target` are cleared.
    pub fn remove_property(&mut self, slot: SlotId) -> Result<(), BuildError> {
        let s = self.m.slot(slot).ok_or(BuildError::Gone)?;
        let (dev, node, clears) = (s.dev, s.node, s.clears());
        if !self.m.added.contains(&dev) {
            self.m.removed_slots.push(RemovedSlot { dev, clears });
        }
        if let Some(n) = self.m.nodes.get_mut(node as usize).and_then(Option::as_mut) {
            n.slots.retain(|x| *x != slot.index);
        }
        self.m.free_slot(slot.index);
        self.m.touched.insert(dev);
        Ok(())
    }

    /// Replaces a property's declared `$format`.
    pub fn set_format(&mut self, slot: SlotId, format: Option<&str>) -> Result<(), BuildError> {
        let s = self.m.slot_mut(slot).ok_or(BuildError::Gone)?;
        let f = effective_format(s.datatype, format, s.implied.as_deref())?;
        if matches!(s.datatype, Datatype::Enum | Datatype::Color)
            && f.as_deref().is_none_or(str::is_empty)
        {
            return Err(BuildError::FormatRequired);
        }
        s.spec.format = f;
        s.apply_format();
        if s.datatype == Datatype::Enum && s.fp.is_some() {
            // An index may name another value now: the next set compares
            // the payload.
            s.fp = Some(Fp::Bytes);
        }
        let dev = s.dev;
        self.m.touched.insert(dev);
        Ok(())
    }

    /// Replaces a property's unit.
    pub fn set_unit(&mut self, slot: SlotId, unit: Option<&str>) -> Result<(), BuildError> {
        let s = self.m.slot_mut(slot).ok_or(BuildError::Gone)?;
        s.spec.unit = unit.map(String::from);
        let dev = s.dev;
        self.m.touched.insert(dev);
        Ok(())
    }

    /// Replaces a property's friendly name.
    pub fn set_name(&mut self, slot: SlotId, name: Option<&str>) -> Result<(), BuildError> {
        let s = self.m.slot_mut(slot).ok_or(BuildError::Gone)?;
        s.spec.name = name.map(String::from);
        let dev = s.dev;
        self.m.touched.insert(dev);
        Ok(())
    }
}

fn effective_format(
    datatype: Datatype,
    declared: Option<&str>,
    implied: Option<&str>,
) -> Result<Option<String>, BuildError> {
    match datatype {
        Datatype::Integer => intersect(declared, implied),
        _ => Ok(declared
            .filter(|d| !d.is_empty())
            .or(implied)
            .map(String::from)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersect_formats() {
        assert_eq!(
            intersect(None, Some("0:255")),
            Ok(Some("0:255".to_string()))
        );
        assert_eq!(
            intersect(Some("10:"), Some("0:255")),
            Ok(Some("10:255".to_string()))
        );
        assert_eq!(
            intersect(Some(":100:5"), Some("0:255")),
            Ok(Some("0:100:5".to_string()))
        );
        assert_eq!(
            intersect(Some("-5:300"), Some("0:255")),
            Ok(Some("0:255".to_string()))
        );
        assert_eq!(intersect(Some("1:2"), None), Ok(Some("1:2".to_string())));
        assert_eq!(
            intersect(Some("x"), Some("0:255")),
            Ok(Some("x".to_string()))
        );
        assert_eq!(
            intersect(Some("300:"), Some("0:255")),
            Err(BuildError::InvalidFormat(Problem::AboveMax))
        );
        assert_eq!(
            intersect(Some(":-1"), Some("0:255")),
            Err(BuildError::InvalidFormat(Problem::BelowMin))
        );
    }

    #[test]
    fn extension_ids() {
        assert!(is_extension_id("org.example.feature"));
        assert!(is_extension_id("energy.ebus.imported"));
        for bad in ["", "plain", "org..x", "Org.x", "org.x "] {
            assert!(!is_extension_id(bad), "{bad:?}");
        }
    }
}
