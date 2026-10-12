//! The `$description` document: model, wire form and validation
//! (convention.md: Device Attributes, Nodes, Properties).
//!
//! [`DeviceDescription::to_json`] writes the same bytes as the Python SDK's
//! `json.dumps(Device.description())`, and [`DeviceDescription::compute_version`]
//! derives the same `version` from the content.

use alloc::collections::BTreeSet;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::HOMIE_DESCRIPTION_VERSION;
use crate::datatype::{Datatype, Decimal, is_float_payload, is_integer_payload};
use crate::id::is_valid_id;
use crate::pyfmt::{Ordered, from_json_ordered, to_json as value_to_json, write_str, write_value};

/// The core `$description` fields (convention.md: Device Attributes). An extra
/// with one of these keys is never written.
pub const CORE_FIELDS: [&str; 9] = [
    "homie",
    "version",
    "nodes",
    "name",
    "type",
    "children",
    "root",
    "parent",
    "extensions",
];

/// One property in a node (convention.md: Properties).
///
/// Built from [`PropertyDescription::new`] outside this crate.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PropertyDescription {
    /// Friendly name; `None` omits it (consumers default it to the property id).
    pub name: Option<String>,
    /// The property's datatype.
    pub datatype: Datatype,
    /// `$format`; omitted when `None` or empty.
    pub format: Option<String>,
    /// Whether the property accepts `/set`; written only when `true`.
    pub settable: bool,
    /// Whether values are retained; written only when `false`.
    pub retained: bool,
    /// Unit; omitted when `None` or empty.
    pub unit: Option<String>,
}

impl PropertyDescription {
    /// A retained, non-settable property with no name, format or unit.
    pub fn new(datatype: Datatype) -> Self {
        PropertyDescription {
            name: None,
            datatype,
            format: None,
            settable: false,
            retained: true,
            unit: None,
        }
    }
}

/// One node in a device (convention.md: Nodes).
///
/// Built from `NodeDescription::default()` outside this crate.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct NodeDescription {
    /// Friendly name; `None` omits it (consumers default it to the node id).
    pub name: Option<String>,
    /// Node type; omitted when `None`.
    pub r#type: Option<String>,
    /// Properties keyed by id, in document order. A repeated id keeps its first
    /// entry.
    pub properties: Vec<(String, PropertyDescription)>,
}

/// A device's `$description` (convention.md: Device Attributes).
///
/// Built from `DeviceDescription::default()` outside this crate.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct DeviceDescription {
    /// Document version: any JSON integer (convention.md: Device Attributes,
    /// `version`); see [`DeviceDescription::compute_version`].
    pub version: i128,
    /// Device type; omitted when `None`.
    pub r#type: Option<String>,
    /// Friendly name; `None` omits it (consumers default it to the device id).
    pub name: Option<String>,
    /// Nodes keyed by id, in document order. A repeated id keeps its first entry.
    pub nodes: Vec<(String, NodeDescription)>,
    /// Ids of child devices.
    pub children: Vec<String>,
    /// Id of the root device; `Some` exactly when this is not the root.
    pub root: Option<String>,
    /// Id of the parent device; `None` means the parent is the root.
    pub parent: Option<String>,
    /// Supported extensions.
    pub extensions: Vec<String>,
    /// Extension-defined top-level fields, written after `extensions`. Keys in
    /// [`CORE_FIELDS`] and repeated keys after the first are skipped.
    pub extras: Vec<(String, Value)>,
}

/// Why [`parse`] rejected a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DescriptionError {
    /// Not valid JSON.
    InvalidJson,
    /// The top level is not a JSON object.
    NotAnObject,
    /// `homie` is absent.
    HomieMissing,
    /// `homie` is not a string of the form `5.x`.
    HomieInvalid,
    /// `version` is absent.
    VersionMissing,
    /// `version` is not an integer in the `i64` or `u64` range.
    VersionInvalid,
    /// A `name` is not a string.
    NameInvalid,
    /// A `type` is not a string.
    TypeInvalid,
    /// `nodes` is not an object.
    NodesInvalid,
    /// A node id is not a valid id.
    NodeIdInvalid,
    /// A node is not an object.
    NodeInvalid,
    /// A node's `properties` is not an object.
    PropertiesInvalid,
    /// A property id is not a valid id.
    PropertyIdInvalid,
    /// A property is not an object.
    PropertyInvalid,
    /// A property has no `datatype`.
    DatatypeMissing,
    /// A property's `datatype` is not a Homie 5 datatype name.
    DatatypeUnknown,
    /// A property's `format` is not a string.
    FormatInvalid,
    /// An `enum` or `color` property has no or an empty `format`
    /// (convention.md: Formats).
    FormatMissing,
    /// A property's `format` breaks its datatype's format rules (convention.md:
    /// Formats).
    FormatIllegal,
    /// A property's `settable` is not a boolean.
    SettableInvalid,
    /// A property's `retained` is not a boolean.
    RetainedInvalid,
    /// A property's `unit` is not a string.
    UnitInvalid,
    /// `children` is not an array of valid ids.
    ChildrenInvalid,
    /// `root` is not a valid id.
    RootInvalid,
    /// `parent` is not a valid id.
    ParentInvalid,
    /// `parent` is present without `root`.
    ParentWithoutRoot,
    /// `extensions` is not an array of strings.
    ExtensionsInvalid,
}

impl DescriptionError {
    /// The camelCase reason code.
    pub const fn as_str(self) -> &'static str {
        match self {
            DescriptionError::InvalidJson => "invalidJson",
            DescriptionError::NotAnObject => "notAnObject",
            DescriptionError::HomieMissing => "homieMissing",
            DescriptionError::HomieInvalid => "homieInvalid",
            DescriptionError::VersionMissing => "versionMissing",
            DescriptionError::VersionInvalid => "versionInvalid",
            DescriptionError::NameInvalid => "nameInvalid",
            DescriptionError::TypeInvalid => "typeInvalid",
            DescriptionError::NodesInvalid => "nodesInvalid",
            DescriptionError::NodeIdInvalid => "nodeIdInvalid",
            DescriptionError::NodeInvalid => "nodeInvalid",
            DescriptionError::PropertiesInvalid => "propertiesInvalid",
            DescriptionError::PropertyIdInvalid => "propertyIdInvalid",
            DescriptionError::PropertyInvalid => "propertyInvalid",
            DescriptionError::DatatypeMissing => "datatypeMissing",
            DescriptionError::DatatypeUnknown => "datatypeUnknown",
            DescriptionError::FormatInvalid => "formatInvalid",
            DescriptionError::FormatMissing => "formatMissing",
            DescriptionError::FormatIllegal => "formatIllegal",
            DescriptionError::SettableInvalid => "settableInvalid",
            DescriptionError::RetainedInvalid => "retainedInvalid",
            DescriptionError::UnitInvalid => "unitInvalid",
            DescriptionError::ChildrenInvalid => "childrenInvalid",
            DescriptionError::RootInvalid => "rootInvalid",
            DescriptionError::ParentInvalid => "parentInvalid",
            DescriptionError::ParentWithoutRoot => "parentWithoutRoot",
            DescriptionError::ExtensionsInvalid => "extensionsInvalid",
        }
    }
}

impl fmt::Display for DescriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl core::error::Error for DescriptionError {}

/// Entries of `items` whose key has not appeared earlier, in order.
fn first_by_key<T>(items: &[(String, T)]) -> impl Iterator<Item = (&str, &T)> {
    let mut seen = BTreeSet::new();
    items
        .iter()
        .filter(move |(k, _)| seen.insert(k.as_str()))
        .map(|(k, v)| (k.as_str(), v))
}

/// Writes `"key": ` preceded by `", "` unless `first`.
fn write_key(out: &mut String, first: &mut bool, key: &str) {
    if !*first {
        out.push_str(", ");
    }
    *first = false;
    write_str(out, key);
    out.push_str(": ");
}

fn write_str_array(out: &mut String, items: &[String]) {
    out.push('[');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        write_str(out, item);
    }
    out.push(']');
}

fn non_empty(s: &Option<String>) -> Option<&str> {
    s.as_deref().filter(|s| !s.is_empty())
}

impl PropertyDescription {
    /// Appends the property object in Python key order.
    fn write_json(&self, out: &mut String) {
        out.push('{');
        let mut first = true;
        if let Some(name) = &self.name {
            write_key(out, &mut first, "name");
            write_str(out, name);
        }
        write_key(out, &mut first, "datatype");
        write_str(out, self.datatype.as_str());
        if let Some(format) = non_empty(&self.format) {
            write_key(out, &mut first, "format");
            write_str(out, format);
        }
        if self.settable {
            write_key(out, &mut first, "settable");
            out.push_str("true");
        }
        if !self.retained {
            write_key(out, &mut first, "retained");
            out.push_str("false");
        }
        if let Some(unit) = non_empty(&self.unit) {
            write_key(out, &mut first, "unit");
            write_str(out, unit);
        }
        out.push('}');
    }

    fn to_value(&self) -> Value {
        let mut m = Map::new();
        if let Some(name) = &self.name {
            m.insert("name".to_string(), Value::String(name.clone()));
        }
        m.insert(
            "datatype".to_string(),
            Value::String(self.datatype.as_str().to_string()),
        );
        if let Some(format) = non_empty(&self.format) {
            m.insert("format".to_string(), Value::String(format.to_string()));
        }
        if self.settable {
            m.insert("settable".to_string(), Value::Bool(true));
        }
        if !self.retained {
            m.insert("retained".to_string(), Value::Bool(false));
        }
        if let Some(unit) = non_empty(&self.unit) {
            m.insert("unit".to_string(), Value::String(unit.to_string()));
        }
        Value::Object(m)
    }
}

impl NodeDescription {
    /// Appends the node object in Python key order.
    fn write_json(&self, out: &mut String) {
        out.push('{');
        let mut first = true;
        if let Some(name) = &self.name {
            write_key(out, &mut first, "name");
            write_str(out, name);
        }
        if let Some(t) = &self.r#type {
            write_key(out, &mut first, "type");
            write_str(out, t);
        }
        write_key(out, &mut first, "properties");
        out.push('{');
        let mut first_prop = true;
        for (id, prop) in first_by_key(&self.properties) {
            write_key(out, &mut first_prop, id);
            prop.write_json(out);
        }
        out.push('}');
        out.push('}');
    }

    fn to_value(&self) -> Value {
        let mut m = Map::new();
        if let Some(name) = &self.name {
            m.insert("name".to_string(), Value::String(name.clone()));
        }
        if let Some(t) = &self.r#type {
            m.insert("type".to_string(), Value::String(t.clone()));
        }
        let mut props = Map::new();
        for (id, prop) in first_by_key(&self.properties) {
            props.insert(id.to_string(), prop.to_value());
        }
        m.insert("properties".to_string(), Value::Object(props));
        Value::Object(m)
    }
}

fn str_array_value(items: &[String]) -> Value {
    Value::Array(items.iter().cloned().map(Value::String).collect())
}

impl DeviceDescription {
    /// Extras that are written: core keys and repeated keys dropped.
    fn written_extras(&self) -> impl Iterator<Item = (&str, &Value)> {
        first_by_key(&self.extras).filter(|(k, _)| !CORE_FIELDS.contains(k))
    }

    /// The document as Python `json.dumps(Device.description())` writes it.
    ///
    /// Key order: `homie`, `version`, `type`, `name`, `nodes`, `children`,
    /// `root`, `parent`, `extensions`, then extras. Objects nested inside an
    /// extra keep `serde_json::Map` iteration order.
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        out.push('{');
        let mut first = true;
        write_key(&mut out, &mut first, "homie");
        write_str(&mut out, HOMIE_DESCRIPTION_VERSION);
        write_key(&mut out, &mut first, "version");
        out.push_str(&self.version.to_string());
        if let Some(t) = &self.r#type {
            write_key(&mut out, &mut first, "type");
            write_str(&mut out, t);
        }
        if let Some(name) = &self.name {
            write_key(&mut out, &mut first, "name");
            write_str(&mut out, name);
        }
        write_key(&mut out, &mut first, "nodes");
        out.push('{');
        let mut first_node = true;
        for (id, node) in first_by_key(&self.nodes) {
            write_key(&mut out, &mut first_node, id);
            node.write_json(&mut out);
        }
        out.push('}');
        write_key(&mut out, &mut first, "children");
        write_str_array(&mut out, &self.children);
        if let Some(root) = &self.root {
            write_key(&mut out, &mut first, "root");
            write_str(&mut out, root);
        }
        if let Some(parent) = &self.parent {
            write_key(&mut out, &mut first, "parent");
            write_str(&mut out, parent);
        }
        write_key(&mut out, &mut first, "extensions");
        write_str_array(&mut out, &self.extensions);
        for (key, value) in self.written_extras() {
            write_key(&mut out, &mut first, key);
            write_value(&mut out, value, false);
        }
        out.push('}');
        out
    }

    /// The document without `version`, as a JSON value.
    fn content_value(&self) -> Value {
        let mut m = Map::new();
        m.insert(
            "homie".to_string(),
            Value::String(HOMIE_DESCRIPTION_VERSION.to_string()),
        );
        if let Some(t) = &self.r#type {
            m.insert("type".to_string(), Value::String(t.clone()));
        }
        if let Some(name) = &self.name {
            m.insert("name".to_string(), Value::String(name.clone()));
        }
        let mut nodes = Map::new();
        for (id, node) in first_by_key(&self.nodes) {
            nodes.insert(id.to_string(), node.to_value());
        }
        m.insert("nodes".to_string(), Value::Object(nodes));
        m.insert("children".to_string(), str_array_value(&self.children));
        if let Some(root) = &self.root {
            m.insert("root".to_string(), Value::String(root.clone()));
        }
        if let Some(parent) = &self.parent {
            m.insert("parent".to_string(), Value::String(parent.clone()));
        }
        m.insert("extensions".to_string(), str_array_value(&self.extensions));
        for (key, value) in self.written_extras() {
            m.insert(key.to_string(), value.clone());
        }
        Value::Object(m)
    }

    /// SHA-256 of the document without `version`, serialized as Python
    /// `json.dumps(content, sort_keys=True)` (Python `_description_content_hash`).
    pub fn content_hash(&self) -> [u8; 32] {
        let text = value_to_json(&self.content_value(), true);
        Sha256::digest(text.as_bytes()).into()
    }

    /// The version the Python SDK assigns: the top 52 bits of
    /// [`content_hash`](Self::content_hash), so it changes whenever the content
    /// does (convention.md: Device Attributes) and stays below 2^53.
    pub fn compute_version(&self) -> u64 {
        let h = self.content_hash();
        let mut top = [0u8; 8];
        top.copy_from_slice(&h[..8]);
        u64::from_be_bytes(top) >> 12
    }

    /// `self` with `version` set to [`compute_version`](Self::compute_version).
    pub fn with_computed_version(mut self) -> Self {
        self.version = i128::from(self.compute_version());
        self
    }
}

/// A node or property [`parse_with_ignored`] left out because one of its known
/// fields holds an illegal value (convention.md: Forward compatibility).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Ignored {
    /// The node's id, as received.
    pub node: String,
    /// The property's id, as received; `None` when the whole node was left out.
    pub property: Option<String>,
    /// The illegal value found.
    pub error: DescriptionError,
}

/// An object's members in document order, keys unique.
type Fields = Vec<(String, Ordered)>;

/// Object nesting [`parse_with_ignored`] reads in document order: the
/// document, `nodes`, a node, and its `properties`.
const ORDERED_LEVELS: usize = 4;

/// The members of `value` if it is an object.
fn into_fields(value: Ordered) -> Option<Fields> {
    match value {
        Ordered::Object(items) => Some(items),
        Ordered::Value(Value::Object(map)) => Some(
            map.into_iter()
                .map(|(k, v)| (k, Ordered::Value(v)))
                .collect(),
        ),
        Ordered::Value(_) => None,
    }
}

fn take(obj: &mut Fields, key: &str) -> Option<Ordered> {
    let i = obj.iter().position(|(k, _)| k == key)?;
    Some(obj.remove(i).1)
}

fn take_value(obj: &mut Fields, key: &str) -> Option<Value> {
    take(obj, key).map(Ordered::into_value)
}

/// The object under `key`: empty when absent, `err` when not an object.
fn take_fields(
    obj: &mut Fields,
    key: &str,
    err: DescriptionError,
) -> Result<Fields, DescriptionError> {
    match take(obj, key) {
        None => Ok(Vec::new()),
        Some(value) => into_fields(value).ok_or(err),
    }
}

fn take_str(
    obj: &mut Fields,
    key: &str,
    err: DescriptionError,
) -> Result<Option<String>, DescriptionError> {
    match take_value(obj, key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(err),
    }
}

fn take_bool(
    obj: &mut Fields,
    key: &str,
    default: bool,
    err: DescriptionError,
) -> Result<bool, DescriptionError> {
    match take_value(obj, key) {
        None => Ok(default),
        Some(Value::Bool(b)) => Ok(b),
        Some(_) => Err(err),
    }
}

fn take_id(
    obj: &mut Fields,
    key: &str,
    err: DescriptionError,
) -> Result<Option<String>, DescriptionError> {
    match take_value(obj, key) {
        None => Ok(None),
        Some(Value::String(s)) if is_valid_id(&s) => Ok(Some(s)),
        Some(_) => Err(err),
    }
}

fn take_str_array(
    obj: &mut Fields,
    key: &str,
    valid: impl Fn(&str) -> bool,
    err: DescriptionError,
) -> Result<Vec<String>, DescriptionError> {
    match take_value(obj, key) {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .into_iter()
            .map(|v| match v {
                Value::String(s) if valid(&s) => Ok(s),
                _ => Err(err),
            })
            .collect(),
        Some(_) => Err(err),
    }
}

/// True for `5.x` with `x` one or more ASCII digits (convention.md: Device
/// Attributes, `homie`).
fn is_homie_5(s: &str) -> bool {
    s.strip_prefix("5.")
        .is_some_and(|minor| !minor.is_empty() && minor.bytes().all(|b| b.is_ascii_digit()))
}

/// Comma-separated values, none empty and none repeated (convention.md:
/// Formats, enum).
fn is_value_list(format: &str) -> bool {
    let mut seen = BTreeSet::new();
    format.split(',').all(|v| !v.is_empty() && seen.insert(v))
}

/// `[min]:[max][:step]` with min and max in the datatype's payload form and a
/// step > 0 (convention.md: Formats, integer and float).
fn is_numeric_format(format: &str, datatype: Datatype) -> bool {
    let bound_ok = |b: &str| {
        b.is_empty()
            || match datatype {
                Datatype::Integer => is_integer_payload(b) && b.parse::<i64>().is_ok(),
                _ => is_float_payload(b),
            }
    };
    let mut parts = format.split(':');
    let (Some(low), Some(high)) = (parts.next(), parts.next()) else {
        return false;
    };
    let step_ok = match parts.next() {
        None => true,
        Some(step) => {
            is_float_payload(step) && Decimal::parse(step).is_some_and(|d| d > Decimal::from_i64(0))
        }
    };
    parts.next().is_none() && bound_ok(low) && bound_ok(high) && step_ok
}

/// Check `format` against the datatype's rules (convention.md: Formats).
fn check_format(datatype: Datatype, format: Option<&str>) -> Result<(), DescriptionError> {
    let format = format.filter(|f| !f.is_empty());
    let legal = match (datatype, format) {
        (Datatype::Enum | Datatype::Color, None) => return Err(DescriptionError::FormatMissing),
        (Datatype::Enum, Some(f)) => is_value_list(f),
        (Datatype::Color, Some(f)) => f.split(',').all(|t| matches!(t, "rgb" | "hsv" | "xyz")),
        (Datatype::Boolean, Some(f)) => is_value_list(f) && f.split(',').count() == 2,
        (Datatype::Integer | Datatype::Float, Some(f)) => is_numeric_format(f, datatype),
        _ => true,
    };
    if legal {
        Ok(())
    } else {
        Err(DescriptionError::FormatIllegal)
    }
}

fn parse_property(value: Ordered) -> Result<PropertyDescription, DescriptionError> {
    let mut obj = into_fields(value).ok_or(DescriptionError::PropertyInvalid)?;
    let datatype = match take_value(&mut obj, "datatype") {
        None => return Err(DescriptionError::DatatypeMissing),
        Some(Value::String(s)) => Datatype::parse(&s).ok_or(DescriptionError::DatatypeUnknown)?,
        Some(_) => return Err(DescriptionError::DatatypeUnknown),
    };
    let format = take_str(&mut obj, "format", DescriptionError::FormatInvalid)?;
    check_format(datatype, format.as_deref())?;
    Ok(PropertyDescription {
        name: take_str(&mut obj, "name", DescriptionError::NameInvalid)?,
        datatype,
        format,
        settable: take_bool(
            &mut obj,
            "settable",
            false,
            DescriptionError::SettableInvalid,
        )?,
        retained: take_bool(
            &mut obj,
            "retained",
            true,
            DescriptionError::RetainedInvalid,
        )?,
        unit: take_str(&mut obj, "unit", DescriptionError::UnitInvalid)?,
    })
}

fn parse_node(
    id: &str,
    value: Ordered,
    ignored: &mut Vec<Ignored>,
) -> Result<NodeDescription, DescriptionError> {
    let mut obj = into_fields(value).ok_or(DescriptionError::NodeInvalid)?;
    let name = take_str(&mut obj, "name", DescriptionError::NameInvalid)?;
    let r#type = take_str(&mut obj, "type", DescriptionError::TypeInvalid)?;
    let props = take_fields(&mut obj, "properties", DescriptionError::PropertiesInvalid)?;
    let mut properties = Vec::new();
    for (prop_id, prop) in props {
        let parsed = if is_valid_id(&prop_id) {
            parse_property(prop)
        } else {
            Err(DescriptionError::PropertyIdInvalid)
        };
        match parsed {
            Ok(p) => properties.push((prop_id, p)),
            Err(error) => ignored.push(Ignored {
                node: id.to_string(),
                property: Some(prop_id),
                error,
            }),
        }
    }
    Ok(NodeDescription {
        name,
        r#type,
        properties,
    })
}

/// Parse and validate a received `$description` (convention.md: Device
/// Attributes, Nodes, Properties); see [`parse_with_ignored`].
pub fn parse(json: &str) -> Result<DeviceDescription, DescriptionError> {
    parse_with_ignored(json).map(|(d, _)| d)
}

/// Parse and validate a received `$description`, and list the nodes and
/// properties left out of it.
///
/// An illegal value in a device-level field rejects the document. An illegal
/// value in a node or property (an invalid id, an unknown datatype, an illegal
/// format, a field of the wrong type) leaves out only that node or property
/// (convention.md: Forward compatibility). Absent optional fields take their
/// convention defaults. Unknown node and property fields are ignored; unknown
/// top-level fields are kept in [`DeviceDescription::extras`]. JSON is read as
/// [`crate::pyfmt::from_json`] reads it.
///
/// Nodes, properties within each node, and extras come out in document order.
/// A repeated key keeps its last value at the position of its first
/// occurrence, as Python `json.loads` does, at every level. Objects nested
/// inside an extra's value come out in `serde_json::Map` iteration order (key
/// order, as `preserve_order` is off), so [`DeviceDescription::to_json`]
/// reproduces the received bytes only when those nested keys are sorted.
pub fn parse_with_ignored(
    json: &str,
) -> Result<(DeviceDescription, Vec<Ignored>), DescriptionError> {
    let value = from_json_ordered(json, ORDERED_LEVELS).ok_or(DescriptionError::InvalidJson)?;
    let mut obj = into_fields(value).ok_or(DescriptionError::NotAnObject)?;
    match take_value(&mut obj, "homie") {
        None => return Err(DescriptionError::HomieMissing),
        Some(Value::String(s)) if is_homie_5(&s) => {}
        Some(_) => return Err(DescriptionError::HomieInvalid),
    }
    let version = match take_value(&mut obj, "version") {
        None => return Err(DescriptionError::VersionMissing),
        Some(Value::Number(n)) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
            .ok_or(DescriptionError::VersionInvalid)?,
        Some(_) => return Err(DescriptionError::VersionInvalid),
    };
    let node_map = take_fields(&mut obj, "nodes", DescriptionError::NodesInvalid)?;
    let children = take_str_array(
        &mut obj,
        "children",
        is_valid_id,
        DescriptionError::ChildrenInvalid,
    )?;
    let extensions = take_str_array(
        &mut obj,
        "extensions",
        |_| true,
        DescriptionError::ExtensionsInvalid,
    )?;
    let root = take_id(&mut obj, "root", DescriptionError::RootInvalid)?;
    let parent = take_id(&mut obj, "parent", DescriptionError::ParentInvalid)?;
    if parent.is_some() && root.is_none() {
        return Err(DescriptionError::ParentWithoutRoot);
    }
    let name = take_str(&mut obj, "name", DescriptionError::NameInvalid)?;
    let r#type = take_str(&mut obj, "type", DescriptionError::TypeInvalid)?;
    let mut ignored = Vec::new();
    let mut nodes = Vec::new();
    for (id, node) in node_map {
        let parsed = if is_valid_id(&id) {
            parse_node(&id, node, &mut ignored)
        } else {
            Err(DescriptionError::NodeIdInvalid)
        };
        match parsed {
            Ok(n) => nodes.push((id, n)),
            Err(error) => ignored.push(Ignored {
                node: id,
                property: None,
                error,
            }),
        }
    }
    // Every core field has been removed; what is left are the extras.
    let extras = obj.into_iter().map(|(k, v)| (k, v.into_value())).collect();
    Ok((
        DeviceDescription {
            version,
            r#type,
            name,
            nodes,
            children,
            root,
            parent,
            extensions,
            extras,
        },
        ignored,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn prop(datatype: Datatype, name: &str) -> PropertyDescription {
        PropertyDescription {
            name: Some(name.to_string()),
            ..PropertyDescription::new(datatype)
        }
    }

    /// Root device with a node and three properties, as the Python SDK builds it.
    fn sample_root() -> DeviceDescription {
        DeviceDescription {
            r#type: Some("ebus/panel".to_string()),
            name: Some("Panel 1".to_string()),
            nodes: vec![(
                "meter".to_string(),
                NodeDescription {
                    name: Some("Meter".to_string()),
                    r#type: None,
                    properties: vec![
                        (
                            "power".to_string(),
                            PropertyDescription {
                                format: Some("-100:100".to_string()),
                                unit: Some("W".to_string()),
                                ..prop(Datatype::Float, "power")
                            },
                        ),
                        (
                            "mode".to_string(),
                            PropertyDescription {
                                format: Some("on,off".to_string()),
                                settable: true,
                                retained: false,
                                ..prop(Datatype::Enum, "Mode \u{b0}C")
                            },
                        ),
                        (
                            "blank".to_string(),
                            PropertyDescription {
                                format: Some(String::new()),
                                unit: Some(String::new()),
                                ..prop(Datatype::String, "blank")
                            },
                        ),
                    ],
                },
            )],
            children: vec!["bridge".to_string()],
            extensions: vec!["eu.ebus.x:1.0:[5.x]".to_string()],
            ..DeviceDescription::default()
        }
        .with_computed_version()
    }

    #[test]
    fn root_bytes_and_version_match_python() {
        let d = sample_root();
        assert_eq!(d.to_json(), PY_ROOT_JSON);
        assert_eq!(d.version, PY_ROOT_VERSION);
    }

    #[test]
    fn child_with_extras_matches_python() {
        let mut extra = Map::new();
        extra.insert("b".to_string(), Value::from(1.5));
        extra.insert("a".to_string(), Value::from(vec![1, 2]));
        let d = DeviceDescription {
            r#type: Some("ebus/circuit".to_string()),
            name: Some("Circuit \u{1f600}".to_string()),
            nodes: vec![(
                "relay".to_string(),
                NodeDescription {
                    name: Some("relay".to_string()),
                    r#type: Some("switch".to_string()),
                    properties: vec![],
                },
            )],
            root: Some("panel-1".to_string()),
            parent: Some("bridge".to_string()),
            extras: vec![
                ("zeta".to_string(), Value::Object(extra)),
                ("name".to_string(), Value::from("ignored")),
                ("alpha".to_string(), Value::from(1e16)),
                ("zeta".to_string(), Value::Null),
            ],
            ..DeviceDescription::default()
        }
        .with_computed_version();
        assert_eq!(d.to_json(), PY_CHILD_JSON);
        assert_eq!(d.version, PY_CHILD_VERSION);
    }

    #[test]
    fn empty_device() {
        let d = DeviceDescription {
            name: Some("x".to_string()),
            ..DeviceDescription::default()
        }
        .with_computed_version();
        assert_eq!(d.to_json(), PY_EMPTY_JSON);
        assert_eq!(d.version, PY_EMPTY_VERSION);
    }

    #[test]
    fn version_is_top_52_bits_of_hash() {
        let d = sample_root();
        let h = d.content_hash();
        let mut expect: u64 = 0;
        for b in &h[..6] {
            expect = (expect << 8) | u64::from(*b);
        }
        expect = (expect << 4) | u64::from(h[6] >> 4);
        assert_eq!(d.compute_version(), expect);
        assert!(d.version < (1i128 << 53));
    }

    #[test]
    fn parse_round_trips() {
        let d = sample_root();
        let parsed = parse(&d.to_json()).unwrap();
        assert_eq!(parsed.version, d.version);
        assert_eq!(parsed.children, d.children);
        assert_eq!(parsed.nodes.len(), 1);
        let (_, node) = &parsed.nodes[0];
        let mode = node.properties.iter().find(|(id, _)| id == "mode").unwrap();
        assert!(mode.1.settable);
        assert!(!mode.1.retained);
        assert_eq!(mode.1.datatype, Datatype::Enum);
        assert_eq!(i128::from(parsed.compute_version()), d.version);
    }

    #[test]
    fn parse_keeps_extras_and_ignores_unknown_inner_fields() {
        let d = parse(
            r#"{"homie":"5.12","version":3,"nodes":{"n":{"x":1,"properties":{"p":{"datatype":"json","future":true}}}},"ext":{"k":1}}"#,
        )
        .unwrap();
        assert_eq!(d.extras.len(), 1);
        assert_eq!(d.extras[0].0, "ext");
        assert_eq!(d.nodes[0].1.properties[0].1.datatype, Datatype::Json);
        assert!(d.nodes[0].1.properties[0].1.retained);
    }

    #[test]
    fn parse_rejects() {
        use DescriptionError as E;
        let cases: &[(&str, E)] = &[
            ("", E::InvalidJson),
            ("[]", E::NotAnObject),
            (r#"{"version":1}"#, E::HomieMissing),
            (r#"{"homie":"4.0","version":1}"#, E::HomieInvalid),
            (r#"{"homie":"5.","version":1}"#, E::HomieInvalid),
            (r#"{"homie":5.0,"version":1}"#, E::HomieInvalid),
            (r#"{"homie":"5.0"}"#, E::VersionMissing),
            (r#"{"homie":"5.0","version":1.5}"#, E::VersionInvalid),
            (r#"{"homie":"5.0","version":"1"}"#, E::VersionInvalid),
            (r#"{"homie":"5.0","version":-0.0}"#, E::VersionInvalid),
            (r#"{"homie":"5.0","version":1,"name":null}"#, E::NameInvalid),
            (r#"{"homie":"5.0","version":1,"type":2}"#, E::TypeInvalid),
            (r#"{"homie":"5.0","version":1,"nodes":[]}"#, E::NodesInvalid),
            (
                r#"{"homie":"5.0","version":1,"children":["A"]}"#,
                E::ChildrenInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"children":"a"}"#,
                E::ChildrenInvalid,
            ),
            (r#"{"homie":"5.0","version":1,"root":""}"#, E::RootInvalid),
            (
                r#"{"homie":"5.0","version":1,"root":"r","parent":"P"}"#,
                E::ParentInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"parent":"p"}"#,
                E::ParentWithoutRoot,
            ),
            (
                r#"{"homie":"5.0","version":1,"extensions":[1]}"#,
                E::ExtensionsInvalid,
            ),
        ];
        for (json, want) in cases {
            assert_eq!(parse(json), Err(*want), "{json}");
        }
    }

    #[test]
    fn parse_accepts_any_integer_version() {
        for (text, want) in [
            ("-5", -5i128),
            ("-9223372036854775808", i128::from(i64::MIN)),
            ("18446744073709551615", i128::from(u64::MAX)),
            ("-0", 0),
        ] {
            let json = alloc::format!(r#"{{"homie":"5.0","version":{text}}}"#);
            assert_eq!(parse(&json).map(|d| d.version), Ok(want), "{json}");
        }
    }

    /// Wraps one property in node `n`, beside a valid property `ok`.
    fn with_property(prop: &str) -> String {
        alloc::format!(
            r#"{{"homie":"5.0","version":1,"nodes":{{"n":{{"properties":{{"ok":{{"datatype":"string"}},"p":{prop}}}}}}}}}"#
        )
    }

    #[test]
    fn illegal_property_is_left_out() {
        use DescriptionError as E;
        let cases: &[(&str, E)] = &[
            ("[]", E::PropertyInvalid),
            ("{}", E::DatatypeMissing),
            (r#"{"datatype":"number"}"#, E::DatatypeUnknown),
            (r#"{"datatype":"vector"}"#, E::DatatypeUnknown),
            (r#"{"datatype":"json","format":{}}"#, E::FormatInvalid),
            (r#"{"datatype":"enum"}"#, E::FormatMissing),
            (r#"{"datatype":"enum","format":""}"#, E::FormatMissing),
            (r#"{"datatype":"color"}"#, E::FormatMissing),
            (r#"{"datatype":"color","format":""}"#, E::FormatMissing),
            (r#"{"datatype":"enum","format":"a,,b"}"#, E::FormatIllegal),
            (r#"{"datatype":"enum","format":"a,b,a"}"#, E::FormatIllegal),
            (r#"{"datatype":"enum","format":","}"#, E::FormatIllegal),
            (
                r#"{"datatype":"color","format":"rgb,cmyk"}"#,
                E::FormatIllegal,
            ),
            (r#"{"datatype":"color","format":"RGB"}"#, E::FormatIllegal),
            (r#"{"datatype":"boolean","format":"on"}"#, E::FormatIllegal),
            (
                r#"{"datatype":"boolean","format":"a,b,c"}"#,
                E::FormatIllegal,
            ),
            (
                r#"{"datatype":"boolean","format":"on,on"}"#,
                E::FormatIllegal,
            ),
            (
                r#"{"datatype":"integer","format":"abc:1:-3"}"#,
                E::FormatIllegal,
            ),
            (r#"{"datatype":"integer","format":"5"}"#, E::FormatIllegal),
            (
                r#"{"datatype":"integer","format":"1.5:2"}"#,
                E::FormatIllegal,
            ),
            (
                r#"{"datatype":"integer","format":"0:10:0"}"#,
                E::FormatIllegal,
            ),
            (
                r#"{"datatype":"integer","format":"0:10:-1"}"#,
                E::FormatIllegal,
            ),
            (
                r#"{"datatype":"integer","format":"0:10:"}"#,
                E::FormatIllegal,
            ),
            (
                r#"{"datatype":"integer","format":"0:1:1:1"}"#,
                E::FormatIllegal,
            ),
            (
                r#"{"datatype":"integer","format":"0:9223372036854775808"}"#,
                E::FormatIllegal,
            ),
            (
                r#"{"datatype":"float","format":"0:1e999"}"#,
                E::FormatIllegal,
            ),
            (r#"{"datatype":"float","format":"x:y"}"#, E::FormatIllegal),
            (
                r#"{"datatype":"string","settable":"true"}"#,
                E::SettableInvalid,
            ),
            (r#"{"datatype":"string","retained":0}"#, E::RetainedInvalid),
            (r#"{"datatype":"string","unit":null}"#, E::UnitInvalid),
            (r#"{"datatype":"string","name":1}"#, E::NameInvalid),
        ];
        for (prop, want) in cases {
            let (d, ignored) = parse_with_ignored(&with_property(prop)).unwrap();
            let ids: Vec<&str> = d.nodes[0]
                .1
                .properties
                .iter()
                .map(|(id, _)| id.as_str())
                .collect();
            assert_eq!(ids, ["ok"], "{prop}");
            assert_eq!(
                ignored,
                [Ignored {
                    node: "n".to_string(),
                    property: Some("p".to_string()),
                    error: *want,
                }],
                "{prop}"
            );
        }
    }

    #[test]
    fn legal_formats_are_kept() {
        for prop in [
            r#"{"datatype":"enum","format":" a,a "}"#,
            r#"{"datatype":"color","format":"xyz,rgb,hsv"}"#,
            r#"{"datatype":"boolean","format":"off,on"}"#,
            r#"{"datatype":"boolean","format":""}"#,
            r#"{"datatype":"integer","format":":"}"#,
            r#"{"datatype":"integer","format":"-5:35:2.5"}"#,
            r#"{"datatype":"integer","format":""}"#,
            r#"{"datatype":"float","format":"10.123:15.123"}"#,
            r#"{"datatype":"float","format":"0::1e-400"}"#,
            r#"{"datatype":"float","format":":-1e3"}"#,
            r#"{"datatype":"string","format":"anything"}"#,
            r#"{"datatype":"json","format":"not a schema"}"#,
        ] {
            let (d, ignored) = parse_with_ignored(&with_property(prop)).unwrap();
            assert_eq!(ignored, [], "{prop}");
            assert_eq!(d.nodes[0].1.properties.len(), 2, "{prop}");
        }
    }

    #[test]
    fn illegal_node_is_left_out() {
        let json = r#"{"homie":"5.0","version":1,"nodes":{
            "n":{"properties":{"ok":{"datatype":"integer"},"p":{"datatype":"vector"}}},
            "m":{"properties":{"q":{"datatype":"string"}}},
            "N":{},
            "a":1,
            "b":{"properties":[]},
            "c":{"name":null},
            "d":{"type":3},
            "e":{"properties":{"P":{"datatype":"string"}}}}}"#;
        let (d, ignored) = parse_with_ignored(json).unwrap();
        let kept: Vec<(&str, Vec<&str>)> = d
            .nodes
            .iter()
            .map(|(id, n)| {
                (
                    id.as_str(),
                    n.properties.iter().map(|(p, _)| p.as_str()).collect(),
                )
            })
            .collect();
        assert_eq!(kept, [("n", vec!["ok"]), ("m", vec!["q"]), ("e", vec![])]);
        use DescriptionError as E;
        let ignored: Vec<(&str, Option<&str>, E)> = ignored
            .iter()
            .map(|i| (i.node.as_str(), i.property.as_deref(), i.error))
            .collect();
        assert_eq!(
            ignored,
            [
                ("n", Some("p"), E::DatatypeUnknown),
                ("N", None, E::NodeIdInvalid),
                ("a", None, E::NodeInvalid),
                ("b", None, E::PropertiesInvalid),
                ("c", None, E::NameInvalid),
                ("d", None, E::TypeInvalid),
                ("e", Some("P"), E::PropertyIdInvalid),
            ]
        );
    }

    #[test]
    fn parse_keeps_document_order() {
        let json = r#"{"homie": "5.0", "version": 7, "nodes": {"zeta": {"properties": {"zeta": {"datatype": "integer"}, "alpha": {"datatype": "integer"}, "mid": {"datatype": "integer"}}}, "alpha": {"properties": {}}, "mid": {"properties": {}}}, "children": [], "extensions": [], "z-extra": 1, "a-extra": {"a": 1, "b": 2}, "m": 2}"#;
        let d = parse(json).unwrap();
        let ids = |items: &[(String, NodeDescription)]| -> Vec<String> {
            items.iter().map(|(id, _)| id.clone()).collect()
        };
        assert_eq!(ids(&d.nodes), ["zeta", "alpha", "mid"]);
        let props: Vec<&str> = d.nodes[0]
            .1
            .properties
            .iter()
            .map(|(id, _)| id.as_str())
            .collect();
        assert_eq!(props, ["zeta", "alpha", "mid"]);
        let extras: Vec<&str> = d.extras.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(extras, ["z-extra", "a-extra", "m"]);
        assert_eq!(d.to_json(), json);
    }

    /// Python `json.loads` keeps a repeated key's last value at its first
    /// position, at every level.
    #[test]
    fn parse_repeated_key_keeps_last_value() {
        let json = r#"{"homie":"4.0","version":1,"nodes":{
            "b":{"properties":{"q":{"datatype":"vector"},"p":{"datatype":"string"},"q":{"datatype":"integer"}}},
            "a":1,
            "b":{"name":"B","properties":{"y":{"datatype":"float"},"x":{"datatype":"json"},"y":{"datatype":"string"}}}},
            "x":1,"w":2,"x":3,"homie":"5.0","nodes":{
            "b":{"properties":{"y":{"datatype":"float"},"x":{"datatype":"json"},"y":{"datatype":"string"}},"name":"B"},
            "a":{},"b":{"name":"B","properties":{"y":{"datatype":"float"},"x":{"datatype":"json"},"y":{"datatype":"string"}}}}}"#;
        let d = parse(json).unwrap();
        let ids: Vec<&str> = d.nodes.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, ["b", "a"]);
        let b = &d.nodes[0].1;
        assert_eq!(b.name.as_deref(), Some("B"));
        let props: Vec<(&str, Datatype)> = b
            .properties
            .iter()
            .map(|(p, d)| (p.as_str(), d.datatype))
            .collect();
        assert_eq!(props, [("y", Datatype::String), ("x", Datatype::Json)]);
        assert_eq!(d.nodes[1].1, NodeDescription::default());
        assert_eq!(
            d.extras,
            [
                ("x".to_string(), Value::from(3)),
                ("w".to_string(), Value::from(2))
            ]
        );
        // A repeated `homie` is judged by its last value.
        assert_eq!(
            parse(r#"{"homie":"5.0","version":1,"homie":"4.0"}"#),
            Err(DescriptionError::HomieInvalid)
        );
    }

    const PY_ROOT_JSON: &str = r#"{"homie": "5.0", "version": 1696986812873435, "type": "ebus/panel", "name": "Panel 1", "nodes": {"meter": {"name": "Meter", "properties": {"power": {"name": "power", "datatype": "float", "format": "-100:100", "unit": "W"}, "mode": {"name": "Mode \u00b0C", "datatype": "enum", "format": "on,off", "settable": true, "retained": false}, "blank": {"name": "blank", "datatype": "string"}}}}, "children": ["bridge"], "extensions": ["eu.ebus.x:1.0:[5.x]"]}"#;
    const PY_ROOT_VERSION: i128 = 1696986812873435;
    const PY_CHILD_JSON: &str = r#"{"homie": "5.0", "version": 2077292240377668, "type": "ebus/circuit", "name": "Circuit \ud83d\ude00", "nodes": {"relay": {"name": "relay", "type": "switch", "properties": {}}}, "children": [], "root": "panel-1", "parent": "bridge", "extensions": [], "zeta": {"a": [1, 2], "b": 1.5}, "alpha": 1e+16}"#;
    const PY_CHILD_VERSION: i128 = 2077292240377668;
    const PY_EMPTY_JSON: &str = r#"{"homie": "5.0", "version": 596216508486095, "name": "x", "nodes": {}, "children": [], "extensions": []}"#;
    const PY_EMPTY_VERSION: i128 = 596216508486095;
}
