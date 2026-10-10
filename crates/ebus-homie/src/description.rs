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
use crate::datatype::Datatype;
use crate::id::is_valid_id;
use crate::pyfmt::{to_json as value_to_json, write_str, write_value};

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
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq, Default)]
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
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DeviceDescription {
    /// Document version; see [`DeviceDescription::compute_version`].
    pub version: u64,
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
    /// `version` is not a non-negative integer that fits in 64 bits.
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
    /// An `enum` or `color` property has no `format` (convention.md: Formats).
    FormatMissing,
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

#[cfg(feature = "std")]
impl std::error::Error for DescriptionError {}

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
        self.version = self.compute_version();
        self
    }
}

fn opt_str(
    obj: &Map<String, Value>,
    key: &str,
    err: DescriptionError,
) -> Result<Option<String>, DescriptionError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(err),
    }
}

fn opt_bool(
    obj: &Map<String, Value>,
    key: &str,
    default: bool,
    err: DescriptionError,
) -> Result<bool, DescriptionError> {
    match obj.get(key) {
        None => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => Err(err),
    }
}

fn opt_id(
    obj: &Map<String, Value>,
    key: &str,
    err: DescriptionError,
) -> Result<Option<String>, DescriptionError> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) if is_valid_id(s) => Ok(Some(s.clone())),
        Some(_) => Err(err),
    }
}

/// True for `5.x` with `x` one or more ASCII digits (convention.md: Device
/// Attributes, `homie`).
fn is_homie_5(s: &str) -> bool {
    s.strip_prefix("5.")
        .is_some_and(|minor| !minor.is_empty() && minor.bytes().all(|b| b.is_ascii_digit()))
}

fn parse_property(value: &Value) -> Result<PropertyDescription, DescriptionError> {
    let obj = value.as_object().ok_or(DescriptionError::PropertyInvalid)?;
    let datatype = match obj.get("datatype") {
        None => return Err(DescriptionError::DatatypeMissing),
        Some(Value::String(s)) => Datatype::parse(s).ok_or(DescriptionError::DatatypeUnknown)?,
        Some(_) => return Err(DescriptionError::DatatypeUnknown),
    };
    let format = opt_str(obj, "format", DescriptionError::FormatInvalid)?;
    if format.is_none() && matches!(datatype, Datatype::Enum | Datatype::Color) {
        return Err(DescriptionError::FormatMissing);
    }
    Ok(PropertyDescription {
        name: opt_str(obj, "name", DescriptionError::NameInvalid)?,
        datatype,
        format,
        settable: opt_bool(obj, "settable", false, DescriptionError::SettableInvalid)?,
        retained: opt_bool(obj, "retained", true, DescriptionError::RetainedInvalid)?,
        unit: opt_str(obj, "unit", DescriptionError::UnitInvalid)?,
    })
}

fn parse_node(value: &Value) -> Result<NodeDescription, DescriptionError> {
    let obj = value.as_object().ok_or(DescriptionError::NodeInvalid)?;
    let mut properties = Vec::new();
    match obj.get("properties") {
        None => {}
        Some(Value::Object(props)) => {
            for (id, prop) in props {
                if !is_valid_id(id) {
                    return Err(DescriptionError::PropertyIdInvalid);
                }
                properties.push((id.clone(), parse_property(prop)?));
            }
        }
        Some(_) => return Err(DescriptionError::PropertiesInvalid),
    }
    Ok(NodeDescription {
        name: opt_str(obj, "name", DescriptionError::NameInvalid)?,
        r#type: opt_str(obj, "type", DescriptionError::TypeInvalid)?,
        properties,
    })
}

/// Parse and validate a received `$description` (convention.md: Device
/// Attributes, Nodes, Properties).
///
/// Absent optional fields take their convention defaults. Unknown node and
/// property fields are ignored; unknown top-level fields are kept in
/// [`DeviceDescription::extras`]. Nodes, properties and extras come out in
/// `serde_json::Map` iteration order.
pub fn parse(json: &str) -> Result<DeviceDescription, DescriptionError> {
    let value: Value = serde_json::from_str(json).map_err(|_| DescriptionError::InvalidJson)?;
    let Value::Object(obj) = value else {
        return Err(DescriptionError::NotAnObject);
    };
    match obj.get("homie") {
        None => return Err(DescriptionError::HomieMissing),
        Some(Value::String(s)) if is_homie_5(s) => {}
        Some(_) => return Err(DescriptionError::HomieInvalid),
    }
    let version = match obj.get("version") {
        None => return Err(DescriptionError::VersionMissing),
        Some(Value::Number(n)) => n.as_u64().ok_or(DescriptionError::VersionInvalid)?,
        Some(_) => return Err(DescriptionError::VersionInvalid),
    };
    let mut nodes = Vec::new();
    match obj.get("nodes") {
        None => {}
        Some(Value::Object(map)) => {
            for (id, node) in map {
                if !is_valid_id(id) {
                    return Err(DescriptionError::NodeIdInvalid);
                }
                nodes.push((id.clone(), parse_node(node)?));
            }
        }
        Some(_) => return Err(DescriptionError::NodesInvalid),
    }
    let children = match obj.get("children") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| match v {
                Value::String(s) if is_valid_id(s) => Ok(s.clone()),
                _ => Err(DescriptionError::ChildrenInvalid),
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err(DescriptionError::ChildrenInvalid),
    };
    let extensions = match obj.get("extensions") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| match v {
                Value::String(s) => Ok(s.clone()),
                _ => Err(DescriptionError::ExtensionsInvalid),
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err(DescriptionError::ExtensionsInvalid),
    };
    let root = opt_id(&obj, "root", DescriptionError::RootInvalid)?;
    let parent = opt_id(&obj, "parent", DescriptionError::ParentInvalid)?;
    if parent.is_some() && root.is_none() {
        return Err(DescriptionError::ParentWithoutRoot);
    }
    let name = opt_str(&obj, "name", DescriptionError::NameInvalid)?;
    let r#type = opt_str(&obj, "type", DescriptionError::TypeInvalid)?;
    let extras = obj
        .iter()
        .filter(|(k, _)| !CORE_FIELDS.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Ok(DeviceDescription {
        version,
        r#type,
        name,
        nodes,
        children,
        root,
        parent,
        extensions,
        extras,
    })
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
        assert!(d.version < (1u64 << 53));
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
        assert_eq!(parsed.compute_version(), d.version);
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
            (r#"{"homie":"5.0","version":-1}"#, E::VersionInvalid),
            (r#"{"homie":"5.0","version":1.5}"#, E::VersionInvalid),
            (r#"{"homie":"5.0","version":"1"}"#, E::VersionInvalid),
            (r#"{"homie":"5.0","version":1,"name":null}"#, E::NameInvalid),
            (r#"{"homie":"5.0","version":1,"type":2}"#, E::TypeInvalid),
            (r#"{"homie":"5.0","version":1,"nodes":[]}"#, E::NodesInvalid),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"N":{}}}"#,
                E::NodeIdInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":1}}"#,
                E::NodeInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":[]}}}"#,
                E::PropertiesInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p_":{}}}}}"#,
                E::PropertyIdInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":[]}}}}"#,
                E::PropertyInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{}}}}}"#,
                E::DatatypeMissing,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{"datatype":"number"}}}}}"#,
                E::DatatypeUnknown,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{"datatype":"json","format":{}}}}}}"#,
                E::FormatInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{"datatype":"enum"}}}}}"#,
                E::FormatMissing,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{"datatype":"color"}}}}}"#,
                E::FormatMissing,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{"datatype":"string","settable":"true"}}}}}"#,
                E::SettableInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{"datatype":"string","retained":0}}}}}"#,
                E::RetainedInvalid,
            ),
            (
                r#"{"homie":"5.0","version":1,"nodes":{"n":{"properties":{"p":{"datatype":"string","unit":null}}}}}"#,
                E::UnitInvalid,
            ),
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

    const PY_ROOT_JSON: &str = r#"{"homie": "5.0", "version": 1696986812873435, "type": "ebus/panel", "name": "Panel 1", "nodes": {"meter": {"name": "Meter", "properties": {"power": {"name": "power", "datatype": "float", "format": "-100:100", "unit": "W"}, "mode": {"name": "Mode \u00b0C", "datatype": "enum", "format": "on,off", "settable": true, "retained": false}, "blank": {"name": "blank", "datatype": "string"}}}}, "children": ["bridge"], "extensions": ["eu.ebus.x:1.0:[5.x]"]}"#;
    const PY_ROOT_VERSION: u64 = 1696986812873435;
    const PY_CHILD_JSON: &str = r#"{"homie": "5.0", "version": 2077292240377668, "type": "ebus/circuit", "name": "Circuit \ud83d\ude00", "nodes": {"relay": {"name": "relay", "type": "switch", "properties": {}}}, "children": [], "root": "panel-1", "parent": "bridge", "extensions": [], "zeta": {"a": [1, 2], "b": 1.5}, "alpha": 1e+16}"#;
    const PY_CHILD_VERSION: u64 = 2077292240377668;
    const PY_EMPTY_JSON: &str = r#"{"homie": "5.0", "version": 596216508486095, "name": "x", "nodes": {}, "children": [], "extensions": []}"#;
    const PY_EMPTY_VERSION: u64 = 596216508486095;
}
