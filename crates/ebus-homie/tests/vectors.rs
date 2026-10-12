//! Cross-language conformance vectors in `vectors/` (see `vectors/README.md`).
//!
//! Each test runs every case of one suite file and reports all failing case ids.

#![cfg(feature = "alloc")]

use ebus_homie::datatype::{self, Current, Datatype, SetValue};
use ebus_homie::description::{self, DeviceDescription, NodeDescription, PropertyDescription};
use ebus_homie::state::{self, DeviceState};
use ebus_homie::topic::{self, TopicKind};
use ebus_homie::{id, payload, pyfmt};
use serde_json::Value;

fn load(suite: &str) -> Vec<Value> {
    let path = format!("{}/../../vectors/{suite}.json", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    // `from_json` rounds floats correctly, so Python repr text reads back exactly.
    let doc = pyfmt::from_json(&text).unwrap_or_else(|| panic!("{path}: not JSON"));
    assert_eq!(doc["suite"], suite, "{path}: suite name");
    match doc.get("cases") {
        Some(Value::Array(cases)) => cases.clone(),
        _ => panic!("{path}: no cases array"),
    }
}

/// Run `check` on every case of `suite`; fail listing every case that failed.
fn run(suite: &str, check: impl Fn(&Value, &Value) -> Result<(), String>) {
    let cases = load(suite);
    assert!(!cases.is_empty(), "{suite}: no cases");
    let mut failures = Vec::new();
    for case in &cases {
        let case_id = case["id"].as_str().unwrap_or("<no id>");
        if let Err(msg) = check(&case["input"], &case["expect"]) {
            failures.push(format!("  {case_id}: {msg}"));
        }
    }
    println!(
        "{suite}: {} of {} passed",
        cases.len() - failures.len(),
        cases.len()
    );
    assert!(
        failures.is_empty(),
        "{suite}: {} of {} cases failed:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

fn field<'a>(v: &'a Value, key: &str) -> Result<&'a Value, String> {
    v.get(key).ok_or_else(|| format!("missing field {key:?}"))
}

fn str_field<'a>(v: &'a Value, key: &str) -> Result<&'a str, String> {
    field(v, key)?
        .as_str()
        .ok_or_else(|| format!("field {key:?} is not a string"))
}

fn opt_str(v: &Value, key: &str) -> Result<Option<String>, String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(other) => Err(format!("field {key:?} is not a string or null: {other}")),
    }
}

fn hex_decode(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err(format!("odd-length hex {hex:?}"));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|e| format!("hex {hex:?}: {e}")))
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The payload bytes of an input: `payload` (text) or `payload_hex`.
fn payload_bytes(input: &Value) -> Result<Vec<u8>, String> {
    if let Some(hex) = input.get("payload_hex") {
        hex_decode(hex.as_str().ok_or("payload_hex is not a string")?)
    } else {
        Ok(str_field(input, "payload")?.as_bytes().to_vec())
    }
}

fn payload_text(input: &Value) -> Result<String, String> {
    String::from_utf8(payload_bytes(input)?).map_err(|e| format!("payload is not UTF-8: {e}"))
}

fn f64_from_bits_hex(hex: &str) -> Result<f64, String> {
    u64::from_str_radix(hex, 16)
        .map(f64::from_bits)
        .map_err(|e| format!("f64 bits {hex:?}: {e}"))
}

fn expect_eq<T: PartialEq + std::fmt::Debug>(got: T, want: T) -> Result<(), String> {
    if got == want {
        Ok(())
    } else {
        Err(format!("got {got:?}, want {want:?}"))
    }
}

/// Datatype by name; an unknown name is `string`, as in Python.
fn datatype_of(input: &Value) -> Result<Datatype, String> {
    Ok(Datatype::parse(str_field(input, "datatype")?).unwrap_or(Datatype::String))
}

/// Compare a `parse_set_payload` result with an expected value `V`.
fn compare_set_value(got: &SetValue, want: &Value) -> Result<(), String> {
    match got {
        SetValue::Json(v) => expect_eq(v, want),
        SetValue::Text(s) | SetValue::String(s) => match want {
            Value::String(w) => expect_eq(s, w),
            other => Err(format!("got {got:?}, want {other}")),
        },
    }
}

fn state_of(v: &Value, key: &str) -> Result<DeviceState, String> {
    let s = str_field(v, key)?;
    DeviceState::parse(s).ok_or_else(|| format!("unknown state {s:?}"))
}

#[test]
fn topics() {
    run("topics", |input, expect| {
        let parsed = topic::parse(str_field(input, "topic")?);
        let valid = field(expect, "valid")?
            .as_bool()
            .ok_or("valid not a bool")?;
        let t = match (parsed, valid) {
            (Err(_), false) => return Ok(()),
            (Ok(t), false) => return Err(format!("accepted as {t:?}, want rejection")),
            (Err(e), true) => return Err(format!("rejected with {}, want accepted", e.as_str())),
            (Ok(t), true) => t,
        };
        expect_eq(t.domain, str_field(expect, "domain")?)?;
        expect_eq(t.device.map(str::to_string), opt_str(expect, "device")?)?;
        let kind = str_field(expect, "kind")?;
        let ok = match (kind, t.kind) {
            ("DeviceState", TopicKind::DeviceState)
            | ("DeviceDescription", TopicKind::DeviceDescription) => true,
            ("DeviceLog", TopicKind::DeviceLog { level }) => level == str_field(expect, "level")?,
            ("DeviceAlert", TopicKind::DeviceAlert { alert_id }) => {
                alert_id == str_field(expect, "alert_id")?
            }
            ("PropertyValue", TopicKind::PropertyValue { node, property })
            | ("PropertySet", TopicKind::PropertySet { node, property })
            | ("PropertyTarget", TopicKind::PropertyTarget { node, property }) => {
                node == str_field(expect, "node")? && property == str_field(expect, "property")?
            }
            ("Broadcast", TopicKind::Broadcast { subtopic }) => {
                subtopic == str_field(expect, "subtopic")?
            }
            _ => false,
        };
        if ok {
            Ok(())
        } else {
            Err(format!("got {:?}, want {expect}", t.kind))
        }
    });
}

#[test]
fn empty_string() {
    run("empty-string", |input, expect| {
        match str_field(input, "direction")? {
            "decode" => {
                let bytes = payload_bytes(input)?;
                match payload::classify(&bytes) {
                    Err(_) => expect_eq(opt_str(expect, "error")?.as_deref(), Some("invalidUtf8")),
                    Ok(payload::Received::Deleted) => {
                        expect_eq(expect.get("deleted"), Some(&Value::Bool(true)))
                    }
                    Ok(payload::Received::Text(s)) => {
                        expect_eq(expect.get("deleted"), Some(&Value::Bool(false)))?;
                        expect_eq(s, str_field(expect, "value")?)
                    }
                }
            }
            "encode" => expect_eq(
                hex_encode(payload::encode_str_bytes(str_field(input, "value")?)),
                str_field(expect, "payload_hex")?.to_string(),
            ),
            "set" => {
                let text = payload_text(input)?;
                let format = opt_str(input, "format")?;
                match datatype::parse_set_payload(
                    &text,
                    datatype_of(input)?,
                    format.as_deref(),
                    None,
                ) {
                    Ok(v) => Err(format!("accepted as {v:?}, want rejection")),
                    Err(p) => expect_eq(p.as_str(), str_field(expect, "problem")?),
                }
            }
            other => Err(format!("unknown direction {other:?}")),
        }
    });
}

#[test]
fn effective_state() {
    run("effective-state", |input, expect| {
        let has_root = field(input, "has_root")?
            .as_bool()
            .ok_or("has_root not a bool")?;
        let root = if has_root {
            Some(state_of(input, "root_state")?)
        } else {
            None
        };
        let got = state::effective_state(root, state_of(input, "own_state")?);
        expect_eq(got, state_of(expect, "state")?)
    });
}

fn payload_suite(suite: &str) {
    run(suite, |input, expect| {
        let text = payload_text(input)?;
        let format = opt_str(input, "format")?;
        let got = datatype::parse_set_payload(&text, datatype_of(input)?, format.as_deref(), None);
        let valid = field(expect, "valid")?
            .as_bool()
            .ok_or("valid not a bool")?;
        match (got, valid) {
            (Ok(v), true) => compare_set_value(&v, field(expect, "value")?),
            (Ok(v), false) => Err(format!(
                "accepted as {v:?}, want {}",
                str_field(expect, "problem")?
            )),
            (Err(p), true) => Err(format!("rejected with {}, want accepted", p.as_str())),
            (Err(p), false) => expect_eq(p.as_str(), str_field(expect, "problem")?),
        }
    });
}

#[test]
fn payloads_integer() {
    payload_suite("payloads/integer");
}

#[test]
fn payloads_float() {
    payload_suite("payloads/float");
}

#[test]
fn payloads_boolean() {
    payload_suite("payloads/boolean");
}

#[test]
fn payloads_string() {
    payload_suite("payloads/string");
}

#[test]
fn payloads_enum() {
    payload_suite("payloads/enum");
}

#[test]
fn payloads_color() {
    payload_suite("payloads/color");
}

#[test]
fn payloads_datetime() {
    payload_suite("payloads/datetime");
}

#[test]
fn payloads_duration() {
    payload_suite("payloads/duration");
}

#[test]
fn payloads_json() {
    payload_suite("payloads/json");
}

/// A received value: decode the wire payload as a controller does, then check
/// it against a property of the case's datatype and format.
#[test]
fn check_value() {
    run("check-value", |input, expect| {
        let bytes = payload_bytes(input)?;
        let text = match payload::classify(&bytes) {
            Ok(payload::Received::Text(text)) => text,
            other => return Err(format!("payload is not a value: {other:?}")),
        };
        let mut property = PropertyDescription::new(datatype_of(input)?);
        property.format = opt_str(input, "format")?;
        let got = datatype::check_value(text, &property);
        let valid = field(expect, "valid")?
            .as_bool()
            .ok_or("valid not a bool")?;
        match (got, valid) {
            (Ok(()), true) => Ok(()),
            (Ok(()), false) => Err(format!("accepted, want {}", str_field(expect, "problem")?)),
            (Err(p), true) => Err(format!("rejected with {}, want accepted", p.as_str())),
            (Err(p), false) => expect_eq(p.as_str(), str_field(expect, "problem")?),
        }
    });
}

#[test]
fn description_parse() {
    run("description", |input, expect| {
        let got = description::parse_with_ignored(str_field(input, "payload")?);
        let valid = field(expect, "valid")?
            .as_bool()
            .ok_or("valid not a bool")?;
        match (got, valid) {
            (Err(_), false) => Ok(()),
            (Ok((d, ignored)), true) => {
                let want = match expect.get("ignored") {
                    None => Vec::new(),
                    Some(Value::Array(items)) => items
                        .iter()
                        .map(|i| Ok((str_field(i, "node")?.to_string(), opt_str(i, "property")?)))
                        .collect::<Result<Vec<_>, String>>()?,
                    Some(other) => return Err(format!("ignored is not an array: {other}")),
                };
                let got: Vec<(String, Option<String>)> = ignored
                    .iter()
                    .map(|i| (i.node.clone(), i.property.clone()))
                    .collect();
                expect_eq(&got, &want)?;
                // Nothing ignored is left in the description.
                for (node, prop) in &want {
                    let kept = d.nodes.iter().find(|(id, _)| id == node);
                    let present = match (kept, prop) {
                        (None, _) => false,
                        (Some(_), None) => true,
                        (Some((_, n)), Some(p)) => n.properties.iter().any(|(id, _)| id == p),
                    };
                    if present {
                        return Err(format!("ignored {node}/{prop:?} is still present"));
                    }
                }
                Ok(())
            }
            (Ok((d, _)), false) => Err(format!(
                "accepted as {d:?}, want rejection ({})",
                expect.get("error").unwrap_or(&Value::Null)
            )),
            (Err(e), true) => Err(format!("rejected with {}, want accepted", e.as_str())),
        }
    });
}

#[test]
fn python_sdk_float_format() {
    run("python-sdk/float-format", |input, expect| {
        let x = f64_from_bits_hex(str_field(input, "f64_bits")?)?;
        expect_eq(pyfmt::format_float(x), opt_str(expect, "payload")?)
    });
}

#[test]
fn python_sdk_float32_format() {
    run("python-sdk/float32-format", |input, expect| {
        let hex = str_field(input, "f32_bits")?;
        let x = u32::from_str_radix(hex, 16)
            .map(f32::from_bits)
            .map_err(|e| format!("f32 bits {hex:?}: {e}"))?;
        expect_eq(pyfmt::format_float32(x), opt_str(expect, "payload")?)
    });
}

#[test]
fn python_sdk_sanitize_id() {
    run("python-sdk/sanitize-id", |input, expect| {
        expect_eq(
            id::sanitize_id(str_field(input, "value")?),
            str_field(expect, "id")?.to_string(),
        )
    });
}

#[test]
fn python_sdk_set_payload() {
    run("python-sdk/set-payload", |input, expect| {
        let text = payload_text(input)?;
        let format = opt_str(input, "format")?;
        let current = current_of(input)?;
        let got =
            datatype::parse_set_payload(&text, datatype_of(input)?, format.as_deref(), current);
        match (got, expect.get("ok"), expect.get("problem")) {
            (Ok(v), Some(want), None) => compare_set_value(&v, want),
            (Err(p), None, Some(want)) => expect_eq(Value::from(p.as_str()), want.clone()),
            (got, _, _) => Err(format!("got {got:?}, want {expect}")),
        }
    });
}

/// The `current` of a `/set` vector: an exact integer, or a float by its bits.
fn current_of(input: &Value) -> Result<Option<Current>, String> {
    match field(input, "current")? {
        Value::Null => Ok(None),
        Value::Number(n) => {
            if let Some(bits) = input.get("current_f64_bits") {
                let bits = bits.as_str().ok_or("current_f64_bits not a string")?;
                Ok(Some(Current::Float(f64_from_bits_hex(bits)?)))
            } else if let Some(i) = n.as_i64() {
                Ok(Some(Current::Integer(i)))
            } else {
                Err(format!("current {n} has no exact form"))
            }
        }
        other => Err(format!("current is not a number: {other}")),
    }
}

/// The `/set` verdict suite through `Tree::handle_message`: a settable
/// property with the case's datatype and format, holding its current value,
/// receives the payload. Cases a device cannot express are skipped: a format
/// the description round trip refuses, a current value outside the format,
/// and a zero-length payload (which deletes, so it never reaches a `/set`
/// parser as text).
#[test]
fn python_sdk_set_payload_through_tree() {
    use ebus_homie::device::{
        DeviceEvent, DeviceSpec, NodeSpec, PropSpec, Tree, TreeConfig, ValueRef,
    };
    use ebus_mqtt::{Instant, Link, Message};
    let ran = std::cell::Cell::new(0usize);
    run("python-sdk/set-payload", |input, expect| {
        let payload = payload_bytes(input)?;
        let datatype = datatype_of(input)?;
        if payload.is_empty() || str_field(input, "datatype")? != datatype.as_str() {
            return Ok(());
        }
        let format = opt_str(input, "format")?;
        let mut spec = PropSpec::new().settable(true);
        spec.format = format;
        let mut tree = Tree::new(TreeConfig::default(), "d", DeviceSpec::default())
            .map_err(|e| e.to_string())?;
        let Ok(slot) = tree.reconfigure(|r| {
            let n = r.add_node(r.root(), "n", NodeSpec::default())?;
            r.add_property(n, "p", datatype, None, spec)
        }) else {
            return Ok(());
        };
        let set = match current_of(input)? {
            Some(Current::Integer(i)) => tree.set_value(slot, ValueRef::Integer(i)),
            Some(Current::Float(f)) => tree.set_value(slot, ValueRef::Float(f)),
            None => Ok(ebus_homie::device::Change::Unchanged),
        };
        if set.is_err() {
            return Ok(());
        }
        tree.handle_link(Instant(0), Link::up(false));
        while tree.poll_command().is_some() {}
        while tree.poll_event().is_some() {}
        tree.handle_message(
            Instant(0),
            Message::new("ebus/5/d/n/p/set", &payload, Some(false)),
        );
        ran.set(ran.get() + 1);
        let event = tree.poll_event();
        if tree.poll_command().is_some() {
            return Err("a /set published something".to_string());
        }
        match (event, expect.get("ok"), expect.get("problem")) {
            (Some(DeviceEvent::Set { value, raw, .. }), Some(want), None) => {
                expect_eq(&*raw, payload.as_slice())?;
                compare_set_value(&value, want)
            }
            (None, None, Some(_)) => expect_eq(tree.rejected_sets(), 1),
            (got, _, _) => Err(format!("got {got:?}, want {expect}")),
        }
    });
    println!("set-payload through Tree: {} cases ran", ran.get());
    assert!(ran.get() >= 70, "only {} cases ran", ran.get());
}

#[test]
fn python_sdk_json_dumps() {
    run("python-sdk/json-dumps", |input, expect| {
        let value = pyfmt::from_json(str_field(input, "json")?).ok_or("input is not JSON")?;
        expect_eq(
            pyfmt::to_json(&value, true),
            str_field(expect, "text")?.to_string(),
        )
    });
}

#[test]
fn python_sdk_encode_duration() {
    run("python-sdk/encode-duration", |input, expect| {
        let secs = field(input, "total_seconds")?
            .as_u64()
            .ok_or("total_seconds not a u64")?;
        let micros = field(input, "microseconds")?
            .as_u64()
            .and_then(|m| u32::try_from(m).ok())
            .ok_or("microseconds not a u32")?;
        expect_eq(
            datatype::encode_duration(secs, micros),
            str_field(expect, "payload")?.to_string(),
        )
    });
}

fn array<'a>(v: &'a Value, key: &str) -> Result<&'a Vec<Value>, String> {
    field(v, key)?
        .as_array()
        .ok_or_else(|| format!("field {key:?} is not an array"))
}

fn strings(v: &Value, key: &str) -> Result<Vec<String>, String> {
    array(v, key)?
        .iter()
        .map(|s| {
            s.as_str()
                .map(str::to_string)
                .ok_or(format!("{key}: non-string"))
        })
        .collect()
}

fn bool_field(v: &Value, key: &str) -> Result<bool, String> {
    field(v, key)?
        .as_bool()
        .ok_or_else(|| format!("field {key:?} is not a bool"))
}

fn build_description(input: &Value) -> Result<DeviceDescription, String> {
    let mut nodes = Vec::new();
    for node in array(input, "nodes")? {
        let mut properties = Vec::new();
        for prop in array(node, "properties")? {
            let dt = str_field(prop, "datatype")?;
            let mut p = PropertyDescription::new(
                Datatype::parse(dt).ok_or_else(|| format!("unknown datatype {dt:?}"))?,
            );
            p.name = opt_str(prop, "name")?;
            p.format = opt_str(prop, "format")?;
            p.settable = bool_field(prop, "settable")?;
            p.retained = bool_field(prop, "retained")?;
            p.unit = opt_str(prop, "unit")?;
            properties.push((str_field(prop, "id")?.to_string(), p));
        }
        let mut n = NodeDescription::default();
        n.name = opt_str(node, "name")?;
        n.r#type = opt_str(node, "type")?;
        n.properties = properties;
        nodes.push((str_field(node, "id")?.to_string(), n));
    }
    let mut extras = Vec::new();
    for pair in array(input, "extras")? {
        match pair.as_array().map(Vec::as_slice) {
            Some([Value::String(k), v]) => extras.push((k.clone(), v.clone())),
            _ => return Err(format!("bad extras entry {pair}")),
        }
    }
    let mut d = DeviceDescription::default();
    d.r#type = opt_str(input, "type")?;
    d.name = opt_str(input, "name")?;
    d.nodes = nodes;
    d.children = strings(input, "children")?;
    d.root = opt_str(input, "root")?;
    d.parent = opt_str(input, "parent")?;
    d.extensions = strings(input, "extensions")?;
    d.extras = extras;
    Ok(d)
}

#[test]
fn python_sdk_description_wire() {
    run("python-sdk/description-wire", |input, expect| {
        let d = build_description(input)?;
        let mut errors = Vec::new();
        let hash = hex_encode(&d.content_hash());
        if hash != str_field(expect, "content_hash_hex")? {
            errors.push(format!("content_hash {hash}"));
        }
        let want_version = field(expect, "version")?
            .as_u64()
            .ok_or("version not a u64")?;
        let version = d.compute_version();
        if version != want_version {
            errors.push(format!("version {version}, want {want_version}"));
        }
        let wire = d.with_computed_version().to_json();
        let want_wire = str_field(expect, "wire")?;
        if wire != want_wire {
            errors.push(format!("wire\n    got  {wire}\n    want {want_wire}"));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    });
}

#[test]
fn python_sdk_description_roundtrip() {
    run("python-sdk/description-roundtrip", |input, expect| {
        let payload = str_field(input, "payload")?;
        let (d, ignored) = description::parse_with_ignored(payload)
            .map_err(|e| format!("rejected: {}", e.as_str()))?;
        if !ignored.is_empty() {
            return Err(format!("ignored {ignored:?}"));
        }
        expect_eq(d.to_json(), str_field(expect, "wire")?.to_string())
    });
}
