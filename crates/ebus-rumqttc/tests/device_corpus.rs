//! Rebuilds every vendored panel-sim capture as a device `Tree`, publishes
//! it to a local mosquitto, and compares what the broker retains with the
//! capture. Skipped when no `mosquitto` binary is found (`MOSQUITTO` names
//! one).
//!
//! Descriptions are compared as parsed documents, without `version`, which
//! each publisher computes for itself. Values are compared byte for byte,
//! with two documented exceptions:
//!
//! - A `float` the capture's publisher wrote in integer layout (`0`,
//!   `-2655`) is published as python-sdk publishes a Python `float` (`0.0`,
//!   `-2655.0`), since a Rust `f64` carries no integer layout; the two must
//!   read back as the same `f64`.
//! - A `json` value is set from a `serde_json::Value`, whose objects this
//!   workspace keeps in key order, so the published object may order its
//!   keys differently; it must parse to the same document at the same
//!   length, which `json.dumps` separators give.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ebus_rumqttc::ebus_homie_tokio::ebus_homie::datatype::Datatype;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::device::{
    Builder, DevIdx, DeviceSpec, NodeSpec, PropSpec, SlotId, Tree, TreeConfig, ValueRef,
};
use ebus_rumqttc::ebus_homie_tokio::ebus_mqtt::{Command, Instant, Link};
use rumqttc::{AsyncClient, MqttOptions, Packet, SubscribeFilter};
use serde_json::{Map, Value};
use support::{Broker, observe, retained, until_message};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// `$description` fields a device spec carries; any other is an extra.
const DEVICE_FIELDS: [&str; 9] = [
    "homie",
    "version",
    "name",
    "type",
    "nodes",
    "children",
    "root",
    "parent",
    "extensions",
];

fn captures() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/panel-sim");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("fixtures/panel-sim")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with("-tree-v1.json"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no captures in {}", dir.display());
    files
}

fn text(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_owned)
}

/// A property's slot and what its values are read as.
struct Slot {
    id: SlotId,
    datatype: Datatype,
    format: Option<String>,
}

/// What a rebuilt capture should retain: descriptions as documents, the
/// rest as bytes.
struct Expected {
    root: String,
    descriptions: BTreeMap<String, Value>,
    payloads: BTreeMap<String, Vec<u8>>,
    /// The `json` values among `payloads`.
    json: BTreeSet<String>,
}

/// The capture's tree as a `Tree` with every captured value set.
fn rebuild(devices: &Map<String, Value>) -> (Tree, Expected) {
    let root = devices
        .iter()
        .find(|(_, d)| d["description"].get("parent").is_none())
        .map(|(id, _)| id.clone())
        .expect("a root");
    let spec = |desc: &Value| DeviceSpec {
        name: text(desc, "name"),
        r#type: text(desc, "type"),
        extensions: desc["extensions"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        extras: desc
            .as_object()
            .expect("description object")
            .iter()
            .filter(|(k, _)| !DEVICE_FIELDS.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    };
    let mut b = Builder::new(
        TreeConfig::default(),
        &root,
        spec(&devices[&root]["description"]),
    )
    .expect("root");
    let mut slots: BTreeMap<String, Slot> = BTreeMap::new();
    let mut descriptions = BTreeMap::new();
    let mut payloads = BTreeMap::new();
    let mut json_topics = BTreeSet::new();
    // Pre-order, children in the order their parent lists them.
    let mut stack: Vec<(String, DevIdx)> = vec![(root.clone(), b.root())];
    while let Some((id, idx)) = stack.pop() {
        let desc = &devices[&id]["description"];
        for (node_id, node) in desc["nodes"].as_object().expect("nodes") {
            let n = b
                .edit()
                .add_node(
                    idx,
                    node_id,
                    NodeSpec {
                        name: text(node, "name"),
                        r#type: text(node, "type"),
                    },
                )
                .expect("node");
            for (prop_id, p) in node["properties"].as_object().expect("properties") {
                let datatype = Datatype::parse(p["datatype"].as_str().expect("datatype"))
                    .expect("known datatype");
                let format = text(p, "format");
                let mut spec = PropSpec::new()
                    .settable(p.get("settable").and_then(Value::as_bool).unwrap_or(false))
                    .retained(p.get("retained").and_then(Value::as_bool).unwrap_or(true));
                spec.name = text(p, "name");
                spec.format = format.clone();
                spec.unit = text(p, "unit");
                let slot = b
                    .edit()
                    .add_property(n, prop_id, datatype, None, spec)
                    .unwrap_or_else(|e| panic!("{id}/{node_id}/{prop_id}: {e}"));
                slots.insert(
                    format!("ebus/5/{id}/{node_id}/{prop_id}"),
                    Slot {
                        id: slot,
                        datatype,
                        format,
                    },
                );
            }
        }
        descriptions.insert(format!("ebus/5/{id}/$description"), desc.clone());
        payloads.insert(format!("ebus/5/{id}/$state"), b"ready".to_vec());
        let children: Vec<String> = desc["children"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let mut added = Vec::new();
        for c in &children {
            let cidx = b
                .edit()
                .add_child(idx, c, spec(&devices[c]["description"]))
                .expect("child");
            added.push((c.clone(), cidx));
        }
        stack.extend(added.into_iter().rev());
    }
    let mut tree = b.finish().expect("tree");
    for (id, d) in devices {
        for kind in ["properties", "numeric_properties"] {
            for (np, v) in d[kind].as_object().into_iter().flatten() {
                let topic = format!("ebus/5/{id}/{np}");
                let payload = v.as_str().expect("payload text");
                let slot = slots
                    .get(&topic)
                    .unwrap_or_else(|| panic!("{topic}: no such property"));
                let json;
                let value = match slot.datatype {
                    Datatype::Integer => ValueRef::Integer(payload.parse().expect("integer")),
                    Datatype::Float => ValueRef::Float(payload.parse().expect("float")),
                    Datatype::Boolean => ValueRef::Boolean(payload == "true"),
                    Datatype::String => ValueRef::String(payload),
                    Datatype::Enum => {
                        let format = slot.format.as_deref().unwrap_or_default();
                        let i = format
                            .split(',')
                            .position(|v| v == payload)
                            .unwrap_or_else(|| panic!("{topic}: {payload} not in {format}"));
                        ValueRef::Enum(u16::try_from(i).expect("enum index"))
                    }
                    Datatype::Json => {
                        json_topics.insert(topic.clone());
                        json = serde_json::from_str::<Value>(payload).expect("json");
                        ValueRef::Json(&json)
                    }
                    other => panic!("{topic}: no {} in the captures", other.as_str()),
                };
                tree.set_value(slot.id, value)
                    .unwrap_or_else(|e| panic!("{topic} = {payload}: {e}"));
                payloads.insert(topic, payload.as_bytes().to_vec());
            }
        }
    }
    (
        tree,
        Expected {
            root,
            descriptions,
            payloads,
            json: json_topics,
        },
    )
}

/// Connects as `id`, hands the tree its link-up, and executes every command
/// it then polls. The connection stays up until the returned task is
/// aborted.
async fn publish(port: u16, mut tree: Tree, id: &str) -> JoinHandle<()> {
    let mut mqtt = MqttOptions::new(id, "127.0.0.1", port);
    mqtt.set_max_packet_size(1 << 20, 1 << 20);
    // mosquitto's receive quota for QoS 2 from an MQTT 3.1.1 client.
    mqtt.set_inflight(20);
    let will = tree.last_will();
    mqtt.set_last_will(rumqttc::LastWill::new(
        will.topic,
        will.payload,
        qos(will.qos),
        will.retain,
    ));
    let (client, mut el) = AsyncClient::new(mqtt, 64);
    let (tx, mut connacks) = mpsc::unbounded_channel();
    let poller = tokio::spawn(async move {
        loop {
            match el.poll().await {
                Ok(rumqttc::Event::Incoming(Packet::ConnAck(a))) => {
                    let _ = tx.send(a.session_present);
                }
                Ok(_) => {}
                Err(e) => panic!("device connection: {e}"),
            }
        }
    });
    let session_present = tokio::time::timeout(Duration::from_secs(5), connacks.recv())
        .await
        .expect("CONNACK in time")
        .expect("poller running");
    tree.handle_link(Instant(0), Link::Up { session_present });
    while let Some(cmd) = tree.poll_command() {
        match cmd {
            Command::Subscribe { filters } => client
                .subscribe_many(
                    filters
                        .into_iter()
                        .map(|(f, q)| SubscribeFilter::new(f, qos(q))),
                )
                .await
                .expect("subscribe"),
            Command::Publish {
                topic,
                payload,
                qos: q,
                retain,
            } => client
                .publish(topic, qos(q), retain, payload)
                .await
                .expect("publish"),
            other => panic!("unexpected {other:?}"),
        }
    }
    poller
}

fn qos(q: ebus_rumqttc::ebus_homie_tokio::ebus_mqtt::QoS) -> rumqttc::QoS {
    use ebus_rumqttc::ebus_homie_tokio::ebus_mqtt::QoS;
    match q {
        QoS::AtMostOnce => rumqttc::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
    }
}

/// The documented exception: a float in integer layout, published with a
/// fraction, equal as `f64`.
fn integer_layout_float(want: &[u8], got: &[u8]) -> bool {
    let (Ok(w), Ok(g)) = (std::str::from_utf8(want), std::str::from_utf8(got)) else {
        return false;
    };
    w.bytes().all(|c| c.is_ascii_digit() || c == b'-')
        && g.strip_suffix(".0") == Some(w)
        && w.parse::<f64>().ok() == g.parse::<f64>().ok()
}

/// The second documented exception: the same JSON document at the same
/// length.
fn reordered_json(want: &[u8], got: &[u8]) -> bool {
    want.len() == got.len()
        && serde_json::from_slice::<Value>(want).ok() == serde_json::from_slice::<Value>(got).ok()
}

/// Differences between what the broker retains and what the capture says;
/// returns how many values the documented exceptions covered.
fn compare(
    rebuilt: &Expected,
    retained: &BTreeMap<String, Vec<u8>>,
    faults: &mut Vec<String>,
) -> usize {
    let mut excepted = 0;
    for (topic, want) in &rebuilt.descriptions {
        let Some(got) = retained.get(topic) else {
            faults.push(format!("{topic}: not retained"));
            continue;
        };
        let mut got: Value = match serde_json::from_slice(got) {
            Ok(v) => v,
            Err(e) => {
                faults.push(format!("{topic}: not JSON: {e}"));
                continue;
            }
        };
        if !got["version"].is_i64() && !got["version"].is_u64() {
            faults.push(format!("{topic}: version {}", got["version"]));
        }
        let mut want = want.clone();
        for doc in [&mut got, &mut want] {
            if let Some(o) = doc.as_object_mut() {
                o.remove("version");
            }
        }
        if got != want {
            faults.push(format!("{topic}:\n  published {got}\n  captured  {want}"));
        }
    }
    for (topic, want) in &rebuilt.payloads {
        match retained.get(topic) {
            None => faults.push(format!("{topic}: not retained")),
            Some(got) if got == want => {}
            Some(got) if integer_layout_float(want, got) => excepted += 1,
            Some(got) if rebuilt.json.contains(topic) && reordered_json(want, got) => {
                excepted += 1;
            }
            Some(got) => faults.push(format!(
                "{topic}: published {:?}, captured {:?}",
                String::from_utf8_lossy(got),
                String::from_utf8_lossy(want)
            )),
        }
    }
    for topic in retained.keys() {
        if !rebuilt.descriptions.contains_key(topic) && !rebuilt.payloads.contains_key(topic) {
            faults.push(format!("{topic}: retained, not in the capture"));
        }
    }
    excepted
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panel_sim_captures_republished_by_a_device_tree() {
    let test = "panel_sim_captures_republished_by_a_device_tree";
    let mut faults = Vec::new();
    let mut excepted = 0;
    for path in captures() {
        // A fresh broker per capture; unlimited queues, so a new subscriber
        // takes the whole tree at once.
        let Some(broker) = Broker::start(test, "max_queued_messages 0") else {
            return;
        };
        let name = path.file_name().expect("file name").to_string_lossy();
        let capture: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read capture"))
                .expect("capture JSON");
        let (tree, want) = rebuild(capture["devices"].as_object().expect("devices"));
        let state = format!("ebus/5/{}/$state", want.root);
        let mut obs = observe(broker.port, "root-observer", &[state.as_str()]).await;
        let device = publish(broker.port, tree, &want.root).await;
        // The root's `ready` is the tree's last publish.
        until_message(&mut obs, 30, &state, b"ready").await;
        let got = retained(broker.port, "ebus/5/#").await;
        let mut mine = Vec::new();
        excepted += compare(&want, &got, &mut mine);
        faults.extend(mine.into_iter().map(|f| format!("{name}: {f}")));
        device.abort();
    }
    eprintln!("{excepted} values compared under a documented exception");
    assert!(
        faults.is_empty(),
        "{} differences:\n{}",
        faults.len(),
        faults.join("\n")
    );
}
