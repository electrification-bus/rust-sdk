//! Parses every device in captured panel trees (tree-v1) and checks every
//! captured value against its property's datatype and format.
//!
//! Always runs over the vendored `fixtures/panel-sim` captures. Set
//! `EBUS_CAPTURE_CORPUS` to a directory to also run over every `*.tree.json`
//! below it; that corpus is never copied into this repository.

#![cfg(feature = "std")]

use std::fs;
use std::path::{Path, PathBuf};

use ebus_homie::datatype::{self, Datatype};
use ebus_homie::description::{self, DeviceDescription};
use serde_json::Value;

#[derive(Default)]
struct Tally {
    files: usize,
    devices: usize,
    values: usize,
    failures: Vec<String>,
    value_faults: Vec<String>,
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

fn property<'a>(
    description: &'a DeviceDescription,
    key: &str,
) -> Option<&'a description::PropertyDescription> {
    let (node_id, property_id) = key.split_once('/')?;
    let (_, node) = description.nodes.iter().find(|(id, _)| id == node_id)?;
    let (_, property) = node.properties.iter().find(|(id, _)| id == property_id)?;
    Some(property)
}

fn check_value(
    tally: &mut Tally,
    origin: &str,
    description: &DeviceDescription,
    key: &str,
    value: &Value,
) {
    tally.values += 1;
    let Some(text) = value.as_str() else {
        // Older captures store numeric values as JSON numbers, not payload text.
        return;
    };
    let Some(property) = property(description, key) else {
        tally.value_faults.push(format!(
            "{origin} {key}: value for a property the description lacks"
        ));
        return;
    };
    if property.datatype == Datatype::String {
        return;
    }
    if let Err(problem) =
        datatype::parse_set_payload(text, property.datatype, property.format.as_deref(), None)
    {
        tally.value_faults.push(format!(
            "{origin} {key}={text:?} ({} {:?}): {}",
            property.datatype.as_str(),
            property.format,
            problem.as_str()
        ));
    }
}

fn check_file(tally: &mut Tally, path: &Path) {
    tally.files += 1;
    let origin = path.display().to_string();
    let tree: Value = match fs::read_to_string(path)
        .map_err(|e| e.to_string())
        .and_then(|text| serde_json::from_str(&text).map_err(|e| e.to_string()))
    {
        Ok(tree) => tree,
        Err(e) => {
            tally.failures.push(format!("{origin}: unreadable: {e}"));
            return;
        }
    };
    let Some(devices) = tree.get("devices").and_then(Value::as_object) else {
        tally.failures.push(format!("{origin}: no devices"));
        return;
    };
    for (device_id, device) in devices {
        let origin = format!("{origin} {device_id}");
        let Some(raw) = device.get("description") else {
            continue;
        };
        tally.devices += 1;
        let parsed = match description::parse_with_ignored(&raw.to_string()) {
            Ok((parsed, ignored)) => {
                for item in ignored {
                    tally.failures.push(format!(
                        "{origin}: ignored {}/{}: {}",
                        item.node,
                        item.property.as_deref().unwrap_or("-"),
                        item.error.as_str()
                    ));
                }
                parsed
            }
            Err(e) => {
                tally.failures.push(format!("{origin}: {}", e.as_str()));
                continue;
            }
        };
        match description::parse(&parsed.to_json()) {
            Ok(again) if again == parsed => {}
            Ok(_) => tally
                .failures
                .push(format!("{origin}: to_json does not round-trip")),
            Err(e) => tally
                .failures
                .push(format!("{origin}: to_json output rejected: {}", e.as_str())),
        }
        for field in ["properties", "numeric_properties"] {
            if let Some(values) = device.get(field).and_then(Value::as_object) {
                for (key, value) in values {
                    check_value(tally, &origin, &parsed, key, value);
                }
            }
        }
    }
}

fn run(dir: &Path, suffix: &str) -> Tally {
    let mut files = Vec::new();
    tree_files(dir, suffix, &mut files);
    files.sort();
    let mut tally = Tally::default();
    for path in &files {
        check_file(&mut tally, path);
    }
    tally
}

/// Fails on any description the parser rejects or cannot round-trip. Values that
/// break their own property's format are publisher faults: `strict_values` makes
/// them failures too, otherwise they are listed.
fn assert_clean(name: &str, tally: &Tally, strict_values: bool) {
    eprintln!(
        "{name}: {} files, {} devices, {} values, {} failures, {} value faults",
        tally.files,
        tally.devices,
        tally.values,
        tally.failures.len(),
        tally.value_faults.len()
    );
    for fault in &tally.value_faults {
        eprintln!("value fault: {fault}");
    }
    assert!(
        !strict_values || tally.value_faults.is_empty(),
        "{name}: {} value faults",
        tally.value_faults.len()
    );
    assert!(tally.files > 0, "{name}: no capture files found");
    assert!(
        tally.failures.is_empty(),
        "{name}: {} failures:\n{}",
        tally.failures.len(),
        tally.failures.join("\n")
    );
}

#[test]
fn panel_sim_captures() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/panel-sim");
    assert_clean("panel-sim", &run(&dir, "-tree-v1.json"), true);
}

#[test]
fn local_capture_corpus() {
    let Some(dir) = std::env::var_os("EBUS_CAPTURE_CORPUS") else {
        eprintln!("EBUS_CAPTURE_CORPUS not set; skipped");
        return;
    };
    assert_clean("local corpus", &run(Path::new(&dir), ".tree.json"), false);
}

/// Every `$description` line of a raw retained-topic dump (`<topic> <payload>`)
/// that the parser accepts in full must serialize back to the same bytes.
#[test]
fn local_capture_corpus_wire() {
    let Some(dir) = std::env::var_os("EBUS_CAPTURE_CORPUS") else {
        eprintln!("EBUS_CAPTURE_CORPUS not set; skipped");
        return;
    };
    let mut files = Vec::new();
    tree_files(Path::new(&dir), "ebus-retained-topics.txt", &mut files);
    let (mut checked, mut failures, mut faults) = (0, Vec::new(), Vec::new());
    for path in &files {
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        for line in text.lines() {
            let Some((topic, payload)) = line.split_once(' ') else {
                continue;
            };
            if !topic.ends_with("/$description") {
                continue;
            }
            checked += 1;
            let origin = format!("{} {topic}", path.display());
            match description::parse_with_ignored(payload) {
                // A document the convention rejects, in whole or in part, is a
                // publisher fault: nothing is left to compare it with.
                Err(e) => faults.push(format!("{origin}: {}", e.as_str())),
                Ok((_, ignored)) if !ignored.is_empty() => faults.push(format!(
                    "{origin}: {} entries ignored, first {}",
                    ignored.len(),
                    ignored[0].error.as_str()
                )),
                Ok((parsed, _)) if parsed.to_json() != payload => {
                    failures.push(format!("{origin}: to_json differs from the wire"))
                }
                Ok(_) => {}
            }
        }
    }
    eprintln!(
        "local corpus wire: {} dumps, {checked} descriptions, {} failures, {} publisher faults",
        files.len(),
        failures.len(),
        faults.len()
    );
    for fault in &faults {
        eprintln!("publisher fault: {fault}");
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
