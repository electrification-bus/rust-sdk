//! Read-only eBus watcher: discovers devices, prints one line per controller
//! event and a summary line after each `DeviceSynced`. It never sends `/set`.
//!
//! ```text
//! cargo run -p ebus-rumqttc --example watch -- wildcard
//! cargo run -p ebus-rumqttc --example watch -- tree <root>
//! cargo run -p ebus-rumqttc --example watch --features rustls -- wildcard   # TLS
//! ```
//!
//! | Variable | Meaning | Default |
//! |---|---|---|
//! | `EBUS_MQTT_HOST` | Configured broker host | `localhost` when `EBUS_MQTT_PORT` is set, else none |
//! | `EBUS_MQTT_PORT` | Configured broker port | 1883, or 8883 with TLS |
//! | `EBUS_BROKER_MODE` | `discovery-only`, `discovery-with-fallback` or `configured-only` | `configured-only` with a configured broker, else `discovery-only` |
//! | `EBUS_MDNS_BACKEND` | `auto`, `avahi` (Linux) or `mdns-sd` | `auto`: Avahi when avahi-daemon runs, else mdns-sd |
//! | `EBUS_MQTT_USER`, `EBUS_MQTT_PASS` | Credentials | none |
//! | `EBUS_MQTT_CA` | PEM CA file; enables TLS (`rustls` feature) | none |
//! | `EBUS_MQTT_INSECURE` | `1` enables TLS without verifying the broker's certificate, for lab brokers with self-signed certificates | unset |
//!
//! With neither `EBUS_MQTT_HOST` nor `EBUS_MQTT_PORT` set, the watcher looks
//! for a broker over mDNS until one is found, and looks it up again when
//! connecting to it fails.
//!
//! Warnings from the controller and the connection go to stderr. Ctrl-C
//! unsubscribes and disconnects.

mod common;

use std::process::ExitCode;
use std::time::Instant;

use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event, Origin, Value};
use ebus_rumqttc::ebus_homie_tokio::ebus_mqtt::Link;
use ebus_rumqttc::ebus_homie_tokio::{ControllerHandle, ControllerOptions, Delivered};

use common::{Stderr, connect};

fn usage() -> String {
    "usage: watch wildcard | watch tree <root>".to_owned()
}

fn config(args: &[String]) -> Result<Config, String> {
    match args {
        [mode] if mode == "wildcard" => Ok(Config::wildcard()),
        [mode, root] if mode == "tree" => {
            Config::tree(root).map_err(|e| format!("invalid root {root:?}: {e}"))
        }
        _ => Err(usage()),
    }
}

fn value(v: &Value) -> String {
    match &v.check {
        Ok(()) => format!("{:?}", &*v.raw),
        Err(p) => format!("{:?} invalid={p}", &*v.raw),
    }
}

fn line(e: &Event) -> String {
    match e {
        Event::Link(Link::Up {
            session_present, ..
        }) => {
            format!("link up session_present={session_present}")
        }
        Event::Link(Link::Down) => "link down".to_owned(),
        Event::DeviceDiscovered { device, state, .. } => {
            format!("discovered {device} state={state}")
        }
        Event::StateChanged {
            device,
            old,
            new,
            effective,
            ..
        } => {
            let old = old.map_or_else(|| "-".to_owned(), |s| s.to_string());
            format!("state {device} {old} -> {new} effective={effective}")
        }
        Event::EffectiveStateChanged {
            device, old, new, ..
        } => {
            format!("effective {device} {old} -> {new}")
        }
        Event::DescriptionChanged {
            device,
            description,
            ignored,
            while_ready,
            ..
        } => format!(
            "description {device} version={} nodes={} children={} ignored={} while_ready={while_ready}",
            description.version,
            description.nodes.len(),
            description.children.len(),
            ignored.len()
        ),
        Event::DescriptionRejected { device, error, .. } => {
            format!("description-rejected {device} {error}")
        }
        Event::PropertyChanged {
            addr,
            value: v,
            origin,
            ..
        } => {
            let origin = match origin {
                Origin::Retained => "retained",
                Origin::Live => "live",
                _ => "unknown",
            };
            format!(
                "property {}/{}/{} = {} {origin}",
                addr.device,
                addr.node,
                addr.property,
                value(v)
            )
        }
        Event::PropertyEvent {
            addr,
            value: v,
            origin,
            ..
        } => {
            let origin = match origin {
                Origin::Retained => " (retained replay)",
                _ => "",
            };
            format!(
                "event {}/{}/{} = {}{origin}",
                addr.device,
                addr.node,
                addr.property,
                value(v)
            )
        }
        Event::PropertyCleared { addr, .. } => {
            format!("cleared {}/{}/{}", addr.device, addr.node, addr.property)
        }
        Event::TargetChanged { addr, target, .. } => format!(
            "target {}/{}/{} = {}",
            addr.device,
            addr.node,
            addr.property,
            target.as_ref().map_or_else(|| "-".to_owned(), value)
        ),
        Event::Alert {
            device,
            alert,
            message,
            ..
        } => format!("alert {device}/{alert} = {message:?}"),
        Event::DeviceSynced {
            device, missing, ..
        } => format!("synced {device} missing={missing}"),
        Event::DeviceStuck {
            device,
            missing,
            attempt,
            gave_up,
            ..
        } => format!("stuck {device} missing={missing:?} attempt={attempt} gave_up={gave_up}"),
        Event::DeviceAbsent { device, .. } => format!("absent {device}"),
        Event::DeviceRemoved { device, .. } => format!("removed {device}"),
        Event::TreeComplete { root, .. } => format!("tree-complete {root}"),
        other => format!("{other:?}"),
    }
}

fn summary(handle: &ControllerHandle, started: Instant) -> String {
    let devices = handle.devices();
    let synced = devices.iter().filter(|d| d.synced).count();
    let (mut properties, mut values) = (0usize, 0usize);
    for d in &devices {
        for (_, _, p) in d.properties() {
            properties += 1;
            values += usize::from(p.value.is_some());
        }
    }
    format!(
        "summary devices={} synced={synced} properties={properties} values={values} elapsed_ms={}",
        devices.len(),
        started.elapsed().as_millis()
    )
}

#[tokio::main]
async fn main() -> ExitCode {
    if log::set_logger(&Stderr).is_ok() {
        log::set_max_level(log::LevelFilter::Warn);
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = match config(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let conn = match connect(ebus_rumqttc::controller_client_id()).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let broker = conn.mqtt.broker_address();
    let run = match ebus_rumqttc::controller(conn.mqtt, config, ControllerOptions::default()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    println!("connecting {}:{}", broker.0, broker.1);
    let started = Instant::now();
    let handle = run.handle;
    let mut events = run.events;
    let mut task = match conn.reresolve {
        Some(f) => tokio::spawn(run.task.reresolve(f).run()),
        None => tokio::spawn(run.task.run()),
    };

    loop {
        tokio::select! {
            d = events.recv() => match d {
                Some(Delivered::Event(e)) => {
                    println!("{}", line(&e));
                    if matches!(e, Event::DeviceSynced { .. }) {
                        println!("{}", summary(&handle, started));
                    }
                }
                Some(Delivered::Overflow { dropped }) => {
                    println!("overflow dropped={dropped}");
                    println!("{}", summary(&handle, started));
                }
                Some(_) => {}
                None => break,
            },
            _ = tokio::signal::ctrl_c() => {
                handle.shutdown().await;
                break;
            }
        }
    }
    match (&mut task).await {
        Ok(Ok(())) => ExitCode::SUCCESS,
        Ok(Err(e)) => {
            eprintln!("{}", common::report(&e));
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
