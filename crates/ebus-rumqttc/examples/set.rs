//! Sends one `/set` and waits for the device to report the value: finds the
//! device, waits until it has synced, sends the value to
//! `<device>/<node>/<property>/set`, then waits until the device publishes
//! the value it was asked for.
//!
//! ```text
//! cargo run -p ebus-rumqttc --example set -- [--root <root>] [--timeout <secs>] <device>/<node>/<property> <value>
//! cargo run -p ebus-rumqttc --example set -- circuit-3/switch/relay true
//! cargo run -p ebus-rumqttc --example set --features rustls -- circuit-3/switch/relay true   # TLS
//! ```
//!
//! Devices are discovered by wildcard, or with `--root` only that root and
//! the devices under it. `--timeout` (default 10 seconds, at most a day)
//! bounds each wait: for the device to sync, and for its new value. The `/set` goes out only
//! when the SDK's own checks pass: the property is declared and settable, the
//! device is `ready`, and the value fits the property's datatype and
//! `$format`. A device that already holds the value publishes nothing, so the
//! example then reports it unchanged without waiting.
//!
//! | Exit code | Meaning |
//! |---|---|
//! | 0 | The device reported the value, or already held it |
//! | 1 | The device was not seen, did not sync or did not report the value in time, or the connection failed |
//! | 2 | Usage or broker settings |
//! | 3 | The SDK refused the `/set`: unknown property, not settable, not ready, or an invalid value |
//!
//! The broker settings are the `watch` example's, listed in
//! `examples/common/mod.rs`: with neither `EBUS_MQTT_HOST` nor
//! `EBUS_MQTT_PORT` set, it looks for a broker over mDNS. The `tree-device`
//! example has settable properties to try it on.

mod common;

use std::process::ExitCode;
use std::time::Duration;

use ebus_rumqttc::ebus_homie_tokio::ebus_homie::PropertyAddr;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event, Origin, SetError};
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::datatype::{
    Current, Datatype, SetValue, decode_json, parse_set_payload,
};
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::device::HomieValue;
use ebus_rumqttc::ebus_homie_tokio::{ControllerHandle, ControllerOptions, Delivered};
use tokio::time::{Instant, timeout_at};

use common::{Stderr, connect};

const USAGE: &str =
    "usage: set [--root <root>] [--timeout <secs>] <device>/<node>/<property> <value>";

/// Default bound on each wait, in seconds.
const TIMEOUT_SECS: u64 = 10;
/// Largest `--timeout`, in seconds.
const MAX_TIMEOUT_SECS: f64 = 86_400.0;

struct Args {
    root: Option<String>,
    timeout: Duration,
    addr: PropertyAddr,
    value: String,
}

fn args(argv: &[String]) -> Result<Args, String> {
    let mut root = None;
    let mut timeout = Duration::from_secs(TIMEOUT_SECS);
    let mut rest = Vec::new();
    let mut it = argv.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--root" => root = Some(it.next().ok_or(USAGE)?.clone()),
            "--timeout" => {
                let s = it.next().ok_or(USAGE)?;
                let secs: f64 = s
                    .parse()
                    .ok()
                    .filter(|t: &f64| *t > 0.0 && *t <= MAX_TIMEOUT_SECS)
                    .ok_or_else(|| {
                        format!("--timeout {s:?} is not a number of seconds above 0, at most {MAX_TIMEOUT_SECS}")
                    })?;
                timeout = Duration::from_secs_f64(secs);
            }
            _ => rest.push(a.clone()),
        }
    }
    let [path, value] = <[String; 2]>::try_from(rest).map_err(|_| USAGE)?;
    let mut levels = path.split('/');
    let addr = match (levels.next(), levels.next(), levels.next(), levels.next()) {
        (Some(d), Some(n), Some(p), None) => PropertyAddr::new(d, n, p),
        _ => None,
    }
    .ok_or_else(|| format!("{path:?} is not <device>/<node>/<property> of valid ids"))?;
    Ok(Args {
        root,
        timeout,
        addr,
        value,
    })
}

fn path(a: &PropertyAddr) -> String {
    format!("{}/{}/{}", a.device, a.node, a.property)
}

/// How the device should report `value` on `addr`: its datatype and the
/// value the device's own `/set` check makes of it, or `None` when the device
/// or the property is not known.
fn expected(handle: &ControllerHandle, addr: &PropertyAddr, value: &str) -> Option<Expected> {
    let dev = handle.device(addr.device.as_str())?;
    let decl = dev
        .description
        .as_deref()?
        .nodes
        .iter()
        .find(|(n, _)| n == addr.node.as_str())?
        .1
        .properties
        .iter()
        .find(|(p, _)| p == addr.property.as_str())?
        .1
        .clone();
    let current = dev
        .property(addr.node.as_str(), addr.property.as_str())?
        .last_valid
        .clone();
    let base = current.as_deref().and_then(|t| match decl.datatype {
        Datatype::Integer => t.parse().ok().map(Current::Integer),
        Datatype::Float => t.parse().ok().map(Current::Float),
        _ => None,
    });
    let value = parse_set_payload(value, decl.datatype, decl.format.as_deref(), base).ok()?;
    Some(Expected {
        datatype: decl.datatype,
        value,
        current,
    })
}

struct Expected {
    datatype: Datatype,
    value: SetValue,
    current: Option<std::sync::Arc<str>>,
}

impl Expected {
    /// True when the payload `raw` is the expected value.
    fn matches(&self, raw: &str) -> bool {
        match &self.value {
            SetValue::Text(t) => match self.datatype {
                Datatype::Integer | Datatype::Float => {
                    matches!((raw.parse::<f64>(), t.parse::<f64>()), (Ok(a), Ok(b)) if a == b)
                }
                // A device re-encodes a duration: `PT60S` comes back `PT1M`.
                Datatype::Duration => {
                    let parse = |p: &str| Duration::from_set(&SetValue::Text(p.into()));
                    matches!((parse(raw), parse(t)), (Some(a), Some(b)) if a.same(&b))
                }
                _ => raw == t,
            },
            SetValue::String(s) => raw == s,
            SetValue::Json(v) => decode_json(raw).is_ok_and(|r| &r == v),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    if log::set_logger(&Stderr).is_ok() {
        log::set_max_level(log::LevelFilter::Warn);
    }
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match args(&argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let config = match &args.root {
        Some(root) => match Config::tree(root) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("invalid root {root:?}: {e}");
                return ExitCode::from(2);
            }
        },
        None => Config::wildcard(),
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
    let handle = run.handle;
    let mut events = run.events;
    let mut task = match conn.reresolve {
        Some(f) => tokio::spawn(run.task.reresolve(f).run()),
        None => tokio::spawn(run.task.run()),
    };

    let code = tokio::select! {
        code = set(&handle, &mut events, &args) => code,
        res = &mut task => {
            match res {
                Ok(Ok(())) => eprintln!("controller stopped"),
                Ok(Err(e)) => eprintln!("{}", common::report(&e)),
                Err(e) => eprintln!("{e}"),
            }
            return ExitCode::FAILURE;
        }
        _ = tokio::signal::ctrl_c() => ExitCode::FAILURE,
    };
    handle.shutdown().await;
    if let Ok(Err(e)) = task.await {
        eprintln!("{}", common::report(&e));
    }
    code
}

/// Waits for the device, sends the `/set` and waits for the new value.
async fn set(
    handle: &ControllerHandle,
    events: &mut ebus_rumqttc::ebus_homie_tokio::EventReceiver,
    args: &Args,
) -> ExitCode {
    let addr = &args.addr;
    let device = addr.device.as_str();

    let deadline = Instant::now() + args.timeout;
    while !handle.device(device).is_some_and(|d| d.synced) {
        match timeout_at(deadline, events.recv()).await {
            Ok(Some(_)) => {}
            Ok(None) => return ExitCode::FAILURE,
            Err(_) => {
                eprintln!(
                    "{device} did not sync within {:?}{}",
                    args.timeout,
                    if handle.device(device).is_some() {
                        ""
                    } else {
                        " (not seen)"
                    }
                );
                return ExitCode::FAILURE;
            }
        }
    }

    // Read before sending: the device's answer can reach the snapshot first.
    let want = expected(handle, addr, &args.value);
    // `set` runs the device's checks on the payload before anything is sent.
    match handle.set(addr.clone(), args.value.as_str()).await {
        Ok(()) => {}
        Err(SetError::NotConnected) => {
            eprintln!("not connected; {} not sent", path(addr));
            return ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("refused {} {:?}: {e}", path(addr), args.value);
            return ExitCode::from(3);
        }
    }
    println!("sent {} {}", path(addr), args.value);
    let Some(want) = want else {
        eprintln!("{} has no declaration to compare with", path(addr));
        return ExitCode::FAILURE;
    };
    if want.current.as_deref().is_some_and(|c| want.matches(c)) {
        println!("unchanged {} {}", path(addr), args.value);
        return ExitCode::SUCCESS;
    }

    let deadline = Instant::now() + args.timeout;
    let mut last = want.current.clone();
    loop {
        match timeout_at(deadline, events.recv()).await {
            Ok(Some(Delivered::Event(Event::PropertyChanged {
                addr: a,
                value,
                origin: Origin::Live,
                ..
            }))) if a == *addr => {
                if want.matches(&value.raw) {
                    println!("reported {} {}", path(addr), value.raw);
                    return ExitCode::SUCCESS;
                }
                last = Some(value.raw);
            }
            Ok(Some(_)) => {}
            Ok(None) => return ExitCode::FAILURE,
            Err(_) => {
                eprintln!(
                    "{} not reported within {:?}; last value {}",
                    path(addr),
                    args.timeout,
                    last.as_deref().unwrap_or("-")
                );
                return ExitCode::FAILURE;
            }
        }
    }
}
