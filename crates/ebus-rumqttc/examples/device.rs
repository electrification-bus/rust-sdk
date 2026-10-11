//! A small eBus device: one node with a temperature (`float`), an uptime
//! (`duration`), a status (`string`), a settable LED (`boolean`) and a
//! `pulse` event (`integer`, not retained). It prints each `/set` it
//! accepts.
//!
//! ```text
//! cargo run -p ebus-rumqttc --example device -- [root-id]
//! cargo run -p ebus-rumqttc --example device --features rustls -- [root-id]   # TLS
//! ```
//!
//! The connection uses the same `EBUS_MQTT_*` variables as the `watch`
//! example; the root id (default `rust-example`) is also the client id.
//! Ctrl-C publishes `disconnected` and disconnects.

mod common;

use std::process::ExitCode;
use std::time::Duration;

use ebus_rumqttc::ebus_homie_tokio::device::{
    DeviceOptions, DeviceSpec, NodeSpec, PropSpec, Shutdown, TreeBuilder, TreeConfig,
};

use common::{Stderr, options};

/// Seconds between pulse events.
const PULSE_SECS: u64 = 10;

#[tokio::main]
async fn main() -> ExitCode {
    if log::set_logger(&Stderr).is_ok() {
        log::set_max_level(log::LevelFilter::Warn);
    }
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "rust-example".to_owned());
    match run(&root).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(root: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mqtt = options(root.to_owned())?;
    let broker = mqtt.broker_address();

    let mut b = TreeBuilder::new(
        TreeConfig::default(),
        root,
        DeviceSpec {
            name: Some("Rust example".into()),
            ..DeviceSpec::default()
        },
    )?;
    let node = b.node(b.root(), "sensor", NodeSpec::typed("example.sensor"))?;
    let temperature = b.property::<f64>(node, "temperature", PropSpec::new().unit("°C"))?;
    let uptime = b.property::<Duration>(node, "uptime", PropSpec::new())?;
    let status = b.property::<String>(node, "status", PropSpec::new())?;
    let mut led = b.settable::<bool>(node, "led", PropSpec::new().name("LED"))?;
    let pulse = b.event::<i64>(node, "pulse", PropSpec::new())?;
    status.set("starting".to_owned())?;
    led.set(false)?;

    let run = ebus_rumqttc::device(mqtt, b, DeviceOptions::default())?;
    let handle = run.handle;
    let mut task = tokio::spawn(run.task.run());
    println!("connecting {}:{} as {root}", broker.0, broker.1);
    status.set("running".to_owned())?;

    let started = tokio::time::Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut pulses: i64 = 0;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let secs = started.elapsed().as_secs();
                // A slow, bounded wave rounded to 0.1, so most ticks repeat
                // the last value and publish nothing.
                let t = 20.0 + 2.0 * (secs as f64 / 60.0).sin();
                temperature.set((t * 10.0).round() / 10.0)?;
                uptime.set(Duration::from_secs(secs))?;
                if secs % PULSE_SECS == 0 && handle.is_connected() {
                    pulses += 1;
                    if let Err(e) = pulse.emit(pulses) {
                        eprintln!("pulse not sent: {e}");
                    }
                }
            }
            req = led.next_set() => {
                let Some(req) = req else { break };
                println!("set led {}", req.value());
                req.accept();
            }
            _ = tokio::signal::ctrl_c() => {
                handle.shutdown(Shutdown::Announce).await;
                break;
            }
            res = &mut task => {
                return match res {
                    Ok(r) => r.map_err(Into::into),
                    Err(e) => Err(e.into()),
                };
            }
        }
    }
    task.await??;
    Ok(())
}
