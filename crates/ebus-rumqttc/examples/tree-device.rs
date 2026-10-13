//! A parent/child device tree on one connection:
//!
//! ```text
//! <root>                  distribution enclosure
//! |-- circuit-1 .. -3     circuits, each with meter/active-power
//! |   '- circuit-3        also switch/relay (boolean, settable)
//! '-- bess-1              battery, with control/reserve (integer 0:100, settable)
//!     '-- mid-1           microgrid interconnect, a grandchild
//! ```
//!
//! The whole tree is built in one `TreeBuilder`, so the root goes `ready`
//! once for the tree. Every device publishes under its own id and shares the
//! root's connection and Last Will. The example prints each `/set` it
//! accepts; turning the relay on or off also moves circuit-3's active power.
//!
//! ```text
//! cargo run -p ebus-rumqttc --example tree-device -- [root-id]
//! cargo run -p ebus-rumqttc --example tree-device --features rustls -- [root-id]   # TLS
//! ```
//!
//! The broker settings are the `watch` example's, listed in
//! `examples/common/mod.rs`: with neither `EBUS_MQTT_HOST` nor
//! `EBUS_MQTT_PORT` set, the tree looks for a broker over mDNS. The root id
//! (default `rust-tree`) is also the client id. The `set` example changes
//! the settable properties:
//!
//! ```text
//! cargo run -p ebus-rumqttc --example set -- circuit-3/switch/relay true
//! ```
//!
//! Ctrl-C publishes `disconnected` for every device and disconnects.

mod common;

use std::process::ExitCode;

use ebus_rumqttc::ebus_homie_tokio::device::{
    DeviceOptions, DeviceSpec, NodeSpec, PropSpec, Shutdown, TreeBuilder, TreeConfig,
};

use common::{Stderr, connect};

/// circuit-3's active power while its relay is on, in watts.
const RELAY_LOAD_W: f64 = 1200.0;

#[tokio::main]
async fn main() -> ExitCode {
    if log::set_logger(&Stderr).is_ok() {
        log::set_max_level(log::LevelFilter::Warn);
    }
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "rust-tree".to_owned());
    match run(&root).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{}", common::report(&*e));
            ExitCode::FAILURE
        }
    }
}

async fn run(root: &str) -> Result<(), Box<dyn std::error::Error>> {
    let conn = connect(root.to_owned()).await?;
    let broker = conn.mqtt.broker_address();

    let mut b = TreeBuilder::new(
        TreeConfig::default(),
        root,
        DeviceSpec::typed("energy.ebus.device.distribution-enclosure")
            .name("Example distribution enclosure"),
    )?;
    let panel = b.root();

    let mut circuit3_power = None;
    for i in 1..=3 {
        let circuit = b.child(
            panel,
            &format!("circuit-{i}"),
            DeviceSpec::typed("energy.ebus.device.circuit").name(&format!("Circuit {i}")),
        )?;
        let meter = b.node(
            circuit,
            "meter",
            NodeSpec::typed("energy.ebus.capability.meter").name("Meter"),
        )?;
        let power = b.property::<f64>(
            meter,
            "active-power",
            PropSpec::new().name("Active power").unit("W"),
        )?;
        power.set(0.0)?;
        if i == 3 {
            circuit3_power = Some((circuit, power));
        }
    }
    let (circuit3, circuit3_power) = circuit3_power.ok_or("circuit-3 not built")?;
    let switch = b.node(
        circuit3,
        "switch",
        NodeSpec::typed("energy.ebus.capability.switch").name("Switch"),
    )?;
    let mut relay = b.settable::<bool>(switch, "relay", PropSpec::new().name("Relay"))?;
    relay.set(false)?;

    let bess = b.child(
        panel,
        "bess-1",
        DeviceSpec::typed("energy.ebus.device.bess").name("Battery"),
    )?;
    let control = b.node(bess, "control", NodeSpec::default().name("Control"))?;
    let mut reserve = b.settable::<u8>(
        control,
        "reserve",
        PropSpec::new()
            .name("Backup reserve")
            .format("0:100")
            .unit("%"),
    )?;
    reserve.set(20)?;
    b.child(
        bess,
        "mid-1",
        DeviceSpec::typed("energy.ebus.device.mid").name("Microgrid interconnect device"),
    )?;

    let run = ebus_rumqttc::device(conn.mqtt, b, DeviceOptions::default())?;
    let handle = run.handle;
    let mut task = match conn.reresolve {
        Some(f) => tokio::spawn(run.task.reresolve(f).run()),
        None => tokio::spawn(run.task.run()),
    };
    println!(
        "connecting {}:{} as {root}: circuit-1, circuit-2, circuit-3, bess-1, mid-1",
        broker.0, broker.1
    );

    loop {
        tokio::select! {
            req = relay.next_set() => {
                let Some(req) = req else { break };
                let on = *req.value();
                println!("set circuit-3/switch/relay {on}");
                req.accept();
                circuit3_power.set(if on { RELAY_LOAD_W } else { 0.0 })?;
            }
            req = reserve.next_set() => {
                let Some(req) = req else { break };
                println!("set bess-1/control/reserve {}", req.value());
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
