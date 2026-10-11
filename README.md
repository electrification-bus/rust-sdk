# rust-sdk

[![CI](https://github.com/electrification-bus/rust-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/electrification-bus/rust-sdk/actions/workflows/ci.yml)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](#rust-version)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Rust SDK for the [Electrification Bus](https://github.com/electrification-bus/specification) (eBus): Homie 5 devices and controllers over MQTT, with mDNS broker discovery. It targets Linux single-board computers first and microcontrollers later.

**Status:** pre-alpha. The controller and device roles are implemented, and mDNS broker discovery and host advertisement run on the Avahi and mdns-sd backends. Nothing is published to crates.io yet.

## What it does

### Controller

A controller watches the devices on a broker and sends them commands. It:

- discovers devices from their Homie topics on the broker: every device in the `ebus` domain, the devices of one tree (a root and its children), or a single device;
- applies each device's `$description` and checks every value against its property's datatype and format;
- tracks each device's `$state` and its effective state (a child of a `lost` root is `lost`);
- reports when a device has received all its retained values (`DeviceSynced`), when its values do not arrive (`DeviceStuck`), and when it goes quiet after a reconnect (`DeviceAbsent`);
- re-syncs after a reconnect and clears values the broker no longer retains;
- sends `/set` and `$broadcast`, checking a `/set` payload against the property first;
- emits nothing when nothing changed: a repeated value, state or description produces no event and no snapshot.

The application reads the latest state as cheap snapshots and receives changes as events.

### Device

A device publishes a tree of Homie devices (a root and its children) and takes commands for them. It:

- builds each device's `$description` from typed properties (`property::<f64>`, `settable::<bool>`, `event::<i64>`) and publishes `init`, the description, the values and `ready`, the root's `$state` last;
- publishes a value only when it changes, and never publishes events while disconnected;
- delivers each valid `/set` to the application, which accepts it, accepts it with the value the device reached, or rejects it; `$target` properties report progress toward the target;
- republishes the whole tree after a reconnect, and installs the root's `lost` state as the connection's last will;
- raises and clears alerts, changes the tree while running, and shuts down by announcing `disconnected`, by letting the will fire, or by removing every topic.

A device and a controller can share one connection (`ebus_rumqttc::pair`).

### Broker discovery

A site has one MQTT broker, and every eBus host at the site connects to it. A host finds it in one of three ways:

- discovers the brokers advertised over mDNS (DNS-SD) and picks one;
- connects to a configured broker;
- uses mDNS to find a particular broker, by its host, its instance name, or a device id any of its broker advertisements carries.

The client sets which broker transports it accepts and its preference among them, by default the specification's order with `_secure-mqtt._tcp` first.

The host also advertises its own eBus services over mDNS. `ebus-discovery-tokio` finds the broker and advertises the host through avahi-daemon over D-Bus on Linux (its `avahi` feature), or on the pure-Rust mdns-sd backend (its `mdns-sd` feature) for Linux without Avahi and for macOS.

## Quick start

Watch every device on a broker:

```rust
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event};
use ebus_rumqttc::ebus_homie_tokio::{AttachOptions, Delivered};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut mqtt = rumqttc::MqttOptions::new(ebus_rumqttc::client_id(), "localhost", 1883);
    // A `$description` can exceed rumqttc's 10 KiB default packet size.
    mqtt.set_max_packet_size(4 << 20, 4 << 20);

    let run = ebus_rumqttc::controller(mqtt, Config::wildcard(), AttachOptions::default())?;
    tokio::spawn(run.task.run());

    let mut events = run.events;
    while let Some(delivered) = events.recv().await {
        if let Delivered::Event(Event::DeviceSynced { device, .. }) = delivered {
            if let Some(view) = run.handle.device(&device) {
                for (node, property, p) in view.properties() {
                    let value = p.value.as_ref().map(|v| &*v.raw);
                    println!("{device}/{node}/{property} = {value:?}");
                }
            }
        }
    }
    Ok(())
}
```

Find the broker over mDNS instead, and find it again if its host gets a new address (`ebus-discovery-tokio` with its `mdns-sd` or `avahi` feature):

```rust
use std::time::Duration;

use ebus_discovery_tokio::ebus_discovery::BrokerConfig;
use ebus_discovery_tokio::{Mdns, find_broker, reresolve};
use ebus_rumqttc::ebus_homie_tokio::AttachOptions;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::Config;
use rumqttc::MqttOptions;

let mdns = Mdns::system().await?;
let cfg = BrokerConfig::default(); // discovery-only: the first broker advertised
let broker = find_broker(&mdns, cfg.clone()).await?;

let id = ebus_rumqttc::client_id();
let options = |id: &str, host: &str, port: u16| {
    let mut mqtt = MqttOptions::new(id, host, port);
    mqtt.set_max_packet_size(4 << 20, 4 << 20);
    mqtt
};
let mqtt = options(&id, &broker.connect_host(), broker.port);
let run = ebus_rumqttc::controller(mqtt, Config::wildcard(), AttachOptions::default())?;
let task = run.task.reresolve(Box::new(move || {
    let (mdns, cfg, broker, id) = (mdns.clone(), cfg.clone(), broker.clone(), id.clone());
    Box::pin(async move {
        let found = reresolve(&mdns, &broker, &cfg, Duration::from_secs(3)).await.ok()??;
        Some(options(&id, &found.connect_host(), found.port))
    })
}));
tokio::spawn(task.run());
```

The `watch` example prints every controller event. With neither `EBUS_MQTT_HOST` nor `EBUS_MQTT_PORT` set it finds the broker over mDNS; with either, it connects to that broker:

```sh
cargo run -p ebus-rumqttc --example watch -- wildcard
EBUS_MQTT_HOST=localhost cargo run -p ebus-rumqttc --example watch -- tree <root>
EBUS_MQTT_HOST=broker.local EBUS_MQTT_CA=ca.pem cargo run -p ebus-rumqttc --example watch --features rustls -- wildcard
```

Its source lists every setting, including `EBUS_BROKER_MODE` and `EBUS_MDNS_BACKEND`: [crates/ebus-rumqttc/examples/watch.rs](crates/ebus-rumqttc/examples/watch.rs).

Publish a device:

```rust
use ebus_rumqttc::ebus_homie_tokio::device::{
    DeviceOptions, DeviceSpec, NodeSpec, PropSpec, TreeBuilder, TreeConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mqtt = rumqttc::MqttOptions::new("my-device", "localhost", 1883);

    let mut b = TreeBuilder::new(TreeConfig::default(), "my-device", DeviceSpec::default())?;
    let node = b.node(b.root(), "sensor", NodeSpec::typed("example.sensor"))?;
    let temperature = b.property::<f64>(node, "temperature", PropSpec::new().unit("°C"))?;
    let mut led = b.settable::<bool>(node, "led", PropSpec::new())?;
    temperature.set(21.5)?;
    led.set(false)?;

    let run = ebus_rumqttc::device(mqtt, b, DeviceOptions::default())?;
    tokio::spawn(run.task.run());

    while let Some(req) = led.next_set().await {
        println!("led -> {}", req.value());
        req.accept();
    }
    Ok(())
}
```

The `device` example adds a duration, an event and a clean shutdown on Ctrl-C, and takes the `watch` example's broker settings:

```sh
cargo run -p ebus-rumqttc --example device -- my-device
```

### Inside a service that already has an MQTT connection

A service can share its own MQTT client with the controller instead of opening a second connection: `ebus_homie_tokio::attach` returns an inbox the client's reader feeds and a command stream the client sends. The host keeps its client version, TLS, last will and reconnect loop. The contract is in the [`ebus-homie-tokio` crate docs](crates/ebus-homie-tokio/src/lib.rs).

A device can share the connection the same way (`ebus_homie_tokio::device::attach_device`, or `attach_pair` with a controller), provided the host installs the device's last will on it. The contract is in the [`device` module docs](crates/ebus-homie-tokio/src/device/mod.rs).

## Crates

| Crate | Role | `no_std` |
|---|---|---|
| `ebus-rumqttc` | The controller, the device, or both, with their own rumqttc 0.25 connection: TCP, or TLS with the `rustls` or `native-tls` feature; a `reresolve` hook finds a discovered broker again after its address changes | no |
| `ebus-homie-tokio` | Runs the controller and the device on tokio over a host-owned MQTT connection, with typed property handles | no |
| `ebus-discovery-tokio` | Finds the site's broker over mDNS, finds it again after an address change, and advertises the host's services, on tokio; Avahi backend under the `avahi` feature (Linux), mdns-sd backend under the `mdns-sd` feature | no |
| `ebus-homie` | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the controller and device state machines. Performs no I/O | yes, with optional `alloc` |
| `ebus-mqtt` | MQTT vocabulary: messages and commands, topic filters and their ownership, and pairing two roles on one connection | yes, with optional `alloc` |
| `ebus-discovery` | eBus DNS-SD contract: service types, TXT records, the advertisement model, and broker ranking and selection. Performs no I/O | yes, with optional `alloc` |

The core crates take `std` (the default), `alloc`, or neither as cargo features, and are driven by the caller: they receive messages and the current time and return the MQTT commands to send. The design, including subscription pacing, reconnect handling and the Homie state rules, is in [doc/design.md](doc/design.md).

## Spec compliance

The [Homie 5 convention](https://homieiot.github.io/) and the [eBus specification](https://github.com/electrification-bus/specification) decide wire behavior. Where the spec leaves a choice open, rust-sdk follows [python-sdk](https://github.com/electrification-bus/python-sdk); where python-sdk deviates from the spec, rust-sdk follows the spec ([python-sdk#110](https://github.com/electrification-bus/python-sdk/issues/110)). Shared test vectors generated from python-sdk and python-service-discovery are in [vectors/](vectors/).

## Rust version

The minimum supported Rust version is 1.85. CI also tests 1.91.1 and the current stable release, and builds the core crates for `thumbv7em-none-eabihf`.

## Development

```sh
cargo test --workspace
```

The end-to-end tests start their own `mosquitto` and skip when it is not installed. Build commands, test settings and house style are in [AGENTS.md](AGENTS.md); how to contribute is in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT; see [LICENSE](LICENSE).
