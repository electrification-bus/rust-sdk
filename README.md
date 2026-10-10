# rust-sdk

[![CI](https://github.com/electrification-bus/rust-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/electrification-bus/rust-sdk/actions/workflows/ci.yml)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](#rust-version)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Rust SDK for the [Electrification Bus](https://github.com/electrification-bus/specification) (eBus): Homie 5 devices and controllers over MQTT, with mDNS discovery. It targets Linux single-board computers first and microcontrollers later.

**Status:** pre-alpha. The controller role is implemented; the device role and mDNS discovery are planned. Nothing is published to crates.io yet.

## What it does

### Controller

A controller watches the devices on a broker and sends them commands. It:

- discovers every device in the `ebus` domain, the devices of one tree (a root and its children), or a single device;
- applies each device's `$description` and checks every value against its property's datatype and format;
- tracks each device's `$state` and its effective state (a child of a `lost` root is `lost`);
- reports when a device has received all its retained values (`DeviceSynced`), when its values do not arrive (`DeviceStuck`), and when it goes quiet after a reconnect (`DeviceAbsent`);
- re-syncs after a reconnect and clears values the broker no longer retains;
- sends `/set` and `$broadcast`, checking a `/set` payload against the property first;
- emits nothing when nothing changed: a repeated value, state or description produces no event and no snapshot.

The application reads the latest state as cheap snapshots and receives changes as events.

### Device (planned)

Publishes a device's `$state`, `$description` and values, handles `/set`, and publishes a value only when it changes.

### Discovery (planned)

Finds eBus brokers over mDNS (DNS-SD), first through Avahi on Linux.

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

The `watch` example prints every controller event:

```sh
cargo run -p ebus-rumqttc --example watch -- wildcard
cargo run -p ebus-rumqttc --example watch -- tree <root>
EBUS_MQTT_HOST=broker.local EBUS_MQTT_CA=ca.pem cargo run -p ebus-rumqttc --example watch --features rustls -- wildcard
```

Its source lists every setting: [crates/ebus-rumqttc/examples/watch.rs](crates/ebus-rumqttc/examples/watch.rs).

### Inside a service that already has an MQTT connection

A service can share its own MQTT client with the controller instead of opening a second connection: `ebus_homie_tokio::attach` returns an inbox the client's reader feeds and a command stream the client sends. The host keeps its client version, TLS, last will and reconnect loop. The contract is in the [`ebus-homie-tokio` crate docs](crates/ebus-homie-tokio/src/lib.rs).

## Crates

| Crate | Role | `no_std` |
|---|---|---|
| `ebus-rumqttc` | The controller with its own rumqttc 0.25 connection: TCP, or TLS with the `rustls` or `native-tls` feature | no |
| `ebus-homie-tokio` | Runs the controller on tokio over a host-owned MQTT connection | no |
| `ebus-homie` | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the controller state machine. Performs no I/O | yes, with optional `alloc` |
| `ebus-mqtt` | MQTT vocabulary: messages and commands, topic filters and their ownership | yes, with optional `alloc` |
| `ebus-discovery` | eBus DNS-SD contract (placeholder) | yes, with optional `alloc` |

The core crates take `std` (the default), `alloc`, or neither as cargo features, and are driven by the caller: they receive messages and the current time and return the MQTT commands to send. The design, including subscription pacing, reconnect handling and the Homie state rules, is in [doc/design.md](doc/design.md).

## Spec compliance

The [Homie 5 convention](https://homieiot.github.io/) and the [eBus specification](https://github.com/electrification-bus/specification) decide wire behavior. Where the spec leaves a choice open, rust-sdk follows [python-sdk](https://github.com/electrification-bus/python-sdk); where python-sdk deviates from the spec, rust-sdk follows the spec ([python-sdk#110](https://github.com/electrification-bus/python-sdk/issues/110)). Shared test vectors generated from python-sdk are in [vectors/](vectors/).

## Rust version

The minimum supported Rust version is 1.85. CI also tests 1.91.1 and the current stable release, and builds the core crates for `thumbv7em-none-eabihf`.

## Development

```sh
cargo test --workspace
```

The end-to-end tests start their own `mosquitto` and skip when it is not installed. Build commands, test settings and house style are in [AGENTS.md](AGENTS.md); how to contribute is in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT; see [LICENSE](LICENSE).
