# ebus-rumqttc

[![crates.io](https://img.shields.io/crates/v/ebus-rumqttc.svg)](https://crates.io/crates/ebus-rumqttc)
[![docs.rs](https://img.shields.io/docsrs/ebus-rumqttc)](https://docs.rs/ebus-rumqttc)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://github.com/electrification-bus/rust-sdk#rust-version)

The eBus Homie 5 controller and device on tokio with their own [rumqttc](https://crates.io/crates/rumqttc) 0.25 connection: `controller`, `device`, or `pair` for both on one connection. The broker, client id, credentials and TLS come from the caller's `rumqttc::MqttOptions`. A host that found its broker over mDNS with [`ebus-discovery-tokio`](https://crates.io/crates/ebus-discovery-tokio) gives the task a `reresolve` hook, which finds the same broker again when connecting fails.

## Usage

Watch every device on a broker:

```rust,no_run
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event};
use ebus_rumqttc::ebus_homie_tokio::{ControllerOptions, Delivered};
use ebus_rumqttc::rumqttc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut mqtt = rumqttc::MqttOptions::new(ebus_rumqttc::controller_client_id(), "localhost", 1883);
    // A `$description` can exceed rumqttc's 10 KiB default packet size.
    mqtt.set_max_packet_size(4 << 20, 4 << 20);

    let run = ebus_rumqttc::controller(mqtt, Config::wildcard(), ControllerOptions::default())?;
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

The [repository](https://github.com/electrification-bus/rust-sdk) has the `watch`, `device`, `tree-device`, `set` and `simple-span-controller` examples.

## Features

| Feature | Default | Enables |
|---|---|---|
| `rustls` | no | MQTT over TLS through rustls |
| `native-tls` | no | MQTT over TLS through the platform's TLS library |

## The eBus crates

`ebus-rumqttc` is one of six crates in [rust-sdk](https://github.com/electrification-bus/rust-sdk), the Rust SDK for [Electrification Bus](https://github.com/electrification-bus/specification) (eBus), which builds on the [Homie 5 MQTT convention](https://homieiot.github.io/):

| Crate | Role |
|---|---|
| [`ebus-mqtt`](https://crates.io/crates/ebus-mqtt) | MQTT vocabulary: messages, commands, topic filters and subscription ownership (`no_std`) |
| [`ebus-homie`](https://crates.io/crates/ebus-homie) | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the controller and device state machines (`no_std`) |
| [`ebus-discovery`](https://crates.io/crates/ebus-discovery) | eBus DNS-SD contract: service types, TXT records, the advertisement model and broker selection (`no_std`) |
| [`ebus-homie-tokio`](https://crates.io/crates/ebus-homie-tokio) | Runs the controller and the device on tokio over an MQTT connection the host owns |
| [`ebus-discovery-tokio`](https://crates.io/crates/ebus-discovery-tokio) | Finds the site's broker over mDNS and advertises the host's services, on tokio |
| **`ebus-rumqttc`** | The controller, the device, or both, with their own rumqttc 0.25 connection |

## Minimum Rust version

1.85.

## License

MIT; see [LICENSE](https://github.com/electrification-bus/rust-sdk/blob/main/LICENSE).
