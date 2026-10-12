# ebus-homie-tokio

[![crates.io](https://img.shields.io/crates/v/ebus-homie-tokio.svg)](https://crates.io/crates/ebus-homie-tokio)
[![docs.rs](https://img.shields.io/docsrs/ebus-homie-tokio)](https://docs.rs/ebus-homie-tokio)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://github.com/electrification-bus/rust-sdk#rust-version)

Tokio runtime for the eBus Homie 5 controller and device. It drives the sans-IO state machines of [`ebus-homie`](https://crates.io/crates/ebus-homie) over an MQTT connection the host already owns: the host keeps its client, TLS, last will and reconnect loop, feeds received messages to an inbox, and sends the commands the SDK yields. The application reads snapshots and events through a handle, and a device publishes through typed property handles. For a connection the SDK opens itself, use [`ebus-rumqttc`](https://crates.io/crates/ebus-rumqttc).

## Usage

```rust,no_run
use ebus_homie_tokio::ebus_homie::controller::Config;
use ebus_homie_tokio::{ControllerOptions, attach_controller};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ebus = attach_controller(Config::tree("panel-1")?, ControllerOptions::default())?;
    tokio::spawn(ebus.driver.run());

    // The host's MQTT reader reports each CONNACK, lost connection and
    // message to the inbox.
    let inbox = ebus.inbox.clone();
    inbox.connected(false);

    // The host's executor hands each command to its MQTT client.
    let mut commands = ebus.commands;
    while let Some(command) = commands.recv().await {
        println!("{command:?}");
    }
    Ok(())
}
```

The contract the host's connection must keep, and a complete example, are in the [crate documentation](https://docs.rs/ebus-homie-tokio); the device's is in the [`device` module](https://docs.rs/ebus-homie-tokio/latest/ebus_homie_tokio/device/).

## The eBus crates

`ebus-homie-tokio` is one of six crates in [rust-sdk](https://github.com/electrification-bus/rust-sdk), the Rust SDK for [Electrification Bus](https://github.com/electrification-bus/specification) (eBus), which builds on the [Homie 5 MQTT convention](https://homieiot.github.io/):

| Crate | Role |
|---|---|
| [`ebus-mqtt`](https://crates.io/crates/ebus-mqtt) | MQTT vocabulary: messages, commands, topic filters and subscription ownership (`no_std`) |
| [`ebus-homie`](https://crates.io/crates/ebus-homie) | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the controller and device state machines (`no_std`) |
| [`ebus-discovery`](https://crates.io/crates/ebus-discovery) | eBus DNS-SD contract: service types, TXT records, the advertisement model and broker selection (`no_std`) |
| **`ebus-homie-tokio`** | Runs the controller and the device on tokio over an MQTT connection the host owns |
| [`ebus-discovery-tokio`](https://crates.io/crates/ebus-discovery-tokio) | Finds the site's broker over mDNS and advertises the host's services, on tokio |
| [`ebus-rumqttc`](https://crates.io/crates/ebus-rumqttc) | The controller, the device, or both, with their own rumqttc 0.25 connection |

## Minimum Rust version

1.85.

## License

MIT; see [LICENSE](https://github.com/electrification-bus/rust-sdk/blob/main/LICENSE).
