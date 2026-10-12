# ebus-discovery-tokio

[![crates.io](https://img.shields.io/crates/v/ebus-discovery-tokio.svg)](https://crates.io/crates/ebus-discovery-tokio)
[![docs.rs](https://img.shields.io/docsrs/ebus-discovery-tokio)](https://docs.rs/ebus-discovery-tokio)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://github.com/electrification-bus/rust-sdk#rust-version)

mDNS broker discovery and host advertisement for eBus on tokio. It runs the sans-IO [`ebus-discovery`](https://crates.io/crates/ebus-discovery) over an mDNS stack: `find_broker` and `list_brokers` find the site's broker, `reresolve` finds the same broker again after its address changes, and `advertise` publishes the host's own services. The backends are avahi-daemon over D-Bus (Linux) and the pure-Rust mdns-sd; `Mdns::system` picks one at run time.

## Usage

```rust,no_run
use ebus_discovery_tokio::ebus_discovery::{BrokerConfig, BrokerEndpoint, BrokerMode};
use ebus_discovery_tokio::{Mdns, find_broker};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mdns = Mdns::system().await?;
    let mut cfg = BrokerConfig::default();
    cfg.mode = Some(BrokerMode::DiscoveryWithFallback);
    cfg.url = Some(BrokerEndpoint::from_url("mqtt://broker.example:1883")?);
    let broker = find_broker(&mdns, cfg).await?;
    println!("connect to {}:{}", broker.connect_host(), broker.port);
    Ok(())
}
```

Enable a backend feature; built with neither, `Mdns::system` returns `Error::NoBackend`, and a host can still pass its own `Backend`.

## Features

| Feature | Default | Enables |
|---|---|---|
| `avahi` | no | The Avahi backend, a client of avahi-daemon over D-Bus; compiles only on Linux |
| `mdns-sd` | no | The mdns-sd backend, with its own responder thread and sockets |

## The eBus crates

`ebus-discovery-tokio` is one of six crates in [rust-sdk](https://github.com/electrification-bus/rust-sdk), the Rust SDK for [Electrification Bus](https://github.com/electrification-bus/specification) (eBus), which builds on the [Homie 5 MQTT convention](https://homieiot.github.io/):

| Crate | Role |
|---|---|
| [`ebus-mqtt`](https://crates.io/crates/ebus-mqtt) | MQTT vocabulary: messages, commands, topic filters and subscription ownership (`no_std`) |
| [`ebus-homie`](https://crates.io/crates/ebus-homie) | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the controller and device state machines (`no_std`) |
| [`ebus-discovery`](https://crates.io/crates/ebus-discovery) | eBus DNS-SD contract: service types, TXT records, the advertisement model and broker selection (`no_std`) |
| [`ebus-homie-tokio`](https://crates.io/crates/ebus-homie-tokio) | Runs the controller and the device on tokio over an MQTT connection the host owns |
| **`ebus-discovery-tokio`** | Finds the site's broker over mDNS and advertises the host's services, on tokio |
| [`ebus-rumqttc`](https://crates.io/crates/ebus-rumqttc) | The controller, the device, or both, with their own rumqttc 0.25 connection |

## Minimum Rust version

1.85.

## License

MIT; see [LICENSE](https://github.com/electrification-bus/rust-sdk/blob/main/LICENSE).
