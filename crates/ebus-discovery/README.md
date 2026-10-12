# ebus-discovery

[![crates.io](https://img.shields.io/crates/v/ebus-discovery.svg)](https://crates.io/crates/ebus-discovery)
[![docs.rs](https://img.shields.io/docsrs/ebus-discovery)](https://docs.rs/ebus-discovery)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://github.com/electrification-bus/rust-sdk#rust-version)

The eBus DNS-SD contract: the service types an eBus host advertises and browses, the broker transports in the specification's preference order, TXT record decoding, checks and building, the advertisement model, merging the sightings an mDNS backend reports, and broker ranking and selection in the sans-IO `BrokerSearch`. It performs no I/O and reads no clock, and builds without `std`. [`ebus-discovery-tokio`](https://crates.io/crates/ebus-discovery-tokio) runs it over a real mDNS stack.

## Usage

A search for a plain MQTT broker, fed one sighting by hand where a backend would report it:

```rust
use ebus_discovery::{
    BrokerConfig, BrokerSearch, Instant, InstanceKey, ScopedAddr, Sighting, Slot, TxtRecord,
};

fn main() -> Result<(), ebus_discovery::BrokerConfigError> {
    let mut search = BrokerSearch::new(Instant(0), BrokerConfig::default())?;
    search.handle(
        Instant(0),
        Sighting::Seen {
            key: InstanceKey {
                service_type: "_mqtt._tcp".into(),
                name: "site-broker".into(),
            },
            slot: Slot::Whole,
            server: "broker.local.".into(),
            port: 1883,
            txt: TxtRecord::new(),
            addrs: vec![ScopedAddr::new([192, 168, 1, 10].into())],
        },
    );
    while let Some(at) = search.poll_timeout() {
        search.handle_timeout(at);
        if search.result().is_some() {
            break;
        }
    }
    let broker = search.result().expect("a broker");
    assert_eq!((broker.host.as_str(), broker.port), ("broker.local", 1883));
    Ok(())
}
```

## Features

| Feature | Default | Enables |
|---|---|---|
| `std` | yes | Links `std`; implies `alloc` |
| `alloc` | via `std` | TXT records, the advertisement model, instances, broker ranking and `BrokerSearch` |
| `log` | no | Diagnostics through the `log` crate |

## The eBus crates

`ebus-discovery` is one of six crates in [rust-sdk](https://github.com/electrification-bus/rust-sdk), the Rust SDK for [Electrification Bus](https://github.com/electrification-bus/specification) (eBus), which builds on the [Homie 5 MQTT convention](https://homieiot.github.io/):

| Crate | Role |
|---|---|
| [`ebus-mqtt`](https://crates.io/crates/ebus-mqtt) | MQTT vocabulary: messages, commands, topic filters and subscription ownership (`no_std`) |
| [`ebus-homie`](https://crates.io/crates/ebus-homie) | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the controller and device state machines (`no_std`) |
| **`ebus-discovery`** | eBus DNS-SD contract: service types, TXT records, the advertisement model and broker selection (`no_std`) |
| [`ebus-homie-tokio`](https://crates.io/crates/ebus-homie-tokio) | Runs the controller and the device on tokio over an MQTT connection the host owns |
| [`ebus-discovery-tokio`](https://crates.io/crates/ebus-discovery-tokio) | Finds the site's broker over mDNS and advertises the host's services, on tokio |
| [`ebus-rumqttc`](https://crates.io/crates/ebus-rumqttc) | The controller, the device, or both, with their own rumqttc 0.25 connection |

## Minimum Rust version

1.85.

## License

MIT; see [LICENSE](https://github.com/electrification-bus/rust-sdk/blob/main/LICENSE).
