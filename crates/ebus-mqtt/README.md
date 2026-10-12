# ebus-mqtt

[![crates.io](https://img.shields.io/crates/v/ebus-mqtt.svg)](https://crates.io/crates/ebus-mqtt)
[![docs.rs](https://img.shields.io/docsrs/ebus-mqtt)](https://docs.rs/ebus-mqtt)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://github.com/electrification-bus/rust-sdk#rust-version)

MQTT vocabulary for eBus: the messages and commands the Homie state machines exchange with an MQTT client, topic filter matching, the ledger of which subscriptions the client holds and who may remove them, and `Pair`, which runs two roles on one connection. It performs no I/O and reads no clock, and builds without `std`.

## Usage

```rust
use ebus_mqtt::filter;

assert!(filter::matches("ebus/5/+/$state", "ebus/5/meter-1/$state"));
assert!(filter::matches("ebus/5/meter-1/#", "ebus/5/meter-1/power/watts"));
// A leading wildcard does not match a topic that starts with `$`.
assert!(!filter::matches("+/5/meter-1/$state", "$SYS/5/meter-1/$state"));
```

## Features

| Feature | Default | Enables |
|---|---|---|
| `std` | yes | Links `std`; implies `alloc` |
| `alloc` | via `std` | `Command`, `LastWill`, `FilterLedger`, `Role` and `Pair` |

## The eBus crates

`ebus-mqtt` is one of six crates in [rust-sdk](https://github.com/electrification-bus/rust-sdk), the Rust SDK for [Electrification Bus](https://github.com/electrification-bus/specification) (eBus), which builds on the [Homie 5 MQTT convention](https://homieiot.github.io/):

| Crate | Role |
|---|---|
| **`ebus-mqtt`** | MQTT vocabulary: messages, commands, topic filters and subscription ownership (`no_std`) |
| [`ebus-homie`](https://crates.io/crates/ebus-homie) | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the controller and device state machines (`no_std`) |
| [`ebus-discovery`](https://crates.io/crates/ebus-discovery) | eBus DNS-SD contract: service types, TXT records, the advertisement model and broker selection (`no_std`) |
| [`ebus-homie-tokio`](https://crates.io/crates/ebus-homie-tokio) | Runs the controller and the device on tokio over an MQTT connection the host owns |
| [`ebus-discovery-tokio`](https://crates.io/crates/ebus-discovery-tokio) | Finds the site's broker over mDNS and advertises the host's services, on tokio |
| [`ebus-rumqttc`](https://crates.io/crates/ebus-rumqttc) | The controller, the device, or both, with their own rumqttc 0.25 connection |

## Minimum Rust version

1.85.

## License

MIT; see [LICENSE](https://github.com/electrification-bus/rust-sdk/blob/main/LICENSE).
