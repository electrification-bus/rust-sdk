# rust-sdk

[![CI](https://github.com/electrification-bus/rust-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/electrification-bus/rust-sdk/actions/workflows/ci.yml)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](#rust-version)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Rust SDK for the [Electrification Bus](https://github.com/electrification-bus/specification) (eBus): Homie 5 devices and controllers over MQTT, with mDNS discovery. It targets Linux single-board computers first and microcontrollers later, and follows the Homie 5 convention and the eBus specification. Where the spec leaves a choice open, it follows [python-sdk](https://github.com/electrification-bus/python-sdk).

**Status:** pre-alpha. The controller role is implemented; the device role and mDNS discovery are planned. Nothing is published to crates.io yet.

## Crates

| Crate | Role | `no_std` |
|---|---|---|
| `ebus-homie` | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the controller state machine. Performs no I/O. | yes, with optional `alloc` |
| `ebus-mqtt` | MQTT vocabulary: messages and commands, topic filters and their ownership. | yes, with optional `alloc` |
| `ebus-discovery` | eBus DNS-SD contract: service types, TXT records, broker selection, and mDNS backends behind features. | yes, with optional `alloc` |
| `ebus-homie-tokio` | Runs the controller on tokio over a host-owned MQTT connection. | no |
| `ebus-rumqttc` | Bundled rumqttc 0.25 connection for the controller on tokio. | no |

The core crates take `std` (the default), `alloc`, or neither as cargo features. The design is in [doc/design.md](doc/design.md).

## Rust version

The minimum supported Rust version is 1.85. CI also tests 1.91.1 and the current stable release.

## Building

```bash
cargo test --workspace
```

## License

MIT; see [LICENSE](LICENSE).
