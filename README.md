# rust-sdk

Rust SDK for the [Electrification Bus](https://github.com/electrification-bus/specification) (eBus): Homie 5 devices and controllers over MQTT, with mDNS discovery. It targets Linux single-board computers first and microcontrollers later, and publishes the same bytes on the wire as [python-sdk](https://github.com/electrification-bus/python-sdk).

**Status:** pre-alpha. The crates below are placeholders; nothing is published to crates.io yet.

## Crates

| Crate | Role | `no_std` |
|---|---|---|
| `ebus-homie` | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the device and controller state machines. Performs no I/O. | yes, with optional `alloc` |
| `ebus-mqtt` | Transport trait, the hold for publishes made while the broker link is down, and the reconnect order. | yes, with optional `alloc` |
| `ebus-discovery` | eBus DNS-SD contract: service types, TXT records, broker selection, and mDNS backends behind features. | yes, with optional `alloc` |
| `ebus-homie-tokio` | Runs devices and controllers on tokio over an `ebus-mqtt` transport. | no |

The core crates take `std` (the default), `alloc`, or neither as cargo features.

## Rust version

The minimum supported Rust version is 1.85. CI also tests 1.91.1 and the current stable release.

## Building

```bash
cargo test --workspace
```

## License

MIT; see [LICENSE](LICENSE).
