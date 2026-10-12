# ebus-homie

[![crates.io](https://img.shields.io/crates/v/ebus-homie.svg)](https://crates.io/crates/ebus-homie)
[![docs.rs](https://img.shields.io/docsrs/ebus-homie)](https://docs.rs/ebus-homie)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://github.com/electrification-bus/rust-sdk#rust-version)

Homie 5 protocol core for eBus: identifiers, topics, the nine datatypes and their payload checks, the `$description` model, and the controller and device state machines. The crate performs no I/O: the caller passes in received MQTT messages and the current time, and sends the commands it returns, so the same core serves tokio services and microcontrollers. It builds without `std`.

## Usage

A controller following one device, fed by hand where an MQTT client would feed it:

```rust
use ebus_homie::controller::{Config, Controller, Event};
use ebus_mqtt::{Command, Instant, Link, Message};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = Controller::new(Config::device("meter-1")?)?;
    let now = Instant(0);
    c.handle_link(now, Link::up(false));
    while let Some(cmd) = c.poll_command() {
        if let Command::Subscribe { filters, .. } = cmd {
            assert!(filters.iter().any(|(f, _)| f == "ebus/5/meter-1/$state"));
        }
    }

    let retained = |topic, payload: &'static str| Message::new(topic, payload.as_bytes(), Some(true));
    c.handle_message(now, retained("ebus/5/meter-1/$state", "ready"));
    c.handle_message(
        now,
        retained(
            "ebus/5/meter-1/$description",
            r#"{"homie":"5.0","version":1,"nodes":{"power":{"properties":{"watts":{"datatype":"float"}}}}}"#,
        ),
    );
    while c.poll_command().is_some() {}
    c.handle_message(now, retained("ebus/5/meter-1/power/watts", "1234.5"));

    let mut synced = false;
    while let Some(e) = c.poll_event() {
        synced |= matches!(e, Event::DeviceSynced { missing: 0, .. });
    }
    assert!(synced);
    let watts = c.device("meter-1").and_then(|d| d.property("power", "watts"));
    assert_eq!(watts.and_then(|p| p.value.as_ref()).map(|v| &*v.raw), Some("1234.5"));
    Ok(())
}
```

`ebus-homie` uses `ebus-mqtt` types for messages, commands and time; add both to `Cargo.toml`. To run the controller or a device on tokio, use [`ebus-homie-tokio`](https://crates.io/crates/ebus-homie-tokio) or [`ebus-rumqttc`](https://crates.io/crates/ebus-rumqttc).

## Features

| Feature | Default | Enables |
|---|---|---|
| `std` | yes | Links `std`; implies `alloc` |
| `alloc` | via `std` | The `controller`, `device` and `description` modules and the allocating parts of `datatype` |
| `log` | no | Diagnostics through the `log` crate |

## The eBus crates

`ebus-homie` is one of six crates in [rust-sdk](https://github.com/electrification-bus/rust-sdk), the Rust SDK for [Electrification Bus](https://github.com/electrification-bus/specification) (eBus), which builds on the [Homie 5 MQTT convention](https://homieiot.github.io/):

| Crate | Role |
|---|---|
| [`ebus-mqtt`](https://crates.io/crates/ebus-mqtt) | MQTT vocabulary: messages, commands, topic filters and subscription ownership (`no_std`) |
| **`ebus-homie`** | Homie 5 protocol core: identifiers, topics, datatypes, `$description`, and the controller and device state machines (`no_std`) |
| [`ebus-discovery`](https://crates.io/crates/ebus-discovery) | eBus DNS-SD contract: service types, TXT records, the advertisement model and broker selection (`no_std`) |
| [`ebus-homie-tokio`](https://crates.io/crates/ebus-homie-tokio) | Runs the controller and the device on tokio over an MQTT connection the host owns |
| [`ebus-discovery-tokio`](https://crates.io/crates/ebus-discovery-tokio) | Finds the site's broker over mDNS and advertises the host's services, on tokio |
| [`ebus-rumqttc`](https://crates.io/crates/ebus-rumqttc) | The controller, the device, or both, with their own rumqttc 0.25 connection |

## Minimum Rust version

1.85.

## License

MIT; see [LICENSE](https://github.com/electrification-bus/rust-sdk/blob/main/LICENSE).
