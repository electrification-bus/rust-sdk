# Changelog

All notable changes to `rust-sdk` are recorded here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Cargo workspace with the crates `ebus-homie`, `ebus-mqtt`, `ebus-discovery` and `ebus-homie-tokio`, and CI for rustfmt, clippy, tests on Rust 1.85, 1.91.1 and stable, a `no_std` build of the core crates, and rustdoc.
- Homie 5 controller role: the sans-IO controller in `ebus-homie`, MQTT topic filters and subscription ownership in `ebus-mqtt`, the tokio driver in `ebus-homie-tokio`, and the bundled rumqttc 0.25 connection in the new `ebus-rumqttc` crate, with a `watch` example and end-to-end tests against mosquitto.
- `check_value` for checking a payload against its property, and `float32` payload formatting.
- Homie 5 device role: the sans-IO device tree in `ebus-homie`, typed property, settable and event handles and the device driver in `ebus-homie-tokio`, and `device()` on the bundled rumqttc connection, with a `device` example, end-to-end tests against mosquitto and python-sdk, and a benchmark.
- `Role` and `Pair` in `ebus-mqtt`, which run a device and a controller on one connection (`ebus_rumqttc::pair`, `attach_pair`).
