# Changelog

All notable changes to `rust-sdk` are recorded here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-12

### Added

- `doc/python-sdk.md`, a guide for python-sdk users mapping each python-sdk pattern to rust-sdk, with code compiled as doctests of the unpublished `guide-doctest` workspace member, which also compiles the root README's examples.
- Cargo workspace with the crates `ebus-homie`, `ebus-mqtt`, `ebus-discovery` and `ebus-homie-tokio`, and CI for rustfmt, clippy, tests on Rust 1.85, 1.91.1 and stable, a `no_std` build of the core crates, and rustdoc.
- Homie 5 controller role: the sans-IO controller in `ebus-homie`, MQTT topic filters and subscription ownership in `ebus-mqtt`, the tokio driver in `ebus-homie-tokio`, and the bundled rumqttc 0.25 connection in the new `ebus-rumqttc` crate, with a `watch` example and end-to-end tests against mosquitto.
- `check_value` for checking a payload against its property, and `float32` payload formatting.
- Homie 5 device role: the sans-IO device tree in `ebus-homie`, typed property, settable and event handles and the device driver in `ebus-homie-tokio`, and `device()` on the bundled rumqttc connection, with a `device` example, end-to-end tests against mosquitto and python-sdk, and a benchmark.
- `Role` and `Pair` in `ebus-mqtt`, which run a device and a controller on one connection (`ebus_rumqttc::pair`, `attach_pair`).
- The eBus DNS-SD contract in `ebus-discovery`: service types and broker preference, TXT record decoding, checks and building, the advertisement model and instance names, merging mDNS sightings into instances, broker ranking in the client's preference order among broker types, matching and selection, finding a broker by a device id any of its transports carries, and the sans-IO `BrokerSearch`, with vectors generated from python-service-discovery.
- `ebus-discovery-tokio`: the `Backend`, `Browse` and `Group` seam; `Mdns` with the Avahi backend over D-Bus (feature `avahi`, Linux) and the mdns-sd backend (feature `mdns-sd`), `Mdns::system` falling back to mdns-sd without avahi-daemon; `list_brokers`, `find_broker` and `reresolve`; `advertise` and `Advertiser`, with instance-name and host-name collision handling, TXT updates that send only what changed, and withdrawal on drop.
- `ebus_rumqttc::Reresolve` and `reresolve` on `ControllerTask`, `DeviceTask` and `PairTask`: after the first failed connect since startup or since the last successful connect, and every third after it, the hook's options replace the connection's, with the device's will, clean session, inflight cap and packet limit applied as at startup.
- `MdnsSdOptions::interfaces` limits the mdns-sd backend to the named interfaces.
- The `watch` and `device` examples find their broker over mDNS when neither `EBUS_MQTT_HOST` nor `EBUS_MQTT_PORT` is set, take `EBUS_BROKER_MODE` and `EBUS_MDNS_BACKEND`, and find a discovered broker again after its address changes.
- The `simple-span-controller` example, a port of python-sdk's: it finds a SPAN panel's broker over mDNS by the SRV target `span-<serial-number>.local`, reads the password from the span-auth file, verifies TLS against the panel's cached or downloaded CA certificate under the panel's host name, and reports the MID's `grid` node and the enclosure's `power-flows`.
- MQTT 5 room in `ebus-mqtt`: `Message`, `LastWill`, `Link`, `Link::Up` and each `Command` variant are `#[non_exhaustive]`; build them with `Message::new`, `LastWill::new`, `Link::up`, `Command::subscribe`, `Command::unsubscribe` and `Command::publish`, and match them with `..`, and `Link` with a `_` arm.
- `#[non_exhaustive]` on each struct variant of `controller::Event` and `device::DeviceEvent`, on `PropertyDescription`, `NodeDescription`, `DeviceDescription`, `DeviceSpec`, `NodeSpec`, `PropSpec`, `SetOptions`, `PropertyView`, `controller::Value`, `description::Ignored`, `Delivered`, `AttachedController`, `AttachedDevice`, `AttachedPair`, `RunningController`, `RunningDevice`, `RunningPair`, `BrokerEndpoint`, `BrokerType`, `EbusTxt`, `TxtReport`, `Instance` (built with `Instance::new`) and `ServiceRecord`. New chained setters: `DeviceSpec::name`, `DeviceSpec::extension`, `DeviceSpec::extra`, `NodeSpec::name`, `SetOptions::allow_not_ready` and `SetOptions::unchecked`. `BROKER_TYPES`, `TCP_BROKER_TYPES` and `BrokerSearch::browse_types` are slices.
- The controller's entry points are named like the device's and the pair's: `attach_controller`, `AttachedController`, `ControllerOptions`, `RunningController` and `controller_client_id`. The core tree builder is `ebus_homie::device::TreeBuilder`, the broker configuration error `BrokerConfigError`, and `DeviceState`'s parse error `ParseStateError`; `ControllerHandle::device` and `devices` return `DeviceView`. `DirtySet` is internal to `ebus-homie-tokio`, and `ebus_homie::pyfmt` is hidden from the documentation.
- Every `ebus-homie` error type implements `core::error::Error` without `std`. `ebus_discovery_tokio::Error`, `ebus_rumqttc::Error` and `AttachError` print only their own context and return the error they wrap from `source()`.
- `ebus_homie::serde_json` and `ebus_homie_tokio::bytes` re-export the crates those types come from; `Debug` for `MdnsBrowse`, `MdnsGroup` and `Reconfigure`; docs.rs builds every crate with all features and marks feature-gated items.
- Each crate has a README, crates.io keywords and categories, and ships its LICENSE; a `v*` tag publishes the six crates to crates.io and creates the GitHub release through `.github/workflows/release.yml`.

[Unreleased]: https://github.com/electrification-bus/rust-sdk/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/electrification-bus/rust-sdk/releases/tag/v0.1.0
