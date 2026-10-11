# Changelog

All notable changes to `rust-sdk` are recorded here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
