# Coming from python-sdk

rust-sdk covers the same two roles as python-sdk, with a different shape: python-sdk calls your functions from its MQTT thread, while rust-sdk hands values, `/set` requests and events to your own tasks through typed handles and channels, so each piece of state has one owner and needs no lock. The SDK also spawns nothing: `ebus_rumqttc::device` and `ebus_rumqttc::controller` return a `task` future that you spawn.

Every Rust block below is compiled as a doctest of `ebus-rumqttc` (`cargo test -p ebus-rumqttc --doc`). They use the crates `ebus-rumqttc` (whose `ebus_homie_tokio` re-export carries `ebus_homie` and `ebus_mqtt`), `rumqttc`, `tokio` and, for discovery, `ebus-discovery-tokio`.

## Device

| python-sdk | rust-sdk |
|---|---|
| `homie.Device(id, mqtt_cfg=cfg)`, `start_mqtt_client()` | `TreeBuilder::new(..)`, then `ebus_rumqttc::device(mqtt, builder, DeviceOptions::default())` and spawn `run.task.run()` |
| `Device(id, parent=root)` | `builder.child(parent, id, DeviceSpec::default())` |
| `add_node_from_dict`, `add_property_from_dict` | `builder.node(..)`, then `builder.property::<T>`, `settable::<T>` or `event::<T>`; the Rust type `T` gives the datatype |
| `homie.Property.set_value(v)` | `Property::set(v)`; an unchanged value publishes nothing |
| Observable `Property`, `add_on_change_callback` | `Property::subscribe()`, a `tokio::sync::watch::Receiver` |
| `set_set_callback(fn)`, `entity_setter` | `Settable::next_set().await`, then `accept()`, `accept_as(v)` or `reject(why)` |
| `bind_property_to_homie` (on-change for retained, on-set for events) | Nothing to bind: `property::<T>` is retained and change-gated, `event::<T>` publishes every `emit` |
| Observable model plus Homie twin (the proxy pattern) | The handle is the model: hand clones of `Property<T>` to your acquisition code |
| `with device.state_transition():` | `DeviceHandle::reconfigure(\|h\| ..)`, one commit |
| `GroupedPropertyDict.bulk_update()` | No call needed: values set between driver passes coalesce, and the latest is published once |
| `device.stop()` | `DeviceHandle::shutdown(Shutdown::Announce).await` |

### Observing a value

```rust,no_run
use ebus_rumqttc::ebus_homie_tokio::device::{NodeSpec, PropSpec, TreeBuilder};

fn temperature(b: &mut TreeBuilder) -> Result<(), Box<dyn std::error::Error>> {
    let node = b.node(b.root(), "sensor", NodeSpec::typed("example.sensor"))?;
    let temp = b.property::<f64>(node, "temperature", PropSpec::new().unit("°C"))?;

    let mut changes = temp.subscribe();
    tokio::spawn(async move {
        while changes.changed().await.is_ok() {
            println!("temperature = {:?}", *changes.borrow_and_update());
        }
    });

    temp.set(21.5)?; // published, and the receiver wakes
    temp.set(21.5)?; // unchanged: nothing published, nothing wakes
    Ok(())
}
```

A `watch` receiver holds only the latest value, so a slow reader sees the newest value rather than every one in between.

### `/set`

```rust,no_run
use ebus_rumqttc::ebus_homie_tokio::device::Settable;

async fn relay(mut relay: Settable<bool>) {
    while let Some(req) = relay.next_set().await {
        match switch_relay(*req.value()) {
            Ok(reached) => req.accept_as(reached),
            Err(why) => req.reject(why),
        }
    }
}

fn switch_relay(on: bool) -> Result<bool, &'static str> {
    Ok(on)
}
```

The request is checked against the property's `$format` and step-rounded before it reaches you. It is a value you own, so the handler can await I/O before it answers. `accept()` sets the property to the requested value (on a property with `$target`, it publishes `$target` and leaves the value to `set`), `accept_as(v)` to the value the device reached, and `reject` publishes nothing.

### Property or event

```rust,no_run
use ebus_rumqttc::ebus_homie_tokio::device::{NodeIdx, PropSpec, TreeBuilder};

fn meter(b: &mut TreeBuilder, node: NodeIdx) -> Result<(), Box<dyn std::error::Error>> {
    let power = b.property::<f64>(node, "active-power", PropSpec::new().unit("W"))?;
    let trip = b.event::<String>(node, "trip", PropSpec::new())?;

    power.set(1850.0)?; // retained; a repeat publishes nothing
    trip.emit("overcurrent".to_owned())?; // not retained; every call publishes
    Ok(())
}
```

`emit` returns `EmitError::Down` while the link is down: an event is never held for later.

### Proxy

```rust,no_run
use std::time::Duration;

use ebus_rumqttc::ebus_homie_tokio::device::{Property, Settable};

async fn proxy(power: Property<f64>, mut relay: Settable<bool>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let _ = power.set(read_meter());
            }
            req = relay.next_set() => {
                let Some(req) = req else { break };
                write_relay(*req.value());
                req.accept();
            }
        }
    }
}

fn read_meter() -> f64 {
    1850.0
}

fn write_relay(_on: bool) {}
```

`Property<T>` is `Clone`, `Send` and `Sync`, checks each value on the caller's thread, and drops unchanged values itself, so it does the observable model's job and no separate model or binding is needed. `Property::get` reads the value back.

### Changing the tree

```rust,no_run
use ebus_rumqttc::ebus_homie_tokio::device::{
    BuildError, DevIdx, DeviceHandle, DeviceSpec, NodeSpec, PropSpec, Property,
};

async fn add_circuit(handle: &DeviceHandle, root: DevIdx) -> Result<Property<f64>, BuildError> {
    handle
        .reconfigure(move |h| {
            let circuit = h.core().add_child(root, "circuit-4", DeviceSpec::default())?;
            let node = h.core().add_node(circuit, "meter", NodeSpec::default())?;
            h.property::<f64>(node, "active-power", PropSpec::new().unit("W"))
        })
        .await
}
```

Like `state_transition()`, the commit publishes `init` and the changed descriptions, then the state. python-sdk always ends in `ready`, even after an exception; rust-sdk returns each device to the state it had (`ready` or `sleeping`), and an error inside the closure changes nothing.

### Many values at once

```rust,no_run
use ebus_rumqttc::ebus_homie_tokio::device::Property;

fn update(circuits: &[Property<f64>], readings: &[f64]) {
    for (p, v) in circuits.iter().zip(readings) {
        let _ = p.set(*v);
    }
}
```

`set` marks the property changed and returns at once; the driver publishes each changed property's latest value when it next runs.

## Controller

| python-sdk | rust-sdk |
|---|---|
| `Controller(mqtt_cfg=cfg)`, `start_discovery()` | `ebus_rumqttc::controller(mqtt, Config::wildcard(), ControllerOptions::default())` and spawn `run.task.run()` |
| `Controller(root_device_id=r)` | `Config::tree(r)?` |
| `set_on_device_discovered_callback` | `Event::DeviceDiscovered` |
| `set_on_device_state_changed_callback` | `Event::StateChanged` (`old: None` right after discovery) |
| `set_on_description_received_callback` | `Event::DescriptionChanged` |
| `set_on_property_changed_callback` | `Event::PropertyChanged`; `Event::PropertyEvent` for a `retained: false` property |
| `set_on_tree_ready_callback` | `Event::TreeComplete` |
| `set_on_device_removed_callback` | `Event::DeviceRemoved` |
| `set_on_disconnect_callback` | `Event::Link(Link::Down)` |
| `get_device(id)`, `get_all_devices()` | `ControllerHandle::device(id)`, `devices()` |
| `DiscoveredDevice.get_property(node, prop)` | `DeviceView::property(node, prop)` |
| `get_effective_state(id)` | `ControllerHandle::effective_state(id)` |
| `set_property(dev, node, prop, value)` | `ControllerHandle::set(PropertyAddr::new(dev, node, prop)?, value).await` |
| `controller.stop()` | `ControllerHandle::shutdown().await` |

### Events and snapshots

```rust,no_run
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::Event;
use ebus_rumqttc::ebus_homie_tokio::ebus_mqtt::Link;
use ebus_rumqttc::ebus_homie_tokio::{ControllerHandle, Delivered, EventReceiver};

async fn watch(handle: ControllerHandle, mut events: EventReceiver) {
    while let Some(d) = events.recv().await {
        match d {
            Delivered::Event(Event::DeviceDiscovered { device, state, .. }) => {
                println!("discovered {device} ({state})");
            }
            Delivered::Event(Event::StateChanged { device, old: Some(old), new, .. }) => {
                println!("{device}: {old} -> {new}");
            }
            Delivered::Event(Event::DescriptionChanged { device, description, .. }) => {
                println!("{device}: {} nodes", description.nodes.len());
            }
            Delivered::Event(Event::PropertyChanged { addr, value, .. }) => {
                println!("{}/{}/{} = {}", addr.device, addr.node, addr.property, value.raw);
            }
            Delivered::Event(Event::Link(Link::Down)) => println!("disconnected"),
            Delivered::Overflow { dropped } => {
                println!("{dropped} events dropped; reading the snapshot");
                for view in handle.devices() {
                    println!("{} is {:?}", view.id, view.effective);
                }
            }
            _ => {}
        }
    }
}
```

The event channel is bounded so a slow reader never stalls the controller; when it fills, `Delivered::Overflow` says how many events were lost and the snapshot holds the current state. The snapshot already reflects an event by the time the event arrives.

### Reading and setting

```rust,no_run
use ebus_rumqttc::ebus_homie_tokio::ControllerHandle;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::PropertyAddr;

async fn relay_on(handle: &ControllerHandle) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(view) = handle.device("circuit-3") {
        let value = view.property("switch", "relay").and_then(|p| p.value.as_ref());
        println!("relay = {:?}", value.map(|v| &*v.raw));
    }
    println!("effective state: {:?}", handle.effective_state("circuit-3"));

    let addr = PropertyAddr::new("circuit-3", "switch", "relay").ok_or("invalid id")?;
    handle.set(addr, "true").await?;
    Ok(())
}
```

`set` refuses the command unless the link is up, the property is settable, the device is `ready` and the payload is valid for the property.

## Discovery and connection

| python-sdk | rust-sdk |
|---|---|
| `mqtt_cfg` dict: `host`, `port`, `authentication` | `rumqttc::MqttOptions::new(client_id, host, port)`, `set_credentials(user, password)` |
| `use_tls`, `tls_ca_cert` | `MqttOptions::set_transport`, with the `rustls` or `native-tls` feature of `ebus-rumqttc` |
| `ebus_service_discovery.mdns.find_broker(mode, url)` | `ebus_discovery_tokio::find_broker(&Mdns::system().await?, BrokerConfig)`, with the `mdns-sd` or `avahi` feature of `ebus-discovery-tokio` |
| `BrokerEndpoint.mqtt_cfg(base)` | `MqttOptions::new(id, endpoint.connect_host(), endpoint.port)` |

```rust,no_run
use ebus_discovery_tokio::ebus_discovery::BrokerConfig;
use ebus_discovery_tokio::{Mdns, find_broker};
use ebus_rumqttc::ebus_homie_tokio::device::{DeviceOptions, DeviceSpec, TreeBuilder, TreeConfig};
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::Config;
use ebus_rumqttc::ebus_homie_tokio::{ControllerOptions, EventReceiver};

async fn connect() -> Result<EventReceiver, Box<dyn std::error::Error>> {
    let mdns = Mdns::system().await?;
    let broker = find_broker(&mdns, BrokerConfig::default()).await?;
    let host = broker.connect_host().into_owned();

    let mut mqtt = rumqttc::MqttOptions::new(ebus_rumqttc::controller_client_id(), &host, broker.port);
    mqtt.set_credentials("user", "password");
    mqtt.set_max_packet_size(4 << 20, 4 << 20); // room for a large `$description`
    let run = ebus_rumqttc::controller(mqtt, Config::wildcard(), ControllerOptions::default())?;
    tokio::spawn(run.task.run());

    let b = TreeBuilder::new(TreeConfig::default(), "meter-1", DeviceSpec::default())?;
    let mqtt = rumqttc::MqttOptions::new("meter-1", &host, broker.port);
    let dev = ebus_rumqttc::device(mqtt, b, DeviceOptions::default())?;
    tokio::spawn(dev.task.run());

    Ok(run.events)
}
```

Pass the root id as the device's client id, as python-sdk does; `ebus_rumqttc::device` keeps the client id in your `MqttOptions` and sets the root's `lost` will on them.

## Shutdown

```rust,no_run
use ebus_rumqttc::ebus_homie_tokio::ControllerHandle;
use ebus_rumqttc::ebus_homie_tokio::device::{DeviceHandle, Shutdown};
use tokio::task::JoinHandle;

async fn stop(
    device: DeviceHandle,
    device_task: JoinHandle<Result<(), ebus_rumqttc::Error>>,
    controller: ControllerHandle,
) {
    device.shutdown(Shutdown::Announce).await; // `disconnected`, root last
    let _ = device_task.await; // the connection has ended
    controller.shutdown().await;
}
```

`Shutdown::Silent` drops the connection so the broker publishes the will; `Shutdown::Remove` clears every topic of the tree.

## simple-span-controller

python-sdk's [`examples/simple-span-controller`](https://github.com/electrification-bus/python-sdk/blob/main/examples/simple-span-controller) and [`crates/ebus-rumqttc/examples/simple-span-controller.rs`](../crates/ebus-rumqttc/examples/simple-span-controller.rs) (helpers in [`examples/common/span.rs`](../crates/ebus-rumqttc/examples/common/span.rs)) take the same arguments.

| Step | python-sdk | rust-sdk |
|---|---|---|
| Find the panel's broker by SRV target `span-<serial>.local` | `discover_panel_broker` | `span::discover` with `PanelSearch` |
| Fall back to the host name after 5 s | `BrokerEndpoint(.., f"span-{serial}.local", port)` | `broker()`, its `direct` endpoint |
| Password | `span_auth_utils.get_panel_credentials` | `SpanAuth::load(..).password(..)` |
| CA certificate | `span_auth_utils.ensure_ca_cert` | `span::ensure_ca_cert`, then `span::tls::client_config` |
| Connect | `Controller(mqtt_cfg=endpoint.mqtt_cfg(base_cfg))` | `ebus_rumqttc::controller(options(..), Config::wildcard(), ..)` |
| Find the MID by device type | `on_description_received` | `Monitor::event` on `Event::DescriptionChanged` |
| Report state changes | `on_device_state_changed` | `Monitor::event` on `Event::StateChanged` |
| Report watched values | `on_property_changed` | `Monitor::event` on `Event::PropertyChanged` and `PropertyCleared`, reading the value from the snapshot |
| "Still waiting" every 60 s | `time.sleep(60)` loop | `interval_at` arm of `span::monitor` |
| Ctrl-C | `controller.stop()` | `handle.shutdown().await` in `span::monitor` |
| Events dropped | (none) | `Monitor::resync` on `Delivered::Overflow` |
| Broker address changed | (none) | `span::panel_reresolve`, passed to `task.reresolve` |

```sh
python-sdk/examples/simple-span-controller <serial-number> [password]
cargo run -p ebus-rumqttc --example simple-span-controller --features rustls -- <serial-number> [password]
```
