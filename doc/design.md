# rust-sdk design

rust-sdk implements the eBus device and controller roles in Rust: Homie 5 over MQTT 3.1.1 (MQTT 5 later, behind a feature) on TCP or TLS, with mDNS discovery. The protocol logic lives in sans-IO core crates that run unchanged on tokio services, on ESP32, and inside a host application that already owns an MQTT connection.

## Crates

| Crate | Role | Depends on | `no_std` |
|---|---|---|---|
| `ebus-homie` | Homie 5 codec (ids, topics, states, payloads, datatypes, `$description`) and the device and controller state machines | `serde_json`, `sha2`, `num-*` (all under `alloc`) | yes, `alloc` optional |
| `ebus-mqtt` | MQTT vocabulary, filter ownership, the offline publish hold and the reconnect order | nothing | yes, `alloc` optional |
| `ebus-discovery` | eBus DNS-SD contract: service types, TXT records, broker selection; mDNS backends behind features | nothing | yes, `alloc` optional |
| `ebus-homie-tokio` | Drives the core state machines on tokio over a host-owned or bundled MQTT connection | `ebus-homie`, `ebus-mqtt`, tokio 1 | no |
| `ebus-rumqttc` (planned) | Bundled rumqttc 0.25 adapter | `ebus-homie-tokio`, rumqttc 0.25 | no |

## Core rules

- Core crates (`ebus-homie`, `ebus-mqtt`, `ebus-discovery`) perform no I/O and depend on no async runtime, clock or lock. Callers pass input in, take output out, and supply the time.
- Features: `std` (default, implies `alloc`), `alloc`, or neither. CI builds the core crates for `thumbv7em-none-eabihf` with no features and with `alloc` ([ci.yml](../.github/workflows/ci.yml)).
- Library code returns errors and must not panic on any input; `unwrap`, `expect` and the panicking macros appear only in tests.
- `unsafe_code = "forbid"` and `missing_docs = "warn"` workspace-wide ([Cargo.toml](../Cargo.toml)).
- Edition 2024, MSRV 1.85. CI tests 1.85, 1.91.1 and stable, and runs rustfmt, clippy `-D warnings` and rustdoc `-D warnings`; every cargo step except rustfmt is `--locked`.

## Spec compliance

The [Homie 5 convention](https://homieiot.github.io/) and the [eBus specification](https://github.com/electrification-bus/specification) (`framework.md`) define wire behavior; where code and spec disagree, the spec wins. Citations below are `framework.md:LINE` at the commit in [.ebus-spec.json](../.ebus-spec.json); `convention.md:LINE` at [homieiot/convention](https://github.com/homieiot/convention) `7edc221`; `homie.py:LINE` at [python-sdk](https://github.com/electrification-bus/python-sdk) `0168eb5`; and `client.py:LINE` at [ebus-mqtt-client](https://github.com/electrification-bus/ebus-mqtt-client) `db88fc1`.

Where the spec leaves a choice open, rust-sdk follows python-sdk: JSON key order and `json.dumps` formatting, float payload formatting, the `$description` `version` value, `Problem` reason strings, subscription pacing constants, and `reason=camelCase,key=value` log messages. Where python-sdk deviates from the spec, rust-sdk follows the spec and the deviation is listed in [python-sdk#110](https://github.com/electrification-bus/python-sdk/issues/110). That covers effective state (python propagates any non-ready root state), integers that step rounding takes past 64 bits, and some `datetime` and `json` edge cases ([vectors/README.md](../vectors/README.md)).

## ebus-homie codec

| Module | Contents |
|---|---|
| [`id`](../crates/ebus-homie/src/id.rs) | `is_valid_id`, `sanitize_id` (python's sanitizer) |
| [`topic`](../crates/ebus-homie/src/topic.rs) | Borrowing `parse` into `Topic`/`TopicKind` (`$state`, `$description`, `$log/{level}`, `$alert/{id}`, value, `/set`, `$target`, `$broadcast`); topic builders |
| [`state`](../crates/ebus-homie/src/state.rs) | `DeviceState` with exact-match `parse`; `effective_state` (state.rs:100-105): only a root in `lost` overrides its tree (convention.md:262-267) |
| [`payload`](../crates/ebus-homie/src/payload.rs) | `classify`: zero-length is `Deleted`, `0x00` is `""`, otherwise UTF-8; `encode_str` |
| [`pyfmt`](../crates/ebus-homie/src/pyfmt.rs) (alloc) | Python-identical float formatting, `json.dumps` output and `json.loads` input |
| [`datatype`](../crates/ebus-homie/src/datatype.rs) | The nine datatypes, payload grammar checks (no alloc), `Problem` codes; with alloc, `parse_set_payload` (datatype.rs:1026) with exact decimal step rounding, and the encoders |
| [`description`](../crates/ebus-homie/src/description.rs) (alloc) | `DeviceDescription` model; `parse_with_ignored` prunes illegal nodes and properties and rejects illegal device-level fields (convention.md:663-669); `to_json` in python's `json.dumps` key order and formatting, byte-identical when keys nested inside an extra are sorted (description.rs:349-353); `content_hash` (description.rs:432) and `compute_version` |

## Controller (planned)

### Shape

The controller is a sans-IO state machine in `ebus-homie::controller` (alloc), in the quinn-proto shape: input through `handle_*`, output through `poll_*`, time only from a caller-supplied `Instant`. One owner task drives it. Every timer is a deadline returned by `poll_timeout`.

| Crate | Additions |
|---|---|
| `ebus-mqtt` | `QoS`, `Instant`, `Message<'a>`, `Command`, `Link`, `LastWill`; `filter::{is_valid_filter, is_valid_topic, matches, overlap}` (`overlap` ported from ebus-mqtt-client `_filters_overlap`, client.py:65-85); `FilterLedger` |
| `ebus-homie` | `controller` module; `datatype::check_value`; optional `log` feature |
| `ebus-homie-tokio` | `attach()`, `Inbox`, `ControllerDriver`, `ControllerHandle`, `EventReceiver`. tokio features `sync`, `time`, `macros`; also `bytes` 1 and `log` 0.4 |
| `ebus-rumqttc` | `controller()`: driver plus rumqttc 0.25 `EventLoop` poller plus command executor. rumqttc with `default-features = false`; pass-through features `rustls` and `native-tls` |

`ebus-rumqttc` is its own crate so that, under Cargo feature unification, one member of a host workspace cannot compile rumqttc 0.25 into every service sharing `ebus-homie-tokio`, and so a rumqttc bump is a semver change in one crate.

### Public API

```rust
// ===== ebus-mqtt =====
pub enum QoS { AtMostOnce = 0, AtLeastOnce = 1, ExactlyOnce = 2 }
/// Milliseconds on the caller's monotonic clock.
pub struct Instant(pub u64); // saturating_since, add_ms (saturating)
pub struct Message<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],
    /// MQTT retain bit; None when the client does not surface it.
    pub retain: Option<bool>,
}
pub enum Link { Up { session_present: bool }, Down }
#[non_exhaustive] // room for v5 properties
pub enum Command {
    Subscribe { filters: Vec<(String, QoS)> }, // one SUBSCRIBE packet
    Unsubscribe { filters: Vec<String> },
    Publish { topic: String, payload: Vec<u8>, qos: QoS, retain: bool },
}
pub struct LastWill { pub topic: String, pub payload: Vec<u8>, pub qos: QoS, pub retain: bool }
pub mod filter {
    pub fn is_valid_filter(f: &str) -> bool;
    pub fn is_valid_topic(t: &str) -> bool;
    pub fn matches(filter: &str, topic: &str) -> bool; // MQTT 3.1.1 s4.7, incl. leading wildcard vs `$`
    pub fn overlap(a: &str, b: &str) -> bool;
}
/// Refcounts per filter string; host filters and `#` are never unsubscribed.
pub struct FilterLedger;
impl FilterLedger {
    pub fn new(host_filters: impl IntoIterator<Item = String>) -> Self;
    pub fn subscribe(&mut self, filters: Vec<(String, QoS)>) -> Option<Command>; // newly held only
    pub fn unsubscribe(&mut self, filters: Vec<String>) -> Option<Command>;     // count reached zero
    pub fn link_up(&mut self, session_present: bool); // clean session clears wire state, keeps counts
    pub fn held(&self) -> impl Iterator<Item = &str>;
}

// ===== ebus-homie::controller =====
pub struct Id(Arc<str>); // validated with id::is_valid_id
pub struct PropertyAddr { pub device: Id, pub node: Id, pub property: Id }
pub enum Domain { Named(String) /* "ebus" */, Any /* "+/5/+/$state" */ }
pub enum Discovery { Wildcard, Device(Id), Tree { root: Id } }
pub struct Pacing { pub batch_devices: usize /* 8 */, pub batch_values: usize /* 500 */ }

#[non_exhaustive]
pub struct Config {
    pub domain: Domain,
    pub discovery: Discovery,
    pub subscribe_qos: QoS,            // ExactlyOnce
    pub pacing: Option<Pacing>,        // Some(default)
    pub stuck_timeout_ms: Option<u64>, // Some(10_000)
    pub max_resubscribe_attempts: u8,  // 3
    pub absent_after_ms: u64,          // 30_000 after a link-up
    pub sync_timeout_ms: u64,          // 10_000
    pub trust_session: bool,           // false: session_present=true is re-paced
    pub max_early_values: usize,       // 1024 per device, before its $description
    pub max_devices: Option<usize>,    // None on Linux
    pub host_filters: Vec<String>,     // owned by the host; never unsubscribed
}
impl Config { pub fn wildcard() -> Self; pub fn tree(root: &str) -> Result<Self, ConfigError>; pub fn device(id: &str) -> Result<Self, ConfigError>; }
pub enum ConfigError { InvalidDomain, InvalidDeviceId, ZeroBatch, InvalidHostFilter }

pub enum Origin { Retained, Live, Unknown } // from Message::retain
pub struct Value { pub raw: Arc<str> /* 0x00 decoded */, pub check: Result<(), Problem> }
pub enum Missing { State, Description, Properties }

#[non_exhaustive]
pub enum Event {
    Link(Link), // after the resubscribe commands are queued
    DeviceDiscovered { device: Id, state: DeviceState },
    StateChanged { device: Id, old: Option<DeviceState>, new: DeviceState, effective: DeviceState },
    EffectiveStateChanged { device: Id, old: DeviceState, new: DeviceState },
    DescriptionChanged { device: Id, description: Arc<DeviceDescription>, ignored: Vec<Ignored>, while_ready: bool },
    DescriptionRejected { device: Id, error: DescriptionError },
    PropertyChanged { addr: PropertyAddr, value: Value, origin: Origin },
    PropertyEvent { addr: PropertyAddr, value: Value }, // retained:false properties, never cached
    PropertyCleared { addr: PropertyAddr },
    TargetChanged { addr: PropertyAddr, target: Option<Value> },
    Alert { device: Id, alert: Id, message: Option<String> }, // None = cleared
    DeviceSynced { device: Id, missing: u32 },
    DeviceStuck { device: Id, missing: Missing, attempt: u8, gave_up: bool },
    DeviceAbsent { device: Id },  // not re-heard after link-up
    DeviceRemoved { device: Id }, // zero-length $state only
    TreeComplete { root: Id },    // edge-triggered, re-arming
}

pub struct SetOptions { pub allow_not_ready: bool, pub unchecked: bool }
#[non_exhaustive]
pub enum SetError { NotConnected, UnknownDevice, UnknownProperty, NotSettable, DeviceNotReady(DeviceState), Invalid(Problem), InvalidTopic }

pub struct DeviceView { pub id: Id, pub domain: Arc<str>, pub state: Option<DeviceState>, pub effective: Option<DeviceState>,
    pub description: Option<Arc<DeviceDescription>>, pub confirmed: bool, pub synced: bool /* values, alerts */ }
pub struct PropertyView { pub value: Option<Value>, pub last_valid: Option<Arc<str>>, pub target: Option<Value> }

impl Controller {
    pub fn new(config: Config) -> Result<Self, ConfigError>;
    pub fn handle_link(&mut self, now: Instant, link: Link);
    pub fn handle_message(&mut self, now: Instant, msg: Message<'_>) -> Handled; // Consumed | Ignored
    pub fn handle_dropped(&mut self, now: Instant, topic: &str);     // host lost messages for this device
    pub fn handle_command_failed(&mut self, now: Instant, cmd: Command); // host could not hand it to its client
    pub fn handle_timeout(&mut self, now: Instant);
    pub fn poll_command(&mut self) -> Option<Command>;
    pub fn poll_event(&mut self) -> Option<Event>;
    pub fn poll_timeout(&self) -> Option<Instant>;
    pub fn set(&mut self, addr: &PropertyAddr, payload: &str, opts: SetOptions) -> Result<(), SetError>;
    pub fn broadcast(&mut self, subtopic: &str, payload: &str) -> Result<(), SetError>;
    pub fn shutdown(&mut self); // unsubscribes every filter it holds
    pub fn wants(&self, topic: &str) -> bool; // "{domain}/5/" prefix, or "*/5/" for Any
    pub fn is_connected(&self) -> bool;
    pub fn device(&self, id: &str) -> Option<&DeviceView>;
    pub fn devices(&self) -> impl Iterator<Item = &DeviceView> + '_;
    pub fn effective_state(&self, id: &str) -> Option<DeviceState>;
    pub fn is_tree_complete(&self, root: &str) -> bool;
}

// ===== ebus-homie::datatype =====
pub fn check_value(text: &str, prop: &PropertyDescription) -> Result<(), Problem>;

// ===== ebus-homie-tokio =====
#[derive(Clone)]
pub struct Inbox; // sync, never blocks, safe from any std::thread
impl Inbox {
    pub fn message(&self, topic: &str, payload: bytes::Bytes, retain: Option<bool>) -> bool; // false = dropped over the byte cap
    pub fn connected(&self, session_present: bool); // never dropped
    pub fn disconnected(&self);                     // never dropped
    pub fn command_failed(&self, cmd: Command);
}
pub struct AttachOptions { pub inbox_bytes: usize /* 4 MiB */, pub event_capacity: usize /* 1024 */ }
pub struct Attached {
    pub handle: ControllerHandle,
    pub inbox: Inbox,
    pub commands: tokio::sync::mpsc::UnboundedReceiver<Command>,
    pub events: EventReceiver,
    pub driver: ControllerDriver,
}
pub fn attach(config: Config, opts: AttachOptions) -> Result<Attached, ConfigError>;
impl ControllerDriver { pub async fn run(self) -> Result<(), DriverError>; } // Send; the host spawns it
pub enum Delivered { Event(Event), Overflow { dropped: u64 } } // Overflow => re-read snapshots
impl EventReceiver { pub async fn recv(&mut self) -> Option<Delivered>; }

#[derive(Clone)] // Send + Sync
pub struct ControllerHandle;
impl ControllerHandle {
    pub fn device(&self, id: &str) -> Option<Arc<DeviceSnapshot>>; // sync snapshot reads
    pub fn devices(&self) -> Vec<Arc<DeviceSnapshot>>;
    pub fn effective_state(&self, id: &str) -> Option<DeviceState>;
    pub fn is_tree_complete(&self, root: &str) -> bool;
    pub fn is_connected(&self) -> bool;
    pub async fn changed(&mut self);
    // Ok = validated and handed to the command queue (QoS 0, no ack)
    pub async fn set(&self, addr: PropertyAddr, payload: impl Into<String>) -> Result<(), SetError>;
    pub async fn set_with(&self, addr: PropertyAddr, payload: impl Into<String>, opts: SetOptions) -> Result<(), SetError>;
    pub fn try_set(&self, addr: PropertyAddr, payload: impl Into<String>) -> Result<(), SetError>; // safe on a reader thread
    pub async fn broadcast(&self, subtopic: &str, payload: &str) -> Result<(), SetError>;
    pub async fn shutdown(&self);
}

// ===== ebus-rumqttc =====
pub struct Running { pub handle: ControllerHandle, pub events: EventReceiver, pub task: ControllerTask }
impl ControllerTask { pub async fn run(self) -> Result<(), Error>; }
pub fn controller(mqtt: rumqttc::MqttOptions, config: Config, opts: AttachOptions) -> Result<Running, ConfigError>;
```

Snapshots are copy-on-write: the driver republishes `Arc<Snapshot>` through a `watch` channel once per inbox drain, with new `Arc<DeviceSnapshot>`s only for devices that changed.

### Discovery

| Mode | Subscriptions |
|---|---|
| `Wildcard` (default) | `{domain}/5/+/$state` with domain `ebus` (framework.md:85, 359-363); `Domain::Any` uses `+/5/+/$state` (convention.md:161-162). Devices are keyed by (domain, id); `/set` goes to the device's own domain. |
| `Device(id)` | That device's `$state`, `$description`, `+/+`, `+/+/$target`, plus `{root}/$state` once its description names a `root` (convention.md:258-260). |
| `Tree{root}` | The root, unpaced. Children are reconciled on the root's init-to-ready edge and when a `$description` arrives while it is `ready` (python homie.py:4522-4527, 4678-4687); removal is leaves-first with a visited set; a declared child that was cleared and returns is re-adopted. |

A device exists once a valid `$state` arrives (convention.md:272); an unparseable `$state` is logged and creates nothing. `$description` is always subscribed per device.

### Subscriptions and pacing

Pacing follows python-sdk#97 with python's constants (homie.py:3566-3576).

1. Stage one, unpaced: `{d}/$state` (non-wildcard modes) and `{d}/$description`. A `$description` that arrives before `$state` is stored.
2. Stage two, once both are held: `{d}/+/+` and `{d}/+/+/$target`, with at most `batch_devices` (8) devices in flight owing at most `batch_values` (500) retained messages, 2 per retained property. A device over the budget is subscribed node by node. One step's filters go out in one `Command::Subscribe`.
3. A burst ends when every owed key has arrived, or when the retained copy of the `$description` resubscribed right after the property filters arrives (MQTT 3.1.1 section 3.8.4). The marker counts when `retain != Some(false)`, as python does (homie.py:4650); with `retain: None` a live republish can end a burst early, and stuck healing bounds the overshoot.

Subscriptions use QoS 2 (convention.md:45). `+/+` also carries `$alert/{id}` (to `Alert` events) and `$log/{level}` (to `log` at debug).

### Stuck healing

Ported from homie.py:4317-4422. Each awaiting or draining device has a deadline of `subscribed_at + stuck_timeout` (10 s).

| Situation | Action |
|---|---|
| Stage one missing `$state` or `$description` | Unsubscribe and resubscribe those filters through the ledger |
| Stage two incomplete | Free the slot and requeue the device with unsubscribe-then-subscribe, so the broker replays its retained messages |
| 3 attempts, or a retry that brought no more messages than the last | `DeviceStuck{gave_up: true}`; subscriptions stay in place |
| `handle_dropped` (inbox overflow) | The same resubscribe, once the backlog drains |
| `handle_command_failed` | Subscribe or Unsubscribe retried with 1-30 s backoff; a `/set` Publish is dropped with `reason=setDropped`, since QoS 0 is now-or-never (convention.md:695) |

### Messages: order, retention, repetition

- A repeated identical `$state` emits nothing.
- `$description` is deduplicated by raw bytes, then by `content_hash`. It is parsed with `parse_with_ignored`; a device-level error emits `DescriptionRejected` and the device stays known by its `$state` with no model.
- A description change while the device is not `ready` is applied silently; on the next ready edge one `DescriptionChanged` fires if the content differs from the last one announced. A change while `ready` is applied and announced at once with `while_ready: true` and `reason=descriptionChangedWhileReady` (convention.md:207). New nodes are paced in; values of vanished properties are dropped.
- Values before the `$description` are buffered raw, last value per (node, property), up to `max_early_values`, and validated when the description arrives. Values for undeclared or pruned properties are dropped at debug level.
- `PropertyChanged` fires for `Origin::Retained` and `Origin::Unknown` only when the wire bytes change; a live republish (`retain == Some(false)`) always emits.
- Properties declared `retained: false` always produce `PropertyEvent` and are never cached (convention.md:370-371); a retained message for one is accepted with a diagnostic.
- A zero-length payload (`payload::classify` gives `Deleted`) clears a property (`PropertyCleared`), a `$target` or an `$alert`; on `$state` it removes the device.
- Every value and `$target` is checked with `check_value` and delivered unmodified as `Value{raw, check}` (convention.md:78); `last_valid` keeps the last good text. `check_value` checks grammar, enum and color membership, min/max, the 64-bit integer range, finite floats, and `0x00` only for `string`. Step rounding is the device's job (convention.md:399).
- Values can arrive after `ready`. `DeviceSynced{missing}` fires once the description is held, no burst is in flight, and either every declared retained property has a value (`missing: 0`, per convention.md:279) or `sync_timeout` expired (`missing > 0`). It re-arms on a description change and on link-up.
- Messages for untracked devices are ignored and trigger no unsubscribe, since they may arrive through a host filter. A subscribe that completes after its device was dropped is undone through the ledger.

### Effective state

`state::effective_state` decides: only a root in `lost` overrides its tree (convention.md:262-267, 284; framework.md:419). When a root's state changes, `EffectiveStateChanged` fires for every descendant whose effective state flips.

### `/set` and `$broadcast`

1. Rejections, in order: `NotConnected` (QoS 0 is never held; convention.md:695); unknown device or property; `NotSettable`; effective state other than `ready` unless `allow_not_ready` (convention.md:279).
2. Unless `unchecked`, the payload must pass `parse_set_payload(text, datatype, format, current)` with `current` from `last_valid`: the same check the device runs, rounding included. The result is discarded.
3. The caller's bytes are sent unchanged, so the `$target` echo is byte-exact (convention.md:470); `""` becomes `0x00` for `string` (convention.md:60-71).
4. QoS 0, non-retained (framework.md:87; convention.md:48-49, 483). There is no retry when the device reports a different value (convention.md:474-475).

`broadcast` publishes `{domain}/5/$broadcast/{subtopic}`, non-retained, QoS 0, after checking every segment is a valid id (convention.md:531-535).

### Reconnect

| Input | Behavior |
|---|---|
| `Link::Down` | Emit `Link(Down)`; discard in-flight pacing work; clear the ledger's wire state. Filters stay held. |
| `Link::Up{session_present: false}`, or `true` without `trust_session` | Queue the discovery filter, then stage one for every known device (the root in tree mode), paced, requiring a fresh `$description` before properties are paced again (homie.py:3952-4015); set `confirmed = false` everywhere; then emit `Link(Up)`. This is ebus-mqtt-client's reconnect order (client.py:936-961). |
| `session_present: true` with `trust_session` | Keep everything; stuck healing covers gaps. |
| A message after `Down` with no `Up` | Treated as `Up{session_present: false}`, logged as `reason=implicitLinkUp`. The host contract still requires forwarding ConnAck. |

A broker restarted without persistence can come back with an empty retained store while devices republish later, so absence is a distinct state: a device not re-heard within `absent_after` gets `DeviceAbsent`, stays in the registry, and heals on its next publish.

### Removal

A zero-length `$state` is the only removal trigger (convention.md:163, 287-289). The controller emits `DeviceRemoved` and unsubscribes the device's property filters it owns. Tree mode keeps the `$state` and `$description` filters so a returning device is heard; wildcard mode drops `$description`. In tree mode a child that leaves its parent's `children` on the parent's ready edge is removed recursively (convention.md:636-641). Later zero-length value topics for a removed device are cleared silently.

### Events

The event channel is bounded and the core never blocks on it. When it is full, events are counted and dropped, and the next delivery is `Overflow{dropped}`, after which the consumer re-reads the snapshots. Events are hints; snapshots are the truth.

## Bring-your-own connection

A host service keeps its own MQTT client, options, TLS, last will and reconnect loop. It depends on `ebus-homie-tokio` with `default-features = false` and feeds the `Inbox`. Example for a service on rumqttc 0.10, whose sync `Client` methods call `pollster::block_on` (rumqttc 0.10.0 `client.rs:224, 245, 276`), so the executor uses the non-blocking `try_*` calls:

```rust
let mut cfg = Config::tree(&root_id)?;
cfg.host_filters = vec!["ebus/5/+/$description".into()];
let ebus = ebus_homie_tokio::attach(cfg, AttachOptions::default())?;
tokio::spawn(ebus.driver.run());

// Existing std::thread reader over connection.iter(): add the eBus arms.
let inbox = ebus.inbox.clone();
for ev in connection.iter() {
    match ev {
        Ok(Event::Incoming(Packet::ConnAck(a))) => inbox.connected(a.session_present),
        Ok(Event::Incoming(Packet::Publish(p))) => {
            if p.topic.starts_with("ebus/5/") {
                inbox.message(&p.topic, p.payload.clone(), Some(p.retain));
            }
            /* existing forwarding */
        }
        Err(_) => { inbox.disconnected(); /* existing backoff */ }
        _ => {}
    }
}

// One executor task; try_* never blocks a tokio worker, so holding the std Mutex is safe.
let mut commands = ebus.commands;
let inbox = ebus.inbox.clone();
tokio::spawn(async move {
    while let Some(cmd) = commands.recv().await {
        let res = {
            let mut c = mqtt.lock().unwrap();
            match &cmd {
                Command::Subscribe { filters } => c.client.try_subscribe_many(
                    filters.iter().map(|(f, q)| SubscribeFilter::new(f.clone(), qos(*q)))),
                Command::Unsubscribe { filters } => filters.iter().try_for_each(|f| c.client.try_unsubscribe(f.clone())),
                Command::Publish { topic, payload, qos: q, retain } => c.client.try_publish(topic.clone(), qos(*q), *retain, payload.clone()),
                _ => Ok(()),
            }
        };
        if res.is_err() { inbox.command_failed(cmd); }
    }
});

ebus.handle.set(addr, "true").await?;
```

Host contract, in the rustdoc and an unpublished `examples/byo-rumqttc-010` crate:

1. Forward ConnAck, Publish and connection errors.
2. Never await a controller call on the reader thread; `try_set` is the form for it.
3. Declare in `host_filters` any filter string the host also subscribes.
4. Leave resubscribing the controller's filters to the controller.
5. Report executor failures through `command_failed`.

A host whose client wrapper drops the retain bit passes `retain: None`; the burst marker then counts any `$description` and stuck healing bounds the error.

## Bundled path: tokio and rumqttc 0.25

```rust
let mut mqtt = rumqttc::MqttOptions::new(format!("ebus-controller-{suffix}"), "localhost", 1883);
mqtt.set_clean_session(true);
let run = ebus_rumqttc::controller(mqtt, Config::wildcard(), AttachOptions::default())?;
tokio::spawn(run.task.run());

let mut events = run.events;
while let Some(d) = events.recv().await {
    match d {
        Delivered::Event(Event::DeviceSynced { device, missing: 0 }) => { /* values present */ }
        Delivered::Overflow { .. } => { let all = run.handle.devices(); /* resync */ }
        _ => {}
    }
}
```

The transport is MQTT over TCP, or TLS through rumqttc's rustls support (MQTTS); WebSocket transports stay off. A service on the broker's own host can use a local plaintext listener, as above.

`ControllerTask::run` joins three futures: the driver; an `EventLoop::poll` loop feeding the `Inbox`, with 1-30 s backoff (client.py:155); and a command executor awaiting `AsyncClient`, separate so a full request channel cannot deadlock against `poll`. The client id is `ebus-controller-` plus a random suffix, after python's `homie-controller-{uuid}` (homie.py:3870).

## Device role (planned)

- `ebus_homie::device::Device` has the controller's shape (`handle_*`, `poll_*`, `poll_timeout`), plus `last_will()`, the root's retained `$state=lost` (convention.md:56-58), and a transition guard that publishes `init`, one `$description`, the values, then `ready` (homie.py:2897-2928).
- `ebus_mqtt::Session` owns the shared `FilterLedger` (refcounts across roles), `PublishHold` and the reconnect order. On `Up`: flush the hold (newest retained publish per topic, QoS 1/2 non-retained in order, QoS 0 dropped, capped at 512; client.py:31, 526-557), then the device's tree refresh, then the controller's resubscribe, then the app notification. Until the device role exists, the controller owns a private ledger.
- A `Role` trait lets one driver run a device and a controller over one link, the co-resident broker-host pattern (framework.md:20, 461).
- Bring-your-own hazards: one connection carries one will (convention.md:56-58); rumqttc 0.10 exposes `EventLoop::pending` (eventloop.rs:56) and replays it after reconnect (eventloop.rs:115-116), the replay hazards of ebus-mqtt-client GH #20 and #21 (client.py:545-556).
- CPU levers for a device that replaces a Python one: borrowed `topic::parse` so the inbound path allocates nothing; publish-on-change compared on final wire bytes; one owner task with no locks; description work only when `content_hash` changes; deadlines instead of polling threads.

## Discovery (planned)

`ebus-discovery` holds the eBus DNS-SD contract (service types, TXT records, broker selection), sans-IO, with mDNS backends behind features; Avahi is the first backend.

## ESP32 (planned)

- `ebus-homie-espidf` on esp-idf-svc (std): the esp-mqtt callback only copies into a `std::sync::mpsc`; one thread loops `recv_timeout(poll_timeout)`, `handle_*`, then executes commands through non-blocking `EspMqttClient::enqueue`.
- `EventPayload::Received` carries no retain bit, so the adapter passes `retain: None`; `Connected(session_present)` is available.
- Chunked payloads are reassembled up to a configured maximum description size; overflow goes to `handle_dropped`. `buffer_size` is sized to the largest description.
- Memory: `max_devices` (cpp-sdk `MAX_DISCOVERED_DEVICES` is 16), a byte-capped inbox (cpp-sdk `CONTROLLER_INBOX_BYTES` is 16 KiB), and `Pacing{batch_devices: 2, batch_values: 64}` as a starting point.

## Testing

| Layer | What | Where |
|---|---|---|
| Unit | Every module; every controller rule with a virtual `Instant`, `FilterLedger` invariants, `filter::matches`/`overlap` against MQTT 3.1.1 section 4.7, `implicitLinkUp`, event `Overflow` | in-crate `#[cfg(test)]` |
| Spec vectors | Topics, empty string, effective state, payloads per datatype, `$description`; derived from the spec, with python-sdk results as expect values except the python-sdk#110 overrides. Planned: `check_value`, the `/set` verdict and description dedup/ready-edge suites | [vectors/](../vectors/), [tests/vectors.rs](../crates/ebus-homie/tests/vectors.rs), generated by [tools/gen_vectors.py](../tools/gen_vectors.py) |
| python-sdk vectors | python-sdk's choices where the spec is open: float format, `json.dumps`, `sanitize_id`, duration encoding, `/set` parsing, `$description` wire bytes and round trip | [vectors/python-sdk/](../vectors/python-sdk/) |
| Panel-sim captures | Masked SPAN panel trees from ebus-panel-sim: every `$description` parses and every non-string text value passes `parse_set_payload` for its datatype and format. Planned: sanitized `(t_ms, topic, payload, retain)` replays: broker restart with an empty retained store, `ready` before values, repeated `$description` (identical and changed), values before the description, a 50-device by 30-property tree, each also with `retain: None` | [fixtures/panel-sim/](../fixtures/panel-sim/), [tests/captures.rs](../crates/ebus-homie/tests/captures.rs) |
| Local capture corpus | `EBUS_CAPTURE_CORPUS=<dir>` also runs the capture test over every `*.tree.json` below it, with value faults reported, and checks that every fully accepted `$description` line in each `ebus-retained-topics.txt` dump round-trips through `to_json` byte for byte | [tests/captures.rs](../crates/ebus-homie/tests/captures.rs) |
| mosquitto e2e (planned) | Broker on a free port, skipped when the binary is absent: pacing under `max_queued_messages 1000` with forced drops and healing; restart without persistence (`DeviceAbsent`, then healing); removal and return; `/set` round trip against a python-sdk device with byte-exact `$target`; a host holding overlapping and identical filters; bring-your-own through a rumqttc 0.10 dev-dependency alongside the bundled 0.25 path | `ebus-homie-tokio/tests`, `ebus-rumqttc/tests` |
| Live panels (planned) | Read-only example binary in wildcard and tree modes, compared with an independent Homie client; time to `DeviceSynced` and CPU per message. `/set` only with the maintainer's go-ahead | manual |

## Embedding in an existing service

- A host service that already owns an MQTT connection uses bring-your-own: the controller runs on the host's runtime (the host spawns the driver) and forces no MQTT client or version on it.
- A host workspace that builds with `--locked` takes the `Cargo.lock` diff as the acceptance gate: `cargo tree -d` should show no new MQTT client, rustls or tokio version.

## Open questions

1. `ebus-rumqttc` as a fifth published crate, or a default-off feature of `ebus-homie-tokio` with less publishing overhead?
2. Trust `session_present: true` by default? Re-pacing is always correct but costs a replay.
3. `DeviceAbsent` devices stay in the registry indefinitely. Is an optional eviction after N minutes wanted?
4. Bring-your-own device role: supported when the host installs `last_will()` and drains the client's pending replay, or does a device always need its own connection?
