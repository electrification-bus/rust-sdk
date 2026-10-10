# rust-sdk design

rust-sdk implements the eBus device and controller roles in Rust: Homie 5 over MQTT 3.1.1 (MQTT 5 later, behind a feature) on TCP or TLS, with mDNS discovery. The protocol logic lives in sans-IO core crates that run unchanged on tokio services, on ESP32, and inside a host application that already owns an MQTT connection.

## Crates

| Crate | Role | Depends on | `no_std` |
|---|---|---|---|
| `ebus-homie` | Homie 5 codec (ids, topics, states, payloads, datatypes, `$description`) and the controller state machine; the device state machine (planned) | `ebus-mqtt`; `serde_json`, `sha2`, `rpds`, `num-*` (under `alloc`); `log` (under `log`) | yes, `alloc` optional |
| `ebus-mqtt` | MQTT vocabulary, topic filters and filter ownership | nothing | yes, `alloc` optional |
| `ebus-discovery` | eBus DNS-SD contract: service types, TXT records, broker selection; mDNS backends behind features (planned) | nothing | yes, `alloc` optional |
| `ebus-homie-tokio` | Drives the core state machines on tokio over a host-owned or bundled MQTT connection | `ebus-homie`, `ebus-mqtt`, tokio 1, `rpds` | no |
| `ebus-rumqttc` | Bundled rumqttc 0.25 adapter | `ebus-homie-tokio`, rumqttc 0.25 | no |

## Core rules

- Input that changes nothing produces nothing: no event, no command, no snapshot republish, and no copy of held state. Two exceptions are deliberate. A SUBSCRIBE for a filter already held is sent again to make the broker replay its retained messages: the `$description` end-of-burst marker, re-pacing after a clean or untrusted link-up, stuck healing, dropped-message healing, the replay after a rejected description, a removed device that returns while its filters are still held, and the retry of a failed SUBSCRIBE. A `retained: false` property emits `PropertyEvent` for every message, since each one is an event.
- Core crates (`ebus-homie`, `ebus-mqtt`, `ebus-discovery`) perform no I/O and depend on no async runtime, clock or lock. Callers pass input in, take output out, and supply the time.
- Features: `std` (default, implies `alloc`), `alloc`, or neither. CI builds the core crates for `thumbv7em-none-eabihf` with no features and with `alloc` ([ci.yml](../.github/workflows/ci.yml)).
- Library code returns errors and must not panic on any input; `unwrap`, `expect` and the panicking macros appear only in tests.
- `unsafe_code = "forbid"` and `missing_docs = "warn"` workspace-wide ([Cargo.toml](../Cargo.toml)).
- Property and alert maps are `rpds` persistent maps (`RedBlackTreeMapSync`, `Send + Sync`; `rpds` 1.2 with `default-features = false`, `rust-version` 1.85): a clone shares every entry, and a change copies only the path to it.
- Edition 2024, MSRV 1.85. CI tests 1.85, 1.91.1 and stable, and runs rustfmt, clippy `-D warnings` and rustdoc `-D warnings`; every cargo step except rustfmt is `--locked`.

## Spec compliance

The [Homie 5 convention](https://homieiot.github.io/) and the [eBus specification](https://github.com/electrification-bus/specification) (`framework.md`) define wire behavior; where code and spec disagree, the spec wins. Citations below are `framework.md:LINE` at the commit in [.ebus-spec.json](../.ebus-spec.json); `convention.md:LINE` at [homieiot/convention](https://github.com/homieiot/convention) `7edc221`; `homie.py:LINE` at [python-sdk](https://github.com/electrification-bus/python-sdk) `0168eb5`; and `client.py:LINE` at [ebus-mqtt-client](https://github.com/electrification-bus/ebus-mqtt-client) `db88fc1`.

Where the spec leaves a choice open, rust-sdk follows python-sdk: JSON key order and `json.dumps` formatting, float payload formatting, the `$description` `version` value, `Problem` reason strings, subscription pacing constants, and the `reason=camelCase,key=value` log format with python's reason strings. Where python-sdk deviates from the spec, rust-sdk follows the spec and the deviation is listed in [python-sdk#110](https://github.com/electrification-bus/python-sdk/issues/110). That covers effective state (python propagates any non-ready root state), integers that step rounding takes past 64 bits, and some `datetime` and `json` edge cases ([vectors/README.md](../vectors/README.md)).

## ebus-homie codec

| Module | Contents |
|---|---|
| [`id`](../crates/ebus-homie/src/id.rs) | `is_valid_id`, `sanitize_id` (python's sanitizer) |
| [`topic`](../crates/ebus-homie/src/topic.rs) | Borrowing `parse` into `Topic`/`TopicKind` (`$state`, `$description`, `$log/{level}`, `$alert/{id}`, value, `/set`, `$target`, `$broadcast`); topic builders |
| [`state`](../crates/ebus-homie/src/state.rs) | `DeviceState` with exact-match `parse`; `effective_state` (state.rs:100-105): only a root in `lost` overrides its tree (convention.md:262-267) |
| [`payload`](../crates/ebus-homie/src/payload.rs) | `classify`: zero-length is `Deleted`, `0x00` is `""`, otherwise UTF-8; `encode_str` |
| [`pyfmt`](../crates/ebus-homie/src/pyfmt.rs) (alloc) | Python-identical float formatting for `f64` (`format_float`); `format_float32` for `f32`, with the shortest digits that read back as the same `f32` in that layout; `json.dumps` output and `json.loads` input |
| [`datatype`](../crates/ebus-homie/src/datatype.rs) | The nine datatypes, payload grammar checks (no alloc), `Problem` codes; with alloc, `parse_set_payload` (datatype.rs:1099) with exact decimal step rounding, and the encoders |
| [`description`](../crates/ebus-homie/src/description.rs) (alloc) | `DeviceDescription` model; `parse_with_ignored` prunes illegal nodes and properties and rejects illegal device-level fields (convention.md:663-669); `to_json` in python's `json.dumps` key order and formatting, byte-identical when keys nested inside an extra are sorted (description.rs:349-353); `content_hash` (description.rs:432) and `compute_version` |

## Controller

### Shape

The controller is a sans-IO state machine in `ebus-homie::controller` (alloc), in the quinn-proto shape: input through `handle_*`, output through `poll_*`, time only from a caller-supplied `Instant`. One owner task drives it. Every timer is a deadline returned by `poll_timeout`.

| Crate | Additions |
|---|---|
| `ebus-mqtt` | `QoS`, `Instant`, `Message<'a>`, `Command`, `Link`, `LastWill`; `filter::{is_valid_filter, is_valid_topic, matches, overlap}` (`overlap` ported from ebus-mqtt-client `_filters_overlap`, client.py:65-85); `FilterLedger` |
| `ebus-homie` | `controller` module; `datatype::check_value`; optional `log` feature |
| `ebus-homie-tokio` | `attach()`, `Inbox`, `ControllerDriver`, `ControllerHandle`, `EventReceiver`, `DeviceSnapshot`. tokio features `sync`, `time`, `macros`; also `bytes` 1, `log` 0.4 and `rpds` |
| `ebus-rumqttc` | `controller()`: driver plus rumqttc 0.25 `EventLoop` poller plus command executor; `client_id()`. rumqttc with `default-features = false`; pass-through features `rustls` and `native-tls` |

`ebus-rumqttc` is a separate published crate, not a feature of `ebus-homie-tokio`: under Cargo feature unification, a feature enabled by one member of a host workspace would compile rumqttc 0.25 into every service sharing `ebus-homie-tokio`, including hosts that bring their own client. A rumqttc bump is then a semver change in one crate.

### Public API

```rust
// ===== ebus-mqtt =====
pub enum QoS { AtMostOnce = 0, AtLeastOnce = 1, ExactlyOnce = 2 } // from_u8
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
    pub fn resubscribe<S: AsRef<str>>(&mut self, filters: impl IntoIterator<Item = S>) -> Option<Command>; // held ones, counts unchanged
    pub fn release_all(&mut self) -> Option<Command>;
    pub fn link_up(&mut self, session_present: bool); // clean session clears wire state, keeps counts
    pub fn held(&self) -> impl Iterator<Item = &str>;
    pub fn count(&self, filter: &str) -> u32;
    pub fn is_host_filter(&self, filter: &str) -> bool;
}

// ===== ebus-homie::controller =====
pub struct Id(Arc<str>); // Id::new(&str) -> Option<Id>, validated with id::is_valid_id
pub struct PropertyAddr { pub device: Id, pub node: Id, pub property: Id } // PropertyAddr::new(&str, &str, &str) -> Option<Self>
pub enum Domain { Named(String) /* "ebus" */, Any /* "+/5/+/$state" */ }
pub enum Discovery { Wildcard, Device(Id), Tree { root: Id } }
#[non_exhaustive]
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
    pub link_up_discards_queued: bool, // false; set by ebus-rumqttc: no SUBSCRIBE queued before a clean link-up is sent after it
    pub max_early_values: usize,       // 1024 per device, before its $description
    pub max_alerts: usize,             // 256 per device
    pub max_devices: Option<usize>,    // None: unbounded
    pub host_filters: Vec<String>,     // owned by the host; never unsubscribed
}
impl Config { pub fn wildcard() -> Self /* also Default */; pub fn tree(root: &str) -> Result<Self, ConfigError>; pub fn device(id: &str) -> Result<Self, ConfigError>; }
#[non_exhaustive]
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
    PropertyEvent { addr: PropertyAddr, value: Value, origin: Origin }, // retained:false properties, never cached
    PropertyCleared { addr: PropertyAddr }, // zero-length value, or not replayed in a re-sync
    TargetChanged { addr: PropertyAddr, target: Option<Value> },
    Alert { device: Id, alert: Id, message: Option<String> }, // None = cleared
    DeviceSynced { device: Id, missing: u32 },
    DeviceStuck { device: Id, missing: Missing, attempt: u8, gave_up: bool },
    DeviceAbsent { device: Id },  // not re-heard after link-up
    DeviceRemoved { device: Id }, // zero-length $state, or a tree child no tracked parent declares
    TreeComplete { root: Id },    // edge-triggered, re-arming
}

pub struct SetOptions { pub allow_not_ready: bool, pub unchecked: bool }
#[non_exhaustive]
pub enum SetError { NotConnected, UnknownDevice, UnknownProperty, NotSettable, DeviceNotReady(DeviceState), Invalid(Problem), InvalidTopic }

pub struct DeviceView { pub id: Id, pub domain: Arc<str>, pub state: Option<DeviceState>, pub effective: Option<DeviceState>,
    pub description: Option<Arc<DeviceDescription>>, pub confirmed: bool, pub synced: bool /* values, alerts */ }
impl DeviceView {
    pub fn property(&self, node: &str, property: &str) -> Option<&PropertyView>;
    pub fn properties(&self) -> impl Iterator<Item = (&Id, &Id, &PropertyView)> + '_;
    pub fn alert(&self, id: &str) -> Option<&str>;
    pub fn alerts(&self) -> impl Iterator<Item = (&Id, &str)> + '_;
    pub fn shares_values(&self, other: &DeviceView) -> bool; // same property and alert maps: a clone not changed since
}
pub struct PropertyView { pub value: Option<Value>, pub last_valid: Option<Arc<str>>, pub target: Option<Value> }
pub enum Handled { Consumed, Ignored }

impl Controller {
    pub fn new(config: Config) -> Result<Self, ConfigError>;
    pub fn config(&self) -> &Config;
    pub fn handle_link(&mut self, now: Instant, link: Link);
    pub fn handle_message(&mut self, now: Instant, msg: Message<'_>) -> Handled;
    pub fn handle_dropped(&mut self, now: Instant, topic: &str);     // host lost messages for this device
    pub fn handle_lost(&mut self, topic: &str);                      // the same, at its place in the message order
    pub fn handle_command_failed(&mut self, now: Instant, cmd: Command); // host could not hand it to its client
    pub fn handle_timeout(&mut self, now: Instant);
    pub fn poll_command(&mut self) -> Option<Command>;
    pub fn poll_event(&mut self) -> Option<Event>;
    pub fn poll_timeout(&self) -> Option<Instant>; // O(1); never later than the earliest deadline
    pub fn set(&mut self, addr: &PropertyAddr, payload: &str, opts: SetOptions) -> Result<(), SetError>;
    pub fn broadcast(&mut self, subtopic: &str, payload: &str) -> Result<(), SetError>;
    pub fn shutdown(&mut self); // unsubscribes every filter it holds, except host filters
    pub fn wants(&self, topic: &str) -> bool; // "{domain}/5/" prefix, or "*/5/" for Any
    pub fn is_connected(&self) -> bool;
    pub fn device(&self, id: &str) -> Option<&DeviceView>;
    pub fn devices(&self) -> impl Iterator<Item = &DeviceView> + '_;
    pub fn devices_with_id<'a>(&'a self, id: &'a str) -> impl Iterator<Item = &'a DeviceView> + 'a; // one per domain
    pub fn effective_state(&self, id: &str) -> Option<DeviceState>;
    pub fn is_tree_complete(&self, root: &str) -> bool;
}

// ===== ebus-homie::datatype =====
pub fn check_value(text: &str, prop: &PropertyDescription) -> Result<(), Problem>;

// ===== ebus-homie-tokio =====
#[derive(Clone)]
pub struct Inbox; // sync, never blocks, safe from any std::thread
impl Inbox {
    pub fn message(&self, topic: impl Into<String>, payload: bytes::Bytes, retain: Option<bool>) -> bool; // false = dropped over the byte cap
    pub fn connected(&self, session_present: bool); // never dropped
    pub fn disconnected(&self);                     // never dropped
    pub fn command_failed(&self, cmd: Command);
}
#[non_exhaustive]
pub struct AttachOptions { pub inbox_bytes: usize /* 4 MiB */, pub event_capacity: usize /* 1024 */ }
pub struct Attached {
    pub handle: ControllerHandle,
    pub inbox: Inbox,
    pub commands: Commands,
    pub events: EventReceiver,
    pub driver: ControllerDriver,
}
// Skips every SUBSCRIBE queued before the controller handled the last clean-session CONNACK reported.
impl Commands {
    pub async fn recv(&mut self) -> Option<Command>;
    pub fn try_recv(&mut self) -> Option<Command>;
    pub fn last_is_stale(&self, last: &Command) -> bool; // for a command held across an await
}
pub fn attach(config: Config, opts: AttachOptions) -> Result<Attached, ConfigError>;
impl ControllerDriver { pub async fn run(self) -> Result<(), DriverError>; } // Send; the host spawns it
#[non_exhaustive]
pub enum DriverError { CommandsClosed }
pub enum Delivered { Event(Event), Overflow { dropped: u64 } } // Overflow => re-read snapshots
impl EventReceiver { pub async fn recv(&mut self) -> Option<Delivered>; pub fn try_recv(&mut self) -> Option<Delivered>; }

pub type DeviceSnapshot = DeviceView;
#[derive(Clone)] // Send + Sync
pub struct ControllerHandle;
impl ControllerHandle {
    pub fn device(&self, id: &str) -> Option<Arc<DeviceSnapshot>>; // sync snapshot reads
    pub fn devices(&self) -> Vec<Arc<DeviceSnapshot>>;
    pub fn effective_state(&self, id: &str) -> Option<DeviceState>;
    pub fn is_tree_complete(&self, root: &str) -> bool;
    pub fn is_connected(&self) -> bool;
    pub async fn changed(&mut self) -> bool; // false once the driver stopped
    // Ok = validated and handed to the command queue (QoS 0, no ack)
    pub async fn set(&self, addr: PropertyAddr, payload: impl Into<String>) -> Result<(), SetError>;
    pub async fn set_with(&self, addr: PropertyAddr, payload: impl Into<String>, opts: SetOptions) -> Result<(), SetError>;
    pub fn try_set(&self, addr: PropertyAddr, payload: impl Into<String>) -> Result<(), SetError>; // safe on a reader thread
    pub async fn broadcast(&self, subtopic: &str, payload: &str) -> Result<(), SetError>;
    pub async fn shutdown(&self);
}

// ===== ebus-rumqttc =====
pub struct Running { pub handle: ControllerHandle, pub events: EventReceiver, pub task: ControllerTask }
impl ControllerTask { pub async fn run(self) -> Result<(), Error>; } // Send
#[non_exhaustive]
pub enum Error { Driver(DriverError) }
pub fn controller(mqtt: rumqttc::MqttOptions, config: Config, opts: AttachOptions) -> Result<Running, ConfigError>;
pub fn client_id() -> String;
```

Snapshots are copy-on-write: the driver republishes `Arc<Snapshot>` through a `watch` channel once per inbox drain in which something changed, with new `Arc<DeviceSnapshot>`s only for devices that changed. Unless a link change, a timer or a shutdown touched every device, it compares only the devices the drain touched. Tree completeness, which walks every tree, is recomputed only after such a change, a `$state` or `$description` message, or a device-level event, never for value traffic alone. A device's properties and alerts are persistent maps (`rpds`, `Arc`-backed), and the controller writes one only when its content changes, so a device is unchanged when its scalar fields are equal, it holds the same description `Arc`, and it still shares both maps with its snapshot (`DeviceView::shares_values`); the comparison takes constant time. A changed device's new snapshot shares every property and alert entry that did not change, including the slots a description change leaves unchanged, and the snapshot's device map, also persistent, copies only the path to each changed id. The driver yields to the runtime after each batch of 256 inputs while a backlog remains.

### Discovery

| Mode | Subscriptions |
|---|---|
| `Wildcard` (default) | `{domain}/5/+/$state` with domain `ebus` (framework.md:85, 359-363); `Domain::Any` uses `+/5/+/$state` (convention.md:161-162). Devices are keyed by (domain, id); `/set` goes to the device's own domain. |
| `Device(id)` | That device's `$state`, `$description`, `+/+`, `+/+/$target`, plus `{root}/$state` once its description names a `root` (convention.md:258-260). |
| `Tree{root}` | The root unpaced, its descendants paced; each device's own `$state` is subscribed (convention.md:259-260). A device's declared children are reconciled when its `$state` changes to `ready` and when its description changes while it is `ready` (python homie.py:4522-4527, 4678-4687); removal is leaves-first with a visited set; a declared child removed by a zero-length `$state` that returns is re-adopted; a child another tracked parent already declares is not subscribed again. |

A device exists once a valid `$state` arrives (convention.md:272); an unparseable `$state` is logged and creates nothing. `$description` is always subscribed per device.

### Subscriptions and pacing

Pacing follows python-sdk#97 with python's constants (homie.py:3566-3576).

1. Stage one, unpaced: `{d}/$state` (non-wildcard modes) and `{d}/$description`. A `$description` that arrives before `$state` is stored.
2. Stage two, once both are held: `{d}/+/+` and `{d}/+/+/$target`, with at most `batch_devices` (8) devices in flight owing at most `batch_values` (500) retained messages, 2 per retained property. A device over the budget is subscribed node by node (`{d}/{n}/+`, `{d}/{n}/+/$target`), with `{d}/$alert/+` in the first chunk. One step's filters go out in one `Command::Subscribe`. When a description stops declaring a node subscribed node by node, that node's filters are released.
3. A burst ends when every owed key has arrived, or when the retained copy of the `$description` resubscribed right after the property filters arrives (MQTT 3.1.1 section 3.8.4). The marker counts when `retain != Some(false)`, as python does (homie.py:4650); with `retain: None` a live republish can end a burst early, and stuck healing bounds the overshoot.

Subscriptions use QoS 2 (convention.md:45). `+/+` also carries `$alert/{id}` (to `Alert` events) and `$log/{level}` (to `log` at debug).

### Clearing what the broker no longer retains

A value, `$target` or alert deleted while the controller did not hear it (the link was down, or the host dropped the message) is absent from the next replay. A re-sync window clears it.

1. Every SUBSCRIBE carrying a device's `$description` filter is numbered, and each retained `$description` received (`retain: Some(true)`) is matched to the device's oldest unmatched SUBSCRIBE. The broker processes SUBSCRIBEs in order (MQTT 3.1.1 section 3.8.4), replays are assumed to arrive in that order (item 7), and some never arrive (the topic is not retained, the connection was lost), so the matched number is never above that of the SUBSCRIBE answered. Unmatched SUBSCRIBEs are kept across a link-up, since a client may send after a CONNACK a SUBSCRIBE queued before it; one is dropped once a later-numbered SUBSCRIBE has been matched, or when the host reports it failed. With `link_up_discards_queued` the host rules that out, and a clean link-up drops every unmatched SUBSCRIBE, along with the SUBSCRIBEs still queued in the controller.
2. A window opens when property filters are subscribed for nodes where the device holds a retained value, a `$target` or, when the filters cover `$alert`, an alert. A `$description` marker follows the property filters: last in the same SUBSCRIBE, or in a SUBSCRIBE of its own when stage one's `$description` precedes them. This covers link-up re-pacing, paced or unpaced (the tree root, device mode, and wildcard mode without pacing), stuck healing, dropped-message healing, and a valid description after a rejected one.
3. The window closes when a matched number reaches its marker's. Every replay of its filters has then arrived (item 7), and the retained values (`PropertyCleared`, with `last_valid` reset), `$target`s (`TargetChanged{target: None}`) and alerts it covers that no message touched since it opened are cleared. `DeviceSynced` waits for open windows until `sync_timeout` expires.
4. A requested replay that never arrives makes that device's later windows close late, never early; they close only once another device's later-numbered SUBSCRIBE is matched, so with no other tracked device they never close again. With `link_up_discards_queued` the delay also ends at the next clean link-up, which covers the common case, a re-sync whose replays are lost with its connection. A window is dropped, clearing nothing, when the host reports its SUBSCRIBE failed, when the host drops a message for its device (reported before any later message, through `handle_lost` or `handle_dropped`), on a rejected description, and on every link-up; past 8 open windows per device the oldest is dropped.
5. No window opens after a link-up that may resume a session (`session_present: true`, or an implicit link-up), since the broker may send a message it already delivered again (section 4.4), so nothing is cleared and `DeviceSynced` does not wait. The replays in that session are still matched, so the windows of the next clean link-up close on time. Nothing is cleared for a device whose `$description` a host filter matches, since the host's own SUBSCRIBEs replay it without a number.
6. The first message with `retain: None` drops every open window, and no window opens after it. A live `$description` is then indistinguishable from a replay, and MQTT orders a client's messages only per topic (section 4.6), so no message the controller receives bounds a replay. A value deleted while the controller did not hear it stays held.
7. Assumed: the client sends commands in the order the controller queues them; the broker delivers every message of the connection, and the retained messages for a SUBSCRIBE's filters before those of any later filter or SUBSCRIBE, an order MQTT gives only per topic (section 4.6); and no `$description` replay is delivered twice, which delivery at QoS 2 rules out (section 4.3.3). A replay delivered twice in a resumed session leaves the device's matching one SUBSCRIBE ahead, and a window after the next clean link-up can then close early if a SUBSCRIBE queued before that link-up is sent after it, which `link_up_discards_queued` rules out.

### Stuck healing

Ported from homie.py:4317-4422. Each awaiting or draining device has a deadline of `subscribed_at + stuck_timeout` (10 s).

| Situation | Action |
|---|---|
| Stage one missing `$state` or `$description`, or the fresh `$description` a link-up requires | Subscribe those filters again through the ledger, so the broker replays their retained messages (MQTT 3.1.1 section 3.8.4) |
| Stage two incomplete | Free the slot and requeue the device; its next SUBSCRIBE makes the broker replay its retained messages |
| 3 attempts, or a retry that brought no more messages than the last | `DeviceStuck{gave_up: true}`; subscriptions stay in place |
| `handle_dropped` (inbox overflow) | The same resubscribe, once the backlog drains; the bundled driver reports each drop with `handle_lost` before the next message, so no later marker closes a window over it; in wildcard mode a dropped `$state` also resubscribes the discovery filter. A message larger than the inbox cap on its own is dropped without healing, since its replay would be dropped again |
| `handle_command_failed` | Subscribe or Unsubscribe retried with 1-30 s backoff; a `/set` Publish is dropped with `reason=setDropped`, since QoS 0 is now-or-never (convention.md:695) |

### Messages: order, retention, repetition

- A repeated identical `$state` emits nothing, and so does a `Link::Down` while the link is down.
- `$description` is deduplicated by raw bytes, then by `content_hash`. It is parsed with `parse_with_ignored`; a device-level error emits `DescriptionRejected` and the device stays known by its `$state` with no model.
- A description change while the device is not `ready` is applied silently; on the next ready edge one `DescriptionChanged` fires if the content differs from the last one announced. A change while `ready` is applied and announced at once with `while_ready: true` and `reason=descriptionChangedWhileReady` (convention.md:207). A change while `sleeping`, also forbidden there, logs `reason=descriptionChangedWhileSleeping` and is announced on the next ready edge with `while_ready: true`. New nodes are paced in; values of vanished properties are dropped.
- A valid `$description` after a rejected one resubscribes the device's held property filters, so the broker replays the values discarded while it had no model.
- Values before the `$description` are buffered raw, last value per (node, property), up to `max_early_values`, and validated when the description arrives. Values for undeclared or pruned properties are dropped at debug level.
- `PropertyChanged`, `TargetChanged` and `Alert` fire only when the payload changes, whatever the origin; a repeat, retained or live, emits nothing.
- Properties declared `retained: false` always produce `PropertyEvent` and are never cached (convention.md:370-371); a retained message for one is accepted with a diagnostic and delivered with `origin: Origin::Retained`, since the broker replays it on every subscribe (convention.md:50).
- A device holds at most `max_alerts` (256) alerts; a new alert id past it is dropped at debug level.
- A zero-length payload (`payload::classify` gives `Deleted`) clears a property (`PropertyCleared`), a `$target` or an `$alert`; on `$state` it removes the device.
- Every value and `$target` is checked with `check_value` and delivered unmodified as `Value{raw, check}` (convention.md:78); `last_valid` keeps the last good text. `check_value` checks grammar, enum and color membership, min/max, the 64-bit integer range, finite floats, and `0x00` only for `string`. Step rounding is the device's job (convention.md:399).
- Values can arrive after `ready`. `DeviceSynced{missing}` fires once the description is held and the device is in no pacing stage, and then either when every declared retained property has a value and no re-sync window is open (`missing: 0`, per convention.md:279) or when `sync_timeout` has expired (`missing` counts the properties without a value). It re-arms on a description change and on an untrusted link-up.
- Messages for untracked devices are ignored and trigger no unsubscribe, since they may arrive through a host filter. A subscribe that completes after its device was dropped is undone through the ledger.

### Effective state

`state::effective_state` decides: only a root in `lost` overrides its tree (convention.md:262-267, 284; framework.md:419). When a root's state changes, `EffectiveStateChanged` fires for every descendant whose effective state flips.

### `/set` and `$broadcast`

1. Rejections, in order: `NotConnected` (QoS 0 is never held; convention.md:695); unknown device or property; `NotSettable`; effective state other than `ready` unless `allow_not_ready` (convention.md:279).
2. Unless `unchecked`, the payload must pass `parse_set_payload(text, datatype, format, current)` with `current` from `last_valid`: the same check the device runs, rounding included. The result is discarded.
3. The caller's bytes are sent unchanged, so the `$target` echo is byte-exact (convention.md:470); `""` becomes `0x00` for `string` (convention.md:60-71).
4. QoS 0, non-retained (framework.md:87; convention.md:48-49, 483). framework.md:87 also decides for retained properties, for which convention.md:45 recommends QoS 2. There is no retry when the device reports a different value (convention.md:474-475).

`broadcast` publishes `{domain}/5/$broadcast/{subtopic}`, non-retained, QoS 0, after checking every segment is a valid id (convention.md:531-535).

### Reconnect

| Input | Behavior |
|---|---|
| `Link::Down` | Emit `Link(Down)`. Filters stay held with their wire state, and in-flight pacing work stays until the next `Up`, which discards it unless the session is trusted. |
| `Link::Up{session_present: false}`, or `true` without `trust_session` | Drop pacing work and windows; queue the discovery filter (wildcard mode), then stage one for every known device: in wildcard mode each device paced; in device mode the device unpaced; in tree mode the root unpaced, then every other tracked device paced, since the clean session dropped the `$state` subscription that monitoring a child requires (convention.md:259-260), and the attribute filters a removed child keeps are subscribed again so a return is heard. Each device stays in stage one until a fresh `$description` arrives (homie.py:3996-4013), so values held from before the link-up do not sync it. Set `confirmed = false` everywhere and `synced = false` on every described device, arm `absent_after` and `sync_timeout`, then emit `Link(Up)`. This is ebus-mqtt-client's reconnect order (client.py:936-961). |
| `session_present: true` with `trust_session` | Keep subscriptions and pacing work, restarting the stuck deadlines; stuck healing covers gaps. Windows are dropped and nothing is cleared. |
| A message after `Down` with no `Up` | Treated as `Up{session_present: false}`, logged as `reason=implicitLinkUp`, except that the ledger keeps its wire state, so a filter released afterward is still unsubscribed, and no window opens until the next clean link-up. The host contract still requires forwarding ConnAck. |

`trust_session` is off by default: re-pacing after `session_present: true` is always correct and costs one replay.

A broker restarted without persistence can come back with an empty retained store while devices republish later, so absence is a distinct state: a device not re-heard within `absent_after` gets `DeviceAbsent`, stays in the registry until a zero-length `$state` or tree reconciliation removes it, and heals on its next publish. Meanwhile stuck healing, when `stuck_timeout_ms` is set, re-requests its `$description` and reports `DeviceStuck{missing: Description}` until it gives up. `DeviceAbsent` is also the only signal for a device whose `$state` was deleted while the controller did not hear it.

### Liveness

A device's liveness is its `$state` together with the root's last will, which sets `$state` to `lost` (convention.md:56-58); MQTT keepalive covers the controller's own link. The controller keeps no per-device or per-connection last-received time, since tracking one would change state on every message. A host that wants one keeps it as connection metadata, stamping messages as it feeds the `Inbox`. A heartbeat, if one is ever needed, is one dedicated property, not a fresh timestamp on every property.

### Removal

A zero-length `$state` removes a device (convention.md:163, 287-289): the controller emits `DeviceRemoved` and unsubscribes the property filters it owns for the device. Tree and device modes keep the `$state` and `$description` filters so a returning device is heard; wildcard mode releases `$description` too, and the discovery filter hears a return. In tree mode, reconciliation removes a child that leaves its parent's `children` (convention.md:636-641), with its descendants, releasing all their filters, except the root and any device another tracked parent still declares. Messages for a removed device are then ignored.

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

Host contract, in the `ebus-homie-tokio` rustdoc and exercised with a rumqttc 0.10 client in [ebus-homie-tokio/tests/mosquitto.rs](../crates/ebus-homie-tokio/tests/mosquitto.rs):

1. Forward ConnAck, Publish and connection errors.
2. Never await a controller call on the reader thread; `try_set` is the form for it.
3. Declare in `host_filters` any filter string the host also subscribes.
4. Leave resubscribing the controller's filters to the controller.
5. Report executor failures through `command_failed`.
6. Set `Config::link_up_discards_queued` only when the client never writes, after a clean-session CONNACK, a request handed to it before that CONNACK, and every command handed to the client after a CONNACK was received from `Commands`, or checked with `Commands::last_is_stale`, after that CONNACK was reported. Without it, a re-sync whose replays are lost with the connection can keep that device's later windows from closing.

The driver counts the commands it sends. At a clean-session `Up` it first sends what the controller has queued, then records that count, and `Commands` skips every SUBSCRIBE at or below it, as well as every SUBSCRIBE received after a clean CONNACK is reported and before the driver handles it. It skips them only with `link_up_discards_queued`, which makes the controller drop their replay records at that link-up and subscribe again every filter it holds. Without the flag the controller keeps those records and still expects the replays, so every command is delivered. A stale UNSUBSCRIBE is still sent; the broker handles it before the link-up's own SUBSCRIBEs.

A host whose client wrapper drops the retain bit passes `retain: None`; the burst marker then counts any `$description`, stuck healing bounds the error, and values the broker stopped retaining are not cleared.

The inbox is capped in bytes and the event channel in deliveries; the command and request channels are unbounded, so a host whose client stalls holds the controller's commands until its executor catches up.

## Bundled path: tokio and rumqttc 0.25

```rust
let mut mqtt = rumqttc::MqttOptions::new(ebus_rumqttc::client_id(), "localhost", 1883);
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

`ControllerTask::run` joins three futures: the driver; an `EventLoop::poll` loop feeding the `Inbox`, with 1-30 s backoff (client.py:155); and a command executor, separate so a full request channel cannot deadlock against `poll`. The executor hands `AsyncClient` nothing while the link is down, since rumqttc would queue it for the next connection, and uses the `try_*` calls, so a request handed over before a disconnect is either written or in the queue a clean-session CONNACK empties; a request that does not fit waits for the event loop to send something, and a filter or topic rumqttc refuses is reported through `command_failed`. After any wait the executor checks `Commands::last_is_stale` again, so no SUBSCRIBE queued before a clean link-up is sent after it, and `controller()` sets `link_up_discards_queued`. A `/set` already handed to the executor when the link drops is sent after the next link-up. After the shutdown's UNSUBSCRIBE the executor sends a DISCONNECT, and the task ends once it is written, or at once when the link is down, discarding the commands not yet written. `controller()` connects with the client id in the caller's `MqttOptions`; `ebus_rumqttc::client_id()` returns `ebus-controller-` plus 16 random hex digits, after python's `homie-controller-{uuid}` (homie.py:3870).

## Device role (planned)

- `ebus_homie::device::Device` has the controller's shape (`handle_*`, `poll_*`, `poll_timeout`), plus `last_will()`, the root's retained `$state=lost` (convention.md:56-58), and a transition guard that publishes `init`, one `$description`, the values, then `ready` (homie.py:2897-2928).
- `ebus_mqtt::Session` owns the shared `FilterLedger` (refcounts across roles), `PublishHold` and the reconnect order. On `Up`: flush the hold (newest retained publish per topic, QoS 1/2 non-retained in order, QoS 0 dropped, capped at 512; client.py:31, 526-557), then the device's tree refresh, then the controller's resubscribe, then the app notification. Until the device role exists, the controller owns a private ledger.
- A `Role` trait lets one driver run a device and a controller over one link, the co-resident broker-host pattern (framework.md:20, 461).
- Bring-your-own hazards: one connection carries one will (convention.md:56-58); rumqttc 0.10 exposes `EventLoop::pending` (eventloop.rs:56) and replays it after reconnect (eventloop.rs:115-116), the replay hazards of ebus-mqtt-client GH #20 and #21 (client.py:545-556).
- A device skips publishing a retained value whose final wire payload equals the last one it published on that topic (homie.py:1368-1379), except in a whole-tree republish, such as the one after a reconnect, which publishes every value so that a broker whose retained store is empty is repopulated (homie.py:1299-1310, python-sdk's `force=True`); a `retained: false` property publishes every value.
- Float payloads use the shortest digits that read back as the same value at the value's own width, `f64` (`datatype::encode_float`) or `f32` (`datatype::encode_float32`), in python-sdk's layout.
- CPU levers for a device that replaces a Python one: borrowed `topic::parse` so the inbound path allocates nothing; one owner task with no locks; description work only when `content_hash` changes; deadlines instead of polling threads.

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
| Spec vectors | Topics, empty string, effective state, payloads per datatype, `$description`, `check_value`; derived from the spec, with python-sdk results as expect values except the python-sdk#110 overrides. Planned: the `/set` verdict and description dedup/ready-edge suites | [vectors/](../vectors/), [tests/vectors.rs](../crates/ebus-homie/tests/vectors.rs), generated by [tools/gen_vectors.py](../tools/gen_vectors.py) |
| python-sdk vectors | python-sdk's choices where the spec is open: float format (`f64`), `json.dumps`, `sanitize_id`, duration encoding, `/set` parsing, `$description` wire bytes and round trip. `float32-format.json` holds rust-sdk's own `f32` choice: the shortest digits that read back as the same `f32` (computed with numpy), in python-sdk's `f64` layout | [vectors/python-sdk/](../vectors/python-sdk/) |
| Panel-sim captures | Masked panel trees from ebus-panel-sim: every `$description` parses and every non-string text value passes `parse_set_payload` for its datatype and format. Each capture is also replayed through the controller from a simulated broker, in wildcard mode and in tree mode from each root, each with the retain bit and with `retain: None`: the retained tree, broker restart with an empty retained store, `ready` before values, repeated `$description` (identical and changed), values before the description, removal and return, and values deleted while disconnected (cleared with the retain bit, kept without it); restart, values before the description and deletion also run in unpaced wildcard mode. A synthetic 50-device by 30-property tree runs the same baseline and node-by-node pacing | [fixtures/panel-sim/](../fixtures/panel-sim/), [tests/captures.rs](../crates/ebus-homie/tests/captures.rs), [tests/controller_replay.rs](../crates/ebus-homie/tests/controller_replay.rs) |
| Local capture corpus | `EBUS_CAPTURE_CORPUS=<dir>` also runs the capture test and the controller replay over every `*.tree.json` below it, with value faults reported, and checks that every fully accepted `$description` line in each `ebus-retained-topics.txt` dump round-trips through `to_json` byte for byte | [tests/captures.rs](../crates/ebus-homie/tests/captures.rs), [tests/controller_replay.rs](../crates/ebus-homie/tests/controller_replay.rs) |
| mosquitto e2e | Broker on a free port, skipped when the binary is absent: pacing that heals the drops of a broker with `max_queued_messages 30`; inbox-overflow drops healed by resubscribing; restart without persistence (`DeviceAbsent`, then healing); removal and return in wildcard and tree modes; discovery and sync of a python-sdk device tree; `/set` round trip against a python-sdk device with byte-exact `/set` payloads and the republished value; a host holding overlapping and identical filters; a re-sync lost with its connection, behind a proxy that passes the SUBSCRIBEs and blocks every broker reply but the CONNACK, followed by a value deleted at the broker and cleared after the reconnect; bring-your-own through a rumqttc 0.10 dev-dependency alongside the bundled 0.25 path | `ebus-homie-tokio/tests`, `ebus-rumqttc/tests` |
| Live panels (planned) | Read-only example binary in wildcard and tree modes, compared with an independent Homie client; time to `DeviceSynced` and CPU per message. `/set` only with the maintainer's go-ahead | manual |

## Embedding in an existing service

- A host service that already owns an MQTT connection uses bring-your-own: the controller runs on the host's runtime (the host spawns the driver) and forces no MQTT client or version on it.
- A host workspace that builds with `--locked` takes the `Cargo.lock` diff as the acceptance gate: `cargo tree -d` should show no new MQTT client, rustls or tokio version.

## Open questions

1. Bring-your-own device role: supported when the host installs `last_will()` and drains the client's pending replay, or does a device always need its own connection?
