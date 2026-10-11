# rust-sdk design

rust-sdk implements the eBus device and controller roles in Rust: Homie 5 over MQTT 3.1.1 (MQTT 5 later, behind a feature) on TCP or TLS, with mDNS broker discovery. The protocol logic lives in sans-IO core crates that run unchanged on tokio services, on ESP32, and inside a host application that already owns an MQTT connection.

## Crates

| Crate | Role | Depends on | `no_std` |
|---|---|---|---|
| `ebus-homie` | Homie 5 codec (ids, topics, states, payloads, datatypes, `$description`) and the controller and device state machines | `ebus-mqtt`; `serde_json`, `sha2`, `rpds`, `num-*` (under `alloc`); `log` (under `log`) | yes, `alloc` optional |
| `ebus-mqtt` | MQTT vocabulary, topic filters and filter ownership; the `Role` trait and `Pair` | nothing | yes, `alloc` optional |
| `ebus-discovery` | eBus DNS-SD contract (planned): service types, TXT records, broker selection | nothing; `ebus-mqtt` (planned) | yes, `alloc` optional |
| `ebus-discovery-tokio` (planned) | Avahi and mdns-sd backends, broker search and host advertisement on tokio | `ebus-discovery`, tokio; zbus (`avahi`), mdns-sd (`mdns-sd`) | no |
| `ebus-homie-tokio` | Drives the core state machines on tokio over a host-owned or bundled MQTT connection | `ebus-homie`, `ebus-mqtt`, tokio 1 (`sync`, `time`, `macros`, `rt`), `rpds` | no |
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

### Device discovery

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

A host service keeps its own MQTT client, options, TLS, last will and reconnect loop; a host that also runs the device role installs the device's will (see [Device role](#device-role)). It depends on `ebus-homie-tokio` with `default-features = false` and feeds the `Inbox`. Example for a service on rumqttc 0.10, whose sync `Client` methods call `pollster::block_on` (rumqttc 0.10.0 `client.rs:224, 245, 276`), so the executor uses the non-blocking `try_*` calls:

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

`device()` and `pair()` run the same three futures around a `DeviceDriver` or `PairDriver`, with the connection settings and shutdown flush given under [Device role](#device-role).

## Device role

### Shape

The device role is a sans-IO state machine, `ebus_homie::device::Tree` (alloc), with the controller's shape: `handle_*` in, `poll_*` out, time from the caller. One `Tree` owns one root device and its descendants, because one connection carries one will (convention.md:56-58) and children share the root's connection (framework.md:419). A process publishing several roots runs one `Tree` and one connection per root. In the first release a driver serves one `Tree`, so each root keeps its own driver, connection and will.

| Crate | Additions |
|---|---|
| `ebus-mqtt` | `Role` trait and `Pair<A, B>` (two roles on one link); `Receipt`; `Handled` moves here, re-exported from `ebus_homie::controller` |
| `ebus-homie` | `device` module (alloc): `Tree`, `Builder` (builds a tree with one description check per device, at `finish`), `ValueCheck` (a slot's checks, copied for another thread), `Reconfigure`, specs, `ValueRef`, `HomieValue`, `HomieEnum`, `DirtySet`, `DeviceEvent`, `AlertError`; `impl Role for Controller`; `Id` and `PropertyAddr` move to the crate root, re-exported from `controller`; `pyfmt::write_float`/`write_float32` and `datatype::write_*` encode into a caller buffer, with `format_float` (pyfmt.rs:160), `format_float32` and the existing encoders as wrappers; `datatype::parse_set_payload_with(text, datatype, format, &NumericFormat, current)`, which takes the numeric format pre-parsed (the format text still serves `enum` and `color`), with `parse_set_payload` (datatype.rs:1099) as a wrapper |
| `ebus-homie-tokio` | `Inbox::receipt`; `Commands::ending`; `device` module: `TreeBuilder`, `ReconfigureHandles`, `Property<T>`, `Settable<T>`, `SetRequest<T>`, `Event<T>`, `DeviceHandle`, `DeviceOptions`, `attach_device`, `attach_pair`, `AttachError`, and re-exports of the core types they take; an internal `Driver<R>` with `DeviceDriver` and `PairDriver` as newtypes over it. `ControllerDriver` keeps its own loop and shares the controller's snapshot and event code with `PairDriver`; `ControllerDriver::run` is unchanged |
| `ebus-rumqttc` | `device()`, `pair()`, `RunningDevice`, `RunningPair`, `DeviceTask`, `PairTask` |

### Public API

```rust
// ===== ebus-mqtt =====
pub enum Handled { Consumed, Ignored }
pub trait Role {
    fn handle_link(&mut self, now: Instant, link: Link);
    fn handle_message(&mut self, now: Instant, msg: Message<'_>) -> Handled;
    fn handle_command_failed(&mut self, now: Instant, cmd: Command);
    fn handle_timeout(&mut self, now: Instant);
    fn handle_dropped(&mut self, _now: Instant, _topic: &str) {} // controller healing
    fn handle_lost(&mut self, _topic: &str) {}
    fn handle_receipt(&mut self, _now: Instant, _r: &Receipt) {} // device delivery tracking
    fn poll_command(&mut self) -> Option<Command>;
    fn poll_timeout(&self) -> Option<Instant>;
    fn wants(&self, topic: &str) -> bool;
    fn owns(&self, cmd: &Command) -> bool; // routes handle_command_failed
    fn holds(&self, filter: &str) -> bool; // this role's ledger count > 0
}
/// A broker or client report on a command already sent, from a host that sets `TreeConfig::receipts`.
#[non_exhaustive]
pub enum Receipt {
    Subscribed { filters: Vec<String>, granted: bool }, // SUBACK; granted is false when any return code is 0x80
    Written { topic: String },                           // a publish written to the connection
    Acknowledged { topic: String, cleared: bool },       // PUBACK or PUBCOMP; cleared: zero-length payload
}
/// Link events and commands: a first, then b. Messages: every role that wants them. Receipts: both roles.
/// A wanted message while down is an implicit link-up for both roles, so the pair keeps the link state.
pub struct Pair<A: Role, B: Role> { pub a: A, pub b: B, /* private: up */ }
impl<A: Role, B: Role> Pair<A, B> { pub fn new(a: A, b: B) -> Self; pub fn is_up(&self) -> bool; }
impl<A: Role, B: Role> Role for Pair<A, B> {}

// ===== ebus-homie::device =====
#[non_exhaustive]
pub struct TreeConfig {
    pub domain: String,               // "ebus" (framework.md:85)
    pub qos: QoS,                     // ExactlyOnce: retained publishes, the will, /set subscriptions (convention.md:45)
    pub max_description_bytes: usize, // 256 KiB; a larger $description is refused
    pub watch_own_state: bool,        // false; true re-asserts the root's $state over a stale `lost`
    pub receipts: bool,               // false; set by ebus-rumqttc: the host reports SUBACKs, writes and acks
    pub max_tombstones: usize,        // 4096 topics
    pub broadcast: bool,              // false; true subscribes {domain}/5/$broadcast/#
    pub host_filters: Vec<String>,
}
pub struct DevIdx(u16);
pub struct NodeIdx(u32);
pub struct SlotId { index: u32, gen: u32 } // a removed slot's id returns Gone
pub struct DeviceSpec { pub name: Option<String>, pub r#type: Option<String>,
    pub extensions: Vec<String>, pub extras: Vec<(String, serde_json::Value)> }
pub struct NodeSpec { pub name: Option<String>, pub r#type: Option<String> }
pub struct PropSpec { pub name: Option<String>, pub format: Option<String>, pub unit: Option<String>,
    pub settable: bool, pub retained: bool /* true */, pub target: bool /* $target on every update, convention.md:333 */ }
impl DeviceSpec { pub fn typed(r#type: &str) -> Self; }
impl NodeSpec { pub fn typed(r#type: &str) -> Self; }
impl PropSpec { pub fn new() -> Self; } // plus by-value setters: name, format, unit, retained, target
#[non_exhaustive]
pub enum BuildError { InvalidId, DuplicateId, NodeIdHasDot, FormatRequired, InvalidFormat(Problem),
    Ignored(Ignored) /* parse_with_ignored pruned something */, CoreKeyInExtras, InvalidExtension,
    DescriptionTooLarge { bytes: usize }, Gone, Capacity /* no device or node index left */ }

/// Borrowed, so nothing is copied before the unchanged-value gate.
pub enum ValueRef<'a> { Integer(i64), Float(f64), Float32(f32), Boolean(bool), String(&'a str),
    Enum(u16 /* index into the format list */), Color(&'a str), Datetime(&'a str),
    Duration { secs: u64, micros: u32 }, Json(&'a serde_json::Value) }
pub trait HomieValue: Clone + 'static {
    const DATATYPE: Datatype;
    fn as_value(&self) -> ValueRef<'_>;
    fn same(&self, other: &Self) -> bool;               // floats by to_bits
    fn from_set(v: &SetValue) -> Option<Self>;
    fn default_format() -> Option<Cow<'static, str>> { None } // u8 -> "0:255"; enum variants
}
// i64, i32, i16, i8, u32, u16, u8, f64, f32, bool, String, Arc<str>, core::time::Duration,
// Color, Datetime, serde_json::Value, and every E: HomieEnum
pub trait HomieEnum: Copy + 'static {
    const VARIANTS: &'static [&'static str]; // becomes $format
    fn index(self) -> u16;
    fn from_index(i: u16) -> Option<Self>;
}

pub enum Presence { Ready, Sleeping, Disconnected, Lost } // Disconnected and Lost: non-root only; the root's `lost` comes from declare_lost
pub enum Change { Unchanged, Queued, Cached /* link down */, Dropped /* event while down */ }
#[non_exhaustive]
pub enum ValueError { NotFinite, OutOfRange, NotInFormat, WrongDatatype, IsEvent, NotEvent, NotTarget, Gone,
    NotForRoot /* set_presence(root, Disconnected | Lost) */ }
#[non_exhaustive]
pub enum AlertError { InvalidId /* convention.md:519 */, EmptyMessage, Gone }
#[non_exhaustive]
pub enum DeviceEvent {
    Set { slot: SlotId, value: SetValue, raw: Box<[u8]> }, // validated and step-rounded; raw as received
    Broadcast { subtopic: String, payload: Box<[u8]> },
    StateReasserted { attempt: u8, gave_up: bool },
    Link(Link),
}
pub enum Shutdown { Announce, Silent, Remove }

impl Tree {
    pub fn new(cfg: TreeConfig, root_id: &str, root: DeviceSpec) -> Result<Self, BuildError>;
    pub fn root(&self) -> DevIdx;
    pub fn last_will(&self) -> LastWill; // {domain}/5/{root}/$state, "lost", cfg.qos, retained
    /// One commit: add_child, add_node, add_property(node, id, datatype, implied_format, spec), add_typed::<T>,
    /// remove_child (not the root: InvalidId), remove_node, remove_property, set_format, set_unit, set_name.
    pub fn reconfigure<R>(&mut self, f: impl FnOnce(&mut Reconfigure<'_>) -> Result<R, BuildError>) -> Result<R, BuildError>;
    pub fn set_value(&mut self, slot: SlotId, v: ValueRef<'_>) -> Result<Change, ValueError>; // target mode: $target first unless it already equals v
    pub fn set_progress(&mut self, slot: SlotId, v: ValueRef<'_>) -> Result<Change, ValueError>; // target mode: value only; set_value while no $target is held
    pub fn set_target(&mut self, slot: SlotId, v: ValueRef<'_>) -> Result<Change, ValueError>;
    pub fn accept_set(&mut self, slot: SlotId, raw: &[u8]) -> Result<Change, ValueError>; // $target := raw bytes
    pub fn clear_value(&mut self, slot: SlotId) -> Result<Change, ValueError>; // zero-length retained; target mode: then $target
    pub fn emit(&mut self, slot: SlotId, v: ValueRef<'_>) -> Result<Change, ValueError>; // retained: false
    pub fn set_presence(&mut self, dev: DevIdx, p: Presence) -> Result<(), ValueError>;
    pub fn raise_alert(&mut self, dev: DevIdx, id: &str, msg: &str) -> Result<Change, AlertError>;
    pub fn clear_alert(&mut self, dev: DevIdx, id: &str) -> Result<Change, AlertError>;
    pub fn declare_lost(&mut self); // the root's state becomes `lost`, connected or not; set_presence(root, Ready) returns it
    pub fn shutdown(&mut self, mode: Shutdown);
    pub fn poll_event(&mut self) -> Option<DeviceEvent>;
    pub fn current(&self, slot: SlotId) -> Option<Current>;
    pub fn is_connected(&self) -> bool;
    pub fn rejected_sets(&self) -> u64; // /set messages refused by the SDK
    pub fn handle_receipt(&mut self, now: Instant, r: &Receipt); // also the Role method
    pub fn value_check(&self, slot: SlotId) -> Option<ValueCheck>; // None once removed
    // Reads: config, devices() (pre-order), device(id), description(dev), state(dev), topic(slot), tombstones()
}
impl Role for Tree {}
/// Each change checked alone; descriptions built and checked once per device at finish. Tree::new is new + finish.
pub struct Builder;
impl Builder {
    pub fn new(cfg: TreeConfig, root_id: &str, root: DeviceSpec) -> Result<Self, BuildError>;
    pub fn edit(&mut self) -> Reconfigure<'_>; // no rollback; a refused change changes nothing
    pub fn set_receipts(&mut self, on: bool);
    pub fn finish(self) -> Result<Tree, BuildError>;
    // Reads: config, root, value_check(slot), topic(slot); Reconfigure also has value_check and topic
}
/// The checks set_value and emit run first, for a handle to run on its own thread.
pub struct ValueCheck;
impl ValueCheck {
    pub fn check(&self, v: ValueRef<'_>) -> Result<(), ValueError>;
    pub fn datatype(&self) -> Datatype; // also is_retained, is_target, is_settable, format
}

/// Lock-free dirty marks, set from any thread, drained by the owner.
pub struct DirtySet; // Box<[AtomicU32]>
impl DirtySet {
    pub fn new(slots: usize) -> Self;
    pub fn mark(&self, i: u32) -> bool;              // true when the bit was clear: wake the owner
    pub fn drain(&self, f: impl FnMut(u32) -> bool); // rotating start; false stops, unvisited bits stay set
}

// ===== ebus-homie-tokio::device =====
impl Inbox { pub fn receipt(&self, r: Receipt); } // never dropped
impl Commands { pub fn ending(&self) -> Option<Shutdown>; } // after recv() returns None: the mode, Silent after abort()
pub struct TreeBuilder; // over a core Builder; descriptions are checked by attach_*
impl TreeBuilder {
    pub fn new(cfg: TreeConfig, root_id: &str, root: DeviceSpec) -> Result<Self, BuildError>;
    pub fn root(&self) -> DevIdx;
    pub fn config(&self) -> &TreeConfig;
    pub fn set_receipts(&mut self, on: bool); // set by ebus-rumqttc
    pub fn child(&mut self, parent: DevIdx, id: &str, spec: DeviceSpec) -> Result<DevIdx, BuildError>;
    pub fn node(&mut self, dev: DevIdx, id: &str, spec: NodeSpec) -> Result<NodeIdx, BuildError>;
    pub fn property<T: HomieValue + Send + Sync>(&mut self, node: NodeIdx, id: &str, spec: PropSpec) -> Result<Property<T>, BuildError>;
    pub fn settable<T: HomieValue + Send + Sync>(&mut self, node: NodeIdx, id: &str, spec: PropSpec) -> Result<Settable<T>, BuildError>;
    pub fn event<T: HomieValue + Send + Sync>(&mut self, node: NodeIdx, id: &str, spec: PropSpec) -> Result<Event<T>, BuildError>;
}
#[derive(Clone)] // Send + Sync
pub struct Property<T>;
impl<T: HomieValue + Send + Sync> Property<T> {
    pub fn set(&self, v: T) -> Result<(), ValueError>; // sync, never blocks; validated on the caller's thread; target mode: as Settable::set
    pub fn clear(&self) -> Result<(), ValueError>;     // target mode: also clears $target
    pub fn get(&self) -> Option<T>;
    pub fn subscribe(&self) -> watch::Receiver<Option<T>>;
    pub fn slot(&self) -> SlotId; // for remove_property, set_format, set_unit, set_name; also on Settable and Event
}
pub struct Settable<T>;
impl<T: HomieValue + Send + Sync> Settable<T> {
    pub fn property(&self) -> Property<T>;
    pub fn set(&self, v: T) -> Result<(), ValueError>;          // target mode: closes the transition
    pub fn progress(&self, v: T) -> Result<(), ValueError>;     // target mode: value only
    pub fn set_target(&self, v: T) -> Result<(), ValueError>;   // target mode: a local change of intent
    pub fn clear(&self) -> Result<(), ValueError>;               // also get(), as on Property
    pub async fn next_set(&mut self) -> Option<SetRequest<T>>;  // None once the tree stops
    pub fn try_next_set(&mut self) -> Option<SetRequest<T>>;
}
#[must_use]
pub struct SetRequest<T>;
impl<T: HomieValue + Send + Sync> SetRequest<T> {
    pub fn value(&self) -> &T;      // validated, step-rounded
    pub fn raw(&self) -> &[u8];     // bytes as received
    pub fn accept(self);            // target mode: $target := raw; otherwise set(value)
    pub fn accept_as(self, v: T);   // reflect the value the device reached; target mode: $target := raw, then set(v)
    pub fn reject(self, why: &str); // reason=propertySetRejected, nothing published
}
#[derive(Clone)]
pub struct Event<T>;
impl<T: HomieValue + Send + Sync> Event<T> { pub fn emit(&self, v: T) -> Result<(), EmitError>; }
#[non_exhaustive]
pub enum EmitError { Down, Full, Invalid(ValueError) }

#[non_exhaustive]
pub struct DeviceOptions { pub inbox_bytes: usize /* 4 MiB */, pub set_capacity: usize /* 16 per settable */,
    pub event_capacity: usize /* 1024 */, pub max_outstanding: usize /* 1024 */,
    pub flush_timeout_ms: u64 /* 2000 */, pub link_up_discards_queued: bool }
#[derive(Clone)]
pub struct DeviceHandle;
impl DeviceHandle {
    pub fn is_connected(&self) -> bool;
    pub fn rejected_sets(&self) -> u64; // as Tree::rejected_sets
    pub fn state(&self, dev: DevIdx) -> watch::Receiver<Option<DeviceState>>; // as Tree::state; None for no such device
    pub async fn set_presence(&self, dev: DevIdx, p: Presence) -> Result<(), ValueError>;
    pub async fn raise_alert(&self, dev: DevIdx, id: &str, msg: &str) -> Result<(), AlertError>;
    pub async fn clear_alert(&self, dev: DevIdx, id: &str) -> Result<(), AlertError>;
    /// One commit; typed handles created in the closure go live with it.
    pub async fn reconfigure<R: Send + 'static>(&self,
        f: impl FnOnce(&mut ReconfigureHandles<'_, '_>) -> Result<R, BuildError> + Send + 'static) -> Result<R, BuildError>;
    pub async fn next_broadcast(&self) -> Option<(String, bytes::Bytes)>; // with TreeConfig::broadcast
    pub async fn declare_lost(&self);
    pub async fn shutdown(&self, mode: Shutdown);
    pub fn abort(&self); // no `disconnected`, no DISCONNECT: the broker fires the will
}
/// Wraps the core `Reconfigure` and registers each typed handle with the driver at commit.
pub struct ReconfigureHandles<'a, 'b>; // &'a mut Reconfigure<'b>: the core commit's lifetime is its own
impl<'b> ReconfigureHandles<'_, 'b> {
    pub fn core(&mut self) -> &mut Reconfigure<'b>; // add_child, add_node, remove_*, set_format, set_unit, set_name
    pub fn property<T: HomieValue + Send + Sync>(&mut self, node: NodeIdx, id: &str, spec: PropSpec) -> Result<Property<T>, BuildError>;
    pub fn settable<T: HomieValue + Send + Sync>(&mut self, node: NodeIdx, id: &str, spec: PropSpec) -> Result<Settable<T>, BuildError>;
    pub fn event<T: HomieValue + Send + Sync>(&mut self, node: NodeIdx, id: &str, spec: PropSpec) -> Result<Event<T>, BuildError>;
}
#[non_exhaustive]
pub enum AttachError { Build(BuildError), Config(ConfigError) }
pub struct AttachedDevice { pub handle: DeviceHandle, pub last_will: LastWill, pub inbox: Inbox,
    pub commands: Commands, pub driver: DeviceDriver }
pub struct AttachedPair { pub device: DeviceHandle, pub controller: ControllerHandle, pub events: EventReceiver,
    pub last_will: LastWill, pub inbox: Inbox, pub commands: Commands, pub driver: PairDriver }
pub fn attach_device(tree: TreeBuilder, opts: DeviceOptions) -> Result<AttachedDevice, BuildError>;
pub fn attach_pair(tree: TreeBuilder, config: Config, opts: DeviceOptions) -> Result<AttachedPair, AttachError>; // config.link_up_discards_queued := opts'
impl DeviceDriver { pub async fn run(self) -> Result<(), DriverError>; } // Send; also PairDriver

// ===== ebus-rumqttc =====
pub fn device(mqtt: rumqttc::MqttOptions, tree: TreeBuilder, opts: DeviceOptions) -> Result<RunningDevice, BuildError>;
pub fn pair(mqtt: rumqttc::MqttOptions, tree: TreeBuilder, config: Config, opts: DeviceOptions) -> Result<RunningPair, AttachError>;
pub struct RunningDevice { pub handle: DeviceHandle, pub task: DeviceTask }
pub struct RunningPair { pub device: DeviceHandle, pub controller: ControllerHandle, pub events: EventReceiver, pub task: PairTask }
impl DeviceTask { pub async fn run(self) -> Result<(), Error>; } // Send; also PairTask
```

A typical service:

```rust
let mut b = TreeBuilder::new(TreeConfig::default(), "panel-1", DeviceSpec::typed("energy.ebus.device.distribution-enclosure"))?;
let c3 = b.child(b.root(), "circuit-3", DeviceSpec::typed("energy.ebus.device.circuit"))?;
let power = b.node(c3, "power", NodeSpec::typed("energy.ebus.capability.power"))?;
let watts: Property<f64> = b.property(power, "active-power", PropSpec::new().unit("W"))?;
let switch = b.node(c3, "switch", NodeSpec::typed("energy.ebus.capability.switch"))?;
let mut relay: Settable<bool> = b.settable(switch, "relay", PropSpec::new())?;
relay.set(false)?;

let mqtt = rumqttc::MqttOptions::new("panel-1", "localhost", 1883);
let run = ebus_rumqttc::device(mqtt, b, DeviceOptions::default())?;
tokio::spawn(run.task.run());

watts.set(1234.5)?; // any thread; an unchanged value costs one compare
while let Some(req) = relay.next_set().await {
    if drive_relay(*req.value()).await.is_ok() { req.accept() } else { req.reject("relayFault") }
}
```

Handles work before the task runs: a value set before the first link-up is stored and goes out in the first refresh.

### Values and the unchanged-value gate

Each slot holds its preformatted value topic (and `$target` topic), its datatype, width and precomputed bounds, a fingerprint of the last value given to it, and that value's wire payload in a reused `Vec<u8>`.

- `Property::set` validates on the caller's thread (finite, min/max, enum and color membership) with the slot's `ValueCheck`, which the driver refreshes after each commit, then calls `watch::Sender::send_if_modified` with `HomieValue::same`. An unchanged value notifies no observer and wakes nothing. A changed one marks the slot in a `DirtySet` (one per 256 slot indices, added as the tree grows), and only a clear-to-set transition wakes the driver.
- On a target-mode slot every update (`set`, `progress`, `set_target`, `clear`, `accept`) instead goes to the driver in the order made, through the handles' request channel, since coalescing would merge a `progress` into a closing `set`. The core's gate still decides what is published. The handles record under the watch lock whether the last update left the slot settled (value and `$target` equal, no transition open) or cleared, so a `set` of the settled value, or a `clear` of a cleared slot, sends nothing and wakes nothing.
- Requests from the handles are applied before a link-up is handled, so a value set before the driver runs is in the first refresh.
- The driver drains dirty slots in batches of 256, reads each watch's latest value and calls `Tree::set_value`. Writes between drains coalesce. Values that come back (A, B, A) before a drain publish nothing.
- The core compares fingerprints before encoding. Floats compare by `to_bits` at their own width: shortest round-trip digits are injective per width (pyfmt.rs:127-134), so equal bits mean an equal payload, and `-0.0` and `0.0` stay distinct. Integers, booleans and enum indices compare by value; strings, colors, datetimes and json compare the encoded bytes with the stored payload. A retained value equal to the last one publishes nothing (homie.py:1377); a `retained: false` property publishes every `emit`, non-retained at QoS 0 (convention.md:50).
- The whole-tree refresh after each link-up is the one forced path (homie.py:1299-1310, python-sdk's `force=True`): it republishes every cached payload so a broker whose retained store is empty is repopulated.
- A narrow integer type gets an implied range `$format` (`u8` is `0:255`), intersected with a declared one, so `/set` validation enforces it.
- Floats use `write_float` (`f64`) or `write_float32` (`f32`): the shortest digits that read back as the same value at the value's own width, in python-sdk's layout.
- An enum's fingerprint is its index. After `set_format` on an enum slot holding a value, the next value compares by payload, since an index may name another value; a typed handle hands its value to the core again at that commit.
- A handle whose property was removed, whose tree was never attached, or whose driver ended or was dropped returns `Gone`.
- `clear` publishes a zero-length retained payload and resets the fingerprint; a value never published publishes nothing (homie.py:1334-1336). On a target-mode slot it then clears `$target` and closes any open transition.

### Lifecycle and publish order

Nothing is published before the first `Link::Up`. Every `Up`, the first included, runs one refresh, generated lazily from the slots as `poll_command` is called:

1. One SUBSCRIBE: `{d}/5/{dev}/+/+/set` for every device in the tree, whether or not it has a settable property, `{d}/5/{root}/$state` with `watch_own_state`, and `{d}/5/$broadcast/#` with `broadcast`, at `cfg.qos`.
2. Tombstones under devices in the tree: zero-length clears of those topics in the tombstone set (below).
3. Devices in pre-order, root first: `init` when the device's description differs from the last one published (the memo starts empty, so the first refresh sends `init` for every device), then `$description` (cached bytes), each target-mode slot's `$target` before its value (convention.md:466-468), every retained value, and every alert.
4. `$state` for each device in post-order, root last: `ready`, `sleeping`, the root's `lost` after `declare_lost`, or a child's `disconnected` or `lost`. With `receipts`, no `$state` is polled until the SUBACK of step 1 is reported.
5. Tombstones under devices no longer in the tree, so a child removed while down is cleared after its parent's new description and state, as while up (convention.md:633-641).

The order follows python's (homie.py:3055-3074): `init`, `$description`, values, alerts, then states with the root last. It differs in two ways: python publishes each device's `$state` right after its own subtree (homie.py:3063-3074), where this design publishes every description and value first and then every `$state`; and python sends `init` only for a device that is `ready` or `sleeping` and whose description changed (homie.py:3055-3056), where this design sends it for any changed description in any state. `ready` follows everything it covers and the `/set` SUBSCRIBE (convention.md:279). With `receipts` (the bundled path), it follows the SUBACK of the `/set` filters: the states wait for a `Receipt::Subscribed` matching the refresh's SUBSCRIBE, and one with `granted: false` is retried as a failed SUBSCRIBE (1-30 s backoff) while the states keep waiting. Without `receipts` (a shared connection whose host does not report SUBACKs), the states follow the SUBSCRIBE directly, relying on the broker processing one connection's packets in order, as python-sdk does (homie.py:1648-1656). After the will, the children read as `lost` through the root until the root's final `$state` (convention.md:262-267). `$description` precedes the values, so a controller checks each value on arrival without buffering early values. `session_present` changes nothing: the refresh always runs. A message while down counts as `Up{session_present: false}` (`reason=implicitLinkUp`). The driver detects such a message before routing it and delivers the `Up` itself, recording the stale-command boundary there; the core's own check covers a caller driving `Tree` directly.

On `Link::Down` the FIFO is emptied; the next refresh regenerates everything from the slots and the tombstone set. The driver stops draining dirty slots while down and drains them all into the core before handling the next `Up`, so the refresh carries the latest values and each goes out once. `emit` while down returns `Dropped` (QoS 0 is now-or-never, convention.md:695). The driver also discards the events still queued for the core on `Down` and before the next `Up`, so an event emitted on one connection is never published on another.

Every zero-length clear the core issues (a removal while down or up, `clear`, `clear_alert`) enters the tombstone set, and every refresh resends the set. A tombstone leaves the set when its topic is published again or, with `receipts`, when the broker has acknowledged (`Receipt::Acknowledged{cleared: true}`) every clear of that topic polled on the current link. Without `receipts` the set shrinks only when a topic is published again. The set holds at most `max_tombstones` (4096) topics; past it the oldest leaves, logged as a warning with `reason=tombstonesOverflow`, and is no longer resent. A clear lost with a dropped connection, or discarded by a client that empties its queue on a clean-session CONNACK (rumqttc 0.25 `eventloop.rs:127-142, 160-163`), is therefore resent by the next refresh. With `receipts`, the description memo advances, in poll order, as each `$description` publish is reported written (`Receipt::Written`); without, when it is polled. Either way, a `Down` reported before a device's closing `$state` is polled resets that device's memo, so the next refresh wraps it in `init` again.

Steady state, while up:

| Input | Output |
|---|---|
| `set` to an equal value, `set_presence` to the current one, `raise_alert` with the same message | nothing |
| `set` to a new value | the value, retained, at `cfg.qos` |
| `emit` | the value, non-retained, QoS 0, every call |
| `set_presence(Sleeping)` | `sleeping` (convention.md:282-283) |
| `raise_alert` with an id that is not a valid id | `Err(InvalidId)`, nothing published (convention.md:519) |
| `raise_alert` with an empty message | `Err(EmptyMessage)`, nothing published: a zero-length payload deletes the alert (convention.md:519, 527) |
| `declare_lost` | the root's retained `lost`; while down, the root's state still becomes `lost` and the next refresh publishes it (homie.py:2538-2541, 2563-2564); `set_presence(root, Ready)` returns the root to `ready` |

Reconfiguration is one commit. The core rebuilds each touched device's `DeviceDescription`, validates it by round trip (`to_json`, then `parse_with_ignored` must ignore nothing, description.rs:354, 702), and computes `content_hash` (description.rs:432) only then. An unchanged hash publishes nothing. A description over `max_description_bytes` is refused and the commit rolled back. A device, node or property given no name is described with its id as its name, as python-sdk writes it (homie.py:917-919, 2174). Otherwise, while up:

| Change | Sequence |
|---|---|
| Property or node added or removed | `init` when the device is `ready` or `sleeping` (convention.md:207), `$description`, the new values, zero-length clears of removed values and `$target`s (convention.md:613-614), then the prior state |
| Children added | One SUBSCRIBE of each new child's `/set` filter; each child `init`, description, values, state, the states waiting for that SUBACK with `receipts`; then the parent `init`, description, prior state, once per commit (convention.md:616, 620-629) |
| Children removed | Parent `init`, description, prior state; then each removed subtree leaves first, `$state` cleared first, then values, `$target`s, alerts and `$description` (convention.md:287-289, 635-641), then one UNSUBSCRIBE of the removed devices' `/set` filters |

While down, a commit changes the model only; the next refresh sends `init` for every changed device, the clears as tombstones (a removed device's after the states), and the `/set` filters of the devices then in the tree.

Shutdown:

- `Announce`: `disconnected` for every device, post-order, root last (convention.md:280-281; homie.py:2473-2498), then DISCONNECT.
- `Silent`: nothing published and no DISCONNECT, so the broker fires the will (homie.py:2467-2472).
- `Remove`: the removal sequence for the whole tree, every topic cleared with the root's `$state` first, then the flush `Announce` uses, then DISCONNECT. Without a clean DISCONNECT the will would publish `lost` and re-create the root (convention.md:272; homie.py:2448-2451, 2459-2466).

A shutdown releases every SUBACK gate, since no retry runs after it: states a refresh held for a SUBACK go out with the shutdown's own. The driver first hands the dirty slots and queued events to the core, then stops once the core has nothing left to send, or at once when the link is down; `Commands::ending` then gives the mode. A `PairDriver` stops with its device; `ControllerHandle::shutdown` on a pair unsubscribes the controller's filters and leaves the device running.

### `/set`

One `{d}/5/{dev}/+/+/set` filter for every device, settable properties or not, at the tree QoS, held while the device is in the tree: adding or removing settability, the last settable included, needs no SUBSCRIBE or UNSUBSCRIBE, and `/set` for a non-settable property is rejected locally (convention.md:279, 481). A `$broadcast` topic is matched first, so a broadcast subtopic may end in `set` (convention.md:533). Routing is a suffix check, borrowed `topic::parse` (topic.rs:130), a device lookup and a binary search over the device's sorted (node, property) table, and allocates nothing. Parsing calls `parse_set_payload_with` with the slot's cached `NumericFormat`.

| Case | Result |
|---|---|
| `retain == Some(true)` | ignored, `reason=propertySetRetainedIgnored` (homie.py:1563-1567; convention.md:48); `retain: None` is processed |
| unknown device or property | ignored at debug |
| property not settable | ignored, `reason=propertySetNotSettable` |
| zero-length, not UTF-8, or `parse_set_payload_with(text, datatype, format, &numeric, current)` fails | `reason=propertySetRejected,problem=…`; nothing published |
| settable's queue full | dropped, `reason=setDropped` |
| otherwise | `DeviceEvent::Set`, delivered to the property's `next_set` |

A `/set` refused with `propertySetNotSettable` or `propertySetRejected` is logged, as python-sdk logs it (homie.py:1587-1591), and counted in a counter read through `Tree::rejected_sets` and `DeviceHandle::rejected_sets`; it produces no event and wakes nothing.

`parse_set_payload` applies step rounding from min, then max, then the current value, and checks min/max after rounding (convention.md:399-413); `current` comes from the slot. Every accepted `/set` is delivered, including one equal to the current value, and in any device state.

Reflection (convention.md:485-486): without `$target`, `accept()` sets the property to the request's value. When that equals the current value, the gate publishes nothing and the retained value already reflects the command; a controller does not retry on a different value (convention.md:474-475). With `$target` (opt-in per property, used for every update, convention.md:333): `accept()` publishes the received bytes to `$target` byte for byte (convention.md:470); `progress(v)` publishes the value only, or, while no `$target` is held (none set yet, or cleared), `$target` and then the value, as `set(v)` does; `set(v)` closes the transition: when `v` differs from the open target, or no transition is open, it publishes `$target` and then the value; otherwise the value only. Every value update on a target-mode slot goes through this rule, from `Settable::set`, `Property::set` (including the handle from `Settable::property()` and a non-settable property built with `PropSpec::target`) or `Tree::set_value`, so the value ends matching `$target` (convention.md:333, 466-468). `clear` clears the value, then `$target`. `set_target(v)` opens a transition from a local command. A dropped `SetRequest` publishes nothing.

### Own `$state`

`watch_own_state` is off by default until a session takeover has been reproduced against mosquitto. With it, a `lost` payload on the root's `$state` is answered with one publish of the root's current state and `StateReasserted` when all of these hold: the link is up, this link-up's closing root `$state` has been polled, the root's current state is not `lost`, and the message is not retained (`retain == Some(false)`, or `None`). `lost` is the only payload a will writes, and the root publishes it only after `declare_lost`, when its state is `lost`; so the echoes of the device's own `init`, `ready`, `sleeping` and `disconnected` (MQTT 3.1.1 has no no-local option) are ignored, and so is everything received before the refresh completes. On a `retain: None` connection, the retained `lost` left by the will is ignored when it arrives during the refresh; arriving later, it draws one re-publish of the state the refresh already published. A will left by a replaced session can land after the new session's `ready`. After 3 re-asserts in one link-up the device stops, reports `gave_up: true` and logs `reason=stateOverwritten`, so two processes with the same root id do not fight forever.

### Sharing a connection with a controller

A device gets its own MQTT connection through `ebus_rumqttc::device()` by default. A host's connection is supported only when the host installs the device's will and leaves republishing after a reconnect to the SDK (the contract below).

`Pair<Tree, Controller>` runs both roles over one link, driven by `PairDriver`:

- Link: `Up` goes to the device, then the controller; the device's commands drain before the controller's, so the refresh precedes the controller's resubscribe. The driver records the stale-command boundary once for the pair. A message while down is an implicit link-up for both roles: `PairDriver` delivers `Up{session_present: false}` to the device and then the controller, and records the boundary, before routing the message, so neither role comes up alone.
- Messages go to every role whose `wants` is true. The device wants its `/set` filters, `$broadcast` and its root `$state`; the controller already returns `Ignored` for `/set` and `$broadcast` (controller/inbound.rs:158-163). The inbox records a dropped `/set` (by its suffix, before admission) apart from other drops and logs it with `reason=setInboxDropped`; it is not routed to the controller. Other drop notices go to the controller only.
- Each role keeps its private `FilterLedger`. `Pair` removes from one role's UNSUBSCRIBE every filter the other role `holds` (`FilterLedger::count`), so `Controller::shutdown`, which releases every filter (ledger.rs:154-162, controller/mod.rs:590), leaves the device's `/set` and `$state` filters in place, and the role that releases a shared filter last unsubscribes it.
- Every receipt goes to both roles; each matches it against its own outstanding commands and ignores the rest.
- `handle_command_failed` goes to the role that `owns` the command. A failed value publish re-marks its slot so the latest value retries; a failed `$state`, `$description`, alert or tombstone is requeued; a failed event is dropped.
- The will is the device root's. One connection carries one will (convention.md:56-58); the controller needs none.

Inside `ebus-homie-tokio`, each command carries an origin tag. Under `link_up_discards_queued`, `Commands` also skips every device command queued before a clean link-up, since the refresh regenerates it from the slots and the tombstone set; a controller `/set` handed over before a drop is still sent after the link-up.

### Bring-your-own contract

`attach_device` and `attach_pair` return the `last_will` with the driver. The host:

1. Installs `last_will` (the root's `$state` = `lost`, retained, at the tree QoS) on the connection before CONNECT, as the connection's only will. A connection whose will is something else cannot host a device.
2. Forwards ConnAck, every Publish matching the device's filters with its retain bit (or `None`), and every connection loss to the `Inbox`.
3. Leaves subscribing `/set` and republishing after a reconnect to the SDK, and publishes nothing under the tree's topics.
4. Sends a client-held replay of an earlier session before anything handed over after the CONNACK. rumqttc 0.10 replays `EventLoop::pending` first (eventloop.rs:115-116), so a stale replay precedes the refresh and the refresh's latest values win; this removes the replay hazards of ebus-mqtt-client GH #20 and #21 (client.py:545-556) without draining the client.
5. Declares in `host_filters` any filter it subscribes that matches the device's `/set` topics.
6. Sizes its client's outgoing packet limit to at least `max_description_bytes` plus 1 KiB.
7. Reports executor failures through `command_failed`.
8. Optionally sets `TreeConfig::receipts` and reports, through `Inbox::receipt`, every SUBACK of a device SUBSCRIBE, every device `$description` publish written, and every broker acknowledgment of a device zero-length publish. Without them the device relies on in-order processing for `ready` and keeps tombstones until their topics return.
9. Ends the connection as each `Shutdown` mode needs, once `Commands::recv` returns `None` (the driver has stopped and every command it sent was received) and the host's client has delivered what it was handed: after `Announce` and `Remove`, a clean DISCONNECT, sent only after those publishes are acknowledged, since a lost connection lets the will overwrite `disconnected` with `lost` (convention.md:280-281) or re-create a removed root; after `Silent` or `abort()`, a drop without DISCONNECT, so the broker fires the will.

### Bundled path

`device()` and `pair()` configure the caller's `MqttOptions` before `AsyncClient::new`: the will from `last_will()` (rumqttc 0.25 `lib.rs:569`), a clean session, inflight capped at 20 (a lower value of the caller's is kept), since mosquitto's default receive quota of 20 discards excess QoS 2 messages from a 3.1.1 client (client.py:549-552), and an outgoing packet limit of `max_description_bytes` plus 1 KiB (`lib.rs:616`), since an oversized publish fails the connection and the refresh would resend it. The broker coordinates and the client id are the caller's (see [Broker discovery](#broker-discovery-planned)); the root id is the documented client id (homie.py:3379). `device()` and `pair()` set `TreeConfig::receipts` and `DeviceOptions::link_up_discards_queued`, and the connection reports through `Inbox::receipt` each SUBACK, each `$description` publish written (`Outgoing::Publish`), and each PUBACK or PUBCOMP of a zero-length retained publish; in a pair the controller ignores the receipts for its own SUBSCRIBEs. It matches rumqttc's packet ids in write order: a SUBSCRIBE and a tracked publish each keep one entry until written, and any other publish only advances a count. A connection error resets the tracking, since rumqttc discards every request of a clean session not yet acknowledged (eventloop.rs:127-142, 160-163), and drops every event rumqttc buffered for the lost connection except received publishes: rumqttc returns buffered events first (eventloop.rs:193) and never clears them, so an `Outgoing::Publish` queued before a failed write (state.rs:345) would otherwise be counted on the next connection. The executor moves `Command::Publish`'s topic and payload into `try_publish` and rebuilds the command from `ClientError::TryRequest` when it does not fit. For `Shutdown::Announce` and `Shutdown::Remove`, the executor counts the publishes it hands to `try_publish` and the poll loop counts the `Outgoing::Publish` events `poll` returns and records `EventLoop::state.inflight()` (rumqttc 0.25 `state.rs:132`) after each event; once `Commands::recv` returns `None`, the executor waits until every handed publish has been written and `inflight()` is zero, bounded by `flush_timeout_ms` and ended early by a lost link, and then sends DISCONNECT, so `poll` is never canceled mid-write. After `Shutdown::Silent` or `abort()`, the poll loop returns without DISCONNECT and drops the connection, so the broker publishes the will. `inflight()` alone is zero while publishes still wait in the request channel (`state.rs:333-334`, `eventloop.rs:78`), and rumqttc writes DISCONNECT without waiting for acks (`state.rs:449-456`).

### Backpressure and CPU levers

- Credit: the driver polls commands, and drains dirty slots and queued events into the core, only while commands sent minus commands received by the executor is below `max_outstanding` (1024); a pair's controller commands count against the same limit. `Commands` publishes its received count to an `AtomicU64` in `Shared`, and the first command received after the driver ran out wakes it through a `Notify`, which the driver always waits on, so a driver out of credit resumes even when the command arrives between its last credit check and its wait. Past it, slots stay dirty and coalesce, and the refresh waits, so a slow broker holds at most `max_outstanding` publishes in the unbounded command channel and none of them stale.
- An unchanged value: one compare under the watch lock, no wake, no encoding, no allocation.
- A changed value: no allocation to compare and encode (`write_float` into a reused buffer, copied into the slot's, which grows only for a payload longer than any it held and starts with room for any number), 2 to build the `Command` (topic and payload), none in the executor; rumqttc adds its topic clone (`client.rs:104`) and, at QoS 1 or 2, its inflight clone (`state.rs:333`).
- A refresh copies cached topics and payloads; it encodes nothing.
- `content_hash` and `to_json` run only at a reconfiguration commit; the serialized `$description` is cached.
- Inbound `/set` routing is borrowed and allocates nothing; each slot caches its parsed `NumericFormat` for `parse_set_payload_with`.
- rumqttc writes and flushes each request separately (`eventloop.rs:244-252`), a per-value floor outside the SDK.

### ESP32

`Tree`, `HomieValue`, `DirtySet`, `Role` and `Pair` are no_std + alloc. `ebus-homie-espidf` builds its handles on `DirtySet` and `std::sync` value cells and drives a `Tree` or a `Pair` from the loop described under ESP32. With `retain: None`, a retained `/set` cannot be told apart and is executed.

### Testing

| Layer | What |
|---|---|
| Core unit, virtual `Instant` | Each lifecycle sequence as an exact command list: boot, reconnect with and without a description change, `Down` mid-refresh and mid-reconfiguration (memo reset, tombstones kept), every reconfiguration row while ready, sleeping and down, child add and remove, each shutdown mode; the gate (`-0.0`/`0.0`, `f32` vs `f64`, enum index, A-B-A); events dropped while down; the `/set` table; `$target` before the value, a `set(v)` that differs from the open target, `Property::set` and `clear` on a target-mode slot, and a local `set_target`; alert id and empty-message refusal; `declare_lost` while down then `Up`; the first settable added and a child with settables added while up, `/set` SUBSCRIBE before `ready`; own-`$state` re-assert and its cap, with echoes of the device's own `init` and `ready` ignored, and off by default; with `receipts`, no `$state` before the `/set` SUBACK, a refused SUBACK retried with the states held, and without `receipts` the states right after the SUBSCRIBE; tombstones leaving on an acknowledgment of every clear polled, a clear-publish-clear sequence kept until the second acknowledgment, the cap of `max_tombstones` with the oldest dropped; the memo advancing on `Written` with `receipts` and on poll without; a rejected `/set` counted with no event; credit-limited `poll_command` with values changed mid-refresh; `progress` with no `$target` held; a `$broadcast` subtopic ending in `set`; a child removed while down cleared after its parent; an enum compared by payload after `set_format` |
| `Pair` unit | Command order on `Up`; `owns` routing; receipts reaching both roles; UNSUBSCRIBE filtering, including a controller shutdown while the device holds the same `$state` filter; drop notices to the controller only, a dropped `/set` logged; a message while down brings both roles up |
| Driver unit, paused tokio clock | Values set before the driver runs in the first refresh; an unchanged `set` notifies no watcher and publishes nothing; A-B-A before a drain; values set while down out once in the refresh; caller-thread refusals; events every call, `Down` and `Full`; `/set` delivery, `accept`, `accept_as`, `reject`, a full set queue, refused sets counted; target mode through the handles; credit never exceeded, with values changed while out of credit sent at their latest; dirty sets growing past 256 slots; presence, alerts and the state watch; reconfiguration handles live after a commit and `Gone` after removal or a refused commit; `Announce`, `abort` and shutdown while down with `Commands::ending`; implicit link-up; stale device commands skipped; `ready` after the SUBACK receipt; a failed publish retried at the latest value; `$broadcast`; a pair's command order, controller shutdown, implicit link-up, failure routing, and a dropped `/set` not routed to the controller; an event queued before a drop not sent after it; a stored credit permit waking the driver; an unchanged target-mode `set` or `clear` sending nothing; `slot()` ids for core changes; handles `Gone` after a refused attach or a dropped driver; an enum handle re-applied after `set_format` |
| Connection unit | SUBACK, written `$description` and acknowledged clear receipts in write order; a refused SUBACK; the flush condition; a publish that does not fit comes back whole; the will, clean session and inflight cap; `abort` and `Announce` end the task while the broker is down; a publish whose write failed with its connection not counted on the next |
| Vectors | `write_float`/`write_float32` against every float vector; the `/set` verdict suite through `Tree::handle_message` |
| Panel-sim corpus | Every vendored capture rebuilt as a `Tree` from its descriptions and values and published to mosquitto: the retained topics equal the capture's, each `$description` as a parsed document without `version`, which each publisher computes, and each value byte for byte, except a `float` the capture wrote in integer layout (`0` is published `0.0`) and the key order of a `json` object, which a `serde_json::Value` sorts |
| Property | Random set, clear, reconfigure and link schedules: after each completed refresh the retained topic set equals the model, and no unchanged retained value is published outside a refresh |
| Allocation count | The `device_bench` example counts allocations when a host binary passes it a counting global allocator, which needs `unsafe` and so stays outside this workspace: an unchanged `set_value` allocates nothing; a changed `f64` allocates 2, to build its `Command`. Planned: the same end to end through the tokio handles |
| mosquitto e2e | The rust controller syncs a rust device tree (`DeviceSynced{missing: 0}`), sees a value change and runs a `/set`; a python-sdk controller reads the tree (`ready`, each `$description`, every value) and sends `/set`: valid, step-rounded, two refused payloads and a non-settable property, the refusals counted and nothing republished; broker restart without persistence republishes the same retained set; unchanged values publish nothing, counted at a proxy; no retained topic under the root after `Remove`; `Announce` publishes `disconnected`, child first; a task dropped without shutdown leaves the will's `lost`; `pair()` on one connection, whose device keeps its `/set` filter once the broker has acknowledged the controller's UNSUBSCRIBE. Planned: byte-exact `$target` through python-sdk; kill the socket, then `lost`, then `ready` after reconnect; a QoS 2 refresh of 20 devices by 300 properties under `max_inflight_messages 20`; a description over 10 KiB; a session takeover with a stale `lost`; bring-your-own through the rumqttc 0.10 dev-dependency |
| Load | `device_bench` on the core: 20 devices by 150 `float` properties, every value set once per tick and 1 in 10 changed, timing `set_value` for unchanged and changed values and `poll_command` per changed value. Planned: 20 devices by 300 properties at 1 Hz through the tokio handles, against the Python process being replaced |

## Broker discovery (planned)

mDNS advertisement and broker discovery are host functions. The host advertises its `.local` hostname, `_ebus._tcp` listing every role it runs, `_device-info._tcp`, a broker it hosts itself, and its REST API (framework.md:76-79). `device()`, `pair()` and `controller()` connect where the caller's `MqttOptions` say; the host resolves its broker before building those options, and finds the same broker again when its address changes ([Addresses and host names](#addresses-and-host-names)).

### Use cases

A site (a home or a building) has one MQTT broker, and every eBus host at the site connects to it. A host finds that broker in one of three ways:

| Use case | What the host does | `mode` |
|---|---|---|
| Discover and pick | Lists the brokers advertised over mDNS and picks the first by the ranking in [Broker selection](#broker-selection) | `discovery-only` (the default) |
| Configured broker | Connects to the configured URL | `configured-only`, or unset with a URL |
| Find a particular broker | Uses mDNS to find the broker it wants: the one matching a configured URL's host or address (`match_configured`), falling back to the URL; or the one with a given instance name or device id (`wanted`) | `discovery-with-fallback` with a URL; `discovery-only` with `wanted` |

A host that runs its own broker connects to it directly, since broker discovery is required only of an entity that does not self-host one (framework.md:80).

Citations `ebus.py`, `_mdns_core.py`, `mdns.py`, `instance.py`, `record.py` and `resolver.py` are at [python-service-discovery](https://github.com/electrification-bus/python-service-discovery) `466cd6a`; `browser.py` at [python-mdns-discovery](https://github.com/electrification-bus/python-mdns-discovery) `4464074`; `txt_records.cpp` and `broker_discovery.h` at cpp-sdk `cc73353`; `network.cpp` at esp32-sdk `a35de16`; `service_daemon.rs` and `service_info.rs` at mdns-sd 0.21.5; `defs.h`, `dbus-protocol.c` and `socket.c` at [avahi](https://github.com/avahi/avahi) `3ed8102`; `eventloop.rs` and `tls.rs` at rumqttc 0.25.1.

### Shape

| Crate | Additions |
|---|---|
| `ebus-discovery` (core, sans-IO) | Service types and the broker preference order; TXT decoding, checks and building; `Identity`, `HttpService`, `BrokerService`, `Advertisement`; `BrokerEndpoint`, `BrokerMode`, `BrokerConfig`; `rank_brokers`, `match_configured`, `select_broker`; `InstanceTable` (merges backend sightings into instances) and the `BrokerSearch` state machine. Service types, `BrokerType`, `BrokerMode` and address scopes without `alloc`, the rest under `alloc`; optional `log`. Depends on `ebus-mqtt` for `Instant` |
| `ebus-discovery-tokio` (new) | The `Backend` trait; `Mdns`, the backend a host holds; `list_brokers`, `find_broker`; `advertise`, `Advertiser`. Features `avahi` and `mdns-sd`, `default = []` |

The backends live in a runtime crate because core crates perform no I/O and depend on no runtime ([Core rules](#core-rules)): Avahi is D-Bus I/O on tokio, and mdns-sd runs its own thread and sockets. They are two features of one crate because both implement one seam and a host or example picks between them at run time. `ebus-discovery-tokio` is published apart from `ebus-homie-tokio` for the reason `ebus-rumqttc` is: feature unification would otherwise compile zbus or mdns-sd into every service of a host workspace that shares the crate.

| Backend | Feature | Hosts | Dependency |
|---|---|---|---|
| Avahi | `avahi` | Linux with avahi-daemon | `zbus` 5.13 with `default-features = false, features = ["tokio"]`, under `[target.'cfg(target_os = "linux")'.dependencies]`, with hand-written proxies for `org.freedesktop.Avahi`; no C library. zbus 5.13 declares `rust-version` 1.85 and 5.14 onward 1.87, so `avahi` keeps the 1.85 floor only where the consumer's workspace resolves with an MSRV-aware resolver (resolver 3, as in [Cargo.toml](../Cargo.toml)) against a `rust-version` of 1.86 or lower |
| mdns-sd | `mdns-sd` (`dep:mdns-sd`) | Linux without Avahi; macOS development machines | `mdns-sd` 0.21.4 or later (`rust-version` 1.71, `#![forbid(unsafe_code)]`); 0.21.4 keeps re-querying unresolved instances while browsing (mdns-sd #494) |

### Public API

```rust
// ===== ebus-discovery (no alloc) =====
pub enum BrokerType { SecureMqtt, MqttWss, MqttWs, Mqtt } // preference order, framework.md:330-335
impl BrokerType {
    pub fn as_str(self) -> &'static str;          // "_secure-mqtt._tcp", ...
    pub fn parse(service_type: &str) -> Option<Self>;
    pub fn default_port(self) -> u16;             // 8883, 9002, 9001, 1883 (ebus.py:57-62)
    pub fn uses_tls(self) -> bool;                // SecureMqtt, MqttWss (ebus.py:64-69)
}
pub const TCP_BROKER_TYPES: [BrokerType; 2];      // SecureMqtt, Mqtt (ebus.py:54)
pub enum BrokerMode { ConfiguredOnly, DiscoveryWithFallback, DiscoveryOnly } // parse(): ebus.py:110-127
pub enum AddrScope { Global, Private, UniqueLocal, Other, LinkLocal, Loopback, Unspecified } // record.py:51-61, 93-108
pub struct ScopedAddr { pub ip: core::net::IpAddr, pub ifindex: Option<u32> }

// ===== ebus-discovery (alloc) =====
pub struct TxtRecord { /* ordered (key, Option<Vec<u8>>) */ } // from_strings, get, to_strings
pub fn check_txt(service_type: &str, txt: &TxtRecord) -> Result<TxtReport, TxtError>;
pub fn parse_ebus_txt(txt: &TxtRecord) -> EbusTxt;   // missing(), is_broker_host()
pub struct Identity { /* device_ids, roles, manufacturer, model, serial_number, ... */ }
pub struct HttpService { /* tls, port, path, version, openapi */ }
pub struct BrokerService { /* broker_type, port, protocol, broker, path, subprotocol */ }
pub struct Advertisement { /* identity, instance, ebus_port, device_info_port, http, brokers */ }
pub struct ServiceRecord { pub service_type: &'static str, pub port: u16, pub txt: TxtRecord }
impl Advertisement {
    /// Every record, all under one instance name; `server` is the SRV target.
    pub fn records(&self, server: &str) -> Result<Vec<ServiceRecord>, TxtError>;
    /// The instance name, then `-2` through `-99` (_mdns_core.py:937-939).
    pub fn instance_names(&self) -> impl Iterator<Item = String>;
}

pub struct InstanceKey { pub service_type: String, pub name: String }
pub enum Slot { Whole, Link { ifindex: u32, v6: bool } }
pub enum Sighting {
    Seen { key: InstanceKey, slot: Slot, server: String, port: u16, txt: TxtRecord, addrs: Vec<ScopedAddr> },
    Gone { key: InstanceKey, slot: Option<Slot> }, // None: every slot
}
pub struct Instance { pub key: InstanceKey, pub server: String, pub port: u16, pub txt: TxtRecord, pub addrs: Vec<ScopedAddr> }
pub struct InstanceTable { /* sans-IO */ }
impl InstanceTable {
    pub fn handle(&mut self, s: Sighting);
    pub fn instances(&self) -> impl Iterator<Item = Instance> + '_;
}

#[derive(Clone, PartialEq, Eq)]
pub struct BrokerEndpoint {
    pub broker_type: BrokerType,
    pub host: String,               // TXT `broker`, else the SRV target (ebus.py:599-614)
    pub port: u16,
    pub server: Option<String>,     // SRV target
    pub device_id: Option<String>,
    pub addrs: Vec<ScopedAddr>,     // candidate order
    pub instance: Option<String>,   // None for a configured broker
}
impl BrokerEndpoint {
    pub fn from_url(url: &str) -> Result<Self, UrlError>; // ebus.py:617-633
    pub fn url(&self) -> String;
    pub fn connect_host(&self) -> Cow<'_, str>;
}

pub enum Wanted { Instance(String), DeviceId(String) }
#[non_exhaustive]
pub struct BrokerConfig {
    pub mode: Option<BrokerMode>,         // None: see Configuration and precedence
    pub url: Option<BrokerEndpoint>,
    pub wanted: Option<Wanted>,
    pub tls: bool,                        // also implied by an mqtts:// URL
    pub accept: Option<Vec<BrokerType>>,  // None: [SecureMqtt] with TLS, else TCP_BROKER_TYPES
    pub allow_unmatched: bool,            // ebus.py:776-781
    pub settle_ms: u64,                   // 3_000
    pub fallback_after_ms: u64,           // 15_000
}
pub struct BrokerSearch { /* sans-IO, one search */ }
impl BrokerSearch {
    pub fn new(now: Instant, cfg: BrokerConfig) -> Result<Self, ConfigError>;
    pub fn browses(&self) -> bool;                     // false in configured-only
    pub fn handle(&mut self, now: Instant, s: Sighting);
    pub fn handle_timeout(&mut self, now: Instant);
    pub fn poll_requery(&mut self) -> bool;
    pub fn poll_timeout(&self) -> Option<Instant>;
    pub fn result(&self) -> Option<&BrokerEndpoint>;   // set once, then fixed
    pub fn candidates(&self) -> Vec<BrokerEndpoint>;   // ranked, accepted only
}

// ===== ebus-discovery-tokio =====
#[derive(Clone)]
pub struct Mdns { /* Avahi or mdns-sd */ }
impl Mdns {
    /// Avahi when built with `avahi`, on Linux, with org.freedesktop.Avahi reachable;
    /// else mdns-sd when built with `mdns-sd`; else Error::NoBackend.
    pub async fn system() -> Result<Mdns, Error>;
    #[cfg(all(feature = "avahi", target_os = "linux"))]
    pub async fn avahi() -> Result<Mdns, Error>;
    #[cfg(feature = "mdns-sd")] pub fn mdns_sd(opts: MdnsSdOptions) -> Result<Mdns, Error>; // port, loopback
}

/// Browses for `window` from the start and returns the accepted brokers, ranked;
/// reads `tls`, `accept` and `wanted` from `cfg`.
pub async fn list_brokers<B: Backend>(b: &B, cfg: &BrokerConfig, window: Duration) -> Result<Vec<BrokerEndpoint>, Error>;
/// Runs a `BrokerSearch` to its result; a caller bounds it with `tokio::time::timeout`.
pub async fn find_broker<B: Backend>(b: &B, cfg: BrokerConfig) -> Result<BrokerEndpoint, Error>;
/// The same broker looked up again by its instance name, with its current addresses;
/// `Ok(None)` when it is not seen within `window`, or for a configured broker.
pub async fn reresolve<B: Backend>(b: &B, broker: &BrokerEndpoint, cfg: &BrokerConfig, window: Duration) -> Result<Option<BrokerEndpoint>, Error>;

#[derive(Default)]
pub enum HostName { #[default] Auto, Own(String) } // Own: "<label>.local."
pub async fn advertise<B: Backend>(b: &B, ad: Advertisement, host: HostName) -> Result<Advertiser, Error>;
impl Advertiser {                      // Drop withdraws from a spawned task
    pub fn update(&self, ad: Advertisement) -> Result<(), Error>; // sends only what changed
    pub fn state(&self) -> watch::Receiver<AdState>; // Registering, Established { instance, server }, Failed
    pub async fn withdraw(self) -> Result<(), Error>;
}

pub trait Backend: Clone + Send + Sync + 'static {
    type Browse: Browse;
    type Group: Group;
    fn browse(&self, service_type: &str) -> impl Future<Output = Result<Self::Browse, Error>> + Send;
    fn server_name(&self, host: &HostName) -> impl Future<Output = Result<String, Error>> + Send;
    fn publish(&self, host: &HostName, instance: &str, records: &[ServiceRecord])
        -> impl Future<Output = Result<Self::Group, Error>> + Send;
}
// Browse: next() -> Option<Sighting>, requery(). Group: next_report() (Established, Collision,
// HostRenamed, Failed, Reset), update_txt(), withdraw().

// ===== ebus-rumqttc =====
/// Returns the broker's options at its current address, or `None` to keep the old ones.
pub type Reresolve = Box<dyn FnMut() -> Pin<Box<dyn Future<Output = Option<MqttOptions>> + Send>> + Send>;
impl ControllerTask { pub fn reresolve(self, f: Reresolve) -> Self; } // also DeviceTask, PairTask
```

The trait methods return `impl Future + Send` (Rust 1.75) rather than using `async fn`, which on a public trait draws rustc's `async_fn_in_trait` warning, and CI denies warnings. `Mdns` implements `Backend` by dispatching to the backend compiled in, so a host picks one at run time without `dyn`.

A service that discovers its broker:

```rust
let mdns = Mdns::system().await?;
let mut cfg = BrokerConfig::default();
cfg.mode = mode.as_deref().map(BrokerMode::parse).transpose()?;
cfg.url = url.as_deref().map(BrokerEndpoint::from_url).transpose()?;
let broker = find_broker(&mdns, cfg.clone()).await?;
let options = options_for(&broker, &creds); // client id, credentials, TLS
let run = ebus_rumqttc::controller(options, Config::wildcard(), AttachOptions::default())?;
let task = run.task.reresolve(Box::new(move || {
    let (m, c, b, creds) = (mdns.clone(), cfg.clone(), broker.clone(), creds.clone());
    Box::pin(async move {
        let found = reresolve(&m, &b, &c, Duration::from_secs(3)).await.ok().flatten()?;
        Some(options_for(&found, &creds))
    })
}));
tokio::spawn(task.run());
```

A host that also runs a broker and a REST API advertises once at startup:

```rust
let ad = Advertisement::new(identity)
    .http(HttpService::https(443, "/api/v1"))
    .broker(BrokerService::new(BrokerType::SecureMqtt, 8883));
let advertiser = advertise(&mdns, ad, HostName::Auto).await?;
```

### TXT records

| Step | Rule |
|---|---|
| Input | Each string splits at its first `=`; a string without `=` is a key with no value; a string with an empty key is skipped |
| Keys | Lowercased (ebus.py:136-153) |
| Duplicates | The first occurrence wins, compared case-insensitively, as in RFC 6763 section 6.4, cpp-sdk `txt_find` (txt_records.cpp:34-40) and mdns-sd `decode_txt_unique` (service_info.rs:982-993); Avahi hands every string, so `TxtRecord` drops later duplicates itself. python keeps the last of keys that differ only in case (ebus.py:144-152) |
| Values | Kept as bytes; `get` decodes UTF-8 with replacement, and a key with no value reads as `""` (ebus.py:136-153) |
| Lists | `roles`, `device_id` and `auth_methods` split at commas, items trimmed, empty items dropped (ebus.py:156-159, 202-216) |
| Missing keys | Listed by `EbusTxt::missing`, never a reason to drop a record (ebus.py:189-192) |
| `protocol` | A value other than `mqtt-v5` or `mqtt-v3.1.1` is kept |
| Building | Keys in python's order: `Identity` (ebus.py:400-438), `HttpService` (ebus.py:465-477), `BrokerService`: `txtvers` and `protocol`, then `broker` and `device_id` for `_secure-mqtt._tcp`, `path` and `subprotocol` for the WebSocket types (ebus.py:534-546; framework.md:301-326). `broker` defaults to the SRV target without its trailing dot. `ebus_version` is the `framework` value in [.ebus-spec.json](../.ebus-spec.json). `roles` is kept as given; with `brokers` not empty and no `broker-host` role (framework.md:106), building logs `reason=brokersWithoutBrokerHostRole`, as python does (_mdns_core.py:1062-1063) |
| Checks | `check_txt` runs on every built record: an empty key, a key with `=`, a key that is not printable ASCII, or a `key=value` string over 255 bytes is an error; `TxtReport` lists strings over 200 bytes and a record over 1300 bytes, logged as warnings (ebus.py:282-311) |

### Merging sightings

The two stacks report differently, and `InstanceTable` turns both into one set of instances keyed by (service type, instance name):

- Avahi resolves each `ItemNew` per (interface, protocol), and each `ServiceResolver.Found` carries one address. The backend sends `Seen` with `Slot::Link { ifindex, v6 }` and that address, and `ItemRemove` sends `Gone` with the same slot.
- mdns-sd's `ServiceResolved` carries the whole address set, sent as `Slot::Whole`; `ServiceRemoved` sends `Gone` for every slot.
- A slot holds the addresses of its latest report, so an address changed in place replaces the old one, as python-mdns-discovery assigns per (interface, protocol) (browser.py:135-145). An instance's addresses are the union of its slots; its server, port and TXT are the latest reported. An instance leaves the table with its last slot.

### Broker selection

`BrokerSearch` is python-service-discovery's sans-IO `BrokerSearch` (_mdns_core.py:216-295) with time made explicit, and `select_broker` and `rank_brokers` are python's functions.

1. Browse all four broker types, so a WebSocket broker is logged with `reason=brokerTransportNotAccepted` (_mdns_core.py:259-260, 272-278).
2. Turn each instance into a `BrokerEndpoint` (ebus.py:599-614): `host` is the TXT `broker` value, trimmed and with one trailing dot removed, else the SRV target; port 0 becomes the type's default port. `from_url` accepts only `mqtt://` and `mqtts://`, with ports 1883 and 8883 by default, and rejects a URL with no host (ebus.py:617-633).
3. Keep the accept set: `_secure-mqtt._tcp` alone when TLS is configured, else `TCP_BROKER_TYPES` (_mdns_core.py:247-253). A TLS configuration therefore never selects a plain broker, and its credentials stay off a plaintext link. ebus-rumqttc has no WebSocket transport, so the WebSocket types are never in the default set.
4. With `wanted`, keep only the brokers whose instance name, or one of whose TXT `device_id` items, equals it. This filter is a rust-sdk rule ([Configuration and precedence](#configuration-and-precedence)).
5. Rank (`rank_brokers`, ebus.py:675-702): sort by preference, then lowercased host, then port. Endpoints sharing a normalized host, SRV target or `device_id` are one broker, which keeps its most-preferred transport (ebus.py:661-672), so a host advertising both `_secure-mqtt._tcp` and `_mqtt._tcp` is one candidate. `list_brokers` returns this list.
6. Select (`select_broker`, ebus.py:745-795):

| Mode | Result |
|---|---|
| `configured-only` | The configured broker, at once; no browse runs. `new` fails without one (_mdns_core.py:257-258) |
| `discovery-only` | The first ranked broker |
| `discovery-with-fallback`, URL configured | The discovered broker `match_configured` pairs with the URL; an unmatched broker only with `allow_unmatched`. With no match `fallback_after_ms` after the search starts, the configured broker |
| `discovery-with-fallback`, no URL | The first ranked broker |

`match_configured` matches when the configured host equals the TXT `broker` name or the SRV target (case-insensitive, trailing dot ignored), or when a configured IP equals an advertised address; a match by address keeps the configured IP as host on a plain transport and the discovered name on TLS, and every match uses the discovered port and transport (ebus.py:712-742). The spec leaves choosing among several brokers open and forbids consumers from assuming a rule (framework.md:337); the first-ranked rule is python's, so without `wanted` every SDK picks the same broker from the same advertisements.

The result is set `settle_ms` (3 s, python's browse timeout, mdns.py:47) after the first accepted broker is seen, so each broker's every transport has been heard and an early `_mqtt._tcp` answer cannot win over the same broker's `_secure-mqtt._tcp`. `fallback_after_ms` is 15 s: python falls back after three 3 s browses separated by 3 s waits (_mdns_core.py:254-256, 288-294; ebus.py:814-818). While no broker is found, `poll_requery` asks for a requery after 3 s, 3 s, then every 30 s, python's schedule (ebus.py:801-825), which meets "SHOULD retry periodically" (framework.md:337). `find_broker` stops browsing once the result is set.

With `log`, the search logs python's reason strings: `brokerDiscovered`, `brokerNotFound`, `brokerFallbackToConfigured`, `brokerTransportNotAccepted`, `brokerChosenAmongSeveral`, `brokerNotChosen` and `configuredBrokerNotDiscovered` (ebus.py:782-794; _mdns_core.py:274-293).

### Configuration and precedence

| `mode` | URL | Effective mode |
|---|---|---|
| set | any | `mode`; `configured-only` without a URL is a `ConfigError` |
| unset | set | `configured-only` |
| unset | unset | `discovery-only` (framework.md:102, 622) |

With `mode` unset, a configured URL is used as configured (`configured-only`). `wanted` applies in `discovery-only` and in `discovery-with-fallback` without a URL; with a URL, `new` returns a `ConfigError`. framework.md:622 defines the default only for the case without a URL; python uses `discovery-only` whenever the mode is unset (ebus.py:130), which would ignore a configured URL. `BrokerMode::parse` accepts the spec's spellings case-insensitively and with `_` for `-`, and rejects other values (ebus.py:110-127). The host builds `BrokerConfig` from its baked-in defaults overridden by its REST "MQTT broker configuration" (framework.md:93, 607).

### Addresses and host names

- The host builds complete `MqttOptions` from the endpoint, with `connect_host()` and `port` as the broker address and its own client id, credentials and TLS settings. rumqttc resolves that address with tokio `lookup_host` on every connect (`eventloop.rs:150-153, 313, 325, 400, 407`), so a host name follows a change of the broker's address across reconnects.
- `connect_host()` is `host` when that is not a `.local` name, else the first IPv4 candidate address, else `host`, for TLS and plain transports alike. The candidate address keeps a connection to a discovered `.local` broker independent of the system resolver. rumqttc uses the same string as the TLS server name (rustls `ServerName::try_from`, which accepts an IP address, `tls.rs:176`; native-tls, `tls.rs:184`), so the broker's certificate carries its `.local` name, its addresses and any FQDN in its SAN, as broker-quickstart's does ([laptop-quickstart.md:29](https://github.com/electrification-bus/broker-quickstart/blob/main/docs/laptop-quickstart.md)). A configured URL has no candidate addresses (ebus.py:633), so a configured `.local` broker connects by name. rumqttc joins host and port as `format!("{domain}:{port}")` (`eventloop.rs:400, 407`), so IPv6 addresses wait for an end-to-end test that shows one connecting.
- A site keeps one broker, but its host can get a new address from DHCP. A discovered `.local` broker reached by address is then found again under its instance name: after the first failed connect that follows a lost link, and after every third failed connect after that, `ebus-rumqttc` calls the task's `Reresolve`, and a returned `MqttOptions` replaces the event loop's before the next attempt (`EventLoop::mqtt_options` is public, `eventloop.rs:74`; `MqttOptions` has no setter for the host alone, `lib.rs:459`). It applies the same will, clean session, inflight cap and packet sizes to the replacement as at startup. The broker and its topics stay the same, so the device and controller handle it as any other reconnect. cpp-sdk and esp32-sdk look the broker's host name up again after every third failed reconnect ([broker_discovery.h:57-69](https://github.com/electrification-bus/cpp-sdk/blob/main/discovery/include/ebus/discovery/broker_discovery.h); esp32-sdk `network.cpp:169-190`). A broker connected by a host name, and a configured broker, need no `Reresolve`. Over TLS the new address verifies only when the broker's certificate lists it; broker-quickstart mints a new certificate when its host gains an address ([laptop-quickstart.md:224](https://github.com/electrification-bus/broker-quickstart/blob/main/docs/laptop-quickstart.md)).
- `candidate_addresses` drops loopback and unspecified addresses, keeps an IPv6 link-local address only with an `ifindex`, and orders global, private or ULA, other, link-local (instance.py:41-56; record.py:51-61, 93-108). Ties sort by address, so the order does not depend on the backend.
- Avahi's `Found` address has no zone, so the backend sets `ifindex` from the item's interface for an IPv6 link-local address; mdns-sd's `ScopedIp` carries it.
- Every host name comparison is case-insensitive and ignores one trailing dot (ebus.py:661-662; instance.py:18-20).

### Advertising

All records share one instance name: the explicit name, else the first device id when it fits one DNS label with a `-99` suffix (60 octets), else the SRV target's host label, logged with `reason=deviceIdTooLongForInstanceName` (_mdns_core.py:104, 1136-1152). A collision on any record withdraws them all and registers them again under the next name, `-2` through `-99`, after which `state` reports `Failed` (_mdns_core.py:937-991). `update` sends `UpdateTxt` only for records whose TXT changed, re-registers on a changed port or record set, and sends nothing for an equal advertisement. A host name that changes after a collision changes the SRV target and the default TXT `broker`, which consumers connect to first; the advertiser then reads the new server name, rebuilds the records with `Advertisement::records(server)` and registers them again.

| | Avahi | mdns-sd |
|---|---|---|
| Host name | `HostName::Auto` only (`Own` is an error): services are added with an empty host, so the SRV target is avahi-daemon's own name, read with `GetHostNameFqdn` for the `broker` TXT | mdns-sd announces a service only with addresses for a host name it publishes itself (service_daemon.rs:5088-5106). `HostName::Auto` is `<gethostname, first label>.local.` (_mdns_core.py:893-895) where no OS responder owns that name, and `<gethostname, first label>-ebus.local.` where one does: on macOS and Windows, and on Linux when an avahi-daemon socket exists (_mdns_core.py:870-883). python-zeroconf publishing addresses under the OS responder's name made mDNSResponder rename the host to `<name>-2.local` (_mdns_core.py:20-29) |
| Addresses | avahi-daemon's | `enable_addr_auto`, loopback interfaces disabled except in tests |
| Registration | One entry group with `AVAHI_PUBLISH_NO_COOKIE`, without which Avahi adds a cookie key to the TXT (defs.h:250); committed once the server is `RUNNING` (defs.h:125-128) | One `ServiceInfo` per record |
| Collision | `EntryGroup.StateChanged` `COLLISION` | `DaemonEvent::NameChange` for the instance name (service_daemon.rs:4577-4600) |
| Host rename | `Server.StateChanged` to `COLLISION` or `REGISTERING` withdraws the group, and `RUNNING` re-reads `GetHostNameFqdn` and registers again (defs.h:125-133) | `NameChange` with an A or AAAA `rr_type`, emitted per interface (service_daemon.rs:4586-4602, 5360-5376) |
| Established | `StateChanged` `ESTABLISHED` | After `register`, revised by a later `NameChange` or `DaemonEvent::Error` |
| TXT change | `UpdateServiceTxt` | `register` again under the same name |
| Withdraw | `Free` on the group | `unregister`, which sends goodbyes |

### Backends

**Avahi** uses the system bus name `org.freedesktop.Avahi`. Browsing uses domain `local` and `AVAHI_LOOKUP_USE_MULTICAST`, and keeps results flagged `AVAHI_LOOKUP_RESULT_LOCAL` (defs.h:274), so a service finds a broker on its own host; python-mdns-discovery skips them (browser.py:382).

- Signals: a browser made with `ServiceBrowserNew` starts by itself 10 ms after creation (dbus-protocol.c:1073), so its first signals can precede a subscription made after the call. For each browser and resolver the backend opens a `zbus::MessageStream` match on the Avahi sender and interface first, calls `Server2.ServiceBrowserPrepare` (or `ServiceResolverPrepare`) and then `Start`, falls back to `ServiceBrowserNew` on `UnknownMethod`, and filters signals by object path.
- Restart: `NameOwnerChanged` for the bus name means every browser, resolver and entry group is gone. The backend sends a `Reset` report, then recreates the browsers and re-commits the groups.
- Absence: no system bus gives `Error::NoSystemBus`; `org.freedesktop.DBus.Error.ServiceUnknown` gives `Error::AvahiUnavailable`, and `Mdns::system()` then tries mdns-sd.
- Requery frees and recreates the browser.

**mdns-sd** runs one daemon thread per `Mdns`.

- Each browse channel holds 10 events (service_daemon.rs:444), and the daemon thread delivers with a blocking send (service_daemon.rs:2927, 2940), so every receiver is drained by its own task.
- Requery stops and restarts the browse for that type.
- Port 5353: mdns-sd sets `SO_REUSEADDR` and `SO_REUSEPORT` (service_daemon.rs:936-941), and avahi-daemon retries its bind with both when the port is taken (socket.c:188-223), so the two stacks share the port in either start order. `MdnsSdOptions::port` moves a test daemon off 5353 (`new_with_port`, service_daemon.rs:355).

### Discovery-bus records

A later backend implements `browse` over the retained records ebus-mdns-discovery publishes at `local/mdns/discovery/v1/{service_type}/{interface}/{instance}` (record.py:24, 159-163). It maps each record to `Seen` as `Record.to_instance()` does (record.py:187-202), an empty payload or `state=removed` to `Gone` (resolver.py:137-156), and keeps the records while `{base}/$state` is not `ready`, marking them untrusted as python's `bus_ready` does (resolver.py:193-203). ebus-mdns-discovery skips services on its own host (browser.py:382), so a broker there is absent from the bus.

### Testing

| Layer | What |
|---|---|
| Core unit, virtual `Instant` | `InstanceTable`: Avahi per-slot sightings merged into one instance, an address replaced in its slot, mdns-sd whole-set reports, slot and whole removal. `BrokerSearch`: each mode row, the settle window, preference, host and port order, the `_secure-mqtt` and `_mqtt` merge, the `device_id` merge, TLS narrowing `accept`, `wanted` by instance name and by device id, fallback at `fallback_after_ms`, `allow_unmatched`, address matches with plain and TLS hosts, the requery schedule, a result that stays fixed after later sightings. `connect_host` for a DNS name, a `.local` name with an IPv4 candidate and without one, on TLS and plain transports. `wanted` with a URL rejected by `new`. The `Reresolve` schedule: the first failed connect after a lost link, then every third. Advertising: one name across records, renames through `-99`, a device id too long for the instance name, TXT updates only for changed records, `brokers` without `broker-host` logged and `roles` unchanged |
| python-service-discovery vectors | Generated by [tools/gen_vectors.py](../tools/gen_vectors.py) into `vectors/python-service-discovery/`: `decode_txt`, `parse_ebus_txt`, `check_txt`; `Identity`, `HttpService` and `BrokerService` TXT as ordered pairs; `BrokerEndpoint.from_instance`, `from_url` and `url`; `rank_brokers`; `match_configured` and `select_broker` for every mode; `BrokerMode.parse`; `RetrySchedule.delays`; address scope and `candidate_addresses`. Duplicate TXT keys are rust-sdk cases |
| mdns-sd to mdns-sd, Linux and macOS | Two daemons on a private port: one advertises under `HostName::Own`, the other lists brokers and finds one; then a TXT update, withdrawal, an instance-name collision and a host-name collision that rebuilds the TXT `broker` |
| Cross-backend, Linux CI | avahi-daemon installed and started beside mosquitto; skipped when the system bus or `org.freedesktop.Avahi` is absent. mdns-sd advertises under `HostName::Own("ebus-test-<random>.local.")` and Avahi browses and resolves it; Avahi advertises and mdns-sd browses it; Avahi to Avahi; an avahi-daemon restart re-commits the advertisement |
| python interop | With `EBUS_PYTHON`: a python-service-discovery advertisement found by each backend, and each backend's advertisement found by python's `find_broker` as the same endpoint |
| mosquitto e2e | Two brokers advertised as `_mqtt._tcp`: `list_brokers` returns both in rank order; `find_broker` in `discovery-only` picks the first and `controller()` syncs a rust device through it; `wanted` picks the other; `discovery-with-fallback` with an unadvertised URL falls back to the configured broker; a TLS configuration ignores a plaintext advertisement; a broker re-advertised at a new address after its link drops is reconnected through `reresolve` |

CI builds `ebus-discovery-tokio` with each feature alone and with both; on macOS `avahi` compiles nothing, since its code and dependency are Linux-only. `ebus-discovery` builds for `thumbv7em-none-eabihf` with and without `alloc`, like the other core crates.

The examples gain `EBUS_BROKER_MODE` and `EBUS_MDNS_BACKEND` (`auto`, `avahi`, `mdns-sd`), and `EBUS_MQTT_HOST` and `EBUS_MQTT_PORT` become the configured URL, so with neither set they run in `discovery-only`. They call `find_broker` once before building their options. `ebus-discovery-tokio` is a dev-dependency of `ebus-rumqttc` with `mdns-sd`, and with `avahi` under `[target.'cfg(target_os = "linux")'.dev-dependencies]`.

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
| mosquitto e2e | Broker on a free port, skipped when the binary is absent: pacing that heals the drops of a broker with `max_queued_messages 30`; inbox-overflow drops healed by resubscribing; restart without persistence (`DeviceAbsent`, then healing); removal and return in wildcard and tree modes; discovery and sync of a python-sdk device tree; `/set` round trip against a python-sdk device with byte-exact `/set` payloads and the republished value; a host holding overlapping and identical filters; a re-sync lost with its connection, behind a proxy that passes the SUBSCRIBEs and blocks every broker reply but the CONNACK, followed by a value deleted at the broker and cleared after the reconnect; bring-your-own through a rumqttc 0.10 dev-dependency alongside the bundled 0.25 path; the device role's cases and the panel-sim corpus under [Device role: Testing](#testing) | `ebus-homie-tokio/tests`, `ebus-rumqttc/tests` |
| Live panels (planned) | Read-only example binary in wildcard and tree modes, compared with an independent Homie client; time to `DeviceSynced` and CPU per message. `/set` only with the maintainer's go-ahead | manual |

## Embedding in an existing service

- A host service that already owns an MQTT connection uses bring-your-own: the controller runs on the host's runtime (the host spawns the driver) and forces no MQTT client or version on it.
- A host workspace that builds with `--locked` takes the `Cargo.lock` diff as the acceptance gate: `cargo tree -d` should show no new MQTT client, rustls or tokio version.
