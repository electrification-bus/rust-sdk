//! Homie 5 protocol core for eBus.
//!
//! The crate performs no I/O: callers pass in received MQTT messages and send the
//! messages it returns, so the same core serves tokio services and microcontrollers.
//!
//! | Module | Contents |
//! |---|---|
//! | [`controller`] | The controller state machine: discovery, pacing, device views, events, `/set` and `$broadcast` (`alloc`) |
//! | [`device`] | The device role: a tree of devices, nodes and properties, publishing and `/set` handling (`alloc`) |
//! | [`description`] | The `$description` model, parsing, validation and wire form (`alloc`) |
//! | [`datatype`] | The nine datatypes, payload checks, `/set` parsing and the encoders |
//! | [`topic`] | Parsing and building `{domain}/5/{device}/...` topics |
//! | [`id`], [`state`], [`payload`] | Topic-level ids, `$state` values, empty-payload rules |
//!
//! A controller runs on the caller's connection and clock. Feed it a link
//! change and messages, then send what [`poll_command`](controller::Controller::poll_command)
//! returns and read what [`poll_event`](controller::Controller::poll_event) returns:
//!
//! ```
//! use ebus_homie::controller::{Config, Controller, Event};
//! use ebus_mqtt::{Command, Instant, Link, Message};
//!
//! let mut c = Controller::new(Config::device("meter-1")?)?;
//! let now = Instant(0);
//! c.handle_link(now, Link::up(false));
//! while let Some(cmd) = c.poll_command() {
//!     if let Command::Subscribe { filters, .. } = cmd {
//!         assert!(filters.iter().any(|(f, _)| f == "ebus/5/meter-1/$state"));
//!     }
//! }
//!
//! let retained = |topic, payload: &'static str| Message::new(topic, payload.as_bytes(), Some(true));
//! c.handle_message(now, retained("ebus/5/meter-1/$state", "ready"));
//! c.handle_message(
//!     now,
//!     retained(
//!         "ebus/5/meter-1/$description",
//!         r#"{"homie":"5.0","version":1,"nodes":{"power":{"properties":{"watts":{"datatype":"float"}}}}}"#,
//!     ),
//! );
//! while c.poll_command().is_some() {}
//! c.handle_message(now, retained("ebus/5/meter-1/power/watts", "1234.5"));
//!
//! let mut synced = false;
//! while let Some(e) = c.poll_event() {
//!     synced |= matches!(e, Event::DeviceSynced { missing: 0, .. });
//! }
//! assert!(synced);
//! let watts = c.device("meter-1").and_then(|d| d.property("power", "watts"));
//! assert_eq!(watts.and_then(|p| p.value.as_ref()).map(|v| &*v.raw), Some("1234.5"));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Features
//!
//! - `std` (default): links `std`; implies `alloc`.
//! - `alloc`: the [`controller`], [`device`] and [`description`] modules and
//!   the allocating parts of [`datatype`].
//! - `log`: diagnostics through the `log` crate.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "alloc")]
mod addr;
#[cfg(feature = "alloc")]
pub mod controller;
pub mod datatype;
#[cfg(feature = "alloc")]
pub mod description;
#[cfg(feature = "alloc")]
pub mod device;
pub mod id;
mod log;
pub mod payload;
// Public only for tests/vectors.rs; the `datatype` encoders are the text API.
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub mod pyfmt;
pub mod state;
pub mod topic;

#[cfg(feature = "alloc")]
pub use addr::{Id, PropertyAddr};
/// The `serde_json` this crate's JSON values ([`serde_json::Value`]) come from.
#[cfg(feature = "alloc")]
pub use serde_json;

// Every error type is a `core::error::Error`, with or without `std`.
const _: () = {
    const fn is_error<E: core::error::Error>() {}
    is_error::<datatype::Problem>();
    is_error::<state::ParseStateError>();
    is_error::<topic::TopicError>();
    #[cfg(feature = "alloc")]
    {
        is_error::<description::DescriptionError>();
        is_error::<controller::ConfigError>();
        is_error::<controller::SetError>();
        is_error::<device::AlertError>();
        is_error::<device::BuildError>();
        is_error::<device::ValueError>();
    }
};

/// Homie convention major version, as it appears in topics (`{domain}/5/...`).
pub const HOMIE_TOPIC_VERSION: &str = "5";

/// Value of the `homie` field in a `$description`.
pub const HOMIE_DESCRIPTION_VERSION: &str = "5.0";

/// Topic domain the eBus framework uses in place of Homie's default `homie`.
pub const EBUS_DOMAIN: &str = "ebus";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn description_version_extends_topic_version() {
        assert!(HOMIE_DESCRIPTION_VERSION.starts_with(HOMIE_TOPIC_VERSION));
    }
}

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
