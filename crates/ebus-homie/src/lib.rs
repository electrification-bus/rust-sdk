//! Homie 5 protocol core for eBus.
//!
//! The crate performs no I/O: callers pass in received MQTT messages and send the
//! messages it returns, so the same core serves tokio services and microcontrollers.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod datatype;
#[cfg(feature = "alloc")]
pub mod description;
pub mod id;
pub mod payload;
#[cfg(feature = "alloc")]
pub mod pyfmt;
pub mod state;
pub mod topic;

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
