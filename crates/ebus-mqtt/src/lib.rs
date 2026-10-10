//! MQTT transport contract for eBus.
//!
//! Defines the transport trait the Homie runtimes publish through, the hold for
//! publishes made while the broker link is down, and the order of work after a
//! reconnect. Client adapters implement the trait.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;
