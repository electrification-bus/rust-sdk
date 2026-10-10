//! MQTT vocabulary for eBus.
//!
//! The messages and commands the Homie state machines exchange with an MQTT
//! client, topic filters and their ownership, the hold for publishes made while
//! the broker link is down, and the order of work after a reconnect.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;
