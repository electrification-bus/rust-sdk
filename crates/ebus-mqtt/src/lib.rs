//! MQTT vocabulary for eBus.
//!
//! The messages and commands the Homie state machines exchange with an MQTT
//! client, topic filter matching, and, with `alloc`, the `FilterLedger` that
//! tracks which subscriptions the client holds and who may remove them.
//!
//! The crate performs no I/O and reads no clock: time arrives as an [`Instant`]
//! the caller supplies, received messages arrive as [`Message`]s, and work for
//! the MQTT client leaves as [`Command`]s.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "alloc")]
use alloc::{string::String, vec::Vec};

pub mod filter;
#[cfg(feature = "alloc")]
mod ledger;

#[cfg(feature = "alloc")]
pub use ledger::FilterLedger;

/// MQTT quality of service level.
///
/// The discriminants are the wire values, so `qos as u8` is the QoS field of a
/// packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum QoS {
    /// QoS 0: delivered at most once, with no acknowledgment.
    AtMostOnce = 0,
    /// QoS 1: delivered at least once.
    AtLeastOnce = 1,
    /// QoS 2: delivered exactly once.
    ExactlyOnce = 2,
}

impl QoS {
    /// The QoS level for a wire value, or `None` for a value above 2.
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::AtMostOnce),
            1 => Some(Self::AtLeastOnce),
            2 => Some(Self::ExactlyOnce),
            _ => None,
        }
    }
}

/// Milliseconds on the caller's monotonic clock.
///
/// The origin is the caller's choice; only differences between two instants
/// from the same clock carry meaning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(pub u64);

impl Instant {
    /// Milliseconds elapsed from `earlier` to `self`, or 0 when `earlier` is later.
    pub const fn saturating_since(self, earlier: Instant) -> u64 {
        self.0.saturating_sub(earlier.0)
    }

    /// The instant `ms` milliseconds after `self`, saturating at `u64::MAX`.
    #[must_use]
    pub const fn add_ms(self, ms: u64) -> Instant {
        Instant(self.0.saturating_add(ms))
    }
}

/// A message received from the broker, borrowed from the client's buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Message<'a> {
    /// Topic name the message was published to.
    pub topic: &'a str,
    /// Application payload, possibly empty.
    pub payload: &'a [u8],
    /// MQTT retain bit; `None` when the client does not surface it.
    pub retain: Option<bool>,
}

/// A change in the state of the connection to the broker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Link {
    /// The broker accepted a connection (CONNACK with return code 0).
    Up {
        /// The CONNACK session present flag: the broker resumed a stored
        /// session, including its subscriptions.
        session_present: bool,
    },
    /// The connection was lost or closed.
    Down,
}

/// Work for the MQTT client.
#[cfg(feature = "alloc")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Command {
    /// Subscribe to every filter in one SUBSCRIBE packet.
    Subscribe {
        /// Topic filters with their requested maximum QoS.
        filters: Vec<(String, QoS)>,
    },
    /// Unsubscribe from every filter in one UNSUBSCRIBE packet.
    Unsubscribe {
        /// Topic filters to remove.
        filters: Vec<String>,
    },
    /// Publish one message.
    Publish {
        /// Topic name.
        topic: String,
        /// Application payload.
        payload: Vec<u8>,
        /// QoS to publish with.
        qos: QoS,
        /// MQTT retain flag.
        retain: bool,
    },
}

/// The Last Will and Testament a client registers when it connects.
#[cfg(feature = "alloc")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LastWill {
    /// Topic the broker publishes the will to.
    pub topic: String,
    /// Will payload.
    pub payload: Vec<u8>,
    /// Will QoS.
    pub qos: QoS,
    /// Will retain flag.
    pub retain: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qos_wire_values_round_trip() {
        for q in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            assert_eq!(QoS::from_u8(q as u8), Some(q));
        }
        assert_eq!(QoS::from_u8(3), None);
        assert_eq!(QoS::from_u8(u8::MAX), None);
        assert!(QoS::AtMostOnce < QoS::AtLeastOnce);
        assert!(QoS::AtLeastOnce < QoS::ExactlyOnce);
    }

    #[test]
    fn instant_saturates() {
        let a = Instant(1_000);
        assert_eq!(a.add_ms(500), Instant(1_500));
        assert_eq!(a.add_ms(500).saturating_since(a), 500);
        assert_eq!(a.saturating_since(a.add_ms(500)), 0);
        assert_eq!(a.saturating_since(a), 0);
        assert_eq!(Instant(u64::MAX - 1).add_ms(10), Instant(u64::MAX));
        assert_eq!(Instant(u64::MAX).saturating_since(Instant(0)), u64::MAX);
        assert_eq!(Instant::default(), Instant(0));
    }

    #[test]
    fn message_is_copy() {
        let m = Message {
            topic: "ebus/5/d/$state",
            payload: b"ready",
            retain: None,
        };
        let n = m;
        assert_eq!(m, n);
    }
}
