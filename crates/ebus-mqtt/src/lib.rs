//! MQTT vocabulary for eBus.
//!
//! The messages and commands the Homie state machines exchange with an MQTT
//! client, topic filter matching, and, with `alloc`, the `FilterLedger` that
//! tracks which subscriptions the client holds and who may remove them, the
//! [`Role`] trait those state machines implement, and [`Pair`], which runs two
//! of them on one link.
//!
//! The crate performs no I/O and reads no clock: time arrives as an [`Instant`]
//! the caller supplies, received messages arrive as [`Message`]s, and work for
//! the MQTT client leaves as [`Command`]s.
//!
//! ```
//! use ebus_mqtt::filter;
//!
//! assert!(filter::matches("ebus/5/+/$state", "ebus/5/meter-1/$state"));
//! assert!(filter::matches("ebus/5/meter-1/#", "ebus/5/meter-1/power/watts"));
//! // A leading wildcard does not match a topic that starts with `$`.
//! assert!(!filter::matches("+/5/meter-1/$state", "$SYS/5/meter-1/$state"));
//! ```
//!
//! # Features
//!
//! - `std` (default): links `std`; implies `alloc`.
//! - `alloc`: [`Command`], [`LastWill`], [`FilterLedger`], [`Role`] and [`Pair`].

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "alloc")]
use alloc::{string::String, vec::Vec};

pub mod filter;
#[cfg(feature = "alloc")]
mod ledger;
#[cfg(feature = "alloc")]
mod role;

#[cfg(feature = "alloc")]
pub use ledger::FilterLedger;
#[cfg(feature = "alloc")]
pub use role::{Pair, Receipt, Role};

/// Whether a role took a received message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Handled {
    /// The message addressed something the role tracks.
    Consumed,
    /// Not for the role.
    Ignored,
}

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
///
/// Built with [`Message::new`] outside this crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Message<'a> {
    /// Topic name the message was published to.
    pub topic: &'a str,
    /// Application payload, possibly empty.
    pub payload: &'a [u8],
    /// MQTT retain bit; `None` when the client does not surface it.
    pub retain: Option<bool>,
}

impl<'a> Message<'a> {
    /// A received message.
    pub const fn new(topic: &'a str, payload: &'a [u8], retain: Option<bool>) -> Self {
        Self {
            topic,
            payload,
            retain,
        }
    }
}

/// A change in the state of the connection to the broker.
///
/// `#[non_exhaustive]`: a `match` on it outside this crate needs a `_` arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Link {
    /// The broker accepted a connection (CONNACK with return code 0).
    ///
    /// Built with [`Link::up`] outside this crate.
    #[non_exhaustive]
    Up {
        /// The CONNACK session present flag: the broker resumed a stored
        /// session, including its subscriptions.
        session_present: bool,
    },
    /// The connection was lost or closed.
    Down,
}

impl Link {
    /// [`Link::Up`] with the CONNACK session present flag.
    pub const fn up(session_present: bool) -> Self {
        Self::Up { session_present }
    }
}

/// Work for the MQTT client.
///
/// Each variant is `#[non_exhaustive]`: match it with `..`, and build it with
/// [`Command::subscribe`], [`Command::unsubscribe`] or [`Command::publish`]
/// outside this crate.
#[cfg(feature = "alloc")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Command {
    /// Subscribe to every filter in one SUBSCRIBE packet.
    #[non_exhaustive]
    Subscribe {
        /// Topic filters with their requested maximum QoS.
        filters: Vec<(String, QoS)>,
    },
    /// Unsubscribe from every filter in one UNSUBSCRIBE packet.
    #[non_exhaustive]
    Unsubscribe {
        /// Topic filters to remove.
        filters: Vec<String>,
    },
    /// Publish one message.
    #[non_exhaustive]
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

#[cfg(feature = "alloc")]
impl Command {
    /// Subscribe to every filter in one SUBSCRIBE packet.
    pub fn subscribe(filters: impl Into<Vec<(String, QoS)>>) -> Self {
        Self::Subscribe {
            filters: filters.into(),
        }
    }

    /// Unsubscribe from every filter in one UNSUBSCRIBE packet.
    pub fn unsubscribe(filters: impl Into<Vec<String>>) -> Self {
        Self::Unsubscribe {
            filters: filters.into(),
        }
    }

    /// Publish one message.
    pub fn publish(
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        qos: QoS,
        retain: bool,
    ) -> Self {
        Self::Publish {
            topic: topic.into(),
            payload: payload.into(),
            qos,
            retain,
        }
    }
}

/// The Last Will and Testament a client registers when it connects.
///
/// Built with [`LastWill::new`] outside this crate.
#[cfg(feature = "alloc")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
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

#[cfg(feature = "alloc")]
impl LastWill {
    /// A will the broker publishes to `topic` when the client drops.
    pub fn new(
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        qos: QoS,
        retain: bool,
    ) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
            qos,
            retain,
        }
    }
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

    #[cfg(feature = "alloc")]
    #[test]
    fn constructors_fill_every_field() {
        use alloc::{string::ToString, vec};
        assert_eq!(
            Command::publish("t", b"p".to_vec(), QoS::AtLeastOnce, true),
            Command::Publish {
                topic: "t".to_string(),
                payload: b"p".to_vec(),
                qos: QoS::AtLeastOnce,
                retain: true,
            }
        );
        let filters = vec![("a/#".to_string(), QoS::ExactlyOnce)];
        assert_eq!(
            Command::subscribe(filters.clone()),
            Command::Subscribe { filters }
        );
        assert_eq!(
            Command::unsubscribe(vec!["a/#".to_string()]),
            Command::Unsubscribe {
                filters: vec!["a/#".to_string()],
            }
        );
        let will = LastWill::new("w", b"lost".to_vec(), QoS::AtMostOnce, false);
        assert_eq!(
            (
                will.topic.as_str(),
                will.payload.as_slice(),
                will.qos,
                will.retain
            ),
            ("w", &b"lost"[..], QoS::AtMostOnce, false)
        );
        assert_eq!(
            Link::up(true),
            Link::Up {
                session_present: true
            }
        );
        assert_eq!(
            Message::new("t", b"p", Some(false)),
            Message {
                topic: "t",
                payload: b"p",
                retain: Some(false),
            }
        );
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

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
