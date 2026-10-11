//! eBus DNS-SD contract.
//!
//! The service types an eBus entity advertises and browses (framework.md,
//! "Detail: mDNS Discovery"), the broker transports with the specification's
//! preference order, the broker modes, and address scopes. With `alloc`: TXT
//! record decoding, checks and building; the advertisement model; merging the
//! sightings an mDNS backend reports into instances; and broker ranking and
//! selection, including the [`BrokerSearch`] state machine.
//!
//! The crate performs no I/O and reads no clock: backends in a runtime crate
//! feed it sightings and the time as an [`Instant`]. Where the specification
//! leaves a choice open it follows python-service-discovery.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;

mod addr;
#[cfg(feature = "alloc")]
mod advertise;
#[cfg(feature = "alloc")]
mod broker;
#[cfg(feature = "alloc")]
mod instance;
mod log;
#[cfg(feature = "alloc")]
mod search;
#[cfg(feature = "alloc")]
mod txt;

pub use addr::{AddrScope, ScopedAddr};
pub use ebus_mqtt::Instant;

#[cfg(feature = "alloc")]
pub use addr::candidate_addresses;
#[cfg(feature = "alloc")]
pub use advertise::{
    Advertisement, BrokerService, HttpService, Identity, MAX_DEFAULT_INSTANCE_OCTETS,
    MAX_RENAME_SUFFIX, ServiceRecord,
};
#[cfg(feature = "alloc")]
pub use broker::{BrokerEndpoint, UrlError, Wanted, match_configured, rank_brokers, select_broker};
#[cfg(feature = "alloc")]
pub use instance::{Instance, InstanceKey, InstanceTable, Sighting, Slot};
#[cfg(feature = "alloc")]
pub use search::{BrokerConfig, BrokerSearch, ConfigError};
#[cfg(feature = "alloc")]
pub use txt::{
    EbusTxt, TXT_STRING_MAX, TXT_STRING_WARN, TXT_TOTAL_WARN, TxtError, TxtRecord, TxtReport,
    check_txt, parse_ebus_txt,
};

/// The `_ebus._tcp` service every eBus entity advertises (framework.md:77).
pub const EBUS_SERVICE: &str = "_ebus._tcp";
/// The `_device-info._tcp` service every eBus entity advertises (framework.md:77).
pub const DEVICE_INFO_SERVICE: &str = "_device-info._tcp";
/// A plain HTTP REST API (framework.md:79).
pub const HTTP_SERVICE: &str = "_http._tcp";
/// An HTTPS REST API (framework.md:79).
pub const HTTPS_SERVICE: &str = "_https._tcp";

/// The framework.md version this crate implements, the default `ebus_version`
/// TXT value: the `framework` value in `.ebus-spec.json`.
pub const EBUS_VERSION: &str = "0.9";

/// The `txtvers` value of every record this crate builds.
pub const TXTVERS: &str = "1";

/// The `device` role (framework.md:248).
pub const ROLE_DEVICE: &str = "device";
/// The `controller` role (framework.md:248).
pub const ROLE_CONTROLLER: &str = "controller";
/// The `broker-host` role (framework.md:106, 248).
pub const ROLE_BROKER_HOST: &str = "broker-host";

/// A broker transport, declared in the specification's preference order
/// (framework.md:330-335), so `Ord` ranks the most preferred first. That
/// order is the default; a client sets its own with [`BrokerConfig::accept`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BrokerType {
    /// `_secure-mqtt._tcp`: MQTT over TLS.
    SecureMqtt,
    /// `_mqtt-wss._tcp`: MQTT over secure WebSocket.
    MqttWss,
    /// `_mqtt-ws._tcp`: MQTT over WebSocket.
    MqttWs,
    /// `_mqtt._tcp`: plain MQTT.
    Mqtt,
}

/// Every broker type, in the specification's preference order.
pub const BROKER_TYPES: [BrokerType; 4] = [
    BrokerType::SecureMqtt,
    BrokerType::MqttWss,
    BrokerType::MqttWs,
    BrokerType::Mqtt,
];

/// The broker types an MQTT-over-TCP client can use; the accepted types when
/// no TLS is configured (ebus.py:54).
pub const TCP_BROKER_TYPES: [BrokerType; 2] = [BrokerType::SecureMqtt, BrokerType::Mqtt];

impl BrokerType {
    /// The DNS-SD service type, without a domain.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SecureMqtt => "_secure-mqtt._tcp",
            Self::MqttWss => "_mqtt-wss._tcp",
            Self::MqttWs => "_mqtt-ws._tcp",
            Self::Mqtt => "_mqtt._tcp",
        }
    }

    /// The broker type of a service type, ignoring ASCII case and an optional
    /// `.local` or `.local.` domain; `None` for any other service.
    pub fn parse(service_type: &str) -> Option<Self> {
        let t = service_type.strip_suffix('.').unwrap_or(service_type);
        let t = match t.len().checked_sub(".local".len()) {
            Some(at) if t.is_char_boundary(at) && t[at..].eq_ignore_ascii_case(".local") => {
                &t[..at]
            }
            _ => t,
        };
        BROKER_TYPES
            .into_iter()
            .find(|b| b.as_str().eq_ignore_ascii_case(t))
    }

    /// The port used when an advertisement carries none (ebus.py:57-62).
    pub const fn default_port(self) -> u16 {
        match self {
            Self::SecureMqtt => 8883,
            Self::MqttWss => 9002,
            Self::MqttWs => 9001,
            Self::Mqtt => 1883,
        }
    }

    /// True for the TLS transports (ebus.py:64-69).
    pub const fn uses_tls(self) -> bool {
        matches!(self, Self::SecureMqtt | Self::MqttWss)
    }

    /// The URL scheme: `mqtts`, `wss`, `ws` or `mqtt` (ebus.py:71-76).
    pub const fn scheme(self) -> &'static str {
        match self {
            Self::SecureMqtt => "mqtts",
            Self::MqttWss => "wss",
            Self::MqttWs => "ws",
            Self::Mqtt => "mqtt",
        }
    }
}

/// How an entity finds its broker (framework.md:102, 614-622).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BrokerMode {
    /// Use the configured broker; no mDNS discovery.
    ConfiguredOnly,
    /// mDNS first; the configured broker if discovery finds no match.
    DiscoveryWithFallback,
    /// mDNS only.
    DiscoveryOnly,
}

/// A broker mode string that names no mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ParseModeError;

impl core::fmt::Display for ParseModeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(
            "unknown broker mode; expected configured-only, discovery-with-fallback or discovery-only",
        )
    }
}

impl core::error::Error for ParseModeError {}

impl BrokerMode {
    /// Every mode, in the specification's order.
    pub const ALL: [BrokerMode; 3] = [
        Self::ConfiguredOnly,
        Self::DiscoveryWithFallback,
        Self::DiscoveryOnly,
    ];

    /// The specification's spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfiguredOnly => "configured-only",
            Self::DiscoveryWithFallback => "discovery-with-fallback",
            Self::DiscoveryOnly => "discovery-only",
        }
    }

    /// A mode from configuration text: the specification's spellings, with
    /// surrounding whitespace, ASCII case and `_` for `-` ignored
    /// (ebus.py:110-127). An empty value is an error, so a host maps "unset" to
    /// `None` itself and the `BrokerConfig` precedence rules apply.
    pub fn parse(text: &str) -> Result<Self, ParseModeError> {
        let text = text.trim();
        Self::ALL
            .into_iter()
            .find(|m| {
                let want = m.as_str().as_bytes();
                want.len() == text.len()
                    && want.iter().zip(text.bytes()).all(|(&w, t)| {
                        let t = t.to_ascii_lowercase();
                        w == if t == b'_' { b'-' } else { t }
                    })
            })
            .ok_or(ParseModeError)
    }
}

impl core::str::FromStr for BrokerMode {
    type Err = ParseModeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// `name` without one trailing dot (instance.py:18-20).
pub fn strip_dot(name: &str) -> &str {
    name.strip_suffix('.').unwrap_or(name)
}

/// True when two host names are equal ignoring case and one trailing dot each
/// (ebus.py:661-662).
pub fn same_host(a: &str, b: &str) -> bool {
    let (a, b) = (strip_dot(a), strip_dot(b));
    a.chars()
        .flat_map(char::to_lowercase)
        .eq(b.chars().flat_map(char::to_lowercase))
}

/// True for a name in the `.local` mDNS domain, ignoring ASCII case and a
/// trailing dot.
pub fn is_local_name(host: &str) -> bool {
    let h = strip_dot(host).as_bytes();
    h.len() > ".local".len() && h[h.len() - ".local".len()..].eq_ignore_ascii_case(b".local")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_type_order_is_preference() {
        let mut v = [
            BrokerType::Mqtt,
            BrokerType::MqttWs,
            BrokerType::SecureMqtt,
            BrokerType::MqttWss,
        ];
        v.sort();
        assert_eq!(v, BROKER_TYPES);
    }

    #[test]
    fn broker_type_parse() {
        for t in BROKER_TYPES {
            assert_eq!(BrokerType::parse(t.as_str()), Some(t));
        }
        assert_eq!(
            BrokerType::parse("_Secure-MQTT._tcp.local."),
            Some(BrokerType::SecureMqtt)
        );
        assert_eq!(
            BrokerType::parse("_mqtt._tcp.local"),
            Some(BrokerType::Mqtt)
        );
        assert_eq!(BrokerType::parse("_mqtt._tcp."), Some(BrokerType::Mqtt));
        assert_eq!(BrokerType::parse("_ebus._tcp"), None);
        assert_eq!(BrokerType::parse("_mqtt._udp"), None);
        assert_eq!(BrokerType::parse(""), None);
        assert_eq!(BrokerType::parse("é.local"), None);
    }

    #[test]
    fn broker_type_properties() {
        let ports: [u16; 4] = BROKER_TYPES.map(BrokerType::default_port);
        assert_eq!(ports, [8883, 9002, 9001, 1883]);
        let tls = BROKER_TYPES.map(BrokerType::uses_tls);
        assert_eq!(tls, [true, true, false, false]);
    }

    #[test]
    fn mode_parse() {
        assert_eq!(
            BrokerMode::parse(" Discovery_Only "),
            Ok(BrokerMode::DiscoveryOnly)
        );
        assert_eq!("configured-only".parse(), Ok(BrokerMode::ConfiguredOnly));
        assert_eq!(BrokerMode::parse(""), Err(ParseModeError));
        assert_eq!(BrokerMode::parse("discovery"), Err(ParseModeError));
    }

    #[test]
    fn host_names() {
        assert!(same_host("Host.Local.", "host.local"));
        assert!(!same_host("host.local..", "host.local"));
        assert!(is_local_name("broker.LOCAL."));
        assert!(!is_local_name("local"));
        assert!(!is_local_name("broker.example.com"));
        assert_eq!(strip_dot("a.local."), "a.local");
        assert_eq!(strip_dot("a.local.."), "a.local.");
    }
}
