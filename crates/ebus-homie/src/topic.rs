//! Parse and build Homie 5 topics `{domain}/5/{device}/...`.

use core::fmt;

#[cfg(feature = "alloc")]
use alloc::{format, string::String};

use crate::HOMIE_TOPIC_VERSION;
use crate::id::is_valid_id;

/// `$log` levels (convention.md: Logging).
pub const LOG_LEVELS: [&str; 5] = ["debug", "info", "warn", "error", "fatal"];

/// What a topic addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicKind<'a> {
    /// `{device}/$state`.
    DeviceState,
    /// `{device}/$description`.
    DeviceDescription,
    /// `{device}/$log/{level}`; `level` is one of [`LOG_LEVELS`].
    DeviceLog {
        /// Log level.
        level: &'a str,
    },
    /// `{device}/$alert/{alert_id}`; the alert id is a valid id.
    DeviceAlert {
        /// Alert id.
        alert_id: &'a str,
    },
    /// `{device}/{node}/{property}`: the property value.
    PropertyValue {
        /// Node id.
        node: &'a str,
        /// Property id.
        property: &'a str,
    },
    /// `{device}/{node}/{property}/set`: a controller command.
    PropertySet {
        /// Node id.
        node: &'a str,
        /// Property id.
        property: &'a str,
    },
    /// `{device}/{node}/{property}/$target`: the intended value.
    PropertyTarget {
        /// Node id.
        node: &'a str,
        /// Property id.
        property: &'a str,
    },
    /// `$broadcast/{subtopic}`; `subtopic` has one or more `/`-separated ids.
    Broadcast {
        /// Everything after `$broadcast/`.
        subtopic: &'a str,
    },
}

/// A parsed topic, borrowing from the input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Topic<'a> {
    /// First level, e.g. `homie` or `ebus`.
    pub domain: &'a str,
    /// Device id; `None` for a broadcast.
    pub device: Option<&'a str>,
    /// What the topic addresses.
    pub kind: TopicKind<'a>,
}

/// Why a topic did not parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopicError {
    /// The topic ends before it addresses anything.
    TooFewLevels,
    /// Levels follow a topic that admits no sub-topics.
    TooManyLevels,
    /// The domain level is not a valid id.
    InvalidDomain,
    /// The version level is not `5`.
    UnsupportedVersion,
    /// The device level is not a valid id.
    InvalidDeviceId,
    /// A `$` level that Homie does not define at that position.
    UnknownAttribute,
    /// The `$log` level is not one of [`LOG_LEVELS`].
    InvalidLogLevel,
    /// The `$alert` id is not a valid id.
    InvalidAlertId,
    /// The node level is not a valid id.
    InvalidNodeId,
    /// The property level is not a valid id.
    InvalidPropertyId,
    /// A `$broadcast` subtopic level is not a valid id.
    InvalidBroadcastSubtopic,
}

impl TopicError {
    /// camelCase error code.
    pub const fn as_str(self) -> &'static str {
        match self {
            TopicError::TooFewLevels => "tooFewLevels",
            TopicError::TooManyLevels => "tooManyLevels",
            TopicError::InvalidDomain => "invalidDomain",
            TopicError::UnsupportedVersion => "unsupportedVersion",
            TopicError::InvalidDeviceId => "invalidDeviceId",
            TopicError::UnknownAttribute => "unknownAttribute",
            TopicError::InvalidLogLevel => "invalidLogLevel",
            TopicError::InvalidAlertId => "invalidAlertId",
            TopicError::InvalidNodeId => "invalidNodeId",
            TopicError::InvalidPropertyId => "invalidPropertyId",
            TopicError::InvalidBroadcastSubtopic => "invalidBroadcastSubtopic",
        }
    }
}

impl fmt::Display for TopicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl core::error::Error for TopicError {}

const BROADCAST: &str = "$broadcast";

/// Parse a Homie 5 topic. Every id level is checked with
/// [`is_valid_id`] (convention.md: Topic IDs); MQTT wildcards are rejected.
pub fn parse(topic: &str) -> Result<Topic<'_>, TopicError> {
    let mut levels = topic.split('/');
    let domain = levels.next().unwrap_or("");
    if !is_valid_id(domain) {
        return Err(TopicError::InvalidDomain);
    }
    match levels.next() {
        None => return Err(TopicError::TooFewLevels),
        Some(v) if v != HOMIE_TOPIC_VERSION => return Err(TopicError::UnsupportedVersion),
        Some(_) => {}
    }
    let device = levels.next().ok_or(TopicError::TooFewLevels)?;

    if device == BROADCAST {
        // Prefix `{domain}/5/$broadcast/` is ASCII, so this slice is on a char boundary.
        let prefix = domain.len() + 1 + HOMIE_TOPIC_VERSION.len() + 1 + BROADCAST.len() + 1;
        let subtopic = topic.get(prefix..).ok_or(TopicError::TooFewLevels)?;
        if subtopic.split('/').all(is_valid_id) {
            return Ok(Topic {
                domain,
                device: None,
                kind: TopicKind::Broadcast { subtopic },
            });
        }
        return Err(TopicError::InvalidBroadcastSubtopic);
    }
    if !is_valid_id(device) {
        return Err(TopicError::InvalidDeviceId);
    }

    let first = levels.next().ok_or(TopicError::TooFewLevels)?;
    let kind = match first {
        "$state" => TopicKind::DeviceState,
        "$description" => TopicKind::DeviceDescription,
        "$log" => {
            let level = levels.next().ok_or(TopicError::TooFewLevels)?;
            if !LOG_LEVELS.contains(&level) {
                return Err(TopicError::InvalidLogLevel);
            }
            TopicKind::DeviceLog { level }
        }
        "$alert" => {
            let alert_id = levels.next().ok_or(TopicError::TooFewLevels)?;
            if !is_valid_id(alert_id) {
                return Err(TopicError::InvalidAlertId);
            }
            TopicKind::DeviceAlert { alert_id }
        }
        node if node.starts_with('$') => return Err(TopicError::UnknownAttribute),
        node => {
            if !is_valid_id(node) {
                return Err(TopicError::InvalidNodeId);
            }
            let property = levels.next().ok_or(TopicError::TooFewLevels)?;
            if !is_valid_id(property) {
                return Err(TopicError::InvalidPropertyId);
            }
            match levels.next() {
                None => TopicKind::PropertyValue { node, property },
                Some("set") => TopicKind::PropertySet { node, property },
                Some("$target") => TopicKind::PropertyTarget { node, property },
                Some(_) => return Err(TopicError::UnknownAttribute),
            }
        }
    };
    if levels.next().is_some() {
        return Err(TopicError::TooManyLevels);
    }
    Ok(Topic {
        domain,
        device: Some(device),
        kind,
    })
}

/// `{domain}/5/{device}/{attr}`, where `attr` includes its `$`
/// (e.g. `"$state"`, `"$log/warn"`). Inputs are not validated.
#[cfg(feature = "alloc")]
pub fn device_attribute(domain: &str, device: &str, attr: &str) -> String {
    format!("{domain}/{HOMIE_TOPIC_VERSION}/{device}/{attr}")
}

/// `{domain}/5/{device}/{node}/{prop}`. Inputs are not validated.
#[cfg(feature = "alloc")]
pub fn property(domain: &str, device: &str, node: &str, prop: &str) -> String {
    format!("{domain}/{HOMIE_TOPIC_VERSION}/{device}/{node}/{prop}")
}

/// `{domain}/5/{device}/{node}/{prop}/set`. Inputs are not validated.
#[cfg(feature = "alloc")]
pub fn property_set(domain: &str, device: &str, node: &str, prop: &str) -> String {
    format!("{domain}/{HOMIE_TOPIC_VERSION}/{device}/{node}/{prop}/set")
}

/// `{domain}/5/{device}/{node}/{prop}/$target`. Inputs are not validated.
#[cfg(feature = "alloc")]
pub fn property_target(domain: &str, device: &str, node: &str, prop: &str) -> String {
    format!("{domain}/{HOMIE_TOPIC_VERSION}/{device}/{node}/{prop}/$target")
}

/// `{domain}/5/$broadcast/{subtopic}`. Inputs are not validated.
#[cfg(feature = "alloc")]
pub fn broadcast(domain: &str, subtopic: &str) -> String {
    format!("{domain}/{HOMIE_TOPIC_VERSION}/{BROADCAST}/{subtopic}")
}

/// Subscription filter for every device's state: `{domain}/5/+/$state`.
#[cfg(feature = "alloc")]
pub fn device_wildcard_state(domain: &str) -> String {
    format!("{domain}/{HOMIE_TOPIC_VERSION}/+/$state")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(topic: &str, device: Option<&str>, kind: TopicKind<'_>) {
        assert_eq!(
            parse(topic),
            Ok(Topic {
                domain: "homie",
                device,
                kind
            }),
            "{topic}"
        );
    }

    #[test]
    fn parses_device_topics() {
        let d = Some("dev-1");
        ok("homie/5/dev-1/$state", d, TopicKind::DeviceState);
        ok(
            "homie/5/dev-1/$description",
            d,
            TopicKind::DeviceDescription,
        );
        ok(
            "homie/5/dev-1/$log/warn",
            d,
            TopicKind::DeviceLog { level: "warn" },
        );
        ok(
            "homie/5/dev-1/$alert/childlost",
            d,
            TopicKind::DeviceAlert {
                alert_id: "childlost",
            },
        );
        ok(
            "homie/5/dev-1/light/power",
            d,
            TopicKind::PropertyValue {
                node: "light",
                property: "power",
            },
        );
        ok(
            "homie/5/dev-1/light/power/set",
            d,
            TopicKind::PropertySet {
                node: "light",
                property: "power",
            },
        );
        ok(
            "homie/5/dev-1/light/power/$target",
            d,
            TopicKind::PropertyTarget {
                node: "light",
                property: "power",
            },
        );
        for (level, t) in LOG_LEVELS.iter().zip([
            "homie/5/d/$log/debug",
            "homie/5/d/$log/info",
            "homie/5/d/$log/warn",
            "homie/5/d/$log/error",
            "homie/5/d/$log/fatal",
        ]) {
            assert_eq!(parse(t).unwrap().kind, TopicKind::DeviceLog { level });
        }
    }

    #[test]
    fn parses_broadcast() {
        ok(
            "homie/5/$broadcast/alert",
            None,
            TopicKind::Broadcast { subtopic: "alert" },
        );
        ok(
            "homie/5/$broadcast/security/alert",
            None,
            TopicKind::Broadcast {
                subtopic: "security/alert",
            },
        );
    }

    #[test]
    fn other_domain() {
        let t = parse("ebus/5/x/$state").unwrap();
        assert_eq!(t.domain, "ebus");
    }

    #[test]
    fn errors() {
        use TopicError::*;
        let cases: &[(&str, TopicError)] = &[
            ("", InvalidDomain),
            ("/5/d/$state", InvalidDomain),
            ("Homie/5/d/$state", InvalidDomain),
            ("+/5/d/$state", InvalidDomain),
            ("homie", TooFewLevels),
            ("homie/5", TooFewLevels),
            ("homie/4/d/$state", UnsupportedVersion),
            ("homie/5.0/d/$state", UnsupportedVersion),
            ("homie//d/$state", UnsupportedVersion),
            ("homie/5/d", TooFewLevels),
            ("homie/5/$broadcast", TooFewLevels),
            ("homie/5/$broadcast/", InvalidBroadcastSubtopic),
            ("homie/5/$broadcast/a//b", InvalidBroadcastSubtopic),
            ("homie/5/$broadcast/a/", InvalidBroadcastSubtopic),
            ("homie/5/$broadcast/#", InvalidBroadcastSubtopic),
            ("homie/5/$broadcast/$x", InvalidBroadcastSubtopic),
            ("homie/5/+/$state", InvalidDeviceId),
            ("homie/5//$state", InvalidDeviceId),
            ("homie/5/D/$state", InvalidDeviceId),
            ("homie/5/d/", InvalidNodeId),
            ("homie/5/d/$state/x", TooManyLevels),
            ("homie/5/d/$description/", TooManyLevels),
            ("homie/5/d/$name", UnknownAttribute),
            ("homie/5/d/$", UnknownAttribute),
            ("homie/5/d/$log", TooFewLevels),
            ("homie/5/d/$log/trace", InvalidLogLevel),
            ("homie/5/d/$log/WARN", InvalidLogLevel),
            ("homie/5/d/$log/warn/x", TooManyLevels),
            ("homie/5/d/$alert", TooFewLevels),
            ("homie/5/d/$alert/$x", InvalidAlertId),
            ("homie/5/d/$alert/a/b", TooManyLevels),
            ("homie/5/d/node", TooFewLevels),
            ("homie/5/d/No/p", InvalidNodeId),
            ("homie/5/d/n/", InvalidPropertyId),
            ("homie/5/d/n/$target", InvalidPropertyId),
            ("homie/5/d/n/+", InvalidPropertyId),
            ("homie/5/d/n/p/get", UnknownAttribute),
            ("homie/5/d/n/p/$set", UnknownAttribute),
            ("homie/5/d/n/p/SET", UnknownAttribute),
            ("homie/5/d/n/p/set/x", TooManyLevels),
            ("homie/5/d/n/p/$target/x", TooManyLevels),
            ("homie/5/d/n/p/", UnknownAttribute),
        ];
        for (topic, err) in cases {
            assert_eq!(parse(topic), Err(*err), "{topic:?}");
        }
    }

    #[test]
    fn total_on_odd_input() {
        for t in [
            "/",
            "//",
            "\u{e9}/5/$broadcast/x",
            "a/5/$broadcast/\u{e9}",
            "a/5/\0/$state",
        ] {
            assert!(parse(t).is_err(), "{t:?}");
        }
    }

    #[test]
    fn error_codes() {
        assert_eq!(TopicError::InvalidLogLevel.as_str(), "invalidLogLevel");
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn error_display() {
        assert_eq!(
            format!("{}", TopicError::UnsupportedVersion),
            "unsupportedVersion"
        );
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn builders_round_trip() {
        let t = device_attribute("ebus", "d", "$state");
        assert_eq!(t, "ebus/5/d/$state");
        assert_eq!(parse(&t).unwrap().kind, TopicKind::DeviceState);

        let t = property("ebus", "d", "n", "p");
        assert_eq!(t, "ebus/5/d/n/p");
        assert!(matches!(
            parse(&t).unwrap().kind,
            TopicKind::PropertyValue { .. }
        ));

        let t = property_set("ebus", "d", "n", "p");
        assert_eq!(t, "ebus/5/d/n/p/set");
        assert!(matches!(
            parse(&t).unwrap().kind,
            TopicKind::PropertySet { .. }
        ));

        let t = property_target("ebus", "d", "n", "p");
        assert_eq!(t, "ebus/5/d/n/p/$target");
        assert!(matches!(
            parse(&t).unwrap().kind,
            TopicKind::PropertyTarget { .. }
        ));

        let t = broadcast("ebus", "security/alert");
        assert_eq!(t, "ebus/5/$broadcast/security/alert");
        assert_eq!(
            parse(&t).unwrap().kind,
            TopicKind::Broadcast {
                subtopic: "security/alert"
            }
        );

        assert_eq!(device_wildcard_state("ebus"), "ebus/5/+/$state");
    }
}
