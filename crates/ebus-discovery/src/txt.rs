//! TXT records: decoding, checks and the `_ebus._tcp` parse (ebus.py:133-311).

use alloc::borrow::Cow;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;

/// Longest single TXT string (`key=value`): one length byte.
pub const TXT_STRING_MAX: usize = 255;
/// A TXT string longer than this is reported by [`check_txt`].
pub const TXT_STRING_WARN: usize = 200;
/// A TXT record longer than this on the wire is reported by [`check_txt`]
/// (RFC 6763 section 6.2).
pub const TXT_TOTAL_WARN: usize = 1300;

/// The `_ebus._tcp` keys the specification requires (framework.md:242-249).
const EBUS_REQUIRED_KEYS: [&str; 4] = ["txtvers", "ebus_version", "roles", "device_id"];

/// A DNS-SD TXT record: ordered keys, each with a value or none.
///
/// Keys compare case-insensitively and are unique: the first occurrence of a
/// key wins (RFC 6763 section 6.4). Values are bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct TxtRecord {
    entries: Vec<(String, Option<Vec<u8>>)>,
}

pub(crate) fn key_eq(a: &str, b: &str) -> bool {
    a.chars()
        .flat_map(char::to_lowercase)
        .eq(b.chars().flat_map(char::to_lowercase))
}

impl TxtRecord {
    /// An empty record.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode the strings of a received record. Each splits at its first `=`;
    /// one without `=` is a key with no value, and one with an empty key is
    /// skipped. Keys are decoded as UTF-8 with replacement and lowercased
    /// (ebus.py:136-153); a later duplicate of a key is dropped.
    pub fn from_strings<I, S>(strings: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let mut txt = Self::new();
        for s in strings {
            let s = s.as_ref();
            let (key, value) = match s.iter().position(|&b| b == b'=') {
                Some(at) => (&s[..at], Some(s[at + 1..].to_vec())),
                None => (s, None),
            };
            if key.is_empty() {
                continue;
            }
            txt.insert_raw(String::from_utf8_lossy(key).to_lowercase(), value);
        }
        txt
    }

    /// Add `key` with `value` unless the record already holds the key;
    /// returns whether it was added.
    pub fn insert_raw(&mut self, key: String, value: Option<Vec<u8>>) -> bool {
        if self.contains_key(&key) {
            return false;
        }
        self.entries.push((key, value));
        true
    }

    /// Add `key=value` unless the record already holds the key; returns
    /// whether it was added.
    pub fn insert(&mut self, key: &str, value: &str) -> bool {
        self.insert_raw(key.to_string(), Some(value.as_bytes().to_vec()))
    }

    /// True when the record holds `key`.
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.iter().any(|(k, _)| key_eq(k, key))
    }

    /// The value of `key` as text, decoded as UTF-8 with replacement; `""`
    /// for a key with no value; `None` when the key is absent.
    pub fn get(&self, key: &str) -> Option<Cow<'_, str>> {
        self.get_raw(key).map(|v| match v {
            Some(bytes) => String::from_utf8_lossy(bytes),
            None => Cow::Borrowed(""),
        })
    }

    /// The raw value of `key`: `Some(None)` for a key with no value.
    pub fn get_raw(&self, key: &str) -> Option<Option<&[u8]>> {
        self.entries
            .iter()
            .find(|(k, _)| key_eq(k, key))
            .map(|(_, v)| v.as_deref())
    }

    /// The entries in order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, Option<&[u8]>)> + '_ {
        self.entries.iter().map(|(k, v)| (k.as_str(), v.as_deref()))
    }

    /// The number of keys.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when the record holds no key.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The record's strings, `key=value` or `key`, in order.
    pub fn to_strings(&self) -> Vec<Vec<u8>> {
        self.entries
            .iter()
            .map(|(k, v)| entry_string(k, v.as_deref()))
            .collect()
    }

    /// Bytes the record occupies on the wire: a length byte per string.
    pub fn wire_size(&self) -> usize {
        self.entries
            .iter()
            .map(|(k, v)| 1 + entry_len(k, v.as_deref()))
            .sum()
    }
}

fn entry_len(key: &str, value: Option<&[u8]>) -> usize {
    key.len() + value.map_or(0, |v| 1 + v.len())
}

fn entry_string(key: &str, value: Option<&[u8]>) -> Vec<u8> {
    let mut s = Vec::with_capacity(entry_len(key, value));
    s.extend_from_slice(key.as_bytes());
    if let Some(v) = value {
        s.push(b'=');
        s.extend_from_slice(v);
    }
    s
}

/// A TXT record mDNS cannot carry, or an advertisement that cannot be built.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TxtError {
    /// A key that is empty, contains `=`, or is not printable ASCII.
    InvalidKey {
        /// The record's service type.
        service_type: String,
        /// The key.
        key: String,
    },
    /// A `key=value` string over [`TXT_STRING_MAX`] bytes.
    StringTooLong {
        /// The record's service type.
        service_type: String,
        /// The key.
        key: String,
        /// The string's length in bytes.
        bytes: usize,
    },
    /// An identity with no device id.
    NoDeviceId,
    /// An empty device id, or one containing a comma.
    InvalidDeviceId(String),
    /// An identity with no role.
    NoRole,
    /// An empty role, or one containing a comma.
    InvalidRole(String),
    /// A required identity field left empty: `manufacturer`, `model`,
    /// `serial_number` or `ebus_version`.
    Missing(&'static str),
    /// A Homie domain that is not one topic level.
    InvalidHomieDomain(String),
    /// Two brokers of one type in one advertisement.
    DuplicateBrokerType(crate::BrokerType),
}

impl fmt::Display for TxtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKey { service_type, key } => {
                write!(f, "{service_type}: invalid TXT key {key:?}")
            }
            Self::StringTooLong {
                service_type,
                key,
                bytes,
            } => write!(
                f,
                "{service_type}: TXT {key:?} is {bytes} bytes; one TXT string holds at most {TXT_STRING_MAX}"
            ),
            Self::NoDeviceId => f.write_str("identity needs at least one device id"),
            Self::InvalidDeviceId(id) => {
                write!(f, "invalid device id {id:?} (empty or contains a comma)")
            }
            Self::NoRole => f.write_str("identity needs at least one role"),
            Self::InvalidRole(role) => write!(f, "invalid role {role:?}"),
            Self::Missing(field) => write!(f, "identity {field} is required"),
            Self::InvalidHomieDomain(d) => {
                write!(f, "invalid Homie domain {d:?} (one topic level)")
            }
            Self::DuplicateBrokerType(t) => {
                write!(f, "more than one broker of type {}", t.as_str())
            }
        }
    }
}

impl core::error::Error for TxtError {}

/// What [`check_txt`] found near the limits; nothing here stops a record.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxtReport {
    /// Keys whose string is over [`TXT_STRING_WARN`] bytes, with its length.
    pub long_strings: Vec<(String, usize)>,
    /// The record's size on the wire.
    pub total_bytes: usize,
}

impl TxtReport {
    /// True when the record is over [`TXT_TOTAL_WARN`] bytes on the wire.
    pub fn is_large(&self) -> bool {
        self.total_bytes > TXT_TOTAL_WARN
    }
}

/// Check that mDNS can carry `txt` (ebus.py:282-311): every key non-empty,
/// without `=` and printable ASCII, and every string at most
/// [`TXT_STRING_MAX`] bytes. Strings and records near the limits are listed
/// in the report and logged as warnings.
pub fn check_txt(service_type: &str, txt: &TxtRecord) -> Result<TxtReport, TxtError> {
    let mut report = TxtReport::default();
    for (key, value) in txt.iter() {
        if key.is_empty() || !key.bytes().all(|b| (0x20..=0x7e).contains(&b) && b != b'=') {
            return Err(TxtError::InvalidKey {
                service_type: service_type.to_string(),
                key: key.to_string(),
            });
        }
        let n = entry_len(key, value);
        if n > TXT_STRING_MAX {
            return Err(TxtError::StringTooLong {
                service_type: service_type.to_string(),
                key: key.to_string(),
                bytes: n,
            });
        }
        if n > TXT_STRING_WARN {
            crate::log::warn!(
                "txtStringNearLimit",
                service = service_type,
                key = key,
                bytes = n
            );
            report.long_strings.push((key.to_string(), n));
        }
    }
    report.total_bytes = txt.wire_size();
    if report.is_large() {
        crate::log::warn!(
            "txtRecordLarge",
            service = service_type,
            bytes = report.total_bytes
        );
    }
    Ok(report)
}

/// A parsed `_ebus._tcp` TXT record (ebus.py:178-216).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EbusTxt {
    /// `txtvers`, or `""`.
    pub txtvers: String,
    /// `ebus_version`, or `""`.
    pub ebus_version: String,
    /// The `roles` list.
    pub roles: Vec<String>,
    /// The `device_id` list.
    pub device_ids: Vec<String>,
    /// The `auth_methods` list.
    pub auth_methods: Vec<String>,
    /// The whole record.
    pub txt: TxtRecord,
}

impl EbusTxt {
    /// The required keys absent or empty; never a reason to drop the record.
    pub fn missing(&self) -> Vec<&'static str> {
        EBUS_REQUIRED_KEYS
            .into_iter()
            .filter(|k| self.txt.get(k).is_none_or(|v| v.is_empty()))
            .collect()
    }

    /// True when `roles` lists `broker-host`.
    pub fn is_broker_host(&self) -> bool {
        self.roles.iter().any(|r| r == crate::ROLE_BROKER_HOST)
    }

    /// The value of any key, as [`TxtRecord::get`].
    pub fn get(&self, key: &str) -> Option<Cow<'_, str>> {
        self.txt.get(key)
    }
}

/// A comma-separated TXT list: items trimmed, empty items dropped
/// (ebus.py:156-159).
pub(crate) fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// Parse an `_ebus._tcp` record: `roles`, `device_id` and `auth_methods` are
/// comma-separated lists, and a missing key parses as empty.
pub fn parse_ebus_txt(txt: &TxtRecord) -> EbusTxt {
    let text = |k: &str| txt.get(k).map(Cow::into_owned).unwrap_or_default();
    let list = |k: &str| txt.get(k).map(|v| split_list(&v)).unwrap_or_default();
    EbusTxt {
        txtvers: text("txtvers"),
        ebus_version: text("ebus_version"),
        roles: list("roles"),
        device_ids: list("device_id"),
        auth_methods: list("auth_methods"),
        txt: txt.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn decoding() {
        let txt = TxtRecord::from_strings([
            &b"Roles=device"[..],
            b"flag",
            b"=nokey",
            b"",
            b"empty=",
            b"ROLES=second",
            b"bin=\xff",
            b"eq=a=b",
        ]);
        let keys: Vec<&str> = txt.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["roles", "flag", "empty", "bin", "eq"]);
        assert_eq!(txt.get("roles").as_deref(), Some("device"));
        assert_eq!(txt.get("FLAG").as_deref(), Some(""));
        assert_eq!(txt.get_raw("flag"), Some(None));
        assert_eq!(txt.get_raw("empty"), Some(Some(&b""[..])));
        assert_eq!(txt.get("bin").as_deref(), Some("\u{fffd}"));
        assert_eq!(txt.get("eq").as_deref(), Some("a=b"));
        assert_eq!(txt.get("absent"), None);
        assert_eq!(
            txt.to_strings(),
            vec![
                b"roles=device".to_vec(),
                b"flag".to_vec(),
                b"empty=".to_vec(),
                b"bin=\xff".to_vec(),
                b"eq=a=b".to_vec()
            ]
        );
        assert_eq!(txt.wire_size(), 13 + 5 + 7 + 6 + 7);
    }

    #[test]
    fn insert_keeps_first() {
        let mut txt = TxtRecord::new();
        assert!(txt.insert("Key", "1"));
        assert!(!txt.insert("kEY", "2"));
        assert_eq!(txt.get("key").as_deref(), Some("1"));
        assert_eq!(txt.iter().next(), Some(("Key", Some(&b"1"[..]))));
    }

    #[test]
    fn checks() {
        let mut ok = TxtRecord::new();
        ok.insert("a key", "v");
        assert_eq!(check_txt("_x._tcp", &ok).map(|r| r.total_bytes), Ok(8));
        for bad in ["", "a=b", "caf\u{e9}", "tab\t"] {
            let mut txt = TxtRecord::new();
            txt.insert_raw(bad.into(), None);
            assert!(matches!(
                check_txt("_x._tcp", &txt),
                Err(TxtError::InvalidKey { .. })
            ));
        }
        let mut long = TxtRecord::new();
        long.insert("k", &"v".repeat(253));
        let r = check_txt("_x._tcp", &long).unwrap();
        assert_eq!(r.long_strings, [("k".to_string(), 255)]);
        assert!(!r.is_large());
        let mut too_long = TxtRecord::new();
        too_long.insert("k", &"v".repeat(254));
        assert!(matches!(
            check_txt("_x._tcp", &too_long),
            Err(TxtError::StringTooLong { bytes: 256, .. })
        ));
        let mut large = TxtRecord::new();
        for i in 0..7 {
            large.insert(&alloc::format!("k{i}"), &"v".repeat(190));
        }
        let r = check_txt("_x._tcp", &large).unwrap();
        assert!(r.is_large());
        assert!(r.long_strings.is_empty());
    }

    #[test]
    fn ebus_txt() {
        let txt = TxtRecord::from_strings([
            "txtvers=1",
            "roles= device , ,broker-host",
            "device_id=a,b",
            "ebus_version=",
        ]);
        let e = parse_ebus_txt(&txt);
        assert_eq!(e.roles, ["device", "broker-host"]);
        assert_eq!(e.device_ids, ["a", "b"]);
        assert!(e.auth_methods.is_empty());
        assert!(e.is_broker_host());
        assert_eq!(e.missing(), ["ebus_version"]);
    }
}
