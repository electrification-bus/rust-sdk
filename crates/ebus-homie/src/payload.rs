//! Raw MQTT payload classification (convention.md: Empty string values).

use core::str::Utf8Error;

/// Wire form of an empty string value: one `0x00` byte, because a zero-length
/// payload deletes a retained topic (convention.md: Empty string values).
pub const EMPTY_STRING_PAYLOAD: &[u8] = b"\x00";

/// A received payload, after empty-string decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Received<'a> {
    /// Zero-length payload: the retained topic was deleted.
    Deleted,
    /// UTF-8 text; a single `0x00` byte decodes to `""`.
    Text(&'a str),
}

/// Classify a received payload.
///
/// Zero-length is [`Received::Deleted`], `[0x00]` is `Text("")`, anything else
/// must be valid UTF-8. The `0x00` decoding applies to every topic here; it is
/// only meaningful for string values, `/set` and `$target` (convention.md:
/// Empty string values), and no other datatype accepts `""`.
pub fn classify(bytes: &[u8]) -> Result<Received<'_>, Utf8Error> {
    if bytes.is_empty() {
        Ok(Received::Deleted)
    } else if bytes == EMPTY_STRING_PAYLOAD {
        Ok(Received::Text(""))
    } else {
        core::str::from_utf8(bytes).map(Received::Text)
    }
}

/// Wire text for a string-datatype value: `""` becomes `"\0"`, anything else is
/// unchanged. Use only for the string datatype.
pub fn encode_str(value: &str) -> &str {
    if value.is_empty() { "\0" } else { value }
}

/// Byte form of [`encode_str`].
pub fn encode_str_bytes(value: &str) -> &[u8] {
    encode_str(value).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_cases() {
        assert_eq!(classify(b""), Ok(Received::Deleted));
        assert_eq!(classify(b"\x00"), Ok(Received::Text("")));
        assert_eq!(classify(b"\x00\x00"), Ok(Received::Text("\0\0")));
        assert_eq!(classify(b"a\x00"), Ok(Received::Text("a\0")));
        assert_eq!(classify(b"ready"), Ok(Received::Text("ready")));
        assert_eq!(
            classify("caf\u{e9}".as_bytes()),
            Ok(Received::Text("caf\u{e9}"))
        );
        assert!(classify(b"\xff").is_err());
        assert!(classify(b"\xc3").is_err());
    }

    #[test]
    fn encode_cases() {
        assert_eq!(encode_str(""), "\0");
        assert_eq!(encode_str_bytes(""), EMPTY_STRING_PAYLOAD);
        assert_eq!(encode_str("x"), "x");
        assert_eq!(classify(encode_str_bytes("")), Ok(Received::Text("")));
    }
}
