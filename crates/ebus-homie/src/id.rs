//! Homie topic-level identifiers (convention.md: Topic IDs).

#[cfg(feature = "alloc")]
use alloc::string::String;

/// True when `s` is a legal Homie id: non-empty and only `a-z`, `0-9` and `-`
/// (convention.md: Topic IDs).
pub fn is_valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Python `re` `\s` for `str` patterns: Unicode `White_Space` plus the ASCII
/// separators U+001C to U+001F, which Python's `str.isspace` also counts.
#[cfg(feature = "alloc")]
fn is_py_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Coerce an arbitrary string to a legal Homie id; port of Python
/// `sanitize_homie_id`.
///
/// Lowercases (Unicode-aware, so U+212A KELVIN SIGN becomes `k`), turns runs of
/// `_`, `.` and whitespace into one `-`, drops every other character outside
/// `[a-z0-9-]`, collapses `-` runs and strips leading and trailing `-`. The
/// result may be empty; callers decide how to handle that. Sanitize each
/// segment of a composite id separately.
#[cfg(feature = "alloc")]
pub fn sanitize_id(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars().flat_map(char::to_lowercase) {
        let mapped = if c == '_' || c == '.' || is_py_space(c) {
            '-'
        } else if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' {
            c
        } else {
            continue;
        };
        if mapped == '-' && (out.is_empty() || out.ends_with('-')) {
            continue;
        }
        out.push(mapped);
    }
    if out.ends_with('-') {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ids() {
        for s in ["a", "0", "-", "abc-123", "000010024176", "-x-"] {
            assert!(is_valid_id(s), "{s:?}");
        }
        for s in [
            "",
            "A",
            "a_b",
            "a b",
            "$state",
            "a/b",
            "caf\u{e9}",
            "+",
            "#",
        ] {
            assert!(!is_valid_id(s), "{s:?}");
        }
    }

    /// Expected outputs produced by Python 3.10 `sanitize_homie_id`.
    #[cfg(feature = "alloc")]
    #[test]
    fn sanitize_python_parity() {
        let cases: &[(&str, &str)] = &[
            ("", ""),
            ("ABC_def", "abc-def"),
            ("  a  b  ", "a-b"),
            ("a..b__c", "a-b-c"),
            ("--x--", "x"),
            ("Hello World!", "hello-world"),
            ("\u{212a}", "k"),
            ("\u{130}stanbul", "istanbul"),
            ("caf\u{e9}", "caf"),
            ("a\u{a0}b", "a-b"),
            ("a\u{2003}b", "a-b"),
            ("x\u{3000}y", "x-y"),
            ("a\u{1c}b", "a-b"),
            ("a\u{85}b", "a-b"),
            ("a\u{200b}b", "ab"),
            ("SN_000010024176", "sn-000010024176"),
            ("_._", ""),
            ("\u{df}", ""),
            ("A-_-B", "a-b"),
            ("a\tb\nc\rd\u{b}e\u{c}f", "a-b-c-d-e-f"),
            ("\u{1680}z", "z"),
            ("\u{2028}\u{2029}", ""),
            ("\u{ff21}", ""),
        ];
        for (input, expected) in cases {
            assert_eq!(sanitize_id(input), *expected, "{input:?}");
        }
    }

    /// Python `\s` matches exactly these code points (enumerated with Python 3.10).
    #[cfg(feature = "alloc")]
    #[test]
    fn py_space_set() {
        const PY: &[u32] = &[
            0x9, 0xa, 0xb, 0xc, 0xd, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x85, 0xa0, 0x1680, 0x2000,
            0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a, 0x2028,
            0x2029, 0x202f, 0x205f, 0x3000,
        ];
        for c in (0..=0x10ffffu32).filter_map(char::from_u32) {
            assert_eq!(
                is_py_space(c),
                PY.contains(&(c as u32)),
                "U+{:04X}",
                c as u32
            );
        }
    }

    /// Every non-ASCII char sanitizes to "" except the two whose Python
    /// lowercase contains an ASCII id character.
    #[cfg(feature = "alloc")]
    #[test]
    fn sanitize_every_non_ascii_char() {
        let mut buf = [0u8; 4];
        for c in (0x80..=0x10ffffu32).filter_map(char::from_u32) {
            let expected = match c {
                '\u{130}' => "i",
                '\u{212a}' => "k",
                _ => "",
            };
            assert_eq!(
                sanitize_id(c.encode_utf8(&mut buf)),
                expected,
                "U+{:04X}",
                c as u32
            );
        }
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn sanitize_output_is_valid_or_empty() {
        for s in ["X Y", "a\u{212a}b", "--", "..a..", "\u{130}"] {
            let out = sanitize_id(s);
            assert!(out.is_empty() || is_valid_id(&out), "{s:?} -> {out:?}");
        }
    }
}
