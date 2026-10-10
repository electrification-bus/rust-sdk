//! Python-compatible text output, so payloads and `$description` documents match
//! the Python SDK byte for byte.
//!
//! [`format_float`] is the Python SDK's float payload form; [`to_json`],
//! [`write_str`] and [`write_value`] reproduce `json.dumps` with its default
//! options (`ensure_ascii=True`, separators `", "` and `": "`).

use alloc::string::String;
use core::fmt::Write as _;

use serde_json::Value;

/// Python `repr(x)` for a finite float; `None` when `x` is NaN or infinite.
///
/// Shortest round-trip digits, fixed notation when the decimal exponent `e`
/// satisfies `-4 <= e < 16`, otherwise `d[.ddd]e±XX` with at least two exponent
/// digits (`1e-05`, `1e+16`). This is the form `json.dumps` writes.
pub fn repr_float(x: f64) -> Option<String> {
    if !x.is_finite() {
        return None;
    }
    // `{:e}` yields the shortest round-trip digits as `[-]d[.ddd]e[-]N`.
    let mut sci = String::new();
    let _ = write!(sci, "{x:e}");
    let (mantissa, exp_text) = sci.split_once('e')?;
    let exp: i32 = exp_text.parse().ok()?;
    let (negative, mantissa) = match mantissa.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, mantissa),
    };
    let mut digits = String::with_capacity(mantissa.len());
    digits.extend(mantissa.chars().filter(|c| *c != '.'));
    round_tie_to_even(&mut digits, exp, x);

    let mut out = String::with_capacity(digits.len() + 8);
    if negative {
        out.push('-');
    }
    if (-4..16).contains(&exp) {
        if exp >= 0 {
            // `exp` is in 0..16, so the cast is lossless.
            let int_len = exp as usize + 1;
            if digits.len() <= int_len {
                out.push_str(&digits);
                out.extend(core::iter::repeat_n('0', int_len - digits.len()));
                out.push_str(".0");
            } else {
                let (int_part, frac_part) = digits.split_at(int_len);
                out.push_str(int_part);
                out.push('.');
                out.push_str(frac_part);
            }
        } else {
            // `exp` is in -4..0, so the cast is lossless.
            out.push_str("0.");
            out.extend(core::iter::repeat_n('0', (-exp - 1) as usize));
            out.push_str(&digits);
        }
    } else {
        let (first, rest) = digits.split_at(1.min(digits.len()));
        out.push_str(first);
        if !rest.is_empty() {
            out.push('.');
            out.push_str(rest);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        let _ = write!(out, "{:02}", exp.unsigned_abs());
    }
    Some(out)
}

/// When `x` lies exactly halfway between two shortest round-trip digit strings,
/// Rust picks the upper one and Python's `dtoa` the even one
/// (`2056655888558458.25` is `...458.3` in Rust, `...458.2` in Python). A tie
/// means the exact decimal value of `x` is `digits` with its last digit lowered
/// by one and a `5` appended, so it only arises when that last digit is odd.
fn round_tie_to_even(digits: &mut String, exp: i32, x: f64) {
    let Some(last) = digits.bytes().last() else {
        return;
    };
    if last % 2 == 0 || (digits.len() == 1 && last == b'1') {
        return;
    }
    let mut lowered = String::with_capacity(digits.len());
    lowered.push_str(digits.get(..digits.len() - 1).unwrap_or(""));
    lowered.push(char::from(last - 1));
    // Every finite f64 has at most 767 significant decimal digits, so this
    // precision prints its exact value.
    let mut exact = String::new();
    let _ = write!(exact, "{:.800e}", x.abs());
    let Some((exact_mantissa, exact_exp)) = exact.split_once('e') else {
        return;
    };
    if exact_exp.parse::<i32>().ok() != Some(exp) {
        return;
    }
    let exact_digits: String = exact_mantissa.chars().filter(|c| *c != '.').collect();
    let exact_digits = exact_digits.trim_end_matches('0');
    let is_tie = exact_digits.len() == lowered.len() + 1
        && exact_digits.starts_with(lowered.as_str())
        && exact_digits.ends_with('5');
    if !is_tie {
        return;
    }
    let mut candidate = String::new();
    let _ = write!(candidate, "{lowered}e{}", exp - (lowered.len() as i32 - 1));
    if candidate.parse::<f64>().ok() == Some(x.abs()) {
        *digits = lowered;
    }
}

/// Python SDK float payload text: `repr(float(x)).replace("e+", "e")`; `None`
/// when `x` is NaN or infinite.
///
/// Examples: `1.0`, `-0.0`, `0.0001`, `1e-05`, `1e16`, `5e-324`.
pub fn format_float(x: f64) -> Option<String> {
    let repr = repr_float(x)?;
    Some(repr.replacen("e+", "e", 1))
}

/// Python `json.dumps(value, sort_keys=sort_keys)` with default options.
///
/// Without `sort_keys`, objects keep `serde_json::Map` iteration order, which is
/// key order (a `BTreeMap`) because the `preserve_order` feature is off. Numbers
/// held as `f64` are written with Python float repr (`1e+16`, `-0.0`).
pub fn to_json(value: &Value, sort_keys: bool) -> String {
    let mut out = String::new();
    write_value(&mut out, value, sort_keys);
    out
}

/// Appends `value` to `out` exactly as Python `json.dumps` would write it.
pub fn write_value(out: &mut String, value: &Value, sort_keys: bool) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                let _ = write!(out, "{i}");
            } else if let Some(u) = n.as_u64() {
                let _ = write!(out, "{u}");
            } else if let Some(text) = n.as_f64().and_then(repr_float) {
                out.push_str(&text);
            } else {
                // Unreachable for serde_json values (no NaN or infinity); Python
                // writes these as `NaN` / `Infinity` / `-Infinity`.
                let f = n.as_f64().unwrap_or(f64::NAN);
                out.push_str(if f.is_nan() {
                    "NaN"
                } else if f > 0.0 {
                    "Infinity"
                } else {
                    "-Infinity"
                });
            }
        }
        Value::String(s) => write_str(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_value(out, item, sort_keys);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            let mut entries: alloc::vec::Vec<(&String, &Value)> = map.iter().collect();
            if sort_keys {
                // UTF-8 byte order equals code point order, as Python sorts.
                entries.sort_by(|a, b| a.0.cmp(b.0));
            }
            for (i, (key, item)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_str(out, key);
                out.push_str(": ");
                write_value(out, item, sort_keys);
            }
            out.push('}');
        }
    }
}

/// Appends `s` as a quoted JSON string, escaped like Python `json.dumps` with
/// `ensure_ascii=True`: only `0x20..=0x7e` pass through (except `"` and `\`),
/// everything else is `\uXXXX` (lowercase hex, astral chars as surrogate pairs),
/// with the short forms `\"`, `\\`, `\n`, `\r`, `\t`, `\b`, `\f`.
pub fn write_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            ' '..='~' => out.push(c),
            _ => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn floats() {
        let cases: &[(f64, &str)] = &[
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (100.0, "100.0"),
            (0.1, "0.1"),
            (0.0001, "0.0001"),
            (1e-5, "1e-05"),
            (1.5e-5, "1.5e-05"),
            (1e16, "1e16"),
            (1.5e16, "1.5e16"),
            (9999999999999998.0, "9999999999999998.0"),
            (123456789012345.6, "123456789012345.6"),
            (5e-324, "5e-324"),
            (1.7976931348623157e308, "1.7976931348623157e308"),
            (-1e-300, "-1e-300"),
            (1e100, "1e100"),
            (0.30000000000000004, "0.30000000000000004"),
            (f64::from_bits(0x431d_3a12_0ea6_a5e9), "2056655888558458.2"),
            (f64::from_bits(0xc2d9_4d11_000d_76c8), "-111275153569243.12"),
            (2.5, "2.5"),
        ];
        for (x, want) in cases {
            assert_eq!(format_float(*x).as_deref(), Some(*want), "{x:e}");
        }
        assert_eq!(repr_float(1e16).as_deref(), Some("1e+16"));
        assert_eq!(format_float(f64::NAN), None);
        assert_eq!(format_float(f64::INFINITY), None);
        assert_eq!(format_float(f64::NEG_INFINITY), None);
    }

    #[test]
    fn string_escapes() {
        let mut out = String::new();
        write_str(&mut out, "a\"b\\c/\n\r\t\u{08}\u{0c}\u{00}\u{1f}\u{7f}");
        assert_eq!(out, r#""a\"b\\c/\n\r\t\b\f\u0000\u001f\u007f""#);
        let mut out = String::new();
        write_str(&mut out, "\u{e9}\u{2028}\u{ffff}\u{1F600}\u{10ffff}");
        assert_eq!(out, "\"\\u00e9\\u2028\\uffff\\ud83d\\ude00\\udbff\\udfff\"");
    }

    #[test]
    fn values() {
        let v = json!({"b": [1, -2, 1.5, 1e16, -0.0, null, true, false], "a": {}, "c": []});
        assert_eq!(
            to_json(&v, true),
            r#"{"a": {}, "b": [1, -2, 1.5, 1e+16, -0.0, null, true, false], "c": []}"#
        );
        assert_eq!(to_json(&json!(u64::MAX), false), "18446744073709551615");
        assert_eq!(to_json(&json!(i64::MIN), false), "-9223372036854775808");
        assert_eq!(
            to_json(&json!({"z": {"y": 1, "x": 2}}), true),
            r#"{"z": {"x": 2, "y": 1}}"#
        );
    }
}
