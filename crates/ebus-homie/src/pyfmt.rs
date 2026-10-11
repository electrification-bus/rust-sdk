//! Python-compatible text output, so payloads and `$description` documents match
//! the Python SDK byte for byte.
//!
//! [`format_float`] is the Python SDK's float payload form, and
//! [`format_float32`] lays out an `f32`'s shortest digits the same way;
//! [`to_json`], [`write_str`] and [`write_value`] reproduce
//! `json.dumps` with its default options (`ensure_ascii=True`, separators `", "`
//! and `": "`); [`from_json`] reads JSON text as `json.loads` does.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::Write as _;

use serde_json::{Map, Number, Value};

/// Python `repr(x)` for a finite float; `None` when `x` is NaN or infinite.
///
/// Shortest round-trip digits, fixed notation when the decimal exponent `e`
/// satisfies `-4 <= e < 16`, otherwise `d[.ddd]e±XX` with at least two exponent
/// digits (`1e-05`, `1e+16`). This is the form `json.dumps` writes.
pub fn repr_float(x: f64) -> Option<String> {
    let mut out = String::new();
    write_f64(&mut out, x, true).then_some(out)
}

/// The float width whose shortest round-trip digits are being laid out.
#[derive(Clone, Copy)]
enum Width {
    F32,
    F64,
}

/// Fixed-capacity ASCII text, so laying out one float allocates nothing.
struct Digits<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> Digits<N> {
    const fn new() -> Self {
        Digits {
            buf: [0; N],
            len: 0,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        self.buf.get(..self.len).unwrap_or(&[])
    }

    fn as_str(&self) -> &str {
        core::str::from_utf8(self.as_bytes()).unwrap_or("")
    }

    /// Appends `b`; false when full.
    fn push(&mut self, b: u8) -> bool {
        match self.buf.get_mut(self.len) {
            Some(slot) => {
                *slot = b;
                self.len += 1;
                true
            }
            None => false,
        }
    }

    fn pop(&mut self) {
        self.len = self.len.saturating_sub(1);
    }
}

impl<const N: usize> core::fmt::Write for Digits<N> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        for b in s.bytes() {
            if !self.push(b) {
                return Err(core::fmt::Error);
            }
        }
        Ok(())
    }
}

/// Room for the `{:e}` text of any `f64` (at most 24 bytes) and for its
/// significant digits plus the tie digit.
type FloatText = Digits<32>;

/// Appends Python's layout of the finite `x` to `out`: with `plus`, a positive
/// exponent is written `e+16` (`repr`), otherwise `e16` (the payload form).
/// False, with `out` unchanged, when `x` is NaN or infinite.
fn write_f64(out: &mut String, x: f64, plus: bool) -> bool {
    if !x.is_finite() {
        return false;
    }
    // `{:e}` yields the shortest round-trip digits as `[-]d[.ddd]e[-]N`.
    let mut sci = FloatText::new();
    write!(sci, "{x:e}").is_ok() && layout(out, sci.as_str(), x, Width::F64, plus)
}

/// Appends the Python `repr` layout of `sci`, the shortest round-trip digits
/// of `x` as `{:e}` prints them for `width` (an `f32` widens to `x` exactly),
/// to `out`; see [`write_f64`] for `plus`. False, with `out` unchanged, when
/// `sci` is not in that form.
fn layout(out: &mut String, sci: &str, x: f64, width: Width, plus: bool) -> bool {
    let Some((mantissa, exp_text)) = sci.split_once('e') else {
        return false;
    };
    let Ok(exp) = exp_text.parse::<i32>() else {
        return false;
    };
    let (negative, mantissa) = match mantissa.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, mantissa),
    };
    let mut digits = FloatText::new();
    for b in mantissa.bytes().filter(|b| *b != b'.') {
        if !digits.push(b) {
            return false;
        }
    }
    round_tie_to_even(&mut digits, exp, x, width);
    let digits = digits.as_str();

    if negative {
        out.push('-');
    }
    if (-4..16).contains(&exp) {
        if exp >= 0 {
            // `exp` is in 0..16, so the cast is lossless.
            let int_len = exp as usize + 1;
            if digits.len() <= int_len {
                out.push_str(digits);
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
            out.push_str(digits);
        }
    } else {
        let (first, rest) = digits.split_at(1.min(digits.len()));
        out.push_str(first);
        if !rest.is_empty() {
            out.push('.');
            out.push_str(rest);
        }
        out.push('e');
        if exp < 0 {
            out.push('-');
        } else if plus {
            out.push('+');
        }
        let _ = write!(out, "{:02}", exp.unsigned_abs());
    }
    true
}

/// When `x` lies exactly halfway between two shortest round-trip digit strings,
/// Rust picks the upper one and Python's `dtoa` the even one
/// (`2056655888558458.25` is `...458.3` in Rust, `...458.2` in Python). A tie
/// means the exact decimal value of `x` is `digits` with its last digit lowered
/// by one and a `5` appended, so it only arises when that last digit is odd.
/// `f32` has ties too: the `f32` `2097152.25` is `2097152.3` in Rust,
/// `2097152.2` in numpy.
fn round_tie_to_even(digits: &mut FloatText, exp: i32, x: f64, width: Width) {
    let Some(&last) = digits.as_bytes().last() else {
        return;
    };
    if last % 2 == 0 || (digits.len == 1 && last == b'1') {
        return;
    }
    let lowered_len = digits.len;
    // The tie digits: `digits` with the last one lowered, then a `5`.
    let mut tie = FloatText::new();
    for &b in digits.as_bytes().get(..lowered_len - 1).unwrap_or(&[]) {
        tie.push(b);
    }
    tie.push(last - 1);
    if !tie.push(b'5') {
        return;
    }
    // Prefilter: `x` rounded to one more significant digit than `digits` must
    // print as the tie digits. Most values fail here, before the exact print.
    let mut pre = Digits::<48>::new();
    if write!(pre, "{:.*e}", lowered_len, x.abs()).is_err()
        || !same_digits(pre.as_str(), exp, tie.as_str())
    {
        return;
    }
    // Every finite f64 has at most 767 significant decimal digits, so this
    // precision prints its exact value; `d.`, 800 digits and `e-324` fit.
    let mut exact = Digits::<816>::new();
    if write!(exact, "{:.800e}", x.abs()).is_err()
        || !same_digits(exact.as_str(), exp, tie.as_str())
    {
        return;
    }
    tie.pop();
    let mut text = Digits::<48>::new();
    if write!(text, "{}e{}", tie.as_str(), exp - (tie.len as i32 - 1)).is_err() {
        return;
    }
    let text = text.as_str();
    let round_trips = match width {
        // `x` is an `f32` widened exactly, so the cast back is exact.
        Width::F32 => text.parse::<f32>().ok() == Some(x.abs() as f32),
        Width::F64 => text.parse::<f64>().ok() == Some(x.abs()),
    };
    if round_trips {
        *digits = tie;
    }
}

/// Whether `sci` (`d[.ddd]eN`, as `{:e}` prints) has exponent `exp` and,
/// ignoring trailing zeros, significant digits `digits` (which must not end in
/// `0`).
fn same_digits(sci: &str, exp: i32, digits: &str) -> bool {
    let Some((mantissa, exp_text)) = sci.split_once('e') else {
        return false;
    };
    if exp_text.parse::<i32>().ok() != Some(exp) {
        return false;
    }
    let mut want = digits.bytes();
    mantissa
        .trim_end_matches('0')
        .bytes()
        .filter(|&b| b != b'.')
        .all(|b| want.next() == Some(b))
        && want.next().is_none()
}

/// Python SDK float payload text: `repr(float(x)).replace("e+", "e")`; `None`
/// when `x` is NaN or infinite.
///
/// Examples: `1.0`, `-0.0`, `0.0001`, `1e-05`, `1e16`, `5e-324`.
pub fn format_float(x: f64) -> Option<String> {
    let mut out = String::new();
    write_float(&mut out, x).then_some(out)
}

/// Appends [`format_float`]'s text for `x` to `out`, allocating only when
/// `out` must grow; false, with `out` unchanged, when `x` is NaN or infinite.
pub fn write_float(out: &mut String, x: f64) -> bool {
    write_f64(out, x, false)
}

/// [`format_float`] for an `f32`: the shortest decimal that reads back as the
/// same `f32`, in the same layout; `None` when `x` is NaN or infinite.
///
/// Examples: `21.4` (not `21.399999618530273`), `40.0`, `-0.0`, `1e-05`,
/// `3.4028235e38`, `1e-45`.
pub fn format_float32(x: f32) -> Option<String> {
    let mut out = String::new();
    write_float32(&mut out, x).then_some(out)
}

/// Appends [`format_float32`]'s text for `x` to `out`, allocating only when
/// `out` must grow; false, with `out` unchanged, when `x` is NaN or infinite.
pub fn write_float32(out: &mut String, x: f32) -> bool {
    if !x.is_finite() {
        return false;
    }
    // `{:e}` on an `f32` yields the `f32` shortest round-trip digits.
    let mut sci = FloatText::new();
    write!(sci, "{x:e}").is_ok() && layout(out, sci.as_str(), f64::from(x), Width::F32, false)
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

/// Most arrays and objects [`from_json`] accepts nested inside one another.
/// Python's limit is its recursion limit (about 1000, less the caller's stack
/// depth); this one bounds the stack the parse, `Drop` and [`to_json`] use.
pub const MAX_JSON_DEPTH: usize = 128;

/// Parse JSON text as Python `json.loads` does, or `None` when it is not JSON.
///
/// Floats are correctly rounded, so Python's `repr` output parses to the same
/// f64. A number with no fraction or exponent is an integer `Value` (`-0` is
/// `0`, as Python makes it). `NaN`, `Infinity` and `-Infinity` are rejected, as
/// in the Python SDK. Rejected where Python accepts, because a `Value` cannot
/// hold the result: an integer outside `i64` and `u64`, a float that overflows
/// to an infinity (`1e400`), a lone surrogate escape (`"\ud800"`), and nesting
/// deeper than [`MAX_JSON_DEPTH`]. Objects keep `serde_json::Map` order (key
/// order); a repeated key keeps its last value, as in Python.
pub fn from_json(text: &str) -> Option<Value> {
    let mut parser = JsonParser {
        text,
        bytes: text.as_bytes(),
        pos: 0,
    };
    parser.skip_ws();
    let value = parser.value(0)?;
    parser.skip_ws();
    (parser.pos == parser.bytes.len()).then_some(value)
}

/// A JSON value read by [`from_json_ordered`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Ordered {
    /// An object's members in document order.
    Object(Vec<(String, Ordered)>),
    /// Any other value, or an object nested too deep to keep its order.
    Value(Value),
}

impl Ordered {
    /// The value with every object as a `serde_json::Map` (key order).
    pub(crate) fn into_value(self) -> Value {
        match self {
            Ordered::Object(items) => Value::Object(
                items
                    .into_iter()
                    .map(|(k, v)| (k, v.into_value()))
                    .collect(),
            ),
            Ordered::Value(v) => v,
        }
    }
}

/// [`from_json`], except that objects nested no deeper than `levels` (the top
/// level is 1) keep their members in document order. A repeated key keeps its
/// last value at the position of its first occurrence, as a Python `dict` does.
pub(crate) fn from_json_ordered(text: &str, levels: usize) -> Option<Ordered> {
    let mut parser = JsonParser {
        text,
        bytes: text.as_bytes(),
        pos: 0,
    };
    parser.skip_ws();
    let value = parser.ordered(0, levels)?;
    parser.skip_ws();
    (parser.pos == parser.bytes.len()).then_some(value)
}

struct JsonParser<'a> {
    text: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl JsonParser<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    /// Python's JSON whitespace: space, tab, newline, carriage return.
    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> Option<()> {
        (self.peek() == Some(byte)).then(|| self.pos += 1)
    }

    fn literal(&mut self, word: &str, value: Value) -> Option<Value> {
        let end = self.pos.checked_add(word.len())?;
        (self.bytes.get(self.pos..end) == Some(word.as_bytes())).then(|| {
            self.pos = end;
            value
        })
    }

    fn value(&mut self, depth: usize) -> Option<Value> {
        match self.peek()? {
            b'{' => self.object(depth + 1),
            b'[' => self.array(depth + 1),
            b'"' => self.string().map(Value::String),
            b't' => self.literal("true", Value::Bool(true)),
            b'f' => self.literal("false", Value::Bool(false)),
            b'n' => self.literal("null", Value::Null),
            b'-' | b'0'..=b'9' => self.number(),
            _ => None,
        }
    }

    fn array(&mut self, depth: usize) -> Option<Value> {
        if depth > MAX_JSON_DEPTH {
            return None;
        }
        self.pos += 1;
        let mut items = Vec::new();
        self.skip_ws();
        if self.eat(b']').is_some() {
            return Some(Value::Array(items));
        }
        loop {
            items.push(self.value(depth)?);
            self.skip_ws();
            match self.peek()? {
                b',' => {
                    self.pos += 1;
                    self.skip_ws();
                }
                b']' => {
                    self.pos += 1;
                    return Some(Value::Array(items));
                }
                _ => return None,
            }
        }
    }

    fn object(&mut self, depth: usize) -> Option<Value> {
        let mut map = Map::new();
        self.members(depth, |parser, key| {
            let item = parser.value(depth)?;
            map.insert(key, item);
            Some(())
        })?;
        Some(Value::Object(map))
    }

    /// The members of the object at the opening brace, each passed to `member`
    /// with its key once the parser is at the start of its value.
    fn members(
        &mut self,
        depth: usize,
        mut member: impl FnMut(&mut Self, String) -> Option<()>,
    ) -> Option<()> {
        if depth > MAX_JSON_DEPTH {
            return None;
        }
        self.pos += 1;
        self.skip_ws();
        if self.eat(b'}').is_some() {
            return Some(());
        }
        loop {
            if self.peek()? != b'"' {
                return None;
            }
            let key = self.string()?;
            self.skip_ws();
            self.eat(b':')?;
            self.skip_ws();
            member(self, key)?;
            self.skip_ws();
            match self.peek()? {
                b',' => {
                    self.pos += 1;
                    self.skip_ws();
                }
                b'}' => {
                    self.pos += 1;
                    return Some(());
                }
                _ => return None,
            }
        }
    }

    /// A value whose objects keep document order while they are nested no
    /// deeper than `levels`.
    fn ordered(&mut self, depth: usize, levels: usize) -> Option<Ordered> {
        if self.peek()? != b'{' || depth >= levels {
            return self.value(depth).map(Ordered::Value);
        }
        let depth = depth + 1;
        let mut items: Vec<(String, Ordered)> = Vec::new();
        let mut index: BTreeMap<String, usize> = BTreeMap::new();
        self.members(depth, |parser, key| {
            let item = parser.ordered(depth, levels)?;
            match index.get(&key).and_then(|&i| items.get_mut(i)) {
                Some(slot) => slot.1 = item,
                None => {
                    index.insert(key.clone(), items.len());
                    items.push((key, item));
                }
            }
            Some(())
        })?;
        Some(Ordered::Object(items))
    }

    /// The string starting at the opening quote (Python's strict mode: no raw
    /// control characters).
    fn string(&mut self) -> Option<String> {
        self.pos += 1;
        let mut out = String::new();
        loop {
            let start = self.pos;
            while matches!(self.peek(), Some(b) if b != b'"' && b != b'\\' && b >= 0x20) {
                self.pos += 1;
            }
            // The run ends at an ASCII byte, so it is a char boundary.
            out.push_str(self.text.get(start..self.pos)?);
            match self.peek()? {
                b'"' => {
                    self.pos += 1;
                    return Some(out);
                }
                b'\\' => {
                    self.pos += 1;
                    let escape = self.peek()?;
                    self.pos += 1;
                    out.push(match escape {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{08}',
                        b'f' => '\u{0c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => self.unicode_escape()?,
                        _ => return None,
                    });
                }
                _ => return None,
            }
        }
    }

    /// The char of a `\uXXXX` escape (after the `u`), joining a surrogate pair.
    fn unicode_escape(&mut self) -> Option<char> {
        let unit = self.hex4()?;
        match unit {
            0xd800..=0xdbff => {
                self.eat(b'\\')?;
                self.eat(b'u')?;
                let low = self.hex4()?;
                if !(0xdc00..=0xdfff).contains(&low) {
                    return None;
                }
                char::from_u32(0x10000 + ((unit - 0xd800) << 10) + (low - 0xdc00))
            }
            _ => char::from_u32(unit),
        }
    }

    fn hex4(&mut self) -> Option<u32> {
        let end = self.pos.checked_add(4)?;
        let digits = self.bytes.get(self.pos..end)?;
        let mut unit = 0u32;
        for &b in digits {
            unit = unit * 16 + char::from(b).to_digit(16)?;
        }
        self.pos = end;
        Some(unit)
    }

    /// `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][-+]?[0-9]+)?`, as Python's scanner
    /// reads it: a float when there is a fraction or exponent, else an integer.
    fn number(&mut self) -> Option<Value> {
        let start = self.pos;
        let negative = self.eat(b'-').is_some();
        match self.peek()? {
            b'0' => self.pos += 1,
            b'1'..=b'9' => self.digits(),
            _ => return None,
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') && matches!(self.bytes.get(self.pos + 1), Some(b'0'..=b'9')) {
            self.pos += 1;
            self.digits();
            is_float = true;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            let mut after = self.pos + 1;
            if matches!(self.bytes.get(after), Some(b'+' | b'-')) {
                after += 1;
            }
            if matches!(self.bytes.get(after), Some(b'0'..=b'9')) {
                self.pos = after;
                self.digits();
                is_float = true;
            }
        }
        let text = self.text.get(start..self.pos)?;
        if is_float {
            let x: f64 = text.parse().ok()?;
            Number::from_f64(x).map(Value::Number)
        } else if negative {
            text.parse::<i64>().ok().map(Value::from)
        } else {
            text.parse::<u64>().ok().map(Value::from)
        }
    }

    fn digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
    }
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
    fn write_float_appends_and_leaves_out_unchanged_on_failure() {
        let mut out = String::from("x=");
        assert!(write_float(&mut out, 1e16));
        assert_eq!(out, "x=1e16");
        assert!(!write_float(&mut out, f64::NAN));
        assert!(!write_float32(&mut out, f32::INFINITY));
        assert_eq!(out, "x=1e16");
        assert!(write_float32(&mut out, 21.4));
        assert_eq!(out, "x=1e1621.4");
        // Extremes fit the fixed buffers.
        for x in [
            f64::MIN,
            f64::MIN_POSITIVE,
            -5e-324,
            -2.2250738585072014e-308,
        ] {
            out.clear();
            assert!(write_float(&mut out, x));
            assert_eq!(Some(out.as_str()), format_float(x).as_deref());
        }
    }

    #[test]
    fn float32_payload_form() {
        let cases: &[(f32, &str)] = &[
            (21.4, "21.4"),
            (40.0, "40.0"),
            (-0.0, "-0.0"),
            (0.1, "0.1"),
            (1e-5, "1e-05"),
            (0.0001, "0.0001"),
            (1e16, "1e16"),
            (123456789.0, "123456790.0"),
            (f32::MAX, "3.4028235e38"),
            (f32::MIN_POSITIVE, "1.1754944e-38"),
            (f32::from_bits(1), "1e-45"),
            // 2097152.25 and 2097153.75: exact ties, rounded to even.
            (f32::from_bits(0x4a00_0001), "2097152.2"),
            (f32::from_bits(0x4a00_0007), "2097153.8"),
        ];
        for (x, want) in cases {
            assert_eq!(format_float32(*x).as_deref(), Some(*want), "{x:e}");
        }
        assert_eq!(format_float32(f32::NAN), None);
        assert_eq!(format_float32(f32::INFINITY), None);
        assert_eq!(format_float32(f32::NEG_INFINITY), None);
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

    #[test]
    fn from_json_accepts_what_python_does() {
        let cases: &[(&str, &str)] = &[
            (" [1, 2] ", "[1, 2]"),
            ("\t{\"b\" : 1 ,\n\"a\":2}\r", r#"{"a": 2, "b": 1}"#),
            (r#"{"a": 1, "a": 2}"#, r#"{"a": 2}"#),
            (
                "[-0, 0, -0.0, 1.0, 1E2, 1e-400]",
                "[0, 0, -0.0, 1.0, 100.0, 0.0]",
            ),
            (
                "[18446744073709551615, -9223372036854775808]",
                "[18446744073709551615, -9223372036854775808]",
            ),
            ("[912068.5437784989]", "[912068.5437784989]"),
            (
                "[0.1, 5e-324, 1.7976931348623157e308]",
                "[0.1, 5e-324, 1.7976931348623157e+308]",
            ),
            (
                r#"["\u00e9\ud83d\ude00\/\b\f\n\r\t\"\\", "\u0000"]"#,
                r#"["\u00e9\ud83d\ude00/\b\f\n\r\t\"\\", "\u0000"]"#,
            ),
            ("[\"caf\u{e9} \u{1F600}\"]", r#"["caf\u00e9 \ud83d\ude00"]"#),
            ("\"s\"", "\"s\""),
            ("true", "true"),
            ("null", "null"),
        ];
        for (input, want) in cases {
            let value = from_json(input).unwrap_or_else(|| panic!("{input}"));
            assert_eq!(to_json(&value, false), *want, "{input}");
        }
    }

    #[test]
    fn from_json_rejects() {
        for input in [
            "",
            " ",
            "[",
            "[1,]",
            "{\"a\":1,}",
            "{a: 1}",
            "['a']",
            "[01]",
            "[1.]",
            "[.5]",
            "[1e]",
            "[+1]",
            "[-]",
            "NaN",
            "[Infinity]",
            "[-Infinity]",
            "[1] x",
            "\u{feff}[1]",
            "[\"\t\"]",
            "[\"\\x\"]",
            "[\"\\u12\"]",
            "[\"\\u12g4\"]",
            "[\"a]",
            "[tru]",
            "[1 2]",
            "{\"a\" 1}",
            "[1e400]",
            "[-1e400]",
            "[18446744073709551616]",
            "[-9223372036854775809]",
            "[\"\\ud800\"]",
            "[\"\\udc00\"]",
            "[\"\\ud800\\u0041\"]",
            "[\u{a0}1]",
        ] {
            assert_eq!(from_json(input), None, "{input:?}");
        }
    }

    #[test]
    fn from_json_depth_limit() {
        let nested = |n: usize| {
            let mut s = "[".repeat(n);
            s.push_str(&"]".repeat(n));
            s
        };
        assert!(from_json(&nested(MAX_JSON_DEPTH)).is_some());
        assert_eq!(from_json(&nested(MAX_JSON_DEPTH + 1)), None);
        let objects = format!(
            "{}1{}",
            "{\"a\":".repeat(MAX_JSON_DEPTH),
            "}".repeat(MAX_JSON_DEPTH)
        );
        assert!(from_json(&objects).is_some());
        assert_eq!(from_json(&nested(100_000)), None);
        for levels in [0, 2, MAX_JSON_DEPTH + 5] {
            assert!(from_json_ordered(&objects, levels).is_some());
            let deeper = format!("[{objects}]");
            assert_eq!(from_json_ordered(&deeper, levels), None);
        }
    }

    fn keys(o: &Ordered) -> Vec<&str> {
        match o {
            Ordered::Object(items) => items.iter().map(|(k, _)| k.as_str()).collect(),
            Ordered::Value(_) => Vec::new(),
        }
    }

    #[test]
    fn from_json_ordered_keeps_document_order() {
        let text = r#" {"z": {"y": {"b": 1, "a": 2}, "x": 0}, "a": [{"d": 1, "c": 2}], "m": 1.1} "#;
        let o = from_json_ordered(text, 2).unwrap();
        assert_eq!(keys(&o), ["z", "a", "m"]);
        let Ordered::Object(items) = &o else {
            panic!("not an object")
        };
        assert_eq!(keys(&items[0].1), ["y", "x"]);
        // Level 3 and objects inside arrays are plain values.
        let Ordered::Object(inner) = &items[0].1 else {
            panic!("not an object")
        };
        assert_eq!(inner[0].1, Ordered::Value(json!({"a": 2, "b": 1})));
        assert_eq!(items[2].1, Ordered::Value(json!(1.1)));
        assert_eq!(o.into_value(), from_json(text).unwrap());
        assert_eq!(
            from_json_ordered("[1]", 3),
            Some(Ordered::Value(json!([1])))
        );
        assert_eq!(from_json_ordered("{} x", 3), None);
        assert_eq!(from_json_ordered(r#"{"a" 1}"#, 3), None);
    }

    #[test]
    fn from_json_ordered_repeated_key_keeps_last_value_first_position() {
        // Python: json.loads('{"b": 1, "a": 2, "b": {"y": 3, "x": 4}}')
        // == {'b': {'y': 3, 'x': 4}, 'a': 2}
        let o = from_json_ordered(r#"{"b": 1, "a": 2, "b": {"y": 3, "x": 4}}"#, 2).unwrap();
        assert_eq!(
            o,
            Ordered::Object(vec![
                (
                    "b".into(),
                    Ordered::Object(vec![
                        ("y".into(), Ordered::Value(json!(3))),
                        ("x".into(), Ordered::Value(json!(4))),
                    ])
                ),
                ("a".into(), Ordered::Value(json!(2))),
            ])
        );
    }

    /// The tie check without the prefilter (exact print every time).
    fn reference_tie(digits: &mut String, exp: i32, x: f64) {
        let Some(last) = digits.bytes().last() else {
            return;
        };
        if last % 2 == 0 || (digits.len() == 1 && last == b'1') {
            return;
        }
        let mut lowered = String::from(&digits[..digits.len() - 1]);
        lowered.push(char::from(last - 1));
        let exact = alloc::format!("{:.800e}", x.abs());
        let (mantissa, exact_exp) = exact.split_once('e').unwrap();
        if exact_exp.parse::<i32>().ok() != Some(exp) {
            return;
        }
        let exact_digits: String = mantissa.chars().filter(|c| *c != '.').collect();
        let exact_digits = exact_digits.trim_end_matches('0');
        if exact_digits.len() == lowered.len() + 1
            && exact_digits.starts_with(lowered.as_str())
            && exact_digits.ends_with('5')
        {
            let candidate = alloc::format!("{lowered}e{}", exp - (lowered.len() as i32 - 1));
            if candidate.parse::<f64>().ok() == Some(x.abs()) {
                *digits = lowered;
            }
        }
    }

    #[test]
    fn tie_prefilter_matches_exact_check() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut xs: alloc::vec::Vec<f64> = (0..30_000)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                f64::from_bits(state)
            })
            .filter(|x| x.is_finite())
            .collect();
        xs.extend((0..30_000).map(|i| f64::from(i) * 0.37 + 0.5));
        // Exact ties: values with few significant bits.
        xs.extend((1..5_000).map(|i| f64::from(i) / 8.0 + 2_056_655_888_558_458.0));
        xs.push(f64::from_bits(0x431d_3a12_0ea6_a5e9));
        xs.push(f64::from_bits(0xc2d9_4d11_000d_76c8));
        let mut ties = 0;
        for x in xs {
            let sci = alloc::format!("{x:e}");
            let (mantissa, exp) = sci.split_once('e').unwrap();
            let exp: i32 = exp.parse().unwrap();
            let shortest: String = mantissa.chars().filter(char::is_ascii_digit).collect();
            let mut got = FloatText::new();
            for b in shortest.bytes() {
                assert!(got.push(b));
            }
            let mut want = shortest.clone();
            round_tie_to_even(&mut got, exp, x, Width::F64);
            reference_tie(&mut want, exp, x);
            assert_eq!(got.as_str(), want, "{x:e}");
            ties += usize::from(got.as_str() != shortest);
        }
        assert!(ties > 0);
    }
}
