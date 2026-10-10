//! Property datatypes and payload grammar (convention.md: Payload).
//!
//! The grammar checks need no allocation. `parse_set_payload`, `check_value`,
//! the exact decimal arithmetic behind step rounding, and the encoders need
//! `alloc`.
//! Every rule mirrors the Python SDK (`ebus_sdk.homie`), and every [`Problem`]
//! code is the Python reason string.

use core::fmt;

#[cfg(feature = "alloc")]
use alloc::string::{String, ToString};
#[cfg(feature = "alloc")]
use alloc::vec::Vec;
#[cfg(feature = "alloc")]
use core::cmp::Ordering;
#[cfg(feature = "alloc")]
use num_bigint::{BigInt, BigUint, Sign};
#[cfg(feature = "alloc")]
use num_integer::Integer;
#[cfg(feature = "alloc")]
use num_traits::{One, Signed, Zero};
#[cfg(feature = "alloc")]
use serde_json::Value;

#[cfg(feature = "alloc")]
use crate::description::PropertyDescription;

/// A Homie 5 property datatype.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Datatype {
    /// `integer`: a 64-bit signed integer.
    Integer,
    /// `float`: a 64-bit floating point number.
    Float,
    /// `boolean`: `true` or `false`.
    Boolean,
    /// `string`: UTF-8 text.
    String,
    /// `enum`: one of the comma-separated values in `$format`.
    Enum,
    /// `color`: `rgb`, `hsv` or `xyz` components.
    Color,
    /// `datetime`: an ISO 8601 date or date-time.
    Datetime,
    /// `duration`: an ISO 8601 duration `PTxHxMxS`.
    Duration,
    /// `json`: a JSON array or object.
    Json,
}

impl Datatype {
    /// The datatype's name as it appears in `$description`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Datatype::Integer => "integer",
            Datatype::Float => "float",
            Datatype::Boolean => "boolean",
            Datatype::String => "string",
            Datatype::Enum => "enum",
            Datatype::Color => "color",
            Datatype::Datetime => "datetime",
            Datatype::Duration => "duration",
            Datatype::Json => "json",
        }
    }

    /// The datatype named `name`, or `None` for a name Homie 5 does not define.
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "integer" => Datatype::Integer,
            "float" => Datatype::Float,
            "boolean" => Datatype::Boolean,
            "string" => Datatype::String,
            "enum" => Datatype::Enum,
            "color" => Datatype::Color,
            "datetime" => Datatype::Datetime,
            "duration" => Datatype::Duration,
            "json" => Datatype::Json,
            _ => return None,
        })
    }
}

/// Why a value or payload is not valid for a datatype.
///
/// [`Problem::as_str`] is the Python SDK's reason string. Some variants exist
/// only so every Python reason has a counterpart; the Rust encoders, being
/// typed, never produce the `*Type` ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Problem {
    /// A boolean given for a numeric datatype.
    BooleanOnNumeric,
    /// Not an `integer` payload, or outside the 64-bit range.
    IntegerGrammar,
    /// A non-whole number given for `integer`.
    IntegerNotWhole,
    /// A value of a type `integer` cannot hold.
    IntegerType,
    /// An integer outside the 64-bit signed range; [`parse_set_payload`]
    /// returns it when step rounding leaves that range (convention.md: Integer).
    IntegerOutOf64Bit,
    /// Step rounding produced a non-whole `integer` (convention.md: Payload).
    IntegerStepNotWhole,
    /// Not a `float` payload, or not finite.
    FloatGrammar,
    /// NaN or an infinity given for `float`.
    FloatNotFinite,
    /// A number beyond the `float` range.
    FloatOutOfRange,
    /// A value of a type `float` cannot hold.
    FloatType,
    /// Below the `$format` minimum.
    BelowMin,
    /// Above the `$format` maximum.
    AboveMax,
    /// Not `true` or `false`.
    BooleanGrammar,
    /// A value of a type `boolean` cannot hold.
    BooleanType,
    /// An empty `enum` value.
    EnumEmpty,
    /// Not one of the `$format` values.
    EnumNotInFormat,
    /// The color type is not `rgb`, `hsv` or `xyz`, or the value is not a color.
    ColorType,
    /// The color type is not listed in `$format`.
    ColorTypeNotInFormat,
    /// Wrong number of color components for the color type.
    ColorComponentCount,
    /// A color component is not a `float` payload.
    ColorComponentGrammar,
    /// A color component is outside its range.
    ColorComponentRange,
    /// Bare components given but `$format` names no color type.
    ColorFormatMissing,
    /// Not an ISO 8601 date or date-time.
    DatetimeGrammar,
    /// A date or time field out of range.
    DatetimeRange,
    /// A value of a type `datetime` cannot hold.
    DatetimeType,
    /// A negative duration.
    DurationNegative,
    /// Not a `PTxHxMxS` duration.
    DurationGrammar,
    /// A value of a type `duration` cannot hold.
    DurationType,
    /// Not valid JSON.
    JsonInvalid,
    /// JSON whose top level is not an array or object.
    JsonNotArrayOrObject,
    /// A `$format` step or bound needs more decimal scale than
    /// [`MAX_FORMAT_SCALE`] for exact step rounding (no Python counterpart).
    FormatScale,
}

impl Problem {
    /// camelCase reason code, equal to the Python SDK's.
    pub const fn as_str(self) -> &'static str {
        match self {
            Problem::BooleanOnNumeric => "booleanOnNumeric",
            Problem::IntegerGrammar => "integerGrammar",
            Problem::IntegerNotWhole => "integerNotWhole",
            Problem::IntegerType => "integerType",
            Problem::IntegerOutOf64Bit => "integerOutOf64Bit",
            Problem::IntegerStepNotWhole => "integerStepNotWhole",
            Problem::FloatGrammar => "floatGrammar",
            Problem::FloatNotFinite => "floatNotFinite",
            Problem::FloatOutOfRange => "floatOutOfRange",
            Problem::FloatType => "floatType",
            Problem::BelowMin => "belowMin",
            Problem::AboveMax => "aboveMax",
            Problem::BooleanGrammar => "booleanGrammar",
            Problem::BooleanType => "booleanType",
            Problem::EnumEmpty => "enumEmpty",
            Problem::EnumNotInFormat => "enumNotInFormat",
            Problem::ColorType => "colorType",
            Problem::ColorTypeNotInFormat => "colorTypeNotInFormat",
            Problem::ColorComponentCount => "colorComponentCount",
            Problem::ColorComponentGrammar => "colorComponentGrammar",
            Problem::ColorComponentRange => "colorComponentRange",
            Problem::ColorFormatMissing => "colorFormatMissing",
            Problem::DatetimeGrammar => "datetimeGrammar",
            Problem::DatetimeRange => "datetimeRange",
            Problem::DatetimeType => "datetimeType",
            Problem::DurationNegative => "durationNegative",
            Problem::DurationGrammar => "durationGrammar",
            Problem::DurationType => "durationType",
            Problem::JsonInvalid => "jsonInvalid",
            Problem::JsonNotArrayOrObject => "jsonNotArrayOrObject",
            Problem::FormatScale => "formatScale",
        }
    }
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Problem {}

// ── Grammar checks (no allocation) ──────────────────────────────────────────

fn digit_run(s: &[u8], i: usize) -> usize {
    s.get(i..).map_or(0, |rest| {
        rest.iter().take_while(|b| b.is_ascii_digit()).count()
    })
}

/// The pieces of a payload matching the Python `_FLOAT_PAYLOAD` grammar
/// `-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE]-?\d+)?`.
#[derive(Clone, Copy)]
#[cfg_attr(not(feature = "alloc"), allow(dead_code))]
struct FloatParts<'a> {
    negative: bool,
    int: &'a [u8],
    frac: &'a [u8],
    exp_negative: bool,
    exp: &'a [u8],
}

fn scan_float(text: &str) -> Option<FloatParts<'_>> {
    let s = text.as_bytes();
    let mut i = 0;
    let negative = s.first() == Some(&b'-');
    if negative {
        i += 1;
    }
    let n_int = digit_run(s, i);
    let int = s.get(i..i + n_int)?;
    i += n_int;
    let mut frac: &[u8] = &[];
    if s.get(i) == Some(&b'.') {
        let n_frac = digit_run(s, i + 1);
        frac = s.get(i + 1..i + 1 + n_frac)?;
        i += 1 + n_frac;
    }
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    let mut exp_negative = false;
    let mut exp: &[u8] = &[];
    if matches!(s.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        let neg = s.get(j) == Some(&b'-');
        if neg {
            j += 1;
        }
        let n_exp = digit_run(s, j);
        if n_exp > 0 {
            exp_negative = neg;
            exp = s.get(j..j + n_exp)?;
            i = j + n_exp;
        }
    }
    (i == s.len()).then_some(FloatParts {
        negative,
        int,
        frac,
        exp_negative,
        exp,
    })
}

/// `text` matches the Python `_FLOAT_PAYLOAD` grammar (finite or not).
fn matches_float_grammar(text: &str) -> bool {
    scan_float(text).is_some()
}

/// `text` is an `integer` payload `-?[0-9]+` (convention.md: Integer). The
/// 64-bit range is not checked.
pub fn is_integer_payload(text: &str) -> bool {
    let digits = text.strip_prefix('-').unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// `text` is a `float` payload (convention.md: Float) whose value is finite as
/// an f64: `1e999` is rejected, as in the Python SDK.
pub fn is_float_payload(text: &str) -> bool {
    matches_float_grammar(text) && text.parse::<f64>().is_ok_and(f64::is_finite)
}

const COLOR_RANGES: [(&str, &[f64]); 3] = [
    ("rgb", &[255.0, 255.0, 255.0]),
    ("hsv", &[360.0, 100.0, 100.0]),
    ("xyz", &[1.0, 1.0]),
];

/// Check a `color` payload (convention.md: Color) against the color types
/// `format` lists. A `None` or empty `format` allows every type.
pub fn check_color(text: &str, format: Option<&str>) -> Result<(), Problem> {
    let mut parts = text.split(',');
    let kind = parts.next().unwrap_or("");
    let ranges = COLOR_RANGES
        .iter()
        .find(|(name, _)| *name == kind)
        .map(|(_, ranges)| *ranges)
        .ok_or(Problem::ColorType)?;
    if let Some(format) = format.filter(|f| !f.is_empty()) {
        if !format.split(',').any(|allowed| allowed == kind) {
            return Err(Problem::ColorTypeNotInFormat);
        }
    }
    if parts.clone().count() != ranges.len() {
        return Err(Problem::ColorComponentCount);
    }
    for (component, &upper) in parts.zip(ranges) {
        if !matches_float_grammar(component) {
            return Err(Problem::ColorComponentGrammar);
        }
        let value = component.parse::<f64>().unwrap_or(f64::NAN);
        if !(0.0..=upper).contains(&value) {
            return Err(Problem::ColorComponentRange);
        }
    }
    Ok(())
}

/// `text` is a `duration` payload `PT[nH][nM][n[.n]S]` other than a bare `PT`
/// (convention.md: Duration).
pub fn is_duration_payload(text: &str) -> bool {
    let Some(rest) = text.strip_prefix("PT") else {
        return false;
    };
    if rest.is_empty() {
        return false;
    }
    let s = rest.as_bytes();
    let mut i = 0;
    // 0: nothing yet, 1: after H, 2: after M, 3: after S.
    let mut stage = 0;
    while i < s.len() {
        let n = digit_run(s, i);
        if n == 0 {
            return false;
        }
        i += n;
        match s.get(i) {
            Some(b'H') if stage < 1 => stage = 1,
            Some(b'M') if stage < 2 => stage = 2,
            Some(b'S') if stage < 3 => stage = 3,
            Some(b'.') if stage < 3 => {
                let f = digit_run(s, i + 1);
                if f == 0 {
                    return false;
                }
                i += 1 + f;
                if s.get(i) != Some(&b'S') {
                    return false;
                }
                stage = 3;
            }
            _ => return false,
        }
        i += 1;
    }
    true
}

#[derive(Clone, Copy, Default)]
struct DatetimeFields {
    year: u32,
    month: Option<u32>,
    day: Option<u32>,
    bmonth: Option<u32>,
    bday: Option<u32>,
    ordinal: Option<u32>,
    week: Option<u32>,
    weekday: Option<u32>,
    hour: Option<u32>,
    minute: Option<u32>,
    second: Option<u32>,
    zhour: Option<u32>,
    zminute: Option<u32>,
    /// The decimal fraction of the time has a nonzero digit.
    fraction_nonzero: bool,
}

/// The value of exactly `n` ASCII digits at `s[i..]`.
fn fixed_digits(s: &[u8], i: usize, n: usize) -> Option<u32> {
    let run = s.get(i..i.checked_add(n)?)?;
    run.iter().try_fold(0u32, |acc, &b| {
        b.is_ascii_digit().then(|| acc * 10 + u32::from(b - b'0'))
    })
}

/// Matches the optional time group of `_DATETIME_PAYLOAD` at `i` and then the
/// end of input. Each optional piece is decided by its first byte, so greedy
/// matching never needs to backtrack here.
fn match_datetime_tail(s: &[u8], mut i: usize, f: &mut DatetimeFields) -> bool {
    if i == s.len() {
        return true;
    }
    if s.get(i) != Some(&b'T') {
        return false;
    }
    let mut t = *f;
    i += 1;
    let Some(hour) = fixed_digits(s, i, 2) else {
        return false;
    };
    t.hour = Some(hour);
    i += 2;
    let skip_colon = |i: usize| if s.get(i) == Some(&b':') { i + 1 } else { i };
    let j = skip_colon(i);
    if let Some(minute) = fixed_digits(s, j, 2) {
        t.minute = Some(minute);
        i = j + 2;
        let j = skip_colon(i);
        if let Some(second) = fixed_digits(s, j, 2) {
            t.second = Some(second);
            i = j + 2;
        }
    }
    if matches!(s.get(i), Some(b'.' | b',')) {
        let n = digit_run(s, i + 1);
        if n > 0 {
            t.fraction_nonzero = s
                .get(i + 1..i + 1 + n)
                .is_some_and(|d| d.iter().any(|&b| b != b'0'));
            i += 1 + n;
        }
    }
    match s.get(i) {
        Some(b'Z') => i += 1,
        Some(b'+' | b'-') => {
            if let Some(zhour) = fixed_digits(s, i + 1, 2) {
                t.zhour = Some(zhour);
                i += 3;
                let j = skip_colon(i);
                if let Some(zminute) = fixed_digits(s, j, 2) {
                    t.zminute = Some(zminute);
                    i = j + 2;
                }
            }
        }
        _ => {}
    }
    if i == s.len() {
        *f = t;
        true
    } else {
        false
    }
}

/// Python `re.fullmatch(_DATETIME_PAYLOAD, text)`, trying the alternatives in
/// the regex's order and returning the groups of the first full match.
fn match_datetime(text: &str) -> Option<DatetimeFields> {
    let s = text.as_bytes();
    let year = fixed_digits(s, 0, 4)?;
    let base = DatetimeFields {
        year,
        ..DatetimeFields::default()
    };
    let at = |i: usize| s.get(i).copied();
    let attempt = |end: usize, fields: DatetimeFields| {
        let mut fields = fields;
        match_datetime_tail(s, end, &mut fields).then_some(fields)
    };

    // -MM(-DD)?
    if at(4) == Some(b'-') {
        if let Some(month) = fixed_digits(s, 5, 2) {
            let with_month = DatetimeFields {
                month: Some(month),
                ..base
            };
            if at(7) == Some(b'-') {
                if let Some(day) = fixed_digits(s, 8, 2) {
                    let fields = DatetimeFields {
                        day: Some(day),
                        ..with_month
                    };
                    if let Some(found) = attempt(10, fields) {
                        return Some(found);
                    }
                }
            }
            if let Some(found) = attempt(7, with_month) {
                return Some(found);
            }
        }
    }
    // MMDD
    if let (Some(bmonth), Some(bday)) = (fixed_digits(s, 4, 2), fixed_digits(s, 6, 2)) {
        let fields = DatetimeFields {
            bmonth: Some(bmonth),
            bday: Some(bday),
            ..base
        };
        if let Some(found) = attempt(8, fields) {
            return Some(found);
        }
    }
    let dash = if at(4) == Some(b'-') { 5 } else { 4 };
    // -?DDD
    if let Some(ordinal) = fixed_digits(s, dash, 3) {
        let fields = DatetimeFields {
            ordinal: Some(ordinal),
            ..base
        };
        if let Some(found) = attempt(dash + 3, fields) {
            return Some(found);
        }
    }
    // -?Www(-?D)?
    if at(dash) == Some(b'W') {
        if let Some(week) = fixed_digits(s, dash + 1, 2) {
            let with_week = DatetimeFields {
                week: Some(week),
                ..base
            };
            let k = dash + 3;
            let kd = if at(k) == Some(b'-') { k + 1 } else { k };
            if let Some(weekday) = fixed_digits(s, kd, 1) {
                let fields = DatetimeFields {
                    weekday: Some(weekday),
                    ..with_week
                };
                if let Some(found) = attempt(kd + 1, fields) {
                    return Some(found);
                }
            }
            if let Some(found) = attempt(k, with_week) {
                return Some(found);
            }
        }
    }
    attempt(4, base)
}

const fn is_leap_year(year: u32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// Whether ISO week-numbering year `year` has 53 weeks: its 1 January is a
/// Thursday, or it is a leap year whose 1 January is a Wednesday.
const fn has_53_weeks(year: u32) -> bool {
    // p(y) is the weekday of 31 December of year y (0 = Sunday, 4 = Thursday).
    const fn p(y: i64) -> i64 {
        (y + y.div_euclid(4) - y.div_euclid(100) + y.div_euclid(400)).rem_euclid(7)
    }
    let y = year as i64;
    p(y) == 4 || p(y - 1) == 3
}

const fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Check a `datetime` payload: an ISO 8601 date or date-time in calendar,
/// ordinal or week form, extended or basic (convention.md: DateTime). The
/// grammar and calendar check follow the Python `_check_datetime_payload`
/// (Python `datetime.date`, so year `0000` with a month is out of range). These
/// ISO 8601-1 rules, which Python does not apply (python-sdk#110), give
/// [`Problem::DatetimeRange`]:
///
/// - hour `24` only as the end of a day: minutes and seconds absent or `00`,
///   and every digit of a decimal fraction `0` (`T24`, `T24:00`, `T24:00:00`,
///   `T24:00:00.000`);
/// - ordinal day `366` only in a leap year;
/// - week `53` only in a year with 53 ISO weeks (1 January a Thursday, or a
///   leap year whose 1 January is a Wednesday).
pub fn check_datetime(text: &str) -> Result<(), Problem> {
    let f = match_datetime(text).ok_or(Problem::DatetimeGrammar)?;
    let has_day = f.day.is_some() || f.bday.is_some() || f.ordinal.is_some() || f.weekday.is_some();
    if f.hour.is_some() && !has_day {
        return Err(Problem::DatetimeGrammar);
    }
    if let Some(month) = f.month.or(f.bmonth) {
        let day = f.day.or(f.bday).unwrap_or(1);
        let valid = (1..=9999).contains(&f.year)
            && (1..=12).contains(&month)
            && (1..=days_in_month(f.year, month)).contains(&day);
        if !valid {
            return Err(Problem::DatetimeRange);
        }
    }
    let limits = [
        (f.ordinal, 1, 366),
        (f.week, 1, 53),
        (f.weekday, 1, 7),
        (f.hour, 0, 24),
        (f.minute, 0, 59),
        (f.second, 0, 60),
        (f.zhour, 0, 23),
        (f.zminute, 0, 59),
    ];
    for (value, low, high) in limits {
        if value.is_some_and(|v| !(low..=high).contains(&v)) {
            return Err(Problem::DatetimeRange);
        }
    }
    let end_of_day =
        f.minute.unwrap_or(0) == 0 && f.second.unwrap_or(0) == 0 && !f.fraction_nonzero;
    if (f.hour == Some(24) && !end_of_day)
        || (f.ordinal == Some(366) && !is_leap_year(f.year))
        || (f.week == Some(53) && !has_53_weeks(f.year))
    {
        return Err(Problem::DatetimeRange);
    }
    Ok(())
}

// ── Exact decimals and numeric $format (alloc) ──────────────────────────────

/// Largest power-of-ten scale exact step rounding will use for a `$format`
/// step or bound (and the base taken from the current value). Larger scales
/// give [`Problem::FormatScale`]; Python has no limit and computes them exactly.
pub const MAX_FORMAT_SCALE: i64 = 20_000;

/// Exponents are clamped to this magnitude when parsed, so `1e999999999999`
/// costs nothing. Every comparison has a payload of moderate magnitude on one
/// side, so the clamp never changes a result.
#[cfg(feature = "alloc")]
const EXPONENT_CLAMP: i64 = 1 << 40;

/// An exact decimal number `mantissa * 10^exponent`, kept with no trailing zero
/// digits in the mantissa (zero has exponent 0).
#[cfg(feature = "alloc")]
#[derive(Debug, Clone)]
pub struct Decimal {
    mantissa: BigInt,
    exponent: i64,
}

#[cfg(feature = "alloc")]
fn pow10(k: u64) -> BigUint {
    let mut result = BigUint::one();
    let mut remaining = k;
    let chunk = BigUint::from(10u32).pow(1024);
    while remaining >= 1024 {
        result *= &chunk;
        remaining -= 1024;
    }
    // remaining < 1024 here.
    result * BigUint::from(10u32).pow(u32::try_from(remaining).unwrap_or(0))
}

#[cfg(feature = "alloc")]
fn digit_count(n: &BigInt) -> i64 {
    if n.is_zero() {
        0
    } else {
        i64::try_from(n.magnitude().to_str_radix(10).len()).unwrap_or(i64::MAX)
    }
}

/// Whether any digit of `int` then `frac` at index `from` or later is nonzero.
#[cfg(feature = "alloc")]
fn any_nonzero_from(int: &[u8], frac: &[u8], from: usize) -> bool {
    int.iter().chain(frac.iter()).skip(from).any(|&b| b != b'0')
}

#[cfg(feature = "alloc")]
impl Decimal {
    fn new(mantissa: BigInt, exponent: i64) -> Self {
        if mantissa.is_zero() {
            return Decimal {
                mantissa,
                exponent: 0,
            };
        }
        let text = mantissa.magnitude().to_str_radix(10);
        let zeros = text.bytes().rev().take_while(|&b| b == b'0').count();
        if zeros == 0 {
            return Decimal { mantissa, exponent };
        }
        let zeros64 = i64::try_from(zeros).unwrap_or(0);
        let divisor = BigInt::from(pow10(zeros as u64));
        Decimal {
            mantissa: mantissa / divisor,
            exponent: exponent.saturating_add(zeros64),
        }
    }

    /// The exact value of a payload in the `float` grammar (no finiteness
    /// check), as Python `Fraction(text)`; `None` if `text` does not match.
    pub fn parse(text: &str) -> Option<Self> {
        Decimal::parse_from(text, None)
    }

    /// [`Decimal::parse`], but digits below `10^lowest` are replaced by a single
    /// `1` at `10^(lowest - 1)` when any of them is nonzero. The result orders,
    /// compares and floors the same as the exact value against every number
    /// that is a multiple of `10^lowest`, and parsing costs no more than the
    /// digits kept.
    fn parse_from(text: &str, lowest: Option<i64>) -> Option<Self> {
        let parts = scan_float(text)?;
        let mut exp = parts.exp.iter().fold(0i64, |acc, &b| {
            (acc * 10 + i64::from(b - b'0')).min(EXPONENT_CLAMP)
        });
        if parts.exp_negative {
            exp = -exp;
        }
        // Exponent of the last digit; the clamp keeps this far from overflow.
        let last_exp = exp - i64::try_from(parts.frac.len()).ok()?;
        let all = parts.int.iter().chain(parts.frac.iter()).copied();
        let lead = all.clone().take_while(|&b| b == b'0').count();
        let significant = parts.int.len() + parts.frac.len() - lead;
        let keep = match lowest {
            None => significant,
            Some(lowest) => {
                // Digit k (from the left, of `significant`) has exponent
                // last_exp + significant - 1 - k; keep those >= lowest.
                let top = i128::from(last_exp) + significant as i128 - 1;
                usize::try_from((top - i128::from(lowest) + 1).max(0))
                    .unwrap_or(usize::MAX)
                    .min(significant)
            }
        };
        let mut digits: Vec<u8> = all.skip(lead).take(keep).collect();
        let dropped = significant - keep;
        let sticky = any_nonzero_from(parts.int, parts.frac, lead + keep);
        let mut exponent = last_exp.saturating_add(i64::try_from(dropped).ok()?);
        if sticky {
            digits.push(b'1');
            exponent -= 1;
        }
        let zeros = digits.iter().rev().take_while(|&&b| b == b'0').count();
        digits.truncate(digits.len() - zeros);
        if digits.is_empty() {
            return Some(Decimal {
                mantissa: BigInt::zero(),
                exponent: 0,
            });
        }
        let magnitude = BigUint::parse_bytes(&digits, 10)?;
        let sign = if parts.negative {
            Sign::Minus
        } else {
            Sign::Plus
        };
        Some(Decimal {
            mantissa: BigInt::from_biguint(sign, magnitude),
            exponent: exponent.saturating_add(i64::try_from(zeros).ok()?),
        })
    }

    /// The exact value of an integer.
    pub fn from_i64(value: i64) -> Self {
        Decimal::new(BigInt::from(value), 0)
    }

    /// The exact value of a finite f64 (every finite f64 is a finite decimal);
    /// `None` for NaN or an infinity.
    pub fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        let bits = value.to_bits();
        let negative = bits >> 63 == 1;
        let biased = i64::try_from((bits >> 52) & 0x7ff).unwrap_or(0);
        let fraction = bits & ((1u64 << 52) - 1);
        let (mantissa, exp2) = if biased == 0 {
            (fraction, -1074i64)
        } else {
            (fraction | (1u64 << 52), biased - 1075)
        };
        let mut magnitude = BigUint::from(mantissa);
        let exponent = if exp2 >= 0 {
            magnitude <<= exp2 as u64;
            0
        } else {
            // m * 2^-k = m * 5^k / 10^k, with k <= 1074.
            let k = u32::try_from(-exp2).unwrap_or(0);
            magnitude *= BigUint::from(5u32).pow(k);
            exp2
        };
        let sign = if negative { Sign::Minus } else { Sign::Plus };
        Some(Decimal::new(
            BigInt::from_biguint(sign, magnitude),
            exponent,
        ))
    }

    /// The nearest f64 (correctly rounded); infinite when out of range.
    pub fn to_f64(&self) -> f64 {
        let text = alloc::format!("{}e{}", self.mantissa, self.exponent);
        text.parse::<f64>().unwrap_or(f64::NAN)
    }

    /// The value as an integer, if it is whole.
    fn to_integer(&self) -> Option<BigInt> {
        if self.exponent < 0 {
            return None;
        }
        let k = u64::try_from(self.exponent).ok()?;
        Some(&self.mantissa * BigInt::from(pow10(k)))
    }
}

#[cfg(feature = "alloc")]
impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        let rank = |s: Sign| match s {
            Sign::Minus => 0,
            Sign::NoSign => 1,
            Sign::Plus => 2,
        };
        let (a, b) = (self.mantissa.sign(), other.mantissa.sign());
        if a != b {
            return rank(a).cmp(&rank(b));
        }
        if a == Sign::NoSign {
            return Ordering::Equal;
        }
        let adjusted_a = self.exponent + digit_count(&self.mantissa);
        let adjusted_b = other.exponent + digit_count(&other.mantissa);
        let magnitude = if adjusted_a != adjusted_b {
            adjusted_a.cmp(&adjusted_b)
        } else {
            // Equal magnitude: the exponents differ by at most the digit counts.
            let low = self.exponent.min(other.exponent);
            let scale = |d: &Decimal| {
                let k = u64::try_from(d.exponent - low).unwrap_or(0);
                d.mantissa.magnitude() * pow10(k)
            };
            scale(self).cmp(&scale(other))
        };
        if a == Sign::Minus {
            magnitude.reverse()
        } else {
            magnitude
        }
    }
}

#[cfg(feature = "alloc")]
impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "alloc")]
impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

#[cfg(feature = "alloc")]
impl Eq for Decimal {}

/// A numeric `$format` `[min]:[max][:step]` (convention.md: Payload, format).
///
/// A format that does not parse imposes no constraint; a bound that is not in
/// the `float` grammar is absent; a step that is not > 0 is ignored. Port of the
/// Python `_numeric_format`.
#[cfg(feature = "alloc")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NumericFormat {
    /// Inclusive minimum.
    pub min: Option<Decimal>,
    /// Inclusive maximum.
    pub max: Option<Decimal>,
    /// Step, always > 0 when present.
    pub step: Option<Decimal>,
}

#[cfg(feature = "alloc")]
impl NumericFormat {
    /// Parse a property's `$format`; `None` or empty imposes no constraint.
    pub fn parse(format: Option<&str>) -> Self {
        let Some(format) = format.filter(|f| !f.is_empty()) else {
            return NumericFormat::default();
        };
        let mut parts = format.split(':');
        let (Some(low), Some(high)) = (parts.next(), parts.next()) else {
            return NumericFormat::default();
        };
        let step = parts.next();
        if parts.next().is_some() {
            return NumericFormat::default();
        }
        NumericFormat {
            min: Decimal::parse(low),
            max: Decimal::parse(high),
            step: step
                .and_then(Decimal::parse)
                .filter(|s| s.mantissa.is_positive()),
        }
    }
}

/// A property's current value, the step base when `$format` has a step but
/// neither bound.
#[cfg(feature = "alloc")]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Current {
    /// An `integer` value.
    Integer(i64),
    /// A `float` value; a non-finite one is not used as a base.
    Float(f64),
}

#[cfg(feature = "alloc")]
fn scale_to(d: &Decimal, scale: i64) -> Result<BigInt, Problem> {
    let k = d.exponent + scale;
    if !(0..=3 * MAX_FORMAT_SCALE).contains(&k) {
        return Err(Problem::FormatScale);
    }
    Ok(&d.mantissa * BigInt::from(pow10(k as u64)))
}

/// `floor((x - base) / step + 1/2) * step + base` exactly.
#[cfg(feature = "alloc")]
fn round_to_step(x: &Decimal, base: &Decimal, step: &Decimal) -> Result<Decimal, Problem> {
    let fits = |d: &Decimal| (-MAX_FORMAT_SCALE..=MAX_FORMAT_SCALE).contains(&d.exponent);
    if !fits(base) || !fits(step) {
        return Err(Problem::FormatScale);
    }
    let scale = 0.max(-base.exponent).max(-step.exponent);
    let b = scale_to(base, scale)?;
    let p = scale_to(step, scale)?;
    // (x - base)/step + 1/2 = (2X + p - 2b) / 2p with X = x * 10^scale.
    let (x_floor, half_or_more) = if x.mantissa.is_zero() {
        (BigInt::zero(), false)
    } else {
        let s = x.exponent + scale;
        if s >= 0 {
            (scale_to(x, scale)?, false)
        } else if -s > digit_count(&x.mantissa) + 1 {
            // |X| < 1/100: floor is 0 or -1.
            if x.mantissa.is_negative() {
                (-BigInt::one(), true)
            } else {
                (BigInt::zero(), false)
            }
        } else {
            let unit = BigInt::from(pow10((-s) as u64));
            let (q, r) = x.mantissa.div_mod_floor(&unit);
            (q, BigInt::from(2) * r >= unit)
        }
    };
    // For integer m and d >= 1, floor((m + t)/d) = floor(m/d) for 0 <= t < 1.
    let two = BigInt::from(2);
    let mut m = &two * &x_floor + &p - &two * &b;
    if half_or_more {
        m += 1;
    }
    let n = m.div_floor(&(&two * &p));
    Ok(Decimal::new(n * p + b, -scale))
}

/// The step base when `format` has a step: `min`, else `max`, else `current`
/// (convention.md: Payload).
#[cfg(feature = "alloc")]
fn step_base(format: &NumericFormat, current: Option<Current>) -> Option<Decimal> {
    format.step.as_ref()?;
    format
        .min
        .clone()
        .or_else(|| format.max.clone())
        .or_else(|| match current {
            Some(Current::Integer(v)) => Some(Decimal::from_i64(v)),
            Some(Current::Float(v)) => Decimal::from_f64(v),
            None => None,
        })
}

/// Round to the format's step from `base` (see [`step_base`]), then check min
/// and max (convention.md: Payload).
#[cfg(feature = "alloc")]
fn round_and_range_check(
    number: Decimal,
    format: &NumericFormat,
    base: Option<&Decimal>,
) -> Result<Decimal, Problem> {
    let mut number = number;
    if let (Some(step), Some(base)) = (&format.step, base) {
        number = round_to_step(&number, base, step)?;
    }
    range_check(&number, format)?;
    Ok(number)
}

/// Check `number` against the format's min and max, with no step rounding.
#[cfg(feature = "alloc")]
fn range_check(number: &Decimal, format: &NumericFormat) -> Result<(), Problem> {
    if format.min.as_ref().is_some_and(|low| number < low) {
        return Err(Problem::BelowMin);
    }
    if format.max.as_ref().is_some_and(|high| number > high) {
        return Err(Problem::AboveMax);
    }
    Ok(())
}

/// The value of an `integer` payload: `-?[0-9]+` within the 64-bit signed
/// range (convention.md: Integer).
#[cfg(feature = "alloc")]
fn integer_value(text: &str) -> Result<i64, Problem> {
    is_integer_payload(text)
        .then(|| text.parse::<i64>().ok())
        .flatten()
        .ok_or(Problem::IntegerGrammar)
}

/// The exact value of a `float` payload already accepted by
/// [`is_float_payload`], for comparison with or rounding to `operands`.
/// Payload digits finer than every operand cannot change such a result, so a
/// long payload costs only the digits that matter.
#[cfg(feature = "alloc")]
fn float_value(text: &str, operands: &[&Option<Decimal>]) -> Result<Decimal, Problem> {
    let lowest = operands
        .iter()
        .filter_map(|d| d.as_ref())
        .map(|d| d.exponent)
        .fold(0, i64::min)
        - 1;
    Decimal::parse_from(text, Some(lowest)).ok_or(Problem::FloatGrammar)
}

/// Grammar and `$format` checks shared by [`parse_set_payload`] and
/// [`check_value`] for the datatypes whose payload is used unchanged:
/// `boolean`, `enum`, `color`, `datetime` and `duration`. Every other datatype
/// is accepted here.
#[cfg(feature = "alloc")]
fn check_text(text: &str, datatype: Datatype, format: Option<&str>) -> Result<(), Problem> {
    match datatype {
        Datatype::Boolean => {
            if text == "true" || text == "false" {
                Ok(())
            } else {
                Err(Problem::BooleanGrammar)
            }
        }
        Datatype::Enum => {
            let allowed = format
                .filter(|f| !f.is_empty())
                .is_none_or(|f| f.split(',').any(|v| v == text));
            if text.is_empty() || !allowed {
                Err(Problem::EnumNotInFormat)
            } else {
                Ok(())
            }
        }
        Datatype::Color => check_color(text, format),
        Datatype::Duration => {
            if is_duration_payload(text) {
                Ok(())
            } else {
                Err(Problem::DurationGrammar)
            }
        }
        Datatype::Datetime => check_datetime(text),
        Datatype::Integer | Datatype::Float | Datatype::String | Datatype::Json => Ok(()),
    }
}

// ── /set parsing and encoders (alloc) ───────────────────────────────────────

/// A validated `/set` value, as the Python SDK hands it to a set callback.
#[cfg(feature = "alloc")]
#[derive(Debug, Clone, PartialEq)]
pub enum SetValue {
    /// The payload text (normalized by step rounding for numbers), for every
    /// datatype but `json` and `string`.
    Text(String),
    /// The decoded `json` array or object.
    Json(Value),
    /// A `string` value, with the `0x00` payload decoded to `""`
    /// (convention.md: String).
    String(String),
}

/// Parse a `json` payload: an array or object (convention.md: JSON), read as
/// [`crate::pyfmt::from_json`] reads it. `NaN` and `Infinity` are rejected.
#[cfg(feature = "alloc")]
pub fn decode_json(text: &str) -> Result<Value, Problem> {
    let value = crate::pyfmt::from_json(text).ok_or(Problem::JsonInvalid)?;
    if value.is_array() || value.is_object() {
        Ok(value)
    } else {
        Err(Problem::JsonNotArrayOrObject)
    }
}

/// Validate an inbound `/set` payload for `datatype`, with step rounding in
/// exact rational arithmetic as the Python `_parse_set_payload` does. A number
/// that rounding changed is returned in its rounded payload form; otherwise the
/// payload text is returned unchanged. An `integer` that rounding takes outside
/// the 64-bit range gives [`Problem::IntegerOutOf64Bit`] (convention.md:
/// Integer); Python accepts it (python-sdk#110).
#[cfg(feature = "alloc")]
pub fn parse_set_payload(
    text: &str,
    datatype: Datatype,
    format: Option<&str>,
    current: Option<Current>,
) -> Result<SetValue, Problem> {
    let unchanged = || Ok(SetValue::Text(text.to_string()));
    match datatype {
        Datatype::Json => decode_json(text).map(SetValue::Json),
        Datatype::Integer => {
            let number = Decimal::from_i64(integer_value(text)?);
            let format = NumericFormat::parse(format);
            let base = step_base(&format, current);
            let rounded = round_and_range_check(number.clone(), &format, base.as_ref())?;
            let whole = rounded.to_integer().ok_or(Problem::IntegerStepNotWhole)?;
            if rounded == number {
                unchanged()
            } else {
                // Rounding can leave the 64-bit range (convention.md: Integer).
                let whole = i64::try_from(&whole).map_err(|_| Problem::IntegerOutOf64Bit)?;
                Ok(SetValue::Text(whole.to_string()))
            }
        }
        Datatype::Float => {
            if !is_float_payload(text) {
                return Err(Problem::FloatGrammar);
            }
            let format = NumericFormat::parse(format);
            let base = step_base(&format, current);
            if format.min.is_none() && format.max.is_none() && base.is_none() {
                return unchanged();
            }
            let number = float_value(text, &[&format.min, &format.max, &format.step, &base])?;
            let rounded = round_and_range_check(number.clone(), &format, base.as_ref())?;
            if rounded == number {
                unchanged()
            } else {
                // Python raises OverflowError here; the value is out of range.
                crate::pyfmt::format_float(rounded.to_f64())
                    .map(SetValue::Text)
                    .ok_or(Problem::FloatOutOfRange)
            }
        }
        Datatype::Boolean
        | Datatype::Enum
        | Datatype::Color
        | Datatype::Duration
        | Datatype::Datetime => check_text(text, datatype, format).and_then(|()| unchanged()),
        Datatype::String => Ok(SetValue::String(if text == "\0" {
            String::new()
        } else {
            text.to_string()
        })),
    }
}

/// Check a received property value or `$target` against its description, as
/// a controller does before using it (convention.md:78).
///
/// `text` is the value as [`crate::payload::classify`] decodes it, so a `0x00`
/// payload arrives as `""`, which only `string` accepts (convention.md:60-72).
/// The checks are each datatype's payload grammar, `enum` and `color`
/// membership in `$format`, `$format` min and max for `integer` and `float`,
/// the 64-bit range for `integer`, finite `float` values, and an array or
/// object for `json`. Values are not rounded to a `$format` step: rounding is
/// the device's job (convention.md:399), so a value off the step is accepted
/// and one beyond min or max is not.
#[cfg(feature = "alloc")]
pub fn check_value(text: &str, prop: &PropertyDescription) -> Result<(), Problem> {
    let format = prop.format.as_deref();
    match prop.datatype {
        Datatype::Integer => {
            let number = Decimal::from_i64(integer_value(text)?);
            range_check(&number, &NumericFormat::parse(format))
        }
        Datatype::Float => {
            if !is_float_payload(text) {
                return Err(Problem::FloatGrammar);
            }
            let format = NumericFormat::parse(format);
            if format.min.is_none() && format.max.is_none() {
                return Ok(());
            }
            let number = float_value(text, &[&format.min, &format.max])?;
            range_check(&number, &format)
        }
        Datatype::Json => decode_json(text).map(|_| ()),
        Datatype::String => Ok(()),
        Datatype::Boolean
        | Datatype::Enum
        | Datatype::Color
        | Datatype::Duration
        | Datatype::Datetime => check_text(text, prop.datatype, format),
    }
}

/// `integer` payload for `value` (convention.md: Integer).
#[cfg(feature = "alloc")]
pub fn encode_integer(value: i64) -> String {
    value.to_string()
}

/// `float` payload for `value`, in the Python SDK form (`1.0`, `1e16`,
/// `1e-05`; convention.md: Float). NaN and infinities give
/// [`Problem::FloatNotFinite`].
#[cfg(feature = "alloc")]
pub fn encode_float(value: f64) -> Result<String, Problem> {
    crate::pyfmt::format_float(value).ok_or(Problem::FloatNotFinite)
}

/// [`encode_float`] for an `f32`: the shortest decimal that reads back as the
/// same `f32` (`21.4`, not `21.399999618530273`), in the same layout.
#[cfg(feature = "alloc")]
pub fn encode_float32(value: f32) -> Result<String, Problem> {
    crate::pyfmt::format_float32(value).ok_or(Problem::FloatNotFinite)
}

/// `boolean` payload for `value` (convention.md: Boolean).
#[cfg(feature = "alloc")]
pub fn encode_boolean(value: bool) -> String {
    String::from(if value { "true" } else { "false" })
}

/// `duration` payload `PTxHxMxS` for a non-negative duration (convention.md:
/// Duration). Port of the Python `_encode_duration` for a `timedelta`: days
/// fold into hours, zero components are omitted, and zero is `PT0S`.
/// `micros` of a million or more carry into the seconds.
#[cfg(feature = "alloc")]
pub fn encode_duration(total_seconds: u64, micros: u32) -> String {
    let total = total_seconds.saturating_add(u64::from(micros / 1_000_000));
    let micros = micros % 1_000_000;
    let hours = total / 3600;
    let minutes = total % 3600 / 60;
    let seconds = total % 60;
    let mut out = String::from("PT");
    if hours != 0 {
        out.push_str(&alloc::format!("{hours}H"));
    }
    if minutes != 0 {
        out.push_str(&alloc::format!("{minutes}M"));
    }
    if micros != 0 {
        let text = alloc::format!("{seconds}.{micros:06}");
        out.push_str(text.trim_end_matches('0'));
        out.push('S');
    } else if seconds != 0 || out == "PT" {
        out.push_str(&alloc::format!("{seconds}S"));
    }
    out
}

/// `json` payload for `value`, as Python `json.dumps(value, allow_nan=False)`
/// (convention.md: JSON). The top level must be an array or object.
#[cfg(feature = "alloc")]
pub fn encode_json(value: &Value) -> Result<String, Problem> {
    if value.is_array() || value.is_object() {
        Ok(crate::pyfmt::to_json(value, false))
    } else {
        Err(Problem::JsonNotArrayOrObject)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datatype_names_round_trip() {
        for dt in [
            Datatype::Integer,
            Datatype::Float,
            Datatype::Boolean,
            Datatype::String,
            Datatype::Enum,
            Datatype::Color,
            Datatype::Datetime,
            Datatype::Duration,
            Datatype::Json,
        ] {
            assert_eq!(Datatype::parse(dt.as_str()), Some(dt));
        }
        assert_eq!(Datatype::parse("Integer"), None);
        assert_eq!(Datatype::parse(""), None);
    }

    #[test]
    fn integer_grammar() {
        for ok in ["0", "-0", "123", "-0007", "9223372036854775808"] {
            assert!(is_integer_payload(ok), "{ok}");
        }
        for bad in ["", "-", "+1", "1.0", " 1", "1 ", "--1", "\u{0661}", "1e3"] {
            assert!(!is_integer_payload(bad), "{bad}");
        }
    }

    #[test]
    fn float_grammar() {
        for ok in [
            "0",
            "1.",
            ".5",
            "-.5",
            "1.e5",
            "1E-3",
            "-0",
            "1.5e308",
            "1e-400",
            "0e99999999999",
        ] {
            assert!(is_float_payload(ok), "{ok}");
        }
        for bad in [
            "", ".", "-", "1e", "1e+3", "+1", "1.5.", "inf", "nan", "1e999", "-1e309", "e5", "1 ",
        ] {
            assert!(!is_float_payload(bad), "{bad}");
        }
    }

    #[test]
    fn color_checks() {
        assert_eq!(check_color("rgb,255,0,0.5", None), Ok(()));
        assert_eq!(check_color("hsv,360,100,100", Some("")), Ok(()));
        assert_eq!(check_color("xyz,0.5,1", Some("rgb,xyz")), Ok(()));
        assert_eq!(check_color("rgb,-0,0,0", None), Ok(()));
        assert_eq!(check_color("", None), Err(Problem::ColorType));
        assert_eq!(check_color("RGB,1,2,3", None), Err(Problem::ColorType));
        assert_eq!(
            check_color("rgb,1,2,3", Some("hsv")),
            Err(Problem::ColorTypeNotInFormat)
        );
        assert_eq!(
            check_color("rgb,1,2", None),
            Err(Problem::ColorComponentCount)
        );
        assert_eq!(
            check_color("xyz,1,1,1", None),
            Err(Problem::ColorComponentCount)
        );
        assert_eq!(
            check_color("rgb,1,2,x", None),
            Err(Problem::ColorComponentGrammar)
        );
        assert_eq!(
            check_color("rgb,1,,3", None),
            Err(Problem::ColorComponentGrammar)
        );
        assert_eq!(
            check_color("rgb,256,0,0", None),
            Err(Problem::ColorComponentRange)
        );
        assert_eq!(
            check_color("rgb,1e999,0,0", None),
            Err(Problem::ColorComponentRange)
        );
        assert_eq!(
            check_color("hsv,0,-1,0", None),
            Err(Problem::ColorComponentRange)
        );
    }

    #[test]
    fn duration_grammar() {
        for ok in [
            "PT0S",
            "PT1H",
            "PT1M",
            "PT1.5S",
            "PT1H2M3S",
            "PT1H3.25S",
            "PT12M0S",
        ] {
            assert!(is_duration_payload(ok), "{ok}");
        }
        for bad in [
            "", "PT", "P1D", "PT1", "PT1.S", "PT.5S", "PT1M1H", "PT1H1H", "PT1S1M", "PT1.5H",
            "pt1s", "PT1SX",
        ] {
            assert!(!is_duration_payload(bad), "{bad}");
        }
    }

    #[test]
    fn datetime_checks() {
        for ok in [
            "2024",
            "2024-02",
            "2024-02-29",
            "20240229",
            "2024-366",
            "2024123",
            "2024-W01",
            "2024W017",
            "2020-W53",
            "2020-W53-7",
            "2026W53",
            "2015-W53",
            "2024-02-29T23:59:60",
            "2024-02-29T24",
            "2024-02-29T24:00",
            "2024-02-29T2400",
            "2024-02-29T24:00:00",
            "2024-02-29T24:00:00.000Z",
            "2024-02-29T24,0",
            "2000-366",
            "2024-02-29T10:30:00.123Z",
            "2024-02-29T10,5+05:30",
            "20240229T103000-0800",
            "2024-123T10:00Z",
            "2024-W01-1T10",
            "0000",
            "0000-123",
        ] {
            assert_eq!(check_datetime(ok), Ok(()), "{ok}");
        }
        for bad in [
            "",
            "202",
            "2024-",
            "2024-1",
            "2024-02-1",
            "2024T10",
            "2024-02T10",
            "2024-W01T10",
            "2024-02-29T1",
            "2024-02-29T10:3",
            "2024-02-29T10.Z",
            "2024-02-29T10+5",
            "2024-02-29 10:00",
            "2024-02-29T10:00z",
            "\u{0662}024",
        ] {
            assert_eq!(check_datetime(bad), Err(Problem::DatetimeGrammar), "{bad}");
        }
        for range in [
            "2023-02-29",
            "1900-02-29",
            "2024-13",
            "2024-00",
            "2024-04-31",
            "0000-01",
            "20241301",
            "2024-000",
            "2024-367",
            "2024-W00",
            "2024-W54",
            "2024-W01-0",
            "2024-W01-8",
            "2024-01-01T25",
            "2024-01-01T10:60",
            "2024-01-01T10:00:61",
            "2024-01-01T10+24",
            "2024-01-01T10+05:60",
            "2024-01-01T24:30",
            "2024-01-01T24:00:01",
            "2024-01-01T24:00:00.001",
            "2024-01-01T24,5",
            "2024-01-01T24:00:60",
            "2023-366",
            "1900-366",
            "2021-W53",
            "2024-W53-7",
            "2024W537",
            "2100-W53",
        ] {
            assert_eq!(
                check_datetime(range),
                Err(Problem::DatetimeRange),
                "{range}"
            );
        }
        assert!(is_leap_year(2000) && !is_leap_year(2100) && is_leap_year(2024));
        // 53-week years: 2015, 2020 (leap, 1 January a Wednesday), 2026.
        for (year, long) in [
            (2015, true),
            (2019, false),
            (2020, true),
            (2021, false),
            (2024, false),
            (2025, false),
            (2026, true),
            (2032, true),
            (0, false),
        ] {
            assert_eq!(has_53_weeks(year), long, "{year}");
        }
        assert_eq!(check_datetime("2000-02-29"), Ok(()));
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn check_value_cases() {
        let prop = |datatype, format: Option<&str>| {
            let mut p = PropertyDescription::new(datatype);
            p.format = format.map(String::from);
            p
        };
        let check = |text, datatype, format| check_value(text, &prop(datatype, format));
        // No step rounding: off-step is fine, beyond min or max is not.
        assert_eq!(check("5", Datatype::Integer, Some("0:10:2")), Ok(()));
        assert_eq!(
            check("11", Datatype::Integer, Some("1:10:3")),
            Err(Problem::AboveMax)
        );
        assert_eq!(
            check("10.2", Datatype::Float, Some("0:10:0.5")),
            Err(Problem::AboveMax)
        );
        assert_eq!(
            check("-0.1", Datatype::Float, Some("0:10")),
            Err(Problem::BelowMin)
        );
        // Exact comparison, past f64 precision.
        let above = "10.0000000000000000001";
        assert_eq!(
            check(above, Datatype::Float, Some("0:10")),
            Err(Problem::AboveMax)
        );
        assert_eq!(
            check("9223372036854775808", Datatype::Integer, None),
            Err(Problem::IntegerGrammar)
        );
        assert_eq!(
            check("1e999", Datatype::Float, None),
            Err(Problem::FloatGrammar)
        );
        // A 0x00 payload decodes to "", which only string accepts.
        assert_eq!(check("", Datatype::String, None), Ok(()));
        assert_eq!(
            check("", Datatype::Enum, Some("a,b")),
            Err(Problem::EnumNotInFormat)
        );
        assert_eq!(check("", Datatype::Json, None), Err(Problem::JsonInvalid));
        assert_eq!(
            check("c", Datatype::Enum, Some("a,b")),
            Err(Problem::EnumNotInFormat)
        );
        assert_eq!(
            check("hsv,0,0,0", Datatype::Color, Some("rgb")),
            Err(Problem::ColorTypeNotInFormat)
        );
        assert_eq!(
            check("1", Datatype::Json, None),
            Err(Problem::JsonNotArrayOrObject)
        );
    }

    #[test]
    fn problem_codes() {
        assert_eq!(Problem::IntegerStepNotWhole.as_str(), "integerStepNotWhole");
        assert_eq!(
            Problem::JsonNotArrayOrObject.as_str(),
            "jsonNotArrayOrObject"
        );
        assert_eq!(Problem::FormatScale.as_str(), "formatScale");
    }

    #[cfg(feature = "alloc")]
    mod alloc_tests {
        use super::super::*;

        fn set(
            text: &str,
            dt: Datatype,
            format: Option<&str>,
            current: Option<Current>,
        ) -> Result<SetValue, Problem> {
            parse_set_payload(text, dt, format, current)
        }

        fn text(s: &str) -> Result<SetValue, Problem> {
            Ok(SetValue::Text(s.into()))
        }

        #[test]
        fn decimal_parse_and_order() {
            let d = |s: &str| Decimal::parse(s).unwrap();
            assert_eq!(d("1.50"), d("1.5"));
            assert_eq!(d("-0"), d("0"));
            assert_eq!(d("0.0e5"), Decimal::from_i64(0));
            assert_eq!(d("1e3"), Decimal::from_i64(1000));
            assert!(d("0.1") < d("0.10000000000000001"));
            assert!(d("-2") < d("-1.99"));
            assert!(d("1e-99999999999999999999") > d("0"));
            assert!(d("-1e-99999999999999999999") < d("0"));
            assert!(d("1e99999999999999999999") > d("1.7e308"));
            assert_eq!(
                Decimal::from_f64(0.1).unwrap(),
                d("0.1000000000000000055511151231257827021181583404541015625")
            );
            assert_eq!(Decimal::from_f64(5e-324).unwrap().to_f64(), 5e-324);
            assert_eq!(Decimal::from_f64(-1.5e300).unwrap().to_f64(), -1.5e300);
            assert!(Decimal::from_f64(f64::NAN).is_none());
            assert!(Decimal::parse("1e").is_none());
        }

        #[test]
        fn numeric_format_parsing() {
            let f = NumericFormat::parse(Some("0:10:0.5"));
            assert_eq!(f.min, Decimal::parse("0"));
            assert_eq!(f.max, Decimal::parse("10"));
            assert_eq!(f.step, Decimal::parse("0.5"));
            assert_eq!(NumericFormat::parse(Some(":10")).min, None);
            assert_eq!(NumericFormat::parse(Some("1:2:0")).step, None);
            assert_eq!(NumericFormat::parse(Some("1:2:-1")).step, None);
            assert_eq!(
                NumericFormat::parse(Some("1:2:3:4")),
                NumericFormat::default()
            );
            assert_eq!(NumericFormat::parse(Some("5")), NumericFormat::default());
            assert_eq!(NumericFormat::parse(Some("")), NumericFormat::default());
            assert_eq!(NumericFormat::parse(None), NumericFormat::default());
            assert_eq!(NumericFormat::parse(Some("a:b")), NumericFormat::default());
        }

        #[test]
        fn integer_set() {
            let i = Datatype::Integer;
            assert_eq!(set("-0007", i, None, None), text("-0007"));
            assert_eq!(
                set("9223372036854775807", i, None, None),
                text("9223372036854775807")
            );
            assert_eq!(
                set("9223372036854775808", i, None, None),
                Err(Problem::IntegerGrammar)
            );
            assert_eq!(set("1.0", i, None, None), Err(Problem::IntegerGrammar));
            assert_eq!(set("", i, None, None), Err(Problem::IntegerGrammar));
            assert_eq!(set("5", i, Some("0:10"), None), text("5"));
            assert_eq!(set("-1", i, Some("0:10"), None), Err(Problem::BelowMin));
            assert_eq!(set("11", i, Some("0:10"), None), Err(Problem::AboveMax));
            assert_eq!(set("11", i, Some(":10"), None), Err(Problem::AboveMax));
            assert_eq!(set("-5", i, Some(":10"), None), text("-5"));
            // Step rounding, half up.
            assert_eq!(set("7", i, Some("0:100:5"), None), text("5"));
            assert_eq!(set("8", i, Some("0:100:5"), None), text("10"));
            assert_eq!(set("-3", i, Some(":100:5"), None), text("-5"));
            assert_eq!(set("-2", i, Some(":100:5"), None), text("0"));
            assert_eq!(set("98", i, Some("0:100:5"), None), text("100"));
            assert_eq!(set("99", i, Some("0:99:5"), None), Err(Problem::AboveMax));
            assert_eq!(
                set("-0007", i, Some("0:100:1"), None),
                Err(Problem::BelowMin)
            );
            assert_eq!(set("07", i, Some("0:100:1"), None), text("07"));
            assert_eq!(
                set("2", i, Some("0:10:1.5"), None),
                Err(Problem::IntegerStepNotWhole)
            );
            assert_eq!(set("3", i, Some("0:10:1.5"), None), text("3"));
            // Base from the current value.
            assert_eq!(set("4", i, Some(":"), Some(Current::Integer(1))), text("4"));
            assert_eq!(
                set("4", i, Some("::3"), Some(Current::Integer(1))),
                text("4")
            );
            assert_eq!(
                set("5", i, Some("::3"), Some(Current::Integer(1))),
                text("4")
            );
            assert_eq!(set("5", i, Some("::3"), None), text("5"));
            assert_eq!(
                set("5", i, Some("::3"), Some(Current::Float(f64::NAN))),
                text("5")
            );
            assert_eq!(
                set("5", i, Some("::2"), Some(Current::Float(0.5))),
                Err(Problem::IntegerStepNotWhole)
            );
            assert_eq!(
                set("9223372036854775807", i, Some("0:1e30:1e20"), None),
                text("0")
            );
            // A rounded value outside 64 bits is rejected (python-sdk#110).
            assert_eq!(
                set(
                    "9223372036854775807",
                    i,
                    Some("::1e19"),
                    Some(Current::Integer(0))
                ),
                Err(Problem::IntegerOutOf64Bit)
            );
            assert_eq!(
                set("9223372036854775807", i, Some("0::2"), None),
                Err(Problem::IntegerOutOf64Bit)
            );
            assert_eq!(
                set(
                    "-9223372036854775808",
                    i,
                    Some("::3"),
                    Some(Current::Integer(0))
                ),
                Err(Problem::IntegerOutOf64Bit)
            );
            assert_eq!(
                set("9223372036854775806", i, Some("0::2"), None),
                text("9223372036854775806")
            );
        }

        #[test]
        fn float_set() {
            let f = Datatype::Float;
            assert_eq!(set("1.", f, None, None), text("1."));
            assert_eq!(set(".5e-3", f, None, None), text(".5e-3"));
            assert_eq!(set("1e999", f, None, None), Err(Problem::FloatGrammar));
            assert_eq!(set("nan", f, None, None), Err(Problem::FloatGrammar));
            assert_eq!(set("1e+5", f, None, None), Err(Problem::FloatGrammar));
            assert_eq!(set("0.5", f, Some("0:1"), None), text("0.5"));
            assert_eq!(
                set("1.0000000000000000001", f, Some("0:1"), None),
                Err(Problem::AboveMax)
            );
            assert_eq!(
                set("-0.0000000000000000001", f, Some("0:1"), None),
                Err(Problem::BelowMin)
            );
            assert_eq!(set("-0", f, Some("0:1"), None), text("-0"));
            // Exact step rounding: 0.1 + 0.2 is not 0.3 in binary, but is here.
            assert_eq!(set("0.3", f, Some("0:1:0.1"), None), text("0.3"));
            assert_eq!(set("0.30", f, Some("0:1:0.1"), None), text("0.30"));
            assert_eq!(set("0.25", f, Some("0:1:0.1"), None), text("0.3"));
            assert_eq!(
                set("0.2499999999999999999", f, Some("0:1:0.1"), None),
                text("0.2")
            );
            assert_eq!(set("0.35", f, Some("0:1:0.1"), None), text("0.4"));
            assert_eq!(set("-0.25", f, Some(":1:0.1"), None), text("-0.2"));
            assert_eq!(set("-0.26", f, Some(":1:0.1"), None), text("-0.3"));
            assert_eq!(set("0.95", f, Some("0:1:0.1"), None), text("1.0"));
            assert_eq!(set("1.04", f, Some("0:1:0.1"), None), text("1.0"));
            assert_eq!(
                set("1.05", f, Some("0:1:0.1"), None),
                Err(Problem::AboveMax)
            );
            assert_eq!(set("3", f, Some("0:100:10"), None), text("0.0"));
            assert_eq!(set("1e2", f, Some("0:1000:10"), None), text("1e2"));
            assert_eq!(
                set("12345678901234567", f, Some("0:1e20:1"), None),
                text("12345678901234567")
            );
            assert_eq!(set("1.5", f, Some("0:1e20:1"), None), text("2.0"));
            assert_eq!(set("1e20", f, Some("0:1e30:7"), None), text("1e20"));
            // Tiny payloads: the floor is decided by sign and the 1/2 boundary.
            assert_eq!(set("1e-400", f, Some("0:1:1"), None), text("0.0"));
            assert_eq!(set("-1e-400", f, Some("-1:1:1"), None), text("0.0"));
            assert_eq!(set("1e-400", f, Some("0.5:2:1"), None), text("0.5"));
            assert_eq!(set("-1e-400", f, Some("-0.5:2:1"), None), text("-0.5"));
            assert_eq!(set("0.5", f, Some("0:1:1"), None), text("1.0"));
            assert_eq!(set("0.4999999999", f, Some("0:1:1"), None), text("0.0"));
            assert_eq!(set("1e-99999999999", f, Some("0:1:1"), None), text("0.0"));
            assert_eq!(
                set("0e99999999999", f, Some("0:1:1"), None),
                text("0e99999999999")
            );
            // Max as base when min is absent.
            assert_eq!(set("9.4", f, Some(":10:1"), None), text("9.0"));
            assert_eq!(set("9.5", f, Some(":10.5:1"), None), text("9.5"));
            assert_eq!(set("9.4", f, Some(":10.5:1"), None), text("9.5"));
            // Base from the current float, converted exactly.
            assert_eq!(
                set("1", f, Some("::0.5"), Some(Current::Float(0.1))),
                text("1.1")
            );
            assert_eq!(
                set("0.35", f, Some("::0.5"), Some(Current::Float(0.1))),
                text("0.1")
            );
            assert_eq!(
                set("1", f, Some("::1"), Some(Current::Integer(5))),
                text("1")
            );
            assert_eq!(
                set("1.2", f, Some("::1"), Some(Current::Float(f64::INFINITY))),
                text("1.2")
            );
            // Rounded past the f64 range (Python raises OverflowError).
            assert_eq!(
                set("1.7e308", f, Some("0:1e400:1e308"), None),
                Err(Problem::FloatOutOfRange)
            );
            assert_eq!(
                set("1", f, Some("0:1:1e-99999"), None),
                Err(Problem::FormatScale)
            );
            assert_eq!(
                set("1", f, Some("1e99999:2e99999:1"), None),
                Err(Problem::FormatScale)
            );
            assert_eq!(
                set("1", f, Some(":1e99999:1"), None),
                Err(Problem::FormatScale)
            );
            // Huge bounds without a step only compare, which is cheap.
            assert_eq!(
                set("1", f, Some("1e-99999999999:1e99999999999"), None),
                text("1")
            );
            assert_eq!(
                set("1", f, Some("1e99999999999:"), None),
                Err(Problem::BelowMin)
            );
        }

        /// Digits below every format operand collapse to one sticky digit,
        /// with the same result as exact arithmetic on the whole payload.
        #[test]
        fn float_set_long_payloads() {
            let f = Datatype::Float;
            let long = |head: &str, fill: &str, n: usize, tail: &str| {
                let mut s = String::from(head);
                s.extend(core::iter::repeat_n(fill, n));
                s.push_str(tail);
                s
            };
            let just_above_one = long("1.", "0", 5000, "1");
            assert_eq!(set(&just_above_one, f, None, None), text(&just_above_one));
            assert_eq!(
                set(&just_above_one, f, Some("0:1"), None),
                Err(Problem::AboveMax)
            );
            let one = long("1.", "0", 5000, "");
            assert_eq!(set(&one, f, Some("0:1"), None), text(&one));
            let below_zero = long("-0.", "0", 5000, "1");
            assert_eq!(
                set(&below_zero, f, Some("0:1"), None),
                Err(Problem::BelowMin)
            );
            let half = Some("0:10:0.5");
            assert_eq!(
                set(&long("0.25", "0", 3000, "1"), f, half, None),
                text("0.5")
            );
            assert_eq!(
                set(&long("0.25", "0", 3000, ""), f, half, None),
                text("0.5")
            );
            assert_eq!(
                set(&long("0.24", "9", 3000, ""), f, half, None),
                text("0.0")
            );
            let signed = Some("-10:10:0.5");
            assert_eq!(
                set(&long("-0.25", "0", 3000, "1"), f, signed, None),
                text("-0.5")
            );
            assert_eq!(
                set(&long("-0.25", "0", 3000, ""), f, signed, None),
                text("0.0")
            );
            assert_eq!(
                set(
                    &long("1e-", "9", 30, ""),
                    f,
                    Some("::1"),
                    Some(Current::Integer(0))
                ),
                text("0.0")
            );
            let many = long("1.", "7", 1_000_000, "");
            assert_eq!(set(&many, f, None, None), text(&many));
            assert_eq!(set(&many, f, Some("0:1e400:0.001"), None), text("1.778"));
        }

        /// `$format` scales beyond [`MAX_FORMAT_SCALE`] give `formatScale` where
        /// Python computes an answer (known divergence).
        #[test]
        fn format_scale_limit() {
            let (i, f) = (Datatype::Integer, Datatype::Float);
            assert_eq!(
                set("5", i, Some("1e30000::1"), None),
                Err(Problem::FormatScale)
            );
            assert_eq!(
                set("5", i, Some("0::1e30000"), None),
                Err(Problem::FormatScale)
            );
            assert_eq!(
                set("5", f, Some(":1e-30000:1"), None),
                Err(Problem::FormatScale)
            );
            assert_eq!(
                set("5", i, Some("1e20000::1"), None),
                Err(Problem::BelowMin)
            );
        }

        #[test]
        fn json_set() {
            let j = Datatype::Json;
            let json = |t: &str| set(t, j, None, None);
            // Correctly rounded floats: Python repr text reads back exactly.
            assert_eq!(
                json("[912068.5437784989]"),
                Ok(SetValue::Json(serde_json::json!([912068.5437784989])))
            );
            let Ok(SetValue::Json(v)) = json("[2.283038836859574890682e201]") else {
                panic!("not json");
            };
            // Python: json.dumps(json.loads(...)).
            assert_eq!(
                crate::pyfmt::to_json(&v, false),
                "[2.2830388368595749e+201]"
            );
            // `-0` is the integer 0, as in Python.
            assert_eq!(
                json("[-0, -0.0]").map(|v| match v {
                    SetValue::Json(v) => crate::pyfmt::to_json(&v, false),
                    _ => String::new(),
                }),
                Ok(String::from("[0, -0.0]"))
            );
            // Python accepts these (python-sdk#110); the convention does not.
            for t in ["[\"\\ud800\"]", "[\"\\udc00\"]", "[1e400]", "[-1e400]"] {
                assert_eq!(json(t), Err(Problem::JsonInvalid), "{t}");
            }
            assert_eq!(json("[18446744073709551616]"), Err(Problem::JsonInvalid));
            assert_eq!(json("[-9223372036854775809]"), Err(Problem::JsonInvalid));
        }

        #[test]
        fn other_set() {
            assert_eq!(set("true", Datatype::Boolean, None, None), text("true"));
            assert_eq!(
                set("True", Datatype::Boolean, None, None),
                Err(Problem::BooleanGrammar)
            );
            assert_eq!(set("a", Datatype::Enum, Some("a,b"), None), text("a"));
            assert_eq!(
                set("c", Datatype::Enum, Some("a,b"), None),
                Err(Problem::EnumNotInFormat)
            );
            assert_eq!(
                set("", Datatype::Enum, Some("a,,b"), None),
                Err(Problem::EnumNotInFormat)
            );
            assert_eq!(set("x", Datatype::Enum, None, None), text("x"));
            assert_eq!(set("x", Datatype::Enum, Some(""), None), text("x"));
            assert_eq!(
                set("", Datatype::Enum, None, None),
                Err(Problem::EnumNotInFormat)
            );
            assert_eq!(
                set("rgb,1,2,3", Datatype::Color, Some("rgb"), None),
                text("rgb,1,2,3")
            );
            assert_eq!(
                set("rgb,1,2", Datatype::Color, None, None),
                Err(Problem::ColorComponentCount)
            );
            assert_eq!(set("PT1S", Datatype::Duration, None, None), text("PT1S"));
            assert_eq!(
                set("PT", Datatype::Duration, None, None),
                Err(Problem::DurationGrammar)
            );
            assert_eq!(
                set("2024-01-01", Datatype::Datetime, None, None),
                text("2024-01-01")
            );
            assert_eq!(
                set("2024-13-01", Datatype::Datetime, None, None),
                Err(Problem::DatetimeRange)
            );
            assert_eq!(
                set("x", Datatype::Datetime, None, None),
                Err(Problem::DatetimeGrammar)
            );
            assert_eq!(
                set("\0", Datatype::String, None, None),
                Ok(SetValue::String(String::new()))
            );
            assert_eq!(
                set("", Datatype::String, None, None),
                Ok(SetValue::String(String::new()))
            );
            assert_eq!(
                set("\0\0", Datatype::String, None, None),
                Ok(SetValue::String("\0\0".into()))
            );
            assert_eq!(
                set(" [1, {\"a\": null}] ", Datatype::Json, None, None),
                Ok(SetValue::Json(serde_json::json!([1, {"a": null}])))
            );
            assert_eq!(
                set("1", Datatype::Json, None, None),
                Err(Problem::JsonNotArrayOrObject)
            );
            assert_eq!(
                set("\"a\"", Datatype::Json, None, None),
                Err(Problem::JsonNotArrayOrObject)
            );
            assert_eq!(
                set("[NaN]", Datatype::Json, None, None),
                Err(Problem::JsonInvalid)
            );
            assert_eq!(
                set("[Infinity]", Datatype::Json, None, None),
                Err(Problem::JsonInvalid)
            );
            assert_eq!(
                set("[1,]", Datatype::Json, None, None),
                Err(Problem::JsonInvalid)
            );
            assert_eq!(
                set("", Datatype::Json, None, None),
                Err(Problem::JsonInvalid)
            );
        }

        #[test]
        fn encoders() {
            assert_eq!(encode_integer(-42), "-42");
            assert_eq!(encode_float(1.0).as_deref(), Ok("1.0"));
            assert_eq!(encode_float(1e16).as_deref(), Ok("1e16"));
            assert_eq!(encode_float(1e-5).as_deref(), Ok("1e-05"));
            assert_eq!(encode_float(f64::NAN), Err(Problem::FloatNotFinite));
            assert_eq!(
                encode_float(f64::NEG_INFINITY),
                Err(Problem::FloatNotFinite)
            );
            assert_eq!(encode_float32(21.4).as_deref(), Ok("21.4"));
            assert_eq!(encode_float32(f32::NAN), Err(Problem::FloatNotFinite));
            assert_eq!(encode_boolean(true), "true");
            assert_eq!(encode_boolean(false), "false");
            assert_eq!(encode_duration(0, 0), "PT0S");
            assert_eq!(encode_duration(3600, 0), "PT1H");
            assert_eq!(encode_duration(86400 + 61, 0), "PT24H1M1S");
            assert_eq!(encode_duration(60, 0), "PT1M");
            assert_eq!(encode_duration(0, 500_000), "PT0.5S");
            assert_eq!(encode_duration(3601, 1), "PT1H1.000001S");
            assert_eq!(encode_duration(1, 2_500_000), "PT3.5S");
            assert_eq!(
                encode_json(&serde_json::json!({"b": [1.5, "\u{e9}"], "a": 1})).as_deref(),
                Ok("{\"a\": 1, \"b\": [1.5, \"\\u00e9\"]}")
            );
            assert_eq!(
                encode_json(&serde_json::json!(1)),
                Err(Problem::JsonNotArrayOrObject)
            );
            assert_eq!(encode_json(&serde_json::json!([])).as_deref(), Ok("[]"));
        }
    }
}
