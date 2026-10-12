//! Typed property values: the borrowed form the core compares and encodes,
//! and the Rust types that map onto Homie datatypes.

use alloc::{borrow::Cow, string::String, sync::Arc};
use core::{fmt, time::Duration};

use serde_json::Value;

use crate::datatype::{
    Datatype, Problem, SetValue, check_color, check_datetime, is_duration_payload,
};

/// A value as the core reads it: borrowed, so nothing is copied before the
/// unchanged-value gate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ValueRef<'a> {
    /// An `integer`.
    Integer(i64),
    /// A `float` held as `f64`.
    Float(f64),
    /// A `float` held as `f32`, encoded with the shortest digits that read
    /// back as the same `f32`.
    Float32(f32),
    /// A `boolean`.
    Boolean(bool),
    /// A `string`; `""` is published as the `0x00` payload.
    String(&'a str),
    /// An `enum`, as an index into the property's `$format` values.
    Enum(u16),
    /// A `color` payload such as `rgb,255,0,0`.
    Color(&'a str),
    /// A `datetime` payload.
    Datetime(&'a str),
    /// A `duration`, to microsecond precision.
    Duration {
        /// Whole seconds.
        secs: u64,
        /// Microseconds below one second.
        micros: u32,
    },
    /// A `json` array or object.
    Json(&'a Value),
}

/// Why a value was refused; nothing was published.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ValueError {
    /// NaN or an infinity.
    NotFinite,
    /// Outside the `$format` range, or a color component out of range.
    OutOfRange,
    /// Not one of the `$format` values: an enum index past the list, or a
    /// color type `$format` does not list.
    NotInFormat,
    /// The value does not fit the property's datatype.
    WrongDatatype,
    /// The property is `retained: false`; use `emit`.
    IsEvent,
    /// The property is retained; `emit` is for `retained: false` ones.
    NotEvent,
    /// The property has no `$target`.
    NotTarget,
    /// The property or device was removed.
    Gone,
    /// `disconnected` and `lost` are for child devices; the root's `lost`
    /// comes from `declare_lost`.
    NotForRoot,
}

impl ValueError {
    /// camelCase reason code.
    pub const fn as_str(self) -> &'static str {
        match self {
            ValueError::NotFinite => "notFinite",
            ValueError::OutOfRange => "outOfRange",
            ValueError::NotInFormat => "notInFormat",
            ValueError::WrongDatatype => "wrongDatatype",
            ValueError::IsEvent => "isEvent",
            ValueError::NotEvent => "notEvent",
            ValueError::NotTarget => "notTarget",
            ValueError::Gone => "gone",
            ValueError::NotForRoot => "notForRoot",
        }
    }
}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl core::error::Error for ValueError {}

/// A Rust type that a property holds.
pub trait HomieValue: Clone + 'static {
    /// The property's datatype.
    const DATATYPE: Datatype;
    /// The value as the core reads it.
    fn as_value(&self) -> ValueRef<'_>;
    /// True when `self` and `other` publish the same payload; floats compare
    /// by bits, so `-0.0` and `0.0` differ.
    fn same(&self, other: &Self) -> bool;
    /// The value of an accepted `/set`, or `None` when it does not fit.
    fn from_set(v: &SetValue) -> Option<Self>;
    /// The `$format` the type implies: a range for a narrow integer, the
    /// variants of an enum.
    fn default_format() -> Option<Cow<'static, str>> {
        None
    }
}

macro_rules! integer {
    ($($t:ty),*) => {$(
        impl HomieValue for $t {
            const DATATYPE: Datatype = Datatype::Integer;
            fn as_value(&self) -> ValueRef<'_> {
                ValueRef::Integer(i64::from(*self))
            }
            fn same(&self, other: &Self) -> bool {
                self == other
            }
            fn from_set(v: &SetValue) -> Option<Self> {
                match v {
                    SetValue::Text(t) => t.parse::<i64>().ok().and_then(|i| <$t>::try_from(i).ok()),
                    _ => None,
                }
            }
            fn default_format() -> Option<Cow<'static, str>> {
                if i128::from(<$t>::MIN) == i128::from(i64::MIN) {
                    return None;
                }
                Some(Cow::Owned(alloc::format!("{}:{}", <$t>::MIN, <$t>::MAX)))
            }
        }
    )*};
}

integer!(i64, i32, i16, i8, u32, u16, u8);

impl HomieValue for f64 {
    const DATATYPE: Datatype = Datatype::Float;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::Float(*self)
    }
    fn same(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::Text(t) => t.parse().ok().filter(|x: &f64| x.is_finite()),
            _ => None,
        }
    }
}

impl HomieValue for f32 {
    const DATATYPE: Datatype = Datatype::Float;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::Float32(*self)
    }
    fn same(&self, other: &Self) -> bool {
        self.to_bits() == other.to_bits()
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::Text(t) => t.parse().ok().filter(|x: &f32| x.is_finite()),
            _ => None,
        }
    }
}

impl HomieValue for bool {
    const DATATYPE: Datatype = Datatype::Boolean;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::Boolean(*self)
    }
    fn same(&self, other: &Self) -> bool {
        self == other
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::Text(t) if t == "true" => Some(true),
            SetValue::Text(t) if t == "false" => Some(false),
            _ => None,
        }
    }
}

impl HomieValue for String {
    const DATATYPE: Datatype = Datatype::String;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::String(self)
    }
    fn same(&self, other: &Self) -> bool {
        self == other
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::String(s) => Some(s.clone()),
            _ => None,
        }
    }
}

impl HomieValue for Arc<str> {
    const DATATYPE: Datatype = Datatype::String;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::String(self)
    }
    fn same(&self, other: &Self) -> bool {
        self == other
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::String(s) => Some(Arc::from(s.as_str())),
            _ => None,
        }
    }
}

impl HomieValue for Duration {
    const DATATYPE: Datatype = Datatype::Duration;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::Duration {
            secs: self.as_secs(),
            micros: self.subsec_micros(),
        }
    }
    fn same(&self, other: &Self) -> bool {
        self.as_secs() == other.as_secs() && self.subsec_micros() == other.subsec_micros()
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::Text(t) => parse_duration(t),
            _ => None,
        }
    }
}

impl HomieValue for Value {
    const DATATYPE: Datatype = Datatype::Json;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::Json(self)
    }
    fn same(&self, other: &Self) -> bool {
        self == other
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::Json(j) => Some(j.clone()),
            _ => None,
        }
    }
}

/// A `color` payload, checked when built (convention.md: Color).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Color(Arc<str>);

impl Color {
    /// `text` as a color (`rgb,255,0,0`, `hsv,...`, `xyz,...`), or the
    /// problem with it.
    pub fn new(text: &str) -> Result<Self, Problem> {
        check_color(text, None).map(|()| Color(Arc::from(text)))
    }

    /// The payload text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl HomieValue for Color {
    const DATATYPE: Datatype = Datatype::Color;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::Color(&self.0)
    }
    fn same(&self, other: &Self) -> bool {
        self == other
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::Text(t) => Color::new(t).ok(),
            _ => None,
        }
    }
}

/// A `datetime` payload, checked when built (convention.md: DateTime).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Datetime(Arc<str>);

impl Datetime {
    /// `text` as a datetime, or the problem with it.
    pub fn new(text: &str) -> Result<Self, Problem> {
        check_datetime(text).map(|()| Datetime(Arc::from(text)))
    }

    /// The payload text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl HomieValue for Datetime {
    const DATATYPE: Datatype = Datatype::Datetime;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::Datetime(&self.0)
    }
    fn same(&self, other: &Self) -> bool {
        self == other
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::Text(t) => Datetime::new(t).ok(),
            _ => None,
        }
    }
}

/// A Rust enum published as a Homie `enum`; its variants become `$format`.
pub trait HomieEnum: Copy + 'static {
    /// The payload of each variant, in index order.
    const VARIANTS: &'static [&'static str];
    /// This variant's index into [`VARIANTS`](Self::VARIANTS).
    fn index(self) -> u16;
    /// The variant at `i`.
    fn from_index(i: u16) -> Option<Self>;
}

impl<E: HomieEnum> HomieValue for E {
    const DATATYPE: Datatype = Datatype::Enum;
    fn as_value(&self) -> ValueRef<'_> {
        ValueRef::Enum(self.index())
    }
    fn same(&self, other: &Self) -> bool {
        self.index() == other.index()
    }
    fn from_set(v: &SetValue) -> Option<Self> {
        match v {
            SetValue::Text(t) => E::VARIANTS
                .iter()
                .position(|v| v == t)
                .and_then(|i| u16::try_from(i).ok())
                .and_then(E::from_index),
            _ => None,
        }
    }
    fn default_format() -> Option<Cow<'static, str>> {
        Some(Cow::Owned(E::VARIANTS.join(",")))
    }
}

/// The value of a `duration` payload `PT[nH][nM][n[.n]S]`, with the
/// fraction truncated to microseconds; `None` when it is not one or
/// overflows.
pub(super) fn parse_duration(text: &str) -> Option<Duration> {
    if !is_duration_payload(text) {
        return None;
    }
    let mut rest = text.get(2..)?;
    let mut secs: u64 = 0;
    let mut micros: u32 = 0;
    while !rest.is_empty() {
        let end = rest.find(['H', 'M', 'S'])?;
        let (number, unit) = (rest.get(..end)?, rest.as_bytes().get(end)?);
        rest = rest.get(end + 1..)?;
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        let whole: u64 = whole.parse().ok()?;
        let scale = match unit {
            b'H' => 3600,
            b'M' => 60,
            _ => 1,
        };
        secs = secs.checked_add(whole.checked_mul(scale)?)?;
        if !fraction.is_empty() {
            let mut digits = fraction.bytes().chain(core::iter::repeat(b'0')).take(6);
            micros = digits.try_fold(0u32, |acc, d| {
                Some(acc * 10 + u32::from(d.checked_sub(b'0')?))
            })?;
        }
    }
    Some(Duration::new(secs, micros.checked_mul(1000)?))
}
