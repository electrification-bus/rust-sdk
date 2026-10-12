//! Device `$state` values (convention.md: Device Lifecycle).

use core::fmt;
use core::str::FromStr;

/// A device `$state` value (convention.md: Device Lifecycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeviceState {
    /// `init`: connected, not yet ready to operate.
    Init,
    /// `ready`: fully published and subscribed.
    Ready,
    /// `disconnected`: cleanly disconnected.
    Disconnected,
    /// `sleeping`: the device is sleeping.
    Sleeping,
    /// `lost`: badly disconnected (the root device's last will).
    Lost,
}

impl DeviceState {
    /// All states, in convention order.
    pub const ALL: [DeviceState; 5] = [
        DeviceState::Init,
        DeviceState::Ready,
        DeviceState::Disconnected,
        DeviceState::Sleeping,
        DeviceState::Lost,
    ];

    /// The wire form, e.g. `"ready"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            DeviceState::Init => "init",
            DeviceState::Ready => "ready",
            DeviceState::Disconnected => "disconnected",
            DeviceState::Sleeping => "sleeping",
            DeviceState::Lost => "lost",
        }
    }

    /// Parse the wire form; exact lowercase match only.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "init" => Some(DeviceState::Init),
            "ready" => Some(DeviceState::Ready),
            "disconnected" => Some(DeviceState::Disconnected),
            "sleeping" => Some(DeviceState::Sleeping),
            "lost" => Some(DeviceState::Lost),
            _ => None,
        }
    }
}

impl fmt::Display for DeviceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned by [`DeviceState::from_str`] for an unknown state string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseStateError {
    /// Not one of the five lowercase state values.
    UnknownState,
}

impl ParseStateError {
    /// camelCase error code.
    pub const fn as_str(self) -> &'static str {
        match self {
            ParseStateError::UnknownState => "unknownState",
        }
    }
}

impl fmt::Display for ParseStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl core::error::Error for ParseStateError {}

impl FromStr for DeviceState {
    type Err = ParseStateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        DeviceState::parse(s).ok_or(ParseStateError::UnknownState)
    }
}

/// Effective state of a device given its root's reported state.
///
/// `root_state` is `None` for a root device or when the root's state is not
/// known. A root in `lost` makes its whole tree `lost`; in every other case the
/// device's own state stands (convention.md: Device hierarchy).
pub fn effective_state(root_state: Option<DeviceState>, own: DeviceState) -> DeviceState {
    match root_state {
        Some(DeviceState::Lost) => DeviceState::Lost,
        _ => own,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for s in DeviceState::ALL {
            assert_eq!(DeviceState::parse(s.as_str()), Some(s));
            assert_eq!(s.as_str().parse::<DeviceState>(), Ok(s));
        }
    }

    #[test]
    fn parse_is_exact() {
        for s in ["", "Ready", "READY", " ready", "ready\0", "unknown"] {
            assert_eq!(DeviceState::parse(s), None, "{s:?}");
            assert_eq!(s.parse::<DeviceState>(), Err(ParseStateError::UnknownState));
        }
    }

    #[test]
    fn effective_state_table() {
        for own in DeviceState::ALL {
            assert_eq!(effective_state(None, own), own);
            assert_eq!(effective_state(Some(DeviceState::Ready), own), own);
            for root in DeviceState::ALL {
                let expected = if root == DeviceState::Lost {
                    DeviceState::Lost
                } else {
                    own
                };
                assert_eq!(effective_state(Some(root), own), expected);
            }
        }
    }
}
