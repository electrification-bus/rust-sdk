//! Validated Homie ids and property addresses, shared by both roles.

use alloc::sync::Arc;
use core::{borrow::Borrow, fmt, ops::Deref};

use crate::id::is_valid_id;

/// A Homie id (device, node, property or alert), validated with
/// [`is_valid_id`] and cheap to clone.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(Arc<str>);

impl Id {
    /// `s` as an id, or `None` when it is not a valid Homie id.
    pub fn new(s: &str) -> Option<Self> {
        is_valid_id(s).then(|| Id(Arc::from(s)))
    }

    /// The id text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for Id {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Id {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for Id {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for Id {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for Id {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One property of one device.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PropertyAddr {
    /// Device id.
    pub device: Id,
    /// Node id.
    pub node: Id,
    /// Property id.
    pub property: Id,
}

impl PropertyAddr {
    /// The address of `device/node/property`, or `None` when any level is not a
    /// valid id.
    pub fn new(device: &str, node: &str, property: &str) -> Option<Self> {
        Some(PropertyAddr {
            device: Id::new(device)?,
            node: Id::new(node)?,
            property: Id::new(property)?,
        })
    }
}
