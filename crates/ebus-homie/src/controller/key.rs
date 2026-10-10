//! Registry keys: a device id within a domain, looked up without allocating.

use alloc::sync::Arc;
use core::{borrow::Borrow, cmp::Ordering};

use super::Id;

/// A device in a domain, ordered by id, then domain.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct DevKey {
    pub(super) id: Id,
    pub(super) domain: Arc<str>,
}

/// A borrowed view of a [`DevKey`], so maps keyed by `DevKey` can be searched
/// with `(&str, &str)`.
pub(super) trait KeyRef {
    fn id(&self) -> &str;
    fn domain(&self) -> &str;
}

impl KeyRef for DevKey {
    fn id(&self) -> &str {
        self.id.as_str()
    }
    fn domain(&self) -> &str {
        &self.domain
    }
}

/// `(id, domain)`.
impl KeyRef for (&str, &str) {
    fn id(&self) -> &str {
        self.0
    }
    fn domain(&self) -> &str {
        self.1
    }
}

impl<'a> Borrow<dyn KeyRef + 'a> for DevKey {
    fn borrow(&self) -> &(dyn KeyRef + 'a) {
        self
    }
}

impl PartialEq for dyn KeyRef + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id() && self.domain() == other.domain()
    }
}

impl Eq for dyn KeyRef + '_ {}

impl PartialOrd for dyn KeyRef + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn KeyRef + '_ {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id()
            .cmp(other.id())
            .then_with(|| self.domain().cmp(other.domain()))
    }
}
