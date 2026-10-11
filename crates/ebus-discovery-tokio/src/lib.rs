//! mDNS broker discovery and host advertisement for eBus on tokio.
//!
//! The protocol decisions live in the sans-IO [`ebus_discovery`] crate; this
//! crate runs them over an mDNS stack. A host makes one [`Mdns`] and uses it
//! for its two jobs:
//!
//! | Job | Call |
//! |---|---|
//! | Find the site's broker before building its MQTT options | [`find_broker`], or [`list_brokers`] to pick from every broker seen |
//! | Find the same broker again after its address changes | [`reresolve`] |
//! | Advertise the host's own services | [`advertise`], then [`Advertiser::update`] and [`Advertiser::withdraw`] |
//!
//! The backends implement [`Backend`]: Avahi under the `avahi` feature, a
//! client of avahi-daemon over D-Bus that compiles only on Linux, and
//! mdns-sd under the `mdns-sd` feature, with its own responder thread and
//! sockets. [`Mdns::system`] picks the backend at run time.
//!
//! ```no_run
//! # #[cfg(feature = "mdns-sd")]
//! # async fn host() -> Result<(), Box<dyn std::error::Error>> {
//! use ebus_discovery_tokio::ebus_discovery::{BrokerConfig, BrokerEndpoint, BrokerMode};
//! use ebus_discovery_tokio::{Mdns, find_broker};
//!
//! let mdns = Mdns::system().await?;
//! let mut cfg = BrokerConfig::default();
//! cfg.mode = Some(BrokerMode::DiscoveryWithFallback);
//! cfg.url = Some(BrokerEndpoint::from_url("mqtt://broker.example:1883")?);
//! let broker = find_broker(&mdns, cfg).await?;
//! let (host, port) = (broker.connect_host().into_owned(), broker.port);
//! # let _ = (host, port);
//! # Ok(())
//! # }
//! ```

mod advertiser;
#[cfg(all(feature = "avahi", target_os = "linux"))]
mod avahi;
mod backend;
#[cfg(test)]
mod fake;
mod mdns;
#[cfg(feature = "mdns-sd")]
mod mdns_sd;
mod search;

pub use ebus_discovery;

pub use advertiser::{AdState, Advertiser, advertise};
pub use backend::{Backend, Browse, Group, GroupReport, HostName};
#[cfg(feature = "mdns-sd")]
pub use mdns::MdnsSdOptions;
pub use mdns::{Mdns, MdnsBrowse, MdnsGroup};
pub use search::{find_broker, list_brokers, reresolve};

use core::fmt;

use ebus_discovery::{ConfigError, TxtError};

/// A discovery or advertising failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// No backend is compiled in, or none is usable on this host.
    NoBackend,
    /// No D-Bus system bus to reach Avahi over.
    NoSystemBus,
    /// avahi-daemon is not running or not reachable.
    AvahiUnavailable,
    /// The call needs a tokio runtime and none is running.
    NoRuntime,
    /// A broker configuration a search cannot run with.
    Config(ConfigError),
    /// An advertisement that cannot be built.
    Txt(TxtError),
    /// A host name the backend cannot advertise under.
    HostName(String),
    /// The backend refused or failed an operation.
    Backend(String),
    /// The backend, or the advertiser's task, has stopped.
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoBackend => f.write_str("no mDNS backend is available"),
            Self::NoSystemBus => f.write_str("no D-Bus system bus"),
            Self::AvahiUnavailable => f.write_str("avahi-daemon is not reachable"),
            Self::NoRuntime => f.write_str("no tokio runtime is running"),
            Self::Config(e) => write!(f, "broker configuration: {e}"),
            Self::Txt(e) => write!(f, "advertisement: {e}"),
            Self::HostName(e) => write!(f, "host name: {e}"),
            Self::Backend(e) => write!(f, "mDNS backend: {e}"),
            Self::Closed => f.write_str("the mDNS backend has stopped"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(e) => Some(e),
            Self::Txt(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Self {
        Self::Config(e)
    }
}

impl From<TxtError> for Error {
    fn from(e: TxtError) -> Self {
        Self::Txt(e)
    }
}

/// A std mutex guard; a poisoned lock yields its data.
pub(crate) fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}
