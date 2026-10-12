//! [`Mdns`], the backend a host holds, dispatching to the one compiled in.

// With no backend compiled in, every method body is an empty match.
#![cfg_attr(
    not(any(feature = "mdns-sd", all(feature = "avahi", target_os = "linux"))),
    allow(unreachable_code, unused_variables)
)]

use core::convert::Infallible;

use ebus_discovery::{ServiceRecord, Sighting, TxtRecord};

use crate::{Backend, Browse, Error, Group, GroupReport, HostName};

/// The mDNS backend a host holds; clones share one backend.
///
/// The backend is chosen at run time from those compiled in, so a host
/// passes `&Mdns` wherever a [`Backend`] is wanted.
#[derive(Clone)]
pub struct Mdns {
    inner: Inner,
}

#[derive(Clone)]
enum Inner {
    #[cfg(all(feature = "avahi", target_os = "linux"))]
    Avahi(crate::avahi::Avahi),
    #[cfg(feature = "mdns-sd")]
    MdnsSd(crate::mdns_sd::MdnsSd),
    /// Lets every match compile when no backend is compiled in.
    #[allow(dead_code)]
    None(Infallible),
}

impl core::fmt::Debug for Mdns {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match &self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            Inner::Avahi(_) => "avahi",
            #[cfg(feature = "mdns-sd")]
            Inner::MdnsSd(_) => "mdns-sd",
            Inner::None(n) => match *n {},
        };
        f.debug_struct("Mdns").field("backend", &name).finish()
    }
}

/// Options for the mdns-sd backend.
#[cfg(feature = "mdns-sd")]
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct MdnsSdOptions {
    /// The UDP port, 5353 by default. On another port mdns-sd answers every
    /// query by unicast to the querier's address and port, which reaches only
    /// one of several daemons sharing that port on one host.
    pub port: u16,
    /// True: the loopback interfaces alone, for tests within one host.
    /// False (the default): every interface but loopback.
    pub loopback: bool,
    /// When not empty, the interfaces with these names alone, in place of
    /// the choice `loopback` makes; python's `interfaces=` list of names.
    pub interfaces: Vec<String>,
}

#[cfg(feature = "mdns-sd")]
impl Default for MdnsSdOptions {
    fn default() -> Self {
        Self {
            port: 5353,
            loopback: false,
            interfaces: Vec::new(),
        }
    }
}

impl Mdns {
    /// The system's backend: Avahi when built with `avahi`, on Linux, with
    /// `org.freedesktop.Avahi` reachable; else mdns-sd when built with
    /// `mdns-sd`; else [`Error::NoBackend`], which is all a build with neither
    /// feature returns.
    pub async fn system() -> Result<Mdns, Error> {
        #[cfg(all(feature = "avahi", target_os = "linux"))]
        match Self::avahi().await {
            Ok(m) => return Ok(m),
            Err(e @ (Error::NoSystemBus | Error::AvahiUnavailable)) => {
                log::info!("reason=avahiUnavailable,error={e}");
            }
            Err(e) => return Err(e),
        }
        #[cfg(feature = "mdns-sd")]
        {
            Self::mdns_sd(MdnsSdOptions::default())
        }
        #[cfg(not(feature = "mdns-sd"))]
        {
            Err(Error::NoBackend)
        }
    }

    /// The Avahi backend: a D-Bus connection to avahi-daemon. Needs a tokio
    /// runtime. [`Error::NoSystemBus`] without a system bus,
    /// [`Error::AvahiUnavailable`] when avahi-daemon is not on it.
    #[cfg(all(feature = "avahi", target_os = "linux"))]
    pub async fn avahi() -> Result<Mdns, Error> {
        Ok(crate::avahi::Avahi::connect().await?.into())
    }

    /// The mdns-sd backend: one responder thread and its sockets. Needs a
    /// tokio runtime.
    #[cfg(feature = "mdns-sd")]
    pub fn mdns_sd(opts: MdnsSdOptions) -> Result<Mdns, Error> {
        Ok(Mdns {
            inner: Inner::MdnsSd(crate::mdns_sd::MdnsSd::new(&opts)?),
        })
    }
}

#[cfg(all(feature = "avahi", target_os = "linux"))]
impl From<crate::avahi::Avahi> for Mdns {
    fn from(a: crate::avahi::Avahi) -> Self {
        Mdns {
            inner: Inner::Avahi(a),
        }
    }
}

/// A browse of an [`Mdns`] backend.
pub struct MdnsBrowse {
    inner: BrowseInner,
}

enum BrowseInner {
    #[cfg(all(feature = "avahi", target_os = "linux"))]
    Avahi(crate::avahi::AvahiBrowse),
    #[cfg(feature = "mdns-sd")]
    MdnsSd(crate::mdns_sd::MdnsSdBrowse),
    #[allow(dead_code)]
    None(Infallible),
}

impl core::fmt::Debug for MdnsBrowse {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match &self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            BrowseInner::Avahi(_) => "avahi",
            #[cfg(feature = "mdns-sd")]
            BrowseInner::MdnsSd(_) => "mdns-sd",
            BrowseInner::None(n) => match *n {},
        };
        f.debug_struct("MdnsBrowse")
            .field("backend", &name)
            .finish_non_exhaustive()
    }
}

/// A published group of an [`Mdns`] backend.
pub struct MdnsGroup {
    inner: GroupInner,
}

enum GroupInner {
    #[cfg(all(feature = "avahi", target_os = "linux"))]
    Avahi(crate::avahi::AvahiGroup),
    #[cfg(feature = "mdns-sd")]
    MdnsSd(crate::mdns_sd::MdnsSdGroup),
    #[allow(dead_code)]
    None(Infallible),
}

impl core::fmt::Debug for MdnsGroup {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match &self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            GroupInner::Avahi(_) => "avahi",
            #[cfg(feature = "mdns-sd")]
            GroupInner::MdnsSd(_) => "mdns-sd",
            GroupInner::None(n) => match *n {},
        };
        f.debug_struct("MdnsGroup")
            .field("backend", &name)
            .finish_non_exhaustive()
    }
}

impl Backend for Mdns {
    type Browse = MdnsBrowse;
    type Group = MdnsGroup;

    async fn browse(&self, service_type: &str) -> Result<MdnsBrowse, Error> {
        let inner = match &self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            Inner::Avahi(m) => BrowseInner::Avahi(m.browse(service_type).await?),
            #[cfg(feature = "mdns-sd")]
            Inner::MdnsSd(m) => BrowseInner::MdnsSd(m.browse(service_type)?),
            Inner::None(n) => match *n {},
        };
        Ok(MdnsBrowse { inner })
    }

    async fn server_name(&self, host: &HostName) -> Result<String, Error> {
        match &self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            Inner::Avahi(m) => m.server_name(host).await,
            #[cfg(feature = "mdns-sd")]
            Inner::MdnsSd(m) => m.server_name(host),
            Inner::None(n) => match *n {},
        }
    }

    async fn publish(
        &self,
        host: &HostName,
        instance: &str,
        records: &[ServiceRecord],
    ) -> Result<MdnsGroup, Error> {
        let inner = match &self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            Inner::Avahi(m) => GroupInner::Avahi(m.publish(host, instance, records).await?),
            #[cfg(feature = "mdns-sd")]
            Inner::MdnsSd(m) => GroupInner::MdnsSd(m.publish(host, instance, records)?),
            Inner::None(n) => match *n {},
        };
        Ok(MdnsGroup { inner })
    }
}

impl Browse for MdnsBrowse {
    async fn next(&mut self) -> Option<Sighting> {
        match &mut self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            BrowseInner::Avahi(b) => b.next().await,
            #[cfg(feature = "mdns-sd")]
            BrowseInner::MdnsSd(b) => b.next().await,
            BrowseInner::None(n) => match *n {},
        }
    }

    async fn requery(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            BrowseInner::Avahi(b) => b.requery().await,
            #[cfg(feature = "mdns-sd")]
            BrowseInner::MdnsSd(b) => b.requery(),
            BrowseInner::None(n) => match *n {},
        }
    }
}

impl Group for MdnsGroup {
    async fn next_report(&mut self) -> Option<GroupReport> {
        match &mut self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            GroupInner::Avahi(g) => g.next_report().await,
            #[cfg(feature = "mdns-sd")]
            GroupInner::MdnsSd(g) => g.next_report().await,
            GroupInner::None(n) => match *n {},
        }
    }

    async fn update_txt(&mut self, service_type: &str, txt: &TxtRecord) -> Result<(), Error> {
        match &mut self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            GroupInner::Avahi(g) => g.update_txt(service_type, txt).await,
            #[cfg(feature = "mdns-sd")]
            GroupInner::MdnsSd(g) => g.update_txt(service_type, txt),
            GroupInner::None(n) => match *n {},
        }
    }

    async fn republish(&mut self, records: &[ServiceRecord]) -> Result<(), Error> {
        match &mut self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            GroupInner::Avahi(g) => g.republish(records).await,
            #[cfg(feature = "mdns-sd")]
            GroupInner::MdnsSd(g) => g.republish(records),
            GroupInner::None(n) => match *n {},
        }
    }

    async fn withdraw(self) -> Result<(), Error> {
        match self.inner {
            #[cfg(all(feature = "avahi", target_os = "linux"))]
            GroupInner::Avahi(g) => g.withdraw().await,
            #[cfg(feature = "mdns-sd")]
            GroupInner::MdnsSd(g) => g.withdraw().await,
            GroupInner::None(n) => match n {},
        }
    }
}
