//! The seam between the discovery logic and an mDNS stack.

use core::future::Future;

use ebus_discovery::{ServiceRecord, Sighting, TxtRecord};

use crate::Error;

/// The host name a host advertises its services under: the SRV target.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum HostName {
    /// The backend's choice: avahi-daemon's own name; for mdns-sd,
    /// `<host>.local.` where no OS mDNS responder owns that name, else
    /// `<host>-ebus.local.`, where `<host>` is the first label of the system
    /// host name.
    #[default]
    Auto,
    /// A name of the form `<label>.local.`, published with this host's
    /// addresses (mdns-sd only).
    Own(String),
}

/// What a published [`Group`] reports.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum GroupReport {
    /// Every record is registered under the instance name.
    Established,
    /// Another host holds the instance name for one of the records.
    Collision,
    /// The host name the records point at was renamed after a collision;
    /// [`Backend::server_name`] gives the new one, and
    /// [`Group::republish`] brings the records' TXT up to date.
    HostRenamed,
    /// The backend gave up on the group.
    Failed(String),
    /// The backend restarted and is registering the group again.
    Reset,
}

/// An mDNS stack: browsing and publishing.
///
/// The methods return `impl Future + Send` so a host can run them on a
/// multi-threaded runtime.
pub trait Backend: Clone + Send + Sync + 'static {
    /// A running browse.
    type Browse: Browse;
    /// A published set of records under one instance name.
    type Group: Group;

    /// Browse one service type, such as `_mqtt._tcp`.
    fn browse(
        &self,
        service_type: &str,
    ) -> impl Future<Output = Result<Self::Browse, Error>> + Send;

    /// The SRV target records published under `host` get, with a trailing
    /// dot; after a host rename, the new name.
    fn server_name(&self, host: &HostName) -> impl Future<Output = Result<String, Error>> + Send;

    /// Register `records` under `instance` and `host`. The group's first
    /// report says whether the name was taken.
    fn publish(
        &self,
        host: &HostName,
        instance: &str,
        records: &[ServiceRecord],
    ) -> impl Future<Output = Result<Self::Group, Error>> + Send;
}

/// A running browse of one service type; dropping it stops the browse.
pub trait Browse: Send + 'static {
    /// The next sighting; `None` once the backend has stopped. Cancel-safe.
    fn next(&mut self) -> impl Future<Output = Option<Sighting>> + Send;

    /// Ask the network again.
    fn requery(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Published records under one instance name; dropping it withdraws them.
pub trait Group: Send + 'static {
    /// The next report; `None` once the backend has stopped. Cancel-safe.
    fn next_report(&mut self) -> impl Future<Output = Option<GroupReport>> + Send;

    /// Replace the TXT record of the record of `service_type`.
    fn update_txt(
        &mut self,
        service_type: &str,
        txt: &TxtRecord,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Register `records` again under the group's instance name, in place,
    /// with the SRV target [`Backend::server_name`] now gives: a record whose
    /// port, TXT or target changed is replaced, a new one added, and one no
    /// longer listed withdrawn. The group reports
    /// [`GroupReport::Established`] again once done. Withdrawing and
    /// publishing again under the same name instead can make a backend take
    /// its own goodbyes for a collision.
    fn republish(
        &mut self,
        records: &[ServiceRecord],
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Withdraw every record, sending goodbyes.
    fn withdraw(self) -> impl Future<Output = Result<(), Error>> + Send;
}
