//! The mdns-sd backend: one responder thread per [`MdnsSd`].
//!
//! mdns-sd keeps one browse per service type, and a browse channel holds 10
//! events that the daemon thread sends with a blocking send
//! (service_daemon.rs:441-445, 2927, 2940). So each type has one hub: a task
//! drains the daemon's channel and fans sightings out to every [`MdnsSdBrowse`]
//! of that type, replaying the instances already seen to a new one.

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ebus_discovery::{
    InstanceKey, ScopedAddr, ServiceRecord, Sighting, Slot, TxtRecord, is_local_name, strip_dot,
};
use mdns_sd::{
    DaemonEvent, IfKind, IfPredicate, RRType, ResolvedService, ScopedIp, ServiceDaemon,
    ServiceEvent, ServiceInfo, TxtProperty,
};
use tokio::sync::mpsc;

use crate::mdns::MdnsSdOptions;
use crate::{Error, GroupReport, HostName, lock};

/// How long a withdrawal waits for the daemon to confirm each record.
const UNREGISTER_WAIT: Duration = Duration::from_secs(2);

/// The sockets of avahi-daemon; one exists while it runs (_mdns_core.py:871).
const AVAHI_SOCKETS: [&str; 2] = ["/run/avahi-daemon/socket", "/var/run/avahi-daemon/socket"];

fn backend_err(e: mdns_sd::Error) -> Error {
    Error::Backend(e.to_string())
}

/// The mdns-sd backend; clones share one daemon, shut down with the last.
#[derive(Clone)]
pub(crate) struct MdnsSd {
    shared: Arc<Shared>,
}

struct Shared {
    daemon: ServiceDaemon,
    state: Arc<Mutex<State>>,
    next_id: AtomicU64,
    rt: tokio::runtime::Handle,
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}

#[derive(Default)]
struct State {
    /// Browses by service type with domain, such as `_mqtt._tcp.local.`.
    hubs: HashMap<String, Hub>,
    groups: HashMap<u64, GroupEntry>,
    /// Host names the daemon renamed after a collision, lowercased, to the
    /// new name.
    renamed: HashMap<String, String>,
}

struct Hub {
    /// Identifies the daemon browse the hub's task drains; a requery starts
    /// a new one.
    generation: u64,
    subscribers: Vec<(u64, mpsc::UnboundedSender<Sighting>)>,
    /// The latest `Seen` of each instance, by lowercased full name.
    seen: BTreeMap<String, Sighting>,
}

struct GroupEntry {
    /// Lowercased full names of the group's records.
    fullnames: Vec<String>,
    /// The SRV target, lowercased.
    host: String,
    notes: mpsc::UnboundedSender<Note>,
    collided: bool,
    /// Lowercased full names registered with probing and not yet announced
    /// under that name. The daemon announces a record once its probes end
    /// without a conflict (service_daemon.rs:2460-2530).
    probing: Vec<String>,
}

/// What the daemon watcher tells a group.
enum Note {
    Report(GroupReport),
    /// Every probing record has been announced under its own name.
    ProbesDone,
}

fn is_loopback_interface(name: &str, ip: IpAddr) -> bool {
    ip.is_loopback() || name == "lo" || name == "lo0"
}

impl MdnsSd {
    pub(crate) fn new(opts: &MdnsSdOptions) -> Result<Self, Error> {
        let rt = tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
        let daemon = ServiceDaemon::new_with_port(opts.port).map_err(backend_err)?;
        let loopback = IfPredicate::new(|i| is_loopback_interface(&i.name, i.ip()));
        if !opts.interfaces.is_empty() {
            daemon.disable_interface(IfKind::All).map_err(backend_err)?;
            let names = opts.interfaces.clone();
            daemon
                .enable_interface(IfKind::Predicate(IfPredicate::new(move |i| {
                    names.contains(&i.name)
                })))
                .map_err(backend_err)?;
        } else if opts.loopback {
            daemon.disable_interface(IfKind::All).map_err(backend_err)?;
            daemon
                .enable_interface(IfKind::Predicate(loopback))
                .map_err(backend_err)?;
        } else {
            daemon
                .disable_interface(IfKind::Predicate(loopback))
                .map_err(backend_err)?;
        }
        let monitor = daemon.monitor().map_err(backend_err)?;
        let state = Arc::new(Mutex::new(State::default()));
        rt.spawn(watch_daemon(state.clone(), monitor));
        Ok(Self {
            shared: Arc::new(Shared {
                daemon,
                state,
                next_id: AtomicU64::new(1),
                rt,
            }),
        })
    }

    pub(crate) fn browse(&self, service_type: &str) -> Result<MdnsSdBrowse, Error> {
        let ty = format!("{}.local.", strip_dot(service_type).to_ascii_lowercase());
        let id = self.shared.id();
        let (tx, rx) = mpsc::unbounded_channel();
        let mut st = lock(&self.shared.state);
        match st.hubs.get_mut(&ty) {
            Some(hub) => {
                for s in hub.seen.values() {
                    let _ = tx.send(s.clone());
                }
                hub.subscribers.push((id, tx));
            }
            None => {
                let events = self.shared.daemon.browse(&ty).map_err(backend_err)?;
                st.hubs.insert(
                    ty.clone(),
                    Hub {
                        generation: id,
                        subscribers: vec![(id, tx)],
                        seen: BTreeMap::new(),
                    },
                );
                self.shared
                    .rt
                    .spawn(pump(self.shared.state.clone(), ty.clone(), id, events));
            }
        }
        drop(st);
        Ok(MdnsSdBrowse {
            shared: self.shared.clone(),
            ty,
            id,
            rx,
        })
    }

    pub(crate) fn server_name(&self, host: &HostName) -> Result<String, Error> {
        let requested = match host {
            HostName::Auto => auto_host_name(
                &gethostname::gethostname().to_string_lossy(),
                os_responder_present(),
            ),
            HostName::Own(name) => own_host_name(name)?,
        };
        Ok(current_name(&lock(&self.shared.state).renamed, &requested))
    }

    pub(crate) fn publish(
        &self,
        host: &HostName,
        instance: &str,
        records: &[ServiceRecord],
    ) -> Result<MdnsSdGroup, Error> {
        let server = self.server_name(host)?;
        let id = self.shared.id();
        let (tx, notes) = mpsc::unbounded_channel();
        lock(&self.shared.state).groups.insert(
            id,
            GroupEntry {
                fullnames: Vec::new(),
                host: server.to_lowercase(),
                notes: tx,
                collided: false,
                probing: Vec::new(),
            },
        );
        let mut group = MdnsSdGroup {
            shared: self.shared.clone(),
            id,
            instance: instance.into(),
            server,
            regs: Vec::new(),
            notes,
            owed: false,
            deferred: None,
        };
        group.republish(records)?;
        Ok(group)
    }
}

impl Shared {
    fn id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// A name the daemon renamed, followed to its latest name.
fn current_name(renamed: &HashMap<String, String>, requested: &str) -> String {
    let mut name = requested.to_string();
    for _ in 0..renamed.len() {
        match renamed.get(&name.to_lowercase()) {
            Some(next) => name = next.clone(),
            None => break,
        }
    }
    name
}

/// `<label>.local.` for `HostName::Own`.
fn own_host_name(name: &str) -> Result<String, Error> {
    let name = strip_dot(name);
    let label = name.len().saturating_sub(".local".len());
    if !is_local_name(name) || name[..label].is_empty() || name[..label].contains('.') {
        return Err(Error::HostName(format!(
            "{name:?} is not of the form <label>.local"
        )));
    }
    Ok(format!("{name}."))
}

/// `HostName::Auto` for mdns-sd: the first label of `system`, with `-ebus`
/// appended where an OS responder owns the plain name (_mdns_core.py:871-895).
fn auto_host_name(system: &str, os_responder: bool) -> String {
    let label = system.split('.').next().unwrap_or("");
    let label = if label.is_empty() { "ebus" } else { label };
    if os_responder {
        format!("{label}-ebus.local.")
    } else {
        format!("{label}.local.")
    }
}

/// True where an OS mDNS responder owns the host's `.local` name: macOS and
/// Windows always, Linux while avahi-daemon runs (_mdns_core.py:874-883).
fn os_responder_present() -> bool {
    cfg!(any(target_os = "macos", windows))
        || AVAHI_SOCKETS
            .iter()
            .any(|p| std::path::Path::new(p).exists())
}

fn service_info(
    service_type: &str,
    instance: &str,
    server: &str,
    port: u16,
    txt: &TxtRecord,
) -> Result<ServiceInfo, Error> {
    let props: Vec<TxtProperty> = txt
        .iter()
        .map(|(k, v)| match v {
            Some(v) => TxtProperty::from((k, v)),
            None => TxtProperty::from(k),
        })
        .collect();
    Ok(ServiceInfo::new(
        &format!("{service_type}.local."),
        instance,
        server,
        (),
        port,
        props,
    )
    .map_err(backend_err)?
    .enable_addr_auto())
}

/// Drain one daemon browse into its hub until the daemon stops it.
async fn pump(
    state: Arc<Mutex<State>>,
    ty: String,
    generation: u64,
    events: mdns_sd::Receiver<ServiceEvent>,
) {
    while let Ok(ev) = events.recv_async().await {
        let (fullname, sighting) = match ev {
            ServiceEvent::ServiceResolved(r) => match seen(&r) {
                Some(s) => (r.fullname.to_lowercase(), s),
                None => continue,
            },
            ServiceEvent::ServiceRemoved(t, fullname) => match key_of(&t, &fullname) {
                Some(key) => (fullname.to_lowercase(), Sighting::Gone { key, slot: None }),
                None => continue,
            },
            _ => continue,
        };
        let mut st = lock(&state);
        let Some(hub) = st.hubs.get_mut(&ty).filter(|h| h.generation == generation) else {
            return;
        };
        let changed = match &sighting {
            Sighting::Seen { .. } => {
                hub.seen.insert(fullname, sighting.clone()).as_ref() != Some(&sighting)
            }
            Sighting::Gone { .. } => hub.seen.remove(&fullname).is_some(),
        };
        if changed {
            hub.subscribers
                .retain(|(_, tx)| tx.send(sighting.clone()).is_ok());
        }
    }
    let mut st = lock(&state);
    if st.hubs.get(&ty).is_some_and(|h| h.generation == generation) {
        st.hubs.remove(&ty);
    }
}

/// Route the daemon's name changes to the groups they concern.
async fn watch_daemon(state: Arc<Mutex<State>>, events: mdns_sd::Receiver<DaemonEvent>) {
    while let Ok(ev) = events.recv_async().await {
        let mut st = lock(&state);
        match ev {
            DaemonEvent::NameChange(c) if matches!(c.rr_type, RRType::A | RRType::AAAA) => {
                let original = c.original.to_lowercase();
                if st.renamed.get(&original) == Some(&c.new_name) {
                    continue;
                }
                log::info!(
                    "reason=hostNameConflict,from={},to={}",
                    c.original,
                    c.new_name
                );
                st.renamed.insert(original.clone(), c.new_name);
                for g in st.groups.values() {
                    if g.host == original {
                        let _ = g.notes.send(Note::Report(GroupReport::HostRenamed));
                    }
                }
            }
            DaemonEvent::NameChange(c) => {
                let original = c.original.to_lowercase();
                for g in st.groups.values_mut() {
                    if !g.collided && g.fullnames.contains(&original) {
                        g.collided = true;
                        let _ = g.notes.send(Note::Report(GroupReport::Collision));
                    }
                }
            }
            DaemonEvent::Announce(fullname, _) => {
                let fullname = fullname.to_lowercase();
                for g in st.groups.values_mut() {
                    let Some(at) = g.probing.iter().position(|n| *n == fullname) else {
                        continue;
                    };
                    g.probing.swap_remove(at);
                    if g.probing.is_empty() && !g.collided {
                        let _ = g.notes.send(Note::ProbesDone);
                    }
                }
            }
            DaemonEvent::Error(e) => {
                let msg = e.to_string();
                log::warn!("reason=mdnsDaemonError,error={msg}");
            }
            _ => {}
        }
    }
}

/// The instance a service type and full name name, with the instance
/// label's `\.` and `\\` escapes undone.
fn key_of(ty_domain: &str, fullname: &str) -> Option<InstanceKey> {
    let ty = strip_dot(ty_domain);
    let service_type = ty
        .len()
        .checked_sub(".local".len())
        .filter(|&at| ty.is_char_boundary(at) && ty[at..].eq_ignore_ascii_case(".local"))
        .map_or(ty, |at| &ty[..at]);
    let full = strip_dot(fullname);
    let at = full.len().checked_sub(ty.len() + 1)?;
    if !full.is_char_boundary(at)
        || !full[at..].starts_with('.')
        || !full[at + 1..].eq_ignore_ascii_case(ty)
    {
        return None;
    }
    let mut name = String::with_capacity(at);
    let mut chars = full[..at].chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => name.extend(chars.next()),
            c => name.push(c),
        }
    }
    Some(InstanceKey {
        service_type: service_type.into(),
        name,
    })
}

fn seen(r: &ResolvedService) -> Option<Sighting> {
    let key = key_of(&r.ty_domain, &r.fullname)?;
    let txt = TxtRecord::from_strings(r.txt_properties.iter().map(|p| {
        let mut s = p.key().as_bytes().to_vec();
        if let Some(v) = p.val() {
            s.push(b'=');
            s.extend_from_slice(v);
        }
        s
    }));
    let mut addrs: Vec<ScopedAddr> = r
        .addresses
        .iter()
        .map(|a| match a {
            ScopedIp::V6(v6) if v6.addr().is_unicast_link_local() => ScopedAddr {
                ip: IpAddr::V6(*v6.addr()),
                ifindex: Some(v6.scope_id().index),
            },
            other => ScopedAddr::new(other.to_ip_addr()),
        })
        .collect();
    addrs.sort();
    Some(Sighting::Seen {
        key,
        slot: Slot::Whole,
        server: r.host.clone(),
        port: r.port,
        txt,
        addrs,
    })
}

/// A browse of one service type on an [`MdnsSd`] daemon.
pub(crate) struct MdnsSdBrowse {
    shared: Arc<Shared>,
    ty: String,
    id: u64,
    rx: mpsc::UnboundedReceiver<Sighting>,
}

impl MdnsSdBrowse {
    pub(crate) async fn next(&mut self) -> Option<Sighting> {
        self.rx.recv().await
    }

    /// Stop the type's daemon browse and start it again (a new query).
    pub(crate) fn requery(&mut self) -> Result<(), Error> {
        let shared = &self.shared;
        let mut st = lock(&shared.state);
        let Some(hub) = st.hubs.get_mut(&self.ty) else {
            return Err(Error::Closed);
        };
        let _ = shared.daemon.stop_browse(&self.ty);
        let events = shared.daemon.browse(&self.ty).map_err(backend_err)?;
        let generation = shared.id();
        hub.generation = generation;
        shared.rt.spawn(pump(
            shared.state.clone(),
            self.ty.clone(),
            generation,
            events,
        ));
        Ok(())
    }
}

impl Drop for MdnsSdBrowse {
    fn drop(&mut self) {
        let mut st = lock(&self.shared.state);
        let Some(hub) = st.hubs.get_mut(&self.ty) else {
            return;
        };
        hub.subscribers.retain(|(id, _)| *id != self.id);
        if hub.subscribers.is_empty() {
            st.hubs.remove(&self.ty);
            let _ = self.shared.daemon.stop_browse(&self.ty);
        }
    }
}

/// Records registered under one instance name on an [`MdnsSd`] daemon.
pub(crate) struct MdnsSdGroup {
    shared: Arc<Shared>,
    id: u64,
    instance: String,
    /// The SRV target the records carry.
    server: String,
    regs: Vec<Reg>,
    notes: mpsc::UnboundedReceiver<Note>,
    /// `Established` is owed once no record probes.
    owed: bool,
    /// Records to register in place once probing ends: registering a record
    /// again while it probes made the daemon rename it `<name> (N)` without
    /// a `NameChange` event.
    deferred: Option<Vec<ServiceRecord>>,
}

/// One registered record.
struct Reg {
    record: ServiceRecord,
    info: ServiceInfo,
}

impl MdnsSdGroup {
    pub(crate) async fn next_report(&mut self) -> Option<GroupReport> {
        loop {
            match self.notes.recv().await? {
                Note::Report(r) => {
                    if r == GroupReport::Collision {
                        self.owed = false;
                        self.deferred = None;
                    }
                    return Some(r);
                }
                Note::ProbesDone => {
                    if let Some(records) = self.deferred.take() {
                        if let Err(e) = self.apply(&records) {
                            return Some(GroupReport::Failed(e.to_string()));
                        }
                    }
                    if self.owed && !self.is_probing() {
                        self.owed = false;
                        return Some(GroupReport::Established);
                    }
                }
            }
        }
    }

    fn is_probing(&self) -> bool {
        lock(&self.shared.state)
            .groups
            .get(&self.id)
            .is_some_and(|g| !g.probing.is_empty())
    }

    /// Register `records` now, or once probing ends.
    fn apply_or_defer(&mut self, records: &[ServiceRecord]) -> Result<(), Error> {
        if self.is_probing() {
            self.deferred = Some(records.to_vec());
            return Ok(());
        }
        self.deferred = None;
        self.apply(records)
    }

    pub(crate) fn update_txt(&mut self, service_type: &str, txt: &TxtRecord) -> Result<(), Error> {
        let mut records = match &self.deferred {
            Some(d) => d.clone(),
            None => self.regs.iter().map(|r| r.record.clone()).collect(),
        };
        let Some(r) = records
            .iter_mut()
            .find(|r| r.service_type.eq_ignore_ascii_case(strip_dot(service_type)))
        else {
            return Err(Error::Backend(format!(
                "no {service_type} record to update"
            )));
        };
        r.txt = txt.clone();
        self.apply_or_defer(&records)
    }

    /// Reports `Established` once every record is announced under its own
    /// name: at once when none probes, else when the daemon has announced
    /// the probing ones. A name the daemon changes reports `Collision`
    /// instead.
    pub(crate) fn republish(&mut self, records: &[ServiceRecord]) -> Result<(), Error> {
        self.owed = true;
        self.apply_or_defer(records)?;
        if self.deferred.is_none() && !self.is_probing() {
            self.owed = false;
            if let Some(g) = lock(&self.shared.state).groups.get(&self.id) {
                let _ = g.notes.send(Note::Report(GroupReport::Established));
            }
        }
        Ok(())
    }

    /// Bring the registrations to `records` under the current host name. A
    /// record already registered is registered again in place, announced
    /// without probing since the name is this host's; withdrawing it first
    /// would make the daemon take its own goodbye for a collision.
    fn apply(&mut self, records: &[ServiceRecord]) -> Result<(), Error> {
        let server = current_name(&lock(&self.shared.state).renamed, &self.server);
        let mut next = Vec::with_capacity(records.len());
        for record in records {
            let held = self
                .regs
                .iter()
                .position(|r| r.record.service_type == record.service_type)
                .map(|at| self.regs.remove(at));
            let in_place = match held {
                Some(h) if h.record == *record && h.info.get_hostname() == server => {
                    next.push(h);
                    continue;
                }
                Some(_) => true,
                None => false,
            };
            let registered = service_info(
                record.service_type,
                &self.instance,
                &server,
                record.port,
                &record.txt,
            )
            .and_then(|mut info| {
                info.set_requires_probe(!in_place);
                let fullname = info.get_fullname().to_lowercase();
                // Before `register`, so its announcement cannot come first.
                if !in_place {
                    self.set_probing(&fullname, true);
                }
                if let Err(e) = self.shared.daemon.register(info.clone()) {
                    self.set_probing(&fullname, false);
                    return Err(backend_err(e));
                }
                Ok(info)
            });
            let info = match registered {
                Ok(info) => info,
                Err(e) => {
                    // Keep tracking what is registered, so withdrawal reaches it.
                    self.regs.append(&mut next);
                    return Err(e);
                }
            };
            next.push(Reg {
                record: record.clone(),
                info,
            });
        }
        for gone in core::mem::replace(&mut self.regs, next) {
            let _ = self.shared.daemon.unregister(gone.info.get_fullname());
        }
        self.server = server;
        let mut st = lock(&self.shared.state);
        if let Some(g) = st.groups.get_mut(&self.id) {
            g.fullnames = self
                .regs
                .iter()
                .map(|r| r.info.get_fullname().to_lowercase())
                .collect();
            let GroupEntry {
                fullnames, probing, ..
            } = g;
            probing.retain(|n| fullnames.contains(n));
            g.host = self.server.to_lowercase();
        }
        Ok(())
    }

    /// Add `fullname` to the records awaiting their announcement, or drop it.
    fn set_probing(&self, fullname: &str, probing: bool) {
        if let Some(g) = lock(&self.shared.state).groups.get_mut(&self.id) {
            g.probing.retain(|n| n != fullname);
            if probing {
                g.probing.push(fullname.into());
            }
        }
    }

    pub(crate) async fn withdraw(mut self) -> Result<(), Error> {
        lock(&self.shared.state).groups.remove(&self.id);
        let mut result = Ok(());
        for reg in core::mem::take(&mut self.regs) {
            match self.shared.daemon.unregister(reg.info.get_fullname()) {
                Ok(status) => {
                    let _ = tokio::time::timeout(UNREGISTER_WAIT, status.recv_async()).await;
                }
                Err(e) => result = Err(backend_err(e)),
            }
        }
        result
    }
}

impl Drop for MdnsSdGroup {
    fn drop(&mut self) {
        lock(&self.shared.state).groups.remove(&self.id);
        for reg in &self.regs {
            let _ = self.shared.daemon.unregister(reg.info.get_fullname());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_host_name_rule() {
        assert_eq!(auto_host_name("box.example.com", false), "box.local.");
        assert_eq!(auto_host_name("box", true), "box-ebus.local.");
        assert_eq!(auto_host_name("", false), "ebus.local.");
    }

    #[test]
    fn own_host_names() {
        assert_eq!(own_host_name("h.local").unwrap(), "h.local.");
        assert_eq!(own_host_name("H.Local.").unwrap(), "H.Local.");
        assert!(own_host_name("h.example.com").is_err());
        assert!(own_host_name(".local").is_err());
        assert!(own_host_name("a.b.local.").is_err());
    }

    #[test]
    fn renamed_chain() {
        let mut m = HashMap::new();
        m.insert("h.local.".to_string(), "h-2.local.".to_string());
        m.insert("h-2.local.".to_string(), "h-3.local.".to_string());
        assert_eq!(current_name(&m, "H.local."), "h-3.local.");
        assert_eq!(current_name(&m, "g.local."), "g.local.");
    }

    #[test]
    fn keys() {
        let k = key_of("_mqtt._tcp.local.", "a\\.b\\\\c._MQTT._tcp.local.").unwrap();
        assert_eq!(k.service_type, "_mqtt._tcp");
        assert_eq!(k.name, "a.b\\c");
        assert!(key_of("_mqtt._tcp.local.", "x._http._tcp.local.").is_none());
        assert!(key_of("_mqtt._tcp.local.", "_mqtt._tcp.local.").is_none());
    }
}
