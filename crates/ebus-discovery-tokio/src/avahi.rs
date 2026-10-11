//! The Avahi backend over avahi-daemon's D-Bus API.
//!
//! One task owns the D-Bus objects and their state; the handles send it
//! commands. Every message the connection receives is forwarded to that task
//! in arrival order, so a signal that avahi-daemon emits before the call that
//! created its object has returned (a `ServiceBrowserNew` browser starts by
//! itself, dbus-protocol.c:1073) waits in the queue until the object's path is
//! known. The proxies are hand-written over [`zbus::Connection::call_method`].

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ebus_discovery::{InstanceKey, ScopedAddr, ServiceRecord, Sighting, Slot, TxtRecord};
use tokio::sync::{mpsc, oneshot};
use zbus::export::futures_core::Stream;
use zbus::export::serde::Serialize;
use zbus::message::{Message, Type as MessageType};
use zbus::zvariant::{DynamicType, OwnedObjectPath};
use zbus::{Connection, MessageStream};

use crate::{Error, GroupReport, HostName};

const AVAHI: &str = "org.freedesktop.Avahi";
const SERVER: &str = "org.freedesktop.Avahi.Server";
const SERVER2: &str = "org.freedesktop.Avahi.Server2";
const BROWSER: &str = "org.freedesktop.Avahi.ServiceBrowser";
const RESOLVER: &str = "org.freedesktop.Avahi.ServiceResolver";
const ENTRY_GROUP: &str = "org.freedesktop.Avahi.EntryGroup";
const DBUS: &str = "org.freedesktop.DBus";

/// Match rules for the signals the backend handles: everything avahi-daemon
/// sends, and avahi-daemon leaving or joining the bus.
const MATCH_RULES: [&str; 2] = [
    "type='signal',sender='org.freedesktop.Avahi'",
    "type='signal',sender='org.freedesktop.DBus',interface='org.freedesktop.DBus',\
     member='NameOwnerChanged',arg0='org.freedesktop.Avahi'",
];

// address.h:40-47
const IF_UNSPEC: i32 = -1;
const PROTO_UNSPEC: i32 = -1;
#[cfg(test)]
const PROTO_INET: i32 = 0;
const PROTO_INET6: i32 = 1;
// defs.h:250, 263
const PUBLISH_NO_COOKIE: u32 = 32;
const LOOKUP_USE_MULTICAST: u32 = 2;
// defs.h:125-133
const SERVER_REGISTERING: i32 = 1;
const SERVER_RUNNING: i32 = 2;
const SERVER_COLLISION: i32 = 3;
const SERVER_FAILURE: i32 = 4;
// defs.h:216-222
const GROUP_ESTABLISHED: i32 = 2;
const GROUP_COLLISION: i32 = 3;
const GROUP_FAILURE: i32 = 4;

/// The Avahi backend; clones share one D-Bus connection, closed with the
/// last.
#[derive(Clone)]
pub(crate) struct Avahi {
    shared: Arc<Shared>,
}

struct Shared {
    cmds: mpsc::UnboundedSender<Cmd>,
    next_id: AtomicU64,
}

impl Shared {
    fn id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn ask<T>(&self, cmd: impl FnOnce(oneshot::Sender<T>) -> Cmd) -> Result<T, Error> {
        let (tx, rx) = oneshot::channel();
        self.cmds.send(cmd(tx)).map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)
    }
}

type Reply<T = ()> = oneshot::Sender<Result<T, Error>>;

enum Cmd {
    Browse {
        id: u64,
        service_type: String,
        tx: mpsc::UnboundedSender<Sighting>,
        reply: Reply,
    },
    Requery {
        id: u64,
        reply: Reply,
    },
    Unbrowse {
        id: u64,
    },
    ServerName {
        reply: Reply<String>,
    },
    Publish {
        id: u64,
        instance: String,
        records: Vec<ServiceRecord>,
        reports: mpsc::UnboundedSender<GroupReport>,
        reply: Reply,
    },
    UpdateTxt {
        id: u64,
        service_type: String,
        txt: TxtRecord,
        reply: Reply,
    },
    Republish {
        id: u64,
        records: Vec<ServiceRecord>,
        reply: Reply,
    },
    Withdraw {
        id: u64,
        reply: Option<Reply>,
    },
}

fn is_error(e: &zbus::Error, names: &[&str]) -> bool {
    matches!(e, zbus::Error::MethodError(name, _, _) if names.contains(&name.as_str()))
}

fn dbus_err(e: zbus::Error) -> Error {
    let gone = is_error(
        &e,
        &[
            "org.freedesktop.DBus.Error.ServiceUnknown",
            "org.freedesktop.DBus.Error.NameHasNoOwner",
        ],
    ) || matches!(&e, zbus::Error::MethodError(name, _, _)
        if name.as_str().starts_with("org.freedesktop.DBus.Error.Spawn"));
    if gone {
        Error::AvahiUnavailable
    } else {
        Error::Backend(e.to_string())
    }
}

/// True for the reply of an avahi-daemon older than 0.8, which has no
/// `Server2` and its `*Prepare` methods.
fn no_server2(e: &zbus::Error) -> bool {
    is_error(
        e,
        &[
            "org.freedesktop.DBus.Error.UnknownMethod",
            "org.freedesktop.DBus.Error.UnknownInterface",
        ],
    )
}

fn is_collision(e: &zbus::Error) -> bool {
    is_error(e, &["org.freedesktop.Avahi.CollisionError"])
}

/// `name` with one trailing dot.
fn with_dot(name: &str) -> String {
    format!("{}.", name.trim_end_matches('.'))
}

impl Avahi {
    pub(crate) async fn connect() -> Result<Self, Error> {
        Self::open(true).await
    }

    /// `server2` false: use the methods of avahi-daemon before 0.8 alone.
    async fn open(server2: bool) -> Result<Self, Error> {
        let rt = tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
        let conn = Connection::system().await.map_err(|e| {
            log::debug!("reason=noSystemBus,error={e}");
            Error::NoSystemBus
        })?;
        // Opened before any match rule exists, so no signal is missed.
        let mut stream = MessageStream::from(&conn);
        stream.set_max_queued(1024);
        for rule in MATCH_RULES {
            conn.call_method(
                Some(DBUS),
                "/org/freedesktop/DBus",
                Some(DBUS),
                "AddMatch",
                &rule,
            )
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        }
        // Asked first, so a call does not start avahi-daemon by bus
        // activation.
        let owned: bool = conn
            .call_method(
                Some(DBUS),
                "/org/freedesktop/DBus",
                Some(DBUS),
                "NameHasOwner",
                &AVAHI,
            )
            .await
            .and_then(|r| r.body().deserialize())
            .map_err(|e| Error::Backend(e.to_string()))?;
        if !owned {
            return Err(Error::AvahiUnavailable);
        }
        let mut worker = Worker::new(conn);
        worker.server2 = server2;
        let state: i32 = worker.server_call("GetState", &()).await?;
        worker.running = state == SERVER_RUNNING;
        worker.host = worker.host_name().await?;
        let (msgs_tx, msgs) = mpsc::unbounded_channel();
        let forwarder = rt.spawn(forward(stream, msgs_tx));
        let (cmds, cmds_rx) = mpsc::unbounded_channel();
        rt.spawn(worker.run(cmds_rx, msgs, forwarder));
        Ok(Self {
            shared: Arc::new(Shared {
                cmds,
                next_id: AtomicU64::new(1),
            }),
        })
    }

    pub(crate) async fn browse(&self, service_type: &str) -> Result<AvahiBrowse, Error> {
        let id = self.shared.id();
        let (tx, rx) = mpsc::unbounded_channel();
        let service_type = ebus_discovery::strip_dot(service_type).to_string();
        // Built first, so a failed browse is still freed by its drop.
        let browse = AvahiBrowse {
            shared: self.shared.clone(),
            id,
            rx,
        };
        self.shared
            .ask(|reply| Cmd::Browse {
                id,
                service_type,
                tx,
                reply,
            })
            .await??;
        Ok(browse)
    }

    pub(crate) async fn server_name(&self, host: &HostName) -> Result<String, Error> {
        match host {
            HostName::Auto => self.shared.ask(|reply| Cmd::ServerName { reply }).await?,
            HostName::Own(name) => Err(Error::HostName(format!(
                "{name}: Avahi advertises under avahi-daemon's own host name; use HostName::Auto"
            ))),
        }
    }

    pub(crate) async fn publish(
        &self,
        host: &HostName,
        instance: &str,
        records: &[ServiceRecord],
    ) -> Result<AvahiGroup, Error> {
        if let HostName::Own(_) = host {
            self.server_name(host).await?;
        }
        let id = self.shared.id();
        let (tx, reports) = mpsc::unbounded_channel();
        let group = AvahiGroup {
            shared: self.shared.clone(),
            id,
            reports,
            withdrawn: false,
        };
        self.shared
            .ask(|reply| Cmd::Publish {
                id,
                instance: instance.into(),
                records: records.to_vec(),
                reports: tx,
                reply,
            })
            .await??;
        Ok(group)
    }
}

/// A browse of one service type: one avahi `ServiceBrowser`, and a
/// `ServiceResolver` per item it reports.
pub(crate) struct AvahiBrowse {
    shared: Arc<Shared>,
    id: u64,
    rx: mpsc::UnboundedReceiver<Sighting>,
}

impl AvahiBrowse {
    pub(crate) async fn next(&mut self) -> Option<Sighting> {
        self.rx.recv().await
    }

    pub(crate) async fn requery(&mut self) -> Result<(), Error> {
        let id = self.id;
        self.shared.ask(|reply| Cmd::Requery { id, reply }).await?
    }
}

impl Drop for AvahiBrowse {
    fn drop(&mut self) {
        let _ = self.shared.cmds.send(Cmd::Unbrowse { id: self.id });
    }
}

/// Records published in one avahi `EntryGroup`.
pub(crate) struct AvahiGroup {
    shared: Arc<Shared>,
    id: u64,
    reports: mpsc::UnboundedReceiver<GroupReport>,
    withdrawn: bool,
}

impl AvahiGroup {
    pub(crate) async fn next_report(&mut self) -> Option<GroupReport> {
        self.reports.recv().await
    }

    pub(crate) async fn update_txt(
        &mut self,
        service_type: &str,
        txt: &TxtRecord,
    ) -> Result<(), Error> {
        let (id, service_type, txt) = (self.id, service_type.to_string(), txt.clone());
        self.shared
            .ask(|reply| Cmd::UpdateTxt {
                id,
                service_type,
                txt,
                reply,
            })
            .await?
    }

    pub(crate) async fn republish(&mut self, records: &[ServiceRecord]) -> Result<(), Error> {
        let (id, records) = (self.id, records.to_vec());
        self.shared
            .ask(|reply| Cmd::Republish { id, records, reply })
            .await?
    }

    pub(crate) async fn withdraw(mut self) -> Result<(), Error> {
        self.withdrawn = true;
        let id = self.id;
        self.shared
            .ask(|reply| Cmd::Withdraw {
                id,
                reply: Some(reply),
            })
            .await?
    }
}

impl Drop for AvahiGroup {
    fn drop(&mut self) {
        if !self.withdrawn {
            let _ = self.shared.cmds.send(Cmd::Withdraw {
                id: self.id,
                reply: None,
            });
        }
    }
}

/// Forward every message the connection receives, in order.
async fn forward(mut stream: MessageStream, tx: mpsc::UnboundedSender<Message>) {
    loop {
        let next = core::future::poll_fn(|cx| core::pin::Pin::new(&mut stream).poll_next(cx)).await;
        match next {
            Some(Ok(m)) if m.message_type() == MessageType::Signal => {
                if tx.send(m).is_err() {
                    return;
                }
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => log::debug!("reason=dbusMessageError,error={e}"),
            None => return,
        }
    }
}

/// An item of a `ServiceBrowser`: interface, protocol and instance name.
type ItemKey = (i32, i32, String);

enum Route {
    Browser(u64),
    Resolver(u64, ItemKey),
    Group(u64),
}

struct BrowseState {
    service_type: String,
    tx: mpsc::UnboundedSender<Sighting>,
    browser: Option<String>,
    resolvers: HashMap<ItemKey, String>,
    /// Items the browser reported and has not removed.
    items: HashSet<ItemKey>,
    /// Items from before a requery or a daemon restart, not reported again
    /// yet; removed at the next `AllForNow`.
    stale: HashSet<ItemKey>,
    /// The latest `Seen` sent for each item.
    seen: HashMap<ItemKey, Sighting>,
}

struct GroupState {
    instance: String,
    records: Vec<ServiceRecord>,
    reports: mpsc::UnboundedSender<GroupReport>,
    path: Option<String>,
    committed: bool,
    established: bool,
    collided: bool,
    /// avahi-daemon's host name when the records were added: their SRV
    /// target.
    host: String,
}

struct Worker {
    conn: Connection,
    routes: HashMap<String, Route>,
    browses: HashMap<u64, BrowseState>,
    groups: HashMap<u64, GroupState>,
    /// The server state is `RUNNING`; entry groups are committed only then.
    running: bool,
    /// avahi-daemon's host name, with a trailing dot.
    host: String,
    server2: bool,
}

impl Worker {
    fn new(conn: Connection) -> Self {
        Self {
            conn,
            routes: HashMap::new(),
            browses: HashMap::new(),
            groups: HashMap::new(),
            running: false,
            host: String::new(),
            server2: true,
        }
    }

    async fn call<B>(
        &self,
        path: &str,
        iface: &str,
        method: &str,
        body: &B,
    ) -> zbus::Result<Message>
    where
        B: Serialize + DynamicType,
    {
        self.conn
            .call_method(Some(AVAHI), path, Some(iface), method, body)
            .await
    }

    async fn server_call<B, R>(&self, method: &str, body: &B) -> Result<R, Error>
    where
        B: Serialize + DynamicType,
        R: for<'d> zbus::zvariant::DynamicDeserialize<'d>,
    {
        let reply = self
            .call("/", SERVER, method, body)
            .await
            .map_err(dbus_err)?;
        reply
            .body()
            .deserialize()
            .map_err(|e| Error::Backend(e.to_string()))
    }

    async fn host_name(&self) -> Result<String, Error> {
        let name: String = self.server_call("GetHostNameFqdn", &()).await?;
        Ok(with_dot(&name))
    }

    /// Create a browser or resolver: `Server2.<prepare>` then `Start`, or
    /// `Server.<new>` on an avahi-daemon without `Server2`.
    async fn create<B>(
        &mut self,
        prepare: &str,
        new: &str,
        iface: &str,
        body: &B,
    ) -> Result<String, Error>
    where
        B: Serialize + DynamicType,
    {
        if self.server2 {
            match self.call("/", SERVER2, prepare, body).await {
                Ok(reply) => {
                    let path: OwnedObjectPath = reply
                        .body()
                        .deserialize()
                        .map_err(|e| Error::Backend(e.to_string()))?;
                    let path = path.as_str().to_string();
                    if let Err(e) = self.call(&path, iface, "Start", &()).await {
                        self.free(&path, iface).await;
                        return Err(dbus_err(e));
                    }
                    return Ok(path);
                }
                Err(e) if no_server2(&e) => self.server2 = false,
                Err(e) => return Err(dbus_err(e)),
            }
        }
        let reply = self.call("/", SERVER, new, body).await.map_err(dbus_err)?;
        let path: OwnedObjectPath = reply
            .body()
            .deserialize()
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(path.as_str().to_string())
    }

    async fn free(&mut self, path: &str, iface: &str) {
        self.routes.remove(path);
        if let Err(e) = self.call(path, iface, "Free", &()).await {
            log::debug!("reason=avahiFreeFailed,path={path},error={e}");
        }
    }

    async fn run(
        mut self,
        mut cmds: mpsc::UnboundedReceiver<Cmd>,
        mut msgs: mpsc::UnboundedReceiver<Message>,
        forwarder: tokio::task::JoinHandle<()>,
    ) {
        loop {
            tokio::select! {
                biased;
                m = msgs.recv() => match m {
                    Some(m) => self.on_message(&m).await,
                    None => break,
                },
                c = cmds.recv() => match c {
                    Some(c) => self.on_cmd(c).await,
                    None => break,
                },
            }
        }
        // Closing the connection frees every object avahi-daemon holds for it.
        forwarder.abort();
    }

    async fn on_cmd(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Browse {
                id,
                service_type,
                tx,
                reply,
            } => {
                self.browses.insert(
                    id,
                    BrowseState {
                        service_type,
                        tx,
                        browser: None,
                        resolvers: HashMap::new(),
                        items: HashSet::new(),
                        stale: HashSet::new(),
                        seen: HashMap::new(),
                    },
                );
                let _ = reply.send(self.start_browser(id).await);
            }
            Cmd::Requery { id, reply } => {
                let _ = reply.send(self.requery(id).await);
            }
            Cmd::Unbrowse { id } => {
                if let Some(b) = self.browses.remove(&id) {
                    if let Some(path) = b.browser {
                        self.free(&path, BROWSER).await;
                    }
                    for path in b.resolvers.into_values() {
                        self.free(&path, RESOLVER).await;
                    }
                }
            }
            Cmd::ServerName { reply } => {
                let _ = reply.send(self.host_name().await);
            }
            Cmd::Publish {
                id,
                instance,
                records,
                reports,
                reply,
            } => {
                self.groups.insert(
                    id,
                    GroupState {
                        instance,
                        records,
                        reports,
                        path: None,
                        committed: false,
                        established: false,
                        collided: false,
                        host: String::new(),
                    },
                );
                let result = if self.running {
                    self.commit(id).await
                } else {
                    Ok(())
                };
                let _ = reply.send(result);
            }
            Cmd::UpdateTxt {
                id,
                service_type,
                txt,
                reply,
            } => {
                let _ = reply.send(self.update_txt(id, &service_type, txt).await);
            }
            Cmd::Republish { id, records, reply } => {
                let _ = reply.send(self.republish(id, records).await);
            }
            Cmd::Withdraw { id, reply } => {
                if let Some(g) = self.groups.remove(&id) {
                    if let Some(path) = g.path {
                        self.free(&path, ENTRY_GROUP).await;
                    }
                }
                if let Some(reply) = reply {
                    let _ = reply.send(Ok(()));
                }
            }
        }
    }

    async fn start_browser(&mut self, id: u64) -> Result<(), Error> {
        let Some(b) = self.browses.get(&id) else {
            return Ok(());
        };
        let body = (
            IF_UNSPEC,
            PROTO_UNSPEC,
            b.service_type.clone(),
            "local",
            LOOKUP_USE_MULTICAST,
        );
        let path = self
            .create("ServiceBrowserPrepare", "ServiceBrowserNew", BROWSER, &body)
            .await?;
        self.routes.insert(path.clone(), Route::Browser(id));
        if let Some(b) = self.browses.get_mut(&id) {
            b.browser = Some(path);
        }
        Ok(())
    }

    /// Free the browser and start a new one, which asks the network again.
    /// The items it does not report again by its first `AllForNow` are
    /// removed.
    async fn requery(&mut self, id: u64) -> Result<(), Error> {
        let Some(b) = self.browses.get_mut(&id) else {
            return Err(Error::Closed);
        };
        let old = b.browser.take();
        let items: Vec<ItemKey> = b.items.drain().collect();
        b.stale.extend(items);
        if let Some(path) = old {
            self.free(&path, BROWSER).await;
        }
        self.start_browser(id).await
    }

    async fn on_message(&mut self, m: &Message) {
        let header = m.header();
        let (Some(iface), Some(member)) = (header.interface(), header.member()) else {
            return;
        };
        let (iface, member) = (iface.as_str(), member.as_str());
        if iface == DBUS {
            if member == "NameOwnerChanged" {
                if let Ok((name, old, new)) = m.body().deserialize::<(String, String, String)>() {
                    if name == AVAHI {
                        self.owner_changed(&old, &new).await;
                    }
                }
            }
            return;
        }
        if iface == SERVER {
            if member == "StateChanged" {
                if let Ok((state, error)) = m.body().deserialize::<(i32, String)>() {
                    self.server_state(state, &error).await;
                }
            }
            return;
        }
        let Some(path) = header.path().map(|p| p.as_str().to_string()) else {
            return;
        };
        match (self.routes.get(&path), iface, member) {
            (Some(&Route::Browser(id)), BROWSER, "ItemNew" | "ItemRemove") => {
                let Ok((i, p, name, ty, domain, _flags)) =
                    m.body()
                        .deserialize::<(i32, i32, String, String, String, u32)>()
                else {
                    return;
                };
                if member == "ItemNew" {
                    self.item_new(id, (i, p, name), &ty, &domain).await;
                } else {
                    self.item_remove(id, (i, p, name)).await;
                }
            }
            (Some(&Route::Browser(id)), BROWSER, "AllForNow") => {
                let stale: Vec<ItemKey> = match self.browses.get_mut(&id) {
                    Some(b) => b.stale.drain().collect(),
                    None => return,
                };
                for key in stale {
                    self.item_remove(id, key).await;
                }
            }
            (Some(Route::Browser(_)), BROWSER, "Failure") => {
                let error: String = m.body().deserialize().unwrap_or_default();
                log::warn!("reason=avahiBrowseFailed,error={error}");
            }
            (Some(Route::Resolver(id, key)), RESOLVER, "Found") => {
                let (id, key) = (*id, key.clone());
                type Found = (
                    i32,
                    i32,
                    String,
                    String,
                    String,
                    String,
                    i32,
                    String,
                    u16,
                    Vec<Vec<u8>>,
                    u32,
                );
                let Ok(f) = m.body().deserialize::<Found>() else {
                    return;
                };
                let Some(b) = self.browses.get_mut(&id) else {
                    return;
                };
                let Some(s) = found_sighting(&f.3, &key, &f.5, &f.7, f.8, &f.9) else {
                    return;
                };
                if b.seen.get(&key) != Some(&s) {
                    b.seen.insert(key, s.clone());
                    let _ = b.tx.send(s);
                }
            }
            (Some(Route::Resolver(id, key)), RESOLVER, "Failure") => {
                let error: String = m.body().deserialize().unwrap_or_default();
                log::debug!("reason=avahiResolveFailed,name={},error={error}", key.2);
                let (id, key) = (*id, key.clone());
                if let Some(b) = self.browses.get_mut(&id) {
                    b.resolvers.remove(&key);
                }
                self.free(&path, RESOLVER).await;
            }
            (Some(&Route::Group(id)), ENTRY_GROUP, "StateChanged") => {
                if let Ok((state, error)) = m.body().deserialize::<(i32, String)>() {
                    self.group_state(id, state, error);
                }
            }
            _ => {}
        }
    }

    async fn item_new(&mut self, id: u64, key: ItemKey, ty: &str, domain: &str) {
        let Some(b) = self.browses.get_mut(&id) else {
            return;
        };
        b.stale.remove(&key);
        b.items.insert(key.clone());
        if b.resolvers.contains_key(&key) {
            return;
        }
        let body = resolver_args(&key, ty, domain);
        match self
            .create(
                "ServiceResolverPrepare",
                "ServiceResolverNew",
                RESOLVER,
                &body,
            )
            .await
        {
            Ok(path) => match self.browses.get_mut(&id) {
                Some(b) => {
                    b.resolvers.insert(key.clone(), path.clone());
                    self.routes.insert(path, Route::Resolver(id, key));
                }
                None => self.free(&path, RESOLVER).await,
            },
            Err(e) => log::warn!("reason=avahiResolverFailed,name={},error={e}", key.2),
        }
    }

    async fn item_remove(&mut self, id: u64, key: ItemKey) {
        let Some(b) = self.browses.get_mut(&id) else {
            return;
        };
        b.items.remove(&key);
        b.stale.remove(&key);
        let resolver = b.resolvers.remove(&key);
        if b.seen.remove(&key).is_some() {
            let _ = b.tx.send(Sighting::Gone {
                key: InstanceKey {
                    service_type: b.service_type.clone(),
                    name: key.2.clone(),
                },
                slot: Some(slot(&key)),
            });
        }
        if let Some(path) = resolver {
            self.free(&path, RESOLVER).await;
        }
    }

    fn group_state(&mut self, id: u64, state: i32, error: String) {
        let Some(g) = self.groups.get_mut(&id) else {
            return;
        };
        let report = match state {
            GROUP_ESTABLISHED if g.committed => {
                g.established = true;
                GroupReport::Established
            }
            GROUP_COLLISION if !g.collided => {
                g.collided = true;
                GroupReport::Collision
            }
            GROUP_FAILURE => GroupReport::Failed(error),
            _ => {
                g.established = false;
                return;
            }
        };
        let _ = g.reports.send(report);
    }

    /// Add the group's records and commit them.
    async fn commit(&mut self, id: u64) -> Result<(), Error> {
        let Some(g) = self.groups.get(&id) else {
            return Ok(());
        };
        let path = match &g.path {
            Some(p) => p.clone(),
            None => {
                let reply = self
                    .call("/", SERVER, "EntryGroupNew", &())
                    .await
                    .map_err(dbus_err)?;
                let path: OwnedObjectPath = reply
                    .body()
                    .deserialize()
                    .map_err(|e| Error::Backend(e.to_string()))?;
                let path = path.as_str().to_string();
                self.routes.insert(path.clone(), Route::Group(id));
                if let Some(g) = self.groups.get_mut(&id) {
                    g.path = Some(path.clone());
                }
                path
            }
        };
        let Some(g) = self.groups.get(&id) else {
            return Ok(());
        };
        let (instance, records) = (g.instance.clone(), g.records.clone());
        for r in &records {
            let body = (
                IF_UNSPEC,
                PROTO_UNSPEC,
                PUBLISH_NO_COOKIE,
                instance.as_str(),
                r.service_type,
                "",
                "",
                r.port,
                r.txt.to_strings(),
            );
            if let Err(e) = self.call(&path, ENTRY_GROUP, "AddService", &body).await {
                // Another group of this avahi-daemon holds the name.
                let collided = is_collision(&e);
                let _ = self.call(&path, ENTRY_GROUP, "Reset", &()).await;
                let Some(g) = self.groups.get_mut(&id) else {
                    return Ok(());
                };
                if collided {
                    if !g.collided {
                        g.collided = true;
                        let _ = g.reports.send(GroupReport::Collision);
                    }
                    return Ok(());
                }
                return Err(dbus_err(e));
            }
        }
        self.call(&path, ENTRY_GROUP, "Commit", &())
            .await
            .map_err(dbus_err)?;
        let host = self.host.clone();
        if let Some(g) = self.groups.get_mut(&id) {
            g.committed = true;
            g.established = false;
            g.host = host;
        }
        Ok(())
    }

    /// Empty a committed group, so it is added and committed again.
    async fn reset(&mut self, id: u64) {
        let Some(g) = self.groups.get_mut(&id) else {
            return;
        };
        g.committed = false;
        g.established = false;
        if let Some(path) = g.path.clone() {
            if let Err(e) = self.call(&path, ENTRY_GROUP, "Reset", &()).await {
                log::debug!("reason=avahiResetFailed,error={e}");
            }
        }
    }

    async fn update_txt(
        &mut self,
        id: u64,
        service_type: &str,
        txt: TxtRecord,
    ) -> Result<(), Error> {
        let Some(g) = self.groups.get_mut(&id) else {
            return Err(Error::Closed);
        };
        let ty = ebus_discovery::strip_dot(service_type);
        let Some(r) = g
            .records
            .iter_mut()
            .find(|r| r.service_type.eq_ignore_ascii_case(ty))
        else {
            return Err(Error::Backend(format!(
                "no {service_type} record to update"
            )));
        };
        r.txt = txt;
        let (path, committed) = (g.path.clone(), g.committed);
        let (instance, record) = (g.instance.clone(), r.clone());
        match path {
            Some(path) if committed => self.send_txt(&path, &instance, &record).await,
            _ => Ok(()),
        }
    }

    async fn send_txt(&self, path: &str, instance: &str, r: &ServiceRecord) -> Result<(), Error> {
        let body = (
            IF_UNSPEC,
            PROTO_UNSPEC,
            PUBLISH_NO_COOKIE,
            instance,
            r.service_type,
            "",
            r.txt.to_strings(),
        );
        self.call(path, ENTRY_GROUP, "UpdateServiceTxt", &body)
            .await
            .map(drop)
            .map_err(dbus_err)
    }

    /// Bring the group to `records`: TXT updates in place when only TXT
    /// changed, else the group emptied and committed again.
    async fn republish(&mut self, id: u64, records: Vec<ServiceRecord>) -> Result<(), Error> {
        let host = self.host.clone();
        let Some(g) = self.groups.get_mut(&id) else {
            return Err(Error::Closed);
        };
        let same_shape = g.committed
            && g.host.eq_ignore_ascii_case(&host)
            && g.records.len() == records.len()
            && g.records
                .iter()
                .zip(&records)
                .all(|(a, b)| a.service_type == b.service_type && a.port == b.port);
        let old = core::mem::replace(&mut g.records, records);
        if same_shape {
            let (path, instance) = (g.path.clone().unwrap_or_default(), g.instance.clone());
            let changed: Vec<ServiceRecord> = g
                .records
                .iter()
                .zip(&old)
                .filter(|(new, old)| new.txt != old.txt)
                .map(|(new, _)| new.clone())
                .collect();
            for r in &changed {
                self.send_txt(&path, &instance, r).await?;
            }
            if let Some(g) = self.groups.get(&id) {
                if g.established {
                    let _ = g.reports.send(GroupReport::Established);
                }
            }
            return Ok(());
        }
        self.reset(id).await;
        if self.running {
            self.commit(id).await
        } else {
            Ok(())
        }
    }

    async fn server_state(&mut self, state: i32, error: &str) {
        match state {
            SERVER_REGISTERING | SERVER_COLLISION => {
                self.running = false;
                let ids: Vec<u64> = self
                    .groups
                    .iter()
                    .filter(|(_, g)| g.committed)
                    .map(|(id, _)| *id)
                    .collect();
                for id in ids {
                    self.reset(id).await;
                    if let Some(g) = self.groups.get(&id) {
                        let _ = g.reports.send(GroupReport::Reset);
                    }
                }
            }
            SERVER_RUNNING => self.server_running().await,
            SERVER_FAILURE => {
                self.running = false;
                log::warn!("reason=avahiServerFailure,error={error}");
                for g in self.groups.values() {
                    let _ = g.reports.send(GroupReport::Failed(error.to_string()));
                }
            }
            _ => {}
        }
    }

    /// The server is `RUNNING`: commit every group not committed, and tell
    /// the groups when the host name changed.
    async fn server_running(&mut self) {
        self.running = true;
        let host = match self.host_name().await {
            Ok(h) => h,
            Err(e) => {
                log::warn!("reason=avahiHostNameFailed,error={e}");
                return;
            }
        };
        let renamed = !self.host.eq_ignore_ascii_case(&host);
        if renamed {
            log::info!("reason=hostRenamed,from={},to={host}", self.host);
        }
        self.host = host;
        let ids: Vec<u64> = self
            .groups
            .iter()
            .filter(|(_, g)| !g.committed && !g.collided)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if let Err(e) = self.commit(id).await {
                if let Some(g) = self.groups.get(&id) {
                    let _ = g.reports.send(GroupReport::Failed(e.to_string()));
                }
            }
        }
        if renamed {
            for g in self.groups.values() {
                let _ = g.reports.send(GroupReport::HostRenamed);
            }
        }
    }

    async fn owner_changed(&mut self, old: &str, new: &str) {
        if !old.is_empty() {
            log::info!("reason=avahiGone");
            // avahi-daemon took every browser, resolver and entry group with it.
            self.routes.clear();
            self.running = false;
            for b in self.browses.values_mut() {
                b.browser = None;
                b.resolvers.clear();
                let items: Vec<ItemKey> = b.items.drain().collect();
                b.stale.extend(items);
            }
            for g in self.groups.values_mut() {
                g.path = None;
                g.committed = false;
                g.established = false;
                g.collided = false;
                let _ = g.reports.send(GroupReport::Reset);
            }
        }
        if !new.is_empty() {
            log::info!("reason=avahiBack");
            self.server2 = true;
            let ids: Vec<u64> = self.browses.keys().copied().collect();
            for id in ids {
                if let Err(e) = self.start_browser(id).await {
                    log::warn!("reason=avahiBrowseFailed,error={e}");
                }
            }
            match self.server_call::<_, i32>("GetState", &()).await {
                Ok(SERVER_RUNNING) => self.server_running().await,
                Ok(_) => {}
                Err(e) => log::warn!("reason=avahiStateFailed,error={e}"),
            }
        }
    }
}

/// `ServiceResolverPrepare` arguments for a browsed item. The address
/// protocol is the item's own, so an IPv4 item resolves an A record and an
/// IPv6 item an AAAA record; with `PROTO_UNSPEC` avahi-daemon reports
/// whichever arrives first, and an IPv4 item can then carry an IPv6 address.
fn resolver_args<'a>(
    key: &ItemKey,
    ty: &'a str,
    domain: &'a str,
) -> (i32, i32, String, &'a str, &'a str, i32, u32) {
    (key.0, key.1, key.2.clone(), ty, domain, key.1, 0)
}

fn slot(key: &ItemKey) -> Slot {
    Slot::Link {
        ifindex: u32::try_from(key.0).unwrap_or(0),
        v6: key.1 == PROTO_INET6,
    }
}

/// The sighting a resolver's `Found` reports: one address, scoped to the
/// item's interface when it is an IPv6 link-local address, which avahi
/// reports without a zone.
fn found_sighting(
    service_type: &str,
    key: &ItemKey,
    host: &str,
    address: &str,
    port: u16,
    txt: &[Vec<u8>],
) -> Option<Sighting> {
    let mut addrs = Vec::new();
    if let Ok(ip) = address.parse::<IpAddr>() {
        let mut a = ScopedAddr::new(ip);
        if let IpAddr::V6(v6) = ip {
            if v6.is_unicast_link_local() {
                a.ifindex = u32::try_from(key.0).ok();
            }
        }
        addrs.push(a);
    }
    if host.is_empty() {
        return None;
    }
    Some(Sighting::Seen {
        key: InstanceKey {
            service_type: ebus_discovery::strip_dot(service_type).into(),
            name: key.2.clone(),
        },
        slot: slot(key),
        server: with_dot(host),
        port,
        // avahi-daemon builds its string list by prepending each string as
        // it parses the packet, and hands that list over D-Bus as it is.
        txt: TxtRecord::from_strings(txt.iter().rev()),
        addrs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Advertiser, advertise};
    use ebus_discovery::{Advertisement, BrokerService, BrokerType, Identity};
    use std::time::Duration;

    /// A browser made with `ServiceBrowserNew` starts by itself, so its
    /// first signals can come before the call returns; none is lost.
    #[tokio::test(flavor = "multi_thread")]
    async fn browse_without_server2() {
        let publisher = match Avahi::connect().await {
            Ok(a) => crate::Mdns::from(a),
            Err(e) => {
                assert!(
                    std::env::var_os("EBUS_REQUIRE_E2E").is_none(),
                    "EBUS_REQUIRE_E2E is set, but {e}"
                );
                eprintln!("skipping: {e}");
                return;
            }
        };
        let name = format!("ns2-{}", std::process::id());
        let ad = Advertisement::new(Identity::new(
            [name.as_str()],
            ["broker-host"],
            "Acme",
            "P1",
            "SN1",
        ))
        .broker(BrokerService::new(BrokerType::Mqtt, 18850));
        let adv: Advertiser = advertise(&publisher, ad, HostName::Auto).await.unwrap();
        let mut state = adv.state();
        tokio::time::timeout(
            Duration::from_secs(10),
            state.wait_for(|s| matches!(s, crate::AdState::Established { .. })),
        )
        .await
        .unwrap()
        .unwrap();

        let old = Avahi::open(false).await.unwrap();
        // Several browsers, each racing its first signals against the reply.
        for _ in 0..5 {
            let mut browse = old.browse("_mqtt._tcp").await.unwrap();
            let found = tokio::time::timeout(Duration::from_secs(10), async {
                while let Some(s) = browse.next().await {
                    if matches!(&s, Sighting::Seen { key, .. } if key.name == name) {
                        return true;
                    }
                }
                false
            })
            .await;
            assert_eq!(found, Ok(true));
        }
        adv.withdraw().await.unwrap();
    }

    #[test]
    fn found_link_local_takes_the_interface() {
        let key = (3, PROTO_INET6, "b".to_string());
        let Some(Sighting::Seen {
            key: k,
            slot: s,
            server,
            port,
            txt,
            addrs,
        }) = found_sighting(
            "_mqtt._tcp",
            &key,
            "box.local",
            "fe80::1",
            1883,
            &[b"k=2".to_vec(), b"k=1".to_vec(), b"txtvers=1".to_vec()],
        )
        else {
            panic!("no sighting");
        };
        assert_eq!(k.service_type, "_mqtt._tcp");
        assert_eq!(k.name, "b");
        assert_eq!(
            s,
            Slot::Link {
                ifindex: 3,
                v6: true
            }
        );
        assert_eq!(server, "box.local.");
        assert_eq!(port, 1883);
        assert_eq!(txt.to_strings(), [b"txtvers=1".to_vec(), b"k=1".to_vec()]);
        assert_eq!(addrs[0].ifindex, Some(3));
    }

    #[test]
    fn found_global_has_no_interface() {
        let key = (2, 0, "b".to_string());
        let Some(Sighting::Seen { slot: s, addrs, .. }) =
            found_sighting("_mqtt._tcp", &key, "box.local.", "192.0.2.7", 1883, &[])
        else {
            panic!("no sighting");
        };
        assert_eq!(
            s,
            Slot::Link {
                ifindex: 2,
                v6: false
            }
        );
        assert_eq!(addrs, vec![ScopedAddr::new("192.0.2.7".parse().unwrap())]);
    }

    #[test]
    fn resolvers_ask_for_the_item_protocol() {
        for proto in [PROTO_INET, PROTO_INET6] {
            let key = (2, proto, "b".to_string());
            let (_, item, _, _, _, aprotocol, _) = resolver_args(&key, "_mqtt._tcp", "local");
            assert_eq!((item, aprotocol), (proto, proto));
        }
    }

    #[test]
    fn found_without_host_is_dropped() {
        let key = (2, 0, "b".to_string());
        assert!(found_sighting("_mqtt._tcp", &key, "", "192.0.2.7", 1883, &[]).is_none());
    }
}
