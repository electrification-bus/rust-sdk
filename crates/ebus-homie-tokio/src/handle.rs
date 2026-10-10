//! Snapshots and the application's handle.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use ebus_homie::controller::{DeviceView, Id, PropertyAddr, SetError, SetOptions};
use ebus_homie::datatype::{self, Current, Datatype};
use ebus_homie::state::DeviceState;
use rpds::RedBlackTreeMapSync;
use tokio::sync::{mpsc::UnboundedSender, oneshot, watch};

/// What the controller knows about one device, as of the last snapshot.
pub type DeviceSnapshot = DeviceView;

/// A request from a [`ControllerHandle`] to the driver.
#[derive(Debug)]
pub(crate) enum Request {
    Set {
        addr: PropertyAddr,
        payload: String,
        opts: SetOptions,
        /// `None` for [`ControllerHandle::try_set`].
        reply: Option<oneshot::Sender<Result<(), SetError>>>,
    },
    Broadcast {
        subtopic: String,
        payload: String,
        reply: oneshot::Sender<Result<(), SetError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// The devices by id, each list sorted by domain. Persistent: publishing a
/// change copies the path to one id, not the map.
type Devices = RedBlackTreeMapSync<Id, Vec<Arc<DeviceSnapshot>>>;

/// The controller's state as the driver last published it.
#[derive(Clone, Debug)]
pub(crate) struct Snapshot {
    /// Devices with a valid `$state`.
    devices: Devices,
    /// Devices held, over every id.
    len: usize,
    pub(crate) connected: bool,
    /// `is_tree_complete` for each device that declares children, and for the
    /// configured tree root.
    pub(crate) trees: Arc<BTreeMap<Id, bool>>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Snapshot {
            devices: Devices::new_sync(),
            len: 0,
            connected: false,
            trees: Arc::new(BTreeMap::new()),
        }
    }
}

impl Snapshot {
    /// Every device with this id, one per domain, in domain order.
    pub(crate) fn list(&self, id: &str) -> &[Arc<DeviceSnapshot>] {
        self.devices.get(id).map_or(&[], Vec::as_slice)
    }

    pub(crate) fn device(&self, id: &str) -> Option<&Arc<DeviceSnapshot>> {
        self.list(id).first()
    }

    /// Every device in (id, domain) order.
    pub(crate) fn sorted(&self) -> impl Iterator<Item = &Arc<DeviceSnapshot>> + '_ {
        self.devices.values().flatten()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Every id held, in order.
    pub(crate) fn ids(&self) -> impl Iterator<Item = &Id> + '_ {
        self.devices.keys()
    }

    /// Replaces the devices with this id; an empty `list` removes them.
    pub(crate) fn set(&mut self, id: &Id, list: Vec<Arc<DeviceSnapshot>>) {
        let old = self.devices.get(id.as_str()).map(Vec::len);
        self.len = self.len - old.unwrap_or(0) + list.len();
        if !list.is_empty() {
            self.devices.insert_mut(id.clone(), list);
        } else if old.is_some() {
            self.devices.remove_mut(id.as_str());
        }
    }

    pub(crate) fn is_tree_complete(&self, root: &str) -> bool {
        match self.trees.get(root) {
            Some(complete) => *complete,
            None => self.device(root).is_some_and(|d| d.description.is_some()),
        }
    }
}

/// The application's handle on a running controller. Cheap to clone; `Send`
/// and `Sync`.
///
/// Reads come from the snapshot the driver last published and never wait for
/// it. The driver republishes the snapshot once per inbox drain in which
/// something changed, sharing the [`DeviceSnapshot`] of every device that did
/// not change. A changed device's new snapshot shares every property and alert
/// that did not change. Events are hints; snapshots are the truth.
#[derive(Clone)]
pub struct ControllerHandle {
    pub(crate) requests: UnboundedSender<Request>,
    pub(crate) snap: watch::Receiver<Arc<Snapshot>>,
}

impl fmt::Debug for ControllerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.snapshot();
        f.debug_struct("ControllerHandle")
            .field("connected", &s.connected)
            .field("devices", &s.len())
            .finish_non_exhaustive()
    }
}

impl ControllerHandle {
    fn snapshot(&self) -> Arc<Snapshot> {
        Arc::clone(&self.snap.borrow())
    }

    /// The device with this id, once its first valid `$state` has arrived.
    /// With [`Domain::Any`](ebus_homie::controller::Domain::Any), an id known
    /// in several domains resolves to the first domain in byte order.
    pub fn device(&self, id: &str) -> Option<Arc<DeviceSnapshot>> {
        self.snapshot().device(id).cloned()
    }

    /// Every device whose first valid `$state` has arrived, in id order.
    pub fn devices(&self) -> Vec<Arc<DeviceSnapshot>> {
        self.snapshot().sorted().cloned().collect()
    }

    /// The device's effective state (convention.md:262-267).
    pub fn effective_state(&self, id: &str) -> Option<DeviceState> {
        self.snapshot().device(id).and_then(|d| d.effective)
    }

    /// True when the device `root` and every device declared under it,
    /// transitively, have published a description.
    pub fn is_tree_complete(&self, root: &str) -> bool {
        self.snapshot().is_tree_complete(root)
    }

    /// True while the broker link is up and the controller has not shut down.
    pub fn is_connected(&self) -> bool {
        self.snapshot().connected
    }

    /// Waits until the driver publishes a snapshot this handle has not seen,
    /// and marks it seen. Returns `false` once the driver has stopped and its
    /// last snapshot was seen; that snapshot stays readable.
    pub async fn changed(&mut self) -> bool {
        self.snap.changed().await.is_ok()
    }

    /// Sends `payload` to the property's `/set` topic with the default
    /// [`SetOptions`]; see [`set_with`](Self::set_with).
    pub async fn set(
        &self,
        addr: PropertyAddr,
        payload: impl Into<String>,
    ) -> Result<(), SetError> {
        self.set_with(addr, payload, SetOptions::default()).await
    }

    /// Sends `payload` to the property's `/set` topic (QoS 0 per
    /// framework.md:87, not retained).
    /// `Ok` means the driver validated it and handed it to the command queue;
    /// nothing confirms delivery (convention.md:474-475). Returns
    /// [`SetError::NotConnected`] once the driver has stopped.
    ///
    /// The checks are those of
    /// [`Controller::set`](ebus_homie::controller::Controller::set). The
    /// request reaches the driver on the first poll, so a future dropped after
    /// that may still send.
    pub async fn set_with(
        &self,
        addr: PropertyAddr,
        payload: impl Into<String>,
        opts: SetOptions,
    ) -> Result<(), SetError> {
        let (reply, rx) = oneshot::channel();
        let req = Request::Set {
            addr,
            payload: payload.into(),
            opts,
            reply: Some(reply),
        };
        if self.requests.send(req).is_err() {
            return Err(SetError::NotConnected);
        }
        rx.await.unwrap_or(Err(SetError::NotConnected))
    }

    /// Like [`set`](Self::set), without waiting: safe on a host's reader
    /// thread. The checks run against the last snapshot; the driver runs them
    /// again and logs `reason=setRejected` when the state changed in between.
    pub fn try_set(&self, addr: PropertyAddr, payload: impl Into<String>) -> Result<(), SetError> {
        let payload = payload.into();
        let opts = SetOptions::default();
        check_set(&self.snapshot(), &addr, &payload, opts)?;
        let req = Request::Set {
            addr,
            payload,
            opts,
            reply: None,
        };
        self.requests.send(req).map_err(|_| SetError::NotConnected)
    }

    /// Publishes `payload` to `{domain}/5/$broadcast/{subtopic}` (QoS 0, not
    /// retained), after checking that every subtopic level is a valid id
    /// (convention.md:531-535).
    pub async fn broadcast(&self, subtopic: &str, payload: &str) -> Result<(), SetError> {
        let (reply, rx) = oneshot::channel();
        let req = Request::Broadcast {
            subtopic: subtopic.to_owned(),
            payload: payload.to_owned(),
            reply,
        };
        if self.requests.send(req).is_err() {
            return Err(SetError::NotConnected);
        }
        rx.await.unwrap_or(Err(SetError::NotConnected))
    }

    /// Unsubscribes every filter the controller holds, except host filters,
    /// and stops the driver once those commands are queued. Returns at once
    /// when the driver has already stopped.
    pub async fn shutdown(&self) {
        let (reply, rx) = oneshot::channel();
        if self.requests.send(Request::Shutdown { reply }).is_ok() {
            let _ = rx.await;
        }
    }
}

/// [`Controller::set`](ebus_homie::controller::Controller::set)'s checks,
/// against a snapshot.
pub(crate) fn check_set(
    snap: &Snapshot,
    addr: &PropertyAddr,
    payload: &str,
    opts: SetOptions,
) -> Result<(), SetError> {
    if !snap.connected {
        return Err(SetError::NotConnected);
    }
    let dev = snap
        .device(addr.device.as_str())
        .ok_or(SetError::UnknownDevice)?;
    let desc = dev
        .description
        .as_deref()
        .ok_or(SetError::UnknownProperty)?;
    let view = dev
        .property(addr.node.as_str(), addr.property.as_str())
        .ok_or(SetError::UnknownProperty)?;
    let decl = desc
        .nodes
        .iter()
        .find(|(n, _)| n == addr.node.as_str())
        .and_then(|(_, node)| {
            node.properties
                .iter()
                .find(|(p, _)| p == addr.property.as_str())
        })
        .map(|(_, p)| p)
        .ok_or(SetError::UnknownProperty)?;
    if !decl.settable {
        return Err(SetError::NotSettable);
    }
    let effective = dev.effective.unwrap_or(DeviceState::Init);
    if effective != DeviceState::Ready && !opts.allow_not_ready {
        return Err(SetError::DeviceNotReady(effective));
    }
    if !opts.unchecked {
        let current = view
            .last_valid
            .as_deref()
            .and_then(|t| match decl.datatype {
                Datatype::Integer => t.parse().ok().map(Current::Integer),
                Datatype::Float => t.parse().ok().map(Current::Float),
                _ => None,
            });
        datatype::parse_set_payload(payload, decl.datatype, decl.format.as_deref(), current)
            .map_err(SetError::Invalid)?;
    }
    Ok(())
}
