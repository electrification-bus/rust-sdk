//! Typed property handles, the device handle and the registry the driver
//! keeps of them.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use bytes::Bytes;
use ebus_homie::datatype::{Datatype, SetValue};
use ebus_homie::device::{
    AlertError, BuildError, DevIdx, HomieValue, NodeIdx, Presence, PropSpec, Reconfigure, Shutdown,
    SlotId, Tree, ValueCheck, ValueError,
};
use ebus_homie::state::DeviceState;
use tokio::sync::{Notify, mpsc, oneshot, watch};

use super::dirty::DirtySet;

/// Slots per dirty set; the registry adds a set when a slot index needs one.
const CHUNK: u32 = 256;

/// A change a handle hands to the driver, applied in the order sent.
pub(crate) type Apply = Box<dyn FnOnce(&mut Tree, &mut Registry) + Send>;

/// An event value, applied when the driver has credit.
pub(crate) type Emit = Box<dyn FnOnce(&mut Tree) + Send>;

/// A request to the driver.
pub(crate) enum DevRequest {
    Apply(Apply),
    Shutdown {
        mode: Shutdown,
        reply: oneshot::Sender<()>,
    },
}

/// State every handle shares with the driver.
pub(crate) struct DevShared {
    /// Wakes the driver: a slot became dirty or an event was queued.
    pub(crate) wake: Notify,
    pub(crate) requests: mpsc::UnboundedSender<DevRequest>,
    pub(crate) emits: mpsc::UnboundedSender<Emit>,
    /// Events queued and not yet applied, and the most allowed.
    pub(crate) emit_queued: AtomicUsize,
    pub(crate) emit_cap: AtomicUsize,
    /// Slots each settable's `/set` queue holds.
    pub(crate) set_cap: AtomicUsize,
    pub(crate) connected: AtomicBool,
    pub(crate) rejected: AtomicU64,
    pub(crate) aborted: AtomicBool,
    /// Each device's state, as the driver last read it.
    pub(crate) states: Mutex<BTreeMap<u16, watch::Sender<Option<DeviceState>>>>,
}

impl DevShared {
    pub(crate) fn new(
        requests: mpsc::UnboundedSender<DevRequest>,
        emits: mpsc::UnboundedSender<Emit>,
    ) -> Self {
        DevShared {
            wake: Notify::new(),
            requests,
            emits,
            emit_queued: AtomicUsize::new(0),
            emit_cap: AtomicUsize::new(1024),
            set_cap: AtomicUsize::new(16),
            connected: AtomicBool::new(false),
            rejected: AtomicU64::new(0),
            aborted: AtomicBool::new(false),
            states: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn states(
        &self,
    ) -> MutexGuard<'_, BTreeMap<u16, watch::Sender<Option<DeviceState>>>> {
        self.states.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn apply(&self, f: Apply) {
        let _ = self.requests.send(DevRequest::Apply(f));
    }
}

/// A `$target` property as its handles last left it, read and written
/// under the value's watch lock.
const TARGET_OPEN: u8 = 0;
/// `$target` and the value both equal the watch's value; no transition open.
const TARGET_SETTLED: u8 = 1;
/// Neither a value nor `$target` is held.
const TARGET_CLEARED: u8 = 2;

/// What a typed handle and its registry entry share.
struct SlotShared<T> {
    id: SlotId,
    topic: Box<str>,
    value: watch::Sender<Option<T>>,
    check: RwLock<ValueCheck>,
    gone: AtomicBool,
    dirty: Arc<DirtySet>,
    bit: u32,
    dev: Arc<DevShared>,
    /// One of the `TARGET_*` states, for a `$target` property.
    target: AtomicU8,
}

impl<T: HomieValue + Send + Sync> SlotShared<T> {
    /// Runs `f` on the property's check, under its lock and without a copy.
    fn with_check<R>(&self, f: impl FnOnce(&ValueCheck) -> R) -> Result<R, ValueError> {
        if self.gone.load(Ordering::Acquire) {
            return Err(ValueError::Gone);
        }
        Ok(f(&self
            .check
            .read()
            .unwrap_or_else(PoisonError::into_inner)))
    }

    /// The property publishes `$target`.
    fn is_target(&self) -> Result<bool, ValueError> {
        self.with_check(ValueCheck::is_target)
    }

    /// On a `$target` property: marks a transition open and hands `f` to the
    /// driver, under the watch lock so requests keep the handles' order.
    fn open(&self, f: Apply) {
        self.value.send_if_modified(|_| {
            self.target.store(TARGET_OPEN, Ordering::Relaxed);
            self.dev.apply(f);
            false
        });
    }
}

/// A retained property's value, set from any thread.
///
/// [`set`](Self::set) checks the value on the caller's thread and compares
/// it with the last one; an equal value publishes nothing and wakes nothing.
/// A changed one marks the property dirty, and the driver publishes the
/// latest value once, however many changes came in between. A value set
/// while the link is down, or before the driver runs, goes out in the next
/// refresh. Cheap to clone; `Send` and `Sync`.
pub struct Property<T> {
    s: Arc<SlotShared<T>>,
}

impl<T> Clone for Property<T> {
    fn clone(&self) -> Self {
        Property {
            s: Arc::clone(&self.s),
        }
    }
}

impl<T> fmt::Debug for Property<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Property")
            .field("topic", &self.s.topic)
            .finish_non_exhaustive()
    }
}

impl<T: HomieValue + Send + Sync> Property<T> {
    /// Sets the value. Never blocks. Refused, with nothing published, when
    /// the value fails the property's `$format` or the property was removed.
    /// On a property with `$target` this closes the transition: `$target` is
    /// published first unless it already equals `v` (convention.md:333,
    /// 466-468).
    pub fn set(&self, v: T) -> Result<(), ValueError> {
        let target = self.s.with_check(|c| {
            if !c.is_retained() {
                return Err(ValueError::IsEvent);
            }
            c.check(v.as_value())?;
            Ok(c.is_target())
        })??;
        if target {
            self.close(v);
            return Ok(());
        }
        if self.store(v) && self.s.dirty.mark(self.s.bit) {
            self.s.dev.wake.notify_one();
        }
        Ok(())
    }

    /// On a `$target` property: sets the value and closes the transition,
    /// sending nothing when both already hold `v` with none open.
    fn close(&self, v: T) {
        let s = &self.s;
        s.value.send_if_modified(|cur| {
            let same = cur.as_ref().is_some_and(|c| c.same(&v));
            if same && s.target.load(Ordering::Relaxed) == TARGET_SETTLED {
                return false;
            }
            s.target.store(TARGET_SETTLED, Ordering::Relaxed);
            let (id, w) = (s.id, v.clone());
            s.dev.apply(Box::new(move |tree, _| {
                log_refused(id, tree.set_value(id, w.as_value()).map(|_| ()));
            }));
            if !same {
                *cur = Some(v);
            }
            !same
        });
    }

    /// Deletes the retained value with a zero-length publish; on a property
    /// with `$target`, `$target` is cleared next. A property with no value
    /// publishes nothing.
    pub fn clear(&self) -> Result<(), ValueError> {
        let s = &self.s;
        let target = s.with_check(|c| {
            if c.is_retained() {
                Ok(c.is_target())
            } else {
                Err(ValueError::IsEvent)
            }
        })??;
        if target {
            s.value.send_if_modified(|cur| {
                let had = cur.take().is_some();
                if !had && s.target.load(Ordering::Relaxed) == TARGET_CLEARED {
                    return false;
                }
                s.target.store(TARGET_CLEARED, Ordering::Relaxed);
                let id = s.id;
                s.dev.apply(Box::new(move |tree, _| {
                    log_refused(id, tree.clear_value(id).map(|_| ()));
                }));
                had
            });
        } else if s.value.send_if_modified(|cur| cur.take().is_some()) && s.dirty.mark(s.bit) {
            s.dev.wake.notify_one();
        }
        Ok(())
    }

    /// The property's id in the tree, for the per-property operations of
    /// [`ReconfigureHandles::core`].
    pub fn slot(&self) -> SlotId {
        self.s.id
    }

    /// The value last set, or `None`.
    pub fn get(&self) -> Option<T> {
        self.s.value.borrow().clone()
    }

    /// A receiver that sees each change of the value as it is set.
    pub fn subscribe(&self) -> watch::Receiver<Option<T>> {
        self.s.value.subscribe()
    }

    /// Stores `v` unless it is the same value; true when it changed.
    fn store(&self, v: T) -> bool {
        self.s.value.send_if_modified(|cur| match cur {
            Some(c) if c.same(&v) => false,
            _ => {
                *cur = Some(v);
                true
            }
        })
    }
}

/// A settable property: a [`Property`] plus the `/set` requests for it.
pub struct Settable<T> {
    prop: Property<T>,
    rx: mpsc::UnboundedReceiver<SetRequest<T>>,
    queued: Arc<AtomicUsize>,
}

impl<T> fmt::Debug for Settable<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Settable")
            .field("topic", &self.prop.s.topic)
            .finish_non_exhaustive()
    }
}

impl<T: HomieValue + Send + Sync> Settable<T> {
    /// The property, to hand to code that only sets values.
    pub fn property(&self) -> Property<T> {
        self.prop.clone()
    }

    /// As [`Property::set`]; on a property with `$target`, closes the
    /// transition.
    pub fn set(&self, v: T) -> Result<(), ValueError> {
        self.prop.set(v)
    }

    /// As [`Property::clear`].
    pub fn clear(&self) -> Result<(), ValueError> {
        self.prop.clear()
    }

    /// The value last set, or `None`.
    pub fn get(&self) -> Option<T> {
        self.prop.get()
    }

    /// As [`Property::slot`].
    pub fn slot(&self) -> SlotId {
        self.prop.slot()
    }

    /// On a property with `$target`, publishes the value only: the
    /// transition stays open.
    pub fn progress(&self, v: T) -> Result<(), ValueError> {
        let s = &self.prop.s;
        target_check(s, &v)?;
        let id = s.id;
        let w = v.clone();
        s.value.send_if_modified(|cur| {
            s.target.store(TARGET_OPEN, Ordering::Relaxed);
            s.dev.apply(Box::new(move |tree, _| {
                log_refused(id, tree.set_progress(id, w.as_value()).map(|_| ()));
            }));
            let changed = !cur.as_ref().is_some_and(|c| c.same(&v));
            if changed {
                *cur = Some(v);
            }
            changed
        });
        Ok(())
    }

    /// On a property with `$target`, opens a transition to `v` from a local
    /// command: `$target` is published when it differs.
    pub fn set_target(&self, v: T) -> Result<(), ValueError> {
        let s = &self.prop.s;
        target_check(s, &v)?;
        let id = s.id;
        s.open(Box::new(move |tree, _| {
            log_refused(id, tree.set_target(id, v.as_value()).map(|_| ()));
        }));
        Ok(())
    }

    /// The next accepted `/set`, or `None` once the driver has stopped.
    /// Cancel-safe.
    pub async fn next_set(&mut self) -> Option<SetRequest<T>> {
        let req = self.rx.recv().await?;
        self.queued.fetch_sub(1, Ordering::AcqRel);
        Some(req)
    }

    /// The next accepted `/set`, when one is queued.
    pub fn try_next_set(&mut self) -> Option<SetRequest<T>> {
        let req = self.rx.try_recv().ok()?;
        self.queued.fetch_sub(1, Ordering::AcqRel);
        Some(req)
    }
}

/// Checks `v` for [`Settable::progress`] and [`Settable::set_target`].
fn target_check<T: HomieValue + Send + Sync>(s: &SlotShared<T>, v: &T) -> Result<(), ValueError> {
    s.with_check(|c| {
        if !c.is_target() {
            return Err(ValueError::NotTarget);
        }
        c.check(v.as_value())
    })?
}

/// One accepted `/set`: checked against the property's `$format` and
/// step-rounded (convention.md:399-413). Dropping it publishes nothing.
#[must_use = "a /set is answered with accept, accept_as or reject"]
pub struct SetRequest<T> {
    value: T,
    raw: Box<[u8]>,
    prop: Property<T>,
}

impl<T: fmt::Debug> fmt::Debug for SetRequest<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SetRequest")
            .field("topic", &self.prop.s.topic)
            .field("value", &self.value)
            .field("raw", &String::from_utf8_lossy(&self.raw))
            .finish()
    }
}

impl<T: HomieValue + Send + Sync> SetRequest<T> {
    /// The value, checked and step-rounded.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// The payload as received.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// Accepts the command. With `$target`, `$target` becomes the received
    /// payload, byte for byte (convention.md:470); without, the property is
    /// set to the request's value, which publishes nothing when it already
    /// holds it (convention.md:485-486).
    pub fn accept(self) {
        let s = &self.prop.s;
        match s.is_target() {
            Ok(true) => {
                let (id, raw) = (s.id, self.raw);
                s.open(Box::new(move |tree, _| {
                    log_refused(id, tree.accept_set(id, &raw).map(|_| ()));
                }));
            }
            _ => log_refused(s.id, self.prop.set(self.value)),
        }
    }

    /// Accepts the command and reflects `v`, the value the device reached.
    /// With `$target`, `$target` becomes the received payload first, and `v`
    /// then closes the transition as [`Property::set`] does.
    pub fn accept_as(self, v: T) {
        let s = &self.prop.s;
        if s.is_target() == Ok(true) {
            let (id, raw) = (s.id, self.raw);
            s.open(Box::new(move |tree, _| {
                log_refused(id, tree.accept_set(id, &raw).map(|_| ()));
            }));
        }
        log_refused(s.id, self.prop.set(v));
    }

    /// Refuses the command: nothing is published, and the refusal is logged
    /// with `reason=propertySetRejected`.
    pub fn reject(self, why: &str) {
        log::info!(
            "reason=propertySetRejected,topic={},why={why}",
            self.prop.s.topic
        );
    }
}

/// Why [`Event::emit`] published nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EmitError {
    /// The link is down; an event is never held (convention.md:695).
    Down,
    /// [`DeviceOptions::event_capacity`](super::DeviceOptions::event_capacity)
    /// events are already queued.
    Full,
    /// The value or the property was refused.
    Invalid(ValueError),
}

impl fmt::Display for EmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmitError::Down => f.write_str("down"),
            EmitError::Full => f.write_str("full"),
            EmitError::Invalid(e) => write!(f, "invalid:{e}"),
        }
    }
}

impl std::error::Error for EmitError {}

struct EventShared {
    id: SlotId,
    topic: Box<str>,
    check: RwLock<ValueCheck>,
    gone: AtomicBool,
    dev: Arc<DevShared>,
}

/// A `retained: false` property: every [`emit`](Self::emit) is published,
/// non-retained at QoS 0 (convention.md:50). Cheap to clone; `Send` and
/// `Sync`.
pub struct Event<T> {
    s: Arc<EventShared>,
    _t: std::marker::PhantomData<fn(T)>,
}

impl<T> Clone for Event<T> {
    fn clone(&self) -> Self {
        Event {
            s: Arc::clone(&self.s),
            _t: std::marker::PhantomData,
        }
    }
}

impl<T> fmt::Debug for Event<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("topic", &self.s.topic)
            .finish_non_exhaustive()
    }
}

impl<T: HomieValue + Send + Sync> Event<T> {
    /// As [`Property::slot`].
    pub fn slot(&self) -> SlotId {
        self.s.id
    }

    /// Queues one event. Never blocks.
    pub fn emit(&self, v: T) -> Result<(), EmitError> {
        let s = &self.s;
        if s.gone.load(Ordering::Acquire) {
            return Err(EmitError::Invalid(ValueError::Gone));
        }
        s.check
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .check(v.as_value())
            .map_err(EmitError::Invalid)?;
        let dev = &s.dev;
        if !dev.connected.load(Ordering::Acquire) {
            return Err(EmitError::Down);
        }
        let cap = dev.emit_cap.load(Ordering::Acquire);
        if dev.emit_queued.fetch_add(1, Ordering::AcqRel) >= cap {
            dev.emit_queued.fetch_sub(1, Ordering::AcqRel);
            return Err(EmitError::Full);
        }
        let id = s.id;
        let sent = dev.emits.send(Box::new(move |tree: &mut Tree| {
            log_refused(id, tree.emit(id, v.as_value()).map(|_| ()));
        }));
        if sent.is_err() {
            dev.emit_queued.fetch_sub(1, Ordering::AcqRel);
            return Err(EmitError::Down);
        }
        dev.wake.notify_one();
        Ok(())
    }
}

fn log_refused(slot: SlotId, r: Result<(), ValueError>) {
    if let Err(e) = r {
        log::warn!("reason=valueRefused,slot={},error={e}", slot.index());
    }
}

/// What the driver keeps for one handle.
pub(crate) trait Entry: Send + Sync {
    fn id(&self) -> SlotId;
    /// Hands the value last set to the core.
    fn apply(&self, tree: &mut Tree);
    /// Queues an accepted `/set`; false when the queue is full.
    fn deliver(&self, value: &SetValue, raw: Box<[u8]>, cap: usize) -> bool;
    /// The property was removed.
    fn retire(&self);
    /// The property's check after a commit.
    fn recheck(&self, check: ValueCheck);
}

struct SlotEntry<T> {
    s: Arc<SlotShared<T>>,
    sets: Option<(mpsc::UnboundedSender<SetRequest<T>>, Arc<AtomicUsize>)>,
}

impl<T: HomieValue + Send + Sync> Entry for SlotEntry<T> {
    fn id(&self) -> SlotId {
        self.s.id
    }

    fn apply(&self, tree: &mut Tree) {
        let id = self.s.id;
        let v = self.s.value.borrow();
        let r = match &*v {
            Some(x) => tree.set_value(id, x.as_value()),
            None => tree.clear_value(id),
        };
        log_refused(id, r.map(|_| ()));
    }

    fn deliver(&self, value: &SetValue, raw: Box<[u8]>, cap: usize) -> bool {
        let Some((tx, queued)) = &self.sets else {
            return true;
        };
        let Some(value) = T::from_set(value) else {
            log::warn!("reason=setValueUnmapped,topic={}", self.s.topic);
            return true;
        };
        if queued.load(Ordering::Acquire) >= cap {
            return false;
        }
        let req = SetRequest {
            value,
            raw,
            prop: Property {
                s: Arc::clone(&self.s),
            },
        };
        if tx.send(req).is_ok() {
            queued.fetch_add(1, Ordering::AcqRel);
        }
        true
    }

    fn retire(&self) {
        self.s.gone.store(true, Ordering::Release);
    }

    fn recheck(&self, check: ValueCheck) {
        let s = &self.s;
        let mut cur = s.check.write().unwrap_or_else(PoisonError::into_inner);
        // An enum value is an index, which may name another value under a
        // new list: the value goes to the core again, whose gate decides.
        let reformat = check.datatype() == Datatype::Enum && cur.format() != check.format();
        let target = check.is_target();
        *cur = check;
        drop(cur);
        if !reformat {
            return;
        }
        if target {
            s.value.send_if_modified(|_| {
                s.target.store(TARGET_OPEN, Ordering::Relaxed);
                false
            });
        } else if s.value.borrow().is_some() && s.dirty.mark(s.bit) {
            s.dev.wake.notify_one();
        }
    }
}

struct EventEntry(Arc<EventShared>);

impl Entry for EventEntry {
    fn id(&self) -> SlotId {
        self.0.id
    }

    fn apply(&self, _tree: &mut Tree) {}

    fn deliver(&self, _value: &SetValue, _raw: Box<[u8]>, _cap: usize) -> bool {
        true
    }

    fn retire(&self) {
        self.0.gone.store(true, Ordering::Release);
    }

    fn recheck(&self, check: ValueCheck) {
        *self.0.check.write().unwrap_or_else(PoisonError::into_inner) = check;
    }
}

/// The driver's index of the handles, by slot index, with their dirty marks.
pub(crate) struct Registry {
    entries: Vec<Option<Box<dyn Entry>>>,
    chunks: Vec<Arc<DirtySet>>,
    /// The chunk the next drain starts at.
    cursor: usize,
    /// Entries made in a commit that has not finished.
    staged: Vec<Box<dyn Entry>>,
    dev: Arc<DevShared>,
}

impl Registry {
    pub(crate) fn new(dev: Arc<DevShared>) -> Self {
        Registry {
            entries: Vec::new(),
            chunks: Vec::new(),
            cursor: 0,
            staged: Vec::new(),
            dev,
        }
    }

    fn chunk(&mut self, index: u32) -> (Arc<DirtySet>, u32) {
        let c = (index / CHUNK) as usize;
        while self.chunks.len() <= c {
            self.chunks.push(Arc::new(DirtySet::new(CHUNK as usize)));
        }
        let set = self
            .chunks
            .get(c)
            .map_or_else(|| Arc::new(DirtySet::new(0)), Arc::clone);
        (set, index % CHUNK)
    }

    fn slot<T: HomieValue + Send + Sync>(
        &mut self,
        id: SlotId,
        topic: &str,
        check: ValueCheck,
        settable: bool,
    ) -> (Property<T>, Option<Settable<T>>) {
        let (dirty, bit) = self.chunk(id.index());
        let (value, _) = watch::channel(None);
        let s = Arc::new(SlotShared {
            id,
            topic: topic.into(),
            value,
            check: RwLock::new(check),
            gone: AtomicBool::new(false),
            dirty,
            bit,
            dev: Arc::clone(&self.dev),
            target: AtomicU8::new(TARGET_CLEARED),
        });
        let prop = Property { s: Arc::clone(&s) };
        let (sets, settable) = if settable {
            let (tx, rx) = mpsc::unbounded_channel();
            let queued = Arc::new(AtomicUsize::new(0));
            let handle = Settable {
                prop: prop.clone(),
                rx,
                queued: Arc::clone(&queued),
            };
            (Some((tx, queued)), Some(handle))
        } else {
            (None, None)
        };
        self.staged.push(Box::new(SlotEntry { s, sets }));
        (prop, settable)
    }

    fn event<T>(&mut self, id: SlotId, topic: &str, check: ValueCheck) -> Event<T> {
        let s = Arc::new(EventShared {
            id,
            topic: topic.into(),
            check: RwLock::new(check),
            gone: AtomicBool::new(false),
            dev: Arc::clone(&self.dev),
        });
        self.staged.push(Box::new(EventEntry(Arc::clone(&s))));
        Event {
            s,
            _t: std::marker::PhantomData,
        }
    }

    /// After a commit: retires the handles of removed properties, refreshes
    /// the checks of the others, and takes in the new ones.
    pub(crate) fn commit(&mut self, tree: &Tree) {
        for e in &mut self.entries {
            let Some(entry) = e else { continue };
            match tree.value_check(entry.id()) {
                Some(check) => entry.recheck(check),
                None => {
                    entry.retire();
                    *e = None;
                }
            }
        }
        self.install();
    }

    /// Takes in the staged entries, without checking the others.
    pub(crate) fn install(&mut self) {
        for entry in std::mem::take(&mut self.staged) {
            let i = entry.id().index() as usize;
            if self.entries.len() <= i {
                self.entries.resize_with(i + 1, || None);
            }
            if let Some(slot) = self.entries.get_mut(i) {
                if let Some(old) = slot.replace(entry) {
                    old.retire();
                }
            }
        }
    }

    /// Drops the entries of a refused commit.
    pub(crate) fn rollback(&mut self) {
        for entry in std::mem::take(&mut self.staged) {
            entry.retire();
        }
    }

    /// Hands up to `budget` dirty values to the core; true when every dirty
    /// mark was taken.
    pub(crate) fn drain(&mut self, tree: &mut Tree, budget: usize) -> bool {
        let n = self.chunks.len();
        if n == 0 {
            return true;
        }
        let mut left = budget;
        for k in 0..n {
            if left == 0 {
                return false;
            }
            let c = (self.cursor + k) % n;
            let Some(set) = self.chunks.get(c) else {
                continue;
            };
            let base = (c as u32) * CHUNK;
            let entries = &self.entries;
            let mut stopped = false;
            set.drain(|bit| {
                if let Some(Some(e)) = entries.get((base + bit) as usize) {
                    e.apply(tree);
                }
                left -= 1;
                if left == 0 {
                    stopped = true;
                    return false;
                }
                true
            });
            if stopped {
                self.cursor = c;
                // Bits after the stop stay set; the caller calls again.
                return false;
            }
        }
        true
    }

    /// Queues an accepted `/set` for its handle.
    pub(crate) fn deliver(&self, slot: SlotId, value: &SetValue, raw: Box<[u8]>) {
        let Some(Some(e)) = self.entries.get(slot.index() as usize) else {
            return;
        };
        if e.id() != slot {
            return;
        }
        let cap = self.dev.set_cap.load(Ordering::Acquire);
        if !e.deliver(value, raw, cap) {
            log::warn!("reason=setDropped,slot={}", slot.index());
        }
    }

    /// Marks every handle removed: the driver stopped.
    pub(crate) fn retire_all(&mut self) {
        for e in self.entries.iter_mut().filter_map(Option::take) {
            e.retire();
        }
        self.rollback();
    }
}

/// A tree that never runs, or a driver dropped before it returned, leaves
/// no handle reporting success.
impl Drop for Registry {
    fn drop(&mut self) {
        self.retire_all();
        self.dev.connected.store(false, Ordering::Release);
    }
}

/// Adds typed handles to a reconfiguration commit; see
/// [`DeviceHandle::reconfigure`]. The handles go live when the commit
/// succeeds; after a refused commit they return `Gone`.
pub struct ReconfigureHandles<'a, 'b> {
    core: &'a mut Reconfigure<'b>,
    reg: &'a mut Registry,
}

impl fmt::Debug for ReconfigureHandles<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReconfigureHandles").finish_non_exhaustive()
    }
}

impl<'b> ReconfigureHandles<'_, 'b> {
    /// The core commit: add or remove children, nodes and properties, and
    /// change formats, units and names.
    pub fn core(&mut self) -> &mut Reconfigure<'b> {
        self.core
    }

    /// Adds a retained property and returns its handle; `spec.settable` and
    /// `spec.retained` are set by this method.
    pub fn property<T: HomieValue + Send + Sync>(
        &mut self,
        node: NodeIdx,
        id: &str,
        spec: PropSpec,
    ) -> Result<Property<T>, BuildError> {
        add_slot::<T>(self.core, self.reg, node, id, spec, false).map(|(p, _)| p)
    }

    /// Adds a settable retained property and returns its handle.
    pub fn settable<T: HomieValue + Send + Sync>(
        &mut self,
        node: NodeIdx,
        id: &str,
        spec: PropSpec,
    ) -> Result<Settable<T>, BuildError> {
        add_slot::<T>(self.core, self.reg, node, id, spec, true)
            .and_then(|(_, s)| s.ok_or(BuildError::Gone))
    }

    /// Adds a `retained: false` property and returns its handle.
    pub fn event<T: HomieValue + Send + Sync>(
        &mut self,
        node: NodeIdx,
        id: &str,
        spec: PropSpec,
    ) -> Result<Event<T>, BuildError> {
        add_event::<T>(self.core, self.reg, node, id, spec)
    }
}

/// Adds a retained property through `core` and stages its handle.
pub(crate) fn add_slot<T: HomieValue + Send + Sync>(
    core: &mut Reconfigure<'_>,
    reg: &mut Registry,
    node: NodeIdx,
    id: &str,
    spec: PropSpec,
    settable: bool,
) -> Result<(Property<T>, Option<Settable<T>>), BuildError> {
    let slot = core.add_typed::<T>(node, id, spec.settable(settable).retained(true))?;
    let check = core.value_check(slot).ok_or(BuildError::Gone)?;
    let topic = core.topic(slot).unwrap_or_default().to_owned();
    Ok(reg.slot::<T>(slot, &topic, check, settable))
}

/// Adds a `retained: false` property through `core` and stages its handle.
pub(crate) fn add_event<T: HomieValue + Send + Sync>(
    core: &mut Reconfigure<'_>,
    reg: &mut Registry,
    node: NodeIdx,
    id: &str,
    spec: PropSpec,
) -> Result<Event<T>, BuildError> {
    let slot = core.add_typed::<T>(node, id, spec.settable(false).retained(false))?;
    let check = core.value_check(slot).ok_or(BuildError::Gone)?;
    let topic = core.topic(slot).unwrap_or_default().to_owned();
    Ok(reg.event::<T>(slot, &topic, check))
}

/// The application's handle on a running device tree. Cheap to clone;
/// `Send` and `Sync`.
#[derive(Clone)]
pub struct DeviceHandle {
    pub(crate) dev: Arc<DevShared>,
    pub(crate) broadcasts: Arc<tokio::sync::Mutex<mpsc::Receiver<(String, Bytes)>>>,
}

impl fmt::Debug for DeviceHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceHandle")
            .field("connected", &self.is_connected())
            .finish_non_exhaustive()
    }
}

impl DeviceHandle {
    /// True while the link is up and the tree has not shut down.
    pub fn is_connected(&self) -> bool {
        self.dev.connected.load(Ordering::Acquire)
    }

    /// `/set` messages refused with `propertySetNotSettable` or
    /// `propertySetRejected`.
    pub fn rejected_sets(&self) -> u64 {
        self.dev.rejected.load(Ordering::Acquire)
    }

    /// The state `dev` publishes; `None` for a device not in the tree.
    pub fn state(&self, dev: DevIdx) -> watch::Receiver<Option<DeviceState>> {
        match self.dev.states().get(&dev.index()) {
            Some(tx) => tx.subscribe(),
            None => watch::channel(None).1,
        }
    }

    async fn call<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Tree, &mut Registry) -> R + Send + 'static,
    ) -> Option<R> {
        let (tx, rx) = oneshot::channel();
        self.dev.apply(Box::new(move |tree, reg| {
            let _ = tx.send(f(tree, reg));
        }));
        rx.await.ok()
    }

    /// Sets a device's state; the same state publishes nothing.
    /// `Disconnected` and `Lost` are for child devices. Returns
    /// [`ValueError::Gone`] once the driver has stopped.
    pub async fn set_presence(&self, dev: DevIdx, p: Presence) -> Result<(), ValueError> {
        self.call(move |tree, _| tree.set_presence(dev, p))
            .await
            .unwrap_or(Err(ValueError::Gone))
    }

    /// Raises or updates an alert (convention.md:519-527); the same message
    /// publishes nothing.
    pub async fn raise_alert(&self, dev: DevIdx, id: &str, msg: &str) -> Result<(), AlertError> {
        let (id, msg) = (id.to_owned(), msg.to_owned());
        self.call(move |tree, _| tree.raise_alert(dev, &id, &msg).map(|_| ()))
            .await
            .unwrap_or(Err(AlertError::Gone))
    }

    /// Deletes an alert; one not raised publishes nothing.
    pub async fn clear_alert(&self, dev: DevIdx, id: &str) -> Result<(), AlertError> {
        let id = id.to_owned();
        self.call(move |tree, _| tree.clear_alert(dev, &id).map(|_| ()))
            .await
            .unwrap_or(Err(AlertError::Gone))
    }

    /// Changes the tree in one commit. Typed handles made in `f` go live
    /// with it; a refused commit changes nothing. Returns
    /// [`BuildError::Gone`] once the driver has stopped.
    pub async fn reconfigure<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut ReconfigureHandles<'_, '_>) -> Result<R, BuildError> + Send + 'static,
    ) -> Result<R, BuildError> {
        self.call(move |tree, reg| {
            let r = tree.reconfigure(|core| {
                let mut h = ReconfigureHandles { core, reg };
                f(&mut h)
            });
            match r {
                Ok(r) => {
                    reg.commit(tree);
                    Ok(r)
                }
                Err(e) => {
                    reg.rollback();
                    Err(e)
                }
            }
        })
        .await
        .unwrap_or(Err(BuildError::Gone))
    }

    /// The next `$broadcast` message (subtopic, payload), with
    /// [`TreeConfig::broadcast`](ebus_homie::device::TreeConfig::broadcast);
    /// `None` once the driver has stopped.
    pub async fn next_broadcast(&self) -> Option<(String, Bytes)> {
        self.broadcasts.lock().await.recv().await
    }

    /// The root's state becomes `lost`, connected or not; while down the
    /// next refresh publishes it.
    pub async fn declare_lost(&self) {
        let _ = self.call(|tree, _| tree.declare_lost()).await;
    }

    /// Stops the tree in `mode` and returns once the driver has handed out
    /// every command and stopped; the host then ends the connection as
    /// [`Commands::ending`](crate::Commands::ending) says. Returns at once
    /// when the driver has already stopped.
    pub async fn shutdown(&self, mode: Shutdown) {
        let (reply, rx) = oneshot::channel();
        if self
            .dev
            .requests
            .send(DevRequest::Shutdown { mode, reply })
            .is_ok()
        {
            let _ = rx.await;
        }
    }

    /// Stops the driver at once: nothing more is published and no
    /// DISCONNECT is sent, so the broker publishes the will.
    pub fn abort(&self) {
        self.dev.aborted.store(true, Ordering::Release);
        self.dev.wake.notify_one();
    }
}
