//! CPU cost of the device core's unchanged-value gate: a tree of 20 devices
//! by 150 `float` properties, every value updated once per tick, about one
//! in ten changed. Reports nanoseconds per `Tree::set_value` for unchanged
//! and changed values, and per changed value to poll its `Command`.
//!
//! ```text
//! cargo run --release -p ebus-homie --example device_bench
//! ```
//!
//! Allocation counts need a counting global allocator, which needs `unsafe`
//! and so cannot live in this workspace: a host binary that has one
//! includes this file as a module and passes its counter to [`run`].

use std::hint::black_box;
use std::time::{Duration, Instant as Clock};

use ebus_homie::device::{
    DeviceSpec, NodeSpec, PropSpec, SlotId, Tree, TreeBuilder, TreeConfig, ValueRef,
};
use ebus_mqtt::{Instant, Link};

/// Devices in the tree, the root included.
pub const DEVICES: usize = 20;
/// `float` properties per device.
pub const PROPS: usize = 150;
/// One value in this many changes per tick.
pub const CHANGE_EVERY: usize = 10;
/// Ticks run before measuring.
pub const WARMUP: usize = 20;
/// Ticks measured.
pub const TICKS: usize = 200;

/// What [`run`] measured.
#[derive(Debug)]
pub struct Stats {
    /// Unchanged `set_value` calls.
    pub unchanged: u64,
    /// Changed `set_value` calls.
    pub changed: u64,
    /// Time in unchanged `set_value` calls.
    pub unchanged_time: Duration,
    /// Time in changed `set_value` calls.
    pub changed_time: Duration,
    /// Time polling the changed values' commands.
    pub poll_time: Duration,
    /// Allocations in unchanged and in changed `set_value` calls, and in
    /// polling; `None` without a counter.
    pub allocs: Option<[u64; 3]>,
}

fn tree() -> (Tree, Vec<SlotId>) {
    let mut b =
        TreeBuilder::new(TreeConfig::default(), "bench-root", DeviceSpec::default()).expect("root");
    let mut devices = vec![b.root()];
    for d in 1..DEVICES {
        let parent = b.root();
        let child = b
            .edit()
            .add_child(parent, &format!("bench-{d:02}"), DeviceSpec::default())
            .expect("child");
        devices.push(child);
    }
    let mut slots = Vec::with_capacity(DEVICES * PROPS);
    for dev in devices {
        let node = b
            .edit()
            .add_node(dev, "meter", NodeSpec::default())
            .expect("node");
        for p in 0..PROPS {
            let slot = b
                .edit()
                .add_typed::<f64>(node, &format!("p{p:03}"), PropSpec::new())
                .expect("property");
            slots.push(slot);
        }
    }
    (b.finish().expect("tree"), slots)
}

/// Runs the benchmark; `allocs`, when given, reads a global allocation
/// count.
pub fn run(allocs: Option<&dyn Fn() -> u64>) -> Stats {
    let (mut tree, slots) = tree();
    let mut values: Vec<f64> = (0..slots.len()).map(|i| i as f64 * 0.1).collect();
    for (slot, v) in slots.iter().zip(&values) {
        tree.set_value(*slot, ValueRef::Float(*v)).expect("value");
    }
    tree.handle_link(Instant(0), Link::up(false));
    while tree.poll_command().is_some() {}

    let count = || allocs.map_or(0, |f| f());
    let mut s = Stats {
        unchanged: 0,
        changed: 0,
        unchanged_time: Duration::ZERO,
        changed_time: Duration::ZERO,
        poll_time: Duration::ZERO,
        allocs: allocs.map(|_| [0; 3]),
    };
    let mut changed: Vec<usize> = Vec::with_capacity(slots.len() / CHANGE_EVERY + 1);
    let mut unchanged: Vec<usize> = Vec::with_capacity(slots.len());
    for tick in 0..WARMUP + TICKS {
        let measure = tick >= WARMUP;
        changed.clear();
        unchanged.clear();
        for (i, v) in values.iter_mut().enumerate() {
            if (i + tick) % CHANGE_EVERY == 0 {
                *v += 0.25;
                changed.push(i);
            } else {
                unchanged.push(i);
            }
        }

        let (a0, t0) = (count(), Clock::now());
        for &i in &unchanged {
            black_box(tree.set_value(slots[i], ValueRef::Float(values[i]))).expect("value");
        }
        let (a1, t1) = (count(), Clock::now());
        for &i in &changed {
            black_box(tree.set_value(slots[i], ValueRef::Float(values[i]))).expect("value");
        }
        let (a2, t2) = (count(), Clock::now());
        let mut polled = 0;
        while let Some(cmd) = tree.poll_command() {
            black_box(&cmd);
            polled += 1;
        }
        let (a3, t3) = (count(), Clock::now());
        assert_eq!(polled, changed.len(), "one publish per changed value");

        if measure {
            s.unchanged += unchanged.len() as u64;
            s.changed += changed.len() as u64;
            s.unchanged_time += t1 - t0;
            s.changed_time += t2 - t1;
            s.poll_time += t3 - t2;
            if let Some(a) = s.allocs.as_mut() {
                a[0] += a1 - a0;
                a[1] += a2 - a1;
                a[2] += a3 - a2;
            }
        }
    }
    s
}

/// Prints `s`.
pub fn report(s: &Stats) {
    let per = |d: Duration, n: u64| d.as_nanos() as f64 / n.max(1) as f64;
    println!(
        "{DEVICES} devices x {PROPS} float properties, {TICKS} ticks, 1 in {CHANGE_EVERY} changed"
    );
    println!(
        "unchanged set_value: {:7.1} ns  ({} calls)",
        per(s.unchanged_time, s.unchanged),
        s.unchanged
    );
    println!(
        "changed set_value:   {:7.1} ns  ({} calls)",
        per(s.changed_time, s.changed),
        s.changed
    );
    println!(
        "poll_command:        {:7.1} ns per changed value",
        per(s.poll_time, s.changed)
    );
    match s.allocs {
        Some([u, c, p]) => {
            println!(
                "allocations: {:.2} per unchanged set_value, {:.2} per changed set_value, {:.2} per changed value polled",
                u as f64 / s.unchanged.max(1) as f64,
                c as f64 / s.changed.max(1) as f64,
                p as f64 / s.changed.max(1) as f64
            );
        }
        None => println!("allocations: not counted"),
    }
}

fn main() {
    report(&run(None));
}
