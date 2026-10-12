//! Merging what an mDNS backend reports into service instances.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use crate::addr::ScopedAddr;
use crate::txt::TxtRecord;

/// A service instance's identity: its type and instance name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstanceKey {
    /// The service type without a domain, such as `_mqtt._tcp`.
    pub service_type: String,
    /// The instance label alone.
    pub name: String,
}

/// Where a backend resolved an instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Slot {
    /// A backend that reports an instance's whole address set at once (mdns-sd).
    Whole,
    /// A backend that resolves an instance per interface and protocol (Avahi).
    Link {
        /// The interface index.
        ifindex: u32,
        /// True for IPv6, false for IPv4.
        v6: bool,
    },
}

/// One report from a backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sighting {
    /// An instance resolved, or resolved again, in one slot.
    Seen {
        /// The instance.
        key: InstanceKey,
        /// Where it was resolved.
        slot: Slot,
        /// The SRV target.
        server: String,
        /// The SRV port.
        port: u16,
        /// The TXT record.
        txt: TxtRecord,
        /// The addresses of this report; they replace the slot's previous ones.
        addrs: Vec<ScopedAddr>,
    },
    /// An instance withdrawn from one slot, or from every slot.
    Gone {
        /// The instance.
        key: InstanceKey,
        /// The slot; `None` for every slot.
        slot: Option<Slot>,
    },
}

/// A service instance as the table holds it.
///
/// Built with [`Instance::new`] outside this crate.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Instance {
    /// The instance.
    pub key: InstanceKey,
    /// The SRV target of the latest report.
    pub server: String,
    /// The SRV port of the latest report.
    pub port: u16,
    /// The TXT record of the latest report.
    pub txt: TxtRecord,
    /// The union of every slot's addresses, without duplicates, in the order
    /// the slots and addresses were first reported.
    pub addrs: Vec<ScopedAddr>,
}

impl Instance {
    /// An instance with the given SRV target, port, TXT record and addresses.
    pub fn new(
        key: InstanceKey,
        server: String,
        port: u16,
        txt: TxtRecord,
        addrs: Vec<ScopedAddr>,
    ) -> Self {
        Self {
            key,
            server,
            port,
            txt,
            addrs,
        }
    }
}

#[derive(Clone, Debug)]
struct Entry {
    server: String,
    port: u16,
    txt: TxtRecord,
    slots: Vec<(Slot, Vec<ScopedAddr>)>,
}

/// The instances a backend currently reports, keyed by (service type,
/// instance name), with no I/O.
///
/// Each slot holds the addresses of its latest report, so an address changed
/// in place replaces the old one; an instance's server, port and TXT are the
/// latest reported, and it leaves the table with its last slot.
#[derive(Clone, Debug, Default)]
pub struct InstanceTable {
    entries: BTreeMap<InstanceKey, Entry>,
}

impl InstanceTable {
    /// An empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one report; true when the instances it yields changed.
    pub fn handle(&mut self, s: Sighting) -> bool {
        match s {
            Sighting::Seen {
                key,
                slot,
                server,
                port,
                txt,
                addrs,
            } => {
                let before = self.get(&key);
                let entry = self.entries.entry(key.clone()).or_insert_with(|| Entry {
                    server: String::new(),
                    port: 0,
                    txt: TxtRecord::new(),
                    slots: Vec::new(),
                });
                entry.server = server;
                entry.port = port;
                entry.txt = txt;
                match entry.slots.iter_mut().find(|(s, _)| *s == slot) {
                    Some((_, held)) => *held = addrs,
                    None => entry.slots.push((slot, addrs)),
                }
                before != self.get(&key)
            }
            Sighting::Gone { key, slot } => {
                let before = self.get(&key);
                let Some(entry) = self.entries.get_mut(&key) else {
                    return false;
                };
                match slot {
                    Some(slot) => entry.slots.retain(|(s, _)| *s != slot),
                    None => entry.slots.clear(),
                }
                if entry.slots.is_empty() {
                    self.entries.remove(&key);
                }
                before != self.get(&key)
            }
        }
    }

    /// One instance.
    pub fn get(&self, key: &InstanceKey) -> Option<Instance> {
        self.entries.get(key).map(|e| instance(key, e))
    }

    /// Every instance, ordered by service type, then instance name.
    pub fn instances(&self) -> impl Iterator<Item = Instance> + '_ {
        self.entries.iter().map(|(k, e)| instance(k, e))
    }

    /// The number of instances.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when the table holds no instance.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn instance(key: &InstanceKey, e: &Entry) -> Instance {
    let mut addrs: Vec<ScopedAddr> = Vec::new();
    for a in e.slots.iter().flat_map(|(_, a)| a) {
        if !addrs.contains(a) {
            addrs.push(*a);
        }
    }
    Instance {
        key: key.clone(),
        server: e.server.clone(),
        port: e.port,
        txt: e.txt.clone(),
        addrs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn key(name: &str) -> InstanceKey {
        InstanceKey {
            service_type: "_mqtt._tcp".into(),
            name: name.into(),
        }
    }

    fn a(s: &str, ifindex: Option<u32>) -> ScopedAddr {
        ScopedAddr {
            ip: s.parse().unwrap(),
            ifindex,
        }
    }

    fn seen(name: &str, slot: Slot, port: u16, addrs: Vec<ScopedAddr>) -> Sighting {
        Sighting::Seen {
            key: key(name),
            slot,
            server: "b.local.".into(),
            port,
            txt: TxtRecord::from_strings(["txtvers=1"]),
            addrs,
        }
    }

    const V4: Slot = Slot::Link {
        ifindex: 2,
        v6: false,
    };
    const V6: Slot = Slot::Link {
        ifindex: 2,
        v6: true,
    };

    #[test]
    fn link_slots_merge_into_one_instance() {
        let mut t = InstanceTable::new();
        assert!(t.handle(seen("b", V4, 1883, vec![a("192.168.1.5", None)])));
        assert!(t.handle(seen("b", V6, 1883, vec![a("fe80::1", Some(2))])));
        assert_eq!(t.len(), 1);
        let i = t.get(&key("b")).unwrap();
        assert_eq!(i.addrs, [a("192.168.1.5", None), a("fe80::1", Some(2))]);
        assert!(!t.handle(seen("b", V6, 1883, vec![a("fe80::1", Some(2))])));
    }

    #[test]
    fn address_replaced_in_its_slot() {
        let mut t = InstanceTable::new();
        t.handle(seen("b", V4, 1883, vec![a("192.168.1.5", None)]));
        t.handle(seen("b", V6, 1883, vec![a("fe80::1", Some(2))]));
        assert!(t.handle(seen("b", V4, 1883, vec![a("192.168.1.6", None)])));
        assert_eq!(
            t.get(&key("b")).unwrap().addrs,
            [a("192.168.1.6", None), a("fe80::1", Some(2))]
        );
    }

    #[test]
    fn latest_server_port_and_txt() {
        let mut t = InstanceTable::new();
        t.handle(seen("b", V4, 1883, vec![a("192.168.1.5", None)]));
        assert!(t.handle(seen("b", V6, 1884, vec![])));
        assert_eq!(t.get(&key("b")).unwrap().port, 1884);
    }

    #[test]
    fn slot_and_whole_removal() {
        let mut t = InstanceTable::new();
        t.handle(seen("b", V4, 1883, vec![a("192.168.1.5", None)]));
        t.handle(seen("b", V6, 1883, vec![a("fe80::1", Some(2))]));
        assert!(!t.handle(Sighting::Gone {
            key: key("b"),
            slot: Some(Slot::Link {
                ifindex: 3,
                v6: false
            }),
        }));
        assert!(t.handle(Sighting::Gone {
            key: key("b"),
            slot: Some(V4),
        }));
        assert_eq!(t.get(&key("b")).unwrap().addrs, [a("fe80::1", Some(2))]);
        assert!(t.handle(Sighting::Gone {
            key: key("b"),
            slot: Some(V6),
        }));
        assert!(t.is_empty());
        assert!(!t.handle(Sighting::Gone {
            key: key("b"),
            slot: None,
        }));
    }

    #[test]
    fn whole_reports() {
        let mut t = InstanceTable::new();
        let both = vec![a("192.168.1.5", None), a("fd00::5", None)];
        assert!(t.handle(seen("b", Slot::Whole, 1883, both.clone())));
        assert!(t.handle(seen("a", Slot::Whole, 1883, both.clone())));
        let names: Vec<String> = t.instances().map(|i| i.key.name).collect();
        assert_eq!(names, ["a", "b"]);
        assert!(t.handle(seen("b", Slot::Whole, 1883, vec![a("192.168.1.5", None)])));
        assert!(t.handle(Sighting::Gone {
            key: key("b"),
            slot: None,
        }));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn removing_a_slot_that_adds_nothing_changes_nothing() {
        let mut t = InstanceTable::new();
        t.handle(seen("b", V4, 1883, vec![a("192.168.1.5", None)]));
        t.handle(seen(
            "b",
            Slot::Link {
                ifindex: 3,
                v6: false,
            },
            1883,
            vec![a("192.168.1.5", None)],
        ));
        assert!(!t.handle(Sighting::Gone {
            key: key("b"),
            slot: Some(V4),
        }));
        assert_eq!(t.len(), 1);
    }
}
