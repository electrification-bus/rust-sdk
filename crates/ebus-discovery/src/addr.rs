//! Address scopes and candidate ordering (record.py:51-61, 93-108; instance.py:41-56).

use core::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[cfg(feature = "alloc")]
use alloc::vec::Vec;

/// The reachability class of an address, declared in candidate preference
/// order (record.py:51-61): routable addresses before link-local ones.
///
/// The classification is python's (record.py:93-108) over python's
/// `ipaddress` tables of addresses that are not globally reachable, so every
/// SDK orders the same advertisement the same way. An IPv4-mapped IPv6
/// address takes its loopback, unspecified, link-local and global tests from
/// the IPv4 address, as python's does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AddrScope {
    /// Globally reachable.
    Global,
    /// IPv4 not globally reachable (RFC 1918 and the other special-purpose blocks).
    Private,
    /// IPv6 unique local, `fc00::/7`.
    UniqueLocal,
    /// Neither global nor private, such as CGNAT `100.64.0.0/10`.
    Other,
    /// IPv6 `fe80::/10` or IPv4 `169.254.0.0/16`.
    LinkLocal,
    /// Loopback.
    Loopback,
    /// The unspecified address.
    Unspecified,
}

impl AddrScope {
    /// The scope of `ip`.
    pub fn of(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(v4) => v4_scope(v4),
            IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) => {
                    if v4.is_loopback() {
                        Self::Loopback
                    } else if v4.is_unspecified() {
                        Self::Unspecified
                    } else if v4.is_link_local() {
                        Self::LinkLocal
                    } else if v4_is_global(v4) {
                        Self::Global
                    } else {
                        Self::Other
                    }
                }
                None => {
                    if v6.is_loopback() {
                        Self::Loopback
                    } else if v6.is_unspecified() {
                        Self::Unspecified
                    } else if in_v6(v6, 0xfe80 << 112, 10) {
                        Self::LinkLocal
                    } else if in_v6(v6, 0xfc00 << 112, 7) {
                        Self::UniqueLocal
                    } else if v6_is_private(v6) {
                        Self::Other
                    } else {
                        Self::Global
                    }
                }
            },
        }
    }

    /// Sort key for candidates: lower first. `Private` and `UniqueLocal` tie
    /// (record.py:51-61).
    pub const fn preference(self) -> u8 {
        match self {
            Self::Global => 0,
            Self::Private | Self::UniqueLocal => 1,
            Self::Other => 2,
            Self::LinkLocal => 3,
            Self::Loopback => 4,
            Self::Unspecified => 5,
        }
    }

    /// The name python-service-discovery gives the scope (`global`,
    /// `private`, `unique-local`, `other`, `link-local`, `loopback`,
    /// `unspecified`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Private => "private",
            Self::UniqueLocal => "unique-local",
            Self::Other => "other",
            Self::LinkLocal => "link-local",
            Self::Loopback => "loopback",
            Self::Unspecified => "unspecified",
        }
    }
}

fn v4_scope(v4: Ipv4Addr) -> AddrScope {
    if v4.is_loopback() {
        AddrScope::Loopback
    } else if v4.is_unspecified() {
        AddrScope::Unspecified
    } else if v4.is_link_local() {
        AddrScope::LinkLocal
    } else if v4_is_private(v4) {
        AddrScope::Private
    } else if v4_is_global(v4) {
        AddrScope::Global
    } else {
        AddrScope::Other
    }
}

fn in_v4(ip: Ipv4Addr, net: u32, prefix: u32) -> bool {
    let shift = 32 - prefix;
    prefix == 0 || (u32::from(ip) >> shift) == (net >> shift)
}

fn in_v6(ip: Ipv6Addr, net: u128, prefix: u32) -> bool {
    let shift = 128 - prefix;
    prefix == 0 || (u128::from(ip) >> shift) == (net >> shift)
}

const fn v4(a: u8, b: u8, c: u8, d: u8) -> u32 {
    u32::from_be_bytes([a, b, c, d])
}

/// Python's `IPv4Address.is_private`: the IANA blocks not globally reachable.
fn v4_is_private(ip: Ipv4Addr) -> bool {
    const NETS: [(u32, u32); 14] = [
        (v4(0, 0, 0, 0), 8),
        (v4(10, 0, 0, 0), 8),
        (v4(127, 0, 0, 0), 8),
        (v4(169, 254, 0, 0), 16),
        (v4(172, 16, 0, 0), 12),
        (v4(192, 0, 0, 0), 24),
        (v4(192, 0, 0, 170), 31),
        (v4(192, 0, 2, 0), 24),
        (v4(192, 168, 0, 0), 16),
        (v4(198, 18, 0, 0), 15),
        (v4(198, 51, 100, 0), 24),
        (v4(203, 0, 113, 0), 24),
        (v4(240, 0, 0, 0), 4),
        (v4(255, 255, 255, 255), 32),
    ];
    const EXCEPT: [(u32, u32); 2] = [(v4(192, 0, 0, 9), 32), (v4(192, 0, 0, 10), 32)];
    NETS.iter().any(|&(n, p)| in_v4(ip, n, p)) && !EXCEPT.iter().any(|&(n, p)| in_v4(ip, n, p))
}

/// Python's `IPv4Address.is_global`: not private and not `100.64.0.0/10`.
fn v4_is_global(ip: Ipv4Addr) -> bool {
    !in_v4(ip, v4(100, 64, 0, 0), 10) && !v4_is_private(ip)
}

/// Python's `IPv6Address.is_private` for an address that is not IPv4-mapped.
fn v6_is_private(ip: Ipv6Addr) -> bool {
    const NETS: [(u128, u32); 11] = [
        (1, 128),
        (0, 128),
        (0xffff_0000_0000, 96),
        (0x0064_ff9b_0001 << 80, 48),
        (0x0100 << 112, 64),
        (0x2001 << 112, 23),
        (0x2001_0db8 << 96, 32),
        (0x2002 << 112, 16),
        (0x3fff << 112, 20),
        (0xfc00 << 112, 7),
        (0xfe80 << 112, 10),
    ];
    const EXCEPT: [(u128, u32); 6] = [
        ((0x2001_0001 << 96) | 1, 128),
        ((0x2001_0001 << 96) | 2, 128),
        (0x2001_0003 << 96, 32),
        (0x2001_0004_0112 << 80, 48),
        (0x2001_0020 << 96, 28),
        (0x2001_0030 << 96, 28),
    ];
    NETS.iter().any(|&(n, p)| in_v6(ip, n, p)) && !EXCEPT.iter().any(|&(n, p)| in_v6(ip, n, p))
}

/// An address an mDNS backend reported, with the interface it was seen on.
///
/// `Ord` sorts by address, then interface, which breaks ties between
/// candidates of equal preference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScopedAddr {
    /// The address.
    pub ip: IpAddr,
    /// The interface index; required to connect to an IPv6 link-local address.
    pub ifindex: Option<u32>,
}

impl ScopedAddr {
    /// An address with no interface.
    pub const fn new(ip: IpAddr) -> Self {
        Self { ip, ifindex: None }
    }

    /// The address's scope.
    pub fn scope(&self) -> AddrScope {
        AddrScope::of(self.ip)
    }

    /// True when the address can be tried: not loopback or unspecified, and
    /// an IPv6 link-local address only with an interface (instance.py:41-56).
    pub fn is_candidate(&self) -> bool {
        match self.scope() {
            AddrScope::Loopback | AddrScope::Unspecified => false,
            AddrScope::LinkLocal => self.ip.is_ipv4() || self.ifindex.is_some(),
            _ => true,
        }
    }
}

impl From<IpAddr> for ScopedAddr {
    fn from(ip: IpAddr) -> Self {
        Self::new(ip)
    }
}

/// The addresses worth trying, most preferred first: [`ScopedAddr::is_candidate`]
/// ones, ordered by [`AddrScope::preference`], then by address, without
/// duplicates.
#[cfg(feature = "alloc")]
pub fn candidate_addresses(addrs: &[ScopedAddr]) -> Vec<ScopedAddr> {
    let mut out: Vec<ScopedAddr> = addrs
        .iter()
        .copied()
        .filter(ScopedAddr::is_candidate)
        .collect();
    out.sort_by_key(|a| (a.scope().preference(), *a));
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(s: &str) -> AddrScope {
        AddrScope::of(s.parse().unwrap())
    }

    #[test]
    fn scopes() {
        assert_eq!(scope("8.8.8.8"), AddrScope::Global);
        assert_eq!(scope("192.168.1.2"), AddrScope::Private);
        assert_eq!(scope("10.0.0.1"), AddrScope::Private);
        assert_eq!(scope("192.0.0.9"), AddrScope::Global);
        assert_eq!(scope("100.64.0.1"), AddrScope::Other);
        assert_eq!(scope("169.254.1.1"), AddrScope::LinkLocal);
        assert_eq!(scope("127.0.0.1"), AddrScope::Loopback);
        assert_eq!(scope("0.0.0.0"), AddrScope::Unspecified);
        assert_eq!(scope("0.1.2.3"), AddrScope::Private);
        assert_eq!(scope("2606:4700::1"), AddrScope::Global);
        assert_eq!(scope("fd00::1"), AddrScope::UniqueLocal);
        assert_eq!(scope("fe80::1"), AddrScope::LinkLocal);
        assert_eq!(scope("::1"), AddrScope::Loopback);
        assert_eq!(scope("::"), AddrScope::Unspecified);
        assert_eq!(scope("2001:db8::1"), AddrScope::Other);
        assert_eq!(scope("2001:20::1"), AddrScope::Global);
        assert_eq!(scope("::ffff:10.0.0.1"), AddrScope::Other);
        assert_eq!(scope("::ffff:8.8.8.8"), AddrScope::Global);
        assert_eq!(scope("::ffff:127.0.0.1"), AddrScope::Loopback);
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn candidates() {
        let a = |s: &str, i: Option<u32>| ScopedAddr {
            ip: s.parse().unwrap(),
            ifindex: i,
        };
        let got = candidate_addresses(&[
            a("fe80::1", None),
            a("fe80::2", Some(3)),
            a("169.254.0.5", None),
            a("127.0.0.1", None),
            a("fd00::1", None),
            a("192.168.1.9", None),
            a("192.168.1.2", None),
            a("8.8.8.8", None),
            a("100.64.0.1", None),
            a("192.168.1.2", None),
        ]);
        assert_eq!(
            got,
            [
                a("8.8.8.8", None),
                a("192.168.1.2", None),
                a("192.168.1.9", None),
                a("fd00::1", None),
                a("100.64.0.1", None),
                a("169.254.0.5", None),
                a("fe80::2", Some(3)),
            ]
        );
    }
}
