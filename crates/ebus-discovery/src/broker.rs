//! Broker endpoints, ranking and selection (ebus.py:565-795).

use alloc::borrow::Cow;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;
use core::net::IpAddr;

use crate::addr::{ScopedAddr, candidate_addresses};
use crate::instance::Instance;
use crate::txt::split_list;
use crate::{BrokerMode, BrokerType, is_local_name, same_host, strip_dot};

/// A broker to connect to, discovered or configured.
///
/// Built with [`from_instance`](Self::from_instance),
/// [`configured`](Self::configured) or [`from_url`](Self::from_url).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct BrokerEndpoint {
    /// The transport.
    pub broker_type: BrokerType,
    /// The TXT `broker` value, else the SRV target, without a trailing dot
    /// (ebus.py:599-614); the configured URL's host for a configured broker.
    pub host: String,
    /// The port.
    pub port: u16,
    /// The SRV target, without a trailing dot.
    pub server: Option<String>,
    /// The TXT `device_id` value, as advertised.
    pub device_id: Option<String>,
    /// The advertised addresses worth trying, in candidate order
    /// ([`candidate_addresses`]).
    pub addrs: Vec<ScopedAddr>,
    /// The instance name; `None` for a configured broker.
    pub instance: Option<String>,
}

/// A configured broker URL that cannot be used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum UrlError {
    /// A scheme other than `mqtt://` or `mqtts://`.
    Scheme,
    /// No host.
    NoHost,
    /// A port that is not a number from 0 to 65535.
    Port,
}

impl fmt::Display for UrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Scheme => "unsupported broker URL: expected mqtt:// or mqtts://",
            Self::NoHost => "broker URL has no host",
            Self::Port => "broker URL has an invalid port",
        })
    }
}

impl core::error::Error for UrlError {}

impl BrokerEndpoint {
    /// The endpoint a browsed broker instance gives (ebus.py:598-614): `host`
    /// is the TXT `broker` value, trimmed and with one trailing dot removed,
    /// else the SRV target; port 0 becomes the type's default port. `None`
    /// for an instance that is not a broker type.
    pub fn from_instance(instance: &Instance) -> Option<Self> {
        let broker_type = BrokerType::parse(&instance.key.service_type)?;
        let server = strip_dot(&instance.server);
        let txt_broker = instance.txt.get("broker");
        let txt_broker = txt_broker.as_deref().map_or("", str::trim);
        let host = if txt_broker.is_empty() {
            server
        } else {
            strip_dot(txt_broker)
        };
        let port = match instance.port {
            0 => broker_type.default_port(),
            p => p,
        };
        Some(Self {
            broker_type,
            host: host.into(),
            port,
            server: (!server.is_empty()).then(|| server.into()),
            device_id: instance
                .txt
                .get("device_id")
                .filter(|d| !d.is_empty())
                .map(Cow::into_owned),
            addrs: candidate_addresses(&instance.addrs),
            instance: Some(instance.key.name.clone()),
        })
    }

    /// A configured broker from an `mqtt://` or `mqtts://` URL
    /// (ebus.py:616-633): the port defaults to 1883 or 8883, and the host is
    /// lowercased, as python's `urlsplit` gives it, except an IPv6 zone.
    pub fn from_url(url: &str) -> Result<Self, UrlError> {
        let url = url.trim();
        let (scheme, rest) = url.split_once(':').ok_or(UrlError::Scheme)?;
        let broker_type = if scheme.eq_ignore_ascii_case("mqtt") {
            BrokerType::Mqtt
        } else if scheme.eq_ignore_ascii_case("mqtts") {
            BrokerType::SecureMqtt
        } else {
            return Err(UrlError::Scheme);
        };
        let netloc = rest.strip_prefix("//").ok_or(UrlError::NoHost)?;
        let netloc = netloc
            .find(['/', '?', '#'])
            .map_or(netloc, |at| &netloc[..at]);
        let hostinfo = netloc.rsplit_once('@').map_or(netloc, |(_, h)| h);
        let (host, port) = match hostinfo.split_once('[') {
            Some((_, bracketed)) => {
                let (host, after) = bracketed.split_once(']').unwrap_or((bracketed, ""));
                (host, after.split_once(':').map_or("", |(_, p)| p))
            }
            None => hostinfo.split_once(':').unwrap_or((hostinfo, "")),
        };
        if host.is_empty() {
            return Err(UrlError::NoHost);
        }
        let port = match port {
            "" => 0,
            p if p.bytes().all(|b| b.is_ascii_digit()) => p.parse().map_err(|_| UrlError::Port)?,
            _ => return Err(UrlError::Port),
        };
        // urlsplit lowercases the host but not an IPv6 zone.
        let host = match host.split_once('%') {
            Some((h, zone)) => format!("{}%{zone}", h.to_lowercase()),
            None => host.to_lowercase(),
        };
        Ok(Self::configured(
            broker_type,
            &host,
            match port {
                0 => broker_type.default_port(),
                p => p,
            },
        ))
    }

    /// A configured broker of `broker_type` at `host` and `port`.
    pub fn configured(broker_type: BrokerType, host: &str, port: u16) -> Self {
        Self {
            broker_type,
            host: host.into(),
            port,
            server: None,
            device_id: None,
            addrs: Vec::new(),
            instance: None,
        }
    }

    /// `scheme://host:port`, with an IPv6 host in brackets (ebus.py:592-596).
    pub fn url(&self) -> String {
        if self.host.contains(':') {
            format!(
                "{}://[{}]:{}",
                self.broker_type.scheme(),
                self.host,
                self.port
            )
        } else {
            format!(
                "{}://{}:{}",
                self.broker_type.scheme(),
                self.host,
                self.port
            )
        }
    }

    /// The host to connect to: `host` when it is not a `.local` name, else the
    /// first IPv4 candidate address, else `host`, for TLS and plain transports
    /// alike.
    pub fn connect_host(&self) -> Cow<'_, str> {
        if !is_local_name(&self.host) {
            return Cow::Borrowed(&self.host);
        }
        match self.addrs.iter().find(|a| a.ip.is_ipv4()) {
            Some(a) => Cow::Owned(a.ip.to_string()),
            None => Cow::Borrowed(&self.host),
        }
    }

    /// The TXT `device_id` items.
    pub fn device_ids(&self) -> Vec<String> {
        self.device_id
            .as_deref()
            .map(split_list)
            .unwrap_or_default()
    }
}

/// The particular broker a host wants among those discovered.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Wanted {
    /// The broker with this instance name, compared ignoring case.
    Instance(String),
    /// The broker whose TXT `device_id` lists this id, under any of its
    /// transports: an endpoint matches when its own `device_id` lists the
    /// id, or when a sighted endpoint of the same broker (sharing a host name
    /// or SRV target), of any broker type, lists it.
    DeviceId(String),
}

impl Wanted {
    /// True when `ep` is the wanted broker. `sighted` is every broker
    /// endpoint seen, of every type, `ep` among them or not; a
    /// [`Wanted::DeviceId`] reads the `device_id` of `ep` and of the
    /// endpoints in `sighted` that share its host name or SRV target.
    pub fn matches(&self, ep: &BrokerEndpoint, sighted: &[BrokerEndpoint]) -> bool {
        match self {
            Self::Instance(name) => ep.instance.as_deref().is_some_and(|i| same_host(i, name)),
            Self::DeviceId(id) => {
                let lists = |e: &BrokerEndpoint| e.device_ids().iter().any(|d| d == id);
                lists(ep) || sighted.iter().any(|s| lists(s) && same_broker(s, ep))
            }
        }
    }
}

fn norm_host(name: &str) -> String {
    strip_dot(name).to_lowercase()
}

/// The normalized host name and SRV target of `ep`, those not empty.
fn host_keys(ep: &BrokerEndpoint) -> impl Iterator<Item = String> + '_ {
    core::iter::once(&ep.host)
        .chain(ep.server.as_ref())
        .map(|h| norm_host(h))
        .filter(|h| !h.is_empty())
}

/// True when `a` and `b` share a host name or SRV target.
fn same_broker(a: &BrokerEndpoint, b: &BrokerEndpoint) -> bool {
    host_keys(a).any(|ka| host_keys(b).any(|kb| ka == kb))
}

/// The names that make two endpoints one broker (ebus.py:665-672).
fn broker_keys(ep: &BrokerEndpoint) -> Vec<(bool, String)> {
    let mut keys: Vec<(bool, String)> = host_keys(ep).map(|h| (false, h)).collect();
    if let Some(d) = &ep.device_id {
        keys.push((true, d.clone()));
    }
    keys
}

/// One endpoint per distinct broker, ranked in the client's preference order
/// (ebus.py:675-702).
///
/// `order` lists the broker types the client accepts, most preferred first;
/// a type listed twice keeps its first position. Endpoints of other types are
/// dropped. The rest are sorted by their type's position in `order`, then
/// lowercased host, then port. Endpoints sharing a host name, SRV target or
/// `device_id` are one broker, which keeps its transport earliest in `order`.
/// With [`BROKER_TYPES`](crate::BROKER_TYPES), narrowed to python's accepted
/// types, as `order`, the ranking is python's.
pub fn rank_brokers(
    endpoints: impl IntoIterator<Item = BrokerEndpoint>,
    order: &[BrokerType],
) -> Vec<BrokerEndpoint> {
    let mut candidates: Vec<(usize, String, BrokerEndpoint)> = endpoints
        .into_iter()
        .filter_map(|e| {
            let at = order.iter().position(|t| *t == e.broker_type)?;
            Some((at, norm_host(&e.host), e))
        })
        .collect();
    candidates.sort_by(|(ta, ha, a), (tb, hb, b)| (ta, ha, a.port).cmp(&(tb, hb, b.port)));
    let mut kept: Vec<(BrokerEndpoint, Vec<(bool, String)>)> = Vec::new();
    for (_, _, ep) in candidates {
        let keys = broker_keys(&ep);
        match kept
            .iter_mut()
            .find(|(_, seen)| seen.iter().any(|k| keys.contains(k)))
        {
            Some((_, seen)) => {
                for k in keys {
                    if !seen.contains(&k) {
                        seen.push(k);
                    }
                }
            }
            None => kept.push((ep, keys)),
        }
    }
    kept.into_iter().map(|(ep, _)| ep).collect()
}

fn parse_ip(text: &str) -> Option<IpAddr> {
    text.split('%').next()?.parse().ok()
}

/// (index into `ranked`, endpoint to use) for the first match.
fn find_configured(
    configured: &BrokerEndpoint,
    ranked: &[BrokerEndpoint],
) -> Option<(usize, BrokerEndpoint)> {
    let want_ip = parse_ip(&configured.host);
    ranked.iter().enumerate().find_map(|(i, ep)| {
        let by_name = same_host(&configured.host, &ep.host)
            || ep
                .server
                .as_deref()
                .is_some_and(|s| same_host(&configured.host, s));
        if by_name {
            return Some((i, ep.clone()));
        }
        let ip = want_ip?;
        if !ep.addrs.iter().any(|a| a.ip == ip) {
            return None;
        }
        let mut use_ep = ep.clone();
        // A TLS client verifies the certificate against the discovered name.
        if !ep.broker_type.uses_tls() {
            use_ep.host.clone_from(&configured.host);
        }
        Some((i, use_ep))
    })
}

/// The discovered broker that is the configured one (ebus.py:712-742).
///
/// A discovered broker matches when the configured host equals its TXT
/// `broker` name or SRV target (case-insensitive, trailing dot ignored), or
/// when a configured IP address equals one of its candidate addresses. The
/// first match in `ranked` is returned with its discovered port and
/// transport; a match by address keeps the configured address as `host` on
/// a plain transport and the discovered name on TLS.
pub fn match_configured(
    configured: &BrokerEndpoint,
    ranked: &[BrokerEndpoint],
) -> Option<BrokerEndpoint> {
    find_configured(configured, ranked).map(|(_, ep)| ep)
}

/// What [`select_broker`] chose.
pub(crate) enum Choice {
    None,
    Configured,
    /// The endpoint to use, and the index of the discovered one it came from.
    Discovered(usize, BrokerEndpoint),
}

pub(crate) fn choose(
    mode: BrokerMode,
    configured: Option<&BrokerEndpoint>,
    ranked: &[BrokerEndpoint],
    allow_unmatched: bool,
) -> Choice {
    let fallback = |c: Option<&BrokerEndpoint>| {
        if c.is_some() {
            Choice::Configured
        } else {
            Choice::None
        }
    };
    match mode {
        BrokerMode::ConfiguredOnly => fallback(configured),
        _ if ranked.is_empty() => match mode {
            BrokerMode::DiscoveryWithFallback => fallback(configured),
            _ => Choice::None,
        },
        BrokerMode::DiscoveryWithFallback if configured.is_some() => {
            match configured.and_then(|c| find_configured(c, ranked)) {
                Some((i, ep)) => Choice::Discovered(i, ep),
                None if allow_unmatched => Choice::Discovered(0, ranked[0].clone()),
                None => Choice::Configured,
            }
        }
        _ => Choice::Discovered(0, ranked[0].clone()),
    }
}

/// Log a choice as python's `select_broker` does (ebus.py:782-794).
pub(crate) fn log_choice(
    choice: &Choice,
    configured: Option<&BrokerEndpoint>,
    ranked: &[BrokerEndpoint],
) {
    let (picked, chosen_url) = match (choice, configured) {
        (Choice::Discovered(i, ep), _) => (Some(*i), ep.url()),
        (Choice::Configured, Some(c)) => (None, c.url()),
        _ => return,
    };
    if ranked.is_empty() {
        return;
    }
    for (i, ep) in ranked.iter().enumerate() {
        if Some(i) != picked {
            crate::log::info!("brokerNotChosen", url = ep.url(), chosen = chosen_url);
        }
    }
    if picked.is_none() {
        crate::log::info!("configuredBrokerNotDiscovered", url = chosen_url);
    } else if ranked.len() > 1 {
        let all: Vec<String> = ranked.iter().map(BrokerEndpoint::url).collect();
        crate::log::info!(
            "brokerChosenAmongSeveral",
            count = ranked.len(),
            chosen = chosen_url,
            candidates = all.join("|")
        );
    }
}

/// Choose the broker to connect to (ebus.py:745-795).
///
/// - `ConfiguredOnly`: `configured`.
/// - `DiscoveryOnly`: the first of `ranked`, or `None`.
/// - `DiscoveryWithFallback`: the discovered broker [`match_configured`]
///   pairs with `configured`, else `configured`; an unmatched discovered
///   broker, the first of `ranked`, only with `allow_unmatched`. With no
///   `configured`, the first of `ranked`.
///
/// Each discovered broker not chosen is logged with `reason=brokerNotChosen`.
pub fn select_broker(
    mode: BrokerMode,
    configured: Option<&BrokerEndpoint>,
    ranked: &[BrokerEndpoint],
    allow_unmatched: bool,
) -> Option<BrokerEndpoint> {
    let choice = choose(mode, configured, ranked, allow_unmatched);
    log_choice(&choice, configured, ranked);
    match choice {
        Choice::None => None,
        Choice::Configured => configured.cloned(),
        Choice::Discovered(_, ep) => Some(ep),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::InstanceKey;
    use crate::txt::TxtRecord;
    use crate::{BROKER_TYPES, TCP_BROKER_TYPES};
    use alloc::vec;

    fn inst(
        t: BrokerType,
        name: &str,
        server: &str,
        port: u16,
        txt: &[&str],
        addrs: &[&str],
    ) -> Instance {
        Instance {
            key: InstanceKey {
                service_type: t.as_str().into(),
                name: name.into(),
            },
            server: server.into(),
            port,
            txt: TxtRecord::from_strings(txt),
            addrs: addrs
                .iter()
                .map(|a| ScopedAddr::new(a.parse().unwrap()))
                .collect(),
        }
    }

    fn ep(t: BrokerType, name: &str, server: &str, txt: &[&str], addrs: &[&str]) -> BrokerEndpoint {
        BrokerEndpoint::from_instance(&inst(t, name, server, 0, txt, addrs)).unwrap()
    }

    #[test]
    fn from_instance() {
        let e = BrokerEndpoint::from_instance(&inst(
            BrokerType::SecureMqtt,
            "panel",
            "Panel.local.",
            0,
            &["broker= cert.example.com. ", "device_id=p1"],
            &["127.0.0.1", "192.168.1.5"],
        ))
        .unwrap();
        assert_eq!(e.host, "cert.example.com");
        assert_eq!(e.server.as_deref(), Some("Panel.local"));
        assert_eq!(e.port, 8883);
        assert_eq!(e.device_id.as_deref(), Some("p1"));
        assert_eq!(e.addrs, [ScopedAddr::new("192.168.1.5".parse().unwrap())]);
        assert_eq!(e.instance.as_deref(), Some("panel"));
        assert_eq!(e.url(), "mqtts://cert.example.com:8883");
        let mut other = inst(BrokerType::Mqtt, "x", "", 1884, &[], &[]);
        other.key.service_type = "_ebus._tcp".into();
        assert_eq!(BrokerEndpoint::from_instance(&other), None);
    }

    #[test]
    fn from_url() {
        let e = BrokerEndpoint::from_url(" MQTTS://user:pw@Broker.Local:8884/x?y ").unwrap();
        assert_eq!(
            (e.broker_type, e.host.as_str(), e.port),
            (BrokerType::SecureMqtt, "broker.local", 8884)
        );
        assert_eq!(e.instance, None);
        let e = BrokerEndpoint::from_url("mqtt://[FE80::1%25En0]").unwrap();
        assert_eq!((e.host.as_str(), e.port), ("fe80::1%25En0", 1883));
        assert_eq!(e.url(), "mqtt://[fe80::1%25En0]:1883");
        assert_eq!(BrokerEndpoint::from_url("mqtt://h:0").unwrap().port, 1883);
        assert_eq!(BrokerEndpoint::from_url("mqtt://h:").unwrap().port, 1883);
        assert_eq!(BrokerEndpoint::from_url("ws://h"), Err(UrlError::Scheme));
        assert_eq!(BrokerEndpoint::from_url("h:1883"), Err(UrlError::Scheme));
        assert_eq!(
            BrokerEndpoint::from_url("mqtt://:1883"),
            Err(UrlError::NoHost)
        );
        assert_eq!(BrokerEndpoint::from_url("mqtt:h"), Err(UrlError::NoHost));
        assert_eq!(
            BrokerEndpoint::from_url("mqtt://h:65536"),
            Err(UrlError::Port)
        );
        assert_eq!(BrokerEndpoint::from_url("mqtt://h:x"), Err(UrlError::Port));
        assert_eq!(BrokerEndpoint::from_url("mqtt://h:+1"), Err(UrlError::Port));
    }

    #[test]
    fn connect_host() {
        let dns = ep(
            BrokerType::SecureMqtt,
            "a",
            "a.local.",
            &["broker=mqtt.example.com"],
            &["192.168.1.5"],
        );
        assert_eq!(dns.connect_host(), "mqtt.example.com");
        for t in [BrokerType::SecureMqtt, BrokerType::Mqtt] {
            let local = ep(
                t,
                "a",
                "a.local.",
                &[],
                &["fd00::5", "192.168.1.5", "10.0.0.2"],
            );
            assert_eq!(local.connect_host(), "10.0.0.2");
            let v6 = ep(t, "a", "a.local.", &[], &["fd00::5"]);
            assert_eq!(v6.connect_host(), "a.local");
        }
        let configured = BrokerEndpoint::from_url("mqtts://b.local").unwrap();
        assert_eq!(configured.connect_host(), "b.local");
    }

    #[test]
    fn ranking_merges_transports_and_device_ids() {
        let ranked = rank_brokers(
            vec![
                ep(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
                ep(BrokerType::Mqtt, "a", "A.local.", &[], &[]),
                ep(
                    BrokerType::SecureMqtt,
                    "a",
                    "a.local.",
                    &["device_id=x"],
                    &[],
                ),
                ep(
                    BrokerType::SecureMqtt,
                    "c",
                    "c.local.",
                    &["broker=c.example.com", "device_id=x"],
                    &[],
                ),
                ep(BrokerType::MqttWs, "d", "d.local.", &[], &[]),
            ],
            BROKER_TYPES,
        );
        let got: Vec<String> = ranked.iter().map(BrokerEndpoint::url).collect();
        assert_eq!(
            got,
            [
                "mqtts://a.local:8883",
                "ws://d.local:9001",
                "mqtt://b.local:1883"
            ]
        );
        let tcp = rank_brokers(ranked, TCP_BROKER_TYPES);
        assert_eq!(tcp.len(), 2);
    }

    #[test]
    fn ranking_orders_by_host_then_port() {
        let mut a = ep(BrokerType::Mqtt, "a2", "a2.local.", &[], &[]);
        a.port = 1884;
        a.host = "a.local".into();
        let b = ep(BrokerType::Mqtt, "b", "B.local.", &[], &[]);
        let ranked = rank_brokers(vec![b, a], BROKER_TYPES);
        assert_eq!(ranked[0].host, "a.local");
    }

    #[test]
    fn match_by_name_and_address() {
        let ranked = vec![
            ep(
                BrokerType::SecureMqtt,
                "s",
                "s.local.",
                &[],
                &["192.168.1.7"],
            ),
            ep(BrokerType::Mqtt, "p", "p.local.", &[], &["192.168.1.8"]),
        ];
        let by_name = BrokerEndpoint::from_url("mqtt://P.LOCAL.").unwrap();
        assert_eq!(
            match_configured(&by_name, &ranked)
                .unwrap()
                .instance
                .as_deref(),
            Some("p")
        );
        let tls_ip = BrokerEndpoint::from_url("mqtt://192.168.1.7").unwrap();
        let m = match_configured(&tls_ip, &ranked).unwrap();
        assert_eq!(
            (m.host.as_str(), m.port, m.broker_type),
            ("s.local", 8883, BrokerType::SecureMqtt)
        );
        let plain_ip = BrokerEndpoint::from_url("mqtts://192.168.1.8:9999").unwrap();
        let m = match_configured(&plain_ip, &ranked).unwrap();
        assert_eq!(
            (m.host.as_str(), m.port, m.broker_type),
            ("192.168.1.8", 1883, BrokerType::Mqtt)
        );
        let none = BrokerEndpoint::from_url("mqtt://other").unwrap();
        assert_eq!(match_configured(&none, &ranked), None);
    }

    #[test]
    fn selection_modes() {
        let ranked = vec![
            ep(BrokerType::Mqtt, "a", "a.local.", &[], &[]),
            ep(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        ];
        let conf_b = BrokerEndpoint::from_url("mqtt://b.local").unwrap();
        let conf_x = BrokerEndpoint::from_url("mqtt://x.local").unwrap();
        let pick = |m, c: Option<&BrokerEndpoint>, r: &[BrokerEndpoint], u| {
            select_broker(m, c, r, u).map(|e| e.host)
        };
        use BrokerMode::*;
        assert_eq!(
            pick(ConfiguredOnly, Some(&conf_x), &ranked, false).as_deref(),
            Some("x.local")
        );
        assert_eq!(pick(ConfiguredOnly, None, &ranked, false), None);
        assert_eq!(
            pick(DiscoveryOnly, Some(&conf_b), &ranked, false).as_deref(),
            Some("a.local")
        );
        assert_eq!(pick(DiscoveryOnly, None, &[], false), None);
        assert_eq!(
            pick(DiscoveryWithFallback, Some(&conf_b), &ranked, false).as_deref(),
            Some("b.local")
        );
        assert_eq!(
            pick(DiscoveryWithFallback, Some(&conf_x), &ranked, false).as_deref(),
            Some("x.local")
        );
        assert_eq!(
            pick(DiscoveryWithFallback, Some(&conf_x), &ranked, true).as_deref(),
            Some("a.local")
        );
        assert_eq!(
            pick(DiscoveryWithFallback, Some(&conf_x), &[], true).as_deref(),
            Some("x.local")
        );
        assert_eq!(
            pick(DiscoveryWithFallback, None, &ranked, false).as_deref(),
            Some("a.local")
        );
        assert_eq!(pick(DiscoveryWithFallback, None, &[], false), None);
    }

    #[test]
    fn ranking_follows_the_client_order() {
        let eps = || {
            vec![
                ep(BrokerType::SecureMqtt, "a", "a.local.", &[], &[]),
                ep(BrokerType::Mqtt, "a", "a.local.", &[], &[]),
                ep(BrokerType::SecureMqtt, "b", "b.local.", &[], &[]),
                ep(BrokerType::Mqtt, "c", "c.local.", &[], &[]),
            ]
        };
        let urls = |order: &[BrokerType]| -> Vec<String> {
            rank_brokers(eps(), order)
                .iter()
                .map(BrokerEndpoint::url)
                .collect()
        };
        assert_eq!(
            urls(&[BrokerType::Mqtt, BrokerType::SecureMqtt]),
            [
                "mqtt://a.local:1883",
                "mqtt://c.local:1883",
                "mqtts://b.local:8883"
            ]
        );
        assert_eq!(
            urls(TCP_BROKER_TYPES),
            [
                "mqtts://a.local:8883",
                "mqtts://b.local:8883",
                "mqtt://c.local:1883"
            ]
        );
        assert_eq!(
            urls(&[BrokerType::Mqtt, BrokerType::SecureMqtt, BrokerType::Mqtt]),
            urls(&[BrokerType::Mqtt, BrokerType::SecureMqtt])
        );
        assert!(urls(&[]).is_empty());
    }

    #[test]
    fn wanted() {
        let e = ep(
            BrokerType::SecureMqtt,
            "Panel-A",
            "a.local.",
            &["device_id=p1, p2"],
            &[],
        );
        assert!(Wanted::Instance("panel-a".into()).matches(&e, &[]));
        assert!(Wanted::DeviceId("p2".into()).matches(&e, &[]));
        assert!(!Wanted::DeviceId("p".into()).matches(&e, &[]));
        assert!(!Wanted::Instance("panel".into()).matches(&e, &[]));
    }

    #[test]
    fn device_id_on_any_type_and_from_the_same_broker() {
        let p1 = Wanted::DeviceId("p1".into());
        // A plain record's own device_id.
        let plain = ep(BrokerType::Mqtt, "a", "a.local.", &["device_id=p1"], &[]);
        assert!(p1.matches(&plain, &[]));
        // The id on a sibling of the same SRV target, the sibling's TXT
        // `broker` naming another host.
        let bare = ep(BrokerType::Mqtt, "a", "A.local.", &[], &[]);
        let secure = ep(
            BrokerType::SecureMqtt,
            "a",
            "a.local",
            &["broker=cert.example.com", "device_id=p1"],
            &[],
        );
        let ws = ep(BrokerType::MqttWs, "w", "a.local.", &["device_id=p1"], &[]);
        assert!(!p1.matches(&bare, &[]));
        assert!(p1.matches(&bare, &[bare.clone(), secure.clone()]));
        assert!(p1.matches(&bare, core::slice::from_ref(&ws)));
        // A sibling by TXT `broker` name.
        let named = ep(
            BrokerType::Mqtt,
            "n",
            "n.local.",
            &["broker=cert.example.com"],
            &[],
        );
        assert!(p1.matches(&named, core::slice::from_ref(&secure)));
        // Another broker's id, and a different id on the same broker.
        let other = ep(
            BrokerType::SecureMqtt,
            "o",
            "o.local.",
            &["device_id=p1"],
            &[],
        );
        assert!(!p1.matches(&bare, core::slice::from_ref(&other)));
        assert!(!Wanted::DeviceId("p2".into()).matches(&bare, &[secure]));
        assert!(!Wanted::Instance("x".into()).matches(&bare, &[ws]));
    }
}
