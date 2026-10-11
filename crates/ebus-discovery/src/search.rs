//! The broker search state machine (_mdns_core.py:216-295; ebus.py:798-825).

use alloc::collections::BTreeSet;
use alloc::vec::Vec;
use core::fmt;

use crate::broker::{BrokerEndpoint, Choice, Wanted, choose, log_choice, rank_brokers};
use crate::instance::{InstanceKey, InstanceTable, Sighting};
use crate::{BROKER_TYPES, BrokerMode, BrokerType, Instant, TCP_BROKER_TYPES};

/// The fast requeries: 3 s apart, python's `RetrySchedule` (ebus.py:801-825).
const REQUERY_FAST_MS: u64 = 3_000;
/// Requeries after the fast ones.
const REQUERY_SLOW_MS: u64 = 30_000;
/// The number of browse attempts, the first included, before the slow interval.
const REQUERY_FAST_ATTEMPTS: u32 = 3;

/// How a host finds its broker, from its defaults overridden by its REST
/// "MQTT broker configuration" (framework.md:93, 607).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct BrokerConfig {
    /// The broker mode; `None`: `ConfiguredOnly` with a URL, else `DiscoveryOnly`.
    pub mode: Option<BrokerMode>,
    /// The configured broker.
    pub url: Option<BrokerEndpoint>,
    /// The particular discovered broker wanted; not with a URL.
    pub wanted: Option<Wanted>,
    /// TLS required; also implied by an `mqtts://` URL.
    pub tls: bool,
    /// The accepted broker types in the client's preference order, most
    /// preferred first; a type listed twice keeps its first position. `None`:
    /// `[SecureMqtt]` with TLS, else [`TCP_BROKER_TYPES`], both in the
    /// specification's order. A list is used as given whatever `tls` says, so
    /// a TLS client listing a plain type can select a plain broker.
    pub accept: Option<Vec<BrokerType>>,
    /// In `DiscoveryWithFallback` with a URL, accept a discovered broker that
    /// does not match it (ebus.py:776-781).
    pub allow_unmatched: bool,
    /// How long after the first usable broker is seen the result is set.
    pub settle_ms: u64,
    /// In `DiscoveryWithFallback` with a URL, how long after the start the
    /// configured broker is used when no discovered broker matched.
    pub fallback_after_ms: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            mode: None,
            url: None,
            wanted: None,
            tls: false,
            accept: None,
            allow_unmatched: false,
            settle_ms: 3_000,
            fallback_after_ms: 15_000,
        }
    }
}

impl BrokerConfig {
    /// The mode in effect: `mode`, else `ConfiguredOnly` with a URL, else
    /// `DiscoveryOnly`.
    pub fn effective_mode(&self) -> BrokerMode {
        self.mode.unwrap_or(if self.url.is_some() {
            BrokerMode::ConfiguredOnly
        } else {
            BrokerMode::DiscoveryOnly
        })
    }

    /// True when TLS is required: `tls`, or a TLS URL.
    pub fn requires_tls(&self) -> bool {
        self.tls || self.url.as_ref().is_some_and(|u| u.broker_type.uses_tls())
    }

    /// The broker types accepted, most preferred first: `accept` without
    /// repeats, else `_secure-mqtt._tcp` alone when TLS is required, else
    /// [`TCP_BROKER_TYPES`] (_mdns_core.py:247-253).
    pub fn accepted(&self) -> Vec<BrokerType> {
        match &self.accept {
            Some(a) => {
                let mut order = Vec::with_capacity(a.len());
                for t in a {
                    if !order.contains(t) {
                        order.push(*t);
                    }
                }
                order
            }
            None if self.requires_tls() => alloc::vec![BrokerType::SecureMqtt],
            None => TCP_BROKER_TYPES.to_vec(),
        }
    }

    /// The configuration errors [`BrokerSearch::new`] rejects.
    pub fn check(&self) -> Result<(), ConfigError> {
        if self.wanted.is_some() && self.url.is_some() {
            return Err(ConfigError::WantedWithUrl);
        }
        if self.effective_mode() == BrokerMode::ConfiguredOnly && self.url.is_none() {
            return Err(ConfigError::NoUrl);
        }
        Ok(())
    }

    /// The accepted brokers among `endpoints`, with `wanted` applied, ranked
    /// in the [`accepted`](Self::accepted) order. `endpoints` is every broker
    /// endpoint seen, of every type: [`Wanted::DeviceId`] reads the
    /// `device_id` of endpoints whose type is not accepted too.
    pub fn rank(&self, endpoints: impl IntoIterator<Item = BrokerEndpoint>) -> Vec<BrokerEndpoint> {
        let order = self.accepted();
        let sighted: Vec<BrokerEndpoint> = endpoints.into_iter().collect();
        let wanted = self.wanted.as_ref();
        rank_brokers(
            sighted
                .iter()
                .filter(|e| wanted.is_none_or(|w| w.matches(e, &sighted)))
                .cloned(),
            &order,
        )
    }
}

/// A broker configuration a search cannot run with.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ConfigError {
    /// `ConfiguredOnly` without a URL.
    NoUrl,
    /// `wanted` together with a URL.
    WantedWithUrl,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoUrl => "configured-only needs a broker URL",
            Self::WantedWithUrl => "a wanted broker cannot be combined with a broker URL",
        })
    }
}

impl core::error::Error for ConfigError {}

/// One broker search, with no I/O: the backend's sightings and the time go
/// in; requeries and, once, the result come out.
///
/// The browse covers every broker type, so a broker of a type not accepted is
/// logged with `reason=brokerTransportNotAccepted`, and a broker wanted by
/// device id is found under an accepted transport when only another of its
/// transports carries the id. The result is set `settle_ms` after a usable
/// broker is first seen, so each broker's every transport has been heard and
/// the one earliest in [`BrokerConfig::accepted`] is chosen; in
/// `DiscoveryWithFallback` with a URL, the configured broker becomes the
/// result `fallback_after_ms` after the start when no usable broker is
/// pending. While no usable broker is seen,
/// [`BrokerSearch::poll_requery`] asks for a requery 3 s, 6 s and 36 s after
/// the start, then every 30 s. Once set, the result never changes.
#[derive(Clone, Debug)]
pub struct BrokerSearch {
    cfg: BrokerConfig,
    mode: BrokerMode,
    table: InstanceTable,
    examined: BTreeSet<InstanceKey>,
    settle_at: Option<Instant>,
    fallback_at: Option<Instant>,
    requery_at: Option<Instant>,
    attempt: u32,
    requery: bool,
    result: Option<BrokerEndpoint>,
}

impl BrokerSearch {
    /// A search starting at `now`. In `ConfiguredOnly` the result is the
    /// configured broker at once.
    pub fn new(now: Instant, cfg: BrokerConfig) -> Result<Self, ConfigError> {
        cfg.check()?;
        let mode = cfg.effective_mode();
        let result = match mode {
            BrokerMode::ConfiguredOnly => cfg.url.clone(),
            _ => None,
        };
        let browses = result.is_none();
        let fallback_at =
            (browses && mode == BrokerMode::DiscoveryWithFallback && cfg.url.is_some())
                .then(|| now.add_ms(cfg.fallback_after_ms));
        Ok(Self {
            mode,
            table: InstanceTable::new(),
            examined: BTreeSet::new(),
            settle_at: None,
            fallback_at,
            requery_at: browses.then(|| now.add_ms(REQUERY_FAST_MS)),
            attempt: 0,
            requery: false,
            result,
            cfg,
        })
    }

    /// False in `ConfiguredOnly`: no browse runs.
    pub fn browses(&self) -> bool {
        self.mode != BrokerMode::ConfiguredOnly
    }

    /// The mode in effect.
    pub fn mode(&self) -> BrokerMode {
        self.mode
    }

    /// The service types to browse: every broker type.
    pub fn browse_types(&self) -> [BrokerType; 4] {
        BROKER_TYPES
    }

    /// Apply a backend report.
    pub fn handle(&mut self, now: Instant, s: Sighting) {
        if !self.table.handle(s) || self.result.is_some() {
            return;
        }
        self.log_not_accepted();
        match self.choice() {
            Choice::Discovered(..) => {
                if self.settle_at.is_none() {
                    self.settle_at = Some(now.add_ms(self.cfg.settle_ms));
                }
            }
            Choice::Configured | Choice::None => self.settle_at = None,
        }
    }

    /// Advance to `now`: settle, fall back or schedule a requery when due.
    pub fn handle_timeout(&mut self, now: Instant) {
        if self.result.is_some() {
            return;
        }
        if let Some(at) = self.settle_at {
            if now < at {
                return;
            }
            self.settle_at = None;
            let ranked = self.candidates();
            let choice = choose(
                self.mode,
                self.cfg.url.as_ref(),
                &ranked,
                self.cfg.allow_unmatched,
            );
            if let Choice::Discovered(_, ep) = &choice {
                log_choice(&choice, self.cfg.url.as_ref(), &ranked);
                crate::log::info!(
                    "brokerDiscovered",
                    url = ep.url(),
                    attempt = self.attempt + 1
                );
                self.result = Some(ep.clone());
                return;
            }
        }
        if let (Some(at), Some(url)) = (self.fallback_at, &self.cfg.url) {
            if now >= at {
                crate::log::info!("brokerFallbackToConfigured", url = url.url());
                self.result = Some(url.clone());
                return;
            }
        }
        if let Some(at) = self.requery_at {
            if now >= at {
                if self.candidates().is_empty() {
                    crate::log::info!("brokerNotFound", attempt = self.attempt + 1);
                }
                self.attempt += 1;
                self.requery = true;
                let next = if self.attempt + 1 < REQUERY_FAST_ATTEMPTS {
                    REQUERY_FAST_MS
                } else {
                    REQUERY_SLOW_MS
                };
                self.requery_at = Some(now.add_ms(next));
            }
        }
    }

    /// True once per requery the search asks the backend for.
    pub fn poll_requery(&mut self) -> bool {
        core::mem::take(&mut self.requery)
    }

    /// The next instant [`BrokerSearch::handle_timeout`] has work at.
    pub fn poll_timeout(&self) -> Option<Instant> {
        if self.result.is_some() {
            return None;
        }
        if self.settle_at.is_some() {
            return self.settle_at;
        }
        match (self.fallback_at, self.requery_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// The broker chosen, set once and then fixed.
    pub fn result(&self) -> Option<&BrokerEndpoint> {
        self.result.as_ref()
    }

    /// The accepted brokers now seen, with `wanted` applied, ranked.
    pub fn candidates(&self) -> Vec<BrokerEndpoint> {
        self.cfg.rank(
            self.table
                .instances()
                .filter_map(|i| BrokerEndpoint::from_instance(&i)),
        )
    }

    fn choice(&self) -> Choice {
        choose(
            self.mode,
            self.cfg.url.as_ref(),
            &self.candidates(),
            self.cfg.allow_unmatched,
        )
    }

    /// Log each newly seen broker whose type is not accepted and whose host
    /// offers no accepted type (_mdns_core.py:270-278).
    fn log_not_accepted(&mut self) {
        let accept = self.cfg.accepted();
        let mut usable: Option<Vec<BrokerEndpoint>> = None;
        for inst in self.table.instances() {
            if self.examined.contains(&inst.key) {
                continue;
            }
            self.examined.insert(inst.key.clone());
            let Some(ep) = BrokerEndpoint::from_instance(&inst) else {
                continue;
            };
            if accept.contains(&ep.broker_type) {
                continue;
            }
            let usable = usable.get_or_insert_with(|| {
                self.cfg.rank(
                    self.table
                        .instances()
                        .filter_map(|i| BrokerEndpoint::from_instance(&i)),
                )
            });
            if !usable.iter().any(|u| crate::same_host(&u.host, &ep.host)) {
                crate::log::info!(
                    "brokerTransportNotAccepted",
                    url = ep.url(),
                    accept = accept
                        .iter()
                        .map(|t| t.as_str())
                        .collect::<Vec<_>>()
                        .join("|")
                );
            }
        }
    }
}

#[cfg(test)]
// The tests set fields one by one, as a host must for a `#[non_exhaustive]` struct.
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::addr::ScopedAddr;
    use crate::instance::Slot;
    use crate::txt::TxtRecord;
    use alloc::string::String;
    use alloc::vec;

    fn seen(t: BrokerType, name: &str, server: &str, txt: &[&str], addrs: &[&str]) -> Sighting {
        Sighting::Seen {
            key: InstanceKey {
                service_type: t.as_str().into(),
                name: name.into(),
            },
            slot: Slot::Whole,
            server: server.into(),
            port: 0,
            txt: TxtRecord::from_strings(txt),
            addrs: addrs
                .iter()
                .map(|a| ScopedAddr::new(a.parse().unwrap()))
                .collect(),
        }
    }

    fn gone(t: BrokerType, name: &str) -> Sighting {
        Sighting::Gone {
            key: InstanceKey {
                service_type: t.as_str().into(),
                name: name.into(),
            },
            slot: None,
        }
    }

    fn search(cfg: BrokerConfig) -> BrokerSearch {
        BrokerSearch::new(Instant(0), cfg).unwrap()
    }

    fn url(u: &str) -> Option<BrokerEndpoint> {
        Some(BrokerEndpoint::from_url(u).unwrap())
    }

    fn result(s: &BrokerSearch) -> Option<String> {
        s.result().map(BrokerEndpoint::url)
    }

    /// Run timeouts up to `until`, returning the instants requeries were asked at.
    fn run(s: &mut BrokerSearch, until: u64) -> Vec<u64> {
        let mut requeries = Vec::new();
        while let Some(at) = s.poll_timeout() {
            if at.0 > until {
                break;
            }
            s.handle_timeout(at);
            if s.poll_requery() {
                requeries.push(at.0);
            }
        }
        requeries
    }

    #[test]
    fn mode_precedence() {
        let s = search(BrokerConfig::default());
        assert_eq!(s.mode(), BrokerMode::DiscoveryOnly);
        assert!(s.browses());
        let mut cfg = BrokerConfig::default();
        cfg.url = url("mqtt://b.local");
        let s = search(cfg.clone());
        assert_eq!(s.mode(), BrokerMode::ConfiguredOnly);
        assert!(!s.browses());
        assert_eq!(result(&s).as_deref(), Some("mqtt://b.local:1883"));
        assert_eq!(s.poll_timeout(), None);
        cfg.mode = Some(BrokerMode::DiscoveryOnly);
        assert_eq!(search(cfg).mode(), BrokerMode::DiscoveryOnly);
    }

    #[test]
    fn config_errors() {
        let mut cfg = BrokerConfig::default();
        cfg.mode = Some(BrokerMode::ConfiguredOnly);
        assert_eq!(
            BrokerSearch::new(Instant(0), cfg).err(),
            Some(ConfigError::NoUrl)
        );
        for mode in [
            None,
            Some(BrokerMode::DiscoveryWithFallback),
            Some(BrokerMode::DiscoveryOnly),
        ] {
            let mut cfg = BrokerConfig::default();
            cfg.mode = mode;
            cfg.url = url("mqtt://b.local");
            cfg.wanted = Some(Wanted::Instance("b".into()));
            assert_eq!(
                BrokerSearch::new(Instant(0), cfg).err(),
                Some(ConfigError::WantedWithUrl)
            );
        }
    }

    #[test]
    fn discovery_only_settles_then_picks_first() {
        let mut s = search(BrokerConfig::default());
        s.handle(
            Instant(100),
            seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        );
        s.handle(
            Instant(200),
            seen(BrokerType::Mqtt, "a", "a.local.", &[], &[]),
        );
        assert_eq!(s.poll_timeout(), Some(Instant(3_100)));
        s.handle_timeout(Instant(3_099));
        assert_eq!(s.result(), None);
        assert_eq!(run(&mut s, 3_100), []);
        assert_eq!(result(&s).as_deref(), Some("mqtt://a.local:1883"));
        assert_eq!(s.poll_timeout(), None);
    }

    #[test]
    fn settle_lets_the_secure_transport_win() {
        let mut s = search(BrokerConfig::default());
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        );
        s.handle(
            Instant(2_000),
            seen(BrokerType::SecureMqtt, "b", "b.local.", &[], &[]),
        );
        assert_eq!(s.poll_timeout(), Some(Instant(3_000)));
        run(&mut s, 3_000);
        assert_eq!(result(&s).as_deref(), Some("mqtts://b.local:8883"));
        assert_eq!(s.candidates().len(), 1);
    }

    fn plain_first() -> BrokerConfig {
        let mut cfg = BrokerConfig::default();
        cfg.accept = Some(vec![BrokerType::Mqtt, BrokerType::SecureMqtt]);
        cfg
    }

    #[test]
    fn client_order_picks_the_plain_transport_of_a_dual_transport_broker() {
        let mut s = search(plain_first());
        s.handle(
            Instant(0),
            seen(BrokerType::SecureMqtt, "b", "b.local.", &[], &[]),
        );
        s.handle(
            Instant(2_000),
            seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        );
        assert_eq!(s.poll_timeout(), Some(Instant(3_000)));
        run(&mut s, 3_000);
        assert_eq!(result(&s).as_deref(), Some("mqtt://b.local:1883"));
        assert_eq!(s.candidates().len(), 1);
    }

    #[test]
    fn client_order_picks_the_plain_broker_of_two() {
        let mut s = search(plain_first());
        s.handle(
            Instant(0),
            seen(BrokerType::SecureMqtt, "a", "a.local.", &[], &[]),
        );
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "z", "z.local.", &[], &[]),
        );
        run(&mut s, 3_000);
        assert_eq!(result(&s).as_deref(), Some("mqtt://z.local:1883"));
        let mut s = search(BrokerConfig::default());
        s.handle(
            Instant(0),
            seen(BrokerType::SecureMqtt, "a", "a.local.", &[], &[]),
        );
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "z", "z.local.", &[], &[]),
        );
        run(&mut s, 3_000);
        assert_eq!(result(&s).as_deref(), Some("mqtts://a.local:8883"));
    }

    #[test]
    fn accepted_keeps_the_first_of_a_repeated_type() {
        let mut cfg = BrokerConfig::default();
        cfg.accept = Some(vec![
            BrokerType::Mqtt,
            BrokerType::SecureMqtt,
            BrokerType::Mqtt,
        ]);
        assert_eq!(cfg.accepted(), [BrokerType::Mqtt, BrokerType::SecureMqtt]);
        assert_eq!(BrokerConfig::default().accepted(), TCP_BROKER_TYPES);
    }

    #[test]
    fn result_stays_fixed() {
        let mut s = search(BrokerConfig::default());
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        );
        run(&mut s, 3_000);
        s.handle(
            Instant(4_000),
            seen(BrokerType::SecureMqtt, "a", "a.local.", &[], &[]),
        );
        s.handle(Instant(4_000), gone(BrokerType::Mqtt, "b"));
        run(&mut s, 100_000);
        assert_eq!(result(&s).as_deref(), Some("mqtt://b.local:1883"));
        assert_eq!(s.candidates()[0].host, "a.local");
    }

    #[test]
    fn repeated_sightings_do_not_extend_the_settle_window() {
        let mut s = search(BrokerConfig::default());
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        );
        s.handle(
            Instant(2_500),
            seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        );
        s.handle(
            Instant(2_600),
            seen(BrokerType::Mqtt, "c", "c.local.", &[], &[]),
        );
        assert_eq!(s.poll_timeout(), Some(Instant(3_000)));
    }

    #[test]
    fn requery_schedule_while_nothing_is_found() {
        let mut s = search(BrokerConfig::default());
        assert_eq!(run(&mut s, 100_000), [3_000, 6_000, 36_000, 66_000, 96_000]);
        assert!(!s.poll_requery());
    }

    #[test]
    fn broker_gone_while_settling_resumes_requeries() {
        let mut s = search(BrokerConfig::default());
        s.handle(
            Instant(1_000),
            seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        );
        s.handle(Instant(3_500), gone(BrokerType::Mqtt, "b"));
        assert_eq!(s.poll_timeout(), Some(Instant(3_000)));
        assert_eq!(run(&mut s, 7_000), [3_000, 6_000]);
        assert_eq!(s.result(), None);
    }

    #[test]
    fn tls_narrows_accept() {
        let mut cfg = BrokerConfig::default();
        cfg.tls = true;
        let mut s = search(cfg);
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "p", "p.local.", &[], &[]),
        );
        assert!(s.candidates().is_empty());
        assert_eq!(s.poll_timeout(), Some(Instant(3_000)));
        s.handle(
            Instant(1_000),
            seen(BrokerType::SecureMqtt, "s", "s.local.", &[], &[]),
        );
        run(&mut s, 4_000);
        assert_eq!(result(&s).as_deref(), Some("mqtts://s.local:8883"));

        let mut cfg = BrokerConfig::default();
        cfg.mode = Some(BrokerMode::DiscoveryWithFallback);
        cfg.url = url("mqtts://x.local");
        assert_eq!(cfg.accepted(), [BrokerType::SecureMqtt]);
        cfg.url = url("mqtt://x.local");
        assert_eq!(cfg.accepted(), TCP_BROKER_TYPES);
        cfg.accept = Some(vec![BrokerType::MqttWs]);
        assert_eq!(cfg.accepted(), [BrokerType::MqttWs]);
        cfg.tls = true;
        cfg.accept = Some(vec![BrokerType::Mqtt, BrokerType::SecureMqtt]);
        assert_eq!(cfg.accepted(), [BrokerType::Mqtt, BrokerType::SecureMqtt]);
    }

    #[test]
    fn websocket_brokers_are_not_selected_by_default() {
        let mut s = search(BrokerConfig::default());
        s.handle(
            Instant(0),
            seen(BrokerType::MqttWss, "w", "w.local.", &[], &[]),
        );
        assert!(s.candidates().is_empty());
        assert_eq!(s.poll_timeout(), Some(Instant(3_000)));
    }

    #[test]
    fn wanted_by_instance_and_device_id() {
        for (wanted, expect) in [
            (Wanted::Instance("B".into()), "mqtt://b.local:1883"),
            (Wanted::DeviceId("d2".into()), "mqtts://c.local:8883"),
        ] {
            let mut cfg = BrokerConfig::default();
            cfg.wanted = Some(wanted);
            let mut s = search(cfg);
            s.handle(
                Instant(0),
                seen(BrokerType::Mqtt, "a", "a.local.", &[], &[]),
            );
            s.handle(
                Instant(0),
                seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
            );
            s.handle(
                Instant(0),
                seen(
                    BrokerType::SecureMqtt,
                    "c",
                    "c.local.",
                    &["device_id=d1,d2"],
                    &[],
                ),
            );
            run(&mut s, 3_000);
            assert_eq!(result(&s).as_deref(), Some(expect));
        }
    }

    fn device_id_cfg(id: &str, accept: &[BrokerType]) -> BrokerConfig {
        let mut cfg = BrokerConfig::default();
        cfg.wanted = Some(Wanted::DeviceId(id.into()));
        cfg.accept = Some(accept.to_vec());
        cfg
    }

    #[test]
    fn device_id_finds_a_plain_broker_by_its_secure_sibling() {
        let mut s = search(device_id_cfg("p1", &[BrokerType::Mqtt]));
        s.handle(
            Instant(0),
            seen(
                BrokerType::SecureMqtt,
                "c",
                "c.local.",
                &["broker=cert.example.com", "device_id=p1"],
                &[],
            ),
        );
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "other", "o.local.", &[], &[]),
        );
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "c", "c.local.", &[], &["10.0.0.5"]),
        );
        run(&mut s, 3_000);
        assert_eq!(result(&s).as_deref(), Some("mqtt://c.local:1883"));
    }

    #[test]
    fn device_id_on_a_plain_record() {
        let mut s = search(device_id_cfg("p1", &[BrokerType::Mqtt]));
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "a", "a.local.", &[], &[]),
        );
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "p", "p.local.", &["device_id=p0,p1"], &[]),
        );
        run(&mut s, 3_000);
        assert_eq!(result(&s).as_deref(), Some("mqtt://p.local:1883"));
    }

    #[test]
    fn device_id_on_a_websocket_sibling() {
        let mut s = search(device_id_cfg("p1", &[BrokerType::Mqtt]));
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "c", "c.local.", &[], &[]),
        );
        s.handle(
            Instant(0),
            seen(BrokerType::MqttWs, "c", "c.local.", &["device_id=p1"], &[]),
        );
        run(&mut s, 3_000);
        assert_eq!(result(&s).as_deref(), Some("mqtt://c.local:1883"));
    }

    #[test]
    fn device_id_not_listed_keeps_searching() {
        let mut s = search(device_id_cfg("p9", &[BrokerType::Mqtt]));
        s.handle(
            Instant(0),
            seen(
                BrokerType::SecureMqtt,
                "c",
                "c.local.",
                &["device_id=p1"],
                &[],
            ),
        );
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "c", "c.local.", &[], &[]),
        );
        assert_eq!(run(&mut s, 7_000), [3_000, 6_000]);
        assert_eq!(s.result(), None);
    }

    #[test]
    fn device_id_never_selects_a_type_not_accepted() {
        let mut s = search(device_id_cfg("p1", &[BrokerType::Mqtt]));
        s.handle(
            Instant(0),
            seen(
                BrokerType::SecureMqtt,
                "c",
                "c.local.",
                &["device_id=p1"],
                &[],
            ),
        );
        // No settle window opens: the next timeout is the first requery.
        assert_eq!(s.poll_timeout(), Some(Instant(3_000)));
        assert!(s.candidates().is_empty());
        assert_eq!(run(&mut s, 7_000), [3_000, 6_000]);
        assert_eq!(s.result(), None);
        // The accepted sibling arrives later and opens the window then.
        s.handle(
            Instant(7_000),
            seen(BrokerType::Mqtt, "c", "c.local.", &[], &[]),
        );
        assert_eq!(s.poll_timeout(), Some(Instant(10_000)));
        run(&mut s, 10_000);
        assert_eq!(result(&s).as_deref(), Some("mqtt://c.local:1883"));
    }

    #[test]
    fn device_id_sibling_opens_the_settle_window_when_it_arrives() {
        let mut s = search(device_id_cfg("p1", &[BrokerType::Mqtt]));
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "c", "c.local.", &[], &[]),
        );
        assert!(s.candidates().is_empty());
        assert_eq!(s.poll_timeout(), Some(Instant(3_000)));
        s.handle(
            Instant(1_000),
            seen(
                BrokerType::SecureMqtt,
                "c",
                "c.local.",
                &["device_id=p1"],
                &[],
            ),
        );
        assert_eq!(s.poll_timeout(), Some(Instant(4_000)));
        assert_eq!(run(&mut s, 4_000), []);
        assert_eq!(result(&s).as_deref(), Some("mqtt://c.local:1883"));
    }

    #[test]
    fn device_id_finds_the_secure_record_of_a_dual_transport_broker() {
        let mut cfg = BrokerConfig::default();
        cfg.wanted = Some(Wanted::DeviceId("d1".into()));
        let mut s = search(cfg);
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "c", "c.local.", &[], &[]),
        );
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "plain-only", "p.local.", &[], &[]),
        );
        s.handle(
            Instant(0),
            seen(
                BrokerType::SecureMqtt,
                "c",
                "c.local.",
                &["device_id=d1"],
                &[],
            ),
        );
        run(&mut s, 3_000);
        assert_eq!(result(&s).as_deref(), Some("mqtts://c.local:8883"));
    }

    #[test]
    fn wanted_absent_keeps_searching() {
        let mut cfg = BrokerConfig::default();
        cfg.mode = Some(BrokerMode::DiscoveryWithFallback);
        cfg.wanted = Some(Wanted::Instance("z".into()));
        let mut s = search(cfg);
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "a", "a.local.", &[], &[]),
        );
        assert_eq!(run(&mut s, 40_000), [3_000, 6_000, 36_000]);
        assert_eq!(s.result(), None);
    }

    fn fallback_cfg(u: &str) -> BrokerConfig {
        let mut cfg = BrokerConfig::default();
        cfg.mode = Some(BrokerMode::DiscoveryWithFallback);
        cfg.url = url(u);
        cfg
    }

    #[test]
    fn fallback_to_configured() {
        let mut s = search(fallback_cfg("mqtt://x.local"));
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "a", "a.local.", &[], &[]),
        );
        assert_eq!(run(&mut s, 14_999), [3_000, 6_000]);
        assert_eq!(s.result(), None);
        run(&mut s, 15_000);
        assert_eq!(result(&s).as_deref(), Some("mqtt://x.local:1883"));
    }

    #[test]
    fn allow_unmatched() {
        let mut cfg = fallback_cfg("mqtt://x.local");
        cfg.allow_unmatched = true;
        let mut s = search(cfg);
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "a", "a.local.", &[], &[]),
        );
        run(&mut s, 3_000);
        assert_eq!(result(&s).as_deref(), Some("mqtt://a.local:1883"));
    }

    #[test]
    fn fallback_matches_by_name_and_address() {
        let mut s = search(fallback_cfg("mqtt://192.168.1.8"));
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "a", "a.local.", &[], &["192.168.1.7"]),
        );
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "p", "p.local.", &[], &["192.168.1.8"]),
        );
        run(&mut s, 3_000);
        let r = s.result().unwrap();
        assert_eq!(
            (r.host.as_str(), r.instance.as_deref()),
            ("192.168.1.8", Some("p"))
        );

        let mut s = search(fallback_cfg("mqtts://192.168.1.8:9999"));
        s.handle(
            Instant(0),
            seen(
                BrokerType::SecureMqtt,
                "s",
                "s.local.",
                &[],
                &["192.168.1.8"],
            ),
        );
        run(&mut s, 3_000);
        let r = s.result().unwrap();
        assert_eq!((r.host.as_str(), r.port), ("s.local", 8883));

        let mut s = search(fallback_cfg("mqtt://B.LOCAL"));
        s.handle(
            Instant(0),
            seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        );
        run(&mut s, 3_000);
        assert_eq!(s.result().unwrap().instance.as_deref(), Some("b"));
    }

    #[test]
    fn a_pending_settle_outlasts_the_fallback_deadline() {
        let mut s = search(fallback_cfg("mqtt://b.local"));
        s.handle(
            Instant(14_000),
            seen(BrokerType::Mqtt, "b", "b.local.", &[], &[]),
        );
        assert_eq!(s.poll_timeout(), Some(Instant(17_000)));
        run(&mut s, 17_000);
        assert_eq!(s.result().unwrap().instance.as_deref(), Some("b"));
    }

    #[test]
    fn device_id_merges_transports_across_hosts() {
        let mut s = search(BrokerConfig::default());
        s.handle(
            Instant(0),
            seen(
                BrokerType::SecureMqtt,
                "x",
                "x.local.",
                &["broker=cert.example.com", "device_id=p1"],
                &[],
            ),
        );
        s.handle(
            Instant(0),
            seen(
                BrokerType::SecureMqtt,
                "y",
                "y.local.",
                &["device_id=p1"],
                &[],
            ),
        );
        assert_eq!(s.candidates().len(), 1);
    }
}
