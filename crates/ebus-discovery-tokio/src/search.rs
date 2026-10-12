//! Listing, finding and re-resolving brokers over a [`Backend`].

use core::future::Future;
use core::task::Poll;
use std::time::Duration;

use ebus_discovery::{
    BROKER_TYPES, BrokerConfig, BrokerEndpoint, BrokerSearch, BrokerType, Instance, InstanceKey,
    InstanceTable, Instant, Sighting, Wanted, is_local_name, same_host,
};
use tokio::time::sleep_until;

use crate::{Backend, Browse, Error};

/// The core's millisecond [`Instant`]s, counted from the start of one call.
struct Clock(tokio::time::Instant);

impl Clock {
    fn new() -> Self {
        Self(tokio::time::Instant::now())
    }

    fn now(&self) -> Instant {
        Instant(u64::try_from(self.0.elapsed().as_millis()).unwrap_or(u64::MAX))
    }

    fn at(&self, t: Instant) -> tokio::time::Instant {
        self.0 + Duration::from_millis(t.0)
    }
}

/// Several browses read as one stream.
struct Browses<Br> {
    items: Vec<Option<Br>>,
}

impl<Br: Browse> Browses<Br> {
    async fn open<B>(b: &B, types: &[BrokerType]) -> Result<Self, Error>
    where
        B: Backend<Browse = Br>,
    {
        let mut items = Vec::with_capacity(types.len());
        for t in types {
            items.push(Some(b.browse(t.as_str()).await?));
        }
        Ok(Self { items })
    }

    /// The next sighting of any browse; `None` once every browse has ended.
    async fn next(&mut self) -> Option<Sighting> {
        loop {
            let mut futs: Vec<_> = self
                .items
                .iter_mut()
                .enumerate()
                .filter_map(|(i, b)| b.as_mut().map(|b| (i, Box::pin(b.next()))))
                .collect();
            if futs.is_empty() {
                return None;
            }
            let (i, s) = core::future::poll_fn(|cx| {
                for (i, f) in &mut futs {
                    if let Poll::Ready(s) = f.as_mut().poll(cx) {
                        return Poll::Ready((*i, s));
                    }
                }
                Poll::Pending
            })
            .await;
            drop(futs);
            match s {
                Some(s) => return Some(s),
                None => self.items[i] = None,
            }
        }
    }

    async fn requery(&mut self) {
        for b in self.items.iter_mut().flatten() {
            if let Err(e) = b.requery().await {
                log::warn!("reason=requeryFailed,error={e}");
            }
        }
    }
}

async fn sleep_until_opt(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => core::future::pending().await,
    }
}

/// Browse for `window` from the start and return the accepted brokers, with
/// `cfg.wanted` applied, ranked ([`BrokerConfig::rank`]). Each broker appears
/// once, under the transport earliest in [`BrokerConfig::accepted`]. The
/// browse covers the accepted types, and every broker type when `cfg.wanted`
/// is a [`Wanted::DeviceId`], whose id another transport of the broker may
/// carry. A configuration [`BrokerSearch::new`] rejects is an
/// [`Error::Config`].
pub async fn list_brokers<B: Backend>(
    b: &B,
    cfg: &BrokerConfig,
    window: Duration,
) -> Result<Vec<BrokerEndpoint>, Error> {
    cfg.check()?;
    let types = match cfg.wanted {
        Some(Wanted::DeviceId(_)) => BROKER_TYPES.to_vec(),
        _ => cfg.accepted(),
    };
    let mut browses = Browses::open(b, &types).await?;
    let mut table = InstanceTable::new();
    let end = tokio::time::Instant::now() + window;
    loop {
        tokio::select! {
            s = browses.next() => match s {
                Some(s) => {
                    table.handle(s);
                }
                None => return Err(Error::Closed),
            },
            () = sleep_until(end) => break,
        }
    }
    Ok(cfg.rank(
        table
            .instances()
            .filter_map(|i| BrokerEndpoint::from_instance(&i)),
    ))
}

/// Run a [`BrokerSearch`] to its result: the configured broker at once in
/// `configured-only`, else the broker the search settles on, or the
/// configured broker when `discovery-with-fallback` finds no match. The
/// browses stop once the result is set. A search that finds nothing runs
/// until the caller stops it, for instance with `tokio::time::timeout`.
pub async fn find_broker<B: Backend>(b: &B, cfg: BrokerConfig) -> Result<BrokerEndpoint, Error> {
    let clock = Clock::new();
    let mut search = BrokerSearch::new(clock.now(), cfg)?;
    if let Some(r) = search.result() {
        return Ok(r.clone());
    }
    let mut browses = Browses::open(b, search.browse_types()).await?;
    loop {
        if let Some(r) = search.result() {
            return Ok(r.clone());
        }
        if search.poll_requery() {
            browses.requery().await;
        }
        let deadline = search.poll_timeout().map(|t| clock.at(t));
        tokio::select! {
            s = browses.next() => match s {
                Some(s) => search.handle(clock.now(), s),
                None => return Err(Error::Closed),
            },
            () = sleep_until_opt(deadline) => search.handle_timeout(clock.now()),
        }
    }
}

/// The same broker looked up again by its instance name and type, with its
/// current addresses, after its host's address changed. Returns once the
/// broker is seen with an IPv4 candidate address, or with any address when
/// its host is not a `.local` name; otherwise at the end of `window`, with
/// the latest sighting. `Ok(None)` for a configured broker, for one not seen
/// within `window`, and for one `cfg` no longer accepts.
pub async fn reresolve<B: Backend>(
    b: &B,
    broker: &BrokerEndpoint,
    cfg: &BrokerConfig,
    window: Duration,
) -> Result<Option<BrokerEndpoint>, Error> {
    let Some(name) = broker.instance.as_deref() else {
        return Ok(None);
    };
    let service_type = broker.broker_type.as_str();
    let mut browse = b.browse(service_type).await?;
    let mut table = InstanceTable::new();
    let end = tokio::time::Instant::now() + window;
    let mut found = None;
    loop {
        tokio::select! {
            s = browse.next() => match s {
                Some(s) => {
                    if !is_instance(&s, service_type, name) || !table.handle(s) {
                        continue;
                    }
                    found = table
                        .instances()
                        .find(|i| key_is(&i.key, service_type, name))
                        .as_ref()
                        .and_then(endpoint_in(cfg));
                    if found.as_ref().is_some_and(ready) {
                        break;
                    }
                }
                None => return Err(Error::Closed),
            },
            () = sleep_until(end) => break,
        }
    }
    Ok(found)
}

/// The endpoint of `i` when `cfg` accepts its type. `wanted` is not applied:
/// the broker was chosen already, and a device id it was found by may be
/// carried by another of its transports.
fn endpoint_in(cfg: &BrokerConfig) -> impl Fn(&Instance) -> Option<BrokerEndpoint> + '_ {
    move |i| {
        let ep = BrokerEndpoint::from_instance(i)?;
        cfg.accepted().contains(&ep.broker_type).then_some(ep)
    }
}

/// True when `ep` has an address [`BrokerEndpoint::connect_host`] uses.
fn ready(ep: &BrokerEndpoint) -> bool {
    !is_local_name(&ep.host) || ep.addrs.iter().any(|a| a.ip.is_ipv4())
}

fn key_is(key: &InstanceKey, service_type: &str, name: &str) -> bool {
    key.service_type.eq_ignore_ascii_case(service_type) && same_host(&key.name, name)
}

fn is_instance(s: &Sighting, service_type: &str, name: &str) -> bool {
    match s {
        Sighting::Seen { key, .. } | Sighting::Gone { key, .. } => key_is(key, service_type, name),
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use ebus_discovery::{BrokerMode, ScopedAddr, Slot, TxtRecord};

    use super::*;
    use crate::fake::Fake;

    fn seen(ty: BrokerType, name: &str, server: &str, port: u16, addrs: &[&str]) -> Sighting {
        Sighting::Seen {
            key: InstanceKey {
                service_type: ty.as_str().into(),
                name: name.into(),
            },
            slot: Slot::Whole,
            server: server.into(),
            port,
            txt: TxtRecord::from_strings(["txtvers=1"]),
            addrs: addrs
                .iter()
                .map(|a| ScopedAddr::new(a.parse::<IpAddr>().unwrap()))
                .collect(),
        }
    }

    /// Let spawned work and the code under test run up to its next wait.
    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    /// A spawned `find_broker`'s result, failing the test when it is not
    /// found within the settle window.
    async fn found(
        search: tokio::task::JoinHandle<Result<BrokerEndpoint, Error>>,
    ) -> BrokerEndpoint {
        tokio::time::timeout(Duration::from_secs(20), search)
            .await
            .expect("found within the settle window")
            .unwrap()
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn find_broker_waits_for_the_preferred_transport() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let search = tokio::spawn(async move { find_broker(&f, BrokerConfig::default()).await });
        settle().await;
        assert_eq!(
            fake.opened(),
            [
                "_secure-mqtt._tcp",
                "_mqtt-wss._tcp",
                "_mqtt-ws._tcp",
                "_mqtt._tcp"
            ]
        );
        fake.emit(seen(BrokerType::Mqtt, "b", "b.local.", 1883, &["10.0.0.5"]));
        tokio::time::sleep(Duration::from_millis(2_900)).await;
        assert!(!search.is_finished());
        fake.emit(seen(
            BrokerType::SecureMqtt,
            "b",
            "b.local.",
            8883,
            &["10.0.0.5"],
        ));
        let ep = found(search).await;
        assert_eq!(ep.broker_type, BrokerType::SecureMqtt);
        assert_eq!(ep.connect_host(), "10.0.0.5");
    }

    #[tokio::test(start_paused = true)]
    async fn find_broker_configured_only_does_not_browse() {
        let fake = Fake::new("me.local.");
        let mut cfg = BrokerConfig::default();
        cfg.url = Some(BrokerEndpoint::from_url("mqtt://broker.example").unwrap());
        let ep = find_broker(&fake, cfg).await.unwrap();
        assert_eq!(ep.url(), "mqtt://broker.example:1883");
        assert!(fake.opened().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn find_broker_falls_back_and_requeries() {
        let fake = Fake::new("me.local.");
        let mut cfg = BrokerConfig::default();
        cfg.mode = Some(BrokerMode::DiscoveryWithFallback);
        cfg.url = Some(BrokerEndpoint::from_url("mqtt://broker.example").unwrap());
        let start = tokio::time::Instant::now();
        let ep = find_broker(&fake, cfg).await.unwrap();
        assert_eq!(start.elapsed(), Duration::from_millis(15_000));
        assert_eq!(ep.host, "broker.example");
        // Requeries at 3 s and 6 s, each of the four browses.
        assert_eq!(fake.requeries().len(), 8);
    }

    #[tokio::test(start_paused = true)]
    async fn find_broker_fails_when_the_backend_stops() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let search = tokio::spawn(async move { find_broker(&f, BrokerConfig::default()).await });
        settle().await;
        fake.close_browses();
        assert!(matches!(search.await.unwrap(), Err(Error::Closed)));
    }

    #[tokio::test(start_paused = true)]
    async fn list_brokers_ranks_the_accepted() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let cfg = BrokerConfig::default();
        let list =
            tokio::spawn(async move { list_brokers(&f, &cfg, Duration::from_secs(1)).await });
        settle().await;
        assert_eq!(fake.opened(), ["_secure-mqtt._tcp", "_mqtt._tcp"]);
        fake.emit(seen(BrokerType::Mqtt, "z", "z.local.", 1883, &["10.0.0.9"]));
        fake.emit(seen(BrokerType::Mqtt, "a", "a.local.", 1883, &["10.0.0.8"]));
        fake.emit(seen(
            BrokerType::SecureMqtt,
            "z",
            "z.local.",
            8883,
            &["10.0.0.9"],
        ));
        let got = list.await.unwrap().unwrap();
        let got: Vec<(BrokerType, &str)> = got
            .iter()
            .map(|e| (e.broker_type, e.instance.as_deref().unwrap()))
            .collect();
        assert_eq!(
            got,
            [(BrokerType::SecureMqtt, "z"), (BrokerType::Mqtt, "a")]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn list_brokers_applies_wanted() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let mut cfg = BrokerConfig::default();
        cfg.wanted = Some(Wanted::Instance("A".into()));
        let list =
            tokio::spawn(async move { list_brokers(&f, &cfg, Duration::from_secs(1)).await });
        settle().await;
        fake.emit(seen(BrokerType::Mqtt, "z", "z.local.", 1883, &[]));
        fake.emit(seen(BrokerType::Mqtt, "a", "a.local.", 1883, &[]));
        let got = list.await.unwrap().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].instance.as_deref(), Some("a"));
    }

    fn with_txt(mut s: Sighting, kv: &str) -> Sighting {
        if let Sighting::Seen { txt, .. } = &mut s {
            let (k, v) = kv.split_once('=').unwrap();
            txt.insert(k, v);
        }
        s
    }

    fn device_id_cfg(id: &str, accept: &[BrokerType]) -> BrokerConfig {
        let mut cfg = BrokerConfig::default();
        cfg.wanted = Some(Wanted::DeviceId(id.into()));
        cfg.accept = Some(accept.to_vec());
        cfg
    }

    #[tokio::test(start_paused = true)]
    async fn list_brokers_follows_the_client_order() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let mut cfg = BrokerConfig::default();
        cfg.accept = Some(vec![BrokerType::Mqtt, BrokerType::SecureMqtt]);
        let list =
            tokio::spawn(async move { list_brokers(&f, &cfg, Duration::from_secs(1)).await });
        settle().await;
        assert_eq!(fake.opened(), ["_mqtt._tcp", "_secure-mqtt._tcp"]);
        fake.emit(seen(BrokerType::SecureMqtt, "a", "a.local.", 8883, &[]));
        fake.emit(seen(BrokerType::SecureMqtt, "z", "z.local.", 8883, &[]));
        fake.emit(seen(BrokerType::Mqtt, "z", "z.local.", 1883, &[]));
        let got = list.await.unwrap().unwrap();
        let got: Vec<(BrokerType, &str)> = got
            .iter()
            .map(|e| (e.broker_type, e.instance.as_deref().unwrap()))
            .collect();
        assert_eq!(
            got,
            [(BrokerType::Mqtt, "z"), (BrokerType::SecureMqtt, "a")]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn find_broker_follows_the_client_order() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let mut cfg = BrokerConfig::default();
        cfg.accept = Some(vec![BrokerType::Mqtt, BrokerType::SecureMqtt]);
        let search = tokio::spawn(async move { find_broker(&f, cfg).await });
        settle().await;
        fake.emit(seen(BrokerType::SecureMqtt, "b", "b.local.", 8883, &[]));
        tokio::time::sleep(Duration::from_millis(2_900)).await;
        fake.emit(seen(BrokerType::Mqtt, "b", "b.local.", 1883, &[]));
        let ep = found(search).await;
        assert_eq!(ep.url(), "mqtt://b.local:1883");
    }

    #[tokio::test(start_paused = true)]
    async fn list_brokers_browses_every_type_for_a_device_id() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let cfg = device_id_cfg("p1", &[BrokerType::Mqtt]);
        let list =
            tokio::spawn(async move { list_brokers(&f, &cfg, Duration::from_secs(1)).await });
        settle().await;
        assert_eq!(
            fake.opened(),
            [
                "_secure-mqtt._tcp",
                "_mqtt-wss._tcp",
                "_mqtt-ws._tcp",
                "_mqtt._tcp"
            ]
        );
        fake.emit(with_txt(
            seen(BrokerType::SecureMqtt, "c", "c.local.", 8883, &[]),
            "device_id=p1",
        ));
        fake.emit(seen(BrokerType::Mqtt, "c", "c.local.", 1883, &[]));
        fake.emit(with_txt(
            seen(BrokerType::SecureMqtt, "d", "d.local.", 8883, &[]),
            "device_id=p1",
        ));
        fake.emit(seen(BrokerType::Mqtt, "e", "e.local.", 1883, &[]));
        let got = list.await.unwrap().unwrap();
        let got: Vec<String> = got.iter().map(BrokerEndpoint::url).collect();
        assert_eq!(got, ["mqtt://c.local:1883"]);
    }

    #[tokio::test(start_paused = true)]
    async fn list_brokers_browses_accepted_types_without_a_device_id() {
        let fake = Fake::new("me.local.");
        let mut cfg = BrokerConfig::default();
        cfg.wanted = Some(Wanted::Instance("c".into()));
        cfg.accept = Some(vec![BrokerType::Mqtt]);
        list_brokers(&fake, &cfg, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(fake.opened(), ["_mqtt._tcp"]);
    }

    #[tokio::test(start_paused = true)]
    async fn find_broker_by_device_id_connects_over_plain_mqtt() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let cfg = device_id_cfg("p1", &[BrokerType::Mqtt]);
        let search = tokio::spawn(async move { find_broker(&f, cfg).await });
        settle().await;
        fake.emit(seen(BrokerType::Mqtt, "c", "c.local.", 1883, &["10.0.0.5"]));
        fake.emit(with_txt(
            seen(BrokerType::SecureMqtt, "c", "c.local.", 8883, &["10.0.0.5"]),
            "device_id=p1",
        ));
        let ep = found(search).await;
        assert_eq!(ep.broker_type, BrokerType::Mqtt);
        assert_eq!((ep.connect_host().as_ref(), ep.port), ("10.0.0.5", 1883));
    }

    fn discovered(name: &str, addr: &str) -> BrokerEndpoint {
        let mut ep = BrokerEndpoint::configured(BrokerType::Mqtt, &format!("{name}.local"), 1883);
        ep.instance = Some(name.into());
        ep.server = Some(format!("{name}.local"));
        ep.addrs = vec![ScopedAddr::new(addr.parse().unwrap())];
        ep
    }

    #[tokio::test(start_paused = true)]
    async fn reresolve_finds_the_new_address() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let old = discovered("b", "10.0.0.5");
        let cfg = BrokerConfig::default();
        let start = tokio::time::Instant::now();
        let task =
            tokio::spawn(async move { reresolve(&f, &old, &cfg, Duration::from_secs(3)).await });
        settle().await;
        assert_eq!(fake.opened(), ["_mqtt._tcp"]);
        fake.emit(seen(
            BrokerType::Mqtt,
            "other",
            "o.local.",
            1883,
            &["10.0.0.1"],
        ));
        fake.emit(seen(BrokerType::Mqtt, "B", "b.local.", 1883, &["10.0.0.7"]));
        let ep = task.await.unwrap().unwrap().unwrap();
        assert_eq!(ep.connect_host(), "10.0.0.7");
        assert_eq!(ep.instance.as_deref(), Some("B"));
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn reresolve_without_ipv4_waits_the_window() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let old = discovered("b", "10.0.0.5");
        let cfg = BrokerConfig::default();
        let start = tokio::time::Instant::now();
        let task =
            tokio::spawn(async move { reresolve(&f, &old, &cfg, Duration::from_secs(3)).await });
        settle().await;
        fake.emit(seen(
            BrokerType::Mqtt,
            "b",
            "b.local.",
            1883,
            &["2001:db8::7"],
        ));
        let ep = task.await.unwrap().unwrap().unwrap();
        assert_eq!(start.elapsed(), Duration::from_secs(3));
        assert_eq!(ep.addrs[0].ip, "2001:db8::7".parse::<IpAddr>().unwrap());
        assert_eq!(ep.connect_host(), "b.local");
    }

    #[tokio::test(start_paused = true)]
    async fn reresolve_none_when_unseen_or_configured() {
        let fake = Fake::new("me.local.");
        let cfg = BrokerConfig::default();
        let got = reresolve(
            &fake,
            &discovered("b", "10.0.0.5"),
            &cfg,
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        assert_eq!(got, None);
        fake.close_browses();
        let configured = BrokerEndpoint::from_url("mqtt://b.local").unwrap();
        let opened = fake.opened().len();
        let got = reresolve(&fake, &configured, &cfg, Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(got, None);
        assert_eq!(fake.opened().len(), opened);
    }

    #[tokio::test(start_paused = true)]
    async fn reresolve_a_broker_found_by_a_siblings_device_id() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let cfg = device_id_cfg("p1", &[BrokerType::Mqtt]);
        let task = tokio::spawn(async move {
            reresolve(
                &f,
                &discovered("b", "10.0.0.5"),
                &cfg,
                Duration::from_secs(3),
            )
            .await
        });
        settle().await;
        fake.emit(seen(BrokerType::Mqtt, "b", "b.local.", 1883, &["10.0.0.7"]));
        let ep = task.await.unwrap().unwrap().unwrap();
        assert_eq!(ep.connect_host(), "10.0.0.7");
    }

    #[tokio::test(start_paused = true)]
    async fn reresolve_none_when_tls_no_longer_accepts_it() {
        let fake = Fake::new("me.local.");
        let f = fake.clone();
        let mut cfg = BrokerConfig::default();
        cfg.tls = true;
        let task = tokio::spawn(async move {
            reresolve(
                &f,
                &discovered("b", "10.0.0.5"),
                &cfg,
                Duration::from_secs(3),
            )
            .await
        });
        settle().await;
        fake.emit(seen(BrokerType::Mqtt, "b", "b.local.", 1883, &["10.0.0.7"]));
        assert_eq!(task.await.unwrap().unwrap(), None);
    }
}
