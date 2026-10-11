//! Advertising a host's services.

use std::sync::{Arc, Mutex};

use ebus_discovery::{Advertisement, ServiceRecord};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{Backend, Error, Group, GroupReport, HostName, lock};

/// Where an advertisement stands.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AdState {
    /// Registering, or registering again after a collision, a host rename or
    /// a backend restart.
    Registering,
    /// Registered.
    Established {
        /// The instance name every record carries.
        instance: String,
        /// The SRV target, with a trailing dot.
        server: String,
    },
    /// Not advertised: every instance name through `-99` collided, or the
    /// backend failed.
    Failed(String),
}

enum Cmd {
    Update(Box<Advertisement>),
    Withdraw(oneshot::Sender<Result<(), Error>>),
}

/// A running advertisement. Dropping it withdraws the records from the
/// advertiser's task.
#[derive(Debug)]
pub struct Advertiser {
    cmds: mpsc::UnboundedSender<Cmd>,
    state: watch::Receiver<AdState>,
    server: Arc<Mutex<String>>,
}

impl core::fmt::Debug for Cmd {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Update(_) => "Update",
            Self::Withdraw(_) => "Withdraw",
        })
    }
}

/// Advertise `ad` under `host`: every record under one instance name, from
/// [`Advertisement::instance_names`]. A collision on any record withdraws
/// them all and registers them again under the next name, `-2` through
/// `-99`; after that the state is [`AdState::Failed`]. A host rename rebuilds
/// the records for the new SRV target and registers them again in place.
///
/// Returns once the records are handed to the backend; [`Advertiser::state`]
/// reports when they are established. Needs a tokio runtime.
pub async fn advertise<B: Backend>(
    b: &B,
    ad: Advertisement,
    host: HostName,
) -> Result<Advertiser, Error> {
    let rt = tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
    let server = b.server_name(&host).await?;
    let records = ad.records(&server)?;
    let names: Vec<String> = ad.instance_names(&server).collect();
    let group = b.publish(&host, &names[0], &records).await?;
    let (state_tx, state) = watch::channel(AdState::Registering);
    let (cmds, rx) = mpsc::unbounded_channel();
    let shared_server = Arc::new(Mutex::new(server.clone()));
    let task = Task {
        backend: b.clone(),
        host,
        ad,
        server,
        names,
        attempt: 0,
        records,
        group: Some(group),
        state: state_tx,
        shared_server: shared_server.clone(),
    };
    rt.spawn(task.run(rx));
    Ok(Advertiser {
        cmds,
        state,
        server: shared_server,
    })
}

impl Advertiser {
    /// Advertise `ad` instead: TXT updates only for the records whose TXT
    /// changed; the records registered again in place when a port or the set
    /// of records changed, or withdrawn and published under the new name
    /// when the instance name changed; nothing for an equal advertisement.
    /// The advertisement is checked here; the update itself runs on the
    /// advertiser's task.
    pub fn update(&self, ad: Advertisement) -> Result<(), Error> {
        let server = lock(&self.server).clone();
        ad.records(&server)?;
        self.cmds
            .send(Cmd::Update(Box::new(ad)))
            .map_err(|_| Error::Closed)
    }

    /// The advertisement's state; a receiver wakes only when it changes.
    pub fn state(&self) -> watch::Receiver<AdState> {
        self.state.clone()
    }

    /// Withdraw every record, sending goodbyes, and wait for it.
    pub async fn withdraw(self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.cmds
            .send(Cmd::Withdraw(tx))
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }
}

struct Task<B: Backend> {
    backend: B,
    host: HostName,
    ad: Advertisement,
    server: String,
    names: Vec<String>,
    attempt: usize,
    records: Vec<ServiceRecord>,
    group: Option<B::Group>,
    state: watch::Sender<AdState>,
    shared_server: Arc<Mutex<String>>,
}

enum Event {
    Report(Option<GroupReport>),
    Cmd(Option<Cmd>),
}

impl<B: Backend> Task<B> {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Cmd>) {
        loop {
            let event = match &mut self.group {
                Some(g) => tokio::select! {
                    r = g.next_report() => Event::Report(r),
                    c = rx.recv() => Event::Cmd(c),
                },
                None => Event::Cmd(rx.recv().await),
            };
            match event {
                Event::Report(r) => self.on_report(r).await,
                Event::Cmd(Some(Cmd::Update(ad))) => self.on_update(*ad).await,
                Event::Cmd(Some(Cmd::Withdraw(reply))) => {
                    let _ = reply.send(self.withdraw().await);
                    return;
                }
                Event::Cmd(None) => {
                    if let Err(e) = self.withdraw().await {
                        log::warn!("reason=withdrawFailed,error={e}");
                    }
                    return;
                }
            }
        }
    }

    fn set_state(&self, new: AdState) {
        self.state.send_if_modified(|s| {
            if *s == new {
                return false;
            }
            *s = new;
            true
        });
    }

    fn instance(&self) -> &str {
        &self.names[self.attempt]
    }

    async fn on_report(&mut self, r: Option<GroupReport>) {
        match r {
            Some(GroupReport::Established) => {
                let new = AdState::Established {
                    instance: self.instance().into(),
                    server: self.server.clone(),
                };
                if self.attempt > 0 && *self.state.borrow() != new {
                    log::info!(
                        "reason=instanceRenamed,from={},to={}",
                        self.names[0],
                        self.instance()
                    );
                }
                self.set_state(new);
            }
            Some(GroupReport::Collision) => {
                log::info!("reason=instanceNameConflict,name={}", self.instance());
                self.withdraw().await.ok();
                if self.attempt + 1 >= self.names.len() {
                    self.fail(format!(
                        "no free instance name from {} to -{}",
                        self.names[0],
                        self.names.len()
                    ));
                    return;
                }
                self.attempt += 1;
                self.publish().await;
            }
            Some(GroupReport::HostRenamed) => {
                let server = match self.backend.server_name(&self.host).await {
                    Ok(s) => s,
                    Err(e) => return self.fail(e.to_string()),
                };
                log::info!("reason=hostRenamed,from={},to={server}", self.server);
                let records = match self.ad.records(&server) {
                    Ok(r) => r,
                    Err(e) => return self.fail(e.to_string()),
                };
                let names: Vec<String> = self.ad.instance_names(&server).collect();
                self.server = server;
                *lock(&self.shared_server) = self.server.clone();
                self.replace(names, records).await;
            }
            Some(GroupReport::Failed(reason)) => self.set_state(AdState::Failed(reason)),
            Some(GroupReport::Reset) => self.set_state(AdState::Registering),
            None => {
                self.group = None;
                self.fail("the mDNS backend stopped".into());
            }
        }
    }

    async fn on_update(&mut self, ad: Advertisement) {
        if ad == self.ad {
            return;
        }
        let records = match ad.records(&self.server) {
            Ok(r) => r,
            Err(e) => {
                log::warn!("reason=advertisementRejected,error={e}");
                return;
            }
        };
        let names: Vec<String> = ad.instance_names(&self.server).collect();
        self.ad = ad;
        let same_shape = records.len() == self.records.len()
            && records
                .iter()
                .zip(&self.records)
                .all(|(a, b)| a.service_type == b.service_type && a.port == b.port);
        if names[0] != self.names[0] || !same_shape {
            self.replace(names, records).await;
            return;
        }
        let old = core::mem::replace(&mut self.records, records);
        let Some(group) = &mut self.group else {
            return;
        };
        for (new, old) in self.records.iter().zip(&old) {
            if new.txt != old.txt {
                if let Err(e) = group.update_txt(new.service_type, &new.txt).await {
                    log::warn!(
                        "reason=txtUpdateFailed,serviceType={},error={e}",
                        new.service_type
                    );
                }
            }
        }
    }

    /// Register `records` again: under new names when the base name
    /// changed, else in place under the name held.
    async fn replace(&mut self, names: Vec<String>, records: Vec<ServiceRecord>) {
        self.records = records;
        if names[0] != self.names[0] {
            self.withdraw().await.ok();
            self.names = names;
            self.attempt = 0;
            self.publish().await;
            return;
        }
        if self.group.is_none() {
            return;
        }
        self.set_state(AdState::Registering);
        let Some(group) = &mut self.group else {
            return;
        };
        if let Err(e) = group.republish(&self.records).await {
            self.fail(e.to_string());
        }
    }

    async fn publish(&mut self) {
        self.set_state(AdState::Registering);
        match self
            .backend
            .publish(&self.host, &self.names[self.attempt], &self.records)
            .await
        {
            Ok(g) => self.group = Some(g),
            Err(e) => self.fail(e.to_string()),
        }
    }

    async fn withdraw(&mut self) -> Result<(), Error> {
        match self.group.take() {
            Some(g) => g.withdraw().await,
            None => Ok(()),
        }
    }

    fn fail(&mut self, reason: String) {
        log::warn!("reason=advertiseFailed,error={reason}");
        self.set_state(AdState::Failed(reason));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ebus_discovery::{BrokerService, BrokerType, HttpService, Identity};

    use super::*;
    use crate::fake::{Fake, Op};

    fn ad() -> Advertisement {
        Advertisement::new(Identity::new(
            ["panel-1"],
            ["device", "broker-host"],
            "Acme",
            "P1",
            "SN1",
        ))
        .http(HttpService::https(443, "/api/v1"))
        .broker(BrokerService::new(BrokerType::SecureMqtt, 8883))
    }

    async fn wait_for(state: &mut watch::Receiver<AdState>, want: AdState) {
        tokio::time::timeout(Duration::from_secs(5), state.wait_for(|s| *s == want))
            .await
            .expect("state reached")
            .expect("advertiser running");
    }

    fn established(instance: &str, server: &str) -> AdState {
        AdState::Established {
            instance: instance.into(),
            server: server.into(),
        }
    }

    /// Run the advertiser's task up to its next wait.
    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn publishes_every_record_under_one_name() {
        let fake = Fake::new("box.local.");
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        wait_for(&mut a.state(), established("panel-1", "box.local.")).await;
        let ops = fake.ops();
        assert_eq!(ops.len(), 1);
        let Op::Publish {
            instance, records, ..
        } = &ops[0]
        else {
            panic!("{ops:?}");
        };
        assert_eq!(instance, "panel-1");
        assert_eq!(*records, ad().records("box.local.").unwrap());
        assert_eq!(records[3].txt.get("broker").as_deref(), Some("box.local"));
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_advertisement_is_an_error() {
        let fake = Fake::new("box.local.");
        let mut bad = ad();
        bad.identity.device_ids.clear();
        assert!(matches!(
            advertise(&fake, bad, HostName::Auto).await,
            Err(Error::Txt(_))
        ));
        assert!(fake.ops().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn collision_renames_all_records() {
        let fake = Fake::new("box.local.");
        fake.take("panel-1");
        fake.take("panel-1-2");
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        wait_for(&mut a.state(), established("panel-1-3", "box.local.")).await;
        let ops: Vec<String> = fake
            .ops()
            .iter()
            .map(|o| match o {
                Op::Publish { instance, .. } => format!("publish {instance}"),
                Op::Withdraw { instance } => format!("withdraw {instance}"),
                Op::UpdateTxt { .. } | Op::Republish { .. } => "update".into(),
            })
            .collect();
        assert_eq!(
            ops,
            [
                "publish panel-1",
                "withdraw panel-1",
                "publish panel-1-2",
                "withdraw panel-1-2",
                "publish panel-1-3"
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn names_exhausted_fails() {
        let fake = Fake::new("box.local.");
        fake.take("panel-1");
        for n in 2..=99 {
            fake.take(&format!("panel-1-{n}"));
        }
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        let mut state = a.state();
        tokio::time::timeout(
            Duration::from_secs(5),
            state.wait_for(|s| matches!(s, AdState::Failed(_))),
        )
        .await
        .unwrap()
        .unwrap();
        let publishes = fake
            .ops()
            .iter()
            .filter(|o| matches!(o, Op::Publish { .. }))
            .count();
        assert_eq!(publishes, 99);
        assert!(matches!(fake.ops().last(), Some(Op::Withdraw { .. })));
    }

    #[tokio::test(start_paused = true)]
    async fn update_sends_only_changed_txt() {
        let fake = Fake::new("box.local.");
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        wait_for(&mut a.state(), established("panel-1", "box.local.")).await;
        fake.clear_ops();

        a.update(ad()).unwrap();
        settle().await;
        assert!(fake.ops().is_empty(), "{:?}", fake.ops());

        let mut changed = ad();
        changed.http[0].path = "/api/v2".into();
        a.update(changed.clone()).unwrap();
        settle().await;
        let want = changed.records("box.local.").unwrap();
        assert_eq!(
            fake.ops(),
            [Op::UpdateTxt {
                instance: "panel-1".into(),
                service_type: "_https._tcp".into(),
                txt: want[2].txt.clone(),
            }]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn update_with_a_new_port_registers_again_in_place() {
        let fake = Fake::new("box.local.");
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        let mut state = a.state();
        wait_for(&mut state, established("panel-1", "box.local.")).await;
        fake.clear_ops();
        let mut moved = ad();
        moved.brokers[0].port = 8884;
        a.update(moved.clone()).unwrap();
        settle().await;
        assert_eq!(
            fake.ops(),
            [Op::Republish {
                instance: "panel-1".into(),
                server: "box.local.".into(),
                records: moved.records("box.local.").unwrap(),
            }]
        );
        wait_for(&mut state, established("panel-1", "box.local.")).await;
    }

    #[tokio::test(start_paused = true)]
    async fn update_with_a_new_name_withdraws_first() {
        let fake = Fake::new("box.local.");
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        let mut state = a.state();
        wait_for(&mut state, established("panel-1", "box.local.")).await;
        fake.clear_ops();
        a.update(ad().instance("Main")).unwrap();
        wait_for(&mut state, established("Main", "box.local.")).await;
        let ops = fake.ops();
        assert_eq!(
            ops[0],
            Op::Withdraw {
                instance: "panel-1".into()
            }
        );
        assert!(matches!(&ops[1], Op::Publish { instance, .. } if instance == "Main"));
    }

    #[tokio::test(start_paused = true)]
    async fn update_rejects_an_invalid_advertisement() {
        let fake = Fake::new("box.local.");
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        let mut bad = ad();
        bad.identity.model.clear();
        assert!(matches!(a.update(bad), Err(Error::Txt(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn host_rename_rebuilds_the_records_in_place() {
        let fake = Fake::new("box.local.");
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        let mut state = a.state();
        wait_for(&mut state, established("panel-1", "box.local.")).await;
        fake.clear_ops();
        fake.set_server("box-2.local.");
        fake.report("panel-1", GroupReport::HostRenamed);
        wait_for(&mut state, established("panel-1", "box-2.local.")).await;
        let ops = fake.ops();
        let [Op::Republish { records, .. }] = &ops[..] else {
            panic!("{ops:?}");
        };
        assert_eq!(records[3].txt.get("broker").as_deref(), Some("box-2.local"));
        // The new server also checks later updates.
        assert!(a.update(ad()).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn host_rename_of_a_host_label_name_renames_the_instance() {
        let fake = Fake::new("box.local.");
        let long = "d".repeat(61);
        let ad = Advertisement::new(Identity::new([long.as_str()], ["device"], "A", "M", "S"));
        let a = advertise(&fake, ad, HostName::Auto).await.unwrap();
        let mut state = a.state();
        wait_for(&mut state, established("box", "box.local.")).await;
        fake.clear_ops();
        fake.set_server("box-2.local.");
        fake.report("box", GroupReport::HostRenamed);
        wait_for(&mut state, established("box-2", "box-2.local.")).await;
        let ops = fake.ops();
        assert_eq!(
            ops[0],
            Op::Withdraw {
                instance: "box".into()
            }
        );
        assert!(matches!(&ops[1], Op::Publish { instance, .. } if instance == "box-2"));
    }

    #[tokio::test(start_paused = true)]
    async fn unchanged_reports_do_not_wake_the_state() {
        let fake = Fake::new("box.local.");
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        let mut state = a.state();
        wait_for(&mut state, established("panel-1", "box.local.")).await;
        state.mark_unchanged();
        fake.report("panel-1", GroupReport::Established);
        settle().await;
        assert!(!state.has_changed().unwrap());
        fake.report("panel-1", GroupReport::Failed("gone".into()));
        settle().await;
        assert!(state.has_changed().unwrap());
        assert_eq!(*state.borrow_and_update(), AdState::Failed("gone".into()));
    }

    #[tokio::test(start_paused = true)]
    async fn withdraw_and_drop_withdraw_the_records() {
        let fake = Fake::new("box.local.");
        let a = advertise(&fake, ad(), HostName::Auto).await.unwrap();
        settle().await;
        a.withdraw().await.unwrap();
        assert_eq!(
            fake.ops().last(),
            Some(&Op::Withdraw {
                instance: "panel-1".into()
            })
        );
        fake.clear_ops();
        let b = advertise(&fake, ad().instance("Main"), HostName::Auto)
            .await
            .unwrap();
        settle().await;
        drop(b);
        settle().await;
        assert_eq!(
            fake.ops().last(),
            Some(&Op::Withdraw {
                instance: "Main".into()
            })
        );
    }
}
