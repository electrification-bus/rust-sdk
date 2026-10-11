//! A scripted backend for the unit tests.

use core::future::Future;
use std::sync::{Arc, Mutex};

use ebus_discovery::{ServiceRecord, Sighting, TxtRecord};
use tokio::sync::mpsc;

use crate::{Backend, Browse, Error, Group, GroupReport, HostName, lock};

/// What the code under test asked the backend to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Publish {
        instance: String,
        server: String,
        records: Vec<ServiceRecord>,
    },
    UpdateTxt {
        instance: String,
        service_type: String,
        txt: TxtRecord,
    },
    Republish {
        instance: String,
        server: String,
        records: Vec<ServiceRecord>,
    },
    Withdraw {
        instance: String,
    },
}

#[derive(Default)]
struct State {
    browses: Vec<(String, mpsc::UnboundedSender<Sighting>)>,
    opened: Vec<String>,
    requeries: Vec<String>,
    server: String,
    taken: Vec<String>,
    ops: Vec<Op>,
    groups: Vec<(String, mpsc::UnboundedSender<GroupReport>)>,
}

#[derive(Clone, Default)]
pub(crate) struct Fake {
    state: Arc<Mutex<State>>,
}

impl Fake {
    pub(crate) fn new(server: &str) -> Self {
        let f = Self::default();
        lock(&f.state).server = server.into();
        f
    }

    /// Deliver `s` to every open browse of its service type.
    pub(crate) fn emit(&self, s: Sighting) {
        let ty = match &s {
            Sighting::Seen { key, .. } | Sighting::Gone { key, .. } => key.service_type.clone(),
        };
        let mut st = lock(&self.state);
        st.browses
            .retain(|(t, tx)| *t != ty || tx.send(s.clone()).is_ok());
    }

    /// End every open browse.
    pub(crate) fn close_browses(&self) {
        lock(&self.state).browses.clear();
    }

    pub(crate) fn opened(&self) -> Vec<String> {
        lock(&self.state).opened.clone()
    }

    pub(crate) fn requeries(&self) -> Vec<String> {
        lock(&self.state).requeries.clone()
    }

    pub(crate) fn ops(&self) -> Vec<Op> {
        lock(&self.state).ops.clone()
    }

    pub(crate) fn clear_ops(&self) {
        lock(&self.state).ops.clear();
    }

    /// Make publishing under `instance` collide.
    pub(crate) fn take(&self, instance: &str) {
        lock(&self.state).taken.push(instance.into());
    }

    pub(crate) fn set_server(&self, server: &str) {
        lock(&self.state).server = server.into();
    }

    /// Send `r` to the live group published under `instance`.
    pub(crate) fn report(&self, instance: &str, r: GroupReport) {
        let st = lock(&self.state);
        for (i, tx) in &st.groups {
            if i == instance {
                let _ = tx.send(r.clone());
            }
        }
    }
}

pub(crate) struct FakeBrowse {
    ty: String,
    rx: mpsc::UnboundedReceiver<Sighting>,
    state: Arc<Mutex<State>>,
}

pub(crate) struct FakeGroup {
    instance: String,
    rx: mpsc::UnboundedReceiver<GroupReport>,
    state: Arc<Mutex<State>>,
}

impl Backend for Fake {
    type Browse = FakeBrowse;
    type Group = FakeGroup;

    fn browse(&self, service_type: &str) -> impl Future<Output = Result<FakeBrowse, Error>> + Send {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut st = lock(&self.state);
        st.browses.push((service_type.into(), tx));
        st.opened.push(service_type.into());
        let b = FakeBrowse {
            ty: service_type.into(),
            rx,
            state: self.state.clone(),
        };
        async move { Ok(b) }
    }

    fn server_name(&self, _host: &HostName) -> impl Future<Output = Result<String, Error>> + Send {
        let server = lock(&self.state).server.clone();
        async move { Ok(server) }
    }

    fn publish(
        &self,
        _host: &HostName,
        instance: &str,
        records: &[ServiceRecord],
    ) -> impl Future<Output = Result<FakeGroup, Error>> + Send {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut st = lock(&self.state);
        let server = st.server.clone();
        st.ops.push(Op::Publish {
            instance: instance.into(),
            server,
            records: records.to_vec(),
        });
        let report = if st.taken.iter().any(|t| t == instance) {
            GroupReport::Collision
        } else {
            GroupReport::Established
        };
        let _ = tx.send(report);
        st.groups.push((instance.into(), tx));
        let g = FakeGroup {
            instance: instance.into(),
            rx,
            state: self.state.clone(),
        };
        async move { Ok(g) }
    }
}

impl Browse for FakeBrowse {
    fn next(&mut self) -> impl Future<Output = Option<Sighting>> + Send {
        self.rx.recv()
    }

    fn requery(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        lock(&self.state).requeries.push(self.ty.clone());
        async { Ok(()) }
    }
}

impl Group for FakeGroup {
    fn next_report(&mut self) -> impl Future<Output = Option<GroupReport>> + Send {
        self.rx.recv()
    }

    fn update_txt(
        &mut self,
        service_type: &str,
        txt: &TxtRecord,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        lock(&self.state).ops.push(Op::UpdateTxt {
            instance: self.instance.clone(),
            service_type: service_type.into(),
            txt: txt.clone(),
        });
        async { Ok(()) }
    }

    fn republish(
        &mut self,
        records: &[ServiceRecord],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        let mut st = lock(&self.state);
        let server = st.server.clone();
        st.ops.push(Op::Republish {
            instance: self.instance.clone(),
            server,
            records: records.to_vec(),
        });
        for (i, tx) in &st.groups {
            if *i == self.instance {
                let _ = tx.send(GroupReport::Established);
            }
        }
        async { Ok(()) }
    }

    fn withdraw(self) -> impl Future<Output = Result<(), Error>> + Send {
        let mut st = lock(&self.state);
        st.ops.push(Op::Withdraw {
            instance: self.instance.clone(),
        });
        st.groups.retain(|(i, _)| *i != self.instance);
        async { Ok(()) }
    }
}
