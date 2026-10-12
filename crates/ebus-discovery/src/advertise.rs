//! What a host advertises: identity, REST API and brokers, and the records
//! they become (ebus.py:330-546; _mdns_core.py:900-939, 1062-1063, 1129-1152).

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::txt::{TxtError, TxtRecord, check_txt};
use crate::{
    BrokerType, DEVICE_INFO_SERVICE, EBUS_SERVICE, EBUS_VERSION, HTTP_SERVICE, HTTPS_SERVICE,
    ROLE_BROKER_HOST, ROLE_CONTROLLER, ROLE_DEVICE, TXTVERS, strip_dot,
};

/// Instance-name suffixes tried on a collision: `-2` through `-99`.
pub const MAX_RENAME_SUFFIX: u32 = 99;
/// Longest device id used as the default instance name: one 63-octet DNS
/// label less the `-99` suffix.
pub const MAX_DEFAULT_INSTANCE_OCTETS: usize = 60;

/// The `homie_version` advertised when [`Identity::homie_version`] is unset.
const HOMIE_VERSION: &str = "5";

/// MQTT protocol versions the specification names for the broker `protocol` key.
const MQTT_PROTOCOLS: [&str; 2] = ["mqtt-v5", "mqtt-v3.1.1"];

/// Add `key=value` when `value` is not empty.
fn put(txt: &mut TxtRecord, key: &str, value: Option<&str>) {
    if let Some(v) = value.filter(|v| !v.is_empty()) {
        txt.insert(key, v);
    }
}

fn add_extras(txt: &mut TxtRecord, extras: &[(String, String)]) {
    for (k, v) in extras {
        txt.insert(k, v);
    }
}

/// An entity's `_ebus._tcp` and `_device-info._tcp` content (ebus.py:330-438).
///
/// One `_ebus._tcp` advertisement covers the whole host, so `device_ids` may
/// list several devices, joined with commas into `device_id`. Recommended
/// keys left empty are omitted. Extra keys never replace a key the identity
/// sets.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Identity {
    /// The host's device ids; the first is the default instance name.
    pub device_ids: Vec<String>,
    /// `device`, `controller`, `broker-host`.
    pub roles: Vec<String>,
    /// Required.
    pub manufacturer: String,
    /// Required.
    pub model: String,
    /// Required, in `_device-info._tcp`.
    pub serial_number: String,
    /// Defaults to [`EBUS_VERSION`].
    pub ebus_version: String,
    /// `device_type`.
    pub device_type: Option<String>,
    /// `name`.
    pub name: Option<String>,
    /// `fw_version`, in both records.
    pub fw_version: Option<String>,
    /// `register`: the registration endpoint path.
    pub register: Option<String>,
    /// `broker_ca`: the broker CA download path.
    pub broker_ca: Option<String>,
    /// `auth_methods`.
    pub auth_methods: Vec<String>,
    /// `hw_version`, in `_device-info._tcp`.
    pub hw_version: Option<String>,
    /// `os_version`, in `_device-info._tcp`.
    pub os_version: Option<String>,
    /// `mac`, in `_device-info._tcp`.
    pub mac: Option<String>,
    /// Keys the specification does not define, appended to `_ebus._tcp`.
    pub extra_ebus_txt: Vec<(String, String)>,
    /// Keys the specification does not define, appended to `_device-info._tcp`.
    pub extra_device_info_txt: Vec<(String, String)>,
    /// The Homie domain the host publishes under; adds `homie_domain`,
    /// `homie_version` and `homie_roles` to `_ebus._tcp`.
    pub homie_domain: Option<String>,
    /// `homie_version` with `homie_domain`; defaults to `5`.
    pub homie_version: Option<String>,
}

impl Identity {
    /// An identity with the required fields; every other field empty.
    pub fn new<D, R>(
        device_ids: impl IntoIterator<Item = D>,
        roles: impl IntoIterator<Item = R>,
        manufacturer: &str,
        model: &str,
        serial_number: &str,
    ) -> Self
    where
        D: Into<String>,
        R: Into<String>,
    {
        Self {
            device_ids: device_ids.into_iter().map(Into::into).collect(),
            roles: roles.into_iter().map(Into::into).collect(),
            manufacturer: manufacturer.into(),
            model: model.into(),
            serial_number: serial_number.into(),
            ebus_version: EBUS_VERSION.into(),
            device_type: None,
            name: None,
            fw_version: None,
            register: None,
            broker_ca: None,
            auth_methods: Vec::new(),
            hw_version: None,
            os_version: None,
            mac: None,
            extra_ebus_txt: Vec::new(),
            extra_device_info_txt: Vec::new(),
            homie_domain: None,
            homie_version: None,
        }
    }

    fn trimmed(list: &[String]) -> impl Iterator<Item = &str> {
        list.iter().map(|s| s.trim())
    }

    /// The `device_id` value: the device ids joined with commas.
    pub fn device_id(&self) -> String {
        Self::trimmed(&self.device_ids)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Check the required fields and both records (ebus.py:371-393).
    pub fn check(&self) -> Result<(), TxtError> {
        if self.device_ids.is_empty() {
            return Err(TxtError::NoDeviceId);
        }
        if let Some(bad) = Self::trimmed(&self.device_ids).find(|d| d.is_empty() || d.contains(','))
        {
            return Err(TxtError::InvalidDeviceId(bad.to_string()));
        }
        if self.roles.is_empty() {
            return Err(TxtError::NoRole);
        }
        if let Some(bad) = Self::trimmed(&self.roles).find(|r| r.is_empty() || r.contains(',')) {
            return Err(TxtError::InvalidRole(bad.to_string()));
        }
        if let Some(d) = &self.homie_domain {
            if d.contains(['/', '+', '#']) {
                return Err(TxtError::InvalidHomieDomain(d.clone()));
            }
        }
        for (field, value) in [
            ("manufacturer", &self.manufacturer),
            ("model", &self.model),
            ("serial_number", &self.serial_number),
            ("ebus_version", &self.ebus_version),
        ] {
            if value.is_empty() {
                return Err(TxtError::Missing(field));
            }
        }
        check_txt(EBUS_SERVICE, &self.ebus_txt())?;
        check_txt(DEVICE_INFO_SERVICE, &self.device_info_txt())?;
        Ok(())
    }

    /// The `_ebus._tcp` record, keys in python's order (ebus.py:400-422).
    pub fn ebus_txt(&self) -> TxtRecord {
        let mut txt = TxtRecord::new();
        txt.insert("txtvers", TXTVERS);
        txt.insert("ebus_version", &self.ebus_version);
        let roles: Vec<&str> = Self::trimmed(&self.roles).collect();
        txt.insert("roles", &roles.join(","));
        txt.insert("device_id", &self.device_id());
        put(&mut txt, "device_type", self.device_type.as_deref());
        put(&mut txt, "name", self.name.as_deref());
        put(&mut txt, "manufacturer", Some(&self.manufacturer));
        put(&mut txt, "model", Some(&self.model));
        put(&mut txt, "fw_version", self.fw_version.as_deref());
        put(&mut txt, "register", self.register.as_deref());
        put(&mut txt, "broker_ca", self.broker_ca.as_deref());
        let auth: Vec<&str> = Self::trimmed(&self.auth_methods).collect();
        put(&mut txt, "auth_methods", Some(&auth.join(",")));
        if let Some(domain) = self.homie_domain.as_deref().filter(|d| !d.is_empty()) {
            txt.insert("homie_domain", domain);
            let version = self.homie_version.as_deref().filter(|v| !v.is_empty());
            txt.insert("homie_version", version.unwrap_or(HOMIE_VERSION));
            let homie: Vec<&str> = roles
                .iter()
                .copied()
                .filter(|r| *r == ROLE_DEVICE || *r == ROLE_CONTROLLER)
                .collect();
            put(&mut txt, "homie_roles", Some(&homie.join(",")));
        }
        add_extras(&mut txt, &self.extra_ebus_txt);
        txt
    }

    /// The `_device-info._tcp` record (ebus.py:424-438).
    pub fn device_info_txt(&self) -> TxtRecord {
        let mut txt = TxtRecord::new();
        txt.insert("txtvers", TXTVERS);
        txt.insert("manufacturer", &self.manufacturer);
        txt.insert("model", &self.model);
        txt.insert("serial_number", &self.serial_number);
        put(&mut txt, "fw_version", self.fw_version.as_deref());
        put(&mut txt, "hw_version", self.hw_version.as_deref());
        put(&mut txt, "os_version", self.os_version.as_deref());
        put(&mut txt, "mac", self.mac.as_deref());
        add_extras(&mut txt, &self.extra_device_info_txt);
        txt
    }
}

/// A REST API advertised as `_http._tcp` or `_https._tcp` (ebus.py:445-477).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct HttpService {
    /// `_https._tcp` when true.
    pub tls: bool,
    /// The API's port.
    pub port: u16,
    /// `path`: the API base path.
    pub path: String,
    /// `version`: the API version, `1.0` by default.
    pub version: String,
    /// `openapi`: the OpenAPI document's path.
    pub openapi: Option<String>,
    /// Keys the specification does not define.
    pub extra_txt: Vec<(String, String)>,
}

impl HttpService {
    /// A plain HTTP API at `port` under `path`.
    pub fn http(port: u16, path: &str) -> Self {
        Self {
            tls: false,
            port,
            path: path.into(),
            version: "1.0".into(),
            openapi: None,
            extra_txt: Vec::new(),
        }
    }

    /// An HTTPS API at `port` under `path`.
    pub fn https(port: u16, path: &str) -> Self {
        Self {
            tls: true,
            ..Self::http(port, path)
        }
    }

    /// `_https._tcp` or `_http._tcp`.
    pub fn service_type(&self) -> &'static str {
        if self.tls {
            HTTPS_SERVICE
        } else {
            HTTP_SERVICE
        }
    }

    /// The TXT record, carrying the identity's `device_id` and `device_type`.
    pub fn txt(&self, identity: &Identity) -> TxtRecord {
        let mut txt = TxtRecord::new();
        txt.insert("txtvers", TXTVERS);
        txt.insert("path", &self.path);
        txt.insert("version", &self.version);
        txt.insert("device_id", &identity.device_id());
        put(&mut txt, "device_type", identity.device_type.as_deref());
        put(&mut txt, "openapi", self.openapi.as_deref());
        add_extras(&mut txt, &self.extra_txt);
        txt
    }
}

/// A broker this host runs (ebus.py:494-546; framework.md:299-326).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct BrokerService {
    /// The transport.
    pub broker_type: BrokerType,
    /// The port.
    pub port: u16,
    /// `protocol`: `mqtt-v5` by default, or `mqtt-v3.1.1`; another value is
    /// kept and logged.
    pub protocol: String,
    /// `broker` for `_secure-mqtt._tcp`: the name the broker's certificate is
    /// issued for; defaults to the SRV target.
    pub broker: Option<String>,
    /// `path` for the WebSocket types, `/mqtt` by default.
    pub path: String,
    /// `subprotocol` for the WebSocket types, `mqtt` by default.
    pub subprotocol: String,
    /// Keys the specification does not define for the type.
    pub extra_txt: Vec<(String, String)>,
}

impl BrokerService {
    /// A broker of `broker_type` at `port`.
    pub fn new(broker_type: BrokerType, port: u16) -> Self {
        Self {
            broker_type,
            port,
            protocol: "mqtt-v5".into(),
            broker: None,
            path: "/mqtt".into(),
            subprotocol: "mqtt".into(),
            extra_txt: Vec::new(),
        }
    }

    /// The TXT record for an advertisement whose SRV target is `server`:
    /// `txtvers` and `protocol`, then `broker` and `device_id` for
    /// `_secure-mqtt._tcp`, or `path` and `subprotocol` for the WebSocket types.
    pub fn txt(&self, identity: &Identity, server: &str) -> TxtRecord {
        let mut txt = TxtRecord::new();
        txt.insert("txtvers", TXTVERS);
        txt.insert("protocol", &self.protocol);
        match self.broker_type {
            BrokerType::SecureMqtt => {
                let broker = self.broker.as_deref().filter(|b| !b.is_empty());
                txt.insert("broker", strip_dot(broker.unwrap_or(server)));
                txt.insert("device_id", &identity.device_id());
            }
            BrokerType::MqttWss | BrokerType::MqttWs => {
                txt.insert("path", &self.path);
                txt.insert("subprotocol", &self.subprotocol);
            }
            BrokerType::Mqtt => {}
        }
        add_extras(&mut txt, &self.extra_txt);
        txt
    }
}

/// One service an advertisement registers.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServiceRecord {
    /// The service type, without a domain.
    pub service_type: &'static str,
    /// The port.
    pub port: u16,
    /// The TXT record.
    pub txt: TxtRecord,
}

/// Everything a host advertises, all under one instance name.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Advertisement {
    /// The identity.
    pub identity: Identity,
    /// The instance name; `None` for the default rule ([`Advertisement::instance_names`]).
    pub instance: Option<String>,
    /// The `_ebus._tcp` port; `None` for the first REST API's port, else 0.
    pub ebus_port: Option<u16>,
    /// The `_device-info._tcp` port, 0 by default.
    pub device_info_port: u16,
    /// REST APIs.
    pub http: Vec<HttpService>,
    /// Brokers, at most one per type.
    pub brokers: Vec<BrokerService>,
}

impl Advertisement {
    /// The identity's `_ebus._tcp` and `_device-info._tcp` records alone.
    pub fn new(identity: Identity) -> Self {
        Self {
            identity,
            instance: None,
            ebus_port: None,
            device_info_port: 0,
            http: Vec::new(),
            brokers: Vec::new(),
        }
    }

    /// With an explicit instance name.
    pub fn instance(mut self, name: &str) -> Self {
        self.instance = Some(name.into());
        self
    }

    /// With a REST API added.
    pub fn http(mut self, http: HttpService) -> Self {
        self.http.push(http);
        self
    }

    /// With a broker added.
    pub fn broker(mut self, broker: BrokerService) -> Self {
        self.brokers.push(broker);
        self
    }

    /// Every record, in python's order (_mdns_core.py:900-939): `_ebus._tcp`,
    /// `_device-info._tcp`, the REST APIs, then the brokers. `server` is the
    /// SRV target, the default TXT `broker`. Every record is checked with
    /// [`check_txt`]; brokers without the `broker-host` role are logged with
    /// `reason=brokersWithoutBrokerHostRole` and `roles` is left as given.
    pub fn records(&self, server: &str) -> Result<Vec<ServiceRecord>, TxtError> {
        let id = &self.identity;
        id.check()?;
        for role in &id.roles {
            let role = role.trim();
            if ![ROLE_DEVICE, ROLE_CONTROLLER, ROLE_BROKER_HOST].contains(&role) {
                crate::log::warn!("unknownEbusRole", role = role);
            }
        }
        for (i, b) in self.brokers.iter().enumerate() {
            if self.brokers[..i]
                .iter()
                .any(|o| o.broker_type == b.broker_type)
            {
                return Err(TxtError::DuplicateBrokerType(b.broker_type));
            }
            if !MQTT_PROTOCOLS.contains(&b.protocol.as_str()) {
                crate::log::warn!("unknownMqttProtocol", protocol = b.protocol);
            }
        }
        if !self.brokers.is_empty() && !id.roles.iter().any(|r| r.trim() == ROLE_BROKER_HOST) {
            crate::log::warn!("brokersWithoutBrokerHostRole", roles = id.roles.join(","));
        }
        let ebus_port = self
            .ebus_port
            .or_else(|| self.http.first().map(|h| h.port))
            .unwrap_or(0);
        let mut out = Vec::with_capacity(2 + self.http.len() + self.brokers.len());
        out.push(ServiceRecord {
            service_type: EBUS_SERVICE,
            port: ebus_port,
            txt: id.ebus_txt(),
        });
        out.push(ServiceRecord {
            service_type: DEVICE_INFO_SERVICE,
            port: self.device_info_port,
            txt: id.device_info_txt(),
        });
        for h in &self.http {
            out.push(ServiceRecord {
                service_type: h.service_type(),
                port: h.port,
                txt: h.txt(id),
            });
        }
        for b in &self.brokers {
            out.push(ServiceRecord {
                service_type: b.broker_type.as_str(),
                port: b.port,
                txt: b.txt(id, server),
            });
        }
        for r in &out[2..] {
            check_txt(r.service_type, &r.txt)?;
        }
        Ok(out)
    }

    /// The instance name all records share: the explicit name, else the first
    /// device id when it fits [`MAX_DEFAULT_INSTANCE_OCTETS`], else the host
    /// label of `server`, logged with `reason=deviceIdTooLongForInstanceName`
    /// (_mdns_core.py:1129-1152).
    pub fn base_instance_name(&self, server: &str) -> String {
        if let Some(name) = self.instance.as_deref().filter(|n| !n.is_empty()) {
            return name.into();
        }
        let first = self.identity.device_ids.first().map_or("", |d| d.trim());
        if first.len() <= MAX_DEFAULT_INSTANCE_OCTETS {
            return first.into();
        }
        let host = strip_dot(server);
        let host = host.rfind(".local").map_or(host, |at| &host[..at]);
        crate::log::info!(
            "deviceIdTooLongForInstanceName",
            deviceId = first,
            instance = host
        );
        host.into()
    }

    /// The instance names to try in turn: the base name
    /// ([`Advertisement::base_instance_name`]), then `-2` through `-99`
    /// (_mdns_core.py:937-939).
    pub fn instance_names(&self, server: &str) -> impl Iterator<Item = String> + use<> {
        let base = self.base_instance_name(server);
        core::iter::once(base.clone())
            .chain((2..=MAX_RENAME_SUFFIX).map(move |n| format!("{base}-{n}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> Identity {
        Identity::new(
            ["panel-1", "meter-2"],
            ["device", "broker-host"],
            "Acme",
            "P1",
            "SN1",
        )
    }

    fn pairs(txt: &TxtRecord) -> Vec<(String, String)> {
        txt.iter()
            .map(|(k, _)| (k.to_string(), txt.get(k).unwrap().into_owned()))
            .collect()
    }

    fn p(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn ebus_record() {
        let mut id = identity();
        id.auth_methods = alloc::vec!["passphrase".into(), " mtls ".into()];
        id.homie_domain = Some("ebus".into());
        id.extra_ebus_txt = p(&[("roles", "x"), ("custom", "1")]);
        assert_eq!(
            pairs(&id.ebus_txt()),
            p(&[
                ("txtvers", "1"),
                ("ebus_version", EBUS_VERSION),
                ("roles", "device,broker-host"),
                ("device_id", "panel-1,meter-2"),
                ("manufacturer", "Acme"),
                ("model", "P1"),
                ("auth_methods", "passphrase,mtls"),
                ("homie_domain", "ebus"),
                ("homie_version", "5"),
                ("homie_roles", "device"),
                ("custom", "1"),
            ])
        );
        assert_eq!(
            pairs(&id.device_info_txt()),
            p(&[
                ("txtvers", "1"),
                ("manufacturer", "Acme"),
                ("model", "P1"),
                ("serial_number", "SN1")
            ])
        );
    }

    #[test]
    fn identity_checks() {
        let mut id = identity();
        id.device_ids.clear();
        assert_eq!(id.check(), Err(TxtError::NoDeviceId));
        let mut id = identity();
        id.device_ids.push("a,b".into());
        assert_eq!(id.check(), Err(TxtError::InvalidDeviceId("a,b".into())));
        let mut id = identity();
        id.roles = alloc::vec![" ".into()];
        assert_eq!(id.check(), Err(TxtError::InvalidRole("".into())));
        let mut id = identity();
        id.model.clear();
        assert_eq!(id.check(), Err(TxtError::Missing("model")));
        let mut id = identity();
        id.homie_domain = Some("a/b".into());
        assert!(matches!(id.check(), Err(TxtError::InvalidHomieDomain(_))));
        let mut id = identity();
        id.name = Some("n".repeat(260));
        assert!(matches!(id.check(), Err(TxtError::StringTooLong { .. })));
    }

    #[test]
    fn broker_records() {
        let id = identity();
        let secure = BrokerService::new(BrokerType::SecureMqtt, 8883);
        assert_eq!(
            pairs(&secure.txt(&id, "host.local.")),
            p(&[
                ("txtvers", "1"),
                ("protocol", "mqtt-v5"),
                ("broker", "host.local"),
                ("device_id", "panel-1,meter-2")
            ])
        );
        let mut ws = BrokerService::new(BrokerType::MqttWs, 9001);
        ws.protocol = "mqtt-v3.1.1".into();
        assert_eq!(
            pairs(&ws.txt(&id, "host.local.")),
            p(&[
                ("txtvers", "1"),
                ("protocol", "mqtt-v3.1.1"),
                ("path", "/mqtt"),
                ("subprotocol", "mqtt")
            ])
        );
        let plain = BrokerService::new(BrokerType::Mqtt, 1883);
        assert_eq!(
            pairs(&plain.txt(&id, "h.local.")),
            p(&[("txtvers", "1"), ("protocol", "mqtt-v5")])
        );
    }

    #[test]
    fn records_order_and_ports() {
        let ad = Advertisement::new(identity())
            .http(HttpService::https(443, "/api/v1"))
            .broker(BrokerService::new(BrokerType::SecureMqtt, 8883))
            .broker(BrokerService::new(BrokerType::Mqtt, 1883));
        let r = ad.records("host.local.").unwrap();
        let types: Vec<(&str, u16)> = r.iter().map(|r| (r.service_type, r.port)).collect();
        assert_eq!(
            types,
            [
                ("_ebus._tcp", 443),
                ("_device-info._tcp", 0),
                ("_https._tcp", 443),
                ("_secure-mqtt._tcp", 8883),
                ("_mqtt._tcp", 1883)
            ]
        );
        let dup = ad
            .clone()
            .broker(BrokerService::new(BrokerType::Mqtt, 1884));
        assert_eq!(
            dup.records("h."),
            Err(TxtError::DuplicateBrokerType(BrokerType::Mqtt))
        );
    }

    #[test]
    fn brokers_without_broker_host_role_keep_roles() {
        let id = Identity::new(["d"], ["device"], "A", "M", "S");
        let ad = Advertisement::new(id).broker(BrokerService::new(BrokerType::Mqtt, 1883));
        let r = ad.records("h.local.").unwrap();
        assert_eq!(r[0].txt.get("roles").as_deref(), Some("device"));
    }

    #[test]
    fn instance_names() {
        let ad = Advertisement::new(identity());
        let names: Vec<String> = ad.instance_names("host.local.").collect();
        assert_eq!(names.len(), 99);
        assert_eq!(names[0], "panel-1");
        assert_eq!(names[1], "panel-1-2");
        assert_eq!(names[98], "panel-1-99");
        assert_eq!(ad.clone().instance("Main").base_instance_name("h."), "Main");
        let long = "d".repeat(61);
        let ad = Advertisement::new(Identity::new([long.as_str()], ["device"], "A", "M", "S"));
        assert_eq!(ad.base_instance_name("box.local."), "box");
        assert_eq!(ad.base_instance_name("box.example.com"), "box.example.com");
        let fits = "d".repeat(60);
        let ad = Advertisement::new(Identity::new([fits.as_str()], ["device"], "A", "M", "S"));
        assert_eq!(ad.base_instance_name("box.local."), fits);
    }
}
