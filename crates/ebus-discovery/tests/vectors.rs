//! python-service-discovery vectors in `vectors/python-service-discovery/`
//! (see `vectors/README.md`).
//!
//! Each test runs every case of one suite file and reports all failing case ids.

#![cfg(feature = "alloc")]

use ebus_discovery::{
    AddrScope, Advertisement, BROKER_TYPES, BrokerConfig, BrokerEndpoint, BrokerMode, BrokerSearch,
    BrokerService, BrokerType, HttpService, Identity, Instance, InstanceKey, Instant, ScopedAddr,
    TxtRecord, candidate_addresses, check_txt, match_configured, parse_ebus_txt, rank_brokers,
    select_broker,
};
use serde_json::{Value, json};

fn load(suite: &str) -> Vec<Value> {
    let path = format!(
        "{}/../../vectors/python-service-discovery/{suite}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let doc: Value = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path}: {e}"));
    assert_eq!(
        doc["suite"],
        format!("python-service-discovery/{suite}"),
        "{path}: suite name"
    );
    match doc.get("cases") {
        Some(Value::Array(cases)) => cases.clone(),
        _ => panic!("{path}: no cases array"),
    }
}

/// Run `check` on every case of `suite`; fail listing every case that failed.
fn run(suite: &str, check: impl Fn(&Value) -> Result<Value, String>) {
    let cases = load(suite);
    assert!(!cases.is_empty(), "{suite}: no cases");
    let mut failures = Vec::new();
    for case in &cases {
        let id = case["id"].as_str().unwrap_or("<no id>");
        match check(&case["input"]) {
            Ok(got) if got == case["expect"] => {}
            Ok(got) => failures.push(format!("  {id}: got {got}, want {}", case["expect"])),
            Err(e) => failures.push(format!("  {id}: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{suite}: {} of {} cases failed:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

fn s<'a>(v: &'a Value, key: &str) -> Result<&'a str, String> {
    v[key]
        .as_str()
        .ok_or_else(|| format!("{key:?} is not a string"))
}

fn opt_s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(String::from)
}

fn strs(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn str_pairs(v: &Value) -> Vec<(String, String)> {
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|p| (p[0].as_str().unwrap().into(), p[1].as_str().unwrap().into()))
                .collect()
        })
        .unwrap_or_default()
}

fn hex(h: &str) -> Vec<u8> {
    (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
        .collect()
}

fn txt_strings(input: &Value) -> Vec<Vec<u8>> {
    match input.get("strings_hex") {
        Some(h) => strs(h).iter().map(|x| hex(x)).collect(),
        None => strs(&input["strings"])
            .into_iter()
            .map(String::into_bytes)
            .collect(),
    }
}

fn txt_pairs(txt: &TxtRecord) -> Value {
    Value::Array(
        txt.iter()
            .map(|(k, _)| json!([k, txt.get(k).unwrap()]))
            .collect(),
    )
}

fn txt_from(pairs: &Value) -> TxtRecord {
    let mut txt = TxtRecord::new();
    for (k, v) in str_pairs(pairs) {
        txt.insert(&k, &v);
    }
    txt
}

fn addr(text: &str) -> ScopedAddr {
    let (ip, zone) = match text.split_once('%') {
        Some((ip, z)) => (ip, Some(z.parse().unwrap())),
        None => (text, None),
    };
    ScopedAddr {
        ip: ip.parse().unwrap(),
        ifindex: zone,
    }
}

fn addr_text(a: &ScopedAddr) -> String {
    match a.ifindex {
        Some(i) => format!("{}%{i}", a.ip),
        None => a.ip.to_string(),
    }
}

fn instance(v: &Value) -> Instance {
    Instance::new(
        InstanceKey {
            service_type: v["service_type"].as_str().unwrap().into(),
            name: v["instance_name"].as_str().unwrap().into(),
        },
        v["server"].as_str().unwrap().into(),
        v["port"].as_u64().unwrap() as u16,
        txt_from(&v["txt"]),
        strs(&v["addresses"]).iter().map(|a| addr(a)).collect(),
    )
}

fn endpoints(instances: &Value) -> Vec<BrokerEndpoint> {
    instances
        .as_array()
        .unwrap()
        .iter()
        .map(|i| BrokerEndpoint::from_instance(&instance(i)).unwrap())
        .collect()
}

/// Python's ranking order: the specification's, narrowed to the accepted
/// types (`null`: all four).
fn accept(v: &Value) -> Vec<BrokerType> {
    let accepted: Option<Vec<BrokerType>> = v.as_array().map(|a| {
        a.iter()
            .map(|t| BrokerType::parse(t.as_str().unwrap()).unwrap())
            .collect()
    });
    BROKER_TYPES
        .iter()
        .copied()
        .filter(|t| accepted.as_ref().is_none_or(|a| a.contains(t)))
        .collect()
}

fn endpoint_out(ep: Option<&BrokerEndpoint>) -> Value {
    match ep {
        Some(ep) => json!({"url": ep.url(), "instance": ep.instance}),
        None => Value::Null,
    }
}

fn identity(v: &Value) -> Identity {
    let mut id = Identity::new(
        strs(&v["device_ids"]),
        strs(&v["roles"]),
        v["manufacturer"].as_str().unwrap(),
        v["model"].as_str().unwrap(),
        v["serial_number"].as_str().unwrap(),
    );
    if let Some(e) = opt_s(v, "ebus_version") {
        id.ebus_version = e;
    }
    id.device_type = opt_s(v, "device_type");
    id.name = opt_s(v, "name");
    id.fw_version = opt_s(v, "fw_version");
    id.register = opt_s(v, "register");
    id.broker_ca = opt_s(v, "broker_ca");
    id.auth_methods = strs(&v["auth_methods"]);
    id.hw_version = opt_s(v, "hw_version");
    id.os_version = opt_s(v, "os_version");
    id.mac = opt_s(v, "mac");
    id.extra_ebus_txt = str_pairs(&v["extra_ebus_txt"]);
    id.extra_device_info_txt = str_pairs(&v["extra_device_info_txt"]);
    id.homie_domain = opt_s(v, "homie_domain");
    id.homie_version = opt_s(v, "homie_version");
    id
}

#[test]
fn ebus_version_matches_spec_sync() {
    let path = format!("{}/../../.ebus-spec.json", env!("CARGO_MANIFEST_DIR"));
    let spec: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(spec["framework"], ebus_discovery::EBUS_VERSION);
}

#[test]
fn decode_txt() {
    run("decode-txt", |input| {
        Ok(json!({"txt": txt_pairs(&TxtRecord::from_strings(txt_strings(input)))}))
    });
}

#[test]
fn parse_ebus_txt_vectors() {
    run("parse-ebus-txt", |input| {
        let e = parse_ebus_txt(&TxtRecord::from_strings(txt_strings(input)));
        Ok(json!({
            "txtvers": e.txtvers, "ebus_version": e.ebus_version, "roles": e.roles,
            "device_ids": e.device_ids, "auth_methods": e.auth_methods,
            "missing": e.missing(), "is_broker_host": e.is_broker_host(),
        }))
    });
}

#[test]
fn check_txt_vectors() {
    run("check-txt", |input| {
        let txt = txt_from(&input["txt"]);
        Ok(match check_txt(s(input, "service_type")?, &txt) {
            Err(_) => json!({"valid": false}),
            Ok(r) => json!({
                "valid": true,
                "long_keys": r.long_strings.iter().map(|(k, _)| k).collect::<Vec<_>>(),
                "large": r.is_large(),
            }),
        })
    });
}

#[test]
fn service_txt() {
    run("service-txt", |input| {
        let id = identity(&input["identity"]);
        match s(input, "record")? {
            "identity" => Ok(match id.check() {
                Err(_) => json!({"error": true}),
                Ok(()) => json!({
                    "ebus": txt_pairs(&id.ebus_txt()),
                    "device_info": txt_pairs(&id.device_info_txt()),
                }),
            }),
            "http" => {
                let h = &input["http"];
                let port = h["port"].as_u64().unwrap() as u16;
                let path = s(h, "path")?;
                let mut svc = if h["tls"] == true {
                    HttpService::https(port, path)
                } else {
                    HttpService::http(port, path)
                };
                if let Some(v) = opt_s(h, "version") {
                    svc.version = v;
                }
                svc.openapi = opt_s(h, "openapi");
                svc.extra_txt = str_pairs(&h["extra_txt"]);
                Ok(json!({"service_type": svc.service_type(), "txt": txt_pairs(&svc.txt(&id))}))
            }
            "broker" => {
                let b = &input["broker"];
                let t = BrokerType::parse(s(b, "service_type")?).ok_or("broker type")?;
                let mut svc = BrokerService::new(t, b["port"].as_u64().unwrap() as u16);
                if let Some(p) = opt_s(b, "protocol") {
                    svc.protocol = p;
                }
                svc.broker = opt_s(b, "broker");
                if let Some(p) = opt_s(b, "path") {
                    svc.path = p;
                }
                if let Some(p) = opt_s(b, "subprotocol") {
                    svc.subprotocol = p;
                }
                svc.extra_txt = str_pairs(&b["extra_txt"]);
                Ok(json!({"txt": txt_pairs(&svc.txt(&id, s(input, "server")?))}))
            }
            other => Err(format!("unknown record {other}")),
        }
    });
}

#[test]
fn broker_endpoint() {
    run("broker-endpoint", |input| {
        let ep = BrokerEndpoint::from_instance(&instance(input)).ok_or("not a broker")?;
        Ok(json!({
            "broker_type": ep.broker_type.as_str(), "host": ep.host, "port": ep.port,
            "server": ep.server, "device_id": ep.device_id, "url": ep.url(),
            "instance": ep.instance, "addrs": ep.addrs.iter().map(addr_text).collect::<Vec<_>>(),
        }))
    });
}

#[test]
fn broker_url() {
    run("broker-url", |input| {
        Ok(match BrokerEndpoint::from_url(s(input, "url")?) {
            Err(_) => json!({"error": true}),
            Ok(ep) => json!({
                "broker_type": ep.broker_type.as_str(), "host": ep.host, "port": ep.port, "url": ep.url(),
            }),
        })
    });
}

#[test]
fn rank_brokers_vectors() {
    run("rank-brokers", |input| {
        let acc = accept(&input["accept"]);
        let ranked = rank_brokers(endpoints(&input["instances"]), &acc);
        Ok(json!({"ranked": ranked.iter().map(|e| endpoint_out(Some(e))).collect::<Vec<_>>()}))
    });
}

#[test]
fn match_configured_vectors() {
    run("match-configured", |input| {
        let ranked = rank_brokers(endpoints(&input["instances"]), BROKER_TYPES);
        let conf = BrokerEndpoint::from_url(s(input, "url")?).map_err(|e| e.to_string())?;
        Ok(json!({"match": endpoint_out(match_configured(&conf, &ranked).as_ref())}))
    });
}

#[test]
fn select_broker_vectors() {
    run("select-broker", |input| {
        let mode = BrokerMode::parse(s(input, "mode")?).map_err(|e| e.to_string())?;
        let acc = accept(&input["accept"]);
        let ranked = rank_brokers(endpoints(&input["instances"]), &acc);
        let conf = match input["url"].as_str() {
            Some(u) => Some(BrokerEndpoint::from_url(u).map_err(|e| e.to_string())?),
            None => None,
        };
        let allow = input["allow_unmatched"] == true;
        let chosen = select_broker(mode, conf.as_ref(), &ranked, allow);
        Ok(json!({"chosen": endpoint_out(chosen.as_ref())}))
    });
}

#[test]
fn broker_mode() {
    run("broker-mode", |input| {
        Ok(match BrokerMode::parse(s(input, "text")?) {
            Ok(m) => json!({"mode": m.as_str()}),
            Err(_) => json!({"error": true}),
        })
    });
}

/// The search asks for a requery at each attempt after the first, after
/// python's delay before that attempt.
#[test]
fn retry_schedule() {
    run("retry-schedule", |input| {
        let attempts = input["attempts"].as_u64().unwrap() as usize;
        let mut search = BrokerSearch::new(Instant(0), BrokerConfig::default()).unwrap();
        let mut delays = vec![0.0];
        let mut last = 0;
        while delays.len() < attempts {
            let at = search.poll_timeout().ok_or("no timeout")?;
            search.handle_timeout(at);
            if search.poll_requery() {
                delays.push((at.0 - last) as f64 / 1000.0);
                last = at.0;
            }
        }
        Ok(json!({"delays_s": delays}))
    });
}

#[test]
fn addresses() {
    run("addresses", |input| {
        if let Some(a) = input["address"].as_str() {
            let scope = AddrScope::of(addr(a).ip);
            return Ok(json!({"scope": scope.as_str(), "preference": scope.preference()}));
        }
        let addrs: Vec<ScopedAddr> = strs(&input["addresses"]).iter().map(|a| addr(a)).collect();
        let c = candidate_addresses(&addrs);
        Ok(json!({"candidates": c.iter().map(addr_text).collect::<Vec<_>>()}))
    });
}

#[test]
fn instance_name() {
    run("instance-name", |input| {
        let id = Identity::new(strs(&input["device_ids"]), ["device"], "A", "M", "S");
        let mut ad = Advertisement::new(id);
        ad.instance = opt_s(input, "instance_name");
        let names: Vec<String> = ad.instance_names(s(input, "server")?).collect();
        Ok(json!({"names": names}))
    });
}
