//! Connects to a SPAN panel's MQTTS broker and reports whether the home is
//! islanded from the grid: it watches the MID child's `grid` node
//! (`islanding-state`, `grid-state`, `grid-forming-entity`) and the
//! enclosure's `power-flows` node. A port of python-sdk's
//! `simple-span-controller`.
//!
//! ```text
//! cargo run -p ebus-rumqttc --example simple-span-controller --features rustls -- <serial-number> [password]
//! cargo run -p ebus-rumqttc --example simple-span-controller --features rustls -- <serial-number> --broker-host 192.0.2.10
//! ```
//!
//! | Option | Meaning | Default |
//! |---|---|---|
//! | `password` | The panel's eBus broker password | `ebus_broker_password` of the panel in the span-auth file |
//! | `--device-id` | The enclosure's device id | the serial number |
//! | `--broker-host` | Connect to this host instead of looking for the panel over mDNS | none |
//! | `--broker-port` | The port of `--broker-host`, and of the panel's host name when mDNS does not find it | 8883 |
//! | `--insecure` | Do not verify the broker's certificate | off |
//! | `--debug` | Print the SDK's debug log | off |
//!
//! The span-auth file is `SPAN_AUTH_FILE`, else `~/.span-auth.json`, as
//! SPAN-API-Client-Docs' `span_auth_utils` keeps it. The panel's CA
//! certificate is cached as `<serial-number>.crt` in `SPAN_CA_CERT_DIR`,
//! else `~/.span-ca-certs`, and downloaded from
//! `http://<host>/api/v2/certificate/ca` when missing or expiring within a
//! day. When it cannot be had, the connection is made without verifying the
//! broker's certificate, with a warning.
//!
//! Without `--broker-host`, the panel is the `_secure-mqtt._tcp`
//! advertisement whose SRV target is `span-<serial-number>.local`, so
//! another broker on the network never receives the panel's credentials.
//! It is connected to at its advertised IPv4 address, and its certificate
//! verified under that host name. When it is not heard within 5 s, the
//! example connects to `span-<serial-number>.local` directly. The user name
//! is the serial number.
//!
//! The controller discovers every device on the broker (`Config::wildcard`),
//! as python-sdk's `Controller` does without a root, and takes the first
//! description of type `energy.ebus.device.mid` as the MID.
//!
//! Ctrl-C unsubscribes and disconnects.

#[path = "common/span.rs"]
mod span;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use ebus_discovery_tokio::Mdns;
use ebus_discovery_tokio::ebus_discovery::{BrokerEndpoint, BrokerType};
use ebus_rumqttc::ebus_homie_tokio::ControllerOptions;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::Config;
use rumqttc::tokio_rustls::rustls::ClientConfig;
use rumqttc::{MqttOptions, TlsConfiguration, Transport};

use span::{Args, CaSource, Line, SpanAuth};

/// How long a lookup of the same broker browses for it.
const RERESOLVE_WINDOW: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() -> ExitCode {
    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    if log::set_logger(&span::Stderr).is_ok() {
        log::set_max_level(if args.debug {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Warn
        });
    }
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

/// The broker, and the mDNS backend it was found with.
struct Broker {
    endpoint: BrokerEndpoint,
    mdns: Option<Mdns>,
}

async fn run(args: Args) -> Result<(), String> {
    let password = match args.password.clone() {
        Some(p) => p,
        None => {
            let path = span::auth_file_path()?;
            if let Some(w) = span::permission_warning(&path) {
                eprintln!("warning: {w}");
            }
            let password = SpanAuth::load(&path)?
                .password(Some(&args.serial))
                .ok_or_else(|| format!("no password for {} in {}", args.serial, path.display()))?;
            println!("using the password from {}", path.display());
            password
        }
    };
    let panel_host = span::panel_host(&args.serial);
    let broker = broker(&args, &panel_host).await;
    // The name the broker's certificate is verified under.
    let tls_name = args.broker_host.clone().unwrap_or(panel_host.clone());

    let ca = if args.insecure {
        eprintln!("warning: --insecure: the broker's certificate is not verified");
        None
    } else {
        let source = CaSource {
            host: tls_name.clone(),
            connect: broker.endpoint.connect_host().into_owned(),
            port: span::HTTP_PORT,
        };
        let path = span::ca_cert_path(&span::ca_cert_dir()?, &args.serial);
        match span::ensure_ca_cert(&path, &source, span::unix_now()).await {
            Ok(pem) => Some(pem),
            Err(e) => {
                eprintln!("warning: CA certificate unavailable ({e}); using insecure TLS");
                None
            }
        }
    };
    let tls = span::tls::client_config(ca.as_deref(), &tls_name)?;
    let connect = {
        let (id, serial) = (ebus_rumqttc::controller_client_id(), args.serial.clone());
        move |ep: &BrokerEndpoint| options(&id, &serial, &password, ep, Arc::clone(&tls))
    };

    let mqtt = connect(&broker.endpoint);
    println!(
        "connecting to the MQTTS broker at {}:{} as {}",
        broker.endpoint.connect_host(),
        broker.endpoint.port,
        args.serial
    );
    let hook = match (&broker.mdns, &broker.endpoint.instance) {
        (Some(mdns), Some(_)) => Some(span::panel_reresolve(
            mdns.clone(),
            broker.endpoint.clone(),
            panel_host,
            RERESOLVE_WINDOW,
            connect,
        )),
        _ => None,
    };

    let run = ebus_rumqttc::controller(mqtt, Config::wildcard(), ControllerOptions::default())
        .map_err(|e| e.to_string())?;
    let handle = run.handle;
    let mut events = run.events;
    let mut task = match hook {
        Some(f) => tokio::spawn(run.task.reresolve(f).run()),
        None => tokio::spawn(run.task.run()),
    };
    let enclosure = args.device_id.as_deref().unwrap_or(&args.serial);
    println!(
        "monitoring islanding (MID {}) and enclosure {}",
        span::MID_NODE,
        span::ENCLOSURE_NODE
    );
    println!("press Ctrl-C to exit");
    let stop = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let mut out = print;
    let monitor = span::monitor(
        &handle,
        &mut events,
        enclosure,
        span::WAITING_EVERY,
        stop,
        &mut out,
    );
    tokio::select! {
        () = monitor => {}
        res = &mut task => return finished(res),
    }
    finished(task.await)
}

fn finished(
    res: Result<Result<(), ebus_rumqttc::Error>, tokio::task::JoinError>,
) -> Result<(), String> {
    match res {
        Ok(Err(e)) => Err(match std::error::Error::source(&e) {
            Some(s) => format!("{e}: {s}"),
            None => e.to_string(),
        }),
        Ok(Ok(())) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}

fn print(line: Line) {
    match line {
        Line::Info(s) => println!("{s}"),
        Line::Warn(s) => eprintln!("warning: {s}"),
    }
}

/// `--broker-host`, else the panel found over mDNS, else its host name.
async fn broker(args: &Args, panel_host: &str) -> Broker {
    if let Some(host) = &args.broker_host {
        println!("using the broker host {host}:{}", args.broker_port);
        return Broker {
            endpoint: BrokerEndpoint::configured(BrokerType::SecureMqtt, host, args.broker_port),
            mdns: None,
        };
    }
    let direct =
        || BrokerEndpoint::configured(BrokerType::SecureMqtt, panel_host, args.broker_port);
    println!("looking for {panel_host} over mDNS");
    let mdns = match Mdns::system().await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("warning: no mDNS ({e}); using the host name {panel_host} directly");
            return Broker {
                endpoint: direct(),
                mdns: None,
            };
        }
    };
    let endpoint = match span::discover(&mdns, panel_host, span::DISCOVERY_WINDOW).await {
        Ok(Some(ep)) => {
            println!(
                "discovered the SPAN panel {} at {}:{}",
                ep.host,
                ep.connect_host(),
                ep.port
            );
            ep
        }
        Ok(None) => {
            eprintln!(
                "warning: mDNS discovery timed out; using the host name {panel_host} directly"
            );
            direct()
        }
        Err(e) => {
            eprintln!("warning: {e}; using the host name {panel_host} directly");
            direct()
        }
    };
    Broker {
        endpoint,
        mdns: Some(mdns),
    }
}

/// Options for the broker `ep`, as the serial number.
fn options(
    id: &str,
    serial: &str,
    password: &str,
    ep: &BrokerEndpoint,
    tls: Arc<ClientConfig>,
) -> MqttOptions {
    let mut mqtt = MqttOptions::new(id, ep.connect_host(), ep.port);
    mqtt.set_max_packet_size(span::MAX_PACKET, span::MAX_PACKET);
    mqtt.set_credentials(serial, password);
    mqtt.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(tls)));
    mqtt
}
