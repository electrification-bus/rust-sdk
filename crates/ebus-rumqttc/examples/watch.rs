//! Read-only eBus watcher: discovers devices, prints one line per controller
//! event and a summary line after each `DeviceSynced`. It never sends `/set`.
//!
//! ```text
//! cargo run -p ebus-rumqttc --example watch -- wildcard
//! cargo run -p ebus-rumqttc --example watch -- tree <root>
//! cargo run -p ebus-rumqttc --example watch --features rustls -- wildcard   # TLS
//! ```
//!
//! | Variable | Meaning | Default |
//! |---|---|---|
//! | `EBUS_MQTT_HOST` | Broker host | `localhost` |
//! | `EBUS_MQTT_PORT` | Broker port | 1883, or 8883 with TLS |
//! | `EBUS_MQTT_USER`, `EBUS_MQTT_PASS` | Credentials | none |
//! | `EBUS_MQTT_CA` | PEM CA file; enables TLS (`rustls` feature) | none |
//! | `EBUS_MQTT_INSECURE` | `1` enables TLS without verifying the broker's certificate, for lab brokers with self-signed certificates | unset |
//!
//! Warnings from the controller and the connection go to stderr. Ctrl-C
//! unsubscribes and disconnects.

use std::process::ExitCode;
use std::time::Instant;

use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::{Config, Event, Origin, Value};
use ebus_rumqttc::ebus_homie_tokio::ebus_mqtt::Link;
use ebus_rumqttc::ebus_homie_tokio::{AttachOptions, ControllerHandle, Delivered};
use rumqttc::MqttOptions;

/// Largest incoming packet; a `$description` can exceed rumqttc's 10 KiB
/// default.
const MAX_PACKET: usize = 4 << 20;

/// Prints warnings and errors to stderr.
struct Stderr;

impl log::Log for Stderr {
    fn enabled(&self, m: &log::Metadata<'_>) -> bool {
        m.level() <= log::Level::Warn
    }

    fn log(&self, r: &log::Record<'_>) {
        if self.enabled(r.metadata()) {
            eprintln!("{} {}: {}", r.level(), r.target(), r.args());
        }
    }

    fn flush(&self) {}
}

fn usage() -> String {
    "usage: watch wildcard | watch tree <root>".to_owned()
}

fn config(args: &[String]) -> Result<Config, String> {
    match args {
        [mode] if mode == "wildcard" => Ok(Config::wildcard()),
        [mode, root] if mode == "tree" => {
            Config::tree(root).map_err(|e| format!("invalid root {root:?}: {e}"))
        }
        _ => Err(usage()),
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn options() -> Result<MqttOptions, String> {
    let host = env("EBUS_MQTT_HOST").unwrap_or_else(|| "localhost".to_owned());
    let ca = env("EBUS_MQTT_CA");
    let insecure = env("EBUS_MQTT_INSECURE").as_deref() == Some("1");
    let tls = ca.is_some() || insecure;
    let port = match env("EBUS_MQTT_PORT") {
        Some(p) => p
            .parse::<u16>()
            .map_err(|_| format!("EBUS_MQTT_PORT {p:?} is not a port"))?,
        None if tls => 8883,
        None => 1883,
    };
    let mut mqtt = MqttOptions::new(ebus_rumqttc::client_id(), host, port);
    mqtt.set_max_packet_size(MAX_PACKET, MAX_PACKET);
    if let Some(user) = env("EBUS_MQTT_USER") {
        mqtt.set_credentials(user, env("EBUS_MQTT_PASS").unwrap_or_default());
    }
    if tls {
        mqtt.set_transport(tls::transport(ca.as_deref(), insecure)?);
    }
    Ok(mqtt)
}

#[cfg(feature = "rustls")]
mod tls {
    use std::sync::Arc;

    use rumqttc::tokio_rustls::rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use rumqttc::tokio_rustls::rustls::crypto::{
        CryptoProvider, verify_tls12_signature, verify_tls13_signature,
    };
    use rumqttc::tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rumqttc::tokio_rustls::rustls::{
        self, ClientConfig, DigitallySignedStruct, SignatureScheme,
    };
    use rumqttc::{TlsConfiguration, Transport};

    /// Accepts any server certificate; signatures are still checked.
    #[derive(Debug)]
    struct NoVerify(Arc<CryptoProvider>);

    impl ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    /// TLS verified against the PEM CA at `ca`, or unverified with `insecure`.
    pub fn transport(ca: Option<&str>, insecure: bool) -> Result<Transport, String> {
        if insecure {
            eprintln!("warning: EBUS_MQTT_INSECURE=1, the broker's certificate is not verified");
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            let config = ClientConfig::builder_with_provider(Arc::clone(&provider))
                .with_safe_default_protocol_versions()
                .map_err(|e| format!("TLS: {e}"))?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
                .with_no_client_auth();
            return Ok(Transport::tls_with_config(TlsConfiguration::Rustls(
                Arc::new(config),
            )));
        }
        let path = ca.ok_or("EBUS_MQTT_CA is not set")?;
        let ca = std::fs::read(path).map_err(|e| format!("EBUS_MQTT_CA {path}: {e}"))?;
        Ok(Transport::tls_with_config(TlsConfiguration::Simple {
            ca,
            alpn: None,
            client_auth: None,
        }))
    }
}

#[cfg(not(feature = "rustls"))]
mod tls {
    /// TLS needs the `rustls` feature.
    pub fn transport(_ca: Option<&str>, _insecure: bool) -> Result<rumqttc::Transport, String> {
        Err("TLS needs the rustls feature: cargo run -p ebus-rumqttc --example watch --features rustls -- ...".to_owned())
    }
}

fn value(v: &Value) -> String {
    match &v.check {
        Ok(()) => format!("{:?}", &*v.raw),
        Err(p) => format!("{:?} invalid={p}", &*v.raw),
    }
}

fn line(e: &Event) -> String {
    match e {
        Event::Link(Link::Up { session_present }) => {
            format!("link up session_present={session_present}")
        }
        Event::Link(Link::Down) => "link down".to_owned(),
        Event::DeviceDiscovered { device, state } => format!("discovered {device} state={state}"),
        Event::StateChanged {
            device,
            old,
            new,
            effective,
        } => {
            let old = old.map_or_else(|| "-".to_owned(), |s| s.to_string());
            format!("state {device} {old} -> {new} effective={effective}")
        }
        Event::EffectiveStateChanged { device, old, new } => {
            format!("effective {device} {old} -> {new}")
        }
        Event::DescriptionChanged {
            device,
            description,
            ignored,
            while_ready,
        } => format!(
            "description {device} version={} nodes={} children={} ignored={} while_ready={while_ready}",
            description.version,
            description.nodes.len(),
            description.children.len(),
            ignored.len()
        ),
        Event::DescriptionRejected { device, error } => {
            format!("description-rejected {device} {error}")
        }
        Event::PropertyChanged {
            addr,
            value: v,
            origin,
        } => {
            let origin = match origin {
                Origin::Retained => "retained",
                Origin::Live => "live",
                _ => "unknown",
            };
            format!(
                "property {}/{}/{} = {} {origin}",
                addr.device,
                addr.node,
                addr.property,
                value(v)
            )
        }
        Event::PropertyEvent {
            addr,
            value: v,
            origin,
        } => {
            let origin = match origin {
                Origin::Retained => " (retained replay)",
                _ => "",
            };
            format!(
                "event {}/{}/{} = {}{origin}",
                addr.device,
                addr.node,
                addr.property,
                value(v)
            )
        }
        Event::PropertyCleared { addr } => {
            format!("cleared {}/{}/{}", addr.device, addr.node, addr.property)
        }
        Event::TargetChanged { addr, target } => format!(
            "target {}/{}/{} = {}",
            addr.device,
            addr.node,
            addr.property,
            target.as_ref().map_or_else(|| "-".to_owned(), value)
        ),
        Event::Alert {
            device,
            alert,
            message,
        } => format!("alert {device}/{alert} = {message:?}"),
        Event::DeviceSynced { device, missing } => format!("synced {device} missing={missing}"),
        Event::DeviceStuck {
            device,
            missing,
            attempt,
            gave_up,
        } => format!("stuck {device} missing={missing:?} attempt={attempt} gave_up={gave_up}"),
        Event::DeviceAbsent { device } => format!("absent {device}"),
        Event::DeviceRemoved { device } => format!("removed {device}"),
        Event::TreeComplete { root } => format!("tree-complete {root}"),
        other => format!("{other:?}"),
    }
}

fn summary(handle: &ControllerHandle, started: Instant) -> String {
    let devices = handle.devices();
    let synced = devices.iter().filter(|d| d.synced).count();
    let (mut properties, mut values) = (0usize, 0usize);
    for d in &devices {
        for (_, _, p) in d.properties() {
            properties += 1;
            values += usize::from(p.value.is_some());
        }
    }
    format!(
        "summary devices={} synced={synced} properties={properties} values={values} elapsed_ms={}",
        devices.len(),
        started.elapsed().as_millis()
    )
}

#[tokio::main]
async fn main() -> ExitCode {
    if log::set_logger(&Stderr).is_ok() {
        log::set_max_level(log::LevelFilter::Warn);
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (config, mqtt) = match config(&args).and_then(|c| Ok((c, options()?))) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let broker = mqtt.broker_address();
    let run = match ebus_rumqttc::controller(mqtt, config, AttachOptions::default()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    println!("connecting {}:{}", broker.0, broker.1);
    let started = Instant::now();
    let handle = run.handle;
    let mut events = run.events;
    let mut task = tokio::spawn(run.task.run());

    loop {
        tokio::select! {
            d = events.recv() => match d {
                Some(Delivered::Event(e)) => {
                    println!("{}", line(&e));
                    if matches!(e, Event::DeviceSynced { .. }) {
                        println!("{}", summary(&handle, started));
                    }
                }
                Some(Delivered::Overflow { dropped }) => {
                    println!("overflow dropped={dropped}");
                    println!("{}", summary(&handle, started));
                }
                None => break,
            },
            _ = tokio::signal::ctrl_c() => {
                handle.shutdown().await;
                break;
            }
        }
    }
    match (&mut task).await {
        Ok(Ok(())) => ExitCode::SUCCESS,
        Ok(Err(e)) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
