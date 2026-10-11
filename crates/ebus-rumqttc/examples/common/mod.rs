//! What the examples share: the broker settings, broker discovery over
//! mDNS, and a stderr logger.
//!
//! | Variable | Meaning | Default |
//! |---|---|---|
//! | `EBUS_MQTT_HOST` | Configured broker host | `localhost` when `EBUS_MQTT_PORT` is set, else none |
//! | `EBUS_MQTT_PORT` | Configured broker port | 1883, or 8883 with TLS |
//! | `EBUS_BROKER_MODE` | `discovery-only`, `discovery-with-fallback` or `configured-only` | `configured-only` with a configured broker, else `discovery-only` |
//! | `EBUS_MDNS_BACKEND` | `auto`, `avahi` (Linux) or `mdns-sd` | `auto`: Avahi when avahi-daemon runs, else mdns-sd |
//! | `EBUS_MQTT_USER`, `EBUS_MQTT_PASS` | Credentials | none |
//! | `EBUS_MQTT_CA` | PEM CA file; enables TLS (`rustls` feature) | none |
//! | `EBUS_MQTT_INSECURE` | `1` enables TLS without verifying the broker's certificate, for lab brokers with self-signed certificates | unset |
//!
//! With neither `EBUS_MQTT_HOST` nor `EBUS_MQTT_PORT` set, the examples look
//! for a broker over mDNS until one is found: `_mqtt._tcp` without TLS,
//! `_secure-mqtt._tcp` with it. A broker found over mDNS is looked up again
//! under its instance name when connecting to it fails, from the first
//! connect on. A configured broker, including the one
//! `discovery-with-fallback` falls back to, is reached by its host name and
//! gets no such lookup.

use std::time::Duration;

use ebus_discovery_tokio::ebus_discovery::{BrokerConfig, BrokerEndpoint, BrokerMode, BrokerType};
use ebus_discovery_tokio::{Mdns, MdnsSdOptions, find_broker, reresolve};
use ebus_rumqttc::Reresolve;
use rumqttc::MqttOptions;

/// Largest incoming packet; a `$description` can exceed rumqttc's 10 KiB
/// default.
pub const MAX_PACKET: usize = 4 << 20;

/// How long a lookup of the same broker browses for it.
const RERESOLVE_WINDOW: Duration = Duration::from_secs(3);

/// Prints warnings and errors to stderr.
pub struct Stderr;

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

pub fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// The broker's options, and the hook that finds a discovered broker again.
pub struct Connect {
    pub mqtt: MqttOptions,
    pub reresolve: Option<Reresolve>,
}

/// What every connection takes from the environment, whichever broker it
/// goes to.
#[derive(Clone)]
struct Client {
    id: String,
    credentials: Option<(String, String)>,
    ca: Option<String>,
    insecure: bool,
}

impl Client {
    fn tls(&self) -> bool {
        self.ca.is_some() || self.insecure
    }

    /// Options for `broker`.
    fn options(&self, broker: &BrokerEndpoint) -> Result<MqttOptions, String> {
        let mut mqtt = MqttOptions::new(self.id.clone(), broker.connect_host(), broker.port);
        mqtt.set_max_packet_size(MAX_PACKET, MAX_PACKET);
        if let Some((user, pass)) = &self.credentials {
            mqtt.set_credentials(user.clone(), pass.clone());
        }
        if broker.broker_type.uses_tls() {
            mqtt.set_transport(tls::transport(self.ca.as_deref(), self.insecure)?);
        }
        Ok(mqtt)
    }
}

/// The broker for `client_id` from the variables above: the configured one,
/// or one found over mDNS.
pub async fn connect(client_id: String) -> Result<Connect, String> {
    let client = Client {
        id: client_id,
        credentials: env("EBUS_MQTT_USER").map(|u| (u, env("EBUS_MQTT_PASS").unwrap_or_default())),
        ca: env("EBUS_MQTT_CA"),
        insecure: env("EBUS_MQTT_INSECURE").as_deref() == Some("1"),
    };
    let tls = client.tls();
    let broker_type = if tls {
        BrokerType::SecureMqtt
    } else {
        BrokerType::Mqtt
    };
    let host = env("EBUS_MQTT_HOST");
    let port = env("EBUS_MQTT_PORT")
        .map(|p| {
            p.parse::<u16>()
                .map_err(|_| format!("EBUS_MQTT_PORT {p:?} is not a port"))
        })
        .transpose()?;
    let mut cfg = BrokerConfig::default();
    if host.is_some() || port.is_some() {
        cfg.url = Some(BrokerEndpoint::configured(
            broker_type,
            host.as_deref().unwrap_or("localhost"),
            port.unwrap_or_else(|| broker_type.default_port()),
        ));
    }
    cfg.mode = env("EBUS_BROKER_MODE")
        .map(|m| BrokerMode::parse(&m).map_err(|e| format!("EBUS_BROKER_MODE {m:?}: {e}")))
        .transpose()?;
    cfg.tls = tls;
    // The TLS settings decide the one transport the example can use.
    cfg.accept = Some(vec![broker_type]);

    if cfg.effective_mode() == BrokerMode::ConfiguredOnly {
        let url = cfg
            .url
            .as_ref()
            .ok_or("EBUS_BROKER_MODE=configured-only needs EBUS_MQTT_HOST or EBUS_MQTT_PORT")?;
        return Ok(Connect {
            mqtt: client.options(url)?,
            reresolve: None,
        });
    }

    let mdns = mdns().await?;
    eprintln!(
        "looking for a {} broker over mDNS ({mdns:?}, {})",
        broker_type.as_str(),
        cfg.effective_mode().as_str()
    );
    let broker = find_broker(&mdns, cfg.clone())
        .await
        .map_err(|e| format!("broker discovery: {e}"))?;
    match &broker.instance {
        Some(name) => eprintln!("found broker {name:?} at {}", broker.url()),
        None => eprintln!("no broker found; using {}", broker.url()),
    }
    let mqtt = client.options(&broker)?;
    let hook: Option<Reresolve> = broker.instance.is_some().then(|| {
        let hook: Reresolve = Box::new(move || {
            let (m, c, b, client) = (mdns.clone(), cfg.clone(), broker.clone(), client.clone());
            Box::pin(async move {
                let found = reresolve(&m, &b, &c, RERESOLVE_WINDOW)
                    .await
                    .ok()
                    .flatten()?;
                client.options(&found).ok()
            })
        });
        hook
    });
    Ok(Connect {
        mqtt,
        reresolve: hook,
    })
}

/// The mDNS backend `EBUS_MDNS_BACKEND` names.
async fn mdns() -> Result<Mdns, String> {
    let backend = env("EBUS_MDNS_BACKEND").unwrap_or_else(|| "auto".to_owned());
    let mdns = match backend.as_str() {
        "auto" => Mdns::system().await,
        "mdns-sd" => Mdns::mdns_sd(MdnsSdOptions::default()),
        #[cfg(target_os = "linux")]
        "avahi" => Mdns::avahi().await,
        #[cfg(not(target_os = "linux"))]
        "avahi" => return Err("EBUS_MDNS_BACKEND=avahi needs Linux".to_owned()),
        other => {
            return Err(format!(
                "EBUS_MDNS_BACKEND {other:?}: auto, avahi or mdns-sd"
            ));
        }
    };
    mdns.map_err(|e| format!("mDNS backend {backend}: {e}"))
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
        Err("TLS needs the rustls feature: cargo run -p ebus-rumqttc --example <name> --features rustls".to_owned())
    }
}
