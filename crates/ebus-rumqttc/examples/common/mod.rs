//! What the examples share: the `EBUS_MQTT_*` connection settings and a
//! stderr logger.
//!
//! | Variable | Meaning | Default |
//! |---|---|---|
//! | `EBUS_MQTT_HOST` | Broker host | `localhost` |
//! | `EBUS_MQTT_PORT` | Broker port | 1883, or 8883 with TLS |
//! | `EBUS_MQTT_USER`, `EBUS_MQTT_PASS` | Credentials | none |
//! | `EBUS_MQTT_CA` | PEM CA file; enables TLS (`rustls` feature) | none |
//! | `EBUS_MQTT_INSECURE` | `1` enables TLS without verifying the broker's certificate, for lab brokers with self-signed certificates | unset |

use rumqttc::MqttOptions;

/// Largest incoming packet; a `$description` can exceed rumqttc's 10 KiB
/// default.
pub const MAX_PACKET: usize = 4 << 20;

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

/// Options for `client_id` from the `EBUS_MQTT_*` variables.
pub fn options(client_id: String) -> Result<MqttOptions, String> {
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
    let mut mqtt = MqttOptions::new(client_id, host, port);
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
        Err("TLS needs the rustls feature: cargo run -p ebus-rumqttc --example <name> --features rustls".to_owned())
    }
}
