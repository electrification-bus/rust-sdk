//! The SPAN panel parts of the `simple-span-controller` example: its command
//! line, the span-auth credential file and the panel's cached CA certificate
//! as `span_auth_utils` keeps them, finding the panel's broker over mDNS, TLS
//! verified against the panel's CA, and the monitor that reports the
//! panel's islanding state and power flows.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ebus_discovery_tokio::ebus_discovery::{
    BrokerConfig, BrokerEndpoint, BrokerType, Instance, InstanceTable, Sighting, same_host,
    strip_dot,
};
use ebus_discovery_tokio::{Backend, Browse, reresolve};
use ebus_rumqttc::Reresolve;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::PropertyAddr;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::controller::Event;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::description::DeviceDescription;
use ebus_rumqttc::ebus_homie_tokio::ebus_homie::state::DeviceState;
use ebus_rumqttc::ebus_homie_tokio::{ControllerHandle, Delivered, EventReceiver};
use rumqttc::MqttOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The `$description` type of the MID child, which carries the `grid` node.
pub const MID_TYPE: &str = "energy.ebus.device.mid";
/// The node watched on the enclosure.
pub const ENCLOSURE_NODE: &str = "power-flows";
/// The node watched on the MID.
pub const MID_NODE: &str = "grid";
/// How long the panel's broker is looked for before connecting to its host
/// name directly.
pub const DISCOVERY_WINDOW: Duration = Duration::from_secs(5);
/// How often the monitor says it is still waiting for the MID.
pub const WAITING_EVERY: Duration = Duration::from_secs(60);
/// Largest incoming packet; a `$description` can exceed rumqttc's 10 KiB
/// default.
pub const MAX_PACKET: usize = 4 << 20;
/// A cached CA certificate expiring sooner than this is downloaded again
/// (`openssl x509 -checkend 86400`).
const EXPIRY_MARGIN_SECS: i64 = 86_400;
/// Where the panel serves its CA certificate.
const CA_PATH: &str = "/api/v2/certificate/ca";
/// The panel's HTTP port.
pub const HTTP_PORT: u16 = 80;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HTTP_RESPONSE: u64 = 1 << 20;

pub const USAGE: &str = "usage: simple-span-controller <serial-number> [password] [--device-id ID] [--broker-host HOST] [--broker-port PORT] [--insecure] [--debug]";

/// The command line.
pub struct Args {
    pub serial: String,
    pub password: Option<String>,
    /// The enclosure's device id; `None`: the serial number.
    pub device_id: Option<String>,
    pub broker_host: Option<String>,
    pub broker_port: u16,
    pub insecure: bool,
    pub debug: bool,
}

impl Args {
    /// The arguments after the program name, as python's argparse takes
    /// them: options as `--name value` or `--name=value`, anywhere, and
    /// only positional arguments after `--`.
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Args, String> {
        let mut positional = Vec::new();
        let (mut device_id, mut broker_host, mut broker_port) = (None, None, None);
        let (mut insecure, mut debug) = (false, false);
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if arg == "--" {
                positional.extend(args.by_ref());
                break;
            }
            let (name, inline) = match arg.split_once('=') {
                Some((n, v)) if n.starts_with("--") => (n.to_owned(), Some(v.to_owned())),
                _ => (arg.clone(), None),
            };
            let mut value = |name: &str| {
                inline
                    .clone()
                    .or_else(|| args.next())
                    .ok_or_else(|| format!("{name} needs a value\n{USAGE}"))
            };
            match name.as_str() {
                "--device-id" => device_id = Some(value("--device-id")?),
                "--broker-host" => broker_host = Some(value("--broker-host")?),
                "--broker-port" => {
                    let p = value("--broker-port")?;
                    broker_port = Some(
                        p.parse::<u16>()
                            .map_err(|_| format!("--broker-port {p:?} is not a port"))?,
                    );
                }
                "--insecure" if inline.is_none() => insecure = true,
                "--debug" if inline.is_none() => debug = true,
                "-h" | "--help" => return Err(USAGE.to_owned()),
                n if n.starts_with('-') && n.len() > 1 => {
                    return Err(format!("unknown option {n}\n{USAGE}"));
                }
                _ => positional.push(arg),
            }
        }
        let mut positional = positional.into_iter();
        let serial = positional.next().ok_or_else(|| USAGE.to_owned())?;
        if serial.is_empty()
            || !serial
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(format!(
                "serial number {serial:?}: expected letters, digits and '-'"
            ));
        }
        let password = positional.next();
        if positional.next().is_some() {
            return Err(USAGE.to_owned());
        }
        Ok(Args {
            serial,
            password,
            device_id,
            broker_host,
            broker_port: broker_port.unwrap_or(BrokerType::SecureMqtt.default_port()),
            insecure,
            debug,
        })
    }
}

/// `span-<serial>.local`, the panel's host name.
pub fn panel_host(serial: &str) -> String {
    format!("span-{serial}.local")
}

/// Prints log records at or above `log::max_level()` to stderr.
pub struct Stderr;

impl log::Log for Stderr {
    fn enabled(&self, m: &log::Metadata<'_>) -> bool {
        m.level() <= log::max_level()
    }

    fn log(&self, r: &log::Record<'_>) {
        if self.enabled(r.metadata()) {
            eprintln!("{} {}: {}", r.level(), r.target(), r.args());
        }
    }

    fn flush(&self) {}
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
}

/// `var` with a leading `~` expanded, else `default` under `home`.
pub fn config_path(
    var: Option<String>,
    home: Option<&Path>,
    default: &str,
) -> Result<PathBuf, String> {
    let under_home = |rest: &str| {
        home.map(|h| h.join(rest))
            .ok_or_else(|| "HOME is not set".to_owned())
    };
    match var.filter(|v| !v.is_empty()) {
        Some(v) if v == "~" => under_home(""),
        Some(v) => match v.strip_prefix("~/") {
            Some(rest) => under_home(rest),
            None => Ok(PathBuf::from(v)),
        },
        None => under_home(default),
    }
}

/// `SPAN_AUTH_FILE`, else `~/.span-auth.json`.
pub fn auth_file_path() -> Result<PathBuf, String> {
    config_path(env("SPAN_AUTH_FILE"), home().as_deref(), ".span-auth.json")
}

/// `SPAN_CA_CERT_DIR`, else `~/.span-ca-certs`.
pub fn ca_cert_dir() -> Result<PathBuf, String> {
    config_path(env("SPAN_CA_CERT_DIR"), home().as_deref(), ".span-ca-certs")
}

/// The warning `span_auth_utils` prints for a file that its group or others
/// may access.
#[cfg(unix)]
pub fn permission_warning(path: &Path) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path).ok()?.permissions().mode();
    (mode & 0o077 != 0).then(|| {
        format!(
            "{} has insecure permissions. Run 'chmod 600 {}' to fix.",
            path.display(),
            path.display()
        )
    })
}

#[cfg(not(unix))]
pub fn permission_warning(_path: &Path) -> Option<String> {
    None
}

/// The span-auth credential file.
pub struct SpanAuth {
    data: serde_json::Value,
}

impl SpanAuth {
    /// The file at `path`; with no file there, one listing no panels.
    pub fn load(path: &Path) -> Result<SpanAuth, String> {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(SpanAuth {
                    data: serde_json::Value::Null,
                });
            }
            Err(e) => return Err(format!("{}: {e}", path.display())),
        };
        let data = serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(SpanAuth { data })
    }

    /// The `ebus_broker_password` of `serial`; with no serial, of
    /// `default_panel`, else of the only panel listed.
    pub fn password(&self, serial: Option<&str>) -> Option<String> {
        let panels = self.data.get("panels")?.as_object()?;
        if panels.is_empty() {
            return None;
        }
        let default = self
            .data
            .get("default_panel")
            .and_then(serde_json::Value::as_str)
            .filter(|d| !d.is_empty());
        let target = match (serial.filter(|s| !s.is_empty()), default) {
            (Some(s), _) | (None, Some(s)) => s,
            (None, None) if panels.len() == 1 => panels.keys().next()?,
            (None, None) => return None,
        };
        panels
            .get(target)?
            .get("ebus_broker_password")?
            .as_str()
            .filter(|p| !p.is_empty())
            .map(str::to_owned)
    }
}

/// `<dir>/<serial>.crt`.
pub fn ca_cert_path(dir: &Path, serial: &str) -> PathBuf {
    dir.join(format!("{serial}.crt"))
}

/// True unless `pem` holds a certificate valid for at least a day after
/// `now`, in seconds since the Unix epoch.
pub fn expiring(pem: &[u8], now: i64) -> bool {
    match not_after(pem) {
        Ok(t) => t < now + EXPIRY_MARGIN_SECS,
        Err(_) => true,
    }
}

/// The `notAfter` of the first PEM certificate in `pem`, in seconds since
/// the Unix epoch.
pub fn not_after(pem: &[u8]) -> Result<i64, String> {
    let der = pem_certificate(pem).ok_or("no PEM certificate")?;
    validity_end(&der).ok_or_else(|| "not an X.509 certificate".to_owned())
}

/// The DER of the first `CERTIFICATE` block.
fn pem_certificate(pem: &[u8]) -> Option<Vec<u8>> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    let text = std::str::from_utf8(pem).ok()?;
    let start = text.find(BEGIN)? + BEGIN.len();
    let end = start + text[start..].find("-----END CERTIFICATE-----")?;
    base64(&text[start..end])
}

fn base64(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in text.bytes().filter(|c| !c.is_ascii_whitespace()) {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

/// One DER element: its tag, its content and what follows it.
fn tlv(b: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = b.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let n = usize::from(first & 0x7f);
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |acc, &x| (acc << 8) | usize::from(x));
        (len, &rest[n..])
    };
    (rest.len() >= len).then(|| (tag, &rest[..len], &rest[len..]))
}

/// `tbsCertificate.validity.notAfter` (RFC 5280 4.1).
fn validity_end(der: &[u8]) -> Option<i64> {
    let (0x30, cert, _) = tlv(der)? else {
        return None;
    };
    let (0x30, tbs, _) = tlv(cert)? else {
        return None;
    };
    let mut rest = tbs;
    // The optional [0] version, then serialNumber, signature and issuer.
    if rest.first() == Some(&0xa0) {
        rest = tlv(rest)?.2;
    }
    for _ in 0..3 {
        rest = tlv(rest)?.2;
    }
    let (0x30, validity, _) = tlv(rest)? else {
        return None;
    };
    let (tag, time, _) = tlv(tlv(validity)?.2)?;
    der_time(tag, time)
}

/// A `UTCTime` (tag 0x17) or `GeneralizedTime` (0x18) in the RFC 5280 form
/// `[YY]YYMMDDHHMMSSZ`, in seconds since the Unix epoch.
fn der_time(tag: u8, t: &[u8]) -> Option<i64> {
    let digits = |s: &[u8]| -> Option<i64> {
        s.iter().try_fold(0i64, |acc, &c| {
            c.is_ascii_digit().then(|| acc * 10 + i64::from(c - b'0'))
        })
    };
    let (year, rest) = match (tag, t.len()) {
        (0x17, 13) => {
            let yy = digits(&t[..2])?;
            (if yy < 50 { 2000 + yy } else { 1900 + yy }, &t[2..])
        }
        (0x18, 15) => (digits(&t[..4])?, &t[4..]),
        _ => return None,
    };
    if rest[10] != b'Z' {
        return None;
    }
    let field = |i: usize| digits(&rest[i..i + 2]);
    let (month, day) = (field(0)?, field(2)?);
    let (hour, minute, second) = (field(4)?, field(6)?, field(8)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// Days from 1970-01-01 to the proleptic Gregorian date, for years from 0.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let (era, yoe) = (y / 400, y % 400);
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Seconds since the Unix epoch.
pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Where the panel's CA certificate is downloaded from: `http://<host><CA_PATH>`,
/// reached at `connect` and `port`.
pub struct CaSource {
    pub host: String,
    pub connect: String,
    pub port: u16,
}

/// The panel's CA certificate, as `ensure_ca_cert` keeps it: the PEM at
/// `path` unless it is not valid for a day after `now`, else downloaded
/// from `source` and written to `path`.
pub async fn ensure_ca_cert(path: &Path, source: &CaSource, now: i64) -> Result<Vec<u8>, String> {
    match std::fs::read(path) {
        Ok(pem) if !expiring(&pem, now) => return Ok(pem),
        Ok(_) => eprintln!(
            "CA certificate {} is not valid for another day, downloading it again",
            path.display()
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("{}: {e}", path.display())),
    }
    let pem = fetch_ca(source).await?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    std::fs::write(path, &pem).map_err(|e| format!("{}: {e}", path.display()))?;
    eprintln!("downloaded the CA certificate to {}", path.display());
    Ok(pem)
}

/// The CA certificate `source` serves, which must parse as a certificate.
pub async fn fetch_ca(source: &CaSource) -> Result<Vec<u8>, String> {
    let host = if source.host.contains(':') {
        format!("[{}]", source.host)
    } else {
        source.host.clone()
    };
    let url = format!("http://{host}{CA_PATH}");
    let get = async {
        let mut s = tokio::net::TcpStream::connect((source.connect.as_str(), source.port)).await?;
        s.write_all(format!("GET {CA_PATH} HTTP/1.0\r\nHost: {host}\r\n\r\n").as_bytes())
            .await?;
        let mut resp = Vec::new();
        s.take(MAX_HTTP_RESPONSE).read_to_end(&mut resp).await?;
        Ok::<_, std::io::Error>(resp)
    };
    let resp = tokio::time::timeout(HTTP_TIMEOUT, get)
        .await
        .map_err(|_| format!("{url}: timed out"))?
        .map_err(|e| format!("{url}: {e}"))?;
    let body = http_body(&resp).map_err(|e| format!("{url}: {e}"))?;
    not_after(body).map_err(|e| format!("{url}: {e}"))?;
    Ok(body.to_vec())
}

/// The body of an HTTP/1.x `200` response, cut to its `Content-Length`.
fn http_body(resp: &[u8]) -> Result<&[u8], String> {
    let end = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("malformed HTTP response")?;
    let head = std::str::from_utf8(&resp[..end]).map_err(|_| "malformed HTTP response")?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .ok_or("malformed HTTP response")?;
    if status != "200" {
        return Err(format!("HTTP status {status}"));
    }
    let body = &resp[end + 4..];
    let len = lines.find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim()
            .eq_ignore_ascii_case("content-length")
            .then(|| v.trim().parse::<usize>().ok())?
    });
    Ok(match len {
        Some(n) if n <= body.len() => &body[..n],
        _ => body,
    })
}

/// `ep` when it is a `_secure-mqtt._tcp` broker whose SRV target is `host`,
/// ignoring case and a trailing dot, with that SRV target as its host
/// whatever its TXT `broker` says.
pub fn panel_broker(mut ep: BrokerEndpoint, host: &str) -> Option<BrokerEndpoint> {
    let server = strip_dot(ep.server.as_deref()?).to_owned();
    if ep.broker_type != BrokerType::SecureMqtt || !same_host(&server, host) {
        return None;
    }
    ep.host = server;
    Some(ep)
}

/// The panel's broker among mDNS sightings: the first advertisement
/// [`panel_broker`] accepts that has an IPv4 address, which
/// [`BrokerEndpoint::connect_host`] connects to.
pub struct PanelSearch {
    host: String,
    table: InstanceTable,
    found: Option<BrokerEndpoint>,
}

impl PanelSearch {
    pub fn new(host: &str) -> PanelSearch {
        PanelSearch {
            host: host.to_owned(),
            table: InstanceTable::new(),
            found: None,
        }
    }

    /// Takes a sighting; true once the result has an IPv4 address.
    pub fn handle(&mut self, s: Sighting) -> bool {
        if self.table.handle(s) {
            let accepted = |i: &Instance| {
                BrokerEndpoint::from_instance(i).and_then(|ep| panel_broker(ep, &self.host))
            };
            let mut all: Vec<BrokerEndpoint> = self
                .table
                .instances()
                .filter_map(|i| accepted(&i))
                .collect();
            self.found = match all.iter().position(has_ipv4) {
                Some(at) => Some(all.swap_remove(at)),
                None => all.into_iter().next(),
            };
        }
        self.found.as_ref().is_some_and(has_ipv4)
    }

    /// The accepted broker, preferring one with an IPv4 address.
    pub fn result(self) -> Option<BrokerEndpoint> {
        self.found
    }
}

fn has_ipv4(ep: &BrokerEndpoint) -> bool {
    ep.addrs.iter().any(|a| a.ip.is_ipv4())
}

/// Browses `_secure-mqtt._tcp` for up to `window` for the broker whose SRV
/// target is `host` ([`PanelSearch`]).
pub async fn discover<B: Backend>(
    b: &B,
    host: &str,
    window: Duration,
) -> Result<Option<BrokerEndpoint>, String> {
    let mut browse = b
        .browse(BrokerType::SecureMqtt.as_str())
        .await
        .map_err(|e| format!("mDNS browse: {e}"))?;
    let mut search = PanelSearch::new(host);
    let end = tokio::time::Instant::now() + window;
    loop {
        tokio::select! {
            s = browse.next() => match s {
                Some(s) => {
                    if search.handle(s) {
                        break;
                    }
                }
                None => break,
            },
            () = tokio::time::sleep_until(end) => break,
        }
    }
    Ok(search.result())
}

/// The [`Reresolve`] hook for the panel's broker `found` over mDNS: looks
/// its instance up again for up to `window` and, when the SRV target is
/// still `host` ([`panel_broker`]), gives `options` for it; otherwise `None`,
/// which keeps the old options.
pub fn panel_reresolve<B: Backend>(
    mdns: B,
    found: BrokerEndpoint,
    host: String,
    window: Duration,
    options: impl Fn(&BrokerEndpoint) -> MqttOptions + Clone + Send + 'static,
) -> Reresolve {
    let mut cfg = BrokerConfig::default();
    cfg.accept = Some(vec![BrokerType::SecureMqtt]);
    Box::new(move || {
        let (m, c, b, h) = (mdns.clone(), cfg.clone(), found.clone(), host.clone());
        let options = options.clone();
        Box::pin(async move {
            let again = reresolve(&m, &b, &c, window).await.ok()??;
            Some(options(&panel_broker(again, &h)?))
        })
    })
}

#[cfg(feature = "rustls")]
pub mod tls {
    use std::sync::Arc;

    use rumqttc::tokio_rustls::rustls::client::WebPkiServerVerifier;
    use rumqttc::tokio_rustls::rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use rumqttc::tokio_rustls::rustls::crypto::{
        CryptoProvider, verify_tls12_signature, verify_tls13_signature,
    };
    use rumqttc::tokio_rustls::rustls::pki_types::pem::PemObject;
    use rumqttc::tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rumqttc::tokio_rustls::rustls::{
        self, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
    };

    /// Verifies the broker's certificate under one fixed name, whatever
    /// address the connection went to; with no CA, accepts any certificate.
    /// Handshake signatures are verified either way.
    #[derive(Debug)]
    struct PanelVerifier {
        name: ServerName<'static>,
        ca: Option<Arc<WebPkiServerVerifier>>,
        provider: Arc<CryptoProvider>,
    }

    impl ServerCertVerifier for PanelVerifier {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            ocsp_response: &[u8],
            now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            match &self.ca {
                Some(v) => {
                    v.verify_server_cert(end_entity, intermediates, &self.name, ocsp_response, now)
                }
                None => Ok(ServerCertVerified::assertion()),
            }
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
                &self.provider.signature_verification_algorithms,
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
                &self.provider.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.provider
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    /// A client configuration that verifies the broker's certificate against
    /// the PEM CA `ca` under `name`, a host name or IP address, so a
    /// connection to the panel's address checks the name the certificate
    /// carries. With no `ca`, any certificate is accepted.
    pub fn client_config(ca: Option<&[u8]>, name: &str) -> Result<Arc<ClientConfig>, String> {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let ca = match ca {
            Some(pem) => {
                let mut roots = RootCertStore::empty();
                for cert in CertificateDer::pem_slice_iter(pem) {
                    let cert = cert.map_err(|e| format!("CA certificate: {e}"))?;
                    roots
                        .add(cert)
                        .map_err(|e| format!("CA certificate: {e}"))?;
                }
                if roots.is_empty() {
                    return Err("CA certificate: no certificate in the PEM".to_owned());
                }
                let v = WebPkiServerVerifier::builder_with_provider(
                    Arc::new(roots),
                    Arc::clone(&provider),
                )
                .build()
                .map_err(|e| format!("CA certificate: {e}"))?;
                Some(v)
            }
            None => None,
        };
        let name = ServerName::try_from(name.to_owned())
            .map_err(|e| format!("TLS server name {name:?}: {e}"))?;
        let config = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("TLS: {e}"))?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PanelVerifier { name, ca, provider }))
            .with_no_client_auth();
        Ok(Arc::new(config))
    }
}

/// A line the monitor reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Line {
    Info(String),
    Warn(String),
}

/// A property's current value text, by device, node and property.
pub type Values<'a> = dyn Fn(&str, &str, &str) -> Option<Arc<str>> + 'a;

/// Watches the enclosure's `power-flows` node and the MID's `grid` node.
///
/// Values come from the controller's snapshot, also when an event names the
/// property: the snapshot can be ahead of events still queued, and comparing
/// a queued event's value with a newer snapshot value would report changes
/// out of order. A consumer that falls behind sees consecutive changes of a
/// property as one.
pub struct Monitor {
    enclosure: String,
    mid: Option<String>,
    /// The last value reported of each watched property.
    values: BTreeMap<(String, String, String), Arc<str>>,
}

impl Monitor {
    pub fn new(enclosure: &str) -> Monitor {
        Monitor {
            enclosure: enclosure.to_owned(),
            mid: None,
            values: BTreeMap::new(),
        }
    }

    /// The MID's device id, once a description of type [`MID_TYPE`] was seen.
    pub fn mid(&self) -> Option<&str> {
        self.mid.as_deref()
    }

    /// The node watched on `device`.
    fn watched(&self, device: &str) -> Option<&'static str> {
        if device == self.enclosure {
            Some(ENCLOSURE_NODE)
        } else if self.mid.as_deref() == Some(device) {
            Some(MID_NODE)
        } else {
            None
        }
    }

    /// The lines `e` gives; `value` reads the controller's snapshot.
    pub fn event(&mut self, e: &Event, value: &Values<'_>) -> Vec<Line> {
        match e {
            Event::StateChanged {
                device, old, new, ..
            } => self.state_changed(device, *old, *new),
            Event::DescriptionChanged {
                device,
                description,
                ..
            } => self.described(device, description, value),
            Event::PropertyChanged { addr, .. } | Event::PropertyCleared { addr, .. } => {
                self.sync(addr, value).into_iter().collect()
            }
            _ => Vec::new(),
        }
    }

    /// The lines for devices whose events were dropped: the MID if it is
    /// among them, and each watched value that differs from the last
    /// reported, or was cleared.
    pub fn resync<'d>(
        &mut self,
        devices: impl IntoIterator<Item = (&'d str, &'d DeviceDescription)>,
        value: &Values<'_>,
    ) -> Vec<Line> {
        let mut out = Vec::new();
        for (device, d) in devices {
            if self.mid.is_none() && d.r#type.as_deref() == Some(MID_TYPE) {
                out.extend(self.described(device, d, value));
                continue;
            }
            let Some(node) = self.watched(device) else {
                continue;
            };
            let Some((_, n)) = d.nodes.iter().find(|(id, _)| id == node) else {
                continue;
            };
            for (prop, _) in &n.properties {
                if let Some(addr) = PropertyAddr::new(device, node, prop) {
                    out.extend(self.sync(&addr, value));
                }
            }
        }
        out
    }

    /// The line for a `$state` change of a watched device.
    fn state_changed(&self, device: &str, old: Option<DeviceState>, new: DeviceState) -> Vec<Line> {
        match old {
            // A discovery (`old: None`) is not a change, as in python-sdk.
            Some(old) if self.watched(device).is_some() => {
                vec![Line::Info(format!("{device}: {old} -> {new}"))]
            }
            _ => Vec::new(),
        }
    }

    fn described(&mut self, device: &str, d: &DeviceDescription, value: &Values<'_>) -> Vec<Line> {
        let mut out = Vec::new();
        if self.mid.is_none() && d.r#type.as_deref() == Some(MID_TYPE) {
            self.mid = Some(device.to_owned());
            out.push(Line::Info(format!(
                "MID (islanding source) discovered: {device}"
            )));
        }
        let Some(node) = self.watched(device) else {
            return out;
        };
        let Some((_, n)) = d.nodes.iter().find(|(id, _)| id == node) else {
            out.push(Line::Warn(format!("node {node:?} not found on {device}")));
            return out;
        };
        out.push(Line::Info(format!(
            "{device}/{node} ({} properties):",
            n.properties.len()
        )));
        for (prop, p) in &n.properties {
            let v = value(device, node, prop);
            let shown = v.as_ref().map_or_else(String::new, |v| {
                format!(" = {v}{}", p.unit.as_deref().unwrap_or(""))
            });
            out.push(Line::Info(format!(
                "  {node}/{prop} ({}){shown}",
                p.datatype.as_str()
            )));
            let key = (device.to_owned(), node.to_owned(), prop.clone());
            match v {
                Some(v) => self.values.insert(key, v),
                None => self.values.remove(&key),
            };
        }
        out
    }

    /// The line for a watched property whose snapshot value differs from the
    /// last one reported: its first value, a change, or its clearing.
    fn sync(&mut self, addr: &PropertyAddr, value: &Values<'_>) -> Option<Line> {
        if self.watched(&addr.device) != Some(addr.node.as_str()) {
            return None;
        }
        let key = (
            addr.device.to_string(),
            addr.node.to_string(),
            addr.property.to_string(),
        );
        let line = match value(&addr.device, &addr.node, &addr.property) {
            Some(v) => match self.values.insert(key, Arc::clone(&v)) {
                None => format!("initial value: {} = {v}", path(addr)),
                Some(old) if old != v => format!("{} changed: {old} -> {v}", path(addr)),
                Some(_) => return None,
            },
            None => format!("{} cleared, was {}", path(addr), self.values.remove(&key)?),
        };
        Some(Line::Info(line))
    }
}

fn path(a: &PropertyAddr) -> String {
    format!("{}/{}/{}", a.device, a.node, a.property)
}

/// Runs a [`Monitor`] for `enclosure` over a controller's events until
/// `stop` completes, which shuts the controller down, or the events end.
/// While the MID is unknown, says so every `waiting_every`.
pub async fn monitor(
    handle: &ControllerHandle,
    events: &mut EventReceiver,
    enclosure: &str,
    waiting_every: Duration,
    stop: impl Future<Output = ()>,
    out: &mut (dyn FnMut(Line) + Send),
) {
    let mut m = Monitor::new(enclosure);
    let value = |d: &str, n: &str, p: &str| {
        handle
            .device(d)?
            .property(n, p)?
            .value
            .as_ref()
            .map(|v| Arc::clone(&v.raw))
    };
    let start = tokio::time::Instant::now() + waiting_every;
    let mut waiting = tokio::time::interval_at(start, waiting_every);
    tokio::pin!(stop);
    loop {
        tokio::select! {
            d = events.recv() => match d {
                Some(Delivered::Event(e)) => m.event(&e, &value).into_iter().for_each(&mut *out),
                Some(Delivered::Overflow { dropped }) => {
                    out(Line::Warn(format!("{dropped} events dropped; reading the snapshot")));
                    let devices = handle.devices();
                    let described = devices
                        .iter()
                        .filter_map(|d| Some((d.id.as_str(), d.description.as_deref()?)));
                    m.resync(described, &value).into_iter().for_each(&mut *out);
                }
                Some(_) => {}
                None => return,
            },
            _ = waiting.tick() => {
                if m.mid().is_none() {
                    out(Line::Info("still waiting to discover the MID (islanding source)".to_owned()));
                }
            }
            () = &mut stop => {
                out(Line::Info("shutting down".to_owned()));
                handle.shutdown().await;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::net::IpAddr;

    use ebus_discovery_tokio::ebus_discovery::{
        InstanceKey, ScopedAddr, ServiceRecord, Slot, TxtRecord,
    };
    use ebus_discovery_tokio::{Error as MdnsError, Group, GroupReport, HostName};
    use ebus_rumqttc::ebus_homie_tokio::ebus_homie::Id;
    use ebus_rumqttc::ebus_homie_tokio::ebus_homie::description::parse;

    use super::*;

    const SERIAL: &str = "nt-0000-xxxxx";

    fn args(a: &[&str]) -> Result<Args, String> {
        Args::parse(a.iter().map(|s| (*s).to_owned()))
    }

    #[test]
    fn command_line() {
        let a = args(&[SERIAL]).unwrap();
        assert_eq!(a.serial, SERIAL);
        assert_eq!(a.password, None);
        assert_eq!(a.device_id, None);
        assert_eq!(a.broker_host, None);
        assert_eq!(a.broker_port, 8883);
        assert!(!a.insecure && !a.debug);

        let a = args(&[
            "--insecure",
            SERIAL,
            "--device-id",
            "enclosure-1",
            "pw",
            "--broker-host=192.0.2.7",
            "--broker-port",
            "1883",
            "--debug",
        ])
        .unwrap();
        assert_eq!(a.password.as_deref(), Some("pw"));
        assert_eq!(a.device_id.as_deref(), Some("enclosure-1"));
        assert_eq!(a.broker_host.as_deref(), Some("192.0.2.7"));
        assert_eq!(a.broker_port, 1883);
        assert!(a.insecure && a.debug);

        // After `--`, a password may start with '-'.
        let a = args(&[SERIAL, "--insecure", "--", "-pw"]).unwrap();
        assert_eq!(a.password.as_deref(), Some("-pw"));
        assert!(a.insecure);
        let a = args(&["--", SERIAL, "--debug"]).unwrap();
        assert_eq!(
            (a.serial.as_str(), a.password.as_deref()),
            (SERIAL, Some("--debug"))
        );
        assert!(!a.debug);

        for bad in [
            &[][..],
            &[SERIAL, "pw", "extra"],
            &[SERIAL, "--broker-port", "x"],
            &[SERIAL, "--broker-host"],
            &[SERIAL, "--verbose"],
            &[SERIAL, "-pw"],
            &[SERIAL, "--", "pw", "extra"],
            &["../etc"],
            &["span.local"],
        ] {
            assert!(args(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn config_paths() {
        let home = Path::new("/home/u");
        let p = |v: Option<&str>| config_path(v.map(str::to_owned), Some(home), ".span-auth.json");
        assert_eq!(p(None).unwrap(), home.join(".span-auth.json"));
        assert_eq!(p(Some("")).unwrap(), home.join(".span-auth.json"));
        assert_eq!(p(Some("~/a/b.json")).unwrap(), home.join("a/b.json"));
        assert_eq!(
            p(Some("/etc/x.json")).unwrap(),
            PathBuf::from("/etc/x.json")
        );
        assert!(config_path(None, None, ".span-auth.json").is_err());
        assert_eq!(
            ca_cert_path(Path::new("/c"), SERIAL),
            PathBuf::from("/c/nt-0000-xxxxx.crt")
        );
    }

    fn auth(json: &str) -> SpanAuth {
        SpanAuth {
            data: serde_json::from_str(json).unwrap(),
        }
    }

    #[test]
    fn auth_panel_choice() {
        let two = r#"{"version": 1, "default_panel": "nt-0000-bbbbb", "panels": {
            "nt-0000-aaaaa": {"ebus_broker_password": "pa"},
            "nt-0000-bbbbb": {"ebus_broker_password": "pb"}}}"#;
        let a = auth(two);
        assert_eq!(a.password(Some("nt-0000-aaaaa")).as_deref(), Some("pa"));
        assert_eq!(a.password(None).as_deref(), Some("pb"));
        assert_eq!(a.password(Some("nt-0000-ccccc")), None);

        let no_default = auth(
            r#"{"panels": {"nt-0000-aaaaa": {"ebus_broker_password": "pa"},
                           "nt-0000-bbbbb": {"ebus_broker_password": "pb"}}}"#,
        );
        assert_eq!(no_default.password(None), None);

        let one = auth(
            r#"{"default_panel": null, "panels": {"nt-0000-aaaaa": {"ebus_broker_password": "pa"}}}"#,
        );
        assert_eq!(one.password(None).as_deref(), Some("pa"));
        assert_eq!(one.password(Some("nt-0000-bbbbb")), None);

        let empty_password = auth(r#"{"panels": {"nt-0000-aaaaa": {"ebus_broker_password": ""}}}"#);
        assert_eq!(empty_password.password(Some("nt-0000-aaaaa")), None);
        let no_password = auth(r#"{"panels": {"nt-0000-aaaaa": {"hostname": "h"}}}"#);
        assert_eq!(no_password.password(Some("nt-0000-aaaaa")), None);
        assert_eq!(auth(r#"{"panels": {}}"#).password(None), None);
    }

    /// A fresh path under the temporary directory.
    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ebus-span-{}-{name}-{}",
            std::process::id(),
            unix_now()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn auth_file_on_disk() {
        let dir = scratch("auth");
        let missing = dir.join("missing.json");
        assert_eq!(SpanAuth::load(&missing).unwrap().password(None), None);
        assert_eq!(permission_warning(&missing), None);

        let path = dir.join("auth.json");
        std::fs::write(
            &path,
            r#"{"panels": {"nt-0000-aaaaa": {"ebus_broker_password": "pa"}}}"#,
        )
        .unwrap();
        assert_eq!(
            SpanAuth::load(&path).unwrap().password(None).as_deref(),
            Some("pa")
        );
        std::fs::write(dir.join("bad.json"), "{").unwrap();
        assert!(SpanAuth::load(&dir.join("bad.json")).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let set = |mode| {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            };
            set(0o600);
            assert_eq!(permission_warning(&path), None);
            for mode in [0o640, 0o604, 0o610] {
                set(mode);
                let w = permission_warning(&path).expect("warning");
                assert!(
                    w.contains("insecure permissions") && w.contains("chmod 600"),
                    "{w}"
                );
            }
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A self-signed CA with a `UTCTime` notAfter of 2036-01-01 00:00:00Z.
    const CA_UTC: &str = include_str!("../../tests/fixtures/ca-utctime.pem");
    const CA_UTC_END: i64 = 2_082_758_400;
    /// A self-signed CA with a `GeneralizedTime` notAfter of
    /// 2126-01-01 00:00:00Z.
    const CA_GENERALIZED: &str = include_str!("../../tests/fixtures/ca-generalizedtime.pem");
    const CA_GENERALIZED_END: i64 = 4_922_899_200;

    #[test]
    fn certificate_expiry() {
        assert_eq!(not_after(CA_UTC.as_bytes()), Ok(CA_UTC_END));
        assert_eq!(not_after(CA_GENERALIZED.as_bytes()), Ok(CA_GENERALIZED_END));
        assert_eq!(der_time(0x17, b"491231235959Z"), Some(2_524_607_999));
        assert_eq!(der_time(0x17, b"500101000000Z"), Some(-631_152_000));
        assert_eq!(der_time(0x17, b"700101000000Z"), Some(0));
        assert_eq!(der_time(0x18, b"20000301000000Z"), Some(951_868_800));
        assert_eq!(der_time(0x17, b"7001010000Z"), None);
        assert_eq!(der_time(0x17, b"701301000000Z"), None);

        let pem = CA_UTC.as_bytes();
        assert!(!expiring(pem, CA_UTC_END - EXPIRY_MARGIN_SECS));
        assert!(expiring(pem, CA_UTC_END - EXPIRY_MARGIN_SECS + 1));
        assert!(expiring(pem, CA_UTC_END + 1));
        assert!(expiring(b"not a certificate", 0));
        let truncated = &CA_UTC[..CA_UTC.len() / 2];
        assert!(expiring(
            format!("{truncated}\n-----END CERTIFICATE-----\n").as_bytes(),
            0
        ));
    }

    /// Serves `body` with `status` to each connection; counts the requests.
    async fn http_server(
        status: &'static str,
        body: &'static str,
    ) -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let mut req = Vec::new();
                let mut buf = [0u8; 1024];
                while !req.ends_with(b"\r\n\r\n") {
                    match s.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => req.extend_from_slice(&buf[..n]),
                    }
                }
                let req = String::from_utf8_lossy(&req).into_owned();
                assert!(
                    req.starts_with(&format!("GET {CA_PATH} HTTP/1.0\r\n")),
                    "{req}"
                );
                assert!(
                    req.contains("\r\nHost: span-nt-0000-xxxxx.local\r\n"),
                    "{req}"
                );
                h.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/x-pem-file\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes()).await;
            }
        });
        (port, hits)
    }

    fn source(port: u16) -> CaSource {
        CaSource {
            host: panel_host(SERIAL),
            connect: "127.0.0.1".to_owned(),
            port,
        }
    }

    #[tokio::test]
    async fn ca_cert_download_and_cache() {
        use std::sync::atomic::Ordering::SeqCst;
        let (port, hits) = http_server("200 OK", CA_UTC).await;
        let dir = scratch("ca").join("certs");
        let path = ca_cert_path(&dir, SERIAL);
        let fresh = CA_UTC_END - 10 * EXPIRY_MARGIN_SECS;

        // Missing: downloaded, its directory created.
        let pem = ensure_ca_cert(&path, &source(port), fresh).await.unwrap();
        assert_eq!(pem, CA_UTC.as_bytes());
        assert_eq!(std::fs::read(&path).unwrap(), CA_UTC.as_bytes());
        assert_eq!(hits.load(SeqCst), 1);

        // Valid for more than a day: the cached file, no request.
        ensure_ca_cert(&path, &source(port), fresh).await.unwrap();
        assert_eq!(hits.load(SeqCst), 1);

        // Expiring within a day, or not a certificate: downloaded again.
        ensure_ca_cert(&path, &source(port), CA_UTC_END - 60)
            .await
            .unwrap();
        assert_eq!(hits.load(SeqCst), 2);
        std::fs::write(&path, "garbage").unwrap();
        ensure_ca_cert(&path, &source(port), fresh).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), CA_UTC.as_bytes());
        assert_eq!(hits.load(SeqCst), 3);
        std::fs::remove_dir_all(dir.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn ca_cert_download_failures() {
        let dir = scratch("ca-fail");
        let path = ca_cert_path(&dir, SERIAL);
        let (port, _) = http_server("404 Not Found", "no").await;
        let e = ensure_ca_cert(&path, &source(port), 0).await.unwrap_err();
        assert!(e.contains("HTTP status 404"), "{e}");
        let (port, _) = http_server("200 OK", "<html>not a certificate</html>").await;
        let e = ensure_ca_cert(&path, &source(port), 0).await.unwrap_err();
        assert!(e.contains("no PEM certificate"), "{e}");
        assert!(!path.exists(), "nothing written");
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = closed.local_addr().unwrap().port();
        drop(closed);
        assert!(ensure_ca_cert(&path, &source(port), 0).await.is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn http_bodies() {
        assert_eq!(
            http_body(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabcdef"),
            Ok(&b"abc"[..])
        );
        assert_eq!(http_body(b"HTTP/1.0 200 OK\r\n\r\nabc"), Ok(&b"abc"[..]));
        assert!(http_body(b"HTTP/1.1 500 Oops\r\n\r\n").is_err());
        assert!(http_body(b"garbage").is_err());
    }

    fn seen(name: &str, server: &str, txt: &[&str], addrs: &[&str]) -> Sighting {
        Sighting::Seen {
            key: InstanceKey {
                service_type: BrokerType::SecureMqtt.as_str().into(),
                name: name.into(),
            },
            slot: Slot::Whole,
            server: server.into(),
            port: 8883,
            txt: TxtRecord::from_strings(txt),
            addrs: addrs
                .iter()
                .map(|a| ScopedAddr::new(a.parse::<IpAddr>().unwrap()))
                .collect(),
        }
    }

    fn instance(service_type: &str, server: &str, txt: &[&str]) -> Instance {
        Instance::new(
            InstanceKey {
                service_type: service_type.into(),
                name: "x".into(),
            },
            server.into(),
            8883,
            TxtRecord::from_strings(txt),
            vec![ScopedAddr::new("192.0.2.1".parse().unwrap())],
        )
    }

    #[test]
    fn srv_target_rule() {
        let host = panel_host(SERIAL);
        let accept =
            |i: Instance| BrokerEndpoint::from_instance(&i).and_then(|e| panel_broker(e, &host));
        let secure = BrokerType::SecureMqtt.as_str();

        let ep = accept(instance(
            secure,
            "span-nt-0000-xxxxx.local.",
            &["txtvers=1"],
        ))
        .unwrap();
        assert_eq!(ep.host, "span-nt-0000-xxxxx.local");
        assert_eq!(ep.connect_host(), "192.0.2.1");
        assert!(accept(instance(secure, "SPAN-NT-0000-XXXXX.LOCAL", &[])).is_some());
        // The SRV target decides, not the TXT `broker` value.
        let ep = accept(instance(
            secure,
            "span-nt-0000-xxxxx.local.",
            &["broker=elsewhere.local"],
        ))
        .unwrap();
        assert_eq!(ep.host, "span-nt-0000-xxxxx.local");
        assert!(
            accept(instance(
                secure,
                "impostor.local.",
                &["broker=span-nt-0000-xxxxx.local"]
            ))
            .is_none()
        );
        assert!(accept(instance(secure, "span-nt-0000-yyyyy.local.", &[])).is_none());
        assert!(accept(instance(secure, "span-nt-0000-xxxxx.local.example.", &[])).is_none());
        assert!(
            accept(instance(
                BrokerType::Mqtt.as_str(),
                "span-nt-0000-xxxxx.local.",
                &[]
            ))
            .is_none()
        );
        assert!(
            panel_broker(
                BrokerEndpoint::configured(BrokerType::SecureMqtt, &host, 8883),
                &host
            )
            .is_none()
        );
    }

    #[test]
    fn panel_search() {
        let host = panel_host(SERIAL);
        let mut s = PanelSearch::new(&host);
        assert!(!s.handle(seen(
            "other",
            "impostor.local.",
            &["broker=span-nt-0000-xxxxx.local"],
            &["192.0.2.66"]
        )));
        assert!(!s.handle(seen(
            "panel",
            "span-nt-0000-xxxxx.local.",
            &[],
            &["fe80::1"]
        )));
        assert!(s.handle(seen(
            "panel",
            "span-nt-0000-xxxxx.local.",
            &[],
            &["fe80::1", "192.0.2.7"]
        )));
        let ep = s.result().unwrap();
        assert_eq!(ep.connect_host(), "192.0.2.7");
        assert_eq!(ep.instance.as_deref(), Some("panel"));

        // Seen only without an IPv4 address: kept, connected to by name.
        let mut s = PanelSearch::new(&host);
        assert!(!s.handle(seen(
            "panel",
            "span-nt-0000-xxxxx.local.",
            &[],
            &["fe80::1"]
        )));
        assert_eq!(s.result().unwrap().connect_host(), host);

        let mut s = PanelSearch::new(&host);
        s.handle(seen("other", "impostor.local.", &[], &["192.0.2.66"]));
        assert_eq!(s.result(), None);
    }

    /// An mDNS backend whose every browse hears `0`, then nothing.
    #[derive(Clone)]
    struct Scripted(Arc<Vec<Sighting>>);

    struct ScriptedBrowse(Arc<Vec<Sighting>>, usize);

    struct NoGroup;

    impl Backend for Scripted {
        type Browse = ScriptedBrowse;
        type Group = NoGroup;

        async fn browse(&self, _: &str) -> Result<ScriptedBrowse, MdnsError> {
            Ok(ScriptedBrowse(Arc::clone(&self.0), 0))
        }

        async fn server_name(&self, _: &HostName) -> Result<String, MdnsError> {
            Err(MdnsError::Closed)
        }

        async fn publish(
            &self,
            _: &HostName,
            _: &str,
            _: &[ServiceRecord],
        ) -> Result<NoGroup, MdnsError> {
            Ok(NoGroup)
        }
    }

    impl Browse for ScriptedBrowse {
        async fn next(&mut self) -> Option<Sighting> {
            let Some(s) = self.0.get(self.1) else {
                return std::future::pending().await;
            };
            self.1 += 1;
            Some(s.clone())
        }

        async fn requery(&mut self) -> Result<(), MdnsError> {
            Ok(())
        }
    }

    impl Group for NoGroup {
        async fn next_report(&mut self) -> Option<GroupReport> {
            None
        }

        async fn update_txt(&mut self, _: &str, _: &TxtRecord) -> Result<(), MdnsError> {
            Ok(())
        }

        async fn republish(&mut self, _: &[ServiceRecord]) -> Result<(), MdnsError> {
            Ok(())
        }

        async fn withdraw(self) -> Result<(), MdnsError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn reresolve_keeps_the_srv_target_rule() {
        let host = panel_host(SERIAL);
        let mut search = PanelSearch::new(&host);
        search.handle(seen(
            "panel",
            "span-nt-0000-xxxxx.local.",
            &[],
            &["192.0.2.7"],
        ));
        let found = search.result().unwrap();
        let window = Duration::from_millis(200);
        let address = |sightings: Vec<Sighting>| {
            let mut hook = panel_reresolve(
                Scripted(Arc::new(sightings)),
                found.clone(),
                host.clone(),
                window,
                |ep: &BrokerEndpoint| MqttOptions::new("id", ep.connect_host(), ep.port),
            );
            async move { hook().await.map(|o| o.broker_address()) }
        };

        // The panel at a new address.
        assert_eq!(
            address(vec![seen(
                "panel",
                "span-nt-0000-xxxxx.local.",
                &[],
                &["192.0.2.8"]
            )])
            .await,
            Some(("192.0.2.8".to_owned(), 8883))
        );
        // The same instance name with another SRV target.
        assert_eq!(
            address(vec![seen(
                "panel",
                "impostor.local.",
                &["broker=span-nt-0000-xxxxx.local"],
                &["192.0.2.66"]
            )])
            .await,
            None
        );
        // Only another instance.
        assert_eq!(
            address(vec![seen(
                "other",
                "span-nt-0000-xxxxx.local.",
                &[],
                &["192.0.2.9"]
            )])
            .await,
            None
        );
    }

    const ENCLOSURE: &str = "nt-0000-xxxxx";
    const MID: &str = "nt-0000-xxxxx-mid";

    fn enclosure_description(with_node: bool) -> Arc<DeviceDescription> {
        let node = if with_node {
            r#""power-flows": {"properties": {"grid": {"datatype": "float", "unit": "W"}, "pv": {"datatype": "float", "unit": "W"}}},"#
        } else {
            ""
        };
        Arc::new(
            parse(&format!(
                r#"{{"homie": "5.0", "version": 1, "type": "energy.ebus.device.distribution-enclosure",
                    "nodes": {{{node} "door": {{"properties": {{"state": {{"datatype": "string"}}}}}}}}}}"#
            ))
            .unwrap(),
        )
    }

    fn mid_description() -> Arc<DeviceDescription> {
        Arc::new(
            parse(
                r#"{"homie": "5.0", "version": 1, "type": "energy.ebus.device.mid", "root": "nt-0000-xxxxx",
                    "nodes": {"grid": {"properties": {
                        "islanding-state": {"datatype": "enum", "format": "ON_GRID,OFF_GRID,UNKNOWN"},
                        "grid-state": {"datatype": "enum", "format": "UP,DOWN,DEGRADED,UNKNOWN"}}}}}"#,
            )
            .unwrap(),
        )
    }

    /// The parts of an [`Event`] the monitor reads. Only the controller
    /// builds an `Event`, so the tests call the handlers `Monitor::event`
    /// dispatches to; the end-to-end test covers the dispatch.
    enum Ev {
        State(Id, Option<DeviceState>, DeviceState),
        Described(Id, Arc<DeviceDescription>),
        /// A value event; the monitor reads the value from the snapshot.
        Property(PropertyAddr),
    }

    impl Monitor {
        fn ev(&mut self, e: &Ev, value: &Values<'_>) -> Vec<Line> {
            match e {
                Ev::State(device, old, new) => self.state_changed(device, *old, *new),
                Ev::Described(device, d) => self.described(device, d, value),
                Ev::Property(addr) => self.sync(addr, value).into_iter().collect(),
            }
        }
    }

    fn described(device: &str, d: Arc<DeviceDescription>) -> Ev {
        Ev::Described(Id::new(device).unwrap(), d)
    }

    /// The event for a value set to `_raw`.
    fn property(device: &str, node: &str, prop: &str, _raw: &str) -> Ev {
        Ev::Property(PropertyAddr::new(device, node, prop).unwrap())
    }

    fn info(s: &str) -> Line {
        Line::Info(s.to_owned())
    }

    fn none(_: &str, _: &str, _: &str) -> Option<Arc<str>> {
        None
    }

    /// The controller snapshot's values, as a test sets them.
    #[derive(Default)]
    struct Snapshot(RefCell<BTreeMap<(String, String, String), Arc<str>>>);

    impl Snapshot {
        fn set(&self, d: &str, n: &str, p: &str, v: Option<&str>) {
            let key = (d.to_owned(), n.to_owned(), p.to_owned());
            let mut values = self.0.borrow_mut();
            match v {
                Some(v) => values.insert(key, v.into()),
                None => values.remove(&key),
            };
        }

        fn values(&self) -> impl Fn(&str, &str, &str) -> Option<Arc<str>> + '_ {
            |d, n, p| {
                let key = (d.to_owned(), n.to_owned(), p.to_owned());
                self.0.borrow().get(&key).cloned()
            }
        }
    }

    fn cleared(device: &str, node: &str, prop: &str) -> Ev {
        Ev::Property(PropertyAddr::new(device, node, prop).unwrap())
    }

    /// Sets a value in the snapshot, then delivers its event, as the
    /// controller does for a consumer that keeps up.
    fn live(
        m: &mut Monitor,
        s: &Snapshot,
        d: &str,
        n: &str,
        p: &str,
        v: Option<&str>,
    ) -> Vec<Line> {
        s.set(d, n, p, v);
        let e = match v {
            Some(v) => property(d, n, p, v),
            None => cleared(d, n, p),
        };
        m.ev(&e, &s.values())
    }

    #[test]
    fn monitor_reports_watched_nodes() {
        let mut m = Monitor::new(ENCLOSURE);
        let s = Snapshot::default();
        // A MID value before the MID is known is not watched yet.
        assert_eq!(
            live(&mut m, &s, MID, "grid", "islanding-state", Some("ON_GRID")),
            []
        );
        s.set(ENCLOSURE, "power-flows", "grid", Some("-120.5"));
        assert_eq!(
            m.ev(
                &described(ENCLOSURE, enclosure_description(true)),
                &s.values()
            ),
            [
                info("nt-0000-xxxxx/power-flows (2 properties):"),
                info("  power-flows/grid (float) = -120.5W"),
                info("  power-flows/pv (float)"),
            ]
        );
        assert_eq!(m.mid(), None);
        assert_eq!(
            m.ev(&described(MID, mid_description()), &s.values()),
            [
                info("MID (islanding source) discovered: nt-0000-xxxxx-mid"),
                info("nt-0000-xxxxx-mid/grid (2 properties):"),
                info("  grid/islanding-state (enum) = ON_GRID"),
                info("  grid/grid-state (enum)"),
            ]
        );
        assert_eq!(m.mid(), Some(MID));

        // A value the report showed: nothing until it changes.
        assert_eq!(
            live(&mut m, &s, MID, "grid", "islanding-state", Some("ON_GRID")),
            []
        );
        assert_eq!(
            live(&mut m, &s, MID, "grid", "islanding-state", Some("OFF_GRID")),
            [info(
                "nt-0000-xxxxx-mid/grid/islanding-state changed: ON_GRID -> OFF_GRID"
            )]
        );
        assert_eq!(
            live(&mut m, &s, ENCLOSURE, "power-flows", "pv", Some("300.0")),
            [info("initial value: nt-0000-xxxxx/power-flows/pv = 300.0")]
        );
        assert_eq!(
            live(&mut m, &s, ENCLOSURE, "power-flows", "pv", Some("300.0")),
            []
        );
        // Other nodes and devices are not watched.
        assert_eq!(
            live(&mut m, &s, ENCLOSURE, "door", "state", Some("OPEN")),
            []
        );
        assert_eq!(
            live(&mut m, &s, "circuit-1", "power-flows", "grid", Some("1")),
            []
        );
        assert_eq!(
            live(&mut m, &s, ENCLOSURE, "power-flows", "pv", None),
            [info("nt-0000-xxxxx/power-flows/pv cleared, was 300.0")]
        );
        assert_eq!(live(&mut m, &s, ENCLOSURE, "power-flows", "pv", None), []);
        assert_eq!(
            live(&mut m, &s, ENCLOSURE, "power-flows", "pv", Some("310.0")),
            [info("initial value: nt-0000-xxxxx/power-flows/pv = 310.0")]
        );
    }

    #[test]
    fn monitor_snapshot_ahead_of_queued_events() {
        let mut m = Monitor::new(ENCLOSURE);
        let s = Snapshot::default();
        // The grid value went to 1.0 and then 2.0 before the description
        // was dequeued; both events are still queued behind it.
        s.set(ENCLOSURE, "power-flows", "grid", Some("2.0"));
        assert_eq!(
            m.ev(
                &described(ENCLOSURE, enclosure_description(true)),
                &s.values()
            ),
            [
                info("nt-0000-xxxxx/power-flows (2 properties):"),
                info("  power-flows/grid (float) = 2.0W"),
                info("  power-flows/pv (float)"),
            ]
        );
        let grid = |v| property(ENCLOSURE, "power-flows", "grid", v);
        assert_eq!(m.ev(&grid("1.0"), &s.values()), []);
        assert_eq!(m.ev(&grid("2.0"), &s.values()), []);

        // Behind again: 3.0 and 4.0 queued, the snapshot at 4.0.
        s.set(ENCLOSURE, "power-flows", "grid", Some("4.0"));
        assert_eq!(
            m.ev(&grid("3.0"), &s.values()),
            [info("nt-0000-xxxxx/power-flows/grid changed: 2.0 -> 4.0")]
        );
        assert_eq!(m.ev(&grid("4.0"), &s.values()), []);
        // A queued value set and then cleared.
        assert_eq!(
            m.ev(
                &property(ENCLOSURE, "power-flows", "pv", "5.0"),
                &s.values()
            ),
            []
        );
        assert_eq!(
            m.ev(&cleared(ENCLOSURE, "power-flows", "pv"), &s.values()),
            []
        );
    }

    #[test]
    fn monitor_states_and_missing_node() {
        let mut m = Monitor::new(ENCLOSURE);
        let state = |device: &str, old: Option<DeviceState>, new: DeviceState| {
            Ev::State(Id::new(device).unwrap(), old, new)
        };
        // Discovery is not a state change.
        assert_eq!(m.ev(&state(ENCLOSURE, None, DeviceState::Ready), &none), []);
        assert_eq!(
            m.ev(
                &state(ENCLOSURE, Some(DeviceState::Init), DeviceState::Ready),
                &none
            ),
            [info("nt-0000-xxxxx: init -> ready")]
        );
        assert_eq!(m.ev(&state(MID, None, DeviceState::Ready), &none), []);
        m.ev(&described(MID, mid_description()), &none);
        assert_eq!(
            m.ev(
                &state(MID, Some(DeviceState::Ready), DeviceState::Lost),
                &none
            ),
            [info("nt-0000-xxxxx-mid: ready -> lost")]
        );
        assert_eq!(
            m.ev(&described(ENCLOSURE, enclosure_description(false)), &none),
            [Line::Warn(
                "node \"power-flows\" not found on nt-0000-xxxxx".to_owned()
            )]
        );
        // A second MID is not taken.
        let other = "nt-0000-xxxxx-mid2";
        assert_eq!(m.ev(&described(other, mid_description()), &none), []);
        assert_eq!(m.mid(), Some(MID));
    }

    #[test]
    fn monitor_resync_after_overflow() {
        let mut m = Monitor::new(ENCLOSURE);
        let enc = enclosure_description(true);
        let mid = mid_description();
        let s = Snapshot::default();
        live(&mut m, &s, ENCLOSURE, "power-flows", "grid", Some("4.0"));
        live(&mut m, &s, ENCLOSURE, "power-flows", "pv", Some("300.0"));
        // Dropped: grid to 4.5 and 5.0, pv cleared, the MID described.
        s.set(ENCLOSURE, "power-flows", "grid", Some("5.0"));
        s.set(ENCLOSURE, "power-flows", "pv", None);
        s.set(MID, "grid", "islanding-state", Some("OFF_GRID"));
        let lines = m.resync([(ENCLOSURE, &*enc), (MID, &*mid)], &s.values());
        assert_eq!(
            lines,
            [
                info("nt-0000-xxxxx/power-flows/grid changed: 4.0 -> 5.0"),
                info("nt-0000-xxxxx/power-flows/pv cleared, was 300.0"),
                info("MID (islanding source) discovered: nt-0000-xxxxx-mid"),
                info("nt-0000-xxxxx-mid/grid (2 properties):"),
                info("  grid/islanding-state (enum) = OFF_GRID"),
                info("  grid/grid-state (enum)"),
            ]
        );
        assert_eq!(
            m.resync([(ENCLOSURE, &*enc), (MID, &*mid)], &s.values()),
            []
        );
        // Events queued behind the overflow are older than the snapshot.
        assert_eq!(
            m.ev(
                &property(ENCLOSURE, "power-flows", "grid", "4.5"),
                &s.values()
            ),
            []
        );
        assert_eq!(
            m.ev(&cleared(ENCLOSURE, "power-flows", "pv"), &s.values()),
            []
        );
        // pv published again with its old value is a first value.
        assert_eq!(
            live(&mut m, &s, ENCLOSURE, "power-flows", "pv", Some("300.0")),
            [info("initial value: nt-0000-xxxxx/power-flows/pv = 300.0")]
        );
    }
}
