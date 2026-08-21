//! TLS certificate management + SHA-256 fingerprint pinning (§5.4).
//!
//! Ports `src/main/websocket-tls.ts`: a self-signed cert is generated with
//! `rcgen` (EC P-256, SHA-256, 10-year validity) and persisted under the data
//! dir as `ws-cert.pem` (`0644`) + `ws-key.pem` (`0600`). It is reused across
//! restarts and regenerated when expired or unparseable. The SHA-256
//! fingerprint over the DER body (colon-separated UPPERCASE hex, e.g.
//! `AB:CD:EF:...`, PROTOCOL §1.2) is exposed for client pinning. Loaded/
//! generated certs are cached in memory.

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rcgen::{
    CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, KeyUsagePurpose,
    PKCS_ECDSA_P256_SHA256,
};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};
use x509_parser::prelude::{FromDer, X509Certificate};

use intent_core::{Error, Result};

/// On-disk name of the certificate (PEM, mode `0644`).
const CERT_FILENAME: &str = "ws-cert.pem";
/// On-disk name of the private key (PEM, mode `0600`).
const KEY_FILENAME: &str = "ws-key.pem";
/// Validity period: 10 years from generation.
const VALIDITY_YEARS: i32 = 10;
/// Interface name prefixes for virtual/container NICs excluded from the SAN.
const SKIP_IFACE_PREFIXES: [&str; 5] = ["vmnet", "bridge", "veth", "docker", "br-"];

/// A self-signed TLS certificate plus its pinned SHA-256 fingerprint.
#[derive(Debug, Clone)]
pub struct TlsCertificate {
    /// PEM-encoded certificate.
    pub cert: String,
    /// PEM-encoded private key.
    pub key: String,
    /// SHA-256 fingerprint of the DER body (colon-separated UPPERCASE hex).
    pub fingerprint256: String,
}

static CACHE: Mutex<Option<TlsCertificate>> = Mutex::new(None);

/// Ensure a TLS certificate is available under `data_dir`. Returns the cached
/// cert if present, otherwise loads a valid persisted cert, otherwise generates
/// and persists a new one. The result is cached in memory.
///
/// # Errors
///
/// Returns an error if generating or persisting a new certificate fails.
///
/// # Panics
///
/// Panics if the in-memory certificate cache mutex is poisoned (a prior panic while holding the lock).
pub fn ensure_tls_certificate(data_dir: &Path) -> Result<TlsCertificate> {
    if let Some(cert) = CACHE.lock().expect("tls cache poisoned").clone() {
        return Ok(cert);
    }
    let cert = match load_existing_cert(data_dir) {
        Some(cert) => cert,
        None => generate_new_cert(data_dir)?,
    };
    *CACHE.lock().expect("tls cache poisoned") = Some(cert.clone());
    Ok(cert)
}

/// SHA-256 fingerprint of the cached certificate, or `None` before one has been
/// loaded or generated.
#[cfg(test)]
pub(crate) fn cert_fingerprint() -> Option<String> {
    CACHE
        .lock()
        .expect("tls cache poisoned")
        .as_ref()
        .map(|c| c.fingerprint256.clone())
}

/// Clear the in-memory cache. Used in tests to ensure hermetic test isolation.
#[cfg(test)]
pub(crate) fn clear_cert_cache() {
    *CACHE.lock().expect("tls cache poisoned") = None;
}

fn cert_paths(data_dir: &Path) -> (PathBuf, PathBuf) {
    (data_dir.join(CERT_FILENAME), data_dir.join(KEY_FILENAME))
}

/// Result of inspecting the persisted certificate on disk, used by `intentd
/// doctor` (§5.7) to report cert validity without generating a new one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertStatus {
    /// No cert/key pair persisted yet (generated on first TCP serve).
    Missing,
    /// A valid, in-window certificate; carries its pinned fingerprint.
    Valid { fingerprint: String },
    /// A persisted certificate that is expired or not yet valid.
    Expired,
    /// A persisted certificate (or key) that could not be parsed.
    Unparseable,
}

/// Inspect the persisted certificate under `data_dir` without mutating it
/// (read-only; never generates). Drives the `doctor` cert-validity check.
pub fn inspect_cert(data_dir: &Path) -> CertStatus {
    let (cert_path, key_path) = cert_paths(data_dir);
    if !cert_path.exists() || !key_path.exists() {
        return CertStatus::Missing;
    }
    let Ok(cert) = std::fs::read_to_string(&cert_path) else {
        return CertStatus::Unparseable;
    };
    let Some(der) = der_from_pem(&cert) else {
        return CertStatus::Unparseable;
    };
    if !is_cert_valid(&der) {
        return CertStatus::Expired;
    }
    CertStatus::Valid {
        fingerprint: compute_fingerprint(&der),
    }
}

/// Compute the colon-separated UPPERCASE hex SHA-256 fingerprint over a DER body.
fn compute_fingerprint(der: &[u8]) -> String {
    Sha256::digest(der)
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// Extract the first certificate's DER body from a PEM string.
fn der_from_pem(pem: &str) -> Option<Vec<u8>> {
    let mut reader: &[u8] = pem.as_bytes();
    let first = rustls_pemfile::certs(&mut reader).next()?;
    first.ok().map(|der| der.as_ref().to_vec())
}

/// Whether a DER certificate parses and is within its validity window now.
fn is_cert_valid(der: &[u8]) -> bool {
    match X509Certificate::from_der(der) {
        Ok((_, cert)) => cert.validity().is_valid(),
        Err(_) => false,
    }
}

/// Load a valid persisted certificate from disk, or `None` if missing,
/// expired, or unparseable (in which case the caller regenerates).
fn load_existing_cert(data_dir: &Path) -> Option<TlsCertificate> {
    let (cert_path, key_path) = cert_paths(data_dir);
    if !cert_path.exists() || !key_path.exists() {
        return None;
    }
    let cert = std::fs::read_to_string(&cert_path).ok()?;
    let key = std::fs::read_to_string(&key_path).ok()?;
    let der = der_from_pem(&cert)?;
    if !is_cert_valid(&der) {
        tracing::info!("existing TLS certificate expired or unparseable; regenerating");
        return None;
    }
    let fingerprint256 = compute_fingerprint(&der);
    Some(TlsCertificate {
        cert,
        key,
        fingerprint256,
    })
}

/// Collect SAN entries: `localhost`, loopback IPs, and every non-internal IPv4
/// address (skipping virtual/container interfaces).
fn collect_san() -> Vec<String> {
    let mut san = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if SKIP_IFACE_PREFIXES
                .iter()
                .any(|p| iface.name.starts_with(p))
            {
                continue;
            }
            if iface.is_loopback() {
                continue;
            }
            if let IpAddr::V4(v4) = iface.ip() {
                let addr = v4.to_string();
                if !san.contains(&addr) {
                    san.push(addr);
                }
            }
        }
    }
    san
}

/// Generate a new self-signed certificate, persist it (cert `0644`, key
/// `0600`), and return it with its fingerprint.
fn generate_new_cert(data_dir: &Path) -> Result<TlsCertificate> {
    let now = OffsetDateTime::now_utc();
    let not_after = now
        .replace_year(now.year() + VALIDITY_YEARS)
        .unwrap_or_else(|_| now + Duration::days(365 * VALIDITY_YEARS as i64));

    let mut params = CertificateParams::new(collect_san()).map_err(internal)?;
    params.not_before = now;
    params.not_after = not_after;
    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "Intent Local");
    params.distinguished_name = dn;

    let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).map_err(internal)?;
    let cert = params.self_signed(&key_pair).map_err(internal)?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    let fingerprint256 = compute_fingerprint(cert.der().as_ref());

    std::fs::create_dir_all(data_dir).map_err(internal)?;
    let (cert_path, key_path) = cert_paths(data_dir);
    write_with_mode(&cert_path, cert_pem.as_bytes(), 0o644)?;
    write_with_mode(&key_path, key_pem.as_bytes(), 0o600)?;

    Ok(TlsCertificate {
        cert: cert_pem,
        key: key_pem,
        fingerprint256,
    })
}

/// Write a file, then enforce its mode (umask-independent) on unix.
fn write_with_mode(path: &Path, contents: &[u8], mode: u32) -> Result<()> {
    std::fs::write(path, contents).map_err(internal)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(internal)?;
    }
    #[cfg(not(unix))]
    let _ = mode;
    Ok(())
}

fn internal<E: std::fmt::Display>(err: E) -> Error {
    Error::Internal(err.to_string())
}

#[cfg(test)]
mod tests;
