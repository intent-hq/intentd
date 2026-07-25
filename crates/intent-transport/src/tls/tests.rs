//! Unit tests for `transport::tls`: generate/persist/reload round-trip,
//! fingerprint format parity with the TS implementation, regeneration on
//! expired/corrupt input, and persisted file modes.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};
use time::{Duration, OffsetDateTime};

use super::*;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut p = std::env::temp_dir();
    p.push(format!(
        "intentd-tls-{tag}-{}-{nanos}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn write_cert_with_validity(dir: &Path, not_before: OffsetDateTime, not_after: OffsetDateTime) {
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.not_before = not_before;
    params.not_after = not_after;
    let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    std::fs::write(dir.join("ws-cert.pem"), cert.pem()).unwrap();
    std::fs::write(dir.join("ws-key.pem"), key_pair.serialize_pem()).unwrap();
}

#[test]
fn generate_persist_reload_roundtrip_same_fingerprint() {
    let dir = unique_dir("roundtrip");
    let generated = generate_new_cert(&dir).unwrap();

    assert!(dir.join("ws-cert.pem").exists());
    assert!(dir.join("ws-key.pem").exists());

    let loaded = load_existing_cert(&dir).expect("valid cert should load");
    assert_eq!(generated.fingerprint256, loaded.fingerprint256);
    assert_eq!(generated.cert, loaded.cert);
    assert_eq!(generated.key, loaded.key);
}

#[test]
fn fingerprint_is_32_uppercase_hex_pairs() {
    let dir = unique_dir("fpformat");
    let cert = generate_new_cert(&dir).unwrap();

    let parts: Vec<&str> = cert.fingerprint256.split(':').collect();
    assert_eq!(parts.len(), 32, "SHA-256 fingerprint must be 32 byte pairs");
    for part in parts {
        assert_eq!(part.len(), 2, "each pair must be two hex digits");
        for ch in part.chars() {
            assert!(
                ch.is_ascii_digit() || ('A'..='F').contains(&ch),
                "fingerprint must be UPPERCASE hex, got {ch:?}",
            );
        }
    }
}

#[test]
fn expired_cert_triggers_regeneration() {
    let dir = unique_dir("expired");
    let now = OffsetDateTime::now_utc();
    write_cert_with_validity(&dir, now - Duration::days(2), now - Duration::days(1));

    assert!(
        load_existing_cert(&dir).is_none(),
        "expired cert must be rejected",
    );

    let regenerated = generate_new_cert(&dir).unwrap();
    let loaded = load_existing_cert(&dir).expect("fresh cert should load");
    assert_eq!(regenerated.fingerprint256, loaded.fingerprint256);
}

#[test]
fn corrupt_cert_triggers_regeneration() {
    let dir = unique_dir("corrupt");
    std::fs::write(dir.join("ws-cert.pem"), b"not a real certificate").unwrap();
    std::fs::write(dir.join("ws-key.pem"), b"not a real key").unwrap();

    assert!(
        load_existing_cert(&dir).is_none(),
        "corrupt cert must be rejected",
    );
}

#[test]
fn san_includes_localhost_and_loopback() {
    let san = collect_san();
    assert!(san.contains(&"localhost".to_string()));
    assert!(san.contains(&"127.0.0.1".to_string()));
    assert!(san.contains(&"::1".to_string()));
}

#[cfg(unix)]
#[test]
fn persisted_file_modes_are_0644_and_0600() {
    use std::os::unix::fs::PermissionsExt;

    let dir = unique_dir("modes");
    generate_new_cert(&dir).unwrap();

    let cert_mode = std::fs::metadata(dir.join("ws-cert.pem"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    let key_mode = std::fs::metadata(dir.join("ws-key.pem"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(cert_mode, 0o644, "cert must be world-readable 0644");
    assert_eq!(key_mode, 0o600, "key must be owner-only 0600");
}

#[test]
fn ensure_caches_and_exposes_fingerprint() {
    // Clear cache before test to ensure isolation from parallel tests.
    clear_cert_cache();

    let dir = unique_dir("ensure");
    let cert = ensure_tls_certificate(&dir).unwrap();
    assert_eq!(cert_fingerprint(), Some(cert.fingerprint256.clone()));

    let again = ensure_tls_certificate(&dir).unwrap();
    assert_eq!(cert.fingerprint256, again.fingerprint256);

    // Clear cache after test to avoid polluting other tests.
    clear_cert_cache();
}

#[test]
fn generated_pem_parses_with_rustls_pemfile() {
    // Regression test for the WSS acceptor path (`ws.rs`): the rcgen-generated
    // cert/key PEM must remain parseable by `rustls_pemfile`, otherwise the
    // WSS listener cannot start in secure mode.
    let dir = unique_dir("rustlsparse");
    let generated = generate_new_cert(&dir).unwrap();

    let mut cert_reader: &[u8] = generated.cert.as_bytes();
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<_, _>>()
        .expect("generated cert PEM must parse");
    assert!(!certs.is_empty(), "cert PEM must contain a certificate");

    let mut key_reader: &[u8] = generated.key.as_bytes();
    let key = rustls_pemfile::private_key(&mut key_reader)
        .expect("generated key PEM must parse")
        .expect("key PEM must contain a private key");
    assert!(matches!(key, rustls::pki_types::PrivateKeyDer::Pkcs8(_)));
}
