//! Git HTTPS regression for intent-hq/intent#6251 (CVE-2026-53583).
//!
//! Linux's bundled libgit2 uses OpenSSL; also check the actual backend so a
//! system library cannot silently substitute a different trust mechanism.
//! Apple Secure Transport and Windows WinHTTP need separate trust fixtures.
//! Each connection runs in a child before libgit2 initializes its global trust
//! store. Neither the parent environment nor host trust/config is changed.

#![cfg(target_os = "linux")]

use std::ffi::{c_char, c_int, CStr};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use intentd_test_support::GuardedChild;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

const CHILD_URL: &str = "INTENT_GIT_TLS_TEST_URL";
const TIMEOUT: Duration = Duration::from_secs(20);

// Public libgit2 API present in both the old and repaired dependency, but not
// exposed by git2 0.21's Version wrapper. No additional native library is linked.
unsafe extern "C" {
    fn git_libgit2_feature_backend(feature: c_int) -> *const c_char;
}

fn require_openssl() {
    let version = git2::Version::get();
    assert!(version.https(), "Git HTTPS is required: {version:?}");
    // SAFETY: GIT_FEATURE_HTTPS (1 << 1) is a valid feature; libgit2 returns a
    // static NUL-terminated string or NULL, checked before constructing CStr.
    let backend = unsafe { git_libgit2_feature_backend(1 << 1) };
    assert!(
        !backend.is_null(),
        "libgit2 did not identify its TLS backend"
    );
    // SAFETY: validated the public API's non-null static string above.
    let backend = unsafe { CStr::from_ptr(backend) }.to_str().unwrap();
    assert!(
        matches!(backend, "openssl" | "openssl-dynamic"),
        "this Linux trust fixture requires OpenSSL, got {backend} ({version:?})"
    );
    eprintln!("Git TLS backend={backend}, {version:?}");
}

#[derive(Clone, Copy)]
enum Expected {
    Accepted,
    HostnameMismatch,
    UntrustedChain,
}

fn connect(expected: Expected) {
    require_openssl();
    let url = std::env::var(CHILD_URL).unwrap();
    let mut remote = git2::Remote::create_detached(url).unwrap();
    let result = remote.connect(git2::Direction::Fetch);
    match expected {
        Expected::Accepted => {
            result.expect("matching trusted certificate must connect");
            let refs = remote.list().expect("read Git advertisement");
            assert_eq!(refs.len(), 1);
            assert_eq!(refs[0].name(), "HEAD");
        }
        Expected::HostnameMismatch | Expected::UntrustedChain => {
            let error = result.expect_err("invalid certificate must be rejected");
            assert_eq!(error.code(), git2::ErrorCode::Certificate, "{error}");
            assert_eq!(error.class(), git2::ErrorClass::Ssl, "{error}");
            let message = match expected {
                Expected::HostnameMismatch => "hostname does not match certificate",
                Expected::UntrustedChain => "the SSL certificate is invalid",
                Expected::Accepted => unreachable!(),
            };
            assert_eq!(error.message(), message);
        }
    }
}

fn authority(name: &str) -> (String, Issuer<'static, KeyPair>) {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.distinguished_name.push(DnType::CommonName, name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = KeyPair::generate().unwrap();
    let pem = params.self_signed(&key).unwrap().pem();
    (pem, Issuer::new(params, key))
}

fn server_config(san: &str, issuer: &Issuer<'_, KeyPair>) -> Arc<rustls::ServerConfig> {
    let mut params = CertificateParams::new(vec![san.into()]).unwrap();
    // A deliberately unrelated CN prevents accidental CN fallback passing a SAN case.
    params
        .distinguished_name
        .push(DnType::CommonName, "unused.invalid");
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, issuer).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Arc::new(
        rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            )
            .unwrap(),
    )
}

fn packet(bytes: &[u8]) -> Vec<u8> {
    let mut packet = format!("{:04x}", bytes.len() + 4).into_bytes();
    packet.extend_from_slice(bytes);
    packet
}

/// Own the server task immediately; abort it on child failure or test panic.
struct Server(JoinHandle<Vec<u8>>);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Server {
    fn start(listener: TcpListener, config: Arc<rustls::ServerConfig>) -> Self {
        Self(tokio::spawn(async move {
            timeout(TIMEOUT, async move {
                let (socket, _) = listener.accept().await.expect("accept loopback client");
                let mut request = Vec::new();
                if let Ok(mut tls) = TlsAcceptor::from(config).accept(socket).await {
                    let mut byte = [0];
                    while matches!(tls.read(&mut byte).await, Ok(1)) {
                        request.push(byte[0]);
                        assert!(request.len() < 8192, "oversized fixture HTTP request");
                        if request.ends_with(b"\r\n\r\n") {
                            let mut body = packet(b"# service=git-upload-pack\n");
                            body.extend_from_slice(b"0000");
                            body.extend(packet(b"1111111111111111111111111111111111111111 HEAD\0multi_ack thin-pack side-band-64k ofs-delta\n"));
                            body.extend_from_slice(b"0000");
                            let header = format!("HTTP/1.1 200 OK\r\nContent-Type: application/x-git-upload-pack-advertisement\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                            tls.write_all(header.as_bytes()).await.unwrap();
                            tls.write_all(&body).await.unwrap();
                            tls.flush().await.unwrap();
                            break;
                        }
                    }
                }
                // TLS rejection may close without close_notify. The child must
                // independently prove a precise certificate error, not an I/O error.
                request
            })
            .await
            .expect("TLS fixture timed out")
        }))
    }
}

async fn run_case(name: &str, host: &str, san: &str, expected: Expected) {
    if std::env::var_os(CHILD_URL).is_some() {
        connect(expected);
        return;
    }
    require_openssl();
    let ipv6 = host == "[::1]";
    let address = if ipv6 {
        IpAddr::V6(Ipv6Addr::LOCALHOST)
    } else {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    };
    let listener = match TcpListener::bind((address, 0)).await {
        Ok(listener) => listener,
        Err(error)
            if ipv6
                && matches!(
                    error.raw_os_error(),
                    Some(libc::EADDRNOTAVAIL | libc::EAFNOSUPPORT | libc::EPROTONOSUPPORT)
                ) =>
        {
            assert!(
                std::env::var_os("CI").is_none(),
                "required IPv6 CI case {name} unavailable: {error}"
            );
            eprintln!("NOT EXECUTED: {name}: IPv6 loopback unavailable: {error}; IPv4/mixed-SAN cases still run");
            return;
        }
        Err(error) => panic!("cannot bind TLS fixture: {error}"),
    };
    let port = listener.local_addr().unwrap().port();
    let mut scratch = tempfile::Builder::new()
        .prefix("intent-git-tls-")
        .tempdir()
        .unwrap();
    scratch
        .disable_cleanup(std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()));
    let (trusted_pem, trusted) = authority("Trusted fixture root");
    let (_, untrusted) = authority("Untrusted fixture root");
    let ca_file = scratch.path().join("root.pem");
    let ca_dir = scratch.path().join("empty-ca-dir");
    std::fs::write(&ca_file, trusted_pem).unwrap();
    std::fs::create_dir(&ca_dir).unwrap();
    let issuer = if matches!(expected, Expected::UntrustedChain) {
        &untrusted
    } else {
        &trusted
    };
    let mut server = Server::start(listener, server_config(san, issuer));
    let log_path = scratch.path().join("child.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env_clear()
        .env("HOME", scratch.path())
        .env("XDG_CONFIG_HOME", scratch.path())
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("SSL_CERT_FILE", ca_file)
        .env("SSL_CERT_DIR", ca_dir)
        .env(CHILD_URL, format!("https://{host}:{port}/repo.git"))
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    // Cargo's test binaries may need the toolchain's dynamic-library search path.
    if let Some(path) = std::env::var_os("LD_LIBRARY_PATH") {
        command.env("LD_LIBRARY_PATH", path);
    }
    let mut child = GuardedChild::spawn(&mut command).expect("spawn isolated git2 client");
    let status = child
        .wait_with_timeout(TIMEOUT)
        .unwrap()
        .expect("git2 client timed out");
    let request = (&mut server.0).await.expect("join TLS server");
    let output = std::fs::read_to_string(log_path).unwrap();
    assert!(
        status.success(),
        "{name}: {output}; HTTP request bytes: {request:?}"
    );
    if matches!(expected, Expected::Accepted) {
        assert!(
            request.starts_with(b"GET /repo.git/info/refs?service=git-upload-pack HTTP/1.1\r\n"),
            "{request:?}"
        );
    } else {
        assert!(
            request.is_empty(),
            "rejected TLS connection reached HTTP: {request:?}"
        );
    }
    eprintln!("{name}: verified ({host}, SAN {san})");
}

macro_rules! tls_case {
    ($name:ident, $host:literal, $san:literal, $expected:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            run_case(stringify!($name), $host, $san, Expected::$expected).await;
        }
    };
}

tls_case!(matching_ipv4, "127.0.0.1", "127.0.0.1", Accepted);
tls_case!(mismatched_ipv4, "127.0.0.1", "127.0.0.2", HostnameMismatch);
tls_case!(matching_dns, "localhost", "localhost", Accepted);
tls_case!(
    mismatched_dns,
    "localhost",
    "other.invalid",
    HostnameMismatch
);
tls_case!(untrusted_ipv4, "127.0.0.1", "127.0.0.1", UntrustedChain);
tls_case!(untrusted_dns, "localhost", "localhost", UntrustedChain);
tls_case!(ipv6_san_on_ipv4, "127.0.0.1", "::1", HostnameMismatch);
tls_case!(matching_ipv6, "[::1]", "::1", Accepted);
tls_case!(mismatched_ipv6, "[::1]", "::2", HostnameMismatch);
tls_case!(ipv4_san_on_ipv6, "[::1]", "127.0.0.1", HostnameMismatch);
