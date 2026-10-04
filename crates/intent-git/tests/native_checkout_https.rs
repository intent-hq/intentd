//! Real authenticated Git smart HTTP on disposable TLS loopback. Caller
//! authority is an injected fixture; Services owner/R entry have separate tests.
//! The child receives only a fixture CA and source URL, never host trust changes.
#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_git::native_checkout::{
    clone_exact, fetch_exact, NativeCheckoutCredentials, NativeCheckoutSelection,
    NativeCheckoutSource,
};
use intentd_test_support::GuardedChild;
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

const LIMIT: Duration = Duration::from_secs(30);
const CHILD: &str = "INTENT_NATIVE_CHECKOUT_FIXTURE";
const BASIC: &str = "Basic b2F1dGgyOmNoZWNrb3V0LWZpeHR1cmUtc2VjcmV0";

struct Credential {
    bad: bool,
    retired: bool,
    url: String,
}
impl NativeCheckoutCredentials for Credential {
    fn with_current(
        &self,
        transfer: &mut (dyn FnMut() -> intent_core::Result<()> + Send),
    ) -> intent_core::Result<()> {
        if self.retired {
            return Err(intent_core::Error::Internal(
                "original fixture retired".into(),
            ));
        }
        transfer()
    }
    fn credential(&mut self, url: &str) -> Result<git2::Cred, git2::Error> {
        assert_eq!(url, self.url);
        if self.retired {
            return Err(git2::Error::from_str("original fixture retired"));
        }
        git2::Cred::userpass_plaintext(
            "oauth2",
            if self.bad {
                "wrong-fixture-token"
            } else {
                "checkout-fixture-secret"
            },
        )
    }
    fn rejected(&self) {
        // The mutable callback result is not a general credential cache.
        REJECTED.with(|v| v.set(true));
    }
    fn with_basic_auth(
        &mut self,
        url: &str,
        prepare: &mut (dyn FnMut(&str, &str) -> intent_core::Result<()> + Send),
    ) -> intent_core::Result<()> {
        assert_eq!(url, self.url);
        if self.retired {
            return Err(intent_core::Error::Internal(
                "original fixture retired".into(),
            ));
        }
        prepare(
            "oauth2",
            if self.bad {
                "wrong-fixture-token"
            } else {
                "checkout-fixture-secret"
            },
        )
    }
}
thread_local! { static REJECTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

fn child(mode: &str) {
    let url = std::env::var("INTENT_NATIVE_URL").unwrap();
    let directory = PathBuf::from(std::env::var_os("INTENT_NATIVE_DEST").unwrap());
    let selection = NativeCheckoutSelection::new(
        "feature/beyond-page-one",
        &std::env::var("INTENT_NATIVE_SHA").unwrap(),
    )
    .unwrap();
    let source = NativeCheckoutSource::https(&url).unwrap();
    let mut credential = Credential {
        bad: mode == "bad",
        retired: mode == "retired",
        url,
    };
    let result = clone_exact(&source, &directory, &selection, &mut credential);
    match mode {
        "success" => {
            assert_eq!(result.unwrap(), selection);
            let repo = git2::Repository::open(&directory).unwrap();
            assert_eq!(
                repo.head().unwrap().name().unwrap(),
                "refs/heads/feature/beyond-page-one"
            );
            assert_eq!(
                repo.head().unwrap().target().unwrap().to_string(),
                selection.commit_sha
            );
            std::fs::write(directory.join("keep-local"), "local").unwrap();
            assert_eq!(
                fetch_exact(&source, &directory, &selection, &mut credential).unwrap(),
                selection
            );
            assert_eq!(
                std::fs::read_to_string(directory.join("keep-local")).unwrap(),
                "local"
            );
            assert!(!REJECTED.with(std::cell::Cell::get));
        }
        "bad" => {
            assert!(
                matches!(result, Err(intent_core::Error::GitAuthorization(_))),
                "{result:?}"
            );
            assert!(REJECTED.with(std::cell::Cell::get));
            assert!(!directory.exists());
        }
        "redirect" | "redirect_authenticated" | "retired" => {
            assert!(result.is_err(), "{mode} must refuse");
            assert!(!directory.exists());
            assert!(
                !REJECTED.with(std::cell::Cell::get),
                "local or redirect refusal is not an upstream denial"
            );
        }
        _ => panic!("invalid fixture mode"),
    }
}

fn repository(root: &Path) -> String {
    let path = root.join("forge/team/project.git");
    std::fs::create_dir_all(&path).unwrap();
    let repo = git2::Repository::init_bare(&path).unwrap();
    let blob = repo.blob(b"private checked out bytes\n").unwrap();
    let mut builder = repo.treebuilder(None).unwrap();
    builder.insert("README", blob, 0o100644).unwrap();
    let tree = repo.find_tree(builder.write().unwrap()).unwrap();
    let signature = git2::Signature::now("fixture", "fixture@example.invalid").unwrap();
    let first = repo
        .commit(
            Some("refs/heads/main"),
            &signature,
            &signature,
            "main",
            &tree,
            &[],
        )
        .unwrap();
    let parent = repo.find_commit(first).unwrap();
    let second = repo
        .commit(
            Some("refs/heads/feature/beyond-page-one"),
            &signature,
            &signature,
            "feature",
            &tree,
            &[&parent],
        )
        .unwrap();
    repo.set_head("refs/heads/main").unwrap();
    second.to_string()
}

fn tls() -> (String, Arc<rustls::ServerConfig>) {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Native checkout fixture root");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&key).unwrap().pem();
    let issuer = Issuer::new(params, key);
    let mut params = CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Native checkout fixture server");
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &issuer).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.der().clone()],
        PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
    )
    .unwrap();
    (ca, Arc::new(config))
}

fn backend(root: &Path, method: &str, target: &str, body: &[u8]) -> Vec<u8> {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env("GIT_PROJECT_ROOT", root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REMOTE_USER", "fixture")
        .env("REQUEST_METHOD", method)
        .env("PATH_INFO", path)
        .env("QUERY_STRING", query)
        .env("CONTENT_TYPE", "application/x-git-upload-pack-request")
        .env("CONTENT_LENGTH", body.len().to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = GuardedChild::spawn(&mut command).unwrap();
    child.stdin.take().unwrap().write_all(body).unwrap();
    let mut output = Vec::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut output)
        .unwrap();
    assert!(child.wait_with_timeout(LIMIT).unwrap().unwrap().success());
    let split = output.windows(4).position(|s| s == b"\r\n\r\n").unwrap();
    let head = String::from_utf8(output[..split].to_vec()).unwrap();
    assert!(
        !head.contains("Status:"),
        "fixture backend rejected the exact repository: {head}"
    );
    let body = &output[split + 4..];
    let mut response = format!(
        "HTTP/1.1 200 OK\r\n{head}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn run(name: &str, mode: &str) {
    if std::env::var_os(CHILD).is_some() {
        let mode = mode.to_owned();
        tokio::task::spawn_blocking(move || child(&mode))
            .await
            .unwrap();
        return;
    }
    let scratch = tempfile::Builder::new()
        .prefix("native-checkout-https-")
        .tempdir()
        .unwrap();
    let sha = repository(scratch.path());
    let (ca, tls) = tls();
    let ca_path = scratch.path().join("ca.pem");
    std::fs::write(&ca_path, ca).unwrap();
    let ca_dir = scratch.path().join("ca-dir");
    std::fs::create_dir(&ca_dir).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("https://127.0.0.1:{port}/forge/team/project.git");
    let requests = Arc::new(Mutex::new(Vec::<(String, bool)>::new()));
    let observed = requests.clone();
    let root = scratch.path().to_path_buf();
    let redirect = mode == "redirect";
    let redirect_authenticated = mode == "redirect_authenticated";
    let server = Server(tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let Ok(mut stream) = TlsAcceptor::from(tls.clone()).accept(socket).await else {
                continue;
            };
            timeout(LIMIT, async {
                let mut header = Vec::new();
                let mut byte = [0];
                while !header.ends_with(b"\r\n\r\n") {
                    if stream.read(&mut byte).await.unwrap_or(0) == 0 { return; }
                    header.push(byte[0]);
                    assert!(header.len() < 16384);
                }
                let header = String::from_utf8(header).unwrap();
                let first = header.lines().next().unwrap().split_whitespace().collect::<Vec<_>>();
                let target = first[1].to_string();
                let authenticated = header.lines().any(|line| line.split_once(':').is_some_and(|(key, value)| key.eq_ignore_ascii_case("authorization") && value.trim() == BASIC));
                observed.lock().unwrap().push((target.clone(), authenticated));
                let length = header.lines().find_map(|line| line.split_once(':').and_then(|(key, value)| key.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().unwrap()))).unwrap_or(0);
                assert!(length < 65536);
                let mut body = vec![0; length];
                stream.read_exact(&mut body).await.unwrap();
                let response = if redirect || (redirect_authenticated && authenticated) {
                    format!("HTTP/1.1 302 Found\r\nLocation: https://127.0.0.1:{port}/another-prefix/private.git\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").into_bytes()
                } else if !authenticated {
                    b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"fixture\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
                } else {
                    let root = root.clone();
                    let method = first[0].to_string();
                    tokio::task::spawn_blocking(move || backend(&root, &method, &target, &body)).await.unwrap()
                };
                stream.write_all(&response).await.unwrap();
                let _ = stream.shutdown().await;
            }).await.expect("fixture connection timed out");
        }
    }));
    let log = scratch.path().join("child.log");
    let out = std::fs::File::create(&log).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env_clear()
        .env("HOME", scratch.path())
        .env("XDG_CONFIG_HOME", scratch.path())
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("SSL_CERT_FILE", ca_path)
        .env("SSL_CERT_DIR", ca_dir)
        .env(CHILD, mode)
        .env("INTENT_NATIVE_URL", url)
        .env("INTENT_NATIVE_SHA", sha)
        .env("INTENT_NATIVE_DEST", scratch.path().join("checkout"))
        .stdin(Stdio::null())
        .stdout(out.try_clone().unwrap())
        .stderr(out);
    if let Some(path) = std::env::var_os("LD_LIBRARY_PATH") {
        command.env("LD_LIBRARY_PATH", path);
    }
    let mut child = GuardedChild::spawn(&mut command).unwrap();
    let result = child
        .wait_with_timeout(LIMIT)
        .unwrap()
        .expect("native checkout child timed out");
    drop(server);
    let output = std::fs::read_to_string(log).unwrap();
    assert!(result.success(), "{output}");
    assert!(!output.contains("checkout-fixture-secret"));
    let requests = requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .all(|(path, _)| path.starts_with("/forge/team/project.git/")),
        "no redirect/prefix escape: {requests:?}"
    );
    if mode == "retired" {
        assert!(requests.is_empty(), "local retirement refuses before HTTP");
        return;
    }
    assert!(!requests.is_empty());
    if mode == "success" {
        assert!(requests.iter().any(|(_, auth)| *auth));
        assert!(requests
            .iter()
            .any(|(path, auth)| *auth && path.ends_with("/git-upload-pack")));
    } else if mode == "bad" {
        assert!(
            requests.iter().all(|(_, auth)| !*auth),
            "no accepted credential: {requests:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_https_clone_fetch_exact_selected_branch() {
    run(
        "authenticated_https_clone_fetch_exact_selected_branch",
        "success",
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_https_credential_reports_only_original_rejection() {
    run(
        "refused_https_credential_reports_only_original_rejection",
        "bad",
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_host_prefix_redirect_never_receives_credential() {
    run(
        "same_host_prefix_redirect_never_receives_credential",
        "redirect",
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_original_callback_never_supplies_credential() {
    run(
        "retired_original_callback_never_supplies_credential",
        "retired",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_redirect_never_sends_a_token_to_another_prefix() {
    run(
        "authenticated_redirect_never_sends_a_token_to_another_prefix",
        "redirect_authenticated",
    )
    .await;
}
