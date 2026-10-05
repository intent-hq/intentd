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
    clone_exact, fetch_exact, fetch_original, push_original, NativeCheckoutCredentials,
    NativeCheckoutSelection, NativeCheckoutSource,
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
    mode: String,
    responses: std::cell::Cell<u32>,
    after_response: Option<Box<dyn Fn() + Send>>,
    after_transfer: Option<Box<dyn Fn() + Send>>,
    publication_calls: std::cell::Cell<u32>,
}
impl NativeCheckoutCredentials for Credential {
    fn with_current(
        &self,
        transfer: &mut (dyn FnMut() -> intent_core::Result<()> + Send),
    ) -> intent_core::Result<()> {
        if self.retired || (self.mode == "retire_before_refs" && self.responses.get() >= 2) {
            return Err(intent_core::Error::Internal(
                "original fixture retired".into(),
            ));
        }
        transfer()?;
        if let Some(after_transfer) = &self.after_transfer {
            self.publication_calls.set(self.publication_calls.get() + 1);
            after_transfer();
        }
        Ok(())
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
        REJECTED.with(|v| v.set(true));
    }
    fn observe(&self, status: u16, _: Option<std::time::Instant>) {
        self.responses.set(self.responses.get() + 1);
        if self.responses.get() == 2 {
            if let Some(after_response) = &self.after_response {
                assert_eq!(status, 200);
                after_response();
            }
        }
        if matches!(status, 401 | 403 | 404) {
            self.rejected();
        }
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
        if self.mode == "retire_before_post" && self.responses.get() > 0 {
            return Err(intent_core::Error::Internal(
                "original fixture retired before POST".into(),
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
        &if mode == "sha_mismatch" {
            "1111111111111111111111111111111111111111".into()
        } else {
            std::env::var("INTENT_NATIVE_SHA").unwrap()
        },
    )
    .unwrap();
    let source = NativeCheckoutSource::https(&url).unwrap();
    let mut credential = Credential {
        bad: mode == "bad",
        retired: mode == "retired",
        url,
        mode: mode.into(),
        responses: std::cell::Cell::new(0),
        after_response: None,
        after_transfer: None,
        publication_calls: std::cell::Cell::new(0),
    };
    let result = clone_exact(&source, &directory, &selection, &mut credential);
    match mode {
        "success"
        | "push"
        | "push_first"
        | "push_retire_before_refs"
        | "push_first_retire_before_refs"
        | "push_ref_locked"
        | "push_newer_tracking"
        | "push_first_newer_tracking"
        | "push_source_changed"
        | "push_head_advanced"
        | "push_branch_changed"
        | "push_lost_response"
        | "fetch_redirect"
        | "push_redirect"
        | "push_post_redirect"
        | "push_post_foreign_origin_redirect"
        | "push_post_port_redirect" => {
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
            assert_eq!(
                std::fs::read_to_string(directory.join("README")).unwrap(),
                "private checked out bytes\n"
            );
            std::fs::write(directory.join("keep-local"), "local").unwrap();
            if mode == "fetch_redirect" {
                assert!(
                    fetch_original(&directory, &source, &selection.branch, &mut credential)
                        .is_err()
                );
            } else {
                assert_eq!(
                    fetch_exact(&source, &directory, &selection, &mut credential).unwrap(),
                    selection
                );
                assert_eq!(
                    fetch_original(&directory, &source, &selection.branch, &mut credential)
                        .unwrap(),
                    selection
                );
            }
            assert_eq!(
                std::fs::read_to_string(directory.join("keep-local")).unwrap(),
                "local"
            );
            if mode.starts_with("push") {
                let branch = if mode.starts_with("push_first") {
                    repo.branch(
                        "new/first-push",
                        &repo.head().unwrap().peel_to_commit().unwrap(),
                        false,
                    )
                    .unwrap();
                    repo.set_head("refs/heads/new/first-push").unwrap();
                    "new/first-push"
                } else {
                    &selection.branch
                };
                let tracking_ref = format!("refs/remotes/origin/{branch}");
                let previous = repo.refname_to_id(&tracking_ref).ok();
                let head = commit(&repo, false);
                let before = intent_git::status::status(&directory).unwrap();
                assert_eq!(before.has_upstream, previous.is_some());
                assert!(!intent_git::history::history(&directory, 1).unwrap()[0].is_pushed);
                if mode.ends_with("retire_before_refs") {
                    credential.mode = "retire_before_refs".into();
                    credential.responses.set(0);
                }
                if mode == "push_ref_locked" {
                    std::fs::write(repo.path().join(format!("{tracking_ref}.lock")), "held")
                        .unwrap();
                }
                if matches!(
                    mode,
                    "push_source_changed" | "push_head_advanced" | "push_branch_changed"
                ) {
                    let path = directory.clone();
                    let mutation = mode.to_owned();
                    credential.responses.set(0);
                    credential.after_response = Some(Box::new(move || {
                        let repo = git2::Repository::open(&path).unwrap();
                        match mutation.as_str() {
                            "push_source_changed" => repo
                                .remote_set_url("origin", "https://changed.invalid/other.git")
                                .unwrap(),
                            "push_head_advanced" => {
                                commit(&repo, false);
                            }
                            "push_branch_changed" => {
                                repo.branch(
                                    "local/other",
                                    &repo.head().unwrap().peel_to_commit().unwrap(),
                                    false,
                                )
                                .unwrap();
                                repo.set_head("refs/heads/local/other").unwrap();
                                commit(&repo, false);
                            }
                            _ => unreachable!(),
                        }
                    }));
                }
                let newer_tracking = if mode.ends_with("newer_tracking") {
                    let newer = commit(&repo, false);
                    repo.reference(
                        &format!("refs/heads/{branch}"),
                        head,
                        true,
                        "restore the original push HEAD",
                    )
                    .unwrap();
                    let path = directory.clone();
                    let name = tracking_ref.clone();
                    credential.after_transfer = Some(Box::new(move || {
                        let repo = git2::Repository::open(&path).unwrap();
                        assert_eq!(repo.refname_to_id(&name).ok(), previous);
                        repo.reference(&name, newer, true, "concurrent tracking publication")
                            .unwrap();
                    }));
                    Some(newer)
                } else {
                    None
                };
                let pushed = push_original(&directory, &source, branch, false, &mut credential);
                if let Some(newer) = newer_tracking {
                    let acknowledged = pushed.unwrap();
                    assert_eq!(acknowledged.branch, branch);
                    assert_eq!(acknowledged.commit_sha, head.to_string());
                    assert_eq!(credential.publication_calls.get(), 1);
                    assert_eq!(repo.head().unwrap().target(), Some(head));
                    assert_eq!(
                        repo.refname_to_id(&tracking_ref).unwrap(),
                        newer,
                        "an acknowledged push must not replace a competing tracking publication"
                    );
                } else if matches!(
                    mode,
                    "push_source_changed" | "push_head_advanced" | "push_branch_changed"
                ) {
                    let acknowledged = pushed.unwrap();
                    assert_eq!(acknowledged.branch, branch);
                    assert_eq!(acknowledged.commit_sha, head.to_string());
                    if mode == "push_source_changed" {
                        assert_eq!(repo.refname_to_id(&tracking_ref).ok(), previous);
                        assert_eq!(repo.head().unwrap().target(), Some(head));
                        assert_eq!(
                            repo.find_remote("origin").unwrap().url().unwrap(),
                            "https://changed.invalid/other.git"
                        );
                    } else {
                        assert_eq!(repo.refname_to_id(&tracking_ref).unwrap(), head);
                        assert_ne!(repo.head().unwrap().target(), Some(head));
                        if mode == "push_branch_changed" {
                            assert_eq!(
                                repo.head().unwrap().name().unwrap(),
                                "refs/heads/local/other"
                            );
                            assert!(repo
                                .find_reference("refs/remotes/origin/local/other")
                                .is_err());
                        } else {
                            assert_eq!(intent_git::status::status(&directory).unwrap().ahead, 1);
                        }
                    }
                } else if matches!(
                    mode,
                    "push_first"
                        | "push_retire_before_refs"
                        | "push_first_retire_before_refs"
                        | "push_ref_locked"
                ) {
                    let acknowledged = pushed.unwrap();
                    assert_eq!(acknowledged.branch, branch);
                    assert_eq!(acknowledged.commit_sha, head.to_string());
                    if mode == "push_first" {
                        assert_pushed(&directory, branch, head);
                    } else {
                        assert_eq!(repo.refname_to_id(&tracking_ref).ok(), previous);
                        assert!(!intent_git::history::history(&directory, 1).unwrap()[0].is_pushed);
                        if mode.ends_with("retire_before_refs") {
                            assert!(
                                credential.with_current(&mut || Ok(())).is_err(),
                                "late delivery remains refused"
                            );
                        }
                    }
                } else if mode == "push" {
                    assert_eq!(pushed.unwrap().commit_sha, head.to_string());
                    assert_pushed(&directory, branch, head);
                    let orphan = commit(&repo, true);
                    assert!(matches!(
                        push_original(
                            &directory,
                            &source,
                            &selection.branch,
                            false,
                            &mut credential
                        ),
                        Err(intent_core::Error::InvalidParams(_))
                    ));
                    assert_eq!(
                        push_original(
                            &directory,
                            &source,
                            &selection.branch,
                            true,
                            &mut credential
                        )
                        .unwrap()
                        .commit_sha,
                        orphan.to_string()
                    );
                    assert_eq!(repo.head().unwrap().target(), Some(orphan));
                    assert_pushed(&directory, branch, orphan);
                } else {
                    assert!(pushed.is_err());
                    if mode == "push_lost_response" {
                        assert!(pushed.unwrap_err().to_string().contains("unknown"));
                    }
                    assert_eq!(repo.head().unwrap().target(), Some(head));
                    assert_eq!(repo.refname_to_id(&tracking_ref).ok(), previous);
                }
            }
            assert!(!REJECTED.with(std::cell::Cell::get));
            if mode != "push_source_changed" {
                assert_eq!(
                    repo.find_remote("origin").unwrap().url().unwrap(),
                    source.url()
                );
            }
        }
        "bad" => {
            assert!(
                matches!(result, Err(intent_core::Error::GitAuthorization(_))),
                "{result:?}"
            );
            assert!(REJECTED.with(std::cell::Cell::get));
            assert!(!directory.exists());
        }
        _ => {
            assert!(result.is_err(), "{mode} must refuse");
            assert!(!directory.exists());
            assert!(
                !REJECTED.with(std::cell::Cell::get),
                "local/redirect/parse refusal is not upstream denial"
            );
        }
    }
}

fn assert_pushed(path: &Path, branch: &str, acknowledged: git2::Oid) {
    let repo = git2::Repository::open(path).unwrap();
    assert_eq!(
        repo.refname_to_id(&format!("refs/remotes/origin/{branch}"))
            .unwrap(),
        acknowledged
    );
    let status = intent_git::status::status(path).unwrap();
    assert!(status.has_upstream);
    assert_eq!(
        (status.ahead, status.behind, status.unpushed_count),
        (0, 0, Some(0))
    );
    assert!(intent_git::history::history(path, 1).unwrap()[0].is_pushed);
}

fn commit(repo: &git2::Repository, orphan: bool) -> git2::Oid {
    let old = repo.head().unwrap().peel_to_commit().unwrap();
    let signature = git2::Signature::now("fixture", "fixture@example.invalid").unwrap();
    let tree = old.tree().unwrap();
    let parents = if orphan { vec![] } else { vec![&old] };
    let new = repo
        .commit(
            None,
            &signature,
            &signature,
            if orphan {
                "forced independent history"
            } else {
                "authorized next commit"
            },
            &tree,
            &parents,
        )
        .unwrap();
    repo.reference(
        repo.head().unwrap().name().unwrap(),
        new,
        true,
        "fixture commit",
    )
    .unwrap();
    new
}

fn repository(root: &Path) -> String {
    let path = root.join("forge/team/project.git");
    std::fs::create_dir_all(&path).unwrap();
    let repo = git2::Repository::init_bare(&path).unwrap();
    repo.config()
        .unwrap()
        .set_bool("http.receivepack", true)
        .unwrap();
    let blob = repo.blob(b"private checked out bytes\n").unwrap();
    let mut builder = repo.treebuilder(None).unwrap();
    builder.insert("README", blob, 0o100_644).unwrap();
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
    let mut params = CertificateParams::new(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
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
        .env(
            "CONTENT_TYPE",
            if path.ends_with("git-receive-pack") {
                "application/x-git-receive-pack-request"
            } else {
                "application/x-git-upload-pack-request"
            },
        )
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
    let forbidden = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let forbidden_port = forbidden.local_addr().unwrap().port();
    let forbidden_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = forbidden_connections.clone();
    let forbidden_server = Server(tokio::spawn(async move {
        loop {
            let (stream, _) = forbidden.accept().await.unwrap();
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            drop(stream);
        }
    }));
    let url = format!("https://127.0.0.1:{port}/forge/team/project.git");
    let requests = Arc::new(Mutex::new(Vec::<(String, bool)>::new()));
    let observed = requests.clone();
    let root = scratch.path().to_path_buf();
    let redirect = mode == "redirect";
    let redirect_authenticated = mode == "redirect_authenticated";
    let mode_owned = mode.to_owned();
    let server = Server(tokio::spawn(async move {
        let mut request_count = 0;
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
                request_count += 1;
                let length = header.lines().find_map(|line| line.split_once(':').and_then(|(key, value)| key.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().unwrap()))).unwrap_or(0);
                assert!(length < 65536);
                let mut body = vec![0; length];
                stream.read_exact(&mut body).await.unwrap();
                let should_redirect = redirect || (redirect_authenticated && authenticated)
                    || mode_owned == "foreign_origin_redirect" || mode_owned == "port_redirect"
                    || (mode_owned.starts_with("upload_post_") && target.ends_with("/git-upload-pack"))
                    || (mode_owned == "fetch_redirect" && request_count > 2)
                    || (mode_owned == "push_redirect" && target.contains("git-receive-pack"))
                    || (mode_owned.starts_with("push_post_") && target.ends_with("/git-receive-pack"));
                let mut response = if should_redirect {
                    if mode_owned.ends_with("foreign_origin_redirect") || mode_owned.ends_with("port_redirect") {
                        let location = if mode_owned.ends_with("foreign_origin_redirect") { format!("https://localhost:{forbidden_port}/forge/team/project.git") } else { format!("https://127.0.0.1:{forbidden_port}/forge/team/project.git") };
                        let response = format!("HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                        stream.write_all(response.as_bytes()).await.unwrap();
                        let _ = stream.shutdown().await;
                        return;
                    }
                    format!("HTTP/1.1 302 Found\r\nLocation: https://127.0.0.1:{port}/another-prefix/private.git\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").into_bytes()
                } else if !authenticated {
                    b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"fixture\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
                } else if mode_owned == "malformed_pack" && target.ends_with("/git-upload-pack") {
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/x-git-upload-pack-result\r\nContent-Length: 25\r\nConnection: close\r\n\r\n0008NAK\n000d\x01bad-pack0000".to_vec()
                } else {
                    let root = root.clone();
                    let method = first[0].to_string();
                    let target = target.clone();
                    tokio::task::spawn_blocking(move || backend(&root, &method, &target, &body)).await.unwrap()
                };
                if mode_owned == "truncated_pack" && target.ends_with("/git-upload-pack") {
                    response.truncate(response.len() - 16);
                }
                if mode_owned == "push_lost_response" && target.ends_with("/git-receive-pack") { return; }
                stream.write_all(&response).await.unwrap();
                let _ = stream.shutdown().await;
            }).await.expect("fixture connection timed out");
        }
    }));
    let log = scratch.path().join("child.log");
    let out = std::fs::File::create(&log).unwrap();
    let empty_path = scratch.path().join("empty-bin");
    std::fs::create_dir(&empty_path).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env_clear()
        .env("PATH", empty_path)
        .env("HOME", scratch.path())
        .env("XDG_CONFIG_HOME", scratch.path())
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("SSL_CERT_FILE", ca_path)
        .env("SSL_CERT_DIR", ca_dir)
        .env(CHILD, mode)
        .env("INTENT_NATIVE_URL", url)
        .env("INTENT_NATIVE_SHA", &sha)
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
    drop(forbidden_server);
    assert_eq!(
        forbidden_connections.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no connection to redirect origin/port"
    );
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
    if mode == "bad" || mode == "retire_before_post" || mode == "sha_mismatch" {
        assert_eq!(requests.len(), 1);
    }
    if mode == "retire_before_refs" {
        assert_eq!(requests.len(), 2);
    }
    if matches!(
        mode,
        "push"
            | "push_first"
            | "push_retire_before_refs"
            | "push_first_retire_before_refs"
            | "push_ref_locked"
            | "push_newer_tracking"
            | "push_first_newer_tracking"
            | "push_source_changed"
            | "push_head_advanced"
            | "push_branch_changed"
            | "push_lost_response"
    ) {
        let repo =
            git2::Repository::open_bare(scratch.path().join("forge/team/project.git")).unwrap();
        let branch = if mode.starts_with("push_first") {
            "new/first-push"
        } else {
            "feature/beyond-page-one"
        };
        let remote = repo.refname_to_id(&format!("refs/heads/{branch}")).unwrap();
        assert_ne!(
            remote.to_string(),
            sha,
            "the actual remote effect is retained"
        );
        if mode.ends_with("newer_tracking") {
            let local = git2::Repository::open(scratch.path().join("checkout")).unwrap();
            assert_eq!(Some(remote), local.head().unwrap().target());
        }
        if mode != "push" {
            assert_eq!(
                requests
                    .iter()
                    .filter(|(path, _)| path.ends_with("/git-receive-pack"))
                    .count(),
                1,
                "ref publication refusal must not retry the confirmed push"
            );
        }
    }
    if matches!(
        mode,
        "success"
            | "push"
            | "push_first"
            | "push_retire_before_refs"
            | "push_first_retire_before_refs"
            | "push_ref_locked"
            | "push_newer_tracking"
            | "push_first_newer_tracking"
            | "push_source_changed"
            | "push_head_advanced"
            | "push_branch_changed"
            | "push_lost_response"
            | "fetch_redirect"
            | "push_redirect"
            | "push_post_redirect"
            | "push_post_foreign_origin_redirect"
            | "push_post_port_redirect"
    ) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_native_fetch_push_preserve_head_and_force() {
    run(
        "authenticated_native_fetch_push_preserve_head_and_force",
        "push",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_native_push_with_lost_response_retains_uncertain_effect() {
    run(
        "admitted_native_push_with_lost_response_retains_uncertain_effect",
        "push_lost_response",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_advertisement_redirect_never_reuses_private_connection() {
    run(
        "fetch_advertisement_redirect_never_reuses_private_connection",
        "fetch_redirect",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_advertisement_redirect_never_reuses_private_connection() {
    run(
        "push_advertisement_redirect_never_reuses_private_connection",
        "push_redirect",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_pack_redirect_never_reuses_private_connection() {
    run(
        "push_pack_redirect_never_reuses_private_connection",
        "push_post_redirect",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_pack_redirect_never_reuses_private_connection() {
    run(
        "upload_pack_redirect_never_reuses_private_connection",
        "upload_post_redirect",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_foreign_origin_redirect_is_refused() {
    run(
        "native_foreign_origin_redirect_is_refused",
        "foreign_origin_redirect",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_port_redirect_is_refused() {
    run("native_port_redirect_is_refused", "port_redirect").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_malformed_pack_never_publishes_checkout() {
    run(
        "native_malformed_pack_never_publishes_checkout",
        "malformed_pack",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_retirement_before_post_sends_no_pack_request() {
    run(
        "native_retirement_before_post_sends_no_pack_request",
        "retire_before_post",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_retirement_after_pack_refuses_ref_publication() {
    run(
        "native_retirement_after_pack_refuses_ref_publication",
        "retire_before_refs",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_upload_post_foreign_origin_redirect_is_refused() {
    run(
        "native_upload_post_foreign_origin_redirect_is_refused",
        "upload_post_foreign_origin_redirect",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_upload_post_port_redirect_is_refused() {
    run(
        "native_upload_post_port_redirect_is_refused",
        "upload_post_port_redirect",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_receive_post_foreign_origin_redirect_is_refused() {
    run(
        "native_receive_post_foreign_origin_redirect_is_refused",
        "push_post_foreign_origin_redirect",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_receive_post_port_redirect_is_refused() {
    run(
        "native_receive_post_port_redirect_is_refused",
        "push_post_port_redirect",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_truncated_valid_pack_never_publishes_checkout() {
    run(
        "native_truncated_valid_pack_never_publishes_checkout",
        "truncated_pack",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_moved_selected_sha_refuses_before_pack_request() {
    run(
        "native_moved_selected_sha_refuses_before_pack_request",
        "sha_mismatch",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_first_push_creates_tracking_ref() {
    run("native_first_push_creates_tracking_ref", "push_first").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_native_push_survives_retired_ref_and_delivery_admission() {
    run(
        "confirmed_native_push_survives_retired_ref_and_delivery_admission",
        "push_retire_before_refs",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_native_first_push_does_not_publish_after_retirement() {
    run(
        "confirmed_native_first_push_does_not_publish_after_retirement",
        "push_first_retire_before_refs",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_native_push_survives_locked_tracking_ref() {
    run(
        "confirmed_native_push_survives_locked_tracking_ref",
        "push_ref_locked",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_push_preserves_newer_tracking_publication() {
    run(
        "native_push_preserves_newer_tracking_publication",
        "push_newer_tracking",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_first_push_preserves_newer_tracking_publication() {
    run(
        "native_first_push_preserves_newer_tracking_publication",
        "push_first_newer_tracking",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_native_push_preserves_changed_source_without_tracking_publication() {
    run(
        "confirmed_native_push_preserves_changed_source_without_tracking_publication",
        "push_source_changed",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_native_push_tracks_acknowledged_sha_after_head_advances() {
    run(
        "confirmed_native_push_tracks_acknowledged_sha_after_head_advances",
        "push_head_advanced",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_native_push_tracks_original_branch_after_branch_switch() {
    run(
        "confirmed_native_push_tracks_original_branch_after_branch_switch",
        "push_branch_changed",
    )
    .await;
}
