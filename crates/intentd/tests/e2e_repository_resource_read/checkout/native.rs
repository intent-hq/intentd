//! Original producer plus authenticated HTTPS Git, using only disposable stores.
//! The isolated test child receives a fixture CA; production needs no helper opt-in.
use super::*;
use intentd_test_support::GuardedChild;
use rustls_pki_types::PrivatePkcs8KeyDer;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const CHILD: &str = "INTENT_CHECKOUT_WIRE_FIXTURE";
const LIMIT: Duration = Duration::from_secs(45);

fn run_tool(mut command: Command) {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = GuardedChild::spawn(&mut command).unwrap();
    assert!(child.wait_with_timeout(LIMIT).unwrap().unwrap().success());
}
fn tls(root: &Path) -> Arc<rustls::ServerConfig> {
    let mut req = Command::new("openssl");
    req.args([
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=checkout-fixture-ca",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout",
    ])
    .arg(root.join("ca-key.pem"))
    .arg("-out")
    .arg(root.join("ca.pem"));
    run_tool(req);
    let mut leaf = Command::new("openssl");
    leaf.args([
        "req",
        "-new",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-subj",
        "/CN=localhost",
        "-keyout",
    ])
    .arg(root.join("key.pem"))
    .arg("-out")
    .arg(root.join("server.csr"));
    run_tool(leaf);
    std::fs::write(
        root.join("server.ext"),
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n",
    ).unwrap();
    let mut sign = Command::new("openssl");
    sign.args(["x509", "-req", "-in"])
        .arg(root.join("server.csr"))
        .arg("-CA")
        .arg(root.join("ca.pem"))
        .arg("-CAkey")
        .arg(root.join("ca-key.pem"))
        .args(["-CAcreateserial", "-days", "1", "-extfile"])
        .arg(root.join("server.ext"))
        .arg("-out")
        .arg(root.join("server.pem"));
    run_tool(sign);
    let mut cert = Command::new("openssl");
    cert.args(["x509", "-in"])
        .arg(root.join("server.pem"))
        .args(["-outform", "DER", "-out"])
        .arg(root.join("cert.der"));
    run_tool(cert);
    let mut key = Command::new("openssl");
    key.args(["pkcs8", "-topk8", "-nocrypt", "-in"])
        .arg(root.join("key.pem"))
        .args(["-outform", "DER", "-out"])
        .arg(root.join("key.der"));
    run_tool(key);
    Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(
                std::fs::read(root.join("cert.der")).unwrap(),
            )],
            PrivatePkcs8KeyDer::from(std::fs::read(root.join("key.der")).unwrap()).into(),
        )
        .unwrap(),
    )
}
fn git(path: &Path, args: &[&str]) -> String {
    let mut command = Command::new("git");
    command
        .args([
            "-c",
            "maintenance.autoDetach=false",
            "-c",
            "gc.autoDetach=false",
        ])
        .arg("-C")
        .arg(path)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "checkout-fixture")
        .env("GIT_AUTHOR_EMAIL", "checkout@example.invalid")
        .env("GIT_COMMITTER_NAME", "checkout-fixture")
        .env("GIT_COMMITTER_EMAIL", "checkout@example.invalid")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = GuardedChild::spawn(&mut command).unwrap();
    assert!(
        child.wait_with_timeout(LIMIT).unwrap().unwrap().success(),
        "owned fixture Git {args:?}"
    );
    let mut output = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut output)
        .unwrap();
    output.trim().to_owned()
}
fn repository(root: &Path) -> String {
    let source = root.join("seed");
    std::fs::create_dir(&source).unwrap();
    git(&source, &["init", "-b", "main"]);
    std::fs::write(source.join("README"), "private original checkout\n").unwrap();
    git(&source, &["add", "README"]);
    git(&source, &["commit", "-m", "main"]);
    git(&source, &["checkout", "-b", "release/later-page"]);
    git(&source, &["commit", "--allow-empty", "-m", "selected"]);
    let selected = git(&source, &["rev-parse", "HEAD"]);
    let destination = root.join("install/Team/Sub/Project.git");
    std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
    git(
        root,
        &[
            "clone",
            "--bare",
            source.to_str().unwrap(),
            destination.to_str().unwrap(),
        ],
    );
    git(&destination, &["symbolic-ref", "HEAD", "refs/heads/main"]);
    git(&destination, &["config", "http.receivepack", "true"]);
    selected
}

#[intent_test_macros::daemon_test]
async fn checkout_legacy_local_repository_and_public_git_route_remain_available() {
    let h = Harness::with_workspace(false).await;
    let initial_provider_calls = h
        .server
        .as_ref()
        .unwrap()
        .state
        .routes
        .lock()
        .unwrap()
        .clone();
    let selected = repository(h.dir.path());
    let source = h.dir.path().join("seed");
    let bare = h.dir.path().join("install/Team/Sub/Project.git");
    git(
        &source,
        &["remote", "add", "origin", bare.to_str().unwrap()],
    );
    let mut client = h.uds().await;
    let response = client
        .rpc(
            "workspace.create",
            json!({"repositoryPath":source,"branch":"legacy/local","baseRef":"release/later-page"}),
        )
        .await;
    let result = success(&response);
    let workspace = result.get("workspace").unwrap_or(result);
    let path = Path::new(workspace["worktreePath"].as_str().unwrap());
    assert_eq!(git(path, &["rev-parse", "HEAD"]), selected);
    git(
        path,
        &["commit", "--allow-empty", "-m", "legacy local route"],
    );
    let pushed = git(path, &["rev-parse", "HEAD"]);
    let response = client
        .rpc(
            "git.push",
            json!({"workspaceId":workspace["id"],"force":false}),
        )
        .await;
    assert_eq!(success(&response)["pushedSha"], pushed);
    success(
        &client
            .rpc("git.fetch", json!({"workspaceId":workspace["id"]}))
            .await,
    );
    assert_eq!(
        git(&bare, &["rev-parse", "refs/heads/legacy/local"]),
        pushed
    );
    assert_eq!(
        git(path, &["rev-parse", "refs/remotes/origin/legacy/local"]),
        pushed
    );
    assert_eq!(
        *h.server.as_ref().unwrap().state.routes.lock().unwrap(),
        initial_provider_calls,
        "local Git does not add provider requests after fixture authentication"
    );
    client.close().await;
    h.finish().await;
}

fn backend(root: PathBuf, method: String, target: &str, body: &[u8]) -> Vec<u8> {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env("GIT_PROJECT_ROOT", root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REMOTE_USER", "checkout-fixture")
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
    let mut bytes = Vec::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(child.wait_with_timeout(LIMIT).unwrap().unwrap().success());
    let split = bytes.windows(4).position(|b| b == b"\r\n\r\n").unwrap();
    let headers = String::from_utf8(bytes[..split].to_vec()).unwrap();
    assert!(!headers.contains("Status:"));
    let body = &bytes[split + 4..];
    let mut result = format!(
        "HTTP/1.1 200 OK\r\n{headers}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    result.extend_from_slice(body);
    result
}

async fn original_child(instance: &str, sha: &str) {
    let h = Harness::from_server(false, Server::at_instance(instance).await).await;
    let server = h.server.as_ref().unwrap();
    let project = json!({"id":42,"path_with_namespace":PROJECT,"web_url":format!("{instance}/{PROJECT}"),"default_branch":"main"});
    set(
        server,
        "/api/v4/projects/Team%2FSub%2FProject",
        project.clone(),
        None,
    );
    set(server, "/api/v4/projects", json!([project]), None);
    let selected = json!({"name":"release/later-page","commit":{"id":sha},"protected":false});
    set(
        server,
        &format!("{BRANCHES}?page=1&per_page=1"),
        json!([branch("main")]),
        Some("2"),
    );
    set(
        server,
        &format!("{BRANCHES}?page=2&per_page=1"),
        json!([selected]),
        None,
    );
    let mut client = h.wss(TOKEN).await;
    let c = ready(
        &client
            .rpc(
                "sourceControl.checkout.capture",
                json!({"provider":"gitlab","instanceBaseUrl":instance}),
            )
            .await,
    )
    .clone();
    let mut b = project_query(&c);
    b["limit"] = json!(1);
    let first = ready(
        &client
            .rpc("sourceControl.checkout.branches", b.clone())
            .await,
    )
    .clone();
    b["cursor"] = first["nextCursor"].clone();
    assert_eq!(
        ready(&client.rpc("sourceControl.checkout.branches", b).await)["items"][0]["commitSha"],
        sha
    );
    let mut selection = project_query(&c);
    selection["branch"] = json!("release/later-page");
    selection["commitSha"] = json!(sha);
    selection["mode"] = json!("cached");
    assert_eq!(
        ready(
            &client
                .rpc("sourceControl.checkout.warm", selection.clone())
                .await
        )["commitSha"],
        sha
    );
    let count = server.count();
    let mut cached = project_query(&c);
    cached["cached"] = json!(true);
    let cache_page = ready(
        &client
            .rpc("sourceControl.checkout.branches", cached.clone())
            .await,
    )
    .clone();
    assert_eq!(cache_page["cached"], true);
    assert_eq!(
        server.count(),
        count,
        "authorized cache hit needs no provider probe"
    );
    assert!(cache_page["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|b| b["name"] == "release/later-page" && b["commitSha"] == sha));
    moved_branch_create_controls(&h, &mut client, &selection, sha).await;
    let mut created = Vec::new();
    for mode in ["direct", "cached"] {
        let mut creator = h.wss(if mode == "cached" { MEMBER } else { TOKEN }).await;
        let c = recovered_capture(&mut creator, instance).await;
        let mut query = project_query(&c);
        query["limit"] = json!(1);
        let page = creator
            .rpc("sourceControl.checkout.branches", query.clone())
            .await;
        query["cursor"] = ready(&page)["nextCursor"].clone();
        let page = creator.rpc("sourceControl.checkout.branches", query).await;
        assert_eq!(ready(&page)["items"][0]["commitSha"], sha);
        let mut selection = project_query(&c);
        selection["branch"] = json!("release/later-page");
        selection["commitSha"] = json!(sha);
        selection["mode"] = json!(mode);
        let resource = if mode == "direct" {
            "merge_requests"
        } else {
            "issues"
        };
        let context_url = format!("{instance}/{PROJECT}/-/{resource}/7?view=parallel#note_1");
        let resolved = creator
            .rpc(
                "sourceControl.checkout.project",
                json!({"checkoutId":c["checkoutId"],"revision":c["revision"],"url":context_url}),
            )
            .await;
        assert_eq!(ready(&resolved)["project"]["projectPath"], PROJECT);
        assert_eq!(ready(&resolved)["contextUrl"], context_url);
        let links = json!([{"kind":if mode=="direct" {"pr"} else {"issue"},"url":context_url,"owner":"Team/Sub","repo":"Project","number":7}]);
        let response=creator.rpc("workspace.create",json!({"repositoryCheckout":selection,"branch":format!("workspace/{mode}"),"contextLinks":links,"idempotencyKey":format!("checkout-{mode}")})).await;
        let result = success(&response);
        let workspace = result.get("workspace").unwrap_or(result);
        let path = workspace["worktreePath"]
            .as_str()
            .expect("standalone native checkout path");
        assert!(Path::new(path).starts_with(h.dir.path()));
        let path = Path::new(path);
        git(
            path,
            &[
                "config",
                "credential.helper",
                &std::env::var("INTENT_CHECKOUT_HELPER").unwrap(),
            ],
        );
        assert_eq!(git(path, &["rev-parse", "HEAD"]), sha);
        assert_eq!(
            git(path, &["branch", "--show-current"]),
            format!("workspace/{mode}")
        );
        assert_eq!(
            git(path, &["remote", "get-url", "origin"]),
            format!("{instance}/{PROJECT}.git")
        );
        assert_eq!(
            std::fs::read_to_string(path.join("README")).unwrap(),
            "private original checkout\n"
        );
        assert_eq!(workspace["baseCommitSha"], sha);
        assert_eq!(workspace["contextLinks"], links);
        created.push((workspace["id"].clone(), path.to_path_buf()));
        let identity = workspace["id"].clone();
        for first in [true, false] {
            git(
                path,
                &[
                    "commit",
                    "--allow-empty",
                    "-m",
                    if first {
                        "native push control"
                    } else {
                        "subsequent native push control"
                    },
                ],
            );
            let pushed = git(path, &["rev-parse", "HEAD"]);
            let before_status = creator
                .rpc(
                    "git.status",
                    json!({"workspaceId":identity,"forceRefresh":true}),
                )
                .await;
            assert_eq!(success(&before_status)["hasUpstream"], !first);
            if !first {
                assert_eq!(success(&before_status)["ahead"], 1);
                assert_eq!(success(&before_status)["unpushedCount"], 1);
            }
            let before_events = creator
                .rpc(
                    "event.query",
                    json!({"workspaceId":identity,"eventType":"changes:git-status"}),
                )
                .await;
            let prior: Vec<_> = success(&before_events)
                .as_array()
                .unwrap()
                .iter()
                .map(|event| event["id"].clone())
                .collect();
            let push = creator
                .rpc("git.push", json!({"workspaceId": identity, "force":false}))
                .await;
            assert_eq!(success(&push)["pushedSha"], pushed);
            assert_eq!(success(&push)["branch"], format!("workspace/{mode}"));
            assert_eq!(
                git(
                    path,
                    &[
                        "rev-parse",
                        &format!("refs/remotes/origin/workspace/{mode}")
                    ]
                ),
                pushed
            );
            let status = creator
                .rpc("git.status", json!({"workspaceId":identity}))
                .await;
            assert_eq!(success(&status)["hasUpstream"], true);
            assert_eq!(success(&status)["ahead"], 0);
            assert_eq!(success(&status)["behind"], 0);
            assert_eq!(success(&status)["unpushedCount"], 0);
            let history = creator
                .rpc(
                    "file-tracking.loadCommits",
                    json!({"workspaceId":identity,"limit":1}),
                )
                .await;
            assert_eq!(success(&history)["commits"][0]["hash"], pushed);
            assert_eq!(success(&history)["commits"][0]["isPushed"], true);
            let events = creator
                .rpc(
                    "event.query",
                    json!({"workspaceId":identity,"eventType":"changes:git-status"}),
                )
                .await;
            assert!(
                success(&events)
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|event| !prior.contains(&event["id"]))
                    .any(|event| {
                        let status = &event["data"]["status"];
                        status["branch"] == format!("workspace/{mode}")
                            && status["hasUpstream"] == true
                            && status["ahead"] == 0
                            && status["unpushedCount"] == 0
                    }),
                "this push must emit its updated tracking status before any fetch: {events}"
            );
        }
        let pushed = git(path, &["rev-parse", "HEAD"]);
        success(
            &creator
                .rpc("git.fetch", json!({"workspaceId":identity}))
                .await,
        );
        assert_eq!(
            git(path, &["rev-parse", "HEAD"]),
            pushed,
            "fetch never resets HEAD"
        );
        assert_eq!(
            git(
                path,
                &[
                    "rev-parse",
                    &format!("refs/remotes/origin/workspace/{mode}")
                ]
            ),
            pushed
        );
        creator.close().await;
    }
    // Retain an original private A response across the real public replacement.
    let mut late_client = h.wss(TOKEN).await;
    let late_capture = ready(
        &late_client
            .rpc(
                "sourceControl.checkout.capture",
                json!({"provider":"gitlab","instanceBaseUrl":instance}),
            )
            .await,
    )
    .clone();
    *server.state.pause.lock().unwrap() = Some("/api/v4/projects".into());
    let late = tokio::spawn(async move {
        let response = late_client
            .rpc("sourceControl.checkout.projects", bound(&late_capture))
            .await;
        late_client.close().await;
        response
    });
    tokio::time::timeout(ACQUIRE, server.state.entered.notified())
        .await
        .unwrap();
    // The actual public auth flow replaces A, not an injected renderer credential.
    assert_eq!(
        success(
            &client
                .rpc(
                    "sourceControl.revoke",
                    json!({"provider":"gitlab","instanceBaseUrl":instance})
                )
                .await
        )["ok"],
        true
    );
    assert!(client
        .rpc("sourceControl.checkout.branches", cached.clone())
        .await
        .get("error")
        .is_some());
    set(
        server,
        "/api/v4/user",
        json!({"id":82,"username":"denied-fixture","name":"Denied fixture"}),
        None,
    );
    let connected=client.rpc("sourceControl.connect",json!({"provider":"gitlab","instanceBaseUrl":instance,"method":"pat","token":"denied-pat"})).await;
    assert!(connected.get("error").is_none(), "{connected}");
    server.set("/api/v4/projects/Team%2FSub%2FProject", 403);
    let replacement = ready(
        &client
            .rpc(
                "sourceControl.checkout.capture",
                json!({"provider":"gitlab","instanceBaseUrl":instance}),
            )
            .await,
    )
    .clone();
    let mut q = project_query(&replacement);
    q["cached"] = json!(true);
    let refused = client.rpc("sourceControl.checkout.branches", q).await;
    assert_eq!(success(&refused)["status"], "unavailable");
    assert!(success(&refused).get("value").is_none());
    server.state.release.notify_one();
    let late = tokio::time::timeout(ACQUIRE, late).await.unwrap().unwrap();
    assert!(late.get("error").is_some() || late["result"]["status"] == "unavailable");
    assert!(
        !late.to_string().contains(PROJECT),
        "late original A private payload is never delivered"
    );
    // Authorized recovery is a new verified public PAT connection, not a fresh capture.
    set(
        server,
        "/api/v4/user",
        json!({"id":42,"username":"fixture","name":"Fixture"}),
        None,
    );
    set(
        server,
        "/api/v4/projects/Team%2FSub%2FProject",
        json!({"id":42,"path_with_namespace":PROJECT,"web_url":format!("{instance}/{PROJECT}"),"default_branch":"main"}),
        None,
    );
    let recovery=client.rpc("sourceControl.connect",json!({"provider":"gitlab","instanceBaseUrl":instance,"method":"pat","token":"stored-pat"})).await;
    assert!(recovery.get("error").is_none(), "{recovery}");
    let recovered = ready(
        &client
            .rpc(
                "sourceControl.checkout.capture",
                json!({"provider":"gitlab","instanceBaseUrl":instance}),
            )
            .await,
    )
    .clone();
    let mut b = project_query(&recovered);
    b["limit"] = json!(1);
    let first = ready(
        &client
            .rpc("sourceControl.checkout.branches", b.clone())
            .await,
    )
    .clone();
    b["cursor"] = first["nextCursor"].clone();
    assert_eq!(
        ready(&client.rpc("sourceControl.checkout.branches", b).await)["items"][0]["commitSha"],
        sha
    );
    let mut recovered_selection = project_query(&recovered);
    recovered_selection["branch"] = json!("release/later-page");
    recovered_selection["commitSha"] = json!(sha);
    recovered_selection["mode"] = json!("cached");
    assert_eq!(
        ready(
            &client
                .rpc("sourceControl.checkout.warm", recovered_selection)
                .await
        )["commitSha"],
        sha
    );
    late_completion_controls(&h, &mut client, instance, sha, &created[0]).await;
    let status = client
        .rpc(
            "sourceControl.authStatus",
            json!({"provider":"gitlab","instanceBaseUrl":instance}),
        )
        .await;
    assert_eq!(success(&status)["instanceBaseUrl"], instance);
    client.close().await;
    h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkout_original_tls_wire_direct_cached_exact_head_and_private_account_replacement() {
    intent_core::caller::with_caller(intent_core::Caller::Daemon,async {
        if std::env::var_os(CHILD).is_some() {
            original_child(&std::env::var("INTENT_CHECKOUT_INSTANCE").unwrap(),&std::env::var("INTENT_CHECKOUT_SHA").unwrap()).await;
            return;
        }
        let directory=common::test_tempdir("checkout-wire-native-");
        let sentinels=directory.path().join("sentinels");std::fs::create_dir(&sentinels).unwrap();
        let helper_log=directory.path().join("unexpected-helper.log");
        for name in ["gh","glab","credential-sentinel"] {
            let path=sentinels.join(name);
            std::fs::write(&path,b"#!/bin/sh\nprintf '%s\\n' unexpected-helper >> \"$INTENT_CHECKOUT_HELPER_LOG\"\nexit 87\n").unwrap();
            std::fs::set_permissions(path,std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let sha=repository(directory.path());let config=tls(directory.path());
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let instance=format!("https://127.0.0.1:{}/install",listener.local_addr().unwrap().port());
        let root=directory.path().to_path_buf();let (stop,mut stopped)=tokio::sync::oneshot::channel();
        let requests=Arc::new(AtomicUsize::new(0));let observed=requests.clone();
        let server=tokio::spawn(async move {
            let mut children=tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _=&mut stopped=>break,
                    accepted=listener.accept()=>{
                        let (socket,_)=accepted.unwrap();let config=config.clone();let root=root.clone();let observed=observed.clone();
                        children.spawn(async move {
                            let mut stream=match tokio_rustls::TlsAcceptor::from(config).accept(socket).await {
                                Ok(stream)=>stream,
                                Err(error)=>{eprintln!("owned checkout TLS handshake failed: {error}");return;}
                            };
                            tokio::time::timeout(LIMIT,async {
                                let mut header=Vec::new();let mut byte=[0];
                                while !header.ends_with(b"\r\n\r\n") {
                                    if stream.read(&mut byte).await.unwrap_or(0)==0{return;}
                                    header.push(byte[0]);assert!(header.len()<16384);
                                }
                                let header=String::from_utf8(header).unwrap();
                                let mut words=header.lines().next().unwrap().split_whitespace();
                                let method=words.next().unwrap().to_owned();let target=words.next().unwrap().to_owned();
                                eprintln!("owned checkout TLS request {method} {target}");
                                assert!(target.starts_with("/install/Team/Sub/Project.git/"));
                                std::fs::OpenOptions::new().create(true).append(true).open(root.join("https-requests")).unwrap().write_all(b".").unwrap();
                                let authorized=header.lines().any(|line|line.split_once(':').is_some_and(|(key,value)|key.eq_ignore_ascii_case("authorization") && value.trim()=="Basic b2F1dGgyOnN0b3JlZC1wYXQ="));
                                let length=header.lines().find_map(|line|line.split_once(':').and_then(|(key,value)|key.eq_ignore_ascii_case("content-length").then(||value.trim().parse::<usize>().unwrap()))).unwrap_or(0);
                                assert!(length<65536);let mut body=vec![0;length];stream.read_exact(&mut body).await.unwrap();
                                let response=if authorized {
                                    observed.fetch_add(1,Ordering::SeqCst);
                                    let stage=if method=="POST" && target.ends_with("/git-upload-pack") {Some("upload")} else if method=="POST" && target.ends_with("/git-receive-pack") {Some("receive")} else {None};
                                    let hold=stage.filter(|stage|std::fs::rename(root.join(format!("hold-{stage}")),root.join(format!("active-{stage}"))).is_ok());
                                    let phase_root=root.clone();
                                    let response=tokio::task::spawn_blocking(move ||backend(root,method,&target,&body)).await.unwrap();
                                    if let Some(stage)=hold {
                                        std::fs::write(phase_root.join(format!("{stage}-entered")),[]).unwrap();
                                        wait_for_file(&phase_root.join(format!("{stage}-release"))).await;
                                    }
                                    response
                                } else {b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"checkout\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()};
                                stream.write_all(&response).await.unwrap();let _=stream.shutdown().await;
                            }).await.unwrap();
                        });
                    },
                    child=children.join_next(),if !children.is_empty()=>{child.unwrap().unwrap();}
                }
            }
            while let Some(child)=children.join_next().await {child.unwrap();}
        });
        let log=directory.path().join("child.log");let output=std::fs::File::create(&log).unwrap();
        let mut command=Command::new(std::env::current_exe().unwrap());
        command.args(["--exact","checkout::native::checkout_original_tls_wire_direct_cached_exact_head_and_private_account_replacement","--nocapture","--test-threads=1"])
            .env(CHILD,"1").env("INTENT_CHECKOUT_INSTANCE",instance).env("INTENT_CHECKOUT_SHA",sha)
            .env("INTENT_CHECKOUT_ROOT",directory.path())
            .env("SSL_CERT_FILE",directory.path().join("ca.pem")).env("SSL_CERT_DIR",directory.path())
            .env("GIT_CONFIG_NOSYSTEM","1").env("GIT_CONFIG_GLOBAL","/dev/null")
            .env("INTENT_CHECKOUT_HELPER",sentinels.join("credential-sentinel"))
            .env("INTENT_CHECKOUT_HELPER_LOG",&helper_log)
            .env("PATH",std::env::join_paths(std::iter::once(sentinels).chain(std::env::split_paths(&std::env::var_os("PATH").unwrap()))).unwrap())
            .stdin(Stdio::null()).stdout(output.try_clone().unwrap()).stderr(output);
        let mut child=GuardedChild::spawn(&mut command).unwrap();
        let outcome=child.wait_with_timeout(LIMIT).unwrap();
        stop.send(()).unwrap();tokio::time::timeout(LIMIT,server).await.unwrap().unwrap();
        let text=std::fs::read_to_string(&log).unwrap();
        eprintln!("owned checkout authenticated request count={}",requests.load(Ordering::SeqCst));
        for receipt in text.lines().filter(|line|line.starts_with("checkout moved-branch receipt ")) { eprintln!("{receipt}"); }
        assert!(outcome.is_some_and(|s|s.success()),"{text}");
        assert!(!helper_log.exists(),"native checkout/fetch/push invoked a user helper, gh, or glab");
        assert!(requests.load(Ordering::SeqCst)>=4,"warm and direct used actual authenticated smart HTTPS");
    }).await;
}

async fn moved_branch_create_controls(
    h: &Harness,
    client: &mut Client,
    selection: &Value,
    selected: &str,
) {
    let root = PathBuf::from(std::env::var_os("INTENT_CHECKOUT_ROOT").unwrap());
    let bare = root.join("install/Team/Sub/Project.git");
    let reference = "refs/heads/release/later-page";
    assert_eq!(git(&bare, &["rev-parse", reference]), selected);
    let moved = git(&bare, &["rev-parse", "refs/heads/main"]);
    assert_ne!(moved, selected);
    git(&bare, &["update-ref", reference, &moved, selected]);

    let before = h.store.list_workspaces(true).await.unwrap().len();
    let mut direct = selection.clone();
    direct["mode"] = json!("direct");
    let refused = client
        .rpc(
            "workspace.create",
            json!({"repositoryCheckout":direct,"branch":"workspace/moved-direct"}),
        )
        .await;
    assert_eq!(
        refused,
        json!({"jsonrpc":"2.0","id":client.id,"error":{"code":-32003,"message":"Forbidden","data":{"code":"forbidden","detail":"Repository checkout unavailable"}}}),
        "assert the final protected wire envelope, not the upstream native error"
    );
    let after_refusal = h.store.list_workspaces(true).await.unwrap().len();
    assert_eq!(after_refusal, before);
    let published_before = h
        .store
        .list_workspaces(true)
        .await
        .unwrap()
        .into_iter()
        .filter(|workspace| workspace.branch == "workspace/moved-direct")
        .count();
    assert_eq!(published_before, 0);

    // This original cache is still authorized and holds the exact selected SHA.
    // A cache hit is neither a remote freshness probe nor a permission probe.
    let requests_before = std::fs::metadata(root.join("https-requests"))
        .unwrap()
        .len();
    let cached = client
        .rpc(
            "workspace.create",
            json!({"repositoryCheckout":selection,"branch":"workspace/moved-cached"}),
        )
        .await;
    let result = success(&cached);
    let workspace = result.get("workspace").unwrap_or(result);
    let path = Path::new(workspace["worktreePath"].as_str().unwrap());
    assert!(path.starts_with(h.dir.path()));
    let cached_head = git(path, &["rev-parse", "HEAD"]);
    assert_eq!(cached_head, selected);
    assert_eq!(workspace["baseCommitSha"], selected);
    assert_eq!(
        git(path, &["branch", "--show-current"]),
        "workspace/moved-cached"
    );
    let requests_after = std::fs::metadata(root.join("https-requests"))
        .unwrap()
        .len();
    assert_eq!(requests_after, requests_before);
    let after_cached = h.store.list_workspaces(true).await.unwrap().len();
    assert_eq!(after_cached, before + 1);
    assert_eq!(git(&bare, &["rev-parse", reference]), moved);
    eprintln!(
        "checkout moved-branch receipt {}",
        json!({"observedSha":selected,"remoteSha":moved,"directResponse":refused,"workspaceCountBefore":before,"workspaceCountAfterDirect":after_refusal,"cachedResponse":cached,"cachedHead":cached_head,"workspaceCountAfterCached":after_cached,"httpsRequestsBeforeCached":requests_before,"httpsRequestsAfterCached":requests_after})
    );
    // Restore only the disposable provider ref for the existing later controls.
    git(&bare, &["update-ref", reference, selected, &moved]);
}

async fn wait_for_file(path: &Path) {
    tokio::time::timeout(ACQUIRE, async {
        while !path.exists() {
            // timing-guard: poll the owned child phase file within the original acquire deadline.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("owned native phase reached within original acquire bound");
}

async fn recovered_capture(client: &mut Client, instance: &str) -> Value {
    ready(
        &client
            .rpc(
                "sourceControl.checkout.capture",
                json!({"provider":"gitlab","instanceBaseUrl":instance}),
            )
            .await,
    )
    .clone()
}

async fn observe_selection(client: &mut Client, capture: &Value, branch: &str, sha: &str) -> Value {
    let page = client
        .rpc("sourceControl.checkout.branches", project_query(capture))
        .await;
    assert!(ready(&page)["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["name"] == branch && item["commitSha"] == sha));
    let mut selection = project_query(capture);
    selection["branch"] = json!(branch);
    selection["commitSha"] = json!(sha);
    selection["mode"] = json!("cached");
    selection
}

async fn late_completion_controls(
    h: &Harness,
    client: &mut Client,
    instance: &str,
    selected: &str,
    workspace: &(Value, PathBuf),
) {
    let root = PathBuf::from(std::env::var_os("INTENT_CHECKOUT_ROOT").unwrap());
    let bare = root.join("install/Team/Sub/Project.git");
    let tree = git(&bare, &["rev-parse", &format!("{selected}^{{tree}}")]);
    let late_sha = git(
        &bare,
        &["commit-tree", &tree, "-p", selected, "-m", "late warm"],
    );
    git(
        &bare,
        &["update-ref", "refs/heads/late/cache-update", &late_sha],
    );
    let server = h.server.as_ref().unwrap();
    set(
        server,
        BRANCHES,
        json!([{"name":"late/cache-update","commit":{"id":late_sha},"protected":false}]),
        None,
    );
    let mut warm_client = h.wss(TOKEN).await;
    let original = recovered_capture(&mut warm_client, instance).await;
    let selection =
        observe_selection(&mut warm_client, &original, "late/cache-update", &late_sha).await;
    std::fs::write(root.join("hold-upload"), []).unwrap();
    let warm = tokio::spawn(async move {
        let result = warm_client
            .rpc("sourceControl.checkout.warm", selection)
            .await;
        warm_client.close().await;
        result
    });
    wait_for_file(&root.join("upload-entered")).await;
    success(
        &client
            .rpc(
                "sourceControl.revoke",
                json!({"provider":"gitlab","instanceBaseUrl":instance}),
            )
            .await,
    );
    set(
        server,
        "/api/v4/user",
        json!({"id":82,"username":"denied-fixture","name":"Denied fixture"}),
        None,
    );
    success(&client.rpc("sourceControl.connect", json!({"provider":"gitlab","instanceBaseUrl":instance,"method":"pat","token":"denied-pat"})).await);
    server.set("/api/v4/projects/Team%2FSub%2FProject", 403);
    let replacement = recovered_capture(client, instance).await;
    let mut q = project_query(&replacement);
    q["cached"] = json!(true);
    let denied = client.rpc("sourceControl.checkout.branches", q).await;
    assert_eq!(success(&denied)["status"], "unavailable");
    assert!(success(&denied).get("value").is_none());
    let before = h.store.list_workspaces(true).await.unwrap().len();
    let mut forbidden_selection = project_query(&replacement);
    forbidden_selection["branch"] = json!("late/cache-update");
    forbidden_selection["commitSha"] = json!(late_sha);
    forbidden_selection["mode"] = json!("cached");
    let denied = client
        .rpc(
            "workspace.create",
            json!({"repositoryCheckout":forbidden_selection}),
        )
        .await;
    assert!(denied.get("error").is_some());
    assert_eq!(h.store.list_workspaces(true).await.unwrap().len(), before);
    std::fs::write(root.join("upload-release"), []).unwrap();
    let result = tokio::time::timeout(ACQUIRE, warm).await.unwrap().unwrap();
    assert!(result.get("error").is_some() || result["result"]["status"] == "unavailable");
    assert!(!result.to_string().contains(&late_sha));
    // Inspect only the owned cache; the late pack cannot publish tracking refs.
    for entry in std::fs::read_dir(h.dir.path().join("workspaces/.repo-cache/qualified")).unwrap() {
        let path = entry.unwrap().path();
        if path.join(".git").exists() {
            assert_eq!(
                git(
                    &path,
                    &[
                        "for-each-ref",
                        "--format=%(refname)",
                        "refs/remotes/origin/late/cache-update"
                    ]
                ),
                ""
            );
        }
    }
    set(
        server,
        "/api/v4/user",
        json!({"id":42,"username":"fixture","name":"Fixture"}),
        None,
    );
    set(
        server,
        "/api/v4/projects/Team%2FSub%2FProject",
        json!({"id":42,"path_with_namespace":PROJECT,"web_url":format!("{instance}/{PROJECT}"),"default_branch":"main"}),
        None,
    );
    success(&client.rpc("sourceControl.connect", json!({"provider":"gitlab","instanceBaseUrl":instance,"method":"pat","token":"stored-pat"})).await);
    let original = recovered_capture(client, instance).await;
    let selection = observe_selection(client, &original, "late/cache-update", &late_sha).await;
    assert_eq!(
        ready(&client.rpc("sourceControl.checkout.warm", selection).await)["commitSha"],
        late_sha
    );
    let mut q = project_query(&original);
    q["cached"] = json!(true);
    let page = client.rpc("sourceControl.checkout.branches", q).await;
    assert!(ready(&page)["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["name"] == "late/cache-update" && item["commitSha"] == late_sha));

    // Hold the server acknowledgment after the one remote ref update exists.
    git(
        &workspace.1,
        &[
            "commit",
            "--allow-empty",
            "-m",
            "confirmed before retirement",
        ],
    );
    let pushed = git(&workspace.1, &["rev-parse", "HEAD"]);
    let branch = git(&workspace.1, &["branch", "--show-current"]);
    let tracking = format!("refs/remotes/origin/{branch}");
    let previous_tracking = git(&workspace.1, &["rev-parse", &tracking]);
    std::fs::write(root.join("hold-receive"), []).unwrap();
    let mut push_client = h.wss(TOKEN).await;
    let id = workspace.0.clone();
    let push = tokio::spawn(async move {
        let result = push_client
            .rpc("git.push", json!({"workspaceId":id,"force":false}))
            .await;
        push_client.close().await;
        result
    });
    wait_for_file(&root.join("receive-entered")).await;
    assert_eq!(
        git(&bare, &["rev-parse", &format!("refs/heads/{branch}")]),
        pushed
    );
    success(
        &client
            .rpc(
                "sourceControl.revoke",
                json!({"provider":"gitlab","instanceBaseUrl":instance}),
            )
            .await,
    );
    std::fs::write(root.join("receive-release"), []).unwrap();
    let result = tokio::time::timeout(ACQUIRE, push).await.unwrap().unwrap();
    assert!(
        result.get("error").is_some(),
        "retired push private reply: {result}"
    );
    assert!(!result.to_string().contains(&pushed));
    assert_eq!(
        git(&workspace.1, &["rev-parse", &tracking]),
        previous_tracking,
        "retirement before acknowledgment refuses local publication, not the confirmed remote outcome"
    );
    assert_eq!(
        git(&bare, &["rev-parse", &format!("refs/heads/{branch}")]),
        pushed
    );
    let events = client
        .rpc(
            "event.query",
            json!({"workspaceId":workspace.0,"eventType":"git:push"}),
        )
        .await;
    assert_eq!(
        success(&events)
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["data"]["commit"] == pushed)
            .count(),
        1,
        "one confirmed effect survives private reply retirement: {events}"
    );
    assert!(client
        .rpc("git.fetch", json!({"workspaceId":workspace.0}))
        .await
        .get("error")
        .is_some());
}
