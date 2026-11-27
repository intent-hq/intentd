//! Intent apps service tests (PROTOCOL §5.60): the bundle hash, manifest
//! rules, approval bound to the hash, the caller checks (user-only, app
//! principal required, feature grants) and per-app storage.
//!
//! Security cases first: each maps to a failure mode in the task note
//! (A6, A7, A8, A12, H1–H7, H10).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_core::apps::AppPrincipal;
use intent_core::{with_caller, AgentId, Caller, Error, HostRole, PrincipalId, WorkspaceId};
use intent_store::Store;
use serde_json::{json, Map, Value};

use super::bundle::{self, bundle_hash};
use crate::Services;

struct Fx {
    dir: tempfile::TempDir,
    svc: Services,
    ws: WorkspaceId,
    root: PathBuf,
}

async fn fx() -> Fx {
    let dir = crate::test_support::test_tempdir("app-ops-");
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let ws = WorkspaceId("ws-apps".to_string());
    let root = dir.path().join("checkout");
    std::fs::create_dir_all(&root).unwrap();
    let mut row = crate::tests::workspace(&ws);
    row.worktree_path = Some(root.to_string_lossy().into_owned());
    store.insert_workspace(&row).await.unwrap();
    let svc = Services::new_isolated(store).with_apps_dir(&dir.path().join("data"));
    Fx { dir, svc, ws, root }
}

fn manifest(id: &str, level: u8, features: &[&str]) -> Value {
    json!({
        "manifestVersion": 1,
        "id": id,
        "name": format!("App {id}"),
        "version": "1.0.0",
        "permissions": { "level": level, "features": features },
    })
}

fn write_bundle(root: &Path, rel: &str, manifest: &Value, files: &[(&str, &str)]) {
    let dir = root.join(rel);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("intent-app.json"), manifest.to_string()).unwrap();
    std::fs::write(dir.join("index.html"), "<h1>app</h1>").unwrap();
    for (path, content) in files {
        let p = dir.join(path);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, content).unwrap();
    }
}

fn params(v: &Value) -> Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

fn owner() -> Caller {
    Caller::Wire {
        principal_id: PrincipalId::new(),
        host_role: HostRole::Owner,
    }
}

fn agent() -> Caller {
    Caller::Agent {
        agent_id: AgentId("agent-1".into()),
    }
}

impl Fx {
    async fn call_as(&self, caller: Caller, method: &str, p: Value) -> intent_core::Result<Value> {
        with_caller(caller, self.svc.app_call_op(method, &params(&p))).await
    }

    async fn call(&self, method: &str, p: Value) -> Value {
        self.call_as(owner(), method, p)
            .await
            .unwrap_or_else(|e| panic!("{method}: {e:?}"))
    }

    async fn install(&self, rel: &str, replace: bool) -> Value {
        self.call(
            "app.install",
            json!({ "workspaceId": self.ws.0, "path": rel, "replace": replace }),
        )
        .await
    }

    async fn approve(&self, id: &str, hash: &str, level: u8, features: &[&str]) -> Value {
        self.call(
            "app.approve",
            json!({ "appId": id, "bundleHash": hash, "level": level, "features": features }),
        )
        .await
    }

    /// Open a session and attach as the host would; returns the app
    /// principal caller and the end signal.
    async fn open_app(&self, id: &str) -> (Caller, tokio::sync::watch::Receiver<bool>) {
        let session = self.call("app.session.open", json!({ "appId": id })).await;
        let token = session["token"].as_str().unwrap().to_owned();
        let attached = with_caller(owner(), async { self.svc.app_session_attach_op(&token) })
            .await
            .unwrap();
        assert!(
            attached.result.get("token").is_none(),
            "attach never echoes the token"
        );
        (Caller::App(attached.principal), attached.ended)
    }
}

fn code(e: &Error) -> String {
    match e {
        Error::App(app) => app.data_code().to_owned(),
        Error::Forbidden(detail) => format!("forbidden: {detail}"),
        other => format!("other: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Bundle hash and manifest
// ---------------------------------------------------------------------------

/// Shared test vector: the SDK and the mock bridge must produce the same
/// string for the same files (computed independently with Python hashlib).
#[test]
fn bundle_hash_matches_the_shared_vector() {
    let files: Vec<(&str, &[u8])> = vec![
        ("index.html", b"<p>hi</p>"),
        ("intent-app.json", br#"{"a":1}"#),
        ("b/c.js", b""),
    ];
    assert_eq!(
        bundle_hash(files),
        "sha256:0195074d8fa677e5650996ab422800f649b0d4536f415c11437d3d3c00aad8d0"
    );
}

/// H4: the framing makes moving bytes between a path and its content, or
/// between two files, change the hash.
#[test]
fn bundle_hash_framing_is_unambiguous() {
    let a = bundle_hash(vec![("ab", b"c".as_slice())]);
    let b = bundle_hash(vec![("a", b"bc".as_slice())]);
    let c = bundle_hash(vec![("a", b"".as_slice()), ("b", b"c".as_slice())]);
    assert_ne!(a, b);
    assert_ne!(b, c);
    // Order of the input does not matter; sorting is by bytes.
    assert_eq!(
        bundle_hash(vec![("b", b"1".as_slice()), ("a", b"2".as_slice())]),
        bundle_hash(vec![("a", b"2".as_slice()), ("b", b"1".as_slice())])
    );
}

#[test]
fn bundle_paths_follow_the_rules() {
    for ok in ["index.html", "a/b.js", "a..b/c"] {
        assert!(bundle::is_bundle_path(ok), "{ok}");
    }
    for bad in ["", "/a", "a/../b", "..", "a\\b", "a//b", "./a", "a\0b"] {
        assert!(!bundle::is_bundle_path(bad), "{bad:?}");
    }
    assert!(!bundle::is_bundle_path(&"a".repeat(513)));
}

#[intent_test_macros::daemon_test]
async fn manifest_rules_are_enforced_with_a_field_pointer() {
    let f = fx().await;
    let cases: Vec<(Value, &str)> = vec![
        (json!({"manifestVersion": 2}), "/manifestVersion"),
        (
            {
                let mut m = manifest("ok-app", 1, &[]);
                m["extra"] = json!(1);
                m
            },
            "/extra",
        ),
        (manifest("Bad_Id", 1, &[]), "/id"),
        (
            manifest("ok-app", 1, &["models"]),
            "/permissions/features/0",
        ),
        (
            manifest("ok-app", 2, &["camera"]),
            "/permissions/features/0",
        ),
        (
            manifest("ok-app", 2, &["models", "models"]),
            "/permissions/features/1",
        ),
        (
            {
                let mut m = manifest("ok-app", 2, &[]);
                m["window"] = json!({ "alwaysOnTop": true });
                m
            },
            "/window/alwaysOnTop",
        ),
        (
            {
                let mut m = manifest("ok-app", 1, &[]);
                m["version"] = json!("1.0");
                m
            },
            "/version",
        ),
        (
            {
                let mut m = manifest("ok-app", 1, &[]);
                m["icon"] = json!("missing.png");
                m
            },
            "/icon",
        ),
    ];
    for (i, (m, field)) in cases.into_iter().enumerate() {
        let rel = format!("bad{i}");
        write_bundle(&f.root, &rel, &m, &[]);
        let e = f
            .call_as(
                owner(),
                "app.install",
                json!({ "workspaceId": f.ws.0, "path": rel }),
            )
            .await
            .unwrap_err();
        let Error::App(app) = &e else {
            panic!("case {i}: {e:?}")
        };
        assert_eq!(app.data_code(), "invalid-manifest", "case {i}");
        assert_eq!(app.data["field"], json!(field), "case {i}: {}", app.data);
    }
}

#[intent_test_macros::daemon_test]
async fn level_three_is_unavailable_in_m1() {
    let f = fx().await;
    write_bundle(&f.root, "full", &manifest("full-app", 3, &[]), &[]);
    let e = f
        .call_as(
            owner(),
            "app.install",
            json!({ "workspaceId": f.ws.0, "path": "full" }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "level-unavailable");
}

/// H2: a symlink anywhere in the bundle, and a path that escapes the
/// workspace root, are refused.
#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn symlinks_and_escapes_are_refused() {
    let f = fx().await;
    write_bundle(&f.root, "linked", &manifest("linked-app", 1, &[]), &[]);
    std::os::unix::fs::symlink("/etc/hostname", f.root.join("linked/leak.txt")).unwrap();
    let e = f
        .call_as(
            owner(),
            "app.install",
            json!({ "workspaceId": f.ws.0, "path": "linked" }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "invalid-bundle");

    let outside = f.dir.path().join("outside");
    write_bundle(&outside, "", &manifest("outside-app", 1, &[]), &[]);
    std::os::unix::fs::symlink(&outside, f.root.join("escape")).unwrap();
    for path in ["escape", "../outside", "/etc"] {
        let e = f
            .call_as(
                owner(),
                "app.install",
                json!({ "workspaceId": f.ws.0, "path": path }),
            )
            .await
            .unwrap_err();
        assert_eq!(code(&e), "path-not-found", "{path}");
    }
}

// ---------------------------------------------------------------------------
// Approval bound to the hash
// ---------------------------------------------------------------------------

/// H5–H7: approval names the installed hash; an edited bundle gets a new
/// hash, resets approval, ends the open session, and cannot open again
/// until approved again. A same-hash replace keeps the approval.
#[intent_test_macros::daemon_test]
async fn an_edited_bundle_needs_approval_again() {
    let f = fx().await;
    write_bundle(
        &f.root,
        "apps/timer",
        &manifest("timer", 2, &["models", "notifications"]),
        &[("app.js", "v1")],
    );
    let installed = f.install("apps/timer", false).await;
    let hash1 = installed["app"]["bundleHash"].as_str().unwrap().to_owned();
    assert_eq!(installed["app"]["approval"]["status"], "pending");
    assert_eq!(installed["app"]["fileCount"], 3);

    // Not approved: no session.
    let e = f
        .call_as(owner(), "app.session.open", json!({ "appId": "timer" }))
        .await
        .unwrap_err();
    assert_eq!(code(&e), "not-approved");

    // The user approves what they saw: another hash is refused.
    let e = f
        .call_as(
            owner(),
            "app.approve",
            json!({ "appId": "timer", "bundleHash": "sha256:00", "level": 2, "features": [] }),
        )
        .await
        .unwrap_err();
    let Error::App(app) = &e else { panic!("{e:?}") };
    assert_eq!(app.data_code(), "hash-mismatch");
    assert_eq!(app.data["bundleHash"], json!(hash1));

    // Wrong level and a feature not requested are invalid.
    for bad in [
        json!({ "appId": "timer", "bundleHash": hash1, "level": 1, "features": [] }),
        json!({ "appId": "timer", "bundleHash": hash1, "level": 2, "features": ["globalShortcuts"] }),
    ] {
        let e = f.call_as(owner(), "app.approve", bad).await.unwrap_err();
        assert_eq!(code(&e), "invalid-params");
    }

    let approved = f.approve("timer", &hash1, 2, &["models"]).await;
    assert_eq!(approved["app"]["approval"]["status"], "approved");
    assert_eq!(approved["app"]["approval"]["features"], json!(["models"]));
    let (_app, mut ended) = f.open_app("timer").await;
    assert!(!*ended.borrow_and_update());
    assert_eq!(
        f.call("app.get", json!({ "appId": "timer" })).await["app"]["open"],
        true
    );

    // Same bytes again: approval is kept and the session ends (a replace
    // always ends sessions).
    let same = f.install("apps/timer", true).await;
    assert_eq!(same["approvalReset"], false);
    assert_eq!(same["app"]["approval"]["status"], "approved");
    assert!(ended.has_changed().unwrap_or(true));
    let (_app, mut ended) = f.open_app("timer").await;
    assert!(!*ended.borrow_and_update());

    // Edit one file: new hash, approval reset, session ended.
    std::fs::write(f.root.join("apps/timer/app.js"), "v2").unwrap();
    let edited = f.install("apps/timer", true).await;
    let hash2 = edited["app"]["bundleHash"].as_str().unwrap().to_owned();
    assert_ne!(hash1, hash2);
    assert_eq!(edited["approvalReset"], true);
    assert_eq!(edited["replaced"], true);
    assert_eq!(edited["app"]["approval"]["status"], "pending");
    assert_eq!(edited["app"]["approval"]["bundleHash"], Value::Null);
    assert_eq!(edited["app"]["open"], false);
    assert!(ended.has_changed().unwrap_or(true), "the session ended");
    let e = f
        .call_as(owner(), "app.session.open", json!({ "appId": "timer" }))
        .await
        .unwrap_err();
    assert_eq!(code(&e), "not-approved");
    // The old approval cannot be replayed against the new bundle.
    let e = f
        .call_as(
            owner(),
            "app.approve",
            json!({ "appId": "timer", "bundleHash": hash1, "level": 2, "features": ["models"] }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "hash-mismatch");
    f.approve("timer", &hash2, 2, &["models"]).await;
    f.open_app("timer").await;

    // readBundle serves the installed (new) bytes and refuses a stale hash.
    let file = f
        .call(
            "app.readBundle",
            json!({ "appId": "timer", "path": "app.js", "bundleHash": hash2 }),
        )
        .await;
    assert_eq!(file["contentBase64"], "djI=");
    assert_eq!(file["mimeType"], "text/javascript");
    let e = f
        .call_as(
            owner(),
            "app.readBundle",
            json!({ "appId": "timer", "path": "app.js", "bundleHash": hash1 }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "hash-mismatch");
}

/// H1: what is installed is what was hashed: editing the workspace copy
/// after install changes nothing that the library serves.
#[intent_test_macros::daemon_test]
async fn the_library_holds_a_copy_of_the_hashed_bytes() {
    let f = fx().await;
    write_bundle(&f.root, "copy", &manifest("copy-app", 1, &[]), &[]);
    let installed = f.install("copy", false).await;
    std::fs::write(f.root.join("copy/index.html"), "<h1>changed</h1>").unwrap();
    let file = f
        .call(
            "app.readBundle",
            json!({ "appId": "copy-app", "path": "index.html" }),
        )
        .await;
    assert_eq!(file["contentBase64"], "PGgxPmFwcDwvaDE+");
    let manifest_bytes = manifest("copy-app", 1, &[]).to_string();
    let entries: Vec<(&str, &[u8])> = vec![
        ("index.html", b"<h1>app</h1>"),
        ("intent-app.json", manifest_bytes.as_bytes()),
    ];
    assert_eq!(installed["app"]["bundleHash"], json!(bundle_hash(entries)));
}

#[intent_test_macros::daemon_test]
async fn read_bundle_refuses_paths_outside_the_bundle() {
    let f = fx().await;
    write_bundle(
        &f.root,
        "rb",
        &manifest("rb-app", 1, &[]),
        &[("sub/x.css", "x")],
    );
    f.install("rb", false).await;
    for path in [
        "",
        "/",
        "sub",
        "../rb-app",
        "sub/../index.html",
        "/etc/passwd",
        "nope.js",
    ] {
        let e = f
            .call_as(
                owner(),
                "app.readBundle",
                json!({ "appId": "rb-app", "path": path }),
            )
            .await
            .unwrap_err();
        assert_eq!(code(&e), "file-not-found", "{path:?}");
    }
    let ok = f
        .call(
            "app.readBundle",
            json!({ "appId": "rb-app", "path": "sub/x.css" }),
        )
        .await;
    assert_eq!(ok["mimeType"], "text/css");
}

/// H10: a dry run validates and stores, ends and publishes nothing.
#[intent_test_macros::daemon_test]
async fn dry_run_stores_nothing() {
    let f = fx().await;
    write_bundle(&f.root, "dry", &manifest("dry-app", 1, &[]), &[]);
    let r = f
        .call(
            "app.install",
            json!({ "workspaceId": f.ws.0, "path": "dry", "dryRun": true }),
        )
        .await;
    assert_eq!(r["dryRun"], true);
    assert_eq!(r["app"]["approval"]["status"], "pending");
    assert_eq!(f.call("app.list", json!({})).await["apps"], json!([]));
    f.install("dry", false).await;
    let e = f
        .call_as(
            owner(),
            "app.install",
            json!({ "workspaceId": f.ws.0, "path": "dry", "dryRun": true }),
        )
        .await
        .unwrap_err();
    assert_eq!(
        code(&e),
        "app-exists",
        "a dry run still reports the conflict"
    );
}

// ---------------------------------------------------------------------------
// Caller checks
// ---------------------------------------------------------------------------

/// A6: an agent can install, list and get, but never approve or open a
/// session.
#[intent_test_macros::daemon_test]
async fn agents_are_refused_the_user_only_methods() {
    let f = fx().await;
    write_bundle(&f.root, "ag", &manifest("ag-app", 1, &[]), &[]);
    let r = f
        .call_as(
            agent(),
            "app.install",
            json!({ "workspaceId": f.ws.0, "path": "ag" }),
        )
        .await
        .unwrap();
    let hash = r["app"]["bundleHash"].as_str().unwrap().to_owned();
    f.call_as(agent(), "app.list", json!({})).await.unwrap();
    f.call_as(agent(), "app.get", json!({ "appId": "ag-app" }))
        .await
        .unwrap();
    let e = f
        .call_as(
            agent(),
            "app.approve",
            json!({ "appId": "ag-app", "bundleHash": hash, "level": 1, "features": [] }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "forbidden: user-only");
    f.approve("ag-app", &hash, 1, &[]).await;
    let e = f
        .call_as(agent(), "app.session.open", json!({ "appId": "ag-app" }))
        .await
        .unwrap_err();
    assert_eq!(code(&e), "forbidden: user-only");
}

/// Guests and host members are refused every library method at the service
/// layer too (the transport refuses them first).
#[intent_test_macros::daemon_test]
async fn non_administrators_are_refused_the_library() {
    let f = fx().await;
    for role in [HostRole::Guest, HostRole::Member] {
        let caller = Caller::Wire {
            principal_id: PrincipalId::new(),
            host_role: role,
        };
        for method in ["app.list", "app.install", "app.session.open", "app.approve"] {
            let e = f
                .call_as(caller.clone(), method, json!({}))
                .await
                .unwrap_err();
            assert!(matches!(e, Error::Forbidden(_)), "{role:?} {method}: {e:?}");
        }
    }
}

/// A7: the app-principal methods refuse every other caller, and an app
/// principal is refused every library method.
#[intent_test_macros::daemon_test]
async fn app_methods_need_the_app_principal_and_apps_get_no_library() {
    let f = fx().await;
    write_bundle(
        &f.root,
        "p",
        &manifest("p-app", 2, &["models", "notifications"]),
        &[],
    );
    let hash = f.install("p", false).await["app"]["bundleHash"]
        .as_str()
        .unwrap()
        .to_owned();
    f.approve("p-app", &hash, 2, &["models", "notifications"])
        .await;
    let (app, _ended) = f.open_app("p-app").await;
    for method in [
        "app.storage.get",
        "app.storage.set",
        "app.storage.delete",
        "app.storage.list",
        "app.notify",
        "app.models.list",
        "app.complete",
    ] {
        for caller in [owner(), agent(), Caller::Daemon] {
            let e = f
                .call_as(
                    caller,
                    method,
                    json!({ "key": "k", "value": 1, "title": "t", "prompt": "p" }),
                )
                .await
                .unwrap_err();
            assert_eq!(code(&e), "forbidden: app principal required", "{method}");
        }
    }
    for method in [
        "app.install",
        "app.uninstall",
        "app.list",
        "app.get",
        "app.approve",
        "app.revoke",
        "app.readBundle",
        "app.session.open",
        "app.session.close",
    ] {
        let e = f
            .call_as(app.clone(), method, json!({ "appId": "p-app" }))
            .await
            .unwrap_err();
        assert!(matches!(e, Error::Forbidden(_)), "{method}: {e:?}");
    }
    // principal.me answers the app shape.
    let me = with_caller(app, f.svc.principal_me_op()).await.unwrap();
    assert_eq!(me["kind"], "app");
    assert_eq!(me["id"], "app:p-app");
    assert_eq!(me["isAdministrator"], false);
}

/// A12: features the user declined are refused at the service level.
#[intent_test_macros::daemon_test]
async fn declined_features_are_refused() {
    let f = fx().await;
    write_bundle(
        &f.root,
        "feat",
        &manifest("feat-app", 2, &["models", "notifications"]),
        &[],
    );
    let hash = f.install("feat", false).await["app"]["bundleHash"]
        .as_str()
        .unwrap()
        .to_owned();
    f.approve("feat-app", &hash, 2, &[]).await;
    let (app, _ended) = f.open_app("feat-app").await;
    let e = f
        .call_as(app.clone(), "app.notify", json!({ "title": "hi" }))
        .await
        .unwrap_err();
    assert_eq!(code(&e), "forbidden: feature notifications not approved");
    for method in ["app.models.list", "app.complete"] {
        let e = f
            .call_as(app.clone(), method, json!({ "prompt": "hi" }))
            .await
            .unwrap_err();
        assert_eq!(
            code(&e),
            "forbidden: feature models not approved",
            "{method}"
        );
    }
}

/// A8/A9: every end reason closes the session, the end signal fires, the
/// token stops attaching, and calls on the old principal are refused.
#[intent_test_macros::daemon_test]
async fn sessions_end_on_every_documented_cause() {
    let f = fx().await;
    write_bundle(&f.root, "s", &manifest("s-app", 1, &[]), &[]);
    let hash = f.install("s", false).await["app"]["bundleHash"]
        .as_str()
        .unwrap()
        .to_owned();
    f.approve("s-app", &hash, 1, &[]).await;

    // A second open ends the first.
    let (first, mut first_end) = f.open_app("s-app").await;
    let (_second, _e2) = f.open_app("s-app").await;
    first_end.changed().await.ok();
    assert!(*first_end.borrow());
    let e = f
        .call_as(first, "app.storage.get", json!({ "key": "k" }))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Forbidden(_)), "{e:?}");

    // close, revoke, uninstall.
    let session = f
        .call("app.session.open", json!({ "appId": "s-app" }))
        .await;
    let closed = f
        .call(
            "app.session.close",
            json!({ "sessionId": session["sessionId"] }),
        )
        .await;
    assert_eq!(closed["closed"], true);
    let token = session["token"].as_str().unwrap().to_owned();
    let e = with_caller(owner(), async { f.svc.app_session_attach_op(&token) })
        .await
        .err()
        .unwrap();
    assert_eq!(code(&e), "session-invalid");
    let again = f
        .call(
            "app.session.close",
            json!({ "sessionId": session["sessionId"] }),
        )
        .await;
    assert_eq!(again["closed"], false);

    let (_a, mut end) = f.open_app("s-app").await;
    let revoked = f.call("app.revoke", json!({ "appId": "s-app" })).await;
    assert_eq!(revoked["revoked"], true);
    assert_eq!(revoked["app"]["approval"]["status"], "revoked");
    assert_eq!(revoked["app"]["approval"]["bundleHash"], json!(hash));
    end.changed().await.ok();
    assert!(*end.borrow());
    let again = f.call("app.revoke", json!({ "appId": "s-app" })).await;
    assert_eq!(again["revoked"], false);

    f.approve("s-app", &hash, 1, &[]).await;
    let (_a, mut end) = f.open_app("s-app").await;
    let open = f.call("app.get", json!({ "appId": "s-app" })).await;
    assert_eq!(open["app"]["open"], true);
    f.call("app.uninstall", json!({ "appId": "s-app" })).await;
    end.changed().await.ok();
    assert!(*end.borrow());
    let e = f
        .call_as(owner(), "app.get", json!({ "appId": "s-app" }))
        .await
        .unwrap_err();
    assert_eq!(code(&e), "app-not-found");
    assert_eq!(
        f.call("app.uninstall", json!({ "appId": "s-app" })).await["removed"],
        false
    );
}

#[intent_test_macros::daemon_test]
async fn opener_end_closes_the_sessions_it_opened() {
    let f = fx().await;
    write_bundle(&f.root, "o", &manifest("o-app", 1, &[]), &[]);
    let hash = f.install("o", false).await["app"]["bundleHash"]
        .as_str()
        .unwrap()
        .to_owned();
    f.approve("o-app", &hash, 1, &[]).await;
    let session = f
        .call("app.session.open", json!({ "appId": "o-app" }))
        .await;
    let token = session["token"].as_str().unwrap().to_owned();
    let mut end = with_caller(owner(), async { f.svc.app_session_attach_op(&token) })
        .await
        .unwrap()
        .ended;
    f.svc
        .app_sessions_end_op(&[session["sessionId"].as_str().unwrap().to_owned()]);
    end.changed().await.ok();
    assert!(*end.borrow());
}

#[intent_test_macros::daemon_test]
async fn attach_rejects_malformed_tokens_without_lookup() {
    let f = fx().await;
    for token in ["", "zz", &"g".repeat(64), &"a".repeat(63), &"a".repeat(64)] {
        let e = f.svc.app_session_attach_op(token).err().unwrap();
        assert_eq!(code(&e), "session-invalid", "{token:?}");
    }
}

// ---------------------------------------------------------------------------
// Storage, notify
// ---------------------------------------------------------------------------

async fn level1_app(f: &Fx, id: &str) -> Caller {
    write_bundle(&f.root, id, &manifest(id, 1, &[]), &[]);
    let hash = f.install(id, false).await["app"]["bundleHash"]
        .as_str()
        .unwrap()
        .to_owned();
    f.approve(id, &hash, 1, &[]).await;
    f.open_app(id).await.0
}

#[intent_test_macros::daemon_test]
async fn storage_is_per_app_and_follows_the_limits() {
    let f = fx().await;
    let a = level1_app(&f, "store-a").await;
    let b = level1_app(&f, "store-b").await;
    let set = f
        .call_as(
            a.clone(),
            "app.storage.set",
            json!({ "key": "k", "value": { "n": 1 } }),
        )
        .await
        .unwrap();
    assert_eq!(set["usedBytes"], 8);
    let got = f
        .call_as(a.clone(), "app.storage.get", json!({ "key": "k" }))
        .await
        .unwrap();
    assert_eq!(got, json!({ "found": true, "value": { "n": 1 } }));
    // Another app never sees it.
    let other = f
        .call_as(b.clone(), "app.storage.get", json!({ "key": "k" }))
        .await
        .unwrap();
    assert_eq!(other, json!({ "found": false, "value": null }));
    // null is a value.
    f.call_as(
        a.clone(),
        "app.storage.set",
        json!({ "key": "n", "value": null }),
    )
    .await
    .unwrap();
    assert_eq!(
        f.call_as(a.clone(), "app.storage.get", json!({ "key": "n" }))
            .await
            .unwrap()["found"],
        true
    );
    for bad in [
        json!(""),
        json!("a\u{1}"),
        json!("a\u{85}"),
        json!("x".repeat(257)),
        json!(1),
    ] {
        let e = f
            .call_as(
                a.clone(),
                "app.storage.set",
                json!({ "key": bad, "value": 1 }),
            )
            .await
            .unwrap_err();
        assert_eq!(code(&e), "invalid-key");
    }
    let big = "x".repeat(256 * 1024);
    let e = f
        .call_as(
            a.clone(),
            "app.storage.set",
            json!({ "key": "big", "value": big }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "value-too-large");

    for i in 0..5 {
        f.call_as(
            a.clone(),
            "app.storage.set",
            json!({ "key": format!("p/{i}"), "value": i }),
        )
        .await
        .unwrap();
    }
    let page = f
        .call_as(
            a.clone(),
            "app.storage.list",
            json!({ "prefix": "p/", "limit": 2 }),
        )
        .await
        .unwrap();
    assert_eq!(page["keys"], json!(["p/0", "p/1"]));
    let next = f
        .call_as(
            a.clone(),
            "app.storage.list",
            json!({ "prefix": "p/", "limit": 2, "cursor": page["nextCursor"] }),
        )
        .await
        .unwrap();
    assert_eq!(next["keys"], json!(["p/2", "p/3"]));
    let last = f
        .call_as(
            a.clone(),
            "app.storage.list",
            json!({ "prefix": "p/", "limit": 2, "cursor": next["nextCursor"] }),
        )
        .await
        .unwrap();
    assert_eq!(last, json!({ "keys": ["p/4"], "nextCursor": null }));
    for bad in [
        json!({ "cursor": "zz" }),
        json!({ "limit": 0 }),
        json!({ "limit": 1001 }),
    ] {
        let e = f
            .call_as(a.clone(), "app.storage.list", bad)
            .await
            .unwrap_err();
        assert_eq!(code(&e), "invalid-params");
    }
    let deleted = f
        .call_as(a.clone(), "app.storage.delete", json!({ "key": "k" }))
        .await
        .unwrap();
    assert_eq!(deleted["deleted"], true);

    // Uninstall deletes the app's storage; a reinstall starts empty.
    f.call_as(
        b.clone(),
        "app.storage.set",
        json!({ "key": "keep", "value": 1 }),
    )
    .await
    .unwrap();
    f.call("app.uninstall", json!({ "appId": "store-b" })).await;
    let b2 = level1_app(&f, "store-b").await;
    let gone = f
        .call_as(b2, "app.storage.get", json!({ "key": "keep" }))
        .await
        .unwrap();
    assert_eq!(gone["found"], false);
}

#[intent_test_macros::daemon_test]
async fn storage_quota_refuses_and_reports_usage() {
    let f = fx().await;
    let a = level1_app(&f, "quota-app").await;
    let chunk = "y".repeat(256 * 1024 - 10);
    let mut used = 0;
    let mut i = 0;
    loop {
        match f
            .call_as(
                a.clone(),
                "app.storage.set",
                json!({ "key": format!("k{i}"), "value": chunk }),
            )
            .await
        {
            Ok(r) => used = r["usedBytes"].as_u64().unwrap(),
            Err(Error::App(app)) => {
                assert_eq!(app.data_code(), "quota-exceeded");
                assert_eq!(app.data["limitBytes"], json!(super::STORAGE_QUOTA_BYTES));
                assert_eq!(app.data["usedBytes"], json!(used));
                break;
            }
            Err(other) => panic!("{other:?}"),
        }
        i += 1;
        assert!(i < 100, "the quota never applied");
    }
    assert!(used <= super::STORAGE_QUOTA_BYTES);
}

#[intent_test_macros::daemon_test]
async fn notify_validates_and_rate_limits() {
    let f = fx().await;
    write_bundle(&f.root, "n", &manifest("n-app", 2, &["notifications"]), &[]);
    let hash = f.install("n", false).await["app"]["bundleHash"]
        .as_str()
        .unwrap()
        .to_owned();
    f.approve("n-app", &hash, 2, &["notifications"]).await;
    let (app, _end) = f.open_app("n-app").await;
    for bad in [
        json!({ "title": "  " }),
        json!({ "title": "x".repeat(129) }),
        json!({ "title": "t", "body": "b".repeat(1025) }),
        json!({ "title": "t", "tag": "" }),
    ] {
        let e = f.call_as(app.clone(), "app.notify", bad).await.unwrap_err();
        assert_eq!(code(&e), "invalid-params");
    }
    for _ in 0..10 {
        let r = f
            .call_as(app.clone(), "app.notify", json!({ "title": "t" }))
            .await
            .unwrap();
        assert!(r["notificationId"].as_str().is_some());
    }
    let e = f
        .call_as(app.clone(), "app.notify", json!({ "title": "t" }))
        .await
        .unwrap_err();
    let Error::App(limited) = &e else {
        panic!("{e:?}")
    };
    assert_eq!(limited.rpc_code, -32603);
    assert_eq!(limited.data_code(), "rate-limited");
    assert!(limited.data["retryAfterMs"].as_u64().unwrap() > 0);
}

#[intent_test_macros::daemon_test]
async fn complete_validates_provider_and_model() {
    let f = fx().await;
    write_bundle(&f.root, "m", &manifest("m-app", 2, &["models"]), &[]);
    let hash = f.install("m", false).await["app"]["bundleHash"]
        .as_str()
        .unwrap()
        .to_owned();
    f.approve("m-app", &hash, 2, &["models"]).await;
    let (app, _end) = f.open_app("m-app").await;
    let listed = f
        .call_as(app.clone(), "app.models.list", json!({}))
        .await
        .unwrap();
    assert!(listed["providers"].is_array());
    for bad in [
        json!({ "prompt": "" }),
        json!({ "prompt": "p", "timeoutMs": 10 }),
        json!({ "prompt": "p", "timeoutMs": 300_001 }),
        json!({ "prompt": "p", "system": "s".repeat(50_001) }),
    ] {
        let e = f
            .call_as(app.clone(), "app.complete", bad)
            .await
            .unwrap_err();
        assert_eq!(code(&e), "invalid-params");
    }
    let e = f
        .call_as(
            app.clone(),
            "app.complete",
            json!({ "prompt": "p", "provider": "no-such-provider" }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "provider-unknown");
    let e = f
        .call_as(
            app.clone(),
            "app.complete",
            json!({ "prompt": "p", "provider": "claude-code", "model": "no-such-model" }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "model-unknown");
}

#[test]
fn principal_has_feature() {
    let p = AppPrincipal {
        app_id: "a".into(),
        name: "A".into(),
        bundle_hash: "sha256:0".into(),
        level: 2,
        features: vec!["models".into()],
        session_id: "as_1".into(),
    };
    assert!(p.has_feature("models"));
    assert!(!p.has_feature("notifications"));
    let _ = Arc::new(p);
}

// ---------------------------------------------------------------------------
// Publisher and signature (reserved: stored, shape-checked, never verified)
// ---------------------------------------------------------------------------

fn sig(hash: &str) -> String {
    json!({
        "sigVersion": 1,
        "algorithm": "ed25519",
        "keyId": "ed25519:abc",
        "bundleHash": hash,
        "signature": "q83v_-A"
    })
    .to_string()
}

#[intent_test_macros::daemon_test]
async fn publisher_and_signature_are_stored_unverified_and_outside_the_hash() {
    let f = fx().await;
    let mut m = manifest("signed-app", 1, &[]);
    m["publisher"] =
        json!({ "name": " Example Labs ", "url": "https://example.com", "keyId": "ed25519:abc" });
    m["updateUrl"] = json!("https://example.com/apps/signed");
    write_bundle(&f.root, "signed", &m, &[("sub/intent-app.sig", "hashed")]);
    let plain = f.install("signed", false).await;
    let hash = plain["app"]["bundleHash"].as_str().unwrap().to_owned();
    assert_eq!(plain["app"]["signature"], Value::Null);
    assert_eq!(
        plain["app"]["publisher"],
        json!({ "name": "Example Labs", "url": "https://example.com", "keyId": "ed25519:abc", "verified": false })
    );
    assert_eq!(plain["app"]["updateUrl"], "https://example.com/apps/signed");
    assert_eq!(
        plain["app"]["fileCount"], 3,
        "a sub-directory sig is a normal file"
    );
    f.approve("signed-app", &hash, 1, &[]).await;

    // Adding a root signature (even one naming another hash) keeps the hash,
    // the counts and the approval; it is stored and shown unverified.
    std::fs::write(
        f.root.join("signed/intent-app.sig"),
        sig(&format!("sha256:{}", "0".repeat(64))),
    )
    .unwrap();
    let signed = f.install("signed", true).await;
    assert_eq!(signed["app"]["bundleHash"], json!(hash));
    assert_eq!(signed["approvalReset"], false);
    assert_eq!(signed["app"]["approval"]["status"], "approved");
    assert_eq!(signed["app"]["fileCount"], plain["app"]["fileCount"]);
    assert_eq!(signed["app"]["sizeBytes"], plain["app"]["sizeBytes"]);
    assert_eq!(signed["app"]["signature"]["status"], "unverified");
    assert_eq!(signed["app"]["signature"]["verified"], false);
    assert_eq!(signed["app"]["signature"]["keyId"], "ed25519:abc");
    let e = f
        .call_as(
            owner(),
            "app.readBundle",
            json!({ "appId": "signed-app", "path": "intent-app.sig" }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "file-not-found");
    f.call(
        "app.readBundle",
        json!({ "appId": "signed-app", "path": "sub/intent-app.sig" }),
    )
    .await;
}

#[intent_test_macros::daemon_test]
async fn malformed_signatures_and_publishers_are_refused() {
    let f = fx().await;
    let good_hash = format!("sha256:{}", "a".repeat(64));
    let bad_sigs = [
        "not json".to_owned(),
        json!({ "sigVersion": 2, "algorithm": "ed25519", "keyId": "k", "bundleHash": good_hash, "signature": "x" }).to_string(),
        json!({ "sigVersion": 1, "algorithm": "Ed25519", "keyId": "k", "bundleHash": good_hash, "signature": "x" }).to_string(),
        json!({ "sigVersion": 1, "algorithm": "ed25519", "keyId": "a b", "bundleHash": good_hash, "signature": "x" }).to_string(),
        json!({ "sigVersion": 1, "algorithm": "ed25519", "keyId": "k", "bundleHash": "sha256:00", "signature": "x" }).to_string(),
        json!({ "sigVersion": 1, "algorithm": "ed25519", "keyId": "k", "bundleHash": good_hash, "signature": "a=" }).to_string(),
        json!({ "sigVersion": 1, "algorithm": "ed25519", "keyId": "k", "bundleHash": good_hash }).to_string(),
        json!({ "sigVersion": 1, "algorithm": "ed25519", "keyId": "k", "bundleHash": good_hash, "signature": "x", "extra": 1 }).to_string(),
        "x".repeat(16 * 1024 + 1),
    ];
    for (i, bad) in bad_sigs.iter().enumerate() {
        let rel = format!("badsig{i}");
        write_bundle(
            &f.root,
            &rel,
            &manifest("sig-app", 1, &[]),
            &[("intent-app.sig", bad)],
        );
        let e = f
            .call_as(
                owner(),
                "app.install",
                json!({ "workspaceId": f.ws.0, "path": rel }),
            )
            .await
            .unwrap_err();
        let Error::App(app) = &e else {
            panic!("case {i}: {e:?}")
        };
        assert_eq!(app.data_code(), "invalid-bundle", "case {i}");
        assert_eq!(app.data["path"], "intent-app.sig", "case {i}");
    }
    let bad_publishers = [
        (json!({ "url": "https://e.com" }), "/publisher/name"),
        (json!({ "name": "  " }), "/publisher/name"),
        (json!({ "name": "a\u{85}b" }), "/publisher/name"),
        (
            json!({ "name": "n", "url": "http://e.com" }),
            "/publisher/url",
        ),
        (
            json!({ "name": "n", "url": "https:///path" }),
            "/publisher/url",
        ),
        (
            json!({ "name": "n", "url": "https://\\path" }),
            "/publisher/url",
        ),
        (
            json!({ "name": "n", "keyId": "has space" }),
            "/publisher/keyId",
        ),
        (json!({ "name": "n", "other": 1 }), "/publisher/other"),
        (json!("Example"), "/publisher"),
    ];
    for (i, (publisher, field)) in bad_publishers.into_iter().enumerate() {
        let mut m = manifest("pub-app", 1, &[]);
        m["publisher"] = publisher;
        let rel = format!("badpub{i}");
        write_bundle(&f.root, &rel, &m, &[]);
        let e = f
            .call_as(
                owner(),
                "app.install",
                json!({ "workspaceId": f.ws.0, "path": rel }),
            )
            .await
            .unwrap_err();
        let Error::App(app) = &e else {
            panic!("case {i}: {e:?}")
        };
        assert_eq!(app.data_code(), "invalid-manifest", "case {i}");
        assert_eq!(app.data["field"], json!(field), "case {i}");
    }
    let mut m = manifest("upd-app", 1, &[]);
    m["updateUrl"] = json!("ftp://e.com");
    write_bundle(&f.root, "badupd", &m, &[]);
    let e = f
        .call_as(
            owner(),
            "app.install",
            json!({ "workspaceId": f.ws.0, "path": "badupd" }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "invalid-manifest");
}

#[test]
fn the_signature_file_is_outside_the_hash() {
    let dir = crate::test_support::test_tempdir("app-sig-hash-");
    std::fs::write(dir.path().join("intent-app.json"), "{}").unwrap();
    let before = bundle::read_bundle_dir(dir.path()).unwrap();
    std::fs::write(dir.path().join("intent-app.sig"), "{}").unwrap();
    let after = bundle::read_bundle_dir(dir.path()).unwrap();
    assert_eq!(before.hash, after.hash);
    assert_eq!(after.files.len(), 1);
    assert_eq!(after.signature.as_deref(), Some(b"{}".as_slice()));
}

#[test]
fn an_explicit_provider_is_never_overridden_by_the_default() {
    use super::{resolve_app_route, AppRoute};
    let providers = vec![
        json!({ "providerId": "claude-code", "available": true, "reason": null,
                "defaultModel": "claude-opus-5-5",
                "models": [{ "id": "claude-opus-5-5" }] }),
        json!({ "providerId": "codex", "available": true, "reason": null,
                "defaultModel": "gpt-codex",
                "models": [{ "id": "gpt-codex" }, { "id": "gpt-mini" }] }),
        json!({ "providerId": "pi", "available": false, "reason": "turned off",
                "defaultModel": null, "models": [] }),
    ];
    let run = |p, m| resolve_app_route(p, m, Some("claude-code"), &providers);
    assert_eq!(
        run(Some("codex"), None).unwrap(),
        AppRoute::Run {
            provider: "codex".into(),
            model: Some("gpt-codex".into())
        }
    );
    assert_eq!(
        run(Some("codex"), Some("gpt-mini")).unwrap(),
        AppRoute::Run {
            provider: "codex".into(),
            model: Some("gpt-mini".into())
        }
    );
    assert_eq!(
        run(None, None).unwrap(),
        AppRoute::Run {
            provider: "claude-code".into(),
            model: Some("claude-opus-5-5".into())
        }
    );
    // The model resolves against the selected provider only.
    assert_eq!(
        code(&run(Some("codex"), Some("claude-opus-5-5")).unwrap_err()),
        "model-unknown"
    );
    assert_eq!(
        code(&run(Some("nope"), None).unwrap_err()),
        "provider-unknown"
    );
    assert_eq!(
        run(Some("pi"), None).unwrap(),
        AppRoute::Unavailable("turned off".into())
    );
    assert!(matches!(
        resolve_app_route(None, None, None, &providers).unwrap(),
        AppRoute::Unavailable(_)
    ));
}
