//! Over-the-wire `specialist.*` slice (PROTOCOL §5.11, §18.2): drive the full
//! CRUD + the ported `specialist.list` against the daemon over a temp UDS,
//! proving camelCase `SpecialistDef` shapes, **3-tier** resolution
//! (project > user > bundled), the bundled tier being read-only, and that
//! malformed/unknown ids map to `-32602`. Directory roots are injected via
//! `Services::with_specialist_dirs` so the test is hermetic.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct TempDb {
    path: PathBuf,
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    // 15s (not 2s): the `resolvedModel` preview's ownership guard can capture
    // the login-shell PATH on first use (one-time per process, 5s cap).
    let n = timeout(Duration::from_secs(15), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

/// Send one request and return the full response envelope (so both `result`
/// and `error` cases can be asserted).
async fn call(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    send(write_half, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(reader).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    resp
}

/// Assert a successful call and return its `result`.
async fn ok(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let resp = call(write_half, reader, id, method, params).await;
    assert!(resp.get("error").is_none(), "rpc {method} errored: {resp}");
    resp["result"].clone()
}

fn write_specialist(dir: &Path, id: &str, name: &str, desc: &str, prompt: &str) {
    std::fs::create_dir_all(dir).unwrap();
    let body = format!("---\nname: \"{name}\"\ndescription: \"{desc}\"\n---\n\n{prompt}");
    std::fs::write(dir.join(format!("{id}.md")), body).unwrap();
}

struct Harness {
    _user: TempDir,
    _bundled: TempDir,
    _work: TempDir,
    _tmp: TempDb,
    _ws_root: tempfile::TempDir,
    user_dir: PathBuf,
    bundled_dir: PathBuf,
    work_dir: PathBuf,
    socket: PathBuf,
    _sock_dir: tempfile::TempDir,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<()>>,
}

async fn start() -> Harness {
    start_with_config(None).await
}

/// Like [`start`] but wires a [`intent_services::SettingsRegistry`] loaded
/// from the given `config.toml` text, so the settings chain participates in
/// the `resolvedModel` preview.
async fn start_with_settings(config_toml: &str) -> Harness {
    start_with_config(Some(config_toml)).await
}

async fn start_with_config(config_toml: Option<&str>) -> Harness {
    let tag = Uuid::new_v4();
    let user = TempDir(std::env::temp_dir().join(format!("intentd-spec-user-{tag}")));
    let bundled = TempDir(std::env::temp_dir().join(format!("intentd-spec-bundled-{tag}")));
    let work = TempDir(std::env::temp_dir().join(format!("intentd-spec-work-{tag}")));
    std::fs::create_dir_all(&user.0).unwrap();
    std::fs::create_dir_all(&bundled.0).unwrap();
    std::fs::create_dir_all(&work.0).unwrap();
    let tmp = TempDb {
        path: std::env::temp_dir().join(format!("intentd-spec-{tag}.db")),
    };
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let mut services = Services::new(store)
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone())
        .with_specialist_dirs(Some(user.0.clone()), Some(bundled.0.clone()));
    if let Some(toml) = config_toml {
        // The config file lives inside the work temp dir so it is swept with it.
        let config_path = work.0.join("config.toml");
        std::fs::write(&config_path, toml).unwrap();
        let registry = intent_services::SettingsRegistry::load(&config_path).expect("load config");
        services = services.with_settings_registry(Arc::new(registry));
    }
    let services: Arc<dyn WorkspaceApi> = Arc::new(services);
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "is-");
    let socket = sock_dir.path().join("uds.sock");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let socket = socket.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });
    Harness {
        user_dir: user.0.clone(),
        bundled_dir: bundled.0.clone(),
        work_dir: work.0.clone(),
        _user: user,
        _bundled: bundled,
        _work: work,
        _tmp: tmp,
        _ws_root: ws_root,
        socket,
        _sock_dir: sock_dir,
        shutdown_tx: Some(shutdown_tx),
        server: Some(server),
    }
}

impl Harness {
    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(s) = self.server.take() {
            let _ = s.await;
        }
    }
}

#[tokio::test]
async fn specialist_full_crud_and_three_tier_resolution() {
    let h = start().await;
    // Seed a bundled (read-only) specialist + a bundled-only one.
    write_specialist(
        &h.bundled_dir,
        "implementor",
        "Implementor",
        "Bundled implementor",
        "You implement.",
    );
    write_specialist(
        &h.bundled_dir,
        "verifier",
        "Verifier",
        "Bundled verifier",
        "You verify.",
    );

    let (read, mut w) = connect_retry(&h.socket).await.into_split();
    let mut r = BufReader::new(read);
    let wp = h.work_dir.to_string_lossy().to_string();

    // list — bundled tier visible (no workspaceId required). The eight embedded
    // reference specialists (PP-2) are always present; the two dir-seeded files
    // override the embedded copies for the same ids.
    let list = ok(&mut w, &mut r, 1, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().expect("specialists array");
    assert_eq!(
        specs.len(),
        8,
        "eight embedded ids, two overridden from dir"
    );
    let imp = specs.iter().find(|s| s["id"] == "implementor").unwrap();
    assert_eq!(imp["source"], "bundled");
    assert_eq!(imp["name"], "Implementor");
    assert_eq!(imp["description"], "Bundled implementor", "dir file wins");
    assert!(imp.get("path").is_none(), "bundled exposes no path");
    let chief = specs
        .iter()
        .find(|s| s["id"] == "chief-of-staff")
        .expect("embedded-only id listed");
    assert_eq!(chief["hidden"], true, "bundled chief-of-staff is hidden");
    assert!(
        imp.get("hidden").is_none(),
        "non-hidden specialists omit the field"
    );

    // get — resolved view of a bundled id.
    let got = ok(
        &mut w,
        &mut r,
        2,
        "specialist.get",
        json!({ "id": "verifier" }),
    )
    .await;
    assert_eq!(got["specialist"]["source"], "bundled");
    assert_eq!(got["specialist"]["prompt"], "You verify.");

    // create — user scope (default) authors a new specialist file. A retired
    // `modelTier` in the spec is tolerated-and-ignored (never echoed).
    let created = ok(
        &mut w,
        &mut r,
        3,
        "specialist.create",
        json!({ "id": "reviewer", "spec": {
            "id": "reviewer", "name": "Reviewer",
            "description": "Reviews diffs", "modelTier": "high",
            "prompt": "You review code changes." } }),
    )
    .await;
    assert_eq!(created["specialist"]["source"], "user");
    assert!(created["specialist"].get("modelTier").is_none());
    assert!(h.user_dir.join("reviewer.md").exists());

    // 3-tier: user file with the same id as bundled wins over bundled.
    let _ = ok(
        &mut w,
        &mut r,
        4,
        "specialist.create",
        json!({ "id": "implementor", "spec": {
            "id": "implementor", "name": "User Implementor",
            "description": "User override", "prompt": "user prompt" } }),
    )
    .await;
    let got = ok(
        &mut w,
        &mut r,
        5,
        "specialist.get",
        json!({ "id": "implementor" }),
    )
    .await;
    assert_eq!(got["specialist"]["source"], "user");
    assert_eq!(got["specialist"]["name"], "User Implementor");

    // 3-tier: project file (via workspacePath) wins over user + bundled.
    let _ = ok(
        &mut w,
        &mut r,
        6,
        "specialist.create",
        json!({ "id": "implementor", "scope": "project", "workspacePath": wp,
            "spec": { "id": "implementor", "name": "Project Implementor",
                "description": "Project override", "prompt": "project prompt" } }),
    )
    .await;
    let got = ok(
        &mut w,
        &mut r,
        7,
        "specialist.get",
        json!({ "id": "implementor", "workspacePath": wp }),
    )
    .await;
    assert_eq!(got["specialist"]["source"], "project");
    assert_eq!(got["specialist"]["name"], "Project Implementor");

    // list matches the TS WSS signature: no params; merges user > bundled only
    // (no project tier). The user-scoped `implementor` override wins over bundled;
    // the project file created above is NOT surfaced by `list`.
    let list = ok(&mut w, &mut r, 8, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().unwrap();
    let imp = specs.iter().find(|s| s["id"] == "implementor").unwrap();
    assert_eq!(imp["source"], "user");
    assert!(specs.iter().any(|s| s["id"] == "reviewer"));
    assert!(specs.iter().any(|s| s["id"] == "verifier"));

    // edit — overwrite the user-scoped reviewer.
    let edited = ok(
        &mut w,
        &mut r,
        9,
        "specialist.edit",
        json!({ "id": "reviewer", "scope": "user", "spec": {
            "id": "reviewer", "name": "Reviewer v2",
            "description": "Reviews diffs carefully", "prompt": "v2" } }),
    )
    .await;
    assert_eq!(edited["specialist"]["name"], "Reviewer v2");

    // delete — remove the user-scoped reviewer.
    let deleted = ok(
        &mut w,
        &mut r,
        10,
        "specialist.delete",
        json!({ "id": "reviewer", "scope": "user" }),
    )
    .await;
    assert_eq!(deleted["success"], true);
    assert!(!h.user_dir.join("reviewer.md").exists());

    h.shutdown().await;
}

#[tokio::test]
async fn specialist_full_frontmatter_wire_parity() {
    let h = start().await;
    // Seed a bundled specialist whose frontmatter carries every optional scalar
    // plus the optional `hidden` boolean and a retired `modelTier` line.
    let body = "---\nname: \"Ralph\"\ndescription: \"Loops forever\"\ncodingAgent: \"claude\"\nmodel: \"opus4.5\"\nmodelTier: \"smart\"\nroleReminder: \"Never stop early\"\nagentType: \"ralph-loop\"\nhidden: true\n---\n\nYou loop.";
    std::fs::write(h.bundled_dir.join("ralph.md"), body).unwrap();

    let (read, mut w) = connect_retry(&h.socket).await.into_split();
    let mut r = BufReader::new(read);

    // get — bundled tier surfaces the full wire shape.
    let got = ok(
        &mut w,
        &mut r,
        1,
        "specialist.get",
        json!({ "id": "ralph" }),
    )
    .await;
    let s = &got["specialist"];
    assert_eq!(s["codingAgent"], "claude");
    assert_eq!(s["model"], "opus4.5");
    assert!(
        s.get("modelTier").is_none(),
        "retired modelTier is never echoed"
    );
    assert_eq!(s["roleReminder"], "Never stop early");
    assert_eq!(s["agentType"], "ralph-loop");
    assert_eq!(s["prompt"], "You loop.");
    assert_eq!(
        s["behaviorPrompt"], "You loop.",
        "behaviorPrompt aliases prompt"
    );
    assert_eq!(s["isCustomized"], false, "bundled is not customized");
    assert_eq!(s["hidden"], true, "hidden frontmatter surfaces on get");

    // list — same fields visible in the list projection.
    let list = ok(&mut w, &mut r, 2, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().unwrap();
    let ralph = specs.iter().find(|s| s["id"] == "ralph").unwrap();
    assert_eq!(ralph["agentType"], "ralph-loop");
    assert_eq!(ralph["roleReminder"], "Never stop early");
    assert_eq!(ralph["isCustomized"], false);
    assert_eq!(ralph["hidden"], true, "hidden surfaces in list");

    // create→get round-trip: a user specialist persists every live field
    // losslessly (the retired `modelTier` is dropped), and the body may be
    // supplied via the `behaviorPrompt` alias.
    let created = ok(
        &mut w,
        &mut r,
        3,
        "specialist.create",
        json!({ "id": "ralph2", "spec": {
            "id": "ralph2", "name": "Ralph II",
            "description": "Loops again", "codingAgent": "claude",
            "model": "opus4.5", "modelTier": "smart",
            "roleReminder": "Keep going", "agentType": "ralph-loop",
            "hidden": true,
            "behaviorPrompt": "Loop body." } }),
    )
    .await;
    assert_eq!(created["specialist"]["agentType"], "ralph-loop");
    assert_eq!(created["specialist"]["isCustomized"], true);
    assert_eq!(created["specialist"]["prompt"], "Loop body.");
    assert_eq!(created["specialist"]["behaviorPrompt"], "Loop body.");
    assert_eq!(created["specialist"]["hidden"], true);

    let got = ok(
        &mut w,
        &mut r,
        4,
        "specialist.get",
        json!({ "id": "ralph2" }),
    )
    .await;
    let s = &got["specialist"];
    assert_eq!(s["source"], "user");
    assert_eq!(s["codingAgent"], "claude");
    assert_eq!(s["model"], "opus4.5");
    assert!(
        s.get("modelTier").is_none(),
        "retired modelTier is dropped on create"
    );
    assert_eq!(s["roleReminder"], "Keep going");
    assert_eq!(s["agentType"], "ralph-loop");
    assert_eq!(s["prompt"], "Loop body.");
    assert_eq!(s["behaviorPrompt"], "Loop body.");
    assert_eq!(s["isCustomized"], true);
    assert_eq!(s["hidden"], true, "create round-trips hidden");

    // edit — a spec that still carries hidden does not drop the flag.
    let edited = ok(
        &mut w,
        &mut r,
        5,
        "specialist.edit",
        json!({ "id": "ralph2", "scope": "user", "spec": {
            "id": "ralph2", "name": "Ralph II",
            "description": "Loops again", "hidden": true,
            "prompt": "Edited body." } }),
    )
    .await;
    assert_eq!(edited["specialist"]["prompt"], "Edited body.");
    assert_eq!(
        edited["specialist"]["hidden"], true,
        "edit round-trips hidden"
    );

    // edit — a spec that omits hidden writes a file without the key; the
    // resolved value then inherits from lower tiers (none hide ralph2, so it
    // resolves not-hidden).
    let edited = ok(
        &mut w,
        &mut r,
        6,
        "specialist.edit",
        json!({ "id": "ralph2", "scope": "user", "spec": {
            "id": "ralph2", "name": "Ralph II",
            "description": "Loops again",
            "prompt": "Edited body." } }),
    )
    .await;
    assert!(
        edited["specialist"].get("hidden").is_none(),
        "edit omitting hidden drops the flag"
    );
    let got = ok(
        &mut w,
        &mut r,
        7,
        "specialist.get",
        json!({ "id": "ralph2" }),
    )
    .await;
    assert!(
        got["specialist"].get("hidden").is_none(),
        "subsequent get confirms omit ⇒ inherit (no lower tier hides ralph2)"
    );

    h.shutdown().await;
}

#[tokio::test]
async fn specialist_hidden_inherits_across_tiers_on_the_wire() {
    let h = start().await;
    // Regression: a user-tier chief-of-staff.md materialized before the hidden
    // feature (no `hidden` key) must still resolve hidden: true, inherited
    // from the embedded bundled floor (PROTOCOL §5.11).
    write_specialist(
        &h.user_dir,
        "chief-of-staff",
        "Chief of Staff",
        "User override",
        "You orchestrate.",
    );

    let (read, mut w) = connect_retry(&h.socket).await.into_split();
    let mut r = BufReader::new(read);

    let list = ok(&mut w, &mut r, 1, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().unwrap();
    let chief = specs.iter().find(|s| s["id"] == "chief-of-staff").unwrap();
    assert_eq!(chief["source"], "user", "user override wins the tier merge");
    assert_eq!(
        chief["hidden"], true,
        "hidden inherited from the embedded floor in list"
    );

    let got = ok(
        &mut w,
        &mut r,
        2,
        "specialist.get",
        json!({ "id": "chief-of-staff" }),
    )
    .await;
    assert_eq!(got["specialist"]["source"], "user");
    assert_eq!(
        got["specialist"]["hidden"], true,
        "hidden inherited from the embedded floor on get"
    );

    // Explicit hidden: false in the user file is the opt-out that unhides.
    std::fs::write(
        h.user_dir.join("chief-of-staff.md"),
        "---\nname: \"Chief of Staff\"\ndescription: \"User override\"\nhidden: false\n---\n\nYou orchestrate.",
    )
    .unwrap();
    let got = ok(
        &mut w,
        &mut r,
        3,
        "specialist.get",
        json!({ "id": "chief-of-staff" }),
    )
    .await;
    assert!(
        got["specialist"].get("hidden").is_none(),
        "explicit false unhides on get"
    );
    let list = ok(&mut w, &mut r, 4, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().unwrap();
    let chief = specs.iter().find(|s| s["id"] == "chief-of-staff").unwrap();
    assert!(
        chief.get("hidden").is_none(),
        "explicit false unhides in list"
    );

    h.shutdown().await;
}

#[tokio::test]
async fn specialist_bundled_read_only_and_invalid_params() {
    let h = start().await;
    write_specialist(
        &h.bundled_dir,
        "implementor",
        "Implementor",
        "Bundled implementor",
        "You implement.",
    );
    let (read, mut w) = connect_retry(&h.socket).await.into_split();
    let mut r = BufReader::new(read);

    // get on an unknown id → -32602.
    let resp = call(&mut w, &mut r, 1, "specialist.get", json!({ "id": "nope" })).await;
    assert_eq!(resp["error"]["code"], -32602);

    // edit with scope:"bundled" is rejected (read-only) → -32602.
    let resp = call(
        &mut w,
        &mut r,
        2,
        "specialist.edit",
        json!({ "id": "implementor", "scope": "bundled",
            "spec": { "id": "implementor", "name": "x", "description": "y" } }),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    // delete a bundled-only id at user scope → file missing → -32602.
    let resp = call(
        &mut w,
        &mut r,
        3,
        "specialist.delete",
        json!({ "id": "implementor", "scope": "user" }),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    // create requires id + spec → missing spec yields -32602.
    let resp = call(
        &mut w,
        &mut r,
        4,
        "specialist.create",
        json!({ "id": "nospec" }),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    h.shutdown().await;
}

/// Write a specialist file with extra frontmatter lines (e.g. `model`) for
/// the resolution-preview tests.
fn write_specialist_frontmatter(dir: &Path, id: &str, extra_frontmatter: &str) {
    std::fs::create_dir_all(dir).unwrap();
    let body =
        format!("---\nname: \"{id}\"\ndescription: \"d\"\n{extra_frontmatter}\n---\n\nYou work.");
    std::fs::write(dir.join(format!("{id}.md")), body).unwrap();
}

/// Find one specialist def by id in a `specialist.list` result.
fn find_spec<'a>(list: &'a Value, id: &str) -> &'a Value {
    list["specialists"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["id"] == id)
        .unwrap_or_else(|| panic!("specialist {id} missing from list"))
}

#[tokio::test]
async fn specialist_resolution_preview() {
    // monorepo#3044: the preview's "default provider" context derives from
    // settings only (no positional fallback), so pin auggie explicitly.
    let h = start_with_settings("[providers]\nactive = \"auggie\"\n").await;
    // Pinned frontmatter model — bare ids are provider-agnostic while no
    // cached catalog disproves ownership (cold caches in this harness).
    write_specialist_frontmatter(&h.user_dir, "pinner", "model: \"opus4.5\"");
    // Retired tier opt-in: tolerated in the file, ignored by resolution.
    write_specialist_frontmatter(&h.user_dir, "tiered", "modelTier: \"smart\"");
    // A second bare pin behaves identically (no static tier tables remain).
    write_specialist_frontmatter(&h.user_dir, "auggie-pin", "model: \"opus4.7\"");
    // No model config at all → provider CLI default → fields omitted.
    write_specialist(&h.user_dir, "plain", "Plain", "d", "You work.");

    let (read, mut w) = connect_retry(&h.socket).await.into_split();
    let mut r = BufReader::new(read);

    // No provider param → default provider (auggie) context.
    let list = ok(&mut w, &mut r, 1, "specialist.list", json!({})).await;
    let pinner = find_spec(&list, "pinner");
    assert_eq!(pinner["resolvedModel"], "opus4.5");
    assert_eq!(pinner["resolvedProvider"], "auggie");
    // Retired modelTier: never echoed, never resolved — with no settings
    // chain the preview falls through to the CLI default (omitted).
    let tiered = find_spec(&list, "tiered");
    assert!(tiered.get("modelTier").is_none());
    assert!(tiered.get("resolvedModel").is_none());
    assert!(tiered.get("resolvedProvider").is_none());
    let auggie_pin = find_spec(&list, "auggie-pin");
    assert_eq!(auggie_pin["resolvedModel"], "opus4.7");
    // No settings chain → CLI default → both preview fields omitted.
    let plain = find_spec(&list, "plain");
    assert!(plain.get("resolvedModel").is_none());
    assert!(plain.get("resolvedProvider").is_none());

    // Explicit provider context: codex.
    let list = ok(
        &mut w,
        &mut r,
        2,
        "specialist.list",
        json!({ "provider": "codex" }),
    )
    .await;
    // The retired tier does not resolve under codex either.
    let tiered = find_spec(&list, "tiered");
    assert!(tiered.get("resolvedModel").is_none());
    assert!(tiered.get("resolvedProvider").is_none());
    // With no cached-catalog disproof, bare pins apply under codex too —
    // absence of evidence is not a mismatch.
    let auggie_pin = find_spec(&list, "auggie-pin");
    assert_eq!(auggie_pin["resolvedModel"], "opus4.7");
    assert_eq!(auggie_pin["resolvedProvider"], "codex");
    let pinner = find_spec(&list, "pinner");
    assert_eq!(pinner["resolvedModel"], "opus4.5");
    assert_eq!(pinner["resolvedProvider"], "codex");

    // specialist.get mirrors the omission for the retired tier.
    let got = ok(
        &mut w,
        &mut r,
        3,
        "specialist.get",
        json!({ "id": "tiered", "provider": "claude-code" }),
    )
    .await;
    assert!(got["specialist"].get("resolvedModel").is_none());

    // Same probe under grok — a tierless provider (dynamic model list).
    let got = ok(
        &mut w,
        &mut r,
        4,
        "specialist.get",
        json!({ "id": "tiered", "provider": "grok" }),
    )
    .await;
    assert!(got["specialist"].get("resolvedModel").is_none());

    // specialist.get mirrors the list decoration.
    let got = ok(
        &mut w,
        &mut r,
        5,
        "specialist.get",
        json!({ "id": "pinner" }),
    )
    .await;
    assert_eq!(got["specialist"]["resolvedModel"], "opus4.5");
    assert_eq!(got["specialist"]["resolvedProvider"], "auggie");

    // Unknown provider → -32602 on both methods.
    let resp = call(
        &mut w,
        &mut r,
        6,
        "specialist.list",
        json!({ "provider": "nope" }),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    let resp = call(
        &mut w,
        &mut r,
        7,
        "specialist.get",
        json!({ "id": "pinner", "provider": "nope" }),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    h.shutdown().await;
}

/// monorepo#3044 follow-up: with NO settings-derived default provider and no
/// `provider` param, a specialist whose own frontmatter pins a provider (a
/// compound `model` prefix, or `codingAgent` + bare `model`) must still
/// preview the concrete provider/model `agent.delegate` would pin — not
/// "Provider default". A specialist with no pin of its own stays
/// undecorated.
#[tokio::test]
async fn specialist_preview_uses_own_pin_without_global_default() {
    let h = start().await;
    write_specialist_frontmatter(
        &h.user_dir,
        "compound-pin",
        "model: \"codex:gpt-5.3-codex\"",
    );
    write_specialist_frontmatter(
        &h.user_dir,
        "agent-pin",
        "codingAgent: \"grok\"\nmodel: \"grok-4\"",
    );
    write_specialist(&h.user_dir, "plain", "Plain", "d", "You work.");

    let (read, mut w) = connect_retry(&h.socket).await.into_split();
    let mut r = BufReader::new(read);

    let list = ok(&mut w, &mut r, 1, "specialist.list", json!({})).await;
    // Compound model prefix names the provider the delegate would spawn on.
    let compound = find_spec(&list, "compound-pin");
    assert_eq!(compound["resolvedModel"], "codex:gpt-5.3-codex");
    assert_eq!(compound["resolvedProvider"], "codex");
    // Frontmatter codingAgent pins the provider; the bare model rides it
    // (cold caches — absence of ownership evidence passes).
    let agent_pin = find_spec(&list, "agent-pin");
    assert_eq!(agent_pin["resolvedModel"], "grok-4");
    assert_eq!(agent_pin["resolvedProvider"], "grok");
    // No pin, no settings default → undecorated ("Provider default").
    let plain = find_spec(&list, "plain");
    assert!(plain.get("resolvedModel").is_none());
    assert!(plain.get("resolvedProvider").is_none());

    // specialist.get mirrors the list decoration.
    let got = ok(
        &mut w,
        &mut r,
        2,
        "specialist.get",
        json!({ "id": "compound-pin" }),
    )
    .await;
    assert_eq!(got["specialist"]["resolvedModel"], "codex:gpt-5.3-codex");
    assert_eq!(got["specialist"]["resolvedProvider"], "codex");

    // An explicit provider param still wins over the specialist's own pin.
    let got = ok(
        &mut w,
        &mut r,
        3,
        "specialist.get",
        json!({ "id": "agent-pin", "provider": "auggie" }),
    )
    .await;
    assert_eq!(got["specialist"]["resolvedProvider"], "auggie");

    h.shutdown().await;
}

#[tokio::test]
async fn specialist_resolution_preview_inherits_settings() {
    let h = start_with_settings(
        "[providers]\nactive = \"auggie\"\n\n[model]\ndefault = \"sonnet4.5\"\n\n[model.providerDefaults]\ncodex = \"gpt-5.3-codex/high\"\n",
    )
    .await;
    // No frontmatter model config → settings chain decides.
    write_specialist(&h.user_dir, "plain", "Plain", "d", "You work.");

    let (read, mut w) = connect_retry(&h.socket).await.into_split();
    let mut r = BufReader::new(read);

    // Default provider (auggie): no providerDefaults entry → model.default
    // resolves.
    let got = ok(
        &mut w,
        &mut r,
        1,
        "specialist.get",
        json!({ "id": "plain" }),
    )
    .await;
    assert_eq!(got["specialist"]["resolvedModel"], "sonnet4.5");
    assert_eq!(got["specialist"]["resolvedProvider"], "auggie");

    // codex: providerDefaults.codex wins over model.default.
    let got = ok(
        &mut w,
        &mut r,
        2,
        "specialist.get",
        json!({ "id": "plain", "provider": "codex" }),
    )
    .await;
    assert_eq!(got["specialist"]["resolvedModel"], "gpt-5.3-codex/high");
    assert_eq!(got["specialist"]["resolvedProvider"], "codex");

    // claude-code: no providerDefaults entry → model.default ("sonnet4.5")
    // applies — with no cached-catalog disproof (cold caches) the ownership
    // guard passes bare ids through.
    let got = ok(
        &mut w,
        &mut r,
        3,
        "specialist.get",
        json!({ "id": "plain", "provider": "claude-code" }),
    )
    .await;
    assert_eq!(got["specialist"]["resolvedModel"], "sonnet4.5");
    assert_eq!(got["specialist"]["resolvedProvider"], "claude-code");

    // Frontmatter model still ranks above the settings chain.
    write_specialist_frontmatter(&h.user_dir, "pinner", "model: \"opus4.5\"");
    let got = ok(
        &mut w,
        &mut r,
        4,
        "specialist.get",
        json!({ "id": "pinner" }),
    )
    .await;
    assert_eq!(got["specialist"]["resolvedModel"], "opus4.5");

    // Retirement regression: a lingering `modelTier: "smart"` file resolves
    // via the settings chain (model.default), not any tier mapping.
    write_specialist_frontmatter(&h.user_dir, "tiered", "modelTier: \"smart\"");
    let got = ok(
        &mut w,
        &mut r,
        5,
        "specialist.get",
        json!({ "id": "tiered" }),
    )
    .await;
    assert!(got["specialist"].get("modelTier").is_none());
    assert_eq!(got["specialist"]["resolvedModel"], "sonnet4.5");
    assert_eq!(got["specialist"]["resolvedProvider"], "auggie");

    h.shutdown().await;
}
