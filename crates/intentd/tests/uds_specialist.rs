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
    let n = timeout(Duration::from_secs(2), reader.read_line(&mut line))
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
    user_dir: PathBuf,
    bundled_dir: PathBuf,
    work_dir: PathBuf,
    socket: PathBuf,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<()>>,
}

async fn start() -> Harness {
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
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(common::hermetic_workspaces_root())
            .with_event_bus(bus.clone())
            .with_specialist_dirs(Some(user.0.clone()), Some(bundled.0.clone())),
    );
    let socket = std::env::temp_dir().join(format!("is-{tag}.sock"));
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
        socket,
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

    // list — bundled tier visible (no workspaceId required). The nine embedded
    // reference specialists (PP-2) are always present; the two dir-seeded files
    // override the embedded copies for the same ids.
    let list = ok(&mut w, &mut r, 1, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().expect("specialists array");
    assert_eq!(specs.len(), 9, "nine embedded ids, two overridden from dir");
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

    // create — user scope (default) authors a new specialist file.
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
    assert_eq!(created["specialist"]["modelTier"], "high");
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
    // plus the optional `hidden` boolean.
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
    assert_eq!(s["modelTier"], "smart");
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

    // create→get round-trip: a user specialist persists every field losslessly,
    // and the body may be supplied via the `behaviorPrompt` alias.
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
    assert_eq!(s["modelTier"], "smart");
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
