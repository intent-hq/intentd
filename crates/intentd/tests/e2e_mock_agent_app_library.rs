//! Hermetic ACP E2E for `ws.appLibrary.*` (Intent apps, PROTOCOL §5.60, Labs).
//!
//! A mock ACP agent calls the `workspace_api` tool over real MCP and installs
//! an app bundle it "built" in its workspace: a dry run, a real install, the
//! library reads, and the contract errors for an invalid bundle, a path
//! outside the workspace and an installed id. It then tries the user-only
//! and app-principal methods through every frame it can name, and each one
//! is refused. The test reads the agent's results from a file it writes and
//! checks the library as the daemon.

use crate::common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use intent_acp::{EventSink, SpawnOptions};
use intent_core::{now_iso, AgentId, Caller, Workspace, WorkspaceApi, WorkspaceId};
use intent_providers::ProviderConfig;
use intent_services::{AgentManager, BusEventSink, EventBus, Services};
use intent_store::Store;
use serde_json::{json, Map, Value};

fn gate() -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping ws.appLibrary e2e: node not on PATH");
        return None;
    }
    if !Path::new(&script).exists() {
        eprintln!("skipping ws.appLibrary e2e: script missing at {script}");
        return None;
    }
    Some(script)
}

fn write_bundle(root: &Path, rel: &str, manifest: &Value) {
    let dir = root.join(rel);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("intent-app.json"), manifest.to_string()).unwrap();
    std::fs::write(dir.join("index.html"), "<h1>demo</h1>").unwrap();
}

fn params(v: &Value) -> Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

const AGENT_JS: &str = r"
    const fail = async (f) => {
        try { await f(); return null; } catch (e) { return String((e && e.message) || e); }
    };
    const out = {};
    out.help = await ws.help('appLibrary');
    out.dry = await ws.appLibrary.install({ path: 'apps/demo', dryRun: true });
    out.listAfterDry = await ws.appLibrary.list();
    out.real = await ws.appLibrary.install('apps/demo');
    out.list = await ws.appLibrary.list();
    out.get = await ws.appLibrary.get('demo-app');
    out.invalid = await fail(() => ws.appLibrary.install({ path: 'apps/broken' }));
    out.missing = await fail(() => ws.appLibrary.install({ path: 'apps/none' }));
    out.outside = await fail(() => ws.appLibrary.install({ path: '../outside' }));
    out.exists = await fail(() => ws.appLibrary.install({ path: 'apps/demo', dryRun: true }));
    out.otherWorkspace = await fail(() => ws.appLibrary.install({ path: 'apps/demo', workspaceId: 'x' }));
    out.unknownApp = await fail(() => ws.appLibrary.get('no-such-app'));
    const hash = out.real.app.bundleHash;
    out.approveBinding = await fail(() => ws.appLibrary.approve('demo-app'));
    out.approveFrame = await fail(() => host({ method: 'appLibrary.approve', args: { appId: 'demo-app', bundleHash: hash, level: 1, features: [] } }));
    out.appApproveFrame = await fail(() => host({ method: 'app.approve', args: { appId: 'demo-app', bundleHash: hash, level: 1, features: [] } }));
    out.sessionOpenFrame = await fail(() => host({ method: 'appLibrary.session.open', args: { appId: 'demo-app' } }));
    out.appSessionOpenFrame = await fail(() => host({ method: 'app.session.open', args: { appId: 'demo-app' } }));
    out.storageFrame = await fail(() => host({ method: 'app.storage.get', args: { key: 'k' } }));
    await ws.file.write('applib.json', JSON.stringify(out));
    return { ok: true };
";

#[intent_test_macros::daemon_test]
async fn agent_installs_an_app_and_cannot_approve_or_open_it() {
    let Some(script) = gate() else { return };

    let tmp = common::test_tempdir("itd-e2e-applib-");
    let ws_root = tmp.path().join("ws");
    std::fs::create_dir_all(&ws_root).unwrap();
    std::fs::create_dir_all(tmp.path().join("outside")).unwrap();
    write_bundle(
        &ws_root,
        "apps/demo",
        &json!({
            "manifestVersion": 1,
            "id": "demo-app",
            "name": "Demo App",
            "version": "1.0.0",
            "permissions": { "level": 1, "features": [] },
        }),
    );
    // No `name`: an invalid manifest.
    write_bundle(
        &ws_root,
        "apps/broken",
        &json!({
            "manifestVersion": 1,
            "id": "broken-app",
            "version": "1.0.0",
            "permissions": { "level": 1, "features": [] },
        }),
    );

    let store = Store::open(&tmp.path().join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new_isolated(store.clone())
        .with_workspaces_root(ws_root.clone())
        .with_settings_registry(common::registry_with_default_provider(&ws_root))
        .with_apps_dir(&tmp.path().join("data"))
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    let ts = now_iso();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "E2E App Library".into(),
            branch: "main".into(),
            created_at: ts.clone(),
            updated_at: ts,
            path: Some(ws_root.to_string_lossy().into_owned()),
            worktree_path: Some(ws_root.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .await
        .expect("insert ws");

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E App Library".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    let script_static: &'static str = Box::leak(script.into_boxed_str());
    let base_args: &'static [&'static str] = Box::leak(vec![script_static].into_boxed_slice());
    let provider = ProviderConfig {
        command: "node",
        base_args,
        supports_authenticate: true,
        supports_mcp_config: true,
        mcp_config_flag: Some("--mcp-config"),
        ..*intent_providers::find_provider("mock").unwrap()
    };
    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": AGENT_JS, "summary": "app library e2e" }
        },
        "response": "app installed",
    })
    .to_string();
    let mut extra_env = BTreeMap::new();
    extra_env.insert("MOCK_AGENT_BEHAVIOR".to_string(), behavior);
    let cwd_dir = common::test_tempdir("itd-agent-cwd-");
    let cwd = cwd_dir.path().to_path_buf();
    let mut opts = SpawnOptions::new(&provider);
    opts.cwd = Some(&cwd);
    opts.extra_env = extra_env;

    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let manager = AgentManager::new(services.clone(), sink, 8)
        .with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd"));
    manager
        .create_agent(
            agent_id.clone(),
            ws.clone(),
            "E2E App Library",
            "interactive",
            cwd.clone(),
            &opts,
        )
        .await
        .expect("create_agent");
    let acp_session = manager
        .start_session(&agent_id, cwd.clone(), &provider)
        .await
        .expect("start_session");
    let block: intent_acp::session::ContentBlock =
        serde_json::from_value(json!({ "type": "text", "text": "install the app" })).unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));
    manager.shutdown().await;

    let raw = std::fs::read_to_string(ws_root.join("applib.json"))
        .expect("applib.json written by the agent");
    let out: Value = serde_json::from_str(&raw).expect("valid JSON");

    let help = out["help"].as_str().expect("ws.help(\"appLibrary\") text");
    assert!(help.contains("ws.appLibrary.install("), "{help}");

    // Dry run: valid, nothing stored.
    assert_eq!(out["dry"]["dryRun"], true, "{out}");
    assert_eq!(out["dry"]["app"]["id"], "demo-app");
    assert!(out["dry"]["nextStep"]
        .as_str()
        .unwrap()
        .starts_with("Dry run"));
    assert_eq!(out["listAfterDry"]["apps"], json!([]), "{out}");

    // Real install: summary, hash, pending approval, and the user's step.
    let app = &out["real"]["app"];
    assert_eq!(out["real"]["dryRun"], false);
    assert_eq!(app["name"], "Demo App");
    assert_eq!(app["version"], "1.0.0");
    let hash = app["bundleHash"].as_str().expect("bundleHash");
    assert!(hash.starts_with("sha256:"), "{hash}");
    assert_eq!(app["approval"]["status"], "pending");
    assert!(app.get("publisher").is_none(), "summary row: {app}");
    let next = out["real"]["nextStep"].as_str().unwrap();
    assert!(next.contains("must approve"), "{next}");
    assert_eq!(out["list"]["apps"][0]["id"], "demo-app");
    assert_eq!(out["get"]["app"]["source"]["workspaceId"], ws.as_str());
    assert_eq!(out["get"]["app"]["source"]["path"], "apps/demo");

    // Contract errors lead with their code.
    let err = |k: &str| {
        out[k]
            .as_str()
            .unwrap_or_else(|| panic!("{k} must fail: {out}"))
    };
    assert!(err("invalid").starts_with("invalid-manifest: "), "{out}");
    assert!(err("invalid").contains(r#""field":"/name""#), "{out}");
    assert!(err("missing").starts_with("path-not-found: "), "{out}");
    assert!(err("outside").starts_with("path-not-found: "), "{out}");
    assert!(err("exists").starts_with("app-exists: "), "{out}");
    assert!(err("otherWorkspace").contains("`workspaceId` is not accepted"));
    assert!(err("unknownApp").starts_with("app-not-found: "), "{out}");

    // No path approves the app, opens a session or acts as the app.
    for k in [
        "approveBinding",
        "approveFrame",
        "appApproveFrame",
        "sessionOpenFrame",
        "appSessionOpenFrame",
        "storageFrame",
    ] {
        err(k);
    }

    // The service refuses the user-only methods for this agent, whatever
    // path reaches it.
    let as_agent = Caller::Agent {
        agent_id: agent_id.clone(),
    };
    for (method, p) in [
        (
            "app.approve",
            json!({ "appId": "demo-app", "bundleHash": hash, "level": 1, "features": [] }),
        ),
        ("app.session.open", json!({ "appId": "demo-app" })),
    ] {
        let e = intent_core::with_caller(
            as_agent.clone(),
            services.app_call(method.to_owned(), params(&p)),
        )
        .await
        .expect_err(method);
        assert!(e.to_string().contains("user-only"), "{method}: {e}");
    }

    // The library holds the app, still pending.
    let got = intent_core::with_caller(
        Caller::Daemon,
        services.app_call(
            "app.get".to_owned(),
            params(&json!({ "appId": "demo-app" })),
        ),
    )
    .await
    .expect("app.get");
    assert_eq!(got["app"]["bundleHash"], hash);
    assert_eq!(got["app"]["approval"]["status"], "pending");
}
