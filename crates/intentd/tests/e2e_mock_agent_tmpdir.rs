//! Hermetic e2e for per-agent temp folders: two real mock agents spawned by
//! the [`AgentManager`] each see their own `TMPDIR` / `TMP` / `TEMP` (honored
//! by Node's `os.tmpdir()` and by `mktemp` in a shell), so a fixed temp file
//! name can no longer collide across agents. The folder belongs to the agent,
//! not its process: after the idle reaper kills both processes, a relaunched
//! agent gets the same folder with the file its previous process wrote. The
//! sweep reclaims an idle agent's folder but never one a live launch holds,
//! and deleting the agent removes its folder.
//!
//! The mock fixture's `MOCK_AGENT_SESSION_LOG` + `MOCK_AGENT_LOG_TMPDIR`
//! seam records what the child saw at `session/new`. Gated on `node` and the
//! mock script; skips cleanly otherwise.

use crate::common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use intent_acp::{EventSink, SpawnOptions};
use intent_core::{now_iso, AgentId, Workspace, WorkspaceApi, WorkspaceId};
use intent_providers::ProviderConfig;
use intent_services::{AgentManager, BusEventSink, EventBus, Services};
use intent_store::Store;
use serde_json::Value;

fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E tmpdir".to_string(),
        branch: "main".to_string(),
        created_at: ts.clone(),
        updated_at: ts,
        ..Default::default()
    }
}

/// What one agent child reported about its temp variables.
struct TmpReport {
    tmpdir: PathBuf,
    raw: Value,
}

fn read_report(log: &Path) -> TmpReport {
    let text = std::fs::read_to_string(log)
        .unwrap_or_else(|e| panic!("session log {} missing: {e}", log.display()));
    let entry: Value = text
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).expect("session log line is JSON"))
        .find(|v| v["method"] == "session/new")
        .unwrap_or_else(|| panic!("no session/new entry in {text}"));
    let raw = entry["tmp"].clone();
    let tmpdir = PathBuf::from(
        raw["TMPDIR"]
            .as_str()
            .unwrap_or_else(|| panic!("TMPDIR unset in the agent: {raw}")),
    );
    TmpReport { tmpdir, raw }
}

/// Spawn (or relaunch) `agent_id` and return what its child reported.
async fn launch(
    manager: &AgentManager,
    provider: &ProviderConfig,
    ws: &WorkspaceId,
    agent_id: &AgentId,
    cwd: &Path,
    scratch: &Path,
    log_name: &str,
) -> TmpReport {
    let log = scratch.join(format!("{log_name}.session.log"));
    let mut extra_env = BTreeMap::new();
    extra_env.insert("MOCK_AGENT_SESSION_LOG".into(), log.display().to_string());
    extra_env.insert("MOCK_AGENT_LOG_TMPDIR".into(), "1".into());
    // A shared value the per-agent folder must override.
    extra_env.insert("TMPDIR".into(), scratch.display().to_string());
    let mut opts = SpawnOptions::new(provider);
    opts.cwd = Some(cwd);
    opts.extra_env = extra_env;
    manager
        .create_agent(
            agent_id.clone(),
            ws.clone(),
            "Tmp",
            "interactive",
            cwd.to_path_buf(),
            &opts,
        )
        .await
        .expect("create_agent");
    manager
        .start_session(agent_id, cwd.to_path_buf(), provider)
        .await
        .expect("start_session");
    read_report(&log)
}

#[intent_test_macros::daemon_test]
async fn agent_tmpdir_is_private_and_survives_relaunch() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping tmpdir E2E: node not on PATH");
        return;
    }
    if !Path::new(&script).exists() {
        eprintln!("skipping tmpdir E2E: script not found at {script}");
        return;
    }

    let db_dir = common::test_tempdir("intentd-e2e-tmpdir-");
    let store = Store::open(&db_dir.path().join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new_isolated(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_settings_registry(common::registry_with_default_provider(ws_root.path()))
        .with_event_bus(bus.clone());
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws))
        .await
        .expect("insert ws");

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

    // Short root, like production's `<os tmp>/intentd-tmp-<hash>`.
    let scratch = common::test_tempdir_in("/tmp", "itd-agtmp-");
    let tmp_root = scratch.path().join("root");
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let manager = Arc::new(
        AgentManager::new(services.clone(), sink, 8)
            .with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd"))
            .with_agent_tmp_root(&tmp_root),
    );

    let cwd_dir = common::test_tempdir("itd-agent-cwd-");
    let cwd = cwd_dir.path().to_path_buf();
    let mut agents = Vec::new();
    for tag in ["one", "two"] {
        let created = services
            .agent_create(
                ws.clone(),
                Some(format!("Tmp {tag}")),
                None,
                None,
                None,
                None,
                intent_core::AgentCreateExtra::default(),
            )
            .await
            .expect("create agent");
        let agent_id = AgentId::from(created["agent"]["id"].as_str().unwrap());
        let report = launch(
            &manager,
            &provider,
            &ws,
            &agent_id,
            &cwd,
            scratch.path(),
            &format!("{tag}-1"),
        )
        .await;
        agents.push((agent_id, report));
    }

    for (_, report) in &agents {
        let raw = &report.raw;
        let dir = report.tmpdir.to_str().unwrap();
        assert_eq!(report.tmpdir.parent(), Some(tmp_root.as_path()), "{raw}");
        assert_eq!(raw["TMP"], dir, "{raw}");
        assert_eq!(raw["TEMP"], dir, "{raw}");
        assert_eq!(raw["osTmpdir"], dir, "Node honors TMPDIR: {raw}");
        let made = raw["mktemp"].as_str().expect("mktemp ran");
        assert!(
            Path::new(made).starts_with(&report.tmpdir),
            "mktemp honors TMPDIR: {raw}"
        );
        assert!(report.tmpdir.is_dir(), "folder exists while the agent runs");
    }
    let (first_id, first) = &agents[0];
    let (_, second) = &agents[1];
    assert_ne!(first.tmpdir, second.tmpdir, "agents never share a temp dir");
    let first_file = PathBuf::from(first.raw["mktemp"].as_str().unwrap());

    // The idle reaper kills both processes; their folders and files stay.
    assert_eq!(
        manager.reap_idle_older_than(Duration::ZERO).await,
        2,
        "idle reaper evicts both agents"
    );
    assert!(first_file.is_file(), "a reaped agent keeps its temp files");
    assert!(second.tmpdir.is_dir(), "a reaped agent keeps its folder");

    // The next turn relaunches the agent into the same folder.
    let relaunched = launch(
        &manager,
        &provider,
        &ws,
        first_id,
        &cwd,
        scratch.path(),
        "one-2",
    )
    .await;
    assert_eq!(
        relaunched.tmpdir, first.tmpdir,
        "a relaunched agent gets its previous folder"
    );
    assert!(
        first_file.is_file(),
        "the previous process's file survives the relaunch"
    );

    // The sweep reclaims the idle agent's folder, never a live one's.
    intent_core::sweep_agent_tmp_root(&tmp_root, Duration::ZERO).expect("sweep");
    assert!(
        first_file.is_file(),
        "the sweep skips a live agent's folder"
    );
    assert!(
        !second.tmpdir.exists(),
        "the sweep reclaims an idle agent's folder: {}",
        second.tmpdir.display()
    );

    // Stopping (like the idle reaper) keeps the folder; deleting the agent
    // removes it.
    assert!(manager.stop(first_id).await, "first agent stopped");
    assert!(first_file.is_file(), "a stopped agent keeps its temp files");
    services.attach_agent_manager(&manager);
    services
        .agent_delete(first_id.clone(), Some(ws.clone()))
        .await
        .expect("delete agent");
    assert!(
        !first.tmpdir.exists(),
        "deleting the agent removes its folder: {}",
        first.tmpdir.display()
    );

    manager.shutdown().await;
}
