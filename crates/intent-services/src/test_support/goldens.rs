/// `SQLite` db (plus its `.config.toml` sibling) inside an RAII temp dir; the
/// dir sweep on drop also covers the `-wal`/`-shm` sidecars.
pub(crate) struct TempDb {
    path: PathBuf,
    _dir: tempfile::TempDir,
}

impl TempDb {
    fn new() -> Self {
        let dir = crate::test_support::test_tempdir("intentd-goldens-");
        let path = dir.path().join("goldens.db");
        Self { path, _dir: dir }
    }
}

pub(crate) async fn setup() -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    // monorepo#3044: delegation requires a resolvable provider (no positional
    // fallback) — seed a configured default with a deterministic
    // `providers.paths` override so availability checks pass hermetically.
    let cfg = PathBuf::from(format!("{}.config.toml", tmp.path.display()));
    let registry = Arc::new(crate::SettingsRegistry::load(cfg).expect("load registry"));
    registry
        .apply(&[
            ("model.defaultProvider".into(), json!("auggie")),
            ("providers.paths".into(), json!({ "auggie": "/bin/sh" })),
            // Goldens assert the legacy immediate report wake — disable the
            // default 30s report debounce.
            ("agents.reportToParentDebounceSeconds".into(), json!(0)),
        ])
        .expect("seed default provider");
    let services = Services::new(store).with_settings_registry(registry);
    (tmp, services, ws)
}

pub(crate) fn pr_monitor_row() -> intent_core::PrMonitor {
    intent_core::PrMonitor {
        monitor_id: intent_core::PrMonitorId::from("prmon-1"),
        workspace_id: WorkspaceId::from("ws-1"),
        agent_id: AgentId::from("agent-pr"),
        repo_owner: "o".to_string(),
        repo_name: "r".to_string(),
        pr_number: 42,
        state: intent_core::PrMonitorState::Active,
        last_snapshot: None,
        baseline_snapshot: None,
        pending_changes: vec![],
        pending_since: None,
        last_change_at: None,
        last_polled_at: None,
        last_error: None,
        created_at: "2026-01-02T03:04:05Z".to_string(),
        updated_at: "2026-01-02T03:04:05Z".to_string(),
    }
}

/// Seed a bare agent session row owned by `ws` (mirrors the hook tests).
pub(crate) async fn seed_agent(svc: &Services, ws: &WorkspaceId, id: &AgentId) {
    let ts = now_iso();
    let session = intent_core::AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Builder".to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: intent_core::AgentStatus::Active,
        is_active: false,
        messages: vec![],
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
        notifications_muted: false,
    };
    svc.store()
        .insert_agent_session(&session)
        .await
        .expect("seed agent");
}

/// All persisted message texts for an agent (each message's text blocks
/// joined), oldest-first — the wake-delivery capture used by the goldens.
/// Wake persistence can lag the op return, so poll until at least `expected`
/// messages are present (generous deadline, monorepo#1358 precedent); on
/// timeout return whatever was seen and let the caller's assert report it.
pub(crate) async fn wake_texts_when(svc: &Services, id: &AgentId, expected: usize) -> Vec<String> {
    let deadline = std::time::Instant::now()
        + intentd_test_support::test_timeout(std::time::Duration::from_secs(10));
    loop {
        let session = svc.store().get_agent_session(id).await.expect("session");
        let texts: Vec<String> = session
            .messages
            .iter()
            .map(|m| {
                m.content
                    .as_array()
                    .map(|blocks| {
                        blocks
                            .iter()
                            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            })
            .collect();
        if texts.len() >= expected || std::time::Instant::now() >= deadline {
            return texts;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

pub(crate) const PREFIX: &str = "[SYSTEM NOTICE] This workspace was archived and has since been \
    unarchived. While it was archived, ";

pub(crate) const RE_ARM_HOOKS: &str =
    "ws.hook.get(hookId) recovers a hook's script for ws.hook.schedule";

pub(crate) const RE_ARM_MONITORS: &str = "ws.pr.monitor re-registers a PR";
