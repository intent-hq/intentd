//! A remote agent's `skill.list` and missing-skill notice (monorepo
//! `docs/protocol/methods/agent-aux.md` § Skills of a remote agent).
use std::sync::{Arc, Mutex};

use intent_core::node_link::MachineSkill;
use intent_core::remote_launch::{RemotePlacementView, REMOTE_PLACEMENT_METADATA_KEY};
use intent_core::{now_iso, with_caller, AgentId, AgentSession, AgentStatus, BoxFuture, Caller};
use serde_json::Value;

use super::leading_skill_name;
use crate::feature_ports::{MachineSkills, RemoteLaunchPort};
use crate::test_support::test_tempdir;
use crate::tests::setup;
use crate::Services;

struct SkillsPort {
    answer: MachineSkills,
    queries: Mutex<Vec<bool>>,
}

impl RemoteLaunchPort for SkillsPort {
    fn placements<'a>(
        &'a self,
        _svc: &'a Services,
    ) -> BoxFuture<'a, intent_core::Result<intent_core::placement::PlatformAvailability>> {
        Box::pin(async { Ok(intent_core::placement::PlatformAvailability::none(now_iso())) })
    }
    fn placement_help<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a intent_core::WorkspaceId,
        _render: intent_core::machine_selection::PlacementHelpRenderer,
    ) -> BoxFuture<'a, Option<String>> {
        Box::pin(async { None })
    }
    fn admit<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a intent_core::WorkspaceId,
        _placement: &'a intent_core::placement::AgentPlacement,
        _needs: &'a intent_core::remote_launch::RemoteLaunchNeeds,
    ) -> BoxFuture<'a, intent_core::Result<RemotePlacementView>> {
        Box::pin(async { Ok(view(None)) })
    }
    fn launch<'a>(
        &'a self,
        _svc: &'a Services,
        _request: intent_core::remote_launch::RemoteLaunchRequest,
    ) -> BoxFuture<'a, intent_core::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn runtime<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, intent_core::Result<Arc<crate::feature_ports::RemoteRuntime>>> {
        Box::pin(async { Err(intent_core::Error::InvalidParams("no runtime".into())) })
    }
    fn machine_skills<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
        query: bool,
    ) -> BoxFuture<'a, MachineSkills> {
        self.queries.lock().unwrap().push(query);
        let answer = self.answer.clone();
        Box::pin(async move { answer })
    }
}

fn skill(name: &str, scope: &str, location: &str) -> MachineSkill {
    MachineSkill {
        name: name.into(),
        description: format!("{name} ({scope})"),
        location: location.into(),
        resource_directory: None,
        scope: scope.into(),
        allowed_tools: None,
        compatibility: None,
    }
}

pub(crate) fn view(skills: Option<Vec<MachineSkill>>) -> RemotePlacementView {
    RemotePlacementView {
        node_id: "node-1".into(),
        lease_id: "lease-1".into(),
        node_name: "Mac Studio".into(),
        placement: intent_core::placement::AgentPlacement::default(),
        effective_isolation: intent_core::remote_launch::EffectiveIsolation::Pending,
        placement_error: None,
        base_commit: "base".into(),
        submodules: None,
        setup: None,
        setup_script: None,
        skills,
        shared_checkout_of: None,
        parent_work: None,
        launched_on: None,
        launched_machine_sign_in: None,
        cloud_start: None,
    }
}

pub(crate) fn session(ws: &intent_core::WorkspaceId, id: &AgentId) -> AgentSession {
    let ts = now_iso();
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Remote".to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: Some("auggie".to_string()),
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Pending,
        is_active: true,
        messages: Vec::new(),
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
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
        notifications_muted: false,
        pinned: false,
    }
}

/// A workspace whose checkout has project skills `proj` and `shared`, an
/// agent `remote` with `placed` (or a local one without), and `answer`
/// from its machine.
async fn fixture(
    placed: Option<RemotePlacementView>,
    answer: MachineSkills,
) -> (
    crate::tests::TempDb,
    Services,
    intent_core::WorkspaceId,
    AgentId,
    Arc<SkillsPort>,
    tempfile::TempDir,
) {
    let (tmp, svc, ws, _) = setup("").await;
    let port = Arc::new(SkillsPort {
        answer,
        queries: Mutex::new(Vec::new()),
    });
    let svc = svc.with_remote_launch_port(port.clone());
    let root = test_tempdir("remote-skills-project-");
    for name in ["proj", "shared"] {
        let dir = root.path().join(".agents/skills").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {name} (project)\n---\nBody\n"),
        )
        .unwrap();
    }
    let mut row = svc.store.get_workspace(&ws).await.unwrap();
    row.repository_path = Some(root.path().to_string_lossy().into_owned());
    svc.store.update_workspace(&row).await.unwrap();
    let id = AgentId::from("remote");
    svc.store
        .insert_agent_session(&session(&ws, &id))
        .await
        .unwrap();
    if let Some(placed) = placed {
        svc.store
            .set_agent_session_metadata_key_json(
                &ws,
                &id,
                REMOTE_PLACEMENT_METADATA_KEY,
                &serde_json::to_string(&placed).unwrap(),
                &now_iso(),
            )
            .await
            .unwrap();
    }
    (tmp, svc, ws, id, port, root)
}

async fn list(svc: &Services, ws: &intent_core::WorkspaceId, id: &AgentId) -> Value {
    with_caller(Caller::Daemon, svc.agent_skill_list(ws.clone(), id.clone()))
        .await
        .unwrap()
}

fn rows(listed: &Value) -> Vec<(String, String)> {
    listed["skills"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["name"].as_str().unwrap().to_owned(),
                s["description"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_local_agent_lists_the_workspace_skills_in_the_envelope() {
    let (_tmp, svc, ws, id, port, _root) = fixture(None, MachineSkills::Offline).await;
    let listed = list(&svc, &ws, &id).await;
    assert_eq!(
        rows(&listed),
        [
            ("proj".to_owned(), "proj (project)".to_owned()),
            ("shared".to_owned(), "shared (project)".to_owned())
        ]
    );
    assert!(listed.get("unavailable").is_none() && listed.get("stale").is_none());
    assert!(
        port.queries.lock().unwrap().is_empty(),
        "no machine is asked"
    );
}

#[tokio::test]
async fn before_its_first_start_the_machine_user_skills_merge_with_project_skills() {
    let answer = MachineSkills::Listed(vec![
        skill("mine", "user", "/Users/me/.claude/skills/mine/SKILL.md"),
        skill("shared", "user", "/Users/me/.claude/skills/shared/SKILL.md"),
    ]);
    let (_tmp, svc, ws, id, port, _root) = fixture(Some(view(None)), answer).await;
    let listed = list(&svc, &ws, &id).await;
    assert_eq!(
        rows(&listed),
        [
            ("mine".to_owned(), "mine (user)".to_owned()),
            ("proj".to_owned(), "proj (project)".to_owned()),
            ("shared".to_owned(), "shared (project)".to_owned()),
        ],
        "a project skill wins a name collision"
    );
    assert!(listed.get("unavailable").is_none(), "{listed}");
    assert_eq!(*port.queries.lock().unwrap(), [true], "asked once");
}

#[tokio::test]
async fn an_older_machine_offers_no_skills_and_machine_update_required() {
    let (_tmp, svc, ws, id, _port, _root) =
        fixture(Some(view(None)), MachineSkills::UpdateRequired).await;
    let listed = list(&svc, &ws, &id).await;
    assert!(
        rows(&listed).is_empty(),
        "an older machine gives its agent no catalog: {listed}"
    );
    assert_eq!(listed["unavailable"], "machine-update-required");
}

#[tokio::test]
async fn an_offline_machine_without_a_report_lists_project_skills() {
    let (_tmp, svc, ws, id, _port, _root) = fixture(Some(view(None)), MachineSkills::Offline).await;
    let listed = list(&svc, &ws, &id).await;
    assert_eq!(rows(&listed).len(), 2, "{listed}");
    assert_eq!(listed["unavailable"], "machine-offline");
}

#[tokio::test]
async fn a_reported_catalog_is_served_and_stale_while_the_machine_is_offline() {
    let reported = vec![skill(
        "deploy",
        "project",
        "/machine/checkout/.agents/skills/deploy/SKILL.md",
    )];
    for (answer, stale) in [
        (MachineSkills::Reachable, false),
        (MachineSkills::Offline, true),
    ] {
        let (_tmp, svc, ws, id, port, _root) =
            fixture(Some(view(Some(reported.clone()))), answer).await;
        let listed = list(&svc, &ws, &id).await;
        assert_eq!(
            rows(&listed),
            [("deploy".to_owned(), "deploy (project)".to_owned())]
        );
        assert_eq!(listed["skills"][0]["location"], reported[0].location);
        assert_eq!(listed.get("stale").is_some(), stale, "{listed}");
        assert_eq!(
            *port.queries.lock().unwrap(),
            [false],
            "a reachability check only"
        );
    }
}

#[tokio::test]
async fn an_agent_of_another_workspace_is_invalid() {
    let (_tmp, svc, _ws, id, _port, _root) = fixture(None, MachineSkills::Offline).await;
    let other = intent_core::WorkspaceId::new();
    let error = with_caller(Caller::Daemon, svc.agent_skill_list(other, id))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            intent_core::Error::NotFound(_) | intent_core::Error::InvalidParams(_)
        ),
        "{error:?}"
    );
}

#[test]
fn a_leading_slash_word_names_a_skill() {
    for (text, expected) in [
        ("/deploy", Some("deploy")),
        ("  /deploy now", Some("deploy")),
        ("/my-skill:v2\nmore", Some("my-skill:v2")),
        ("/usr/bin/env", None),
        ("/", None),
        ("please /deploy", None),
        ("/ deploy", None),
    ] {
        assert_eq!(leading_skill_name(text), expected, "{text:?}");
    }
}

async fn notices(svc: &Services, id: &AgentId) -> Vec<intent_core::AgentMessage> {
    svc.store
        .get_agent_messages(id, None)
        .await
        .unwrap()
        .into_iter()
        .filter(|m| {
            m.metadata
                .as_ref()
                .is_some_and(|meta| meta["type"] == "skill_unavailable")
        })
        .collect()
}

#[tokio::test]
async fn an_unknown_skill_gets_a_notice_and_a_known_one_does_not() {
    let reported = vec![skill("deploy", "user", "/m/deploy/SKILL.md")];
    let (_tmp, svc, ws, id, _port, _root) =
        fixture(Some(view(Some(reported))), MachineSkills::Reachable).await;
    svc.note_unavailable_skill(&ws, &id, "/deploy now").await;
    assert!(notices(&svc, &id).await.is_empty());
    svc.note_unavailable_skill(&ws, &id, "/review this").await;
    let found = notices(&svc, &id).await;
    assert_eq!(found.len(), 1);
    let meta = found[0].metadata.clone().unwrap();
    assert_eq!(meta["skill"], "review");
    assert_eq!(meta["machine"], "Mac Studio");
    assert_eq!(found[0].role, "system");
    let text = serde_json::to_string(&found[0].content).unwrap();
    assert!(text.contains("**/review** isn't on Mac Studio."), "{text}");
}

#[tokio::test]
async fn no_notice_before_the_machine_reported_or_for_a_local_agent() {
    for placed in [Some(view(None)), None] {
        let (_tmp, svc, ws, id, _port, _root) = fixture(placed, MachineSkills::Reachable).await;
        svc.note_unavailable_skill(&ws, &id, "/review").await;
        assert!(notices(&svc, &id).await.is_empty());
    }
}

fn row(role: &str, text: &str, metadata: Option<&Value>) -> intent_core::AgentMessage {
    serde_json::from_value(serde_json::json!({
        "id": "m1",
        "agentId": "remote",
        "seq": 1,
        "role": role,
        "contentBlocks": [{ "type": "text", "text": text }],
        "metadata": metadata,
        "timestamp": "2026-11-05T00:00:00Z",
    }))
    .unwrap()
}

#[test]
fn only_a_persons_slash_message_is_checked() {
    use super::person_skill_text;
    assert_eq!(
        person_skill_text(&row("user", "/deploy now", None)),
        Some("/deploy now")
    );
    assert_eq!(person_skill_text(&row("user", "deploy now", None)), None);
    assert_eq!(person_skill_text(&row("assistant", "/deploy", None)), None);
    assert_eq!(person_skill_text(&row("system", "/deploy", None)), None);
    let from_agent = serde_json::json!({ "fromAgentId": "agent-2" });
    assert_eq!(
        person_skill_text(&row("user", "/deploy", Some(&from_agent))),
        None
    );
    let system = serde_json::json!({ "source": "system" });
    assert_eq!(
        person_skill_text(&row("user", "/deploy", Some(&system))),
        None
    );
}
