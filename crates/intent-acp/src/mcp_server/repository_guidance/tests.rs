use super::*;

pub(crate) fn scope() -> ExecutionScope {
    ExecutionScope {
        daemon_id: "daemon-A".into(),
        authority_scope_id: "admitted-workspace-1".into(),
        authority_generation: 9_007_199_254_740_993,
    }
}

pub(crate) fn revision(sequence: u64) -> RepositoryContextRevision {
    RepositoryContextRevision::new("boot-A", sequence)
}

pub(crate) fn session(version: Option<&str>) -> AgentSession {
    let mut value = json!({
        "id":"agent-1","workspaceId":"workspace-1","name":"fixture",
        "status":"active","createdAt":"2026-09-27","updatedAt":"2026-09-27"
    });
    if let Some(version) = version {
        value["harnessVersion"] = json!(version);
    }
    serde_json::from_value(value).unwrap()
}

pub(crate) fn operation_result(id: u64) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":{
        "isError":false,"content":[{"type":"text","text":"Operation target: original project; remote A"}],
        "structuredContent":{"project":"original-project","remoteSourceSha":"A"}
    }})
}

fn output(candidate: GuidanceCandidate, connection: &ConnectionToken) -> Value {
    serde_json::from_str(
        &BridgeResponse {
            value: operation_result(1),
            guidance: Some(candidate),
            guidance_request: None,
        }
        .into_line(connection),
    )
    .unwrap()
}

#[test]
fn only_exact_current_typed_revision_can_append_and_never_changes_operation_identity() {
    let fence = RepositoryGuidanceFence::default();
    let lease = fence
        .replace_context(scope(), revision(9_007_199_254_740_993))
        .unwrap();
    let connection = ConnectionLifetime::new();
    for sequence in [9_007_199_254_740_992, 9_007_199_254_740_994] {
        let candidate = lease
            .current_candidate(scope(), revision(sequence), "must be omitted".into())
            .unwrap();
        assert_eq!(output(candidate, &connection.token()), operation_result(1));
    }
    let candidate = lease
        .current_candidate(
            scope(),
            revision(9_007_199_254_740_993),
            "current guidance".into(),
        )
        .unwrap();
    let mut result = output(candidate, &connection.token());
    assert_eq!(
        result["result"]["content"]
            .as_array_mut()
            .unwrap()
            .pop()
            .unwrap()["text"],
        "current guidance"
    );
    assert_eq!(result, operation_result(1));
    assert!(!lease.advance(scope(), revision(9_007_199_254_740_992)));
    assert!(fence
        .replace_context(scope(), revision(9_007_199_254_740_992))
        .is_none());
}

#[test]
fn replaced_scope_epoch_and_retired_leases_cannot_be_revived_by_late_candidates() {
    for replacement in [
        (
            ExecutionScope {
                daemon_id: "daemon-B".into(),
                ..scope()
            },
            revision(1),
        ),
        (
            ExecutionScope {
                authority_scope_id: "other-admitted-caller".into(),
                ..scope()
            },
            revision(1),
        ),
        (
            ExecutionScope {
                authority_generation: scope().authority_generation + 1,
                ..scope()
            },
            revision(1),
        ),
        (scope(), RepositoryContextRevision::new("boot-B", 1)),
    ] {
        let fence = RepositoryGuidanceFence::default();
        let old = fence.replace_context(scope(), revision(12)).unwrap();
        let candidate = old
            .current_candidate(scope(), revision(12), "old guidance".into())
            .unwrap();
        let current = fence
            .replace_context(replacement.0.clone(), replacement.1.clone())
            .unwrap();
        assert!(!old.advance(scope(), revision(u64::MAX)));
        assert!(!current.advance(scope(), revision(u64::MAX)));
        let connection = ConnectionLifetime::new();
        assert_eq!(output(candidate, &connection.token()), operation_result(1));
        let current_candidate = current
            .current_candidate(replacement.0, replacement.1, "current".into())
            .unwrap();
        fence.retire();
        assert_eq!(
            output(current_candidate, &connection.token()),
            operation_result(1)
        );
    }
}

#[test]
fn unavailable_context_retires_targets_and_accepts_only_unstamped_guidance() {
    let fence = RepositoryGuidanceFence::default();
    let old = fence.replace_context(scope(), revision(1)).unwrap();
    let candidate = old
        .current_candidate(scope(), revision(1), "old target".into())
        .unwrap();
    let unavailable = fence.unavailable_context().unwrap();
    let connection = ConnectionLifetime::new();
    assert_eq!(output(candidate, &connection.token()), operation_result(1));
    assert!(!unavailable.advance(scope(), revision(2)));
    let wrong_kind = unavailable
        .current_candidate(scope(), revision(1), "old target".into())
        .unwrap();
    assert_eq!(output(wrong_kind, &connection.token()), operation_result(1));
    let candidate = unavailable
        .unavailable_candidate("Context unavailable; no previous target".into())
        .unwrap();
    let result = output(candidate, &connection.token());
    assert_eq!(
        result["result"]["content"][1]["text"],
        "Context unavailable; no previous target"
    );
}

#[test]
fn connection_retirement_and_budget_checks_suppress_only_guidance() {
    let fence = RepositoryGuidanceFence::default();
    let lease = fence.replace_context(scope(), revision(1)).unwrap();
    assert!(lease
        .current_candidate(scope(), revision(1), String::new())
        .is_none());
    assert!(lease
        .current_candidate(scope(), revision(1), "é".repeat(4097))
        .is_none());
    let candidate = lease
        .current_candidate(scope(), revision(1), "é".repeat(4096))
        .unwrap();
    let connection = ConnectionLifetime::new();
    let token = connection.token();
    drop(connection);
    assert_eq!(output(candidate, &token), operation_result(1));
}

#[derive(Default)]
struct InertSource(std::sync::atomic::AtomicUsize);
impl RepositoryGuidanceSource for InertSource {
    fn prepare<'a>(
        &'a self,
        _: &'a WorkspaceId,
        _: &'a Caller,
        _: &'a RepositoryGuidanceFence,
    ) -> Pin<Box<dyn Future<Output = Option<GuidanceCandidate>> + Send + 'a>> {
        self.0.fetch_add(1, AtomicOrdering::Relaxed);
        Box::pin(async { None })
    }
}

#[test]
fn immutable_session_gating_does_not_consult_features_or_parent_stamp() {
    let workspace = WorkspaceId::from("workspace-1");
    let caller = AgentId::from("agent-1");
    for version in [
        None,
        Some(""),
        Some("1.0"),
        Some("1.1"),
        Some("2.0"),
        Some("2.1"),
        Some("2.2"),
        Some("2.3"),
        Some("2.4"),
        Some("2.5"),
        Some("2.6"),
        Some("2.7"),
        Some("2.8"),
        Some("2.9"),
        Some("3"),
        Some("3.0.0"),
        Some(" 3.0"),
        Some("3.0\n"),
        Some("unknown"),
        Some("3.1"),
    ] {
        assert!(
            GuidanceBinding::new(
                &session(version),
                &workspace,
                Some(&caller),
                Arc::new(InertSource::default())
            )
            .is_none(),
            "{version:?}"
        );
    }
    for features in [
        None,
        Some(json!({"stateSnapshot":false})),
        Some(json!({"stateSnapshot":true})),
    ] {
        let mut session = session(Some("3.0"));
        session.harness_features = features;
        session.parent_agent_id = Some("old-parent".into());
        assert!(GuidanceBinding::new(
            &session,
            &workspace,
            Some(&caller),
            Arc::new(InertSource::default())
        )
        .is_some());
        assert!(
            GuidanceBinding::new(&session, &workspace, None, Arc::new(InertSource::default()))
                .is_none()
        );
        assert!(GuidanceBinding::new(
            &session,
            &WorkspaceId::from("foreign"),
            Some(&caller),
            Arc::new(InertSource::default())
        )
        .is_none());
        session.retired_at = Some("retired".into());
        assert!(GuidanceBinding::new(
            &session,
            &workspace,
            Some(&caller),
            Arc::new(InertSource::default())
        )
        .is_none());
    }
}

#[tokio::test]
async fn unbound_or_changed_caller_is_not_admitted_to_the_optional_source() {
    let workspace = WorkspaceId::from("workspace-1");
    let caller = AgentId::from("agent-1");
    let source = Arc::new(InertSource::default());
    let binding = GuidanceBinding::new(
        &session(Some("3.0")),
        &workspace,
        Some(&caller),
        source.clone(),
    )
    .unwrap();
    assert!(binding.capture(&workspace, Some(&caller)).is_none());
    let foreign = Caller::Agent {
        agent_id: "different-agent".into(),
    };
    assert!(intent_core::with_caller(foreign, async {
        binding.capture(&workspace, Some(&caller))
    })
    .await
    .is_none());
    assert_eq!(source.0.load(AtomicOrdering::Relaxed), 0);
}

#[test]
fn qualified_evidence_cannot_escape_legacy_serialization_or_preparation_helpers() {
    let fence = RepositoryGuidanceFence::default();
    let lease = fence.replace_context(scope(), revision(1)).unwrap();
    let lifetime = ConnectionLifetime::new();
    for prepared in [false, true] {
        let candidate = lease
            .current_candidate(scope(), revision(1), "private optional text".into())
            .unwrap()
            .with_optional_evidence(McpOptionalEvidence::new(Arc::new(())));
        let response = BridgeResponse {
            value: operation_result(1),
            guidance: Some(candidate),
            guidance_request: None,
        };
        let line = if prepared {
            response.prepare_line().into_line(&lifetime.token())
        } else {
            response.into_line(&lifetime.token())
        };
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap(),
            operation_result(1)
        );
    }
}

#[test]
fn prepared_optional_variants_only_downgrade_after_evidence_is_separated() {
    let fence = RepositoryGuidanceFence::default();
    let lease = fence.replace_context(scope(), revision(1)).unwrap();
    let lifetime = ConnectionLifetime::new();
    for explicit_base in [false, true] {
        let candidate = lease
            .current_candidate(scope(), revision(1), "qualified optional fixture".into())
            .unwrap()
            .with_optional_evidence(McpOptionalEvidence::new(Arc::new(())));
        let response = BridgeResponse {
            value: operation_result(1),
            guidance: Some(candidate),
            guidance_request: None,
        };
        let (packet, evidence) = response.prepare_delivery_line();
        assert!(evidence.is_some());
        // This is only an encoding/downgrade unit test, not an authority grant.
        let packet = if explicit_base {
            packet.without_guidance()
        } else {
            assert!(lease.advance(scope(), revision(2)));
            packet
        };
        let line = packet.into_line(&lifetime.token());
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap(),
            operation_result(1)
        );
    }
}

#[tokio::test]
async fn qualified_unpolled_delivery_never_starts_source_or_optional_leaf() {
    use crate::mcp_server::private_results::tests::{optional, Api};
    use std::sync::atomic::Ordering;
    let state = optional::State::new();
    let server = optional::server(Arc::new(Api::new()), state.clone(), false);
    let response = optional::response(&server, optional::READ).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let life = ConnectionLifetime::new();
    let token = life.token();
    let pending = response.enqueue(tx, &token);
    drop(pending);
    assert_eq!(state.capture.load(Ordering::SeqCst), 0);
    assert_eq!(state.constructor.load(Ordering::SeqCst), 0);
    assert_eq!(state.leaves_dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn qualified_mode_is_fixed_at_binding_before_source_preparation() {
    use crate::mcp_server::private_results::tests::{optional, Api};
    use crate::mcp_server::WorkspaceMcpServer;
    use std::sync::atomic::Ordering;
    let state = optional::State::new();
    let server = WorkspaceMcpServer::new(Arc::new(Api::new()), "workspace-1".into())
        .with_caller_agent_id(Some("agent-1".into()))
        .with_repository_guidance(
            &session(Some("3.0")),
            Arc::new(optional::Source(state.clone())),
        );
    state.qualified.store(false, Ordering::SeqCst);
    let (_, value) =
        optional::deliver(optional::response(&server, "return 'ordinary';").await).await;
    assert!(value.to_string().contains("ordinary"));
    assert!(!optional::has_guidance(&value));
    assert_eq!(state.qualifications.load(Ordering::SeqCst), 1);
    assert_eq!(state.constructor.load(Ordering::SeqCst), 0);
}
