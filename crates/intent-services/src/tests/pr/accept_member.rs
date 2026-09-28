//! The renderer's in-band accept-changes result must retain execution policy.
use super::*;
use intent_core::{with_caller, Caller, HostRole, Principal, PrincipalId, WorkspaceRole};
use std::sync::atomic::Ordering;

async fn member(svc: &Services, retained_guest: Option<&WorkspaceId>) -> Caller {
    let person = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    svc.store.upsert_principal(&person).await.unwrap();
    sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
        .bind(person.id.as_str())
        .bind(now_iso())
        .execute(svc.store.write_pool())
        .await
        .unwrap();
    if let Some(ws) = retained_guest {
        svc.store
            .add_workspace_member(ws, &person.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    Caller::Wire {
        principal_id: person.id,
        // A previously admitted guest gains authority from the durable row.
        host_role: if retained_guest.is_some() {
            HostRole::Guest
        } else {
            HostRole::Member
        },
    }
}

fn take_invalidation(svc: &Services) -> bool {
    let notification = svc.execution_invalidation.notified();
    tokio::pin!(notification);
    std::future::Future::poll(
        notification.as_mut(),
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
    )
    .is_ready()
}

fn assert_safe_failure(result: &serde_json::Value) {
    assert_eq!(result["success"], false, "{result}");
    let failed = result["steps"].as_array().unwrap().last().unwrap();
    assert_eq!(failed["id"], "create-pr");
    assert_eq!(failed["status"], "failed");
    assert_eq!(failed["error"], result["error"]);
    let message = result["error"].as_str().unwrap();
    for semantic in ["connected host", "Git authorization", "owner", "retry"] {
        assert!(message.contains(semantic), "{result}");
    }
    assert!(!result.to_string().contains("private-"), "{result}");
    assert!(result["executionAuthorization"].is_null());
}

#[intent_test_macros::daemon_test]
async fn member_accept_changes_classifies_in_band_auth_and_preserves_owner_errors() {
    type Case = (fn() -> ScError, bool);
    let cases: [Case; 6] = [
        (|| ScError::NotConfigured("private-missing".into()), true),
        (|| ScError::Auth("private-rejected".into()), true),
        (
            || ScError::Auth("insufficient_scope private-scope".into()),
            true,
        ),
        (|| ScError::Api("ordinary server failure".into()), false),
        (|| ScError::RateLimited("quota exhausted".into()), false),
        (|| ScError::NotFound("missing repository".into()), false),
    ];
    for retained_guest in [false, true] {
        let (_db, _work, _bare, svc, ws, _) = ac_setup(StubForge::default()).await;
        let member = member(&svc, retained_guest.then_some(&ws)).await;
        let owner = Caller::Wire {
            principal_id: svc.store.get_primary_principal().await.unwrap().id,
            host_role: HostRole::Owner,
        };
        for (error, authorization) in cases {
            let forge = Arc::new(StubForge {
                create_pr_error: Some(error),
                ..Default::default()
            });
            let svc = svc.clone().with_source_control(forge.clone());
            let invoke = || svc.accept_changes_execute(ws.clone(), json!({"action":"create-pr"}));
            let legacy = with_caller(owner.clone(), invoke()).await.unwrap();
            let expected = match error() {
                ScError::RateLimited(message) => format!("source control rate limited: {message}"),
                e => format!("internal error: {e}"),
            };
            assert_eq!(legacy["error"], expected);
            assert_eq!(legacy["steps"][0]["error"], expected);
            let _ = take_invalidation(&svc);
            let result = with_caller(member.clone(), invoke()).await.unwrap();
            assert_eq!(forge.create_pr_calls.load(Ordering::SeqCst), 2);
            if authorization {
                assert_safe_failure(&result);
                assert!(take_invalidation(&svc));
            } else {
                assert_eq!(result, legacy);
                assert!(!take_invalidation(&svc));
            }
            assert!(svc
                .store
                .get_workspace(&ws)
                .await
                .unwrap()
                .pr_number
                .is_none());
        }
    }
}

#[intent_test_macros::daemon_test]
async fn member_accept_changes_failed_pr_preserves_commit_push_and_allows_recovery() {
    let forge = StubForge {
        create_pr_error: Some(|| ScError::Auth("private-rejected".into())),
        ..Default::default()
    };
    let (_db, _work, bare, svc, ws, work) = ac_setup(forge).await;
    let member = member(&svc, None).await;
    std::fs::write(work.join("feature.txt"), "member work\n").unwrap();
    let result = with_caller(
        member.clone(),
        svc.accept_changes_execute(
            ws.clone(),
            json!({
                "action":"commit", "commitMessage":"feat: member work",
                "options":{"stageUnstaged":true,"pushAfterCommit":true,"createPRAfterPush":true}
            }),
        ),
    )
    .await
    .unwrap();
    assert_safe_failure(&result);
    assert_eq!(result["steps"][0]["id"], "commit");
    assert_eq!(result["steps"][0]["status"], "completed");
    assert_eq!(result["steps"][1]["id"], "push");
    assert_eq!(result["steps"][1]["status"], "completed");
    let hash = result["result"]["commitHash"].as_str().unwrap();
    assert_eq!(result["result"]["pushedSha"], hash);
    let remote = git2::Repository::open_bare(bare.path()).unwrap();
    assert_eq!(
        remote
            .find_reference("refs/heads/feature")
            .unwrap()
            .target()
            .unwrap()
            .to_string(),
        hash
    );
    let local = git2::Repository::open(&work).unwrap();
    let commit = local.head().unwrap().peel_to_commit().unwrap();
    assert_eq!(commit.id().to_string(), hash);
    assert_eq!(commit.author().name().unwrap(), "Tester");
    assert_eq!(commit.committer().email().unwrap(), "t@e.dev");
    let recovered = svc.with_source_control(Arc::new(StubForge::default()));
    let created = with_caller(
        member.clone(),
        recovered.accept_changes_execute(ws.clone(), json!({"action":"create-pr"})),
    )
    .await
    .unwrap();
    assert_eq!(created["success"], true, "{created}");
    assert_eq!(created["result"]["prNumber"], 7);
    let status = with_caller(member, recovered.accept_changes_get_status(ws))
        .await
        .unwrap();
    assert_eq!(status["existingPR"]["number"], 7);
    assert_eq!(status["isPushed"], true);
    assert_eq!(local.head().unwrap().target().unwrap().to_string(), hash);
}

#[intent_test_macros::daemon_test]
async fn member_accept_changes_keeps_workspace_authority_and_existing_guest_rights() {
    let (_db, _work, _bare, svc, ws, _) = ac_setup(StubForge::default()).await;
    let admitted = member(&svc, None).await;
    let Caller::Wire { principal_id, .. } = admitted.clone() else {
        unreachable!()
    };
    let forge = Arc::new(StubForge {
        create_pr_error: Some(|| ScError::Auth("private-rejected".into())),
        ..Default::default()
    });
    let svc = svc.with_source_control(forge.clone());
    for target in [WorkspaceId::new(), WorkspaceId::chief()] {
        assert!(matches!(
            with_caller(
                admitted.clone(),
                svc.accept_changes_execute(target, json!({"action":"create-pr"}))
            )
            .await,
            Err(Error::NotFound(_))
        ));
    }
    svc.store.remove_host_member(&principal_id).await.unwrap();
    for id in [principal_id.clone(), PrincipalId::new()] {
        let stale_or_unknown = Caller::Wire {
            principal_id: id,
            host_role: HostRole::Member,
        };
        assert!(matches!(
            with_caller(
                stale_or_unknown,
                svc.accept_changes_execute(ws.clone(), json!({"action":"create-pr"}))
            )
            .await,
            Err(Error::NotFound(_))
        ));
    }
    let guest = Caller::Wire {
        principal_id: principal_id.clone(),
        host_role: HostRole::Guest,
    };
    assert!(matches!(
        with_caller(
            guest.clone(),
            svc.accept_changes_execute(ws.clone(), json!({"action":"create-pr"}))
        )
        .await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(forge.create_pr_calls.load(Ordering::SeqCst), 0);
    assert!(!take_invalidation(&svc));
    // A scoped collaborator keeps the pre-existing accept-changes right and
    // legacy error contract; host membership is not silently granted.
    svc.store
        .add_workspace_member(&ws, &principal_id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let result = with_caller(
        guest,
        svc.accept_changes_execute(ws, json!({"action":"create-pr"})),
    )
    .await
    .unwrap();
    assert_eq!(
        result["error"],
        "internal error: source control auth error: private-rejected"
    );
    assert_eq!(result["steps"][0]["error"], result["error"]);
    assert_eq!(forge.create_pr_calls.load(Ordering::SeqCst), 1);
}
