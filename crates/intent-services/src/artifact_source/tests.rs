//! Real indexed source and membership fixtures; no artifact allocation authority.
use super::*;
use intent_core::{note_artifact::request::Primitive, Caller, NoteId, WorkspaceId};
use serde_json::{json, Value};

pub(super) struct ReadPark {
    pub after: bool,
    pub entered: Notify,
    pub release: Notify,
}

async fn page(
    services: &Services,
    ws: &WorkspaceId,
    note: &NoteId,
    principal: &str,
    value: Value,
) -> Value {
    services
        .store
        .read_note_page(
            &ws.0,
            &note.0,
            principal,
            serde_json::from_value(value).unwrap(),
            &json!("source-fixture"),
        )
        .await
        .unwrap()
}

async fn binding_as(
    services: &Services,
    ws: &WorkspaceId,
    note: &NoteId,
    principal: &str,
) -> CanonicalSourceBinding {
    let first = page(
        services,
        ws,
        note,
        principal,
        json!({"kind":"source","maxSourceBytes":128,"maxWireBytes":8192}),
    )
    .await;
    let context = page(services, ws, note, principal, json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128,"maxWireBytes":8192})).await;
    let owner = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["nodeType"] == "diffBlock" || v["nodeType"] == "mermaidBlock")
        .unwrap();
    let root = page(
        services,
        ws,
        note,
        principal,
        json!({"kind":"metadata","ref":owner["attributesRef"],"maxItems":1,"maxWireBytes":8192}),
    )
    .await;
    let fields = page(services, ws, note, principal, json!({"kind":"metadata","ref":root["items"][0]["childrenRef"],"maxItems":128,"maxWireBytes":8192})).await;
    let code = fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["key"] == "code")
        .unwrap();
    CanonicalSourceBinding {
        scope: serde_json::from_value(first["scope"].clone()).unwrap(),
        snapshot_id: first["snapshotId"].as_str().unwrap().into(),
        source_revision: first["sourceRevision"].as_str().unwrap().into(),
        primitive: if owner["nodeType"] == "diffBlock" {
            Primitive::Diff
        } else {
            Primitive::Mermaid
        },
        owner_ref: owner["nativeRef"].as_str().unwrap().into(),
        source_ref: code["valueRef"].as_str().unwrap().into(),
    }
}

async fn binding(services: &Services, ws: &WorkspaceId, note: &NoteId) -> CanonicalSourceBinding {
    binding_as(services, ws, note, "daemon").await
}

#[tokio::test]
async fn canonical_source_session_opens_real_indexed_binding_without_arena() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff title\n-old\n+é😀\n```").await;
    let binding = binding(&services, &ws, &note).await;
    let grant = services
        .store
        .authorize_canonical_source(&ws.0, "daemon", &binding)
        .await
        .unwrap();
    assert_eq!(grant.scope, binding.scope);
    let opened = intent_core::with_caller(
        Caller::Daemon,
        services.open_canonical_source_session(binding),
    )
    .await;
    assert!(
        opened.is_ok(),
        "real source-only session must open without an artifact arena"
    );
    drop(opened);
    drop(grant);
    services.store.close().await;
}

fn request(reference: &str, kind: &str, cursor: Option<&str>) -> NotePageRequest {
    let mut value = json!({"kind":kind,"maxItems":1,"maxWireBytes":8192});
    value[if kind == "context" {
        "contextRef"
    } else {
        "ref"
    }] = json!(reference);
    if let Some(cursor) = cursor {
        value["cursor"] = json!(cursor);
    }
    serde_json::from_value(value).unwrap()
}
async fn read(
    session: &CanonicalSourceSession,
    reference: &str,
    kind: &str,
    cursor: Option<&str>,
) -> Value {
    let id = json!("escaped\"source-id");
    let page = session
        .read(request(reference, kind, cursor), id.clone())
        .unwrap()
        .await
        .unwrap();
    assert!(
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"result":page}))
            .unwrap()
            .len()
            <= 8192
    );
    page
}
async fn code_start(session: &CanonicalSourceSession, binding: &CanonicalSourceBinding) {
    let owner = read(session, &binding.owner_ref, "context", None).await;
    assert!(session
        .read(request(&binding.owner_ref, "context", None), json!(1))
        .is_err());
    let attrs = read(
        session,
        owner["items"][0]["attributesRef"].as_str().unwrap(),
        "metadata",
        None,
    )
    .await;
    let fields = attrs["items"][0]["childrenRef"].as_str().unwrap();
    let mut cursor = None;
    for _ in 0..16 {
        let page = read(session, fields, "metadata", cursor.as_deref()).await;
        if page["items"][0]["key"] == "code" {
            assert_eq!(page["items"][0]["valueRef"], binding.source_ref);
            return;
        }
        cursor = Some(page["nextCursor"].as_str().unwrap().to_owned());
    }
    panic!("fixture attributes missing code");
}
async fn open(services: &Services, binding: CanonicalSourceBinding) -> CanonicalSourceSession {
    intent_core::with_caller(
        Caller::Daemon,
        services.open_canonical_source_session(binding),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn canonical_source_session_streams_exact_scalar_empty_and_encoded_attributes() {
    let long = format!("-old\n+{}END", "é😀e\u{301}\"\t".repeat(1800));
    for (source, expected) in [
        ("```diff title\n-old\n+é😀\n```".into(), "-old\n+é😀".into()),
        (
            "<div data-type=\"diff-block\" data-diff-code=\"\"></div>".into(),
            String::new(),
        ),
        (
            "<div data-type=\"diff-block\" data-diff-code=\"YWJjCg==\"></div>".into(),
            "YWJjCg==".into(),
        ),
        (
            "```mermaid title\ngraph TD\n A[Alpha]\n```".into(),
            "graph TD\n A[Alpha]".into(),
        ),
        (format!("```diff title\n{long}\n```"), long),
    ] {
        let (_tmp, services, ws, note) = crate::tests::setup(&source).await;
        let binding = binding(&services, &ws, &note).await;
        let session = open(&services, binding.clone()).await;
        // The discovery reads only 128 source bytes, even for the long value.
        code_start(&session, &binding).await;
        let mut next = binding.source_ref.clone();
        let mut text = String::new();
        let mut pages = 0;
        loop {
            let page = read(&session, &next, "context", None).await;
            let item = &page["items"][0];
            assert_eq!(
                item["offset"].as_u64().unwrap(),
                text.encode_utf16().count() as u64
            );
            text.push_str(item["text"].as_str().unwrap());
            pages += 1;
            if item["nextRef"].is_null() {
                break;
            }
            assert!(session
                .read(request(&next, "context", None), json!(1))
                .is_err());
            next = item["nextRef"].as_str().unwrap().into();
        }
        assert_eq!(text, expected);
        if expected.len() > 8192 {
            assert!(pages > 1);
        }
        assert!(session
            .read(request(&next, "context", None), json!(1))
            .is_err());
        session.close().await.unwrap();
        session.close().await.unwrap();
        assert!(session.inner.state.lock().unwrap().grant.is_none());
        drop(session);
        assert_eq!(services.canonical_source_admission.available_permits(), 256);
        services.store.close().await;
    }
}

#[tokio::test]
async fn canonical_source_session_rejects_unrelated_refs_and_mutated_source() {
    let (_tmp, services, ws, note) =
        crate::tests::setup("```diff title\n+one\n```\n\n```diff title\n+two\n```").await;
    let binding = binding(&services, &ws, &note).await;
    let session = open(&services, binding.clone()).await;
    // The code ref is valid in this exact snapshot, but not the admitted next step.
    assert!(session
        .read(request(&binding.source_ref, "context", None), json!(1))
        .is_err());
    let mut wrong = binding.clone();
    wrong.owner_ref = binding.source_ref.clone();
    assert!(intent_core::with_caller(
        Caller::Daemon,
        services.open_canonical_source_session(wrong)
    )
    .await
    .is_err());
    let mut note_value = services.store.get_note(&ws, &note).await.unwrap();
    note_value.content = "```diff title\n+changed\n```".into();
    services.store.update_note(&note_value).await.unwrap();
    assert!(session
        .read(request(&binding.owner_ref, "context", None), json!(1))
        .unwrap()
        .await
        .is_err());
    session.close().await.unwrap();
    drop(session);
    services.store.close().await;
}

#[tokio::test]
async fn canonical_source_session_close_and_unpolled_cancel_retain_pending_owner() {
    for cancel in [false, true] {
        let (_tmp, services, ws, note) = crate::tests::setup("```diff title\n+one\n```").await;
        services.canonical_source_admission.close();
        let mut services = services;
        services.canonical_source_admission = Arc::new(tokio::sync::Semaphore::new(1));
        let binding = binding(&services, &ws, &note).await;
        let session = open(&services, binding.clone()).await;
        let park = Arc::new(ReadPark {
            after: false,
            entered: Notify::new(),
            release: Notify::new(),
        });
        *session.inner.park.lock().unwrap() = Some(park.clone());
        let pending = session
            .read(request(&binding.owner_ref, "context", None), json!(1))
            .unwrap();
        park.entered.notified().await;
        assert!(session
            .read(request(&binding.owner_ref, "context", None), json!(1))
            .is_err());
        assert!(session.inner.state.lock().unwrap().grant.is_some());
        let mut pending = Some(pending);
        if cancel {
            // Cancellation alone must revoke even an unpolled returned future.
            drop(pending.take());
            assert!(session.inner.state.lock().unwrap().closed);
        }
        let wait = session.close();
        assert!(session.inner.state.lock().unwrap().closed);
        assert!(session.inner.state.lock().unwrap().grant.is_some());
        assert_eq!(services.canonical_source_admission.available_permits(), 0);
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), &mut wait)
                .await
                .is_err()
        );
        park.release.notify_one();
        wait.await.unwrap();
        if let Some(pending) = pending {
            assert!(pending.await.is_err());
        }
        assert!(session.inner.state.lock().unwrap().grant.is_none());
        session.close().await.unwrap();
        // Closed session object still owns its finite admission slot.
        assert_eq!(services.canonical_source_admission.available_permits(), 0);
        drop(session);
        assert_eq!(services.canonical_source_admission.available_permits(), 1);
        services.store.close().await;
    }
}

#[tokio::test]
async fn canonical_source_session_original_deadline_is_not_extended_by_pin() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff title\n+one\n```").await;
    let binding = binding(&services, &ws, &note).await;
    let session = open(&services, binding.clone()).await;
    let original = session.inner.deadline;
    // Clock injection exercises the exact original signed deadline; it does not
    // mutate the Store snapshot or claim that its monotonic timer elapsed.
    *session.inner.now.lock().unwrap() = Some(original);
    assert!(matches!(
        session.read(request(&binding.owner_ref, "context", None), json!(1)),
        Err(Error::NotePage(NotePageError::Expired))
    ));
    assert_eq!(session.inner.deadline, original);
    session.close().await.unwrap();
    drop(session);
    services.store.close().await;
}

async fn member(
    services: &Services,
    workspace: &WorkspaceId,
) -> (Caller, intent_core::PrincipalId) {
    let id = intent_core::PrincipalId::new();
    services
        .store
        .upsert_principal(&intent_core::Principal {
            id: id.clone(),
            github_user_id: None,
            login: Some("artifact-recovery-guest".into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
            identity: None,
        })
        .await
        .unwrap();
    services
        .store
        .add_workspace_member(workspace, &id, intent_core::WorkspaceRole::Collaborator)
        .await
        .unwrap();
    (
        Caller::Wire {
            principal_id: id.clone(),
            host_role: intent_core::HostRole::Guest,
        },
        id,
    )
}

#[tokio::test]
async fn canonical_source_session_rechecks_captured_member_for_actual_read_outcomes() {
    for read_error in [false, true] {
        let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
        let daemon_binding = binding(&services, &ws, &note).await;
        let (caller, id) = member(&services, &ws).await;
        assert!(intent_core::with_caller(
            caller.clone(),
            services.open_canonical_source_session(daemon_binding)
        )
        .await
        .is_err());
        let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
        let session = intent_core::with_caller(
            caller,
            services.open_canonical_source_session(binding.clone()),
        )
        .await
        .unwrap();
        assert_eq!(session.inner.principal, format!("principal:{}", id.0));
        let park = Arc::new(ReadPark {
            after: !read_error,
            entered: Notify::new(),
            release: Notify::new(),
        });
        *session.inner.park.lock().unwrap() = Some(park.clone());
        let pending = session
            .read(
                request(&binding.owner_ref, "context", None),
                json!("held-real-page"),
            )
            .unwrap();
        park.entered.notified().await;
        services
            .store
            .remove_workspace_member(&ws, &id)
            .await
            .unwrap();
        if read_error {
            // Real Store lookup now fails, rather than injecting an error result.
            services.store.delete_note(&ws, &note).await.unwrap();
        }
        park.release.notify_one();
        let outcome = intent_core::with_caller(Caller::Daemon, pending).await;
        assert!(
            matches!(outcome, Err(Error::NotFound(_) | Error::Forbidden(_))),
            "revoked guest received {outcome:?}"
        );
        assert!(!session.inner.state.lock().unwrap().in_flight);
        assert!(session.inner.state.lock().unwrap().closed);
        session.close().await.unwrap();
        drop(session);
        services.store.close().await;
    }
}

#[tokio::test]
async fn canonical_source_session_metadata_and_recreation_retire_continuations() {
    for recreate in [false, true] {
        let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
        let binding = binding(&services, &ws, &note).await;
        let session = open(&services, binding.clone()).await;
        code_start(&session, &binding).await;
        let mut value = services.store.get_note(&ws, &note).await.unwrap();
        if recreate {
            services.store.delete_note(&ws, &note).await.unwrap();
            services.store.insert_note(&value).await.unwrap();
        } else {
            value.title = "changed metadata".into();
            services.store.update_note(&value).await.unwrap();
        }
        let outcome = session
            .read(request(&binding.source_ref, "context", None), json!(1))
            .unwrap()
            .await;
        assert!(
            matches!(
                outcome,
                Err(Error::NotePage(
                    NotePageError::Stale | NotePageError::Expired
                ))
            ),
            "stale source yielded {outcome:?}"
        );
        session.close().await.unwrap();
        drop(session);
        services.store.close().await;
    }
}

#[tokio::test]
async fn canonical_source_session_close_waits_for_real_read_pool_acquisition() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    let binding = binding(&services, &ws, &note).await;
    let session = open(&services, binding.clone()).await;
    let park = Arc::new(ReadPark {
        after: false,
        entered: Notify::new(),
        release: Notify::new(),
    });
    *session.inner.park.lock().unwrap() = Some(park.clone());
    let pending = session
        .read(request(&binding.owner_ref, "context", None), json!(1))
        .unwrap();
    park.entered.notified().await;
    let mut connections = Vec::new();
    for _ in 0..services.store.read_pool().options().get_max_connections() {
        connections.push(services.store.read_pool().acquire().await.unwrap());
    }
    // Release the scheduling barrier into the real Store read, whose pool is
    // exhausted. This observes SQL acquisition, not an in-VM SQLite pause.
    park.release.notify_one();
    tokio::task::yield_now().await;
    drop(pending);
    let wait = session.close();
    tokio::pin!(wait);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut wait)
            .await
            .is_err()
    );
    assert!(session.inner.state.lock().unwrap().grant.is_some());
    assert!(session.inner.state.lock().unwrap().in_flight);
    drop(connections);
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut wait)
        .await
        .unwrap()
        .unwrap();
    assert!(session.inner.state.lock().unwrap().grant.is_none());
    drop(session);
    services.store.close().await;
}

#[tokio::test]
async fn canonical_source_session_unconsumed_result_keeps_read_and_session_admission() {
    let (_tmp, mut services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    services.canonical_source_admission = Arc::new(tokio::sync::Semaphore::new(1));
    let binding = binding(&services, &ws, &note).await;
    let session = open(&services, binding.clone()).await;
    assert!(matches!(
        intent_core::with_caller(
            Caller::Daemon,
            services.open_canonical_source_session(binding.clone())
        )
        .await,
        Err(Error::NotePage(NotePageError::Budget))
    ));
    let pending = session
        .read(request(&binding.owner_ref, "context", None), json!(1))
        .unwrap();
    // Wait for the actual worker, leaving its bounded response unconsumed.
    loop {
        let settled = session.inner.settled.notified();
        tokio::pin!(settled);
        settled.as_mut().enable();
        if !session.inner.state.lock().unwrap().in_flight {
            break;
        }
        settled.await;
    }
    assert!(matches!(
        session.read(request(&binding.owner_ref, "context", None), json!(1)),
        Err(Error::NotePage(NotePageError::Budget))
    ));
    let wait = session.close();
    drop(session);
    wait.await.unwrap();
    assert_eq!(services.canonical_source_admission.available_permits(), 0);
    assert!(pending.await.is_err());
    assert_eq!(services.canonical_source_admission.available_permits(), 1);
    services.store.close().await;
}

async fn await_worker(session: &CanonicalSourceSession) {
    loop {
        let settled = session.inner.settled.notified();
        tokio::pin!(settled);
        settled.as_mut().enable();
        if !session.inner.state.lock().unwrap().in_flight {
            return;
        }
        settled.await;
    }
}

#[tokio::test]
async fn canonical_source_review_ready_result_rechecks_member_and_source() {
    for mutation in ["member", "code", "recreate", "error-member"] {
        let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
        let (caller, id) = member(&services, &ws).await;
        let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
        let session = intent_core::with_caller(
            caller,
            services.open_canonical_source_session(binding.clone()),
        )
        .await
        .unwrap();
        let park = Arc::new(ReadPark {
            after: false,
            entered: Notify::new(),
            release: Notify::new(),
        });
        *session.inner.park.lock().unwrap() = Some(park.clone());
        let pending = session
            .read(request(&binding.owner_ref, "context", None), json!(1))
            .unwrap();
        park.entered.notified().await;
        if mutation == "error-member" {
            services.store.delete_note(&ws, &note).await.unwrap();
        }
        park.release.notify_one();
        await_worker(&session).await;
        // Stored outcome exists; returned future has never been polled.
        match mutation {
            "member" | "error-member" => {
                services
                    .store
                    .remove_workspace_member(&ws, &id)
                    .await
                    .unwrap();
            }
            "code" => {
                let mut value = services.store.get_note(&ws, &note).await.unwrap();
                value.content = "```diff\n+new\n```".into();
                services.store.update_note(&value).await.unwrap();
            }
            "recreate" => {
                let value = services.store.get_note(&ws, &note).await.unwrap();
                services.store.delete_note(&ws, &note).await.unwrap();
                services.store.insert_note(&value).await.unwrap();
            }
            _ => unreachable!(),
        }
        let outcome = intent_core::with_caller(Caller::Daemon, pending).await;
        if mutation.contains("member") {
            assert!(
                matches!(outcome, Err(Error::NotFound(_) | Error::Forbidden(_))),
                "ready {mutation} outcome escaped: {outcome:?}"
            );
        } else {
            assert!(
                matches!(
                    outcome,
                    Err(Error::NotePage(
                        NotePageError::Stale | NotePageError::Expired
                    ))
                ),
                "ready {mutation} outcome escaped: {outcome:?}"
            );
        }
        session.close().await.unwrap();
        drop(session);
        services.store.close().await;
    }
}

#[tokio::test]
async fn canonical_source_review_worker_failure_has_explicit_close_outcome() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    let binding = binding(&services, &ws, &note).await;
    let session = open(&services, binding.clone()).await;
    let park = Arc::new(ReadPark {
        after: false,
        entered: Notify::new(),
        release: Notify::new(),
    });
    *session.inner.park.lock().unwrap() = Some(park.clone());
    let pending = session
        .read(request(&binding.owner_ref, "context", None), json!(1))
        .unwrap();
    park.entered.notified().await;
    let mut connections = Vec::new();
    for _ in 0..services.store.read_pool().options().get_max_connections() {
        connections.push(services.store.read_pool().acquire().await.unwrap());
    }
    park.release.notify_one();
    // Inject while the real read cannot acquire SQL; this proves supervisor
    // failure handling, not interruption/retirement inside SQLite's VM.
    session.inner.fail_worker.notify_one();
    assert!(pending.await.is_err());
    drop(connections);
    let outcome = tokio::time::timeout(std::time::Duration::from_millis(50), session.close()).await;
    assert!(
        matches!(outcome, Ok(Err(Error::Internal(ref message))) if message.contains("quarantined")),
        "worker failure did not report explicit quarantine: {outcome:?}"
    );
    assert!(session.inner.state.lock().unwrap().grant.is_some());
    assert!(session.inner.state.lock().unwrap().quarantined);
    drop(session);
    assert_eq!(services.canonical_source_admission.available_permits(), 255);
    assert_eq!(services.canonical_source_owners.lock().unwrap().len(), 1);
    services.store.close().await;
}

#[tokio::test]
async fn canonical_source_review_cancelled_final_authorization_retains_real_query() {
    let (_tmp, mut services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    services.canonical_source_admission = Arc::new(tokio::sync::Semaphore::new(1));
    let binding = binding(&services, &ws, &note).await;
    let session = open(&services, binding.clone()).await;
    let mut pending = Box::pin(
        session
            .read(request(&binding.owner_ref, "context", None), json!(1))
            .unwrap(),
    );
    await_worker(&session).await;
    let mut connections = Vec::new();
    for _ in 0..services.store.read_pool().options().get_max_connections() {
        connections.push(services.store.read_pool().acquire().await.unwrap());
    }
    // Poll the buffered response into its INLINE final authorization. Daemon
    // membership needs no query; real source authorization waits for a reader.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut pending)
            .await
            .is_err()
    );
    assert!(session.inner.state.lock().unwrap().in_flight);
    drop(pending);
    assert!(session.inner.state.lock().unwrap().closed);
    let wait = session.close();
    tokio::pin!(wait);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut wait)
            .await
            .is_err()
    );
    assert!(session.inner.state.lock().unwrap().grant.is_some());
    assert_eq!(services.canonical_source_admission.available_permits(), 0);
    assert_eq!(services.canonical_source_owners.lock().unwrap().len(), 1);
    drop(connections);
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut wait)
        .await
        .unwrap()
        .unwrap();
    assert!(!session.inner.state.lock().unwrap().quarantined);
    assert!(session.inner.state.lock().unwrap().grant.is_none());
    drop(session);
    let permit = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        services.canonical_source_admission.acquire(),
    )
    .await
    .unwrap()
    .unwrap();
    drop(permit);
    assert!(services.canonical_source_owners.lock().unwrap().is_empty());
    services.store.close().await;
}

#[tokio::test]
async fn canonical_source_review_open_cancel_keeps_hold_through_actual_query_wait() {
    let (_tmp, mut services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    services.canonical_source_admission = Arc::new(tokio::sync::Semaphore::new(1));
    let binding = binding(&services, &ws, &note).await;
    let control = Arc::new(ownership::OpenTest {
        entered: Notify::new(),
        release: Notify::new(),
        fail: std::sync::atomic::AtomicBool::new(false),
    });
    *services.canonical_source_open_test.lock().unwrap() = Some(control.clone());
    let mut pending = Box::pin(intent_core::with_caller(
        Caller::Daemon,
        services.open_canonical_source_session(binding),
    ));
    tokio::select! { _ = &mut pending => panic!("open bypassed test barrier"), ()=control.entered.notified()=>{} }
    assert!(services.canonical_source_owners.lock().unwrap()[0]
        .hold
        .lock()
        .unwrap()
        .is_some());
    let mut connections = Vec::new();
    for _ in 0..services.store.read_pool().options().get_max_connections() {
        connections.push(services.store.read_pool().acquire().await.unwrap());
    }
    control.release.notify_one();
    drop(pending);
    assert_eq!(services.canonical_source_admission.available_permits(), 0);
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(10),
        services.canonical_source_admission.acquire()
    )
    .await
    .is_err());
    drop(connections);
    let permit = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        services.canonical_source_admission.acquire(),
    )
    .await
    .unwrap()
    .unwrap();
    drop(permit);
    assert!(services.canonical_source_owners.lock().unwrap().is_empty());
    services.store.close().await;
}

#[tokio::test]
async fn canonical_source_review_open_and_adoption_failure_retain_registered_owner() {
    for open_failure in [true, false] {
        let (_tmp, mut services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
        services.canonical_source_admission = Arc::new(tokio::sync::Semaphore::new(1));
        let binding = binding(&services, &ws, &note).await;
        if open_failure {
            let control = Arc::new(ownership::OpenTest {
                entered: Notify::new(),
                release: Notify::new(),
                fail: std::sync::atomic::AtomicBool::new(true),
            });
            *services.canonical_source_open_test.lock().unwrap() = Some(control.clone());
            let mut pending = Box::pin(intent_core::with_caller(
                Caller::Daemon,
                services.open_canonical_source_session(binding),
            ));
            tokio::select! { _ = &mut pending => panic!("open bypassed test barrier"), ()=control.entered.notified()=>{} }
            control.release.notify_one();
            assert!(matches!(pending.await, Err(Error::Internal(_))));
        } else {
            let session = open(&services, binding.clone()).await;
            let pending = session
                .read(request(&binding.owner_ref, "context", None), json!(1))
                .unwrap();
            await_worker(&session).await;
            session
                .inner
                .fail_adoption
                .store(true, std::sync::atomic::Ordering::Release);
            assert!(matches!(pending.await, Err(Error::Internal(_))));
            assert!(session.close().await.is_err());
            drop(session);
        }
        // These injected failures precede the next SQL call. Retention is
        // conservative; neither case claims actual SQLite interruption/retirement.
        {
            let owners = services.canonical_source_owners.lock().unwrap();
            assert_eq!(owners.len(), 1);
            assert!(owners[0].hold.lock().unwrap().is_some());
            assert!(owners[0]
                .uncertain
                .load(std::sync::atomic::Ordering::Acquire));
        }
        assert_eq!(services.canonical_source_admission.available_permits(), 0);
        services.store.close().await;
    }
}

#[tokio::test]
async fn canonical_source_review_close_during_final_authorization_prevents_adoption() {
    let (_tmp, services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
    let binding = binding(&services, &ws, &note).await;
    let session = open(&services, binding.clone()).await;
    let mut pending = Box::pin(
        session
            .read(request(&binding.owner_ref, "context", None), json!(1))
            .unwrap(),
    );
    await_worker(&session).await;
    let mut connections = Vec::new();
    for _ in 0..services.store.read_pool().options().get_max_connections() {
        connections.push(services.store.read_pool().acquire().await.unwrap());
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut pending)
            .await
            .is_err()
    );
    assert!(session.inner.state.lock().unwrap().in_flight);
    let close = session.close();
    assert!(session.inner.state.lock().unwrap().closed);
    assert!(session.inner.state.lock().unwrap().grant.is_some());
    drop(connections);
    assert!(pending.await.is_err());
    close.await.unwrap();
    assert!(matches!(
        session.inner.state.lock().unwrap().step,
        Step::Owner(_)
    ));
    drop(session);
    assert!(services.canonical_source_owners.lock().unwrap().is_empty());
    services.store.close().await;
}

#[tokio::test]
async fn canonical_source_wire_initial_membership_owns_wait_cancel_and_denial() {
    for mode in ["allow", "deny", "cancel", "cancel-deny"] {
        let (_tmp, mut services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
        services.canonical_source_admission = Arc::new(tokio::sync::Semaphore::new(1));
        let (caller, id) = member(&services, &ws).await;
        let binding = binding_as(&services, &ws, &note, &format!("principal:{}", id.0)).await;
        let mut connections = Vec::new();
        for _ in 0..services.store.read_pool().options().get_max_connections() {
            connections.push(services.store.read_pool().acquire().await.unwrap());
        }
        let mut pending = Some(Box::pin(services.open_canonical_source_session(binding)));
        assert!(intent_core::with_caller(
            caller,
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                pending.as_mut().unwrap()
            )
        )
        .await
        .is_err());
        {
            let owners = services.canonical_source_owners.lock().unwrap();
            assert_eq!(owners.len(), 1);
            // Membership is principal/workspace-only. No snapshot retention or
            // source authority is needed yet; the work/admission owner IS needed.
            assert!(owners[0].hold.lock().unwrap().is_none());
            assert!(owners[0].grant.lock().unwrap().is_none());
        }
        assert_eq!(services.canonical_source_admission.available_permits(), 0);
        if mode.contains("deny") {
            services
                .store
                .remove_workspace_member(&ws, &id)
                .await
                .unwrap();
        }
        if mode.contains("cancel") {
            drop(pending.take());
        }
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            services.canonical_source_admission.acquire()
        )
        .await
        .is_err());
        drop(connections);
        if let Some(pending) = pending {
            let outcome = intent_core::with_caller(Caller::Daemon, pending).await;
            if mode == "deny" {
                assert!(matches!(
                    outcome,
                    Err(Error::NotFound(_) | Error::Forbidden(_))
                ));
            } else {
                let session = outcome.unwrap();
                session.close().await.unwrap();
                drop(session);
            }
        }
        let permit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            services.canonical_source_admission.acquire(),
        )
        .await
        .unwrap()
        .unwrap();
        drop(permit);
        assert!(services.canonical_source_owners.lock().unwrap().is_empty());
        // This proves owned membership-query completion, not the generic pool's
        // asynchronous housekeeping has the source-session close acknowledgement.
        services.store.close().await;
    }
}

#[tokio::test]
async fn canonical_source_open_rollback_ack_and_cleanup_uncertainty_keep_real_hold() {
    for interrupt_cleanup in [false, true] {
        let (_tmp, mut services, ws, note) = crate::tests::setup("```diff\n+one\n```").await;
        services.canonical_source_admission = Arc::new(tokio::sync::Semaphore::new(1));
        let binding = binding(&services, &ws, &note).await;
        let mut value = services.store.get_note(&ws, &note).await.unwrap();
        let control = Arc::new(ownership::OpenTest {
            entered: Notify::new(),
            release: Notify::new(),
            fail: std::sync::atomic::AtomicBool::new(false),
        });
        *services.canonical_source_open_test.lock().unwrap() = Some(control.clone());
        let mut pending = Box::pin(intent_core::with_caller(
            Caller::Daemon,
            services.open_canonical_source_session(binding),
        ));
        tokio::select! { _=&mut pending=>panic!("open bypassed source barrier"), ()=control.entered.notified()=>{} }
        value.content = "```diff\n+changed\n```".into();
        services.store.update_note(&value).await.unwrap();
        let mut connections = Vec::new();
        for _ in 0..services.store.read_pool().options().get_max_connections() {
            connections.push(services.store.read_pool().acquire().await.unwrap());
        }
        connections.pop().unwrap().close().await.unwrap();
        let options = services
            .store
            .read_pool()
            .connect_options()
            .as_ref()
            .clone()
            .optimize_on_close(interrupt_cleanup, None);
        services.store.read_pool().set_connect_options(options);
        let mut instrumented = services.store.read_pool().acquire().await.unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let timed_out = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let interrupt = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let interruptions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let mut handle = instrumented.lock_handle().await.unwrap();
            let observed = entered.clone();
            let gate = release.clone();
            let flag = interrupt.clone();
            let fallback = timed_out.clone();
            handle.set_rollback_hook(move || {
                observed.notify_one();
                let (_held, wait) = gate
                    .1
                    .wait_timeout_while(
                        gate.0.lock().unwrap(),
                        std::time::Duration::from_secs(5),
                        |ready| !*ready,
                    )
                    .unwrap();
                fallback.store(wait.timed_out(), std::sync::atomic::Ordering::Release);
                if !wait.timed_out() && interrupt_cleanup {
                    flag.store(true, std::sync::atomic::Ordering::Release);
                }
            });
            let flag = interrupt.clone();
            let count = interruptions.clone();
            handle.set_progress_handler(1, move || {
                if flag.load(std::sync::atomic::Ordering::Acquire) {
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    false
                } else {
                    true
                }
            });
        }
        drop(instrumented);
        control.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut pending)
                .await
                .is_err()
        );
        assert!(services.canonical_source_owners.lock().unwrap()[0]
            .hold
            .lock()
            .unwrap()
            .is_some());
        assert_eq!(services.canonical_source_admission.available_permits(), 0);
        *release.0.lock().unwrap() = true;
        release.1.notify_one();
        let outcome = pending.await;
        drop(connections);
        assert!(
            !timed_out.load(std::sync::atomic::Ordering::Acquire),
            "actual open rollback resumed through timeout instead of explicit release"
        );
        if interrupt_cleanup {
            // Actual SQLite progress interruption during cleanup on an optimized
            // test connection. It is NOT proof that a failed close retired IO.
            assert!(matches!(outcome, Err(Error::Internal(_))));
            assert!(interruptions.load(std::sync::atomic::Ordering::Relaxed) > 0);
            let owners = services.canonical_source_owners.lock().unwrap();
            assert_eq!(owners.len(), 1);
            assert!(owners[0].hold.lock().unwrap().is_some());
            assert!(owners[0]
                .uncertain
                .load(std::sync::atomic::Ordering::Acquire));
            assert_eq!(services.canonical_source_admission.available_permits(), 0);
        } else {
            assert!(matches!(
                outcome,
                Err(Error::NotePage(NotePageError::Stale))
            ));
            assert!(services.canonical_source_owners.lock().unwrap().is_empty());
            assert_eq!(services.canonical_source_admission.available_permits(), 1);
        }
        services.store.close().await;
    }
}
