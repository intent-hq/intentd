use super::*;
use intent_core::{note_delete::NoteDeleteIdentity, NoteId, WorkspaceApi};

fn request(registry: &Registry, ws: &str, id: &str) -> NoteDeleteSchedule {
    NoteDeleteSchedule {
        workspace_id: ws.into(),
        note_id: id.into(),
        note_instance_id: format!("instance:{id}"),
        expected_version: 0,
        source_revision: "r:0:g".into(),
        operation_key: NoteDeleteKey {
            epoch: registry.epoch.clone(),
            issued_tick_ms: registry.tick().unwrap(),
            nonce: uuid::Uuid::new_v4().to_string(),
        },
        undo_delay_ms: 15000,
    }
}
#[tokio::test(start_paused = true)]
async fn replay_deadline_receipt_capacity_and_expired_key_never_rearm() {
    let registry = Registry::default();
    let r = request(&registry, "ws", "note");
    registry.reserve(&r, "owner").unwrap();
    let (first, _) = registry.admit(&r).unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(!registry.reserve(&r, "owner").unwrap().0);
    let mut changed = r.clone();
    changed.undo_delay_ms += 1;
    assert!(matches!(
        registry.reserve(&changed, "owner"),
        Err(Error::NoteDelete(NoteDeleteError::KeyMismatch))
    ));
    let receipt = registry
        .transition(
            &r.operation_key,
            NoteDeleteState::Pending,
            NoteDeleteState::Cancelled,
            Some(NoteDeleteReason::Cancelled),
        )
        .unwrap()
        .unwrap();
    assert_eq!(receipt.deadline_tick_ms, first.deadline_tick_ms);
    registry.settle(&r.operation_key).unwrap();
    for i in 1..WORKSPACE_CAPACITY {
        registry
            .reserve(&request(&registry, "ws", &format!("n{i}")), "owner")
            .unwrap();
    }
    assert!(matches!(
        registry.reserve(&request(&registry, "ws", "overflow"), "owner"),
        Err(Error::NoteDelete(NoteDeleteError::Quota))
    ));
    tokio::time::advance(Duration::from_millis(RECEIPT_TTL_MS + 1)).await;
    assert!(matches!(
        registry.reserve(&r, "owner"),
        Err(Error::NoteDelete(NoteDeleteError::KeyExpired))
    ));
    assert_eq!(
        registry.state.lock().unwrap().entries.len(),
        WORKSPACE_CAPACITY - 1,
        "only settled expired receipt can be evicted"
    );
}
#[tokio::test(start_paused = true)]
async fn physical_commit_debt_and_ambiguous_outcome_hold_exclusion() {
    let registry = Registry::default();
    let r = request(&registry, "ws", "note");
    registry.reserve(&r, "owner").unwrap();
    registry.admit(&r).unwrap();
    registry
        .transition(
            &r.operation_key,
            NoteDeleteState::Pending,
            NoteDeleteState::Committing,
            None,
        )
        .unwrap()
        .unwrap();
    assert!(
        registry
            .transition(
                &r.operation_key,
                NoteDeleteState::Pending,
                NoteDeleteState::Cancelled,
                None
            )
            .unwrap()
            .is_none(),
        "cancel cannot defeat an owned claim"
    );
    tokio::time::advance(Duration::from_secs(600)).await;
    let changed = request(&registry, "ws", "note");
    assert!(matches!(
        registry.reserve(&changed, "owner"),
        Err(Error::NoteDelete(NoteDeleteError::AlreadyPending))
    ));
    let uncertain = registry.settle_unknown(&r.operation_key).unwrap();
    assert_eq!(
        uncertain.expires_tick_ms,
        Some(registry.tick().unwrap() + RECEIPT_TTL_MS)
    );
    assert!(matches!(
        registry.reserve(&changed, "owner"),
        Err(Error::NoteDelete(NoteDeleteError::AlreadyPending))
    ));
    tokio::time::advance(Duration::from_millis(RECEIPT_TTL_MS + 1)).await;
    assert!(
        registry
            .reserve(&request(&registry, "ws", "note"), "owner")
            .unwrap()
            .0
    );
}
#[tokio::test]
async fn cancel_winning_before_claim_and_global_capacity_are_fail_closed() {
    let registry = Registry::default();
    let r = request(&registry, "ws", "note");
    registry.reserve(&r, "owner").unwrap();
    registry.admit(&r).unwrap();
    registry
        .transition(
            &r.operation_key,
            NoteDeleteState::Pending,
            NoteDeleteState::Cancelled,
            Some(NoteDeleteReason::Cancelled),
        )
        .unwrap()
        .unwrap();
    assert!(registry
        .transition(
            &r.operation_key,
            NoteDeleteState::Pending,
            NoteDeleteState::Committing,
            None
        )
        .unwrap()
        .is_none());
    for i in 1..GLOBAL_CAPACITY {
        registry
            .reserve(&request(&registry, &format!("ws{i}"), "n"), "owner")
            .unwrap();
    }
    assert!(matches!(
        registry.reserve(&request(&registry, "new-ws", "n"), "owner"),
        Err(Error::NoteDelete(NoteDeleteError::Quota))
    ));
    registry.close();
    assert!(matches!(
        registry.reserve(&request(&registry, "new-ws", "n"), "owner"),
        Err(Error::NoteDelete(NoteDeleteError::ShuttingDown))
    ));
}
async fn schedule(
    svc: &Services,
    ws: &WorkspaceId,
    note: &NoteId,
    delay: u64,
) -> (NoteDeleteSchedule, NoteDeleteOperationResponse) {
    let status = svc
        .note_delete_status(NoteDeleteStatus {
            workspace_id: ws.clone(),
            note_id: Some(note.clone()),
            operation_key: None,
        })
        .await
        .unwrap();
    let identity = status.current.unwrap();
    let request = NoteDeleteSchedule {
        workspace_id: ws.clone(),
        note_id: note.clone(),
        note_instance_id: identity.note_instance_id,
        expected_version: identity.revision,
        source_revision: identity.source_revision,
        operation_key: NoteDeleteKey {
            epoch: status.epoch,
            issued_tick_ms: status.server_tick_ms,
            nonce: uuid::Uuid::new_v4().to_string(),
        },
        undo_delay_ms: delay,
    };
    let response = svc.schedule_note_delete(request.clone()).await.unwrap();
    (request, response)
}
async fn settled(svc: &Services, request: &NoteDeleteSchedule) -> NoteDeleteReceipt {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let response = svc
                .note_delete_status(NoteDeleteStatus {
                    workspace_id: request.workspace_id.clone(),
                    note_id: Some(request.note_id.clone()),
                    operation_key: Some(request.operation_key.clone()),
                })
                .await
                .unwrap();
            if let Some(NoteDeleteOperation::Receipt(r)) = response.operation {
                if r.expires_tick_ms.is_some() {
                    return r;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
async fn cleanup(svc: &Services) {
    svc.note_deletions.close();
    svc.note_deletions.tasks.shutdown().await;
    svc.store.close().await;
}
#[intent_test_macros::daemon_test]
async fn cancel_preserves_exact_record_history_and_concurrent_edit() {
    let (_tmp, svc, ws, id) =
        crate::tests::setup_versioned("😀\r\n@@@task\nliteral\n1 | display-looking").await;
    let (request, _) = schedule(&svc, &ws, &id, 60000).await;
    let mut note = svc.store.get_note(&ws, &id).await.unwrap();
    note.content.push_str("\nnewer edit");
    note.tags = vec!["kept".into()];
    svc.store.update_note(&note).await.unwrap();
    let before = svc.store.get_note(&ws, &id).await.unwrap();
    let versions = svc.store.list_note_versions(&ws, &id).await.unwrap();
    let response = svc
        .cancel_note_delete(NoteDeleteCancel {
            workspace_id: ws.clone(),
            note_id: id.clone(),
            operation_key: request.operation_key.clone(),
        })
        .await
        .unwrap();
    assert!(
        matches!(response.operation,NoteDeleteOperation::Receipt(r) if r.state==NoteDeleteState::Cancelled)
    );
    assert_eq!(
        settled(&svc, &request).await.state,
        NoteDeleteState::Cancelled
    );
    assert_eq!(svc.store.get_note(&ws, &id).await.unwrap(), before);
    assert_eq!(
        svc.store.list_note_versions(&ws, &id).await.unwrap(),
        versions
    );
    cleanup(&svc).await;
}
#[intent_test_macros::daemon_test]
async fn expiry_deletes_once_and_lost_ack_replays_without_rearming() {
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("exact").await;
    let (request, _) = schedule(&svc, &ws, &id, 1).await;
    let terminal = settled(&svc, &request).await;
    assert_eq!(terminal.state, NoteDeleteState::Deleted);
    assert!(svc.store.get_note(&ws, &id).await.is_err());
    let replay = svc.schedule_note_delete(request.clone()).await.unwrap();
    assert_eq!(
        replay.operation,
        NoteDeleteOperation::Receipt(terminal.clone())
    );
    let cancel = svc
        .cancel_note_delete(NoteDeleteCancel {
            workspace_id: ws.clone(),
            note_id: id.clone(),
            operation_key: request.operation_key.clone(),
        })
        .await
        .unwrap();
    assert_eq!(cancel.operation, NoteDeleteOperation::Receipt(terminal));
    cleanup(&svc).await;
}
#[intent_test_macros::daemon_test]
async fn shutdown_cancels_pending_without_touching_authoritative_row() {
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("pending at shutdown").await;
    let before = svc.store.get_note(&ws, &id).await.unwrap();
    let (request, _) = schedule(&svc, &ws, &id, 60000).await;
    svc.note_deletions.close();
    svc.note_deletions.tasks.shutdown().await;
    let terminal = settled(&svc, &request).await;
    assert_eq!(terminal.state, NoteDeleteState::Cancelled);
    assert_eq!(terminal.reason, Some(NoteDeleteReason::Shutdown));
    assert_eq!(svc.store.get_note(&ws, &id).await.unwrap(), before);
    let restarted = Services::new(svc.store.clone());
    let old = restarted
        .note_delete_status(NoteDeleteStatus {
            workspace_id: ws.clone(),
            note_id: Some(id),
            operation_key: Some(request.operation_key),
        })
        .await
        .unwrap();
    assert!(
        matches!(old.operation,Some(NoteDeleteOperation::Unknown(u)) if u.reason==NoteDeleteUnknownReason::PreviousEpoch)
    );
    cleanup(&restarted).await;
}

async fn grace_event(sub: &mut crate::Subscription) -> serde_json::Value {
    let events = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(events.len(), 1);
    serde_json::to_value(&events[0]).unwrap()
}
#[intent_test_macros::daemon_test]
async fn cancel_after_deadline_before_claim_emits_one_terminal_invalidation() {
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("race source").await;
    let bus = crate::EventBus::new(svc.store.clone());
    let svc = svc.with_event_bus(bus.clone());
    let mut sub = bus.subscribe(crate::SubscriptionFilter {
        event_types: vec![intent_core::events::NOTE_DELETE_OPERATION.into()],
        workspace_id: Some(ws.to_string()),
        ..Default::default()
    });
    let pause = Arc::new(TestPause::default());
    *svc.note_deletions.before_claim.lock().unwrap() = Some(pause.clone());
    let (request, _) = schedule(&svc, &ws, &id, 1).await;
    tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
        .await
        .unwrap();
    assert_eq!(grace_event(&mut sub).await["data"]["state"], "PENDING");
    let cancel = NoteDeleteCancel {
        workspace_id: ws.clone(),
        note_id: id.clone(),
        operation_key: request.operation_key.clone(),
    };
    svc.cancel_note_delete(cancel.clone()).await.unwrap();
    let event = grace_event(&mut sub).await;
    assert_eq!(event["data"]["state"], "CANCELLED");
    assert_eq!(
        event["data"]["operationKey"],
        serde_json::to_value(&request.operation_key).unwrap()
    );
    pause.release.notify_one();
    assert_eq!(
        settled(&svc, &request).await.state,
        NoteDeleteState::Cancelled
    );
    svc.cancel_note_delete(cancel).await.unwrap();
    // A later distinct pending event fences delivery of all earlier events.
    let (second, _) = schedule(&svc, &ws, &id, 60000).await;
    let next = grace_event(&mut sub).await;
    assert_eq!(
        next["data"]["state"], "PENDING",
        "no duplicate cancellation in timer branch"
    );
    assert_eq!(
        next["data"]["operationKey"],
        serde_json::to_value(second.operation_key).unwrap()
    );
    cleanup(&svc).await;
}
#[intent_test_macros::daemon_test]
async fn commit_failure_stays_committing_until_owned_settlement_then_exposes_uncertainty() {
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("uncertain source").await;
    // Actual SQLite COMMIT rejects this deferred reference; the store reports
    // an ambiguous acknowledgment conservatively even though rollback succeeds.
    sqlx::query("CREATE TABLE grace_commit_guard(note_id TEXT,workspace_id TEXT,FOREIGN KEY(note_id,workspace_id) REFERENCES note(id,workspace_id) DEFERRABLE INITIALLY DEFERRED)").execute(svc.store.write_pool()).await.unwrap();
    sqlx::query("INSERT INTO grace_commit_guard VALUES (?,?)")
        .bind(id.as_str())
        .bind(ws.as_str())
        .execute(svc.store.write_pool())
        .await
        .unwrap();
    let pause = Arc::new(TestPause::default());
    *svc.note_deletions.after_commit.lock().unwrap() = Some(pause.clone());
    let (request, _) = schedule(&svc, &ws, &id, 1).await;
    tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
        .await
        .unwrap();
    let status = svc
        .note_delete_status(NoteDeleteStatus {
            workspace_id: ws.clone(),
            note_id: Some(id.clone()),
            operation_key: Some(request.operation_key.clone()),
        })
        .await
        .unwrap();
    assert!(
        matches!(status.operation,Some(NoteDeleteOperation::Receipt(ref r)) if r.state==NoteDeleteState::Committing && r.expires_tick_ms.is_none())
    );
    assert_eq!(status.pending.len(), 1);
    assert!(!status.pending[0].can_cancel);
    let cancelled = svc
        .cancel_note_delete(NoteDeleteCancel {
            workspace_id: ws.clone(),
            note_id: id.clone(),
            operation_key: request.operation_key.clone(),
        })
        .await
        .unwrap();
    assert!(
        matches!(cancelled.operation,NoteDeleteOperation::Receipt(r) if r.state==NoteDeleteState::Committing)
    );
    pause.release.notify_one();
    let final_receipt = settled(&svc, &request).await;
    assert_eq!(final_receipt.state, NoteDeleteState::OutcomeUnknown);
    assert!(final_receipt.expires_tick_ms.is_some());
    let snapshot = svc
        .note_delete_status(NoteDeleteStatus {
            workspace_id: ws.clone(),
            note_id: None,
            operation_key: None,
        })
        .await
        .unwrap();
    assert_eq!(snapshot.pending.len(), 1);
    assert_eq!(snapshot.pending[0].state, NoteDeleteState::OutcomeUnknown);
    assert!(!snapshot.pending[0].can_cancel);
    assert_eq!(
        svc.store.get_note(&ws, &id).await.unwrap().content,
        "uncertain source"
    );
    cleanup(&svc).await;
}

#[intent_test_macros::daemon_test]
async fn foreign_authorized_principal_cannot_cancel_or_read_private_receipt() {
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("private receipt").await;
    let mut guest = svc.store.get_primary_principal().await.unwrap();
    guest.id = intent_core::PrincipalId::from("grace-guest");
    guest.is_primary = false;
    guest.identity = None;
    guest.github_user_id = None;
    guest.login = None;
    svc.store.upsert_principal(&guest).await.unwrap();
    svc.store
        .add_workspace_member(&ws, &guest.id, intent_core::WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let (request, _) = schedule(&svc, &ws, &id, 60000).await;
    let caller = Caller::Wire {
        principal_id: guest.id,
        host_role: intent_core::HostRole::Guest,
    };
    intent_core::caller::with_caller(caller, async {
        let public = svc
            .note_delete_status(NoteDeleteStatus {
                workspace_id: ws.clone(),
                note_id: Some(id.clone()),
                operation_key: None,
            })
            .await
            .unwrap();
        assert_eq!(public.pending.len(), 1);
        assert!(!public.pending[0].can_cancel);
        assert!(matches!(
            svc.cancel_note_delete(NoteDeleteCancel {
                workspace_id: ws.clone(),
                note_id: id.clone(),
                operation_key: request.operation_key.clone()
            })
            .await,
            Err(Error::NoteDelete(NoteDeleteError::Forbidden))
        ));
        assert!(matches!(
            svc.note_delete_status(NoteDeleteStatus {
                workspace_id: ws.clone(),
                note_id: Some(id.clone()),
                operation_key: Some(request.operation_key.clone())
            })
            .await,
            Err(Error::NoteDelete(NoteDeleteError::Forbidden))
        ));
    })
    .await;
    cleanup(&svc).await;
}
#[intent_test_macros::daemon_test]
async fn historical_receipt_never_marks_a_recreated_current_incarnation() {
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("old incarnation").await;
    let (request, _) = schedule(&svc, &ws, &id, 60000).await;
    let mut replacement = svc.store.get_note(&ws, &id).await.unwrap();
    svc.store.delete_note(&ws, &id).await.unwrap();
    replacement.content = "new incarnation".into();
    svc.store.insert_note(&replacement).await.unwrap();
    let status = svc
        .note_delete_status(NoteDeleteStatus {
            workspace_id: ws.clone(),
            note_id: Some(id.clone()),
            operation_key: Some(request.operation_key.clone()),
        })
        .await
        .unwrap();
    assert_ne!(
        status.current.unwrap().note_instance_id,
        request.note_instance_id
    );
    assert!(status.pending.is_empty());
    assert!(
        matches!(status.operation,Some(NoteDeleteOperation::Receipt(r)) if r.note_instance_id==request.note_instance_id)
    );
    svc.cancel_note_delete(NoteDeleteCancel {
        workspace_id: ws.clone(),
        note_id: id.clone(),
        operation_key: request.operation_key,
    })
    .await
    .unwrap();
    assert_eq!(svc.store.get_note(&ws, &id).await.unwrap(), replacement);
    cleanup(&svc).await;
}
#[intent_test_macros::daemon_test]
async fn cancellation_preserves_empty_html_task_syntax_large_source_and_supported_metadata() {
    let samples = [
        String::new(),
        "<p>😀 raw &amp; source</p>\r\n".into(),
        "@@@task\n# literal task\n@@@\n1 | display-looking\n".into(),
        "😀abcdefghijk\r\n".repeat(32768),
    ];
    for (index, source) in samples.into_iter().enumerate() {
        let (_tmp, svc, ws, id) = crate::tests::setup_versioned(&source).await;
        let mut note = svc.store.get_note(&ws, &id).await.unwrap();
        note.content_type = if index == 0 {
            intent_core::ContentType::PlainText
        } else {
            intent_core::ContentType::Markdown
        };
        note.is_pinned = true;
        note.is_archived = true;
        note.tags = vec!["identity".into(), "😀".into()];
        svc.store.update_note(&note).await.unwrap();
        let before = svc.store.get_note(&ws, &id).await.unwrap();
        assert_eq!(before.content, source);
        let (request, response) = schedule(&svc, &ws, &id, 60000).await;
        assert!(serde_json::to_vec(&response).unwrap().len() < 2048);
        svc.cancel_note_delete(NoteDeleteCancel {
            workspace_id: ws.clone(),
            note_id: id.clone(),
            operation_key: request.operation_key.clone(),
        })
        .await
        .unwrap();
        assert_eq!(
            settled(&svc, &request).await.state,
            NoteDeleteState::Cancelled
        );
        assert_eq!(svc.store.get_note(&ws, &id).await.unwrap(), before);
        cleanup(&svc).await;
    }
}
#[intent_test_macros::daemon_test]
async fn legacy_credential_revoked_during_grace_never_deletes() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Authority(AtomicBool);
    impl intent_core::caller::LegacyCredentialAuthority for Authority {
        fn authorize(&self) -> intent_core::BoxFuture<'_, Result<CredentialLease>> {
            Box::pin(async {
                if self.0.load(Ordering::SeqCst) {
                    Ok(Box::new(()) as CredentialLease)
                } else {
                    Err(Error::Forbidden("revoked fixture".into()))
                }
            })
        }
    }
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("revoked grace").await;
    let primary = svc.store.get_primary_principal().await.unwrap();
    let authority = Arc::new(Authority(AtomicBool::new(true)));
    let wire = WireCredential::Legacy {
        principal_id: primary.id.clone(),
        authority: authority.clone(),
    };
    let caller = Caller::Wire {
        principal_id: primary.id,
        host_role: intent_core::HostRole::Owner,
    };
    let pause = Arc::new(TestPause::default());
    *svc.note_deletions.before_claim.lock().unwrap() = Some(pause.clone());
    let (request, _) = intent_core::caller::with_caller(
        caller,
        intent_core::caller::with_wire_credential(Some(wire), schedule(&svc, &ws, &id, 1)),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
        .await
        .unwrap();
    authority.0.store(false, Ordering::SeqCst);
    pause.release.notify_one();
    // Original owner cannot authenticate with its revoked bearer. Observe the
    // internal terminal receipt only to prove background authentication.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let terminal = svc
                .note_deletions
                .state
                .lock()
                .unwrap()
                .entries
                .get(&request.operation_key)
                .and_then(|e| e.receipt.clone());
            if let Some(r) = terminal.filter(|r| r.expires_tick_ms.is_some()) {
                assert_eq!(r.state, NoteDeleteState::Failed);
                assert_eq!(r.reason, Some(NoteDeleteReason::AuthorityLost));
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        svc.store.get_note(&ws, &id).await.unwrap().content,
        "revoked grace"
    );
    cleanup(&svc).await;
}

#[intent_test_macros::daemon_test]
async fn note_removed_during_grace_is_conflict_without_recreation() {
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("removed elsewhere").await;
    let pause = Arc::new(TestPause::default());
    *svc.note_deletions.before_claim.lock().unwrap() = Some(pause.clone());
    let (request, _) = schedule(&svc, &ws, &id, 1).await;
    tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
        .await
        .unwrap();
    svc.store.delete_note(&ws, &id).await.unwrap();
    pause.release.notify_one();
    let receipt = settled(&svc, &request).await;
    assert_eq!(receipt.state, NoteDeleteState::Conflict);
    assert_eq!(receipt.reason, Some(NoteDeleteReason::NoteMissing));
    assert!(svc.store.get_note(&ws, &id).await.is_err());
    cleanup(&svc).await;
}

#[intent_test_macros::daemon_test]
async fn claimed_work_keeps_worker_and_shutdown_debt_until_physical_settlement() {
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("claimed shutdown").await;
    let pause = Arc::new(TestPause::default());
    *svc.note_deletions.after_commit.lock().unwrap() = Some(pause.clone());
    let (request, _) = schedule(&svc, &ws, &id, 1).await;
    tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
        .await
        .unwrap();
    assert!(svc.store.get_note(&ws, &id).await.is_err());
    assert_eq!(svc.note_deletions.workers.available_permits(), 3);
    svc.note_deletions.close();
    let draining = svc.note_deletions.tasks.shutdown();
    tokio::pin!(draining);
    let pending = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(draining.as_mut(), cx).is_pending())
    })
    .await;
    assert!(pending);
    let response = svc
        .note_delete_status(NoteDeleteStatus {
            workspace_id: ws.clone(),
            note_id: Some(id.clone()),
            operation_key: Some(request.operation_key.clone()),
        })
        .await
        .unwrap();
    assert!(
        matches!(response.operation,Some(NoteDeleteOperation::Receipt(r)) if r.state==NoteDeleteState::Committing && r.expires_tick_ms.is_none())
    );
    pause.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), draining)
        .await
        .unwrap();
    assert_eq!(svc.note_deletions.workers.available_permits(), 4);
    assert_eq!(
        settled(&svc, &request).await.state,
        NoteDeleteState::Deleted
    );
    svc.store.close().await;
}

#[tokio::test(start_paused = true)]
async fn registry_restart_and_unknown_key_never_prove_survival_or_rearm() {
    let registry = Registry::default();
    let request = request(&registry, "ws", "note");
    registry.reserve(&request, "owner").unwrap();
    registry.admit(&request).unwrap();
    let restarted = Registry::default();
    let cancel = NoteDeleteCancel {
        workspace_id: request.workspace_id.clone(),
        note_id: request.note_id.clone(),
        operation_key: request.operation_key.clone(),
    };
    let unknown = restarted.response(&cancel, "owner").unwrap();
    assert!(
        matches!(unknown.operation,NoteDeleteOperation::Unknown(u) if u.reason==NoteDeleteUnknownReason::PreviousEpoch)
    );
    assert!(matches!(
        restarted.reserve(&request, "owner"),
        Err(Error::NoteDelete(NoteDeleteError::KeyExpired))
    ));
    assert!(restarted.state.lock().unwrap().entries.is_empty());
}

fn grace_comment(note: &NoteId, id: &str) -> intent_core::Comment {
    use intent_core::*;
    Comment {
        id: id.into(),
        thread_id: id.into(),
        note_id: Some(note.clone()),
        kind: CommentType::Comment,
        content: "new comment during grace".into(),
        author: "Author".into(),
        author_type: AuthorType::User,
        author_principal_id: None,
        author_identity: None,
        status: CommentStatus::Open,
        parent_id: None,
        anchor: None,
        anchor_text: None,
        anchor_before: None,
        anchor_after: None,
        suggestion_original: None,
        suggestion_proposed: None,
        agent_id: None,
        is_orphaned: None,
        created_at: "2026-10-08T00:00:00Z".into(),
        updated_at: "2026-10-08T00:00:00Z".into(),
    }
}
#[intent_test_macros::daemon_test]
async fn actual_task_metadata_and_late_comments_survive_cancel_and_follow_canonical_delete() {
    for cancel in [true, false] {
        let (_tmp, svc, ws, id) = crate::tests::setup_versioned("task source").await;
        let mut task = svc.store.get_note(&ws, &id).await.unwrap();
        task.metadata.task = Some(intent_core::TaskMetadata {
            status: intent_core::TaskStatus::Complete,
            acceptance_criteria: vec!["kept criterion".into()],
            ..Default::default()
        });
        svc.store.update_note(&task).await.unwrap();
        let task = svc.store.get_note(&ws, &id).await.unwrap();
        let mut dependent = task.clone();
        dependent.id = "dependent".into();
        dependent.content = "dependent exact source".into();
        dependent.metadata.task = Some(intent_core::TaskMetadata {
            status: intent_core::TaskStatus::NotStarted,
            depends_on: vec![id.clone()],
            ..Default::default()
        });
        svc.store.insert_note(&dependent).await.unwrap();
        let bus = crate::EventBus::new(svc.store.clone());
        let svc = svc.with_event_bus(bus.clone());
        let mut events = bus.subscribe(crate::SubscriptionFilter {
            event_types: vec![
                "note:deleted".into(),
                "task:ready-tasks-changed".into(),
                "note:updated".into(),
            ],
            workspace_id: Some(ws.to_string()),
            ..Default::default()
        });
        let pause = Arc::new(TestPause::default());
        *svc.note_deletions.before_claim.lock().unwrap() = Some(pause.clone());
        let (request, _) = schedule(&svc, &ws, &id, 1).await;
        tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
            .await
            .unwrap();
        let comment = grace_comment(&id, "late-comment");
        svc.store.insert_comment(&ws, &comment).await.unwrap();
        // Comment-only insertion does not change note identity or task state.
        assert_eq!(svc.store.get_note(&ws, &id).await.unwrap(), task);
        if cancel {
            svc.cancel_note_delete(NoteDeleteCancel {
                workspace_id: ws.clone(),
                note_id: id.clone(),
                operation_key: request.operation_key.clone(),
            })
            .await
            .unwrap();
        }
        pause.release.notify_one();
        let receipt = settled(&svc, &request).await;
        if cancel {
            assert_eq!(receipt.state, NoteDeleteState::Cancelled);
            assert_eq!(svc.store.get_note(&ws, &id).await.unwrap(), task);
            assert_eq!(svc.store.list_comments(&id).await.unwrap(), vec![comment]);
        } else {
            assert_eq!(receipt.state, NoteDeleteState::Deleted);
            assert!(svc.store.list_comments(&id).await.unwrap().is_empty());
            let deleted = grace_event(&mut events).await;
            assert_eq!(deleted["type"], "note:deleted");
            let ready = grace_event(&mut events).await;
            assert_eq!(ready["type"], "task:ready-tasks-changed");
            assert_eq!(ready["data"]["readyTaskIds"], serde_json::json!([]));
            assert_eq!(
                ready["data"]["triggeredBy"],
                serde_json::json!({"noteId":id,"reason":"note-deleted"})
            );
            let updated = grace_event(&mut events).await;
            assert_eq!(updated["type"], "note:updated");
            assert_eq!(updated["data"]["noteId"], "dependent");
        }
        assert_eq!(
            svc.store.get_note(&ws, &dependent.id).await.unwrap(),
            dependent
        );
        cleanup(&svc).await;
    }
}

#[intent_test_macros::daemon_test]
async fn deleted_callback_tail_holds_slot_and_receipt_even_when_publish_fails() {
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("callback tail").await;
    let bus = crate::EventBus::new(svc.store.clone());
    let svc = svc.with_event_bus(bus);
    let pause = Arc::new(TestPause::default());
    *svc.note_deletions.after_commit.lock().unwrap() = Some(pause.clone());
    let (request, _) = schedule(&svc, &ws, &id, 1).await;
    tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
        .await
        .unwrap();
    let mut writer = svc.store.write_pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *writer)
        .await
        .unwrap();
    pause.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let receipt = svc.note_deletions.state.lock().unwrap().entries[&request.operation_key]
                .receipt
                .clone()
                .unwrap();
            if receipt.state == NoteDeleteState::Deleted {
                assert!(receipt.expires_tick_ms.is_none());
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(svc.note_deletions.workers.available_permits(), 3);
    assert!(svc.store.get_note(&ws, &id).await.is_err());
    // The callback's persisted event fails after the canonical commit. The
    // operation must remain Deleted, never rollback/Failed or resurrected.
    sqlx::query("CREATE TRIGGER reject_grace_event BEFORE INSERT ON event BEGIN SELECT RAISE(ABORT,'callback event failure'); END").execute(&mut *writer).await.unwrap();
    sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
    drop(writer);
    assert_eq!(
        settled(&svc, &request).await.state,
        NoteDeleteState::Deleted
    );
    assert_eq!(svc.note_deletions.workers.available_permits(), 4);
    cleanup(&svc).await;
}

#[tokio::test]
async fn result_bound_covers_maximum_pending_snapshot_and_rejects_larger_serialization() {
    let text = "\u{0001}".repeat(128);
    let key = NoteDeleteKey {
        epoch: uuid::Uuid::new_v4().to_string(),
        issued_tick_ms: MAX_SAFE_INTEGER,
        nonce: uuid::Uuid::new_v4().to_string(),
    };
    let pending = NoteDeletePending {
        operation_key: key.clone(),
        note_id: text.clone().into(),
        note_instance_id: text.clone(),
        state: NoteDeleteState::OutcomeUnknown,
        sequence: MAX_SAFE_INTEGER,
        deadline_tick_ms: MAX_SAFE_INTEGER,
        delete_at: "2000-01-01T00:00:00.000000000Z".into(),
        can_cancel: false,
    };
    let receipt = NoteDeleteReceipt {
        operation_key: key,
        workspace_id: text.clone().into(),
        note_id: text.clone().into(),
        note_instance_id: text.clone(),
        state: NoteDeleteState::OutcomeUnknown,
        sequence: MAX_SAFE_INTEGER,
        deadline_tick_ms: MAX_SAFE_INTEGER,
        delete_at: pending.delete_at.clone(),
        expires_tick_ms: Some(MAX_SAFE_INTEGER),
        reason: Some(NoteDeleteReason::CommitOutcomeUnknown),
    };
    let snapshot = NoteDeleteStatusResponse {
        epoch: uuid::Uuid::new_v4().to_string(),
        server_tick_ms: MAX_SAFE_INTEGER,
        sequence: MAX_SAFE_INTEGER,
        current: None,
        pending: vec![pending.clone(); WORKSPACE_CAPACITY],
        operation: None,
    };
    assert_eq!(serde_json::to_vec(&snapshot).unwrap().len(), 477_080);
    assert!(bounded(snapshot).is_ok());
    // A keyed note response has at most one current-incarnation marker.
    let response = NoteDeleteStatusResponse {
        epoch: uuid::Uuid::new_v4().to_string(),
        server_tick_ms: MAX_SAFE_INTEGER,
        sequence: MAX_SAFE_INTEGER,
        current: Some(NoteDeleteIdentity {
            note_instance_id: text.clone(),
            revision: i64::try_from(MAX_SAFE_INTEGER).unwrap(),
            source_revision: text,
        }),
        pending: vec![pending],
        operation: Some(NoteDeleteOperation::Receipt(receipt)),
    };
    assert!(serde_json::to_vec(&response).unwrap().len() <= 477_080);
    assert!(bounded(response).is_ok());
    assert!(matches!(
        bounded("x".repeat(MAX_RESULT_BYTES)),
        Err(Error::NoteDelete(NoteDeleteError::Unavailable))
    ));
}

#[intent_test_macros::daemon_test]
async fn dropped_schedule_request_does_not_abandon_owned_admission_or_rearm() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct Admission {
        calls: AtomicUsize,
        entered: Notify,
        release: Notify,
    }
    impl intent_core::caller::LegacyCredentialAuthority for Admission {
        fn authorize(&self) -> intent_core::BoxFuture<'_, Result<CredentialLease>> {
            Box::pin(async {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                Ok(Box::new(()) as CredentialLease)
            })
        }
    }
    let (_tmp, svc, ws, id) = crate::tests::setup_versioned("lost acknowledgment").await;
    let status = svc
        .note_delete_status(NoteDeleteStatus {
            workspace_id: ws.clone(),
            note_id: Some(id.clone()),
            operation_key: None,
        })
        .await
        .unwrap();
    let identity = status.current.unwrap();
    let request = NoteDeleteSchedule {
        workspace_id: ws.clone(),
        note_id: id.clone(),
        note_instance_id: identity.note_instance_id,
        expected_version: identity.revision,
        source_revision: identity.source_revision,
        operation_key: NoteDeleteKey {
            epoch: status.epoch,
            issued_tick_ms: status.server_tick_ms,
            nonce: uuid::Uuid::new_v4().to_string(),
        },
        undo_delay_ms: 60000,
    };
    let principal = svc.store.get_primary_principal().await.unwrap();
    let gate = Arc::new(Admission::default());
    let wire = WireCredential::Legacy {
        principal_id: principal.id.clone(),
        authority: gate.clone(),
    };
    let caller = Caller::Wire {
        principal_id: principal.id,
        host_role: intent_core::HostRole::Owner,
    };
    let mut abandoned = Box::pin(intent_core::caller::with_caller(
        caller.clone(),
        intent_core::caller::with_wire_credential(
            Some(wire.clone()),
            svc.schedule_note_delete(request.clone()),
        ),
    ));
    tokio::time::timeout(Duration::from_secs(5),async {tokio::select! {result=&mut abandoned=>panic!("admission unexpectedly completed {result:?}"),()=gate.entered.notified()=>{}}}).await.unwrap();
    drop(abandoned);
    assert_eq!(svc.note_deletions.state.lock().unwrap().entries.len(), 1);
    gate.release.notify_one();
    intent_core::caller::with_caller(
        caller,
        intent_core::caller::with_wire_credential(Some(wire), async {
            let replay = svc.schedule_note_delete(request.clone()).await.unwrap();
            let again = svc.schedule_note_delete(request.clone()).await.unwrap();
            assert_eq!(replay.operation, again.operation);
            svc.cancel_note_delete(NoteDeleteCancel {
                workspace_id: ws.clone(),
                note_id: id.clone(),
                operation_key: request.operation_key.clone(),
            })
            .await
            .unwrap();
            assert_eq!(
                settled(&svc, &request).await.state,
                NoteDeleteState::Cancelled
            );
        }),
    )
    .await;
    assert_eq!(
        svc.store.get_note(&ws, &id).await.unwrap().content,
        "lost acknowledgment"
    );
    cleanup(&svc).await;
}
