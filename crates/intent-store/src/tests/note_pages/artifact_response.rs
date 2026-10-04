//! Prepared public DTO/recovery controls; no router or profile is registered.
use super::*;

#[tokio::test]
async fn artifact_response_retains_original_accepted_deadline_text() {
    let (store, _temporary, _note, mut request) = artifact_begin_fixture().await;
    let future = intent_core::iso_from_unix_secs(
        i64::try_from(intent_core::now_epoch_ms() / 1000).unwrap() + 30,
    );
    request.expires_at = future.trim_end_matches('Z').to_owned() + ".123456789+00:00";
    sign_artifact_begin(&mut request);
    request.validate("pages").unwrap();
    store
        .begin_note_artifact_journal("alice", "pages", &request, artifact_retention(&request))
        .await
        .unwrap();
    let row = sqlx::query("SELECT * FROM note_artifact_job WHERE principal='alice' AND workspace_id='pages' AND job_id='job'")
        .fetch_one(store.artifact_pool().unwrap()).await.unwrap();
    let stored = row.try_get::<String, _>("expires_at_text");
    assert_eq!(
        stored.ok().as_deref(),
        Some(request.expires_at.as_str()),
        "accepted deadline spelling/precision is not retained for original DTO replay"
    );
    let status = store
        .note_artifact_journal_status("alice", "pages", &request.job_id, &request.header_digest)
        .await
        .unwrap()
        .unwrap();
    let public = store
        .note_artifact_job_receipt("alice", "pages", &status)
        .await
        .unwrap()
        .rpc_result(&json!(17))
        .unwrap();
    assert_eq!(public["kind"], "artifactJobState");
    assert_eq!(public["expiresAt"], request.expires_at);
    assert_eq!(
        public["reservation"],
        serde_json::to_value(&request.header.reservation).unwrap()
    );
    assert_eq!(
        intent_core::parse_iso(public["statusUntil"].as_str().unwrap())
            .unwrap()
            .unix_timestamp_nanos(),
        i128::from(artifact_retention(&request)) * 1_000_000
    );
    assert!(public.get("generation").is_none() && public.get("cleanupComplete").is_none());
    assert!(store
        .note_artifact_job_receipt("mallory", "pages", &status)
        .await
        .is_err());
    assert!(store
        .note_artifact_job_receipt("alice", "other", &status)
        .await
        .is_err());
    assert!(store
        .note_artifact_job_receipt("alice", "pages", &status)
        .await
        .unwrap()
        .rpc_result(&json!("\"".repeat(4096)))
        .is_err());
    capture("deadline", &request, &json!({"preparedJobState":public}));
    store.close().await;
}

#[tokio::test]
async fn artifact_response_recovers_original_released_receipt_without_reviving() {
    let (store, _temporary, _note, begin) = artifact_begin_fixture().await;
    let (seal, admit) = artifact_publication_requests(&store, &begin).await;
    store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .unwrap();
    let sealed_state = store
        .note_artifact_journal_status("alice", "pages", &begin.job_id, &begin.header_digest)
        .await
        .unwrap()
        .unwrap();
    let append = artifact_append_request(
        &sealed_state,
        0,
        &begin.header_digest,
        r#"{"kind":"diff.manifest","value":{}}"#,
    );
    let ack = store
        .append_note_artifact_journal(
            "alice",
            "pages",
            &append,
            &crate::ArtifactJournalRecordCost {
                index_entries: 1,
                storage_bytes: 128,
                final_manifest: true,
            },
        )
        .await
        .unwrap();
    let original_ack = store
        .note_artifact_job_receipt("alice", "pages", &ack)
        .await
        .unwrap()
        .rpc_result(&json!(3))
        .unwrap();
    assert_eq!(original_ack["state"], "building");
    assert_eq!(original_ack["currentDigest"], append.digest);
    assert_eq!(original_ack["nextSequence"], 1);
    assert!(original_ack.get("privateArtifactRef").is_none());
    let original = store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .unwrap();
    let receipt = store
        .recover_note_artifact_receipt("alice", "pages", &begin.job_id, &begin.header_digest)
        .await
        .unwrap()
        .rpc_result(&json!(1))
        .unwrap();
    assert_eq!(receipt["kind"], "artifactLease");
    assert_eq!(receipt["artifactRef"], original.artifact_ref);
    assert_eq!(receipt["expiresAt"], begin.expires_at);
    store
        .release_note_artifact_lease("alice", "pages", &original.artifact_ref)
        .await
        .unwrap();
    let after = store
        .recover_note_artifact_receipt("alice", "pages", &begin.job_id, &begin.header_digest)
        .await
        .unwrap()
        .rpc_result(&json!(2))
        .unwrap();
    assert_eq!(after, receipt);
    assert!(store
        .read_note_artifact_journal_record("alice", "pages", &original.artifact_ref, 0)
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT released FROM note_artifact_lease")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        1
    );
    assert!(store
        .recover_note_artifact_receipt("alice", "pages", &begin.job_id, &"0".repeat(64))
        .await
        .is_err());
    for (principal, workspace) in [("mallory", "pages"), ("alice", "other")] {
        let hidden = store
            .recover_note_artifact_receipt(
                principal,
                workspace,
                &begin.job_id,
                &begin.header_digest,
            )
            .await
            .unwrap()
            .rpc_result(&json!(1))
            .unwrap();
        assert_eq!(
            hidden,
            json!({"kind":"artifactUnknown","jobId":begin.job_id,"headerDigest":begin.header_digest})
        );
    }
    store
        .abort_note_artifact_journal("alice", "pages", &admit.job_ref)
        .await
        .unwrap();
    let terminal = store
        .recover_note_artifact_receipt("alice", "pages", &begin.job_id, &begin.header_digest)
        .await
        .unwrap()
        .rpc_result(&json!(1))
        .unwrap();
    assert_eq!(terminal["state"], "aborted");
    assert_eq!(terminal["kind"], "artifactJobState");
    assert!(terminal.get("privateArtifactRef").is_none());
    assert_eq!(terminal["expiresAt"], begin.expires_at);
    capture(
        "recovery",
        &begin,
        &json!({"originalAppendAckAfterSeal":original_ack,"admitted":receipt,"afterOrdinaryRelease":after,"afterAbort":terminal,
        "internalOriginalLease":{"artifactRef":original.artifact_ref,"generation":original.generation,"expiresAtEpochMs":original.expires_at},
        "admitInput":admit,"sealInput":seal}),
    );
    store.close().await;
}

#[tokio::test]
async fn artifact_response_retention_is_not_unknown_or_renewed_authority() {
    let (store, _temporary, _note, mut begin) = artifact_begin_fixture().await;
    begin.expires_at = intent_core::iso_ms_from_now(750);
    sign_artifact_begin(&mut begin);
    let expiry = i64::try_from(
        intent_core::parse_iso(&begin.expires_at)
            .unwrap()
            .unix_timestamp_nanos()
            / 1_000_000
            + 1,
    )
    .unwrap();
    store
        .begin_note_artifact_journal("alice", "pages", &begin, expiry)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    let retained = store
        .recover_note_artifact_receipt("alice", "pages", &begin.job_id, &begin.header_digest)
        .await
        .unwrap()
        .rpc_result(&json!(1))
        .unwrap();
    assert_eq!(retained["kind"], "artifactJobState");
    assert_eq!(retained["state"], "expired");
    assert!(retained.get("privateArtifactRef").is_none());
    assert_eq!(retained["expiresAt"], begin.expires_at);
    let unknown = store
        .recover_note_artifact_receipt(
            "alice",
            "pages",
            "never-authoritatively-recorded",
            &begin.header_digest,
        )
        .await
        .unwrap()
        .rpc_result(&json!(1))
        .unwrap();
    assert_eq!(unknown["kind"], "artifactUnknown");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_lease")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT sum(jobs_reserved) FROM note_artifact_capacity")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        3
    );
    store.close().await;
}

fn capture(name: &str, begin: &intent_core::note_artifact::request::ArtifactBegin, result: &Value) {
    let Ok(directory) = std::env::var("ARTIFACT_RESPONSE_CAPTURE_DIR") else {
        return;
    };
    let path = std::path::Path::new(&directory).join(format!("{name}.json"));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"boundary":"Actual Store-generated prepared DTO. No public RPC/profile/physical authority. Signed references are historical test evidence, not live construction grants.","beginInput":begin,"result":result})).unwrap();
}

#[tokio::test]
async fn artifact_response_waits_for_physical_transaction_before_terminal_disclosure() {
    let (store, _temporary, _note, begin) = artifact_begin_fixture().await;
    let (seal, admit) = artifact_publication_requests(&store, &begin).await;
    store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .unwrap();
    store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .unwrap();
    let store = Arc::new(store);
    let mut held = store.artifact_pool().unwrap().begin().await.unwrap();
    sqlx::query("UPDATE note_artifact_job SET state='aborted'")
        .execute(&mut *held)
        .await
        .unwrap();
    let other = store.clone();
    let query = begin.clone();
    let mut pending = tokio::spawn(async move {
        other
            .recover_note_artifact_receipt("alice", "pages", &query.job_id, &query.header_digest)
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    held.commit().await.unwrap();
    let response = pending
        .await
        .unwrap()
        .unwrap()
        .rpc_result(&json!(1))
        .unwrap();
    assert_eq!(response["state"], "aborted");
    assert_eq!(response["kind"], "artifactJobState");
    assert!(response.get("privateArtifactRef").is_none());
    capture(
        "held-transaction",
        &begin,
        &json!({"afterPhysicalCommit":response}),
    );
    store.close().await;
}

#[tokio::test]
async fn artifact_response_retention_covers_exact_fractional_expiry() {
    let seconds = i64::try_from(intent_core::now_epoch_ms() / 1000).unwrap() + 30;
    let base = intent_core::iso_from_unix_secs(seconds);
    let mut observations = Vec::new();
    for suffix in [
        ".123456789Z",
        ".123456789+00:00",
        ".123Z",
        ".123000000+00:00",
    ] {
        let (store, _temporary, _note, mut request) = artifact_begin_fixture().await;
        request.expires_at = base.trim_end_matches('Z').to_owned() + suffix;
        sign_artifact_begin(&mut request);
        let nanos = intent_core::parse_iso(&request.expires_at)
            .unwrap()
            .unix_timestamp_nanos();
        let minimum = i64::try_from((nanos + 999_999) / 1_000_000).unwrap();
        assert!(
            store
                .begin_note_artifact_journal("alice", "pages", &request, minimum - 1)
                .await
                .is_err(),
            "retention before exact fractional expiry was accepted: {}",
            request.expires_at
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_job")
                .fetch_one(store.artifact_pool().unwrap())
                .await
                .unwrap(),
            0
        );
        let original = store
            .begin_note_artifact_journal("alice", "pages", &request, minimum)
            .await
            .unwrap();
        assert_eq!(original.status_until, minimum);
        assert_eq!(original.expires_at, minimum);
        let public = store
            .note_artifact_job_receipt("alice", "pages", &original)
            .await
            .unwrap()
            .rpc_result(&json!(1))
            .unwrap();
        assert_eq!(public["expiresAt"], request.expires_at);
        assert_eq!(public["headerDigest"], request.header_digest);
        assert!(
            intent_core::parse_iso(public["statusUntil"].as_str().unwrap())
                .unwrap()
                .unix_timestamp_nanos()
                >= nanos
        );
        let replay = store
            .begin_note_artifact_journal("alice", "pages", &request, minimum + 1)
            .await
            .unwrap();
        assert_eq!(
            replay.status_until, minimum,
            "retry must not extend original retention"
        );
        let replay = store
            .note_artifact_job_receipt("alice", "pages", &replay)
            .await
            .unwrap()
            .rpc_result(&json!(1))
            .unwrap();
        assert_eq!(public, replay);
        // Equal instants with another accepted spelling still have distinct digest identities.
        let mut alternate = request.clone();
        alternate.expires_at = if suffix.ends_with('Z') {
            request.expires_at.trim_end_matches('Z').to_owned() + "+00:00"
        } else {
            request.expires_at.trim_end_matches("+00:00").to_owned() + "Z"
        };
        sign_artifact_begin(&mut alternate);
        assert_eq!(
            intent_core::parse_iso(&alternate.expires_at)
                .unwrap()
                .unix_timestamp_nanos(),
            nanos
        );
        assert_ne!(alternate.header_digest, request.header_digest);
        assert!(store
            .begin_note_artifact_journal("alice", "pages", &alternate, minimum)
            .await
            .is_err());
        // The existing UTC-only request policy is not widened for equivalent nonzero offsets.
        let shifted = intent_core::iso_from_unix_secs(seconds + 3600);
        alternate.expires_at = shifted.trim_end_matches('Z').to_owned()
            + suffix.trim_end_matches('Z').trim_end_matches("+00:00")
            + "+01:00";
        sign_artifact_begin(&mut alternate);
        assert_eq!(
            intent_core::parse_iso(&alternate.expires_at)
                .unwrap()
                .unix_timestamp_nanos(),
            nanos
        );
        assert!(alternate.validate("pages").is_err());
        observations.push(
            json!({"beginInput":request,"minimumRetentionEpochMs":minimum,"preparedResult":public}),
        );
        store.close().await;
    }
    assert_eq!(
        intent_core::parse_iso(observations[0]["beginInput"]["expiresAt"].as_str().unwrap()),
        intent_core::parse_iso(observations[1]["beginInput"]["expiresAt"].as_str().unwrap())
    );
    assert_eq!(
        intent_core::parse_iso(observations[2]["beginInput"]["expiresAt"].as_str().unwrap()),
        intent_core::parse_iso(observations[3]["beginInput"]["expiresAt"].as_str().unwrap())
    );
}
