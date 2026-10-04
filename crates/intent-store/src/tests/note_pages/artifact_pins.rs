use super::*;

async fn source_pressure(store: &Store) {
    for _ in 0..256 {
        page(store, json!({"kind":"source"})).await;
    }
}

#[tokio::test]
async fn artifact_generation_pin_survives_capacity_until_consumer_release() {
    let (store, _temporary, _note, begin) = artifact_begin_fixture().await;
    let (seal, admit) = artifact_publication_requests(&store, &begin).await;
    for _ in 0..3 {
        store
            .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
            .await
            .unwrap();
    }
    source_pressure(&store).await;
    store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .expect("building generation lost its source under capacity pressure");
    source_pressure(&store).await;
    let lease = store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .expect("sealed generation lost its source under capacity pressure");
    for _ in 0..3 {
        assert_eq!(
            store
                .admit_note_artifact_journal("alice", "pages", &admit)
                .await
                .unwrap(),
            lease
        );
    }
    drop(begin);
    drop(seal);
    source_pressure(&store).await;
    assert!(store
        .read_note_artifact_journal_record("alice", "pages", &lease.artifact_ref, 0)
        .await
        .unwrap()
        .is_some());
    store
        .release_note_artifact_lease("alice", "pages", &lease.artifact_ref)
        .await
        .unwrap();
    // Historical receipt replay is not a new source owner.
    assert_eq!(
        store
            .admit_note_artifact_journal("alice", "pages", &admit)
            .await
            .unwrap(),
        lease
    );
    source_pressure(&store).await;
    assert!(store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .is_err());
}

#[tokio::test]
async fn artifact_generation_pin_abort_and_replay_do_not_retain_source() {
    let (store, _temporary, _note, begin) = artifact_begin_fixture().await;
    let state = store
        .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
        .await
        .unwrap();
    store
        .abort_note_artifact_journal("alice", "pages", &state.job_ref)
        .await
        .unwrap();
    let replay = store
        .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
        .await
        .unwrap();
    assert_eq!(replay.state, "aborted");
    source_pressure(&store).await;
    assert!(store
        .authorize_note_artifact_source("pages", "alice", &begin.header)
        .await
        .is_err());
}

#[tokio::test]
async fn artifact_generation_pin_capacity_is_finite_and_retirement_frees_slot() {
    let (store, _temporary, _note, mut begin) = artifact_begin_fixture().await;
    sqlx::query("UPDATE note_artifact_capacity SET payload_limit=1048576,record_limit=2048,index_limit=2048,storage_limit=4194304,job_limit=1024")
        .execute(store.artifact_pool().unwrap()).await.unwrap();
    let mut first = None;
    for job in 0..256 {
        begin.job_id = format!("job-{job}");
        sign_artifact_begin(&mut begin);
        let state = store
            .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
            .await
            .unwrap();
        if first.is_none() {
            first = Some(state);
        }
    }
    // Identical replay needs no additional slot even at capacity.
    store
        .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
        .await
        .unwrap();
    begin.job_id = "over-capacity".into();
    sign_artifact_begin(&mut begin);
    assert!(matches!(
        store
            .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
            .await,
        Err(Error::NotePage(NotePageError::Budget))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_artifact_job")
            .fetch_one(store.artifact_pool().unwrap())
            .await
            .unwrap(),
        256
    );
    store
        .abort_note_artifact_journal("alice", "pages", &first.unwrap().job_ref)
        .await
        .unwrap();
    store
        .begin_note_artifact_journal("alice", "pages", &begin, artifact_retention(&begin))
        .await
        .unwrap();
}

#[tokio::test]
async fn artifact_generation_pin_original_deadline_retires_authority_and_owner() {
    let (store, _temporary, _note, mut begin) = artifact_begin_fixture().await;
    begin.expires_at = intent_core::iso_ms_from_now(2000);
    sign_artifact_begin(&mut begin);
    let (seal, admit) = artifact_publication_requests(&store, &begin).await;
    let state = store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .unwrap();
    let lease = store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .unwrap();
    // Wait for the actual admitted deadline, not a guessed scheduling delay.
    let remaining = u64::try_from(state.expires_at)
        .unwrap()
        .saturating_sub(intent_core::now_epoch_ms());
    tokio::time::sleep(std::time::Duration::from_millis(remaining + 1)).await;
    assert!(store
        .read_note_artifact_journal_record("alice", "pages", &lease.artifact_ref, 0)
        .await
        .is_err());
    assert_eq!(
        store.expire_note_artifact_journals(1).await.unwrap(),
        vec![state.generation]
    );
    source_pressure(&store).await;
    assert!(store
        .authorize_note_artifact_source("pages", "alice", &begin.header)
        .await
        .is_err());
}

#[tokio::test]
async fn artifact_generation_pin_cancelled_release_waits_for_physical_commit() {
    let (store, _temporary, _note, begin) = artifact_begin_fixture().await;
    let (seal, admit) = artifact_publication_requests(&store, &begin).await;
    store
        .seal_note_artifact_journal("alice", "pages", &seal)
        .await
        .unwrap();
    let lease = store
        .admit_note_artifact_journal("alice", "pages", &admit)
        .await
        .unwrap();
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let mut reached_tx = Some(reached_tx);
    let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            if let Some(sender) = reached_tx.take() {
                let _ = sender.send(());
                return resume_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .is_ok();
            }
            true
        });
    drop(connection);
    let mut release =
        Box::pin(store.release_note_artifact_lease("alice", "pages", &lease.artifact_ref));
    let reached = tokio::select! {
        result = tokio::time::timeout(std::time::Duration::from_secs(10), reached_rx) => matches!(result, Ok(Ok(()))),
        _ = &mut release => false,
    };
    drop(release);
    assert!(reached);
    source_pressure(&store).await;
    let pinned = store
        .authorize_note_artifact_source("pages", "alice", &begin.header)
        .await;
    let _ = resume_tx.send(());
    assert!(
        pinned.is_ok(),
        "source retired before cancelled physical commit settled"
    );
    drop(pinned);
    let mut connection = store.artifact_pool().unwrap().acquire().await.unwrap();
    connection.lock_handle().await.unwrap().remove_commit_hook();
    drop(connection);
    // Repeated release waits for and confirms the original retirement as well.
    store
        .release_note_artifact_lease("alice", "pages", &lease.artifact_ref)
        .await
        .unwrap();
    source_pressure(&store).await;
    assert!(store
        .authorize_note_artifact_source("pages", "alice", &begin.header)
        .await
        .is_err());
}
