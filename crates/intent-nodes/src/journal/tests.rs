use super::*;

fn entry(text: &str) -> JournalEntry {
    JournalEntry {
        agent_id: AgentId::from("agent-1"),
        workspace_id: WorkspaceId::from("ws-1"),
        turn_id: Some("turn-1".into()),
        recorded_at: "2026-10-27T00:00:00Z".into(),
        body: JournalBody::Notification {
            notification: serde_json::json!({ "text": text }),
        },
    }
}

fn seqs(records: &[JournalRecord]) -> Vec<u64> {
    records.iter().map(|r| r.seq.0).collect()
}

fn record_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(RECORD_SUFFIX))
        .collect();
    names.sort();
    names
}

#[test]
fn append_assigns_contiguous_sequences_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = Journal::open(dir.path()).unwrap();
    journal.use_lease("lease-1").unwrap();
    assert_eq!(journal.append(entry("a")).unwrap().seq, NodeCounter(1));
    assert_eq!(journal.append(entry("b")).unwrap().seq, NodeCounter(2));
    drop(journal);

    let journal = Journal::open(dir.path()).unwrap();
    assert_eq!(journal.last_seq(), NodeCounter(2));
    assert_eq!(journal.lease_id(), Some("lease-1"));
    let records = journal.records_after(NodeCounter(0)).unwrap();
    assert_eq!(seqs(&records), vec![1, 2]);
    assert_eq!(records[1].body, entry("b").body);
    assert_eq!(records[1].turn_id.as_deref(), Some("turn-1"));
}

#[test]
fn acknowledge_deletes_acked_records_and_replay_starts_after_ack() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = Journal::open(dir.path()).unwrap();
    journal.use_lease("lease-1").unwrap();
    for text in ["a", "b", "c"] {
        journal.append(entry(text)).unwrap();
    }
    journal.acknowledge(NodeCounter(2)).unwrap();
    assert_eq!(record_files(dir.path()), vec![record_name(3)]);
    // A replay from an older head ack still starts after the durable ack.
    assert_eq!(
        seqs(&journal.records_after(NodeCounter(0)).unwrap()),
        vec![3]
    );
    // An ack at or below the current one is a no-op.
    journal.acknowledge(NodeCounter(1)).unwrap();
    assert_eq!(journal.acked_seq(), NodeCounter(2));
    drop(journal);

    let mut journal = Journal::open(dir.path()).unwrap();
    assert_eq!(journal.acked_seq(), NodeCounter(2));
    assert_eq!(journal.last_seq(), NodeCounter(3));
    assert_eq!(journal.append(entry("d")).unwrap().seq, NodeCounter(4));
}

#[test]
fn acknowledge_past_last_record_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = Journal::open(dir.path()).unwrap();
    journal.use_lease("lease-1").unwrap();
    journal.append(entry("a")).unwrap();
    let error = journal.acknowledge(NodeCounter(5)).unwrap_err();
    assert!(
        error.to_string().contains("node-journal-ack-ahead"),
        "{error}"
    );
    assert_eq!(journal.acked_seq(), NodeCounter(0));
}

#[test]
fn a_new_lease_restarts_the_sequence_and_drops_old_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = Journal::open(dir.path()).unwrap();
    assert!(journal.use_lease("lease-1").unwrap());
    assert!(!journal.use_lease("lease-1").unwrap());
    journal.append(entry("a")).unwrap();
    journal.append(entry("b")).unwrap();
    journal.acknowledge(NodeCounter(1)).unwrap();

    assert!(journal.use_lease("lease-2").unwrap());
    assert_eq!(journal.last_seq(), NodeCounter(0));
    assert_eq!(journal.acked_seq(), NodeCounter(0));
    assert!(record_files(dir.path()).is_empty());
    assert_eq!(journal.append(entry("c")).unwrap().seq, NodeCounter(1));
    drop(journal);

    let journal = Journal::open(dir.path()).unwrap();
    assert_eq!(journal.lease_id(), Some("lease-2"));
    assert_eq!(
        seqs(&journal.records_after(NodeCounter(0)).unwrap()),
        vec![1]
    );
}

#[test]
fn open_discards_pending_writes_and_leftover_acked_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = Journal::open(dir.path()).unwrap();
    journal.use_lease("lease-1").unwrap();
    journal.append(entry("a")).unwrap();
    journal.append(entry("b")).unwrap();
    let acked = fs::read(dir.path().join(record_name(1))).unwrap();
    journal.acknowledge(NodeCounter(1)).unwrap();
    drop(journal);
    // A crash between saving the ack and deleting the record, and a crash
    // mid-append.
    fs::write(dir.path().join(record_name(1)), acked).unwrap();
    fs::write(
        dir.path()
            .join(format!("{}{PENDING_SUFFIX}", record_name(3))),
        b"partial",
    )
    .unwrap();

    let journal = Journal::open(dir.path()).unwrap();
    assert_eq!(journal.last_seq(), NodeCounter(2));
    assert_eq!(record_files(dir.path()), vec![record_name(2)]);
    assert!(!dir
        .path()
        .join(format!("{}{PENDING_SUFFIX}", record_name(3)))
        .exists());
}

#[test]
fn open_refuses_a_gap_or_a_corrupt_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = Journal::open(dir.path()).unwrap();
    journal.use_lease("lease-1").unwrap();
    for text in ["a", "b", "c"] {
        journal.append(entry(text)).unwrap();
    }
    drop(journal);
    fs::remove_file(dir.path().join(record_name(2))).unwrap();
    let error = Journal::open(dir.path()).unwrap_err();
    assert!(
        error.to_string().contains("node-journal-corrupt"),
        "{error}"
    );

    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join(STATE), b"{not json").unwrap();
    let error = Journal::open(dir.path()).unwrap_err();
    assert!(
        error.to_string().contains("node-journal-corrupt"),
        "{error}"
    );
}

#[cfg(unix)]
#[test]
fn journal_files_are_private() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let mut journal = Journal::open(dir.path()).unwrap();
    journal.use_lease("lease-1").unwrap();
    journal.append(entry("secret output")).unwrap();
    for name in [STATE.to_owned(), record_name(1)] {
        let mode = fs::metadata(dir.path().join(&name))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "{name} mode {mode:o}");
    }
}
