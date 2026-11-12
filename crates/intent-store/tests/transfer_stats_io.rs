//! `Store::transfer_table_stats` must size a workspace without reading its
//! large values (intent-hq/intent#9144).
//!
//! `workspace.transfer.plan` took 25 s on a 10 GB database because the size
//! estimate loaded every TEXT and BLOB in full, so `SQLite` read every overflow
//! page of the export. The test counts the bytes this process reads from disk
//! (`rchar` in `/proc/self/io`), so the budget is deterministic, not
//! wall-clock. It is the only test in this binary: other tests' reads would
//! otherwise count too. Linux only, because `/proc/self/io` is Linux only.
#![cfg(target_os = "linux")]

use intent_core::WorkspaceId;
use intent_store::Store;

const ROWS: i64 = 200;
const VALUE_BYTES: i64 = 32 * 1024;

/// Bytes this process has read through `read`-family syscalls so far.
#[track_caller]
fn process_read_bytes() -> i64 {
    let io = std::fs::read_to_string("/proc/self/io").expect("read /proc/self/io");
    io.lines()
        .find_map(|line| line.strip_prefix("rchar:"))
        .and_then(|value| value.trim().parse().ok())
        .expect("rchar line in /proc/self/io")
}

async fn exec(store: &Store, sql: &str) {
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(store.write_pool())
        .await
        .unwrap_or_else(|e| panic!("seed failed: {sql}: {e}"));
}

#[tokio::test]
async fn transfer_table_stats_skips_large_value_pages() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&dir.path().join("store.db"))
        .await
        .expect("open store");
    let t = "2026-01-01T00:00:00Z";
    exec(
        &store,
        &format!(
            "INSERT INTO workspace (id, title, branch, created_at, updated_at) \
             VALUES ('ws', 'T', 'main', '{t}', '{t}')"
        ),
    )
    .await;
    exec(
        &store,
        &format!(
            "INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) \
             VALUES ('agent', 'ws', 'A', 'idle', '{t}', '{t}')"
        ),
    )
    .await;
    // Large TEXT in `agent_message.content` and large BLOB in
    // `agent_message_payload.body`: both storage classes the export carries.
    exec(
        &store,
        &format!(
            "WITH RECURSIVE k(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM k WHERE n < {ROWS}) \
             INSERT INTO agent_message \
             (id, agent_id, seq, role, content, created_at, usage_model, usage_origin) \
             SELECT 'm-' || n, 'agent', n, 'user', hex(zeroblob({VALUE_BYTES} / 2)), \
             '{t}', 'test-model', 'human' FROM k"
        ),
    )
    .await;
    exec(
        &store,
        &format!(
            "INSERT INTO agent_message_payload \
             (message_id, agent_id, block_ordinal, kind, encoding, body) \
             SELECT id, agent_id, 0, 'tool_result_output', 'none', zeroblob({VALUE_BYTES}) \
             FROM agent_message"
        ),
    )
    .await;

    let before = process_read_bytes();
    let stats = store
        .transfer_table_stats(&WorkspaceId("ws".to_string()))
        .await
        .expect("stats");
    let read = process_read_bytes() - before;

    for table in ["agent_message", "agent_message_payload"] {
        let stat = stats.iter().find(|s| s.name == table).expect(table);
        assert_eq!(stat.row_count, ROWS, "{table} row count stays exact");
        assert!(
            stat.approx_bytes >= ROWS * VALUE_BYTES,
            "{table} estimate still counts the large values: {}",
            stat.approx_bytes
        );
    }
    // Loading the values reads all of them; the leaf pages holding the record
    // headers that carry their lengths are a small fraction of that.
    let value_bytes = 2 * ROWS * VALUE_BYTES;
    assert!(
        read < value_bytes / 4,
        "transfer stats read {read} bytes from disk; the large values total {value_bytes}"
    );
    store.close().await;
}
