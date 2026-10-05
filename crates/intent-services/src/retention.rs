//! Per-tick body of the daemon's retention/compaction sweep (§10.2 /
//! finding F4). The `intentd` binary owns the timer and the pool
//! maintenance (`incremental_vacuum` / `optimize`); the sweeps themselves
//! live here so a single tick can be driven directly in tests with a fixed
//! `now` and a live [`SettingsFile`] snapshot.

use intent_core::settings_file::SettingsFile;
use intent_core::Result;
use intent_store::Store;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

/// Fixed TTL for persisted `agent:tool:call` events, swept on the same tick as
/// the ephemeral families. Tool calls are the dominant share of the event
/// table (87% of live data on the dev seat) and no consumer reads them beyond
/// bounded recent windows — replay uses `agent_message`, live streaming uses
/// the in-memory bus — so 6h comfortably covers every durable reader
/// (`event.agentActivity` / `event.workspaceSummary` default to ≤60-minute
/// windows) while capping steady-state storage at a quarter of the old 24h.
pub const TOOL_CALL_RETENTION_HOURS: u32 = 6;

/// What one [`run_retention_tick`] did. Each sweep is independent: a failed
/// sweep is logged and counted as `0`, never aborts the tick.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionTickOutcome {
    /// Ephemeral events removed by `Store::delete_ephemeral_events_before`.
    pub ephemeral_events_removed: u64,
    /// `agent:tool:call` events removed by `Store::delete_tool_call_events_before`.
    pub tool_call_events_removed: u64,
    /// Full tool bodies compacted into `*_replay` previews.
    pub tool_payloads_compacted: u64,
    /// Logical operation/root rows removed by one bounded reclamation batch.
    pub note_operations: intent_store::NoteOperationReclaimStats,
}

fn iso_before(now: OffsetDateTime, ago: Duration) -> String {
    (now - ago).format(&Rfc3339).unwrap_or_default()
}

/// One tool-payload retention step: reads `agents.toolPayloadRetentionDays`
/// and `agents.historyReplayToolContentChars` from `settings` (the caller
/// passes the LIVE registry snapshot, so a value changed from the Settings UI
/// applies on the next tick without a restart) and, when the window is
/// `> 0`, compacts full tool bodies whose message is older than `now − days`
/// into replay previews via `Store::compact_tool_payloads_before`. Returns
/// the number of rows compacted; `Ok(0)` without touching the store when the
/// sweep is disabled (`0` days).
///
/// # Errors
///
/// Propagates the store error when the compaction fails.
pub async fn run_tool_payload_compaction_tick(
    store: &Store,
    settings: &SettingsFile,
    now: OffsetDateTime,
) -> Result<u64> {
    let Some(days) = crate::settings::tool_payload_retention_days(settings) else {
        return Ok(0);
    };
    let replay_chars = crate::settings::history_replay_tool_content_chars(settings);
    let cutoff = iso_before(now, Duration::days(i64::from(days)));
    let compacted = store
        .compact_tool_payloads_before(&cutoff, replay_chars)
        .await?;
    if compacted > 0 {
        tracing::info!(
            compacted,
            cutoff,
            retention_days = days,
            replay_chars,
            "tool-payload retention sweep compacted full tool bodies into replay previews"
        );
    }
    Ok(compacted)
}

/// [`run_retention_tick_at`] with `now = OffsetDateTime::now_utc()` — the
/// entry point the daemon's retention loop calls each tick.
pub async fn run_retention_tick(
    store: &Store,
    stream_retention_hours: u32,
    settings: &SettingsFile,
) -> RetentionTickOutcome {
    run_retention_tick_at(
        store,
        stream_retention_hours,
        settings,
        OffsetDateTime::now_utc(),
    )
    .await
}

/// One full retention tick at a fixed `now`. When `stream_retention_hours > 0`
/// it deletes high-volume ephemeral events older than that TTL plus
/// `agent:tool:call` events older than [`TOOL_CALL_RETENTION_HOURS`]; `0`
/// disables ONLY the event sweeps. The tool-payload compaction
/// ([`run_tool_payload_compaction_tick`]) runs on every tick regardless, so
/// `agents.toolPayloadRetentionDays` takes effect even on a seat with the
/// event sweep turned off. Every sweep failure is logged and the tick
/// continues with the next sweep.
pub async fn run_retention_tick_at(
    store: &Store,
    stream_retention_hours: u32,
    settings: &SettingsFile,
    now: OffsetDateTime,
) -> RetentionTickOutcome {
    let mut outcome = RetentionTickOutcome::default();
    // This sweep also runs when event retention is disabled. One transaction
    // removes at most 64 operation children and 64 unpinned source pieces;
    // persisted queue progress lets the next tick resume after interruption.
    match u64::try_from(now.unix_timestamp_nanos() / 1_000_000) {
        Ok(now_ms) => match store.reclaim_note_operations_batch(now_ms).await {
            Ok(stats) => outcome.note_operations = stats,
            Err(error) => tracing::warn!(%error, "note operation retention sweep failed"),
        },
        Err(error) => tracing::warn!(%error, "invalid note operation retention time"),
    }
    if stream_retention_hours > 0 {
        let cutoff = iso_before(now, Duration::hours(i64::from(stream_retention_hours)));
        match store.delete_ephemeral_events_before(&cutoff).await {
            Ok(removed) => {
                if removed > 0 {
                    tracing::info!(
                        removed,
                        cutoff,
                        "event retention sweep trimmed ephemeral events"
                    );
                }
                outcome.ephemeral_events_removed = removed;
            }
            Err(e) => tracing::warn!(error = %e, "event retention sweep failed"),
        }
        let tool_cutoff = iso_before(now, Duration::hours(i64::from(TOOL_CALL_RETENTION_HOURS)));
        match store.delete_tool_call_events_before(&tool_cutoff).await {
            Ok(removed) => {
                if removed > 0 {
                    tracing::info!(
                        removed,
                        cutoff = tool_cutoff,
                        "event retention sweep trimmed agent:tool:call events"
                    );
                }
                outcome.tool_call_events_removed = removed;
            }
            Err(e) => tracing::warn!(error = %e, "tool-call retention sweep failed"),
        }
    }
    match run_tool_payload_compaction_tick(store, settings, now).await {
        Ok(compacted) => outcome.tool_payloads_compacted = compacted,
        Err(e) => tracing::warn!(error = %e, "tool-payload retention sweep failed"),
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::{run_retention_tick_at, OffsetDateTime, SettingsFile, Store};

    #[tokio::test]
    async fn note_operation_reclamation_runs_with_event_retention_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("retention.db")).await.unwrap();
        for (key, until) in [("expired", 2_i64), ("keeper", 100)] {
            sqlx::query("INSERT INTO note_operation(operation_key,principal,backend_id,workspace_id,note_id,instance_id,operation_id,payload_digest,admission_expires,retain_until,outcome) VALUES(?,'p','b','w','n','i',?,'digest',1,?,'{}')")
                .bind(key).bind(key).bind(until).execute(store.write_pool()).await.unwrap();
            for sequence in 0..130 {
                sqlx::query("INSERT INTO note_operation_item(operation_key,kind,sequence,value) VALUES(?,'effects',?,'{}')")
                    .bind(key).bind(sequence).execute(store.write_pool()).await.unwrap();
            }
        }
        let settings = SettingsFile::default();
        let now = OffsetDateTime::from_unix_timestamp(3).unwrap();
        let mut removed = 0;
        for _ in 0..32 {
            let outcome = run_retention_tick_at(&store, 0, &settings, now).await;
            assert_eq!(outcome.ephemeral_events_removed, 0);
            assert_eq!(outcome.tool_call_events_removed, 0);
            assert!(outcome.note_operations.child_rows <= 64);
            assert!(outcome.note_operations.root_pieces <= 64);
            removed += outcome.note_operations.operations;
            if removed != 0 {
                break;
            }
        }
        assert_eq!(removed, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM note_operation WHERE operation_key='keeper'"
            )
            .fetch_one(store.read_pool())
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM note_operation_item WHERE operation_key='keeper'"
            )
            .fetch_one(store.read_pool())
            .await
            .unwrap(),
            130
        );
        store.close().await;
    }
}
