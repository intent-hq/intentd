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
