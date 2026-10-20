//! Per-tick body of the daemon's retention/compaction sweep (§10.2 /
//! finding F4). The `intentd` binary owns the timer and `PRAGMA optimize`;
//! the sweeps and the time-paced freelist drain
//! ([`run_incremental_vacuum_drain`]) live here so a single tick can be
//! driven directly in tests with a fixed `now` and a live [`SettingsFile`]
//! snapshot.

use intent_core::settings_file::SettingsFile;
use intent_core::Result;
use intent_store::Store;
use std::time::Duration as StdDuration;
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

/// Longest one `PRAGMA incremental_vacuum` chunk should hold the
/// single-connection write pool once its per-page cost is measured
/// (intent-hq/intent#7448: a fixed 2000-page step held it for 8–13 s on a
/// 33 GB database, timing out queued writes at the 10 s acquire deadline).
pub const VACUUM_CHUNK_HOLD_BUDGET: StdDuration = StdDuration::from_millis(250);

/// First chunk of every drain, sized before any measurement: ≈225 ms even at
/// the worst observed 7 ms/page.
pub const VACUUM_PROBE_CHUNK_PAGES: u32 = 32;

/// Smallest paced chunk — keeps progress when a single page is very slow.
pub const VACUUM_MIN_CHUNK_PAGES: u32 = 8;

/// Largest paced chunk (≈8 MiB at 4 KiB pages) — the old fixed step, which
/// small or fast databases still reach.
pub const VACUUM_MAX_CHUNK_PAGES: u32 = 2000;

/// Floor on the pause between chunks; the pause is otherwise the previous
/// chunk's own duration, capping the drain's writer duty cycle at ≈50%.
pub const VACUUM_MIN_PAUSE: StdDuration = StdDuration::from_millis(50);

/// Share of the retention tick interval a drain may run for (wall time,
/// pauses included): 1/6 — 10 minutes on the default hourly tick.
pub const VACUUM_DRAIN_TICK_DIVISOR: u32 = 6;

/// A chunk never grows past this multiple of the previous one, so one fast
/// (cache-warm) chunk cannot trigger a huge slow one.
const VACUUM_CHUNK_GROWTH_CAP: u32 = 2;

/// Pacing knobs for [`run_incremental_vacuum_drain`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VacuumDrainPolicy {
    /// Target hold per chunk ([`VACUUM_CHUNK_HOLD_BUDGET`]).
    pub hold_budget: StdDuration,
    /// Size of the first, unmeasured chunk ([`VACUUM_PROBE_CHUNK_PAGES`]).
    pub probe_chunk_pages: u32,
    /// Lower chunk-size clamp ([`VACUUM_MIN_CHUNK_PAGES`]).
    pub min_chunk_pages: u32,
    /// Upper chunk-size clamp ([`VACUUM_MAX_CHUNK_PAGES`]).
    pub max_chunk_pages: u32,
    /// Floor on the pause between chunks ([`VACUUM_MIN_PAUSE`]).
    pub min_pause: StdDuration,
    /// Wall-time budget for the whole drain, pauses included.
    pub drain_budget: StdDuration,
}

impl VacuumDrainPolicy {
    /// The production policy for a retention loop ticking every
    /// `tick_interval`: default chunk pacing and a drain budget of
    /// `tick_interval / VACUUM_DRAIN_TICK_DIVISOR`.
    #[must_use]
    pub fn for_tick_interval(tick_interval: StdDuration) -> Self {
        Self {
            hold_budget: VACUUM_CHUNK_HOLD_BUDGET,
            probe_chunk_pages: VACUUM_PROBE_CHUNK_PAGES,
            min_chunk_pages: VACUUM_MIN_CHUNK_PAGES,
            max_chunk_pages: VACUUM_MAX_CHUNK_PAGES,
            min_pause: VACUUM_MIN_PAUSE,
            drain_budget: tick_interval / VACUUM_DRAIN_TICK_DIVISOR,
        }
    }
}

/// Next chunk size for a measured `ms_per_page`: the largest page count whose
/// projected hold (`pages × ms_per_page`) fits `hold_budget`, clamped to
/// `[min_pages, max_pages]` and to at most [`VACUUM_CHUNK_GROWTH_CAP`] × the
/// `previous` chunk. A non-positive or non-finite cost (a chunk too fast to
/// time) grows by the cap. Only the `min_pages` floor can project past the
/// budget — when a single page costs more than `hold_budget / min_pages`.
/// The result is never 0 (`min_pages` is floored at 1): `SQLite` reads
/// `PRAGMA incremental_vacuum(0)` as "the whole freelist".
#[must_use]
pub fn vacuum_chunk_pages(
    ms_per_page: f64,
    hold_budget: StdDuration,
    previous: u32,
    min_pages: u32,
    max_pages: u32,
) -> u32 {
    let min_pages = min_pages.max(1);
    let max_pages = max_pages.max(min_pages);
    let ceiling = previous
        .max(1)
        .saturating_mul(VACUUM_CHUNK_GROWTH_CAP)
        .clamp(min_pages, max_pages);
    if !(ms_per_page.is_finite() && ms_per_page > 0.0) {
        return ceiling;
    }
    let budget_ms = hold_budget.as_secs_f64() * 1000.0;
    let fit = (budget_ms / ms_per_page).floor();
    // `fit` is non-negative and finite here, and float-to-int `as` casts
    // saturate, so a huge fit becomes `u32::MAX` before the clamp.
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let fit = fit as u32;
    fit.clamp(min_pages, ceiling)
}

/// Chunk-size state for one drain: starts at the probe size and resizes each
/// chunk from a per-page cost estimate that tracks slowdowns immediately and
/// speedups gradually (asymmetric smoothing), so the hold stays under budget
/// when the page cache goes cold mid-drain.
#[derive(Debug, Clone)]
pub struct VacuumPacer {
    policy: VacuumDrainPolicy,
    next_pages: u32,
    ms_per_page: Option<f64>,
}

impl VacuumPacer {
    #[must_use]
    pub fn new(policy: VacuumDrainPolicy) -> Self {
        let min_pages = policy.min_chunk_pages.max(1);
        let next_pages = policy
            .probe_chunk_pages
            .clamp(min_pages, policy.max_chunk_pages.max(min_pages));
        Self {
            policy,
            next_pages,
            ms_per_page: None,
        }
    }

    /// Pages to request in the next chunk.
    #[must_use]
    pub fn next_chunk_pages(&self) -> u32 {
        self.next_pages
    }

    /// Current per-page cost estimate in milliseconds, once measured.
    #[must_use]
    pub fn ms_per_page(&self) -> Option<f64> {
        self.ms_per_page
    }

    /// Projected hold of the next chunk at the current estimate (zero
    /// before the first measurement).
    #[must_use]
    pub fn projected_next_hold(&self) -> StdDuration {
        self.ms_per_page.map_or(StdDuration::ZERO, |ms| {
            StdDuration::from_secs_f64(f64::from(self.next_pages) * ms / 1000.0)
        })
    }

    /// Feed one finished chunk (`pages_freed` pages in `elapsed`) and resize
    /// the next one. A chunk that freed nothing carries no cost signal and
    /// leaves the pacer unchanged.
    pub fn record(&mut self, pages_freed: u64, elapsed: StdDuration) {
        if pages_freed == 0 {
            return;
        }
        #[expect(clippy::cast_precision_loss)]
        let sample = elapsed.as_secs_f64() * 1000.0 / pages_freed as f64;
        let estimate = match self.ms_per_page {
            Some(prev) if sample < prev => f64::midpoint(prev, sample),
            _ => sample,
        };
        self.ms_per_page = Some(estimate);
        self.next_pages = vacuum_chunk_pages(
            estimate,
            self.policy.hold_budget,
            self.next_pages,
            self.policy.min_chunk_pages,
            self.policy.max_chunk_pages,
        );
    }
}

/// Why [`run_incremental_vacuum_drain`] stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VacuumDrainStop {
    /// The freelist is empty.
    Drained,
    /// A chunk released nothing although pages remain — e.g. a database
    /// whose `auto_vacuum` is NONE, where the pragma is a no-op.
    NothingFreed,
    /// The drain budget ran out with pages still on the freelist.
    Budget,
    /// A chunk failed (logged); the next tick retries.
    Error,
}

/// What one [`run_incremental_vacuum_drain`] did.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VacuumDrainOutcome {
    /// Pages released across all chunks.
    pub pages_freed: u64,
    /// Chunks executed (including a final no-op or failed one).
    pub chunks: u32,
    /// Total time the vacuum statements held the write connection.
    pub active: StdDuration,
    /// Longest single chunk hold.
    pub max_chunk: StdDuration,
    /// Freelist pages left after the last successful chunk.
    pub freelist_remaining: Option<u64>,
    /// Final per-page cost estimate in milliseconds.
    pub ms_per_page: Option<f64>,
    /// Why the drain stopped.
    pub stop: VacuumDrainStop,
}

impl VacuumDrainOutcome {
    /// Ticks still needed to empty the freelist at this drain's rate:
    /// `Some(0)` when nothing remains, `None` when it is unknown — no pages
    /// freed, the freelist unread, or a drain cut short by an error (its
    /// partial rate would wildly overstate the estimate).
    #[must_use]
    pub fn ticks_to_drain(&self) -> Option<u64> {
        let remaining = self.freelist_remaining?;
        match self.stop {
            VacuumDrainStop::Drained => Some(0),
            VacuumDrainStop::Budget if remaining == 0 => Some(0),
            VacuumDrainStop::Budget if self.pages_freed > 0 => {
                Some(remaining.div_ceil(self.pages_freed))
            }
            _ => None,
        }
    }
}

/// Release freelist pages in time-paced chunks (intent-hq/intent#7448).
/// Each chunk is one [`Store::incremental_vacuum_chunk`] on the
/// single-connection write pool, sized by [`VacuumPacer`] to hold it for at
/// most ≈`policy.hold_budget`; between chunks the connection goes back to
/// the (FIFO-fair) pool and the drain pauses for the previous chunk's
/// duration (at least `policy.min_pause`), so writes queued during a chunk
/// run before the next one. Stops when the freelist is empty, a chunk frees
/// nothing, a chunk fails, or the next pause plus the next chunk's
/// projected hold would exceed `policy.drain_budget`.
pub async fn run_incremental_vacuum_drain(
    store: &Store,
    policy: VacuumDrainPolicy,
) -> VacuumDrainOutcome {
    let started = std::time::Instant::now();
    let mut pacer = VacuumPacer::new(policy);
    let mut chunks = 0u32;
    let mut pages_freed = 0u64;
    let mut active = StdDuration::ZERO;
    let mut max_chunk = StdDuration::ZERO;
    let mut freelist_remaining = None;
    let stop = loop {
        chunks += 1;
        let chunk = match store
            .incremental_vacuum_chunk(pacer.next_chunk_pages())
            .await
        {
            Ok(chunk) => chunk,
            Err(e) => {
                tracing::warn!(error = %e, "incremental vacuum failed");
                break VacuumDrainStop::Error;
            }
        };
        pages_freed += chunk.pages_freed;
        active += chunk.elapsed;
        max_chunk = max_chunk.max(chunk.elapsed);
        freelist_remaining = Some(chunk.freelist_remaining);
        if chunk.freelist_remaining == 0 {
            break VacuumDrainStop::Drained;
        }
        if chunk.pages_freed == 0 {
            break VacuumDrainStop::NothingFreed;
        }
        pacer.record(chunk.pages_freed, chunk.elapsed);
        let pause = chunk.elapsed.max(policy.min_pause);
        // Stop unless the pause AND the next chunk's projected hold both fit,
        // so a slow floor-sized chunk cannot overrun the budget.
        if started.elapsed() + pause + pacer.projected_next_hold() >= policy.drain_budget {
            break VacuumDrainStop::Budget;
        }
        tokio::time::sleep(pause).await;
    };
    VacuumDrainOutcome {
        pages_freed,
        chunks,
        active,
        max_chunk,
        freelist_remaining,
        ms_per_page: pacer.ms_per_page(),
        stop,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{EventActor, WorkspaceId};
    use intent_store::NewEvent;
    use sqlx::ConnectOptions;

    const BUDGET: StdDuration = StdDuration::from_millis(250);

    /// Projected hold of a chunk sized for `ms_per_page`.
    fn projected_ms(pages: u32, ms_per_page: f64) -> f64 {
        f64::from(pages) * ms_per_page
    }

    #[test]
    fn chunk_keeps_projected_hold_under_budget() {
        // The #7448 host measured 4–7 ms/page; a generous previous chunk
        // takes the growth cap out of play so only the budget binds.
        for ms_per_page in [0.01, 0.2, 1.0, 4.0, 5.5, 7.0, 13.0] {
            let pages = vacuum_chunk_pages(ms_per_page, BUDGET, 2000, 8, 2000);
            assert!(
                projected_ms(pages, ms_per_page) <= 250.0 || pages == 8,
                "{ms_per_page} ms/page -> {pages} pages projects {} ms",
                projected_ms(pages, ms_per_page)
            );
        }
        assert_eq!(vacuum_chunk_pages(5.0, BUDGET, 2000, 8, 2000), 50);
        assert_eq!(vacuum_chunk_pages(7.0, BUDGET, 2000, 8, 2000), 35);
    }

    #[test]
    fn chunk_is_clamped_to_min_and_max() {
        // Very slow pages still make progress at the floor.
        assert_eq!(vacuum_chunk_pages(100.0, BUDGET, 64, 8, 2000), 8);
        // Very fast pages stop at the old fixed 2000-page step.
        assert_eq!(vacuum_chunk_pages(0.001, BUDGET, 2000, 8, 2000), 2000);
    }

    #[test]
    fn chunk_growth_is_capped_at_twice_the_previous() {
        // 0.01 ms/page would fit 25,000 pages, but one fast chunk only
        // doubles the next.
        assert_eq!(vacuum_chunk_pages(0.01, BUDGET, 32, 8, 2000), 64);
        assert_eq!(vacuum_chunk_pages(0.01, BUDGET, 1500, 8, 2000), 2000);
    }

    #[test]
    fn chunk_is_never_zero_pages() {
        // `PRAGMA incremental_vacuum(0)` would release the whole freelist.
        assert_eq!(vacuum_chunk_pages(1_000.0, BUDGET, 64, 0, 2000), 1);
        let pacer = VacuumPacer::new(VacuumDrainPolicy {
            probe_chunk_pages: 0,
            min_chunk_pages: 0,
            max_chunk_pages: 0,
            ..VacuumDrainPolicy::for_tick_interval(StdDuration::from_secs(3600))
        });
        assert_eq!(pacer.next_chunk_pages(), 1);
    }

    #[test]
    fn projected_next_hold_tracks_estimate() {
        let mut pacer = VacuumPacer::new(VacuumDrainPolicy::for_tick_interval(
            StdDuration::from_secs(3600),
        ));
        assert_eq!(pacer.projected_next_hold(), StdDuration::ZERO);
        pacer.record(32, StdDuration::from_millis(160));
        // 5 ms/page -> 50 pages -> 250 ms.
        assert_eq!(pacer.next_chunk_pages(), 50);
        assert_eq!(pacer.projected_next_hold(), StdDuration::from_millis(250));
    }

    #[test]
    fn zero_or_invalid_cost_grows_by_the_cap() {
        assert_eq!(vacuum_chunk_pages(0.0, BUDGET, 32, 8, 2000), 64);
        assert_eq!(vacuum_chunk_pages(f64::NAN, BUDGET, 32, 8, 2000), 64);
        assert_eq!(vacuum_chunk_pages(f64::INFINITY, BUDGET, 32, 8, 2000), 64);
    }

    #[test]
    fn pacer_shrinks_on_slow_chunk_and_recovers_gradually() {
        let mut pacer = VacuumPacer::new(VacuumDrainPolicy::for_tick_interval(
            StdDuration::from_secs(3600),
        ));
        assert_eq!(pacer.next_chunk_pages(), VACUUM_PROBE_CHUNK_PAGES);
        // Fast probe: 32 pages in 3.2 ms (0.1 ms/page) -> doubles.
        pacer.record(32, StdDuration::from_micros(3200));
        assert_eq!(pacer.next_chunk_pages(), 64);
        // Cold cache: 64 pages in 448 ms (7 ms/page) -> shrinks at once to
        // what fits the budget.
        pacer.record(64, StdDuration::from_millis(448));
        assert_eq!(pacer.next_chunk_pages(), 35);
        assert!((pacer.ms_per_page().unwrap() - 7.0).abs() < 1e-9);
        // A faster chunk only halves the gap to the estimate.
        pacer.record(35, StdDuration::from_millis(35));
        assert!((pacer.ms_per_page().unwrap() - 4.0).abs() < 1e-9);
        assert_eq!(pacer.next_chunk_pages(), 62);
        // A chunk that freed nothing is not a cost signal.
        pacer.record(0, StdDuration::from_secs(5));
        assert_eq!(pacer.next_chunk_pages(), 62);
    }

    #[test]
    fn production_policy_budgets_a_sixth_of_the_tick() {
        let policy = VacuumDrainPolicy::for_tick_interval(StdDuration::from_secs(3600));
        assert_eq!(policy.drain_budget, StdDuration::from_secs(600));
        assert_eq!(policy.hold_budget, VACUUM_CHUNK_HOLD_BUDGET);
    }

    #[test]
    fn ticks_to_drain_estimate() {
        use VacuumDrainStop::{Budget, Drained, Error, NothingFreed};
        let outcome = |freed, remaining, stop| VacuumDrainOutcome {
            pages_freed: freed,
            chunks: 1,
            active: StdDuration::ZERO,
            max_chunk: StdDuration::ZERO,
            freelist_remaining: remaining,
            ms_per_page: None,
            stop,
        };
        assert_eq!(
            outcome(50_000, Some(3_883_407), Budget).ticks_to_drain(),
            Some(78)
        );
        assert_eq!(outcome(10, Some(0), Drained).ticks_to_drain(), Some(0));
        assert_eq!(outcome(0, Some(10), Budget).ticks_to_drain(), None);
        assert_eq!(outcome(0, Some(10), NothingFreed).ticks_to_drain(), None);
        assert_eq!(outcome(10, None, Error).ticks_to_drain(), None);
        // A partial rate from an errored drain is not an estimate.
        assert_eq!(outcome(32, Some(3_883_407), Error).ticks_to_drain(), None);
    }

    /// Open a store whose file already holds `blobs` × 64 KiB of deleted
    /// filler, i.e. a non-empty freelist. `incremental` picks the
    /// `auto_vacuum` mode the file is created with (it cannot change later
    /// without a full VACUUM).
    async fn store_with_freelist(dir: &tempfile::TempDir, incremental: bool, blobs: i64) -> Store {
        let path = dir.path().join("vacuum.db");
        {
            let opts = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true);
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(opts)
                .await
                .expect("open seed pool");
            let mode = if incremental { "INCREMENTAL" } else { "NONE" };
            sqlx::query(&format!("PRAGMA auto_vacuum = {mode}"))
                .execute(&pool)
                .await
                .expect("auto_vacuum");
            sqlx::query("CREATE TABLE filler (id INTEGER PRIMARY KEY, data BLOB)")
                .execute(&pool)
                .await
                .expect("create filler");
            for i in 0..blobs {
                sqlx::query("INSERT INTO filler (id, data) VALUES (?, zeroblob(65536))")
                    .bind(i)
                    .execute(&pool)
                    .await
                    .expect("insert filler");
            }
            sqlx::query("DROP TABLE filler")
                .execute(&pool)
                .await
                .expect("drop filler");
            pool.close().await;
        }
        let store = Store::open(&path).await.expect("open store");
        let freelist = store.freelist_count().await.expect("freelist");
        assert!(
            freelist > 0,
            "seed should leave a freelist (got {freelist})"
        );
        store
    }

    fn fixed_chunk_policy(pages: u32, drain_budget: StdDuration) -> VacuumDrainPolicy {
        VacuumDrainPolicy {
            hold_budget: VACUUM_CHUNK_HOLD_BUDGET,
            probe_chunk_pages: pages,
            min_chunk_pages: pages,
            max_chunk_pages: pages,
            min_pause: StdDuration::from_millis(10),
            drain_budget,
        }
    }

    #[tokio::test]
    async fn drain_empties_freelist_over_several_chunks() {
        let dir = crate::test_support::test_tempdir("vacuum-drain-");
        let store = store_with_freelist(&dir, true, 64).await;
        let before = store
            .freelist_count()
            .await
            .expect("freelist")
            .cast_unsigned();
        let outcome = run_incremental_vacuum_drain(
            &store,
            VacuumDrainPolicy::for_tick_interval(StdDuration::from_secs(3600)),
        )
        .await;
        assert_eq!(outcome.stop, VacuumDrainStop::Drained, "{outcome:?}");
        assert_eq!(outcome.pages_freed, before);
        assert_eq!(outcome.freelist_remaining, Some(0));
        assert!(
            outcome.chunks > 1,
            "probe chunk is smaller than the freelist: {outcome:?}"
        );
        assert_eq!(store.freelist_count().await.expect("freelist"), 0);
        assert_eq!(outcome.ticks_to_drain(), Some(0));
    }

    #[tokio::test]
    async fn drain_stops_at_budget_with_pages_left() {
        let dir = crate::test_support::test_tempdir("vacuum-budget-");
        let store = store_with_freelist(&dir, true, 64).await;
        let outcome =
            run_incremental_vacuum_drain(&store, fixed_chunk_policy(4, StdDuration::ZERO)).await;
        assert_eq!(outcome.stop, VacuumDrainStop::Budget, "{outcome:?}");
        assert_eq!(outcome.chunks, 1);
        assert!((1..=4).contains(&outcome.pages_freed), "{outcome:?}");
        let remaining = outcome.freelist_remaining.expect("remaining");
        assert!(remaining > 0);
        assert_eq!(
            store
                .freelist_count()
                .await
                .expect("freelist")
                .cast_unsigned(),
            remaining
        );
        assert!(outcome.ticks_to_drain().is_some_and(|t| t > 0));
    }

    #[tokio::test]
    async fn drain_stops_when_nothing_is_freed_on_non_incremental_db() {
        let dir = crate::test_support::test_tempdir("vacuum-none-");
        let store = store_with_freelist(&dir, false, 64).await;
        let outcome = run_incremental_vacuum_drain(
            &store,
            VacuumDrainPolicy::for_tick_interval(StdDuration::from_secs(3600)),
        )
        .await;
        assert_eq!(outcome.stop, VacuumDrainStop::NothingFreed, "{outcome:?}");
        assert_eq!(outcome.chunks, 1);
        assert_eq!(outcome.pages_freed, 0);
    }

    /// The point of #7448: a write issued while a drain is in progress gets
    /// the single write connection between chunks instead of waiting for
    /// the whole drain.
    #[tokio::test]
    async fn concurrent_write_completes_mid_drain() {
        let dir = crate::test_support::test_tempdir("vacuum-write-");
        let store = store_with_freelist(&dir, true, 64).await;
        let before = store.freelist_count().await.expect("freelist");
        // 4-page chunks with ≥10 ms pauses over several hundred free pages:
        // the drain runs for over a second.
        let drain = tokio::spawn({
            let store = store.clone();
            async move {
                let outcome = run_incremental_vacuum_drain(
                    &store,
                    fixed_chunk_policy(4, StdDuration::from_secs(120)),
                )
                .await;
                (outcome, std::time::Instant::now())
            }
        });
        // Wait until the drain has actually started releasing pages. Poll
        // through an independent read-only connection (a WAL reader sees each
        // committed chunk) so the probe never competes for the write pool.
        let mut probe = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(dir.path().join("vacuum.db"))
            .read_only(true)
            .connect()
            .await
            .expect("probe connection");
        tokio::time::timeout(StdDuration::from_secs(30), async {
            loop {
                let freelist: i64 = sqlx::query_scalar("PRAGMA freelist_count")
                    .fetch_one(&mut probe)
                    .await
                    .expect("probe freelist");
                if freelist < before {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drain started");

        let write_started = std::time::Instant::now();
        store
            .insert_event(&NewEvent {
                workspace_id: WorkspaceId::new(),
                timestamp: "2026-10-20T00:00:00Z".to_string(),
                event_type: "file:changed".to_string(),
                actor: EventActor::default(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: serde_json::json!({}),
            })
            .await
            .expect("write during drain");
        let write_done = std::time::Instant::now();

        let (outcome, drain_done) = drain.await.expect("drain task");
        assert_eq!(outcome.stop, VacuumDrainStop::Drained, "{outcome:?}");
        assert!(
            write_done < drain_done,
            "the write must not wait for the whole drain ({outcome:?})"
        );
        // A 4-page chunk on this small file takes milliseconds, so the write
        // waits at most one short chunk; 2 s is pure scheduler headroom for
        // a loaded CI host, still far below the multi-second holds of #7448.
        assert!(
            write_done - write_started < StdDuration::from_secs(2),
            "write took {:?} (max chunk {:?})",
            write_done - write_started,
            outcome.max_chunk
        );
    }
}
