//! PR-monitor repository: CRUD for agent-owned pull-request watches. Rows are
//! written through by the centralized monitor loop and rehydrated at boot via
//! [`Store::load_active_pr_monitors`]. The `repo_owner` / `repo_name` identity
//! is compared under `COLLATE NOCASE` (migration `0119`) — the SQL counterpart
//! of the case-insensitive `RepoRef` identity in `intent-sourcecontrol` —
//! while the stored casing is kept verbatim.

use std::sync::LazyLock;

use intent_core::{AgentId, PrMonitor, PrMonitorId, PrMonitorState, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

const COLUMNS: &str = "monitor_id, workspace_id, agent_id, repo_owner, repo_name, pr_number, \
    state, last_snapshot, baseline_snapshot, pending_changes, pending_since, last_change_at, \
    last_polled_at, last_error, created_at, updated_at";

fn state_to_db(state: PrMonitorState) -> &'static str {
    match state {
        PrMonitorState::Active => "active",
        PrMonitorState::Completed => "completed",
        PrMonitorState::Cancelled => "cancelled",
    }
}

fn state_from_db(s: &str) -> Result<PrMonitorState> {
    match s {
        "active" => Ok(PrMonitorState::Active),
        "completed" => Ok(PrMonitorState::Completed),
        "cancelled" => Ok(PrMonitorState::Cancelled),
        _ => Err(intent_core::Error::Internal(format!(
            "invalid pr monitor state: {s}"
        ))),
    }
}

/// The persisted `pending_changes` column is a JSON array of change lines; a
/// NULL or unparseable value reads as "nothing pending" rather than failing
/// the row (a monitor must never become unreadable because of a bad blob).
fn pending_from_db(raw: Option<String>) -> Vec<String> {
    raw.and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
        .unwrap_or_default()
}

fn pending_to_db(pending: &[String]) -> Option<String> {
    if pending.is_empty() {
        return None;
    }
    serde_json::to_string(pending).ok()
}

/// The fixed prefix of the global forge rate-limit pause annotation a PR
/// monitor carries in `last_error` while the pause is active
/// (monorepo#2961); [`pr_monitor_pause_error`] builds the full annotation.
/// The store owns the shape because the guarded write-backs compose on it
/// in SQL (see [`PrMonitorPollUpdate::last_error`]).
pub const PR_MONITOR_PAUSE_MARKER: &str = "rate limited; PR monitor polling paused";

/// The pause annotation naming the pause's RFC 3339 (whole-second, UTC)
/// deadline — `None` only in the window between the deadline elapsing and
/// the gate re-opening. Every annotation is the marker, `" until "`, then
/// the deadline; the SQL that composes annotations reads the deadline back
/// out at that fixed offset ([`pause_deadline_secs_sql`]).
#[must_use]
pub fn pr_monitor_pause_error(until: Option<&str>) -> String {
    match until {
        Some(until) => format!("{PR_MONITOR_PAUSE_MARKER}{PAUSE_UNTIL_SEPARATOR}{until}"),
        None => PR_MONITOR_PAUSE_MARKER.to_string(),
    }
}

const PAUSE_UNTIL_SEPARATOR: &str = " until ";

/// The SQL expression for the deadline named by the pause `annotation` (an
/// expression: the marker, [`PAUSE_UNTIL_SEPARATOR`], an RFC 3339 instant),
/// as whole Unix seconds — `NULL` for an empty expression, a bare-marker
/// annotation, or an unparsable deadline. Deadlines and the write
/// timestamps they are compared against are both reduced to whole seconds
/// here: the annotation's deadline is already whole-second (the gate's
/// wall-clock deadline with its fraction dropped) while `updated_at`
/// carries a fraction, so a string comparison mixed the two precisions and
/// misordered `…00Z` against `…00.500Z`. At whole seconds a deadline equal
/// to the write's second still stands — the gate may re-open anywhere
/// inside that second, so the annotation must not clear before it.
fn pause_deadline_secs_sql(annotation: &str, marker: &str) -> String {
    let skip = PAUSE_UNTIL_SEPARATOR.len() + 1;
    instant_secs_sql(&format!("substr({annotation}, length({marker}) + {skip})"))
}

/// The SQL expression for `instant` (an RFC 3339 UTC expression, any
/// fraction) as whole Unix seconds — the other side of a
/// [`pause_deadline_secs_sql`] comparison. The fraction is cut off
/// textually (`YYYY-MM-DDTHH:MM:SS` is the first 19 bytes) rather than left
/// to `SQLite`, which rounds sub-millisecond fractions and would carry
/// `…00.9995Z` into the next second.
fn instant_secs_sql(instant: &str) -> String {
    format!("CAST(strftime('%s', substr({instant}, 1, 19)) AS INTEGER)")
}

/// Everything one poll write-back can change on a monitor row — the named
/// fields keep the two snapshot columns (and the three timestamp-ish
/// options) from being transposable at call sites. See
/// [`Store::update_pr_monitor_poll`] for the concurrency-guard semantics of
/// `expected_updated_at`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PrMonitorPollUpdate<'a> {
    /// The most recent poll's snapshot (per-poll activity anchor).
    pub last_snapshot: Option<&'a str>,
    /// The emit-baseline snapshot pending changes are recomputed against.
    pub baseline_snapshot: Option<&'a str>,
    pub pending_changes: &'a [String],
    pub pending_since: Option<&'a str>,
    pub last_change_at: Option<&'a str>,
    pub last_polled_at: Option<&'a str>,
    /// The caller's `last_error`: a genuine fetch error, `None` after a
    /// success. NOT written verbatim — the statement composes the landed
    /// value from its genuine part and the row's CURRENT pause annotation
    /// (see [`pause_preserving_last_error_sql`]): an unexpired annotation
    /// on the row survives the write, a pause annotation the caller's value
    /// carries ([`pr_monitor_pause_error`], e.g. the error of a fetch that
    /// itself hit the limit) is stripped and never written — the row, not
    /// the caller, decides whether a pause is in force and until when.
    pub last_error: Option<&'a str>,
    pub updated_at: &'a str,
    /// The `updated_at` the caller read; the write lands only if it still
    /// matches (optimistic concurrency).
    pub expected_updated_at: &'a str,
}

/// The SQL expression for the pause annotation the row's CURRENT
/// `last_error` carries (from the marker to the end), `''` when it carries
/// none. `marker` is the `?N` placeholder bound to [`PR_MONITOR_PAUSE_MARKER`].
fn row_pause_sql(marker: &str) -> String {
    format!(
        "CASE WHEN instr(COALESCE(last_error, ''), {marker}) > 0 \
             THEN substr(last_error, instr(last_error, {marker})) ELSE '' END"
    )
}

/// The SQL expression for the `last_error` a guarded write-back lands,
/// composed from the row's CURRENT `last_error` and the caller's `genuine`
/// (a `?N` placeholder): the caller's value minus any pause annotation it
/// carries, then `"; "` and the ROW's pause annotation — kept iff its
/// deadline is not before `floor` (a `?N` placeholder bound to this
/// write's `updated_at`), compared at whole seconds
/// ([`pause_deadline_secs_sql`]). A row without an unexpired annotation
/// lands without one, whatever the caller captured; a row with one keeps
/// exactly it. `marker` is the `?N` placeholder bound to
/// [`PR_MONITOR_PAUSE_MARKER`].
///
/// Composed in SQL rather than from a Rust-side capture because the bulk
/// stamp ([`Store::annotate_active_pr_monitors_pause`]) and the bulk clear
/// ([`Store::clear_active_pr_monitors_pause`]) deliberately leave
/// `updated_at` alone: a pause opening, extending, or being lifted between
/// a poll's gate read and its write-back cannot fail the guard, so the
/// write must be able neither to clobber a stamp nor to resurrect a cleared
/// one. The row is therefore the ONLY source of truth for the pause: the
/// caller's capture never introduces an annotation nor moves the row's to
/// another deadline — an annotation on the row is no proof it names the
/// same pause the caller read (a lifted pause's capture landing after a
/// shorter pause was stamped would otherwise re-extend it past the lift
/// that reconciles it), and every opening or extension is stamped on the
/// rows by the serialized gate transition anyway. An annotation whose
/// deadline has passed the floor is dropped — the first post-pause write
/// clears the pause.
fn pause_preserving_last_error_sql(genuine: &str, marker: &str, floor: &str) -> String {
    let row_pause = row_pause_sql(marker);
    let row_deadline = pause_deadline_secs_sql(&row_pause, marker);
    let floor_secs = instant_secs_sql(floor);
    let pause = format!("CASE WHEN {row_deadline} >= {floor_secs} THEN {row_pause} ELSE NULL END");
    let caller_genuine = format!(
        "NULLIF(CASE WHEN instr(COALESCE({genuine}, ''), {marker}) > 0 \
                    THEN rtrim(substr({genuine}, 1, instr({genuine}, {marker}) - 1), '; ') \
                    ELSE {genuine} END, '')"
    );
    format!(
        "CASE WHEN {pause} IS NULL THEN {caller_genuine} \
              WHEN {caller_genuine} IS NULL THEN {pause} \
              ELSE {caller_genuine} || '; ' || {pause} END"
    )
}

static UPDATE_PR_MONITOR_POLL_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "UPDATE pr_monitor SET last_snapshot = ?1, baseline_snapshot = ?2, \
         pending_changes = ?3, pending_since = ?4, last_change_at = ?5, last_polled_at = ?6, \
         last_error = {}, updated_at = ?8 \
         WHERE monitor_id = ?9 AND state = 'active' AND updated_at = ?10",
        pause_preserving_last_error_sql("?7", "?11", "?8")
    )
});

static ADOPT_PR_MONITOR_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "UPDATE pr_monitor SET agent_id = ?1, last_snapshot = ?2, baseline_snapshot = ?3, \
         pending_changes = ?4, pending_since = ?5, last_change_at = ?6, last_polled_at = ?7, \
         last_error = {}, updated_at = ?9 \
         WHERE monitor_id = ?10 AND agent_id = ?11 AND state = 'active' AND updated_at = ?12",
        pause_preserving_last_error_sql("?8", "?13", "?9")
    )
});

/// The bulk stamp ([`Store::annotate_active_pr_monitors_pause`]): `?1` is
/// the new pause annotation, `?2` the marker. A row whose current
/// annotation already names a LATER deadline than `?1` is left alone (the
/// `WHERE` excludes it, so it is not counted either): stamps are issued
/// from concurrent sweeps and can land out of order, and a delayed earlier
/// stamp must not roll an extended deadline back. Otherwise an earlier
/// annotation — bare, or appended to a genuine error — is replaced, a
/// genuine error gains the annotation, an empty `last_error` becomes it.
static ANNOTATE_ACTIVE_PR_MONITORS_PAUSE_SQL: LazyLock<String> = LazyLock::new(|| {
    let row_deadline = pause_deadline_secs_sql(&row_pause_sql("?2"), "?2");
    let new_deadline = pause_deadline_secs_sql("?1", "?2");
    format!(
        "UPDATE pr_monitor SET last_error = CASE \
             WHEN last_error IS NULL OR last_error = '' THEN ?1 \
             WHEN instr(last_error, ?2) = 1 THEN ?1 \
             WHEN instr(last_error, ?2) > 1 \
                 THEN substr(last_error, 1, instr(last_error, ?2) - 1) || ?1 \
             ELSE last_error || '; ' || ?1 \
         END \
         WHERE state = 'active' AND NOT COALESCE({row_deadline} > {new_deadline}, 0)"
    )
});

/// The bulk clear ([`Store::clear_active_pr_monitors_pause`]): `?1` is the
/// marker, `?2` the pause annotation whose lift is being reconciled, or
/// `NULL`. The annotation — from the marker to the end — is cut off every
/// active row's `last_error` whose deadline is not LATER than `?2`'s (any
/// deadline when `?2` is `NULL`, or when either side does not parse): a
/// bare annotation leaves `NULL`, an appended one leaves the genuine error
/// in front (its `"; "` separator trimmed). A row naming a later deadline
/// than the lifted one carries a pause opened after the lift, which this
/// clear must not erase. Rows carrying no annotation are not touched (and
/// not counted).
static CLEAR_ACTIVE_PR_MONITORS_PAUSE_SQL: LazyLock<String> = LazyLock::new(|| {
    let row_deadline = pause_deadline_secs_sql(&row_pause_sql("?1"), "?1");
    let lifted_deadline = pause_deadline_secs_sql("?2", "?1");
    format!(
        "UPDATE pr_monitor \
         SET last_error = NULLIF(rtrim(substr(last_error, 1, instr(last_error, ?1) - 1), '; '), '') \
         WHERE state = 'active' AND instr(COALESCE(last_error, ''), ?1) > 0 \
           AND NOT COALESCE({row_deadline} > {lifted_deadline}, 0)"
    )
});

/// The narrow projection of one non-cancelled monitor row consumed by the
/// `workspace.list` / `workspace.subscribe` seq-0 PR merge: the identity /
/// lifecycle columns plus the handful of scalar fields the list decoration
/// reads out of `last_snapshot`. The snapshot scalars are extracted in SQL
/// (`json_extract`), so the row's JSON blob columns (`last_snapshot`,
/// `baseline_snapshot`, `pending_changes`) are never shipped to Rust or
/// deserialized on this hot path — deliberately not a [`PrMonitor`], so the
/// type itself guarantees the bulk read cannot regrow the blobs
/// (intent-hq/monorepo#3878).
///
/// The `snapshot_*` fields are `None` when the monitor has no snapshot yet or
/// the persisted blob is not valid JSON (mirroring the tolerant
/// `serde_json::from_str(..).ok()` parse this projection replaced);
/// `snapshot_url` / `snapshot_title` / `snapshot_is_draft` are mandatory in
/// a serialized snapshot, so `Some` on any of them means "snapshot present".
#[derive(Debug, Clone)]
pub struct PrMonitorListEntry {
    pub workspace_id: WorkspaceId,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: i64,
    pub state: PrMonitorState,
    pub created_at: String,
    pub updated_at: String,
    /// `$.url` of `last_snapshot` — the PR's HTML URL.
    pub snapshot_url: Option<String>,
    /// `$.title` of `last_snapshot`.
    pub snapshot_title: Option<String>,
    /// `$.headSha` of `last_snapshot` (optional in the snapshot itself).
    pub snapshot_head_sha: Option<String>,
    /// `$.requirements.state` of `last_snapshot` — the checklist's 4-value
    /// lifecycle word (`open` / `draft` / `closed` / `merged`).
    pub snapshot_state: Option<String>,
    /// `$.requirements.isDraft` of `last_snapshot`.
    pub snapshot_is_draft: Option<bool>,
    /// `$.requirements.mergeable` of `last_snapshot` (tri-state: omitted
    /// while the forge is still computing).
    pub snapshot_mergeable: Option<bool>,
}

/// One workspace's PR-monitor inputs for `workspace.get`, read by
/// [`Store::load_workspace_pr_monitor_reads`] in a single statement.
#[derive(Debug, Clone, Default)]
pub struct WorkspacePrMonitorReads {
    /// The displayStatus derivation rows — ACTIVE rows plus the latest
    /// COMPLETED row, oldest first — carrying only what the fold reads
    /// (`state`, `last_snapshot`, `updated_at`, identity):
    /// `baseline_snapshot` / `pending_changes` are never selected and read
    /// as absent.
    pub display_rows: Vec<PrMonitor>,
    /// Every non-cancelled row as the PR-merge projection, oldest first.
    pub list_entries: Vec<PrMonitorListEntry>,
}

/// The [`PrMonitorListEntry`] projection query over the non-cancelled rows,
/// with `extra_filter` appended to the inner `WHERE` (the archived-workspace
/// exclusion). The blob columns are never
/// selected: the few `last_snapshot` scalars the merge consumes are
/// `json_extract`ed in SQL, guarded by `json_valid` so a malformed blob
/// degrades to NULL scalars instead of failing the query.
fn non_cancelled_list_entry_sql(extra_filter: &str) -> String {
    format!(
        "SELECT workspace_id, repo_owner, repo_name, pr_number, state, created_at, \
         updated_at, \
         json_extract(snapshot, '$.url') AS snapshot_url, \
         json_extract(snapshot, '$.title') AS snapshot_title, \
         json_extract(snapshot, '$.headSha') AS snapshot_head_sha, \
         json_extract(snapshot, '$.requirements.state') AS snapshot_state, \
         json_extract(snapshot, '$.requirements.isDraft') AS snapshot_is_draft, \
         json_extract(snapshot, '$.requirements.mergeable') AS snapshot_mergeable \
         FROM (SELECT workspace_id, repo_owner, repo_name, pr_number, state, created_at, \
         updated_at, \
         CASE WHEN json_valid(last_snapshot) THEN last_snapshot END AS snapshot \
         FROM pr_monitor WHERE state != 'cancelled'{extra_filter}) \
         ORDER BY created_at"
    )
}

/// The single statement behind [`Store::load_workspace_pr_monitor_reads`]
/// (one `?` bind: the workspace id), exposed so the plan-shape regression
/// test EXPLAINs the exact production SQL.
///
/// Shape matters here, not just the projection: the per-state
/// `ROW_NUMBER()` that picks the latest COMPLETED row runs in a window
/// sorter, and every column of the ranked subquery's rows is copied into
/// that sorter's records. Ranking the narrow identity (`monitor_id`, keyed
/// by `state` / `updated_at`) alone and joining back to `pr_monitor` on its
/// primary key afterwards keeps `last_snapshot` out of the sorter, so the
/// blob is read only after `state_rank` is known and only reaches the
/// result row for the displayStatus set — ranking the full row instead
/// carried every completed monitor's snapshot into the window sort
/// (intentd#2001 review).
fn workspace_pr_monitor_reads_sql() -> &'static str {
    "SELECT monitor_id, agent_id, workspace_id, repo_owner, repo_name, pr_number, \
     state, created_at, updated_at, \
     json_extract(snapshot, '$.url') AS snapshot_url, \
     json_extract(snapshot, '$.title') AS snapshot_title, \
     json_extract(snapshot, '$.headSha') AS snapshot_head_sha, \
     json_extract(snapshot, '$.requirements.state') AS snapshot_state, \
     json_extract(snapshot, '$.requirements.isDraft') AS snapshot_is_draft, \
     json_extract(snapshot, '$.requirements.mergeable') AS snapshot_mergeable, \
     display_row, \
     CASE WHEN display_row THEN last_snapshot END AS display_snapshot \
     FROM (SELECT m.monitor_id, m.agent_id, m.workspace_id, m.repo_owner, m.repo_name, \
     m.pr_number, m.state, m.created_at, m.updated_at, m.last_snapshot, \
     CASE WHEN json_valid(m.last_snapshot) THEN m.last_snapshot END AS snapshot, \
     (m.state = 'active' OR r.state_rank = 1) AS display_row \
     FROM (SELECT monitor_id, \
     ROW_NUMBER() OVER (PARTITION BY state ORDER BY updated_at DESC) AS state_rank \
     FROM pr_monitor WHERE workspace_id = ? AND state != 'cancelled') AS r \
     JOIN pr_monitor AS m ON m.monitor_id = r.monitor_id) \
     ORDER BY created_at"
}

fn list_entry_from_row(r: &SqliteRow) -> Result<PrMonitorListEntry> {
    let err =
        |e: sqlx::Error| intent_core::Error::Internal(format!("read pr monitor list row: {e}"));
    let get = |col: &str| -> Result<String> { r.try_get::<String, _>(col).map_err(err) };
    let get_opt =
        |col: &str| -> Result<Option<String>> { r.try_get::<Option<String>, _>(col).map_err(err) };
    Ok(PrMonitorListEntry {
        workspace_id: WorkspaceId(get("workspace_id")?),
        repo_owner: get("repo_owner")?,
        repo_name: get("repo_name")?,
        pr_number: r.try_get::<i64, _>("pr_number").map_err(err)?,
        state: state_from_db(&get("state")?)?,
        created_at: get("created_at")?,
        updated_at: get("updated_at")?,
        snapshot_url: get_opt("snapshot_url")?,
        snapshot_title: get_opt("snapshot_title")?,
        snapshot_head_sha: get_opt("snapshot_head_sha")?,
        snapshot_state: get_opt("snapshot_state")?,
        snapshot_is_draft: r
            .try_get::<Option<bool>, _>("snapshot_is_draft")
            .map_err(err)?,
        snapshot_mergeable: r
            .try_get::<Option<bool>, _>("snapshot_mergeable")
            .map_err(err)?,
    })
}

fn monitor_from_row(r: &SqliteRow) -> Result<PrMonitor> {
    let err = |e: sqlx::Error| intent_core::Error::Internal(format!("read pr monitor row: {e}"));
    let get = |col: &str| -> Result<String> { r.try_get::<String, _>(col).map_err(err) };
    let get_opt =
        |col: &str| -> Result<Option<String>> { r.try_get::<Option<String>, _>(col).map_err(err) };
    Ok(PrMonitor {
        monitor_id: PrMonitorId(get("monitor_id")?),
        workspace_id: WorkspaceId(get("workspace_id")?),
        agent_id: AgentId(get("agent_id")?),
        repo_owner: get("repo_owner")?,
        repo_name: get("repo_name")?,
        pr_number: r.try_get::<i64, _>("pr_number").map_err(err)?,
        state: state_from_db(&get("state")?)?,
        last_snapshot: get_opt("last_snapshot")?,
        baseline_snapshot: get_opt("baseline_snapshot")?,
        pending_changes: pending_from_db(get_opt("pending_changes")?),
        pending_since: get_opt("pending_since")?,
        last_change_at: get_opt("last_change_at")?,
        last_polled_at: get_opt("last_polled_at")?,
        last_error: get_opt("last_error")?,
        created_at: get("created_at")?,
        updated_at: get("updated_at")?,
    })
}

impl Store {
    /// Insert a new PR-monitor row. Returns `false` (without inserting) when
    /// a unique index rejects the row: `idx_pr_monitor_identity` (an ACTIVE
    /// monitor for the same `(agent, repo, PR)` triple already exists — the
    /// caller re-arms that row instead) or `idx_pr_monitor_workspace_identity`
    /// (another agent in the same workspace already holds the ACTIVE monitor
    /// for `(workspace, repo, PR)` — the caller refuses, naming the owner
    /// found via [`Store::find_active_pr_monitor_in_workspace`]).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the insert fails for any reason other than a unique-index rejection (which returns `Ok(false)`).
    pub async fn insert_pr_monitor(&self, m: &PrMonitor) -> Result<bool> {
        let sql = format!(
            "INSERT INTO pr_monitor ({COLUMNS}) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
        match sqlx::query(&sql)
            .bind(&m.monitor_id.0)
            .bind(&m.workspace_id.0)
            .bind(&m.agent_id.0)
            .bind(&m.repo_owner)
            .bind(&m.repo_name)
            .bind(m.pr_number)
            .bind(state_to_db(m.state))
            .bind(&m.last_snapshot)
            .bind(&m.baseline_snapshot)
            .bind(pending_to_db(&m.pending_changes))
            .bind(&m.pending_since)
            .bind(&m.last_change_at)
            .bind(&m.last_polled_at)
            .bind(&m.last_error)
            .bind(&m.created_at)
            .bind(&m.updated_at)
            .execute(self.write_pool())
            .await
        {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_database_error()
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation) =>
            {
                Ok(false)
            }
            Err(e) => Err(intent_core::Error::Internal(format!(
                "insert pr monitor failed: {e}"
            ))),
        }
    }

    /// Get a PR monitor by id; `NotFound` when absent.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the PR monitor does not exist; `Error::Internal` if the database operation fails.
    pub async fn get_pr_monitor(&self, monitor_id: &PrMonitorId) -> Result<PrMonitor> {
        let sql = format!("SELECT {COLUMNS} FROM pr_monitor WHERE monitor_id = ?");
        let row = sqlx::query(&sql)
            .bind(&monitor_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("get pr monitor failed: {e}")))?;
        match row {
            Some(r) => monitor_from_row(&r),
            None => Err(intent_core::Error::NotFound(format!(
                "pr monitor {} not found",
                monitor_id.0
            ))),
        }
    }

    /// The ACTIVE monitor an agent already owns for `(owner, name, number)`,
    /// if any — the idempotent re-register lookup. `owner` / `name` match
    /// case-insensitively (forge slugs are case-insensitive; migration `0119`).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn find_active_pr_monitor(
        &self,
        agent_id: &AgentId,
        repo_owner: &str,
        repo_name: &str,
        pr_number: i64,
    ) -> Result<Option<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE agent_id = ? \
             AND repo_owner = ? COLLATE NOCASE AND repo_name = ? COLLATE NOCASE \
             AND pr_number = ? AND state = 'active'"
        );
        let row = sqlx::query(&sql)
            .bind(&agent_id.0)
            .bind(repo_owner)
            .bind(repo_name)
            .bind(pr_number)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("find pr monitor failed: {e}")))?;
        row.as_ref().map(monitor_from_row).transpose()
    }

    /// The ACTIVE monitor any agent in `workspace_id` holds for
    /// `(owner, name, number)`, if any — the workspace-scoped "who already
    /// monitors this PR" lookup (`idx_pr_monitor_workspace_identity` makes
    /// it at most one row). Owner-agnostic: compare `agent_id` to tell an
    /// idempotent re-register from another agent's duplicate. `owner` /
    /// `name` match case-insensitively.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn find_active_pr_monitor_in_workspace(
        &self,
        workspace_id: &WorkspaceId,
        repo_owner: &str,
        repo_name: &str,
        pr_number: i64,
    ) -> Result<Option<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE workspace_id = ? \
             AND repo_owner = ? COLLATE NOCASE AND repo_name = ? COLLATE NOCASE \
             AND pr_number = ? AND state = 'active'"
        );
        let row = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .bind(repo_owner)
            .bind(repo_name)
            .bind(pr_number)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("find pr monitor in workspace failed: {e}"))
            })?;
        row.as_ref().map(monitor_from_row).transpose()
    }

    /// List every monitor owned by an agent, oldest first (all states — the
    /// caller filters).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_pr_monitors_by_agent(&self, agent_id: &AgentId) -> Result<Vec<PrMonitor>> {
        let sql =
            format!("SELECT {COLUMNS} FROM pr_monitor WHERE agent_id = ? ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .bind(&agent_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("list pr monitors by agent failed: {e}"))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// List every monitor in a workspace, oldest first (all states).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_pr_monitors_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        let sql =
            format!("SELECT {COLUMNS} FROM pr_monitor WHERE workspace_id = ? ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("list pr monitors by workspace failed: {e}"))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// List an agent's ACTIVE monitors only, oldest first — the SQL-filtered
    /// counterpart to [`Store::list_pr_monitors_by_agent`] for read paths
    /// that only care about active rows (idle-visibility's
    /// `waitingOnPrMonitors`), so cost is O(active monitors) rather than
    /// O(all monitor history for the agent).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_active_pr_monitors_by_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE agent_id = ? AND state = 'active' \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&agent_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!(
                    "list active pr monitors by agent failed: {e}"
                ))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// List a workspace's ACTIVE monitors only, oldest first — the
    /// SQL-filtered counterpart to [`Store::list_pr_monitors_by_workspace`]
    /// for `agent.list`/`agent.diagnostics`, so cost is O(active monitors in
    /// the workspace) rather than O(all monitor history in the workspace).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_active_pr_monitors_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE workspace_id = ? AND state = 'active' \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!(
                    "list active pr monitors by workspace failed: {e}"
                ))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// List a workspace's ACTIVE monitors plus only its most recently updated
    /// COMPLETED monitor (`LIMIT 1`), oldest first — the displayStatus
    /// derivation read (active rows feed the open-PR signals, the latest
    /// completed row the merged signal, matching linked-PR "latest" step-6
    /// semantics). Completed rows are retained indefinitely, so the bound
    /// keeps this hot-path read O(active monitors) instead of O(all monitor
    /// history in the workspace); cancelled rows are excluded entirely.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_display_status_pr_monitors_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE workspace_id = ? AND state = 'active' \
             UNION ALL \
             SELECT {COLUMNS} FROM (SELECT {COLUMNS} FROM pr_monitor WHERE workspace_id = ? \
             AND state = 'completed' ORDER BY updated_at DESC LIMIT 1) \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!(
                    "list display-status pr monitors by workspace failed: {e}"
                ))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// Display-status monitor rows for every requested workspace in one
    /// statement: all active rows plus the latest completed row per workspace.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` when the query or row projection fails.
    pub async fn list_display_status_pr_monitors_by_workspaces(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<Vec<PrMonitor>> {
        if workspace_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; workspace_ids.len()].join(",");
        let sql = format!(
            "SELECT {COLUMNS} FROM (\
                SELECT {COLUMNS}, ROW_NUMBER() OVER (\
                    PARTITION BY workspace_id, state ORDER BY updated_at DESC\
                ) AS state_rank FROM pr_monitor \
                WHERE workspace_id IN ({placeholders}) AND state IN ('active', 'completed')\
             ) WHERE state = 'active' OR state_rank = 1 \
             ORDER BY workspace_id, created_at"
        );
        let mut query = sqlx::query(&sql);
        for id in workspace_ids {
            query = query.bind(&id.0);
        }
        let rows = query.fetch_all(self.read_pool()).await.map_err(|e| {
            intent_core::Error::Internal(format!(
                "batch list display-status pr monitors failed: {e}"
            ))
        })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// Every `active` monitor across all workspaces, oldest first — the poll
    /// loop's per-tick read and the boot rehydration read.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn load_active_pr_monitors(&self) -> Result<Vec<PrMonitor>> {
        let sql =
            format!("SELECT {COLUMNS} FROM pr_monitor WHERE state = 'active' ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("load active pr monitors failed: {e}"))
            })?;
        rows.iter().map(monitor_from_row).collect()
    }

    /// Every non-cancelled (active or completed) monitor across all
    /// workspaces, oldest first, as narrow [`PrMonitorListEntry`] projections
    /// — the single bulk read backing the `workspace.list` /
    /// `workspace.subscribe` seq-0 PR merge. Completed rows are retained so
    /// merged PRs stay visible; cancelled rows are excluded (they are removed
    /// from the UI), matching the services-level per-workspace view
    /// ([`Services::pr_monitors_for_workspace`]). Unless `include_archived`,
    /// rows owned by archived workspaces are filtered in SQL so cost tracks
    /// the workspaces the list call actually returns.
    ///
    /// The blob columns are never selected: the few `last_snapshot` scalars
    /// the merge consumes are `json_extract`ed in SQL (guarded by
    /// `json_valid` so a malformed blob degrades to NULL scalars instead of
    /// failing the query), and `baseline_snapshot` / `pending_changes` are
    /// not touched at all — this read grows with monitor history, and
    /// hydrating full snapshot JSON per row put it at 1.26s for 440 rows on
    /// one of the hottest RPCs (intent-hq/monorepo#3878).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn load_non_cancelled_pr_monitor_list_entries(
        &self,
        include_archived: bool,
    ) -> Result<Vec<PrMonitorListEntry>> {
        let archived_filter = if include_archived {
            ""
        } else {
            " AND workspace_id IN (SELECT id FROM workspace WHERE archived = 0)"
        };
        let rows = sqlx::query(&non_cancelled_list_entry_sql(archived_filter))
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("load pr monitor list entries failed: {e}"))
            })?;
        rows.iter().map(list_entry_from_row).collect()
    }

    /// One workspace's PR-monitor inputs for `workspace.get` in ONE
    /// statement: every non-cancelled row as the narrow
    /// [`PrMonitorListEntry`] projection (the PR merge, same shape as
    /// [`Store::load_non_cancelled_pr_monitor_list_entries`]) plus the
    /// displayStatus rows — the ACTIVE rows and the single most recently
    /// updated COMPLETED row, exactly the set
    /// [`Store::list_display_status_pr_monitors_by_workspace`] returns. The
    /// `last_snapshot` blob reaches the result only for those displayStatus
    /// rows (`display_snapshot`, NULL elsewhere) and is read only after the
    /// per-state ranking — the ranking runs over the narrow monitor identity
    /// and joins back on the primary key ([`workspace_pr_monitor_reads_sql`]),
    /// so the window sorter never carries snapshot bytes for the completed
    /// history. Each non-cancelled row still costs one `last_snapshot` read
    /// for its `json_extract`ed scalars (the projection covers the whole
    /// non-cancelled history), so the statement is O(non-cancelled monitors)
    /// in blob reads but O(displayStatus rows) in blob bytes materialized
    /// into the result; `baseline_snapshot` / `pending_changes` are never
    /// selected (`idx_pr_monitor_workspace` SEARCH, then a primary-key
    /// SEARCH per ranked row). No archived filter: `workspace.get` serves
    /// archived workspaces too.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn load_workspace_pr_monitor_reads(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<WorkspacePrMonitorReads> {
        let rows = sqlx::query(workspace_pr_monitor_reads_sql())
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!(
                    "load pr monitor reads for workspace failed: {e}"
                ))
            })?;
        let err =
            |e: sqlx::Error| intent_core::Error::Internal(format!("read pr monitor list row: {e}"));
        let mut reads = WorkspacePrMonitorReads::default();
        for r in &rows {
            let entry = list_entry_from_row(r)?;
            if r.try_get::<bool, _>("display_row").map_err(err)? {
                reads.display_rows.push(PrMonitor {
                    monitor_id: PrMonitorId(r.try_get::<String, _>("monitor_id").map_err(err)?),
                    workspace_id: entry.workspace_id.clone(),
                    agent_id: AgentId(r.try_get::<String, _>("agent_id").map_err(err)?),
                    repo_owner: entry.repo_owner.clone(),
                    repo_name: entry.repo_name.clone(),
                    pr_number: entry.pr_number,
                    state: entry.state,
                    last_snapshot: r
                        .try_get::<Option<String>, _>("display_snapshot")
                        .map_err(err)?,
                    baseline_snapshot: None,
                    pending_changes: Vec::new(),
                    pending_since: None,
                    last_change_at: None,
                    last_polled_at: None,
                    last_error: None,
                    created_at: entry.created_at.clone(),
                    updated_at: entry.updated_at.clone(),
                });
            }
            reads.list_entries.push(entry);
        }
        Ok(reads)
    }

    /// Set a monitor's lifecycle state. Every legal transition starts from
    /// `active`, so the update is guarded on it; returns `false` when the row
    /// is absent or already terminal (a concurrent cancel/complete won) so
    /// the caller can skip its side effects instead of resurrecting the row.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn update_pr_monitor_state(
        &self,
        monitor_id: &PrMonitorId,
        state: PrMonitorState,
        updated_at: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE pr_monitor SET state = ?, updated_at = ? \
             WHERE monitor_id = ? AND state = 'active'",
        )
        .bind(state_to_db(state))
        .bind(updated_at)
        .bind(&monitor_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| {
            intent_core::Error::Internal(format!("update pr monitor state failed: {e}"))
        })?;
        Ok(res.rows_affected() > 0)
    }

    /// Terminalize an ACTIVE monitor: `completed` plus the cleared pending
    /// state in ONE statement, guarded like [`Store::update_pr_monitor_poll`]
    /// on `expected_updated_at`. A single write means no window in which a
    /// concurrent re-parent (adoption, intent-hq/intent#5079) can land
    /// between the poll write-back and the state flip and have its row
    /// completed under it while the final wake goes to the previous owner.
    /// Returns `false` when the guard fails (the row moved or is already
    /// terminal) so the caller skips the wake.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn complete_pr_monitor(
        &self,
        monitor_id: &PrMonitorId,
        updated_at: &str,
        expected_updated_at: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE pr_monitor SET state = 'completed', pending_changes = NULL, \
             pending_since = NULL, last_change_at = NULL, updated_at = ? \
             WHERE monitor_id = ? AND state = 'active' AND updated_at = ?",
        )
        .bind(updated_at)
        .bind(&monitor_id.0)
        .bind(expected_updated_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| intent_core::Error::Internal(format!("complete pr monitor failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// Write back everything one poll can change: the last-poll snapshot, the
    /// emit-baseline snapshot, the pending changes and their debounce anchors,
    /// the poll timestamp, and the last forge error. One statement so a reader
    /// never observes a baseline that moved without its pending changes.
    ///
    /// Optimistic-concurrency guarded: the write only lands when the row is
    /// still `active` AND its `updated_at` still equals
    /// [`PrMonitorPollUpdate::expected_updated_at`] (the value the caller
    /// read). Returns `false` when the guard fails — a concurrent
    /// flush/cancel/re-register/poll moved the row, and the caller must
    /// discard its stale image (skip emits) rather than clobber.
    ///
    /// `last_error` is composed in the statement against the row's current
    /// value ([`pause_preserving_last_error_sql`]): a rate-limit pause
    /// annotation the row already carries, with a deadline not before this
    /// write's `updated_at`, survives the write — the bulk stamp does not
    /// move `updated_at`, so the guard alone cannot protect it.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn update_pr_monitor_poll(
        &self,
        monitor_id: &PrMonitorId,
        update: PrMonitorPollUpdate<'_>,
    ) -> Result<bool> {
        let res = sqlx::query(&UPDATE_PR_MONITOR_POLL_SQL)
            .bind(update.last_snapshot)
            .bind(update.baseline_snapshot)
            .bind(pending_to_db(update.pending_changes))
            .bind(update.pending_since)
            .bind(update.last_change_at)
            .bind(update.last_polled_at)
            .bind(update.last_error)
            .bind(update.updated_at)
            .bind(&monitor_id.0)
            .bind(update.expected_updated_at)
            .bind(PR_MONITOR_PAUSE_MARKER)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("update pr monitor poll failed: {e}"))
            })?;
        Ok(res.rows_affected() > 0)
    }

    /// Annotate `last_error` on EVERY active monitor across all workspaces
    /// with the global forge rate-limit `pause` (monorepo#2961,
    /// [`pr_monitor_pause_error`]): the monitors the paused sweeps will not
    /// reach must not sit under a stale checklist with an empty `lastError`.
    /// An earlier annotation — bare, or appended to a genuine error as
    /// `"<error>; <pause>"` — is replaced by `pause`, so re-stamping on a
    /// deadline extension names the new deadline without stacking; a genuine
    /// fetch error already on the row is kept and `pause` appended to it; an
    /// empty `last_error` becomes `pause`. Monotonic: a row already naming a
    /// deadline LATER than `pause`'s keeps it and is not counted — stamps
    /// from concurrent sweeps can land out of order, and a delayed earlier
    /// stamp must not roll an extension back while the gate still holds the
    /// later deadline.
    ///
    /// Deliberately leaves `updated_at` (the optimistic-concurrency token)
    /// and `last_polled_at` alone: the stamp is an annotation, not a poll, so
    /// an in-flight poll's guarded write-back still lands — and, composing
    /// against the row in SQL, lands WITH this annotation whether the stamp
    /// ran before or after the poll read the row. Returns the number of rows
    /// touched.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn annotate_active_pr_monitors_pause(&self, pause: &str) -> Result<u64> {
        let res = sqlx::query(&ANNOTATE_ACTIVE_PR_MONITORS_PAUSE_SQL)
            .bind(pause)
            .bind(PR_MONITOR_PAUSE_MARKER)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!(
                    "annotate active pr monitors pause failed: {e}"
                ))
            })?;
        Ok(res.rows_affected())
    }

    /// Strip the rate-limit pause annotation ([`pr_monitor_pause_error`])
    /// from `last_error` on EVERY active monitor across all workspaces — the
    /// inverse of [`Store::annotate_active_pr_monitors_pause`], for when the
    /// pause is no longer in force BEFORE the deadline the annotations name:
    /// the gate lifted early because the quota recovered, or the daemon
    /// restarted with an open gate while the rows still name a deadline
    /// persisted by the previous process. The guarded write-backs keep an
    /// unexpired annotation on purpose, so without this clear the surfaces
    /// would report a pause until the stale deadline aged out.
    ///
    /// `lifted` is the pause annotation the lift observed on the gate: only
    /// annotations naming a deadline not LATER than its are stripped, so a
    /// clear delayed past a NEWER pause's stamp (a lift and a fresh trigger
    /// from concurrent sweeps) cannot erase that pause while the gate holds
    /// it. `None` (the gate was found open with no lift of its own — boot)
    /// strips every annotation. A bare annotation leaves `NULL`, one
    /// appended to a genuine error leaves the error; rows without an
    /// annotation, and terminal rows, are untouched. Like the stamp, leaves
    /// `updated_at` and `last_polled_at` alone — an in-flight write-back
    /// still lands, and lands WITHOUT the cleared annotation even if it
    /// captured it (see [`pause_preserving_last_error_sql`]). Returns the
    /// number of rows cleared.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn clear_active_pr_monitors_pause(&self, lifted: Option<&str>) -> Result<u64> {
        let res = sqlx::query(&CLEAR_ACTIVE_PR_MONITORS_PAUSE_SQL)
            .bind(PR_MONITOR_PAUSE_MARKER)
            .bind(lifted)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                intent_core::Error::Internal(format!("clear active pr monitors pause failed: {e}"))
            })?;
        Ok(res.rows_affected())
    }

    /// Re-parent an ACTIVE monitor from `from_agent_id` to `to_agent_id` and
    /// re-arm it in the same statement (the [`Store::update_pr_monitor_poll`]
    /// write-back), so a reader never observes the new owner with the old
    /// owner's pending changes. Backs orphaned-monitor adoption
    /// (intent-hq/intent#5079).
    ///
    /// Guarded like the poll write-back — the row must still be `active`
    /// with the caller's `expected_updated_at` — AND still owned by
    /// `from_agent_id`, so two siblings adopting concurrently cannot both
    /// win. Returns `false` when the guard fails. `last_error` composes
    /// against the row like the poll write-back.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn adopt_pr_monitor(
        &self,
        monitor_id: &PrMonitorId,
        from_agent_id: &AgentId,
        to_agent_id: &AgentId,
        update: PrMonitorPollUpdate<'_>,
    ) -> Result<bool> {
        let res = sqlx::query(&ADOPT_PR_MONITOR_SQL)
            .bind(&to_agent_id.0)
            .bind(update.last_snapshot)
            .bind(update.baseline_snapshot)
            .bind(pending_to_db(update.pending_changes))
            .bind(update.pending_since)
            .bind(update.last_change_at)
            .bind(update.last_polled_at)
            .bind(update.last_error)
            .bind(update.updated_at)
            .bind(&monitor_id.0)
            .bind(&from_agent_id.0)
            .bind(update.expected_updated_at)
            .bind(PR_MONITOR_PAUSE_MARKER)
            .execute(self.write_pool())
            .await
            .map_err(|e| intent_core::Error::Internal(format!("adopt pr monitor failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use intent_core::{
        now_iso, AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
        WorkspaceStatus,
    };
    use uuid::Uuid;

    /// A unique temp DB path cleaned up on drop (mirrors `crate::tests::TempDb`,
    /// which is private to that module).
    struct TempDb {
        path: std::path::PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("test-pr-monitor-{}.db", Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let mut sidecar = self.path.clone().into_os_string();
                sidecar.push(suffix);
                let _ = std::fs::remove_file(&sidecar);
            }
        }
    }

    fn test_workspace(ws_id: &WorkspaceId, ts: &str) -> Workspace {
        Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            context_links: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            browser_client_id: None,
            pull_requests_total: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    fn test_session(agent_id: &AgentId, ws_id: &WorkspaceId, ts: &str) -> AgentSession {
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: "Owner".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
            messages: vec![],
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            stats: None,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
            notifications_muted: false,
        }
    }

    /// Open a store with one workspace + agent session (the FK targets a
    /// `pr_monitor` row needs) and return them.
    async fn store_with_owner() -> (TempDb, Store, WorkspaceId, AgentId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-pr-monitor".to_string());
        store
            .insert_workspace(&test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&test_session(&agent_id, &ws_id, &ts))
            .await
            .expect("insert session");
        (tmp, store, ws_id, agent_id)
    }

    fn test_monitor(ws_id: &WorkspaceId, agent_id: &AgentId, ts: &str) -> PrMonitor {
        PrMonitor {
            monitor_id: PrMonitorId::new(),
            workspace_id: ws_id.clone(),
            agent_id: agent_id.clone(),
            repo_owner: "o".to_string(),
            repo_name: "r".to_string(),
            pr_number: 42,
            state: PrMonitorState::Active,
            last_snapshot: Some(r#"{"v":1}"#.to_string()),
            baseline_snapshot: Some(r#"{"v":1}"#.to_string()),
            pending_changes: Vec::new(),
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
        }
    }

    /// The displayStatus derivation read returns every ACTIVE row plus only
    /// the most recently updated COMPLETED row (`LIMIT 1`) — never older
    /// completed rows (retained indefinitely) and never cancelled rows —
    /// so the hot-path read stays bounded.
    #[tokio::test]
    async fn display_status_read_bounds_completed_rows_to_latest() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let mk = |pr_number: i64, state: PrMonitorState, created: &str, updated: &str| {
            let mut m = test_monitor(&ws_id, &agent_id, created);
            m.pr_number = pr_number;
            m.state = state;
            m.updated_at = updated.to_string();
            m
        };
        let active_a = mk(
            1,
            PrMonitorState::Active,
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        let active_b = mk(
            2,
            PrMonitorState::Active,
            "2026-01-02T00:00:00Z",
            "2026-01-02T00:00:00Z",
        );
        let completed_old = mk(
            3,
            PrMonitorState::Completed,
            "2026-01-03T00:00:00Z",
            "2026-01-03T00:00:00Z",
        );
        let completed_latest = mk(
            4,
            PrMonitorState::Completed,
            "2026-01-04T00:00:00Z",
            "2026-01-05T00:00:00Z",
        );
        let cancelled = mk(
            5,
            PrMonitorState::Cancelled,
            "2026-01-06T00:00:00Z",
            "2026-01-06T00:00:00Z",
        );
        for m in [
            &active_a,
            &active_b,
            &completed_old,
            &completed_latest,
            &cancelled,
        ] {
            assert!(store.insert_pr_monitor(m).await.expect("insert"));
        }

        let rows = store
            .list_display_status_pr_monitors_by_workspace(&ws_id)
            .await
            .expect("list");
        let ids: Vec<&str> = rows.iter().map(|m| m.monitor_id.0.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                active_a.monitor_id.0.as_str(),
                active_b.monitor_id.0.as_str(),
                completed_latest.monitor_id.0.as_str(),
            ],
            "all active rows + only the latest completed row, oldest first"
        );
    }

    /// The `workspace.list` bulk read (intent-hq/monorepo#3878) returns
    /// [`PrMonitorListEntry`] projections: the snapshot scalars the list
    /// decoration consumes arrive `json_extract`ed in SQL, and the blob
    /// columns (`last_snapshot`, `baseline_snapshot`, `pending_changes`) are
    /// never returned or deserialized — the entry type carries no fields for
    /// them, so the shape is enforced at compile time. A missing or malformed
    /// `last_snapshot` degrades to NULL scalars instead of failing the query,
    /// cancelled rows are excluded, and archived-workspace rows are filtered
    /// unless `include_archived`. The per-workspace `workspace.get` read
    /// serves the same projection scoped to one workspace, archived or not,
    /// alongside the displayStatus rows in the same statement.
    #[tokio::test]
    async fn list_entries_project_snapshot_scalars_without_blobs() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let snapshot = serde_json::json!({
            "title": "Monitored PR",
            "url": "https://github.com/o/r/pull/1",
            "headSha": "abc123",
            "conversationCount": 0,
            "reviewCommentCount": 0,
            "requirements": {
                "state": "merged",
                "isDraft": false,
                "hasConflicts": false,
                "isBehind": false,
                "mergeable": true,
                "checks": {
                    "total": 0, "passed": 0, "failed": 0, "pending": 0,
                    "items": [], "failingRequired": [], "pendingRequired": [],
                    "requiredKnown": true
                },
                "approvals": { "decision": "none", "have": 0, "changesRequested": 0 },
                "threads": { "unresolved": 0 },
                "rulesKnown": false
            }
        })
        .to_string();
        let mk = |pr_number: i64, state: PrMonitorState, snap: Option<String>, created: &str| {
            let mut m = test_monitor(&ws_id, &agent_id, created);
            m.pr_number = pr_number;
            m.state = state;
            m.last_snapshot = snap;
            m.baseline_snapshot = Some(r#"{"big":"blob"}"#.to_string());
            m.pending_changes = vec!["mergeable: true → false".to_string()];
            m
        };
        for m in [
            mk(
                1,
                PrMonitorState::Active,
                Some(snapshot),
                "2026-01-01T00:00:00Z",
            ),
            mk(2, PrMonitorState::Completed, None, "2026-01-02T00:00:00Z"),
            mk(
                3,
                PrMonitorState::Active,
                Some("{not json".to_string()),
                "2026-01-03T00:00:00Z",
            ),
            mk(4, PrMonitorState::Cancelled, None, "2026-01-04T00:00:00Z"),
        ] {
            assert!(store.insert_pr_monitor(&m).await.expect("insert"));
        }
        // A monitor in an archived workspace: excluded unless include_archived.
        let ts = now_iso();
        let archived_ws = WorkspaceId("ws-pr-monitor-archived".to_string());
        let mut w = test_workspace(&archived_ws, &ts);
        w.archived = true;
        w.status = WorkspaceStatus::Archived;
        store.insert_workspace(&w).await.expect("archived ws");
        let mut m = test_monitor(&archived_ws, &agent_id, "2026-01-05T00:00:00Z");
        m.pr_number = 5;
        assert!(store.insert_pr_monitor(&m).await.expect("insert archived"));

        let entries = store
            .load_non_cancelled_pr_monitor_list_entries(false)
            .await
            .expect("list entries");
        let numbers: Vec<i64> = entries.iter().map(|e| e.pr_number).collect();
        assert_eq!(
            numbers,
            vec![1, 2, 3],
            "cancelled + archived-workspace rows excluded, oldest first"
        );

        // Snapshot-backed row: scalars extracted from the JSON blob in SQL.
        let with_snap = &entries[0];
        assert_eq!(with_snap.workspace_id, ws_id);
        assert_eq!(with_snap.state, PrMonitorState::Active);
        assert_eq!(
            with_snap.snapshot_url.as_deref(),
            Some("https://github.com/o/r/pull/1")
        );
        assert_eq!(with_snap.snapshot_title.as_deref(), Some("Monitored PR"));
        assert_eq!(with_snap.snapshot_head_sha.as_deref(), Some("abc123"));
        assert_eq!(with_snap.snapshot_state.as_deref(), Some("merged"));
        assert_eq!(with_snap.snapshot_is_draft, Some(false));
        assert_eq!(with_snap.snapshot_mergeable, Some(true));

        // Snapshotless and malformed-snapshot rows read as NULL scalars
        // (mirroring the tolerant serde parse this projection replaced).
        for e in [&entries[1], &entries[2]] {
            assert_eq!(e.snapshot_url, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_title, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_head_sha, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_state, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_is_draft, None, "pr {}", e.pr_number);
            assert_eq!(e.snapshot_mergeable, None, "pr {}", e.pr_number);
        }

        let all = store
            .load_non_cancelled_pr_monitor_list_entries(true)
            .await
            .expect("list entries incl. archived");
        let numbers: Vec<i64> = all.iter().map(|e| e.pr_number).collect();
        assert_eq!(
            numbers,
            vec![1, 2, 3, 5],
            "include_archived adds the archived workspace's row; cancelled stays excluded"
        );

        // Per-workspace `workspace.get` read: the same projection over this
        // workspace's non-cancelled rows (archived served regardless), plus
        // the displayStatus rows — active rows and only the LATEST completed
        // row — carrying the snapshot blob.
        let mut older_completed = mk(
            6,
            PrMonitorState::Completed,
            Some(r#"{"v":6}"#.to_string()),
            "2025-12-31T00:00:00Z",
        );
        older_completed.updated_at = "2025-12-31T00:00:00Z".to_string();
        assert!(store
            .insert_pr_monitor(&older_completed)
            .await
            .expect("insert older completed"));
        let reads = store
            .load_workspace_pr_monitor_reads(&ws_id)
            .await
            .expect("workspace reads");
        assert_eq!(
            reads
                .list_entries
                .iter()
                .map(|e| e.pr_number)
                .collect::<Vec<_>>(),
            vec![6, 1, 2, 3],
            "list entries: every non-cancelled row, oldest first"
        );
        assert_eq!(
            reads.list_entries[1].snapshot_url.as_deref(),
            Some("https://github.com/o/r/pull/1")
        );
        assert_eq!(
            reads.list_entries[1].snapshot_state.as_deref(),
            Some("merged")
        );
        assert_eq!(
            reads
                .display_rows
                .iter()
                .map(|m| (m.pr_number, m.state))
                .collect::<Vec<_>>(),
            vec![
                (1, PrMonitorState::Active),
                (2, PrMonitorState::Completed),
                (3, PrMonitorState::Active),
            ],
            "display rows: active rows plus only the latest completed row"
        );
        let display_1 = &reads.display_rows[0];
        assert_eq!(display_1.workspace_id, ws_id);
        assert_eq!(display_1.agent_id, agent_id);
        assert!(
            display_1
                .last_snapshot
                .as_deref()
                .is_some_and(|s| s.contains("\"headSha\":\"abc123\"")),
            "display rows carry the snapshot blob"
        );
        assert_eq!(
            display_1.baseline_snapshot, None,
            "baseline blob is never selected"
        );
        assert!(display_1.pending_changes.is_empty());
        assert_eq!(
            reads.display_rows[2].last_snapshot.as_deref(),
            Some("{not json"),
            "the display row carries the raw blob; the fold tolerates it"
        );
        let archived_reads = store
            .load_workspace_pr_monitor_reads(&archived_ws)
            .await
            .expect("archived workspace reads");
        assert_eq!(
            archived_reads
                .list_entries
                .iter()
                .map(|e| e.pr_number)
                .collect::<Vec<_>>(),
            vec![5],
            "workspace read has no archived filter"
        );
        assert_eq!(archived_reads.display_rows.len(), 1);
    }

    /// Plan-shape guard for [`workspace_pr_monitor_reads_sql`] (intentd#2001
    /// review): the per-state `ROW_NUMBER()` must rank the narrow monitor
    /// identity and join back on the primary key, so no `last_snapshot`
    /// column read from the `pr_monitor` table cursor happens before the
    /// window sorter has sorted (`SorterSort`) — a read before it means the
    /// blob is being copied into the sorter record for every non-cancelled
    /// row, which is what the outer `display_snapshot` CASE cannot undo.
    /// `baseline_snapshot` / `pending_changes` must not be read at all. The
    /// old rank-the-full-row shape is kept as a positive control: it MUST
    /// trip the same check, so an opcode rename in a bundled-SQLite bump
    /// fails loudly instead of leaving the guard vacuous. Output equivalence
    /// of the two shapes is covered by the round-trip test above; this test
    /// also runs the statement against a history of large completed
    /// snapshots to pin the one-display-blob result.
    #[tokio::test]
    async fn workspace_pr_monitor_reads_rank_narrow_identity_before_blob_read() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let table_root: i64 =
            sqlx::query("SELECT rootpage FROM sqlite_master WHERE name = 'pr_monitor'")
                .fetch_one(store.read_pool())
                .await
                .expect("pr_monitor rootpage")
                .get("rootpage");
        let mut ordinals = std::collections::HashMap::new();
        for row in sqlx::query("PRAGMA table_info(pr_monitor)")
            .fetch_all(store.read_pool())
            .await
            .expect("pr_monitor columns")
        {
            ordinals.insert(row.get::<String, _>("name"), row.get::<i64, _>("cid"));
        }
        let blob_ordinals: Vec<(&'static str, i64)> =
            ["last_snapshot", "baseline_snapshot", "pending_changes"]
                .into_iter()
                .map(|c| (c, ordinals[c]))
                .collect();

        // For a statement: (blob columns read from a `pr_monitor` table cursor
        // before the first `SorterSort`, blob columns read anywhere).
        let blob_reads = |ops: &[(String, i64, i64)]| -> (Vec<&'static str>, Vec<&'static str>) {
            let table_cursors: Vec<i64> = ops
                .iter()
                .filter(|(op, _, p2)| op == "OpenRead" && *p2 == table_root)
                .map(|(_, p1, _)| *p1)
                .collect();
            let first_sort = ops
                .iter()
                .position(|(op, _, _)| op == "SorterSort")
                .unwrap_or(ops.len());
            let all: Vec<(usize, &'static str)> = ops
                .iter()
                .enumerate()
                .filter(|(_, (op, p1, _))| op == "Column" && table_cursors.contains(p1))
                .filter_map(|(i, (_, _, p2))| {
                    blob_ordinals
                        .iter()
                        .find(|(_, cid)| cid == p2)
                        .map(|(name, _)| (i, *name))
                })
                .collect();
            (
                all.iter()
                    .filter(|(i, _)| *i < first_sort)
                    .map(|(_, n)| *n)
                    .collect(),
                all.iter().map(|(_, n)| *n).collect(),
            )
        };
        let ws = ws_id.0.as_str();
        let pool = store.read_pool();
        let explain = |sql: String| async move {
            sqlx::query(&sql)
                .bind(ws)
                .fetch_all(pool)
                .await
                .expect("explain")
        };

        let sql = workspace_pr_monitor_reads_sql();
        let details: Vec<String> = explain(format!("EXPLAIN QUERY PLAN {sql}"))
            .await
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect();
        assert!(
            details
                .iter()
                .any(|d| d.contains("USING INDEX idx_pr_monitor_workspace")),
            "ranking must SEARCH the workspace index, plan: {details:?}"
        );
        assert!(
            details
                .iter()
                .any(|d| d.contains("SEARCH m USING INDEX sqlite_autoindex_pr_monitor_1")),
            "the projection must join back on the monitor_id primary key, plan: {details:?}"
        );
        let ops: Vec<(String, i64, i64)> = explain(format!("EXPLAIN {sql}"))
            .await
            .iter()
            .map(|row| {
                (
                    row.get::<String, _>("opcode"),
                    row.get::<i64, _>("p1"),
                    row.get::<i64, _>("p2"),
                )
            })
            .collect();
        assert!(
            ops.iter().any(|(op, _, _)| op == "SorterSort"),
            "window ranking is expected to run through a sorter, opcodes: {ops:?}"
        );
        let (before_sort, anywhere) = blob_reads(&ops);
        assert!(
            before_sort.is_empty(),
            "blob column(s) {before_sort:?} are read from the pr_monitor table cursor \
             before the window sort — the ranked subquery must carry only the narrow \
             identity, opcodes: {ops:?}"
        );
        assert!(
            anywhere.iter().all(|n| *n == "last_snapshot"),
            "baseline_snapshot / pending_changes must never be read: {anywhere:?}"
        );

        // Positive control: the pre-review shape (rank the full row, CASE
        // the blob afterwards) reads `last_snapshot` into the sorter record.
        let control = "SELECT monitor_id, agent_id, workspace_id, repo_owner, repo_name, \
             pr_number, state, created_at, updated_at, \
             json_extract(snapshot, '$.url') AS snapshot_url, \
             (state = 'active' OR state_rank = 1) AS display_row, \
             CASE WHEN state = 'active' OR state_rank = 1 THEN last_snapshot END \
             AS display_snapshot \
             FROM (SELECT monitor_id, agent_id, workspace_id, repo_owner, repo_name, \
             pr_number, state, created_at, updated_at, last_snapshot, \
             CASE WHEN json_valid(last_snapshot) THEN last_snapshot END AS snapshot, \
             ROW_NUMBER() OVER (PARTITION BY state ORDER BY updated_at DESC) AS state_rank \
             FROM pr_monitor WHERE workspace_id = ? AND state != 'cancelled') \
             ORDER BY created_at";
        let control_ops: Vec<(String, i64, i64)> = explain(format!("EXPLAIN {control}"))
            .await
            .iter()
            .map(|row| {
                (
                    row.get::<String, _>("opcode"),
                    row.get::<i64, _>("p1"),
                    row.get::<i64, _>("p2"),
                )
            })
            .collect();
        let (control_before_sort, _) = blob_reads(&control_ops);
        assert!(
            control_before_sort.contains(&"last_snapshot"),
            "positive control lost: the rank-the-full-row shape no longer shows a \
             `last_snapshot` Column read before `SorterSort`, so the guard above is \
             vacuous — re-verify the opcode names for this SQLite version, \
             opcodes: {control_ops:?}"
        );

        // Large completed history: one active row plus many completed rows
        // with ~64 KiB snapshots; only the active row and the LATEST
        // completed row carry a display snapshot, every row projects scalars.
        let pad = "x".repeat(64 * 1024);
        let ts = now_iso();
        for i in 1..=40_i64 {
            let mut m = test_monitor(&ws_id, &agent_id, &ts);
            m.pr_number = i;
            m.state = if i == 40 {
                PrMonitorState::Active
            } else {
                PrMonitorState::Completed
            };
            m.created_at = format!("2026-01-01T00:{i:02}:00Z");
            m.updated_at = format!("2026-01-02T00:{i:02}:00Z");
            m.last_snapshot = Some(format!(
                r#"{{"url":"https://github.com/o/r/pull/{i}","title":"t{i}","pad":"{pad}"}}"#
            ));
            assert!(store.insert_pr_monitor(&m).await.expect("insert"));
        }
        let reads = store
            .load_workspace_pr_monitor_reads(&ws_id)
            .await
            .expect("workspace reads");
        assert_eq!(reads.list_entries.len(), 40);
        assert_eq!(
            reads.list_entries[0].snapshot_url.as_deref(),
            Some("https://github.com/o/r/pull/1")
        );
        assert_eq!(
            reads
                .display_rows
                .iter()
                .map(|m| (m.pr_number, m.state))
                .collect::<Vec<_>>(),
            vec![
                (39, PrMonitorState::Completed),
                (40, PrMonitorState::Active)
            ],
            "display rows: latest completed (by updated_at) plus the active row"
        );
        assert!(reads
            .display_rows
            .iter()
            .all(|m| m.last_snapshot.as_deref().is_some_and(|s| s.contains(&pad))));
    }

    /// `baseline_snapshot` round-trips through insert/get, and
    /// `update_pr_monitor_poll` moves it independently of `last_snapshot`
    /// (they are distinct columns: the poll baseline advances every poll,
    /// the emit baseline only on delivered wakes).
    #[tokio::test]
    async fn baseline_snapshot_round_trip() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let ts = now_iso();
        let m = test_monitor(&ws_id, &agent_id, &ts);
        assert!(store.insert_pr_monitor(&m).await.expect("insert"));

        let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(read.baseline_snapshot.as_deref(), Some(r#"{"v":1}"#));
        assert_eq!(read.last_snapshot.as_deref(), Some(r#"{"v":1}"#));

        let now = now_iso();
        assert!(store
            .update_pr_monitor_poll(
                &m.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: Some(r#"{"v":3}"#),
                    baseline_snapshot: Some(r#"{"v":2}"#),
                    pending_changes: &["mergeable: true → false".to_string()],
                    pending_since: Some(&now),
                    last_change_at: Some(&now),
                    last_polled_at: Some(&now),
                    last_error: None,
                    updated_at: &now,
                    expected_updated_at: &m.updated_at,
                },
            )
            .await
            .expect("update poll"));
        let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(
            read.last_snapshot.as_deref(),
            Some(r#"{"v":3}"#),
            "poll baseline moved"
        );
        assert_eq!(
            read.baseline_snapshot.as_deref(),
            Some(r#"{"v":2}"#),
            "emit baseline written independently"
        );
        assert_eq!(read.pending_changes, vec!["mergeable: true → false"]);
    }

    /// `adopt_pr_monitor` re-parents and re-arms in one guarded write: the
    /// new owner lands together with the refreshed baseline and cleared
    /// pending state; a stale `expected_updated_at` or a wrong `from` owner
    /// leaves the row untouched (intent-hq/intent#5079).
    #[tokio::test]
    async fn adopt_pr_monitor_reparents_and_rearms_under_guard() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let ts = now_iso();
        let mut m = test_monitor(&ws_id, &agent_id, &ts);
        m.pending_changes = vec!["checks: 1 failing".to_string()];
        m.pending_since = Some(ts.clone());
        assert!(store.insert_pr_monitor(&m).await.expect("insert"));
        let adopter = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&test_session(&adopter, &ws_id, &ts))
            .await
            .expect("adopter session");
        let stranger = AgentId(format!("agent-{}", Uuid::new_v4()));

        let now = now_iso();
        let stale = PrMonitorPollUpdate {
            updated_at: &now,
            expected_updated_at: "1970-01-01T00:00:00Z",
            ..Default::default()
        };
        assert!(
            !store
                .adopt_pr_monitor(&m.monitor_id, &agent_id, &adopter, stale)
                .await
                .expect("stale adopt"),
            "stale expected_updated_at must not adopt"
        );
        let wrong_from = PrMonitorPollUpdate {
            updated_at: &now,
            expected_updated_at: &m.updated_at,
            ..Default::default()
        };
        assert!(
            !store
                .adopt_pr_monitor(&m.monitor_id, &stranger, &adopter, wrong_from)
                .await
                .expect("wrong-from adopt"),
            "a from-owner mismatch must not adopt"
        );
        let untouched = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(untouched.agent_id, agent_id);
        assert_eq!(untouched.pending_changes, vec!["checks: 1 failing"]);

        assert!(store
            .adopt_pr_monitor(
                &m.monitor_id,
                &agent_id,
                &adopter,
                PrMonitorPollUpdate {
                    last_snapshot: Some(r#"{"v":2}"#),
                    baseline_snapshot: Some(r#"{"v":2}"#),
                    pending_changes: &[],
                    last_polled_at: Some(&now),
                    updated_at: &now,
                    expected_updated_at: &m.updated_at,
                    ..Default::default()
                },
            )
            .await
            .expect("adopt"));
        let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(read.agent_id, adopter, "re-parented");
        assert_eq!(read.state, PrMonitorState::Active);
        assert_eq!(read.baseline_snapshot.as_deref(), Some(r#"{"v":2}"#));
        assert_eq!(read.last_snapshot.as_deref(), Some(r#"{"v":2}"#));
        assert!(read.pending_changes.is_empty(), "pending cleared");
        assert!(read.pending_since.is_none());
        assert_eq!(read.updated_at, now);
        assert!(
            store
                .find_active_pr_monitor(&agent_id, "o", "r", 42)
                .await
                .expect("old owner lookup")
                .is_none(),
            "the old owner no longer holds it"
        );
        assert_eq!(
            store
                .find_active_pr_monitor(&adopter, "o", "r", 42)
                .await
                .expect("new owner lookup")
                .map(|m| m.monitor_id),
            Some(m.monitor_id.clone())
        );
    }

    /// `complete_pr_monitor` flips `completed` and clears the pending state
    /// in one guarded write: a stale `expected_updated_at` (the row moved —
    /// e.g. an adoption re-parented it) leaves the row active and untouched,
    /// and the successful write is not repeatable.
    #[tokio::test]
    async fn complete_pr_monitor_is_one_guarded_write() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let ts = now_iso();
        let mut m = test_monitor(&ws_id, &agent_id, &ts);
        m.pending_changes = vec!["checks: 1 failing".to_string()];
        m.pending_since = Some(ts.clone());
        m.last_change_at = Some(ts.clone());
        assert!(store.insert_pr_monitor(&m).await.expect("insert"));

        let now = now_iso();
        assert!(
            !store
                .complete_pr_monitor(&m.monitor_id, &now, "1970-01-01T00:00:00Z")
                .await
                .expect("stale complete"),
            "a stale expected_updated_at must not complete"
        );
        let untouched = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(untouched.state, PrMonitorState::Active);
        assert_eq!(untouched.pending_changes, vec!["checks: 1 failing"]);

        assert!(store
            .complete_pr_monitor(&m.monitor_id, &now, &m.updated_at)
            .await
            .expect("complete"));
        let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(read.state, PrMonitorState::Completed);
        assert!(read.pending_changes.is_empty());
        assert!(read.pending_since.is_none());
        assert!(read.last_change_at.is_none());
        assert_eq!(read.updated_at, now);
        assert_eq!(read.last_snapshot, m.last_snapshot, "snapshots untouched");
        assert!(
            !store
                .complete_pr_monitor(&m.monitor_id, &now_iso(), &now)
                .await
                .expect("repeat complete"),
            "a terminal row is never completed twice"
        );
    }

    /// An RFC 3339 whole-second UTC timestamp `secs` seconds from now (the
    /// shape of a pause deadline).
    fn rfc3339_from_now(secs: i64) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .cast_signed();
        intent_core::iso_from_unix_secs(now + secs)
    }

    /// The pause annotation reaches every ACTIVE row (across workspaces) and
    /// skips terminal rows: an empty `last_error` becomes the pause, a
    /// genuine error keeps the error and gains the pause, and an earlier
    /// annotation (bare or appended) is replaced by the new deadline rather
    /// than stacked. It moves neither `updated_at` nor `last_polled_at`, so a
    /// poll write-back guarded on the pre-stamp `updated_at` still lands —
    /// with the annotation, composed in SQL.
    #[tokio::test]
    async fn active_monitors_pause_annotation_composes_and_spares_terminal_rows() {
        let t1 = pr_monitor_pause_error(Some(&rfc3339_from_now(600)));
        let t2 = pr_monitor_pause_error(Some(&rfc3339_from_now(1800)));
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let ts = now_iso();
        let empty = test_monitor(&ws_id, &agent_id, &ts);
        let mut genuine = test_monitor(&ws_id, &agent_id, &ts);
        genuine.pr_number = 43;
        genuine.last_error = Some("forge down".to_string());
        let mut bare = test_monitor(&ws_id, &agent_id, &ts);
        bare.pr_number = 44;
        bare.last_error = Some(t1.clone());
        let mut appended = test_monitor(&ws_id, &agent_id, &ts);
        appended.pr_number = 45;
        appended.last_error = Some(format!("HTTP 502; {t1}"));
        let mut completed = test_monitor(&ws_id, &agent_id, &ts);
        completed.pr_number = 46;
        completed.state = PrMonitorState::Completed;
        completed.last_error = Some("old failure".to_string());
        for m in [&empty, &genuine, &bare, &appended, &completed] {
            assert!(store.insert_pr_monitor(m).await.expect("insert"));
        }

        let stamped = store
            .annotate_active_pr_monitors_pause(&t2)
            .await
            .expect("stamp");
        assert_eq!(stamped, 4);
        for (m, expected) in [
            (&empty, t2.clone()),
            (&genuine, format!("forge down; {t2}")),
            (&bare, t2.clone()),
            (&appended, format!("HTTP 502; {t2}")),
        ] {
            let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
            assert_eq!(read.last_error.as_deref(), Some(expected.as_str()));
            assert_eq!(
                read.updated_at, m.updated_at,
                "the guard token is untouched"
            );
            assert_eq!(read.last_polled_at, None, "a stamp is not a poll");
        }
        let terminal = store
            .get_pr_monitor(&completed.monitor_id)
            .await
            .expect("get");
        assert_eq!(terminal.last_error.as_deref(), Some("old failure"));

        // A poll that read the row before the stamp — and captured its
        // `last_error` with the gate still open, `None` — still writes back
        // against the pre-stamp guard token, and the row keeps the stamped
        // annotation: the statement composes it, the caller cannot clobber.
        let now = now_iso();
        assert!(store
            .update_pr_monitor_poll(
                &empty.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: empty.last_snapshot.as_deref(),
                    baseline_snapshot: empty.baseline_snapshot.as_deref(),
                    pending_changes: &[],
                    pending_since: None,
                    last_change_at: None,
                    last_polled_at: Some(&now),
                    last_error: None,
                    updated_at: &now,
                    expected_updated_at: &empty.updated_at,
                },
            )
            .await
            .expect("poll write-back"));
        let landed = store.get_pr_monitor(&empty.monitor_id).await.expect("get");
        assert_eq!(landed.last_error.as_deref(), Some(t2.as_str()));
        assert_eq!(landed.last_polled_at.as_deref(), Some(now.as_str()));
    }

    /// The exact gate-to-SQL schedules a Rust-side capture cannot close,
    /// driven at the statement level: the caller's `last_error` was
    /// composed from a gate read that a bulk stamp then overtook, and the
    /// guard cannot catch it (the stamp leaves `updated_at` alone). Whatever
    /// the caller captured, the landed row keeps the ROW's unexpired
    /// annotation — the caller's is neither written nor allowed to move the
    /// deadline — keeps the caller's genuine error in front, never stacks
    /// annotations, and drops an annotation whose deadline has passed — the
    /// first post-pause write clears it. `adopt_pr_monitor` composes the
    /// same way.
    #[tokio::test]
    async fn poll_write_back_composes_the_pause_against_the_row_not_the_capture() {
        async fn write_back(
            store: &Store,
            m: &PrMonitor,
            captured: Option<&str>,
        ) -> (PrMonitor, String) {
            let now = now_iso();
            assert!(store
                .update_pr_monitor_poll(
                    &m.monitor_id,
                    PrMonitorPollUpdate {
                        last_snapshot: m.last_snapshot.as_deref(),
                        baseline_snapshot: m.baseline_snapshot.as_deref(),
                        pending_changes: &[],
                        pending_since: None,
                        last_change_at: None,
                        last_polled_at: Some(&now),
                        last_error: captured,
                        updated_at: &now,
                        expected_updated_at: &m.updated_at,
                    },
                )
                .await
                .expect("write-back"));
            (store.get_pr_monitor(&m.monitor_id).await.expect("get"), now)
        }

        let t1 = pr_monitor_pause_error(Some(&rfc3339_from_now(600)));
        let t2 = pr_monitor_pause_error(Some(&rfc3339_from_now(1800)));
        let expired = pr_monitor_pause_error(Some(&rfc3339_from_now(-60)));
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let m = test_monitor(&ws_id, &agent_id, &now_iso());
        assert!(store.insert_pr_monitor(&m).await.expect("insert"));

        // Pause OPENS between the gate read (open → `None`) and the write.
        let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t1).await.unwrap(),
            1
        );
        let (row, now) = write_back(&store, &read, None).await;
        assert_eq!(row.last_error.as_deref(), Some(t1.as_str()));
        assert_eq!(row.updated_at, now, "the guarded write landed");

        // Pause EXTENDS (T1 → T2) between a gate read that saw T1 and the
        // write: the later deadline wins, once.
        let read = row;
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t2).await.unwrap(),
            1
        );
        let (row, _) = write_back(&store, &read, Some(&t1)).await;
        assert_eq!(row.last_error.as_deref(), Some(t2.as_str()));

        // A genuine error captured with the stale T1 lands in front of T2;
        // a stale capture composed from an earlier genuine error is replaced
        // by this write's genuine error, never appended.
        let read = row;
        let (row, _) = write_back(&store, &read, Some(&format!("forge down; {t1}"))).await;
        assert_eq!(row.last_error, Some(format!("forge down; {t2}")));
        let read = row;
        let (row, _) = write_back(&store, &read, Some("HTTP 502")).await;
        assert_eq!(row.last_error, Some(format!("HTTP 502; {t2}")));

        // A capture AHEAD of the row (a later deadline than the stamp on
        // it) does not move the row either: an annotation on the row is
        // no proof it is the pause the caller read, and the extension's own
        // serialized stamp is what lands a later deadline.
        let t3 = pr_monitor_pause_error(Some(&rfc3339_from_now(3600)));
        let read = row;
        let (row, _) = write_back(&store, &read, Some(&t3)).await;
        assert_eq!(row.last_error.as_deref(), Some(t2.as_str()));
        let read = row;
        let (row, _) = write_back(&store, &read, Some(&format!("HTTP 502; {t3}"))).await;
        assert_eq!(row.last_error, Some(format!("HTTP 502; {t2}")));

        // Once the row's annotation has expired (aged in place: the bulk
        // stamp is monotonic and would not roll T2 back), a write clears it
        // — with or without a genuine error, and even if the caller still
        // carries the stale annotation.
        set_last_error(&store, &m.monitor_id, Some(&expired)).await;
        let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        let (row, _) = write_back(&store, &read, Some(&expired)).await;
        assert_eq!(row.last_error, None);
        set_last_error(&store, &m.monitor_id, Some(&expired)).await;
        let read = row;
        let (row, _) = write_back(&store, &read, Some("forge down")).await;
        assert_eq!(row.last_error.as_deref(), Some("forge down"));

        // Adoption composes the same way: a re-arm with `last_error: None`
        // landing after a fresh stamp keeps the pause.
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t1).await.unwrap(),
            1
        );
        let adopter = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&test_session(&adopter, &ws_id, &now_iso()))
            .await
            .expect("adopter session");
        let now = now_iso();
        assert!(store
            .adopt_pr_monitor(
                &m.monitor_id,
                &agent_id,
                &adopter,
                PrMonitorPollUpdate {
                    last_snapshot: row.last_snapshot.as_deref(),
                    baseline_snapshot: row.last_snapshot.as_deref(),
                    pending_changes: &[],
                    last_polled_at: Some(&now),
                    updated_at: &now,
                    expected_updated_at: &row.updated_at,
                    ..Default::default()
                },
            )
            .await
            .expect("adopt"));
        let re_parented = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(re_parented.agent_id, adopter);
        assert_eq!(re_parented.last_error.as_deref(), Some(t1.as_str()));
    }

    /// Set a row's `last_error` in place, moving nothing else — how a test
    /// ages an annotation past its deadline without a clock.
    async fn set_last_error(store: &Store, monitor_id: &PrMonitorId, last_error: Option<&str>) {
        sqlx::query("UPDATE pr_monitor SET last_error = ?1 WHERE monitor_id = ?2")
            .bind(last_error)
            .bind(&monitor_id.0)
            .execute(store.write_pool())
            .await
            .expect("set last_error");
    }

    /// The pause deadline is whole-second while a write's `updated_at`
    /// carries a fraction: compared as strings, `…06:00:00Z` sorted AFTER
    /// `…06:00:00.500Z` (`'Z' > '.'`), so an annotation whose deadline had
    /// passed survived the first post-pause success landing inside the next
    /// second. Both sides now compare at whole seconds: a write in an
    /// earlier second or the deadline's own second keeps the annotation
    /// (the gate may re-open anywhere inside that second, so the pause must
    /// not clear early), a write in a later second clears it — whatever the
    /// fraction on either side.
    #[tokio::test]
    async fn pause_expiry_compares_deadline_and_write_at_whole_seconds() {
        let deadline = "2030-01-01T06:00:00Z";
        let pause = pr_monitor_pause_error(Some(deadline));
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let m = test_monitor(&ws_id, &agent_id, "2030-01-01T05:00:00.000Z");
        assert!(store.insert_pr_monitor(&m).await.expect("insert"));

        let mut expected_updated_at = m.updated_at.clone();
        for (write_at, kept) in [
            ("2030-01-01T05:59:59.999Z", true),
            ("2030-01-01T06:00:00Z", true),
            ("2030-01-01T06:00:00.500Z", true),
            ("2030-01-01T06:00:00.999999Z", true),
            ("2030-01-01T06:00:01Z", false),
            ("2030-01-01T06:00:01.000Z", false),
        ] {
            set_last_error(&store, &m.monitor_id, Some(&pause)).await;
            assert!(store
                .update_pr_monitor_poll(
                    &m.monitor_id,
                    PrMonitorPollUpdate {
                        last_snapshot: m.last_snapshot.as_deref(),
                        baseline_snapshot: m.baseline_snapshot.as_deref(),
                        pending_changes: &[],
                        last_polled_at: Some(write_at),
                        last_error: None,
                        updated_at: write_at,
                        expected_updated_at: &expected_updated_at,
                        ..Default::default()
                    },
                )
                .await
                .expect("write-back"));
            let row = store.get_pr_monitor(&m.monitor_id).await.expect("get");
            assert_eq!(
                row.last_error.as_deref(),
                kept.then_some(pause.as_str()),
                "write at {write_at} against deadline {deadline}"
            );
            expected_updated_at = row.updated_at;
        }

        // The caller's stale capture, expired the same way, clears too.
        set_last_error(&store, &m.monitor_id, Some(&pause)).await;
        let write_at = "2030-01-01T06:00:01.250Z";
        assert!(store
            .update_pr_monitor_poll(
                &m.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: m.last_snapshot.as_deref(),
                    baseline_snapshot: m.baseline_snapshot.as_deref(),
                    pending_changes: &[],
                    last_polled_at: Some(write_at),
                    last_error: Some(&format!("HTTP 502; {pause}")),
                    updated_at: write_at,
                    expected_updated_at: &expected_updated_at,
                    ..Default::default()
                },
            )
            .await
            .expect("write-back"));
        let row = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(row.last_error.as_deref(), Some("HTTP 502"));
    }

    /// Bulk stamps are issued from concurrent sweeps and can land out of
    /// order: a delayed `stamp(T1)` arriving after `stamp(T2)` must not roll
    /// the rows back to T1 while the gate still holds T2. The bulk stamp
    /// keeps the later unexpired deadline — the same "latest wins" rule as
    /// the guarded write-back — and does not count the rows it leaves alone.
    #[tokio::test]
    async fn active_monitors_pause_annotation_keeps_the_later_deadline() {
        async fn assert_rows(store: &Store, expected: [(&PrMonitor, String); 4]) {
            for (m, expected) in expected {
                let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
                assert_eq!(read.last_error.as_deref(), Some(expected.as_str()));
                assert_eq!(
                    read.updated_at, m.updated_at,
                    "the guard token is untouched"
                );
            }
        }

        let t1 = pr_monitor_pause_error(Some(&rfc3339_from_now(600)));
        let t2 = pr_monitor_pause_error(Some(&rfc3339_from_now(1800)));
        let t3 = pr_monitor_pause_error(Some(&rfc3339_from_now(3600)));
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let ts = now_iso();
        let empty = test_monitor(&ws_id, &agent_id, &ts);
        let mut genuine = test_monitor(&ws_id, &agent_id, &ts);
        genuine.pr_number = 43;
        genuine.last_error = Some("forge down".to_string());
        let mut bare = test_monitor(&ws_id, &agent_id, &ts);
        bare.pr_number = 44;
        bare.last_error = Some(t1.clone());
        let mut appended = test_monitor(&ws_id, &agent_id, &ts);
        appended.pr_number = 45;
        appended.last_error = Some(format!("HTTP 502; {t1}"));
        for m in [&empty, &genuine, &bare, &appended] {
            assert!(store.insert_pr_monitor(m).await.expect("insert"));
        }
        let expect_all = |pause: &str| {
            [
                (&empty, pause.to_string()),
                (&genuine, format!("forge down; {pause}")),
                (&bare, pause.to_string()),
                (&appended, format!("HTTP 502; {pause}")),
            ]
        };

        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t2).await.unwrap(),
            4
        );
        assert_rows(&store, expect_all(&t2)).await;

        // The delayed earlier stamp is a no-op — nothing rolls back, nothing
        // is counted.
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t1).await.unwrap(),
            0
        );
        assert_rows(&store, expect_all(&t2)).await;

        // A genuine extension still lands everywhere.
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t3).await.unwrap(),
            4
        );
        assert_rows(&store, expect_all(&t3)).await;

        // Re-stamping the SAME deadline (a later sweep re-reading the gate)
        // is idempotent — counted, unchanged.
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t3).await.unwrap(),
            4
        );
        assert_rows(&store, expect_all(&t3)).await;
    }

    /// The bulk clear strips an UNEXPIRED annotation — which the guarded
    /// write-back would otherwise keep until its deadline aged out — from
    /// every active row: a bare annotation leaves `NULL`, an appended one
    /// leaves the genuine error; rows without an annotation, and terminal
    /// rows, are neither touched nor counted, and no row's guard token or
    /// `lastPolledAt` moves.
    #[tokio::test]
    async fn clearing_the_pause_strips_unexpired_annotations_from_active_rows_only() {
        let t1 = pr_monitor_pause_error(Some(&rfc3339_from_now(1800)));
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let ts = now_iso();
        let mut bare = test_monitor(&ws_id, &agent_id, &ts);
        bare.last_error = Some(t1.clone());
        let mut appended = test_monitor(&ws_id, &agent_id, &ts);
        appended.pr_number = 43;
        appended.last_error = Some(format!("HTTP 502; {t1}"));
        let mut genuine = test_monitor(&ws_id, &agent_id, &ts);
        genuine.pr_number = 44;
        genuine.last_error = Some("forge down".to_string());
        let empty = {
            let mut m = test_monitor(&ws_id, &agent_id, &ts);
            m.pr_number = 45;
            m
        };
        let mut completed = test_monitor(&ws_id, &agent_id, &ts);
        completed.pr_number = 46;
        completed.state = PrMonitorState::Completed;
        completed.last_error = Some(t1.clone());
        for m in [&bare, &appended, &genuine, &empty, &completed] {
            assert!(store.insert_pr_monitor(m).await.expect("insert"));
        }

        assert_eq!(
            store
                .clear_active_pr_monitors_pause(Some(&t1))
                .await
                .unwrap(),
            2
        );
        for (m, expected) in [
            (&bare, None),
            (&appended, Some("HTTP 502")),
            (&genuine, Some("forge down")),
            (&empty, None),
            (&completed, Some(t1.as_str())),
        ] {
            let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
            assert_eq!(read.last_error.as_deref(), expected, "PR {}", m.pr_number);
            assert_eq!(
                read.updated_at, m.updated_at,
                "the guard token is untouched"
            );
            assert_eq!(
                read.last_polled_at, m.last_polled_at,
                "the clear is not a poll"
            );
        }

        // Nothing left to clear: a repeat is a counted no-op of zero.
        assert_eq!(
            store
                .clear_active_pr_monitors_pause(Some(&t1))
                .await
                .unwrap(),
            0
        );
        assert_eq!(store.clear_active_pr_monitors_pause(None).await.unwrap(), 0);

        // A poll write-back that read the row before the clear still lands
        // (the clear moved no guard token), and with the row's annotation
        // gone the first success leaves `lastError` empty.
        let write_at = now_iso();
        assert!(store
            .update_pr_monitor_poll(
                &appended.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: appended.last_snapshot.as_deref(),
                    baseline_snapshot: appended.baseline_snapshot.as_deref(),
                    pending_changes: &[],
                    last_polled_at: Some(&write_at),
                    last_error: None,
                    updated_at: &write_at,
                    expected_updated_at: &appended.updated_at,
                    ..Default::default()
                },
            )
            .await
            .expect("write-back"));
        let read = store
            .get_pr_monitor(&appended.monitor_id)
            .await
            .expect("get");
        assert_eq!(read.last_error, None);
    }

    /// intent-hq/intentd#1945 (review r4033765063): the clear leaves
    /// `updated_at` alone, so a poll / error / adoption write that captured
    /// the closed gate BEFORE the lift still lands afterwards carrying the
    /// lifted, unexpired T1 — and used to restore it, after which every
    /// later success kept it until T1 aged out. Replayed with the production
    /// statements: row at T1 = 07:00:00Z, the clear, then the pre-lift
    /// captures landing at 06:00:01Z onward. None resurrects the pause, a
    /// genuine error captured with it lands alone, and the next successes
    /// stay clean — the row, not the capture, says whether a pause is in
    /// force, and until when: a capture naming a later deadline than the
    /// pause the row DOES carry leaves that pause as stamped.
    #[tokio::test]
    async fn a_stale_pre_lift_capture_landing_after_the_clear_does_not_resurrect_the_pause() {
        async fn write_back(
            store: &Store,
            m: &PrMonitor,
            captured: Option<&str>,
            write_at: &str,
        ) -> PrMonitor {
            assert!(store
                .update_pr_monitor_poll(
                    &m.monitor_id,
                    PrMonitorPollUpdate {
                        last_snapshot: m.last_snapshot.as_deref(),
                        baseline_snapshot: m.baseline_snapshot.as_deref(),
                        pending_changes: &[],
                        last_polled_at: Some(write_at),
                        last_error: captured,
                        updated_at: write_at,
                        expected_updated_at: &m.updated_at,
                        ..Default::default()
                    },
                )
                .await
                .expect("write-back"));
            let row = store.get_pr_monitor(&m.monitor_id).await.expect("get");
            assert_eq!(row.updated_at, write_at, "the guarded write landed");
            row
        }

        let t1 = pr_monitor_pause_error(Some("2030-01-01T07:00:00Z"));
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let m = test_monitor(&ws_id, &agent_id, "2030-01-01T05:00:00.000Z");
        assert!(store.insert_pr_monitor(&m).await.expect("insert"));
        set_last_error(&store, &m.monitor_id, Some(&t1)).await;
        // The images the in-flight writers read: the row under T1.
        let pre_lift = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(pre_lift.last_error.as_deref(), Some(t1.as_str()));

        // The lift's reconciliation lands first.
        assert_eq!(
            store
                .clear_active_pr_monitors_pause(Some(&t1))
                .await
                .unwrap(),
            1
        );

        // A success that captured T1 lands: no pause comes back.
        let row = write_back(&store, &pre_lift, Some(&t1), "2030-01-01T06:00:01Z").await;
        assert_eq!(row.last_error, None, "the stale capture is not written");
        // Nor does a later success, whatever it carries.
        let row = write_back(&store, &row, None, "2030-01-01T06:00:30Z").await;
        assert_eq!(row.last_error, None);
        let row = write_back(&store, &row, Some(&t1), "2030-01-01T06:00:31Z").await;
        assert_eq!(row.last_error, None);

        // A genuine error captured with the stale T1 lands alone.
        let row = write_back(
            &store,
            &row,
            Some(&format!("forge down; {t1}")),
            "2030-01-01T06:01:00Z",
        )
        .await;
        assert_eq!(row.last_error.as_deref(), Some("forge down"));
        let row = write_back(&store, &row, None, "2030-01-01T06:01:30Z").await;
        assert_eq!(row.last_error, None);

        // Adoption composes the same way: a re-arm whose capture carried
        // T1 lands re-parented and clean.
        let adopter = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&test_session(&adopter, &ws_id, &now_iso()))
            .await
            .expect("adopter session");
        let write_at = "2030-01-01T06:02:00Z";
        assert!(store
            .adopt_pr_monitor(
                &m.monitor_id,
                &agent_id,
                &adopter,
                PrMonitorPollUpdate {
                    last_snapshot: row.last_snapshot.as_deref(),
                    baseline_snapshot: row.last_snapshot.as_deref(),
                    pending_changes: &[],
                    last_polled_at: Some(write_at),
                    last_error: Some(&t1),
                    updated_at: write_at,
                    expected_updated_at: &row.updated_at,
                    ..Default::default()
                },
            )
            .await
            .expect("adopt"));
        let row = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(row.agent_id, adopter);
        assert_eq!(row.last_error, None);

        // Where the row DOES carry a pause, the capture cannot move it: a
        // fresh stamp at T1 with a capture naming T2 (an extension read
        // ahead of its stamp — or a lifted pause, indistinguishable here)
        // stays at T1 until T2's own stamp lands.
        let t2 = pr_monitor_pause_error(Some("2030-01-01T08:00:00Z"));
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t1).await.unwrap(),
            1
        );
        let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        let row = write_back(&store, &read, Some(&t2), "2030-01-01T06:03:00Z").await;
        assert_eq!(row.last_error.as_deref(), Some(t1.as_str()));
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t2).await.unwrap(),
            1
        );
        let row = write_back(&store, &row, Some(&t1), "2030-01-01T06:03:01Z").await;
        assert_eq!(row.last_error.as_deref(), Some(t2.as_str()));
    }

    /// intent-hq/intentd#1945 (review r4033765051): the gate transition
    /// and the bulk clear are separate operations, so a clear delayed past
    /// a NEWER pause's stamp — sweep A lifts T1, an in-flight fetch in
    /// sweep B trips the limit, opens T2 and stamps it, then A's clear
    /// lands — used to erase every T2 annotation while the gate held T2,
    /// and no paused tick stamps again. Replayed with the production
    /// statements: `annotate(T2)` then the clear for the lifted T1 leaves
    /// T2 in place, genuine prefixes intact, while a row still at T1 is
    /// cleared; the clear for T2 itself, and the boot clear with no lifted
    /// deadline, strip everything.
    #[tokio::test]
    async fn a_delayed_clear_for_a_lifted_pause_leaves_a_newer_pause_in_place() {
        let t1 = pr_monitor_pause_error(Some(&rfc3339_from_now(600)));
        let t2 = pr_monitor_pause_error(Some(&rfc3339_from_now(1800)));
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let ts = now_iso();
        let mut bare = test_monitor(&ws_id, &agent_id, &ts);
        bare.last_error = Some(t1.clone());
        let mut appended = test_monitor(&ws_id, &agent_id, &ts);
        appended.pr_number = 43;
        appended.last_error = Some(format!("HTTP 502; {t1}"));
        // A row the newer stamp did not reach (it was a terminal row when
        // T2 was stamped, re-activated since — or simply a delayed stamp).
        let mut stale = test_monitor(&ws_id, &agent_id, &ts);
        stale.pr_number = 44;
        stale.state = PrMonitorState::Completed;
        stale.last_error = Some(t1.clone());
        for m in [&bare, &appended, &stale] {
            assert!(store.insert_pr_monitor(m).await.expect("insert"));
        }
        let errors = |store: &Store| {
            let store = store.clone();
            let ids = [
                bare.monitor_id.clone(),
                appended.monitor_id.clone(),
                stale.monitor_id.clone(),
            ];
            async move {
                let mut out = Vec::new();
                for id in &ids {
                    out.push(store.get_pr_monitor(id).await.expect("get").last_error);
                }
                out
            }
        };

        // Sweep A lifted T1; before its clear runs, sweep B opens and stamps
        // T2 on the active rows.
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t2).await.unwrap(),
            2
        );
        sqlx::query("UPDATE pr_monitor SET state = 'active' WHERE monitor_id = ?1")
            .bind(&stale.monitor_id.0)
            .execute(store.write_pool())
            .await
            .expect("re-activate");

        // A's delayed clear for T1: the T2 rows survive with their genuine
        // prefixes, only the row still at T1 is cleared.
        assert_eq!(
            store
                .clear_active_pr_monitors_pause(Some(&t1))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            errors(&store).await,
            vec![Some(t2.clone()), Some(format!("HTTP 502; {t2}")), None]
        );
        for m in [&bare, &appended] {
            let read = store.get_pr_monitor(&m.monitor_id).await.expect("get");
            assert_eq!(
                read.updated_at, m.updated_at,
                "the guard token is untouched"
            );
        }

        // The lift of T2 itself clears T2.
        assert_eq!(
            store
                .clear_active_pr_monitors_pause(Some(&t2))
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            errors(&store).await,
            vec![None, Some("HTTP 502".into()), None]
        );

        // Boot: no lifted deadline strips every annotation, whatever it
        // names — including one the deadline of which does not parse.
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&t2).await.unwrap(),
            3
        );
        set_last_error(
            &store,
            &stale.monitor_id,
            Some(&pr_monitor_pause_error(None)),
        )
        .await;
        assert_eq!(store.clear_active_pr_monitors_pause(None).await.unwrap(), 3);
        assert_eq!(
            errors(&store).await,
            vec![None, Some("HTTP 502".into()), None]
        );
    }

    /// intent-hq/intentd#1945 (review r4034060318): an annotation on the
    /// row is not proof of the SAME pause the caller captured. Pause O
    /// (08:00) is lifted and cleared, a shorter pause N (07:00) opens and is
    /// stamped, then a write-back that captured O ahead of the lift lands
    /// on the now-annotated row with an unchanged guard token — the
    /// "later deadline wins" rule used to upgrade N to the obsolete O, which
    /// the lift of N (scoped to N) then left in place. Replayed with the
    /// production statements across the poll, error, and adoption paths:
    /// the caller never advances the row's deadline — only the serialized
    /// bulk stamp does — so the row stays at N, a genuine error captured
    /// with O lands in front of N, and the lift of N leaves the row bare.
    #[tokio::test]
    async fn a_stale_capture_of_a_lifted_pause_cannot_upgrade_a_newer_pause() {
        async fn write_back(
            store: &Store,
            m: &PrMonitor,
            captured: Option<&str>,
            write_at: &str,
        ) -> PrMonitor {
            assert!(store
                .update_pr_monitor_poll(
                    &m.monitor_id,
                    PrMonitorPollUpdate {
                        last_snapshot: m.last_snapshot.as_deref(),
                        baseline_snapshot: m.baseline_snapshot.as_deref(),
                        pending_changes: &[],
                        last_polled_at: Some(write_at),
                        last_error: captured,
                        updated_at: write_at,
                        expected_updated_at: &m.updated_at,
                        ..Default::default()
                    },
                )
                .await
                .expect("write-back"));
            let row = store.get_pr_monitor(&m.monitor_id).await.expect("get");
            assert_eq!(row.updated_at, write_at, "the guarded write landed");
            row
        }

        let o = pr_monitor_pause_error(Some("2030-01-01T08:00:00Z"));
        let n = pr_monitor_pause_error(Some("2030-01-01T07:00:00Z"));
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        let m = test_monitor(&ws_id, &agent_id, "2030-01-01T05:00:00.000Z");
        assert!(store.insert_pr_monitor(&m).await.expect("insert"));

        // Pause O is stamped; the in-flight writers read the row under O.
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&o).await.unwrap(),
            1
        );
        let pre_lift = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(pre_lift.last_error.as_deref(), Some(o.as_str()));

        // O is lifted early and cleared; a shorter pause N opens and is
        // stamped. Neither moves the guard token.
        assert_eq!(
            store
                .clear_active_pr_monitors_pause(Some(&o))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&n).await.unwrap(),
            1
        );
        let stamped = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(stamped.last_error.as_deref(), Some(n.as_str()));
        assert_eq!(stamped.updated_at, pre_lift.updated_at);

        // Poll path: a success that captured O lands — the row stays at N.
        let row = write_back(&store, &pre_lift, Some(&o), "2030-01-01T06:00:33Z").await;
        assert_eq!(
            row.last_error.as_deref(),
            Some(n.as_str()),
            "a stale capture of a lifted pause must not advance the row's deadline"
        );

        // Error path: a genuine error captured with O lands in front of N.
        let row = write_back(
            &store,
            &row,
            Some(&format!("forge down; {o}")),
            "2030-01-01T06:00:34Z",
        )
        .await;
        assert_eq!(row.last_error, Some(format!("forge down; {n}")));

        // Adoption path: a re-arm whose capture carried O lands re-parented
        // and still at N.
        let adopter = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&test_session(&adopter, &ws_id, &now_iso()))
            .await
            .expect("adopter session");
        let write_at = "2030-01-01T06:00:35Z";
        assert!(store
            .adopt_pr_monitor(
                &m.monitor_id,
                &agent_id,
                &adopter,
                PrMonitorPollUpdate {
                    last_snapshot: row.last_snapshot.as_deref(),
                    baseline_snapshot: row.last_snapshot.as_deref(),
                    pending_changes: &[],
                    last_polled_at: Some(write_at),
                    last_error: Some(&o),
                    updated_at: write_at,
                    expected_updated_at: &row.updated_at,
                    ..Default::default()
                },
            )
            .await
            .expect("adopt"));
        let row = store.get_pr_monitor(&m.monitor_id).await.expect("get");
        assert_eq!(row.agent_id, adopter);
        assert_eq!(row.last_error.as_deref(), Some(n.as_str()));

        // The lift of N reconciles the row: nothing obsolete is left behind.
        assert_eq!(
            store
                .clear_active_pr_monitors_pause(Some(&n))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .get_pr_monitor(&m.monitor_id)
                .await
                .expect("get")
                .last_error,
            None
        );
    }

    /// The 0089 migration backfills `baseline_snapshot` from `last_snapshot`
    /// on pre-existing rows (simulated by dropping the column and re-running
    /// the migration file verbatim), so active monitors upgraded across the
    /// migration keep a usable emit baseline. A NULL `last_snapshot` stays
    /// NULL.
    #[tokio::test]
    async fn migration_backfills_baseline_from_last_snapshot() {
        let (_tmp, store, ws_id, agent_id) = store_with_owner().await;
        sqlx::query("ALTER TABLE pr_monitor DROP COLUMN baseline_snapshot")
            .execute(store.write_pool())
            .await
            .expect("drop 0089 column");

        let ts = now_iso();
        for (id, snapshot) in [
            ("prmon-polled", Some(r#"{"v":7}"#)),
            ("prmon-never-polled", None),
        ] {
            sqlx::query(
                "INSERT INTO pr_monitor (monitor_id, workspace_id, agent_id, repo_owner, \
                 repo_name, pr_number, state, last_snapshot, created_at, updated_at) \
                 VALUES (?, ?, ?, 'o', 'r', ?, 'active', ?, ?, ?)",
            )
            .bind(id)
            .bind(&ws_id.0)
            .bind(&agent_id.0)
            .bind(if snapshot.is_some() { 1_i64 } else { 2_i64 })
            .bind(snapshot)
            .bind(&ts)
            .bind(&ts)
            .execute(store.write_pool())
            .await
            .expect("insert raw pre-0089 row");
        }

        sqlx::raw_sql(include_str!("../migrations/0089_pr_monitor_baseline.sql"))
            .execute(store.write_pool())
            .await
            .expect("re-run 0089 migration");

        let polled = store
            .get_pr_monitor(&PrMonitorId("prmon-polled".to_string()))
            .await
            .expect("get polled");
        assert_eq!(
            polled.baseline_snapshot.as_deref(),
            Some(r#"{"v":7}"#),
            "baseline backfilled from last_snapshot"
        );
        let never = store
            .get_pr_monitor(&PrMonitorId("prmon-never-polled".to_string()))
            .await
            .expect("get never-polled");
        assert_eq!(never.baseline_snapshot, None, "NULL stays NULL");
    }

    /// A second agent session in `ws_id` (the FK target a second-owner
    /// `pr_monitor` row needs).
    async fn add_agent(store: &Store, ws_id: &WorkspaceId) -> AgentId {
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&test_session(&agent_id, ws_id, &now_iso()))
            .await
            .expect("insert second session");
        agent_id
    }

    /// `idx_pr_monitor_workspace_identity`: a second ACTIVE monitor on the
    /// same `(workspace, repo, PR)` is rejected as `Ok(false)` even when a
    /// DIFFERENT agent inserts it (the per-agent index alone would have let
    /// it through), the same agent's duplicate is still `Ok(false)` via the
    /// per-agent index, another workspace may monitor the same PR, and once
    /// the owner's row turns terminal the other agent can register.
    #[tokio::test]
    async fn workspace_identity_index_rejects_second_agents_active_monitor() {
        let (_tmp, store, ws_id, owner) = store_with_owner().await;
        let other = add_agent(&store, &ws_id).await;
        let ts = now_iso();

        let first = test_monitor(&ws_id, &owner, &ts);
        assert!(store.insert_pr_monitor(&first).await.expect("insert owner"));
        assert!(
            !store
                .insert_pr_monitor(&test_monitor(&ws_id, &owner, &ts))
                .await
                .expect("same-agent duplicate"),
            "per-agent index still rejects the owner's own duplicate"
        );
        let duplicate = test_monitor(&ws_id, &other, &ts);
        assert!(
            !store
                .insert_pr_monitor(&duplicate)
                .await
                .expect("other-agent duplicate"),
            "workspace index rejects another agent's active monitor on the same PR"
        );
        assert!(
            store.get_pr_monitor(&duplicate.monitor_id).await.is_err(),
            "rejected row was not inserted"
        );

        // A different PR in the same workspace is fine.
        let mut other_pr = test_monitor(&ws_id, &other, &ts);
        other_pr.pr_number = 43;
        assert!(store.insert_pr_monitor(&other_pr).await.expect("other pr"));

        // Another workspace may monitor the same PR (no cross-workspace rule).
        let ws_b = WorkspaceId("ws-pr-monitor-b".to_string());
        store
            .insert_workspace(&test_workspace(&ws_b, &ts))
            .await
            .expect("insert ws b");
        let agent_b = add_agent(&store, &ws_b).await;
        assert!(store
            .insert_pr_monitor(&test_monitor(&ws_b, &agent_b, &ts))
            .await
            .expect("other workspace"));

        // Once the owner's row is terminal the other agent can register.
        assert!(store
            .update_pr_monitor_state(&first.monitor_id, PrMonitorState::Cancelled, &now_iso())
            .await
            .expect("cancel owner"));
        assert!(
            store
                .insert_pr_monitor(&test_monitor(&ws_id, &other, &ts))
                .await
                .expect("register after cancel"),
            "terminal rows do not block a fresh registration"
        );
    }

    /// `find_active_pr_monitor_in_workspace` returns the workspace's ACTIVE
    /// row for the PR whichever agent owns it, ignores other PRs and other
    /// workspaces, and returns `None` once only terminal rows remain.
    #[tokio::test]
    async fn find_active_pr_monitor_in_workspace_is_owner_agnostic() {
        let (_tmp, store, ws_id, owner) = store_with_owner().await;
        let other = add_agent(&store, &ws_id).await;
        let ts = now_iso();

        assert_eq!(
            store
                .find_active_pr_monitor_in_workspace(&ws_id, "o", "r", 42)
                .await
                .expect("find empty"),
            None
        );

        let first = test_monitor(&ws_id, &owner, &ts);
        assert!(store.insert_pr_monitor(&first).await.expect("insert owner"));
        let mut other_pr = test_monitor(&ws_id, &other, &ts);
        other_pr.pr_number = 43;
        assert!(store.insert_pr_monitor(&other_pr).await.expect("other pr"));

        let found = store
            .find_active_pr_monitor_in_workspace(&ws_id, "o", "r", 42)
            .await
            .expect("find")
            .expect("owner's row found by a workspace-scoped lookup");
        assert_eq!(found.monitor_id, first.monitor_id);
        assert_eq!(found.agent_id, owner, "the owning agent is reported");
        assert_ne!(found.agent_id, other);
        assert_eq!(
            store
                .find_active_pr_monitor_in_workspace(&ws_id, "o", "r", 44)
                .await
                .expect("find other pr"),
            None,
            "unmonitored PR"
        );
        assert_eq!(
            store
                .find_active_pr_monitor_in_workspace(
                    &WorkspaceId("ws-elsewhere".to_string()),
                    "o",
                    "r",
                    42
                )
                .await
                .expect("find other ws"),
            None,
            "scoped to the workspace"
        );

        assert!(store
            .update_pr_monitor_state(&first.monitor_id, PrMonitorState::Completed, &now_iso())
            .await
            .expect("complete"));
        assert_eq!(
            store
                .find_active_pr_monitor_in_workspace(&ws_id, "o", "r", 42)
                .await
                .expect("find after complete"),
            None,
            "terminal rows are not returned"
        );
    }

    /// The 0118 migration dedupes pre-existing duplicate ACTIVE monitors on
    /// one `(workspace, repo, PR)` before creating the workspace-scoped
    /// unique index (simulated by dropping the index, inserting the
    /// duplicates, and re-running the migration file verbatim): the oldest
    /// row (`created_at`, then `monitor_id`) stays `active`, the others turn
    /// `cancelled`, and rows for other PRs / other workspaces are untouched.
    #[tokio::test]
    async fn migration_dedupes_duplicate_active_monitors_oldest_wins() {
        let (_tmp, store, ws_id, owner) = store_with_owner().await;
        sqlx::query("DROP INDEX idx_pr_monitor_workspace_identity")
            .execute(store.write_pool())
            .await
            .expect("drop 0118 index");
        let other = add_agent(&store, &ws_id).await;
        let third = add_agent(&store, &ws_id).await;
        let ws_b = WorkspaceId("ws-pr-monitor-b".to_string());
        store
            .insert_workspace(&test_workspace(&ws_b, &now_iso()))
            .await
            .expect("insert ws b");
        let agent_b = add_agent(&store, &ws_b).await;

        let mk = |id: &str, ws: &WorkspaceId, agent: &AgentId, pr: i64, created: &str| {
            let mut m = test_monitor(ws, agent, created);
            m.monitor_id = PrMonitorId(id.to_string());
            m.pr_number = pr;
            m
        };
        let rows = [
            // Duplicates on PR 42 in ws: `oldest` wins on created_at; `tie-a`
            // and `tie-b` share a later created_at and lose regardless.
            mk("prmon-tie-b", &ws_id, &third, 42, "2026-01-02T00:00:00Z"),
            mk("prmon-oldest", &ws_id, &other, 42, "2026-01-01T00:00:00Z"),
            mk("prmon-tie-a", &ws_id, &owner, 42, "2026-01-02T00:00:00Z"),
            // Different PR in the same workspace: untouched.
            mk("prmon-other-pr", &ws_id, &owner, 43, "2026-01-03T00:00:00Z"),
            // Same PR in another workspace: untouched.
            mk(
                "prmon-other-ws",
                &ws_b,
                &agent_b,
                42,
                "2026-01-03T00:00:00Z",
            ),
        ];
        for m in &rows {
            assert!(
                store.insert_pr_monitor(m).await.expect("insert"),
                "{} inserts while the workspace index is absent",
                m.monitor_id.0
            );
        }
        // PR 44: two rows with equal `created_at` exercise the monitor_id
        // tiebreak on its own.
        for (id, agent) in [("prmon-tie-z", &owner), ("prmon-tie-y", &other)] {
            let m = mk(id, &ws_id, agent, 44, "2026-01-05T00:00:00Z");
            assert!(store.insert_pr_monitor(&m).await.expect("insert tie"));
        }

        sqlx::raw_sql(include_str!(
            "../migrations/0118_pr_monitor_workspace_identity.sql"
        ))
        .execute(store.write_pool())
        .await
        .expect("re-run 0118 migration");

        let state_of = |id: &str| {
            let store = &store;
            let id = PrMonitorId(id.to_string());
            async move { store.get_pr_monitor(&id).await.expect("get").state }
        };
        assert_eq!(state_of("prmon-oldest").await, PrMonitorState::Active);
        assert_eq!(state_of("prmon-tie-a").await, PrMonitorState::Cancelled);
        assert_eq!(state_of("prmon-tie-b").await, PrMonitorState::Cancelled);
        assert_eq!(
            state_of("prmon-other-pr").await,
            PrMonitorState::Active,
            "other PR in the workspace untouched"
        );
        assert_eq!(
            state_of("prmon-other-ws").await,
            PrMonitorState::Active,
            "same PR in another workspace untouched"
        );
        assert_eq!(
            state_of("prmon-tie-y").await,
            PrMonitorState::Active,
            "equal created_at: lowest monitor_id wins"
        );
        assert_eq!(state_of("prmon-tie-z").await, PrMonitorState::Cancelled);

        let active_in_ws = store
            .list_active_pr_monitors_by_workspace(&ws_id)
            .await
            .expect("list active");
        let ids: Vec<&str> = active_in_ws
            .iter()
            .map(|m| m.monitor_id.0.as_str())
            .collect();
        assert_eq!(ids, vec!["prmon-oldest", "prmon-other-pr", "prmon-tie-y"]);

        // The re-created index enforces the rule from here on.
        assert!(
            !store
                .insert_pr_monitor(&mk(
                    "prmon-late",
                    &ws_id,
                    &third,
                    42,
                    "2026-01-09T00:00:00Z"
                ))
                .await
                .expect("insert after migration"),
            "workspace index rejects a new duplicate"
        );
    }

    /// Forge repo slugs are case-insensitive: both identity indexes and both
    /// active-monitor lookups compare `repo_owner` / `repo_name` under
    /// `COLLATE NOCASE` (migration `0119`), while the stored casing is kept
    /// verbatim. A different repo that merely shares letters is still
    /// distinct.
    #[tokio::test]
    async fn identity_indexes_and_lookups_ignore_repo_slug_case() {
        let (_tmp, store, ws_id, owner) = store_with_owner().await;
        let other = add_agent(&store, &ws_id).await;
        let ts = now_iso();

        let mut first = test_monitor(&ws_id, &owner, &ts);
        first.repo_owner = "Intent-HQ".to_string();
        first.repo_name = "IntentD".to_string();
        assert!(store.insert_pr_monitor(&first).await.expect("insert owner"));

        let found = store
            .find_active_pr_monitor(&owner, "intent-hq", "intentd", 42)
            .await
            .expect("find per-agent")
            .expect("per-agent lookup matches a case variant");
        assert_eq!(found.monitor_id, first.monitor_id);
        assert_eq!(found.repo_owner, "Intent-HQ", "stored casing preserved");
        assert_eq!(found.repo_name, "IntentD", "stored casing preserved");
        let found = store
            .find_active_pr_monitor_in_workspace(&ws_id, "INTENT-HQ", "intentd", 42)
            .await
            .expect("find in workspace")
            .expect("workspace lookup matches a case variant");
        assert_eq!(found.monitor_id, first.monitor_id);

        let mut same_agent = test_monitor(&ws_id, &owner, &ts);
        same_agent.repo_owner = "intent-hq".to_string();
        same_agent.repo_name = "intentd".to_string();
        assert!(
            !store
                .insert_pr_monitor(&same_agent)
                .await
                .expect("same-agent case variant"),
            "per-agent index rejects a case-variant duplicate"
        );
        let mut other_agent = test_monitor(&ws_id, &other, &ts);
        other_agent.repo_owner = "intent-hq".to_string();
        other_agent.repo_name = "INTENTD".to_string();
        assert!(
            !store
                .insert_pr_monitor(&other_agent)
                .await
                .expect("other-agent case variant"),
            "workspace index rejects another agent's case-variant duplicate"
        );

        let mut distinct = test_monitor(&ws_id, &other, &ts);
        distinct.repo_owner = "intent-hq".to_string();
        distinct.repo_name = "intentd-fe".to_string();
        assert!(
            store
                .insert_pr_monitor(&distinct)
                .await
                .expect("distinct repo"),
            "a different repo is not a duplicate"
        );
        assert_eq!(
            store
                .find_active_pr_monitor(&owner, "intent-hq", "intent", 42)
                .await
                .expect("find other repo"),
            None
        );
    }

    /// The 0119 migration dedupes pre-existing ACTIVE monitors whose repo
    /// slugs differ only by case before re-creating both identity indexes
    /// with `COLLATE NOCASE` (simulated by dropping the indexes, inserting
    /// the case-variant duplicates, and re-running the migration file
    /// verbatim): the oldest row (`created_at`, then `monitor_id`) stays
    /// `active`, the rest turn `cancelled`, rows for other PRs / other
    /// workspaces are untouched, and the new indexes reject fresh
    /// case-variant duplicates.
    #[tokio::test]
    async fn migration_dedupes_case_variant_active_monitors_oldest_wins() {
        let (_tmp, store, ws_id, owner) = store_with_owner().await;
        for idx in [
            "idx_pr_monitor_identity",
            "idx_pr_monitor_workspace_identity",
        ] {
            sqlx::query(&format!("DROP INDEX {idx}"))
                .execute(store.write_pool())
                .await
                .expect("drop identity index");
        }
        let other = add_agent(&store, &ws_id).await;
        let ws_b = WorkspaceId("ws-pr-monitor-b".to_string());
        store
            .insert_workspace(&test_workspace(&ws_b, &now_iso()))
            .await
            .expect("insert ws b");
        let agent_b = add_agent(&store, &ws_b).await;

        let mk = |id: &str,
                  ws: &WorkspaceId,
                  agent: &AgentId,
                  slug: (&str, &str),
                  pr: i64,
                  created: &str| {
            let mut m = test_monitor(ws, agent, created);
            m.monitor_id = PrMonitorId(id.to_string());
            m.repo_owner = slug.0.to_string();
            m.repo_name = slug.1.to_string();
            m.pr_number = pr;
            m
        };
        let rows = [
            // Same agent, case-variant slugs: oldest wins.
            mk(
                "prmon-upper",
                &ws_id,
                &owner,
                ("O", "R"),
                42,
                "2026-01-02T00:00:00Z",
            ),
            mk(
                "prmon-lower",
                &ws_id,
                &owner,
                ("o", "r"),
                42,
                "2026-01-01T00:00:00Z",
            ),
            // Another agent, another case variant of the same PR: loses too.
            mk(
                "prmon-mixed",
                &ws_id,
                &other,
                ("o", "R"),
                42,
                "2026-01-03T00:00:00Z",
            ),
            // Different PR in the same workspace: untouched.
            mk(
                "prmon-other-pr",
                &ws_id,
                &other,
                ("O", "R"),
                43,
                "2026-01-03T00:00:00Z",
            ),
            // Same PR in another workspace: untouched.
            mk(
                "prmon-other-ws",
                &ws_b,
                &agent_b,
                ("O", "r"),
                42,
                "2026-01-03T00:00:00Z",
            ),
        ];
        for m in &rows {
            assert!(
                store.insert_pr_monitor(m).await.expect("insert"),
                "{} inserts while the identity indexes are absent",
                m.monitor_id.0
            );
        }

        sqlx::raw_sql(include_str!(
            "../migrations/0119_pr_monitor_identity_nocase.sql"
        ))
        .execute(store.write_pool())
        .await
        .expect("re-run 0119 migration");

        let state_of = |id: &str| {
            let store = &store;
            let id = PrMonitorId(id.to_string());
            async move { store.get_pr_monitor(&id).await.expect("get").state }
        };
        assert_eq!(state_of("prmon-lower").await, PrMonitorState::Active);
        assert_eq!(state_of("prmon-upper").await, PrMonitorState::Cancelled);
        assert_eq!(state_of("prmon-mixed").await, PrMonitorState::Cancelled);
        assert_eq!(
            state_of("prmon-other-pr").await,
            PrMonitorState::Active,
            "other PR in the workspace untouched"
        );
        assert_eq!(
            state_of("prmon-other-ws").await,
            PrMonitorState::Active,
            "same PR in another workspace untouched"
        );
        assert_eq!(
            store
                .get_pr_monitor(&PrMonitorId("prmon-other-pr".to_string()))
                .await
                .expect("get")
                .repo_owner,
            "O",
            "the migration never rewrites stored casing"
        );

        // The re-created indexes enforce the rule from here on.
        assert!(
            !store
                .insert_pr_monitor(&mk(
                    "prmon-late-agent",
                    &ws_id,
                    &owner,
                    ("O", "R"),
                    42,
                    "2026-01-09T00:00:00Z"
                ))
                .await
                .expect("insert after migration"),
            "per-agent index rejects a new case-variant duplicate"
        );
        assert!(
            !store
                .insert_pr_monitor(&mk(
                    "prmon-late-ws",
                    &ws_id,
                    &other,
                    ("O", "R"),
                    42,
                    "2026-01-09T00:00:00Z"
                ))
                .await
                .expect("insert after migration"),
            "workspace index rejects a new case-variant duplicate"
        );
    }
}
