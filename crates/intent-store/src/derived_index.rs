//! Detection and offline repair of derived search indexes
//! (intent-hq/intent#8592).
//!
//! `note_fts` (0141) and `agent_message_fts` (0074) are contentless FTS5
//! tables maintained by triggers on `note` / `agent_message`. When one is
//! damaged, every write that fires its trigger fails with
//! `(code: 267) database disk image is malformed` (`SQLITE_CORRUPT_VTAB`),
//! even though the canonical rows are intact. Because the indexes hold only
//! derived data, rebuilding them from the canonical tables loses nothing.
//!
//! [`check_integrity`] is read-only (safe under `query_only`, e.g. from
//! `intentd doctor` while the daemon runs). [`repair_derived_indexes`] is the
//! offline path: it takes daemon ownership, copies the database files to a
//! backup directory, rebuilds the damaged indexes and re-checks.

use std::path::{Path, PathBuf};
use std::time::Duration;

use sqlx::SqlitePool;

use crate::{Error, Result};

/// A derived full-text index that can be rebuilt from canonical tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DerivedIndex {
    /// `note_fts`, rebuilt from `note` + `note_search_ctx`.
    NoteSearch,
    /// `agent_message_fts` + `agent_message_search_ctx`, rebuilt from
    /// `agent_message`.
    MessageSearch,
}

impl DerivedIndex {
    /// Every derived index, in repair order.
    pub const ALL: [Self; 2] = [Self::NoteSearch, Self::MessageSearch];

    /// The FTS5 table name.
    #[must_use]
    pub fn table(self) -> &'static str {
        match self {
            Self::NoteSearch => "note_fts",
            Self::MessageSearch => "agent_message_fts",
        }
    }

    /// Short human description for CLI output.
    #[must_use]
    pub fn description(self) -> &'static str {
        match self {
            Self::NoteSearch => "note search index",
            Self::MessageSearch => "chat message search index",
        }
    }

    /// The index owning `table`: the FTS5 table itself or one of its
    /// `_data` / `_idx` / `_config` / `_docsize` / `_content` shadow tables.
    fn owning(table: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|index| {
            table == index.table()
                || table
                    .strip_prefix(index.table())
                    .and_then(|rest| rest.strip_prefix('_'))
                    .is_some_and(|suffix| {
                        matches!(suffix, "data" | "idx" | "config" | "docsize" | "content")
                    })
        })
    }

    async fn rebuild(self, conn: &mut sqlx::SqliteConnection) -> Result<()> {
        match self {
            Self::NoteSearch => rebuild_note_search(conn).await,
            Self::MessageSearch => crate::agent_repo::rebuild_agent_message_search(conn).await,
        }
    }
}

/// One `PRAGMA integrity_check` row, attributed where possible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityProblem {
    /// The row as reported by `SQLite`.
    pub message: String,
    /// The table the row refers to, when it names one (directly, through an
    /// index name, or through a B-tree root page).
    pub table: Option<String>,
    /// Set when the problem is the logical content of a derived FTS5 index,
    /// which [`repair_derived_indexes`] can rebuild. Page-level (B-tree)
    /// damage is never marked repairable, even inside a shadow table.
    pub repairable: Option<DerivedIndex>,
}

/// Classified `PRAGMA integrity_check` result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntegrityReport {
    pub problems: Vec<IntegrityProblem>,
}

impl IntegrityReport {
    /// True when `SQLite` reported a single `ok` row.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.problems.is_empty()
    }

    /// Derived indexes a rebuild can fix, without duplicates, in repair order.
    #[must_use]
    pub fn damaged_indexes(&self) -> Vec<DerivedIndex> {
        DerivedIndex::ALL
            .into_iter()
            .filter(|index| self.problems.iter().any(|p| p.repairable == Some(*index)))
            .collect()
    }

    /// True when some problem is outside the rebuildable derived indexes.
    #[must_use]
    pub fn has_unrepairable(&self) -> bool {
        self.problems.iter().any(|p| p.repairable.is_none())
    }
}

/// Run `PRAGMA integrity_check` and attribute each problem. Read-only.
///
/// # Errors
///
/// Returns `Error::Internal` when the check itself cannot run (for example a
/// damaged schema); the caller should then treat the whole file as suspect.
pub async fn check_integrity(pool: &SqlitePool) -> Result<IntegrityReport> {
    let rows: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(pool)
        .await
        .map_err(|e| Error::Internal(format!("integrity_check failed: {e}")))?;
    if rows.len() == 1 && rows[0] == "ok" {
        return Ok(IntegrityReport::default());
    }
    let mut problems = Vec::with_capacity(rows.len());
    for message in rows {
        let (table, repairable) = attribute(pool, &message).await;
        problems.push(IntegrityProblem {
            message,
            table,
            repairable,
        });
    }
    Ok(IntegrityReport { problems })
}

/// Attribute one integrity row. Lookups that fail leave the row unattributed.
async fn attribute(pool: &SqlitePool, message: &str) -> (Option<String>, Option<DerivedIndex>) {
    // FTS5 xIntegrity: "malformed inverted index for FTS5 table main.note_fts".
    if let Some(name) = word_after(message, "FTS5 table main.") {
        let index = DerivedIndex::owning(&name);
        return (Some(name), index);
    }
    // B-tree damage: "Tree 23 page 45 cell 3: ..." (first row may carry a
    // "*** in database main ***" header line).
    if let Some(root) = word_after(message, "Tree ").and_then(|n| n.parse::<i64>().ok()) {
        let table = sqlx::query_scalar::<_, String>(
            "SELECT tbl_name FROM sqlite_schema WHERE rootpage = ?",
        )
        .bind(root)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        return (table, None);
    }
    // Index content: "row 5 missing from index note_ws" / "wrong # of entries in index X".
    if let Some(index) = word_after(message, "index ") {
        let table = sqlx::query_scalar::<_, String>(
            "SELECT tbl_name FROM sqlite_schema WHERE type = 'index' AND name = ?",
        )
        .bind(&index)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        return (table, None);
    }
    (None, None)
}

/// The identifier immediately following `prefix` in `message`.
fn word_after(message: &str, prefix: &str) -> Option<String> {
    let start = message.find(prefix)? + prefix.len();
    let word: String = message[start..]
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    (!word.is_empty()).then_some(word)
}

/// Rebuild `note_fts` from `note` on `conn`, inside a write transaction.
/// Re-syncs `note_search_ctx` first (adds rows for any note missing one and
/// refreshes its filter columns), keeping existing `search_id`s stable.
/// The tag expression mirrors the 0141 triggers; keep them in sync.
async fn rebuild_note_search(conn: &mut sqlx::SqliteConnection) -> Result<()> {
    let step = |what: &'static str| move |e: sqlx::Error| Error::Internal(format!("{what}: {e}"));
    sqlx::query(
        "INSERT INTO note_search_ctx(note_id, workspace_id, is_archived, updated_at) \
         SELECT n.id, n.workspace_id, n.is_archived, n.updated_at FROM note n \
         WHERE NOT EXISTS (SELECT 1 FROM note_search_ctx c \
                           WHERE c.note_id = n.id AND c.workspace_id = n.workspace_id)",
    )
    .execute(&mut *conn)
    .await
    .map_err(step("note search context backfill failed"))?;
    sqlx::query(
        "UPDATE note_search_ctx SET is_archived = n.is_archived, updated_at = n.updated_at \
         FROM note n WHERE n.id = note_search_ctx.note_id \
           AND n.workspace_id = note_search_ctx.workspace_id \
           AND (note_search_ctx.is_archived IS NOT n.is_archived \
                OR note_search_ctx.updated_at IS NOT n.updated_at)",
    )
    .execute(&mut *conn)
    .await
    .map_err(step("note search context refresh failed"))?;
    sqlx::query("INSERT INTO note_fts(note_fts) VALUES('delete-all')")
        .execute(&mut *conn)
        .await
        .map_err(step("note search delete-all failed"))?;
    sqlx::query(
        "INSERT INTO note_fts(rowid, title, content, tags) \
         SELECT c.search_id, n.title, n.content, \
                COALESCE((SELECT group_concat(value, ' ') FROM json_each(n.tags) \
                          WHERE type = 'text'), '') \
         FROM note n JOIN note_search_ctx c \
           ON c.note_id = n.id AND c.workspace_id = n.workspace_id",
    )
    .execute(&mut *conn)
    .await
    .map_err(step("note search backfill failed"))?;
    Ok(())
}

/// What [`repair_derived_indexes`] did.
#[derive(Debug, Clone)]
pub struct RepairReport {
    /// Directory holding the pre-repair copy of the database files.
    pub backup_dir: PathBuf,
    /// Integrity before any change.
    pub before: IntegrityReport,
    /// Indexes rebuilt, in order.
    pub rebuilt: Vec<DerivedIndex>,
    /// Integrity after the rebuild.
    pub after: IntegrityReport,
}

/// Offline repair: take exclusive daemon ownership of `db_path` (fails while
/// intentd runs), copy the database and its WAL into a new timestamped
/// directory under `backup_root`, then rebuild every derived index that
/// `PRAGMA integrity_check` reports as damaged (or all of them with `force`)
/// and check again. Canonical tables are only read. Refuses a database whose
/// migration ledger does not exactly match this build, so the rebuild SQL
/// always matches the schema.
///
/// # Errors
///
/// Returns `Error::Internal` when the database is missing, another process
/// owns it, the schema version differs from this build, the backup cannot be
/// written, or a rebuild statement fails (its transaction rolls back).
pub async fn repair_derived_indexes(
    db_path: &Path,
    backup_root: &Path,
    force: bool,
) -> Result<RepairReport> {
    if !std::fs::metadata(db_path).is_ok_and(|m| m.len() > 0) {
        return Err(Error::Internal(format!(
            "no database at {}",
            db_path.display()
        )));
    }
    let prepared = crate::Store::prepare_for_daemon(db_path).map_err(|e| {
        Error::Internal(format!(
            "{e}; stop intentd (`intentd stop`) and quit the Intent app before repairing"
        ))
    })?;
    {
        let diagnostic = crate::DiagnosticStore::open(&prepared.physical).await?;
        let status = diagnostic.migration_status().await;
        diagnostic.close().await;
        let status = status?;
        if !status.is_current() {
            return Err(Error::Internal(format!(
                "database schema does not match this intentd build (applied {:?}, expected {:?}); \
                 repair with the intentd version that last ran on this database",
                status.applied, status.expected
            )));
        }
    }
    let backup_dir = backup_database_files(&prepared.physical, backup_root)?;

    let pool = crate::connect_write_owned(
        &prepared.physical,
        Duration::from_secs(30),
        Some(prepared.owner.clone()),
    )
    .await?;
    let result = async {
        let before = check_integrity(&pool).await?;
        let rebuilt: Vec<DerivedIndex> = if force {
            DerivedIndex::ALL.to_vec()
        } else {
            before.damaged_indexes()
        };
        for index in &rebuilt {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("repair begin failed: {e}")))?;
            index.rebuild(&mut tx).await?;
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("repair of {} failed to commit: {e}", index.table()))
            })?;
        }
        let after = check_integrity(&pool).await?;
        let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&pool)
            .await;
        Ok::<_, Error>((before, rebuilt, after))
    }
    .await;
    pool.close().await;
    let (before, rebuilt, after) = result?;
    Ok(RepairReport {
        backup_dir,
        before,
        rebuilt,
        after,
    })
}

/// Copy the database file and, when present, its `-wal` into a new
/// directory `<backup_root>/<file name>-<UTC timestamp>`. No connection is
/// open while this runs and the caller holds daemon ownership, so the pair is
/// a consistent snapshot; the `-shm` index is rebuilt by `SQLite` on open.
fn backup_database_files(db: &Path, backup_root: &Path) -> Result<PathBuf> {
    let io = |what: &str, path: &Path, e: std::io::Error| {
        Error::Internal(format!("backup {what} {}: {e}", path.display()))
    };
    let name = db.file_name().map_or_else(
        || "intentd.db".to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let stamp = utc_stamp();
    std::fs::create_dir_all(backup_root).map_err(|e| io("create", backup_root, e))?;
    // A second repair within the same second gets a numbered sibling
    // rather than reusing (or failing on) an existing backup.
    let mut attempt = 1;
    let dir = loop {
        let candidate = if attempt == 1 {
            backup_root.join(format!("{name}-{stamp}"))
        } else {
            backup_root.join(format!("{name}-{stamp}-{attempt}"))
        };
        match std::fs::create_dir(&candidate) {
            Ok(()) => break candidate,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && attempt < 100 => {
                attempt += 1;
            }
            Err(e) => return Err(io("create", &candidate, e)),
        }
    };
    let mut sources = vec![db.to_path_buf()];
    let mut wal = db.as_os_str().to_os_string();
    wal.push("-wal");
    let wal = PathBuf::from(wal);
    if wal.exists() {
        sources.push(wal);
    }
    for source in sources {
        let file_name = source.file_name().unwrap_or_default();
        let target = dir.join(file_name);
        std::fs::copy(&source, &target).map_err(|e| io("copy", &source, e))?;
        std::fs::File::open(&target)
            .and_then(|f| f.sync_all())
            .map_err(|e| io("sync", &target, e))?;
    }
    #[cfg(unix)]
    std::fs::File::open(&dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| io("sync", &dir, e))?;
    Ok(dir)
}

/// Compact UTC stamp (`20261105T132655Z`) for backup directory names.
fn utc_stamp() -> String {
    let iso = intent_core::clock::now_iso();
    let seconds = iso.split(['.', 'Z', '+']).next().unwrap_or_default();
    let compact: String = seconds.chars().filter(|c| *c != '-' && *c != ':').collect();
    format!("{compact}Z")
}
