//! Durable local selection facts, separate from permission and live Git identity.
//!
//! Snapshots own the original database allocation and one closed SQL snapshot.
//! No current remote, account, monitor override or legacy hint proves a target.

use std::sync::Arc;

use intent_core::{
    HistoricalTargetSource, RepositoryRootId, RepositoryRootKind, SavedReviewSelection, WorkspaceId,
};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection};

use crate::repository_lifecycle::{LifecycleDomain, LifecycleWrite};
use crate::{Error, Result, Store};

/// Positive durable root counter, comparable only within its original domain/key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepositoryRootIncarnation(u64);
impl RepositoryRootIncarnation {
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Positive durable choice counter, independent of root and authority counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepositorySelectionRevision(u64);
impl RepositorySelectionRevision {
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Original stored root facts. Canonical filesystem/Git checks remain separate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositorySelectionBinding {
    Primary {
        repository_path: Option<String>,
        worktree_path: Option<String>,
        is_remote: bool,
    },
    Registered {
        path: String,
    },
}

/// Absence, an explicit reset, and retained intent are distinct durable states.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryStoredSelection {
    NeverSaved,
    Reset,
    Saved(SavedReviewSelection),
}

/// An owned observation, not serializable, constructible by IDs, or a grant.
/// Fields are private so a caller cannot rewrite a captured comparison stamp.
pub struct RepositorySelectionSnapshot {
    domain: Arc<LifecycleDomain>,
    root: RepositoryRootId,
    binding: Option<RepositorySelectionBinding>,
    state: Option<SelectionRow>,
    selection: Option<RepositoryStoredSelection>,
}

impl RepositorySelectionSnapshot {
    #[must_use]
    pub fn root(&self) -> &RepositoryRootId {
        &self.root
    }
    #[must_use]
    pub fn binding(&self) -> Option<&RepositorySelectionBinding> {
        self.binding.as_ref()
    }
    #[must_use]
    pub fn root_incarnation(&self) -> Option<RepositoryRootIncarnation> {
        self.state
            .as_ref()
            .map(|r| RepositoryRootIncarnation(r.incarnation))
    }
    #[must_use]
    pub fn selection_revision(&self) -> Option<RepositorySelectionRevision> {
        self.state
            .as_ref()
            .map(|r| RepositorySelectionRevision(r.revision))
    }
    #[must_use]
    pub fn selection(&self) -> Option<&RepositoryStoredSelection> {
        self.selection.as_ref()
    }

    fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.domain, &other.domain)
            && self.root == other.root
            && self.binding == other.binding
            && self.state == other.state
    }
}

/// Only explicit local choices are writable. There is no canonical-proof setter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositorySelectionChange {
    Automatic,
    ExplicitRemote { remote_name: String },
    Reset,
}

/// Fresh observations accompanying the outcome of the original comparison.
pub enum RepositorySelectionWriteResult {
    Applied(RepositorySelectionSnapshot),
    Unchanged(RepositorySelectionSnapshot),
    Conflict(RepositorySelectionSnapshot),
    MissingRoot(RepositorySelectionSnapshot),
}

/// Facts about this SQL attempt, independent of successful result projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepositorySelectionPersistence {
    NotAttempted,
    NoEffect,
    Committed {
        revision: RepositorySelectionRevision,
    },
    Unknown,
}

pub struct RepositorySelectionWriteOutcome {
    pub result: Result<RepositorySelectionWriteResult>,
    pub persistence: RepositorySelectionPersistence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SelectionRow {
    incarnation: u64,
    revision: u64,
    present: bool,
    binding: RepositorySelectionBinding,
    choice_incarnation: u64,
    mode: String,
    remote: Option<String>,
    source: Option<HistoricalTargetSource>,
    record_id: Option<String>,
}

fn error() -> Error {
    Error::Internal("invalid or unavailable repository selection continuity".into())
}
fn sql_error(_: sqlx::Error) -> Error {
    error()
}
fn positive(row: &SqliteRow, field: &str) -> Result<u64> {
    let n: i64 = row.try_get(field).map_err(sql_error)?;
    u64::try_from(n).ok().filter(|n| *n > 0).ok_or_else(error)
}
fn text(row: &SqliteRow, field: &str) -> Result<Option<String>> {
    row.try_get(field).map_err(sql_error)
}
fn bit(row: &SqliteRow, field: &str) -> Result<bool> {
    match row.try_get::<i64, _>(field).map_err(sql_error)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(error()),
    }
}
fn key(root: &RepositoryRootId) -> (&str, &str) {
    match &root.kind {
        RepositoryRootKind::Primary => ("primary", ""),
        RepositoryRootKind::Registered { git_root_id } => ("registered", git_root_id.as_str()),
    }
}
fn stored_binding(row: &SqliteRow, root: &RepositoryRootId) -> Result<RepositorySelectionBinding> {
    match &root.kind {
        RepositoryRootKind::Primary => {
            if text(row, "registered_path")?.is_some() {
                return Err(error());
            }
            Ok(RepositorySelectionBinding::Primary {
                repository_path: text(row, "repository_path")?,
                worktree_path: text(row, "worktree_path")?,
                is_remote: bit(row, "is_remote")?,
            })
        }
        RepositoryRootKind::Registered { .. } => {
            if text(row, "repository_path")?.is_some()
                || text(row, "worktree_path")?.is_some()
                || row
                    .try_get::<Option<i64>, _>("is_remote")
                    .map_err(sql_error)?
                    .is_some()
            {
                return Err(error());
            }
            Ok(RepositorySelectionBinding::Registered {
                path: text(row, "registered_path")?.ok_or_else(error)?,
            })
        }
    }
}
fn decode(row: &SqliteRow, root: &RepositoryRootId) -> Result<SelectionRow> {
    let source = match text(row, "historical_source")?.as_deref() {
        None => None,
        Some("workspace-metadata") => Some(HistoricalTargetSource::WorkspaceMetadata),
        Some("registered-root-metadata") => Some(HistoricalTargetSource::RegisteredRootMetadata),
        Some(_) => return Err(error()),
    };
    let state = SelectionRow {
        incarnation: positive(row, "root_incarnation")?,
        revision: positive(row, "selection_revision")?,
        present: bit(row, "root_present")?,
        binding: stored_binding(row, root)?,
        choice_incarnation: positive(row, "choice_incarnation")?,
        mode: row.try_get("choice_mode").map_err(sql_error)?,
        remote: text(row, "remote_name")?,
        source,
        record_id: text(row, "historical_record_id")?,
    };
    if state.choice_incarnation > state.incarnation
        || state.source.is_some() != state.record_id.is_some()
        || state.record_id.as_ref().is_some_and(String::is_empty)
    {
        return Err(error());
    }
    match state.mode.as_str() {
        "never" | "reset" | "automatic" if state.remote.is_none() && state.source.is_none() => {}
        "remote" if state.source.is_none() && state.remote.as_deref().is_some_and(valid_remote) => {
        }
        "unresolved" if state.remote.is_none() => {}
        _ => return Err(error()),
    }
    Ok(state)
}
fn valid_remote(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 1024
        && name.trim() == name
        && !name.chars().any(char::is_control)
}
fn project(row: &SelectionRow, root: &RepositoryRootId) -> Result<RepositoryStoredSelection> {
    if row.choice_incarnation != row.incarnation && row.mode != "never" {
        let (source, record_id) = match &root.kind {
            RepositoryRootKind::Primary => (
                HistoricalTargetSource::WorkspaceMetadata,
                root.workspace_id.0.clone(),
            ),
            RepositoryRootKind::Registered { git_root_id } => (
                HistoricalTargetSource::RegisteredRootMetadata,
                git_root_id.0.clone(),
            ),
        };
        return Ok(RepositoryStoredSelection::Saved(
            SavedReviewSelection::UnresolvedHistorical {
                source: Some(source),
                record_id: Some(record_id),
            },
        ));
    }
    Ok(match row.mode.as_str() {
        "never" => RepositoryStoredSelection::NeverSaved,
        "reset" => RepositoryStoredSelection::Reset,
        "automatic" => RepositoryStoredSelection::Saved(SavedReviewSelection::Automatic),
        "remote" => RepositoryStoredSelection::Saved(SavedReviewSelection::ExplicitRemote {
            remote_name: row.remote.clone().ok_or_else(error)?,
        }),
        "unresolved" => {
            RepositoryStoredSelection::Saved(SavedReviewSelection::UnresolvedHistorical {
                source: row.source,
                record_id: row.record_id.clone(),
            })
        }
        _ => return Err(error()),
    })
}

async fn read_at(
    store: &Store,
    root: &RepositoryRootId,
    conn: &mut SqliteConnection,
) -> Result<RepositorySelectionSnapshot> {
    let workspace =
        sqlx::query("SELECT repository_path,worktree_path,is_remote FROM workspace WHERE id=?")
            .bind(root.workspace_id.as_str())
            .fetch_optional(&mut *conn)
            .await
            .map_err(sql_error)?;
    let binding = match (&root.kind, workspace) {
        (_, None) => None,
        (RepositoryRootKind::Primary, Some(row)) => Some(RepositorySelectionBinding::Primary {
            repository_path: text(&row, "repository_path")?,
            worktree_path: text(&row, "worktree_path")?,
            is_remote: bit(&row, "is_remote")?,
        }),
        (RepositoryRootKind::Registered { git_root_id }, Some(_)) => {
            sqlx::query("SELECT path FROM workspace_git_root WHERE id=? AND workspace_id=?")
                .bind(git_root_id.as_str())
                .bind(root.workspace_id.as_str())
                .fetch_optional(&mut *conn)
                .await
                .map_err(sql_error)?
                .map(|r| {
                    Ok(RepositorySelectionBinding::Registered {
                        path: r.try_get("path").map_err(sql_error)?,
                    })
                })
                .transpose()?
        }
    };
    let (kind, id) = key(root);
    let state = sqlx::query("SELECT * FROM repository_selection_state WHERE workspace_id=? AND root_kind=? AND root_id=?")
        .bind(root.workspace_id.as_str()).bind(kind).bind(id).fetch_optional(&mut *conn).await.map_err(sql_error)?
        .map(|r| decode(&r,root)).transpose()?;
    if binding.is_some() != state.as_ref().is_some_and(|r| r.present)
        || state
            .as_ref()
            .is_some_and(|r| r.present && binding.as_ref() != Some(&r.binding))
    {
        return Err(error());
    }
    let selection = state.as_ref().map(|r| project(r, root)).transpose()?;
    Ok(RepositorySelectionSnapshot {
        domain: store.repository_lifecycle.clone(),
        root: root.clone(),
        binding,
        state,
        selection,
    })
}

fn desired(change: &RepositorySelectionChange) -> (&str, Option<&str>) {
    match change {
        RepositorySelectionChange::Automatic => ("automatic", None),
        RepositorySelectionChange::Reset => ("reset", None),
        RepositorySelectionChange::ExplicitRemote { remote_name } => ("remote", Some(remote_name)),
    }
}
enum SelectionComparison {
    Write(RepositorySelectionSnapshot),
    Finished(RepositorySelectionWriteResult),
}

fn classify(
    current: RepositorySelectionSnapshot,
    original: &RepositorySelectionSnapshot,
    change: &RepositorySelectionChange,
) -> SelectionComparison {
    if current.binding.is_none() {
        return SelectionComparison::Finished(RepositorySelectionWriteResult::MissingRoot(current));
    }
    if !current.matches(original) {
        return SelectionComparison::Finished(RepositorySelectionWriteResult::Conflict(current));
    }
    let (mode, remote) = desired(change);
    if current.state.as_ref().is_some_and(|r| {
        r.choice_incarnation == r.incarnation && r.mode == mode && r.remote.as_deref() == remote
    }) {
        return SelectionComparison::Finished(RepositorySelectionWriteResult::Unchanged(current));
    }
    SelectionComparison::Write(current)
}
fn settled(
    lifecycle: LifecycleWrite,
    result: RepositorySelectionWriteResult,
    persistence: RepositorySelectionPersistence,
) -> RepositorySelectionWriteOutcome {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| lifecycle.settle()))
        .map(|()| result)
        .map_err(|_| Error::Internal("repository selection settlement failed".into()));
    RepositorySelectionWriteOutcome {
        result,
        persistence,
    }
}

impl Store {
    /// Read root membership, binding, continuity and saved intent in one closed
    /// transaction. The result neither grants access nor performs filesystem I/O.
    ///
    /// # Errors
    /// Refuses invalid domains and present roots with missing/malformed provenance.
    pub async fn repository_selection_snapshot(
        &self,
        root: &RepositoryRootId,
    ) -> Result<RepositorySelectionSnapshot> {
        let _serial = self.repository_lifecycle_write().await?;
        let mut tx = self.read_pool().begin().await.map_err(sql_error)?;
        let snapshot = read_at(self, root, &mut tx).await?;
        tx.commit().await.map_err(sql_error)?;
        Ok(snapshot)
    }

    /// Compare against the original domain/root/choice facts. Unknown attempts
    /// are never retried; an old snapshot cannot borrow a replacement incarnation.
    pub async fn write_repository_selection(
        &self,
        original: &RepositorySelectionSnapshot,
        change: RepositorySelectionChange,
    ) -> RepositorySelectionWriteOutcome {
        let mut persistence = RepositorySelectionPersistence::NotAttempted;
        match self
            .write_selection_inner(original, &change, &mut persistence)
            .await
        {
            Ok(outcome) => outcome,
            Err(e) => RepositorySelectionWriteOutcome {
                result: Err(e),
                persistence,
            },
        }
    }

    /// Persist an explicit reset through the same comparison/transaction path.
    pub async fn reset_repository_selection(
        &self,
        original: &RepositorySelectionSnapshot,
    ) -> RepositorySelectionWriteOutcome {
        self.write_repository_selection(original, RepositorySelectionChange::Reset)
            .await
    }

    async fn write_selection_inner(
        &self,
        original: &RepositorySelectionSnapshot,
        change: &RepositorySelectionChange,
        persistence: &mut RepositorySelectionPersistence,
    ) -> Result<RepositorySelectionWriteOutcome> {
        if !Arc::ptr_eq(&self.repository_lifecycle, &original.domain) {
            return Err(Error::InvalidParams(
                "repository selection snapshot belongs to another database".into(),
            ));
        }
        if let RepositorySelectionChange::ExplicitRemote { remote_name } = change {
            if !valid_remote(remote_name) {
                return Err(Error::InvalidParams(
                    "invalid repository remote name".into(),
                ));
            }
        }
        let mut lifecycle = self.repository_lifecycle_write().await?;
        let mut read = self.read_pool().begin().await.map_err(sql_error)?;
        let current = read_at(self, &original.root, &mut read).await?;
        read.commit().await.map_err(sql_error)?;
        if let SelectionComparison::Finished(result) = classify(current, original, change) {
            return Ok(settled(
                lifecycle,
                result,
                RepositorySelectionPersistence::NoEffect,
            ));
        }
        lifecycle.begin_selection_change(&original.root)?;
        *persistence = RepositorySelectionPersistence::Unknown;
        lifecycle.resume_serialization().await?;
        let mut tx = self.write_pool().begin().await.map_err(sql_error)?;
        let current = read_at(self, &original.root, &mut tx).await?;
        let current = match classify(current, original, change) {
            SelectionComparison::Write(current) => current,
            SelectionComparison::Finished(result) => {
                tx.rollback().await.map_err(sql_error)?;
                return Ok(settled(
                    lifecycle,
                    result,
                    RepositorySelectionPersistence::NoEffect,
                ));
            }
        };
        let state = current.state.as_ref().ok_or_else(error)?;
        let next = state
            .revision
            .checked_add(1)
            .and_then(|n| i64::try_from(n).ok())
            .ok_or_else(error)?;
        let (mode, remote) = desired(change);
        let (kind, id) = key(&original.root);
        let changed=sqlx::query("UPDATE repository_selection_state SET selection_revision=?,choice_incarnation=root_incarnation,choice_mode=?,remote_name=?,historical_source=NULL,historical_record_id=NULL WHERE workspace_id=? AND root_kind=? AND root_id=? AND root_incarnation=? AND selection_revision=?")
            .bind(next).bind(mode).bind(remote).bind(original.root.workspace_id.as_str()).bind(kind).bind(id)
            .bind(i64::try_from(state.incarnation).map_err(|_|error())?).bind(i64::try_from(state.revision).map_err(|_|error())?)
            .execute(&mut *tx).await.map_err(sql_error)?;
        if changed.rows_affected() != 1 {
            return Err(error());
        }
        let applied = read_at(self, &original.root, &mut tx).await?;
        tx.commit().await.map_err(sql_error)?;
        let committed = RepositorySelectionPersistence::Committed {
            revision: RepositorySelectionRevision(u64::try_from(next).map_err(|_| error())?),
        };
        *persistence = committed;
        Ok(settled(
            lifecycle,
            RepositorySelectionWriteResult::Applied(applied),
            committed,
        ))
    }
}

/// Only the existing transfer transaction calls this; it already owns Database.
/// Capturing before the original INSERT distinguishes an unchanged local choice
/// from a new/rebound imported primary. There is no independent writer or retry.
pub(crate) async fn imported_primary_before(
    conn: &mut SqliteConnection,
    id: &WorkspaceId,
) -> Result<Option<(i64, i64)>> {
    sqlx::query_as("SELECT root_incarnation,selection_revision FROM repository_selection_state WHERE workspace_id=? AND root_kind='primary' AND root_id='' AND root_present=1")
        .bind(id.as_str()).fetch_optional(conn).await.map_err(sql_error)
}
pub(crate) async fn classify_imported_primary(
    conn: &mut SqliteConnection,
    id: &WorkspaceId,
    before: Option<(i64, i64)>,
) -> Result<()> {
    let after = imported_primary_before(conn, id).await?.ok_or_else(error)?;
    if before.is_some_and(|b| b.0 == after.0) {
        return Ok(());
    }
    sqlx::query("UPDATE repository_selection_state SET selection_revision=selection_revision+1,choice_incarnation=root_incarnation,choice_mode='unresolved',remote_name=NULL,historical_source=NULL,historical_record_id=NULL WHERE workspace_id=? AND root_kind='primary' AND root_id='' AND root_present=1")
        .bind(id.as_str()).execute(conn).await.map_err(sql_error)?;
    Ok(())
}

#[cfg(test)]
mod tests;
