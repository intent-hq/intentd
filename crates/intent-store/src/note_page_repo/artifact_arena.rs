//! Separate ownership for artifact journal transactions. Database page limits
//! are not a proof of total physical/journal/temp or constructor reservations.
use super::{db_error, invalid, ArtifactSourceGrant};
use crate::Store;
use intent_core::{Error, Result};
use sqlx::{
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode, SqlitePoolOptions,
        SqliteSynchronous,
    },
    Sqlite, SqlitePool, Transaction,
};
use std::path::{Path, PathBuf};

mod ownership;
use ownership::SourceOwners;
#[cfg(all(test, unix))]
mod physical_tests;
#[cfg(all(test, target_os = "linux"))]
mod statement_journal_tests;

pub(crate) struct ArtifactArena {
    pub pool: SqlitePool,
    pub(super) sources: std::sync::Arc<SourceOwners>,
    path: PathBuf,
    max_pages: u32,
}

impl Store {
    /// Keep source mutation and runtime eviction barred until the `SQLite` worker
    /// has acknowledged COMMIT, even when the caller abandons its response.
    /// `SQLx` can finish a queued COMMIT after dropping its receiver, so ordinary
    /// independent transaction drops do not preserve this cross-file ordering.
    pub(super) async fn commit_artifact_with_source_guard(
        source_guard: Transaction<'static, Sqlite>,
        transaction: Transaction<'static, Sqlite>,
        source: ArtifactSourceGrant,
    ) -> Result<()> {
        // The already-held sole main writer bounds this settlement task to one.
        // No abort handle escapes; Store::close waits for its pooled connections.
        tokio::spawn(async move {
            let committed = transaction.commit().await.map_err(db_error);
            let released = source_guard.rollback().await.map_err(db_error);
            drop(source);
            committed?;
            released
        })
        .await
        .map_err(|error| Error::Internal(format!("artifact commit settlement: {error}")))?
    }

    /// Install one trusted application-owned artifact arena, shared by Store
    /// clones. This prepared seam requires an explicit finite database page cap;
    /// it does not authorize artifacts or prove a physical allocation reservation.
    /// All artifact journal tables/transactions live here, not in the main DB.
    /// The caller must separately reserve journal/temp/cache/constructor headroom
    /// before exposing this through a production service. No default is enabled.
    ///
    /// # Errors
    /// Rejects main-database aliasing, changed configuration, invalid page caps,
    /// schema mismatch, an arena already exceeding the cap, and database failures.
    pub async fn configure_note_artifact_arena(&self, path: &Path, max_pages: u32) -> Result<()> {
        if max_pages == 0 || max_pages > 0x7fff_fffe || !path.is_absolute() {
            return Err(invalid());
        }
        let main: String =
            sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name='main'")
                .fetch_one(self.read_pool())
                .await
                .map_err(db_error)?;
        // Resolve the existing parent even when the arena file is not created.
        let normalized = match std::fs::canonicalize(path) {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::canonicalize(path.parent().ok_or_else(invalid)?)
                    .map_err(|e| Error::Internal(format!("artifact arena parent: {e}")))?
                    .join(path.file_name().ok_or_else(invalid)?)
            }
            Err(error) => return Err(Error::Internal(format!("artifact arena path: {error}"))),
        };
        let main = std::fs::canonicalize(main)
            .map_err(|e| Error::Internal(format!("main database path: {e}")))?;
        if normalized == main {
            return Err(invalid());
        }
        #[cfg(unix)]
        if let Ok(arena_metadata) = std::fs::metadata(&normalized) {
            use std::os::unix::fs::MetadataExt;
            let main_metadata = std::fs::metadata(&main)
                .map_err(|e| Error::Internal(format!("main database metadata: {e}")))?;
            if arena_metadata.dev() == main_metadata.dev()
                && arena_metadata.ino() == main_metadata.ino()
            {
                return Err(invalid());
            }
        }
        let arena = self
            .artifact_arena
            .get_or_try_init(|| async {
                let options = SqliteConnectOptions::new()
                    .filename(&normalized)
                    .create_if_missing(true)
                    .foreign_keys(true)
                    .locking_mode(SqliteLockingMode::Exclusive)
                    .journal_mode(SqliteJournalMode::Delete)
                    .synchronous(SqliteSynchronous::Full)
                    .pragma("page_size", "4096")
                    .pragma("auto_vacuum", "NONE")
                    .pragma("max_page_count", max_pages.to_string())
                    .pragma("mmap_size", "0");
                // One connection serializes both reads and writes. No parked arena
                // readers can extend rollback-journal lifetimes behind the owner.
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .idle_timeout(None)
                    .max_lifetime(None)
                    .connect_with(options)
                    .await
                    .map_err(db_error)?;
                let initialize = async {
                    // Retain the physical file lock for this connection's
                    // lifetime. A second Store/process cannot install another
                    // reader/writer owner behind the single-connection budget.
                    let mut tx = pool.begin_with("BEGIN EXCLUSIVE").await.map_err(db_error)?;
                    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
                        .fetch_one(&mut *tx).await.map_err(db_error)?;
                    if page_size != 4096 { return Err(invalid()); }
                    // Existing files cannot change this geometry by pragma
                    // alone. Reject relocation modes rather than silently run
                    // extra page-move/journaling work or VACUUM user storage.
                    let auto_vacuum: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
                        .fetch_one(&mut *tx).await.map_err(db_error)?;
                    if auto_vacuum != 0 { return Err(invalid()); }
                    let actual: i64 = sqlx::query_scalar("PRAGMA max_page_count")
                        .fetch_one(&mut *tx)
                        .await
                        .map_err(db_error)?;
                    if actual != i64::from(max_pages) {
                        return Err(invalid());
                    }
                    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
                        .fetch_one(&mut *tx)
                        .await
                        .map_err(db_error)?;
                    if version == 0 {
                        let tables: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                            .fetch_one(&mut *tx).await.map_err(db_error)?;
                        if tables != 0 { return Err(invalid()); }
                        sqlx::raw_sql(include_str!("artifact_arena.sql"))
                            .execute(&mut *tx)
                            .await
                            .map_err(db_error)?;
                        sqlx::query("INSERT INTO note_artifact_arena_owner(singleton,backend_id,max_pages) VALUES (1,?,?)")
                            .bind(&self.note_pages.backend).bind(max_pages).execute(&mut *tx).await.map_err(db_error)?;
                        sqlx::query("PRAGMA user_version=1")
                            .execute(&mut *tx)
                            .await
                            .map_err(db_error)?;
                    } else if version != 1 {
                        return Err(Error::Internal("Unsupported artifact arena schema".into()));
                    }
                    let owner: (String, i64) = sqlx::query_as("SELECT backend_id,max_pages FROM note_artifact_arena_owner WHERE singleton=1")
                        .fetch_one(&mut *tx).await.map_err(db_error)?;
                    if owner != (self.note_pages.backend.clone(), i64::from(max_pages)) { return Err(invalid()); }
                    tx.commit().await.map_err(db_error)?;
                    Ok(())
                }
                .await;
                if let Err(error) = initialize {
                    pool.close().await;
                    return Err(error);
                }
                Ok(ArtifactArena {
                    pool,
                    sources: std::sync::Arc::default(),
                    path: normalized.clone(),
                    max_pages,
                })
            })
            .await?;
        if arena.path != normalized || arena.max_pages != max_pages {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn artifact_pool(&self) -> Result<&SqlitePool> {
        self.artifact_arena
            .get()
            .map(|arena| &arena.pool)
            .ok_or_else(|| Error::InvalidParams("Artifact arena is not configured".into()))
    }

    pub(super) fn artifact_sources(&self) -> Result<std::sync::Arc<SourceOwners>> {
        self.artifact_arena
            .get()
            .map(|arena| arena.sources.clone())
            .ok_or_else(invalid)
    }

    /// A cancelled response must not strand a committed retirement's source pin.
    /// The sole arena connection settles prior readers before this commit; each
    /// operation also retains its own grant until its physical I/O settles.
    pub(super) async fn commit_artifact_retirement(
        &self,
        tx: Transaction<'static, Sqlite>,
        generations: Vec<String>,
    ) -> Result<()> {
        let sources = self.artifact_sources()?;
        tokio::spawn(async move {
            tx.commit().await.map_err(db_error)?;
            sources.retire(&generations);
            Ok(())
        })
        .await
        .map_err(|error| Error::Internal(format!("artifact retirement settlement: {error}")))?
    }
}

impl ArtifactArena {
    pub(crate) async fn close(&self) {
        self.pool.close().await;
        self.sources.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn artifact_arena_rejects_second_live_connection_owner() {
        let directory = tempfile::tempdir().unwrap();
        let main_path = directory.path().join("main.sqlite");
        let arena_path = directory.path().join("arena.sqlite");
        let first = Store::open(&main_path).await.unwrap();
        first
            .configure_note_artifact_arena(&arena_path, 64)
            .await
            .unwrap();
        let second = Store::open(&main_path).await.unwrap();
        let duplicate = second.configure_note_artifact_arena(&arena_path, 64).await;
        first.close().await;
        assert!(
            duplicate.is_err(),
            "two independent arena pools admitted against one physical file"
        );
        // A failed acquisition must not poison OnceCell or retain a file lock.
        second
            .configure_note_artifact_arena(&arena_path, 64)
            .await
            .unwrap();
        second.close().await;
    }

    #[tokio::test]
    async fn artifact_arena_rejects_existing_auto_vacuum_relocation() {
        for mode in ["FULL", "INCREMENTAL"] {
            let directory = tempfile::tempdir().unwrap();
            let main_path = directory.path().join("main.sqlite");
            let arena_path = directory.path().join("arena.sqlite");
            let first = Store::open(&main_path).await.unwrap();
            first
                .configure_note_artifact_arena(&arena_path, 64)
                .await
                .unwrap();
            first.close().await;
            let outside = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(SqliteConnectOptions::new().filename(&arena_path))
                .await
                .unwrap();
            sqlx::query(&format!("PRAGMA auto_vacuum={mode}"))
                .execute(&outside)
                .await
                .unwrap();
            sqlx::query("VACUUM").execute(&outside).await.unwrap();
            let actual: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
                .fetch_one(&outside)
                .await
                .unwrap();
            assert_ne!(actual, 0, "fixture must actually enable relocation");
            outside.close().await;
            let reopened = Store::open(&main_path).await.unwrap();
            let result = reopened
                .configure_note_artifact_arena(&arena_path, 64)
                .await;
            reopened.close().await;
            assert!(
                result.is_err(),
                "arena accepted existing {mode} auto-vacuum configuration"
            );
        }
    }

    #[tokio::test]
    async fn artifact_arena_rejects_changed_database_page_geometry() {
        let directory = tempfile::tempdir().unwrap();
        let main_path = directory.path().join("main.sqlite");
        let arena_path = directory.path().join("arena.sqlite");
        let store = Store::open(&main_path).await.unwrap();
        store
            .configure_note_artifact_arena(&arena_path, 64)
            .await
            .unwrap();
        store.close().await;
        let outside = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&arena_path)
                    .pragma("page_size", "8192"),
            )
            .await
            .unwrap();
        sqlx::query("VACUUM").execute(&outside).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA page_size")
                .fetch_one(&outside)
                .await
                .unwrap(),
            8192
        );
        outside.close().await;
        let reopened = Store::open(&main_path).await.unwrap();
        let changed = reopened
            .configure_note_artifact_arena(&arena_path, 64)
            .await;
        reopened.close().await;
        assert!(
            changed.is_err(),
            "page-count allowance silently accepted twice the database bytes"
        );
    }

    #[tokio::test]
    async fn artifact_arena_owns_journals_without_capping_main() {
        let directory = tempfile::tempdir().unwrap();
        let main_path = directory.path().join("main.sqlite");
        let arena_path = directory.path().join("arena.sqlite");
        let store = Store::open(&main_path).await.unwrap();
        assert!(store.artifact_pool().is_err());
        assert!(store
            .configure_note_artifact_arena(&main_path, 64)
            .await
            .is_err());
        #[cfg(unix)]
        {
            let alias = directory.path().join("alias.sqlite");
            std::fs::hard_link(&main_path, &alias).unwrap();
            assert!(store
                .configure_note_artifact_arena(&alias, 64)
                .await
                .is_err());
        }
        store
            .configure_note_artifact_arena(&arena_path, 64)
            .await
            .unwrap();
        store
            .clone()
            .configure_note_artifact_arena(&arena_path, 64)
            .await
            .unwrap();
        assert!(store
            .configure_note_artifact_arena(&arena_path, 65)
            .await
            .is_err());
        for table in [
            "note_artifact_job",
            "note_artifact_record",
            "note_artifact_ack",
            "note_artifact_lease",
        ] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?"
                )
                .bind(table)
                .fetch_one(store.read_pool())
                .await
                .unwrap(),
                0
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?"
                )
                .bind(table)
                .fetch_one(store.artifact_pool().unwrap())
                .await
                .unwrap(),
                1
            );
        }
        assert_eq!(
            sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
                .fetch_one(store.artifact_pool().unwrap())
                .await
                .unwrap(),
            "delete"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA max_page_count")
                .fetch_one(store.artifact_pool().unwrap())
                .await
                .unwrap(),
            64
        );
        assert!(
            sqlx::query_scalar::<_, i64>("PRAGMA max_page_count")
                .fetch_one(store.read_pool())
                .await
                .unwrap()
                > 64
        );
        store.close().await;
        let other = Store::open(&directory.path().join("other.sqlite"))
            .await
            .unwrap();
        assert!(
            other
                .configure_note_artifact_arena(&arena_path, 64)
                .await
                .is_err(),
            "an arena must not be adopted by a different main-store backend"
        );
    }

    #[tokio::test]
    async fn artifact_arena_page_cap_and_reuse_preserve_high_water() {
        let directory = tempfile::tempdir().unwrap();
        let main_path = directory.path().join("main.sqlite");
        let arena_path = directory.path().join("arena.sqlite");
        let store = Store::open(&main_path).await.unwrap();
        store
            .configure_note_artifact_arena(&arena_path, 64)
            .await
            .unwrap();
        let pool = store.artifact_pool().unwrap();
        sqlx::query("CREATE TABLE arena_test_payload(id INTEGER PRIMARY KEY, body BLOB NOT NULL)")
            .execute(pool)
            .await
            .unwrap();
        let mut high_water = 0;
        let mut accepted = None;
        for cycle in 0..3 {
            let mut count = 0;
            for _ in 0..128 {
                let result =
                    sqlx::query("INSERT INTO arena_test_payload(body) VALUES (zeroblob(8192))")
                        .execute(pool)
                        .await;
                match result {
                    Ok(_) => count += 1,
                    Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("13") => {
                        break
                    }
                    other => panic!("unexpected arena write result: {other:?}"),
                }
            }
            assert!((1..128).contains(&count));
            assert_eq!(*accepted.get_or_insert(count), count);
            let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
                .fetch_one(pool)
                .await
                .unwrap();
            assert!(pages <= 64);
            let bytes = std::fs::metadata(&arena_path).unwrap().len();
            assert_eq!(bytes, u64::try_from(pages).unwrap() * 4096);
            if cycle > 0 {
                assert_eq!(bytes, high_water);
            }
            high_water = bytes;
            sqlx::query("DELETE FROM arena_test_payload")
                .execute(pool)
                .await
                .unwrap();
            assert_eq!(std::fs::metadata(&arena_path).unwrap().len(), high_water);
            // An exhausted artifact DB must not impose its quota on main writes.
            sqlx::query("CREATE TABLE IF NOT EXISTS unrelated_progress(value INTEGER)")
                .execute(store.write_pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO unrelated_progress VALUES (?)")
                .bind(cycle)
                .execute(store.write_pool())
                .await
                .unwrap();
        }
        store.close().await;
        let reopened = Store::open(&main_path).await.unwrap();
        reopened
            .configure_note_artifact_arena(&arena_path, 64)
            .await
            .unwrap();
        assert_eq!(std::fs::metadata(&arena_path).unwrap().len(), high_water);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM unrelated_progress")
                .fetch_one(reopened.read_pool())
                .await
                .unwrap(),
            3
        );
        // DB high-water/reuse only: this does not measure journal peak, allocated
        // filesystem blocks, temp memory, or prove a full physical reservation.
        reopened.close().await;
    }
}
