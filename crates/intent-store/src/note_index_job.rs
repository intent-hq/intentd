//! Demand-owned conversion before a page read snapshot. Incomplete rows stay
//! behind durable readiness fences; only the final transaction admits them.
use crate::{
    note_annotation_repo::AnchorOccurrence,
    note_page_index::{sql_offset, Prepared},
    note_write_connection::NoteWriteConnection,
    Store,
};
use intent_core::{note_page::NotePageError, Error, Result};
use sqlx::{Row, SqliteConnection};
#[cfg(not(unix))]
use std::path::PathBuf;
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, LazyLock, Mutex, Weak,
    },
    time::{Duration, Instant},
};
use tokio::sync::{watch, Semaphore};

const QUEUE_LIMIT: usize = 32;
const LEASE_MS: u64 = 30_000;
const BATCH_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Key {
    workspace: String,
    note: String,
    instance: String,
    revision: i64,
    generation: String,
    profile: String,
    comments: String,
}
struct Head {
    key: Key,
    id: i64,
    page_ready: bool,
    anchors_ready: bool,
}

#[expect(clippy::needless_pass_by_value)] // Result::map_err transfers ownership.
fn db(error: sqlx::Error) -> Error {
    Error::Internal(format!("note index preparation: {error}"))
}
fn failure(kind: NotePageError) -> Error {
    Error::NotePage(kind)
}

async fn head(conn: &mut SqliteConnection, workspace: &str, note: &str) -> Result<Head> {
    let row=sqlx::query("SELECT n.rev,h.instance_id,h.current_rev,h.indexed_rev,h.generation,h.profile_revision,a.id,a.source_rev,a.anchors_rev,a.comment_revision FROM note n JOIN note_page_head h ON h.workspace_id=n.workspace_id AND h.note_id=n.id JOIN note_annotation_head a ON a.workspace_id=n.workspace_id AND a.note_id=n.id WHERE n.workspace_id=? AND n.id=? AND NOT EXISTS(SELECT 1 FROM note_annotation_workspace_retirement WHERE workspace_id=n.workspace_id)")
        .bind(workspace).bind(note).fetch_optional(conn).await.map_err(db)?
        .ok_or_else(|| Error::NotFound("note index".into()))?;
    let revision: i64 = row.try_get("rev").map_err(db)?;
    if row.try_get::<i64, _>("current_rev").map_err(db)? != revision
        || row.try_get::<i64, _>("source_rev").map_err(db)? != revision
    {
        return Err(failure(NotePageError::Stale));
    }
    let profile: String = row.try_get("profile_revision").map_err(db)?;
    let page_ready = row.try_get::<i64, _>("indexed_rev").map_err(db)? == revision
        && profile == crate::note_page_index::profile_revision();
    Ok(Head {
        key: Key {
            workspace: workspace.into(),
            note: note.into(),
            instance: row.try_get("instance_id").map_err(db)?,
            revision,
            generation: row.try_get("generation").map_err(db)?,
            profile,
            comments: row.try_get("comment_revision").map_err(db)?,
        },
        id: row.try_get("id").map_err(db)?,
        page_ready,
        anchors_ready: row.try_get::<i64, _>("anchors_rev").map_err(db)? == revision,
    })
}

#[derive(Clone)]
enum Outcome {
    Ready,
    Page(NotePageError),
    Missing,
    Failed(String),
}
impl Outcome {
    fn from_result(result: Result<()>) -> Self {
        match result {
            Ok(()) => Self::Ready,
            Err(Error::NotePage(kind)) => Self::Page(kind),
            Err(Error::NotFound(_)) => Self::Missing,
            Err(error) => Self::Failed(error.to_string()),
        }
    }
    fn result(&self) -> Result<()> {
        match self {
            Self::Ready => Ok(()),
            Self::Page(kind) => Err(failure(*kind)),
            Self::Missing => Err(Error::NotFound("note index".into())),
            Self::Failed(error) => Err(Error::Internal(error.clone())),
        }
    }
}
struct Job {
    owner: Arc<Owner>,
    key: Key,
    demand: AtomicU64,
    cancelled: AtomicBool,
    result: watch::Sender<Option<Outcome>>,
    finished: Mutex<Option<Instant>>,
}
impl Job {
    fn live(&self) -> bool {
        !self.owner.closing.load(Ordering::Acquire)
            && !self.cancelled.load(Ordering::Acquire)
            && intent_core::now_epoch_ms().saturating_sub(self.demand.load(Ordering::Acquire))
                < LEASE_MS
    }
    fn check(&self) -> Result<()> {
        if self.live() {
            Ok(())
        } else {
            Err(failure(NotePageError::Preparing))
        }
    }
    async fn verify(&self, conn: &mut SqliteConnection) -> Result<Head> {
        self.check()?;
        let current = head(conn, &self.key.workspace, &self.key.note).await?;
        if current.key != self.key {
            return Err(failure(NotePageError::Stale));
        }
        Ok(current)
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct Pause {
    pub staged: bool,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[derive(Default)]
pub(crate) struct Owner {
    pub(crate) closing: AtomicBool,
}

pub(crate) struct Coordinator {
    jobs: Mutex<HashMap<Key, Arc<Job>>>,
    slot: Semaphore,
    #[cfg(test)]
    pub builds: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub pause: Mutex<Option<Arc<Pause>>>,
}
impl Default for Coordinator {
    fn default() -> Self {
        Self {
            jobs: Mutex::default(),
            slot: Semaphore::new(1),
            #[cfg(test)]
            builds: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            pause: Mutex::default(),
        }
    }
}

#[derive(Hash, PartialEq, Eq)]
enum Database {
    #[cfg(unix)]
    File(u64, u64),
    #[cfg(not(unix))]
    Path(PathBuf),
}
static DOMAINS: LazyLock<Mutex<HashMap<Database, Weak<Coordinator>>>> =
    LazyLock::new(Mutex::default);
pub(crate) fn domain_for(path: &Path) -> Result<Arc<Coordinator>> {
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(path)
            .map_err(|e| Error::Internal(format!("index database identity: {e}")))?;
        Database::File(meta.dev(), meta.ino())
    };
    #[cfg(not(unix))]
    let identity = Database::Path(
        std::fs::canonicalize(path)
            .map_err(|e| Error::Internal(format!("index database identity: {e}")))?,
    );
    let mut domains = DOMAINS
        .lock()
        .map_err(|_| Error::Internal("index domains poisoned".into()))?;
    domains.retain(|_, v| v.strong_count() > 0);
    if let Some(domain) = domains.get(&identity).and_then(Weak::upgrade) {
        return Ok(domain);
    }
    let domain = Arc::new(Coordinator::default());
    domains.insert(identity, Arc::downgrade(&domain));
    Ok(domain)
}

impl Coordinator {
    #[cfg(test)]
    pub(crate) fn expire_demands(&self) {
        for job in self.jobs.lock().unwrap().values() {
            job.demand.store(0, Ordering::Release);
        }
    }

    #[cfg(test)]
    pub(crate) fn queued_identities(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }

    #[cfg(test)]
    async fn pause(&self, staged: bool) {
        let pause = self.pause.lock().expect("index test pause").clone();
        if let Some(pause) = pause.filter(|p| p.staged == staged) {
            pause.entered.notify_one();
            pause.release.notified().await;
        }
    }

    pub(crate) async fn close(&self, owner: &Arc<Owner>) {
        let jobs: Vec<_> = {
            let jobs = self.jobs.lock().expect("index jobs");
            // Serialize admission with shutdown. Other independent Store
            // handles retain their own pools and demand ownership.
            owner.closing.store(true, Ordering::Release);
            jobs.values()
                .filter(|job| Arc::ptr_eq(&job.owner, owner))
                .cloned()
                .collect()
        };
        for job in &jobs {
            job.cancelled.store(true, Ordering::Release);
        }
        for job in jobs {
            let mut result = job.result.subscribe();
            while result.borrow_and_update().is_none() {
                if result.changed().await.is_err() {
                    break;
                }
            }
        }
    }
}

impl Store {
    /// A read fence for consumers whose persisted references address index rows.
    pub(crate) async fn ready_note_content_generation(
        &self,
        workspace: &str,
        note: &str,
    ) -> Result<String> {
        sqlx::query_scalar("SELECT content_generation FROM note_page_head WHERE workspace_id=? AND note_id=? AND indexed_rev=current_rev AND profile_revision=?")
            .bind(workspace).bind(note).bind(crate::note_page_index::profile_revision())
            .fetch_optional(self.read_pool()).await.map_err(db)?.ok_or_else(||failure(NotePageError::Expired))
    }

    /// Prepare just this note before establishing a read snapshot. Services must
    /// authorize before calling, and recheck authorization after the wait.
    pub(crate) async fn ensure_note_index(
        &self,
        workspace: &str,
        note: &str,
        anchors: bool,
    ) -> Result<()> {
        let current = {
            let mut conn = self.read_pool().acquire().await.map_err(db)?;
            head(&mut conn, workspace, note).await?
        };
        if current.page_ready && (!anchors || current.anchors_ready) {
            return Ok(());
        }
        if self.note_index_owner.closing.load(Ordering::Acquire) {
            return Err(failure(NotePageError::Preparing));
        }
        let (job, created) = {
            let mut jobs = self
                .note_index_jobs
                .jobs
                .lock()
                .map_err(|_| Error::Internal("index jobs poisoned".into()))?;
            if self.note_index_owner.closing.load(Ordering::Acquire) {
                return Err(failure(NotePageError::Preparing));
            }
            for job in jobs.values().filter(|job| {
                job.key.workspace == current.key.workspace
                    && job.key.note == current.key.note
                    && job.key != current.key
            }) {
                job.cancelled.store(true, Ordering::Release);
            }
            jobs.retain(|_, job| match job.result.borrow().as_ref() {
                None => true,
                Some(
                    Outcome::Ready | Outcome::Page(NotePageError::Preparing | NotePageError::Stale),
                ) => false,
                Some(_) => job
                    .finished
                    .lock()
                    .expect("index finish time")
                    .is_some_and(|at| at.elapsed() < Duration::from_secs(30)),
            });
            if let Some(job) = jobs.get(&current.key) {
                job.demand
                    .store(intent_core::now_epoch_ms(), Ordering::Release);
                (job.clone(), false)
            } else {
                if jobs.len() >= QUEUE_LIMIT {
                    return Err(failure(NotePageError::Preparing));
                }
                let (result, _) = watch::channel(None);
                let job = Arc::new(Job {
                    owner: self.note_index_owner.clone(),
                    key: current.key.clone(),
                    demand: AtomicU64::new(intent_core::now_epoch_ms()),
                    cancelled: AtomicBool::new(false),
                    result,
                    finished: Mutex::new(None),
                });
                jobs.insert(current.key, job.clone());
                (job, true)
            }
        };
        if created {
            let store = self.clone();
            let job = job.clone();
            tokio::spawn(async move {
                let worker_job = job.clone();
                // Retain a supervisor even when an unexpected worker panic occurs,
                // so waiters and shutdown always receive a terminal result.
                let result =
                    tokio::spawn(async move { store.run_note_index_job(&worker_job).await })
                        .await
                        .unwrap_or_else(|error| {
                            Err(Error::Internal(format!("note index task: {error}")))
                        });
                *job.finished.lock().expect("index finish time") = Some(Instant::now());
                job.result.send_replace(Some(Outcome::from_result(result)));
            });
        }
        let mut result = job.result.subscribe();
        tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                if let Some(value) = result.borrow_and_update().clone() {
                    return value.result();
                }
                result
                    .changed()
                    .await
                    .map_err(|_| Error::Internal("index job result closed".into()))?;
            }
        })
        .await
        .map_err(|_| failure(NotePageError::Preparing))?
    }

    async fn run_note_index_job(&self, job: &Arc<Job>) -> Result<()> {
        let _permit = loop {
            job.check()?;
            match tokio::time::timeout(Duration::from_secs(5), self.note_index_jobs.slot.acquire())
                .await
            {
                Ok(Ok(permit)) => break permit,
                Ok(Err(_)) => return Err(failure(NotePageError::Preparing)),
                Err(_) => {}
            }
        };
        let (note, roots, original) = {
            let mut conn = NoteWriteConnection::begin(self).await?;
            let captured=async {
                let current=job.verify(&mut conn).await?;
                // Measure in SQLite before hydrating the canonical fields.
                let (source,aux):(i64,i64)=sqlx::query_as("SELECT length(CAST(content AS BLOB)),length(CAST(title AS BLOB))+length(CAST(tags AS BLOB))+length(CAST(COALESCE(task_json,'') AS BLOB)) FROM note WHERE workspace_id=? AND id=?")
                    .bind(&job.key.workspace).bind(&job.key.note).fetch_one(&mut *conn).await.map_err(db)?;
                if source>sql_offset(crate::note_index_budget::SOURCE_BYTES)? || aux>sql_offset(crate::note_index_budget::AUX_BYTES)? { return Err(failure(NotePageError::ResourceLimit)); }
                let row=sqlx::query("SELECT * FROM note WHERE workspace_id=? AND id=?").bind(&job.key.workspace).bind(&job.key.note).fetch_one(&mut *conn).await.map_err(db)?;
                let note=crate::note_repo::map_note_row(&row)?;
                let root_bytes: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(bytes),0) FROM (SELECT length(CAST(id AS BLOB))+64 AS bytes FROM comment WHERE workspace_id=? AND note_id=? AND parent_id IS NULL LIMIT 65537)")
                    .bind(&job.key.workspace).bind(&job.key.note).fetch_one(&mut *conn).await.map_err(db)?;
                if root_bytes > sql_offset(crate::note_index_budget::AUX_BYTES)? { return Err(failure(NotePageError::ResourceLimit)); }
                let roots:Vec<String>=sqlx::query_scalar("SELECT id FROM comment WHERE workspace_id=? AND note_id=? AND parent_id IS NULL AND COALESCE(json_type(extra_json,'$.isOrphaned'),'null')!='true' ORDER BY id LIMIT 65537")
                    .bind(&job.key.workspace).bind(&job.key.note).fetch_all(&mut *conn).await.map_err(db)?;
                if roots.len()>65536 || roots.iter().map(String::len).sum::<usize>()>crate::note_index_budget::AUX_BYTES { return Err(failure(NotePageError::ResourceLimit)); }
                Ok((note,roots,current))
            }.await;
            conn.finish(captured, "capture note index").await?
        };
        if original.page_ready && original.anchors_ready {
            return Ok(());
        }
        #[cfg(test)]
        self.note_index_jobs.builds.fetch_add(1, Ordering::Relaxed);
        let page_needed = !original.page_ready;
        let live_job = job.clone();
        let computed = tokio::task::spawn_blocking(move || {
            crate::note_index_budget::run(Arc::new(move || live_job.live()), || {
                let prepared = if page_needed {
                    Some(crate::note_page_index::prepare(&note)?)
                } else {
                    None
                };
                let ids: Vec<_> = roots.iter().map(String::as_str).collect();
                let anchors = crate::note_annotation_repo::anchor_occurrences(&note.content, &ids)?;
                Ok::<_, Error>((prepared, anchors))
            })?
        })
        .await
        .map_err(|e| Error::Internal(format!("note index worker: {e}")))?;
        job.check()?;
        let (prepared, occurrences) = computed?;
        #[cfg(test)]
        self.note_index_jobs.pause(false).await;
        job.check()?;
        self.install_note_index(job, original.id, prepared, occurrences)
            .await
    }

    async fn index_batch<T>(
        &self,
        job: &Job,
        page_pending: bool,
        action: impl for<'a> FnOnce(&'a mut SqliteConnection) -> intent_core::BoxFuture<'a, Result<T>>,
    ) -> Result<T> {
        let mut conn = NoteWriteConnection::begin(self).await?;
        let result = async {
            let current = job.verify(&mut conn).await?;
            if (page_pending && current.page_ready) || current.anchors_ready {
                return Err(failure(NotePageError::Stale));
            }
            action(&mut conn).await
        }
        .await;
        conn.finish(result, "stage note index").await
    }

    async fn install_note_index(
        &self,
        job: &Job,
        anchor_head: i64,
        prepared: Option<Prepared>,
        anchors: Vec<AnchorOccurrence>,
    ) -> Result<()> {
        let page_pending = prepared.is_some();
        let mut conn = NoteWriteConnection::begin(self).await?;
        let result = async {
            job.verify(&mut conn).await?;
            if page_pending {
                sqlx::query(
                    "UPDATE note_page_head SET indexed_rev=-1 WHERE workspace_id=? AND note_id=?",
                )
                .bind(&job.key.workspace)
                .bind(&job.key.note)
                .execute(&mut *conn)
                .await
                .map_err(db)?;
            }
            sqlx::query("UPDATE note_annotation_head SET anchors_rev=-1 WHERE id=?")
                .bind(anchor_head)
                .execute(&mut *conn)
                .await
                .map_err(db)?;
            Ok(())
        }
        .await;
        conn.finish(result, "begin note index staging").await?;
        let mut clears = vec![("note_comment_anchor", "head_id", 32)];
        if page_pending {
            clears.extend([
                ("note_page_entry", "note", 8),
                ("note_page_piece", "note", 64),
            ]);
        }
        for (table, scope, take) in clears {
            loop {
                let (workspace, note) = (job.key.workspace.clone(), job.key.note.clone());
                let sql = if scope == "head_id" {
                    format!("DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE head_id=? LIMIT {take})")
                } else {
                    format!("DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE workspace_id=? AND note_id=? LIMIT {take})")
                };
                let removed = self
                    .index_batch(job, page_pending, move |conn| {
                        Box::pin(async move {
                            let query = sqlx::query(&sql);
                            let query = if scope == "head_id" {
                                query.bind(anchor_head)
                            } else {
                                query.bind(workspace).bind(note)
                            };
                            Ok(query.execute(conn).await.map_err(db)?.rows_affected())
                        })
                    })
                    .await?;
                if removed == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        let content_generation = uuid::Uuid::new_v4().simple().to_string();
        if let Some(prepared) = &prepared {
            for chunk in prepared.pieces.chunks(64) {
                let pieces: Vec<_> = chunk
                    .iter()
                    .map(|p| {
                        Ok((
                            sql_offset(p.start)?,
                            sql_offset(p.end)?,
                            p.text.clone(),
                            sql_offset(p.byte_start)?,
                            sql_offset(p.scalar_start)?,
                            sql_offset(p.lf_start)?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                let (workspace, note, generation) = (
                    job.key.workspace.clone(),
                    job.key.note.clone(),
                    content_generation.clone(),
                );
                self.index_batch(job,true,move |conn|Box::pin(async move {
                    for (start,end,text,byte,scalar,lf) in pieces {
                        sqlx::query("INSERT INTO note_page_piece(workspace_id,note_id,start,end,text,byte_start,scalar_start,lf_start,content_generation) VALUES (?,?,?,?,?,?,?,?,?)")
                            .bind(&workspace).bind(&note).bind(start).bind(end).bind(text).bind(byte).bind(scalar).bind(lf).bind(&generation).execute(&mut *conn).await.map_err(db)?;
                    } Ok(())
                })).await?;
            }
            let mut offset = 0;
            while offset < prepared.entries.len() {
                let mut end = offset;
                let mut bytes = 0;
                while end < prepared.entries.len() && end - offset < 128 {
                    let row = &prepared.entries[end];
                    let length = row.0.len() + row.2.len() + 64;
                    if length > BATCH_BYTES {
                        return Err(failure(NotePageError::ResourceLimit));
                    }
                    if bytes + length > BATCH_BYTES {
                        break;
                    }
                    bytes += length;
                    end += 1;
                }
                let rows = prepared.entries[offset..end].to_vec();
                let (workspace, note) = (job.key.workspace.clone(), job.key.note.clone());
                self.index_batch(job, true, move |conn| {
                    Box::pin(async move {
                        for (collection, position, value, start, end) in rows {
                            sqlx::query("INSERT INTO note_page_entry VALUES (?,?,?,?,?,?,?)")
                                .bind(&workspace)
                                .bind(&note)
                                .bind(collection)
                                .bind(position)
                                .bind(value)
                                .bind(start)
                                .bind(end)
                                .execute(&mut *conn)
                                .await
                                .map_err(db)?;
                        }
                        Ok(())
                    })
                })
                .await?;
                offset = end;
            }
            for chunk in prepared.artifacts.chunks(128) {
                let rows = chunk.to_vec();
                let (workspace, note) = (job.key.workspace.clone(), job.key.note.clone());
                self.index_batch(job,true,move |conn|Box::pin(async move {
                    for (native,source,primitive) in rows {
                        sqlx::query("INSERT INTO note_artifact_source(workspace_id,note_id,native_collection,source_collection,primitive) VALUES (?,?,?,?,?)").bind(&workspace).bind(&note).bind(native).bind(source).bind(primitive).execute(&mut *conn).await.map_err(db)?;
                    } Ok(())
                })).await?;
            }
        }
        for chunk in anchors.chunks(32) {
            let rows = chunk.to_vec();
            self.index_batch(job,page_pending,move |conn|Box::pin(async move {
                for anchor in rows {
                    let result=sqlx::query("INSERT INTO note_comment_anchor(head_id,comment_id,occurrence_id,start,end,thread_id) SELECT head_id,comment_id,?,?,?,thread_id FROM note_comment_projection WHERE head_id=? AND comment_id=? AND parent_id IS NULL")
                        .bind(anchor.occurrence_id).bind(anchor.source_range.start).bind(anchor.source_range.end).bind(anchor_head).bind(anchor.comment_id).execute(&mut *conn).await.map_err(db)?;
                    if result.rows_affected()!=1 { return Err(failure(NotePageError::Stale)); }
                } Ok(())
            })).await?;
        }
        #[cfg(test)]
        self.note_index_jobs.pause(true).await;
        let (workspace, note, revision) = (
            job.key.workspace.clone(),
            job.key.note.clone(),
            job.key.revision,
        );
        self.index_batch(job,page_pending,move |conn|Box::pin(async move {
            if let Some(p)=prepared {
                sqlx::query("UPDATE note_page_head SET indexed_rev=?,profile_revision=?,source_length=?,source_bytes=?,scalar_count=?,lf_count=?,task_count=?,content_generation=? WHERE workspace_id=? AND note_id=?")
                    .bind(revision).bind(crate::note_page_index::profile_revision()).bind(p.source_length).bind(p.source_bytes).bind(p.scalar_count).bind(p.lf_count).bind(p.task_count).bind(content_generation).bind(workspace).bind(note).execute(&mut *conn).await.map_err(db)?;
            }
            sqlx::query("UPDATE note_annotation_head SET anchors_rev=source_rev WHERE id=?").bind(anchor_head).execute(conn).await.map_err(db)?;
            Ok(())
        })).await
    }
}
