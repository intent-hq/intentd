//! Standalone comment mutations and derived anchor publication share one writer.
//! No canonical source repair or source-body collection across notes happens here.
use super::{encode_comment_json, extra_map_to_json, ExtraFields, COMMENT_COLUMNS};
use crate::{enum_to_db, Store};
use intent_core::{Comment, CommentStatus, Error, NoteId, Result, WorkspaceId};
use serde_json::{Map, Value};
use sqlx::Row;
use sqlx::{Sqlite, Transaction};
use std::collections::BTreeSet;

// Owner tests may scope this hook around the real public wrapper after module
// registration. The notification means mutation completed inside an uncommitted
// writer; cancellation of the pending future drops that same transaction.
#[cfg(test)]
tokio::task_local! {
    pub(crate) static AFTER_MUTATION: tokio::sync::mpsc::UnboundedSender<()>;
}

fn db(error: &sqlx::Error) -> Error {
    Error::Internal(format!("comment anchor transaction: {error}"))
}

async fn begin<'a>(store: &'a Store, workspace: &WorkspaceId) -> Result<Transaction<'a, Sqlite>> {
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|error| db(&error))?;
    let retiring: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM note_annotation_workspace_retirement WHERE workspace_id=?)",
    )
    .bind(workspace.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| db(&error))?;
    if retiring {
        return Err(Error::NotFound("note annotations".into()));
    }
    Ok(tx)
}

async fn finish(
    mut tx: Transaction<'_, Sqlite>,
    workspace: &WorkspaceId,
    notes: &[Option<String>],
) -> Result<()> {
    #[cfg(test)]
    if let Ok(observer) = AFTER_MUTATION.try_with(Clone::clone) {
        observer.send(()).expect("comment mutation observer alive");
        std::future::pending::<()>().await;
    }
    let scopes: BTreeSet<&str> = notes.iter().filter_map(Option::as_deref).collect();
    for note in scopes {
        crate::note_annotation_repo::rebuild_note_anchors(
            &mut tx,
            workspace,
            &NoteId(note.into()),
            None,
        )
        .await?;
    }
    tx.commit().await.map_err(|error| db(&error))
}

pub(super) async fn insert(
    store: &Store,
    workspace_id: &WorkspaceId,
    c: &Comment,
    legacy_extra: &Map<String, Value>,
) -> Result<()> {
    let mut tx = begin(store, workspace_id).await?;
    let (anchor_json, extra_json) = encode_comment_json(c, legacy_extra)?;
    let sql = format!(
        "INSERT INTO comment ({COMMENT_COLUMNS}, workspace_id) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    );
    sqlx::query(&sql)
        .bind(&c.id)
        .bind(&c.thread_id)
        .bind(c.note_id.as_ref().map(|n| n.0.clone()))
        .bind(enum_to_db(&c.kind)?)
        .bind(&c.content)
        .bind(&c.author)
        .bind(enum_to_db(&c.author_type)?)
        .bind(enum_to_db(&c.status)?)
        .bind(&c.parent_id)
        .bind(anchor_json)
        .bind(&c.anchor_text)
        .bind(extra_json)
        .bind(&c.created_at)
        .bind(&c.updated_at)
        .bind(&workspace_id.0)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("insert comment failed: {e}")))?;
    finish(tx, workspace_id, &[c.note_id.as_ref().map(|n| n.0.clone())]).await
}

pub(super) async fn update(store: &Store, workspace_id: &WorkspaceId, c: &Comment) -> Result<()> {
    let anchor_json = serde_json::to_string(&c.anchor)
        .map_err(|e| Error::Internal(format!("encode anchor failed: {e}")))?;
    let extra = ExtraFields {
        // Creation attribution is immutable. Carry these from the stored
        // row below, including absence on legacy comments.
        author_principal_id: None,
        author_identity: None,
        anchor_before: c.anchor_before.clone(),
        anchor_after: c.anchor_after.clone(),
        suggestion_original: c.suggestion_original.clone(),
        suggestion_proposed: c.suggestion_proposed.clone(),
        agent_id: c.agent_id.clone(),
        is_orphaned: c.is_orphaned,
    };
    let mut merged = extra.to_map()?;
    // Carry over preserved legacy/unknown keys from the existing row.
    let mut tx = begin(store, workspace_id).await?;
    let old = sqlx::query("SELECT note_id,extra_json FROM comment WHERE id=? AND workspace_id=?")
        .bind(&c.id)
        .bind(workspace_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("read comment extras failed: {e}")))?
        .ok_or_else(|| Error::NotFound(format!("comment {}", c.id)))?;
    let old_note: Option<String> = old.get("note_id");
    let existing: Option<String> = old.get("extra_json");
    if let Some(raw) = existing {
        if let Ok(Value::Object(old)) = serde_json::from_str::<Value>(&raw) {
            for (k, v) in old {
                // A non-bool `isOrphaned` can only be a legacy value the
                // importer preserved verbatim (the store itself only ever
                // encodes booleans here) — carry it over too.
                let legacy_orphaned = k == "isOrphaned" && !matches!(v, Value::Bool(_));
                if matches!(k.as_str(), "authorPrincipalId" | "authorIdentity")
                    || !ExtraFields::KNOWN_KEYS.contains(&k.as_str())
                    || legacy_orphaned
                {
                    merged.entry(k).or_insert(v);
                }
            }
        }
    }
    let extra_json = extra_map_to_json(merged)?;
    let res = sqlx::query(
        "UPDATE comment SET thread_id=?, note_id=?, kind=?, content=?, author=?, \
             author_type=?, status=?, parent_id=?, anchor_json=?, anchor_text=?, extra_json=?, \
             updated_at=? WHERE id=? AND workspace_id=?",
    )
    .bind(&c.thread_id)
    .bind(c.note_id.as_ref().map(|n| n.0.clone()))
    .bind(enum_to_db(&c.kind)?)
    .bind(&c.content)
    .bind(&c.author)
    .bind(enum_to_db(&c.author_type)?)
    .bind(enum_to_db(&c.status)?)
    .bind(&c.parent_id)
    .bind(anchor_json)
    .bind(&c.anchor_text)
    .bind(extra_json)
    .bind(&c.updated_at)
    .bind(&c.id)
    .bind(&workspace_id.0)
    .execute(&mut *tx)
    .await
    .map_err(|e| Error::Internal(format!("update comment failed: {e}")))?;
    if res.rows_affected() == 0 {
        return Err(Error::NotFound(format!("comment {}", c.id)));
    }
    finish(
        tx,
        workspace_id,
        &[old_note, c.note_id.as_ref().map(|n| n.0.clone())],
    )
    .await
}

pub(super) async fn delete(store: &Store, workspace_id: &WorkspaceId, id: &str) -> Result<()> {
    let mut tx = begin(store, workspace_id).await?;
    let note: Option<String> =
        sqlx::query_scalar("DELETE FROM comment WHERE id=? AND workspace_id=? RETURNING note_id")
            .bind(id)
            .bind(workspace_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("delete comment failed: {e}")))?
            .ok_or_else(|| Error::NotFound(format!("comment {id}")))?;
    finish(tx, workspace_id, &[note]).await
}

pub(super) async fn delete_in_note(
    store: &Store,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    id: &str,
) -> Result<String> {
    let mut tx = begin(store, workspace_id).await?;
    let thread = sqlx::query_scalar::<_, String>(
        "DELETE FROM comment WHERE id=? AND workspace_id=? AND note_id=? RETURNING thread_id",
    )
    .bind(id)
    .bind(workspace_id.as_str())
    .bind(note_id.as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| Error::Internal(format!("delete comment failed: {e}")))?
    .ok_or_else(|| Error::NotFound(format!("comment {id}")))?;
    finish(tx, workspace_id, &[Some(note_id.0.clone())]).await?;
    Ok(thread)
}

pub(super) async fn set_status(
    store: &Store,
    workspace_id: &WorkspaceId,
    thread_id: &str,
    status: CommentStatus,
    updated_at: &str,
) -> Result<u64> {
    let mut tx = begin(store, workspace_id).await?;
    // IDs only, read before mutation on the same writer. A legacy thread may
    // span multiple notes; finalize each independently without collecting bodies.
    let notes: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT DISTINCT note_id FROM comment WHERE thread_id=? AND workspace_id=? ORDER BY note_id",
    ).bind(thread_id).bind(workspace_id.as_str()).fetch_all(&mut *tx).await.map_err(|error| db(&error))?;
    let res = sqlx::query(
        "UPDATE comment SET status=?,updated_at=? WHERE thread_id=? AND workspace_id=?",
    )
    .bind(enum_to_db(&status)?)
    .bind(updated_at)
    .bind(thread_id)
    .bind(workspace_id.as_str())
    .execute(&mut *tx)
    .await
    .map_err(|e| Error::Internal(format!("set thread status failed: {e}")))?;
    let affected = res.rows_affected();
    finish(tx, workspace_id, &notes).await?;
    Ok(affected)
}

#[cfg(test)]
#[path = "anchor_writes_tests.rs"]
mod tests;
