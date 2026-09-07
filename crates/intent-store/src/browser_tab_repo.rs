//! Daemon-owned browser tab registry repository (REV-2 Model 2 & 6). One row
//! in `browser_tab` per logical tab; `host_client_id` is the logical client
//! (§5.17) that owns the live webview. Writes are host-scoped: a report about
//! a tab hosted elsewhere is rejected with `Error::InvalidParams`.
//!
//! `closed_at` is a reconciliation tombstone (see `0115_browser_tab.sql`):
//! rows closed daemon-side while their host was offline stay hidden from every
//! list until the host acknowledges the close — every
//! [`Store::sync_browser_tabs`] that still reports the id answers `drop`
//! (the tombstone is retained until a later snapshot omits the id), and the
//! host's own [`Store::remove_browser_tab`] purges it outright. A stale
//! upsert never revives a tombstone and never re-homes a tab.
//!
//! Every mutation runs its read / ownership check / write inside one
//! transaction on the single-connection write pool, so concurrent reports
//! (registry RPCs are dispatched on separate tasks) serialize and a failed
//! snapshot leaves nothing half-applied.

use std::collections::HashSet;

use intent_core::{
    now_iso, AgentId, BrowserTab, BrowserTabInput, BrowserTabSize, BrowserTabSyncResult,
    BrowserTabUpsertOutcome, BrowserTabVisibility, ClientId, Error, Result, WorkspaceId,
};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection, SqlitePool, Transaction};

use crate::Store;

type WriteTxn<'a> = Transaction<'a, sqlx::Sqlite>;

const COLUMNS: &str = "tab_id, workspace_id, host_client_id, url, requested_url, title, \
     owner_agent_id, owner_agent_name, visibility, emulated_width, emulated_height, \
     created_at, updated_at, closed_at";

/// A stored row including its tombstone marker.
struct StoredTab {
    tab: BrowserTab,
    closed: bool,
}

impl Store {
    /// Every open tab of workspace `id`, oldest first (stable wire order).
    /// Lenient on unknown workspaces (empty).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_browser_tabs(&self, id: &WorkspaceId) -> Result<Vec<BrowserTab>> {
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM browser_tab \
             WHERE workspace_id = ? AND closed_at IS NULL ORDER BY created_at, tab_id"
        ))
        .bind(&id.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list browser tabs failed: {e}")))?;
        rows.iter().map(|r| map_row(r).map(|s| s.tab)).collect()
    }

    /// Every open tab hosted by `host`, across workspaces, oldest first.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_browser_tabs_by_host(&self, host: &ClientId) -> Result<Vec<BrowserTab>> {
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM browser_tab \
             WHERE host_client_id = ? AND closed_at IS NULL ORDER BY created_at, tab_id"
        ))
        .bind(&host.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list browser tabs by host failed: {e}")))?;
        rows.iter().map(|r| map_row(r).map(|s| s.tab)).collect()
    }

    /// One open tab by id, or `None` (tombstoned rows count as absent).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_browser_tab(&self, tab_id: &str) -> Result<Option<BrowserTab>> {
        Ok(self
            .get_stored_tab(tab_id)
            .await?
            .filter(|s| !s.closed)
            .map(|s| s.tab))
    }

    async fn get_stored_tab(&self, tab_id: &str) -> Result<Option<StoredTab>> {
        let mut conn = self
            .read_pool()
            .acquire()
            .await
            .map_err(|e| Error::Internal(format!("get browser tab acquire failed: {e}")))?;
        fetch_stored(&mut conn, tab_id).await
    }

    /// Host-reported upsert (`browser.upsertTab`): insert a new row hosted by
    /// `host`, or update the host-reported fields of an existing open row of
    /// the same host. Returns how the row changed so the caller can emit the
    /// matching event — `Unchanged` when the report matched the stored state
    /// (no write, no event). Read, ownership check and write share one write
    /// transaction, so two hosts racing on a fresh id cannot both win.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidParams` if the tab exists (open or tombstoned)
    /// with a different host, or is tombstoned — a daemon-side close the host
    /// has not acknowledged yet; the tombstone is kept so the next
    /// [`Store::sync_browser_tabs`] still tells the host to drop it.
    /// `Error::Internal` if the database operation fails.
    pub async fn upsert_browser_tab(
        &self,
        host: &ClientId,
        input: BrowserTabInput,
    ) -> Result<BrowserTabUpsertOutcome> {
        let pool = self.write_pool().clone();
        crate::with_write_txn_retry(|| {
            let input = input.clone();
            let pool = pool.clone();
            async move {
                let mut tx = begin(&pool, "upsert browser tab").await?;
                let outcome = match fetch_stored(&mut tx, &input.tab_id).await? {
                    None => {
                        let tab = new_tab(host, input);
                        write_tab(&mut tx, &tab).await?;
                        BrowserTabUpsertOutcome::Opened(tab)
                    }
                    Some(stored) => {
                        reject_foreign_host(&stored.tab, host)?;
                        reject_tombstone(&stored)?;
                        reject_workspace_move(&stored.tab, &input)?;
                        let changes = stored.tab.changes_from(&input);
                        if changes.is_empty() {
                            BrowserTabUpsertOutcome::Unchanged(stored.tab)
                        } else {
                            let mut updated = stored.tab;
                            updated.apply_input(input);
                            updated.updated_at = now_iso();
                            write_tab(&mut tx, &updated).await?;
                            BrowserTabUpsertOutcome::Updated {
                                tab: updated,
                                changes: serde_json::Value::Object(changes),
                            }
                        }
                    }
                };
                commit(tx, "upsert browser tab").await?;
                Ok(outcome)
            }
        })
        .await
    }

    /// Host-reported close (`browser.removeTab`): delete the row outright and
    /// return it (for the `browser:tab-closed` payload). Unknown or already
    /// tombstoned ids are an idempotent `None` (the tombstone is purged since
    /// the host has now dropped the tab).
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidParams` if the tab is hosted by another client;
    /// `Error::Internal` if the database operation fails.
    pub async fn remove_browser_tab(
        &self,
        host: &ClientId,
        tab_id: &str,
    ) -> Result<Option<BrowserTab>> {
        let pool = self.write_pool().clone();
        crate::with_write_txn_retry(|| {
            let pool = pool.clone();
            async move {
                let mut tx = begin(&pool, "remove browser tab").await?;
                let Some(stored) = fetch_stored(&mut tx, tab_id).await? else {
                    return Ok(None);
                };
                reject_foreign_host(&stored.tab, host)?;
                delete_row(&mut tx, tab_id).await?;
                commit(tx, "remove browser tab").await?;
                Ok((!stored.closed).then_some(stored.tab))
            }
        })
        .await
    }

    /// Daemon-side close of a tab whose host may be offline (a viewer / agent
    /// `browser.closeTab { force }`, REV-2 Model 6): the row is tombstoned so
    /// it disappears from every list now and the host is told to drop it on
    /// its next [`Store::sync_browser_tabs`]. Returns the closed row, or
    /// `None` when there was no open row.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn close_browser_tab(&self, tab_id: &str) -> Result<Option<BrowserTab>> {
        let pool = self.write_pool().clone();
        crate::with_write_txn_retry(|| {
            let pool = pool.clone();
            async move {
                let mut tx = begin(&pool, "close browser tab").await?;
                let Some(StoredTab { tab, closed: false }) = fetch_stored(&mut tx, tab_id).await?
                else {
                    return Ok(None);
                };
                sqlx::query("UPDATE browser_tab SET closed_at = ? WHERE tab_id = ?")
                    .bind(now_iso())
                    .bind(tab_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Internal(format!("close browser tab failed: {e}")))?;
                commit(tx, "close browser tab").await?;
                Ok(Some(tab))
            }
        })
        .await
    }

    /// Claim migration (REV-2 Model 3 & 5): an agent `claimTab` on a tab
    /// hosted elsewhere was dispatched to the workspace's driving client, so
    /// the row moves there — `host_client_id` becomes `new_host` and
    /// `owner_agent_id` the claiming agent. Returns the updated row with the
    /// field-wise `changes` (`hostClientId` / `ownerAgentId`), or `None` when
    /// there is no open row or nothing differs (the host already matches and
    /// the owner is unchanged — the host's own report covers that case).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn reassign_browser_tab_host(
        &self,
        tab_id: &str,
        new_host: &ClientId,
        owner_agent_id: Option<&AgentId>,
    ) -> Result<Option<(BrowserTab, serde_json::Value)>> {
        let pool = self.write_pool().clone();
        crate::with_write_txn_retry(|| {
            let pool = pool.clone();
            let owner_agent_id = owner_agent_id.cloned();
            async move {
                let mut tx = begin(&pool, "reassign browser tab host").await?;
                let Some(StoredTab { tab, closed: false }) = fetch_stored(&mut tx, tab_id).await?
                else {
                    return Ok(None);
                };
                let mut changes = serde_json::Map::new();
                if tab.host_client_id != *new_host {
                    changes.insert(
                        "hostClientId".to_string(),
                        serde_json::Value::String(new_host.0.clone()),
                    );
                }
                if owner_agent_id.is_some() && tab.owner_agent_id != owner_agent_id {
                    changes.insert(
                        "ownerAgentId".to_string(),
                        serde_json::json!(owner_agent_id),
                    );
                }
                if changes.is_empty() {
                    return Ok(None);
                }
                let mut updated = tab;
                updated.host_client_id = new_host.clone();
                if owner_agent_id.is_some() {
                    updated.owner_agent_id = owner_agent_id;
                }
                updated.updated_at = now_iso();
                rehome_tab(&mut tx, &updated).await?;
                commit(tx, "reassign browser tab host").await?;
                Ok(Some((updated, serde_json::Value::Object(changes))))
            }
        })
        .await
    }

    /// Driving-client switch (REV-2 Model 10, `workspace.setBrowserClient`):
    /// every open **claimed** tab (`owner_agent_id` set) of workspace `id`
    /// not already hosted by `new_host` moves there. Returns each moved row
    /// with its `changes` (`{ hostClientId }`), oldest first; unclaimed tabs
    /// stay on their physical host.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails; nothing is
    /// persisted in that case.
    pub async fn reassign_claimed_browser_tabs(
        &self,
        id: &WorkspaceId,
        new_host: &ClientId,
    ) -> Result<Vec<(BrowserTab, serde_json::Value)>> {
        let pool = self.write_pool().clone();
        crate::with_write_txn_retry(|| {
            let pool = pool.clone();
            async move {
                let mut tx = begin(&pool, "reassign claimed browser tabs").await?;
                let rows = sqlx::query(&format!(
                    "SELECT {COLUMNS} FROM browser_tab \
                     WHERE workspace_id = ? AND closed_at IS NULL \
                     AND owner_agent_id IS NOT NULL AND host_client_id <> ? \
                     ORDER BY created_at, tab_id"
                ))
                .bind(&id.0)
                .bind(&new_host.0)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("list claimed browser tabs failed: {e}")))?;
                let mut moved = Vec::with_capacity(rows.len());
                let now = now_iso();
                for row in &rows {
                    let mut tab = map_row(row)?.tab;
                    tab.host_client_id = new_host.clone();
                    tab.updated_at.clone_from(&now);
                    rehome_tab(&mut tx, &tab).await?;
                    moved.push((
                        tab,
                        serde_json::json!({ "hostClientId": new_host.0.clone() }),
                    ));
                }
                commit(tx, "reassign claimed browser tabs").await?;
                Ok(moved)
            }
        })
        .await
    }

    /// Host snapshot reconciliation (`browser.syncTabs`, REV-2 Model 6), one
    /// transaction for the whole reconcile. `snapshot` is the host's full tab
    /// set across workspaces (duplicate ids after the first are ignored). Per
    /// snapshot tab: unknown ⇒ created (`opened`); open and hosted by `host`
    /// ⇒ host fields refreshed (`updated` when anything differs); tombstoned
    /// ⇒ the host must `drop` it (the tombstone is retained, so a repeated
    /// stale snapshot keeps answering `drop` instead of reviving the tab);
    /// hosted elsewhere ⇒ `drop` (a tab has exactly one host; the row is
    /// untouched). Every row of `host` absent from the snapshot is deleted —
    /// open rows are reported in `closed`, tombstones are purged silently
    /// (the host has acknowledged the drop by omitting the id).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails; nothing is
    /// persisted in that case.
    pub async fn sync_browser_tabs(
        &self,
        host: &ClientId,
        snapshot: Vec<BrowserTabInput>,
    ) -> Result<BrowserTabSyncResult> {
        let pool = self.write_pool().clone();
        crate::with_write_txn_retry(|| {
            let snapshot = snapshot.clone();
            let pool = pool.clone();
            async move {
                let mut tx = begin(&pool, "sync browser tabs").await?;
                let mut result = BrowserTabSyncResult::default();
                let mut seen: HashSet<String> = HashSet::new();
                for input in snapshot {
                    if !seen.insert(input.tab_id.clone()) {
                        continue;
                    }
                    match fetch_stored(&mut tx, &input.tab_id).await? {
                        None => {
                            let tab = new_tab(host, input);
                            write_tab(&mut tx, &tab).await?;
                            result.opened.push(tab);
                        }
                        Some(StoredTab { tab, closed: false }) if tab.host_client_id == *host => {
                            reject_workspace_move(&tab, &input)?;
                            let changes = tab.changes_from(&input);
                            if changes.is_empty() {
                                continue;
                            }
                            let mut updated = tab;
                            updated.apply_input(input);
                            updated.updated_at = now_iso();
                            write_tab(&mut tx, &updated).await?;
                            result
                                .updated
                                .push((updated, serde_json::Value::Object(changes)));
                        }
                        Some(StoredTab { tab, .. }) => result.drop.push(tab.tab_id),
                    }
                }
                let rows = sqlx::query(&format!(
                    "SELECT {COLUMNS} FROM browser_tab WHERE host_client_id = ? \
                     ORDER BY created_at, tab_id"
                ))
                .bind(&host.0)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("list browser tabs by host failed: {e}")))?;
                for row in &rows {
                    let stored = map_row(row)?;
                    if seen.contains(&stored.tab.tab_id) {
                        continue;
                    }
                    delete_row(&mut tx, &stored.tab.tab_id).await?;
                    if !stored.closed {
                        result.closed.push(stored.tab);
                    }
                }
                commit(tx, "sync browser tabs").await?;
                Ok(result)
            }
        })
        .await
    }
}

async fn begin<'a>(pool: &SqlitePool, what: &str) -> Result<WriteTxn<'a>> {
    pool.begin()
        .await
        .map_err(|e| Error::Internal(format!("{what} begin failed: {e}")))
}

async fn commit(tx: WriteTxn<'_>, what: &str) -> Result<()> {
    tx.commit()
        .await
        .map_err(|e| Error::Internal(format!("{what} commit failed: {e}")))
}

async fn fetch_stored(conn: &mut SqliteConnection, tab_id: &str) -> Result<Option<StoredTab>> {
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM browser_tab WHERE tab_id = ?"
    ))
    .bind(tab_id)
    .fetch_optional(conn)
    .await
    .map_err(|e| Error::Internal(format!("get browser tab failed: {e}")))?;
    row.as_ref().map(map_row).transpose()
}

/// Insert a new row or refresh the host-reported fields of an open row.
/// Never re-homes (`host_client_id`), re-dates (`created_at`) or revives
/// (`closed_at`) an existing row — callers have already established the
/// row is theirs and open.
async fn write_tab(conn: &mut SqliteConnection, tab: &BrowserTab) -> Result<()> {
    sqlx::query(
        "INSERT INTO browser_tab (tab_id, workspace_id, host_client_id, url, requested_url, \
         title, owner_agent_id, owner_agent_name, visibility, emulated_width, \
         emulated_height, created_at, updated_at, closed_at) \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL) \
         ON CONFLICT(tab_id) DO UPDATE SET \
         workspace_id = excluded.workspace_id, \
         url = excluded.url, requested_url = excluded.requested_url, title = excluded.title, \
         owner_agent_id = excluded.owner_agent_id, \
         owner_agent_name = excluded.owner_agent_name, visibility = excluded.visibility, \
         emulated_width = excluded.emulated_width, \
         emulated_height = excluded.emulated_height, updated_at = excluded.updated_at",
    )
    .bind(&tab.tab_id)
    .bind(&tab.workspace_id.0)
    .bind(&tab.host_client_id.0)
    .bind(&tab.url)
    .bind(&tab.requested_url)
    .bind(&tab.title)
    .bind(tab.owner_agent_id.as_ref().map(|a| a.0.as_str()))
    .bind(&tab.owner_agent_name)
    .bind(tab.visibility.as_str())
    .bind(tab.emulated_size.map(|s| i64::from(s.width)))
    .bind(tab.emulated_size.map(|s| i64::from(s.height)))
    .bind(&tab.created_at)
    .bind(&tab.updated_at)
    .execute(conn)
    .await
    .map_err(|e| Error::Internal(format!("write browser tab failed: {e}")))?;
    Ok(())
}

/// The one write allowed to move an open row to another host: the daemon-
/// initiated claim / driving-client migration (`reassign_*` above). Only the
/// host, the owner and `updated_at` change; everything else stays as the
/// previous host last reported it.
async fn rehome_tab(conn: &mut SqliteConnection, tab: &BrowserTab) -> Result<()> {
    sqlx::query(
        "UPDATE browser_tab SET host_client_id = ?, owner_agent_id = ?, updated_at = ? \
         WHERE tab_id = ? AND closed_at IS NULL",
    )
    .bind(&tab.host_client_id.0)
    .bind(tab.owner_agent_id.as_ref().map(|a| a.0.as_str()))
    .bind(&tab.updated_at)
    .bind(&tab.tab_id)
    .execute(conn)
    .await
    .map_err(|e| Error::Internal(format!("rehome browser tab failed: {e}")))?;
    Ok(())
}

async fn delete_row(conn: &mut SqliteConnection, tab_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM browser_tab WHERE tab_id = ?")
        .bind(tab_id)
        .execute(conn)
        .await
        .map_err(|e| Error::Internal(format!("delete browser tab failed: {e}")))?;
    Ok(())
}

fn reject_foreign_host(tab: &BrowserTab, host: &ClientId) -> Result<()> {
    if tab.host_client_id == *host {
        return Ok(());
    }
    Err(Error::InvalidParams(format!(
        "browser tab {} is hosted by client {}",
        tab.tab_id, tab.host_client_id
    )))
}

/// A `tabId` is bound to the workspace that created it: `browser:tab-*`
/// events are workspace-scoped, so a row silently changing `workspaceId`
/// would leave the old workspace's subscribers with a ghost tab. A report
/// naming another workspace is rejected as invalid params (in a sync, the
/// whole snapshot is rolled back) and the row stays untouched.
fn reject_workspace_move(tab: &BrowserTab, input: &BrowserTabInput) -> Result<()> {
    if tab.workspace_id == input.workspace_id {
        return Ok(());
    }
    Err(Error::InvalidParams(format!(
        "browser tab {} belongs to workspace {}; tabs do not move between workspaces",
        tab.tab_id, tab.workspace_id
    )))
}

fn reject_tombstone(stored: &StoredTab) -> Result<()> {
    if !stored.closed {
        return Ok(());
    }
    Err(Error::InvalidParams(format!(
        "browser tab {} was closed by the daemon; drop it (browser.syncTabs reports it in drop)",
        stored.tab.tab_id
    )))
}

fn new_tab(host: &ClientId, input: BrowserTabInput) -> BrowserTab {
    let now = now_iso();
    let BrowserTabInput {
        tab_id,
        workspace_id,
        url,
        requested_url,
        title,
        owner_agent_id,
        owner_agent_name,
        visibility,
        emulated_size,
    } = input;
    BrowserTab {
        tab_id,
        workspace_id,
        host_client_id: host.clone(),
        url,
        requested_url,
        title,
        owner_agent_id,
        owner_agent_name,
        visibility,
        emulated_size,
        created_at: now.clone(),
        updated_at: now,
    }
}

fn map_row(r: &SqliteRow) -> Result<StoredTab> {
    let width: Option<i64> = r.get("emulated_width");
    let height: Option<i64> = r.get("emulated_height");
    let emulated_size = match (width, height) {
        (Some(w), Some(h)) => Some(BrowserTabSize {
            width: u32::try_from(w)
                .map_err(|e| Error::Internal(format!("browser tab width out of range: {e}")))?,
            height: u32::try_from(h)
                .map_err(|e| Error::Internal(format!("browser tab height out of range: {e}")))?,
        }),
        _ => None,
    };
    let visibility: String = r.get("visibility");
    let closed_at: Option<String> = r.get("closed_at");
    Ok(StoredTab {
        tab: BrowserTab {
            tab_id: r.get("tab_id"),
            workspace_id: WorkspaceId(r.get("workspace_id")),
            host_client_id: ClientId(r.get("host_client_id")),
            url: r.get("url"),
            requested_url: r.get("requested_url"),
            title: r.get("title"),
            owner_agent_id: r.get::<Option<String>, _>("owner_agent_id").map(AgentId),
            owner_agent_name: r.get("owner_agent_name"),
            visibility: BrowserTabVisibility::parse(&visibility),
            emulated_size,
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        },
        closed: closed_at.is_some(),
    })
}

#[cfg(test)]
mod tests;
