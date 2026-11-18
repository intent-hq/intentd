//! Daemon-owned browser tab registry (Postgres). Mirrors
//! `browser_tab_repo.rs`, including its process-local `displayed` overlay;
//! every mutation runs in one tenant write transaction.

use std::collections::HashSet;

use intent_core::{
    now_iso, AgentId, BrowserTab, BrowserTabInput, BrowserTabSize, BrowserTabSyncResult,
    BrowserTabUpsertOutcome, BrowserTabVisibility, ClientId, Error, Result, WorkspaceId,
};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, Row};

use super::{PgStore, WriteTx};
use crate::browser_tab_repo::{
    new_tab, reject_foreign_host, reject_tombstone, reject_workspace_move, DisplayedOverlay,
    StoredTab, COLUMNS,
};

/// `SQLite` orders text by bytes.
const ORDER: &str = "ORDER BY created_at COLLATE \"C\", tab_id COLLATE \"C\"";

fn failed(what: &str) -> impl Fn(sqlx::Error) -> Error + '_ {
    move |e| Error::Internal(format!("{what} failed: {e}"))
}

async fn commit(tx: WriteTx, what: &str) -> Result<()> {
    tx.commit()
        .await
        .map_err(|e| Error::Internal(format!("{what} commit failed: {e}")))
}

fn map_row(r: &PgRow, displayed: &DisplayedOverlay) -> Result<StoredTab> {
    let get = failed("read browser tab");
    let width: Option<i64> = r.try_get("emulated_width").map_err(&get)?;
    let height: Option<i64> = r.try_get("emulated_height").map_err(&get)?;
    let emulated_size = match (width, height) {
        (Some(w), Some(h)) => Some(BrowserTabSize {
            width: u32::try_from(w)
                .map_err(|e| Error::Internal(format!("browser tab width out of range: {e}")))?,
            height: u32::try_from(h)
                .map_err(|e| Error::Internal(format!("browser tab height out of range: {e}")))?,
        }),
        _ => None,
    };
    let visibility: String = r.try_get("visibility").map_err(&get)?;
    let closed_at: Option<String> = r.try_get("closed_at").map_err(&get)?;
    let tab_id: String = r.try_get("tab_id").map_err(&get)?;
    Ok(StoredTab {
        tab: BrowserTab {
            displayed: displayed.get(&tab_id),
            tab_id,
            workspace_id: WorkspaceId(r.try_get("workspace_id").map_err(&get)?),
            host_client_id: ClientId(r.try_get("host_client_id").map_err(&get)?),
            url: r.try_get("url").map_err(&get)?,
            requested_url: r.try_get("requested_url").map_err(&get)?,
            title: r.try_get("title").map_err(&get)?,
            owner_agent_id: r
                .try_get::<Option<String>, _>("owner_agent_id")
                .map_err(&get)?
                .map(AgentId),
            owner_agent_name: r.try_get("owner_agent_name").map_err(&get)?,
            visibility: BrowserTabVisibility::parse(&visibility),
            navigation_only: r.try_get::<i64, _>("navigation_only").map_err(&get)? != 0,
            emulated_size,
            created_at: r.try_get("created_at").map_err(&get)?,
            updated_at: r.try_get("updated_at").map_err(&get)?,
        },
        closed: closed_at.is_some(),
    })
}

impl PgStore {
    async fn open_tabs_where(
        &self,
        filter: &str,
        value: &str,
        what: &str,
    ) -> Result<Vec<BrowserTab>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM browser_tab \
             WHERE tenant_id = $1 AND {filter} = $2 AND closed_at IS NULL {ORDER}"
        )))
        .bind(self.tenant())
        .bind(value)
        .fetch_all(&mut *tx)
        .await
        .map_err(failed(what))?;
        tx.commit().await.map_err(failed(what))?;
        rows.iter()
            .map(|r| map_row(r, &self.browser_tab_displayed).map(|s| s.tab))
            .collect()
    }

    pub async fn list_browser_tabs(&self, id: &WorkspaceId) -> Result<Vec<BrowserTab>> {
        self.open_tabs_where("workspace_id", &id.0, "list browser tabs")
            .await
    }

    pub async fn list_browser_tabs_by_host(&self, host: &ClientId) -> Result<Vec<BrowserTab>> {
        self.open_tabs_where("host_client_id", &host.0, "list browser tabs by host")
            .await
    }

    pub async fn get_browser_tab(&self, tab_id: &str) -> Result<Option<BrowserTab>> {
        let mut tx = self.begin().await?;
        let stored = self.fetch_stored_tab(&mut tx, tab_id).await?;
        tx.commit().await.map_err(failed("get browser tab"))?;
        Ok(stored.filter(|s| !s.closed).map(|s| s.tab))
    }

    async fn fetch_stored_tab(
        &self,
        conn: &mut PgConnection,
        tab_id: &str,
    ) -> Result<Option<StoredTab>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM browser_tab WHERE tenant_id = $1 AND tab_id = $2"
        )))
        .bind(self.tenant())
        .bind(tab_id)
        .fetch_optional(conn)
        .await
        .map_err(failed("get browser tab"))?;
        row.as_ref()
            .map(|r| map_row(r, &self.browser_tab_displayed))
            .transpose()
    }

    /// Insert a new row or refresh the host-reported fields of an open row;
    /// never re-homes, re-dates or revives one (see `write_tab` on `SQLite`).
    async fn write_tab(&self, tx: &mut WriteTx, tab: &BrowserTab) -> Result<()> {
        sqlx::query(
            "INSERT INTO browser_tab (tenant_id, tab_id, workspace_id, host_client_id, url, \
             requested_url, title, owner_agent_id, owner_agent_name, visibility, emulated_width, \
             emulated_height, created_at, updated_at, navigation_only, closed_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NULL) \
             ON CONFLICT (tenant_id, tab_id) DO UPDATE SET \
             workspace_id = excluded.workspace_id, \
             url = excluded.url, requested_url = excluded.requested_url, title = excluded.title, \
             owner_agent_id = excluded.owner_agent_id, \
             owner_agent_name = excluded.owner_agent_name, visibility = excluded.visibility, \
             emulated_width = excluded.emulated_width, \
             emulated_height = excluded.emulated_height, updated_at = excluded.updated_at, \
             navigation_only = GREATEST(browser_tab.navigation_only, excluded.navigation_only)",
        )
        .bind(self.tenant())
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
        .bind(i64::from(tab.navigation_only))
        .execute(&mut **tx)
        .await
        .map_err(failed("write browser tab"))?;
        Ok(())
    }

    async fn rehome_tab(&self, tx: &mut WriteTx, tab: &BrowserTab) -> Result<()> {
        sqlx::query(
            "UPDATE browser_tab SET host_client_id = $2, owner_agent_id = $3, updated_at = $4 \
             WHERE tenant_id = $1 AND tab_id = $5 AND closed_at IS NULL",
        )
        .bind(self.tenant())
        .bind(&tab.host_client_id.0)
        .bind(tab.owner_agent_id.as_ref().map(|a| a.0.as_str()))
        .bind(&tab.updated_at)
        .bind(&tab.tab_id)
        .execute(&mut **tx)
        .await
        .map_err(failed("rehome browser tab"))?;
        Ok(())
    }

    async fn rehome_rows(
        &self,
        tx: &mut WriteTx,
        rows: &[PgRow],
        new_host: &ClientId,
    ) -> Result<Vec<(BrowserTab, serde_json::Value)>> {
        let mut moved = Vec::with_capacity(rows.len());
        let now = now_iso();
        for row in rows {
            let mut tab = map_row(row, &self.browser_tab_displayed)?.tab;
            let mut changes = serde_json::Map::new();
            changes.insert(
                "hostClientId".to_string(),
                serde_json::Value::String(new_host.0.clone()),
            );
            if tab.displayed.take().is_some() {
                changes.insert("displayed".to_string(), serde_json::Value::Null);
            }
            tab.host_client_id = new_host.clone();
            tab.updated_at.clone_from(&now);
            self.rehome_tab(tx, &tab).await?;
            moved.push((tab, serde_json::Value::Object(changes)));
        }
        Ok(moved)
    }

    async fn delete_tab_row(&self, tx: &mut WriteTx, tab_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM browser_tab WHERE tenant_id = $1 AND tab_id = $2")
            .bind(self.tenant())
            .bind(tab_id)
            .execute(&mut **tx)
            .await
            .map_err(failed("delete browser tab"))?;
        Ok(())
    }

    pub async fn upsert_browser_tab(
        &self,
        host: &ClientId,
        input: BrowserTabInput,
    ) -> Result<BrowserTabUpsertOutcome> {
        let mut tx = self.begin_write().await?;
        let outcome = match self.fetch_stored_tab(&mut tx, &input.tab_id).await? {
            None => {
                let tab = new_tab(host, input);
                self.write_tab(&mut tx, &tab).await?;
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
                    self.write_tab(&mut tx, &updated).await?;
                    BrowserTabUpsertOutcome::Updated {
                        tab: updated,
                        changes: serde_json::Value::Object(changes),
                    }
                }
            }
        };
        commit(tx, "upsert browser tab").await?;
        if !matches!(outcome, BrowserTabUpsertOutcome::Unchanged(_)) {
            self.browser_tab_displayed.record(outcome.tab());
        }
        Ok(outcome)
    }

    pub async fn remove_browser_tab(
        &self,
        host: &ClientId,
        tab_id: &str,
    ) -> Result<Option<BrowserTab>> {
        let mut tx = self.begin_write().await?;
        let Some(stored) = self.fetch_stored_tab(&mut tx, tab_id).await? else {
            return Ok(None);
        };
        reject_foreign_host(&stored.tab, host)?;
        self.delete_tab_row(&mut tx, tab_id).await?;
        commit(tx, "remove browser tab").await?;
        self.browser_tab_displayed.forget(tab_id);
        Ok((!stored.closed).then_some(stored.tab))
    }

    pub async fn close_browser_tab(&self, tab_id: &str) -> Result<Option<BrowserTab>> {
        let mut tx = self.begin_write().await?;
        let Some(StoredTab { tab, closed: false }) = self.fetch_stored_tab(&mut tx, tab_id).await?
        else {
            return Ok(None);
        };
        sqlx::query("UPDATE browser_tab SET closed_at = $2 WHERE tenant_id = $1 AND tab_id = $3")
            .bind(self.tenant())
            .bind(now_iso())
            .bind(tab_id)
            .execute(&mut *tx)
            .await
            .map_err(failed("close browser tab"))?;
        commit(tx, "close browser tab").await?;
        self.browser_tab_displayed.forget(tab_id);
        Ok(Some(tab))
    }

    pub async fn reassign_browser_tab_host(
        &self,
        tab_id: &str,
        new_host: &ClientId,
        owner_agent_id: Option<&AgentId>,
    ) -> Result<Option<(BrowserTab, serde_json::Value)>> {
        let owner_agent_id = owner_agent_id.cloned();
        let mut tx = self.begin_write().await?;
        let Some(StoredTab { tab, closed: false }) = self.fetch_stored_tab(&mut tx, tab_id).await?
        else {
            return Ok(None);
        };
        let mut changes = serde_json::Map::new();
        let host_changes = tab.host_client_id != *new_host;
        if tab.navigation_only && host_changes {
            return Err(Error::InvalidParams(
                "native browser tabs cannot move between clients".into(),
            ));
        }
        if host_changes {
            changes.insert(
                "hostClientId".to_string(),
                serde_json::Value::String(new_host.0.clone()),
            );
            if tab.displayed.is_some() {
                changes.insert("displayed".to_string(), serde_json::Value::Null);
            }
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
        if host_changes {
            updated.displayed = None;
        }
        if owner_agent_id.is_some() {
            updated.owner_agent_id = owner_agent_id;
        }
        updated.updated_at = now_iso();
        self.rehome_tab(&mut tx, &updated).await?;
        commit(tx, "reassign browser tab host").await?;
        if host_changes {
            self.browser_tab_displayed.forget(tab_id);
        }
        Ok(Some((updated, serde_json::Value::Object(changes))))
    }

    pub async fn reassign_claimed_browser_tabs(
        &self,
        id: &WorkspaceId,
        new_host: &ClientId,
    ) -> Result<Vec<(BrowserTab, serde_json::Value)>> {
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM browser_tab \
             WHERE tenant_id = $1 AND workspace_id = $2 AND closed_at IS NULL \
             AND owner_agent_id IS NOT NULL AND navigation_only = 0 AND host_client_id <> $3 {ORDER}"
        )))
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&new_host.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(failed("list claimed browser tabs"))?;
        let moved = self.rehome_rows(&mut tx, &rows, new_host).await?;
        commit(tx, "reassign claimed browser tabs").await?;
        self.browser_tab_displayed
            .forget_all(moved.iter().map(|(t, _)| t.tab_id.as_str()));
        Ok(moved)
    }

    pub async fn reassign_browser_tabs_from_hosts(
        &self,
        new_host: &ClientId,
        old_hosts: &[ClientId],
        still_movable: &(dyn Fn(&ClientId) -> bool + Send + Sync),
    ) -> Result<Vec<(BrowserTab, serde_json::Value)>> {
        if old_hosts.is_empty() {
            return Ok(Vec::new());
        }
        let mut tx = self.begin_write().await?;
        let old_hosts: Vec<String> = old_hosts
            .iter()
            .filter(|h| still_movable(h))
            .map(|h| h.0.clone())
            .collect();
        if old_hosts.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM browser_tab \
             WHERE tenant_id = $1 AND closed_at IS NULL AND navigation_only = 0 \
             AND host_client_id = ANY($2) {ORDER}"
        )))
        .bind(self.tenant())
        .bind(&old_hosts)
        .fetch_all(&mut *tx)
        .await
        .map_err(failed("list superseded browser tabs"))?;
        let moved = self.rehome_rows(&mut tx, &rows, new_host).await?;
        commit(tx, "rehome superseded browser tabs").await?;
        self.browser_tab_displayed
            .forget_all(moved.iter().map(|(t, _)| t.tab_id.as_str()));
        Ok(moved)
    }

    pub async fn sync_browser_tabs(
        &self,
        host: &ClientId,
        snapshot: Vec<BrowserTabInput>,
    ) -> Result<BrowserTabSyncResult> {
        let mut tx = self.begin_write().await?;
        let mut result = BrowserTabSyncResult::default();
        let mut seen: HashSet<String> = HashSet::new();
        let mut purged: Vec<String> = Vec::new();
        for input in snapshot {
            if !seen.insert(input.tab_id.clone()) {
                continue;
            }
            match self.fetch_stored_tab(&mut tx, &input.tab_id).await? {
                None => {
                    let tab = new_tab(host, input);
                    self.write_tab(&mut tx, &tab).await?;
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
                    self.write_tab(&mut tx, &updated).await?;
                    result
                        .updated
                        .push((updated, serde_json::Value::Object(changes)));
                }
                Some(StoredTab { tab, .. }) => result.drop.push(tab.tab_id),
            }
        }
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM browser_tab WHERE tenant_id = $1 AND host_client_id = $2 {ORDER}"
        )))
        .bind(self.tenant())
        .bind(&host.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(failed("list browser tabs by host"))?;
        for row in &rows {
            let stored = map_row(row, &self.browser_tab_displayed)?;
            if seen.contains(&stored.tab.tab_id) {
                continue;
            }
            self.delete_tab_row(&mut tx, &stored.tab.tab_id).await?;
            purged.push(stored.tab.tab_id.clone());
            if !stored.closed {
                result.closed.push(stored.tab);
            }
        }
        commit(tx, "sync browser tabs").await?;
        for tab in result
            .opened
            .iter()
            .chain(result.updated.iter().map(|(t, _)| t))
        {
            self.browser_tab_displayed.record(tab);
        }
        for tab_id in &purged {
            self.browser_tab_displayed.forget(tab_id);
        }
        Ok(result)
    }
}
