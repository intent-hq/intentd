//! Bounded active key selection precedes payload projection.
use crate::Store;
use intent_core::{
    script_lists::{self, ScriptListOptions},
    AgentId, Error, Result, WorkspaceId,
};
use serde_json::{json, Value};
fn db(e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("script list storage: {e}"))
}
impl Store {
    /// Select at most limit+1 eligible keys and project only the returned page.
    /// # Errors
    /// Rejects invalid cursors; database errors fail closed.
    pub async fn script_summary_page(
        &self,
        ws: &WorkspaceId,
        owner: Option<&AgentId>,
        monitor: bool,
        options: &ScriptListOptions,
    ) -> Result<Value> {
        let method = if monitor {
            "scriptMonitor.list"
        } else {
            "script.list"
        };
        let scope = script_lists::scope(method, ws.as_str(), owner.map(AgentId::as_str));
        let boundary = options.boundary(&scope)?;
        let (time, id) = boundary.as_ref().map_or((None, None), |(time, id)| {
            (Some(time.as_str()), Some(id.as_str()))
        });
        let sql = if monitor {
            "SELECT created_at,id FROM script_monitor WHERE workspace_id=? AND state='active' AND (? IS NULL OR agent_id=?) AND (? IS NULL OR (created_at,id)<(?,?)) ORDER BY created_at DESC,id DESC LIMIT ?"
        } else {
            "SELECT created_at,id FROM script WHERE workspace_id=? AND archived_at IS NULL AND (? IS NULL OR ? IS NULL) AND (? IS NULL OR (created_at,id)<(?,?)) ORDER BY created_at DESC,id DESC LIMIT ?"
        };
        let mut keys: Vec<(String, String)> = sqlx::query_as(sql)
            .bind(ws.as_str())
            .bind(owner.map(AgentId::as_str))
            .bind(owner.map(AgentId::as_str))
            .bind(time)
            .bind(time)
            .bind(id)
            .bind(i64::try_from(options.limit + 1).map_err(db)?)
            .fetch_all(self.read_pool())
            .await
            .map_err(db)?;
        let more = keys.len() > options.limit;
        keys.truncate(options.limit);
        let ids = json!(keys.iter().map(|(_, id)| id).collect::<Vec<_>>()).to_string();
        // SQL strips large fields and bounds character prefixes before Rust materialization.
        let projection = if monitor {
            "SELECT json_object('monitorId',id,'workspaceId',workspace_id,'agentId',agent_id,'scriptId',script_id,'runId',run_id,'scriptName',json_extract(row_json,'$.scriptName'),'mode',json_extract(row_json,'$.mode'),'state','active','createdAt',created_at,'expiresAt',json_extract(row_json,'$.expiresAt'),'lineCount',json_extract(row_json,'$.lineCount'),'outputPatternPreview',substr(json_extract(row_json,'$.outputPattern'),1,160),'patternBytes',length(CAST(json_extract(row_json,'$.outputPattern') AS BLOB))) FROM script_monitor WHERE workspace_id=? AND state='active' AND (? IS NULL OR agent_id=?) AND id IN (SELECT value FROM json_each(?)) ORDER BY created_at DESC,id DESC"
        } else {
            "SELECT json_object('id',id,'workspaceId',workspace_id,'name',name,'mode',mode,'source',source,'createdAt',created_at,'updatedAt',updated_at,'category',category,'autoStart',auto_start,'purpose',purpose,'commandPreview',substr(command,1,160),'commandBytes',length(CAST(command AS BLOB)),'lastRun',CASE WHEN last_run IS NULL THEN NULL ELSE json_remove(json(last_run),'$.error') END,'lastError',substr(json_extract(last_run,'$.error'),1,256),'lastErrorBytes',length(CAST(json_extract(last_run,'$.error') AS BLOB))) FROM script WHERE workspace_id=? AND archived_at IS NULL AND id IN (SELECT value FROM json_each(?)) ORDER BY created_at DESC,id DESC"
        };
        let query = sqlx::query_scalar(projection).bind(ws.as_str());
        let query = if monitor {
            query
                .bind(owner.map(AgentId::as_str))
                .bind(owner.map(AgentId::as_str))
        } else {
            query
        };
        let rows: Vec<String> = query
            .bind(ids)
            .fetch_all(self.read_pool())
            .await
            .map_err(db)?;
        let rows: Vec<Value> = rows
            .into_iter()
            .map(|raw| {
                let mut row: Value = serde_json::from_str(&raw).map_err(db)?;
                script_lists::strip_nulls(&mut row);
                let (field, bytes, flag) = if monitor {
                    (
                        "outputPatternPreview",
                        "patternBytes",
                        "outputPatternTruncated",
                    )
                } else {
                    ("commandPreview", "commandBytes", "commandTruncated")
                };
                if let Some(text) = row[field].as_str() {
                    let (prefix, _) = script_lists::prefix(text, 160);
                    let prefix = prefix.to_owned();
                    row[flag] =
                        json!(row[bytes].as_u64().unwrap_or_default() > prefix.len() as u64);
                    row[field] = json!(prefix);
                }
                row.as_object_mut()
                    .ok_or_else(|| db("invalid projection object"))?
                    .remove(bytes);
                if !monitor {
                    if let Some(n) = row["autoStart"].as_i64() {
                        row["autoStart"] = json!(n != 0);
                    }
                    if let Some(error) = row["lastError"].as_str() {
                        let (prefix, _) = script_lists::prefix(error, 256);
                        let prefix = prefix.to_owned();
                        if row["lastErrorBytes"].as_u64().unwrap_or_default() > prefix.len() as u64
                        {
                            row["lastRun"]["errorTruncated"] = json!(true);
                        }
                        row["lastRun"]["error"] = json!(prefix);
                    }
                    row.as_object_mut()
                        .ok_or_else(|| db("invalid projection object"))?
                        .remove("lastError");
                    row.as_object_mut()
                        .ok_or_else(|| db("invalid projection object"))?
                        .remove("lastErrorBytes");
                }
                Ok(row)
            })
            .collect::<Result<_>>()?;
        let mut result = json!({"view":"summary"});
        result[if monitor { "monitors" } else { "scripts" }] = json!(rows);
        if more {
            if let Some((time, id)) = keys.last() {
                result["nextCursor"] = json!(script_lists::cursor(&scope, time, id));
            }
        }
        Ok(result)
    }
}
