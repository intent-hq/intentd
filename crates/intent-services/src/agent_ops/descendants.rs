//! `ws.agent.stop` / `ws.agent.delete`: a parent agent ends one of its own
//! sub-agents (direct or deeper) together with that sub-agent's whole
//! subtree. `stop` retires the subtree (the conversations stay and the user
//! can restore them); `delete` removes it. Both end each agent where it
//! runs: a local provider is killed, a remote agent gets its final stop on
//! its machine, and an E2B sandbox started for the agent is deleted.

use intent_core::{AgentId, Error, Result};
use intent_store::AgentSessionSummary;
use serde_json::{json, Value};

use crate::Services;

/// Longest parent chain walked to prove ancestry. Delegation depth is
/// capped far below this; the bound only stops a corrupt chain.
const MAX_ANCESTRY_HOPS: usize = 64;

impl Services {
    /// End `target` and its descendants for `caller`, which must be a
    /// strict ancestor of `target`. Agents already retired (for `stop`) or
    /// already gone are skipped. Per-agent failures are reported in
    /// `failed` and do not stop the rest.
    pub(crate) async fn agent_end_descendant_op(
        &self,
        caller: AgentId,
        target: AgentId,
        delete: bool,
        reason: Option<String>,
    ) -> Result<Value> {
        let verb = if delete { "delete" } else { "stop" };
        if caller == target {
            return Err(Error::InvalidParams(format!(
                "cannot {verb} yourself: ws.agent.{verb} acts on your sub-agents; use ws.agent.retire to retire yourself"
            )));
        }
        let session = self
            .store
            .get_agent_session_summary(&target)
            .await
            .map_err(|_| Error::NotFound(format!("agent session {target}")))?;
        if !self.is_ancestor(&caller, &session).await? {
            return Err(Error::InvalidParams(format!(
                "cannot {verb} {target}: it is not one of your sub-agents. You can {verb} only agents you created (and their sub-agents)."
            )));
        }
        let reason = reason.unwrap_or_else(|| format!("stopped by parent agent {caller}"));
        let mut ended = Vec::new();
        let mut failed = Vec::new();
        // The target first: its retire mark fences its own further turns,
        // so it cannot start sub-agents while its subtree is being ended.
        match self.stop_one_descendant(&session, &reason).await {
            Ok(true) if !delete => ended.push(target.0.clone()),
            Ok(_) => {}
            Err(e) => {
                failed.push(json!({ "agentId": target.0, "error": e.to_string() }));
                return Ok(end_result(delete, &target, &ended, &failed));
            }
        }
        // Then its descendants, deepest first, read again until no new
        // one appears (one started just before the fence).
        let mut seen = std::collections::HashSet::new();
        loop {
            let mut subtree = self.collect_retire_descendants(&target).await?;
            subtree.retain(|m| seen.insert(m.id.clone()));
            if subtree.is_empty() {
                break;
            }
            subtree.reverse();
            for member in &subtree {
                let id = member.id.clone();
                let outcome = if delete {
                    self.agent_delete_op(id.clone(), None).await.map(|_| true)
                } else {
                    self.stop_one_descendant(member, &reason).await
                };
                match outcome {
                    Ok(true) => ended.push(id.0),
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(caller = %caller, agent = %id, error = %e, "ending a sub-agent failed");
                        failed.push(json!({ "agentId": id.0, "error": e.to_string() }));
                    }
                }
            }
        }
        if delete {
            // A descendant that could not be deleted keeps its (retired)
            // parent row, so the caller can still reach it and retry.
            if failed.is_empty() {
                self.agent_delete_op(target.clone(), None).await?;
                ended.push(target.0.clone());
            } else {
                failed.push(json!({
                    "agentId": target.0,
                    "error": "kept (stopped) because a sub-agent of it could not be deleted; retry",
                }));
            }
        }
        Ok(end_result(delete, &target, &ended, &failed))
    }
    /// Retire one subtree member and end its runtime. `Ok(false)` when it
    /// was already retired or is gone.
    async fn stop_one_descendant(
        &self,
        member: &AgentSessionSummary,
        reason: &str,
    ) -> Result<bool> {
        let Ok(fresh) = self.store.get_agent_session_summary(&member.id).await else {
            return Ok(false);
        };
        if fresh.retired_at.is_some() {
            return Ok(false);
        }
        let transitioned = self.retire_one_session(&fresh, Some(reason)).await?;
        // The retire mark fences new turns; this ends the running one
        // (`retire_one_session` aborts the worker only for wire callers).
        if let Some(manager) = self.agent_manager() {
            manager.retire(&member.id).await;
        }
        Ok(transitioned.is_some())
    }

    /// True when `ancestor` is on `session`'s parent chain.
    async fn is_ancestor(&self, ancestor: &AgentId, session: &AgentSessionSummary) -> Result<bool> {
        let mut parent = session.parent_agent_id.clone();
        for _ in 0..MAX_ANCESTRY_HOPS {
            let Some(id) = parent else {
                return Ok(false);
            };
            if &id == ancestor {
                return Ok(true);
            }
            parent = match self.store.get_agent_session_summary(&id).await {
                Ok(row) => row.parent_agent_id.clone(),
                Err(Error::NotFound(_)) => None,
                Err(e) => return Err(e),
            };
        }
        Ok(false)
    }
}

/// The `ws.agent.stop` / `ws.agent.delete` result.
fn end_result(delete: bool, target: &AgentId, ended: &[String], failed: &[Value]) -> Value {
    let key = if delete { "deleted" } else { "stopped" };
    json!({
        "ok": failed.is_empty(),
        "success": failed.is_empty(),
        "agentId": target.0,
        key: ended,
        "failed": failed,
    })
}
