//! Conversation goal service path (PROTOCOL §5.5 "Conversation goal",
//! intent-hq/intent#9646): the `/goal` command and `agent.setGoal`.
//!
//! Both share one write path: validate, compare-and-set the stored goal
//! through the narrow store writer (never advancing `updatedAt`), persist one
//! transcript-only `goal_command` notice row outside any turn, and emit
//! `agent:updated { agentId, goal }` when the stored goal changed. Nothing
//! here wakes the agent, touches its queue, attention, unread or turn state,
//! or revives an archived workspace. The per-turn prompt block is rendered
//! by the harness in the turn envelope ([`Services::agent_conversation_goal`]).

use intent_core::conversation_goal::{self, GoalAction, GoalError};
use intent_core::events::AGENT_UPDATED;
use intent_core::{AgentId, Error, Result, WorkspaceId};
use serde_json::{json, Value};

use crate::Services;

/// Bound on compare-and-set retries when concurrent goal writers keep
/// winning the race; each retry re-reads the stored goal.
const MAX_GOAL_WRITE_ATTEMPTS: usize = 8;

/// `NotFound` from a session lookup or write is the contract's
/// `agent not found` (the agent is unknown or was deleted mid-call).
fn agent_not_found(e: Error) -> Error {
    match e {
        Error::NotFound(_) => Error::ConversationGoal(GoalError::AgentNotFound),
        other => other,
    }
}

impl Services {
    /// The agent's workspace and stored goal after the caller gates.
    /// Membership is checked on the same terms as `agent.sendMessage` /
    /// `agent.queueMessage`; an explicit `workspace_id` must be the agent's
    /// own. An unknown or deleted agent is `agent not found`, a retired one
    /// `agent is retired`. An agent caller is refused outright: one agent
    /// cannot set another agent's goal (the RPC seam already refuses every
    /// agent-origin route; this is the backstop).
    async fn goal_target(
        &self,
        workspace_id: Option<&WorkspaceId>,
        agent_id: &AgentId,
    ) -> Result<(WorkspaceId, Option<String>)> {
        if matches!(
            intent_core::caller::current_caller(),
            Some(intent_core::caller::Caller::Agent { .. })
        ) {
            return Err(Error::Forbidden(
                "agents cannot set a conversation goal".to_string(),
            ));
        }
        match workspace_id {
            Some(workspace_id) => self
                .require_agent_member_in(agent_id, workspace_id)
                .await
                .map_err(agent_not_found)?,
            None => self
                .require_agent_member(agent_id)
                .await
                .map_err(agent_not_found)?,
        }
        let session = self
            .store
            .get_agent_session_summary(agent_id)
            .await
            .map_err(agent_not_found)?;
        if workspace_id.is_some_and(|ws| *ws != session.workspace_id) {
            return Err(Error::ConversationGoal(GoalError::AgentNotFound));
        }
        if session.retired_at.is_some() {
            return Err(Error::ConversationGoal(GoalError::AgentRetired));
        }
        // A retired workspace is read-only (§5.1). `agent.sendMessage` is
        // exempt from the RPC retired gate because a user message revives
        // the workspace, but a `/goal` command revives nothing, so it is
        // refused here like `agent.setGoal` and `agent.queueMessage` are.
        let workspace = self.store.get_workspace(&session.workspace_id).await?;
        if workspace.retired_at.is_some() {
            return Err(crate::retire::retired_error(&session.workspace_id));
        }
        Ok((session.workspace_id.clone(), session.goal.clone()))
    }

    /// Move the stored goal to `next`, starting from the observed `current`.
    /// Returns the action and the goal after it. A compare-and-set lost to a
    /// concurrent writer re-reads and recomputes, so the action always
    /// describes the write that actually landed.
    async fn write_goal(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        mut current: Option<String>,
        next: Option<String>,
    ) -> Result<(GoalAction, Option<String>)> {
        for _ in 0..MAX_GOAL_WRITE_ATTEMPTS {
            let action = conversation_goal::write_action(current.as_deref(), next.as_deref());
            if !action.writes() {
                return Ok((action, current));
            }
            if self
                .store
                .set_agent_goal(workspace_id, agent_id, current.as_deref(), next.as_deref())
                .await
                .map_err(agent_not_found)?
            {
                self.invalidate_agent_list_cache(workspace_id);
                self.publish_agent_mutation_event(
                    workspace_id,
                    agent_id,
                    AGENT_UPDATED,
                    json!({ "agentId": agent_id.0, "goal": next }),
                )
                .await;
                return Ok((action, next));
            }
            let stored = self
                .store
                .get_agent_session_summary(agent_id)
                .await
                .map_err(agent_not_found)?;
            current.clone_from(&stored.goal);
        }
        Err(Error::Internal(
            "the conversation goal kept changing concurrently; try again".to_string(),
        ))
    }

    /// Persist the transcript-only `goal_command` notice row and publish it
    /// on `agent:message`. Written outside any turn through the plain
    /// append: a `system` row moves no preview, `lastMessageRole`,
    /// `updatedAt`, unread marker or attention state. Returns the row id.
    async fn persist_goal_notice(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        action: GoalAction,
        goal: Option<&str>,
    ) -> Result<String> {
        let principal = crate::principal_ops::attributed_caller_id();
        let content = conversation_goal::notice_content(action, goal);
        let metadata = conversation_goal::notice_metadata(
            action,
            goal,
            principal.as_ref().map(|p| p.0.as_str()),
        );
        // The store allocates `seq` inside the insert, so this out-of-turn
        // append can never collide with a finishing turn or a user send.
        let message = self
            .store
            .append_agent_message_with_metadata(
                agent_id,
                "system",
                &content,
                Some(&metadata),
                &intent_core::now_iso(),
            )
            .await?;
        self.invalidate_agent_list_cache(workspace_id);
        self.publish_agent_message_events(workspace_id, agent_id, &message, None)
            .await;
        Ok(message.id)
    }

    /// The notice row for a goal write that already landed. A failure here
    /// cannot undo the write, so it is logged and the call still succeeds
    /// with the stored state (no `noticeMessageId`): the client then clears
    /// its composer and the `agent:updated` it received stays true.
    async fn notice_after_write(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        action: GoalAction,
        goal: Option<&str>,
    ) -> Option<String> {
        match self
            .persist_goal_notice(workspace_id, agent_id, action, goal)
            .await
        {
            Ok(id) => Some(id),
            Err(e) => {
                tracing::warn!(
                    agent_id = %agent_id,
                    action = action.as_str(),
                    error = %e,
                    "conversation goal stored but its notice row could not be written"
                );
                None
            }
        }
    }

    /// `agent.setGoal`: set, replace or remove (`None` / empty after trim)
    /// the conversation goal. `unchanged` writes nothing (no row, no event,
    /// no `noticeMessageId`).
    pub(crate) async fn agent_set_goal_op(
        &self,
        workspace_id: Option<WorkspaceId>,
        agent_id: AgentId,
        goal: Option<String>,
    ) -> Result<Value> {
        let next = conversation_goal::normalize(goal.as_deref())?;
        let (workspace_id, current) = self.goal_target(workspace_id.as_ref(), &agent_id).await?;
        let (action, goal) = self
            .write_goal(&workspace_id, &agent_id, current, next)
            .await?;
        let mut result = json!({ "action": action.as_str(), "goal": goal });
        if action.writes() {
            if let Some(id) = self
                .notice_after_write(&workspace_id, &agent_id, action, goal.as_deref())
                .await
            {
                result["noticeMessageId"] = json!(id);
            }
        }
        Ok(result)
    }

    /// A recognized `/goal` command: an empty statement shows the current
    /// goal, any other sets or replaces it. Always persists one notice row.
    pub(crate) async fn agent_goal_command_op(
        &self,
        workspace_id: Option<WorkspaceId>,
        agent_id: AgentId,
        statement: String,
    ) -> Result<Value> {
        let next = conversation_goal::normalize(Some(&statement))?;
        let (workspace_id, current) = self.goal_target(workspace_id.as_ref(), &agent_id).await?;
        let (action, goal) = match next {
            None => (GoalAction::Shown, current),
            Some(next) => {
                self.write_goal(&workspace_id, &agent_id, current, Some(next))
                    .await?
            }
        };
        let notice_id = if action.writes() {
            self.notice_after_write(&workspace_id, &agent_id, action, goal.as_deref())
                .await
        } else {
            // Nothing changed: a failed notice fails the command, and a
            // retry is harmless.
            Some(
                self.persist_goal_notice(&workspace_id, &agent_id, action, goal.as_deref())
                    .await?,
            )
        };
        let mut command = json!({ "name": "goal", "action": action.as_str(), "goal": goal });
        if let Some(id) = notice_id {
            command["noticeMessageId"] = json!(id);
        }
        Ok(json!({ "success": true, "queued": false, "command": command }))
    }

    /// The agent's stored conversation goal for the per-turn prompt block,
    /// or `None` when unset or unreadable. Metadata-only read (no transcript
    /// load), like the role reminder.
    pub(crate) async fn agent_conversation_goal(&self, agent_id: &AgentId) -> Option<String> {
        self.store
            .get_agent_session_summary(agent_id)
            .await
            .ok()?
            .goal
            .clone()
    }
}
