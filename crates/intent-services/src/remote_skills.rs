//! A remote agent's skills are its machine's (monorepo
//! `docs/protocol/methods/agent-aux.md` § Skills of a remote agent): the
//! `skill.list` answer for its `agentId`, `skills:changed` for its reported
//! catalog, and the notice for a `/name` its machine does not have.

use intent_core::events::SKILLS_CHANGED;
use intent_core::node_link::MachineSkill;
use intent_core::remote_launch::RemotePlacementView;
use intent_core::{now_iso, AgentId, Error, Result, WorkspaceId};
use intent_store::NewEvent;
use serde_json::{json, Value};

use crate::feature_ports::MachineSkills;
use crate::Services;

/// `unavailable` when the machine cannot be asked.
const MACHINE_OFFLINE: &str = "machine-offline";
/// `unavailable` when the machine's link is older than nodeProtocol 12.
const MACHINE_UPDATE_REQUIRED: &str = "machine-update-required";

/// The skill a message asks for: its first word when that is `/name`, with
/// `name` free of `/` and whitespace.
pub(crate) fn leading_skill_name(text: &str) -> Option<&str> {
    let rest = text.trim_start().strip_prefix('/')?;
    let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let name = &rest[..end];
    (!name.is_empty() && !name.contains('/')).then_some(name)
}

impl Services {
    /// `skill.list` with `agentId`: `{ skills, stale?, unavailable? }`,
    /// name-sorted, before paging. A backend agent gets the workspace's list.
    ///
    /// # Errors
    ///
    /// As `skill.list` for the workspace; [`Error::InvalidParams`] when
    /// `agent_id` is not an agent of `workspace_id`.
    pub async fn agent_skill_list(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> Result<Value> {
        self.require_member(&workspace_id).await?;
        let not_here =
            || Error::InvalidParams(format!("agent {agent_id} is not in this workspace"));
        let session = match self.store.get_agent_session_summary(&agent_id).await {
            Ok(session) if session.workspace_id == workspace_id => session,
            Ok(_) | Err(Error::NotFound(_)) => return Err(not_here()),
            Err(error) => return Err(error),
        };
        let Some(view) = RemotePlacementView::from_metadata(session.metadata.as_ref()) else {
            let listed = intent_core::WorkspaceApi::skill_list(self, workspace_id).await?;
            return Ok(json!({ "skills": listed }));
        };
        let port = self.ports.remote_launch();
        if let Some(reported) = view.skills {
            let mut result = json!({ "skills": sorted(reported) });
            if port.machine_skills(self, &agent_id, false).await == MachineSkills::Offline {
                result["stale"] = json!(true);
            }
            return Ok(result);
        }
        let project = self.backend_project_skills(&workspace_id).await?;
        Ok(match port.machine_skills(self, &agent_id, true).await {
            MachineSkills::Listed(user) => {
                let mut by_name: std::collections::BTreeMap<String, MachineSkill> = user
                    .into_iter()
                    .map(|skill| (skill.name.clone(), skill))
                    .collect();
                for skill in project {
                    by_name.insert(skill.name.clone(), skill);
                }
                json!({ "skills": by_name.into_values().collect::<Vec<_>>() })
            }
            // An older machine gives its agent no catalog: offer nothing.
            MachineSkills::UpdateRequired => {
                json!({ "skills": [], "unavailable": MACHINE_UPDATE_REQUIRED })
            }
            MachineSkills::Offline | MachineSkills::Reachable => {
                json!({ "skills": sorted(project), "unavailable": MACHINE_OFFLINE })
            }
        })
    }

    /// The project skills of the workspace's checkout on the backend: what
    /// the agent's checkout has at its base commit.
    async fn backend_project_skills(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<MachineSkill>> {
        let ws = self.store.get_workspace(workspace_id).await?;
        let Some(path) = crate::git_ops::worktree_path(&ws) else {
            return Ok(Vec::new());
        };
        let skills = crate::skills::discover_skills(&path.to_string_lossy()).await;
        Ok(skills
            .into_iter()
            .filter(|skill| skill.scope == "project")
            .map(crate::skills::SkillMetadata::into_machine)
            .collect())
    }

    /// The machine reported `skills` for a remote agent: keep them with its
    /// placement and announce a change. The caller stores the view; this
    /// emits `skills:changed` with the agent.
    pub async fn publish_agent_skills_changed(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) {
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: now_iso(),
            event_type: SKILLS_CHANGED.to_string(),
            actor: crate::system_actor(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({ "workspaceId": workspace_id.as_str(), "agentId": agent_id.as_str() }),
        };
        crate::publish_event(self.event_bus.as_ref(), event).await;
    }

    /// A message to a remote agent that starts with `/name` its machine did
    /// not report gets a `skill_unavailable` notice. Nothing before the
    /// machine reported a catalog, and nothing for a backend agent.
    pub(crate) async fn note_unavailable_skill(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        text: &str,
    ) {
        let Some(name) = leading_skill_name(text) else {
            return;
        };
        let Ok(session) = self.store.get_agent_session_summary(agent_id).await else {
            return;
        };
        let Some(view) = RemotePlacementView::from_metadata(session.metadata.as_ref()) else {
            return;
        };
        let Some(reported) = view.skills else {
            return;
        };
        if reported.iter().any(|skill| skill.name == name) {
            return;
        }
        let machine = view.node_name;
        let content = json!([{
            "type": "text",
            "text": format!(
                "**/{name}** isn't on {machine}. Add it to ~/.agents/skills or ~/.claude/skills \
                 there, or run this agent on the backend."
            ),
        }]);
        let metadata = json!({ "type": "skill_unavailable", "skill": name, "machine": machine });
        match self
            .store
            .append_agent_message_with_metadata(
                agent_id,
                "system",
                &content,
                Some(&metadata),
                &now_iso(),
            )
            .await
        {
            Ok(message) => {
                self.invalidate_agent_list_cache(workspace_id);
                self.publish_agent_message_events(workspace_id, agent_id, &message, None)
                    .await;
            }
            Err(error) => {
                tracing::warn!(agent = %agent_id, %error, "could not record the missing-skill notice");
            }
        }
    }
}

fn sorted(mut skills: Vec<MachineSkill>) -> Vec<MachineSkill> {
    skills.sort_by(|a, b| a.name.cmp(&b.name));
    skills
}

#[cfg(test)]
mod tests;
