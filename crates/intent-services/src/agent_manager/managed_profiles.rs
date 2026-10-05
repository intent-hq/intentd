//! Interactive staging. Effective external MCP catalogs remain deferred until
//! delivery can enforce their execution options and live Services gates.
use super::{
    json, normalize_mcp_servers, AgentId, AgentManager, AgentSession, Arc, Connection, Error,
    NormalizedMcpServers, PathBuf, Result, SpawnOptions, Value,
};
use crate::provider_profile::{
    acp::{acquire_acp, AcpAcquisition, AcquiredAcpProfile, ManagedAcpLaunch},
    acquisition::LaunchInputs,
    ManagedProviderProfile, ProfileDirectory, ProfileIdentity, ProfilePurpose,
};
use crate::spawned_provider_catalog::{
    resolve_project_catalog, CatalogInputs, CatalogPurpose, CatalogSnapshot, ExecutionOptions,
    ExplicitServer,
};
use intent_acp::session;

pub(super) const SELECTION_KEY: &str = "intentManagedClaudeProfile";

pub(super) struct SessionProfile {
    pub profile: ManagedProviderProfile,
    acquired: Box<AcquiredAcpProfile>,
    catalog_fingerprint: String,
}

pub(super) struct Plan {
    acquired: Box<AcquiredAcpProfile>,
    catalog: CatalogSnapshot,
    parent: PathBuf,
    workspace: String,
    agent: String,
}

fn failure() -> Error {
    Error::InvalidInput("Managed session configuration is no longer supported; restore its verified configuration before resuming.".into())
}

fn deferred(provider: &str, reason: &'static str, was_managed: bool) -> Result<Option<Plan>> {
    if was_managed {
        return Err(failure());
    }
    tracing::debug!(
        provider,
        purpose = "interactive",
        reason,
        "provider profile deferred; preserving existing interactive behavior"
    );
    Ok(None)
}

impl Plan {
    pub fn skill_catalog(&self) -> String {
        crate::skills::build_skills_catalog(&self.catalog.skills.skills)
    }

    pub fn matches(&self, previous: &SessionProfile) -> bool {
        self.catalog.fingerprint == previous.catalog_fingerprint
            && self.acquired.same_configuration(&previous.acquired)
    }

    pub async fn prepare(
        self,
        opts: &SpawnOptions<'_>,
        instructions: &str,
        servers: &NormalizedMcpServers,
    ) -> Result<(intent_acp::spawn::PreparedProvider, Arc<SessionProfile>)> {
        let parent = self.parent.clone();
        let workspace = self.workspace.clone();
        let agent = self.agent.clone();
        let directory = tokio::task::spawn_blocking(move || {
            ProfileDirectory::persistent(
                &parent,
                &ProfileIdentity {
                    workspace: &workspace,
                    agent: &agent,
                    provider: "claude-code",
                },
            )
        })
        .await
        .map_err(|_| failure())?
        .map_err(|e| Error::InvalidInput(e.to_string()))?;
        let ManagedAcpLaunch {
            prepared,
            mut profile,
        } = self
            .acquired
            .prepare(
                opts,
                LaunchInputs {
                    purpose: ProfilePurpose::Interactive,
                    approved_servers: servers,
                    model: opts.model,
                    instructions,
                    has_skill_instructions: !self.catalog.skills.skills.is_empty(),
                    intent_policy: &[],
                },
                directory,
            )
            .await
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        // Interactive sessions already use a replacement system prompt, not
        // the native preset. Keep their resolved instructions and tool rules.
        profile.session_meta["systemPrompt"] = json!(instructions);
        Ok((
            prepared,
            Arc::new(SessionProfile {
                profile,
                acquired: self.acquired,
                catalog_fingerprint: self.catalog.fingerprint,
            }),
        ))
    }
}

impl AgentManager {
    pub(super) async fn select_managed_profile(
        &self,
        session: &AgentSession,
        cwd: &std::path::Path,
        opts: &SpawnOptions<'_>,
    ) -> Result<Option<Plan>> {
        let provider = opts.provider.id;
        if provider != "claude-code" {
            return deferred(
                provider,
                if provider == "pi" {
                    "Pi ACP and external MCP gateway delivery is not verified"
                } else {
                    "native source controls are not verified for this interactive provider"
                },
                false,
            );
        }
        // A durable selection marker is a requirement to reacquire, never
        // authority. Losing profile files must not restore ambient discovery.
        let selected_before = session
            .metadata
            .as_ref()
            .and_then(|m| m.get(SELECTION_KEY))
            .is_some()
            || self.managed_profile(&session.id).is_some();
        // Keep native history outside the disposable agent-configs startup sweep.
        let Some(parent) = self
            .agent_config_root
            .as_ref()
            .and_then(|p| p.parent())
            .map(|p| p.join("provider-profiles"))
        else {
            return deferred(
                provider,
                "persistent managed session storage is unavailable",
                selected_before,
            );
        };
        let profile_path = ProfileDirectory::persistent_path(
            &parent,
            &ProfileIdentity {
                workspace: &session.workspace_id.0,
                agent: &session.id.0,
                provider,
            },
        );
        let profile_exists = tokio::task::spawn_blocking(move || {
            std::fs::symlink_metadata(profile_path.join("mcp.json")).is_ok()
        })
        .await
        .map_err(|_| failure())?;
        let was_managed = selected_before || profile_exists;
        if selected_before && !profile_exists && session.acp_session_id.is_some() {
            return Err(failure());
        }
        if opts.rules_file.is_some() {
            return deferred(
                provider,
                "caller-supplied instruction-file delivery is not verified",
                was_managed,
            );
        }
        if session.acp_session_id.is_some() && !was_managed {
            return deferred(
                provider,
                "existing native history has no verified managed profile",
                false,
            );
        }
        let Ok(catalog) = self.managed_catalog(session, cwd).await else {
            return deferred(
                provider,
                "workspace catalog cannot be represented by the verified delivery",
                was_managed,
            );
        };
        if !catalog.servers.is_empty() {
            return deferred(
                provider,
                "enabled external MCP requires delivery with live Services invocation gates",
                was_managed,
            );
        }
        match acquire_acp(opts, cwd).await {
            AcpAcquisition::Deferred(reason) => {
                deferred(provider, reason.explanation(), was_managed)
            }
            AcpAcquisition::Ready(acquired) => Ok(Some(Plan {
                acquired,
                catalog,
                parent,
                workspace: session.workspace_id.0.clone(),
                agent: session.id.0.clone(),
            })),
        }
    }

    pub(super) async fn managed_catalog(
        &self,
        session: &AgentSession,
        cwd: &std::path::Path,
    ) -> Result<CatalogSnapshot> {
        let settings = self.services.effective_settings();
        let configs: serde_json::Map<String, Value> =
            match self.services.secrets.load("mcp.servers").await {
                Ok(Some(raw)) => serde_json::from_str(&raw).map_err(|_| failure())?,
                Ok(None) => serde_json::Map::default(),
                Err(_) => return Err(failure()),
            };
        let explicit: Vec<_> = configs
            .into_iter()
            .map(|(id, cfg)| {
                let name = cfg
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&id)
                    .to_owned();
                let server = normalize_mcp_servers(&json!({&name: cfg})).remove(&name);
                ExplicitServer {
                    id,
                    name,
                    enabled: cfg.get("enabled").and_then(Value::as_bool).unwrap_or(false),
                    server,
                    execution: ExecutionOptions::default(),
                }
            })
            .collect();
        let workspace_disabled = self
            .services
            .store
            .workspace_mcp_disabled_servers(&session.workspace_id)
            .await?;
        let root = crate::git_ops::worktree_path(
            &self
                .services
                .store
                .get_workspace(&session.workspace_id)
                .await?,
        );
        let workspace_id = session.workspace_id.0.clone();
        let cwd = cwd.to_owned();
        let environment = std::env::vars().collect();
        tokio::task::spawn_blocking(move || {
            resolve_project_catalog(&CatalogInputs {
                workspace_id: &workspace_id,
                root: root.as_deref(),
                cwd: &cwd,
                purpose: CatalogPurpose::Interactive,
                enable_user_servers: crate::mcp_servers::enable_user_servers(&settings),
                explicit: &explicit,
                global_disabled_ids: &settings.mcp.disabled_servers,
                workspace_disabled_ids: &workspace_disabled,
                environment,
            })
        })
        .await
        .map_err(|_| failure())?
        .map_err(|_| failure())
    }

    pub(super) fn managed_profile(&self, agent_id: &AgentId) -> Option<Arc<SessionProfile>> {
        let handles = self.handles.lock().unwrap();
        let local = handles.get(agent_id)?.execution.local.as_ref()?;
        let profile = local.resources.lock().unwrap().managed_profile.clone();
        profile
    }

    pub(crate) fn managed_session_meta(
        &self,
        agent_id: &AgentId,
        conn: &Connection,
        provider: &str,
        legacy: Option<session::Meta>,
    ) -> Result<Option<session::Meta>> {
        let handles = self.handles.lock().unwrap();
        let Some(local) = handles
            .get(agent_id)
            .and_then(|h| h.execution.local.as_ref())
        else {
            return Ok(legacy);
        };
        let resources = local.resources.lock().unwrap();
        let Some(profile) = resources.managed_profile.as_ref() else {
            return Ok(legacy);
        };
        if provider != "claude-code" || !std::ptr::eq(resources.connection.as_ref(), conn) {
            return Err(failure());
        }
        merge_session_meta(&profile.profile.session_meta, legacy)
    }
}

fn merge_session_meta(
    profile: &Value,
    mut legacy: Option<session::Meta>,
) -> Result<Option<session::Meta>> {
    let meta = legacy.get_or_insert_with(Default::default);
    let options = &profile["claudeCode"]["options"];
    let claude = meta
        .entry("claudeCode".to_owned())
        .or_insert_with(|| json!({"options":{}}));
    let target = claude
        .get_mut("options")
        .and_then(Value::as_object_mut)
        .ok_or_else(failure)?;
    for (key, value) in options.as_object().ok_or_else(failure)? {
        target.insert(key.clone(), value.clone());
    }
    meta.insert("systemPrompt".into(), profile["systemPrompt"].clone());
    Ok(legacy)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn managed_new_and_load_controls_preserve_role_restrictions() {
        let controls = json!({"systemPrompt":"OWNED", "claudeCode":{"options":{
            "strictMcpConfig":true,"settingSources":[],"extraArgs":{"disable-slash-commands":""}
        }}});
        for legacy_prompt in ["new", "load"] {
            let legacy: session::Meta =
                serde_json::from_value(json!({"systemPrompt":legacy_prompt,
                "claudeCode":{"options":{"settingSources":["user"],"strictMcpConfig":false,
                "disallowedTools":["Task","Write"]}}}))
                .unwrap();
            let merged =
                serde_json::to_value(merge_session_meta(&controls, Some(legacy)).unwrap()).unwrap();
            assert_eq!(merged["systemPrompt"], "OWNED");
            assert_eq!(
                merged["claudeCode"]["options"]["disallowedTools"],
                json!(["Task", "Write"])
            );
            assert_eq!(merged["claudeCode"]["options"]["settingSources"], json!([]));
            assert_eq!(merged["claudeCode"]["options"]["strictMcpConfig"], true);
        }
        assert!(merge_session_meta(
            &controls,
            Some(serde_json::from_value(json!({"claudeCode":false})).unwrap())
        )
        .is_err());
    }
}
