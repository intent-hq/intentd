//! Final command/profile integration shared by persistent and ephemeral launches.
use crate::provider_profiles::{
    LaunchPurpose, PolicySource, ProviderLaunchProfile, ProviderProfileRequest,
};
use intent_acp::NormalizedMcpServers;
use intent_core::{Error, Result};
use std::path::{Path, PathBuf};

pub(crate) fn command_env(command: &tokio::process::Command, key: &str) -> Option<String> {
    command
        .as_std()
        .get_envs()
        .find(|(k, _)| *k == key)
        .map_or_else(
            || std::env::var(key).ok(),
            |(_, v)| v.map(|v| v.to_string_lossy().into_owned()),
        )
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn prepare(
    provider: &str,
    purpose: LaunchPurpose,
    command: &tokio::process::Command,
    root: &Path,
    workspace: &Path,
    cwd: &Path,
    identity: Option<&str>,
    mcp: &NormalizedMcpServers,
    policy: &[PolicySource],
) -> Result<ProviderLaunchProfile> {
    let home = command_env(command, "HOME")
        .or_else(|| command_env(command, "USERPROFILE"))
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            Error::InvalidInput(
                "Provider home is unavailable; cannot safely resolve configuration.".into(),
            )
        })?;
    let provider_home = match provider {
        "codex" => command_env(command, "CODEX_HOME").map(PathBuf::from),
        "opencode" | "unsloth" => {
            command_env(command, "XDG_CONFIG_HOME").map(|s| PathBuf::from(s).join("opencode"))
        }
        _ => None,
    };
    // Only explicit daemon command overrides are eligible as trusted routing.
    let routing = command
        .as_std()
        .get_envs()
        .find(|(k, _)| *k == "OPENCODE_CONFIG_CONTENT")
        .and_then(|(_, v)| v)
        .and_then(|v| serde_json::from_str(&v.to_string_lossy()).ok());
    let profile = crate::provider_profiles::prepare_provider_profile(ProviderProfileRequest {
        provider_id: provider,
        detected_version: None,
        purpose,
        owned_root: root,
        persistent_identity: identity,
        resume: false,
        home: &home,
        provider_home: provider_home.as_deref(),
        workspace_root: workspace,
        launch_cwd: cwd,
        owned_mcp: mcp,
        policy_sources: policy,
        native_config_files: &[],
        native_skill_roots: &[],
        trusted_launch_config: routing.as_ref(),
    })
    .map_err(|e| Error::InvalidInput(e.to_string()))?;
    for d in &profile.diagnostics {
        tracing::warn!(
            provider,
            code = d.code,
            message = d.message,
            "Provider configuration limitation"
        );
    }
    tracing::warn!(provider, "Remote/MDM policy discovery is unverified; supplied authoritative policy is enforced and native policy remains active");
    Ok(profile)
}

impl crate::Services {
    pub(crate) fn validate_provider_configuration(&self, provider: &str) -> Result<()> {
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .ok_or_else(|| Error::InvalidInput("Provider policy home unavailable".into()))?;
        let mut sources = crate::provider_profiles::local_policy_sources(provider, &home);
        if provider == "codex" {
            if let Some(home) = std::env::var_os("CODEX_HOME") {
                sources.push(PolicySource::Unsupported(
                    PathBuf::from(home).join("managed_config.toml"),
                ));
            }
        }
        sources.extend(self.provider_policy_sources(provider));
        crate::provider_profiles::load_mcp_policy(provider, &sources)
            .map(|_| ())
            .map_err(|e| Error::InvalidInput(e.to_string()))
    }

    /// Supply authoritative policy discovered by the host integration. These
    /// inputs are never accepted from workspace files or agent arguments.
    /// An enrolled but unreadable source must be represented as Unavailable.
    ///
    /// # Panics
    /// Panics if the trusted policy registry lock was poisoned.
    pub fn set_provider_policy_sources(&self, provider: &str, sources: Vec<PolicySource>) {
        self.provider_policy
            .lock()
            .unwrap()
            .insert(provider.to_owned(), sources);
    }

    pub(crate) fn provider_policy_sources(&self, provider: &str) -> Vec<PolicySource> {
        self.provider_policy
            .lock()
            .unwrap()
            .get(provider)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) async fn mcp_caller_policy(
        &self,
        workspace: Option<&intent_core::WorkspaceId>,
        caller: Option<&intent_core::AgentId>,
    ) -> Result<crate::provider_profiles::McpPolicy> {
        let (Some(workspace), Some(caller)) = (workspace, caller) else {
            return Err(Error::InvalidParams(
                "MCP tools require a context-authenticated workspace and agent".into(),
            ));
        };
        let session = self.store.get_agent_session(caller).await?;
        if &session.workspace_id != workspace {
            return Err(Error::InvalidParams(
                "MCP caller does not belong to this workspace".into(),
            ));
        }
        if let Some(profile) = self.agent_manager().and_then(|m| m.active_profile(caller)) {
            return profile
                .reload_policy(&self.provider_policy_sources(profile.provider_id()))
                .map_err(|e| Error::InvalidInput(e.to_string()));
        }
        let provider = crate::agent_session::resolve_provider_id(
            session.provider.as_deref(),
            crate::agent_session::derived_default_provider(&self.effective_settings()).as_deref(),
        )
        .ok_or_else(|| Error::InvalidParams("Cannot resolve MCP caller provider".into()))?;
        let provider = intent_providers::find_provider_or_legacy_alias(&provider)
            .ok_or_else(|| Error::InvalidParams("Unknown MCP caller provider".into()))?
            .id;
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .ok_or_else(|| Error::InvalidParams("Cannot resolve policy home".into()))?;
        let mut sources = crate::provider_profiles::local_policy_sources(provider, &home);
        if provider == "codex" {
            if let Some(home) = std::env::var_os("CODEX_HOME") {
                sources.push(PolicySource::Unsupported(
                    PathBuf::from(home).join("managed_config.toml"),
                ));
            }
        }
        sources.extend(self.provider_policy_sources(provider));
        crate::provider_profiles::load_mcp_policy(provider, &sources)
            .map_err(|e| Error::InvalidInput(e.to_string()))
    }
}

#[cfg(unix)]
pub(crate) fn pi_native_wrapper(
    profile: &mut ProviderLaunchProfile,
) -> std::result::Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let wrapper = profile.path().join("pi-native.sh");
    let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
    let args = profile
        .native_args
        .iter()
        .map(|s| quote(s))
        .collect::<Vec<_>>()
        .join(" ");
    let command = crate::pi_cli::resolve_real_pi_command();
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\nexec {} {} \"$@\"\n", quote(&command), args),
    )
    .map_err(|e| e.to_string())?;
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| e.to_string())?;
    profile.env.insert(
        "PI_ACP_PI_COMMAND".into(),
        wrapper.to_string_lossy().into_owned(),
    );
    Ok(())
}
#[cfg(not(unix))]
pub(crate) fn pi_native_wrapper(_: &mut ProviderLaunchProfile) -> std::result::Result<(), String> {
    Err("Pi native configuration wrapper requires a Unix host".into())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use intent_acp::{NormalizedMcpServer, SpawnOptions};
    use serde_json::{json, Value};
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn final_codex_child_receives_owned_map_and_stable_profile() {
        let dir = crate::test_support::test_tempdir("provider-final-command");
        let home = dir.path().join("home");
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(cwd.join(".codex")).unwrap();
        std::fs::write(
            cwd.join(".codex/config.toml"),
            "[mcp_servers.native]\ncommand='false'\n",
        )
        .unwrap();
        let bin = dir.path().join("fake-npx");
        std::fs::write(
            &bin,
            "#!/bin/sh\nprintf '%s\\n%s\\n' \"$CODEX_CONFIG\" \"$CODEX_HOME\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o700)).unwrap();
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        opts.npx_fallback_binary = Some(&bin);
        opts.npx_fallback_package = Some(intent_providers::CODEX_ACP_NPX_PACKAGE);
        opts.extra_env
            .insert("HOME".into(), home.to_string_lossy().into_owned());
        opts.extra_env.insert(
            "CODEX_HOME".into(),
            home.join(".codex").to_string_lossy().into_owned(),
        );
        opts.extra_env
            .insert("CODEX_CONFIG".into(), "{\"unsafe\":true}".into());
        let mut command = intent_acp::spawn::build_command(&opts);
        let mut mcp = NormalizedMcpServers::new();
        mcp.insert(
            "selected".into(),
            NormalizedMcpServer::Stdio {
                command: "echo".into(),
                args: vec![],
                env: std::collections::BTreeMap::new(),
            },
        );
        let profile = prepare(
            "codex",
            LaunchPurpose::Persistent,
            &command,
            dir.path(),
            &cwd,
            &cwd,
            Some("ws-agent"),
            &mcp,
            &[],
        )
        .unwrap();
        let path = profile.path().to_owned();
        assert!(profile.session_mcp.is_empty());
        profile.apply_to_command(&mut command);
        let output = command.output().await.unwrap();
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        let config: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert_eq!(config["agents"]["enabled"], false);
        assert_eq!(config["features"]["multi_agent_v2"], false);
        assert_eq!(config["mcp_servers"]["native"]["enabled"], false);
        assert_eq!(
            config["mcp_servers"]
                .as_object()
                .unwrap()
                .values()
                .filter(|v| v["enabled"] == true)
                .count(),
            1
        );
        assert!(config.get("unsafe").is_none());
        assert_eq!(text.lines().nth(1), path.to_str());
        drop(profile);
        intent_core::agent_configs::sweep_agent_configs(dir.path()).unwrap();
        // The sweep preserves the stable profile, even though ordinary files disappear.
        assert!(path.is_dir());
    }

    #[tokio::test]
    async fn ephemeral_profile_meta_retains_tool_denial_and_has_no_mcp() {
        let dir = crate::test_support::test_tempdir("ephemeral-profile-meta");
        let command = crate::acp_adapter::AcpAdapterCommand::binary("/bin/true".into(), vec![])
            .for_provider("claude-code")
            .env("HOME", dir.path())
            .cwd(dir.path().to_owned())
            .prepare_profile(LaunchPurpose::PromptTest, vec![])
            .await
            .unwrap();
        let meta = command
            .merge_profile_meta(Some(
                json!({"claudeCode":{"options":{"tools":[]}},"systemPrompt":"utility"}),
            ))
            .unwrap();
        assert_eq!(meta["claudeCode"]["options"]["tools"], json!([]));
        assert_eq!(meta["claudeCode"]["options"]["strictMcpConfig"], true);
        assert_eq!(meta["systemPrompt"], "utility");
    }
    #[tokio::test]
    async fn ephemeral_profile_outlives_cancelled_utility_until_tree_cleanup() {
        use std::process::Stdio;
        use std::time::Duration;
        let dir = crate::test_support::test_tempdir("utility-profile-lease");
        let started = dir.path().join("started");
        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .args(["-c", "printf ready > \"$STARTED\"; sleep 30"])
            .env("STARTED", &started)
            .env("HOME", dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let profile =
            ephemeral_command_profile("auggie", LaunchPurpose::Completion, &command, dir.path())
                .unwrap();
        let path = profile.path().to_owned();
        // caller-binding: allow — test owns only a subprocess and profile, with no service calls.
        let runner = tokio::spawn(run_utility(
            command,
            profile,
            Vec::new(),
            Duration::from_millis(300),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !started.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(path.exists());
        runner.abort();
        let _ = runner.await;
        assert!(
            path.exists(),
            "cancelled caller must not remove a live child's profile"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owned cleanup removed the ephemeral profile");
    }
}

pub(crate) fn ephemeral_command_profile(
    provider: &str,
    purpose: LaunchPurpose,
    command: &tokio::process::Command,
    cwd: &Path,
) -> Result<ProviderLaunchProfile> {
    let root = tempfile::Builder::new()
        .prefix("intent-provider-cli-")
        .tempdir()
        .map_err(|e| Error::Internal(format!("Provider profile: {e}")))?;
    let mut profile = prepare(
        provider,
        purpose,
        command,
        root.path(),
        cwd,
        cwd,
        None,
        &NormalizedMcpServers::new(),
        &[],
    )?;
    profile.hold_parent(root);
    Ok(profile)
}

pub(crate) async fn run_utility(
    mut command: tokio::process::Command,
    profile: ProviderLaunchProfile,
    input: Vec<u8>,
    timeout: std::time::Duration,
) -> std::result::Result<std::process::Output, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    profile.apply_to_command(&mut command);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn().map_err(|e| e.to_string())?;
    let pid = child.id().ok_or("Provider child has no pid")?;
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    // caller-binding: allow — subprocess I/O and tree cleanup only; policy was checked before spawn.
    tokio::spawn(async move {
        let profile = std::mem::ManuallyDrop::new(profile);
        let run = async {
            let write = async {
                if let Some(mut stdin) = stdin {
                    let _ = stdin.write_all(&input).await;
                }
            };
            let read_out = async {
                let mut bytes = Vec::new();
                if let Some(mut out) = stdout {
                    out.read_to_end(&mut bytes).await?;
                }
                Ok::<_, std::io::Error>(bytes)
            };
            let read_err = async {
                let mut bytes = Vec::new();
                if let Some(mut err) = stderr {
                    err.read_to_end(&mut bytes).await?;
                }
                Ok::<_, std::io::Error>(bytes)
            };
            let ((), stdout, stderr, status) =
                tokio::join!(write, read_out, read_err, child.wait());
            Ok::<_, std::io::Error>(std::process::Output {
                status: status?,
                stdout: stdout?,
                stderr: stderr?,
            })
        };
        let result = tokio::time::timeout(timeout, run).await;
        if crate::acp_adapter::reap_child(&mut child, pid).await {
            drop(std::mem::ManuallyDrop::into_inner(profile));
        }
        result
            .map_err(|_| "timed out".to_string())?
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|_| "Provider cleanup task failed".to_string())?
}
