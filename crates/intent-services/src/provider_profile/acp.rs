//! Sealed native acquisition delivered through the pinned Claude ACP adapter.
//! The adapter reads `CLAUDE_CODE_EXECUTABLE` after metadata options, so the
//! acquired executable is authoritative for new and loaded sessions alike.
use std::path::{Path, PathBuf};

use intent_acp::spawn::{prepare_provider, PreparedProvider, SpawnOptions};

use super::acquisition::{acquire_native, AcquiredNativeProfile, LaunchInputs, NativeAcquisition};
use super::staged::DeferredReason;
use super::{ManagedProviderProfile, ProfileDirectory, ProfileError, ProfileResult};

pub enum AcpAcquisition {
    Ready(Box<AcquiredAcpProfile>),
    Deferred(DeferredReason),
}

pub struct AcquiredAcpProfile {
    native: Box<AcquiredNativeProfile>,
    npx: PathBuf,
    launcher_identity: super::ConfigurationIdentity,
}

/// Both resources must remain owned until the spawned process group is reaped.
pub struct ManagedAcpLaunch {
    pub prepared: PreparedProvider,
    pub profile: ManagedProviderProfile,
}

fn supported_delivery(opts: &SpawnOptions<'_>, workspace: &Path) -> bool {
    opts.provider.id == "claude-code"
        && opts.via_npx()
        && opts.npx_fallback_package == Some(intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE)
        && opts.cwd == Some(workspace)
        && intent_acp::spawn::build_args(opts)
            == [
                intent_acp::spawn::NPX_NO_WORKSPACES_ARG,
                "-y",
                intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE,
            ]
        && opts.extra_env.iter().all(|(key, value)| {
            key.starts_with("GIT_") || key == "NODE_OPTIONS" && heap_limit(value).is_some()
        })
        && opts.env_mcp_config.is_none()
        && opts.unsloth_endpoint.is_none()
}

async fn launcher_identity(path: &Path) -> Option<super::ConfigurationIdentity> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let discovered = intent_providers::find_npx()?;
        let actual = std::fs::canonicalize(&path).ok()?;
        if actual != std::fs::canonicalize(discovered).ok()? {
            return None;
        }
        let meta = std::fs::metadata(&actual).ok()?;
        if !meta.is_file() || meta.len() > 1024 * 1024 {
            return None;
        }
        let contents = std::fs::read(&actual).ok()?;
        Some(super::ConfigurationIdentity::from_value(
            &serde_json::json!({
                "path": actual, "bytes": contents,
            }),
        ))
    })
    .await
    .ok()
    .flatten()
}

fn heap_limit(value: &str) -> Option<u32> {
    value
        .strip_prefix("--max-old-space-size=")?
        .parse::<u32>()
        .ok()
        .filter(|n| *n > 0)
}

/// Decide before changing any launch or profile. Unknown adapter deliveries
/// preserve legacy behavior; a Ready result never authorizes native fallback.
pub async fn acquire_acp(opts: &SpawnOptions<'_>, workspace: &Path) -> AcpAcquisition {
    if !supported_delivery(opts, workspace) {
        return AcpAcquisition::Deferred(match opts.provider.id {
            "claude-code" => DeferredReason::AdapterDelivery,
            "pi" => DeferredReason::PiGatewayDelivery,
            _ => DeferredReason::ProviderControls,
        });
    }
    let Some(npx) = opts.npx_fallback_binary else {
        return AcpAcquisition::Deferred(DeferredReason::AdapterDelivery);
    };
    let Some(launcher_identity) = launcher_identity(npx).await else {
        return AcpAcquisition::Deferred(DeferredReason::AdapterDelivery);
    };
    match acquire_native(opts.provider.id, workspace).await {
        NativeAcquisition::Ready(native) => AcpAcquisition::Ready(Box::new(AcquiredAcpProfile {
            native,
            launcher_identity,
            npx: npx.to_owned(),
        })),
        NativeAcquisition::Deferred(reason) => AcpAcquisition::Deferred(reason),
    }
}

impl AcquiredAcpProfile {
    pub(crate) fn same_configuration(&self, other: &Self) -> bool {
        self.npx == other.npx
            && self.launcher_identity == other.launcher_identity
            && self.native.same_configuration(&other.native)
    }
    #[must_use]
    pub fn executable(&self) -> &Path {
        self.native.executable()
    }

    #[must_use]
    pub fn workspace(&self) -> &Path {
        self.native.workspace()
    }

    /// Prepare without spawning. Native authority is revalidated by build;
    /// runtime arguments stay on the native profile, never on the ACP CLI.
    /// # Errors
    /// Changed delivery, source, policy or profile failures are hard errors.
    pub async fn prepare(
        &self,
        opts: &SpawnOptions<'_>,
        inputs: LaunchInputs<'_>,
        directory: ProfileDirectory,
    ) -> ProfileResult<ManagedAcpLaunch> {
        if !supported_delivery(opts, self.workspace())
            || opts.npx_fallback_binary != Some(self.npx.as_path())
            || launcher_identity(&self.npx).await.as_ref() != Some(&self.launcher_identity)
        {
            return Err(ProfileError::UnsupportedIsolation {
                provider: "claude-code".into(),
                missing: "ACP delivery changed after acquisition; reacquire before launching",
            });
        }
        let mut profile = self.native.build(inputs, directory).await?;
        if let Some(limit) = opts.node_max_old_space_mb.or_else(|| {
            opts.extra_env
                .get("NODE_OPTIONS")
                .and_then(|value| heap_limit(value))
        }) {
            profile
                .environment
                .set("NODE_OPTIONS", format!("--max-old-space-size={limit}"));
        }
        let npx = self.npx.clone();
        let workspace = self.workspace().to_owned();
        let launch_root = opts.npx_launch_root.map(Path::to_owned);
        let extra_env = opts.extra_env.clone();
        let mut prepared = tokio::task::spawn_blocking(move || {
            let provider =
                intent_providers::find_provider("claude-code").ok_or(ProfileError::Io)?;
            let mut opts = SpawnOptions::new(provider);
            opts.cwd = Some(&workspace);
            opts.npx_launch_root = launch_root.as_deref();
            opts.npx_fallback_binary = Some(&npx);
            opts.npx_fallback_package = Some(intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE);
            opts.extra_env = extra_env;
            prepare_provider(&opts).map_err(|_| ProfileError::Io)
        })
        .await
        .map_err(|_| ProfileError::Io)??;
        self.native.apply_environment(&mut prepared.command);
        prepared.command.envs(&opts.extra_env);
        // Preserve the neutral npm directory and workspace-selector removals.
        let selectors: Vec<_> = prepared
            .command
            .as_std()
            .get_envs()
            .filter(|(key, _)| {
                key.to_str()
                    .is_some_and(intent_acp::spawn::is_npm_workspace_selector_env)
            })
            .map(|(key, _)| key.to_owned())
            .collect();
        for key in selectors {
            prepared.command.env_remove(key);
        }
        profile
            .environment
            .apply_to_command(prepared.command.as_std_mut());
        Ok(ManagedAcpLaunch { prepared, profile })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_pinned_delivery_and_acquired_workspace_are_supported() {
        let provider = intent_providers::find_provider("claude-code").unwrap();
        let cwd = Path::new("/workspace");
        let mut opts = SpawnOptions::new(provider);
        opts.cwd = Some(cwd);
        opts.npx_fallback_binary = Some(Path::new("/usr/bin/npx"));
        opts.npx_fallback_package = Some(intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE);
        assert!(supported_delivery(&opts, cwd));
        assert!(!supported_delivery(&opts, Path::new("/other")));
        opts.extra_env
            .insert("GIT_AUTHOR_NAME".into(), "Fixture".into());
        assert!(supported_delivery(&opts, cwd));
        opts.extra_env
            .insert("ANTHROPIC_BASE_URL".into(), "unacquired".into());
        assert!(!supported_delivery(&opts, cwd));
        opts.extra_env.clear();
        opts.npx_fallback_package = Some("unverified-adapter");
        assert!(!supported_delivery(&opts, cwd));
        opts.npx_fallback_package = Some(intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE);
        opts.provider_binary = Some(Path::new("/custom/adapter"));
        assert!(!supported_delivery(&opts, cwd));
    }

    #[tokio::test]
    #[ignore = "requires Linux bwrap, node, and pinned Claude packages in INTENT_CLAUDE_FIXTURE_MODULES"]
    async fn acquired_claude_acp_new_load_fixture() {
        use serde_json::json;
        if std::env::var_os("INTENT_ACP_FIXTURE_CHILD").is_some() {
            let cwd = std::env::current_dir().unwrap();
            let parent = PathBuf::from(std::env::var_os("INTENT_ACP_FIXTURE_STATE").unwrap());
            let provider = intent_providers::find_provider("claude-code").unwrap();
            let mut opts = SpawnOptions::new(provider);
            opts.cwd = Some(&cwd);
            opts.npx_launch_root = Some(&parent);
            opts.npx_fallback_binary = Some(Path::new("/usr/bin/npx"));
            opts.npx_fallback_package = Some(intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE);
            let acquired = match acquire_acp(&opts, &cwd).await {
                AcpAcquisition::Ready(acquired) => acquired,
                AcpAcquisition::Deferred(reason) => {
                    panic!("synthetic supported ACP acquisition deferred: {reason:?}")
                }
            };
            let ephemeral = std::env::var_os("INTENT_ACP_FIXTURE_EPHEMERAL").is_some();
            let purpose = if ephemeral {
                super::super::ProfilePurpose::Ephemeral
            } else {
                super::super::ProfilePurpose::Interactive
            };
            let servers = if ephemeral {
                intent_acp::NormalizedMcpServers::new()
            } else {
                let value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(cwd.join("approved.json")).unwrap())
                        .unwrap();
                intent_acp::normalize_mcp_servers(&value)
            };
            let directory = ProfileDirectory::persistent(
                &parent,
                &super::super::ProfileIdentity {
                    workspace: "fixture",
                    agent: if ephemeral {
                        "ephemeral"
                    } else {
                        "interactive"
                    },
                    provider: "claude-code",
                },
            )
            .unwrap();
            let launch = acquired
                .prepare(
                    &opts,
                    LaunchInputs {
                        purpose,
                        approved_servers: &servers,
                        model: Some("claude-sonnet-4-6"),
                        instructions: "OWNED-INTERACTIVE-INSTRUCTIONS",
                        has_skill_instructions: false,
                        intent_policy: &[],
                    },
                    directory,
                )
                .await
                .unwrap();
            let cmd = launch.prepared.command.as_std();
            let env: std::collections::BTreeMap<_, _> = cmd
                .get_envs()
                .filter_map(|(k, v)| {
                    v.map(|v| {
                        (
                            k.to_string_lossy().into_owned(),
                            v.to_string_lossy().into_owned(),
                        )
                    })
                })
                .collect();
            println!(
                "LAUNCH:{}",
                json!({
                    "program":cmd.get_program().to_str().unwrap(), "args":cmd.get_args().map(|arg| arg.to_str().unwrap()).collect::<Vec<_>>(), "cwd":cmd.get_current_dir(), "env":env,
                    "meta":launch.profile.session_meta, "servers":intent_acp::to_acp_session_mcp_servers(&servers),
                })
            );
            tokio::task::spawn_blocking(|| {
                let mut line = String::new();
                std::io::stdin().read_line(&mut line).unwrap();
            })
            .await
            .unwrap();
            drop(launch);
            return;
        }
        let modules = std::env::var("INTENT_CLAUDE_FIXTURE_MODULES").expect("set pinned modules");
        let output = tokio::process::Command::new("node")
            .env_remove("NODE_OPTIONS")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("src/provider_profile/fixtures/acquired-claude-acp.mjs"),
            )
            .arg(modules)
            .arg(std::env::current_exe().unwrap())
            .arg("provider_profile::acp::tests::acquired_claude_acp_new_load_fixture")
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "ACP fixture failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout)
            .contains("PASS acquired ACP new/load/respawn and ephemeral inventory"));
    }
}
