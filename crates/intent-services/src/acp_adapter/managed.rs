//! Ephemeral integration of the sealed ACP acquisition. Acquisition is per run,
//! after admission, never cached along with the model catalog command.
//!
//! Managed coverage is the shared acquisition's pinned Claude ACP/native route:
//! completion, provider test prompt, and model/auth session probes all pass here.
//! Auggie print (including enhancement), Codex/Pi/other ACP and native model-list
//! commands remain explicitly deferred. No static Codex skill deny list is added.
use std::{collections::BTreeMap, sync::Arc};

use super::{AcpAdapterCommand, AdapterChild, HeldWhileLive, SpawnedAdapter};
use crate::provider_profile::{
    acp::{acquire_acp, AcpAcquisition, ManagedAcpLaunch},
    acquisition::LaunchInputs,
    staged::DeferredReason,
    ProfileDirectory, ProfilePurpose,
};
use intent_acp::spawn::SpawnOptions;
use intent_acp::ConnectionHooks;
use serde_json::Value;
use tokio::sync::{mpsc, OwnedSemaphorePermit};

pub(super) struct ProfileGuard {
    // Field order matters: remove the profile before its private parent.
    _directory: ProfileDirectory,
    _parent: tempfile::TempDir,
}

pub(super) struct Prepared {
    launch: ManagedAcpLaunch,
    parent: tempfile::TempDir,
}

fn private_parent() -> std::io::Result<tempfile::TempDir> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("intent-ephemeral-profile-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    builder.tempdir()
}

pub(crate) fn log_deferred(provider: &str, reason: DeferredReason) {
    tracing::debug!(
        provider,
        purpose = "ephemeral",
        reason = reason.explanation(),
        "provider profile deferred; preserving existing short-lived launch restrictions"
    );
}

/// A managed profile owns every loader option. A completion's string system
/// prompt is deliberately a replacement for the agent preset, not an append.
pub(crate) fn session_meta(legacy: Option<Value>, managed: Option<Value>) -> Option<Value> {
    let Some(mut managed) = managed else {
        return legacy;
    };
    let utility = crate::complete_ops::one_shot_session_shape("claude-code", "", None)
        .1
        .unwrap();
    managed["systemPrompt"] = legacy
        .as_ref()
        .and_then(|meta| meta.get("systemPrompt"))
        .filter(|value| value.is_string())
        .cloned()
        .unwrap_or_else(|| utility["systemPrompt"].clone());
    Some(managed)
}

fn options<'a>(cmd: &'a AcpAdapterCommand, cwd: &'a std::path::Path) -> Option<SpawnOptions<'a>> {
    let provider = intent_providers::find_provider(cmd.profile_provider?)?;
    let mut opts = SpawnOptions::new(provider);
    opts.cwd = Some(cwd);
    opts.npx_launch_root = cmd.npx_launch_root.as_deref();
    if cmd.via_npx {
        opts.npx_fallback_binary = Some(&cmd.program);
        opts.npx_fallback_package = provider.npx_only_package.or(provider.fallback_npx_package);
    } else {
        opts.provider_binary = Some(&cmd.program);
    }
    for (key, value) in &cmd.envs {
        opts.extra_env
            .insert(key.clone(), value.to_str()?.to_owned());
    }
    // There is no equivalent SpawnOptions removal channel. Never change the
    // actual invocation merely to make it eligible for managed acquisition.
    if !cmd.envs_removed.is_empty() || intent_acp::spawn::build_args(&opts) != cmd.args {
        return None;
    }
    Some(opts)
}

pub(super) async fn prepare(cmd: &AcpAdapterCommand) -> Result<Option<Prepared>, String> {
    let provider = cmd.profile_provider.unwrap_or("unknown");
    if provider != "claude-code" {
        log_deferred(
            provider,
            if provider == "pi" {
                DeferredReason::PiGatewayDelivery
            } else {
                DeferredReason::ProviderControls
            },
        );
        return Ok(None);
    }
    let cwd = cmd.working_dir();
    let Some(opts) = options(cmd, &cwd) else {
        log_deferred(provider, DeferredReason::AdapterDelivery);
        return Ok(None);
    };
    let acquired = match acquire_acp(&opts, &cwd).await {
        AcpAcquisition::Deferred(reason) => {
            log_deferred(provider, reason);
            return Ok(None);
        }
        AcpAcquisition::Ready(acquired) => acquired,
    };
    if cmd
        .installed
        .as_ref()
        .is_some_and(|installed| installed.context.runtime.path() != acquired.executable())
    {
        return Err(crate::provider_models::INSTALLED_SOURCE_CHANGED.to_owned());
    }
    // After Ready, every error is final. No caller can reinterpret a failed
    // publication/revalidation as permission to launch native configuration.
    let parent = private_parent().map_err(|_| "cannot create private ephemeral profile parent")?;
    let directory = ProfileDirectory::ephemeral(parent.path()).map_err(|e| e.to_string())?;
    let launch = acquired
        .prepare(
            &opts,
            LaunchInputs {
                purpose: ProfilePurpose::Ephemeral,
                approved_servers: &BTreeMap::new(),
                model: None,
                instructions: "",
                has_skill_instructions: false,
                intent_policy: &[],
            },
            directory,
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(Some(Prepared { launch, parent }))
}

pub(super) fn spawn(
    cmd: &AcpAdapterCommand,
    prepared: Prepared,
    slot: OwnedSemaphorePermit,
) -> Result<SpawnedAdapter, String> {
    let cwd = cmd.working_dir();
    let opts = options(cmd, &cwd).ok_or("managed ephemeral launch changed after selection")?;
    let Prepared { launch, parent } = prepared;
    let ManagedAcpLaunch { prepared, profile } = launch;
    let (note_tx, notifications) = mpsc::unbounded_channel();
    let (req_tx, requests) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        auth_required_stdout_marker: cmd.auth_required_stdout_marker,
        notifications: Some(note_tx),
        requests: Some(req_tx),
        ..Default::default()
    };
    let spawned = intent_acp::spawn::spawn_prepared_provider(&opts, prepared, hooks)
        .map_err(|_| "managed ephemeral adapter could not start")?;
    let (child, conn, npx_dir) = spawned.into_parts();
    let spawn_pid = child.id().expect("newly spawned adapter has a pid");
    Ok(SpawnedAdapter {
        profile_meta: Some(profile.session_meta),
        child: AdapterChild {
            child: Some(child),
            spawn_pid,
            held: Some(HeldWhileLive {
                npx_launch_dir: npx_dir.map(Arc::new),
                slot,
                installed: cmd.installed.clone(),
                managed: Some(ProfileGuard {
                    _directory: profile.directory,
                    _parent: parent,
                }),
            }),
        },
        conn,
        notifications,
        requests,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use crate::acp_adapter::{initialize_params, spawn_adapter};
    #[cfg(unix)]
    use crate::acp_adapter::{AdapterSlots, RetainUnlessSwept};
    use serde_json::json;
    #[cfg(unix)]
    use std::{path::PathBuf, process::Stdio, time::Duration};

    #[test]
    fn managed_meta_keeps_utility_prompt_and_authoritative_inventory_controls() {
        let legacy = json!({"systemPrompt":"literal utility $prompt", "claudeCode":{"options":{"settingSources":["user"],"tools":["Read"],"extraArgs":{"plugin-dir":"ambient"}}}});
        let managed = json!({"systemPrompt":{"type":"preset","preset":"claude_code"},"claudeCode":{"options":{"settingSources":[],"tools":[],"strictMcpConfig":true,"extraArgs":{"disable-slash-commands":""}}}});
        let result = session_meta(Some(legacy), Some(managed.clone())).unwrap();
        assert_eq!(result["systemPrompt"], "literal utility $prompt");
        assert_eq!(result["claudeCode"], managed["claudeCode"]);
        let default = session_meta(None, Some(managed)).unwrap();
        assert!(default["systemPrompt"]
            .as_str()
            .unwrap()
            .contains("text-only utility"));
    }

    #[test]
    fn deferred_metadata_and_command_restrictions_are_unchanged() {
        let legacy = crate::complete_ops::one_shot_session_shape("claude-code", "prompt", None).1;
        assert_eq!(session_meta(legacy.clone(), None), legacy);
        assert_eq!(session_meta(None, None), None);
        let provider = intent_providers::find_provider("codex").unwrap();
        let cmd =
            crate::complete_ops::one_shot_launch(provider, None, Some("/test/npx".into()), None)
                .unwrap();
        assert!(cmd.envs.iter().any(|(key, value)| key == "CODEX_CONFIG"
            && value == intent_providers::CODEX_SUBAGENT_POLICY_CONFIG));
    }

    #[test]
    fn altered_adapter_invocation_cannot_be_replaced_by_managed_launch() {
        let cmd = AcpAdapterCommand::npx(
            "/test/npx".into(),
            intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE,
        );
        assert!(options(&cmd, std::path::Path::new("/test/workspace")).is_some());
        assert!(options(
            &cmd.clone().args(["--unverified".into()]),
            std::path::Path::new("/test/workspace")
        )
        .is_none());
        assert!(options(
            &cmd.env_remove("AUTH"),
            std::path::Path::new("/test/workspace")
        )
        .is_none());
    }

    #[cfg(unix)]
    async fn guarded_child() -> (AdapterChild, PathBuf, PathBuf) {
        use tokio::io::AsyncReadExt;
        let parent = private_parent().unwrap();
        let directory = ProfileDirectory::ephemeral(parent.path()).unwrap();
        let profile_path = directory.path().to_owned();
        let parent_path = parent.path().to_owned();
        directory
            .write_private("mcp.json", b"{\"mcpServers\":{}}")
            .unwrap();
        let mut command = tokio::process::Command::new("sh");
        command
            .args(["-c", "printf ready; read -r finish"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);
        let child = command.spawn().unwrap();
        let slots = AdapterSlots::new(1);
        let mut child = AdapterChild {
            spawn_pid: child.id().unwrap(),
            child: Some(child),
            held: Some(HeldWhileLive {
                slot: slots.acquire(Duration::from_secs(1)).await.unwrap(),
                npx_launch_dir: None,
                installed: None,
                managed: Some(ProfileGuard {
                    _directory: directory,
                    _parent: parent,
                }),
            }),
        };
        let mut ready = [0; 5];
        child
            .stdout
            .as_mut()
            .unwrap()
            .read_exact(&mut ready)
            .await
            .unwrap();
        assert_eq!(&ready, b"ready");
        assert!(profile_path.join("mcp.json").exists());
        (child, profile_path, parent_path)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_profile_survives_until_reap_and_is_removed_afterward() {
        let (mut child, profile, parent) = guarded_child().await;
        child.reap().await;
        assert!(!profile.exists());
        assert!(!parent.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_transfers_managed_profile_to_detached_cleanup() {
        let (child, profile, parent) = guarded_child().await;
        drop(child);
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut check = tokio::time::interval(Duration::from_millis(10));
            while parent.exists() {
                check.tick().await;
            }
        })
        .await
        .expect("managed profile cleanup did not finish");
        assert!(!profile.exists());
    }

    #[cfg(unix)]
    #[test]
    fn unconfirmed_reap_retains_managed_profile() {
        let parent = private_parent().unwrap();
        let directory = ProfileDirectory::ephemeral(parent.path()).unwrap();
        let parent_path = parent.path().to_owned();
        let profile_path = directory.path().to_owned();
        RetainUnlessSwept(
            None,
            None,
            Some(ProfileGuard {
                _directory: directory,
                _parent: parent,
            }),
        )
        .remove(false);
        assert!(
            profile_path.exists(),
            "unconfirmed cleanup must not remove a possibly live profile"
        );
        std::fs::remove_dir_all(parent_path).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "requires bwrap and pinned Claude modules at INTENT_CLAUDE_FIXTURE_MODULES"]
    async fn native_ephemeral_callers_have_zero_inventory() {
        if let Ok(mode) = std::env::var("INTENT_EPHEMERAL_CHILD") {
            let cwd = std::env::current_dir().unwrap();
            if mode.starts_with("inventory") {
                let provider = intent_providers::find_provider("claude-code").unwrap();
                let cmd = crate::complete_ops::one_shot_launch(
                    provider,
                    None,
                    Some("/usr/bin/npx".into()),
                    None,
                )
                .unwrap()
                .cwd(cwd)
                .prepare_installed()
                .await
                .unwrap();
                let mut adapter = spawn_adapter(&cmd, Duration::from_secs(20)).await.unwrap();
                assert_eq!(
                    adapter.profile_meta.is_some(),
                    mode == "inventory",
                    "actual invocation must select the expected managed/deferred branch"
                );
                adapter
                    .conn
                    .request_timeout("initialize", initialize_params(), Duration::from_secs(10))
                    .await
                    .unwrap();
                let response = adapter.conn.request_timeout("session/new", json!({
                    "cwd":cmd.working_dir(), "mcpServers":[],
                    "_meta":session_meta(cmd.probe_session_meta(), adapter.profile_meta.take()),
                }), Duration::from_secs(20)).await.unwrap();
                assert!(response["sessionId"].is_string());
                tokio::time::timeout(Duration::from_secs(10), async {
                    while let Some(event) = adapter.notifications.recv().await {
                        if event.params["update"]["sessionUpdate"] == "available_commands_update" {
                            if mode == "inventory" {
                                assert_eq!(event.params["update"]["availableCommands"], json!([]));
                            }
                            return;
                        }
                    }
                    panic!("adapter closed before reporting command inventory");
                })
                .await
                .unwrap();
                adapter.child.reap().await;
            } else if mode == "models" {
                let result = crate::provider_models::fetch_claude_code_models().await;
                assert!(result.models.is_some(), "model discovery failed");
            } else if mode == "provider-test" {
                let result = crate::provider_test_prompt::provider_test_prompt(
                    None,
                    "claude-code",
                    Some("claude-sonnet-4-6"),
                    &std::collections::HashMap::new(),
                    None,
                )
                .await
                .unwrap();
                assert_eq!(result["ok"], true, "{result}");
            } else {
                let state = crate::tests::test_tempdir("intent-ephemeral-service-");
                let store = intent_store::Store::open(&state.path().join("store.db"))
                    .await
                    .unwrap();
                let registry = Arc::new(
                    crate::SettingsRegistry::load(state.path().join("config.toml")).unwrap(),
                );
                registry
                    .apply(&[("model.defaultProvider".into(), json!("claude-code"))])
                    .unwrap();
                let services = crate::Services::new(store).with_settings_registry(registry);
                let timeout = if mode == "timeout" { 200 } else { 15_000 };
                let run = intent_core::caller::spawn_with_current_caller(async move {
                    let _state = state;
                    services
                        .agent_complete_once_op(
                            "Reply OK".into(),
                            Some("EPHEMERAL-UTILITY-INSTRUCTIONS".into()),
                            Some("claude-sonnet-4-6".into()),
                            None,
                            None,
                            Some(timeout),
                            None,
                        )
                        .await
                });
                if mode == "cancel" {
                    let marker = cwd.join("request-started");
                    tokio::time::timeout(Duration::from_secs(25), async {
                        let mut check = tokio::time::interval(Duration::from_millis(10));
                        while !marker.exists() {
                            check.tick().await;
                        }
                    })
                    .await
                    .expect("completion never reached synthetic endpoint");
                    run.abort();
                    assert!(run.await.unwrap_err().is_cancelled());
                    tokio::time::timeout(Duration::from_secs(10), async {
                        let mut check = tokio::time::interval(Duration::from_millis(10));
                        while super::super::live_adapters() != 0 {
                            check.tick().await;
                        }
                    })
                    .await
                    .expect("cancelled adapter was not reaped");
                } else {
                    let result = run.await.unwrap();
                    if mode == "timeout" || mode == "error" {
                        assert!(result.is_err(), "{result:?}");
                    } else {
                        assert_eq!(result.unwrap()["text"], "OK");
                    }
                }
            }
            println!("PASS ephemeral caller {mode}");
            return;
        }
        let modules =
            std::env::var("INTENT_CLAUDE_FIXTURE_MODULES").expect("set pinned Claude modules");
        let output = tokio::process::Command::new("node")
            .env_remove("NODE_OPTIONS")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("src/acp_adapter/ephemeral-fixture.mjs"),
            )
            .arg(modules)
            .arg(std::env::current_exe().unwrap())
            .arg("acp_adapter::managed::tests::native_ephemeral_callers_have_zero_inventory")
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "ephemeral native fixture:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout)
            .contains("PASS ephemeral inventory and callers"));
    }
}
