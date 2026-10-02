use super::*;
use std::os::unix::{ffi::OsStringExt, fs::PermissionsExt};

fn executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn fixture(cli: InstalledCli, body: &str) -> (tempfile::TempDir, InstalledContext) {
    let root = tempfile::tempdir().unwrap();
    executable(&root.path().join(cli.command()), body);
    let runtime = cli
        .resolve_in_dirs(&[root.path().to_owned()], false)
        .unwrap();
    let inherited = BTreeMap::from([
        (OsString::from("HOME"), root.path().as_os_str().to_owned()),
        ("PATH".into(), "/bin:/usr/bin".into()),
    ]);
    let context = InstalledContext::from_inputs(
        runtime,
        &BTreeMap::new(),
        inherited,
        &intent_core::cli_env::CodexEnvNames::default(),
    )
    .unwrap();
    (root, context)
}

fn launch(context: &InstalledContext, cwd: &Path) -> Command {
    let mut cmd = Command::new("unused-adapter");
    cmd.current_dir(cwd);
    context.apply(&mut cmd);
    cmd
}

fn env(command: &Command, key: &str) -> Option<OsString> {
    command
        .as_std()
        .get_envs()
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| v.map(OsString::from))
}

#[tokio::test]
async fn installed_cli_wrapper_upgrade_changes_catalog_identity_without_adapter_change() {
    let (root, context) = fixture(
        InstalledCli::Codex,
        "#!/bin/sh\nprintf 'codex-cli '; cat \"$HOME/version\"\n",
    );
    std::fs::write(root.path().join("version"), "1.2.3").unwrap();
    let cmd = launch(&context, root.path());
    let (first, _) = context.observe(&cmd).await.unwrap();
    std::fs::write(root.path().join("version"), "1.2.4").unwrap();
    let (second, _) = context.observe(&cmd).await.unwrap();
    assert_ne!(context.key(&first), context.key(&second));
}

#[tokio::test]
async fn installed_cli_version_uses_final_environment_and_cwd() {
    let (root,context)=fixture(InstalledCli::Claude,"#!/bin/sh\n[ \"$CLAUDE_CODE_EXECUTABLE\" = \"$0\" ] || exit 8\n[ \"$CLAUDE_CODE_OAUTH_TOKEN\" = trusted ] || exit 9\n[ -f ./cwd-marker ] || exit 10\nprintf '2.3.4 (Claude Code)\\n'\n");
    std::fs::write(root.path().join("cwd-marker"), "").unwrap();
    let mut cmd = Command::new("adapter");
    cmd.current_dir(root.path())
        .env("CLAUDE_CODE_OAUTH_TOKEN", "trusted")
        .env("CLAUDE_CODE_EXECUTABLE", "/untrusted/claude");
    context.apply(&mut cmd);
    assert_eq!(
        context.observe(&cmd).await.unwrap().1,
        "2.3.4 (Claude Code)"
    );
}

#[test]
fn installed_cli_env_namespace_custom_credentials_os_values_and_policy_precedence() {
    let (root, context) = fixture(InstalledCli::Codex, "#!/bin/sh\nexit 0\n");
    let captured = BTreeMap::from([
        ("CODEX_CUSTOM_TOKEN".into(), "shell".into()),
        ("GATEWAY_TOKEN".into(), "custom".into()),
        ("HTTPS_PROXY".into(), "shell-proxy".into()),
    ]);
    let mut inherited = context.env.clone();
    let opaque = OsString::from_vec(vec![b'p', 0xff]);
    inherited.insert("HTTPS_PROXY".into(), opaque.clone());
    let names = intent_core::cli_env::CodexEnvNames::from_config(
        "[model_providers.gateway]\nenv_key='GATEWAY_TOKEN'\n",
    )
    .unwrap();
    let context =
        InstalledContext::from_inputs(context.runtime, &captured, inherited, &names).unwrap();
    let mut cmd = Command::new("adapter");
    cmd.env("CODEX_CUSTOM_TOKEN", "trusted")
        .env("CODEX_HOME", root.path().join("isolated"))
        .env("CoDeX_PaTh", "/wrong")
        .env("CODEX_CONFIG", "bad");
    context.apply(&mut cmd);
    assert_eq!(env(&cmd, "CODEX_CUSTOM_TOKEN"), Some("trusted".into()));
    assert_eq!(env(&cmd, "GATEWAY_TOKEN"), Some("custom".into()));
    assert_eq!(env(&cmd, "HTTPS_PROXY"), Some(opaque));
    assert_eq!(
        env(&cmd, "CODEX_HOME"),
        Some(root.path().join("isolated").into_os_string())
    );
    assert_eq!(env(&cmd, "CoDeX_PaTh"), None);
    assert_eq!(
        env(&cmd, "CODEX_PATH"),
        Some(context.runtime.path().as_os_str().into())
    );
    assert_eq!(
        env(&cmd, "CODEX_CONFIG"),
        Some(intent_providers::CODEX_SUBAGENT_POLICY_CONFIG.into())
    );
}

#[tokio::test]
async fn installed_cli_auth_and_config_changes_do_not_reuse_last_good_identity() {
    let (root, context) = fixture(
        InstalledCli::Codex,
        "#!/bin/sh\nprintf 'codex-cli 1.2.3\\n'\n",
    );
    let cmd = launch(&context, root.path());
    let (identity, _) = context.observe(&cmd).await.unwrap();
    let before = context.key(&identity);
    std::fs::create_dir(root.path().join(".codex")).unwrap();
    std::fs::write(
        root.path().join(".codex/auth.json"),
        r#"{"token":"new-private-token"}"#,
    )
    .unwrap();
    let after = InstalledContext::from_inputs(
        context.runtime.clone(),
        &BTreeMap::new(),
        context.env.clone(),
        &intent_core::cli_env::CodexEnvNames::default(),
    )
    .unwrap();
    assert_ne!(before, after.key(&identity));
    assert!(!after.key(&identity).contains("token"));
}

#[tokio::test]
async fn installed_cli_disappearing_never_runs_adapter_or_bundled_cli() {
    let (root, context) = fixture(
        InstalledCli::Codex,
        "#!/bin/sh\nprintf 'codex-cli 1.2.3\\n'\n",
    );
    std::fs::remove_file(context.runtime.path()).unwrap();
    let adapter = root.path().join("adapter");
    executable(&adapter, "#!/bin/sh\ntouch unexpected-adapter\n");
    let cmd =
        crate::acp_adapter::AcpAdapterCommand::binary(adapter, vec![]).cwd(root.path().to_owned());
    assert!(cmd.prepare_with_context(context).await.is_err());
    assert!(!root.path().join("unexpected-adapter").exists());
}

#[tokio::test]
async fn installed_cli_version_output_is_bounded_and_errors_do_not_echo_output() {
    let (root, context) = fixture(
        InstalledCli::Codex,
        "#!/bin/sh\nyes secret-runtime-output\n",
    );
    let error = context
        .observe(&launch(&context, root.path()))
        .await
        .err()
        .unwrap();
    assert!(error.contains("limit"));
    assert!(!error.contains("secret"));
}

#[tokio::test]
async fn installed_cli_ephemeral_isolation_uses_effective_home_before_env_override() {
    let (root, context) = fixture(
        InstalledCli::Codex,
        "#!/bin/sh\n[ -f \"$CODEX_HOME/auth.json\" ] || exit 5\nprintf 'codex-cli 1.2.3\\n'\n",
    );
    std::fs::create_dir(root.path().join(".codex")).unwrap();
    std::fs::write(root.path().join(".codex/auth.json"), "{}").unwrap();
    std::fs::write(
        root.path().join(".codex/config.toml"),
        "model='test-model'\n[mcp_servers.bad]\ncommand='bad'\n",
    )
    .unwrap();
    let cmd = crate::acp_adapter::AcpAdapterCommand::binary(PathBuf::from("/bin/true"), vec![])
        .env("CODEX_HOME", "/untrusted/home")
        .prepare_with_context(context)
        .await
        .unwrap();
    assert!(cmd.installed_key().unwrap().starts_with("installed-v1:"));
}

/// Paired with fake ACP adapters: unit tests must never invoke host credentials
/// or require a developer's installed CLI. The caller owns the fixture home.
pub(crate) fn context_in(cli: InstalledCli, root: &Path) -> InstalledContext {
    std::fs::create_dir_all(root).unwrap();
    executable(&root.join(cli.command()), "#!/bin/sh\nprintf '1.2.3\\n'\n");
    let runtime = cli.resolve_in_dirs(&[root.to_owned()], false).unwrap();
    InstalledContext::from_inputs(
        runtime,
        &BTreeMap::new(),
        BTreeMap::from([
            ("HOME".into(), root.as_os_str().to_owned()),
            (
                "PATH".into(),
                std::env::join_paths(intent_core::path_utils::enhanced_path_dirs()).unwrap(),
            ),
        ]),
        &intent_core::cli_env::CodexEnvNames::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn installed_cli_isolated_profile_outlives_launch_command_and_child_cleanup() {
    use crate::acp_adapter::{spawn_adapter_in, AcpAdapterCommand, AdapterSlots};
    let (root, context) = fixture(
        InstalledCli::Codex,
        "#!/bin/sh\nprintf '%s' \"$CODEX_HOME\" > \"$HOME/profile-location\"\nprintf '1.2.3\\n'\n",
    );
    let cmd = AcpAdapterCommand::binary("/bin/sh".into(), vec!["-c".into(), "sleep 30".into()])
        .prepare_with_context(context)
        .await
        .unwrap();
    let profile =
        PathBuf::from(std::fs::read_to_string(root.path().join("profile-location")).unwrap());
    let slots = AdapterSlots::new(1);
    let mut child = spawn_adapter_in(&slots, &cmd, Duration::from_secs(1))
        .await
        .unwrap()
        .child;
    drop(cmd);
    assert!(profile.is_dir());
    assert_eq!(slots.available(), 0);
    child.reap().await;
    assert!(!profile.exists());
    assert_eq!(slots.available(), 1);
}

#[tokio::test]
async fn installed_cli_version_timeout_reaps_process_group() {
    let (root, context) = fixture(
        InstalledCli::Codex,
        "#!/bin/sh\nsleep 30 &\necho $! > \"$HOME/child-pid\"\nwait\n",
    );
    let error = context
        .observe(&launch(&context, root.path()))
        .await
        .err()
        .unwrap();
    assert!(error.contains("timed out"));
    let pid: i32 = std::fs::read_to_string(root.path().join("child-pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    // A killed descendant can briefly be a zombie until its new parent reaps it.
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
        assert!(
            stat.is_empty()
                || stat
                    .split_once(") ")
                    .is_some_and(|(_, s)| s.starts_with('Z'))
        );
    }
    #[cfg(not(target_os = "linux"))]
    assert_ne!(unsafe { libc::kill(pid, 0) }, 0);
}

#[tokio::test]
async fn installed_cli_prepared_catalog_rechecks_wrapper_before_launch() {
    use crate::acp_adapter::{spawn_adapter_in, AcpAdapterCommand, AdapterSlots};
    let (root, context) = fixture(InstalledCli::Claude, "#!/bin/sh\ncat \"$HOME/version\"\n");
    std::fs::write(root.path().join("version"), "1.2.3").unwrap();
    let cmd = AcpAdapterCommand::binary("/bin/true".into(), vec![])
        .prepare_with_context(context)
        .await
        .unwrap();
    std::fs::write(root.path().join("version"), "1.2.4").unwrap();
    let error = spawn_adapter_in(&AdapterSlots::new(1), &cmd, Duration::from_secs(1))
        .await
        .err()
        .unwrap();
    assert!(
        matches!(error, crate::acp_adapter::SpawnError::Spawn(message)
        if message == crate::provider_models::INSTALLED_SOURCE_CHANGED)
    );
}

#[cfg(target_os = "linux")]
async fn detached_version_children(mode: &str) {
    struct Cleanup(i32);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            unsafe {
                libc::kill(self.0, libc::SIGKILL);
            }
        }
    }
    let (root, context) = fixture(
        InstalledCli::Claude,
        r"#!/usr/bin/python3
import subprocess,os,pathlib,time
child=subprocess.Popen(['/bin/sleep','120'],start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
pathlib.Path(os.environ['HOME'],'detached-pid').write_text(str(child.pid))
if os.environ['VERSION_MODE']!='success':time.sleep(120)
print('2.0.0 (Claude Code)')
",
    );
    let mut unrelated = Command::new("/bin/sleep")
        .arg("120")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut command = launch(&context, root.path());
    command.env("VERSION_MODE", mode);
    let mut operation = tokio::spawn(async move { context.observe(&command).await.map(|_| ()) });
    let file = root.path().join("detached-pid");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !file.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pid: i32 = std::fs::read_to_string(file).unwrap().parse().unwrap();
    let emergency = Cleanup(pid);
    if mode == "cancel" {
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
    } else {
        let result = (&mut operation).await.unwrap();
        if mode == "success" {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(result.unwrap_err().contains("timed out"));
        }
    }
    let cleaned = tokio::time::timeout(Duration::from_secs(7), async {
        while std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|s| {
            !s.rsplit_once(") ")
                .is_some_and(|(_, rest)| rest.starts_with('Z'))
        }) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        unrelated.try_wait().unwrap().is_none(),
        "unrelated process was killed"
    );
    unrelated.kill().await.unwrap();
    drop(emergency);
    assert!(cleaned, "{mode} left detached version child alive");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn installed_cli_successful_version_reaps_detached_children() {
    detached_version_children("success").await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn installed_cli_timed_out_version_reaps_detached_children() {
    detached_version_children("timeout").await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn installed_cli_cancelled_version_reaps_detached_children() {
    detached_version_children("cancel").await;
}

// Exercise the macOS fallback on Unix CI too. This only promises ownership of
// the direct child and its process group, not detached descendant containment.
async fn ordinary_version_children(mode: &str) {
    let root = tempfile::tempdir().unwrap();
    let pid_file = root.path().join("pid");
    let mut command = Command::new("/bin/sh");
    command.args(["-c", "sleep 120 </dev/null >/dev/null 2>&1 & echo $! > \"$PID_FILE\"; if [ \"$VERSION_MODE\" = success ]; then printf '1.2.3\\n'; else wait; fi"])
        .env("PID_FILE", &pid_file).env("VERSION_MODE", mode);
    let operation = tokio::spawn(version_output_uncontained(command));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !pid_file.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pid: i32 = std::fs::read_to_string(pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    if mode == "cancel" {
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
    } else {
        let result = operation.await.unwrap();
        if mode == "success" {
            assert_eq!(result.unwrap(), b"1.2.3\n");
        } else {
            assert!(result.unwrap_err().contains("timed out"));
        }
    }
    let result = tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            #[cfg(target_os = "linux")]
            let live = std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|s| {
                !s.rsplit_once(") ")
                    .is_some_and(|(_, rest)| rest.starts_with('Z'))
            });
            #[cfg(not(target_os = "linux"))]
            let live = unsafe { libc::kill(pid, 0) } == 0;
            if !live {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if result.is_err() {
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
    }
    assert!(result.is_ok(), "{mode}: ordinary child survived cleanup");
}

#[tokio::test]
async fn installed_cli_macos_fallback_success_reaps_process_group() {
    ordinary_version_children("success").await;
}
#[tokio::test]
async fn installed_cli_macos_fallback_timeout_reaps_process_group() {
    ordinary_version_children("timeout").await;
}
#[tokio::test]
async fn installed_cli_macos_fallback_cancellation_reaps_process_group() {
    ordinary_version_children("cancel").await;
}
