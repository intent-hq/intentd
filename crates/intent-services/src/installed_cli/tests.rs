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

async fn assert_ordinary_launch(context: InstalledContext, root: &Path) {
    use crate::acp_adapter::{spawn_adapter_in, AcpAdapterCommand, AdapterSlots};
    let selected = context.runtime.path().to_owned();
    let path_env = context.runtime.cli().path_env();
    let command = launch(&context, root);
    assert_eq!(env(&command, path_env), Some(selected.into_os_string()));
    context.observe(&command).await.unwrap();
    let cmd = AcpAdapterCommand::binary(
        "/bin/sh".into(),
        vec!["-c".into(), "printf launched > launch-marker".into()],
    )
    .cwd(root.to_owned())
    .prepare_with_context(context)
    .await
    .unwrap();
    assert!(
        cmd.installed_key().is_none(),
        "ordinary launches have no catalog identity"
    );
    let mut child = spawn_adapter_in(&AdapterSlots::new(1), &cmd, Duration::from_secs(1))
        .await
        .unwrap()
        .child;
    tokio::time::timeout(Duration::from_secs(3), async {
        while !root.join("launch-marker").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    child.reap().await;
}

#[tokio::test]
async fn installed_cli_launch_accepts_oversized_claude_config() {
    let (root, context) = fixture(InstalledCli::Claude, "#!/bin/sh\nprintf '2.0.0\\n'\n");
    let config = serde_json::json!({"history": "x".repeat(1024 * 1024)});
    std::fs::write(root.path().join(".claude.json"), config.to_string()).unwrap();
    let context = InstalledContext::from_inputs(
        context.runtime,
        &BTreeMap::new(),
        context.env,
        &context.names,
    )
    .unwrap_or_else(|error| panic!("catalog inspection must not block launch: {error}"));
    let error = context
        .clone()
        .with_catalog_fingerprint()
        .await
        .err()
        .unwrap();
    assert!(error.contains("inspection limit"));
    assert_ordinary_launch(context, root.path()).await;
}

#[tokio::test]
async fn installed_cli_launch_ignores_unreadable_other_provider_file() {
    let (root, context) = fixture(InstalledCli::Codex, "#!/bin/sh\nprintf '1.2.3\\n'\n");
    // A directory is deterministically unreadable as a credential file, even
    // when tests run with privileges that bypass ordinary file permissions.
    let mut inherited = context.env;
    inherited.insert(
        "CLAUDE_CODE_CLIENT_KEY".into(),
        root.path().as_os_str().to_owned(),
    );
    let context =
        InstalledContext::from_inputs(context.runtime, &BTreeMap::new(), inherited, &context.names)
            .unwrap_or_else(|error| panic!("unrelated credentials must not block launch: {error}"));
    context.clone().with_catalog_fingerprint().await.unwrap();
    assert_ordinary_launch(context, root.path()).await;
}

#[tokio::test]
async fn installed_cli_catalog_retains_explicit_custom_credential_file_inputs() {
    let (root, context) = fixture(InstalledCli::Codex, "#!/bin/sh\nprintf '1.2.3\\n'\n");
    let mut inherited = context.env;
    inherited.insert(
        "AWS_SHARED_CREDENTIALS_FILE".into(),
        root.path().as_os_str().to_owned(),
    );
    let names = intent_core::cli_env::CodexEnvNames::from_config(
        "[model_providers.gateway]\nenv_key='AWS_SHARED_CREDENTIALS_FILE'\n",
    )
    .unwrap();
    let context =
        InstalledContext::from_inputs(context.runtime, &BTreeMap::new(), inherited, &names)
            .unwrap();
    let command = launch(&context, root.path());
    assert_eq!(
        env(&command, "AWS_SHARED_CREDENTIALS_FILE"),
        Some(root.path().as_os_str().to_owned())
    );
    assert!(context.clone().with_catalog_fingerprint().await.is_err());
    assert_ordinary_launch(context, root.path()).await;
}

#[tokio::test]
async fn installed_cli_wrapper_upgrade_changes_catalog_identity_without_adapter_change() {
    let (root, context) = fixture(
        InstalledCli::Codex,
        "#!/bin/sh\nprintf 'codex-cli '; cat \"$HOME/version\"\n",
    );
    let context = context.with_catalog_fingerprint().await.unwrap();
    std::fs::write(root.path().join("version"), "1.2.3").unwrap();
    let cmd = launch(&context, root.path());
    let (first, _) = context.observe(&cmd).await.unwrap();
    std::fs::write(root.path().join("version"), "1.2.4").unwrap();
    let (second, _) = context.observe(&cmd).await.unwrap();
    assert_ne!(context.key(&first).unwrap(), context.key(&second).unwrap());
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
    let context = context.with_catalog_fingerprint().await.unwrap();
    let cmd = launch(&context, root.path());
    let (identity, _) = context.observe(&cmd).await.unwrap();
    let before = context.key(&identity).unwrap();
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
    .unwrap()
    .with_catalog_fingerprint()
    .await
    .unwrap();
    assert_ne!(before, after.key(&identity).unwrap());
    assert!(!after.key(&identity).unwrap().contains("token"));
}

async fn claude_catalog_key(
    context: &InstalledContext,
    identity: &InstalledCliIdentity,
    state: &str,
) -> String {
    std::fs::write(
        PathBuf::from(context.env.get(std::ffi::OsStr::new("HOME")).unwrap()).join(".claude.json"),
        state,
    )
    .unwrap();
    InstalledContext::from_inputs(
        context.runtime.clone(),
        &BTreeMap::new(),
        context.env.clone(),
        &context.names,
    )
    .unwrap()
    .with_catalog_fingerprint()
    .await
    .unwrap()
    .key(identity)
    .unwrap()
}

#[tokio::test]
async fn installed_cli_claude_state_churn_keeps_catalog_identity() {
    let (root, context) = fixture(InstalledCli::Claude, "#!/bin/sh\nprintf '2.0.0\\n'\n");
    let context = context.with_catalog_fingerprint().await.unwrap();
    let (identity, _) = context
        .observe(&launch(&context, root.path()))
        .await
        .unwrap();
    let base = claude_catalog_key(
        &context,
        &identity,
        r#"{"numStartups":1,"cachedGrowthBookFeaturesAt":1,"oauthAccount":{"accountUuid":"a","organizationUuid":"o","profileFetchedAt":1},"modelAccessCache":[]}"#,
    )
    .await;
    // Claude Code rewrites these during ordinary runs, including model discovery.
    let churned = claude_catalog_key(
        &context,
        &identity,
        r#"{"oauthAccount":{"profileFetchedAt":2,"organizationUuid":"o","accountUuid":"a"},"cachedGrowthBookFeaturesAt":2,"numStartups":2,"modelAccessCache":["x"],"tipsHistory":{"t":3}}"#,
    )
    .await;
    assert_eq!(base, churned);

    for meaningful in [
        r#"{"oauthAccount":{"accountUuid":"b","organizationUuid":"o"}}"#,
        r#"{"oauthAccount":{"accountUuid":"a","organizationUuid":"p"}}"#,
        r#"{"oauthAccount":{"accountUuid":"a","organizationUuid":"o"},"primaryApiKey":"k"}"#,
        r#"{"oauthAccount":{"accountUuid":"a","organizationUuid":"o"},"customApiKeyResponses":{"approved":["k"]}}"#,
        "{}",
        "not json",
    ] {
        let changed = claude_catalog_key(&context, &identity, meaningful).await;
        assert_ne!(base, changed, "{meaningful} must invalidate the catalog");
    }
    let unparseable = claude_catalog_key(&context, &identity, "not json").await;
    assert_ne!(
        unparseable,
        claude_catalog_key(&context, &identity, "not json either").await
    );

    let before_settings = claude_catalog_key(
        &context,
        &identity,
        r#"{"oauthAccount":{"accountUuid":"a","organizationUuid":"o"}}"#,
    )
    .await;
    assert_eq!(base, before_settings);
    std::fs::create_dir(root.path().join(".claude")).unwrap();
    std::fs::write(
        root.path().join(".claude/settings.json"),
        r#"{"model":"opus"}"#,
    )
    .unwrap();
    let after_settings = claude_catalog_key(
        &context,
        &identity,
        r#"{"oauthAccount":{"accountUuid":"a","organizationUuid":"o"}}"#,
    )
    .await;
    assert_ne!(base, after_settings);

    // Explicit credential-file inputs keep raw bytes even when named `.claude.json`.
    let credential = root.path().join("credentials/.claude.json");
    std::fs::create_dir(credential.parent().unwrap()).unwrap();
    let mut env = context.env.clone();
    env.insert(
        "GOOGLE_APPLICATION_CREDENTIALS".into(),
        credential.clone().into_os_string(),
    );
    let credential_key = |json: &'static str| {
        std::fs::write(&credential, json).unwrap();
        let (runtime, env, names) = (context.runtime.clone(), env.clone(), &context.names);
        let identity = &identity;
        async move {
            InstalledContext::from_inputs(runtime, &BTreeMap::new(), env, names)
                .unwrap()
                .with_catalog_fingerprint()
                .await
                .unwrap()
                .key(identity)
                .unwrap()
        }
    };
    let first = credential_key(r#"{"client_email":"a@example.com"}"#).await;
    let second = credential_key(r#"{"client_email":"b@example.com"}"#).await;
    assert_ne!(first, second);
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
        .prepare_with_context(context.with_catalog_fingerprint().await.unwrap())
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
const DETACHED_VERSION_FIXTURE: &str = r"#!/usr/bin/python3
import subprocess,os,pathlib,time,json
child=subprocess.Popen(['/bin/sleep','120'],start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
root=pathlib.Path(os.environ['HOME'])
controlled=(root/'observe-publication').exists()
staging=root/'detached-pid.staging'
with staging.open('w') as output:
    if controlled:
        try:
            (root/'detached-pid').stat()
            published=True
        except FileNotFoundError:
            published=False
        (root/'publication-observed').write_text(json.dumps(dict(home=str(root),pid=child.pid,published=published)))
    output.write(str(child.pid))
os.replace(staging,root/'detached-pid')
if controlled: (root/'publication-finished').touch()
if os.environ['VERSION_MODE']!='success':time.sleep(120)
print('2.0.0 (Claude Code)')
";

// A pidfd can only signal the process we inspected, even if its numeric PID
// has since been reused. The unique fixture HOME excludes unrelated processes.
#[cfg(target_os = "linux")]
struct DetachedCleanup(Option<std::os::fd::OwnedFd>);

#[cfg(target_os = "linux")]
impl DetachedCleanup {
    fn capture(pid: i32, root: &Path) -> Result<Self, String> {
        Self::capture_with(pid, root, |pid| {
            std::fs::read(format!("/proc/{pid}/environ"))
        })
    }

    fn capture_with(
        pid: i32,
        root: &Path,
        read_identity: impl FnOnce(i32) -> std::io::Result<Vec<u8>>,
    ) -> Result<Self, String> {
        use std::os::fd::FromRawFd;
        use std::os::unix::ffi::OsStrExt;
        if pid <= 1 {
            return Err(format!("invalid detached PID: {pid}"));
        }
        // SAFETY: pidfd_open has no pointer arguments and returns an owned fd.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(Self(None))
            } else {
                Err(format!("open detached pidfd: {error}"))
            };
        }
        // Linux returns descriptors in the C int range; reject an invalid
        // syscall result before assigning ownership rather than truncating it.
        let fd = i32::try_from(fd).map_err(|error| format!("invalid detached pidfd: {error}"))?;
        // SAFETY: a successful pidfd_open returned a new descriptor.
        let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
        let identity = read_identity(pid);
        // An unreaped zombie can deny environ access even though it is ours.
        // Only the pinned process's terminal state proves there is no target;
        // EACCES, ENOENT and an empty environment alone prove nothing. Check
        // after reading so an exit/PID reuse during that read cannot grant
        // signal authority from another process's environment.
        let retired = Self::pidfd_retired(&fd)?;
        match identity {
            Err(error)
                if !matches!(
                    error.kind(),
                    std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
                ) =>
            {
                Err(format!("inspect detached child identity: {error}"))
            }
            _ if retired => Ok(Self(None)),
            Ok(bytes) => {
                let mut home = b"HOME=".to_vec();
                home.extend_from_slice(root.as_os_str().as_bytes());
                if bytes.split(|byte| *byte == 0).any(|entry| entry == home) {
                    Ok(Self(Some(fd)))
                } else {
                    Err(format!(
                        "PID {pid} does not belong to fixture {}",
                        root.display()
                    ))
                }
            }
            Err(error) => Err(format!("inspect detached child identity: {error}")),
        }
    }

    fn pidfd_retired(fd: &std::os::fd::OwnedFd) -> Result<bool, String> {
        use std::os::fd::AsRawFd;
        let mut poll = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll points to one initialized descriptor and never blocks.
        let result = if unsafe { libc::poll(&raw mut poll, 1, 0) } < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(poll.revents)
        };
        Self::polled_retirement(result)
    }

    fn polled_retirement(result: std::io::Result<i16>) -> Result<bool, String> {
        let events = result.map_err(|error| format!("poll detached pidfd: {error}"))?;
        if events & !(libc::POLLIN | libc::POLLHUP) != 0 {
            return Err(format!("unexpected detached pidfd events: {events}"));
        }
        Ok(events & (libc::POLLIN | libc::POLLHUP) != 0)
    }

    fn retired(&self) -> Result<bool, String> {
        self.0.as_ref().map_or(Ok(true), Self::pidfd_retired)
    }
}

#[cfg(target_os = "linux")]
impl Drop for DetachedCleanup {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        if let Some(fd) = &self.0 {
            // SAFETY: the fd owns the verified child identity; null siginfo
            // requests ordinary signal delivery, including if the PID changed.
            unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    fd.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                );
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn detached_identity_child(root: &Path) -> intentd_test_support::GuardedChild {
    intentd_test_support::GuardedChild::spawn(
        std::process::Command::new("/bin/sleep")
            .arg("120")
            .env("HOME", root),
    )
    .unwrap()
}

#[cfg(target_os = "linux")]
fn detached_identity_zombie(child: &mut intentd_test_support::GuardedChild) {
    child.kill().unwrap();
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::uninit();
    // SAFETY: info is writable; WNOWAIT leaves our owned child unreaped so
    // its PID cannot be reused while the regression inspects the zombie.
    assert_eq!(
        unsafe {
            libc::waitid(
                libc::P_PID,
                child.id(),
                info.as_mut_ptr(),
                libc::WEXITED | libc::WNOWAIT,
            )
        },
        0
    );
}

#[cfg(target_os = "linux")]
#[test]
fn installed_cli_detached_identity_observes_unreaped_child_exit() {
    let root = tempfile::tempdir().unwrap();
    let mut child = detached_identity_child(root.path());
    let pid = i32::try_from(child.id()).unwrap();
    let live = DetachedCleanup::capture(pid, root.path()).unwrap();
    assert!(live.0.is_some());
    detached_identity_zombie(&mut child);
    let identity_errno = std::fs::read(format!("/proc/{pid}/environ"))
        .err()
        .and_then(|error| error.raw_os_error());
    let retired = DetachedCleanup::capture(pid, root.path());
    child.wait().unwrap();
    drop(live);
    eprintln!("owned zombie identity errno: {identity_errno:?}");
    assert!(
        retired
            .unwrap_or_else(|error| panic!("confirmed exited child: {error}"))
            .0
            .is_none(),
        "a retired child must never become a signal target"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn installed_cli_detached_identity_observes_exit_during_inspection() {
    let root = tempfile::tempdir().unwrap();
    let mut child = detached_identity_child(root.path());
    let pid = i32::try_from(child.id()).unwrap();
    let retired = DetachedCleanup::capture_with(pid, root.path(), |pid| {
        detached_identity_zombie(&mut child);
        std::fs::read(format!("/proc/{pid}/environ"))
    });
    child.wait().unwrap();
    assert!(retired.unwrap().0.is_none());
}

#[cfg(target_os = "linux")]
#[test]
fn installed_cli_detached_identity_rejects_live_unverified_children() {
    let root = tempfile::tempdir().unwrap();
    let mut child = detached_identity_child(root.path());
    let pid = i32::try_from(child.id()).unwrap();
    for identity in [
        Err(std::io::Error::from_raw_os_error(libc::EACCES)),
        Err(std::io::Error::from_raw_os_error(libc::ENOENT)),
        Err(std::io::Error::from_raw_os_error(libc::EIO)),
        Ok(Vec::new()),
        Ok(b"HOME=/unrelated-fixture\0".to_vec()),
    ] {
        let captured = DetachedCleanup::capture_with(pid, root.path(), |_| identity);
        assert!(captured.is_err(), "unverified live child accepted");
        drop(captured);
        assert!(child.try_wait().unwrap().is_none());
    }
    child.kill().unwrap();
    child.wait().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn installed_cli_detached_identity_preserves_unknown_errors_after_exit() {
    let root = tempfile::tempdir().unwrap();
    let mut child = detached_identity_child(root.path());
    let pid = i32::try_from(child.id()).unwrap();
    let captured = DetachedCleanup::capture_with(pid, root.path(), |_| {
        detached_identity_zombie(&mut child);
        Err(std::io::Error::from_raw_os_error(libc::EIO))
    });
    child.wait().unwrap();
    assert!(
        captured.is_err(),
        "exit must not hide an unknown read error"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn installed_cli_detached_identity_requires_positive_terminal_evidence() {
    assert_eq!(DetachedCleanup::polled_retirement(Ok(0)), Ok(false));
    for events in [libc::POLLIN, libc::POLLHUP, libc::POLLIN | libc::POLLHUP] {
        assert_eq!(DetachedCleanup::polled_retirement(Ok(events)), Ok(true));
    }
    for events in [libc::POLLERR, libc::POLLNVAL, libc::POLLIN | libc::POLLERR] {
        assert!(DetachedCleanup::polled_retirement(Ok(events)).is_err());
    }
    for errno in [libc::EINTR, libc::EIO] {
        assert!(
            DetachedCleanup::polled_retirement(Err(std::io::Error::from_raw_os_error(errno)))
                .is_err()
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn installed_cli_detached_identity_cleanup_signals_only_verified_child() {
    let root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut child = detached_identity_child(root.path());
    let mut unrelated = detached_identity_child(other.path());
    let pid = i32::try_from(child.id()).unwrap();
    let guard = DetachedCleanup::capture(pid, root.path()).unwrap();
    assert!(!guard.retired().unwrap());
    drop(guard);
    let retired = child.wait_with_timeout(Duration::from_secs(3)).unwrap();
    let unrelated_alive = unrelated.try_wait().unwrap().is_none();
    unrelated.kill().unwrap();
    unrelated.wait().unwrap();
    assert!(
        retired.is_some(),
        "verified child survived emergency cleanup"
    );
    assert!(unrelated_alive, "unrelated child was killed");
}

#[cfg(target_os = "linux")]
fn read_detached_publication(root: &Path, pid: i32) -> Result<bool, String> {
    let bytes = std::fs::read(root.join("publication-observed")).map_err(|e| e.to_string())?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    let published = value["published"]
        .as_bool()
        .ok_or("publication observation has no boolean result")?;
    if value != serde_json::json!({"home": root, "pid": pid, "published": published}) {
        return Err("publication observation does not match this fixture invocation".into());
    }
    Ok(published)
}

#[cfg(target_os = "linux")]
#[test]
fn installed_cli_detached_publication_requires_complete_owned_observation() {
    let root = tempfile::tempdir().unwrap();
    let pid = 123;
    assert!(read_detached_publication(root.path(), pid).is_err());
    for invalid in [
        "not json".to_owned(),
        "{}".to_owned(),
        serde_json::json!({"home": "/other", "pid": pid, "published": false}).to_string(),
        serde_json::json!({"home": root.path(), "pid": pid + 1, "published": false}).to_string(),
        serde_json::json!({"home": root.path(), "pid": pid, "published": "false"}).to_string(),
    ] {
        std::fs::write(root.path().join("publication-observed"), invalid).unwrap();
        assert!(read_detached_publication(root.path(), pid).is_err());
    }
    for published in [false, true] {
        std::fs::write(
            root.path().join("publication-observed"),
            serde_json::json!({"home": root.path(), "pid": pid, "published": published})
                .to_string(),
        )
        .unwrap();
        assert_eq!(read_detached_publication(root.path(), pid), Ok(published));
    }
}

#[cfg(target_os = "linux")]
async fn detached_version_children(mode: &str, controlled: bool) {
    detached_version_children_from(mode, controlled, DETACHED_VERSION_FIXTURE).await;
}

#[cfg(target_os = "linux")]
async fn detached_version_children_from(mode: &str, controlled: bool, body: &str) {
    let (root, context) = fixture(InstalledCli::Claude, body);
    if controlled {
        std::fs::write(root.path().join("observe-publication"), "").unwrap();
    }
    let mut unrelated = Command::new("/bin/sleep")
        .arg("120")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut command = launch(&context, root.path());
    command.env("VERSION_MODE", mode);
    let mut operation = tokio::spawn(async move { context.observe(&command).await.map(|_| ()) });
    let file = root.path().join("detached-pid");
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        while !file.exists() || (controlled && !root.path().join("publication-finished").exists()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let child = ready.map_err(|e| e.to_string()).and_then(|()| {
        let text = std::fs::read_to_string(&file).map_err(|e| e.to_string())?;
        let pid = text
            .parse::<i32>()
            .map_err(|e| format!("PID {text:?}: {e}"))?;
        DetachedCleanup::capture(pid, root.path()).map(|guard| (pid, guard))
    });
    // Readiness/read/parse failures still join observe, which owns bounded
    // descendant cleanup. Aborting the caller alone would detach that owner.
    let cancelled = mode == "cancel" && child.is_ok();
    if cancelled {
        operation.abort();
    }
    let result = (&mut operation).await;
    // Observe at the open-before-write point inside the fixture, then read the
    // retained result after joining its owner. No scheduler-dependent hold has
    // to fit inside the production version deadline. Opening the final file
    // instead of staging still deterministically records published=true and fails.
    let publication = controlled.then(|| {
        child
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|(pid, _)| read_detached_publication(root.path(), *pid))
    });
    let cleaned = if let Ok((_, guard)) = &child {
        tokio::time::timeout(Duration::from_secs(7), async {
            while !guard.retired()? {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, String>(())
        })
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result)
    } else {
        Err("child identity was not captured; retirement not observed".into())
    };
    let unrelated_alive = unrelated.try_wait().map(|status| status.is_none());
    let unrelated_reaped = unrelated.kill().await;
    let identity = child.map(|(pid, emergency)| {
        drop(emergency);
        pid
    });
    eprintln!(
        "detached fixture: mode={mode} controlled={controlled} pid={identity:?} publication={publication:?} owner={result:?} retired={cleaned:?} unrelated_alive={unrelated_alive:?} unrelated_reaped={unrelated_reaped:?}"
    );
    // All assertions are after owner completion, emergency cleanup,
    // and unrelated kill/reap, including the original pre-parse failure path.
    let _pid = identity.expect("detached child PID readiness and identity");
    if cancelled {
        assert!(result.unwrap_err().is_cancelled());
    } else {
        let result = result.unwrap();
        if mode == "success" {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(result.unwrap_err().contains("timed out"));
        }
    }
    assert!(unrelated_alive.unwrap(), "unrelated process was killed");
    unrelated_reaped.unwrap();
    cleaned.unwrap_or_else(|error| panic!("{mode} detached child retirement: {error}"));
    if let Some(publication) = publication {
        assert!(
            !publication.expect("publication observation"),
            "detached PID was published before its contents were complete"
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn installed_cli_successful_version_reaps_detached_children() {
    detached_version_children("success", false).await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn installed_cli_timed_out_version_reaps_detached_children() {
    detached_version_children("timeout", false).await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn installed_cli_cancelled_version_reaps_detached_children() {
    detached_version_children("cancel", false).await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn installed_cli_detached_pid_is_published_only_when_complete() {
    detached_version_children("success", true).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[should_panic(expected = "detached PID was published before its contents were complete")]
async fn installed_cli_detached_publication_observer_rejects_early_exposure() {
    let early = DETACHED_VERSION_FIXTURE.replace(
        "staging=root/'detached-pid.staging'",
        "staging=root/'detached-pid'",
    );
    detached_version_children_from("success", true, &early).await;
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

#[cfg(target_os = "linux")]
pub(crate) fn owner_failure_context(root: &Path) -> InstalledContext {
    let context = context_in(InstalledCli::Codex, root);
    executable(
        &root.join("codex"),
        r"#!/usr/bin/python3
import pathlib,os,signal,time
root=pathlib.Path(os.environ['HOME'])
(root/'version-profile').write_text(os.environ['CODEX_HOME'])
(root/'version-cwd').write_text(os.getcwd())
if (root/'hang-owner').exists(): time.sleep(30)
if (root/'fail-owner').exists():
    parent=os.getppid()
    assert b'intentd-codex-diagnostic' in pathlib.Path(f'/proc/{parent}/cmdline').read_bytes()
    os.kill(parent,signal.SIGKILL)
print('codex-cli 1.2.3')
",
    );
    context
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn installed_cli_preparation_retains_actual_directories_on_owner_failure() {
    for fail in [false, true] {
        let root = crate::test_support::test_tempdir("installed-version-profile");
        let context = owner_failure_context(root.path());
        if fail {
            std::fs::write(root.path().join("fail-owner"), "").unwrap();
        }
        let result = crate::acp_adapter::AcpAdapterCommand::npx(
            root.path().join("npx"),
            intent_providers::CODEX_ACP_NPX_PACKAGE,
        )
        .prepare_with_context(context)
        .await;
        assert_eq!(
            result.is_err(),
            fail,
            "owner failure={fail}: {:?}",
            result.as_ref().err()
        );
        let profile =
            PathBuf::from(std::fs::read_to_string(root.path().join("version-profile")).unwrap());
        let cwd = PathBuf::from(std::fs::read_to_string(root.path().join("version-cwd")).unwrap());
        drop(result);
        let retained = (profile.exists(), cwd.exists());
        if fail {
            let _ = std::fs::remove_dir_all(&profile);
            let _ = std::fs::remove_dir_all(&cwd);
        }
        assert_eq!(
            retained,
            (fail, fail),
            "retain the actual profile and launch directory only when cleanup is unconfirmed"
        );
    }
}

#[cfg(target_os = "linux")]
pub(crate) async fn cancel_version_owner(operation: tokio::task::JoinHandle<()>, root: &Path) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !root.join("version-cwd").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let profile = PathBuf::from(std::fs::read_to_string(root.join("version-profile")).unwrap());
    let cwd = PathBuf::from(std::fs::read_to_string(root.join("version-cwd")).unwrap());
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    assert!(profile.is_dir());
    assert!(cwd.is_dir());
    tokio::time::timeout(Duration::from_secs(8), async {
        while profile.exists() || cwd.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual profile and launch directory released after confirmed cleanup");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn installed_cli_cancelled_preparation_releases_actual_directories_after_cleanup() {
    let root = crate::test_support::test_tempdir("installed-version-cancel");
    let context = owner_failure_context(root.path());
    std::fs::write(root.path().join("hang-owner"), "").unwrap();
    let npx = root.path().join("npx");
    let operation = tokio::spawn(async move {
        let _ = crate::acp_adapter::AcpAdapterCommand::npx(
            npx,
            intent_providers::CODEX_ACP_NPX_PACKAGE,
        )
        .prepare_with_context(context)
        .await;
    });
    cancel_version_owner(operation, root.path()).await;
}

#[tokio::test]
async fn codex_prelaunch_rejects_confirmed_old_but_allows_newer_unknown_and_other_cli() {
    let minimum = intent_providers::adapter_cli::requirement("codex")
        .unwrap()
        .minimum
        .to_string();
    for (cli, version, rejected) in [
        (InstalledCli::Codex, "codex-cli 0.114.0", true),
        (InstalledCli::Codex, minimum.as_str(), false),
        (InstalledCli::Codex, "codex-cli 99.0.0", false),
        (InstalledCli::Codex, "unknown", false),
        (InstalledCli::Claude, "0.0.1", false),
    ] {
        let (root, context) = fixture(cli, &format!("#!/bin/sh\nprintf '%s' '{version}'\n"));
        let prepared = crate::acp_adapter::AcpAdapterCommand::binary(
            "/must-not-launch-adapter".into(),
            vec![],
        )
        .cwd(root.path().to_owned())
        .prepare_with_context(context)
        .await;
        if rejected {
            let Err(error) = prepared else {
                panic!("old Codex was accepted");
            };
            assert!(error.contains("0.114.0"), "{error}");
            assert!(error.contains(&minimum), "{error}");
            assert!(error.contains("Upgrade"), "{error}");
        } else {
            assert!(
                prepared.is_ok(),
                "{version}: {}",
                prepared.err().unwrap_or_default()
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires INTENT_CODEX_OLD_TEST_BINARY pointing to a real below-minimum Codex CLI"]
async fn codex_real_old_runtime_is_rejected_before_adapter_start() {
    let binary = std::env::var_os("INTENT_CODEX_OLD_TEST_BINARY").expect("set old Codex path");
    let (root, context) = fixture(InstalledCli::Codex, "#!/bin/sh\nexit 99\n");
    std::fs::remove_file(root.path().join("codex")).unwrap();
    std::os::unix::fs::symlink(binary, root.path().join("codex")).unwrap();
    let result =
        crate::acp_adapter::AcpAdapterCommand::binary("/must-not-launch-adapter".into(), vec![])
            .cwd(root.path().to_owned())
            .prepare_with_context(context)
            .await;
    let Err(error) = result else {
        panic!("old runtime reached adapter preparation");
    };
    let requirement = intent_providers::adapter_cli::requirement("codex").unwrap();
    assert!(error.contains("0.114.0"), "{error}");
    assert!(error.contains(&requirement.minimum.to_string()), "{error}");
    println!("{error}");
}
