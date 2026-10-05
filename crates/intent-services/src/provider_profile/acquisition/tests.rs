use super::*;
use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt as _;

struct Fixture {
    root: tempfile::TempDir,
    workspace: PathBuf,
    etc: PathBuf,
    config: PathBuf,
    env: BTreeMap<String, String>,
}
impl Fixture {
    fn new() -> Self {
        let root = super::super::tests::scratch();
        let workspace = root.path().join("repo");
        let etc = root.path().join("etc");
        let config = root.path().join("home/.claude");
        for path in [&workspace, &etc.join("claude-code"), &config] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::write(
            config.join("settings.json"),
            r#"{"model":"claude-sonnet-4-6","hooks":{"SessionStart":[{"command":"never-run"}]}}"#,
        )
        .unwrap();
        let env = BTreeMap::from([
            (
                "HOME".into(),
                root.path().join("home").to_str().unwrap().into(),
            ),
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("ANTHROPIC_BASE_URL".into(), "http://127.0.0.1:1".into()),
            ("ANTHROPIC_API_KEY".into(), "synthetic-literal-$key".into()),
        ]);
        Self {
            root,
            workspace,
            etc,
            config,
            env,
        }
    }
    fn context(&self, native: Option<&Path>) -> InstalledContext {
        let bin = self.root.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let executable = bin.join("claude");
        if let Some(native) = native {
            std::os::unix::fs::symlink(native, &executable).unwrap();
        } else {
            std::fs::write(
                &executable,
                "#!/bin/sh\n[ \"$1\" = --version ] || exit 2\nprintf '2.1.280 (Claude Code)\\n'\n",
            )
            .unwrap();
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let runtime = InstalledCli::Claude.resolve_in_dirs(&[bin], false).unwrap();
        // Model the actual service precedence: captured login-shell credentials,
        // inherited HOME/PATH, and authoritative selected executable.
        let captured = self
            .env
            .iter()
            .filter(|(k, _)| k.starts_with("ANTHROPIC_"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let inherited: BTreeMap<OsString, OsString> = self
            .env
            .iter()
            .filter(|(k, _)| !k.starts_with("ANTHROPIC_"))
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        InstalledContext::from_inputs(
            runtime,
            &captured,
            inherited,
            &intent_core::cli_env::CodexEnvNames::default(),
        )
        .unwrap()
    }
    async fn acquire(&self, native: Option<&Path>) -> NativeAcquisition {
        acquire_context(
            self.context(native),
            self.workspace.clone(),
            self.etc.clone(),
        )
        .await
    }
    fn inspect(&self) -> Result<(AuthModelContext, ConfigurationIdentity), DeferredReason> {
        inspect_sources(&self.env, &self.workspace, &self.etc)
    }
}
fn ready(result: NativeAcquisition) -> AcquiredNativeProfile {
    match result {
        NativeAcquisition::Ready(acquired) => *acquired,
        NativeAcquisition::Deferred(reason) => panic!("unexpected deferral: {reason:?}"),
    }
}
fn inputs(servers: &NormalizedMcpServers) -> LaunchInputs<'_> {
    LaunchInputs {
        purpose: ProfilePurpose::Interactive,
        approved_servers: servers,
        model: None,
        instructions: "OWNED-STAGED-INSTRUCTIONS",
        has_skill_instructions: false,
        intent_policy: &[],
    }
}

#[tokio::test]
async fn installed_capture_acquires_native_route_then_builds_with_policy() {
    let f = Fixture::new();
    std::fs::write(
        f.etc.join("claude-code/managed-settings.json"),
        r#"{"allowedMcpServers":[{"serverName":"approved"}]}"#,
    )
    .unwrap();
    let acquired = ready(f.acquire(None).await);
    let mut launch = tokio::process::Command::new(acquired.executable());
    launch.env("ANTHROPIC_BASE_URL", "https://api.anthropic.com");
    launch.env("CLAUDE_CODE_REMOTE_SETTINGS_PATH", "unacquired");
    acquired.apply_environment(&mut launch);
    let frozen: BTreeMap<_, _> = launch
        .as_std()
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
    assert_eq!(frozen["ANTHROPIC_BASE_URL"], "http://127.0.0.1:1");
    assert!(!frozen.contains_key("CLAUDE_CODE_REMOTE_SETTINGS_PATH"));
    let servers =
        intent_acp::normalize_mcp_servers(&json!({"approved":{"command":"fixture-never-run"}}));
    let profile = acquired
        .build(
            inputs(&servers),
            ProfileDirectory::ephemeral(f.root.path()).unwrap(),
        )
        .await
        .unwrap();
    profile.ensure_launchable().unwrap();
    let mut environment = BTreeMap::new();
    profile.environment.apply_to_map(&mut environment);
    assert_eq!(environment["ANTHROPIC_API_KEY"], "synthetic-literal-$key");
    assert_eq!(environment["ANTHROPIC_BASE_URL"], "http://127.0.0.1:1");
    assert!(profile
        .runtime_args
        .iter()
        .any(|s| s == "claude-sonnet-4-6"));
    assert!(!profile.session_meta.to_string().contains("never-run\"}]"));
    let denied =
        intent_acp::normalize_mcp_servers(&json!({"other":{"command":"fixture-never-run"}}));
    assert!(matches!(
        acquired
            .build(
                inputs(&denied),
                ProfileDirectory::ephemeral(f.root.path()).unwrap()
            )
            .await,
        Err(ProfileError::PolicyDenied { .. })
    ));
}

#[test]
fn missing_accounts_or_files_never_establish_policy_authority() {
    for base in [
        None,
        Some("https://api.anthropic.com"),
        Some("https://API.ANTHROPIC.COM:443/"),
        Some("not-a-url"),
    ] {
        let mut f = Fixture::new();
        if let Some(base) = base {
            f.env.insert("ANTHROPIC_BASE_URL".into(), base.into());
        } else {
            f.env.remove("ANTHROPIC_BASE_URL");
        }
        assert!(matches!(f.inspect(), Err(DeferredReason::PolicyAuthority)));
    }
    let mut f = Fixture::new();
    f.env.remove("ANTHROPIC_API_KEY");
    assert!(matches!(f.inspect(), Err(DeferredReason::AuthProjection)));
}

#[test]
fn unresolved_sources_and_hidden_settings_routes_defer() {
    for relative in [
        "etc/claude-code/managed-mcp.json",
        "etc/claude-code/managed-settings.d",
        "home/.claude/.credentials.json",
        "home/.claude/remote-settings.json",
        "home/.claude/policy-limits.json",
        "home/.claude/state",
        "home/.config/anthropic",
    ] {
        let f = Fixture::new();
        let path = f.root.path().join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "{}").unwrap();
        assert!(f.inspect().is_err(), "unresolved source {relative}");
    }
    for settings in [
        json!({"policyHelper":"never"}),
        json!({"apiKeyHelper":"never"}),
        json!({"awsAuthRefresh":"never"}),
        json!({"deniedMcpServers":[{"serverName":"blocked"}]}),
        json!({"env":{"_CLAUDE_CODE_ASSUME_FIRST_PARTY_BASE_URL":"1"}}),
        json!({"env":{"ANTHROPIC_BASE_URL":"https://api.anthropic.com"}}),
    ] {
        let f = Fixture::new();
        std::fs::write(f.config.join("settings.json"), settings.to_string()).unwrap();
        assert!(f.inspect().is_err());
    }
    for key in [
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_REMOTE_SETTINGS_PATH",
        "ANTHROPIC_PROFILE",
        "_CLAUDE_CODE_ASSUME_FIRST_PARTY_BASE_URL",
        "WSL_DISTRO_NAME",
    ] {
        let mut f = Fixture::new();
        f.env.insert(key.into(), "fixture".into());
        assert!(matches!(f.inspect(), Err(DeferredReason::PolicyAuthority)));
    }
}

#[tokio::test]
async fn acquired_source_or_runtime_change_is_a_hard_failure() {
    for change in ["auth", "policy", "runtime"] {
        let f = Fixture::new();
        let acquired = ready(f.acquire(None).await);
        match change {
            "auth" => {
                std::fs::write(f.config.join("settings.json"), "{\"model\":\"changed\"}").unwrap();
            }
            "policy" => std::fs::write(
                f.etc.join("claude-code/managed-settings.json"),
                "{\"allowedMcpServers\":[]}",
            )
            .unwrap(),
            _ => std::fs::write(
                acquired.executable(),
                "#!/bin/sh\nprintf '2.1.281 (Claude Code)\\n'\n",
            )
            .unwrap(),
        }
        assert!(acquired
            .build(
                inputs(&BTreeMap::new()),
                ProfileDirectory::ephemeral(f.root.path()).unwrap()
            )
            .await
            .is_err());
    }
}

#[tokio::test]
async fn acquired_builder_rejects_native_assertions_and_enforces_intent_policy() {
    let f = Fixture::new();
    let acquired = ready(f.acquire(None).await);
    let servers = BTreeMap::new();
    let native = [policy::PolicySource::inline(
        policy::PolicyScope::Host,
        "unchecked",
        policy::PolicyFormat::ClaudeManagedMcp,
        "{}",
    )];
    let mut request = inputs(&servers);
    request.intent_policy = &native;
    assert!(acquired
        .build(request, ProfileDirectory::ephemeral(f.root.path()).unwrap())
        .await
        .is_err());
    let restrictions = [policy::PolicySource::inline(
        policy::PolicyScope::Host,
        "intent",
        policy::PolicyFormat::Intent,
        r#"{"allowSkills":false}"#,
    )];
    let mut request = inputs(&servers);
    request.intent_policy = &restrictions;
    request.has_skill_instructions = true;
    assert!(matches!(
        acquired
            .build(request, ProfileDirectory::ephemeral(f.root.path()).unwrap())
            .await,
        Err(ProfileError::PolicyDenied { .. })
    ));
}

#[tokio::test]
#[ignore = "requires pinned Claude runtime/SDK, node and Linux bwrap"]
async fn acquired_native_profile_preserves_actual_cli_auth_model_instructions() {
    let modules =
        std::env::var("INTENT_CLAUDE_FIXTURE_MODULES").expect("set pinned Claude modules");
    let native = Path::new(&modules).join("@anthropic-ai/claude-agent-sdk-linux-x64/claude");
    let mut f = Fixture::new();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    f.env.insert(
        "ANTHROPIC_BASE_URL".into(),
        format!("http://{}", listener.local_addr().unwrap()),
    );
    drop(listener);
    let acquired = ready(f.acquire(Some(&native)).await);
    let profile = acquired
        .build(
            inputs(&BTreeMap::new()),
            ProfileDirectory::ephemeral(f.root.path()).unwrap(),
        )
        .await
        .unwrap();
    let mut environment = BTreeMap::new();
    profile.environment.apply_to_map(&mut environment);
    let file = f.root.path().join("controls.json");
    std::fs::write(&file,json!({"directory":profile.directory.path(),"environment":environment,"runtime_args":profile.runtime_args,"session_meta":profile.session_meta}).to_string()).unwrap();
    let output = tokio::process::Command::new("node")
        .env_remove("NODE_OPTIONS")
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/provider_profile/fixtures/staged-claude.mjs"),
        )
        .arg(modules)
        .arg(file)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "synthetic native acquisition fixture: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("PASS staged Claude"));
}

#[tokio::test]
async fn native_environment_expansion_is_not_raw_identity_matching() {
    let f = Fixture::new();
    std::fs::write(
        f.etc.join("claude-code/managed-settings.json"),
        r#"{"deniedMcpServers":[{"serverCommand":["${HOME}/bin/mcp"]}]}"#,
    )
    .unwrap();
    assert!(matches!(
        f.inspect(),
        Err(DeferredReason::PolicyAcquisition)
    ));
    std::fs::remove_file(f.etc.join("claude-code/managed-settings.json")).unwrap();
    let acquired = ready(f.acquire(None).await);
    let servers =
        intent_acp::normalize_mcp_servers(&json!({"approved":{"command":"${HOME}/bin/mcp"}}));
    assert!(acquired
        .build(
            inputs(&servers),
            ProfileDirectory::ephemeral(f.root.path()).unwrap()
        )
        .await
        .is_err());
}
