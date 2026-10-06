use super::*;
use intent_acp::mcp_config::NormalizedMcpServer;
use serde_json::json;
use std::fs;

fn fixture() -> tempfile::TempDir {
    crate::test_support::test_tempdir("provider-profiles")
}

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

fn request<'a>(
    root: &'a Path,
    provider: &'a str,
    mcp: &'a NormalizedMcpServers,
) -> ProviderProfileRequest<'a> {
    ProviderProfileRequest {
        provider_id: provider,
        detected_version: None,
        purpose: LaunchPurpose::Persistent,
        owned_root: root,
        persistent_identity: Some("agent-fixture"),
        resume: false,
        home: root,
        provider_home: None,
        workspace_root: root,
        launch_cwd: root,
        owned_mcp: mcp,
        policy_sources: &[],
        native_config_files: &[],
        native_skill_roots: &[],
        trusted_launch_config: None,
    }
}

fn stdio() -> NormalizedMcpServer {
    NormalizedMcpServer::Stdio {
        command: "server".into(),
        args: vec!["serve".into()],
        env: BTreeMap::default(),
    }
}

#[test]
fn codex_preserves_transport_and_avoids_native_field_inheritance() {
    let dir = fixture();
    write(
        &dir.path().join(".codex/config.toml"),
        r#"
[mcp_servers.local]
command = "poison"
[mcp_servers.local.env]
SECRET = "native"
[mcp_servers.remote]
url = "https://example.test/mcp"
[mcp_servers.remote.http_headers]
Authorization = "native"
"#,
    );
    let owned = [
        ("local".into(), stdio()),
        (
            "remote".into(),
            NormalizedMcpServer::Http {
                url: "https://intent.test/mcp".into(),
                headers: None,
            },
        ),
    ]
    .into();
    let profile = prepare_provider_profile(request(dir.path(), "codex", &owned)).unwrap();
    let config: Value = serde_json::from_str(&profile.env["CODEX_CONFIG"]).unwrap();
    assert_eq!(config["mcp_servers"]["local"]["enabled"], false);
    assert!(config["mcp_servers"]["local"].get("url").is_none());
    assert_eq!(config["mcp_servers"]["remote"]["enabled"], false);
    assert!(config["mcp_servers"]["remote"].get("command").is_none());
    assert!(profile.session_mcp.is_empty());
    for (internal, logical) in &profile.mcp_name_mapping {
        assert_ne!(internal, logical);
        assert!(config["mcp_servers"][internal].get("SECRET").is_none());
        assert!(config["mcp_servers"][internal]
            .get("http_headers")
            .is_none());
    }
    assert_eq!(config["agents"]["enabled"], false);
    assert_eq!(config["features"]["multi_agent_v2"], false);
}

#[test]
fn profile_persists_session_data_but_never_imports_native_mcp_or_hooks() {
    let dir = fixture();
    write(
        &dir.path().join(".codex/auth.json"),
        r#"{"tokens":{"refresh_token":"fixture"}}"#,
    );
    write(
        &dir.path().join(".codex/config.toml"),
        r#"
model = "custom"
model_provider = "gateway"
cli_auth_credentials_store = "file"
[model_providers.gateway]
base_url = "https://model.test/v1"
env_key = "CUSTOM_KEY"
[hooks]
evil = "do-not-copy"
"#,
    );
    let empty = NormalizedMcpServers::new();
    let profile = prepare_provider_profile(request(dir.path(), "codex", &empty)).unwrap();
    let path = profile.path().to_path_buf();
    let seed = fs::read_to_string(path.join("config.toml")).unwrap();
    assert!(seed.contains("https://model.test/v1"));
    assert!(seed.contains("cli_auth_credentials_store"));
    assert!(!seed.contains("do-not-copy"));
    write(&path.join("sessions/preserved"), "session");
    write(&path.join("auth.json"), "refreshed");
    drop(profile);
    let mut next = request(dir.path(), "codex", &empty);
    next.resume = true;
    let profile = prepare_provider_profile(next).unwrap();
    assert_eq!(profile.path(), path);
    assert_eq!(
        fs::read_to_string(path.join("auth.json")).unwrap(),
        "refreshed"
    );
    assert_eq!(
        fs::read_to_string(path.join("sessions/preserved")).unwrap(),
        "session"
    );
}

#[test]
fn codex_reinventory_catches_new_names_and_reports_reload_and_catalog_limits() {
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    let profile = prepare_provider_profile(request(dir.path(), "codex", &empty)).unwrap();
    assert!(profile
        .diagnostics
        .iter()
        .any(|d| d.code == "native-command-catalog-residual"));
    assert!(profile
        .diagnostics
        .iter()
        .any(|d| d.code == "native-config-reload-residual"));
    drop(profile);
    write(
        &dir.path().join(".codex/config.toml"),
        "[mcp_servers.new_native]\nurl='https://new.test/mcp'\n",
    );
    let profile = prepare_provider_profile(request(dir.path(), "codex", &empty)).unwrap();
    let config: Value = serde_json::from_str(&profile.env["CODEX_CONFIG"]).unwrap();
    assert_eq!(config["mcp_servers"]["new_native"]["enabled"], false);
}

#[test]
fn codex_mixed_transport_layers_and_sse_are_explicit_errors() {
    let dir = fixture();
    write(
        &dir.path().join(".codex/config.toml"),
        "[mcp_servers.same]\ncommand='server'\n",
    );
    let other = dir.path().join("other.toml");
    write(&other, "[mcp_servers.same]\nurl='https://example.test'\n");
    let empty = NormalizedMcpServers::new();
    let files = vec![other];
    let mut req = request(dir.path(), "codex", &empty);
    req.native_config_files = &files;
    assert_eq!(
        prepare_provider_profile(req).err().unwrap().code,
        "native-transport-conflict"
    );
    let sse = [(
        "legacy".into(),
        NormalizedMcpServer::Sse {
            url: "https://example.test/sse".into(),
            headers: None,
        },
    )]
    .into();
    assert_eq!(
        prepare_provider_profile(request(dir.path(), "codex", &sse))
            .err()
            .unwrap()
            .code,
        "mcp-transport-unsupported"
    );
}

#[test]
fn codex_resume_preserves_bundled_skills_without_exempting_native_roots() {
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    let profile = prepare_provider_profile(request(dir.path(), "codex", &empty)).unwrap();
    let bundled = profile.path().join("skills/.system/skill-creator/SKILL.md");
    let personal = dir.path().join(".agents/skills/find-skills/SKILL.md");
    let native_system = dir.path().join(".codex/skills/.system/native/SKILL.md");
    let profile_user = profile.path().join("skills/custom/SKILL.md");
    for path in [&bundled, &personal, &native_system, &profile_user] {
        write(path, "---\nname: fixture\ndescription: fixture\n---\n");
    }
    drop(profile);
    let mut req = request(dir.path(), "codex", &empty);
    req.resume = true;
    let resumed = prepare_provider_profile(req).unwrap();
    let config: Value = serde_json::from_str(&resumed.env["CODEX_CONFIG"]).unwrap();
    let disabled = config["skills"]["config"].as_array().unwrap();
    assert!(!disabled
        .iter()
        .any(|entry| entry["path"] == bundled.to_string_lossy().as_ref()));
    for path in [&personal, &native_system, &profile_user] {
        assert!(disabled
            .iter()
            .any(|entry| entry["path"] == path.to_string_lossy().as_ref()
                && entry["enabled"] == false));
    }
}

#[test]
fn claude_preserves_tool_free_metadata_and_refuses_exclusive_policy() {
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    let mut req = request(dir.path(), "claude-code", &empty);
    req.purpose = LaunchPurpose::Completion;
    let p = prepare_provider_profile(req).unwrap();
    let mut meta = json!({"claudeCode":{"options":{"tools":[],"extraArgs":{"custom":null}}}});
    p.merge_session_meta(&mut meta);
    assert_eq!(meta["claudeCode"]["options"]["tools"], json!([]));
    assert_eq!(meta["claudeCode"]["options"]["strictMcpConfig"], true);
    assert!(meta["claudeCode"]["options"]["extraArgs"]
        .get("custom")
        .is_some());
    let file = dir.path().join("managed-mcp.json");
    write(&file, r#"{"mcpServers":{}}"#);
    let sources = [PolicySource::ClaudeExclusive(file)];
    let mut req = request(dir.path(), "claude-code", &empty);
    req.policy_sources = &sources;
    assert_eq!(
        prepare_provider_profile(req).err().unwrap().code,
        "managed-policy-conflict"
    );
}

#[test]
fn policy_checks_real_identity_denies_bridge_and_never_aliases_name_rules() {
    let dir = fixture();
    let file = dir.path().join("requirements.toml");
    write(
        &file,
        "[mcp_servers.allowed]\nidentity={command='server'}\n",
    );
    let sources = [PolicySource::CodexRequirements(file)];
    let policy = load_mcp_policy("codex", &sources).unwrap();
    assert!(policy.allows_server("allowed", &stdio()));
    assert!(!policy.allows_server("workspace-mcp", &stdio()));
    assert!(!policy.allows_server(
        "allowed",
        &NormalizedMcpServer::Http {
            url: "https://example.test".into(),
            headers: None
        }
    ));
    let owned = [("allowed".into(), stdio())].into();
    let mut req = request(dir.path(), "codex", &owned);
    req.policy_sources = &sources;
    assert_eq!(
        prepare_provider_profile(req).err().unwrap().code,
        "managed-policy-conflict"
    );
}

#[test]
fn claude_policy_command_rules_override_name_allow_and_denies_win() {
    let dir = fixture();
    let file = dir.path().join("managed-settings.json");
    write(
        &file,
        r#"{"allowedMcpServers":[{"serverName":"named"},{"serverCommand":["server","serve"]}],"deniedMcpServers":[{"serverName":"workspace-mcp"}]}"#,
    );
    let policy = load_mcp_policy("claude-code", &[PolicySource::ClaudeSettings(file)]).unwrap();
    assert!(policy.allows_server("named", &stdio()));
    let mut bad = stdio();
    if let NormalizedMcpServer::Stdio { args, .. } = &mut bad {
        args.push("--evil".into());
    }
    assert!(!policy.allows_server("named", &bad));
    assert!(!policy.allows_server("workspace-mcp", &stdio()));
}

#[test]
fn every_catalog_provider_has_honest_outcomes_and_ephemeral_cleanup() {
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    for provider in intent_providers::all_provider_ids() {
        let mut req = request(dir.path(), provider, &empty);
        req.purpose = LaunchPurpose::ModelProbe;
        let p = prepare_provider_profile(req).unwrap();
        assert!(!p.diagnostics.is_empty(), "{provider}");
        let path = p.path().to_path_buf();
        assert!(path.is_dir());
        drop(p);
        assert!(!path.exists(), "temporary {provider} profile leaked");
    }
}

#[test]
fn pi_suppression_is_native_args_and_preserves_explicit_extension() {
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    let p = prepare_provider_profile(request(dir.path(), "pi", &empty)).unwrap();
    assert!(p.args.is_empty());
    assert!(p.native_args.contains(&"--no-skills".into()));
    assert!(p.native_args.contains(&"--no-extensions".into()));
    assert!(!p.env.contains_key("PI_ACP_PI_COMMAND"));
}

#[test]
fn pi_verification_is_scoped_to_the_exercised_native_version() {
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    for (version, outcome) in [
        (None, CapabilityOutcome::Unverified),
        (Some("0.81.0"), CapabilityOutcome::Verified),
        (Some("0.82.0"), CapabilityOutcome::Unverified),
    ] {
        let mut req = request(dir.path(), "pi", &empty);
        req.detected_version = version;
        let profile = prepare_provider_profile(req).unwrap();
        assert_eq!(profile.capabilities.skills, outcome);
    }
}

#[test]
fn open_code_preserves_owned_unsloth_routing_and_data_locations() {
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    let config = json!({"provider":{"unsloth-studio":{"options":{"baseURL":"http://localhost:1234/v1","apiKey":"fixture"}}},"model":"unsloth-studio/test","permission":{"task":"deny"}});
    let mut req = request(dir.path(), "unsloth", &empty);
    req.trusted_launch_config = Some(&config);
    let p = prepare_provider_profile(req).unwrap();
    let actual: Value = serde_json::from_str(&p.env["OPENCODE_CONFIG_CONTENT"]).unwrap();
    assert_eq!(actual["provider"], config["provider"]);
    assert_eq!(actual["permission"]["task"], "deny");
    assert_eq!(actual["permission"]["skill"], "deny");
    assert!(!p.env.contains_key("HOME"));
    assert!(!p.env.contains_key("XDG_DATA_HOME"));
}

#[test]
fn active_profiles_are_leased_and_restart_cleanup_only_removes_abandoned_temporary_data() {
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    let persistent = prepare_provider_profile(request(dir.path(), "codex", &empty)).unwrap();
    assert_eq!(
        prepare_provider_profile(request(dir.path(), "codex", &empty))
            .err()
            .unwrap()
            .code,
        "profile-in-use"
    );
    let mut req = request(dir.path(), "mock", &empty);
    req.purpose = LaunchPurpose::Completion;
    let temporary = prepare_provider_profile(req).unwrap();
    let abandoned = dir.path().join("provider-profiles-v1/ephemeral-abandoned");
    write(&abandoned.join(".lease"), "");
    write(&abandoned.join("secret"), "synthetic");
    assert_eq!(cleanup_abandoned_profiles(dir.path()).unwrap(), 1);
    assert!(persistent.path().is_dir());
    assert!(temporary.path().is_dir());
    assert!(!abandoned.exists());
}

#[test]
fn failure_cleans_temporary_profiles_and_missing_resume_is_explicit() {
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    let mut req = request(dir.path(), "codex", &empty);
    req.resume = true;
    assert_eq!(
        prepare_provider_profile(req).err().unwrap().code,
        "resume-profile-missing"
    );
    write(&dir.path().join(".codex/config.toml"), "this is not TOML");
    let mut req = request(dir.path(), "codex", &empty);
    req.purpose = LaunchPurpose::Completion;
    assert_eq!(
        prepare_provider_profile(req).err().unwrap().code,
        "native-config-invalid"
    );
    assert!(!fs::read_dir(dir.path().join("provider-profiles-v1"))
        .unwrap()
        .any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("ephemeral-")));
}

#[cfg(unix)]
#[test]
fn credential_files_are_private_and_profile_symlinks_are_rejected() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let dir = fixture();
    let empty = NormalizedMcpServers::new();
    write(&dir.path().join(".codex/auth.json"), "synthetic");
    let p = prepare_provider_profile(request(dir.path(), "codex", &empty)).unwrap();
    assert_eq!(
        fs::metadata(p.path()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(p.path().join("auth.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let path = p.path().to_owned();
    drop(p);
    fs::remove_file(path.join("auth.json")).unwrap();
    let target = dir.path().join("untouched");
    write(&target, "original");
    symlink(&target, path.join("auth.json")).unwrap();
    assert_eq!(
        prepare_provider_profile(request(dir.path(), "codex", &empty))
            .err()
            .unwrap()
            .code,
        "config-boundary-escape"
    );
    assert_eq!(fs::read_to_string(target).unwrap(), "original");
}

#[test]
fn unknown_enforced_tool_policy_is_rejected_and_errors_redact_contents() {
    let dir = fixture();
    let file = dir.path().join("policy.json");
    write(
        &file,
        r#"{"permissions":{"deny":["mcp__secret__write"]},"token":"secret-value"}"#,
    );
    let error = load_mcp_policy("claude-code", &[PolicySource::ClaudeSettings(file)])
        .err()
        .unwrap();
    assert_eq!(error.code, "managed-policy-unsupported");
    assert!(!error.to_string().contains("secret-value"));
    for provider in intent_providers::all_provider_ids() {
        assert_eq!(
            load_mcp_policy(provider, &[PolicySource::Unavailable])
                .err()
                .unwrap()
                .code,
            "managed-policy-unsupported"
        );
    }
}

#[test]
fn managed_url_rules_and_empty_allowlists_are_enforced_for_both_transports() {
    let dir = fixture();
    let file = dir.path().join("policy.json");
    write(
        &file,
        r#"{"allowedMcpServers":[{"serverUrl":"https://*.example.test/*"}],"deniedMcpServers":[{"serverUrl":"https://bad.example.test/*"}]}"#,
    );
    let policy =
        load_mcp_policy("claude-code", &[PolicySource::ClaudeSettings(file.clone())]).unwrap();
    for server in [
        NormalizedMcpServer::Http {
            url: "https://ok.example.test/mcp".into(),
            headers: None,
        },
        NormalizedMcpServer::Sse {
            url: "https://ok.example.test/sse".into(),
            headers: None,
        },
    ] {
        assert!(policy.allows_server("logical", &server));
        assert!(policy.allows_tool("logical", &server, "read"));
    }
    assert!(!policy.allows_server(
        "logical",
        &NormalizedMcpServer::Http {
            url: "https://bad.example.test/mcp".into(),
            headers: None
        }
    ));
    assert!(!policy.allows_server("logical", &stdio()));
    write(&file, r#"{"allowedMcpServers":[]}"#);
    assert!(
        !load_mcp_policy("claude-code", &[PolicySource::ClaudeSettings(file)])
            .unwrap()
            .allows_server("logical", &stdio())
    );
}

#[test]
fn skill_snapshot_is_separate_from_acp_command_exposure() {
    let dir = fixture();
    let path = dir.path().join(".agents/skills/native/SKILL.md");
    write(
        &path,
        "---\nname: native\ndescription: fixture\n---\nsecret-skill-body",
    );
    let empty = NormalizedMcpServers::new();
    let p = prepare_provider_profile(request(dir.path(), "codex", &empty)).unwrap();
    let config: Value = serde_json::from_str(&p.env["CODEX_CONFIG"]).unwrap();
    assert!(config["skills"]["config"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v["path"] == json!(path.canonicalize().unwrap()) && v["enabled"] == false));
    assert!(!p.env["CODEX_CONFIG"].contains("secret-skill-body"));
    assert_eq!(p.capabilities.skills, CapabilityOutcome::Partial);
    assert!(p
        .diagnostics
        .iter()
        .any(|d| d.code == "native-command-catalog-residual"));
}

#[test]
fn ephemeral_profiles_never_inject_mcp_or_weaken_blanket_tool_denial() {
    let dir = fixture();
    let owned = [("fixture".into(), stdio())].into();
    let denial = json!({"permission":"deny"});
    for provider in [
        "claude-code",
        "codex",
        "opencode",
        "unsloth",
        "auggie",
        "pi",
    ] {
        let mut req = request(dir.path(), provider, &owned);
        req.purpose = LaunchPurpose::Completion;
        req.trusted_launch_config = Some(&denial);
        let p = prepare_provider_profile(req).unwrap();
        assert!(p.approved_mcp.is_empty());
        assert!(p.session_mcp.is_empty());
        if let Some(config) = p.env.get("OPENCODE_CONFIG_CONTENT") {
            let config: Value = serde_json::from_str(config).unwrap();
            assert_eq!(config["permission"], "deny");
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn non_regular_config_is_rejected_without_blocking_a_subprocess() {
    const CHILD_ROOT: &str = "INTENTD_PROFILE_FIFO_TEST_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let empty = NormalizedMcpServers::new();
        let error = prepare_provider_profile(request(Path::new(&root), "codex", &empty))
            .err()
            .expect("a named pipe must not be read as native config");
        assert_eq!(error.code, "profile-config-not-file");
        return;
    }
    let dir = fixture();
    let config = dir.path().join(".codex/config.toml");
    fs::create_dir_all(config.parent().unwrap()).unwrap();
    assert!(std::process::Command::new("mkfifo")
        .arg(&config)
        .status()
        .unwrap()
        .success());
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "provider_profiles::tests::non_regular_config_is_rejected_without_blocking_a_subprocess",
            "--nocapture",
        ])
        .env(CHILD_ROOT, dir.path())
        .kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(5), command.output())
        .await
        .expect("provider profile preparation blocked on a named pipe")
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        storage::read_optional(Path::new("/dev/null"))
            .unwrap_err()
            .code,
        "profile-config-not-file"
    );
    let ordinary = dir.path().join("ordinary.json");
    write(&ordinary, "{}");
    let linked = dir.path().join("linked.json");
    std::os::unix::fs::symlink(ordinary, &linked).unwrap();
    assert_eq!(
        storage::read_optional(&linked).unwrap(),
        Some(b"{}".to_vec())
    );
}

#[test]
fn managed_url_rules_preserve_host_path_boundaries_and_native_normalization() {
    let dir = fixture();
    let file = dir.path().join("policy.json");
    for (pattern, target, matches) in [
        (
            "http://127.0.0.1:1/x/../mcp",
            "http://127.0.0.1:1/mcp",
            true,
        ),
        (
            "http://127.0.0.1:1/x/%2e%2e/mcp",
            "http://127.0.0.1:1/mcp",
            true,
        ),
        (
            "http://127.0.0.1:1/x/.%2E/mcp",
            "http://127.0.0.1:1/mcp",
            true,
        ),
        (
            "http://127.0.0.1:1/mcp",
            "http://127.0.0.1:1/x/../mcp",
            true,
        ),
        ("http://127.0.0.1:1/", "http://127.0.0.1:1/mcp", false),
        (
            "http://127.0.0.1:1/mcp",
            "http://127.0.0.1:1/mcp?q=1",
            false,
        ),
        (
            "http://127.0.0.1:1/mcp*",
            "http://127.0.0.1:1/mcp?q=1",
            true,
        ),
        ("http://127.0.0.1:1", "http://127.0.0.1:1/mcp?q=1", true),
        ("http://127.0.0.1", "http://127.0.0.1/mcp", true),
        (
            "https://MCP.Example.test.",
            "https://mcp.example.test/Mcp",
            true,
        ),
        (
            "https://mcp.example.test/*",
            "https://MCP.EXAMPLE.TEST./mcp",
            true,
        ),
        (
            "https://*.example.test/*",
            "https://a.example.test/mcp",
            true,
        ),
        (
            "https://*.example.test/*",
            "https://evil.test/x.example.test/mcp",
            false,
        ),
        (
            "https://*.example.test/*",
            "https://evil.test/?x=a.example.test/mcp",
            false,
        ),
        (
            "https://mcp.example.test/Mcp",
            "https://mcp.example.test/mcp",
            false,
        ),
        ("http://localhost:*/*", "http://localhost:4321/mcp", true),
        (
            "*://mcp.example.test/*",
            "http://mcp.example.test/mcp",
            true,
        ),
        (
            "https://mcp.example.test/*",
            "https://mcp.example.test:4321/mcp",
            false,
        ),
    ] {
        for key in ["allowedMcpServers", "deniedMcpServers"] {
            write(&file, &json!({key:[{"serverUrl":pattern}]}).to_string());
            let policy =
                load_mcp_policy("claude-code", &[PolicySource::ClaudeSettings(file.clone())])
                    .unwrap();
            for server in [
                NormalizedMcpServer::Http {
                    url: target.into(),
                    headers: None,
                },
                NormalizedMcpServer::Sse {
                    url: target.into(),
                    headers: None,
                },
            ] {
                let expected = if key == "allowedMcpServers" {
                    matches
                } else {
                    !matches
                };
                assert_eq!(
                    policy.allows_server("fixture", &server),
                    expected,
                    "{key}: {pattern} vs {target}"
                );
            }
        }
    }
    for pattern in [
        "https://user@host/mcp",
        "https://host/mcp?q=*",
        "h*://host/*",
        "${HOST}/mcp",
    ] {
        write(
            &file,
            &json!({"deniedMcpServers":[{"serverUrl":pattern}]}).to_string(),
        );
        assert!(
            load_mcp_policy("claude-code", &[PolicySource::ClaudeSettings(file.clone())]).is_err()
        );
    }
}

#[test]
fn opencode_remote_import_keeps_oauth_disabled_through_profile_generation() {
    let dir = fixture();
    write(
        &dir.path().join("opencode.json"),
        &json!({"mcp":{"remote":{
            "type":"remote", "url":"https://example.test/mcp", "oauth":false,
            "headers":{"X-Fixture":"preserved"}
        }}})
        .to_string(),
    );
    let discovered = crate::project_mcp::discover_project_mcp(dir.path(), dir.path());
    assert!(discovered.diagnostics.is_empty());
    assert!(discovered.servers.contains_key("remote"));
    let profile =
        prepare_provider_profile(request(dir.path(), "opencode", &discovered.servers)).unwrap();
    let config: Value = serde_json::from_str(&profile.env["OPENCODE_CONFIG_CONTENT"]).unwrap();
    assert_eq!(config["mcp"]["remote"]["oauth"], false);
    assert_eq!(config["mcp"]["remote"]["headers"]["X-Fixture"], "preserved");
}

#[test]
fn excessive_deny_glob_is_rejected_at_policy_load_instead_of_becoming_unrestricted() {
    let dir = fixture();
    let file = dir.path().join("policy.json");
    let pattern = format!("https://example.test/{}", "a*".repeat(10_000));
    write(
        &file,
        &json!({"deniedMcpServers":[{"serverUrl":pattern}]}).to_string(),
    );
    let error = load_mcp_policy("claude-code", &[PolicySource::ClaudeSettings(file)])
        .err()
        .expect("oversized deny must reject policy loading");
    assert_eq!(error.code, "managed-policy-unsupported");
}
