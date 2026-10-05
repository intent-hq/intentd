use super::*;
use intent_acp::NormalizedMcpServer;
use serde_json::json;

fn scratch() -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix("intent-provider-profile-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    builder.tempdir().unwrap()
}

fn server(args: &[&str]) -> NormalizedMcpServer {
    NormalizedMcpServer::Stdio {
        command: "approved-server".into(),
        args: args.iter().map(|v| (*v).into()).collect(),
        env: BTreeMap::default(),
    }
}

#[test]
fn absent_and_empty_codex_allowlists_differ() {
    let absent = policy::parse_codex_requirements("", "system").unwrap();
    absent.validate_server("bridge", &server(&[])).unwrap();
    let empty = policy::parse_codex_requirements("[mcp_servers]", "system").unwrap();
    assert!(empty.validate_server("bridge", &server(&[])).is_err());
}

#[test]
fn codex_policy_matches_name_executable_and_ordered_arguments() {
    let policy = policy::parse_codex_requirements(
        r#"
[mcp_servers.allowed.identity.command]
executable = "approved-server"
args = [{match="exact",value="--safe"},{match="prefix",value="scope:"}]
"#,
        "system",
    )
    .unwrap();
    policy
        .validate_server("allowed", &server(&["--safe", "scope:read"]))
        .unwrap();
    assert!(policy
        .validate_server("different", &server(&["--safe", "scope:read"]))
        .is_err());
    assert!(policy
        .validate_server("allowed", &server(&["scope:read", "--safe"]))
        .is_err());
    assert!(policy
        .validate_server("allowed", &server(&["--safe", "scope:read", "--unsafe"]))
        .is_err());
}

#[test]
fn applicable_unknown_requirements_fail_without_echoing_values() {
    let error = policy::parse_codex_requirements("secret_requirement = 'DO-NOT-PRINT'", "system")
        .err()
        .unwrap();
    assert!(!error.to_string().contains("DO-NOT-PRINT"));
    assert!(error.to_string().contains("unsupported"));
}

#[test]
fn only_selected_provider_and_explicit_host_scope_apply() {
    use policy::{PolicyFormat, PolicyScope, PolicySource};
    let unrelated = PolicySource::unavailable(
        PolicyScope::Provider("droid".into()),
        "org",
        "remote policy fetch unavailable",
    );
    let intent = PolicySource::inline(
        PolicyScope::Host,
        "intent",
        PolicyFormat::Intent,
        r#"{"allowMcp":[]}"#,
    );
    let snapshot = policy::read_host_policy("pi", &[unrelated, intent]).unwrap();
    assert!(snapshot.validate_server("allowed", &server(&[])).is_err());
    let applicable = PolicySource::unavailable(
        PolicyScope::Provider("pi".into()),
        "org",
        "remote policy fetch unavailable",
    );
    assert!(policy::read_host_policy("pi", &[applicable]).is_err());
}

#[test]
#[cfg(unix)]
fn persistent_identity_survives_drop_but_ephemeral_cleanup_waits_for_last_guard() {
    let root = scratch();
    let identity = ProfileIdentity {
        workspace: "../workspace",
        agent: "a/b",
        provider: "pi",
    };
    let owned = ProfileDirectory::persistent(root.path(), &identity).unwrap();
    let path = owned.path().to_owned();
    owned
        .write_private("session.json", b"fixture-session")
        .unwrap();
    drop(owned);
    let resumed = ProfileDirectory::persistent(root.path(), &identity).unwrap();
    assert_eq!(resumed.path(), path);
    assert_eq!(
        std::fs::read(path.join("session.json")).unwrap(),
        b"fixture-session"
    );
    let ephemeral = ProfileDirectory::ephemeral(root.path()).unwrap();
    let temporary_path = ephemeral.path().to_owned();
    let child_guard = ephemeral.clone();
    drop(ephemeral);
    assert!(temporary_path.exists());
    drop(child_guard);
    assert!(!temporary_path.exists());
}

#[test]
fn codex_auth_projection_excludes_executable_config_and_keeps_routing() {
    let safe = auth::project_codex_config(
        r#"
model_provider = "private"
model = "custom-model"
[model_providers.private]
name = "Private"
base_url = "https://model.invalid/v1"
env_key = "FIXTURE_KEY"
wire_api = "responses"
[mcp_servers.native]
command = "must-not-run"
[hooks]
start = "must-not-run"
[plugins.ambient]
enabled = true
"#,
    )
    .unwrap();
    assert_eq!(safe["model_provider"], "private");
    assert_eq!(safe["model_providers"]["private"]["env_key"], "FIXTURE_KEY");
    assert!(safe.get("mcp_servers").is_none());
    assert!(safe.get("hooks").is_none());
    assert!(safe.get("plugins").is_none());
}

#[test]
#[cfg(unix)]
fn pi_profile_has_positive_controls_and_preserves_explicit_auth_and_model() {
    let root = scratch();
    let dir = ProfileDirectory::ephemeral(root.path()).unwrap();
    let inputs = AuthModelContext {
        model: Some("fixture-model".into()),
        credential_environment: [("OPENAI_API_KEY".into(), "fixture-secret".into())].into(),
        ..Default::default()
    };
    let profile = prepare_profile_candidate(
        "pi",
        ProfilePurpose::Ephemeral,
        dir,
        &BTreeMap::default(),
        &inputs,
        &policy::HostPolicySnapshot::default(),
    )
    .unwrap();
    assert!(profile.runtime_args.contains(&"--no-skills".into()));
    assert!(profile.runtime_args.contains(&"--no-extensions".into()));
    assert!(!profile.runtime_args.contains(&"-e".into()));
    assert_eq!(profile.model.as_deref(), Some("fixture-model"));
    let mut env = [
        ("PI_CODING_AGENT_DIR".into(), "ambient".into()),
        ("NODE_OPTIONS".into(), "--require ambient.js".into()),
    ]
    .into();
    profile.environment.apply_to_map(&mut env);
    assert_eq!(env["OPENAI_API_KEY"], "fixture-secret");
    assert_ne!(env["PI_CODING_AGENT_DIR"], "ambient");
    assert!(!env.contains_key("NODE_OPTIONS"));
}

#[test]
#[cfg(unix)]
fn ephemeral_catalog_is_rejected_before_writing_profile() {
    let root = scratch();
    let dir = ProfileDirectory::ephemeral(root.path()).unwrap();
    let servers = [("external".into(), server(&[]))].into();
    assert!(prepare_profile_candidate(
        "pi",
        ProfilePurpose::Ephemeral,
        dir,
        &servers,
        &AuthModelContext::default(),
        &policy::HostPolicySnapshot::default()
    )
    .is_err());
}

#[test]
#[cfg(unix)]
fn claude_metadata_is_strict_and_does_not_preserve_user_settings() {
    let root = scratch();
    let profile = prepare_profile_candidate(
        "claude-code",
        ProfilePurpose::Interactive,
        ProfileDirectory::ephemeral(root.path()).unwrap(),
        &BTreeMap::default(),
        &AuthModelContext::default(),
        &policy::HostPolicySnapshot::default(),
    )
    .unwrap();
    let opts = &profile.session_meta["claudeCode"]["options"];
    assert_eq!(opts["strictMcpConfig"], true);
    assert_eq!(opts["settingSources"], json!([]));
    assert_eq!(opts["extraArgs"]["disable-slash-commands"], "");
    assert!(
        profile.ensure_launchable().is_err(),
        "candidate metadata alone cannot certify the installed CLI"
    );
}

#[test]
#[cfg(unix)]
fn codex_profile_always_retains_subagent_denial() {
    let root = scratch();
    let profile = prepare_profile_candidate(
        "codex",
        ProfilePurpose::Interactive,
        ProfileDirectory::ephemeral(root.path()).unwrap(),
        &BTreeMap::default(),
        &AuthModelContext::default(),
        &policy::HostPolicySnapshot::default(),
    )
    .unwrap();
    let mut env = BTreeMap::default();
    profile.environment.apply_to_map(&mut env);
    let config: serde_json::Value = serde_json::from_str(&env["CODEX_CONFIG"]).unwrap();
    let mut required: serde_json::Value =
        serde_json::from_str(intent_providers::CODEX_SUBAGENT_POLICY_CONFIG).unwrap();
    // The managed profile preserves both legacy denials and the current flag.
    required["features"]["multi_agent"] = json!(false);
    for (key, value) in required.as_object().unwrap() {
        assert_eq!(&config[key], value);
    }
    assert!(
        profile.ensure_launchable().is_err(),
        "isolated CODEX_HOME does not suppress project skills"
    );
}

#[test]
fn independent_restrictions_intersect_and_denials_win() {
    use policy::{PolicyFormat, PolicyScope, PolicySource};
    let allow = PolicySource::inline(
        PolicyScope::Host,
        "intent-allow",
        PolicyFormat::Intent,
        r#"{"allowMcp":[{"name":"allowed"}]}"#,
    );
    let deny = PolicySource::inline(
        PolicyScope::Host,
        "intent-deny",
        PolicyFormat::Intent,
        r#"{"denyMcp":[{"identity":{"command":"approved-server"}}]}"#,
    );
    let restricted = policy::read_host_policy("pi", &[allow, deny]).unwrap();
    assert!(restricted.validate_server("allowed", &server(&[])).is_err());
    assert_eq!(restricted.provenance(), ["intent-allow", "intent-deny"]);
}

#[test]
fn regex_url_policy_is_anchored_and_transport_specific() {
    let restricted = policy::parse_codex_requirements(
        r#"
[mcp_servers.web.identity.url]
match = "regex"
expression = 'https://allowed\.invalid/[a-z]+'
"#,
        "system",
    )
    .unwrap();
    let http = |url: &str| NormalizedMcpServer::Http {
        url: url.into(),
        headers: None,
    };
    restricted
        .validate_server("web", &http("https://allowed.invalid/read"))
        .unwrap();
    assert!(restricted
        .validate_server("web", &http("https://allowed.invalid/read?secret=fixture"))
        .is_err());
    assert!(restricted
        .validate_server(
            "web",
            &NormalizedMcpServer::Sse {
                url: "https://allowed.invalid/read".into(),
                headers: None
            }
        )
        .is_err());
}

#[test]
fn policy_requires_explicit_compatible_launch_values() {
    let policy = policy::parse_codex_requirements(
        r#"
allowed_approval_policies = ["on-request"]
allowed_sandbox_modes = ["read-only"]
[features]
multi_agent = false
"#,
        "system",
    )
    .unwrap();
    let features = [("multi_agent".into(), false)].into();
    policy
        .validate_launch(Some("on-request"), Some("read-only"), &features)
        .unwrap();
    assert!(policy
        .validate_launch(None, Some("read-only"), &features)
        .is_err());
    assert!(policy
        .validate_launch(Some("never"), Some("read-only"), &features)
        .is_err());
    assert!(policy
        .validate_launch(Some("on-request"), Some("read-only"), &BTreeMap::default())
        .is_err());
}

#[test]
fn unreadable_or_malformed_applicable_files_fail_but_optional_absence_is_normal() {
    use policy::{PolicyFormat, PolicyInput, PolicyScope, PolicySource};
    let root = scratch();
    let path = root.path().join("requirements.toml");
    let source = |path| PolicySource {
        scope: PolicyScope::Provider("codex".into()),
        label: "system".into(),
        format: PolicyFormat::CodexRequirements,
        input: PolicyInput::File {
            path,
            optional: true,
        },
    };
    policy::read_host_policy("codex", &[source(path.clone())]).unwrap();
    std::fs::write(&path, "auth = \"FIXTURE_SECRET").unwrap();
    let error = policy::read_host_policy("codex", &[source(path)])
        .err()
        .unwrap();
    assert!(!error.to_string().contains("FIXTURE_SECRET"));
    assert!(policy::read_host_policy("codex", &[source(root.path().to_owned())]).is_err());
}

#[test]
#[cfg(unix)]
fn profile_permissions_and_atomic_writes_do_not_follow_target_symlinks() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let root = scratch();
    let dir = ProfileDirectory::ephemeral(root.path()).unwrap();
    let outside = root.path().join("outside");
    std::fs::write(&outside, "untouched").unwrap();
    symlink(&outside, dir.path().join("auth.json")).unwrap();
    dir.write_private("auth.json", b"fixture-credential")
        .unwrap();
    assert_eq!(std::fs::read_to_string(&outside).unwrap(), "untouched");
    assert_eq!(
        std::fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(dir.path().join("auth.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(dir.write_private("../escaped", b"never").is_err());
    let alias = root.path().join("alias");
    symlink(dir.path(), &alias).unwrap();
    assert!(ProfileDirectory::ephemeral(&alias).is_err());
    let public_parent = root.path().join("public");
    std::fs::create_dir(&public_parent).unwrap();
    std::fs::set_permissions(&public_parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(ProfileDirectory::ephemeral(&public_parent).is_err());
}

#[test]
#[cfg(unix)]
fn delete_requires_no_remaining_child_guard() {
    let root = scratch();
    let identity = ProfileIdentity {
        provider: "pi",
        workspace: "workspace",
        agent: "agent",
    };
    let directory = ProfileDirectory::persistent(root.path(), &identity).unwrap();
    let path = directory.path().to_owned();
    let child = directory.clone();
    assert!(directory.remove_persistent().is_err());
    assert!(path.exists());
    child.remove_persistent().unwrap();
    assert!(!path.exists());
}

#[test]
#[cfg(unix)]
fn custom_codex_credentials_survive_without_importing_config_controls() {
    let root = scratch();
    let context = AuthModelContext {
        credential_environment:[("CUSTOM_API_KEY".into(),"fixture-only".into())].into(),
        codex_routing_toml:Some("model_provider='custom'\n[model_providers.custom]\nenv_key='CUSTOM_API_KEY'\nbase_url='https://fixture.invalid/v1'\nwire_api='responses'".into()),
        ..Default::default()
    };
    let profile = prepare_profile_candidate(
        "codex",
        ProfilePurpose::Ephemeral,
        ProfileDirectory::ephemeral(root.path()).unwrap(),
        &BTreeMap::default(),
        &context,
        &policy::HostPolicySnapshot::default(),
    )
    .unwrap();
    let mut env = BTreeMap::default();
    profile.environment.apply_to_map(&mut env);
    assert_eq!(env["CUSTOM_API_KEY"], "fixture-only");
    let config: Value = serde_json::from_str(&env["CODEX_CONFIG"]).unwrap();
    assert_eq!(
        config["model_providers"]["custom"]["base_url"],
        "https://fixture.invalid/v1"
    );
}

#[test]
#[cfg(unix)]
fn unsloth_requires_and_preserves_typed_endpoint_and_compaction() {
    let root = scratch();
    let missing = prepare_profile_candidate(
        "unsloth",
        ProfilePurpose::Interactive,
        ProfileDirectory::ephemeral(root.path()).unwrap(),
        &BTreeMap::default(),
        &AuthModelContext::default(),
        &policy::HostPolicySnapshot::default(),
    );
    assert!(missing.is_err());
    let context = AuthModelContext {
        endpoint: Some(ModelEndpoint {
            provider_id: "unsloth-studio".into(),
            model_id: "fixture-model".into(),
            base_url: "http://127.0.0.1:1/v1".into(),
            api_key: "fixture-key".into(),
            context_window: Some(32000),
            max_output_tokens: Some(1024),
            compaction_reserved: Some(2048),
        }),
        credentials: vec![auth::CredentialFile::OpenCodeAuth(
            json!({"fixture":{"type":"api","key":"fixture-key"}}),
        )],
        ..Default::default()
    };
    let profile = prepare_profile_candidate(
        "unsloth",
        ProfilePurpose::Interactive,
        ProfileDirectory::ephemeral(root.path()).unwrap(),
        &BTreeMap::default(),
        &context,
        &policy::HostPolicySnapshot::default(),
    )
    .unwrap();
    let mut env = BTreeMap::default();
    profile.environment.apply_to_map(&mut env);
    let config: Value = serde_json::from_str(&env["OPENCODE_CONFIG_CONTENT"]).unwrap();
    assert_eq!(config["model"], "unsloth-studio/fixture-model");
    assert_eq!(config["model"], config["small_model"]);
    assert_eq!(config["compaction"]["reserved"], 2048);
    assert_eq!(
        config["provider"]["unsloth-studio"]["options"]["apiKey"],
        "fixture-key"
    );
    assert!(profile
        .directory
        .path()
        .join("opencode/auth.json")
        .is_file());
}

#[test]
fn credential_helpers_are_not_silently_executed_or_lost() {
    assert!(auth::project_claude_settings(&json!({"apiKeyHelper":"fixture-helper"})).is_err());
    assert!(auth::CredentialFile::PiAuth(
        json!({"custom":{"type":"api_key","key":"!fixture-helper"}})
    )
    .material()
    .is_err());
    assert!(auth::project_codex_config("profile='custom'").is_err());
    let safe = auth::project_claude_settings(&json!({"hooks":{"start":"never"},"env":{"NODE_OPTIONS":"never","ANTHROPIC_API_KEY":"fixture-only"}})).unwrap();
    assert!(safe.get("hooks").is_none());
    assert!(safe["env"].get("NODE_OPTIONS").is_none());
    assert_eq!(safe["env"]["ANTHROPIC_API_KEY"], "fixture-only");
}

#[test]
#[cfg(unix)]
fn verified_pi_path_has_a_positive_gate_and_unknown_versions_do_not() {
    let root = scratch();
    let servers = BTreeMap::default();
    let auth = AuthModelContext::default();
    let policy = policy::HostPolicySnapshot::default();
    let request = |version| ProfileRequest {
        provider: "pi",
        runtime: RuntimeIdentity {
            native_version: version,
            adapter_version: None,
            os: "linux",
            arch: "x86_64",
        },
        purpose: ProfilePurpose::Ephemeral,
        directory: ProfileDirectory::ephemeral(root.path()).unwrap(),
        approved_servers: &servers,
        auth_model: &auth,
        policy: &policy,
    };
    let profile = prepare_provider_profile(request("0.81.0")).unwrap();
    profile.ensure_launchable().unwrap();
    assert!(profile.runtime_args.contains(&"--no-approve".into()));
    assert!(profile.runtime_args.contains(&"--no-tools".into()));
    assert!(prepare_provider_profile(request("0.80.0")).is_err());
    assert!(prepare_provider_profile(request("9.0.0")).is_err());
    let mut acp_request = request("0.81.0");
    acp_request.runtime.adapter_version = Some("0.0.34");
    assert!(prepare_provider_profile(acp_request).is_err());
}

#[test]
fn local_codex_defaults_are_not_requirements_and_unrelated_policy_is_untouched() {
    let root = scratch();
    std::fs::create_dir(root.path().join("codex")).unwrap();
    std::fs::write(
        root.path().join("codex/config.toml"),
        "not requirements or even valid TOML",
    )
    .unwrap();
    std::fs::write(root.path().join("codex/requirements.toml"), "[mcp_servers]").unwrap();
    let pi = policy::read_linux_system_policy("pi", root.path(), &[]).unwrap();
    pi.validate_server("allowed", &server(&[])).unwrap();
    let codex = policy::read_linux_system_policy("codex", root.path(), &[]).unwrap();
    assert!(codex.validate_server("allowed", &server(&[])).is_err());
    std::fs::write(
        root.path().join("codex/managed_config.toml"),
        "unsupported legacy constraints",
    )
    .unwrap();
    assert!(policy::read_linux_system_policy("codex", root.path(), &[]).is_err());
}

#[test]
fn native_precedence_cannot_be_mistaken_for_independent_policy_intersection() {
    use policy::{PolicyFormat, PolicyScope, PolicySource};
    let native = || {
        PolicySource::inline(
            PolicyScope::Provider("codex".into()),
            "native",
            PolicyFormat::CodexRequirements,
            "[mcp_servers]",
        )
    };
    assert!(policy::read_host_policy("codex", &[native(), native()]).is_err());
}

#[test]
#[cfg(unix)]
fn dangling_managed_policy_is_present_and_must_not_be_ignored() {
    use policy::{PolicyFormat, PolicyInput, PolicyScope, PolicySource};
    let root = scratch();
    let path = root.path().join("requirements.toml");
    std::os::unix::fs::symlink(root.path().join("missing"), &path).unwrap();
    let source = PolicySource {
        scope: PolicyScope::Provider("codex".into()),
        label: "system".into(),
        format: PolicyFormat::CodexRequirements,
        input: PolicyInput::File {
            path,
            optional: true,
        },
    };
    assert!(policy::read_host_policy("codex", &[source]).is_err());
}

#[test]
#[cfg(unix)]
#[ignore = "requires the official Pi 0.81.0 package at INTENT_PI_FIXTURE_RUNTIME"]
fn generated_pi_profile_passes_real_cli_fixture() {
    let runtime = std::env::var("INTENT_PI_FIXTURE_RUNTIME")
        .expect("set INTENT_PI_FIXTURE_RUNTIME to the official 0.81.0 package directory");
    let root = scratch();
    let context = AuthModelContext {
        model: Some("fixture-model".into()),
        endpoint: Some(ModelEndpoint {
            provider_id: "fixture".into(),
            model_id: "fixture-model".into(),
            base_url: "http://127.0.0.1:1/v1".into(),
            api_key: "fixture-only".into(),
            context_window: Some(32000),
            max_output_tokens: Some(1024),
            compaction_reserved: None,
        }),
        credentials: vec![auth::CredentialFile::PiAuth(
            json!({"fixture":{"type":"api_key","key":"fixture-only"}}),
        )],
        ..Default::default()
    };
    let profile = prepare_provider_profile(ProfileRequest {
        provider: "pi",
        runtime: RuntimeIdentity {
            native_version: "0.81.0",
            adapter_version: None,
            os: "linux",
            arch: "x86_64",
        },
        purpose: ProfilePurpose::Ephemeral,
        directory: ProfileDirectory::ephemeral(root.path()).unwrap(),
        approved_servers: &BTreeMap::default(),
        auth_model: &context,
        policy: &policy::HostPolicySnapshot::default(),
    })
    .unwrap();
    let mut environment = BTreeMap::new();
    profile.environment.apply_to_map(&mut environment);
    let controls = json!({"directory":profile.directory.path(),"runtime_args":profile.runtime_args,"environment":environment});
    let file = root.path().join("controls.json");
    std::fs::write(&file, controls.to_string()).unwrap();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/provider_profile/fixtures/pi-cli.py");
    let output = std::process::Command::new("python3")
        .arg(fixture)
        .arg(runtime)
        .arg(file)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Pi fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("PASS Pi 0.81.0"));
}

#[test]
fn every_registered_provider_has_an_explicit_profile_strategy() {
    for provider in intent_providers::all_provider_ids() {
        let evidence = capability_evidence(provider).unwrap();
        assert!(!evidence.source_baseline.is_empty());
    }
    assert!(capability_evidence("unregistered").is_err());
}

#[test]
fn revision_url_denials_cover_http_and_sse() {
    use policy::{PolicyFormat, PolicyScope, PolicySource};
    let url = "https://denied.invalid/mcp";
    for (provider, format, documents) in [
        (
            "pi",
            PolicyFormat::Intent,
            vec![
                json!({"denyMcp":[{"identity":{"url":url}}]}),
                json!({"allowMcp":[{"name":"remote"}],"denyMcp":[{"identity":{"url":url}}]}),
            ],
        ),
        (
            "claude-code",
            PolicyFormat::ClaudeManagedMcp,
            vec![
                json!({"deniedMcpServers":[{"serverUrl":url}]}),
                json!({"allowedMcpServers":[{"serverName":"remote"}],"deniedMcpServers":[{"serverUrl":url}]}),
            ],
        ),
    ] {
        for document in documents {
            let source =
                PolicySource::inline(PolicyScope::Host, "fixture", format, &document.to_string());
            let policy = policy::read_host_policy(provider, &[source]).unwrap();
            for server in [
                NormalizedMcpServer::Http {
                    url: url.into(),
                    headers: None,
                },
                NormalizedMcpServer::Sse {
                    url: url.into(),
                    headers: None,
                },
            ] {
                assert!(
                    policy.validate_server("remote", &server).is_err(),
                    "URL denial must cover every remote transport"
                );
            }
        }
    }
}

#[test]
#[cfg(unix)]
fn revision_reopened_profile_cannot_delete_live_identity() {
    let root = scratch();
    let identity = ProfileIdentity {
        workspace: "fixture",
        agent: "one",
        provider: "pi",
    };
    let first = ProfileDirectory::persistent(root.path(), &identity).unwrap();
    let second = ProfileDirectory::persistent(&root.path().join("."), &identity).unwrap();
    assert!(matches!(
        second.remove_persistent(),
        Err(ProfileError::InUse)
    ));
    assert!(first.path().exists());
    let second = ProfileDirectory::persistent(root.path(), &identity).unwrap();
    assert!(matches!(
        first.remove_persistent(),
        Err(ProfileError::InUse)
    ));
    let path = second.path().to_owned();
    second.remove_persistent().unwrap();
    assert!(!path.exists());
}

fn fixture_endpoint(key: &str) -> ModelEndpoint {
    ModelEndpoint {
        provider_id: "fixture".into(),
        model_id: "fixture-model".into(),
        base_url: "http://127.0.0.1:1/v1".into(),
        api_key: key.into(),
        context_window: Some(32000),
        max_output_tokens: Some(1024),
        compaction_reserved: None,
    }
}

#[test]
#[cfg(unix)]
fn revision_withdrawn_route_preserves_history_and_retained_credentials() {
    let root = scratch();
    let identity = ProfileIdentity {
        workspace: "fixture",
        agent: "one",
        provider: "pi",
    };
    let directory = ProfileDirectory::persistent(root.path(), &identity).unwrap();
    let profile = prepare_profile_candidate(
        "pi",
        ProfilePurpose::Interactive,
        directory,
        &BTreeMap::new(),
        &AuthModelContext {
            endpoint: Some(fixture_endpoint("fixture-only")),
            credentials: vec![auth::CredentialFile::PiAuth(
                json!({"fixture":{"type":"api_key","key":"fixture-only"}}),
            )],
            ..Default::default()
        },
        &policy::HostPolicySnapshot::default(),
    )
    .unwrap();
    profile
        .directory
        .write_private("history.jsonl", b"fixture history")
        .unwrap();
    let path = profile.directory.path().to_owned();
    drop(profile);
    let profile = prepare_profile_candidate(
        "pi",
        ProfilePurpose::Interactive,
        ProfileDirectory::persistent(root.path(), &identity).unwrap(),
        &BTreeMap::new(),
        &AuthModelContext::default(),
        &policy::HostPolicySnapshot::default(),
    )
    .unwrap();
    assert!(
        !path.join("models.json").exists(),
        "withdrawn generated model route must be removed"
    );
    assert!(
        path.join("auth.json").exists(),
        "omitted credential update means retain native refresh state"
    );
    assert_eq!(
        std::fs::read(path.join("history.jsonl")).unwrap(),
        b"fixture history"
    );
    drop(profile);
}

#[test]
#[cfg(unix)]
fn revision_failed_rebuild_does_not_publish_partial_configuration() {
    let root = scratch();
    let directory = ProfileDirectory::ephemeral(root.path()).unwrap();
    directory
        .write_private("settings.json", b"original")
        .unwrap();
    let result = prepare_profile_candidate(
        "pi",
        ProfilePurpose::Interactive,
        directory.clone(),
        &BTreeMap::new(),
        &AuthModelContext {
            endpoint: Some(fixture_endpoint("fixture-only")),
            credentials: vec![auth::CredentialFile::CodexAuth(json!({}))],
            ..Default::default()
        },
        &policy::HostPolicySnapshot::default(),
    );
    assert!(result.is_err());
    assert_eq!(
        std::fs::read(directory.path().join("settings.json")).unwrap(),
        b"original"
    );
    assert!(!directory.path().join("models.json").exists());
}

#[test]
#[cfg(unix)]
#[ignore = "requires official Pi 0.81.0 at INTENT_PI_FIXTURE_RUNTIME"]
fn revision_pi_native_auth_preserves_literal_values() {
    let runtime = std::env::var("INTENT_PI_FIXTURE_RUNTIME").unwrap();
    let root = scratch();
    for key in [
        "fixture-only",
        "fixture$PROFILE_REVIEW_MISSING",
        "${MISSING}$$!tail",
        "!fixture-literal",
    ] {
        let profile = prepare_profile_candidate(
            "pi",
            ProfilePurpose::Ephemeral,
            ProfileDirectory::ephemeral(root.path()).unwrap(),
            &BTreeMap::new(),
            &AuthModelContext {
                endpoint: Some(fixture_endpoint(key)),
                ..Default::default()
            },
            &policy::HostPolicySnapshot::default(),
        )
        .unwrap();
        let input = root.path().join("expected.json");
        std::fs::write(&input, json!({"apiKey":key}).to_string()).unwrap();
        let output = std::process::Command::new("node")
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("HOME", root.path())
            .env("PI_OFFLINE", "1")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("src/provider_profile/fixtures/pi-auth.mjs"),
            )
            .arg(&runtime)
            .arg(profile.directory.path())
            .arg(&input)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "native fixture must resolve exact synthetic credential; no credential values logged"
        );
        let rebuilt = prepare_profile_candidate(
            "pi",
            ProfilePurpose::Ephemeral,
            profile.directory.clone(),
            &BTreeMap::new(),
            &AuthModelContext::default(),
            &policy::HostPolicySnapshot::default(),
        )
        .unwrap();
        std::fs::write(&input, json!({"absent":true}).to_string()).unwrap();
        let output = std::process::Command::new("node")
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("HOME", root.path())
            .env("PI_OFFLINE", "1")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("src/provider_profile/fixtures/pi-auth.mjs"),
            )
            .arg(&runtime)
            .arg(rebuilt.directory.path())
            .arg(&input)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "native runtime must not retain withdrawn generated route"
        );
    }
}

#[test]
#[cfg(unix)]
fn revision_credentials_have_explicit_retain_replace_and_remove_semantics() {
    let root = scratch();
    let directory = ProfileDirectory::ephemeral(root.path()).unwrap();
    let prepare = |context: &AuthModelContext| {
        prepare_profile_candidate(
            "pi",
            ProfilePurpose::Interactive,
            directory.clone(),
            &BTreeMap::new(),
            context,
            &policy::HostPolicySnapshot::default(),
        )
    };
    let first = AuthModelContext {
        credentials: vec![auth::CredentialFile::PiAuth(
            json!({"fixture":{"type":"api_key","key":"first"}}),
        )],
        ..Default::default()
    };
    prepare(&first).unwrap();
    let refreshed = b"{\"fixture\":{\"type\":\"api_key\",\"key\":\"refreshed\"}}";
    directory.write_private("auth.json", refreshed).unwrap();
    let retained = prepare(&AuthModelContext::default()).unwrap();
    assert_eq!(
        std::fs::read(directory.path().join("auth.json")).unwrap(),
        refreshed
    );
    let replaced = prepare(&first).unwrap();
    assert!(retained.configuration_identity != replaced.configuration_identity);
    let revoke = AuthModelContext {
        credential_removals: vec![auth::CredentialKind::Pi],
        ..Default::default()
    };
    prepare(&revoke).unwrap();
    assert!(!directory.path().join("auth.json").exists());
    let conflict = AuthModelContext {
        credential_removals: vec![auth::CredentialKind::Pi],
        ..first
    };
    assert!(prepare(&conflict).is_err());
    assert!(!directory.path().join("auth.json").exists());
}

#[test]
#[cfg(unix)]
fn revision_failed_filesystem_rebuild_preserves_previous_files() {
    let root = scratch();
    let directory = ProfileDirectory::ephemeral(root.path()).unwrap();
    directory
        .write_private("settings.json", b"old settings")
        .unwrap();
    std::fs::create_dir(directory.path().join("models.json")).unwrap();
    let result = prepare_profile_candidate(
        "pi",
        ProfilePurpose::Interactive,
        directory.clone(),
        &BTreeMap::new(),
        &AuthModelContext {
            endpoint: Some(fixture_endpoint("fixture-only")),
            ..Default::default()
        },
        &policy::HostPolicySnapshot::default(),
    );
    assert!(result.is_err());
    assert_eq!(
        std::fs::read(directory.path().join("settings.json")).unwrap(),
        b"old settings"
    );
    assert!(directory.path().join("models.json").is_dir());
}

#[test]
fn revision_policy_identity_binds_provider_and_effective_restrictions() {
    use policy::{PolicyFormat, PolicyScope, PolicySource};
    let snapshot = |provider, text| {
        policy::read_host_policy(
            provider,
            &[PolicySource::inline(
                PolicyScope::Host,
                "fixture",
                PolicyFormat::Intent,
                text,
            )],
        )
        .unwrap()
    };
    let a = snapshot("pi", r#"{"denyMcp":[{"name":"blocked"}]}"#);
    let b = snapshot("pi", r#"{ "denyMcp" : [ {"name": "blocked"} ] }"#);
    assert!(a.identity() == b.identity());
    assert!(a.identity() != snapshot("pi", "{}").identity());
    assert!(a.identity() != snapshot("codex", r#"{"denyMcp":[{"name":"blocked"}]}"#).identity());
    assert!(a.validate_provider("codex").is_err());
    assert!(snapshot("pi", "{}").identity() != snapshot("pi", r#"{"allowMcp":[]}"#).identity());
}

#[test]
#[cfg(unix)]
fn revision_profile_identity_tracks_policy_routing_and_retained_auth_without_history() {
    use policy::{PolicyFormat, PolicyScope, PolicySource};
    let root = scratch();
    let directory = ProfileDirectory::ephemeral(root.path()).unwrap();
    let prepare = |context: &AuthModelContext, policy: &policy::HostPolicySnapshot| {
        prepare_profile_candidate(
            "pi",
            ProfilePurpose::Interactive,
            directory.clone(),
            &BTreeMap::new(),
            context,
            policy,
        )
        .unwrap()
    };
    let context = AuthModelContext::default();
    let unrestricted = policy::HostPolicySnapshot::default();
    let a = prepare(&context, &unrestricted);
    directory
        .write_private("history.jsonl", b"native history")
        .unwrap();
    assert!(a.configuration_identity == prepare(&context, &unrestricted).configuration_identity);
    let denied = policy::read_host_policy(
        "pi",
        &[PolicySource::inline(
            PolicyScope::Host,
            "fixture",
            PolicyFormat::Intent,
            r#"{"allowSkills":false}"#,
        )],
    )
    .unwrap();
    assert!(a.configuration_identity != prepare(&context, &denied).configuration_identity);
    let routed = AuthModelContext {
        endpoint: Some(fixture_endpoint("fixture")),
        ..Default::default()
    };
    assert!(a.configuration_identity != prepare(&routed, &unrestricted).configuration_identity);
    directory.write_private("auth.json", b"{}").unwrap();
    assert!(a.configuration_identity != prepare(&context, &unrestricted).configuration_identity);
}

#[test]
#[cfg(unix)]
fn revision_unsupported_runtime_does_not_mutate_existing_profile() {
    let root = scratch();
    let directory = ProfileDirectory::ephemeral(root.path()).unwrap();
    directory
        .write_private("settings.json", b"previous settings")
        .unwrap();
    let result = prepare_provider_profile(ProfileRequest {
        provider: "pi",
        runtime: RuntimeIdentity {
            native_version: "unsupported",
            adapter_version: None,
            os: "linux",
            arch: "x86_64",
        },
        purpose: ProfilePurpose::Interactive,
        directory: directory.clone(),
        approved_servers: &BTreeMap::new(),
        auth_model: &AuthModelContext::default(),
        policy: &policy::HostPolicySnapshot::default(),
    });
    assert!(result.is_err());
    assert_eq!(
        std::fs::read(directory.path().join("settings.json")).unwrap(),
        b"previous settings"
    );
}
