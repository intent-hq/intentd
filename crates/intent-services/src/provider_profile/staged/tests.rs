use super::*;
use crate::provider_profile::{auth, ProfileError};
use serde_json::json;
use std::collections::BTreeMap;

fn scratch() -> tempfile::TempDir {
    super::super::tests::scratch()
}

fn request<'a>(
    provider: &'a str,
    etc: &'a Path,
    auth: &'a AuthModelContext,
    servers: &'a NormalizedMcpServers,
) -> SelectionRequest<'a> {
    SelectionRequest {
        provider,
        runtime: RuntimeIdentity {
            native_version: if provider == "pi" {
                "0.81.0"
            } else {
                "2.1.280"
            },
            adapter_version: None,
            os: "linux",
            arch: "x86_64",
        },
        sdk_version: None,
        purpose: ProfilePurpose::Interactive,
        approved_servers: servers,
        auth_model: auth,
        instructions: "Owned workspace instructions.",
        has_skill_instructions: false,
        policy: PolicyAcquisition {
            etc_root: etc,
            authority: PolicyAuthority::LocalFilesOnly,
            additional: &[],
        },
    }
}

fn managed(request: SelectionRequest<'_>) -> ManagedProfilePlan<'_> {
    match select(request) {
        ProfileSelection::Managed(plan) => *plan,
        ProfileSelection::Deferred(reason) => panic!("unexpected deferral: {reason:?}"),
    }
}

fn deferred(request: SelectionRequest<'_>) -> DeferredReason {
    match select(request) {
        ProfileSelection::Managed(_) => panic!("unexpected managed selection"),
        ProfileSelection::Deferred(reason) => reason,
    }
}

#[test]
fn exact_stage_matrix_defers_unverified_paths_without_writes() {
    let root = scratch();
    let auth = AuthModelContext::default();
    let servers = BTreeMap::new();
    for provider in intent_providers::all_provider_ids() {
        for purpose in [ProfilePurpose::Interactive, ProfilePurpose::Ephemeral] {
            let mut r = request(provider, root.path(), &auth, &servers);
            r.purpose = purpose;
            if matches!(provider, "pi" | "claude-code") {
                let _ = managed(r);
            } else {
                assert_eq!(deferred(r), DeferredReason::ProviderControls);
            }
        }
    }
    let mut r = request("claude-code", root.path(), &auth, &servers);
    r.runtime.native_version = "2.1.281";
    assert_eq!(deferred(r), DeferredReason::RuntimeVersion);
    let mut r = request("pi", root.path(), &auth, &servers);
    r.runtime.os = "macos";
    assert_eq!(deferred(r), DeferredReason::Platform);
    let mut r = request("pi", root.path(), &auth, &servers);
    r.runtime.adapter_version = Some("0.81.1");
    assert_eq!(deferred(r), DeferredReason::PiGatewayDelivery);
    let mut r = request("claude-code", root.path(), &auth, &servers);
    r.runtime.adapter_version = Some("0.81.1");
    assert_eq!(deferred(r), DeferredReason::AdapterDelivery);
    let mut r = request("claude-code", root.path(), &auth, &servers);
    r.runtime.adapter_version = Some("0.81.1");
    r.sdk_version = Some("0.3.280");
    let _ = managed(r);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn exclusive_and_unresolved_policy_defer_but_post_selection_changes_fail() {
    let root = scratch();
    let auth = AuthModelContext::default();
    let servers = BTreeMap::new();
    let mut r = request("claude-code", root.path(), &auth, &servers);
    r.policy.authority = PolicyAuthority::Unresolved;
    assert_eq!(deferred(r), DeferredReason::PolicyAuthority);
    let plan = managed(request("claude-code", root.path(), &auth, &servers));
    std::fs::create_dir(root.path().join("claude-code")).unwrap();
    std::fs::write(root.path().join("claude-code/managed-mcp.json"), "{}").unwrap();
    assert_eq!(
        deferred(request("claude-code", root.path(), &auth, &servers)),
        DeferredReason::ExclusiveManagedMcp
    );
    assert!(plan
        .build(ProfileDirectory::ephemeral(root.path()).unwrap())
        .is_err());
    std::fs::remove_file(root.path().join("claude-code/managed-mcp.json")).unwrap();
    let plan = managed(request("claude-code", root.path(), &auth, &servers));
    std::fs::write(
        root.path().join("claude-code/managed-settings.json"),
        r#"{"allowedMcpServers":[]}"#,
    )
    .unwrap();
    assert!(plan
        .build(ProfileDirectory::ephemeral(root.path()).unwrap())
        .is_err());
}

#[test]
fn managed_restrictions_work_and_denials_cannot_fall_back() {
    let root = scratch();
    let auth = AuthModelContext::default();
    std::fs::create_dir(root.path().join("claude-code")).unwrap();
    std::fs::write(
        root.path().join("claude-code/managed-settings.json"),
        r#"{"allowedMcpServers":[{"serverName":"approved"}]}"#,
    )
    .unwrap();
    let servers = intent_acp::normalize_mcp_servers(
        &json!({"approved":{"command":"fixture-only","args":[]}}),
    );
    managed(request("claude-code", root.path(), &auth, &servers))
        .build(ProfileDirectory::ephemeral(root.path()).unwrap())
        .unwrap()
        .ensure_launchable()
        .unwrap();
    let denied =
        intent_acp::normalize_mcp_servers(&json!({"denied":{"command":"fixture-only","args":[]}}));
    assert!(matches!(
        managed(request("claude-code", root.path(), &auth, &denied))
            .build(ProfileDirectory::ephemeral(root.path()).unwrap()),
        Err(ProfileError::PolicyDenied { .. })
    ));
    assert_eq!(
        deferred(request("pi", root.path(), &auth, &servers)),
        DeferredReason::PiGatewayDelivery
    );
}

#[test]
fn managed_auth_failure_is_an_error_and_does_not_replace_existing_files() {
    let root = scratch();
    let servers = BTreeMap::new();
    let auth = AuthModelContext {
        credential_environment: BTreeMap::from([("NODE_OPTIONS".into(), "must-not-run".into())]),
        ..Default::default()
    };
    let directory = ProfileDirectory::ephemeral(root.path()).unwrap();
    let path = directory.path().to_owned();
    std::fs::write(path.join("mcp.json"), "before").unwrap();
    assert!(matches!(
        managed(request("claude-code", root.path(), &auth, &servers)).build(directory.clone()),
        Err(ProfileError::InvalidAuth(_))
    ));
    assert_eq!(
        std::fs::read_to_string(path.join("mcp.json")).unwrap(),
        "before"
    );
}

#[test]
fn staged_claude_preserves_typed_auth_model_instructions_and_rebuild_identity() {
    let root = scratch();
    let servers = BTreeMap::new();
    let auth = AuthModelContext {
        model: Some("fixture-model".into()),
        credential_environment: BTreeMap::from([(
            "ANTHROPIC_API_KEY".into(),
            "synthetic-$key".into(),
        )]),
        credentials: vec![auth::CredentialFile::ClaudeCredentials(
            json!({"fixture":"synthetic-refresh-state"}),
        )],
        claude_settings: Some(
            json!({"hooks":{"SessionStart":"never"},"env":{"NODE_OPTIONS":"never"}}),
        ),
        ..Default::default()
    };
    let directory = ProfileDirectory::ephemeral(root.path()).unwrap();
    let a = managed(request("claude-code", root.path(), &auth, &servers))
        .build(directory.clone())
        .unwrap();
    let options = &a.session_meta["claudeCode"]["options"];
    assert_eq!(options["model"], "fixture-model");
    assert_eq!(options["settings"]["disableClaudeAiConnectors"], true);
    assert!(options["settings"].get("hooks").is_none());
    assert_eq!(
        a.session_meta["systemPrompt"]["append"],
        "Owned workspace instructions."
    );
    let mut env = BTreeMap::from([("NODE_OPTIONS".into(), "ambient".into())]);
    a.environment.apply_to_map(&mut env);
    assert!(!env.contains_key("NODE_OPTIONS"));
    assert_eq!(env["ANTHROPIC_API_KEY"], "synthetic-$key");
    assert_eq!(
        std::fs::read_to_string(directory.path().join("mcp.json")).unwrap(),
        "{\"mcpServers\":{}}"
    );
    let mut r = request("claude-code", root.path(), &auth, &servers);
    r.instructions = "Changed owned instructions.";
    let b = managed(r).build(directory).unwrap();
    assert!(a.configuration_identity != b.configuration_identity);
}

#[test]
fn ephemeral_and_host_policy_reject_owned_skills_without_discarding_plain_instructions() {
    let root = scratch();
    let auth = AuthModelContext::default();
    let servers = BTreeMap::new();
    for provider in ["pi", "claude-code"] {
        let mut r = request(provider, root.path(), &auth, &servers);
        r.purpose = ProfilePurpose::Ephemeral;
        managed(r)
            .build(ProfileDirectory::ephemeral(root.path()).unwrap())
            .unwrap();
        let mut r = request(provider, root.path(), &auth, &servers);
        r.purpose = ProfilePurpose::Ephemeral;
        r.has_skill_instructions = true;
        assert!(matches!(
            managed(r).build(ProfileDirectory::ephemeral(root.path()).unwrap()),
            Err(ProfileError::EphemeralCatalog)
        ));
        let sources = [policy::PolicySource::inline(
            policy::PolicyScope::Host,
            "intent",
            policy::PolicyFormat::Intent,
            r#"{"allowSkills":false}"#,
        )];
        let mut r = request(provider, root.path(), &auth, &servers);
        r.has_skill_instructions = true;
        r.policy.additional = &sources;
        assert!(matches!(
            managed(r).build(ProfileDirectory::ephemeral(root.path()).unwrap()),
            Err(ProfileError::PolicyDenied { .. })
        ));
    }
}

#[test]
#[ignore = "requires pinned Claude runtime/SDK, node and Linux bwrap"]
fn generated_staged_claude_preserves_native_auth_model_and_instructions() {
    let modules =
        std::env::var("INTENT_CLAUDE_FIXTURE_MODULES").expect("set pinned Claude modules");
    let root = scratch();
    let servers = BTreeMap::new();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let auth = AuthModelContext {
        model: Some("claude-sonnet-4-6".into()),
        credential_environment: BTreeMap::from([
            ("ANTHROPIC_API_KEY".into(), "synthetic-literal-$key".into()),
            ("ANTHROPIC_BASE_URL".into(), url),
        ]),
        ..Default::default()
    };
    let mut r = request("claude-code", root.path(), &auth, &servers);
    r.instructions = "OWNED-STAGED-INSTRUCTIONS";
    let profile = managed(r)
        .build(ProfileDirectory::ephemeral(root.path()).unwrap())
        .unwrap();
    let mut environment = BTreeMap::new();
    profile.environment.apply_to_map(&mut environment);
    let controls = json!({"directory":profile.directory.path(),"environment":environment,"runtime_args":profile.runtime_args,"session_meta":profile.session_meta});
    let file = root.path().join("controls.json");
    std::fs::write(&file, controls.to_string()).unwrap();
    let output = std::process::Command::new("node")
        .env_remove("NODE_OPTIONS")
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/provider_profile/fixtures/staged-claude.mjs"),
        )
        .arg(modules)
        .arg(file)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "staged fixture: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("PASS staged Claude"));
}

#[test]
#[ignore = "requires official Pi 0.81.0 package"]
fn generated_staged_pi_profile_preserves_cli_resume_and_instruction_sources() {
    let runtime = std::env::var("INTENT_PI_FIXTURE_RUNTIME").expect("set pinned Pi runtime");
    let root = scratch();
    let servers = BTreeMap::new();
    let auth = AuthModelContext {
        model: Some("fixture-model".into()),
        endpoint: Some(super::super::ModelEndpoint {
            provider_id: "fixture".into(),
            model_id: "fixture-model".into(),
            base_url: "http://127.0.0.1:1/v1".into(),
            api_key: "synthetic-$key".into(),
            context_window: Some(32000),
            max_output_tokens: Some(1024),
            compaction_reserved: None,
        }),
        ..Default::default()
    };
    let mut r = request("pi", root.path(), &auth, &servers);
    r.instructions = "OWNED-STAGED-INSTRUCTIONS";
    r.purpose = ProfilePurpose::Ephemeral;
    let profile = managed(r)
        .build(ProfileDirectory::ephemeral(root.path()).unwrap())
        .unwrap();
    let mut environment = BTreeMap::new();
    profile.environment.apply_to_map(&mut environment);
    let file = root.path().join("controls.json");
    std::fs::write(&file,json!({"directory":profile.directory.path(),"runtime_args":profile.runtime_args,"environment":environment}).to_string()).unwrap();
    for (program, fixture, marker) in [
        ("node", "staged-pi.mjs", "PASS staged Pi"),
        ("python3", "pi-cli.py", "PASS Pi 0.81.0"),
    ] {
        let output = std::process::Command::new(program)
            .env_remove("NODE_OPTIONS")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("src/provider_profile/fixtures")
                    .join(fixture),
            )
            .arg(&runtime)
            .arg(&file)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{fixture}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains(marker));
    }
}

#[test]
fn ephemeral_catalog_error_is_not_a_gateway_deferral() {
    let root = scratch();
    let auth = AuthModelContext::default();
    let servers = intent_acp::normalize_mcp_servers(
        &json!({"approved":{"command":"fixture-only","args":[]}}),
    );
    for provider in ["pi", "claude-code"] {
        let mut r = request(provider, root.path(), &auth, &servers);
        r.purpose = ProfilePurpose::Ephemeral;
        assert!(matches!(
            managed(r).build(ProfileDirectory::ephemeral(root.path()).unwrap()),
            Err(ProfileError::EphemeralCatalog)
        ));
    }
}

#[test]
fn unsupported_policy_and_auth_are_explicit_preselection_deferrals() {
    let root = scratch();
    let servers = BTreeMap::new();
    let auth = AuthModelContext {
        claude_settings: Some(json!({"apiKeyHelper":"fixture-do-not-execute"})),
        ..Default::default()
    };
    assert_eq!(
        deferred(request("claude-code", root.path(), &auth, &servers)),
        DeferredReason::AuthProjection
    );
    let auth = AuthModelContext::default();
    std::fs::create_dir(root.path().join("claude-code")).unwrap();
    std::fs::write(
        root.path().join("claude-code/managed-settings.json"),
        r#"{"allowedMcpServers":[{"serverUrl":"https://*.invalid/*"}]}"#,
    )
    .unwrap();
    assert_eq!(
        deferred(request("claude-code", root.path(), &auth, &servers)),
        DeferredReason::PolicyAcquisition
    );
    let sources = [policy::PolicySource::unavailable(
        policy::PolicyScope::Provider("pi".into()),
        "organization",
        "policy unavailable",
    )];
    let mut r = request("pi", root.path(), &auth, &servers);
    r.policy.authority = PolicyAuthority::EffectiveSourcesResolved;
    r.policy.additional = &sources;
    assert_eq!(deferred(r), DeferredReason::PolicyAcquisition);
}
