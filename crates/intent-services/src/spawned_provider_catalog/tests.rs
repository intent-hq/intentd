use super::*;
use serde_json::json;

fn write(root: &Path, relative: &str, text: &str) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

fn input(root: &Path) -> CatalogInputs<'_> {
    CatalogInputs {
        workspace_id: "workspace",
        root: Some(root),
        cwd: root,
        purpose: CatalogPurpose::Interactive,
        enable_user_servers: true,
        explicit: &[],
        global_disabled_ids: &[],
        workspace_disabled_ids: &[],
        environment: BTreeMap::new(),
    }
}

fn explicit(id: &str, name: &str) -> ExplicitServer {
    ExplicitServer {
        id: id.into(),
        name: name.into(),
        enabled: true,
        server: Some(NormalizedMcpServer::Stdio {
            command: "explicit".into(),
            args: vec![],
            env: BTreeMap::new(),
        }),
        execution: ExecutionOptions::default(),
    }
}

#[test]
fn disabled_ids_are_not_names_and_workspace_restrictions_leave_tombstones() {
    let root = crate::test_support::test_tempdir("spawn-catalog-ids-");
    write(
        root.path(),
        ".mcp.json",
        r#"{"mcpServers":{"alpha":{"command":"project"}}}"#,
    );
    let definitions = [explicit("srv-a", "alpha"), explicit("srv-b", "srv-a")];
    let disabled = ["srv-a".into()];
    for workspace_disabled in [false, true] {
        let mut inputs = input(root.path());
        inputs.explicit = &definitions;
        if workspace_disabled {
            inputs.workspace_disabled_ids = &disabled;
        } else {
            inputs.global_disabled_ids = &disabled;
        }
        let catalog = resolve_project_catalog(&inputs).unwrap();
        assert_eq!(catalog.servers.keys().collect::<Vec<_>>(), vec!["srv-a"]);
        assert!(!catalog.entries["alpha"].enabled);
        assert_eq!(
            catalog.entries["alpha"].identity,
            ServerIdentity::Intent { id: "srv-a".into() }
        );
    }
}

#[test]
fn precedence_replaces_whole_definitions_and_retains_source_identity() {
    let root = crate::test_support::test_tempdir("spawn-catalog-precedence-");
    write(
        root.path(),
        ".pi/mcp.json",
        r#"{"mcpServers":{"tool":{"command":"pi","env":{"OLD":"old"}}}}"#,
    );
    write(
        root.path(),
        ".mcp.json",
        r#"{"mcpServers":{"tool":{"command":"root"}}}"#,
    );
    write(root.path(), "nested/.codex/config.toml", "[mcp_servers.tool]\ncommand = './bin/tool'\nargs = ['nested']\ncwd = './run'\nrequired = true\nstartup_timeout_sec = 2.5\nenabled_tools = []\n");
    let mut inputs = input(root.path());
    let cwd = root.path().join("nested");
    inputs.cwd = &cwd;
    let catalog = resolve_project_catalog(&inputs).unwrap();
    let entry = &catalog.entries["tool"];
    assert_eq!(
        entry.identity,
        ServerIdentity::Project {
            workspace_id: "workspace".into(),
            source_relative_path: "nested/.codex/config.toml".into(),
            server_key: "tool".into(),
        }
    );
    let NormalizedMcpServer::Stdio { command, args, env } = &catalog.servers["tool"] else {
        panic!()
    };
    assert_eq!(command, &cwd.join("bin/tool").to_string_lossy());
    assert_eq!(args, &["nested"]);
    assert!(env.is_empty());
    assert_eq!(entry.execution.cwd, Some(cwd.join("run")));
    assert_eq!(entry.execution.startup_timeout_ms, Some(2500));
    assert_eq!(entry.execution.enabled_tools, Some(vec![]));
    assert!(entry.execution.required);
    let definitions = [explicit("explicit", "tool")];
    inputs.explicit = &definitions;
    let catalog = resolve_project_catalog(&inputs).unwrap();
    assert_eq!(
        catalog.entries["tool"].execution,
        ExecutionOptions::default()
    );
}

#[test]
fn all_formats_and_jsonc_are_supported_without_loading_other_settings() {
    let root = crate::test_support::test_tempdir("spawn-catalog-formats-");
    for (path, content) in [
        (".pi/mcp.json", r#"{"mcpServers":{"pi":{"command":"pi"}}}"#),
        (".augment/settings.json", r#"{"plugins":["never-run"],"mcpServers":{"augment":{"url":"https://example.invalid/sse","type":"sse"}}}"#),
        (".opencode/opencode.json", r#"{"mcp":{"opencode1":{"type":"local","command":["node","server.js"],"environment":{"A":"B"}}}}"#),
        (".opencode/opencode.jsonc", "{ /*comment*/ \"mcp\": {\"opencode2\": {\"type\":\"remote\",\"url\":\"https://example.invalid/a//b\",},},}"),
        ("opencode.json", r#"{"mcp":{"opencode3":{"enabled":false}}}"#),
        ("opencode.jsonc", "{ // comment\n\"mcp\":{\"opencode4\":{\"type\":\"local\",\"command\":[\"node\"]}}}"),
        (".grok/config.toml", "[mcp_servers.grok]\ncommand='grok'"),
        (".factory/mcp.json", r#"{"mcpServers":{"factory":{"command":"factory","disabledTools":["delete"],"timeout":50,"connectTimeout":25}}}"#),
        (".codex/config.toml", "[mcp_servers.codex]\nurl='https://example.invalid'\nbearer_token_env_var='TOKEN'\nenv_http_headers={X='TOKEN'}"),
        (".mcp.json", r#"{"mcpServers":{"common":{"command":"common"}}}"#),
    ] { write(root.path(), path, content); }
    let mut inputs = input(root.path());
    inputs
        .environment
        .insert("TOKEN".into(), "fixture-token".into());
    let catalog = resolve_project_catalog(&inputs).unwrap();
    assert_eq!(catalog.entries.len(), 10);
    assert_eq!(catalog.servers.len(), 9);
    assert_eq!(
        catalog.entries["factory"].execution.disabled_tools,
        vec!["delete"]
    );
    assert_eq!(
        catalog.entries["factory"].execution.tool_timeout_ms,
        Some(50)
    );
    let NormalizedMcpServer::Http {
        headers: Some(headers),
        ..
    } = &catalog.servers["codex"]
    else {
        panic!()
    };
    assert_eq!(headers["Authorization"], "Bearer fixture-token");
}

#[test]
fn invalid_sources_fail_with_redacted_diagnostics() {
    let root = crate::test_support::test_tempdir("spawn-catalog-errors-");
    for content in [
        r#"{"mcpServers":{"a":{"command":123}}}"#,
        r#"{"mcpServers":{"a":{"command":"ok","url":"SECRET"}}}"#,
        r#"{"mcpServers":{"a":{"command":"ok","headersHelper":"SECRET"}}}"#,
        r#"{"mcpServers":{"a":{"type":"sdk","url":"SECRET"}}}"#,
        r#"{"mcpServers":{"a":{"command":"ok","args":[12]}}}"#,
        r#"{"mcpServers":{"a":{"command":"a"},"a":{"command":"b"}}}"#,
        r#"{"mcpServers": {"a": "SECRET"}}"#,
        r#"{"mcpServers": null}"#,
        r#"{"mcpServers": "SECRET""#,
    ] {
        write(root.path(), ".mcp.json", content);
        let err = resolve_project_catalog(&input(root.path()))
            .err()
            .expect("must reject");
        assert_eq!(err.source, ".mcp.json");
        assert!(!err.to_string().contains("SECRET"));
    }
}

#[test]
fn reserved_names_duplicate_explicit_and_sanitized_collisions_are_errors() {
    let root = crate::test_support::test_tempdir("spawn-catalog-collisions-");
    for names in [
        vec!["workspace-mcp"],
        vec!["workspace_mcp"],
        vec!["workspace mcp"],
        vec!["a b", "a_b"],
        vec!["a.b", "a-b"],
    ] {
        let map: serde_json::Map<_, _> = names
            .into_iter()
            .map(|name| (name.into(), json!({"enabled":false})))
            .collect();
        write(
            root.path(),
            ".mcp.json",
            &json!({"mcpServers":map}).to_string(),
        );
        assert!(resolve_project_catalog(&input(root.path())).is_err());
    }
    std::fs::remove_file(root.path().join(".mcp.json")).unwrap();
    let definitions = [explicit("a", "same"), explicit("b", "same")];
    let mut inputs = input(root.path());
    inputs.explicit = &definitions;
    assert!(resolve_project_catalog(&inputs).is_err());
}

#[test]
fn ephemeral_and_disabled_catalogs_do_not_read_malformed_mcp() {
    let root = crate::test_support::test_tempdir("spawn-catalog-purpose-");
    write(root.path(), ".mcp.json", "malformed");
    let mut inputs = input(root.path());
    inputs.purpose = CatalogPurpose::Ephemeral;
    let catalog = resolve_project_catalog(&inputs).unwrap();
    assert!(catalog.servers.is_empty());
    assert!(catalog.skills.skills.is_empty());
    inputs.purpose = CatalogPurpose::Interactive;
    inputs.enable_user_servers = false;
    assert!(resolve_project_catalog(&inputs).unwrap().servers.is_empty());
    inputs.enable_user_servers = true;
    inputs.root = None;
    assert!(resolve_project_catalog(&inputs).unwrap().servers.is_empty());
}

#[test]
fn expansion_is_format_specific_and_never_reads_ambient_environment() {
    let root = crate::test_support::test_tempdir("spawn-catalog-expansion-");
    write(
        root.path(),
        ".mcp.json",
        r#"{"mcpServers":{"a":{"command":"${BIN}","args":["${MISSING:-fallback}","$LITERAL"]}}}"#,
    );
    let mut inputs = input(root.path());
    assert!(resolve_project_catalog(&inputs).is_err());
    inputs.environment.insert("BIN".into(), "tool".into());
    let catalog = resolve_project_catalog(&inputs).unwrap();
    let NormalizedMcpServer::Stdio { args, .. } = &catalog.servers["a"] else {
        panic!()
    };
    assert_eq!(args, &["fallback", "$LITERAL"]);
    write(
        root.path(),
        "opencode.json",
        r#"{"mcp":{"b":{"type":"local","command":["{env:BIN}","{file:secret}"]}}}"#,
    );
    assert!(resolve_project_catalog(&inputs).is_err());
}

#[cfg(unix)]
#[test]
fn mcp_symlinks_cannot_import_host_sources_or_escape_cwd_boundary() {
    use std::os::unix::fs::symlink;
    let root = crate::test_support::test_tempdir("spawn-catalog-links-");
    let home = crate::test_support::test_tempdir("spawn-catalog-home-");
    write(
        home.path(),
        "host.json",
        r#"{"mcpServers":{"host":{"command":"host"}}}"#,
    );
    symlink(home.path().join("host.json"), root.path().join(".mcp.json")).unwrap();
    assert!(resolve_project_catalog(&input(root.path())).is_err());
    std::fs::remove_file(root.path().join(".mcp.json")).unwrap();
    symlink(home.path(), root.path().join("escape")).unwrap();
    let cwd = root.path().join("escape");
    let mut inputs = input(root.path());
    inputs.cwd = &cwd;
    assert!(resolve_project_catalog(&inputs).is_err());
}

#[test]
fn disabled_project_entries_need_no_credentials_but_must_be_well_typed() {
    let root = crate::test_support::test_tempdir("spawn-catalog-disabled-");
    write(
        root.path(),
        ".pi/mcp.json",
        r#"{"mcpServers":{"a":{"command":"fallback"}}}"#,
    );
    write(
        root.path(),
        ".mcp.json",
        r#"{"mcpServers":{"a":{"enabled":false,"command":"${MISSING}"}}}"#,
    );
    let catalog = resolve_project_catalog(&input(root.path())).unwrap();
    assert!(catalog.servers.is_empty());
    assert!(!catalog.entries["a"].enabled);
    for value in [
        json!({"enabled":false,"env":[]}),
        json!({"enabled":false,"required":"yes"}),
        json!({"enabled":false,"args":{}}),
    ] {
        write(
            root.path(),
            ".mcp.json",
            &json!({"mcpServers":{"a":value}}).to_string(),
        );
        assert!(resolve_project_catalog(&input(root.path())).is_err());
    }
}

#[test]
fn source_order_is_total_and_unknown_disabled_ids_are_inert() {
    let root = crate::test_support::test_tempdir("spawn-catalog-order-");
    for &(path, format) in parser::SOURCES {
        let content = match format {
            parser::Format::Grok | parser::Format::Codex => {
                format!("[mcp_servers.same]\ncommand='{path}'")
            }
            parser::Format::OpenCode => {
                json!({"mcp":{"same":{"type":"local","command":[path]}}}).to_string()
            }
            _ => json!({"mcpServers":{"same":{"command":path}}}).to_string(),
        };
        write(root.path(), path, &content);
    }
    let unknown = ["same".into()];
    let mut inputs = input(root.path());
    inputs.global_disabled_ids = &unknown;
    for &(path, _) in parser::SOURCES.iter().rev() {
        let catalog = resolve_project_catalog(&inputs).unwrap();
        assert_eq!(catalog.entries["same"].source, path);
        assert!(catalog.servers.contains_key("same"));
        std::fs::remove_file(root.path().join(path)).unwrap();
    }
}

#[test]
fn discovery_is_read_only_bounded_and_fingerprinted() {
    let parent = crate::test_support::test_tempdir("spawn-catalog-bounds-");
    let root = parent.path().join("project");
    std::fs::create_dir_all(&root).unwrap();
    write(
        parent.path(),
        ".mcp.json",
        r#"{"mcpServers":{"ambient":{"command":"never"}}}"#,
    );
    let inputs = input(&root);
    let before = resolve_project_catalog(&inputs).unwrap();
    assert!(before.servers.is_empty());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    let text = r#"{"mcpServers":{"a":{"command":"first"}}}"#;
    write(&root, ".mcp.json", text);
    let first = resolve_project_catalog(&inputs).unwrap();
    assert_ne!(before.fingerprint, first.fingerprint);
    assert_eq!(
        first.fingerprint,
        resolve_project_catalog(&inputs).unwrap().fingerprint
    );
    assert_eq!(
        std::fs::read_to_string(root.join(".mcp.json")).unwrap(),
        text
    );
    write(&root, ".mcp.json", &" ".repeat(MAX_SOURCE_BYTES + 1));
    assert!(resolve_project_catalog(&inputs).is_err());
    write(&root,".mcp.json",&json!({"mcpServers":(0..=MAX_SERVERS).map(|n|(format!("server-{n}"),json!({"enabled":false}))).collect::<serde_json::Map<_,_>>()} ).to_string());
    assert!(resolve_project_catalog(&inputs).is_err());
    std::fs::remove_file(root.join(".mcp.json")).unwrap();
    let cwd = (0..MAX_PROJECT_DEPTH).fold(root.clone(), |path, _| path.join("nested"));
    std::fs::create_dir_all(&cwd).unwrap();
    let mut inputs = input(&root);
    inputs.cwd = &cwd;
    assert!(resolve_project_catalog(&inputs).is_err());
}

#[test]
fn expansion_budget_and_format_specific_timeout_are_preserved() {
    let root = crate::test_support::test_tempdir("spawn-catalog-expansion-limit-");
    write(
        root.path(),
        "opencode.json",
        r#"{"mcp":{"a":{"type":"local","command":["node","{literal"],"timeout":7000}}}"#,
    );
    let catalog = resolve_project_catalog(&input(root.path())).unwrap();
    assert_eq!(
        catalog.entries["a"].execution.startup_timeout_ms,
        Some(7000)
    );
    assert_eq!(catalog.entries["a"].execution.tool_timeout_ms, None);
    assert_eq!(
        catalog.entries["a"].execution.cwd,
        Some(root.path().to_path_buf())
    );
    write(
        root.path(),
        ".mcp.json",
        &json!({"mcpServers":{"b":{"command":"node","args":vec!["${BIG}";10]}}}).to_string(),
    );
    let mut inputs = input(root.path());
    inputs
        .environment
        .insert("BIG".into(), "x".repeat(MAX_SOURCE_BYTES));
    assert!(resolve_project_catalog(&inputs).is_err());
}

#[test]
fn malformed_toml_and_jsonc_are_rejected_without_echoing_values() {
    let root = crate::test_support::test_tempdir("spawn-catalog-syntax-");
    for (path, text) in [
        (
            ".codex/config.toml",
            "[mcp_servers.a]\ncommand='SECRET'\ncommand='duplicate'",
        ),
        ("opencode.jsonc", "{,}"),
        ("opencode.jsonc", "{/* SECRET"),
    ] {
        write(root.path(), path, text);
        let err = resolve_project_catalog(&input(root.path()))
            .err()
            .expect("reject invalid syntax");
        assert!(!err.to_string().contains("SECRET"));
        std::fs::remove_file(root.path().join(path)).unwrap();
    }
}

#[test]
fn losing_definitions_and_explicit_tombstones_do_not_require_credentials() {
    let root = crate::test_support::test_tempdir("spawn-catalog-deferred-env-");
    write(
        root.path(),
        ".codex/config.toml",
        "[mcp_servers.a]\nurl='https://example.invalid'\nbearer_token_env_var='UNAVAILABLE'",
    );
    write(
        root.path(),
        ".mcp.json",
        r#"{"mcpServers":{"a":{"command":"${ALSO_UNAVAILABLE}"}}}"#,
    );
    let mut definitions = [explicit("id-a", "a")];
    definitions[0].enabled = false;
    let mut inputs = input(root.path());
    inputs.explicit = &definitions;
    assert!(resolve_project_catalog(&inputs).unwrap().servers.is_empty());
    inputs.explicit = &[];
    assert!(resolve_project_catalog(&inputs).is_err());
    write(
        root.path(),
        ".mcp.json",
        r#"{"mcpServers":{"a":{"command":"project-winner"}}}"#,
    );
    assert!(resolve_project_catalog(&inputs)
        .unwrap()
        .servers
        .contains_key("a"));
}
