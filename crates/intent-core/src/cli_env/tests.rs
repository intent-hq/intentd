use super::*;

#[test]
fn custom_codex_credentials_are_exact_config_references_not_whole_namespaces() {
    let names = CodexEnvNames::from_config(
        r#"
        [model_providers.gateway]
        env_key = "CODEX_GATEWAY_TOKEN"
        env_http_headers = { "X-Credential" = "GATEWAY_HEADER_TOKEN" }
        http_headers = { "X-Literal" = "DO_NOT_CAPTURE_LITERAL" }
        [profiles.enterprise.model_providers.other]
        env_key = "ENTERPRISE_KEY"
        [mcp_servers.unrelated]
        env_key = "DO_NOT_CAPTURE_MCP"
    "#,
    )
    .unwrap();
    for name in [
        "CODEX_GATEWAY_TOKEN",
        "GATEWAY_HEADER_TOKEN",
        "ENTERPRISE_KEY",
    ] {
        assert!(names.contains(name));
    }
    for name in [
        "X-Credential",
        "DO_NOT_CAPTURE_LITERAL",
        "DO_NOT_CAPTURE_MCP",
        "CODEX_UNRELATED",
    ] {
        assert!(!names.contains(name));
    }
}

#[test]
fn config_references_cannot_import_runtime_policy_or_loader_controls() {
    for key in [
        "CODEX_PATH",
        "codex_path",
        "Codex_Config",
        "node_options",
        "CODEX_CONFIG",
        "INITIAL_AGENT_MODE",
        "CLAUDE_CODE_EXECUTABLE",
        "NODE_OPTIONS",
        "LD_PRELOAD",
        "DYLD_INSERT_LIBRARIES",
        "PATH",
        "HOME",
        "SHELL",
        "INTENTD_SOCKET",
        "BAD=NAME",
        "BAD\nNAME",
        "",
        "1BAD",
    ] {
        let config = format!(
            "[model_providers.gateway]\nenv_key = {}",
            serde_json::to_string(key).unwrap()
        );
        assert!(!CodexEnvNames::from_config(&config).unwrap().contains(key));
    }
}

#[test]
fn codex_config_read_is_bounded_and_parse_errors_do_not_expose_source() {
    let home = tempfile::tempdir().unwrap();
    assert!(!CodexEnvNames::from_home(home.path())
        .unwrap()
        .contains("TOKEN"));
    let file = home.path().join("config.toml");
    std::fs::write(&file, "[model_providers.gateway]\nenv_key = \"TOKEN\"").unwrap();
    assert!(CodexEnvNames::from_home(home.path())
        .unwrap()
        .contains("TOKEN"));
    std::fs::write(&file, "token = 'synthetic-secret-value\n").unwrap();
    let error = CodexEnvNames::from_home(home.path()).err().unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(!error.to_string().contains("synthetic-secret-value"));
    std::fs::write(&file, vec![b' '; 1024 * 1024 + 1]).unwrap();
    assert!(CodexEnvNames::from_home(home.path()).is_err());
    std::fs::remove_file(&file).unwrap();
    std::fs::create_dir(&file).unwrap();
    assert!(CodexEnvNames::from_home(home.path()).is_err());
}
