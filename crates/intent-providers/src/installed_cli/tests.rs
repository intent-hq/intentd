use super::*;

fn scratch() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("intent-installed-cli-")
        .tempdir()
        .unwrap()
}

fn executable(path: &Path, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).into(), (*v).into()))
        .collect()
}

#[test]
fn canonical_names_are_separate_from_adapters_and_missing_is_actionable() {
    let dir = scratch();
    for name in ["codex-acp", "claude-agent-acp", "npx"] {
        executable(&dir.path().join(name), "adapter");
    }
    for cli in [InstalledCli::Codex, InstalledCli::Claude] {
        let err = cli
            .resolve_in_dirs(&[dir.path().to_owned()], false)
            .unwrap_err();
        assert!(err.to_string().contains(cli.command()));
        assert!(err.to_string().contains("execution host"));
        assert!(err.to_string().contains("no bundled runtime"));
    }
    assert_eq!(
        InstalledCli::for_provider("codex"),
        Some(InstalledCli::Codex)
    );
    assert_eq!(
        InstalledCli::for_provider("claude-code"),
        Some(InstalledCli::Claude)
    );
    assert_eq!(InstalledCli::for_provider("mock"), None);
}

#[test]
fn canonical_discovery_respects_order_spaces_and_enriched_directory_sources() {
    let dir = scratch();
    // The injected list models enhanced_path_dirs: inherited first, then
    // known locations/version managers, then a captured login-shell directory.
    let dirs: Vec<_> = [
        "inherited path",
        ".local/bin",
        ".npm-global/bin",
        ".volta/bin",
        ".asdf/shims",
        ".nvm/versions/node/v24.1.0/bin",
        "login shell only",
    ]
    .into_iter()
    .map(|part| dir.path().join(part))
    .collect();
    for cli in [InstalledCli::Codex, InstalledCli::Claude] {
        for candidate in &dirs {
            executable(&candidate.join(cli.command()), "#!/bin/sh\nexit 0\n");
        }
        for expected in &dirs {
            let found = cli.resolve_in_dirs(&dirs, false).unwrap();
            assert_eq!(found.path(), expected.join(cli.command()));
            assert!(found.path().is_absolute());
            std::fs::remove_file(found.path()).unwrap();
        }
        assert!(cli.resolve_in_dirs(&dirs, false).is_err());
    }
}

#[test]
fn relative_path_entries_become_absolute_without_changing_process_cwd() {
    let dir = tempfile::Builder::new()
        .prefix(".cli-test-")
        .tempdir_in(".")
        .unwrap();
    executable(&dir.path().join("codex"), "#!/bin/sh\nexit 0\n");
    let runtime = InstalledCli::Codex
        .resolve_in_dirs(&[dir.path().to_owned()], false)
        .unwrap();
    assert!(runtime.path().is_absolute());
    assert_eq!(
        std::fs::canonicalize(runtime.path()).unwrap(),
        std::fs::canonicalize(dir.path().join("codex")).unwrap()
    );
}

#[test]
fn windows_uses_exe_cmd_bat_and_never_extensionless_npm_shims() {
    let dir = scratch();
    for cli in [InstalledCli::Codex, InstalledCli::Claude] {
        let bare = dir.path().join(cli.command());
        executable(&bare, "#!/bin/sh\nexit 0\n");
        assert!(cli.resolve_in_dirs(&[dir.path().into()], true).is_err());
        for ext in ["exe", "cmd", "bat"] {
            executable(&bare.with_extension(ext), "wrapper");
        }
        for ext in ["exe", "cmd", "bat"] {
            let runtime = cli.resolve_in_dirs(&[dir.path().into()], true).unwrap();
            assert_eq!(runtime.path(), bare.with_extension(ext));
            std::fs::remove_file(runtime.path()).unwrap();
        }
    }
}

#[cfg(unix)]
#[test]
fn rejects_directories_non_executable_files_and_broken_links() {
    let dir = scratch();
    let path = dir.path().join("codex");
    std::fs::create_dir(&path).unwrap();
    let resolve = || InstalledCli::Codex.resolve_in_dirs(&[dir.path().into()], false);
    assert!(resolve().is_err());
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, "plain file").unwrap();
    assert!(resolve().is_err());
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink("absent", &path).unwrap();
    assert!(resolve().is_err());
}

#[test]
fn audited_environment_preserves_empty_values_and_daemon_authority() {
    let dir = scratch();
    for cli in [InstalledCli::Codex, InstalledCli::Claude] {
        executable(&dir.path().join(cli.command()), "#!/bin/sh\nexit 0\n");
        let runtime = cli.resolve_in_dirs(&[dir.path().into()], false).unwrap();
        let captured = env(&[
            ("OPENAI_API_KEY", "shell-openai"),
            ("ANTHROPIC_AUTH_TOKEN", "shell-anthropic"),
            ("CODEX_HOME", "/shell/codex"),
            ("CLAUDE_CONFIG_DIR", "/shell/claude"),
            ("HTTPS_PROXY", "shell-proxy"),
            ("NODE_EXTRA_CA_CERTS", "/shell/cert"),
            ("NO_PROXY", "shell-bypass"),
            ("RANDOM_SECRET", "must-not-forward"),
            ("CODEX_PATH", "/shell/alternate"),
            ("CLAUDE_CODE_EXECUTABLE", "/shell/alternate"),
            ("CODEX_CONFIG", "unsafe"),
            ("NODE_OPTIONS", "--require=untrusted"),
        ]);
        let inherited = env(&[("HTTPS_PROXY", "daemon-proxy"), ("NO_PROXY", "")]);
        let overrides = env(&[
            ("CODEX_HOME", "/isolated/probe"),
            ("CLAUDE_CONFIG_DIR", "/isolated/probe"),
            ("NODE_OPTIONS", "daemon-node-policy"),
            ("CODEX_CONFIG", "unsafe"),
            ("CODEX_PATH", "/override/alternate"),
            ("codex_path", "/override/lowercase"),
            ("Codex_Config", "unsafe"),
            ("CLAUDE_CODE_EXECUTABLE", "/override/alternate"),
        ]);
        let selected = runtime
            .environment(&captured, &inherited, &overrides, &CodexEnvNames::default())
            .unwrap();
        assert_eq!(selected["HTTPS_PROXY"], "daemon-proxy");
        assert_eq!(selected["NO_PROXY"], "");
        assert_eq!(selected["NODE_EXTRA_CA_CERTS"], "/shell/cert");
        assert_eq!(selected["CODEX_HOME"], "/isolated/probe");
        assert_eq!(selected["CLAUDE_CONFIG_DIR"], "/isolated/probe");
        assert_eq!(selected["NODE_OPTIONS"], "daemon-node-policy");
        assert!(!selected.contains_key("RANDOM_SECRET"));
        assert!(!selected.contains_key("codex_path"));
        assert_eq!(selected[cli.path_env()], runtime.path().to_str().unwrap());
        if cli == InstalledCli::Codex {
            assert_eq!(
                selected["CODEX_CONFIG"],
                crate::CODEX_SUBAGENT_POLICY_CONFIG
            );
            assert_eq!(selected["OPENAI_API_KEY"], "shell-openai");
            assert!(!selected.contains_key("ANTHROPIC_AUTH_TOKEN"));
            assert!(!selected.contains_key("CLAUDE_CODE_EXECUTABLE"));
        } else {
            assert_eq!(selected["ANTHROPIC_AUTH_TOKEN"], "shell-anthropic");
            assert!(!selected.contains_key("OPENAI_API_KEY"));
            assert!(!selected.contains_key("CODEX_PATH"));
        }
    }
}

#[test]
fn audited_environment_excludes_policy_and_unknown_names() {
    for cli in [InstalledCli::Codex, InstalledCli::Claude] {
        for key in [
            "CODEX_PATH",
            "CLAUDE_CODE_EXECUTABLE",
            "CODEX_CONFIG",
            "NODE_OPTIONS",
            "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS",
            "CODEX_UNKNOWN",
            "UNRELATED_SECRET",
        ] {
            assert!(!cli.accepts_env(key), "unexpected shell capture: {key}");
        }
    }
    for key in [
        "AWS_SHARED_CREDENTIALS_FILE",
        "ANTHROPIC_BEDROCK_BASE_URL",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "ANTHROPIC_FOUNDRY_BASE_URL",
        "ANTHROPIC_DEFAULT_SONNET_MODEL",
        "CLAUDE_CODE_CLIENT_KEY",
    ] {
        assert!(InstalledCli::Claude.accepts_env(key));
    }
}

#[cfg(unix)]
#[test]
fn identity_tracks_same_path_atomic_replacement_and_symlink_target() {
    let dir = scratch();
    let entry = dir.path().join("codex");
    let payload = dir.path().join("payload");
    executable(&payload, "version-one");
    std::os::unix::fs::symlink(&payload, &entry).unwrap();
    let runtime = InstalledCli::Codex
        .resolve_in_dirs(&[dir.path().into()], false)
        .unwrap();
    assert_eq!(runtime.path(), entry); // preserve the launch spelling
    let first = runtime.identity("1.0").unwrap();
    assert!(first == runtime.identity("1.0").unwrap());
    assert!(runtime.identity("").is_err());
    executable(&payload, "longer replacement written in place");
    let in_place = runtime.identity("1.0").unwrap();
    assert!(first != in_place);
    let replacement = dir.path().join("replacement");
    executable(&replacement, "longer replacement written in place");
    // An atomic install may preserve length, mtime and the reported version.
    std::fs::File::options()
        .write(true)
        .open(&replacement)
        .unwrap()
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::fs::metadata(&payload).unwrap().modified().unwrap()),
        )
        .unwrap();
    std::fs::rename(&replacement, &payload).unwrap();
    let second = runtime.identity("1.0").unwrap();
    assert!(in_place != second);
    executable(&replacement, "version-two");
    std::fs::remove_file(&entry).unwrap();
    std::os::unix::fs::symlink(&replacement, &entry).unwrap();
    assert!(second != runtime.identity("1.0").unwrap());
    std::fs::remove_file(&replacement).unwrap();
    assert!(runtime.identity("1.0").is_err());
}

#[cfg(unix)]
#[test]
fn non_unicode_path_is_rejected_instead_of_launching_a_lossy_spelling() {
    use std::os::unix::ffi::OsStrExt;
    let dir = scratch();
    let bin = dir.path().join(std::ffi::OsStr::from_bytes(b"bin-\xff"));
    executable(&bin.join("codex"), "#!/bin/sh\nexit 0\n");
    let runtime = InstalledCli::Codex.resolve_in_dirs(&[bin], false).unwrap();
    assert_eq!(
        runtime
            .environment(
                &BTreeMap::new(),
                &BTreeMap::new(),
                &BTreeMap::new(),
                &CodexEnvNames::default()
            )
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[cfg(unix)]
#[test]
fn unchanged_script_wrapper_observes_upgraded_payload_and_environment() {
    let dir = scratch();
    let wrapper = dir.path().join("claude");
    let payload = dir.path().join("payload");
    executable(
        &wrapper,
        "#!/bin/sh\nexec \"$(dirname \"$0\")/payload\" \"$@\"\n",
    );
    executable(
        &payload,
        "#!/bin/sh\nprintf '%s' \"1.0:$ANTHROPIC_AUTH_TOKEN:$HTTPS_PROXY\"\n",
    );
    let runtime = InstalledCli::Claude
        .resolve_in_dirs(&[dir.path().into()], false)
        .unwrap();
    let selected = runtime
        .environment(
            &env(&[
                ("ANTHROPIC_AUTH_TOKEN", "synthetic"),
                ("HTTPS_PROXY", "synthetic-proxy"),
            ]),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &CodexEnvNames::default(),
        )
        .unwrap();
    let version = || {
        let output = std::process::Command::new(runtime.path())
            .arg("--version")
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .envs(&selected)
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap()
    };
    let first_version = version();
    assert_eq!(first_version, "1.0:synthetic:synthetic-proxy");
    let first = runtime.identity(&first_version).unwrap();
    executable(
        &payload,
        "#!/bin/sh\nprintf '%s' \"2.0:$ANTHROPIC_AUTH_TOKEN:$HTTPS_PROXY\"\n",
    );
    // Entry-point metadata cannot see a payload replacement behind a wrapper.
    assert!(first == runtime.identity(&first_version).unwrap());
    let second_version = version();
    assert_eq!(second_version, "2.0:synthetic:synthetic-proxy");
    assert!(first != runtime.identity(&second_version).unwrap());
}

#[test]
fn custom_codex_auth_and_header_credentials_survive_overlay_and_isolation() {
    let dir = scratch();
    executable(&dir.path().join("codex"), "#!/bin/sh\nexit 0\n");
    let runtime = InstalledCli::Codex
        .resolve_in_dirs(&[dir.path().into()], false)
        .unwrap();
    let names = CodexEnvNames::from_config(
        r#"
        [model_providers.gateway]
        env_key = "CODEX_GATEWAY_TOKEN"
        env_http_headers = { "Authorization" = "CUSTOM_HEADER_TOKEN", "Reserved" = "CODEX_PATH" }
    "#,
    )
    .unwrap();
    let captured = env(&[
        ("CODEX_GATEWAY_TOKEN", "shell-key"),
        ("CUSTOM_HEADER_TOKEN", "shell-header"),
        ("CODEX_UNRELATED", "excluded"),
        ("CODEX_PATH", "/wrong"),
        ("CLAUDE_CODE_API_KEY_HELPER_TTL_MS", "30000"),
        ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ]);
    let inherited = env(&[("CODEX_GATEWAY_TOKEN", "")]);
    let overrides = env(&[
        ("CUSTOM_HEADER_TOKEN", "daemon-header"),
        ("CODEX_HOME", "/isolated"),
    ]);
    let selected = runtime
        .environment(&captured, &inherited, &overrides, &names)
        .unwrap();
    assert_eq!(selected["CODEX_GATEWAY_TOKEN"], "");
    assert_eq!(selected["CUSTOM_HEADER_TOKEN"], "daemon-header");
    assert_eq!(selected["CODEX_HOME"], "/isolated");
    assert_eq!(selected["CODEX_PATH"], runtime.path().to_str().unwrap());
    assert!(!selected.contains_key("CODEX_UNRELATED"));
    executable(&dir.path().join("claude"), "#!/bin/sh\nexit 0\n");
    let claude = InstalledCli::Claude
        .resolve_in_dirs(&[dir.path().into()], false)
        .unwrap();
    let selected = claude
        .environment(&captured, &BTreeMap::new(), &BTreeMap::new(), &names)
        .unwrap();
    assert!(!selected.contains_key("CODEX_GATEWAY_TOKEN"));
    assert!(!selected.contains_key("CUSTOM_HEADER_TOKEN"));
    assert_eq!(selected["CLAUDE_CODE_API_KEY_HELPER_TTL_MS"], "30000");
    assert_eq!(selected["CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"], "1");
}
