use super::*;
use crate::test_support::test_tempdir;
use serde_json::json;

fn put(root: &Path, name: &str, text: &str) {
    let path = root.join(name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

fn stdio(command: &str) -> NormalizedMcpServer {
    NormalizedMcpServer::Stdio {
        command: command.into(),
        args: vec![],
        env: BTreeMap::new(),
    }
}

#[test]
fn audited_json_formats_preserve_stdio_and_remote_semantics() {
    for source in [
        ".mcp.json",
        ".factory/mcp.json",
        ".cursor/mcp.json",
        ".augment/settings.json",
        ".augment/settings.local.json",
    ] {
        let tmp = test_tempdir("project-mcp-json-");
        put(tmp.path(), source, &json!({"mcpServers": {
            "stdio": {"command":"node", "args":["a b.js", "--flag"], "env":{"TOKEN":"secret", "EMPTY":""}},
            "http": {"type":"http", "url":"https://example.test/mcp", "headers":{"Authorization":"secret"}},
            "sse": {"type":"sse", "url":"https://example.test/sse"},
            "off": {"disabled":true}
        }}).to_string());
        let found = discover_project_mcp(tmp.path(), tmp.path());
        assert!(
            found.diagnostics.is_empty(),
            "{source}: {:?}",
            found.diagnostics
        );
        assert_eq!(found.servers.len(), 3, "{source}");
        assert_eq!(found.disabled_names, BTreeSet::from(["off".into()]));
        assert_eq!(
            found.servers["stdio"],
            NormalizedMcpServer::Stdio {
                command: "node".into(),
                args: vec!["a b.js".into(), "--flag".into()],
                env: BTreeMap::from([
                    ("TOKEN".into(), "secret".into()),
                    ("EMPTY".into(), "".into())
                ]),
            }
        );
        assert!(
            matches!(&found.servers["http"], NormalizedMcpServer::Http {headers:Some(h),..} if h["Authorization"] == "secret")
        );
        assert!(matches!(
            &found.servers["sse"],
            NormalizedMcpServer::Sse { .. }
        ));
    }
}

#[test]
fn codex_and_grok_toml_preserve_transport_and_disables() {
    for source in [".codex/config.toml", ".grok/config.toml"] {
        let tmp = test_tempdir("project-mcp-toml-");
        put(
            tmp.path(),
            source,
            r#"
[mcp_servers.stdio]
command = "python"
args = ["-m", "server"]
env = {TOKEN = "secret"}
[mcp_servers.remote]
url = "https://example.test/mcp"
http_headers = {Authorization = "secret"}
[mcp_servers.off]
enabled = false
"#,
        );
        let found = discover_project_mcp(tmp.path(), tmp.path());
        assert!(
            found.diagnostics.is_empty(),
            "{source}: {:?}",
            found.diagnostics
        );
        assert_eq!(found.servers.len(), 2);
        assert!(found.disabled_names.contains("off"));
        assert!(
            matches!(&found.servers["remote"], NormalizedMcpServer::Http {headers:Some(h),..} if h["Authorization"] == "secret")
        );
    }
}

#[test]
fn opencode_jsonc_preserves_argv_env_and_string_comment_markers() {
    for source in [
        "opencode.json",
        "opencode.jsonc",
        ".opencode/opencode.json",
        ".opencode/opencode.jsonc",
    ] {
        let tmp = test_tempdir("project-mcp-opencode-");
        put(
            tmp.path(),
            source,
            r#"{
            // this is a comment
            "mcp": {
                "local": {"type":"local", "command":["node","a b.js"], "environment":{"X":"/*literal*/"}},
                "remote": {"type":"remote", "url":"https://example.test/mcp", "oauth":false,},
                "off": {"enabled":false},
            }, /* trailing comment */
        }"#,
        );
        let found = discover_project_mcp(tmp.path(), tmp.path());
        assert!(
            found.diagnostics.is_empty(),
            "{source}: {:?}",
            found.diagnostics
        );
        assert_eq!(
            found.servers["local"],
            NormalizedMcpServer::Stdio {
                command: "node".into(),
                args: vec!["a b.js".into()],
                env: BTreeMap::from([("X".into(), "/*literal*/".into())]),
            }
        );
        assert!(found.disabled_names.contains("off"));
    }
}

#[test]
fn every_project_tier_resolves_enabled_disabled_collisions_before_intent_merge() {
    for lower_disabled in [false, true] {
        for upper_disabled in [false, true] {
            for (lower, upper) in [
                (".augment/settings.json", ".augment/settings.local.json"),
                (".codex/config.toml", ".mcp.json"),
                (".mcp.json", "nested/.mcp.json"),
                ("nested/.factory/mcp.json", ".mcp.json"),
                (".factory/mcp.json", ".grok/config.toml"),
            ] {
                let tmp = test_tempdir("project-mcp-precedence-");
                for (path, disabled, command) in [
                    (lower, lower_disabled, "lower"),
                    (upper, upper_disabled, "upper"),
                ] {
                    let text = if path.ends_with("toml") {
                        format!(
                            "[mcp_servers.same]\ncommand = {command:?}\nenabled = {}",
                            !disabled
                        )
                    } else {
                        json!({"mcpServers":{"same":{"command":command,"disabled":disabled}}})
                            .to_string()
                    };
                    put(tmp.path(), path, &text);
                }
                std::fs::create_dir_all(tmp.path().join("nested")).unwrap();
                let found = discover_project_mcp(tmp.path(), &tmp.path().join("nested"));
                assert_eq!(
                    found.disabled_names.contains("same"),
                    upper_disabled,
                    "{lower} {upper}"
                );
                assert_eq!(
                    found.servers.get("same"),
                    (!upper_disabled).then(|| stdio("upper")).as_ref()
                );
                assert!(found.diagnostics.iter().any(|d| d.code == "collision"));
                let merged = merge_project_mcp(
                    found,
                    BTreeMap::from([("same".into(), stdio("intent"))]),
                    &BTreeSet::new(),
                    None,
                );
                assert_eq!(merged.servers["same"], stdio("intent"));
                assert!(!merged.disabled_names.contains("same"));
            }
        }
    }
}

#[test]
fn authoritative_id_and_name_denies_win_even_over_intent_and_bridge() {
    let configs =
        json!({"id-1":{"name":"logical","enabled":true}, "id-2":{"name":"off","enabled":false}});
    for (global, workspace) in [
        (BTreeSet::from(["id-1".into()]), BTreeSet::new()),
        (BTreeSet::new(), BTreeSet::from(["logical".into()])),
    ] {
        let denied = intent_mcp_disabled_names(&configs, &global, &workspace);
        assert!(
            denied.contains("id-1")
                && denied.contains("logical")
                && denied.contains("off")
                && denied.contains("id-2")
        );
        let project = ProjectMcpDiscovery {
            servers: BTreeMap::from([("logical".into(), stdio("project"))]),
            ..Default::default()
        };
        let merged = merge_project_mcp(
            project,
            BTreeMap::from([("logical".into(), stdio("intent"))]),
            &denied,
            Some(stdio("bridge")),
        );
        assert!(!merged.servers.contains_key("logical"));
        assert_eq!(merged.servers["workspace-mcp"], stdio("bridge"));
    }
    let project = ProjectMcpDiscovery {
        servers: BTreeMap::from([("workspace-mcp".into(), stdio("bad"))]),
        ..Default::default()
    };
    let merged = merge_project_mcp(
        project,
        BTreeMap::new(),
        &BTreeSet::from(["workspace-mcp".into()]),
        Some(stdio("bridge")),
    );
    assert!(merged.servers.is_empty());
}

#[test]
fn reserved_bridge_cannot_be_replaced_or_disabled_by_project() {
    let tmp = test_tempdir("project-mcp-reserved-");
    put(
        tmp.path(),
        ".mcp.json",
        r#"{"mcpServers":{"workspace-mcp":{"disabled":true}}}"#,
    );
    let found = discover_project_mcp(tmp.path(), tmp.path());
    assert!(found.disabled_names.is_empty());
    let merged = merge_project_mcp(
        found,
        BTreeMap::from([("workspace-mcp".into(), stdio("imposter"))]),
        &BTreeSet::new(),
        Some(stdio("bridge")),
    );
    assert_eq!(merged.servers["workspace-mcp"], stdio("bridge"));
}

#[test]
fn unsupported_or_malformed_entries_are_diagnosed_without_leaking_values() {
    for entry in [
        json!({"command":"secret","cwd":"secret"}),
        json!({"command":"secret","args":[1]}),
        json!({"command":"secret","env":{"SECRET":12}}),
        json!({"url":"secret","oauth":true}),
        json!({"command":"secret","url":"secret"}),
        json!({"command":"secret","timeout":42}),
        json!({"command":"secret","env":{"SECRET":"${SECRET}"}}),
        json!({"command":"secret","enabled":"false"}),
        json!({"command":"secret","enabled_tools":["secret"]}),
    ] {
        let tmp = test_tempdir("project-mcp-invalid-");
        put(
            tmp.path(),
            ".mcp.json",
            &json!({"mcpServers":{"server":entry}}).to_string(),
        );
        let found = discover_project_mcp(tmp.path(), tmp.path());
        assert!(found.servers.is_empty(), "{entry}");
        assert!(!found.diagnostics.is_empty());
        assert!(!format!("{:?}", found.diagnostics).contains("secret"));
    }
}

#[test]
fn bad_files_and_unverified_pi_schema_have_redacted_diagnostics() {
    let tmp = test_tempdir("project-mcp-files-");
    put(tmp.path(), ".mcp.json", r#"{"mcpServers":{"secret""#);
    put(tmp.path(), ".codex/config.toml", "secret = !!!");
    put(
        tmp.path(),
        ".pi/mcp.json",
        r#"{"mcpServers":{"invented":{"command":"secret"}}}"#,
    );
    let found = discover_project_mcp(tmp.path(), tmp.path());
    assert!(found.servers.is_empty());
    assert_eq!(found.diagnostics.len(), 3);
    assert!(!format!("{:?}", found.diagnostics).contains("secret"));
}

#[test]
fn relative_command_uses_launch_cwd_and_arguments_remain_literal() {
    let tmp = test_tempdir("project-mcp-relative-");
    std::fs::create_dir_all(tmp.path().join("nested")).unwrap();
    put(
        tmp.path(),
        ".mcp.json",
        r#"{"mcpServers":{"script":{"command":"./server","args":["./input","a b"]},"path":{"command":"node"}}}"#,
    );
    let cwd = tmp.path().join("nested");
    let found = discover_project_mcp(tmp.path(), &cwd);
    assert_eq!(
        found.servers["script"],
        NormalizedMcpServer::Stdio {
            command: cwd.join("server").to_string_lossy().into_owned(),
            args: vec!["./input".into(), "a b".into()],
            env: BTreeMap::new()
        }
    );
    assert_eq!(found.servers["path"], stdio("node"));
}

#[test]
fn discovery_never_reads_above_workspace_or_outside_cwd() {
    let tmp = test_tempdir("project-mcp-boundary-");
    put(
        tmp.path(),
        ".mcp.json",
        r#"{"mcpServers":{"personal":{"command":"bad"}}}"#,
    );
    let root = tmp.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    assert!(discover_project_mcp(&root, &root).servers.is_empty());
    let outside = discover_project_mcp(&root, tmp.path());
    assert!(outside.servers.is_empty());
    assert_eq!(outside.diagnostics[0].code, "boundary");
}

#[cfg(unix)]
#[test]
fn symlinked_files_directories_and_cwd_cannot_escape_workspace() {
    use std::os::unix::fs::symlink;
    let tmp = test_tempdir("project-mcp-symlink-");
    let root = tmp.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    put(
        tmp.path(),
        "outside/mcp.json",
        r#"{"mcpServers":{"escape":{"command":"bad"}}}"#,
    );
    symlink(tmp.path().join("outside/mcp.json"), root.join(".mcp.json")).unwrap();
    symlink(tmp.path().join("outside"), root.join(".factory")).unwrap();
    let found = discover_project_mcp(&root, &root);
    assert!(found.servers.is_empty());
    assert_eq!(
        found
            .diagnostics
            .iter()
            .filter(|d| d.code == "boundary")
            .count(),
        2
    );
    assert_eq!(
        discover_project_mcp(&root, &root.join(".factory")).diagnostics[0].code,
        "boundary"
    );
}

#[test]
fn rejected_winner_masks_lower_entry_but_intent_can_supply_replacement() {
    let tmp = test_tempdir("project-mcp-rejected-");
    put(
        tmp.path(),
        ".factory/mcp.json",
        r#"{"mcpServers":{"same":{"command":"lower"}}}"#,
    );
    put(
        tmp.path(),
        ".mcp.json",
        r#"{"mcpServers":{"same":{"command":"higher","cwd":"unsupported"}}}"#,
    );
    let found = discover_project_mcp(tmp.path(), tmp.path());
    assert!(found.servers.is_empty());
    assert!(found.disabled_names.is_empty());
    assert!(found.sources["same"].ends_with(".mcp.json"));
    let merged = merge_project_mcp(
        found,
        BTreeMap::from([("same".into(), stdio("intent"))]),
        &BTreeSet::new(),
        None,
    );
    assert_eq!(merged.servers["same"], stdio("intent"));
}

#[test]
fn opencode_implicit_oauth_and_tool_policy_cannot_be_silently_dropped() {
    for value in [
        json!({"mcp":{"server":{"type":"remote","url":"https://example.test"}}}),
        json!({"mcp":{"server":{"type":"local","command":["node"]}},"tools":{"server_*":false}}),
    ] {
        let tmp = test_tempdir("project-mcp-opencode-policy-");
        put(tmp.path(), "opencode.json", &value.to_string());
        let found = discover_project_mcp(tmp.path(), tmp.path());
        assert!(found.servers.is_empty());
        assert_eq!(found.diagnostics[0].code, "unsupported");
    }
}

#[test]
fn file_size_and_depth_limits_are_explicit() {
    let tmp = test_tempdir("project-mcp-limits-");
    put(
        tmp.path(),
        ".mcp.json",
        &" ".repeat(usize::try_from(MAX_CONFIG_BYTES).unwrap() + 1),
    );
    assert_eq!(
        discover_project_mcp(tmp.path(), tmp.path()).diagnostics[0].code,
        "limit"
    );
    let cwd = (0..MAX_ANCESTORS).fold(tmp.path().to_path_buf(), |p, _| p.join("d"));
    std::fs::create_dir_all(&cwd).unwrap();
    assert_eq!(
        discover_project_mcp(tmp.path(), &cwd).diagnostics[0].code,
        "limit"
    );
}

#[test]
fn jsonc_lexing_does_not_accept_json5_or_join_comment_separated_tokens() {
    for invalid in [
        "{,}",
        "[,]",
        r#"{"a":tr/*comment*/ue}"#,
        r#"{"a":1/*unterminated}"#,
        "{'a':1}",
        "{a:1}",
    ] {
        assert!(parse_jsonc(invalid).is_none(), "{invalid}");
    }
    assert_eq!(
        parse_jsonc(r#"{"a":"escaped \\\" // still literal",/*removed*/}"#).unwrap()["a"],
        "escaped \\\" // still literal"
    );
}

#[test]
fn authoritative_disables_expand_id_name_collision_chains() {
    let configs = json!({"a":{"name":"b","enabled":true},"b":{"name":"c","enabled":true},"c":{"name":"d","enabled":true}});
    assert_eq!(
        intent_mcp_disabled_names(&configs, &BTreeSet::from(["d".into()]), &BTreeSet::new()),
        BTreeSet::from(["a".into(), "b".into(), "c".into(), "d".into()])
    );
}

#[test]
fn relative_executable_escape_is_diagnosed_without_rewriting_plain_args() {
    let tmp = test_tempdir("project-mcp-exec-boundary-");
    put(
        tmp.path(),
        ".mcp.json",
        r#"{"mcpServers":{"escape":{"command":"../outside"}}}"#,
    );
    let found = discover_project_mcp(tmp.path(), tmp.path());
    assert!(found.servers.is_empty());
    assert_eq!(found.diagnostics[0].code, "unsupported");
}
