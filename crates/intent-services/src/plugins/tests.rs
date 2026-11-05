use super::*;
use serde_json::json;

fn write(path: &Path, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

fn manifest(name: &str) -> String {
    json!({ "$schema": PLUGIN_SCHEMA, "name": name }).to_string()
}

fn mcp(servers: &Value) -> String {
    json!({ "$schema": MCP_SCHEMA, "mcpServers": servers }).to_string()
}

fn skill(name: &str) -> String {
    format!("---\nname: {name}\ndescription: Does {name}.\n---\nBody\n")
}

struct Fixture {
    tmp: tempfile::TempDir,
    root: PathBuf,
    data: PathBuf,
}

fn fixture(name: &str) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("plugins").join(name);
    write(&root.join(MANIFEST_FILE), &manifest(name));
    let root = root.canonicalize().unwrap();
    let data = tmp.path().join("plugin-data").join(name);
    Fixture { tmp, root, data }
}

fn load(f: &Fixture) -> LoadedPlugin {
    load_plugin(&f.root, &f.data).expect("plugin loads")
}

#[test]
fn plugin_names_follow_spec_rule() {
    for ok in ["a", "acme-tools", "acme.tools", "a1", &"a".repeat(64)] {
        assert!(is_valid_plugin_name(ok), "{ok}");
    }
    for bad in [
        "",
        "-a",
        "a-",
        ".a",
        "a.",
        "a--b",
        "a..b",
        "Acme",
        "a_b",
        "a b",
        &"a".repeat(65),
    ] {
        assert!(!is_valid_plugin_name(bad), "{bad}");
    }
}

#[test]
fn manifest_violations_reject_the_plugin() {
    let cases = [
        json!([]),
        json!({ "name": "p" }),
        json!({ "$schema": "https://example.com/other.json", "name": "p" }),
        json!({ "$schema": PLUGIN_SCHEMA }),
        json!({ "$schema": PLUGIN_SCHEMA, "name": "P" }),
        json!({ "$schema": PLUGIN_SCHEMA, "name": "p", "version": 1 }),
        json!({ "$schema": PLUGIN_SCHEMA, "name": "p", "keywords": ["a", 2] }),
        json!({ "$schema": PLUGIN_SCHEMA, "name": "p", "author": "me" }),
        json!({ "$schema": PLUGIN_SCHEMA, "name": "p", "author": { "name": "me", "twitter": "x" } }),
    ];
    for case in cases {
        let f = fixture("p");
        write(&f.root.join(MANIFEST_FILE), &case.to_string());
        assert!(load_plugin(&f.root, &f.data).is_err(), "{case}");
    }
    let f = fixture("p");
    std::fs::remove_file(f.root.join(MANIFEST_FILE)).unwrap();
    assert!(load_plugin(&f.root, &f.data).is_err());
}

#[test]
fn unknown_fields_and_bad_extensions_are_reported_not_fatal() {
    let f = fixture("p");
    write(
        &f.root.join(MANIFEST_FILE),
        &json!({
            "$schema": PLUGIN_SCHEMA,
            "name": "p",
            "version": "1.2.0",
            "author": { "name": "Ada" },
            "keywords": ["x"],
            "commands": [],
            "extensions": "nope",
        })
        .to_string(),
    );
    let plugin = load(&f);
    assert_eq!(plugin.manifest.version.as_deref(), Some("1.2.0"));
    assert_eq!(plugin.manifest.author.unwrap().name.as_deref(), Some("Ada"));
    assert_eq!(plugin.diagnostics.len(), 2, "{:?}", plugin.diagnostics);
}

#[test]
fn skills_are_one_level_deep_and_bad_entries_are_skipped() {
    let f = fixture("p");
    write(&f.root.join("skills/alpha/SKILL.md"), &skill("alpha"));
    write(
        &f.root.join("skills/nested/deeper/SKILL.md"),
        &skill("deeper"),
    );
    std::fs::create_dir_all(f.root.join("skills/dir-as-file/SKILL.md")).unwrap();
    let plugin = load(&f);
    let names: Vec<_> = plugin.skills.iter().map(|s| s.directory.as_str()).collect();
    assert_eq!(names, ["alpha"]);
    assert_eq!(plugin.diagnostics.len(), 1, "{:?}", plugin.diagnostics);
}

#[cfg(unix)]
#[test]
fn symlink_escapes_are_isolated_to_their_component() {
    let f = fixture("p");
    let outside = f.tmp.path().join("outside");
    write(&outside.join("SKILL.md"), &skill("evil"));
    write(&f.root.join("skills/good/SKILL.md"), &skill("good"));
    std::os::unix::fs::symlink(&outside, f.root.join("skills/evil")).unwrap();
    write(&outside.join("server"), "#!/bin/sh\n");
    std::os::unix::fs::symlink(outside.join("server"), f.root.join("server")).unwrap();
    write(
        &f.root.join(MCP_FILE),
        &mcp(&json!({
            "escape": { "type": "stdio", "command": "./server" },
            "ok": { "type": "stdio", "command": "node" },
        })),
    );
    let plugin = load(&f);
    assert_eq!(plugin.skills.len(), 1);
    assert_eq!(plugin.skills[0].directory, "good");
    assert_eq!(plugin.mcp_servers.keys().collect::<Vec<_>>(), ["ok"]);

    let g = fixture("q");
    std::os::unix::fs::symlink(&outside, g.root.join("skills")).unwrap();
    let plugin = load(&g);
    assert!(plugin.skills.is_empty());
    assert_eq!(plugin.diagnostics.len(), 1);
}

#[test]
fn invalid_mcp_file_disables_only_mcp() {
    for body in [
        "{".to_string(),
        json!({ "mcpServers": {} }).to_string(),
        json!({ "$schema": MCP_SCHEMA }).to_string(),
        json!({ "$schema": MCP_SCHEMA, "mcpServers": {}, "extra": 1 }).to_string(),
    ] {
        let f = fixture("p");
        write(&f.root.join("skills/alpha/SKILL.md"), &skill("alpha"));
        write(&f.root.join(MCP_FILE), &body);
        let plugin = load(&f);
        assert!(plugin.mcp_servers.is_empty(), "{body}");
        assert_eq!(plugin.skills.len(), 1, "{body}");
        assert_eq!(plugin.diagnostics.len(), 1, "{body}");
    }
}

#[test]
fn stdio_servers_expand_variables_once_in_permitted_fields() {
    let f = fixture("p");
    write(&f.root.join("bin/server"), "#!/bin/sh\n");
    std::fs::create_dir_all(f.root.join("work")).unwrap();
    write(
        &f.root.join(MCP_FILE),
        &mcp(&json!({
            "local": {
                "type": "stdio",
                "command": "./bin/server",
                "args": ["--root=${PLUGIN_ROOT}", "${PLUGIN_DATA}/db", "$${PLUGIN_ROOT}", "${HOME}"],
                "env": { "CACHE": "${PLUGIN_DATA}/cache", "${PLUGIN_ROOT}": "kept" },
                "cwd": "./work",
            },
            "bare": { "type": "stdio", "command": "node", "cwd": "${PLUGIN_DATA}/state" },
        })),
    );
    let plugin = load(&f);
    let root = f.root.to_string_lossy();
    let data = f.data.to_string_lossy();
    let PluginMcpServer::Stdio {
        command,
        args,
        env,
        cwd,
    } = &plugin.mcp_servers["local"]
    else {
        panic!("stdio expected");
    };
    assert_eq!(command, &f.root.join("bin/server").to_string_lossy());
    assert_eq!(
        args,
        &[
            format!("--root={root}"),
            format!("{data}/db"),
            format!("${root}"),
            "${HOME}".to_string(),
        ]
    );
    assert_eq!(env["CACHE"], format!("{data}/cache"));
    assert_eq!(env["${PLUGIN_ROOT}"], "kept");
    assert_eq!(cwd, &f.root.join("work"));
    let PluginMcpServer::Stdio { command, cwd, .. } = &plugin.mcp_servers["bare"] else {
        panic!("stdio expected");
    };
    assert_eq!(command, "node");
    assert_eq!(cwd, &f.data.join("state"));
}

#[test]
fn invalid_server_entries_are_skipped_individually() {
    let f = fixture("p");
    write(
        &f.root.join(MCP_FILE),
        &mcp(&json!({
            "shell": { "type": "stdio", "command": "node server.js" },
            "absolute": { "type": "stdio", "command": "/usr/bin/node" },
            "parent": { "type": "stdio", "command": "../node" },
            "missing": { "type": "stdio", "command": "./nope" },
            "root-env": { "type": "stdio", "command": "node", "env": { "PLUGIN_ROOT": "/x" } },
            "cwd-out": { "type": "stdio", "command": "node", "cwd": "/tmp" },
            "data-escape": { "type": "stdio", "command": "node", "cwd": "${PLUGIN_DATA}/../x" },
            "no-type": { "command": "node" },
            "ws": { "type": "websocket", "url": "wss://x.example" },
            "plain-http": { "type": "streamable-http", "url": "http://example.com/mcp" },
            "userinfo": { "type": "streamable-http", "url": "https://u:p@example.com/mcp" },
            "fragment": { "type": "streamable-http", "url": "https://example.com/mcp#x" },
            "dup-headers": { "type": "sse", "url": "https://example.com", "headers": { "X-A": "1", "x-a": "2" } },
            "remote": { "type": "streamable-http", "url": "https://example.com/mcp", "headers": { "X-Team": "t" } },
            "loopback": { "type": "streamable-http", "url": "http://127.0.0.1:3000/mcp" },
            "loopback6": { "type": "sse", "url": "http://[::1]:3000/sse" },
        })),
    );
    let plugin = load(&f);
    assert_eq!(
        plugin.mcp_servers.keys().collect::<Vec<_>>(),
        ["loopback", "loopback6", "remote"]
    );
    assert_eq!(plugin.diagnostics.len(), 13, "{:#?}", plugin.diagnostics);
}

#[test]
fn installed_plugins_honor_registry_and_directory_names() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = PluginPaths::new(tmp.path());
    write(
        &paths.plugin_dir("alpha").join(MANIFEST_FILE),
        &manifest("alpha"),
    );
    write(
        &paths.plugin_dir("beta").join(MANIFEST_FILE),
        &manifest("beta"),
    );
    write(
        &paths.plugin_dir("renamed").join(MANIFEST_FILE),
        &manifest("gamma"),
    );
    write(&paths.plugin_dir("broken").join(MANIFEST_FILE), "{");
    let mut registry = Registry::default();
    registry.plugins.insert(
        "beta".into(),
        RegistryEntry {
            enabled: false,
            ..RegistryEntry::default()
        },
    );
    paths.write_registry(&registry).unwrap();
    assert_eq!(paths.read_registry(), registry);

    let installed = list_installed(&paths);
    let enabled: Vec<_> = installed
        .iter()
        .filter_map(|p| p.enabled_plugin().map(|p| p.manifest.name.as_str()))
        .collect();
    assert_eq!(enabled, ["alpha"]);
    let rejected: Vec<_> = installed
        .iter()
        .filter_map(|p| match p {
            InstalledPlugin::Rejected { directory, .. } => Some(directory.as_str()),
            InstalledPlugin::Loaded { .. } => None,
        })
        .collect();
    assert_eq!(rejected, ["broken", "renamed"]);
}

#[test]
fn session_names_are_provider_safe() {
    assert_eq!(
        mcp_server_name("acme.tools", "search api"),
        "acme_tools-search_api"
    );
    assert_eq!(skill_name("acme", "review"), "acme:review");
}

#[test]
fn enabled_plugin_skills_join_the_catalog_namespaced() {
    let home = tempfile::tempdir().unwrap();
    let paths = PluginPaths::new(home.path().join(".intent"));
    for name in ["acme", "off"] {
        let root = paths.plugin_dir(name);
        write(&root.join(MANIFEST_FILE), &manifest(name));
        write(&root.join("skills/review/SKILL.md"), &skill("review"));
    }
    let mut registry = Registry::default();
    registry.plugins.insert(
        "off".into(),
        RegistryEntry {
            enabled: false,
            ..RegistryEntry::default()
        },
    );
    paths.write_registry(&registry).unwrap();
    write(
        &home.path().join(".agents/skills/review/SKILL.md"),
        &skill("review"),
    );

    let names: Vec<_> = crate::skills::discover_skills_sync(None, Some(home.path().to_path_buf()))
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(names, ["acme:review", "review"]);
}

#[test]
fn session_servers_launch_through_plugin_exec_without_bridge_secrets() {
    let f = fixture("acme");
    write(
        &f.root.join(MCP_FILE),
        &mcp(&json!({
            "search": { "type": "stdio", "command": "node", "args": ["s.js"], "env": { "MODE": "x" } },
            "docs": { "type": "streamable-http", "url": "https://docs.example/mcp" },
            "taken": { "type": "streamable-http", "url": "https://other.example/mcp" },
        })),
    );
    let plugin = load(&f);
    let mut out = intent_acp::NormalizedMcpServers::new();
    out.insert(
        "acme-taken".into(),
        intent_acp::NormalizedMcpServer::Http {
            url: "https://user.example".into(),
            headers: None,
        },
    );
    let launcher_env = BTreeMap::from([("PATH".to_string(), "/opt/intent:/usr/bin".to_string())]);
    merge_session_servers(&[plugin], "intentd", &launcher_env, &mut out);

    let intent_acp::NormalizedMcpServer::Stdio { command, args, env } = &out["acme-search"] else {
        panic!("stdio expected");
    };
    assert_eq!(command, "intentd");
    let root = f.root.to_string_lossy().into_owned();
    assert_eq!(
        args,
        &["plugin-exec", "--cwd", root.as_str(), "--", "node", "s.js"]
    );
    assert_eq!(
        env,
        &BTreeMap::from([
            ("MODE".to_string(), "x".to_string()),
            ("PATH".to_string(), "/opt/intent:/usr/bin".to_string()),
            (DATA_VAR.to_string(), f.data.to_string_lossy().into_owned()),
            (ROOT_VAR.to_string(), root.clone()),
        ])
    );
    assert!(
        matches!(&out["acme-docs"], intent_acp::NormalizedMcpServer::Http { url, .. } if url == "https://docs.example/mcp")
    );
    assert!(
        matches!(&out["acme-taken"], intent_acp::NormalizedMcpServer::Http { url, .. } if url == "https://user.example")
    );
}
