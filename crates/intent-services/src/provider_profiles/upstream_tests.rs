//! Opt-in no-prompt smoke tests against unmodified installed providers.
//! No real HOME, credentials, model prompts or adapter rewriting.
use super::*;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn write(path: &Path, text: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

fn req<'a>(
    root: &'a Path,
    home: &'a Path,
    cwd: &'a Path,
    provider: &'a str,
    mcp: &'a NormalizedMcpServers,
) -> ProviderProfileRequest<'a> {
    ProviderProfileRequest {
        provider_id: provider,
        detected_version: None,
        purpose: LaunchPurpose::ModelProbe,
        owned_root: root,
        persistent_identity: None,
        resume: false,
        home,
        provider_home: None,
        workspace_root: cwd,
        launch_cwd: cwd,
        owned_mcp: mcp,
        policy_sources: &[],
        native_config_files: &[],
        native_skill_roots: &[],
        trusted_launch_config: None,
    }
}

#[tokio::test]
async fn generated_environment_replaces_poisoned_configuration_in_a_real_child() {
    let dir = crate::test_support::test_tempdir("profile-child");
    let mcp = NormalizedMcpServers::new();
    for provider in ["codex", "opencode", "unsloth"] {
        let p = prepare_provider_profile(req(dir.path(), dir.path(), dir.path(), provider, &mcp))
            .unwrap();
        let mut command = tokio::process::Command::new("python3");
        command
            .args(["-c", "import os,json; print(json.dumps(dict(os.environ)))"])
            .env(
                "CODEX_CONFIG",
                r#"{"mcp_servers":{"evil":{"command":"evil"}}}"#,
            )
            .env("OPENCODE_CONFIG", "/poison/config")
            .env("OPENCODE_CONFIG_CONTENT", r#"{"mcp":{"evil":{}}}"#)
            .env("CUSTOM_MODEL_KEY", "synthetic-key");
        p.apply_to_command(&mut command);
        let output = command.output().await.unwrap();
        assert!(output.status.success());
        let env: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(env["CUSTOM_MODEL_KEY"], "synthetic-key");
        if provider == "codex" {
            assert!(!env["CODEX_CONFIG"].as_str().unwrap().contains("evil"));
        } else {
            assert!(env.get("OPENCODE_CONFIG").is_none());
            assert!(!env["OPENCODE_CONFIG_CONTENT"]
                .as_str()
                .unwrap()
                .contains("evil"));
        }
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires INTENTD_PROFILE_CLAUDE_SDK_JS and INTENTD_PROFILE_CLAUDE_BIN; file-based controls only"]
async fn upstream_claude_suppresses_native_files_and_keeps_owned_injection() {
    let sdk =
        std::env::var("INTENTD_PROFILE_CLAUDE_SDK_JS").expect("set unmodified SDK 0.3.280 sdk.mjs");
    let binary = std::env::var("INTENTD_PROFILE_CLAUDE_BIN").expect("set CLI 2.1.280 path");
    let dir = crate::test_support::test_tempdir("profile-upstream-claude");
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    write(
        &home.join(".claude.json"),
        r#"{"mcpServers":{"user_poison":{"command":"/bin/false"}}}"#,
    );
    write(
        &project.join(".mcp.json"),
        r#"{"mcpServers":{"project_poison":{"command":"/bin/false"}}}"#,
    );
    write(
        &home.join(".claude/skills/poison/SKILL.md"),
        "---\nname: poison\ndescription: fixture\n---\nDo nothing.\n",
    );
    let script = dir.path().join("probe.mjs");
    write(
        &script,
        r"
import {pathToFileURL} from 'node:url';
const {query}=await import(pathToFileURL(process.env.FIXTURE_SDK).href);
async function* idle(){await new Promise(()=>{});}
const options=JSON.parse(process.env.FIXTURE_OPTIONS);
const q=query({prompt:idle(),options:{...options,cwd:process.cwd(),pathToClaudeCodeExecutable:process.env.FIXTURE_CLI}});
try { console.log(JSON.stringify({commands:(await q.supportedCommands()).map(c=>c.name),mcp:await q.mcpServerStatus()})); }
finally {q.close();process.exit(0);}
",
    );
    let owned = [(
        "intent_sentinel".into(),
        intent_acp::mcp_config::NormalizedMcpServer::Stdio {
            command: "/bin/false".into(),
            args: vec![],
            env: BTreeMap::default(),
        },
    )]
    .into();
    let mut request = req(dir.path(), &home, &project, "claude-code", &owned);
    request.purpose = LaunchPurpose::Persistent;
    request.persistent_identity = Some("claude-fixture");
    let p = prepare_provider_profile(request).unwrap();
    for isolated in [false, true] {
        let options = if isolated {
            let mut options = p.session_meta["claudeCode"]["options"].clone();
            options["mcpServers"] = to_auggie_mcp_config(&p.approved_mcp)["mcpServers"].clone();
            options
        } else {
            json!({"settingSources":["user","project","local"]})
        };
        let mut command = tokio::process::Command::new("node");
        command
            .arg(&script)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("HOME", &home)
            .env("FIXTURE_SDK", &sdk)
            .env("FIXTURE_CLI", &binary)
            .env("FIXTURE_OPTIONS", options.to_string())
            .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
            .current_dir(&project)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        let child = command.spawn().unwrap();
        let _group = ProcessGroup(i32::try_from(child.id().unwrap()).unwrap());
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(45), child.wait_with_output())
                .await
                .unwrap()
                .unwrap();
        assert!(output.status.success());
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        let commands = result["commands"].as_array().unwrap();
        let servers = result["mcp"].as_array().unwrap();
        if isolated {
            assert!(!commands.iter().any(|v| v == "poison"));
            assert!(servers.iter().any(|v| v["name"] == "intent_sentinel"));
            assert!(!servers
                .iter()
                .any(|v| v["name"] == "user_poison" || v["name"] == "project_poison"));
        } else {
            assert!(commands.iter().any(|v| v == "poison"));
            assert!(servers.iter().any(|v| v["name"] == "user_poison"));
        }
    }
}

#[tokio::test]
#[ignore = "requires INTENTD_PROFILE_PI_JS pointing to unmodified Pi 0.81.0 cli.js"]
async fn upstream_pi_retains_explicit_extension_under_ambient_suppression() {
    let cli = std::env::var("INTENTD_PROFILE_PI_JS").expect("set installed Pi cli.js path");
    let dir = crate::test_support::test_tempdir("profile-upstream-pi");
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    write(
        &home.join(".agents/skills/poison/SKILL.md"),
        "---\nname: poison\ndescription: sentinel\n---\nDo nothing.\n",
    );
    write(&home.join(".pi/agent/extensions/ambient.js"),"export default function(pi) { pi.registerCommand('ambient-sentinel', { description:'fixture', handler:async()=>{} }); }\n");
    let explicit = dir.path().join("explicit.js");
    write(&explicit,"export default function(pi) { pi.registerCommand('intent-sentinel', { description:'fixture', handler:async()=>{} }); }\n");
    let mcp = NormalizedMcpServers::new();
    let p = prepare_provider_profile(req(dir.path(), &home, &project, "pi", &mcp)).unwrap();
    for isolated in [false, true] {
        let mut command = tokio::process::Command::new("node");
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("HOME", &home)
            .current_dir(&project)
            .arg(&cli)
            .args(["--mode", "rpc", "--no-themes"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if isolated {
            command.args(&p.native_args).arg("-e").arg(&explicit);
        }
        let mut child = command.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"{\"id\":\"fixture\",\"type\":\"get_commands\"}\n")
            .await
            .unwrap();
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
                .await
                .unwrap()
                .unwrap();
        let values: Vec<Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        let commands = values
            .iter()
            .find(|v| v["command"] == "get_commands")
            .unwrap()["data"]["commands"]
            .as_array()
            .unwrap();
        let names: Vec<_> = commands.iter().filter_map(|v| v["name"].as_str()).collect();
        if isolated {
            assert!(names.contains(&"intent-sentinel"));
            assert!(!names.contains(&"ambient-sentinel"));
            assert!(!names.contains(&"skill:poison"));
        } else {
            assert!(names.contains(&"ambient-sentinel"));
            assert!(names.contains(&"skill:poison"));
        }
    }
}

#[tokio::test]
#[ignore = "requires INTENTD_PROFILE_OPENCODE_BIN pointing to unmodified OpenCode 1.18.18"]
async fn upstream_opencode_suppresses_common_roots_and_reports_home_residual() {
    let binary = std::env::var("INTENTD_PROFILE_OPENCODE_BIN").expect("set OpenCode binary path");
    let dir = crate::test_support::test_tempdir("profile-upstream-oc");
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    write(
        &project.join("opencode.json"),
        r#"{"mcp":{"project_poison":{"type":"local","command":["/bin/false"]}}}"#,
    );
    write(
        &home.join(".config/opencode/opencode.json"),
        r#"{"mcp":{"user_poison":{"type":"local","command":["/bin/false"]}}}"#,
    );
    write(
        &home.join(".opencode/opencode.json"),
        r#"{"mcp":{"home_residual":{"type":"local","command":["/bin/false"]}}}"#,
    );
    let mcp = [(
        "remote_fixture".into(),
        intent_acp::mcp_config::NormalizedMcpServer::Http {
            url: "http://127.0.0.1:1/owned".into(),
            headers: None,
        },
    )]
    .into();
    let mut request = req(dir.path(), &home, &project, "opencode", &mcp);
    request.purpose = LaunchPurpose::Persistent;
    request.persistent_identity = Some("upstream-opencode-fixture");
    let p = prepare_provider_profile(request).unwrap();
    let mut command = tokio::process::Command::new(binary);
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .env("HOME", &home)
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"))
        .env("OPENCODE_DISABLE_MODELS_FETCH", "1")
        .current_dir(&project)
        .args(["debug", "config"])
        .kill_on_drop(true);
    p.apply_to_command(&mut command);
    let output = tokio::time::timeout(std::time::Duration::from_secs(40), command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(config["mcp"].get("project_poison").is_none());
    assert!(config["mcp"].get("user_poison").is_none());
    assert_eq!(config["mcp"]["remote_fixture"]["oauth"], false);
    assert!(
        config["mcp"].get("home_residual").is_some(),
        "documented residual changed; re-audit before upgrading claims"
    );
}

#[cfg(unix)]
struct ProcessGroup(i32);
#[cfg(unix)]
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.0),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires INTENTD_PROFILE_CODEX_ADAPTER_JS and INTENTD_PROFILE_CODEX_BIN; no authenticated/persisted resume claim"]
async fn upstream_codex_owned_map_reaches_start_and_live_thread_resume() {
    let adapter = std::env::var("INTENTD_PROFILE_CODEX_ADAPTER_JS")
        .expect("set codex-acp 2.1.0 dist/index.js");
    let binary = std::env::var("INTENTD_PROFILE_CODEX_BIN").expect("set native Codex 0.160.0 path");
    let dir = crate::test_support::test_tempdir("profile-upstream-codex");
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    write(&home.join(".codex/config.toml"),"model_provider='fixture'\nmodel='fixture'\n[model_providers.fixture]\nname='No-network fixture'\nbase_url='http://127.0.0.1:1/v1'\nwire_api='responses'\nrequires_openai_auth=false\n");
    write(&project.join(".codex/config.toml"),"[mcp_servers.native]\nurl='http://127.0.0.1:1/native'\n[mcp_servers.native.http_headers]\nInherited='poison'\n");
    let owned = [(
        "native".into(),
        intent_acp::mcp_config::NormalizedMcpServer::Http {
            url: "http://127.0.0.1:1/owned".into(),
            headers: None,
        },
    )]
    .into();
    let mut request = req(dir.path(), &home, &project, "codex", &owned);
    request.purpose = LaunchPurpose::Persistent;
    request.persistent_identity = Some("upstream-codex-fixture");
    let p = prepare_provider_profile(request).unwrap();
    let logs = dir.path().join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    let mut command = tokio::process::Command::new("node");
    command
        .arg(adapter)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .env("HOME", &home)
        .env("CODEX_PATH", binary)
        .env("APP_SERVER_LOGS", &logs)
        .current_dir(&project)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command.process_group(0);
    p.apply_to_command(&mut command);
    let mut child = command.spawn().unwrap();
    let _group = ProcessGroup(i32::try_from(child.id().unwrap()).unwrap());
    let mut stdin = child.stdin.take().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut session = String::new();
    for id in 1..=4 {
        let (method, params) = match id {
            1 => (
                "initialize",
                json!({"protocolVersion":1,"clientInfo":{"name":"intent-profile-test","version":"1"},"clientCapabilities":{}}),
            ),
            2 => ("session/new", json!({"cwd":project,"mcpServers":[]})),
            3 => (
                "session/load",
                json!({"sessionId":session,"cwd":project,"mcpServers":[]}),
            ),
            _ => (
                "session/resume",
                json!({"sessionId":session,"cwd":project,"mcpServers":[]}),
            ),
        };
        stdin
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(40), async {
            loop {
                let line = lines.next_line().await.unwrap().expect("adapter exited");
                let value: Value = serde_json::from_str(&line).unwrap();
                if value["id"] == id {
                    break value;
                }
            }
        })
        .await
        .unwrap();
        assert!(result.get("error").is_none(), "{method}: {result}");
        if id == 2 {
            session = result["result"]["sessionId"].as_str().unwrap().into();
        }
    }
    child.kill().await.unwrap();
    let log = std::fs::read_to_string(logs.join("app-server.log")).unwrap();
    assert!(log.contains("thread/start"));
    assert!(log.contains("thread/resume"));
    let internal = p.mcp_name_mapping.keys().next().unwrap();
    assert!(log.contains(internal));
    assert!(log.contains("mcp_servers"));
    // This unprompted thread may use the adapter's live-thread fallback. It is
    // deliberately not an authenticated or persisted-across-process resume test.
}
