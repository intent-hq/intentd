//! Real npm/npx, offline: only the adapter executable is a deterministic fixture.
//! Set `INTENTD_NPX_RECOVERY_ARTIFACT_DIR` to retain a JSON receipt per scenario.
//! `INTENTD_TEST_KEEP_TMP` also keeps the seeded caches and ACP request journals.

#![cfg(unix)]

#[path = "npx_cache_recovery/blocking_faults.rs"]
mod blocking_faults;
#[path = "npx_cache_recovery/queue_faults.rs"]
mod queue_faults;
#[path = "npx_cache_recovery/storage_faults.rs"]
mod storage_faults;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use intent_acp::npx_cache::{repair_npx_cache, CommandEnvironment};
use intent_acp::spawn::{
    prepare_provider, spawn_prepared_provider, spawn_provider, SpawnOptions, NPX_NO_WORKSPACES_ARG,
};
use intent_acp::transport::ConnectionHooks;
use intent_providers::{find_provider, resolve_on_path, CLAUDE_AGENT_ACP_NPX_PACKAGE};
use serde_json::{json, Value};
use sha2::{Digest, Sha512};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::process::Command;

const ACP_FIXTURE: &str = r"#!/usr/bin/env node
const fs = require('node:fs');
const readline = require('node:readline');
readline.createInterface({ input: process.stdin }).on('line', line => {
  const request = JSON.parse(line);
  if (!Object.hasOwn(request, 'id')) return;
  fs.appendFileSync(process.env.NPX_RECOVERY_JOURNAL,
    JSON.stringify({method: request.method, params: request.params, pid: process.pid}) + '\n');
  let result;
  switch (request.method) {
    case 'initialize': result = {protocolVersion: 1, agentCapabilities: {loadSession: false}}; break;
    case 'session/new': result = {sessionId: 'cache-recovery-session'}; break;
    case 'session/prompt': result = {stopReason: 'end_turn'}; break;
    default:
      process.stdout.write(JSON.stringify({jsonrpc: '2.0', id: request.id,
        error: {code: -32601, message: 'Unexpected fixture request'}}) + '\n');
      return;
  }
  process.stdout.write(JSON.stringify({jsonrpc: '2.0', id: request.id, result}) + '\n');
});
";

// The same preload reaches npm's config child. Block only the repair Node
// process reading the exact installed manifest; never npm's own package.json.
const NODE_PRELOAD: &str = r"
const fs = require('node:fs');
if (process.env.NPX_RECOVERY_PROCESSES) {
  fs.appendFileSync(process.env.NPX_RECOVERY_PROCESSES,
    JSON.stringify({pid: process.pid, argv: process.argv}) + '\n');
}
const originalStat = fs.lstatSync;
fs.lstatSync = function(file, ...args) {
  if (process.argv[1] === process.env.NPX_RECOVERY_SPEC &&
      process.argv[2] === process.env.NPX_RECOVERY_NPM &&
      String(file) === process.env.NPX_RECOVERY_BLOCK_MANIFEST) {
    fs.writeFileSync(process.env.NPX_RECOVERY_STARTED,
      JSON.stringify({pid: process.pid, cwd: process.cwd()}));
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  }
  return originalStat.call(this, file, ...args);
};
";

struct Fixture {
    root: tempfile::TempDir,
    npx: PathBuf,
    cache: PathBuf,
    entry: PathBuf,
    package: PathBuf,
    npmrc: PathBuf,
    env: BTreeMap<String, String>,
}

impl Fixture {
    fn new() -> Option<Self> {
        let Some(npx) = resolve_on_path("npx") else {
            eprintln!("SKIP npx cache recovery: install Node with npm/npx to run this scenario");
            return None;
        };
        assert!(
            npx.parent().unwrap().join("npm").is_file(),
            "real npx must have its sibling npm"
        );
        let mut root = tempfile::Builder::new()
            .prefix("intent-acp-npx-recovery-")
            .tempdir()
            .unwrap();
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            root.disable_cleanup(true);
        }
        let cache = root.path().join("custom npm cache with spaces");
        let hash = npm_cache_key(CLAUDE_AGENT_ACP_NPX_PACKAGE);
        let entry = cache.join("_npx").join(&hash[..16]);
        let (name, version) = CLAUDE_AGENT_ACP_NPX_PACKAGE.rsplit_once('@').unwrap();
        let package = entry.join("node_modules").join(name);
        fs::create_dir_all(&package).unwrap();
        fs::create_dir_all(entry.join("node_modules/.bin")).unwrap();
        write_json(
            &package.join("package.json"),
            &json!({"name": name, "version": version, "bin": {"claude-agent-acp": "fixture.cjs"}}),
        );
        let script = package.join("fixture.cjs");
        fs::write(&script, ACP_FIXTURE).unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        symlink(
            Path::new("..").join(name).join("fixture.cjs"),
            entry.join("node_modules/.bin/claude-agent-acp"),
        )
        .unwrap();
        fs::write(entry.join("keep-me.txt"), b"unrelated cache entry content").unwrap();
        fs::create_dir_all(cache.join("_npx/unrelated-entry")).unwrap();
        fs::write(
            cache.join("_npx/unrelated-entry/keep.txt"),
            b"other package",
        )
        .unwrap();
        let npmrc = root.path().join("user npm config with spaces.npmrc");
        fs::write(&npmrc, format!("cache={}\n", cache.display())).unwrap();
        let home = root.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let empty_global = root.path().join("global.npmrc");
        fs::write(&empty_global, "").unwrap();
        let mut env = BTreeMap::from([
            ("HOME".into(), home.to_string_lossy().into_owned()),
            ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
        ]);
        // Both casings prevent inherited npm configuration from winning. No
        // package metadata or tarballs are available: a fetch must fail offline.
        for (key, value) in [
            ("userconfig", npmrc.to_string_lossy().into_owned()),
            ("globalconfig", empty_global.to_string_lossy().into_owned()),
            ("offline", "true".into()),
            ("registry", "http://127.0.0.1:9/".into()),
            ("audit", "false".into()),
            ("fund", "false".into()),
            ("update_notifier", "false".into()),
            ("ignore_scripts", "true".into()),
        ] {
            env.insert(format!("npm_config_{key}"), value.clone());
            env.insert(format!("NPM_CONFIG_{}", key.to_uppercase()), value);
        }
        Some(Self {
            root,
            npx,
            cache,
            entry,
            package,
            npmrc,
            env,
        })
    }

    fn instrument_node(&mut self) -> PathBuf {
        let preload = self.root.path().join("node preload with spaces.cjs");
        fs::write(&preload, NODE_PRELOAD).unwrap();
        self.env.insert(
            "NODE_OPTIONS".into(),
            format!("--require={}", serde_json::to_string(&preload).unwrap()),
        );
        self.env.insert(
            "NPX_RECOVERY_SPEC".into(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE.into(),
        );
        self.env.insert(
            "NPX_RECOVERY_NPM".into(),
            self.npx
                .with_file_name("npm")
                .to_string_lossy()
                .into_owned(),
        );
        let processes = self.root.path().join("node-processes.ndjson");
        self.env.insert(
            "NPX_RECOVERY_PROCESSES".into(),
            processes.to_string_lossy().into_owned(),
        );
        processes
    }

    fn manifest(&self) -> PathBuf {
        self.entry.join("package.json")
    }

    async fn prime_registry_metadata(&mut self) {
        let (name, version) = CLAUDE_AGENT_ACP_NPX_PACKAGE.rsplit_once('@').unwrap();
        write_json(
            &self.manifest(),
            &json!({"private": true, "dependencies": BTreeMap::from([(name, version)])}),
        );
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let registry = format!("http://{}/", listener.local_addr().unwrap());
        for key in ["npm_config_registry", "NPM_CONFIG_REGISTRY"] {
            self.env.insert(key.into(), registry.clone());
        }
        let metadata = serde_json::to_vec(&json!({
            "name": name,
            "dist-tags": {"latest": version},
            "versions": BTreeMap::from([(version, json!({
                "name": name,
                "version": version,
                "bin": {"claude-agent-acp": "fixture.cjs"},
                "dist": {"tarball": format!("{registry}fixture.tgz")}
            }))])
        }))
        .unwrap();
        let server = tokio::spawn(async move {
            let serve = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut bytes = [0; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let length = socket.read(&mut bytes).await.unwrap();
                    assert!(length > 0 && request.len() < 16_384);
                    request.extend_from_slice(&bytes[..length]);
                }
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: public, max-age=31536000\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    metadata.len()
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                socket.write_all(&metadata).await.unwrap();
                socket.shutdown().await.unwrap();
            };
            tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_secs(30)),
                serve,
            )
            .await
            .expect("local npm registry request");
        });
        let output = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(30)),
            Command::new(&self.npx)
                .env_clear()
                .envs(&self.env)
                .env("npm_config_cache", &self.cache)
                .env("NPM_CONFIG_CACHE", &self.cache)
                .current_dir(self.root.path())
                .args([
                    NPX_NO_WORKSPACES_ARG,
                    "--save=true",
                    "--yes",
                    "--offline=false",
                ])
                .arg(format!("--package={CLAUDE_AGENT_ACP_NPX_PACKAGE}"))
                .args(["--", "node", "--version"])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("cache the local registry metadata")
        .unwrap();
        server.await.unwrap();
        assert!(
            output.status.success(),
            "npx cache priming failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.starts_with(b"v"));
        fs::remove_file(self.manifest()).unwrap();
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.npx);
        command
            .env_clear()
            .envs(&self.env)
            .current_dir(self.root.path())
            .args([
                NPX_NO_WORKSPACES_ARG,
                "--save=true",
                "-y",
                CLAUDE_AGENT_ACP_NPX_PACKAGE,
            ]);
        command
    }

    async fn missing_manifest_control(&self) -> Value {
        let output = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(30)),
            Command::new(&self.npx)
                .env_clear()
                .envs(&self.env)
                .current_dir(self.root.path())
                .args([NPX_NO_WORKSPACES_ARG, "--save=true", "--yes"])
                .arg(format!("--package={CLAUDE_AGENT_ACP_NPX_PACKAGE}"))
                .args(["--", "node", "--version"])
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("unrepaired npx control completes")
        .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() {
            // Older npm releases can run the installed bin without the root
            // manifest. Keep them supported while recording the control result.
            assert!(output.stdout.starts_with(b"v"));
            eprintln!("npm does not exhibit the missing-manifest regression: {stderr}");
        } else {
            assert!(
                stderr.contains("ENOENT")
                    && stderr.contains(self.manifest().to_string_lossy().as_ref()),
                "control must fail for the missing manifest, not another npm error: {stderr}"
            );
        }
        if self.manifest().exists() {
            fs::remove_file(self.manifest()).unwrap();
        }
        json!({"regressionReproduced": !output.status.success(),
            "exitCode": output.status.code(), "stderr": stderr})
    }

    fn options(&self, journal: &Path) -> SpawnOptions<'_> {
        let mut opts = SpawnOptions::new(find_provider("claude-code").unwrap());
        opts.npx_fallback_binary = Some(&self.npx);
        opts.npx_fallback_package = Some(CLAUDE_AGENT_ACP_NPX_PACKAGE);
        opts.npx_launch_root = Some(self.root.path());
        opts.cwd = Some(self.root.path());
        opts.extra_env = self.env.clone();
        // SpawnOptions inherits the daemon environment. Pin the cache explicitly
        // here; the helper scenarios above use only the custom npmrc setting.
        for key in ["npm_config_cache", "NPM_CONFIG_CACHE"] {
            opts.extra_env
                .insert(key.into(), self.cache.to_string_lossy().into_owned());
        }
        opts.extra_env.insert(
            "NPX_RECOVERY_JOURNAL".into(),
            journal.to_string_lossy().into_owned(),
        );
        opts
    }

    fn assert_repaired(&self) -> Value {
        let manifest: Value = serde_json::from_slice(&fs::read(self.manifest()).unwrap()).unwrap();
        let (name, version) = CLAUDE_AGENT_ACP_NPX_PACKAGE.rsplit_once('@').unwrap();
        assert_eq!(manifest["dependencies"][name], version);
        self.assert_unrelated_preserved();
        manifest
    }

    fn assert_unrelated_preserved(&self) {
        assert_eq!(
            fs::read(self.entry.join("keep-me.txt")).unwrap(),
            b"unrelated cache entry content"
        );
        assert_eq!(
            fs::read(self.cache.join("_npx/unrelated-entry/keep.txt")).unwrap(),
            b"other package"
        );
        assert_eq!(
            fs::read(self.package.join("fixture.cjs")).unwrap(),
            ACP_FIXTURE.as_bytes()
        );
    }

    fn report(&self, scenario: &str, observations: &Value) {
        let report = json!({
            "scenario": scenario,
            "status": "passed",
            "packageSpec": CLAUDE_AGENT_ACP_NPX_PACKAGE,
            "npx": self.npx,
            "cacheKey": self.entry.file_name().unwrap().to_string_lossy(),
            "offline": true,
            "observations": observations,
        });
        if let Some(dir) = std::env::var_os("INTENTD_NPX_RECOVERY_ARTIFACT_DIR") {
            let dir = PathBuf::from(dir);
            fs::create_dir_all(&dir).unwrap();
            let path = dir.join(format!("{scenario}.json"));
            write_json(&path, &report);
            eprintln!("npx recovery artifact: {}", path.display());
        } else {
            eprintln!("npx recovery receipt: {report}");
        }
    }
}

fn write_json(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

#[track_caller]
fn npm_cache_key(package: &str) -> String {
    let mut hash = String::with_capacity(16);
    for byte in Sha512::digest(package.as_bytes()).iter().take(8) {
        write!(hash, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hash
}

async fn turn(fixture: &Fixture, name: &str, prepared: bool) -> Vec<Value> {
    let journal = fixture.root.path().join(format!("{name}.ndjson"));
    let opts = fixture.options(&journal);
    let mut agent = if prepared {
        spawn_prepared_provider(
            &opts,
            prepare_provider(&opts).unwrap(),
            ConnectionHooks::default(),
        )
    } else {
        spawn_provider(&opts, ConnectionHooks::default())
    }
    .expect("spawn real npx after explicit setup repair");
    // Always reap the npm process group, including when a protocol assertion
    // fails. Return the outcome first, then assert after cleanup.
    let outcome = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(30)),
        async {
            let conn = agent.connection();
            let initialized = conn.request_timeout("initialize", json!({"protocolVersion": 1, "clientCapabilities": {}}),
                intentd_test_support::test_timeout(Duration::from_secs(30))).await?;
            let session = conn.request("session/new", json!({"cwd": fixture.root.path(), "mcpServers": []})).await?;
            let response = conn.request("session/prompt", json!({"sessionId": session["sessionId"], "prompt": [{"type": "text", "text": "exactly one turn"}]})).await?;
            Ok::<_, intent_acp::AcpError>((initialized, session, response))
        },
    ).await;
    let stderr = agent.connection().recent_stderr();
    agent.kill().await.expect("reap npm and its adapter");
    let (initialized, session, response) = outcome
        .expect("ACP fixture timed out")
        .unwrap_or_else(|e| panic!("ACP failed: {e}; stderr: {stderr:?}"));
    assert_eq!(initialized["protocolVersion"], 1);
    assert_eq!(session["sessionId"], "cache-recovery-session");
    assert_eq!(response["stopReason"], "end_turn");
    let journal: Vec<Value> = fs::read_to_string(journal)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        journal
            .iter()
            .map(|row| row["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["initialize", "session/new", "session/prompt"],
        "one session and exactly one prompt, without replay"
    );
    assert_eq!(journal[1]["params"]["cwd"], json!(fixture.root.path()));
    journal
}

#[tokio::test]
async fn npx_cache_recovery_explicit_setup_real_npm_completes_one_turn_without_replay() {
    let Some(mut fixture) = Fixture::new() else {
        return;
    };
    fixture.prime_registry_metadata().await;
    assert!(!fixture.manifest().exists());
    let control = fixture.missing_manifest_control().await;
    assert!(
        repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    let requests = turn(&fixture, "setup-repair", false).await;
    let manifest = fixture.assert_repaired();
    fixture.report(
        "setup-recovery",
        &json!({"unrepairedControl": control, "manifest": manifest, "requests": requests, "promptCount": 1}),
    );
}

#[tokio::test]
async fn npx_cache_recovery_concurrent_real_launches_share_a_repaired_cache() {
    let Some(mut fixture) = Fixture::new() else {
        return;
    };
    fixture.prime_registry_metadata().await;
    let first_command = fixture.command();
    let second_command = fixture.command();
    let (first_repair, second_repair) = tokio::join!(
        repair_npx_cache(
            &first_command,
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            ()
        ),
        repair_npx_cache(
            &second_command,
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            ()
        )
    );
    assert!(
        first_repair || second_repair,
        "one setup publishes the manifest"
    );
    let (first, second) = tokio::join!(
        turn(&fixture, "first", false),
        turn(&fixture, "second", true)
    );
    fixture.assert_repaired();
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await,
        "an already published repair is a no-op"
    );
    fixture.report(
        "concurrent-launches",
        &json!({"first": first, "second": second, "promptCount": 2}),
    );
}

#[tokio::test]
async fn npx_cache_recovery_resolves_npmrc_cache_and_preserves_healthy_manifest() {
    let Some(fixture) = Fixture::new() else {
        return;
    };
    assert!(fixture.npmrc.to_string_lossy().contains(' '));
    assert!(
        repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await,
        "missing manifest in npmrc-configured cache is repaired"
    );
    let manifest = fixture.assert_repaired();
    // Custom formatting and extra fields must survive a healthy no-op byte for byte.
    let mut healthy = manifest;
    healthy["fixtureSentinel"] = json!("preserve me");
    write_json(&fixture.manifest(), &healthy);
    let before = fs::read(fixture.manifest()).unwrap();
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert_eq!(fs::read(fixture.manifest()).unwrap(), before);
    fixture.assert_unrelated_preserved();
    fixture.report(
        "npmrc-and-healthy-noop",
        &json!({"cachePathContainsSpaces": true, "healthyBytesPreserved": true}),
    );
}

#[tokio::test]
async fn npx_cache_recovery_rejects_wrong_pin_unregistered_spec_and_invalid_json() {
    let Some(fixture) = Fixture::new() else {
        return;
    };
    let installed_manifest = fixture.package.join("package.json");
    let original = fs::read(&installed_manifest).unwrap();
    let mut wrong: Value = serde_json::from_slice(&original).unwrap();
    wrong["version"] = json!("0.0.0-unregistered-fixture");
    write_json(&installed_manifest, &wrong);
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert!(
        !fixture.manifest().exists(),
        "wrong installed version cannot authorize repair"
    );
    fs::write(&installed_manifest, &original).unwrap();
    fs::write(&installed_manifest, b"{invalid installed manifest").unwrap();
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert!(!fixture.manifest().exists());
    fs::write(&installed_manifest, original).unwrap();
    let (name, _) = CLAUDE_AGENT_ACP_NPX_PACKAGE.rsplit_once('@').unwrap();
    for spec in [
        name.to_string(),
        format!("{name}@latest"),
        format!("{name}@0.0.0"),
        "file:./local-package".into(),
    ] {
        assert!(
            !repair_npx_cache(&fixture.command(), &spec, CommandEnvironment::Complete, ()).await,
            "unregistered spec: {spec}"
        );
        assert!(!fixture.manifest().exists());
    }
    // A complete, correctly hashed cache for an arbitrary exact pin must also
    // be refused. Merely testing an absent unknown entry would miss this guard.
    let unknown_name = "@intent-test/unregistered-adapter";
    let (_, version) = CLAUDE_AGENT_ACP_NPX_PACKAGE.rsplit_once('@').unwrap();
    let unknown_spec = format!("{unknown_name}@{version}");
    let hash = npm_cache_key(&unknown_spec);
    let unknown_entry = fixture.cache.join("_npx").join(&hash[..16]);
    let unknown_package = unknown_entry.join("node_modules").join(unknown_name);
    fs::create_dir_all(&unknown_package).unwrap();
    let unknown_manifest = json!({"name": unknown_name, "version": version});
    write_json(&unknown_package.join("package.json"), &unknown_manifest);
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            &unknown_spec,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert!(!unknown_entry.join("package.json").exists());
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(unknown_package.join("package.json")).unwrap())
            .unwrap(),
        unknown_manifest
    );
    fs::write(fixture.manifest(), b"{invalid root manifest").unwrap();
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert_eq!(
        fs::read(fixture.manifest()).unwrap(),
        b"{invalid root manifest"
    );
    fixture.assert_unrelated_preserved();
    fixture.report("unsafe-input-noops", &json!({"wrongPin": "no-op", "invalidInstalledManifest": "no-op", "unregisteredSpecs": "no-op", "invalidRootManifest": "preserved"}));
}

#[tokio::test]
async fn npx_cache_recovery_does_not_create_an_unpopulated_cache_entry() {
    let Some(fixture) = Fixture::new() else {
        return;
    };
    fs::remove_dir_all(&fixture.entry).unwrap();
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert!(
        !fixture.entry.exists(),
        "recovery must not install or create an empty entry"
    );
    assert_eq!(
        fs::read(fixture.cache.join("_npx/unrelated-entry/keep.txt")).unwrap(),
        b"other package"
    );
    fixture.report(
        "missing-entry-noop",
        &json!({"entryCreated": false, "unrelatedEntryPreserved": true}),
    );
}

#[tokio::test]
async fn npx_cache_recovery_does_not_follow_symlinked_manifest_package_or_entry() {
    let Some(fixture) = Fixture::new() else {
        return;
    };
    let outside = fixture.root.path().join("outside");
    fs::create_dir_all(&outside).unwrap();
    let outside_manifest = outside.join("package.json");
    symlink(&outside_manifest, fixture.manifest()).unwrap();
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert!(
        !outside_manifest.exists(),
        "dangling symlink target stays absent"
    );
    assert!(fs::symlink_metadata(fixture.manifest())
        .unwrap()
        .file_type()
        .is_symlink());
    fs::remove_file(fixture.manifest()).unwrap();

    let installed = fixture.package.join("package.json");
    fs::rename(&installed, &outside_manifest).unwrap();
    let original = fs::read(&outside_manifest).unwrap();
    symlink(&outside_manifest, &installed).unwrap();
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert!(!fixture.manifest().exists());
    assert_eq!(fs::read(&outside_manifest).unwrap(), original);
    fs::remove_file(installed).unwrap();
    fs::rename(&outside_manifest, fixture.package.join("package.json")).unwrap();

    let moved_package = outside.join("adapter");
    fs::rename(&fixture.package, &moved_package).unwrap();
    symlink(&moved_package, &fixture.package).unwrap();
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert!(!fixture.manifest().exists());
    fs::remove_file(&fixture.package).unwrap();
    fs::rename(&moved_package, &fixture.package).unwrap();

    let moved_entry = outside.join("entry");
    fs::rename(&fixture.entry, &moved_entry).unwrap();
    symlink(&moved_entry, &fixture.entry).unwrap();
    assert!(
        !repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    assert!(!moved_entry.join("package.json").exists());
    fs::remove_file(&fixture.entry).unwrap();
    fs::rename(&moved_entry, &fixture.entry).unwrap();
    fixture.assert_unrelated_preserved();
    fixture.report("symlink-noops", &json!({"rootManifest": "preserved", "installedManifest": "preserved", "packageDirectory": "preserved", "cacheEntry": "preserved"}));
}

#[tokio::test]
async fn npx_cache_recovery_normal_healthy_launches_start_no_repair_process() {
    let Some(mut fixture) = Fixture::new() else {
        return;
    };
    fixture.prime_registry_metadata().await;
    assert!(
        repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        )
        .await
    );
    // Start recording only after setup. Both public launch paths must remain
    // synchronous and launch only npx plus its adapter, with no repair preflight.
    let processes = fixture.instrument_node();
    let normal = turn(&fixture, "healthy-normal", false).await;
    let prepared = turn(&fixture, "healthy-prepared", true).await;
    let rows: Vec<Value> = fs::read_to_string(processes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        rows.len() >= 4,
        "preload must observe npx and adapter on both launches: {rows:?}"
    );
    let npm = fixture.npx.with_file_name("npm");
    for row in &rows {
        let argv = row["argv"].as_array().unwrap();
        assert!(
            !(argv.get(1).and_then(Value::as_str) == Some(CLAUDE_AGENT_ACP_NPX_PACKAGE)
                && argv.get(2).and_then(Value::as_str) == npm.to_str()),
            "normal startup launched a repair preflight: {row}"
        );
        assert!(
            !argv
                .windows(3)
                .any(|args| args == [json!("config"), json!("get"), json!("cache")]),
            "normal startup queried npm cache configuration: {row}"
        );
    }
    for requests in [&normal, &prepared] {
        let adapter_pid = &requests[0]["pid"];
        assert!(
            rows.iter().any(|row| &row["pid"] == adapter_pid),
            "the preload must observe each actual adapter process"
        );
    }
    fixture.report(
        "normal-launch-no-repair",
        &json!({"processes": rows,
        "normal": normal, "prepared": prepared, "repairProcesses": 0, "configProcesses": 0}),
    );
}

// The owned repair job must keep this private cwd alive until its child has
// exited AND been reaped. A zombie still answers kill(pid, 0), so ESRCH proves
// stronger cleanup than a signal or a caller timeout alone.
struct CwdLease {
    cwd: Option<tempfile::TempDir>,
    started: PathBuf,
    dropped: Option<tokio::sync::oneshot::Sender<Value>>,
}

impl Drop for CwdLease {
    fn drop(&mut self) {
        let marker = fs::read(&self.started)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or(Value::Null);
        let reaped = marker["pid"]
            .as_i64()
            .and_then(|pid| i32::try_from(pid).ok())
            .is_some_and(|pid| {
                nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
                    == Err(nix::errno::Errno::ESRCH)
            });
        let helper_exited = marker["helperPid"]
            .as_i64()
            .and_then(|pid| i32::try_from(pid).ok())
            .map(process_exited);
        let cwd_existed = self.cwd.as_ref().is_some_and(|cwd| cwd.path().is_dir());
        drop(self.cwd.take());
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(
                json!({"child": marker, "childReapedBeforeLeaseDrop": reaped,
                "cwdExistedUntilLeaseDrop": cwd_existed,
                "helperExitedBeforeLeaseDrop": helper_exited}),
            );
        }
    }
}

async fn stalled_repair(cancel: bool) {
    let Some(mut fixture) = Fixture::new() else {
        return;
    };
    fixture.instrument_node();
    let started = fixture.root.path().join("blocked-read.json");
    fixture.env.insert(
        "NPX_RECOVERY_STARTED".into(),
        started.to_string_lossy().into_owned(),
    );
    fixture.env.insert(
        "NPX_RECOVERY_BLOCK_MANIFEST".into(),
        fixture
            .package
            .join("package.json")
            .to_string_lossy()
            .into_owned(),
    );
    let cwd = tempfile::Builder::new()
        .prefix("npx-repair-lease-")
        .tempdir()
        .unwrap();
    let cwd_path = cwd.path().to_owned();
    let canonical_cwd = fs::canonicalize(&cwd_path).unwrap();
    let (dropped, dropped_rx) = tokio::sync::oneshot::channel();
    let lease = CwdLease {
        cwd: Some(cwd),
        started: started.clone(),
        dropped: Some(dropped),
    };
    let mut command = fixture.command();
    command.current_dir(&cwd_path);
    let repair = tokio::spawn(async move {
        repair_npx_cache(
            &command,
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            lease,
        )
        .await
    });
    // Poll an observable startup marker, never guess with a settling sleep.
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(15)),
        async {
            loop {
                if fs::read(&started)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .is_some()
                {
                    break;
                }
                assert!(
                    !repair.is_finished(),
                    "repair exited before it reached the stalled manifest metadata read"
                );
                // timing-guard: Poll the child-written marker; time alone never establishes readiness.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .expect("repair child reaches the exact installed manifest metadata read");
    assert!(
        cwd_path.is_dir(),
        "private cwd stays leased while child is blocked"
    );
    let marker: Value = serde_json::from_slice(&fs::read(&started).unwrap()).unwrap();
    assert_eq!(marker["cwd"], json!(canonical_cwd));
    if cancel {
        repair.abort();
        let outcome = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(15)),
            repair,
        )
        .await
        .expect("outer repair cancellation completes");
        assert!(outcome.unwrap_err().is_cancelled());
    } else {
        let repaired = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(15)),
            repair,
        )
        .await
        .expect("repair returns within the scaled hang guard despite blocked filesystem IO")
        .unwrap();
        assert!(!repaired, "stalled repair is advisory and returns false");
    }
    let cleanup = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(15)),
        dropped_rx,
    )
    .await
    .expect("owned repair job eventually releases its lease")
    .expect("lease sends a cleanup receipt");
    assert_eq!(cleanup["childReapedBeforeLeaseDrop"], true, "{cleanup}");
    assert_eq!(cleanup["cwdExistedUntilLeaseDrop"], true, "{cleanup}");
    assert!(
        !cwd_path.exists(),
        "lease removes its private cwd after reaping"
    );
    assert!(
        !fixture.manifest().exists(),
        "blocked repair never publishes a manifest"
    );
    fixture.assert_unrelated_preserved();
    fixture.report(
        if cancel {
            "cancelled-repair-reaps-before-lease-drop"
        } else {
            "stalled-cache-read-deadline"
        },
        &json!({"cancelled": cancel, "repairResult": if cancel { Value::Null } else { json!(false) }, "cleanup": cleanup}),
    );
}

#[tokio::test]
async fn npx_cache_recovery_stalled_manifest_read_returns_false_and_reaps_child() {
    stalled_repair(false).await;
}

#[tokio::test]
async fn npx_cache_recovery_caller_cancellation_reaps_child_before_releasing_cwd() {
    stalled_repair(true).await;
}

// Orphaned grandchildren can briefly be zombies on Unix, especially with a
// container init. A zombie has exited and cannot use the cwd; do not require
// this test process to become a global subreaper to collect another parent's child.
fn process_exited(pid: i32) -> bool {
    let absent = || {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
            == Err(nix::errno::Errno::ESRCH)
    };
    if absent() {
        return true;
    }
    let state = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output();
    state.is_ok_and(|state| {
        state.status.success()
            && String::from_utf8_lossy(&state.stdout)
                .trim_start()
                .starts_with('Z')
    }) || absent()
}

struct ResidualHelperGuard(PathBuf);

impl Drop for ResidualHelperGuard {
    fn drop(&mut self) {
        // Also clean up when a regression assertion fails. The helper never
        // inherits a test runner pipe and must not survive a failing scenario.
        if let Some(pid) = fs::read(&self.0)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .and_then(|marker| marker["helperPid"].as_i64())
            .and_then(|pid| i32::try_from(pid).ok())
        {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
}

#[tokio::test]
async fn npx_cache_recovery_reaps_residual_group_helper_after_leader_exits() {
    let Some(mut fixture) = Fixture::new() else {
        return;
    };
    let real_npx = fixture.npx.clone();
    let bin = fixture.root.path().join("selected npm wrapper");
    fs::create_dir_all(&bin).unwrap();
    // Capture requires basename npx plus sibling npm. Only npm is executed by
    // repair; the eventual normal repair below uses the original real npm.
    symlink(&real_npx, bin.join("npx")).unwrap();
    let npm = bin.join("npm");
    fs::write(&npm, r"#!/usr/bin/env node
const fs = require('node:fs');
const {spawn} = require('node:child_process');
const helper = spawn(process.execPath, ['-e', `
  const fs = require('node:fs');
  const {execFileSync} = require('node:child_process');
  const pgid = Number(execFileSync('ps', ['-o', 'pgid=', '-p', String(process.pid)], {encoding: 'utf8'}).trim());
  fs.writeFileSync(process.env.NPX_RECOVERY_STARTED, JSON.stringify({
    pid: Number(process.env.NPX_RECOVERY_LEADER), helperPid: process.pid,
    pgid, cwd: process.cwd()
  }));
  setInterval(() => {}, 1000);
`], {stdio: 'ignore', env: {...process.env, NPX_RECOVERY_LEADER: String(process.ppid)}});
helper.unref();
const ready = setInterval(() => {
  if (!fs.existsSync(process.env.NPX_RECOVERY_STARTED)) return;
  clearInterval(ready);
  // No inherited stdout/stderr pipe on the helper: the repair leader can exit
  // successfully while its same-group descendant remains alive.
  process.stdout.write(process.env.NPX_RECOVERY_CACHE + '\n', () => process.exit(0));
}, 5);
").unwrap();
    fs::set_permissions(&npm, fs::Permissions::from_mode(0o755)).unwrap();
    fixture.npx = bin.join("npx");
    fixture.instrument_node(); // Its repair argv[2] now matches the wrapper npm.
    let marker = fixture.root.path().join("residual-helper.json");
    fixture.env.insert(
        "NPX_RECOVERY_STARTED".into(),
        marker.to_string_lossy().into_owned(),
    );
    fixture.env.insert(
        "NPX_RECOVERY_CACHE".into(),
        fixture.cache.to_string_lossy().into_owned(),
    );
    let _helper_cleanup = ResidualHelperGuard(marker.clone());
    let cwd = tempfile::Builder::new()
        .prefix("npx-residual-lease-")
        .tempdir()
        .unwrap();
    let cwd_path = cwd.path().to_owned();
    let canonical_cwd = fs::canonicalize(&cwd_path).unwrap();
    let (dropped, dropped_rx) = tokio::sync::oneshot::channel();
    let lease = CwdLease {
        cwd: Some(cwd),
        started: marker.clone(),
        dropped: Some(dropped),
    };
    let mut command = fixture.command();
    command.current_dir(&cwd_path);
    let repaired = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(15)),
        repair_npx_cache(
            &command,
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            lease,
        ),
    )
    .await
    .expect("caller completes after repair leader exits with a residual helper");
    let cleanup = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(15)),
        dropped_rx,
    )
    .await
    .expect("residual helper cleanup releases cwd ownership and admission")
    .expect("lease sends residual helper cleanup receipt");
    let child = &cleanup["child"];
    assert_eq!(
        child["pid"], child["pgid"],
        "helper must inherit the repair leader's process group: {cleanup}"
    );
    assert_eq!(
        child["cwd"],
        json!(canonical_cwd),
        "helper used the leased private cwd"
    );
    assert_eq!(cleanup["childReapedBeforeLeaseDrop"], true, "{cleanup}");
    assert_eq!(cleanup["helperExitedBeforeLeaseDrop"], true, "{cleanup}");
    assert_eq!(cleanup["cwdExistedUntilLeaseDrop"], true, "{cleanup}");
    assert!(
        !cwd_path.exists(),
        "private cwd is released after group cleanup"
    );
    assert!(
        repaired,
        "successful leader exit remains a successful repair after residual helper cleanup"
    );
    fixture.assert_repaired();
    fs::remove_file(fixture.manifest()).unwrap();
    fixture.npx = real_npx;
    fixture.env.remove("NODE_OPTIONS");
    let subsequent = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(15)),
        repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        ),
    )
    .await
    .expect("a later normal repair can acquire admission");
    assert!(subsequent, "residual group cleanup must release the permit");
    fixture.assert_repaired();
    fixture.report(
        "residual-group-helper-cleanup",
        &json!({"repaired": repaired,
        "cleanup": cleanup, "subsequentRealNpmRepair": subsequent}),
    );
}
