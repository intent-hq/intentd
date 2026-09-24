//! Real import -> public session -> first-turn selection contract.
//!
//! Ordinary Cargo/nextest runs require the shared monorepo fixtures and never
//! rewrite them. `scripts/test-transfer-selection-contract.sh --regenerate`
//! is the explicit golden update route; `--output PATH` emits a fresh envelope.
//! Both require a clean, committed Cargo build. No provider turn is started.

use std::io::{Cursor, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use intent_core::{AgentId, Caller, WorkspaceApi, WorkspaceId};
use intent_store::Store;
use intentd_test_support::GuardedChild;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{test_support::test_tempdir, Services, SettingsRegistry};

const TEST_NAME: &str = "transfer_selection_contract::public_import_matches_contract";
const SOURCE: &[u8] = include_bytes!("transfer_selection_contract.rs");
const BUILD_REVISION: Option<&str> = option_env!("TRANSFER_SELECTION_BUILD_REVISION");

fn sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            write!(&mut hex, "{byte:02x}").unwrap();
            hex
        })
}

fn component_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn fixture_root() -> PathBuf {
    if let Some(path) = std::env::var_os("TRANSFER_SELECTION_FIXTURE_ROOT") {
        let path = PathBuf::from(path);
        assert!(
            path.is_absolute(),
            "TRANSFER_SELECTION_FIXTURE_ROOT must be absolute"
        );
        return path;
    }
    let root = component_root().join("../..");
    let modules = std::fs::read_to_string(root.join(".gitmodules")).unwrap_or_default();
    assert!(modules.lines().any(|line| line.trim() == "path = packages/intentd"),
        "standalone checkout: set TRANSFER_SELECTION_FIXTURE_ROOT to the shared monorepo fixture directory");
    root.join("docs/protocol/fixtures/transfer-selection")
}

fn read_json(path: &Path) -> Value {
    let bytes = std::fs::read(path).unwrap_or_else(|e| {
        panic!(
            "required fixture {}: {e}; set TRANSFER_SELECTION_FIXTURE_ROOT",
            path.display()
        )
    });
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn write_json(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn executable(path: &Path, text: &str) {
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

/// An isolated child avoids process-global HOME/PATH/auth caches racing other
/// lib tests. It gets only fixture binaries, a fake login shell, and scratch
/// settings/state. Its current Cargo-built test binary is the only Rust program.
fn run_imports(root: &Path, contract: &Value, fault: &str) -> Result<Value, String> {
    write_json(&root.join("contract.json"), contract);
    let bin = root.join(".augment/bin");
    std::fs::create_dir_all(&bin).unwrap();
    let sentinel =
        "#!/bin/sh\nprintf 'unexpected provider launch' > \"$HOME/provider-launched\"\nexit 91\n";
    for provider in ["auggie", "codex-acp"] {
        executable(&bin.join(provider), sentinel);
    }
    let shell = root.join("shell");
    executable(
        &shell,
        "#!/bin/sh\nprintf '__INTENT_PATH_S__%s__INTENT_PATH_E__' \"$PATH\"\n",
    );
    let log_path = root.join("worker.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env_clear()
        .env("HOME", root)
        .env("XDG_CONFIG_HOME", root)
        .env("XDG_DATA_HOME", root)
        .env("PATH", &bin)
        .env("SHELL", shell)
        .env("TMPDIR", root)
        .env("TRANSFER_SELECTION_WORKER", root)
        .env("TRANSFER_SELECTION_FAULT", fault)
        .env("INTENTD_ASSERT_BOUND_CALLER", "1")
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    // Coverage children write distinct profiles, preserving nextest's file
    // placeholders, without inheriting credentials or provider configuration.
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    let mut child = GuardedChild::spawn(&mut command).unwrap();
    let status = child.wait_with_timeout(Duration::from_secs(180)).unwrap();
    if !status.is_some_and(|status| status.success()) {
        return Err(format!(
            "import worker failed ({status:?}):\n{}",
            std::fs::read_to_string(log_path).unwrap()
        ));
    }
    assert!(
        !root.join("provider-launched").exists(),
        "a provider was launched"
    );
    Ok(read_json(&root.join("cases.json")))
}

async fn import_case(root: &Path, contract: &Value, case: &Value, fault: &str) -> Value {
    let store = Store::open(&root.join("store.db")).await.unwrap();
    let registry = Arc::new(SettingsRegistry::load(root.join("settings.toml")).unwrap());
    let defaults = &contract["destinationDefaults"];
    let mut enabled = contract["enabledProviders"].clone();
    enabled["codex"] = case["codexEnabled"].clone();
    let bin = PathBuf::from(std::env::var_os("HOME").unwrap()).join(".augment/bin");
    registry
        .apply(&[
            ("model.defaultProvider".into(), defaults["provider"].clone()),
            ("model.default".into(), defaults["model"].clone()),
            (
                "model.defaultReasoningEffort".into(),
                defaults["reasoningEffort"].clone(),
            ),
            ("providers.enabled".into(), enabled),
            (
                "providers.paths".into(),
                json!({"auggie": bin.join("auggie"), "codex": bin.join("codex-acp")}),
            ),
        ])
        .unwrap();
    let services = Services::new(store.clone())
        .with_settings_registry(registry)
        .with_models_cache_dir(root)
        .with_workspaces_root(root.join("workspaces"))
        .with_assets_root(root.join("assets"));
    for (provider, models) in contract["models"].as_object().unwrap() {
        let source = crate::model_catalog::source_for(provider).unwrap();
        services.models_catalog.store_for_test(
            provider,
            &(source.version_key)(),
            models.as_array().unwrap().clone(),
        );
        assert_eq!(
            services.cached_models().fresh_catalog(provider).as_ref(),
            models.as_array()
        );
    }
    let workspace = WorkspaceId::from("ws-import-contract");
    let agent = AgentId::from("agent-import-contract");
    let timestamp = "2026-01-01T00:00:00Z";
    let manifest = json!({
        "formatVersion": 1, "creatingIntentdVersion": env!("CARGO_PKG_VERSION"),
        "workspaceId": workspace.0, "createdAt": timestamp,
        "tables": [], "assets": [], "attachments": [],
        "git": {"hasRepository": false, "dirtyFiles": [], "sandboxBranches": [], "submodules": []}
    });
    let mut session_row = json!({
        "id": agent.0, "workspace_id": workspace.0, "name": "Transfer contract",
        "status": "idle", "is_active": 0, "created_at": timestamp, "updated_at": timestamp,
        "effort_levels": "[\"source-only-effort\"]"
    });
    session_row
        .as_object_mut()
        .unwrap()
        .extend(case["input"].as_object().unwrap().clone());
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default();
    zip.start_file("manifest.json", options).unwrap();
    write!(zip, "{manifest}").unwrap();
    for (table, row) in [
        (
            "workspace",
            json!({"id": workspace.0, "title": "Transfer contract", "branch": "main", "status": "Active", "created_at": timestamp, "updated_at": timestamp}),
        ),
        ("agent_session", session_row),
    ] {
        zip.start_file(format!("rows/{table}.jsonl"), options)
            .unwrap();
        writeln!(zip, "{row}").unwrap();
    }
    let archive = zip.finish().unwrap().into_inner();
    let digest = sha256(&archive);
    let begin = services
        .workspace_import_begin(manifest, archive.len() as u64, digest)
        .await
        .unwrap();
    let id = begin["importId"].as_str().unwrap().to_string();
    services
        .workspace_import_chunk(
            id.clone(),
            0,
            base64::engine::general_purpose::STANDARD.encode(archive),
        )
        .await
        .unwrap();
    services.workspace_import_commit(id).await.unwrap();

    if fault == "legacy-alias" && case["sourceProvider"] == "acp" {
        // Controlled equivalent of a0d0752: leave the active alias in the DB
        // after import. Public getSession must then fail the SAME assertions
        // as the positive exporter, independently of golden/provenance checks.
        sqlx::query("UPDATE agent_session SET provider = 'acp' WHERE id = ?")
            .bind(&agent.0)
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    let stored_session = store.get_agent_session(&agent).await.unwrap();
    let (provider, model, effort) = crate::agent_manager::imported_spawn_selection_for_test(
        &stored_session,
        &services.effective_settings(),
    );
    let public = services.agent_get_session(agent, None).await.unwrap();
    let rows = store.transfer_export_rows(&workspace).await.unwrap();
    let raw = &rows
        .iter()
        .find(|(table, _)| table == "agent_session")
        .unwrap()
        .1[0];
    let persisted: serde_json::Map<String, Value> = [
        "provider",
        "model",
        "reasoning_effort",
        "last_turn_provider",
        "last_turn_model",
    ]
    .into_iter()
    .map(|key| (key.into(), raw[key].clone()))
    .collect();
    let observed = json!({
        "id": case["id"], "session": serde_json::to_value(public).unwrap(),
        "persisted": persisted,
        "firstTurn": {"provider": provider, "model": model, "reasoningEffort": effort}
    });
    assert_observation(
        case,
        &contract["expectations"][case["expectation"].as_str().unwrap()],
        &observed,
    );
    store.close().await;
    observed
}

fn assert_observation(case: &Value, expectation: &Value, observed: &Value) {
    for (key, expected) in expectation["selection"].as_object().unwrap() {
        if expected.is_null() {
            assert!(
                observed["session"].get(key).is_none(),
                "{}: Auto must omit {key}",
                case["id"]
            );
        } else {
            assert_eq!(
                &observed["session"][key], expected,
                "{}: public {key}",
                case["id"]
            );
        }
    }
    for group in ["expectedPersistedSelection", "expectedRawHistory"] {
        for (key, expected) in case[group].as_object().unwrap() {
            assert_eq!(
                &observed["persisted"][key], expected,
                "{}: raw {key}",
                case["id"]
            );
        }
    }
    assert_eq!(
        observed["firstTurn"], expectation["firstTurn"],
        "{}: first turn",
        case["id"]
    );
    for key in [
        "attentionRequestKind",
        "attentionRequestReason",
        "effortLevels",
    ] {
        assert!(
            observed["session"].get(key).is_none(),
            "{}: unexpected {key}",
            case["id"]
        );
    }
}

// Use the contract owner's serializer/normalizer, including its complete-row
// freshness comparison. No duplicated hashing or broad recursive scrubbing.
const VERIFY: &str = r#"
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { isDeepStrictEqual } from 'node:util';
const [root, raw, component, mode, output, compiledRevision, compiledGenerator] = process.argv.slice(1);
const { assertContract, assertGenerated, assertFresh, normalizeCases, hashJson } = await import(
  pathToFileURL(path.resolve(root, '../../../../scripts/check-transfer-selection-contract.mjs')));
const read = p => JSON.parse(fs.readFileSync(p, 'utf8'));
const contract = read(path.join(root, 'contract.json'));
assertContract(contract);
const cases = normalizeCases(read(raw));
const goldenPath = path.join(root, 'public-sessions.json');
if (mode === 'check') {
  const golden = read(goldenPath);
  assertGenerated(contract, golden);
  if (!isDeepStrictEqual(golden.cases, cases)) throw new Error('stale public-sessions.json: freshly imported full responses differ');
} else {
  const git = (...args) => execFileSync('git', ['-C', component, ...args], {encoding: 'utf8'}).trim();
  const revision = git('rev-parse', 'HEAD');
  if (git('status', '--porcelain', '--untracked-files=all')) throw new Error('export requires clean committed intentd source');
  if (compiledRevision !== revision) throw new Error('Cargo build revision differs: use scripts/test-transfer-selection-contract.sh');
  const generator = createHash('sha256').update(fs.readFileSync(path.join(component, 'crates/intent-services/src/transfer_selection_contract.rs'))).digest('hex');
  if (generator !== compiledGenerator) throw new Error('compiled generator differs from source');
  const artifact = {formatVersion: 1, normalizationVersion: 1, provenance: {
    kind: 'intentd-public-import', generator: 'intent-services/transfer-selection-contract',
    intentdRevision: revision, intentdDirty: false, generatorSha256: generator,
    contractSha256: hashJson(contract), payloadSha256: hashJson(cases),
  }, cases};
  assertGenerated(contract, artifact, {intentdRevision: revision, generatorSha256: generator});
  if (mode !== 'regenerate') assertFresh(contract, read(goldenPath), artifact);
  const destination = mode === 'regenerate' ? goldenPath : output;
  // An export cannot overwrite any existing file, especially a linked golden.
  fs.writeFileSync(destination, JSON.stringify(artifact, null, 2) + '\n', {flag: mode === 'regenerate' ? 'w' : 'wx'});
  console.log(`emitted ${cases.length} public imports from ${revision}: ${destination}`);
}
"#;

fn verify(root: &Path, rows: &Path, mode: &str, output: &Path) -> std::process::Output {
    Command::new("node")
        .env_remove("NODE_OPTIONS")
        .args(["--input-type=module", "-e", VERIFY])
        .arg(root)
        .arg(rows)
        .arg(component_root())
        .arg(mode)
        .arg(output)
        .arg(BUILD_REVISION.unwrap_or(""))
        .arg(sha256(SOURCE))
        .output()
        .expect("Node is required to validate the shared transfer-selection contract")
}

#[test]
fn public_import_matches_contract() {
    if let Some(root) = std::env::var_os("TRANSFER_SELECTION_WORKER") {
        let root = PathBuf::from(root);
        let contract = read_json(&root.join("contract.json"));
        let fault = std::env::var("TRANSFER_SELECTION_FAULT").unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let cases = runtime.block_on(intent_core::with_caller(Caller::Daemon, async {
            let mut observations = Vec::new();
            for case in contract["cases"].as_array().unwrap() {
                let temporary = test_tempdir("transfer-selection-case-");
                observations.push(import_case(temporary.path(), &contract, case, &fault).await);
            }
            Value::Array(observations)
        }));
        write_json(&root.join("cases.json"), &cases);
        return;
    }
    let fixtures = fixture_root();
    let temporary = test_tempdir("transfer-selection-run-");
    let rows = run_imports(
        temporary.path(),
        &read_json(&fixtures.join("contract.json")),
        "",
    )
    .unwrap();
    write_json(&temporary.path().join("fresh.json"), &rows);
    let output = std::env::var_os("TRANSFER_SELECTION_OUTPUT").map(PathBuf::from);
    let regenerate = std::env::var("TRANSFER_SELECTION_REGENERATE").is_ok_and(|value| value == "1");
    assert!(
        !regenerate || output.is_none(),
        "choose regeneration or export"
    );
    let mode = if regenerate {
        "regenerate"
    } else if output.is_some() {
        "export"
    } else {
        "check"
    };
    let result = verify(
        &fixtures,
        &temporary.path().join("fresh.json"),
        mode,
        output.as_deref().unwrap_or(Path::new("")),
    );
    assert!(
        result.status.success(),
        "contract validation failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    println!("{}", String::from_utf8_lossy(&result.stdout));
}

#[test]
fn rejects_legacy_alias_regression_and_cleans_failure() {
    let root;
    let failure;
    {
        let temporary = test_tempdir("transfer-selection-regression-");
        root = temporary.path().to_path_buf();
        failure = run_imports(
            &root,
            &read_json(&fixture_root().join("contract.json")),
            "legacy-alias",
        )
        .unwrap_err();
    }
    assert!(
        failure.contains("acp:direct:codex=true") && failure.contains("public provider"),
        "{failure}"
    );
    assert!(
        !root.exists(),
        "failed import temporary root leaked: {}",
        root.display()
    );
}

#[test]
fn full_response_drift_is_rejected_without_rewriting_golden() {
    let fixtures = fixture_root();
    let golden = std::fs::read(fixtures.join("public-sessions.json")).unwrap();
    let root;
    {
        let temporary = test_tempdir("transfer-selection-drift-");
        root = temporary.path().to_path_buf();
        let mut cases = read_json(&fixtures.join("public-sessions.json"))["cases"].clone();
        cases[0]["session"]["unexpectedPublicField"] = json!(true);
        let path = root.join("drift.json");
        write_json(&path, &cases);
        let result = verify(&fixtures, &path, "check", Path::new(""));
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("stale public-sessions.json"));
    }
    assert!(!root.exists(), "comparison temporary root leaked");
    assert_eq!(
        std::fs::read(fixtures.join("public-sessions.json")).unwrap(),
        golden
    );
}
