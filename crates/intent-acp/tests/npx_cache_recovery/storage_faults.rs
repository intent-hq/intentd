//! Fault the real repair child, leaving its real npm config subprocess untouched.

use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::time::Duration;

use intent_acp::npx_cache::{repair_npx_cache, CommandEnvironment};
use intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE;
use serde_json::{json, Value};

use super::{turn, Fixture};

const COMPETING_MANIFEST: &[u8] =
    b"{\n  \"private\": true, \"competitor\": \"preserve exact bytes\"\n}\n";

const STORAGE_PRELOAD: &str = r"
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const record = event => fs.appendFileSync(process.env.NPX_STORAGE_EVENTS,
  JSON.stringify({...event, pid: process.pid, argv: process.argv}) + '\n');
if (process.argv.slice(2).join(' ') === 'config get cache --workspaces=false') {
  record({stage: 'npm-config', code: 'REAL_NPM'});
}
if (process.argv[1] === process.env.NPX_STORAGE_SPEC &&
    process.argv[2] === process.env.NPX_STORAGE_NPM) {
  const mode = process.env.NPX_STORAGE_MODE;
  const entry = process.env.NPX_STORAGE_ENTRY;
  const manifest = path.join(entry, 'package.json');
  const original = {};
  for (const name of ['openSync', 'writeFileSync', 'writeSync', 'fsyncSync',
                       'closeSync', 'linkSync', 'unlinkSync']) original[name] = fs[name];
  let candidate;
  let output;
  let injected = false;
  const inject = (stage, code, details = {}) => {
    injected = true;
    record({stage, code, injection: true, candidate, ...details});
  };
  const fail = code => { const error = new Error('injected ' + code); error.code = code; throw error; };
  fs.openSync = function(file, flags, ...args) {
    if (path.dirname(String(file)) === entry &&
        path.basename(String(file)).startsWith('.intent-manifest-') && flags === 'wx') {
      candidate = String(file);
      if (mode === 'open-erofs') {
        inject('candidate-open', 'EROFS');
        fail('EROFS');
      }
      output = original.openSync.call(this, file, flags, ...args);
      return output;
    }
    return original.openSync.call(this, file, flags, ...args);
  };
  fs.writeFileSync = function(file, data, ...args) {
    if (!injected && file === output && mode === 'partial-enospc') {
      const bytes = Buffer.from(data);
      const length = Math.min(11, bytes.length - 1);
      const written = original.writeSync.call(fs, file, bytes, 0, length, null);
      inject('candidate-partial-write', 'ENOSPC', {
        written, intended: bytes.length, partialHex: fs.readFileSync(candidate).toString('hex')
      });
      fail('ENOSPC');
    }
    return original.writeFileSync.call(this, file, data, ...args);
  };
  fs.fsyncSync = function(fd) {
    const result = original.fsyncSync.call(this, fd);
    if (!injected && fd === output && (mode === 'delete-entry' || mode === 'replace-entry')) {
      const durableBytes = fs.readFileSync(candidate).toString('hex');
      if (mode === 'delete-entry') {
        fs.rmSync(entry, {recursive: true});
        inject('after-fsync-before-revalidation', 'ENTRY_DELETED', {durableBytes});
      } else {
        fs.renameSync(entry, process.env.NPX_STORAGE_DISPLACED);
        fs.mkdirSync(entry);
        original.writeFileSync.call(fs, path.join(entry, 'replacement-sentinel'), 'replacement must survive');
        inject('after-fsync-before-revalidation', 'ENTRY_REPLACED', {durableBytes});
      }
    }
    return result;
  };
  fs.linkSync = function(source, destination) {
    if (String(source) === candidate && String(destination) === manifest) {
      if (mode === 'link-eacces' || mode === 'link-enotsup') {
        const code = mode === 'link-eacces' ? 'EACCES' : 'ENOTSUP';
        inject('manifest-link', code);
        fail(code);
      }
      if (mode === 'competing-manifest') {
        original.writeFileSync.call(fs, manifest,
          Buffer.from(process.env.NPX_STORAGE_COMPETING_HEX, 'hex'), {flag: 'wx'});
        inject('before-manifest-link', 'EEXIST', {manifestHex: fs.readFileSync(manifest).toString('hex')});
      }
      record({stage: 'publication-attempt', code: 'LINK', source, destination});
    }
    try {
      return original.linkSync.call(this, source, destination);
    } catch (error) {
      record({stage: 'publication-error', code: error.code});
      throw error;
    }
  };
  fs.closeSync = function(fd) {
    const candidateClosed = fd === output;
    const result = original.closeSync.call(this, fd);
    if (candidateClosed) {
      output = undefined;
      record({stage: 'candidate-close', code: 'CLOSED'});
    }
    return result;
  };
  fs.unlinkSync = function(file) {
    if (String(file) !== candidate) return original.unlinkSync.call(this, file);
    const result = original.unlinkSync.call(this, file);
    record({stage: 'candidate-finally', code: 'UNLINKED', candidate});
    return result;
  };
}
";

fn install_fault(fixture: &mut Fixture, mode: &str) -> PathBuf {
    let preload = fixture.root.path().join("storage fault preload.cjs");
    let events = fixture.root.path().join("storage-events.ndjson");
    fs::write(&preload, STORAGE_PRELOAD).unwrap();
    for (key, value) in [
        (
            "NODE_OPTIONS",
            format!("--require={}", serde_json::to_string(&preload).unwrap()),
        ),
        ("NPX_STORAGE_MODE", mode.to_owned()),
        ("NPX_STORAGE_EVENTS", events.to_string_lossy().into_owned()),
        ("NPX_STORAGE_SPEC", CLAUDE_AGENT_ACP_NPX_PACKAGE.to_owned()),
        (
            "NPX_STORAGE_NPM",
            fixture
                .npx
                .with_file_name("npm")
                .to_string_lossy()
                .into_owned(),
        ),
        (
            "NPX_STORAGE_ENTRY",
            fixture.entry.to_string_lossy().into_owned(),
        ),
        (
            "NPX_STORAGE_DISPLACED",
            fixture
                .root
                .path()
                .join("displaced-entry")
                .to_string_lossy()
                .into_owned(),
        ),
        ("NPX_STORAGE_COMPETING_HEX", hex(COMPETING_MANIFEST)),
    ] {
        fixture.env.insert(key.into(), value);
    }
    events
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

// Snapshot only this test's private entry so actual eviction can be undone for
// the later real-npm/ACP proof. Preserve symlinks rather than following them.
fn copy_entry(source: &Path, destination: &Path) {
    fs::create_dir(destination).unwrap();
    for item in fs::read_dir(source).unwrap() {
        let item = item.unwrap();
        let target = destination.join(item.file_name());
        let kind = item.file_type().unwrap();
        if kind.is_symlink() {
            symlink(fs::read_link(item.path()).unwrap(), target).unwrap();
        } else if kind.is_dir() {
            copy_entry(&item.path(), &target);
        } else {
            fs::copy(item.path(), &target).unwrap();
            fs::set_permissions(target, item.metadata().unwrap().permissions()).unwrap();
        }
    }
}

#[track_caller]
fn assert_no_candidates(entry: &Path) {
    let candidates: Vec<_> = fs::read_dir(entry)
        .unwrap()
        .map(|item| item.unwrap().file_name())
        .filter(|name| name.to_string_lossy().starts_with(".intent-manifest-"))
        .collect();
    assert!(
        candidates.is_empty(),
        "temporary candidates remain: {candidates:?}"
    );
}

async fn repair(fixture: &Fixture) -> bool {
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(15)),
        repair_npx_cache(
            &fixture.command(),
            CLAUDE_AGENT_ACP_NPX_PACKAGE,
            CommandEnvironment::Complete,
            (),
        ),
    )
    .await
    .expect("real cache repair completes within the scaled hang guard")
}

async fn storage_fault(mode: &str, stage: &str, code: &str) {
    let Some(mut fixture) = Fixture::new() else {
        return;
    };
    fixture.prime_registry_metadata().await;
    let backup = fixture.root.path().join("entry-backup");
    let eviction = matches!(mode, "delete-entry" | "replace-entry");
    if eviction {
        copy_entry(&fixture.entry, &backup);
    }
    let outside = fixture.root.path().join("outside-entry-sentinel");
    fs::write(&outside, b"outside selected cache entry").unwrap();
    let npmrc = fs::read(&fixture.npmrc).unwrap();
    let events_path = install_fault(&mut fixture, mode);
    let failed = repair(&fixture).await;
    assert!(!failed, "injected {mode} must make repair advisory false");
    let events: Vec<Value> = fs::read_to_string(events_path)
        .expect("preload must record real npm configuration and the injection")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let injections: Vec<_> = events
        .iter()
        .filter(|row| row["injection"] == true)
        .collect();
    assert_eq!(
        injections.len(),
        1,
        "exactly one storage fault must fire: {events:?}"
    );
    let injection = injections[0];
    assert_eq!(injection["stage"], stage);
    assert_eq!(injection["code"], code);
    assert_eq!(injection["argv"][1], CLAUDE_AGENT_ACP_NPX_PACKAGE);
    assert_eq!(
        injection["argv"][2],
        json!(fixture.npx.with_file_name("npm"))
    );
    assert!(
        events.iter().any(|row| row["stage"] == "npm-config"),
        "real npm config must run: {events:?}"
    );
    assert_eq!(fs::read(&outside).unwrap(), b"outside selected cache entry");
    assert_eq!(fs::read(&fixture.npmrc).unwrap(), npmrc);
    assert_eq!(
        fs::read(fixture.cache.join("_npx/unrelated-entry/keep.txt")).unwrap(),
        b"other package"
    );

    if mode == "delete-entry" {
        assert!(
            !fixture.entry.exists(),
            "repair must not recreate an evicted entry"
        );
    } else if mode == "replace-entry" {
        assert!(
            !fixture.manifest().exists(),
            "repair must not publish into a replacement entry"
        );
        assert_eq!(
            fs::read(fixture.entry.join("replacement-sentinel")).unwrap(),
            b"replacement must survive"
        );
        assert_eq!(
            fs::read_dir(&fixture.entry).unwrap().count(),
            1,
            "replacement must contain only its sentinel"
        );
        let displaced = fixture.root.path().join("displaced-entry");
        assert!(!displaced.join("package.json").exists());
        let relative_package = fixture.package.strip_prefix(&fixture.entry).unwrap();
        assert_eq!(
            fs::read(displaced.join(relative_package).join("fixture.cjs")).unwrap(),
            super::ACP_FIXTURE.as_bytes()
        );
        assert_eq!(
            fs::read(displaced.join("keep-me.txt")).unwrap(),
            b"unrelated cache entry content"
        );
        assert_eq!(
            fs::read(
                displaced.join(
                    injection["candidate"]
                        .as_str()
                        .unwrap()
                        .rsplit('/')
                        .next()
                        .unwrap()
                )
            )
            .unwrap(),
            hex_to_bytes(injection["durableBytes"].as_str().unwrap())
        );
    } else {
        if mode == "competing-manifest" {
            assert_eq!(fs::read(fixture.manifest()).unwrap(), COMPETING_MANIFEST);
            assert!(
                events
                    .iter()
                    .any(|row| row["stage"] == "publication-error" && row["code"] == "EEXIST"),
                "real hard-link must reject the competing manifest"
            );
            assert!(events
                .iter()
                .any(|row| row["stage"] == "publication-attempt"));
        } else {
            assert!(
                !fixture.manifest().exists(),
                "failed writes must not publish a root manifest"
            );
        }
        assert_no_candidates(&fixture.entry);
        fixture.assert_unrelated_preserved();
        if mode != "open-erofs" {
            assert!(
                events.iter().any(|row| row["stage"] == "candidate-finally"),
                "finally must unlink ordinary candidates: {events:?}"
            );
            assert!(events.iter().any(|row| row["stage"] == "candidate-close"));
        }
    }
    if eviction {
        assert!(
            !events
                .iter()
                .any(|row| row["stage"] == "publication-attempt"),
            "changed entry must fail revalidation before link"
        );
        let durable: Value =
            serde_json::from_slice(&hex_to_bytes(injection["durableBytes"].as_str().unwrap()))
                .unwrap();
        assert!(
            durable["dependencies"].is_object(),
            "eviction must happen after a real write and fsync"
        );
    }
    if mode == "partial-enospc" {
        let written = injection["written"].as_u64().unwrap();
        assert!(written > 0 && written < injection["intended"].as_u64().unwrap());
        assert_eq!(
            injection["partialHex"].as_str().unwrap().len(),
            usize::try_from(written).unwrap() * 2
        );
    }

    // Clear the fault only after observing its effects. Restore deliberately
    // evicted test data; ordinary failures recover against the unchanged entry.
    fixture.env.remove("NODE_OPTIONS");
    if eviction {
        if fixture.entry.exists() {
            fs::remove_dir_all(&fixture.entry).unwrap();
        }
        fs::rename(backup, &fixture.entry).unwrap();
    } else if mode == "competing-manifest" {
        fs::remove_file(fixture.manifest()).unwrap();
    }
    let subsequent = repair(&fixture).await;
    assert!(
        subsequent,
        "a later real npm repair must succeed after {mode}"
    );
    let manifest = fixture.assert_repaired();
    assert_no_candidates(&fixture.entry);
    let normal = turn(&fixture, &format!("{mode}-normal"), false).await;
    let prepared = turn(&fixture, &format!("{mode}-prepared"), true).await;
    fixture.assert_unrelated_preserved();
    assert_eq!(fs::read(&outside).unwrap(), b"outside selected cache entry");
    fixture.report(
        &format!("storage-{mode}"),
        &json!({
            "failedRepairResult": failed, "events": events,
            "unrelatedSentinelsPreserved": true, "changedEntryNotPublished": eviction,
            "ordinaryCandidateCleanup": !eviction, "subsequentRealNpmRepair": subsequent,
            "manifest": manifest, "normal": normal, "prepared": prepared
        }),
    );
}

fn hex_to_bytes(encoded: &str) -> Vec<u8> {
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[tokio::test]
async fn npx_cache_recovery_candidate_open_erofs() {
    storage_fault("open-erofs", "candidate-open", "EROFS").await;
}

#[tokio::test]
async fn npx_cache_recovery_partial_write_enospc() {
    storage_fault("partial-enospc", "candidate-partial-write", "ENOSPC").await;
}

#[tokio::test]
async fn npx_cache_recovery_manifest_link_eacces() {
    storage_fault("link-eacces", "manifest-link", "EACCES").await;
}

#[tokio::test]
async fn npx_cache_recovery_manifest_link_enotsup() {
    storage_fault("link-enotsup", "manifest-link", "ENOTSUP").await;
}

#[tokio::test]
async fn npx_cache_recovery_competing_manifest_stays_byte_identical() {
    storage_fault("competing-manifest", "before-manifest-link", "EEXIST").await;
}

#[tokio::test]
async fn npx_cache_recovery_eviction_after_fsync_does_not_recreate_entry() {
    storage_fault(
        "delete-entry",
        "after-fsync-before-revalidation",
        "ENTRY_DELETED",
    )
    .await;
}

#[tokio::test]
async fn npx_cache_recovery_replacement_after_fsync_does_not_publish() {
    storage_fault(
        "replace-entry",
        "after-fsync-before-revalidation",
        "ENTRY_REPLACED",
    )
    .await;
}
