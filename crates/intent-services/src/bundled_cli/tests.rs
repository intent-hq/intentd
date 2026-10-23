#![cfg(unix)]

use super::*;
use std::os::unix::fs::PermissionsExt;

fn bundled(version: &str) -> BundledCli {
    BundledCli::new("@openai/codex", version).unwrap()
}

/// A stand-in npm that records each call and installs a `codex` printing the
/// requested version. `behavior` runs first and may exit early.
fn fake_npm(dir: &Path, behavior: &str) -> PathBuf {
    let npm = dir.join("npm");
    let log = dir.join("npm.log");
    std::fs::write(
        &npm,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> '{log}'
[ "$1" = install ] || exit 90
prefix=""; spec=""
while [ "$#" -gt 0 ]; do
  case "$1" in --prefix) shift; prefix="$1";; --*) ;; *) spec="$1";; esac
  shift
done
[ "$(pwd -P)" = "$(cd "$prefix" && pwd -P)" ] || exit 91
{behavior}
mkdir -p "$prefix/node_modules/.bin"
printf '#!/bin/sh\necho "codex-cli %s"\n' "${{spec##*@}}" > "$prefix/node_modules/.bin/codex"
chmod 755 "$prefix/node_modules/.bin/codex"
"#,
            log = log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&npm, std::fs::Permissions::from_mode(0o755)).unwrap();
    npm
}

fn calls(dir: &Path) -> Vec<String> {
    std::fs::read_to_string(dir.join("npm.log"))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn path_env() -> String {
    std::env::var("PATH").unwrap_or_default()
}

const TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test]
async fn first_use_installs_the_exact_spec_and_later_uses_skip_npm() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let npm = fake_npm(temp.path(), "");
    let pin = bundled("0.160.1");
    assert!(existing(&root, InstalledCli::Codex, &pin).is_none());

    let runtime = ensure_in(&root, &npm, &path_env(), InstalledCli::Codex, &pin, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(runtime.cli(), InstalledCli::Codex);
    assert!(runtime.path().starts_with(version_dir(&root, &pin)));
    let output = std::process::Command::new(runtime.path()).output().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        // cli-fixture-version: allow — installer tests use an explicit pin, not the adapter metadata
        "codex-cli 0.160.1"
    );
    let recorded = calls(temp.path());
    assert_eq!(recorded.len(), 1);
    assert!(
        recorded[0].ends_with("@openai/codex@0.160.1"),
        "{recorded:?}"
    );

    let again = ensure_in(&root, &npm, &path_env(), InstalledCli::Codex, &pin, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(again, runtime);
    assert_eq!(existing(&root, InstalledCli::Codex, &pin), Some(runtime));
    assert_eq!(calls(temp.path()).len(), 1, "a complete install is reused");
}

#[tokio::test]
async fn concurrent_callers_share_one_install() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let npm = fake_npm(temp.path(), "sleep 1");
    let pin = bundled("0.160.1");
    let path = path_env();
    let (a, b) = tokio::join!(
        ensure_in(&root, &npm, &path, InstalledCli::Codex, &pin, TIMEOUT),
        ensure_in(&root, &npm, &path, InstalledCli::Codex, &pin, TIMEOUT),
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(calls(temp.path()).len(), 1);
}

#[tokio::test]
async fn failures_leave_no_trusted_partial_install() {
    for (behavior, expected) in [
        ("echo 'npm ERR! 404 Not Found' >&2; exit 1", "404 Not Found"),
        (
            "mkdir -p \"$prefix/node_modules\"; exit 0",
            "did not provide a codex executable",
        ),
        ("sleep 5", "timed out"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let npm = fake_npm(temp.path(), behavior);
        let pin = bundled("0.160.1");
        let error = ensure_in(
            &root,
            &npm,
            &path_env(),
            InstalledCli::Codex,
            &pin,
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        assert!(error.contains(expected), "{behavior}: {error}");
        assert!(error.contains("@openai/codex@0.160.1"), "{error}");
        assert!(existing(&root, InstalledCli::Codex, &pin).is_none());
        assert!(!version_dir(&root, &pin).exists(), "{behavior}");
    }
}

#[tokio::test]
async fn incomplete_or_mismatched_copies_are_reinstalled() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let npm = fake_npm(temp.path(), "");
    let pin = bundled("0.160.1");
    let dir = version_dir(&root, &pin);
    let bin = dir.join("node_modules/.bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::write(bin.join("codex"), "#!/bin/sh\necho stale\n").unwrap();
    std::fs::set_permissions(bin.join("codex"), std::fs::Permissions::from_mode(0o755)).unwrap();
    // No marker: never trusted.
    assert!(existing(&root, InstalledCli::Codex, &pin).is_none());
    std::fs::write(dir.join(READY), "@openai/codex@0.159.0").unwrap();
    assert!(existing(&root, InstalledCli::Codex, &pin).is_none());

    let runtime = ensure_in(&root, &npm, &path_env(), InstalledCli::Codex, &pin, TIMEOUT)
        .await
        .unwrap();
    let output = std::process::Command::new(runtime.path()).output().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        // cli-fixture-version: allow — installer tests use an explicit pin, not the adapter metadata
        "codex-cli 0.160.1"
    );
    assert_eq!(calls(temp.path()).len(), 1);
}

#[tokio::test]
async fn new_pins_keep_recent_versions_and_drop_abandoned_staging() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let npm = fake_npm(temp.path(), "");
    let pins = ["0.158.0", "0.159.0", "0.160.0", "0.160.1"].map(bundled);
    ensure_in(
        &root,
        &npm,
        &path_env(),
        InstalledCli::Codex,
        &pins[0],
        TIMEOUT,
    )
    .await
    .unwrap();
    let parent = version_dir(&root, &pins[0]).parent().unwrap().to_owned();
    let abandoned = parent.join(".install-abandoned");
    std::fs::create_dir_all(&abandoned).unwrap();
    for pin in &pins[1..] {
        ensure_in(&root, &npm, &path_env(), InstalledCli::Codex, pin, TIMEOUT)
            .await
            .unwrap();
    }
    // Another daemon on an older pin keeps its runtime; only the oldest goes.
    assert!(existing(&root, InstalledCli::Codex, &pins[0]).is_none());
    for pin in &pins[1..] {
        assert!(
            existing(&root, InstalledCli::Codex, pin).is_some(),
            "{}",
            pin.spec()
        );
    }
    assert!(!abandoned.exists());
}

#[tokio::test]
async fn installs_wait_for_another_process_holding_the_install_lock() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let npm = fake_npm(temp.path(), "");
    let pin = bundled("0.160.1");
    let parent = version_dir(&root, &pin).parent().unwrap().to_owned();
    std::fs::create_dir_all(&parent).unwrap();
    // Stands in for another daemon mid-install: an exclusive lock on the
    // same file, taken through a separate open file description.
    let other = nix::fcntl::Flock::lock(
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(parent.join(LOCK))
            .unwrap(),
        nix::fcntl::FlockArg::LockExclusive,
    )
    .map_err(|(_, errno)| errno)
    .unwrap();
    let path = path_env();
    let install = tokio::spawn({
        let (root, npm, pin) = (root.clone(), npm.clone(), pin.clone());
        async move { ensure_in(&root, &npm, &path, InstalledCli::Codex, &pin, TIMEOUT).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!install.is_finished(), "the install must wait for the lock");
    assert!(
        calls(temp.path()).is_empty(),
        "npm must not run while another process installs"
    );
    drop(other);
    install.await.unwrap().unwrap();
    assert_eq!(calls(temp.path()).len(), 1);
}

#[tokio::test]
async fn a_placeholder_cli_is_never_marked_complete() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    // Installs a `codex` that fails like an unfinished install-script placeholder.
    let npm = fake_npm(
        temp.path(),
        "mkdir -p \"$prefix/node_modules/.bin\"; printf '#!/bin/sh\\necho placeholder >&2\\nexit 1\\n' > \"$prefix/node_modules/.bin/codex\"; chmod 755 \"$prefix/node_modules/.bin/codex\"; exit 0",
    );
    let pin = bundled("0.160.1");
    let error = ensure_in(&root, &npm, &path_env(), InstalledCli::Codex, &pin, TIMEOUT)
        .await
        .unwrap_err();
    assert!(
        error.contains("not runnable") && error.contains("placeholder"),
        "{error}"
    );
    assert!(existing(&root, InstalledCli::Codex, &pin).is_none());
    assert!(!version_dir(&root, &pin).exists());
}

#[tokio::test]
async fn a_copy_another_daemon_completed_meanwhile_is_adopted() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let pin = bundled("0.160.1");
    let destination = version_dir(&root, &pin);
    // While this install runs, "another daemon" finishes the same version.
    let npm = fake_npm(
        temp.path(),
        &format!(
            // cli-fixture-version: allow — installer tests use an explicit pin, not the adapter metadata
            "d='{dest}'; mkdir -p \"$d/node_modules/.bin\"; printf '#!/bin/sh\\necho \"codex-cli 0.160.1\"\\n' > \"$d/node_modules/.bin/codex\"; chmod 755 \"$d/node_modules/.bin/codex\"; printf '@openai/codex@0.160.1' > \"$d/{READY}\"; echo other > \"$d/owner\"",
            dest = destination.display()
        ),
    );
    let runtime = ensure_in(&root, &npm, &path_env(), InstalledCli::Codex, &pin, TIMEOUT)
        .await
        .unwrap();
    assert!(runtime.path().starts_with(&destination));
    assert!(
        destination.join("owner").exists(),
        "the finished copy is kept, not replaced"
    );
}

#[test]
fn version_dirs_are_plain_relative_names() {
    let root = Path::new("/r");
    assert_eq!(
        version_dir(root, &bundled("0.160.1")),
        Path::new("/r/openai_codex/0.160.1")
    );
    let claude = BundledCli::new("@anthropic-ai/claude-code", "2.1.280").unwrap();
    assert!(BundledCli::new("x", "^2.1.0").is_none());
    assert!(BundledCli::new("x", "2.1.0-beta.1").is_none());
    assert_eq!(
        version_dir(root, &claude),
        Path::new("/r/anthropic-ai_claude-code/2.1.280")
    );
}

/// The nice value an agent process should start at: the daemon's own plus
/// the configured increment, capped at 19 (see `intent_acp::AgentPriority`).
#[cfg(target_os = "linux")]
fn demoted_nice() -> i32 {
    // SAFETY: plain syscall wrapper with no pointer arguments.
    let own = unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) };
    (own + intent_acp::AgentPriority::from_env().nice_increment).min(19)
}

/// Regression: the bundled-CLI `npm install` (the heaviest disk user an
/// agent launch triggers) ran at the daemon's own priority.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn npm_install_runs_at_reduced_priority() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let record = temp.path().join("npm.nice");
    let npm = fake_npm(
        temp.path(),
        &format!("cut -d' ' -f19 /proc/$$/stat > '{}'", record.display()),
    );
    let pin = bundled("0.160.1");
    ensure_in(&root, &npm, &path_env(), InstalledCli::Codex, &pin, TIMEOUT)
        .await
        .unwrap();
    let nice: i32 = std::fs::read_to_string(&record)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(nice >= demoted_nice(), "npm ran at nice {nice}");
}
