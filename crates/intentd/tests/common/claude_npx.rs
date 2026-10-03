//! Isolated canonical Claude and pinned-package dispatch for WSS fixtures.

use std::path::{Path, PathBuf};

pub fn in_subprocess(test_name: &str) -> bool {
    use intentd_test_support::GuardedChild;
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    if std::env::var("INTENTD_CLAUDE_NPX_TEST").as_deref() == Ok(test_name) {
        return false;
    }
    let Some(node) = intent_providers::resolve_on_path("node") else {
        eprintln!("skipping {test_name}: node not on PATH");
        return true;
    };
    let root = super::test_tempdir("itd-claude-npx-process-");
    let bin = root.path().join("bin");
    let home = root.path().join("home");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    for (name, body) in [
        (
            "claude",
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$MOCK_CLAUDE_ROOT/cli-calls\"\n[ \"$#\" = 1 ] && [ \"$1\" = --version ] || exit 91\nprintf 'claude 1.0.0\\n'\n".to_owned(),
        ),
        ("node", "#!/bin/sh\nexec \"$MOCK_CLAUDE_NODE\" \"$@\"\n".to_owned()),
        (
            "npx",
            format!(
                "#!/bin/sh\nif [ \"$#\" = 1 ] && [ \"$1\" = --version ]; then echo 10.9.2; exit 0; fi\n[ \"$#\" = 3 ] && [ \"$1\" = --workspaces=false ] && [ \"$2\" = -y ] && [ \"$3\" = '{}' ] || exit 92\n[ \"$CLAUDE_CODE_EXECUTABLE\" = \"$MOCK_CLAUDE_ROOT/bin/claude\" ] || exit 93\nprintf '%s\\n' \"$CLAUDE_CODE_EXECUTABLE\" >> \"$MOCK_CLAUDE_ROOT/adapter-calls\"\nIFS= read -r adapter < \"$MOCK_CLAUDE_ROOT/adapter-path\"\nexec \"$adapter\" \"$@\"\n",
                intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE
            ),
        ),
        (
            "legacy-adapter",
            "#!/bin/sh\nprintf 'unexpected legacy launch\\n' >> \"$MOCK_CLAUDE_ROOT/legacy-calls\"\nexit 94\n".to_owned(),
        ),
    ] {
        let path = bin.join(name);
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let log_path = root.path().join("test.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env_clear()
        .env("HOME", &home)
        .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
        .env("SHELL", "/bin/sh")
        .env("INTENTD_CLAUDE_NPX_TEST", test_name)
        .env("MOCK_CLAUDE_ROOT", root.path())
        .env("MOCK_CLAUDE_NODE", node)
        .env("CLAUDE_CODE_EXECUTABLE", bin.join("legacy-adapter"))
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log));
    if let Some(multiplier) = std::env::var_os("INTENTD_TEST_TIMEOUT_MULTIPLIER") {
        cmd.env("INTENTD_TEST_TIMEOUT_MULTIPLIER", multiplier);
    }
    let mut child = GuardedChild::spawn(&mut cmd).unwrap();
    let status = child
        .wait_with_timeout(super::test_timeout(Duration::from_secs(180)))
        .unwrap()
        .expect("isolated Claude WSS test timed out");
    assert!(
        status.success(),
        "{test_name}: {}",
        std::fs::read_to_string(log_path).unwrap()
    );
    assert!(!root.path().join("legacy-calls").exists());
    let calls = std::fs::read_to_string(root.path().join("cli-calls")).unwrap();
    assert!(
        !calls.is_empty() && calls.lines().all(|line| line == "--version"),
        "{calls}"
    );
    let launches = std::fs::read_to_string(root.path().join("adapter-calls")).unwrap();
    assert!(!launches.is_empty());
    assert!(
        launches
            .lines()
            .all(|line| Path::new(line) == bin.join("claude")),
        "{launches}"
    );
    true
}

pub fn select_adapter(adapter: &Path) {
    let root = PathBuf::from(std::env::var_os("MOCK_CLAUDE_ROOT").unwrap());
    std::fs::write(
        root.join("adapter-path"),
        format!("{}\n", adapter.display()),
    )
    .unwrap();
}

pub fn legacy_override() -> PathBuf {
    PathBuf::from(std::env::var_os("MOCK_CLAUDE_ROOT").unwrap()).join("bin/legacy-adapter")
}
