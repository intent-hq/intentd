//! Installed launcher help must work before installation without touching state.

use std::process::Command;

#[test]
fn lifecycle_help_is_available_without_installation_or_state_changes() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("not-created");
    for (args, usage) in [
        (vec!["--help"], "Usage: intentd"),
        (vec!["-h"], "Usage: intentd"),
        (vec!["start", "--help"], "Usage: intentd start"),
        (vec!["start", "-h"], "Usage: intentd start"),
        (vec!["restart", "--help"], "Usage: intentd restart"),
        (vec!["restart", "-h"], "Usage: intentd restart"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_intentd-sitter"))
            .args(&args)
            .env("INTENTD_DATA_DIR", &data_dir)
            .env_remove("INTENTD_CHANNEL")
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}: {output:?}");
        let help = String::from_utf8(output.stdout).unwrap();
        assert!(help.contains(usage), "{help}");
        assert!(help.contains("Windows"), "{help}");
        assert!(!data_dir.exists(), "help created daemon state for {args:?}");
    }
}

#[test]
fn root_help_lists_lifecycle_and_start_help_lists_serve_options() {
    let dir = tempfile::tempdir().unwrap();
    // Invalid persisted state must not prevent help or be rewritten by it.
    let sitter_dir = dir.path().join("sitter");
    std::fs::create_dir(&sitter_dir).unwrap();
    let state = sitter_dir.join("state.json");
    std::fs::write(&state, "invalid state").unwrap();
    for (args, expected) in [
        (
            vec!["--help"],
            vec![
                "start",
                "status",
                "stop",
                "restart",
                "serve",
                "--sitter-channel",
                "intentd help",
                "intentd help <COMMAND>",
            ],
        ),
        (
            vec!["start", "--help"],
            vec![
                "--mode",
                "--insecure",
                "--resume-all",
                "--specialists-dir",
                "INTENTD_DATA_DIR",
                "start.log",
                "60 seconds",
                "intentd help serve",
            ],
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_intentd-sitter"))
            .args(args)
            .env("INTENTD_DATA_DIR", dir.path())
            .env_remove("INTENTD_CHANNEL")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let help = String::from_utf8(output.stdout).unwrap();
        assert!(!help.contains("intentd serve --help"), "{help}");
        for item in expected {
            assert!(help.contains(item), "missing {item}: {help}");
        }
        assert_eq!(std::fs::read_to_string(&state).unwrap(), "invalid state");
        assert_eq!(std::fs::read_dir(&sitter_dir).unwrap().count(), 1);
    }
}
