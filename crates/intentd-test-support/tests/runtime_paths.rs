use intentd_test_support::runtime_paths::{binary_path, manifest_dir};
use std::process::Command;

// Spawn this very test to isolate environment changes from parallel tests.
#[test]
fn runtime_paths_child() {
    let Ok(mode) = std::env::var("PATH_RESOLVER_CASE") else {
        return;
    };
    let fallback = std::env::current_exe().unwrap();
    if mode == "fallback" {
        assert_eq!(binary_path("fixture", fallback.to_str().unwrap()), fallback);
        assert_eq!(
            manifest_dir(fallback.parent().unwrap().to_str().unwrap()),
            fallback.parent().unwrap()
        );
    } else if mode == "override" {
        assert_eq!(
            binary_path("fixture", "unavailable-producer-binary"),
            fallback
        );
        assert_eq!(
            manifest_dir("unavailable-producer-source"),
            fallback.parent().unwrap()
        );
    } else if mode == "bad-binary" {
        binary_path("fixture", fallback.to_str().unwrap());
    } else if mode == "bad-source" {
        manifest_dir(fallback.parent().unwrap().to_str().unwrap());
    } else {
        panic!("unknown case");
    }
}

#[test]
fn explicit_invalid_paths_never_fall_back_to_existing_producer_files() {
    let exe = std::env::current_exe().unwrap();
    for (mode, binary, source, success) in [
        ("fallback", None, None, true),
        (
            "override",
            Some(exe.as_os_str()),
            exe.parent().map(std::path::Path::as_os_str),
            true,
        ),
        ("bad-binary", Some(std::ffi::OsStr::new("")), None, false),
        (
            "bad-binary",
            Some(std::ffi::OsStr::new("/missing/archive/binary")),
            None,
            false,
        ),
        (
            "bad-binary",
            exe.parent().map(std::path::Path::as_os_str),
            None,
            false,
        ),
        (
            "bad-source",
            None,
            Some(std::ffi::OsStr::new("/missing/archive/source")),
            false,
        ),
    ] {
        let mut command = Command::new(&exe);
        command
            .args(["--exact", "runtime_paths_child", "--nocapture"])
            .env("PATH_RESOLVER_CASE", mode)
            .env_remove("NEXTEST_BIN_EXE_fixture")
            .env_remove("CARGO_MANIFEST_DIR");
        if let Some(value) = binary {
            command.env("NEXTEST_BIN_EXE_fixture", value);
        }
        if let Some(value) = source {
            command.env("CARGO_MANIFEST_DIR", value);
        }
        let result = command.output().unwrap();
        assert_eq!(
            result.status.success(),
            success,
            "{mode}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
}
