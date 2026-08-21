//! Node-side coverage for the bundled pi MCP extension
//! (`intent-services/src/pi_mcp_extension.ts`): shells out to
//! `tests/fixtures/pi-mcp-extension-test.mjs`, which drives the extension's
//! MCP client core against the `mock-mcp-server.mjs` fixture and the extension
//! factory against a TCP bridge stand-in (handshake, tools/list, tools/call
//! forwarding, reconnect-on-drop, graceful degradation). Skipped if `node` is
//! unavailable, matching the other node-gated suites.

// Pulls in the shared ctor (hermetic-root guard + NODE_DISABLE_COMPILE_CACHE)
// so the spawned node child cannot leave residue at the TMPDIR root.
mod common;

use std::path::PathBuf;
use std::process::Command;

fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

#[test]
fn pi_mcp_extension_client_round_trips_against_mock_server() {
    if !node_available() {
        eprintln!(
            "skipping pi_mcp_extension_client_round_trips_against_mock_server: node not on PATH"
        );
        return;
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let driver = manifest.join("tests/fixtures/pi-mcp-extension-test.mjs");
    let extension = manifest.join("../intent-services/src/pi_mcp_extension.ts");
    let mock = manifest.join("tests/fixtures/mock-mcp-server.mjs");
    assert!(extension.exists(), "missing {}", extension.display());

    let output = Command::new("node")
        .arg(&driver)
        .arg(&extension)
        .arg(&mock)
        .output()
        .expect("failed to spawn node");
    assert!(
        output.status.success(),
        "pi-mcp-extension-test.mjs failed (status {:?})\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
