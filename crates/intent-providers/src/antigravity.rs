//! Google Antigravity runs the user's `agy` CLI as an ACP server: `agy acp`
//! (agy 1.5.0 or later).

pub const COMMAND: &str = "agy";

/// Catalog and auth probes start a session, and agy saves every session in
/// its conversation history. Probes use their own app-data directory so they
/// never appear there. agy resolves it under `~/.gemini` and rejects an
/// absolute path. The sign-in is shared with the default directory.
pub const PROBE_APP_DATA_DIR: &str = "antigravity-intent-probe";
