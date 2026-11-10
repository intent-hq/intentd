//! Google Antigravity runs the user's `agy` CLI as an ACP server: `agy acp`
//! (agy 1.5.0 or later).

pub const COMMAND: &str = "agy";

/// Catalog and auth probes start a session, and agy saves every session in
/// its conversation history. Probes use their own app-data directory so they
/// never appear there. agy resolves it under `~/.gemini` and rejects an
/// absolute path. The sign-in is shared with the default directory.
pub const PROBE_APP_DATA_DIR: &str = "antigravity-intent-probe";

/// First agy release with the `acp` subcommand. A launch updates an older
/// agy (intent-services `agy_cli`).
pub const MIN_VERSION: &str = "1.5.0";
pub const MIN_VERSION_TRIPLE: (u64, u64, u64) = (1, 5, 0);
