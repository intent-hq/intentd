//! Google Antigravity runs the user's `agy` CLI as an ACP server: `agy acp`
//! (agy 1.5.0 or later).

pub const COMMAND: &str = "agy";

/// Default Gemini root directory under the user's home directory
/// (`agy acp --gemini-dir`).
pub const DEFAULT_GEMINI_DIR: &str = ".gemini";

/// Default app-data directory under the Gemini root (`agy acp --app-data-dir`).
/// Catalog and auth probes run in an ephemeral `--gemini-dir` while keeping
/// this default app-data directory name, seeding only the user's file-backed
/// auth files from `~/.gemini/antigravity-cli` so headless Linux (no OS
/// keyring) sees standard `agy acp --login` credentials without writing probe
/// sessions into the user's conversation history.
pub const DEFAULT_APP_DATA_DIR: &str = "antigravity-cli";

/// File-backed OAuth token stored inside `<gemini-dir>/<app-data-dir>` when no
/// OS keyring / Secret Service is available (e.g. headless Linux).
pub const OAUTH_TOKEN_FILE: &str = "antigravity-oauth-token";

/// Saved ACP auth-method state under `<gemini-dir>/<app-data-dir>`.
pub const ACP_AUTH_STATE_REL_PATH: &str = "cache/acp-auth-state.json";

/// First agy release with the `acp` subcommand. A launch updates an older
/// agy (intent-services `agy_cli`).
pub const MIN_VERSION: &str = "1.5.0";
pub const MIN_VERSION_TRIPLE: (u64, u64, u64) = (1, 5, 0);
