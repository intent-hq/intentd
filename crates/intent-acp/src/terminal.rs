//! Client-served terminal capability — stubbed until the PTY host lands (M6).
//!
//! The handshake advertises `terminal: true`, so a provider may still issue
//! `terminal/*` requests. Until M6 we answer each with a clean JSON-RPC error
//! rather than leaving the request to time out, keeping `intent-acp` free of any
//! PTY dependency (§6.7).

use crate::error::JsonRpcError;

/// JSON-RPC error code for an unsupported method (mirrors `-32601`
/// "Method not found").
const METHOD_NOT_FOUND: i64 = -32601;

/// The `terminal/*` methods a provider may call (parity with the ACP schema).
pub const TERMINAL_METHODS: [&str; 5] = [
    "terminal/create",
    "terminal/output",
    "terminal/wait_for_exit",
    "terminal/release",
    "terminal/kill",
];

/// Whether `method` is a client-served terminal method.
pub fn is_terminal_method(method: &str) -> bool {
    TERMINAL_METHODS.contains(&method)
}

/// The error returned for any `terminal/*` request until the M6 PTY host ships.
pub fn unsupported_error(method: &str) -> JsonRpcError {
    JsonRpcError {
        code: METHOD_NOT_FOUND,
        message: format!("{method} is not supported until M6 (terminal/PTY host)"),
        data: None,
    }
}
