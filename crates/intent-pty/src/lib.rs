//! intent-pty — unified portable-pty host for terminals and scripts (§12).
//!
//! Depends on `intent-core` only (§3.2). Stub only — portable-pty is pulled
//! when the terminal/script host lands in Wave 3.

pub use intent_core::Result;

pub mod host {
    //! PTY host: service/command modes, auto-restart, URL/port detection — stub.
}

pub mod scrollback {
    //! Scrollback ring buffers — stub.
}

pub mod attach {
    //! Multi-client attach / detach — stub.
}
