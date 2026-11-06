//! Client for the E2B sandbox REST API and the in-sandbox envd process API.
//!
//! This is transport only: it creates, resumes, pauses and deletes sandboxes,
//! lists them by metadata, and runs commands inside them. Placement and
//! bootstrap logic live elsewhere.
//!
//! GUARDRAIL: the E2B API key and the per-sandbox access tokens are secrets.
//! They are held as [`SecretString`], whose `Debug` is redacted, and error
//! messages from E2B are scrubbed of the key before they are kept.

mod client;
mod envd;
mod error;
mod secret;

pub use client::{
    ConnectedSandbox, CreateSandbox, E2bClient, E2bConfig, ListSandboxes, ListedSandbox, Sandbox,
    SandboxState, DEFAULT_API_URL, DEFAULT_ENVD_URL,
};
pub use envd::{
    OutputChunk, ProcessEvent, ProcessExit, ProcessOutput, ProcessSpec, ProcessStream, ENVD_PORT,
};
pub use error::E2bError;
pub use secret::{load_api_key, SecretString, API_KEY_ACCOUNT};

#[cfg(test)]
mod fake;
#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;
