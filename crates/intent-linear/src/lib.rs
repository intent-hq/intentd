//! intent-linear — Linear engine + DTOs backing the `linear.*` surface.
//!
//! Depends on `intent-core` only among workspace crates. It defines the P0
//! [`LinearEngine`] (auth probe + issue list/search) backed by a thin GraphQL
//! [`LinearClient`] over `reqwest` (rustls, no OpenSSL), the flattened
//! [`LinearIssueResult`] DTO mirroring the FE wire shape, and a
//! [`LinearRegistry`] that builds the engine from settings with graceful
//! `NotConfigured`.
//!
//! No `linear.*` wire methods or routing live here — those map onto this
//! engine in a later milestone (LIN-WIRE).
//!
//! GUARDRAIL: the Linear API key is a secret. It is read only to authenticate
//! HTTP requests; it is never logged, printed, or returned across the wire.
//! Only derived identity (the viewer `login`) crosses the wire.

pub mod client;
pub mod engine;
pub mod error;
pub mod model;
pub mod registry;
pub mod token;

pub use engine::LinearEngine;
pub use error::{Error, Result};
pub use model::{
    AuthStatus, CreateIssueRequest, IssueFilter, LinearIssuePage, LinearIssueResult, LinearLabel,
    LinearProject, LinearTeam, LinearUser, LinearWorkflowState, UpdateIssueRequest,
};
pub use registry::{LinearRegistry, LinearSettings};
pub use token::TokenSource;
