//! intent-sentry — Sentry engine + DTOs backing the `sentry.*` surface.
//!
//! Sibling to `intent-linear` but Sentry is REST-only (no GraphQL): a thin
//! [`SentryClient`] over `reqwest` posts to `https://sentry.io/api/0/...` and
//! the P0 [`SentryEngine`] surface covers `authStatus`, `listIssues`, and
//! `searchIssues`. Flattened [`SentryIssueResult`] mirrors the FE wire shape
//! field-for-field (`src/features/sentry-auth/types.ts`). A
//! [`SentryRegistry`] builds the engine from settings, surfacing a graceful
//! [`Error::NotConfigured`] when no credential pair is available.
//!
//! No `sentry.*` wire methods or routing live here — those map onto this
//! engine in a later milestone (SEN-WIRE).
//!
//! GUARDRAIL: the Sentry auth token is a secret. It is read only to build the
//! `Authorization` header; it is never logged, printed, or returned across
//! the wire. Only derived identity (the org slug) crosses the wire.

pub mod client;
pub mod engine;
pub mod error;
pub mod model;
pub mod registry;
pub mod token;

pub use engine::SentryEngine;
pub use error::{Error, Result};
pub use model::{
    FetchIssuesRequest, IssueStatusFilter, SentryAuthState, SentryIssueLevel, SentryIssuePage,
    SentryIssueResult, SentryIssueStatus, SentryProject,
};
pub use registry::{SentryRegistry, SentrySettings};
pub use token::TokenSource;
