//! The provider boundary for cloud machines: the calls a backend makes to
//! create, run, pause, resume and delete one machine per agent, whatever
//! service hosts it. [`crate::e2b::E2bClient`] is the first adapter.
//!
//! GUARDRAIL: provider keys and per-machine access tokens are
//! [`SecretString`]s, and [`CloudError`] messages are already scrubbed of
//! them by the adapter.

mod error;
mod process;
mod secret;

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

pub use error::{CloudError, CloudErrorKind, QuotaLimit};
pub use process::{ProcessExit, ProcessOutput, ProcessSpec};
pub use secret::SecretString;

pub type CloudFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, CloudError>> + Send + 'a>>;

/// A new machine.
#[derive(Debug, Clone, Default)]
pub struct CreateInstance {
    /// The image (E2B template) to start, as the provider names it.
    pub image: String,
    /// Seconds until the provider stops the machine unless kept alive
    /// (see [`CloudProvider::keep_alive`]).
    pub timeout_secs: Option<u32>,
    /// Pause instead of delete when the timeout runs out.
    pub pause_on_timeout: Option<bool>,
    /// Labels the machine is listed by. An adapter may encode them in the
    /// provider's own tag format, but [`CloudProvider::list`] must return
    /// them exactly as given: the reaper reads them back.
    pub tags: BTreeMap<String, String>,
    /// Makes a retried create return the first machine. Reuse it on every retry.
    pub idempotency_key: Option<String>,
    /// The provider's compute plan for the size (Northflank: the deployment
    /// plan). E2B ignores it: its size is in the template.
    pub plan: Option<String>,
    /// Disk of a new machine, in GiB, for a provider that makes one.
    pub disk_gib: Option<u32>,
}

/// A machine the backend can run commands in.
#[derive(Debug, Clone)]
pub struct CloudInstance {
    pub id: String,
    pub image: String,
    /// Authenticates [`CloudProvider::run_command`], when the provider
    /// issues one per machine.
    pub access_token: Option<SecretString>,
    /// The machine's own volume, for a provider that makes one.
    pub volume_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Connected {
    pub instance: CloudInstance,
    /// `true` when the machine was paused and the connect resumed it.
    pub resumed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceState {
    Running,
    Paused,
    /// A state the backend does not act on.
    Other,
}

#[derive(Debug, Clone, Default)]
pub struct ListInstances {
    /// Every pair must match.
    pub tags: BTreeMap<String, String>,
    /// Empty means the provider's default.
    pub states: Vec<InstanceState>,
}

#[derive(Debug, Clone)]
pub struct ListedInstance {
    pub id: String,
    pub state: InstanceState,
    pub tags: BTreeMap<String, String>,
    /// RFC 3339; empty when the provider did not say.
    pub started_at: String,
}

/// One provider account (one saved key).
pub trait CloudProvider: Send + Sync + fmt::Debug {
    /// The provider's id (`CloudMachine.provider`).
    fn kind(&self) -> &'static str;

    /// Create and start a machine.
    fn create<'a>(&'a self, request: &'a CreateInstance) -> CloudFuture<'a, CloudInstance>;

    /// Connect to machine `id`, resuming it when paused, and keep it alive
    /// for at least `timeout_secs`. Returns only once
    /// [`Self::run_command`] can succeed.
    fn connect<'a>(&'a self, id: &'a str, timeout_secs: u32) -> CloudFuture<'a, Connected>;

    /// Run a command in the machine and wait for it to finish.
    fn run_command<'a>(
        &'a self,
        instance: &'a CloudInstance,
        spec: &'a ProcessSpec,
    ) -> CloudFuture<'a, ProcessOutput>;

    /// Pause a running machine.
    fn pause<'a>(&'a self, id: &'a str) -> CloudFuture<'a, ()>;

    /// Keep machine `id` alive for `timeout_secs` from now. With E2B this is
    /// a lease: when the backend stops renewing it, the provider pauses
    /// the machine itself.
    fn keep_alive<'a>(&'a self, id: &'a str, timeout_secs: u32) -> CloudFuture<'a, ()>;

    /// Delete a machine. `false` when it was already gone.
    fn delete<'a>(&'a self, id: &'a str) -> CloudFuture<'a, bool>;

    /// The machines that match the filter.
    fn list<'a>(&'a self, filter: &'a ListInstances) -> CloudFuture<'a, Vec<ListedInstance>>;

    /// Test the key with one cheap read.
    fn check_key(&self) -> CloudFuture<'_, ()>;

    /// Whether `image` is ready to start, as this key sees it. The format
    /// is the provider's (E2B: `<template>:<tag>`).
    fn image_ready<'a>(&'a self, image: &'a str) -> CloudFuture<'a, bool>;

    /// Whether the provider pauses a machine itself once
    /// [`Self::keep_alive`] stops being renewed. Without such a lease, the
    /// backend's idle pause is the only one, and `keep_alive` does nothing.
    fn has_lease(&self) -> bool {
        true
    }

    /// Whether a paused machine resumes with its memory and processes.
    /// `false`: only its disk is kept, and the daemon must be started again.
    fn keeps_memory(&self) -> bool {
        true
    }
}
