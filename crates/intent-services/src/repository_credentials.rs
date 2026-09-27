//! Inactive repository credential lifetime engine. Existing auth/settings writers
//! must supply verified, settled facts; this module never persists or compensates.
//! Canonical instance names and deserialized scopes do not adopt legacy secrets.
//! Production registration and real writer/authority hooks are deliberately absent.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::{
    ExecutionScope, RepositoryConnectionScope, RepositoryProvider, RepositoryTarget,
};
use intent_sourcecontrol::{GitlabDescriptor, SecretString};
use uuid::Uuid;

#[path = "repository_credentials/acquire.rs"]
mod acquire;
#[path = "repository_credentials/authority.rs"]
pub(crate) mod authority;
#[path = "repository_credentials/state.rs"]
mod state;
pub(crate) use acquire::BoundGitlabRequestCredentials;
pub(crate) use authority::{RepositoryAuthority, RepositoryAuthorityRequest};

pub(crate) type Result<T> = std::result::Result<T, RepositoryCredentialError>;

/// Local outcomes only. Upstream denials/quota stay in the provider error model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryCredentialError {
    Retired,
    Missing,
    Unverified,
    Disconnected,
    ChildDisabled,
    Mutating,
    Indeterminate,
    AuthorityDenied,
    AuthorityUnavailable,
    BoundaryMismatch,
    SecretMismatch,
    StaleMutation,
    CounterExhausted,
    TimedOut,
    Backoff,
}
impl fmt::Display for RepositoryCredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "repository credential unavailable ({self:?})")
    }
}
impl std::error::Error for RepositoryCredentialError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryCredentialUse {
    NativeRead,
    NativePush,
    NativeReviewCreate,
    ChildGit,
}

/// Chosen once by the existing lifetime owner, never an auto/fallback chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryCredentialSource {
    GitlabSecretSlot,
    GitlabEnvironment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepositoryAccountKey {
    pub(crate) provider: RepositoryProvider,
    pub(crate) instance_base_url: String,
    pub(crate) account_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepositoryConnectionBinding {
    pub(crate) daemon_id: String,
    pub(crate) account: RepositoryAccountKey,
    pub(crate) scope: RepositoryConnectionScope,
}

/// An attestation from the existing writer's exact endpoint/token verification.
/// Calling this with a configured name, login or unverified user ID is invalid.
/// It contains no credential and cannot publish without a winning mutation ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedRepositoryAccount {
    descriptor: GitlabDescriptor,
    account_id: String,
    source: RepositoryCredentialSource,
}
impl VerifiedRepositoryAccount {
    pub(crate) fn from_verified_user(
        descriptor: GitlabDescriptor,
        immutable_id: u64,
        source: RepositoryCredentialSource,
    ) -> Result<Self> {
        if immutable_id == 0 {
            return Err(RepositoryCredentialError::Unverified);
        }
        Ok(Self {
            descriptor,
            account_id: immutable_id.to_string(),
            source,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryConnectionState {
    Unverified,
    Ready,
    Mutating,
    Disconnected,
    Indeterminate,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryMutationKind {
    Replace,
    Refresh,
    Disconnect,
}

/// Never serialized or supplied by a client. Reservation precedes the writer's
/// first await; begin precedes its first mutation, under its existing gates.
#[derive(Debug)]
pub(crate) struct RepositoryMutationTicket {
    epoch: Uuid,
    id: u64,
    generation: u64,
    secret_revision: u64,
    kind: RepositoryMutationKind,
}

/// Facts from the sole persistence/compensation owner AFTER it has settled.
pub(crate) enum SettledCredentialState {
    Verified(VerifiedRepositoryAccount),
    Compensated(VerifiedRepositoryAccount),
    Disconnected,
    Indeterminate,
}

#[derive(Clone)]
struct Published {
    binding: RepositoryConnectionBinding,
    verified: VerifiedRepositoryAccount,
}
struct Mutation {
    id: u64,
    kind: RepositoryMutationKind,
    previous: Option<Published>,
    previous_child_enabled: bool,
}
struct State {
    status: RepositoryConnectionState,
    published: Option<Published>,
    generation: u64,
    secret_revision: u64,
    child_revision: u64,
    child_enabled: bool,
    mutation_id: u64,
    reservation: Option<u64>,
    active: Option<Mutation>,
    backoff_until: Option<Instant>,
}

/// One volatile binding for the existing singleton GitLab credential slot.
/// A new directory always has a fresh private epoch, even if daemonId is reused.
pub(crate) struct RepositoryConnectionDirectory {
    epoch: Uuid,
    daemon_id: String,
    state: Mutex<State>,
}

pub(crate) struct RepositoryCredentialAdmission {
    epoch: Uuid,
    binding: RepositoryConnectionBinding,
    descriptor: GitlabDescriptor,
    request: RepositoryAuthorityRequest,
    authority: Arc<dyn RepositoryAuthority>,
    child_revision: Option<u64>,
}
impl fmt::Debug for RepositoryCredentialAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RepositoryCredentialAdmission")
            .finish_non_exhaustive()
    }
}

/// Secret loader input: exact source, binding and revision, never current defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepositorySecretRequest {
    pub(crate) binding: RepositoryConnectionBinding,
    pub(crate) secret_revision: u64,
    pub(crate) source: RepositoryCredentialSource,
}
pub(crate) trait RepositorySecretReader: Send + Sync {
    fn load<'a>(
        &'a self,
        expected: &'a RepositorySecretRequest,
    ) -> authority::CredentialFuture<'a, RepositorySecretSnapshot>;
}
/// The existing lifetime owner vouches for this exact stored/environment snapshot.
pub(crate) struct RepositorySecretSnapshot {
    pub(crate) request: RepositorySecretRequest,
    pub(crate) token: SecretString,
}
impl fmt::Debug for RepositorySecretSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RepositorySecretSnapshot")
            .finish_non_exhaustive()
    }
}

/// Dispatch metadata survives retirement; it never proves that an effect happened.
#[derive(Debug, Clone)]
pub(crate) struct RepositoryDispatchStamp {
    epoch: Uuid,
    binding: RepositoryConnectionBinding,
    secret_revision: u64,
    child_revision: Option<u64>,
    use_kind: RepositoryCredentialUse,
}
pub(crate) struct RepositoryCredentialTicket {
    token: SecretString,
    stamp: RepositoryDispatchStamp,
}
impl fmt::Debug for RepositoryCredentialTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RepositoryCredentialTicket")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "../tests/repository_credentials/provider.rs"]
mod tests_provider;
#[cfg(test)]
#[path = "../tests/repository_credentials/state.rs"]
mod tests_state;
