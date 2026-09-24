//! Typed domain errors that map to JSON-RPC error codes (§11.1, PROTOCOL §9).
//!
//! Kept small but principled: `InvalidParams` and `NotFound` both surface as
//! `-32602` (per PROTOCOL §9 "not found" lookups are invalid-params), while
//! `Internal` is `-32603`. Higher layers translate these into JSON-RPC error
//! objects via [`Error::code`].

/// Domain error type for intentd.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A required parameter was missing or malformed.
    #[error("invalid params: {0}")]
    InvalidParams(String),

    /// A requested entity does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// An unexpected internal failure (I/O, persistence, serialization).
    #[error("internal error: {0}")]
    Internal(String),

    /// A conditional write lost an optimistic-concurrency check: the entity's
    /// current `rev` did not match the supplied `expectedVersion`. Carries the
    /// current entity so the client can reconcile (PROTOCOL §4, §5.6 — surfaced
    /// as `-32005` with `error.data.current`).
    #[error("conflict: version mismatch")]
    Conflict { current: serde_json::Value },

    /// A requested operation is not supported on this platform or configuration.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// Invalid input provided to an operation (e.g., file already exists, path doesn't exist).
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// A workspace base ref could not be resolved during checkout
    /// provisioning. Surfaces as `-32602` with the same human message as the
    /// plain `InvalidParams` shape plus machine-readable
    /// `error.data = { code: "base-ref-unresolvable", baseRef }` so clients
    /// stop matching on prose (monorepo#761).
    #[error("invalid params: cannot resolve base ref '{base_ref}'")]
    BaseRefUnresolvable { base_ref: String },

    /// A `workspace.create` clone/provisioning step failed.
    /// Carries a machine-readable category plus a sanitized human-readable
    /// detail (git stderr tail with credentials redacted) so clients can show
    /// the underlying cause instead of a bare "Internal error" and key
    /// behavior off `error.data.code` (monorepo#825/#826). User-fixable
    /// categories (`PathInvalid`, `DestinationExistsNonEmpty`) surface as
    /// `-32602`; environmental ones (`AuthRequired`, `AskpassMissing`,
    /// `RepoNotFound`, `AccessDenied`, `Network`, `Other`) as `-32603` with
    /// the detail preserved in the message.
    #[error("workspace.create clone failed ({}): {detail}", category.as_str())]
    CloneFailed {
        category: CloneErrorCategory,
        detail: String,
    },

    /// The `voice.transcribe` provider API key is missing. Surfaces as
    /// `-32603` with the same "Internal error" message as the plain
    /// `Internal` shape plus machine-readable
    /// `error.data = { code: "voice-no-api-key", detail }` so clients stop
    /// matching on prose (monorepo#1448). `detail` carries the descriptive
    /// text unchanged from the pre-structured shape.
    #[error("internal error: {detail}")]
    VoiceNotConfigured { detail: String },

    /// A `git.showFile` path resolves to a non-blob tree entry (a `160000`
    /// gitlink / submodule pin, or a `040000` tree), so there is no file
    /// content to return. Surfaces as `-32602` with machine-readable
    /// `error.data = { code: "not-a-file", path, mode }` so clients can route
    /// gitlink entries to a dedicated presentation instead of matching on an
    /// opaque "Internal error" (monorepo#1739). `mode` is the octal tree-entry
    /// mode string (e.g. `"160000"`).
    #[error("invalid params: path is not a file at this ref: {path} (mode {mode})")]
    NotAFile { path: String, mode: String },

    /// The TCP (WSS) listener is not running, so `pairing.getInfo` has no
    /// port to embed in the pairing payload. Surfaces as `-32603` with the
    /// same human message as the previous `Unsupported` shape plus
    /// machine-readable `error.data = { code: "listener-down" }` so
    /// `intentd pair` stops matching on prose (monorepo#1822).
    #[error(
        "unsupported: TCP listener is not running — ensure the WSS listener is enabled \
         (server.wsApi.enabled) and started successfully (check daemon logs for bind \
         errors, e.g. port already in use) before pairing"
    )]
    ListenerDown,

    /// The tunnel is not running, so `workspace.invite.create` has no `tc`
    /// address to embed in the (tunnel-only) invite link. Surfaces as
    /// `-32603` with machine-readable `error.data = { code: "tunnel-down" }`
    /// so clients route it without matching on prose. Distinct from
    /// [`Error::ListenerDown`]: the WSS listener IS up here.
    #[error(
        "tunnel is not running — invite links are tunnel-only; enable the tunnel \
         (server.tunnel.enabled) and wait for it to come up before inviting"
    )]
    TunnelDown,

    /// A `repo.warmCache` request was rejected because an opportunistic warm
    /// is already in flight (global single-flight — at most one warm
    /// daemon-wide). Surfaces as `-32603` with machine-readable
    /// `error.data = { code: "warm-in-flight", owner, repo }` naming the repo
    /// currently being warmed, so clients key off `error.data.code` instead
    /// of prose. Deliberately not queued: the caller fires and forgets.
    #[error("repo cache warm already in flight for {owner}/{repo}")]
    WarmInFlight { owner: String, repo: String },

    /// An `agent.completeOnce` call gave up waiting for a slot in the
    /// daemon-wide ephemeral-adapter bound (`agents.maxConcurrentAdapters`):
    /// the daemon was already running its full complement of adapter chains
    /// and the caller's own `timeoutMs` elapsed while queued. Surfaces as
    /// `-32603` with machine-readable
    /// `error.data = { code: "adapter-busy", provider, waitedMs, limit }` so a
    /// client can distinguish "the daemon is saturated, retry later" from "the
    /// model was slow" without matching on prose (monorepo#2062). Nothing was
    /// spawned and no model was asked, so a retry is always safe.
    #[error(
        "no free adapter slot for {provider} after {waited_ms}ms \
         (agents.maxConcurrentAdapters = {limit})"
    )]
    AdapterBusy {
        provider: String,
        waited_ms: u64,
        limit: u32,
    },

    /// `sourceControl.connect { method: "device" }` against a forge host that
    /// cannot run an OAuth device grant: no client id resolves for it, or the
    /// instance rejected the grant (GitLab < 17.1 / application not enabled
    /// for it). Surfaces as `-32603` with machine-readable
    /// `error.data = { code: "device-grant-unsupported", provider, host }` —
    /// the FE keys its PAT fallback on this code (PROTOCOL §5.27).
    #[error("device grant unsupported on {provider} host {host}")]
    DeviceGrantUnsupported { provider: String, host: String },

    /// The forge rejected a source-control credential: a PAT offered to
    /// `sourceControl.connect { method: "pat" }` that fails the host's user
    /// probe (nothing is stored), or a stored/env credential the host rejects
    /// on `sourceControl.getUser`. Surfaces as `-32603` with machine-readable
    /// `error.data = { code: "source-control-unauthorized", provider, host }`.
    /// A merely *absent* credential is not an error (PROTOCOL §5.27).
    #[error("{provider} host {host} rejected the credential")]
    SourceControlUnauthorized { provider: String, host: String },

    /// The source-control forge rate-limited a request (REST 403/429 with an
    /// exhausted quota). Distinct from `Internal` so background sweeps can
    /// detect the class and pause globally until the quota window resets, and
    /// so logs stop misreporting quota exhaustion as
    /// "source control auth error" (monorepo#2961). Surfaces as `-32603`.
    #[error("source control rate limited: {0}")]
    RateLimited(String),

    /// The bound caller lacks the capability for this operation (multiplayer
    /// w3 capability matrix: an Owner-only or administrator-only method
    /// invoked by a collaborator). Surfaces as `-32003 Forbidden` — the same
    /// code the transport's default-deny allowlist uses — so a client cannot
    /// tell a service-layer refusal from a transport one. A non-member is
    /// answered with `NotFound`, never `Forbidden`, so membership itself is
    /// not disclosed.
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// A workspace-invite operation was refused for a reason the client must
    /// key off machine-readably (multiplayer w4): the invite is unknown /
    /// expired / revoked / already redeemed, the pinned GitHub login does not
    /// match the authorizing account, the owner has no GitHub identity to
    /// invite from, or the gist identity proof did not check out. Surfaces
    /// as `-32602` (`-32603` for the daemon-side conditions) with
    /// `error.data = { code: kind.as_str() }`.
    #[error("{}", .0.message())]
    Invite(InviteErrorKind),

    /// A guest-side identity-proof operation
    /// (`sourceControl.identityProof.create` / `.delete`, or the
    /// `github.identityProof.*` aliases) was refused for a reason the client
    /// must key off machine-readably: no token is stored for the provider,
    /// the stored token lacks the scope the proof needs, or the forge could
    /// not be reached. Surfaces as `-32603` with
    /// `error.data = { code: kind.as_str() }`.
    #[error("{}", .0.message())]
    IdentityProof(IdentityProofErrorKind),

    /// Host half of the identity proof: the forge at `host` will not serve
    /// the guest's proof to this daemon — anonymous reads are restricted on
    /// that instance and the host holds no credential for it (or the one it
    /// holds was refused) — so the claimed identity can be neither confirmed
    /// nor denied. Surfaces as `-32603` with
    /// `error.data = { code: "identity-unverifiable", host }` (protocol 10.8).
    #[error("cannot verify identity on {host}")]
    IdentityUnverifiable { host: String },
}

/// Machine-readable reason an identity-proof operation was refused,
/// surfaced on the wire as `error.data.code`. The GitHub kinds are the
/// gist proof's original codes; the GitLab kinds are their snippet-proof
/// twins (protocol 10.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityProofErrorKind {
    /// No GitHub token is stored (`github.connect` never completed, or the
    /// token was revoked / rejected by GitHub).
    NotConnected,
    /// The stored token lacks the `gist` OAuth scope; the user must
    /// re-authorize (`github.connect`) to grant it.
    ScopeMissing,
    /// GitHub could not be reached.
    Unreachable,
    /// No GitLab token is stored for the instance (`sourceControl.connect`
    /// never completed for it, the instance is not the bound one, or the
    /// token was revoked / rejected).
    GitlabNotConnected,
    /// The stored GitLab token lacks the `api` scope snippets need; the user
    /// must reconnect with a token that grants it.
    GitlabScopeMissing,
    /// The GitLab instance could not be reached.
    GitlabUnreachable,
}

impl IdentityProofErrorKind {
    /// Stable wire identifier for this kind (`error.data.code`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            IdentityProofErrorKind::NotConnected => "github-not-connected",
            IdentityProofErrorKind::ScopeMissing => "github-scope-missing",
            IdentityProofErrorKind::Unreachable => "github-unreachable",
            IdentityProofErrorKind::GitlabNotConnected => "gitlab-not-connected",
            IdentityProofErrorKind::GitlabScopeMissing => "gitlab-scope-missing",
            IdentityProofErrorKind::GitlabUnreachable => "gitlab-unreachable",
        }
    }

    /// Human-readable message for this kind.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            IdentityProofErrorKind::NotConnected => {
                "internal error: GitHub is not connected — sign in (github.connect) before \
                 proving your identity"
            }
            IdentityProofErrorKind::ScopeMissing => {
                "internal error: the stored GitHub token lacks the `gist` scope — sign in again \
                 (github.connect) to grant it"
            }
            IdentityProofErrorKind::Unreachable => "internal error: GitHub could not be reached",
            IdentityProofErrorKind::GitlabNotConnected => {
                "internal error: GitLab is not connected for this host — sign in \
                 (sourceControl.connect) before proving your identity"
            }
            IdentityProofErrorKind::GitlabScopeMissing => {
                "internal error: the stored GitLab token lacks the `api` scope — reconnect \
                 (sourceControl.connect) with a token that grants it"
            }
            IdentityProofErrorKind::GitlabUnreachable => {
                "internal error: the GitLab instance could not be reached"
            }
        }
    }
}

/// Machine-readable reason an invite / join operation was refused, surfaced
/// on the wire as `error.data.code` (multiplayer w4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InviteErrorKind {
    /// No invite has this id, or the secret does not match it.
    NotFound,
    /// The invite's `expiresAt` has passed.
    Expired,
    /// The owner revoked the invite.
    Revoked,
    /// The invite was already redeemed (single use).
    Redeemed,
    /// The invite is pinned to another GitHub login.
    PinMismatch,
    /// The pinned login does not name a GitHub account.
    PinUnknown,
    /// Minting an invite requires the owner's linked GitHub identity
    /// (`github.authStatus.isConfigured`).
    GithubIdentityRequired,
    /// The primary user's GitHub identity cannot change while other
    /// principals or open invites exist (reconnect guard).
    IdentityLocked,
    /// The host is throttling unauthenticated invite requests: the
    /// listener-wide start budget (a token bucket, so a burst of serial
    /// requests trips it with nothing in flight) or the outstanding-nonce
    /// cap is spent; retry later.
    FlowBusy,
    /// The workspace's guest cap (`sharing.maxGuestsPerWorkspace`) is spent
    /// by its collaborators plus open invites; no further invite is minted.
    GuestLimit,
    /// The workspace's collaborators already reach the guest cap; the join
    /// is refused and the invite stays open.
    WorkspaceFull,
    /// The `credential` an `invite.accept` presented is unknown to this host
    /// or was revoked; the guest must join through the gist identity proof.
    CredentialInvalid,
    /// The gist an `invite.prove` named does not prove the claimed login:
    /// unknown gist, another owner, no proof file or a first line that is
    /// not the nonce, a gist created before the nonce was issued, or a
    /// nonce this host never issued for the invite (or already consumed).
    ProofInvalid,
    /// The nonce an `invite.prove` presented was issued but its 10-minute
    /// lifetime passed; the guest must start over with `invite.challenge`.
    ProofExpired,
    /// GitHub could not be reached (or answered a server error) while
    /// verifying the proof gist; the nonce stays valid for a retry.
    GithubUnreachable,
    /// The proven (or credential-bound) GitHub account is the host owner's
    /// own — the primary principal. The owner cannot join its own host as a
    /// guest: no per-principal credential is ever minted for the primary
    /// row, and the invite stays open.
    OwnerSelfJoin,
    /// The workspace is archived: archiving detaches every guest and closes
    /// every open invite, so no invite is minted, no member is added and no
    /// join is committed against it until it is unarchived. Nothing was
    /// written.
    WorkspaceArchived,
}

impl InviteErrorKind {
    /// Stable wire identifier for this kind (`error.data.code`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            InviteErrorKind::NotFound => "invite-not-found",
            InviteErrorKind::Expired => "invite-expired",
            InviteErrorKind::Revoked => "invite-revoked",
            InviteErrorKind::Redeemed => "invite-redeemed",
            InviteErrorKind::PinMismatch => "invite-pin-mismatch",
            InviteErrorKind::PinUnknown => "invite-pin-unknown",
            InviteErrorKind::GithubIdentityRequired => "github-identity-required",
            InviteErrorKind::IdentityLocked => "primary-identity-locked",
            InviteErrorKind::FlowBusy => "invite-flow-busy",
            InviteErrorKind::GuestLimit => "guest-limit",
            InviteErrorKind::WorkspaceFull => "workspace-full",
            InviteErrorKind::CredentialInvalid => "credential-invalid",
            InviteErrorKind::ProofInvalid => "proof-invalid",
            InviteErrorKind::ProofExpired => "proof-expired",
            InviteErrorKind::GithubUnreachable => "github-unreachable",
            InviteErrorKind::OwnerSelfJoin => "owner-self-join",
            InviteErrorKind::WorkspaceArchived => "workspace-archived",
        }
    }

    /// Human-readable message for this kind.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            InviteErrorKind::NotFound => "invalid params: invite not found",
            InviteErrorKind::Expired => "invalid params: invite has expired",
            InviteErrorKind::Revoked => "invalid params: invite was revoked",
            InviteErrorKind::Redeemed => "invalid params: invite was already redeemed",
            InviteErrorKind::PinMismatch => {
                "invalid params: this invite is pinned to a different GitHub account"
            }
            InviteErrorKind::PinUnknown => {
                "invalid params: pinLogin does not name a GitHub account"
            }
            InviteErrorKind::GithubIdentityRequired => {
                "unsupported: inviting requires a linked GitHub identity — connect GitHub \
                 (github.connect) before creating an invite"
            }
            InviteErrorKind::IdentityLocked => {
                "unsupported: the primary GitHub identity cannot change while other \
                 principals or open invites exist"
            }
            InviteErrorKind::FlowBusy => {
                "internal error: the host is throttling invite requests; retry shortly"
            }
            InviteErrorKind::GuestLimit => {
                "invalid params: this workspace has reached its guest limit (collaborators \
                 plus open invites); revoke an invite or remove a member first"
            }
            InviteErrorKind::WorkspaceFull => {
                "invalid params: this workspace has reached its guest limit; ask the owner \
                 to make room"
            }
            InviteErrorKind::CredentialInvalid => {
                "invalid params: the credential is unknown to this host or was revoked; \
                 join through the GitHub identity proof instead"
            }
            InviteErrorKind::ProofInvalid => {
                "invalid params: the gist does not prove the claimed GitHub identity for \
                 this invite"
            }
            InviteErrorKind::ProofExpired => {
                "invalid params: the identity-proof nonce expired; request a new challenge"
            }
            InviteErrorKind::GithubUnreachable => {
                "internal error: GitHub could not be reached to verify the identity proof"
            }
            InviteErrorKind::OwnerSelfJoin => {
                "invalid params: this GitHub account owns the host; open it from your \
                 paired daemons instead of joining as a guest"
            }
            InviteErrorKind::WorkspaceArchived => {
                "invalid params: this workspace is archived; unarchive it before inviting \
                 or adding members"
            }
        }
    }

    /// JSON-RPC 2.0 numeric error code for this kind.
    #[must_use]
    pub fn code(self) -> i32 {
        match self {
            InviteErrorKind::NotFound
            | InviteErrorKind::Expired
            | InviteErrorKind::Revoked
            | InviteErrorKind::Redeemed
            | InviteErrorKind::PinMismatch
            | InviteErrorKind::PinUnknown
            | InviteErrorKind::GuestLimit
            | InviteErrorKind::WorkspaceFull
            | InviteErrorKind::CredentialInvalid
            | InviteErrorKind::ProofInvalid
            | InviteErrorKind::ProofExpired
            | InviteErrorKind::OwnerSelfJoin
            | InviteErrorKind::WorkspaceArchived => -32602,
            InviteErrorKind::GithubIdentityRequired
            | InviteErrorKind::IdentityLocked
            | InviteErrorKind::FlowBusy
            | InviteErrorKind::GithubUnreachable => -32603,
        }
    }
}

/// Machine-readable category for a failed clone/provisioning step, surfaced
/// on the wire as `error.data.code` (PROTOCOL §9, monorepo#826).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneErrorCategory {
    /// The clone destination path is missing, malformed, or not creatable.
    PathInvalid,
    /// The clone destination already exists (and is not an empty directory).
    DestinationExistsNonEmpty,
    /// The remote rejected the clone for lack of credentials.
    AuthRequired,
    /// The daemon's askpass helper script could not be executed (missing or
    /// unreachable — e.g. macOS quarantine relocating the app bundle). Looks
    /// like an auth failure in git's stderr, but the fix is local
    /// (monorepo#837).
    AskpassMissing,
    /// The remote reports the repository does not exist ("Repository not
    /// found", HTTP 404). With credential injection in play (monorepo#825),
    /// GitHub also answers 404 for private repositories the presented token
    /// cannot see.
    RepoNotFound,
    /// The remote refused access to an existing repository (HTTP 403,
    /// "access denied").
    AccessDenied,
    /// The remote could not be reached (DNS, connect, timeout).
    Network,
    /// Any other clone failure; the detail still carries the stderr tail.
    Other,
}

impl CloneErrorCategory {
    /// Stable wire identifier for this category (`error.data.code`).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            CloneErrorCategory::PathInvalid => "path-invalid",
            CloneErrorCategory::DestinationExistsNonEmpty => "destination-exists-non-empty",
            CloneErrorCategory::AuthRequired => "auth-required",
            CloneErrorCategory::AskpassMissing => "askpass-missing",
            CloneErrorCategory::RepoNotFound => "repo-not-found",
            CloneErrorCategory::AccessDenied => "access-denied",
            CloneErrorCategory::Network => "network",
            CloneErrorCategory::Other => "clone-failed",
        }
    }
}

impl Error {
    /// JSON-RPC 2.0 numeric error code for this error (PROTOCOL §9).
    #[must_use]
    pub fn code(&self) -> i32 {
        match self {
            Error::InvalidParams(_)
            | Error::NotFound(_)
            | Error::InvalidInput(_)
            | Error::BaseRefUnresolvable { .. }
            | Error::NotAFile { .. } => -32602,
            Error::CloneFailed { category, .. } => match category {
                CloneErrorCategory::PathInvalid | CloneErrorCategory::DestinationExistsNonEmpty => {
                    -32602
                }
                CloneErrorCategory::AuthRequired
                | CloneErrorCategory::AskpassMissing
                | CloneErrorCategory::RepoNotFound
                | CloneErrorCategory::AccessDenied
                | CloneErrorCategory::Network
                | CloneErrorCategory::Other => -32603,
            },
            Error::Internal(_)
            | Error::VoiceNotConfigured { .. }
            | Error::ListenerDown
            | Error::TunnelDown
            | Error::WarmInFlight { .. }
            | Error::AdapterBusy { .. }
            | Error::DeviceGrantUnsupported { .. }
            | Error::SourceControlUnauthorized { .. }
            | Error::RateLimited(_)
            | Error::IdentityProof(_)
            | Error::IdentityUnverifiable { .. }
            // Unsupported: map to internal error for now
            | Error::Unsupported(_) => -32603,
            Error::Conflict { .. } => -32005,
            Error::Forbidden(_) => -32003,
            Error::Invite(kind) => kind.code(),
        }
    }
}

/// Convenience result alias used across the workspace.
pub type Result<T> = std::result::Result<T, Error>;
