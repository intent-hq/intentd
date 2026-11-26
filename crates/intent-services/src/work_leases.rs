//! The store leases that make single-flight work single-flight across the
//! processes of one tenant (see `intent_store::LeaseKeeper`): their scopes,
//! keys and TTL.

use std::time::Duration;

use intent_core::WorkspaceId;

/// How long a work lease lasts without renewal. Its [`LeaseKeeper`]
/// renews it at half this while the work runs; a crashed holder's lease
/// lapses after it.
///
/// [`LeaseKeeper`]: intent_store::LeaseKeeper
pub(crate) const WORK_LEASE_TTL: Duration = Duration::from_secs(30);

/// An automatic PR refresh of one workspace or git root.
pub(crate) const PR_REFRESH_SCOPE: &str = "pr-refresh";

/// One PR monitor's poll (legacy and qualified monitors alike; monitor ids
/// are unique).
pub(crate) const PR_MONITOR_POLL_SCOPE: &str = "pr-monitor-poll";

/// The GitLab credential refresh of one host: the exchange of the stored
/// refresh token and the persist of the new pair.
pub(crate) const GITLAB_REFRESH_SCOPE: &str = "gitlab-credential-refresh";

/// The [`PR_REFRESH_SCOPE`] key: the workspace id, or `<workspace>/<root>`
/// for one of its git roots.
pub(crate) fn pr_refresh_key(workspace_id: &WorkspaceId, root_id: Option<&str>) -> String {
    match root_id {
        Some(root) => format!("{}/{root}", workspace_id.as_str()),
        None => workspace_id.as_str().to_owned(),
    }
}
