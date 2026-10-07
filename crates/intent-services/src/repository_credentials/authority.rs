//! Sole private bridge to the captured caller/root authority owner.
use std::future::Future;
use std::pin::Pin;

use super::{
    ExecutionScope, GitlabDescriptor, RepositoryConnectionScope, RepositoryCredentialUse,
    RepositoryTarget, Result,
};

pub(crate) type CredentialFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Exact approved destinations. These are trusted, credential-free service facts,
/// never a displayed remote URL reconstructed into authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RepositoryCredentialTransport {
    GitlabApi(GitlabDescriptor),
    GitHttps(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepositoryAuthorityRequest {
    pub(crate) execution: ExecutionScope,
    pub(crate) target: RepositoryTarget,
    pub(crate) connection: RepositoryConnectionScope,
    pub(crate) use_kind: RepositoryCredentialUse,
    pub(crate) allowed_transport: RepositoryCredentialTransport,
}

/// The authority owner captures original `Caller`, `WireCredential`, root,
/// operation/stage and current durable facts. Public DTO equality is not a grant.
pub(crate) trait RepositoryAuthority: Send + Sync {
    fn revalidate<'a>(
        &'a self,
        request: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>>;
}

pub(crate) trait RepositoryAuthorityFence: Send {
    /// Consume once. Under the authority leaf fence, call `action` exactly once
    /// on success, never on rejection. Action may lock the credential directory,
    /// never await or do I/O. Never acquire the authority fence while holding the
    /// directory lock. Already released tokens/effects cannot be recalled later.
    fn dispatch(self: Box<Self>, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()>;
}
