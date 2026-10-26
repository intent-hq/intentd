//! On-demand local identity for a registered repository before workspace creation.

use intent_core::repository_resource_pages::{RepositoryKnownQuery, RepositoryKnownSelection};

use super::{
    denied, entry, read_limit, unavailable, BoxFuture, Error, Frame, Instant, Ordering,
    RepositoryReadContext, Result, Services, State,
};

pub(crate) fn resolve(
    services: &Services,
    q: RepositoryKnownQuery,
) -> BoxFuture<'_, Result<RepositoryKnownSelection>> {
    entry(services, &Frame::ResolveKnown(q.clone()), move |r| {
        Box::pin(async move {
            if !matches!(q.context, RepositoryReadContext::Host {})
                || !std::path::Path::new(&q.path).is_absolute()
            {
                return Err(unavailable());
            }
            let lease = r.lease(&q.read_lifetime_id)?;
            *r.state.lock().map_err(denied)? = State::Reading(lease.clone());
            let selection = tokio::time::timeout(read_limit(), async {
                r.validate(&lease).await?;
                if !r
                    .base
                    .connection
                    .services
                    .store
                    .known_repo_exists(&q.path)
                    .await?
                {
                    return Err(unavailable());
                }
                let resolver =
                    crate::repository_context_live::resolver(Some(&lease.facts)).map_err(denied)?;
                let original = r.clone();
                let original_lease = lease.clone();
                let path = q.path.clone();
                // The blocking leaf retains the original bounded frame permits even
                // if its caller expires; no replacement frame or authority is made.
                let observed = tokio::task::spawn_blocking(move || {
                    // Caller task-locals do not cross into spawn_blocking. The
                    // original caller was validated above and is validated again
                    // below; check its captured cancellation/deadline here.
                    if original.base.completed.load(Ordering::Acquire)
                        || Instant::now() >= original.deadline
                    {
                        return Err(unavailable());
                    }
                    original
                        .base
                        .connection
                        .parent
                        .check_current()
                        .map_err(denied)?;
                    original
                        .base
                        .lifetime
                        .as_ref()
                        .ok_or_else(unavailable)?
                        .retirement()
                        .check_current()
                        .map_err(denied)?;
                    original_lease.check()?;
                    crate::repository_context_reader::known::selection(
                        std::path::Path::new(&path),
                        &resolver,
                        &crate::repository_context_reader::GitConfigEnvironment::default(),
                    )
                    .map_err(denied)
                })
                .await
                .map_err(denied)??;
                r.validate(&lease).await?;
                if !r
                    .base
                    .connection
                    .services
                    .store
                    .known_repo_exists(&q.path)
                    .await?
                {
                    return Err(unavailable());
                }
                Ok::<_, Error>(observed)
            })
            .await
            .map_err(denied)??;
            // Local observations carry the same original caller/settings/account
            // final-output guard as capture. They grant no remote read or write.
            *r.state.lock().map_err(denied)? = State::Capture(lease.clone());
            Ok(RepositoryKnownSelection {
                context: q.context,
                read_lifetime_id: lease.id.clone(),
                scope: lease.scope.clone(),
                revision: lease.revision(),
                path: q.path,
                selection,
            })
        })
    })
}
