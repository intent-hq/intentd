//! Share observations only inside one original physical caller and authority.
//! Lease expiry/release cannot retire a sibling lease's shared observations.

use super::*;

pub(super) struct Group {
    context: RepositoryReadContext,
    shared: bool,
    authority: scoped::Authority,
    settings: Arc<SettingsSnapshot>,
    connection: intent_core::RepositoryConnectionScope,
    instance: String,
    pub(super) scope: ExecutionScope,
    pub(super) observations: ConnectionObservations,
}

impl Drop for Group {
    fn drop(&mut self) {
        self.observations.retire();
    }
}

pub(super) fn group(
    c: &Connection,
    context: &RepositoryReadContext,
    authority: &scoped::Authority,
    settings: &Arc<SettingsSnapshot>,
    scope: &ExecutionScope,
    settled: &RepositorySettledConnection,
    shared: bool,
) -> Result<Arc<Group>> {
    let binding = &settled.selected().binding.scope;
    let instance = settled.descriptor().instance().as_str();
    let mut groups = c.resource.groups.lock().map_err(denied)?;
    groups.retain(|group| group.strong_count() > 0);
    for group in groups.iter().filter_map(Weak::upgrade) {
        if shared
            && group.shared
            && group.context == *context
            && group.authority == *authority
            && Arc::ptr_eq(&group.settings, settings)
            && group.connection == *binding
            && group.instance == instance
            && group.observations.is_active()
        {
            return Ok(group);
        }
    }
    // Capacity remains bounded even while immutable queued outputs retain old
    // leases. It is never a history of every former account or query.
    if groups.len() >= LIMIT {
        return Err(unavailable());
    }
    let group = Arc::new(Group {
        context: context.clone(),
        shared,
        authority: authority.clone(),
        settings: settings.clone(),
        connection: binding.clone(),
        instance: instance.into(),
        scope: scope.clone(),
        observations: ConnectionObservations::new(scope.clone(), binding.clone()),
    });
    groups.push(Arc::downgrade(&group));
    Ok(group)
}
