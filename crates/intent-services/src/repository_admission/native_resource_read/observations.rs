//! Share observations only inside one original physical caller and authority.
//! Lease expiry/release cannot retire a sibling lease's shared observations.

use super::{
    credential, credentials, denied, scoped, unavailable, Arc, AtomicBool, Connection,
    ConnectionObservations, ExecutionScope, Lease, Mutex, Ordering, RepositoryCredentialError,
    RepositoryReadContext, RepositoryResourceKind, RepositorySettledConnection, Request, Result,
    Retired, SettingsSnapshot, Weak, LIMIT,
};

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

impl Group {
    pub(super) fn supports_pages(&self) -> bool {
        self.shared
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

use crate::pr_monitor::qualified_cache::{
    AcceptedCacheDenial, CacheAdmissionAction, CacheDenialCoverage, GuardedCacheDenial,
};
use intent_core::repository_resource_pages::{
    RepositoryInvalidationCoverage, RepositoryListKind, RepositoryResourceEvent,
    RepositoryResourceInvalidated,
};

struct Recipients {
    groups: Vec<Arc<Group>>,
    notices: Vec<AcceptedCacheDenial>,
}

/// This holder belongs to one admitted operation, never to a task-local or a
/// global current caller. Its observer cannot acquire another owner/cache lock.
pub(super) struct Denials {
    request: Arc<Request>,
    lease: Arc<Lease>,
    active: AtomicBool,
    recipients: Mutex<Option<Recipients>>,
}

struct ClearRecipients<'a>(&'a Denials);
impl Drop for ClearRecipients<'_> {
    fn drop(&mut self) {
        self.0
            .recipients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.0.active.store(false, Ordering::Release);
    }
}

impl Denials {
    pub(super) fn new(request: &Arc<Request>, lease: &Arc<Lease>) -> Self {
        Self {
            request: request.clone(),
            lease: lease.clone(),
            active: AtomicBool::new(false),
            recipients: Mutex::new(None),
        }
    }

    pub(super) fn with_current(
        &self,
        action: &mut CacheAdmissionAction<'_>,
    ) -> credentials::Result<()> {
        if self.active.swap(true, Ordering::AcqRel) {
            return Err(RepositoryCredentialError::Indeterminate);
        }
        let _cleanup = ClearRecipients(self);
        let c = &self.request.base.connection;
        // Inventory stays locked until the response is applied. A new capture
        // therefore cannot slip between recipient selection and denial.
        let groups = c.resource.groups.lock().map_err(credential)?;
        let recipients = groups
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|group| {
                group.connection == self.lease.settled.selected().binding.scope
                    && group.instance == self.lease.settled.descriptor().instance().as_str()
                    && Arc::ptr_eq(&group.settings, &self.lease.settings.1)
                    && group.observations.is_active()
            })
            .collect();
        *self.recipients.lock().map_err(credential)? = Some(Recipients {
            groups: recipients,
            notices: Vec::new(),
        });
        let result = self
            .request
            .with_scope(&self.lease, action)
            .map_err(credential);
        let recipients = self.recipients.lock().map_err(credential)?.take();
        drop(groups);
        if let Some(recipients) = recipients {
            self.notify(recipients);
        }
        result
    }

    pub(super) fn observe(&self, denial: &GuardedCacheDenial<'_>) {
        let mut holder = self
            .recipients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(recipients) = holder.as_mut() else {
            return;
        };
        let mut applied = false;
        for group in &recipients.groups {
            applied |= denial.apply_to(&group.observations);
        }
        if applied {
            let notice = denial.notification();
            if !recipients.notices.contains(&notice) {
                recipients.notices.push(notice);
            }
        }
    }

    fn notify(&self, recipients: Recipients) {
        let c = &self.request.base.connection;
        let mut removed = Vec::new();
        let mut overflow = false;
        {
            let mut feed = c
                .resource
                .feed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if feed.closed {
                return;
            }
            for notice in recipients.notices {
                let connection = matches!(notice.coverage, CacheDenialCoverage::Connection);
                let ids: Vec<_> = feed
                    .leases
                    .iter()
                    .filter(|(_, lease)| {
                        (connection || lease.group.shared)
                            && recipients
                                .groups
                                .iter()
                                .any(|group| Arc::ptr_eq(group, &lease.group))
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
                if ids.is_empty() {
                    continue;
                }
                let Some(sequence) = feed
                    .sequence
                    .checked_add(1)
                    .filter(|_| feed.notices.len() < LIMIT)
                else {
                    overflow = true;
                    break;
                };
                feed.sequence = sequence;
                let event = match notice.coverage {
                    CacheDenialCoverage::Connection => {
                        // New requests cannot look these leases up. Their original
                        // guarded rejection may still be transferred exactly once.
                        for id in &ids {
                            if let Some(lease) = feed.leases.remove(id) {
                                removed.push(lease);
                            }
                        }
                        RepositoryResourceEvent::Retired(Retired {
                            read_lifetime_ids: ids,
                            sequence: sequence.to_string(),
                            all_retired: false,
                            terminal: false,
                        })
                    }
                    coverage => {
                        let coverage = match coverage {
                            CacheDenialCoverage::Project => {
                                RepositoryInvalidationCoverage::Project {}
                            }
                            CacheDenialCoverage::Resource(target) => {
                                RepositoryInvalidationCoverage::Resource { target }
                            }
                            CacheDenialCoverage::List(kind) => {
                                RepositoryInvalidationCoverage::List {
                                    resource_kind: match kind {
                                        RepositoryResourceKind::MergeRequest => {
                                            RepositoryListKind::MergeRequest
                                        }
                                        RepositoryResourceKind::Issue => RepositoryListKind::Issue,
                                        RepositoryResourceKind::PullRequest => continue,
                                    },
                                }
                            }
                            CacheDenialCoverage::Connection => unreachable!(),
                        };
                        RepositoryResourceEvent::Invalidated(RepositoryResourceInvalidated {
                            read_lifetime_ids: ids,
                            sequence: sequence.to_string(),
                            repository: notice.repository,
                            coverage,
                        })
                    }
                };
                feed.notices.push_back(event);
            }
        }
        drop(removed);
        if overflow {
            c.resource.close();
        }
        c.resource.notify.notify_waiters();
    }
}
