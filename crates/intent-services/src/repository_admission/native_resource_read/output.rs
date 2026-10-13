//! Retain original read admission through the actual immutable socket output.
//! A queued response is not permission to write after its source is retired.

use intent_core::repository_request::RepositoryPrivateOutput;

use super::*;

pub(super) fn bind(r: &Request, frame: &str) -> Result<Option<Arc<dyn RepositoryPrivateOutput>>> {
    let state = r.state.lock().map_err(denied)?.clone();
    match state {
        State::Public => Ok(None),
        State::Capture(_) | State::Read(..) => Ok(Some(Arc::new(Output {
            connection: r.base.connection.clone(),
            state,
            target: r.frame.resource_target().cloned(),
            admitted: r.output_admitted.clone(),
            deadline: r.deadline,
            frame: frame.into(),
        }))),
        State::Empty | State::Reading(_) => Err(unavailable()),
    }
}

struct Output {
    connection: Arc<Connection>,
    state: State,
    target: Option<ReviewTarget>,
    admitted: Arc<AtomicBool>,
    deadline: Instant,
    frame: String,
}

impl RepositoryPrivateOutput for Output {
    fn frame(&self) -> &str {
        &self.frame
    }

    fn with_current(&self, transfer: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        if !self.admitted.load(Ordering::Acquire) || Instant::now() >= self.deadline {
            return Err(unavailable());
        }
        let lease = match &self.state {
            State::Capture(lease) | State::Read(lease, ..) => lease,
            _ => return Err(unavailable()),
        };
        lease.check()?;
        let mut transferred = None;
        let mut guarded = |check: &crate::pr_monitor::qualified_cache::CacheRevalidationCheck<
            '_,
        >| {
            self.connection.parent.native_dispatch(|| {
                lease.lifetime.retirement().native_dispatch(|| {
                    if Instant::now() >= lease.deadline || Instant::now() >= self.deadline {
                        return Err(AdmissionError::Retired);
                    }
                    lease.settings.0.with_original_snapshot(&lease.settings.1, |current| {
                        if !current {
                            return Err(AdmissionError::Retired);
                        }
                        let mut action = || {
                            check()?;
                            transferred = Some(transfer());
                            Ok(())
                        };
                        Ok(match &self.state {
                            State::Capture(_) => RepositoryConnectionFacts::with_native_context_current(Some(&lease.facts), |current| {
                                if current { action() } else { Err(RepositoryCredentialError::Retired) }
                            }),
                            State::Read(_, eligibility, response, failure, _, collection) => {
                                if *failure {
                                    if let Some(response) = response {
                                        let mut accepted_rejection = false;
                                        with_response(eligibility, response, self.target.as_ref(), collection.as_deref(), &mut |disposition| {
                                            accepted_rejection = disposition == RepositoryResponseDisposition::AcceptedCredentialRejection;
                                            if matches!(disposition, RepositoryResponseDisposition::NotApplied | RepositoryResponseDisposition::Unattributed | RepositoryResponseDisposition::Indeterminate) {
                                                return Err(RepositoryCredentialError::Retired);
                                            }
                                            if accepted_rejection { action()?; }
                                            Ok(())
                                        }).map_err(|_| AdmissionError::Retired)?;
                                        if accepted_rejection { return Ok(Ok(())); }
                                    }
                                }
                                eligibility.with_current(&mut action)
                            }
                            _ => Err(RepositoryCredentialError::Retired),
                        })
                    })
                })
            }).map_err(|_| RepositoryCredentialError::Retired)?
        };
        if let State::Read(_, _, _, false, Some(delivery), _) = &self.state {
            delivery.with_revalidation(&mut guarded).map_err(denied)?;
        } else {
            guarded(&|| Ok(())).map_err(denied)?;
        }
        transferred.ok_or_else(unavailable)?
    }
}
