//! Resume the agents a stop interrupted, once the backend serves again: the
//! daemon at start and an orchestrator each time it claims a tenant run the
//! same sweep.
//!
//! The candidates are captured (`Services::prepare_startup_resume`) before
//! any client is served, so a client never offers a manual resume for an
//! agent the sweep is about to resume. Each resume claims the agent's
//! pending `interrupted_agent` row atomically, and on Postgres that write
//! needs the tenant's lease: an instance that lost the tenant cannot resume
//! what the new owner resumes.

use intent_core::settings_file::ResumeInterruptedOnStart;
use intent_core::AgentId;
use intent_services::{Services, StartupResumeCandidates};
use tokio::sync::watch;

/// Whether a claimed tenant resumes its interrupted agents: always, unless
/// `agents.resumeInterruptedOnStart` is `off`. An orchestrator has no
/// display, so `auto` resumes, as it does on a headless daemon.
#[must_use]
pub fn resume_on_claim(setting: ResumeInterruptedOnStart) -> bool {
    !matches!(setting, ResumeInterruptedOnStart::Off)
}

/// Resume `candidates` one at a time until `stopping` turns true. Runs off
/// the listener path; a stop skips the candidates not started yet, and an
/// admitted resume finishes, so its durable claim is never stranded.
/// Provider turns themselves stay owned by the agent manager.
///
/// # Panics
/// When the `INTENTD_TEST_STARTUP_RESUME_GATE` test seam names a socket
/// that cannot be connected (tests only; production never sets it).
pub async fn run_startup_resume_sweep(
    services: &Services,
    candidates: StartupResumeCandidates,
    stopping: watch::Receiver<bool>,
) {
    #[cfg(unix)]
    let mut stopping = stopping;
    let ids = candidates.ids().to_vec();
    tracing::info!(
        count = ids.len(),
        "resume-on-start: resuming interrupted agents"
    );
    // Deterministic e2e seam, including release-mode tests. Holding a provider
    // handshake would not hold the sweep, since provider turns are background.
    #[cfg(unix)]
    if let Some(path) = std::env::var_os("INTENTD_TEST_STARTUP_RESUME_GATE") {
        use tokio::io::AsyncReadExt;
        tokio::select! {
            biased;
            _ = stopping.wait_for(|stop| *stop) => return,
            () = async {
                let mut gate = tokio::net::UnixStream::connect(path).await.expect("resume test gate");
                let _ = gate.read_u8().await;
            } => {},
        }
    }
    let candidates = &candidates;
    resume_startup_candidates(ids, &stopping, |agent_id| async move {
        services
            .resume_startup_candidate(candidates, &agent_id)
            .await
    })
    .await;
}

/// Never cancel the callback after admitting a candidate: it may already have
/// claimed the interruption or persisted its continuation. A stop skips every
/// later candidate, while the caller joins this loop before manager teardown.
pub async fn resume_startup_candidates<F, Fut>(
    ids: Vec<AgentId>,
    stopping: &watch::Receiver<bool>,
    mut resume: F,
) where
    F: FnMut(AgentId) -> Fut,
    Fut: std::future::Future<Output = intent_core::Result<()>>,
{
    let mut resumed = 0;
    let mut failed = 0;
    for agent_id in ids {
        if *stopping.borrow() {
            break;
        }
        match resume(agent_id.clone()).await {
            Ok(()) => {
                tracing::info!(%agent_id, "resume-on-start: resumed agent");
                resumed += 1;
            }
            Err(error) => {
                tracing::warn!(%agent_id, %error, "resume-on-start: failed to resume agent");
                failed += 1;
            }
        }
    }
    tracing::info!(
        resumed,
        failed,
        "resume-on-start: auto-resume sweep complete"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn startup_recovery_shutdown_drains_admitted_operation_and_skips_next() {
        let (stop, stopping) = watch::channel(false);
        let (arrived, arrival) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let sweep = tokio::spawn(async move {
            let mut barriers = Some((arrived, released));
            resume_startup_candidates(
                vec![AgentId::from("first"), AgentId::from("second")],
                &stopping,
                move |_| {
                    let (arrived, released) =
                        barriers.take().expect("no second admission after stop");
                    let calls = calls.clone();
                    async move {
                        arrived.send(()).unwrap();
                        released.await.unwrap();
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
            .await;
        });
        arrival.await.unwrap();
        stop.send(true).unwrap();
        assert!(
            !sweep.is_finished(),
            "the admitted operation is still owned and draining"
        );
        release.send(()).unwrap();
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(1)),
            sweep,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_claimed_tenant_resumes_unless_the_setting_is_off() {
        assert!(resume_on_claim(ResumeInterruptedOnStart::Auto));
        assert!(resume_on_claim(ResumeInterruptedOnStart::On));
        assert!(!resume_on_claim(ResumeInterruptedOnStart::Off));
    }
}
