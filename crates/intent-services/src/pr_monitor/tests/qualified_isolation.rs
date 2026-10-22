//! Real GitLab HTTP stalls must not hold the existing GitHub poll lane.
use super::*;
use crate::repository_read_source::tests::{
    routing_json, routing_options, run, ActualRead, ReadServer,
};

const MR: &str = "/api/v4/projects/group%2Fproject/merge_requests/4";

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_slow_or_failed_gitlab_preserves_healthy_github_progress() {
    for failure in ["slow", "outage", "quota"] {
        let provider = ReadServer::new().await;
        let fixture = ActualRead::new(&provider).await;
        routing_options(&fixture);
        let registered =
            routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
        assert_eq!(registered["ok"], true);
        let gitlab_id = PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
        let forge = StubForge::new();
        let service = fixture
            .auth
            .service
            .as_ref()
            .clone()
            .with_source_control(Arc::new(forge.clone()));
        let github = service
            .pr_monitor_register(
                &fixture.git.workspace.id,
                &fixture.agent,
                "group",
                "project",
                4,
            )
            .await
            .unwrap()
            .0;
        assert_ne!(
            github.monitor_id, gitlab_id,
            "colliding numeric identities stay separate"
        );
        let baseline = service.store.get_pr_monitor(&gitlab_id).await.unwrap();
        let fetched = forge.fetches();
        match failure {
            "slow" => provider.pause(MR),
            "outage" => provider.status(MR, 503),
            "quota" => provider.status(MR, 429),
            _ => unreachable!(),
        }
        let polling = tokio::spawn({
            let service = service.clone();
            async move {
                service.poll_pr_monitors().await;
            }
        });
        if failure == "slow" {
            provider.entered().await;
            tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_secs(10)),
                async {
                    while forge.fetches() == fetched {
                        tokio::task::yield_now().await;
                    }
                },
            )
            .await
            .unwrap();
            assert!(!polling.is_finished(), "GitLab request must still be held");
            // A second sweep cannot wait on the occupied GitLab lane.
            tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_secs(10)),
                service.poll_pr_monitors(),
            )
            .await
            .unwrap();
            assert!(forge.fetches() >= fetched + 2);
            provider.resume();
        }
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            polling,
        )
        .await
        .unwrap()
        .unwrap();
        if failure != "slow" {
            service.poll_pr_monitors().await;
            assert!(forge.fetches() >= fetched + 2);
        }
        assert!(!service.sweeps_rate_limited());
        let github_after = service
            .store
            .get_pr_monitor(&github.monitor_id)
            .await
            .unwrap();
        assert!(github_after.last_error.is_none());
        assert_ne!(github_after.last_polled_at, github.last_polled_at);
        let gitlab_after = service.store.get_pr_monitor(&gitlab_id).await.unwrap();
        assert_eq!(gitlab_after.baseline_snapshot, baseline.baseline_snapshot);
        assert!(gitlab_after.pending_changes.is_empty());
        assert_ne!(gitlab_after.last_snapshot, github_after.last_snapshot);
    }
}
