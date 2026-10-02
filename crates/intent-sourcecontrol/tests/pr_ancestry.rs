//! The comparison adapter uses live, immutable revisions and one HTTP attempt.
#[path = "support/qwen.rs"]
mod qwen;

use intent_sourcecontrol::{Error, PrAncestry, RepoRef, SourceControl};
use qwen::MockQwen;
use serde_json::json;

const BASE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn live_base_and_fork_identity_compare_in_base_repository() {
    let mock = MockQwen::start(10978).await;
    mock.edit(|s| {
        s.pr["baseRefOid"] = json!("cccccccccccccccccccccccccccccccccccccccc");
        s.pr["baseRef"] = json!({"target":{"oid":BASE}});
        s.pr["headRepository"] = json!({"id":"fork-id"});
        s.compare = json!({"behind_by":17,"status":"diverged","commits":[{}]});
    });
    let repo = RepoRef::new("base", "repo");
    let observation = mock.sc.pr_observation(&repo, 10978).await.unwrap().unwrap();
    let identity = observation.ancestry_identity.unwrap();
    assert_eq!(identity.base_sha, BASE);
    assert_eq!(identity.head_repository.as_deref(), Some("fork-id"));
    assert_eq!(
        mock.calls("/compare/"),
        0,
        "observation alone never compares"
    );
    let measured = mock.sc.pr_ancestry(&repo, &identity).await.unwrap();
    assert_eq!(
        serde_json::to_value(&measured).unwrap(),
        json!({
            "status":"known","baseSha":BASE,"headSha":identity.head_sha,"behindBy":17
        })
    );
    let path = format!(
        "/repos/base/repo/compare/{BASE}...{}?per_page=1&page=1",
        identity.head_sha
    );
    assert_eq!(mock.calls(&path), 1);
    let mut invalid = identity.clone();
    invalid.base_sha = "main/../../branch".into();
    assert_eq!(
        mock.sc.pr_ancestry(&repo, &invalid).await.unwrap(),
        PrAncestry::Unknown
    );
    assert_eq!(
        mock.calls("/compare/"),
        1,
        "never compare symbolic or malformed revisions"
    );
}

#[tokio::test]
async fn comparison_has_no_error_retries_and_preserves_quota_errors() {
    let mock = MockQwen::start(10978).await;
    mock.edit(|s| s.pr["baseRef"] = json!({"target":{"oid":BASE}}));
    let repo = RepoRef::new("base", "repo");
    let identity = mock
        .sc
        .pr_observation(&repo, 10978)
        .await
        .unwrap()
        .unwrap()
        .ancestry_identity
        .unwrap();
    for (status, message, quota) in [
        (404, "missing fork commit", false),
        (500, "server unavailable", false),
        (403, "forbidden", false),
        (403, "API rate limit exceeded", true),
        (429, "too many requests", true),
    ] {
        mock.edit(|s| {
            s.compare_status = status;
            s.compare = json!({"message":message});
        });
        let before = mock.calls("/compare/");
        let error = mock.sc.pr_ancestry(&repo, &identity).await.unwrap_err();
        assert_eq!(matches!(error, Error::RateLimited(_)), quota, "{error:?}");
        assert_eq!(
            mock.calls("/compare/"),
            before + 1,
            "one attempt even on server failure"
        );
    }
}

#[test]
fn ancestry_serialization_rejects_invalid_counts_and_keeps_unknown_distinct() {
    assert_eq!(
        serde_json::to_value(PrAncestry::default()).unwrap(),
        json!({"status":"unknown"})
    );
    for count in [json!(-1), json!(null), json!("0"), json!(1.2)] {
        assert!(serde_json::from_value::<PrAncestry>(json!({
            "status":"known","baseSha":BASE,"headSha":BASE,"behindBy":count
        }))
        .is_err());
    }
}
