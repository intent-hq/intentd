//! intent#5881: captured Qwen inputs through the production GitHub parser.
//! These are regression expectations, deliberately red before the fix.

#[path = "support/qwen.rs"]
mod qwen;

use intent_sourcecontrol::{RepoRef, SourceControl};
use qwen::{MockQwen, ReadMode};

#[tokio::test]
async fn qwen_40_records_survive_both_graphql_reads_and_rest_reordering() {
    let mock = MockQwen::start(10978).await;
    let repo = RepoRef::new("QwenLM", "qwen-code");
    for mode in [ReadMode::Folded, ReadMode::Standalone, ReadMode::Rest] {
        mock.edit(|s| {
            s.mode = mode;
            s.nodes.reverse();
        });
        let before = mock.calls("/graphql");
        if mode == ReadMode::Folded {
            let observation = mock.sc.pr_observation(&repo, 10978).await.unwrap().unwrap();
            assert!(observation.signals.checks_known);
            assert_eq!(observation.signals.checks.len(), 40);
        } else if mode == ReadMode::Standalone {
            let signals = mock.sc.merge_requirements(&repo, 10978).await.unwrap();
            assert!(signals.checks_known);
            assert_eq!(signals.checks.len(), 40);
        } else {
            let runs = mock
                .sc
                .check_runs(&repo, "5f95d4661d6cd87bafd03bcdf76eaf5fd99f4d0c")
                .await
                .unwrap();
            assert_eq!(runs.len(), 40);
            assert!(runs.iter().all(|r| r.started_at.is_some()));
        }
        if mode != ReadMode::Rest {
            assert!(mock.calls("/graphql") > before);
        }
    }
}

#[tokio::test]
async fn qwen_136_folded_observation_reads_every_context() {
    let mock = MockQwen::start(11506).await;
    let observation = mock
        .sc
        .pr_observation(&RepoRef::new("QwenLM", "qwen-code"), 11506)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        observation.signals.checks.len(),
        136,
        "the captured rollup has 36 records on page two"
    );
    assert!(observation.signals.checks_known);
    assert!(mock.calls("/graphql") >= 2, "must request the second page");
}

#[tokio::test]
async fn qwen_136_standalone_requirements_read_every_context() {
    let mock = MockQwen::start(11506).await;
    mock.edit(|s| s.mode = ReadMode::Standalone);
    let signals = mock
        .sc
        .merge_requirements(&RepoRef::new("QwenLM", "qwen-code"), 11506)
        .await
        .unwrap();
    assert_eq!(
        signals.checks.len(),
        136,
        "the captured rollup has 36 records on page two"
    );
    assert!(signals.checks_known);
    assert!(mock.calls("/graphql") >= 2, "must request the second page");
}

#[tokio::test]
async fn qwen_rest_fallback_reads_both_pages_and_surfaces_failure() {
    let mock = MockQwen::start(11506).await;
    let repo = RepoRef::new("QwenLM", "qwen-code");
    let head = "5130e78720f4ceec6ac4702e406ad5f36dc827c9";
    let runs = mock.sc.check_runs(&repo, head).await.unwrap();
    assert_eq!(runs.len(), 136);
    assert_eq!(mock.calls("/check-runs"), 2);
    mock.edit(|s| {
        s.mode = ReadMode::Degraded;
        s.nodes.reverse();
    });
    assert!(mock.sc.check_runs(&repo, head).await.is_err());
}
