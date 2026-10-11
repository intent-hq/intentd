//! Actual legacy producer rows shared with the desktop service/row regressions.
//! Regenerate with `INTENTD_UPDATE_GOLDENS=1 cargo test -p intent-services --lib`
//! filtered to `pr_monitor::tests::compact_required_checks::compact_required_checks_golden`.
use super::*;

const GOLDEN: &str = "src/pr_monitor/tests/fixtures/compact-required-checks.json";

fn check(name: &str, state: CheckState, required: bool) -> RollupCheck {
    RollupCheck {
        name: name.into(),
        kind: RollupCheckKind::CheckRun,
        state,
        is_required: required,
        url: None,
        started_at: None,
    }
}

fn producer_case(name: &str, checks: Vec<RollupCheck>, known: bool) -> Value {
    let forge = ForgeState {
        checks,
        approvals: vec!["reviewer".into()],
        ..Default::default()
    };
    let mut signals = forge.signals();
    signals.branch_rules = Some(stub_branch_rules());
    let fallback: Vec<_> = forge
        .checks
        .iter()
        .map(|c| CheckRun {
            name: c.name.clone(),
            state: c.state,
            url: c.url.clone(),
            started_at: c.started_at.clone(),
        })
        .collect();
    let requirements = pr_ops::merge_requirements(
        &forge.pr_record(42),
        known.then_some(&signals),
        &fallback,
        &pr_ops::aggregate_reviews(&forge.reviews()),
        known.then_some(0),
    );
    let mut baseline = snapshot(|s| s.requirements = requirements.clone());
    if name == "unknown-threads" {
        baseline.requirements.threads.unresolved = None;
    }
    let mut monitor = crate::v1_goldens::pr_monitor_row();
    monitor.last_snapshot = Some(serde_json::to_string(&baseline).unwrap());
    json!({
        "name": name,
        "monitor": pr_monitor_wire(&monitor, None),
        "requirements": baseline.requirements,
    })
}

fn producer_cases() -> Value {
    use CheckState::{Failure, Pending};
    let mut cases = vec![
        producer_case("empty", vec![], true),
        producer_case("pending-build", vec![check("build", Pending, true)], true),
        producer_case(
            "multiple-pending",
            vec![check("build", Pending, true), check("e2e", Pending, true)],
            true,
        ),
        producer_case(
            "mixed-required-optional",
            vec![
                check("test", Failure, true),
                check("lint", Failure, true),
                check("build", Pending, true),
                check("e2e", Pending, true),
                check("optional-fail", Failure, false),
                check("optional-pending", Pending, false),
            ],
            true,
        ),
        producer_case(
            "optional-only",
            vec![
                check("optional-fail", Failure, false),
                check("optional-pending", Pending, false),
            ],
            true,
        ),
        producer_case(
            "unknown-required",
            vec![check("test", Failure, true), check("build", Pending, true)],
            false,
        ),
        producer_case("unknown-empty", vec![], false),
        producer_case("unknown-threads", vec![], true),
    ];
    let mut monitor = crate::v1_goldens::pr_monitor_row();
    cases.push(json!({"name": "missing-baseline", "monitor": pr_monitor_wire(&monitor, None)}));
    monitor.last_error =
        Some("rate limited; PR monitor polling paused until 2026-01-02T04:00:00Z".into());
    cases.push(json!({
        "name": "paused-missing-baseline",
        "monitor": pr_monitor_wire(&monitor, Some("2026-01-02T04:00:00Z")),
    }));
    json!({"cases": cases})
}

#[test]
fn compact_required_checks_golden() {
    let actual = producer_cases();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(GOLDEN);
    let bytes = format!("{}\n", serde_json::to_string_pretty(&actual).unwrap());
    if std::env::var_os("INTENTD_UPDATE_GOLDENS").is_some_and(|v| v == "1") {
        std::fs::write(&path, &bytes).unwrap();
    }
    assert_eq!(std::fs::read_to_string(path).unwrap(), bytes);
}

#[test]
fn compact_required_checks_are_counts_while_full_requirements_keep_names() {
    let actual = producer_cases();
    let cases = actual["cases"].as_array().unwrap();
    for (case, (failed, pending, total_failed, total_pending, known)) in cases.iter().zip([
        (0, 0, 0, 0, true),
        (0, 1, 0, 1, true),
        (0, 2, 0, 2, true),
        (2, 2, 3, 3, true),
        (0, 0, 1, 1, true),
        (0, 0, 1, 1, false),
        (0, 0, 0, 0, false),
        (0, 0, 0, 0, true),
    ]) {
        let compact = &case["monitor"]["lastSnapshot"]["checks"];
        let full = &case["requirements"]["checks"];
        assert_eq!(
            compact["failingRequired"].as_u64(),
            Some(failed),
            "{}: {compact}",
            case["name"]
        );
        assert_eq!(
            compact["pendingRequired"].as_u64(),
            Some(pending),
            "{}: {compact}",
            case["name"]
        );
        assert_eq!(compact["failed"], total_failed);
        assert_eq!(compact["pending"], total_pending);
        assert_eq!(compact["requiredKnown"], known);
        assert_eq!(
            full["failingRequired"].as_array().unwrap().len() as u64,
            failed
        );
        assert_eq!(
            full["pendingRequired"].as_array().unwrap().len() as u64,
            pending
        );
        assert_eq!(full["requiredKnown"], known);
        assert!(case["monitor"].get("pausedUntil").is_none());
    }
    assert_eq!(
        cases[1]["requirements"]["checks"]["pendingRequired"],
        json!(["build"])
    );
    assert_eq!(
        cases[3]["requirements"]["checks"]["failingRequired"],
        json!(["test", "lint"])
    );
    assert_eq!(
        cases[3]["requirements"]["checks"]["pendingRequired"],
        json!(["build", "e2e"])
    );
    let unknown = &cases[6];
    assert_eq!(unknown["monitor"]["lastSnapshot"]["rulesKnown"], false);
    assert_eq!(
        unknown["monitor"]["lastSnapshot"]["approvals"].get("needed"),
        Some(&Value::Null)
    );
    assert_eq!(
        unknown["monitor"]["lastSnapshot"]["threads"].get("resolutionRequired"),
        Some(&Value::Null)
    );
    assert!(unknown["requirements"]["approvals"].get("needed").is_none());
    assert!(unknown["requirements"]["threads"]
        .get("resolutionRequired")
        .is_none());
    for index in [5, 6, 7] {
        assert!(cases[index]["monitor"]["lastSnapshot"]["threads"]
            .get("unresolved")
            .is_none());
    }
    for case in &cases[8..] {
        for key in ["lastSnapshot", "title", "url"] {
            assert!(case["monitor"].get(key).is_none(), "{case}");
        }
    }
    assert!(cases[8]["monitor"].get("pausedUntil").is_none());
    assert_eq!(cases[9]["monitor"]["pausedUntil"], "2026-01-02T04:00:00Z");
}
