#!/usr/bin/env python3
"""Offline safety evidence; no test contacts GitHub or cancels a live run."""

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest


SCRIPT = Path(__file__).with_name("retire-obsolete-merge-runs.py")
SPEC = importlib.util.spec_from_file_location("retirement", SCRIPT)
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)
REPO = "intent-hq/intentd"
SHA = "a" * 40
REF = "gh-readonly-queue/main/pr-2400-" + "b" * 40


def run(number=10, **overrides):
    return dict(id=number, run_attempt=1, workflow_id=123,
                path=".github/workflows/ci.yml", event="merge_group",
                repository={"full_name": REPO}, head_repository={"full_name": REPO},
                head_branch=REF, head_sha=SHA, status="queued", conclusion=None,
                **overrides)


def queue_page(heads=(), more=False, cursor=None):
    return {"data": {"repository": {"mergeQueue": {"entries": {
        "nodes": [{"id": f"MQE_{i}", "headCommit": {"oid": sha} if sha else None,
                   "state": "AWAITING_CHECKS" if sha else "QUEUED"}
                  for i, sha in enumerate(heads)],
        "pageInfo": {"hasNextPage": more, "endCursor": cursor},
    }}}}}


def reference(sha=SHA, name=REF):
    return {"ref": "refs/heads/" + name, "object": {"type": "commit", "sha": sha}}


def evidence(*runs):
    runs = list(runs or [run()])
    return {"repository": REPO,
            "workflow": {"id": 123, "path": ".github/workflows/ci.yml"},
            "run_pages": [{"total_count": len(runs), "workflow_runs": runs}],
            "refreshed_runs": copy.deepcopy(runs), "refs": [],
            "queue_pages": [queue_page()]}


class FakeGitHub:
    """REST cancel is addressed by run ID, not a historical attempt."""

    def __init__(self, bundle):
        self.bundle = copy.deepcopy(bundle)
        self.mutations = []

    def snapshot(self):
        return copy.deepcopy(self.bundle)

    def cancel(self, run_id):
        self.mutations.append(("POST", f"/repos/{REPO}/actions/runs/{run_id}/cancel"))
        return self.bundle["refreshed_runs"][0]["run_attempt"]


class PolicyTests(unittest.TestCase):
    def assess(self, bundle):
        api = FakeGitHub(bundle)
        result = POLICY.assess(api.snapshot())
        self.assertFalse(result["cancellation_supported"])
        self.assertEqual(result["mode"], "read-only")
        self.assertTrue(all(d["action"] == "skip" for d in result["decisions"]))
        self.assertEqual(api.mutations, [])
        return result

    def reason(self, bundle):
        return self.assess(bundle)["decisions"][0]["reason"]

    def test_mixed_current_absent_and_release_runs(self):
        current, absent, release = run(10), run(20), run(30)
        current["head_sha"] = "c" * 40
        current["head_branch"] += "-replacement"
        release["event"] = "push"
        data = evidence(current, absent, release)
        data["refs"] = [reference(current["head_sha"], current["head_branch"])]
        data["queue_pages"] = [queue_page([current["head_sha"]])]
        result = self.assess(data)
        self.assertEqual([(d["id"], d["reason"]) for d in result["decisions"]],
                         [(10, "current-candidate"), (20, "candidate-not-observed"),
                          (30, "out-of-scope")])
        self.assertEqual(result["decisions"][1]["head_ref"], "refs/heads/" + REF)
        self.assertEqual(result["decisions"][1]["head_sha"], SHA)
        self.assertEqual(result["decisions"][1]["run_attempt"], 1)
        self.assertEqual(result["decisions"][1]["repository"], REPO)

    def test_repository_description_is_not_candidate_identity(self):
        data = evidence(run())
        data["refreshed_runs"][0]["repository"]["description"] = "Updated description"
        self.assertEqual(self.reason(data), "candidate-not-observed")

    def test_completed_and_historical_attempts_are_successful_no_action_cases(self):
        data = evidence(run())
        data["refreshed_runs"][0].update(status="completed", conclusion="cancelled")
        self.assertEqual(self.reason(data), "completed")
        # A known obsolete attempt is NOT a currently cancellable run ID.
        data["refreshed_runs"][0].update(run_attempt=2, status="queued", conclusion=None)
        self.assertEqual(self.reason(data), "historical-attempt")

    def test_delayed_duplicate_destroyed_hints_do_not_override_current_state(self):
        data = evidence(run())
        data["events"] = [{"action": "destroyed", "merge_group": {
            "head_ref": "refs/heads/" + REF, "head_sha": SHA}}] * 2
        data["queue_pages"] = [queue_page([SHA])]
        self.assertEqual(self.reason(data), "current-candidate")

    def test_same_ref_same_sha_and_same_sha_other_ref_are_protected(self):
        for name in (REF, REF + "-another-candidate"):
            with self.subTest(ref=name):
                data = evidence(run())
                data["refs"] = [reference(name=name)]
                self.assertEqual(self.reason(data), "ref-protects-sha")

    def test_replaced_ref_is_only_an_observation_not_cancellation_authority(self):
        data = evidence(run())
        data["refs"] = [reference("d" * 40)]
        self.assertEqual(self.reason(data), "candidate-ref-replaced")

    def test_unknown_queue_head_prevents_absence_claim(self):
        data = evidence(run())
        data["queue_pages"] = [queue_page([None])]
        self.assertEqual(self.reason(data), "unknown-candidate-activity")

    def test_exact_identity_and_allowlist(self):
        changes = {"repository": {"full_name": "other/repo"},
                   "head_repository": {"full_name": "other/repo"},
                   "workflow_id": 999, "path": ".github/workflows/release.yml",
                   "head_branch": "main", "event": "workflow_dispatch"}
        for field, value in changes.items():
            with self.subTest(field=field):
                target = run()
                target[field] = value
                self.assertEqual(self.reason(evidence(target)), "out-of-scope")
        for event in ("pull_request", "push", "schedule", "workflow_run"):
            target = run()
            target["event"] = event
            self.assertEqual(self.reason(evidence(target)), "out-of-scope")

    def test_refresh_identity_change_and_rerun_are_protected(self):
        for field, value in (("head_sha", "d" * 40), ("head_branch", REF + "-new"),
                             ("workflow_id", 999), ("id", 11), ("run_attempt", 2)):
            with self.subTest(field=field):
                data = evidence(run())
                data["refreshed_runs"][0][field] = value
                self.assertIn(self.reason(data), ("identity-changed", "historical-attempt", "unknown-run"))

    def test_late_discovery_and_second_queue_page(self):
        data = evidence(run(10), run(20))
        data["run_pages"] = [{"total_count": 2, "workflow_runs": [run(10)]},
                             {"total_count": 2, "workflow_runs": [run(20)]}]
        data["queue_pages"] = [queue_page([], True, "one"), queue_page([SHA], False, "two")]
        self.assertEqual([d["id"] for d in self.assess(data)["decisions"]], [10, 20])
        self.assertTrue(all(d["reason"] == "current-candidate" for d in self.assess(data)["decisions"]))

    def test_incomplete_changing_duplicate_or_excessive_pagination_is_unknown(self):
        for mutate in (
            lambda d: d["run_pages"][0].update(total_count=2),
            lambda d: d["run_pages"].append({"total_count": 2, "workflow_runs": [run(10)]}),
            lambda d: d["run_pages"][0].update(total_count=1001),
            lambda d: d.update(queue_pages=[queue_page([], True, "one")]),
            lambda d: d.update(queue_pages=[queue_page([], False), queue_page()]),
            lambda d: d.update(queue_pages=[queue_page([], True, None), queue_page()]),
        ):
            data = evidence(run())
            mutate(data)
            self.assertFalse(self.assess(data)["complete"])

    def test_api_errors_and_malformed_responses_never_mean_absence(self):
        for field in ("workflow", "refs", "queue_pages", "run_pages", "refreshed_runs"):
            for error in (None, {}, {"message": "Not Found", "status": 404},
                          {"message": "Bad credentials", "status": 401},
                          {"message": "rate limit exceeded", "status": 403},
                          {"message": "server error", "status": 502}):
                with self.subTest(field=field, error=error):
                    data = evidence(run())
                    data[field] = error
                    self.assertFalse(self.assess(data)["complete"])
        data = evidence(run())
        data["queue_pages"][0]["errors"] = [{"message": "partial response"}]
        self.assertFalse(self.assess(data)["complete"])

    def test_final_read_cannot_prevent_ref_reuse_after_that_read(self):
        api = FakeGitHub(evidence(run()))
        last_read = api.snapshot()
        api.bundle["refs"] = [reference()]
        api.bundle["queue_pages"] = [queue_page([SHA])]
        result = POLICY.assess(last_read)
        self.assertEqual(result["decisions"][0]["reason"], "candidate-not-observed")
        self.assertEqual(result["decisions"][0]["action"], "skip")
        self.assertEqual(api.mutations, [])
        # Counterexample to adding POST after any finite set of reads.
        api.cancel(10)
        self.assertEqual(api.mutations, [("POST", f"/repos/{REPO}/actions/runs/10/cancel")])
        self.assertEqual(api.bundle["queue_pages"][0]["data"]["repository"]["mergeQueue"]
                         ["entries"]["nodes"][0]["headCommit"]["oid"], SHA)

    def test_run_completes_and_is_rerun_between_read_and_cancel(self):
        api = FakeGitHub(evidence(run()))
        last_read = api.snapshot()
        api.bundle["refreshed_runs"][0].update(status="completed", conclusion="failure")
        api.bundle["refreshed_runs"][0].update(run_attempt=2, status="queued", conclusion=None)
        self.assertEqual(POLICY.assess(last_read)["decisions"][0]["action"], "skip")
        self.assertEqual(api.mutations, [])
        # Supported rerun lifecycle: the same endpoint now addresses attempt 2.
        self.assertEqual(api.cancel(10), 2)
        self.assertEqual(api.mutations, [("POST", f"/repos/{REPO}/actions/runs/10/cancel")])

    def test_cli_is_offline_and_has_no_apply_mode(self):
        result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), "--dry-run"],
                                input=json.dumps(evidence()), text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(json.loads(result.stdout)["cancellation_supported"])
        result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), "--apply"],
                                input="{}", text=True, capture_output=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)

    def test_malformed_cli_input_is_an_error_not_a_successful_empty_scan(self):
        for content in ("not JSON", "{}", "null", "[]"):
            result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT)],
                                    input=content, text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 2)
            output = json.loads(result.stdout)
            self.assertFalse(output["complete"])
            self.assertFalse(output["cancellation_supported"])
            self.assertEqual(output["decisions"], [])

    def test_unknown_status_missing_identity_and_older_attempt_are_not_eligible(self):
        for field, value in (("status", "new-status"), ("head_sha", None),
                             ("run_attempt", 0), ("run_attempt", True),
                             ("conclusion", "failure")):
            with self.subTest(field=field, value=value):
                data = evidence(run())
                data["refreshed_runs"][0][field] = value
                self.assertEqual(self.reason(data), "unknown-run")
        data = evidence(run())
        data["run_pages"][0]["workflow_runs"][0]["run_attempt"] = 2
        self.assertEqual(self.reason(data), "identity-changed")


if __name__ == "__main__":
    unittest.main()
