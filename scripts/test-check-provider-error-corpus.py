#!/usr/bin/env python3
"""Offline tests for scripts/check-provider-error-corpus.py.

The package snippets below copy the shapes of the pinned adapters' real code
(claude-agent-acp dist, Claude Agent SDK sdk.d.ts / sdk.mjs, codex-acp
dist/index.js), so the extraction is exercised without network access.
"""

import copy
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "check_provider_error_corpus", Path(__file__).with_name("check-provider-error-corpus.py"))
check_corpus = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_corpus)

CORPUS = json.loads((ROOT / check_corpus.CORPUS).read_text())

SESSION_FAILURE_JS = '''
export function providerFailureCategory(errorKind, isUsageLimit = false) {
    if (isUsageLimit)
        return "quota_exhausted";
    switch (errorKind) {
        case "authentication_failed":
        case "oauth_org_not_allowed":
            return "auth_required";
        case "billing_error":
        case "account_on_hold":
            return "quota_exhausted";
        case "verification_required":
        case "cloud_credential_error":
            return "access_denied";
        case "rate_limit":
            return "rate_limited";
        case "overloaded":
            return "overloaded";
        case "invalid_request":
        case "model_not_found":
            return "bad_request";
        case "max_output_tokens":
            return "context_exhausted";
        case "server_error":
        case "unknown":
        case undefined:
            return "provider_error";
        default:
            return "provider_error";
    }
}
'''
ACP_AGENT_JS = '''
await failActiveWithSessionFailure("worker_shutdown", internalErrorForClient({ errorKind: "worker_shutdown" }));
await failActiveWithSessionFailure("internal_error", RequestError.internalError(errorKindData("no_result"), TURN_NO_RESULT_MESSAGE));
await failActiveWithSessionFailure("transport_lost", internalErrorForClient({ errorKind: "transport_lost" }));
'''
SDK_TYPES = (
    "export declare type SDKAssistantMessageError = 'authentication_failed' | "
    "'oauth_org_not_allowed' | 'account_on_hold' | 'verification_required' | "
    "'billing_error' | 'rate_limit' | 'overloaded' | 'invalid_request' | "
    "'model_not_found' | 'server_error' | 'unknown' | 'max_output_tokens' | "
    "'cloud_credential_error';\n"
)
PREFIXES = [
    "You've hit your", "You've reached your", "You're out of usage credits",
    "Your org is out of usage · add funds to continue",
    "Your org is out of usage · contact your admin",
    "Your seat type doesn't include usage credits", "Your seat type doesn't include usage",
    "Your usage allocation has been disabled by your admin",
    "Your group's usage limit is set to $0", "Fable 5 requires usage credits",
    "You're out of extra usage", "Your seat type doesn't include extra usage",
]


def quote(prefix):
    # Like the real sdk.d.ts: single quotes unless the text holds an apostrophe.
    return json.dumps(prefix, ensure_ascii=False) if "'" in prefix else f"'{prefix}'"


def sdk_types(prefixes, union=SDK_TYPES):
    tuple_type = ", ".join(quote(p) for p in prefixes)
    return (union
            + "export declare const ORG_POLICY_LIMIT_PREFIXES: readonly "
              "['This service is disabled for your org'];\n"
            + f"export declare const USAGE_LIMIT_ERROR_PREFIXES: readonly [{tuple_type}];\n")


CODEX_INDEX_JS = '''
var STRING_CODEX_ERROR_CATEGORIES = {
  contextWindowExceeded: "context_exhausted",
  sessionBudgetExceeded: "budget_exhausted",
  usageLimitExceeded: "quota_exhausted",
  rateLimitExceeded: "rate_limited",
  flexUnavailable: "provider_error",
  serverOverloaded: "overloaded",
  cyberPolicy: "policy_denied",
  misalignmentPolicyViolation: "policy_denied",
  tooManyDenials: "provider_error",
  internalServerError: "internal_error",
  unauthorized: "auth_required",
  badRequest: "bad_request",
  threadRollbackFailed: "provider_error",
  sandboxError: "provider_error",
  other: "provider_error"
};
var STRUCTURED_CODEX_ERROR_CATEGORIES = {
  httpConnectionFailed: "transport_lost",
  responseStreamConnectionFailed: "transport_lost",
  responseStreamDisconnected: "transport_lost",
  responseTooManyFailedAttempts: "transport_lost",
  activeTurnNotSteerable: "provider_error"
};
var CodexEventHandler = class {
  async createErrorEvent(params) {
    if (error62 === "usageLimitExceeded") {
      this.failure = RequestError.internalError(
        this.createTurnErrorData(params.error)
      );
    }
  }
};
'''


def run_check(corpus=CORPUS, adapter_js=ACP_AGENT_JS, union=SDK_TYPES,
              prefixes=PREFIXES, codex=CODEX_INDEX_JS, session_failure=SESSION_FAILURE_JS):
    return check_corpus.check(
        corpus,
        {"dist/session-failure-extension.js": session_failure, "dist/acp-agent.js": adapter_js},
        sdk_types(prefixes, union), codex)


class CheckProviderErrorCorpus(unittest.TestCase):
    def test_current_corpus_matches_the_pinned_adapter_shapes(self):
        self.assertEqual(run_check(), [])

    def test_new_sdk_error_kind_fails(self):
        problems = run_check(union=SDK_TYPES.replace(";", " | 'plan_exhausted';"))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("`plan_exhausted`", problems[0])
        self.assertIn("does not classify it", problems[0])

    def test_new_adapter_own_error_kind_fails(self):
        problems = run_check(adapter_js=ACP_AGENT_JS + 'internalErrorForClient({ errorKind: "spend_cap" })')
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("`spend_cap`", problems[0])

    def test_new_codex_kinds_fail(self):
        codex = CODEX_INDEX_JS.replace(
            'other: "provider_error"', 'other: "provider_error",\n  creditsExhausted: "quota_exhausted"'
        ).replace(
            'activeTurnNotSteerable: "provider_error"',
            'activeTurnNotSteerable: "provider_error",\n  quotaGateway: "provider_error"')
        problems = run_check(codex=codex)
        self.assertEqual(len(problems), 2, problems)
        self.assertIn("codexErrorInfo `creditsExhausted`", problems[0])
        self.assertIn("structured codexErrorInfo `quotaGateway`", problems[1])

    def test_removed_kind_fails(self):
        problems = run_check(codex=CODEX_INDEX_JS.replace('  sandboxError: "provider_error",\n', ""))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("no longer emits", problems[0])

    def test_adapter_moving_a_kind_into_quota_fails(self):
        moved = SESSION_FAILURE_JS.replace(
            'case "overloaded":\n            return "overloaded";',
            'case "overloaded":\n            return "rate_limited";')
        problems = run_check(session_failure=moved)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("`overloaded` under rate_limited", problems[0])

    def test_divergence_note_accepts_a_deliberate_disagreement(self):
        corpus = copy.deepcopy(CORPUS)
        corpus["kinds"]["codex"]["values"]["rateLimitExceeded"] = {
            "expect": "not-quota", "divergence": "deliberate in this test"}
        self.assertEqual(run_check(corpus=corpus), [])
        del corpus["kinds"]["codex"]["values"]["rateLimitExceeded"]["divergence"]
        self.assertEqual(len(run_check(corpus=corpus)), 1)

    def test_new_usage_limit_prefix_without_rendering_fails(self):
        problems = run_check(prefixes=PREFIXES + ["Your plan's pooled allowance is spent"])
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("Your plan's pooled allowance is spent", problems[0])

    def test_prefix_tuple_parses_both_quote_styles_and_brackets(self):
        types = ("export declare const USAGE_LIMIT_ERROR_PREFIXES: readonly "
                 "['Your org is out of usage · contact your admin', \"You've [briefly] hit\"];")
        self.assertEqual(check_corpus.claude_usage_limit_prefixes(types),
                         ["Your org is out of usage · contact your admin", "You've [briefly] hit"])

    def test_dropping_the_usage_limit_override_refuses(self):
        dropped = SESSION_FAILURE_JS.replace(
            '    if (isUsageLimit)\n        return "quota_exhausted";\n', "")
        self.assertNotEqual(dropped, SESSION_FAILURE_JS)
        with self.assertRaises(check_corpus.Refusal):
            run_check(session_failure=dropped)

    def test_codex_raising_another_kind_fails(self):
        codex = CODEX_INDEX_JS.replace(
            "var CodexEventHandler = class {",
            "var CodexEventHandler = class {\n  f() {\n"
            '    if (error62 === "rateLimitExceeded") {\n'
            "      this.failure = RequestError.internalError(data);\n    }\n  }")
        problems = run_check(codex=codex)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("['rateLimitExceeded', 'usageLimitExceeded']", problems[0])

    def test_network_failure_is_its_own_exit(self):
        self.assertTrue(issubclass(check_corpus.NetworkFailure, check_corpus.Refusal))
        self.assertEqual(check_corpus.NETWORK_EXIT, 75)
        urlopen, sleep = check_corpus.urllib.request.urlopen, check_corpus.time.sleep

        def unreachable(*args, **kwargs):
            raise OSError("registry unreachable")
        check_corpus.urllib.request.urlopen = unreachable
        check_corpus.time.sleep = lambda seconds: None
        try:
            with self.assertRaises(check_corpus.NetworkFailure):
                check_corpus.fetch("https://registry.npmjs.org/x")
        finally:
            check_corpus.urllib.request.urlopen, check_corpus.time.sleep = urlopen, sleep

    def test_unreadable_packages_refuse_instead_of_passing(self):
        with self.assertRaises(check_corpus.Refusal):
            run_check(union="export declare type Other = 'x';\n")
        with self.assertRaises(check_corpus.Refusal):
            run_check(session_failure="export function other() {}\n")
        with self.assertRaises(check_corpus.Refusal):
            check_corpus.claude_usage_limit_prefixes(SDK_TYPES)
        with self.assertRaises(check_corpus.Refusal):
            run_check(codex=CODEX_INDEX_JS.replace("error62 ===", "error62 !=="))
        with self.assertRaises(check_corpus.Refusal):
            run_check(codex="var STRING_CODEX_ERROR_CATEGORIES = {};")

    def test_corpus_entries_are_well_formed(self):
        for adapter, table in CORPUS["kinds"].items():
            for name in ("values", "structuredValues"):
                for kind, entry in table.get(name, {}).items():
                    self.assertIn(entry.get("expect"), ("quota", "not-quota"), (adapter, kind))
        for index, rendering in enumerate(CORPUS["renderings"]):
            self.assertIn(rendering.get("adapter"), CORPUS["kinds"], index)
            self.assertIn(rendering.get("expect"), ("quota", "not-quota"), index)
            self.assertIsInstance(rendering.get("source"), str, index)
            self.assertIsInstance(rendering.get("message"), str, index)


if __name__ == "__main__":
    unittest.main()
