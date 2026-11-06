#!/usr/bin/env node
// Tests for the nightly payload-sweep reporter (scripts/nightly-payload-*.mjs)
// and its contract with the sweep and the two nightly workflows.
// Run: ./scripts/test-nightly-payload-report.mjs (node:test, no dependencies).
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import { main } from './nightly-payload-failures.mjs';
import { githubClient } from './nightly-payload-github.mjs';
import { synchronizeIssues } from './nightly-payload-issues.mjs';
import {
  ARTIFACT,
  BUDGET_BYTES,
  EXPECTED_TESTS,
  JOB_NAME,
  TEST_STEP,
  UPLOAD_STEP,
  WORKFLOW_NAME,
  WORKFLOW_PATH,
  analyzeRun,
  failureKey,
  marker,
  parseLog,
  trustedRun,
} from './nightly-payload-report.mjs';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const read = (path) => readFileSync(join(ROOT, path), 'utf8');
const SHA = 'a'.repeat(40);

const PASS_LOG = `running 1 test
test test_support::large_state::tests::large_state_fixture_full_scale_build_time ... ok

test result: ok. 1 passed; 0 failed; 0 ignored
running 1 test
test payload_budget_sweep_full_scale ... ok

test result: ok. 1 passed; 0 failed; 0 ignored
`;
function failLog(problems) {
  return `running 1 test
test test_support::large_state::tests::large_state_fixture_full_scale_build_time ... ok
running 1 test
test payload_budget_sweep_full_scale ... FAILED

failures:

---- payload_budget_sweep_full_scale stdout ----
payload budget setup: fixture 40s
thread 'payload_budget_sweep_full_scale' panicked at crates/intent-app/tests/payload_budget/main.rs:725:5:
payload budget sweep failed (${problems.length} problems):
${problems.map((p) => `  ${p}`).join('\n')}
note: run with \`RUST_BACKTRACE=1\` environment variable to display a backtrace

failures:
    payload_budget_sweep_full_scale

test result: FAILED. 0 passed; 1 failed; 0 ignored
`;
}
const NEW = 'NEW OFFENDER note.list max: 640000 bytes > 512000 budget — bound the reply (paginate, project, cap), do not add it to baseline-full.txt';
const GROWTH = 'GROWTH agent.list default: 1300000 bytes, recorded 1141286 (+13%, tolerance 5%) — the offender got bigger';
const STALE = 'STALE comment.list max (recorded 6332060): now within budget or no longer swept — delete its line from baseline-full.txt';
const SIZE_REPORT = `   1300000 OVER agent.list default\n    640000 OVER note.list max\n      1200 ok   workspace.list default\n`;

function source({ log = PASS_LOG, testOutcome = 'success', attempt = 1, runId = 900, report = SIZE_REPORT } = {}) {
  const run = {
    id: runId,
    run_attempt: attempt,
    head_sha: SHA,
    head_branch: 'main',
    event: 'schedule',
    status: 'completed',
    updated_at: '2026-11-02T05:00:00Z',
    path: WORKFLOW_PATH,
    name: WORKFLOW_NAME,
    repository: { id: 7, full_name: 'intent-hq/intentd' },
    head_repository: { full_name: 'intent-hq/intentd' },
  };
  const jobs = [
    {
      id: 50 + attempt,
      name: JOB_NAME,
      run_id: runId,
      run_attempt: attempt,
      head_sha: SHA,
      status: 'completed',
      conclusion: testOutcome,
      completed_at: '2026-11-02T04:30:00Z',
      steps: [
        { name: TEST_STEP, conclusion: testOutcome },
        { name: UPLOAD_STEP, conclusion: 'success' },
      ],
    },
  ];
  const artifacts = [
    { id: 4242, name: ARTIFACT, expired: false, workflow_run: { id: runId, head_sha: SHA, repository_id: 7 } },
  ];
  const documents = {
    [ARTIFACT]: {
      outcome: {
        schemaVersion: 1,
        repository: 'intent-hq/intentd',
        runId: String(runId),
        runAttempt: String(attempt),
        sha: SHA,
        ref: 'refs/heads/main',
        event: 'schedule',
        testOutcome,
        reportPresent: report !== null,
      },
      log,
      report,
    },
  };
  const release = {
    tag: 'v0.11.105',
    publishedAt: '2026-11-02T07:06:04Z',
    prerelease: true,
    url: 'https://github.com/intent-hq/intentd/releases/tag/v0.11.105',
  };
  return { run, jobs, artifacts, documents, release };
}
const failing = (problems, extra = {}) => source({ log: failLog(problems), testOutcome: 'failure', ...extra });

function issueStore(initial = []) {
  const issues = structuredClone(initial);
  const comments = [];
  const writes = [];
  return {
    issues,
    writes,
    comments_: comments,
    canonical: () => ({ state: 'none', number: null }),
    inventory: () => structuredClone({ issues, comments }),
    issue: (number) => {
      const issue = issues.find((i) => i.number === number);
      if (!issue) throw new Error('404 issue');
      return structuredClone(issue);
    },
    comments: (number) => structuredClone(comments.filter((c) => c.issue_url.endsWith(`/${number}`))),
    create: (title, body) => {
      const issue = {
        number: Math.max(100, ...issues.map((i) => i.number)) + 1,
        title,
        body,
        state: 'open',
        type: { name: 'Bug' },
        assignees: [{ login: 'panghy' }],
        labels: [{ name: 'component:intentd' }, { name: 'agent-filed' }],
      };
      issues.push(issue);
      writes.push({ create: issue.number });
      return structuredClone(issue);
    },
    comment: (number, body) => {
      const comment = {
        id: comments.length + 1,
        issue_url: `https://api.github.com/repos/intent-hq/intent/issues/${number}`,
        body,
      };
      comments.push(comment);
      writes.push({ comment: number });
      return comment;
    },
    reopen: (number) => {
      issues.find((i) => i.number === number).state = 'open';
      writes.push({ reopen: number });
    },
  };
}
const human = (body, extra = {}) => ({
  number: 20,
  title: 'Human issue',
  body,
  state: 'open',
  labels: [],
  assignees: [{ login: 'maintainer' }],
  ...extra,
});

describe('log parsing', () => {
  it('reads per-test results and every sweep problem with its bytes', () => {
    const parsed = parseLog(failLog([NEW, GROWTH, STALE, 'chat.subscribe default: snapshot missing seq']));
    assert.equal(parsed.results.get('payload_budget_sweep_full_scale'), 'FAILED');
    assert.equal(parsed.results.get('large_state_fixture_full_scale_build_time'), 'ok');
    assert.equal(parsed.declared, 4);
    assert.deepEqual(
      parsed.problems.map(({ category, method, variant, bytes, recorded }) => [category, method, variant, bytes, recorded]),
      [
        ['new-offender', 'note.list', 'max', 640000, null],
        ['growth', 'agent.list', 'default', 1300000, 1141286],
        ['stale', 'comment.list', 'max', null, 6332060],
        ['sweep-error', 'chat.subscribe', 'default', null, null],
      ],
    );
    assert.deepEqual(parsed.unparsed, []);
  });
});

describe('contract with the sweep and the workflows', () => {
  it('matches the sweep failure messages and budget the parser reads', () => {
    const sweep = read('crates/intent-app/tests/payload_budget/main.rs');
    for (const format of [
      '"NEW OFFENDER {method} {variant}: {bytes} bytes > {READ_PAYLOAD_BUDGET_BYTES} \\',
      '"GROWTH {method} {variant}: {bytes} bytes, recorded {recorded} (+{}%, \\',
      '"STALE {method} {variant} (recorded {recorded}): now within budget or no \\',
      '"payload budget sweep failed ({} problems):\\n  {}"',
      'payload-budget-report-{baseline_file}',
      'async fn payload_budget_sweep_full_scale()',
    ])
      assert.ok(sweep.includes(format), `payload_budget/main.rs no longer contains ${format}`);
    assert.match(
      read('crates/intent-transport/src/catalog.rs'),
      /pub const READ_PAYLOAD_BUDGET_BYTES: usize = 500 \* 1024;/,
    );
    assert.equal(BUDGET_BYTES, 512000);
    assert.ok(
      read('crates/intent-services/src/test_support/large_state.rs').includes(
        'async fn large_state_fixture_full_scale_build_time()',
      ),
    );
  });
  it('binds the reporter constants to the sweep workflow', () => {
    const sweep = read(WORKFLOW_PATH);
    assert.ok(sweep.includes(`name: ${WORKFLOW_NAME}\n`));
    assert.ok(sweep.includes(`    name: ${JOB_NAME}\n`));
    assert.ok(sweep.includes(`      - name: ${TEST_STEP}\n`));
    assert.ok(sweep.includes(`      - name: ${UPLOAD_STEP}\n`));
    assert.ok(sweep.includes(`          name: ${ARTIFACT}\n`));
    assert.ok(sweep.includes('INTENTD_LARGE_STATE_SCALE: full'));
    for (const test of EXPECTED_TESTS) assert.ok(sweep.includes(`--ignored ${test}`), test);
    assert.ok(sweep.includes('payload-budget-report-baseline-full.txt'));
  });
  it('makes the reporter fail loudly before reading anything when the token is missing', () => {
    const report = read('.github/workflows/nightly-payload-report.yml');
    assert.ok(report.includes(`    workflows: [${WORKFLOW_NAME}]\n`));
    const guard = report.indexOf('- name: Require MONOREPO_ISSUES_TOKEN');
    assert.ok(guard > 0 && guard < report.indexOf('- name: Collect and validate sweep evidence'));
    assert.match(report.slice(guard), /MONOREPO_ISSUES_TOKEN: \$\{\{ secrets\.MONOREPO_ISSUES_TOKEN \}\}[\s\S]*::error[\s\S]*exit 1/);
    assert.ok(!/continue-on-error/.test(report));
    assert.ok(report.includes('GH_TOKEN: ${{ secrets.MONOREPO_ISSUES_TOKEN }}'));
    assert.ok(report.includes('group: nightly-payload-issue-writer'));
  });
});

describe('run analysis', () => {
  it('plans nothing for a passing night', () => {
    const plan = analyzeRun(source());
    assert.deepEqual(plan.items, []);
    assert.deepEqual(plan.incidents, []);
    assert.ok(trustedRun(plan.run));
  });
  it('plans one budget item per method and variant with the measured bytes', () => {
    const plan = analyzeRun(failing([NEW, GROWTH]));
    assert.deepEqual(plan.incidents, []);
    assert.deepEqual(
      plan.items.map((i) => [i.kind, i.method, i.variant, i.bytes, i.budget, i.sizeLine]),
      [
        ['budget', 'note.list', 'max', 640000, 512000, '640000 OVER note.list max'],
        ['budget', 'agent.list', 'default', 1300000, 512000, '1300000 OVER agent.list default'],
      ],
    );
    assert.equal(plan.items[0].key, failureKey({ kind: 'budget', method: 'note.list', variant: 'max' }));
    assert.deepEqual(plan.artifact, { id: 4242, name: ARTIFACT });
  });
  it('keys a non-budget failure by its test name', () => {
    const log = PASS_LOG.replace(
      'large_state_fixture_full_scale_build_time ... ok',
      'large_state_fixture_full_scale_build_time ... FAILED',
    ) + '\n---- test_support::large_state::tests::large_state_fixture_full_scale_build_time stdout ----\nassertion `left == right` failed\n\nfailures:\n';
    const plan = analyzeRun(source({ log, testOutcome: 'failure' }));
    assert.equal(plan.items.length, 1);
    assert.equal(plan.items[0].kind, 'test');
    assert.equal(plan.items[0].test, 'large_state_fixture_full_scale_build_time');
    assert.match(plan.items[0].evidence, /left == right/);
  });
  it('reports an evidence gap when a full-scale test produced no result', () => {
    const plan = analyzeRun(source({ log: 'error[E0425]: cannot find value\n', testOutcome: 'failure' }));
    assert.equal(plan.items.length, 1);
    assert.equal(plan.items[0].kind, 'infrastructure');
    assert.match(plan.incidents[0], /No result for large_state_fixture_full_scale_build_time/);
  });
  it('names a failed setup step ahead of the skipped sweep it caused', () => {
    // A crates.io outage that outlasts the fetch retries (intent-hq/intent#8585).
    const data = source({ testOutcome: 'skipped' });
    data.jobs[0].conclusion = 'failure';
    data.jobs[0].steps.unshift({ name: 'Fetch locked crates', conclusion: 'failure' });
    const plan = analyzeRun(data);
    assert.equal(plan.items.length, 1);
    assert.equal(plan.items[0].kind, 'infrastructure');
    assert.match(plan.incidents[0], /Setup, recording or upload step failed: Fetch locked crates/);
  });
  it('suppresses individual issues for widespread failures', () => {
    const problems = Array.from({ length: 26 }, (_, i) => `NEW OFFENDER m${i}.read default: 600000 bytes > 512000 budget — x`);
    const plan = analyzeRun(failing(problems));
    assert.equal(plan.items.length, 1);
    assert.match(plan.incidents[0], /Widespread failures/);
  });
  it('rejects evidence from another run', () => {
    const data = failing([NEW]);
    data.documents[ARTIFACT].outcome.runId = '1';
    assert.match(analyzeRun(data).incidents[0], /Stale or mismatched artifact context/);
  });
});

describe('issue synchronization', () => {
  it('files a new failure once with the required body facts', () => {
    const client = issueStore();
    const results = synchronizeIssues(analyzeRun(failing([NEW])), client, { write: true });
    assert.deepEqual(client.writes, [{ create: 101 }]);
    assert.equal(results[0].action, 'create');
    const [issue] = client.issues;
    assert.equal(issue.title, '[nightly payload] note.list max fails the 500 KiB read payload budget');
    for (const fact of [
      marker(results[0].key),
      `https://github.com/intent-hq/intentd/commit/${SHA}`,
      'Latest released intentd version: [v0.11.105]',
      'Method: note.list',
      'Variant: max',
      'Measured: 640000 bytes (625.0 KiB) against the 512000 bytes (500.0 KiB) read payload budget',
      'https://github.com/intent-hq/intentd/actions/runs/900/attempts/1',
      'https://github.com/intent-hq/intentd/actions/runs/900/artifacts/4242',
    ])
      assert.ok(issue.body.includes(fact), fact);
  });
  it('comments on the open issue when the failure repeats on a later night', () => {
    const client = issueStore();
    synchronizeIssues(analyzeRun(failing([NEW])), client, { write: true });
    const results = synchronizeIssues(analyzeRun(failing([NEW], { runId: 901 })), client, { write: true });
    assert.deepEqual(client.writes, [{ create: 101 }, { comment: 101 }]);
    assert.equal(results[0].action, 'update');
    assert.match(client.comments_[0].body, /actions\/runs\/901\/attempts\/1/);
  });
  it('does not write again when the reporter reruns for the same attempt', () => {
    const client = issueStore();
    synchronizeIssues(analyzeRun(failing([NEW])), client, { write: true });
    const results = synchronizeIssues(analyzeRun(failing([NEW])), client, { write: true });
    assert.deepEqual(client.writes, [{ create: 101 }]);
    assert.equal(results[0].action, 'already-recorded');
  });
  it('dedups against an existing issue carrying the marker instead of filing another', () => {
    const plan = analyzeRun(failing([NEW]));
    const client = issueStore([human(`Tracked here.\n${marker(plan.items[0].key)}`)]);
    const results = synchronizeIssues(plan, client, { write: true });
    assert.deepEqual(client.writes, [{ comment: 20 }]);
    assert.equal(results[0].issue, 20);
  });
  it('dedups against a human issue that adopted the marker in a comment', () => {
    const plan = analyzeRun(failing([NEW]));
    const client = issueStore([human('Something about note.list')]);
    client.comments_.push({
      id: 9,
      issue_url: 'https://api.github.com/repos/intent-hq/intent/issues/20',
      body: marker(plan.items[0].key),
    });
    synchronizeIssues(plan, client, { write: true });
    assert.deepEqual(client.writes, [{ comment: 20 }]);
  });
  it('reopens an issue closed as completed when the failure recurs, never closes on a pass', () => {
    const plan = analyzeRun(failing([NEW]));
    const client = issueStore([
      human(marker(plan.items[0].key), { state: 'closed', state_reason: 'completed', closed_at: '2026-11-01T00:00:00Z' }),
    ]);
    const results = synchronizeIssues(plan, client, { write: true });
    assert.deepEqual(client.writes, [{ reopen: 20 }, { comment: 20 }]);
    assert.equal(results[0].action, 'reopen');
    synchronizeIssues(analyzeRun(source({ runId: 902 })), client, { write: true });
    assert.equal(client.issues[0].state, 'open');
  });
  it('keeps a not-planned closure closed and asks its owner', () => {
    const plan = analyzeRun(failing([NEW]));
    const client = issueStore([
      human(marker(plan.items[0].key), { state: 'closed', state_reason: 'not_planned', closed_at: '2026-11-01T00:00:00Z' }),
    ]);
    const results = synchronizeIssues(plan, client, { write: true });
    assert.deepEqual(client.writes, [{ comment: 20 }]);
    assert.equal(results[0].action, 'closed-recurrence');
  });
  it('makes no writes on a dry run', () => {
    const client = issueStore();
    const results = synchronizeIssues(analyzeRun(failing([NEW, GROWTH])), client);
    assert.deepEqual(client.writes, []);
    assert.deepEqual(results.map((r) => r.action), ['create', 'create']);
  });
});

describe('GitHub client', () => {
  it('creates issues with Type Bug, the intentd labels and the triage owner', () => {
    const calls = [];
    const run = (args, options) => {
      calls.push({ args, input: options.input && JSON.parse(options.input) });
      return JSON.stringify({ number: 5, title: 't', body: 'b', state: 'open', assignees: [] });
    };
    githubClient({ run, directory: tmpdir() }).create('t', 'b');
    assert.deepEqual(calls[0].args.slice(0, 2), ['api', 'repos/intent-hq/intent/issues']);
    assert.deepEqual(calls[0].input, {
      title: 't',
      body: 'b',
      type: 'Bug',
      labels: ['component:intentd', 'agent-filed'],
      assignees: ['panghy'],
    });
  });
  it('picks the newest published release, alpha included', () => {
    const run = () =>
      JSON.stringify([
        { tag_name: 'v0.11.34', draft: false, prerelease: false, published_at: '2026-10-01T00:00:00Z', html_url: 'u1' },
        { tag_name: 'v0.11.106', draft: true, prerelease: true, published_at: null, html_url: 'u3' },
        { tag_name: 'v0.11.105', draft: false, prerelease: true, published_at: '2026-11-02T07:06:04Z', html_url: 'u2' },
      ]);
    assert.deepEqual(githubClient({ run, directory: tmpdir() }).latestRelease(), {
      tag: 'v0.11.105',
      publishedAt: '2026-11-02T07:06:04Z',
      prerelease: true,
      url: 'u2',
    });
  });
});

describe('reporter command', () => {
  it('fails loudly and keeps the plan when MONOREPO_ISSUES_TOKEN is missing', () => {
    const directory = mkdtempSync(join(tmpdir(), 'nightly-payload-'));
    try {
      writeFileSync(join(directory, 'plan.json'), JSON.stringify(analyzeRun(failing([NEW]))));
      const stderr = process.stderr.write;
      process.stderr.write = () => true;
      let status;
      try {
        status = main(['publish', '--out', directory, '--write'], {
          env: { GITHUB_EVENT_NAME: 'workflow_run', GITHUB_REPOSITORY: 'intent-hq/intentd', GITHUB_REF: 'refs/heads/main' },
          createClient: () => issueStore(),
        });
      } finally {
        process.stderr.write = stderr;
      }
      assert.equal(status, 1);
      const error = JSON.parse(readFileSync(join(directory, 'error.json'), 'utf8'));
      assert.match(error.error, /MONOREPO_ISSUES_TOKEN is not set/);
      assert.match(readFileSync(join(directory, 'summary.md'), 'utf8'), /MONOREPO_ISSUES_TOKEN/);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
  it('refuses writes outside the trusted workflow_run job even with a token', () => {
    const directory = mkdtempSync(join(tmpdir(), 'nightly-payload-'));
    try {
      writeFileSync(join(directory, 'plan.json'), JSON.stringify(analyzeRun(failing([NEW]))));
      const client = issueStore();
      const stderr = process.stderr.write;
      process.stderr.write = () => true;
      let status;
      try {
        status = main(['publish', '--out', directory, '--write'], {
          env: { GH_TOKEN: 'x', GITHUB_EVENT_NAME: 'push' },
          createClient: () => client,
        });
      } finally {
        process.stderr.write = stderr;
      }
      assert.equal(status, 1);
      assert.deepEqual(client.writes, []);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
