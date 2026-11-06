// Analysis half of the nightly payload-sweep reporter: turns one completed
// "Nightly Payload Sweep" run (job history, artifact list and the evidence
// artifact's outcome.json / test.log / size-report.txt) into a plan of issue
// candidates. Ported from cloudlands-fe scripts/nightly-test-report.mjs (the
// nightly browser reporter): the same trusted-run scope, stable per-failure
// marker, per-attempt occurrence marker and "evidence gap" incident, with the
// Playwright report parsing replaced by the cargo test log of the sweep.
import { createHash } from 'node:crypto';

export const SOURCE_REPO = 'intent-hq/intentd';
export const ISSUE_REPO = 'intent-hq/intent';
export const OWNER = 'panghy';
export const WORKFLOW_PATH = '.github/workflows/nightly-payload-sweep.yml';
export const WORKFLOW_NAME = 'Nightly Payload Sweep';
export const JOB_NAME = 'Full-scale payload sweep';
export const TEST_STEP = 'Full-scale sweep';
export const UPLOAD_STEP = 'Upload sweep evidence';
export const ARTIFACT = 'nightly-payload-sweep';
// intent-transport catalog READ_PAYLOAD_BUDGET_BYTES (500 KiB); the suite pins it.
export const BUDGET_BYTES = 500 * 1024;
// The ignored full-scale tests the sweep step runs; each must report a result.
export const EXPECTED_TESTS = [
  'large_state_fixture_full_scale_build_time',
  'payload_budget_sweep_full_scale',
];
export const SWEEP_TEST = 'payload_budget_sweep_full_scale';
export const MAX_ITEMS = 25;

const record = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const requireValue = (ok, message) => {
  if (!ok) throw new Error(message);
};

export function trustedRun(run) {
  return (
    run?.repository?.full_name === SOURCE_REPO &&
    run?.head_repository?.full_name === SOURCE_REPO &&
    run.head_branch === 'main' &&
    ['schedule', 'workflow_dispatch'].includes(run.event) &&
    run.path === WORKFLOW_PATH &&
    run.name === WORKFLOW_NAME
  );
}

// One issue per distinct failure: a budget problem is keyed by method +
// variant (a NEW OFFENDER that later GROWs stays on one issue), any other
// failure by the test name, an evidence gap by the fixed infrastructure key.
export function failureKey(item) {
  const identity =
    item.kind === 'budget'
      ? [SOURCE_REPO, 'payload-budget', item.method, item.variant]
      : [SOURCE_REPO, item.kind, item.test, item.category];
  return `v1:${createHash('sha256').update(JSON.stringify(identity)).digest('hex')}`;
}
export const marker = (key) => `<!-- nightly-payload-failure:${key} -->`;
export const occurrenceMarker = (run, key, attempt = run.run_attempt) =>
  `<!-- nightly-payload-seen:${run.id}:${attempt}:${key} -->`;

export function itemTitle(item) {
  if (item.kind === 'budget')
    return `[nightly payload] ${item.method} ${item.variant} fails the 500 KiB read payload budget`;
  if (item.kind === 'infrastructure') return '[nightly payload] Nightly payload sweep CI evidence is incomplete';
  return `[nightly payload] ${item.test} failed`;
}

// The problem lines come from crates/intent-app/tests/payload_budget/main.rs
// (`ratchet` and the sweep's own checks); the suite pins those format strings.
const PROBLEM = /^(NEW OFFENDER|GROWTH|STALE) (\S+) (\S+?):? (.*)$/;
const OTHER_PROBLEM = /^([a-z][\w]*(?:\.[\w]+)+) (\S+?): (.*)$/;

export function parseProblem(line) {
  let match = PROBLEM.exec(line);
  if (match) {
    const [, kind, method, variant, rest] = match;
    const bytes = /^(\d+) bytes/.exec(rest) ?? /^\(recorded (\d+)\)/.exec(rest);
    const recorded = /recorded (\d+)/.exec(rest);
    return {
      category: kind.toLowerCase().replace(' ', '-'),
      method,
      variant,
      bytes: kind === 'STALE' ? null : bytes ? Number(bytes[1]) : null,
      recorded: recorded ? Number(recorded[1]) : null,
      problem: line,
    };
  }
  match = OTHER_PROBLEM.exec(line);
  if (match)
    return {
      category: 'sweep-error',
      method: match[1],
      variant: match[2],
      bytes: null,
      recorded: null,
      problem: line,
    };
  return null;
}

// Parses `cargo test` output: per-test results and, for a failed sweep, the
// problem list of its "payload budget sweep failed (N problems):" panic.
export function parseLog(text) {
  requireValue(typeof text === 'string', 'Missing test log');
  const lines = text.split(/\r?\n/);
  const results = new Map();
  for (const line of lines) {
    const match = /^test (\S+) \.\.\. (ok|FAILED|ignored)\b/.exec(line);
    if (!match) continue;
    const name = match[1].split('::').at(-1);
    const previous = results.get(name);
    requireValue(!previous || previous === match[2], `Conflicting results for ${name}`);
    results.set(name, match[2]);
  }
  const problems = [];
  let unparsed = [];
  let declared = null;
  const start = lines.findIndex((l) => /payload budget sweep failed \((\d+) problems\):/.test(l));
  if (start >= 0) {
    declared = Number(/\((\d+) problems\)/.exec(lines[start])[1]);
    for (const line of lines.slice(start + 1)) {
      if (!/^ {2}\S/.test(line)) break;
      const problem = parseProblem(line.trim());
      if (problem) problems.push(problem);
      else unparsed.push(line.trim());
    }
  }
  const panics = new Map();
  for (let i = 0; i < lines.length; i += 1) {
    const match = /^---- (\S+) stdout ----$/.exec(lines[i]);
    if (!match) continue;
    const body = [];
    for (const line of lines.slice(i + 1)) {
      if (/^---- \S+ stdout ----$/.test(line) || /^failures:$/.test(line)) break;
      body.push(line);
    }
    panics.set(match[1].split('::').at(-1), body.join('\n').trim());
  }
  return { results, problems, unparsed, declared, panics };
}

function latestJob(jobs, run) {
  const matching = jobs.filter((job) => job.name === JOB_NAME);
  for (const job of matching)
    requireValue(
      Number.isSafeInteger(job.run_attempt) &&
        job.run_attempt >= 1 &&
        job.run_attempt <= run.run_attempt &&
        job.run_id === run.id &&
        job.head_sha === run.head_sha,
      `Invalid job history for ${JOB_NAME}`,
    );
  const latest = Math.max(...matching.map((j) => j.run_attempt));
  const candidates = matching.filter((j) => j.run_attempt === latest);
  requireValue(candidates.length === 1, `Missing or ambiguous job history for ${JOB_NAME}`);
  const job = candidates[0];
  requireValue(
    job.status === 'completed' &&
      Number.isSafeInteger(job.id) &&
      job.id > 0 &&
      typeof job.completed_at === 'string' &&
      Number.isFinite(Date.parse(job.completed_at)) &&
      typeof job.conclusion === 'string' &&
      Array.isArray(job.steps),
    `Incomplete job history for ${JOB_NAME}`,
  );
  return job;
}

function validateContext(data, run, job) {
  requireValue(
    record(data) &&
      data.schemaVersion === 1 &&
      data.repository === SOURCE_REPO &&
      data.runId === String(run.id) &&
      data.sha === run.head_sha &&
      data.ref === `refs/heads/${run.head_branch}` &&
      data.event === run.event &&
      data.runAttempt === String(job.run_attempt),
    'Stale or mismatched artifact context',
  );
}

function sizeLine(report, method, variant) {
  if (typeof report !== 'string') return null;
  return (
    report
      .split('\n')
      .find((line) => {
        const parts = line.trim().split(/\s+/);
        return parts.length === 4 && parts[2] === method && parts[3] === variant;
      })
      ?.trim() ?? null
  );
}

export function analyzeRun({ run, jobs, artifacts, documents, release }) {
  requireValue(Array.isArray(jobs), 'Unreadable job history');
  const incidents = [];
  const items = [];
  let artifact = null;
  let job = null;
  try {
    job = latestJob(jobs, run);
    const found = artifacts.filter((a) => a.name === ARTIFACT);
    requireValue(
      found.length === 1 && !found[0].expired,
      `Missing, expired or ambiguous artifact: ${ARTIFACT}`,
    );
    artifact = found[0];
    requireValue(
      artifact.workflow_run?.id === run.id &&
        artifact.workflow_run?.head_sha === run.head_sha &&
        artifact.workflow_run?.repository_id === run.repository.id,
      'Mismatched artifact origin',
    );
    const doc = documents[ARTIFACT];
    requireValue(doc && !doc.archiveError, `Unreadable archive: ${doc?.archiveError}`);
    const outcome = doc.outcome;
    validateContext(outcome, run, job);
    // A failed setup step (e.g. the crate fetch) skips the sweep; name it
    // rather than the skip it caused.
    const failedSteps = job.steps.filter((s) => s.conclusion === 'failure' && s.name !== TEST_STEP);
    requireValue(
      !failedSteps.length,
      `Setup, recording or upload step failed: ${failedSteps.map((s) => s.name).join(', ')}`,
    );
    requireValue(
      ['success', 'failure'].includes(outcome.testOutcome),
      'Test execution skipped or cancelled',
    );
    requireValue(['success', 'failure'].includes(job.conclusion), `Job ended ${job.conclusion}`);
    requireValue(
      job.steps.some((s) => s.name === UPLOAD_STEP && s.conclusion === 'success'),
      'Evidence upload was not successful',
    );
    const parsed = parseLog(doc.log);
    for (const name of EXPECTED_TESTS)
      requireValue(
        ['ok', 'FAILED'].includes(parsed.results.get(name)),
        `No result for ${name}; the full-scale test did not run`,
      );
    if (outcome.testOutcome === 'success')
      requireValue(
        [...parsed.results.values()].every((r) => r !== 'FAILED'),
        'Passing step with failed tests in its log',
      );
    requireValue(
      outcome.reportPresent === true || parsed.results.get(SWEEP_TEST) !== 'ok',
      'Missing size report from a passing sweep',
    );
    const observedAt = job.completed_at;
    const occurrence = (evidence) => ({
      attempt: job.run_attempt,
      observedAt,
      job: job.id,
      artifacts: [ARTIFACT],
      evidence,
    });
    for (const [name, result] of parsed.results) {
      if (result !== 'FAILED') continue;
      if (name === SWEEP_TEST && parsed.problems.length) {
        // A multi-line problem ends the indented list early; the remainder is
        // filed under the test with the whole panic as evidence.
        if (parsed.declared !== parsed.problems.length + parsed.unparsed.length)
          parsed.unparsed.push(`(problem list declared ${parsed.declared}, read ${parsed.problems.length + parsed.unparsed.length})`);
        for (const problem of parsed.problems) {
          const item = {
            kind: 'budget',
            test: SWEEP_TEST,
            ...problem,
            budget: BUDGET_BYTES,
            sizeLine: sizeLine(doc.report, problem.method, problem.variant),
          };
          items.push({ ...item, key: failureKey(item), occurrence: occurrence(problem.problem) });
        }
        if (!parsed.unparsed.length) continue;
      }
      const evidence = [
        ...(name === SWEEP_TEST ? parsed.unparsed : []),
        parsed.panics.get(name) ?? '',
      ]
        .join('\n')
        .trim()
        .slice(0, 6000);
      const item = { kind: 'test', test: name, category: 'test-failure', evidence };
      items.push({ ...item, key: failureKey(item), occurrence: occurrence(evidence) });
    }
    requireValue(
      outcome.testOutcome !== 'failure' || items.length > 0,
      'Test command failed without reported test failures (build error or crash)',
    );
    requireValue(
      items.length <= MAX_ITEMS,
      'Widespread failures require CI triage; individual issue creation suppressed',
    );
  } catch (error) {
    items.length = 0;
    incidents.push(`${ARTIFACT}: ${error.message}`);
  }
  const unique = new Map();
  for (const { occurrence, ...item } of items) {
    const existing = unique.get(item.key);
    if (existing) existing.occurrences.push(occurrence);
    else unique.set(item.key, { ...item, occurrences: [occurrence] });
  }
  const planned = [...unique.values()];
  if (incidents.length) {
    const incident = {
      kind: 'infrastructure',
      test: WORKFLOW_PATH,
      category: 'incomplete-ci-evidence',
      evidence: incidents.join('\n'),
    };
    planned.unshift({
      ...incident,
      key: failureKey(incident),
      occurrences: [
        {
          // Incompleteness is an observation about this reporting attempt.
          attempt: run.run_attempt,
          observedAt: run.updated_at,
          artifacts: artifacts.map((a) => a.name),
          evidence: incident.evidence,
        },
      ],
    });
  }
  return {
    schemaVersion: 1,
    run,
    release,
    artifact: artifact ? { id: artifact.id, name: artifact.name } : null,
    incidents,
    items: planned,
  };
}

export function safeText(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('@', '@​')
    .replaceAll('`', '&#96;');
}
