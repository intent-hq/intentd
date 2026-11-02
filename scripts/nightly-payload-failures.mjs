#!/usr/bin/env node
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { githubClient, readJson } from './nightly-payload-github.mjs';
import { synchronizeIssues } from './nightly-payload-issues.mjs';
import { analyzeRun, ARTIFACT, safeText, SOURCE_REPO, trustedRun } from './nightly-payload-report.mjs';

// Reporter for .github/workflows/nightly-payload-sweep.yml, run by
// nightly-payload-report.yml from default-branch code only. Ported from
// cloudlands-fe scripts/nightly-test-failures.mjs:
//   collect --run ID [--attempt N] --out DIR   read the source run, its jobs and
//       its evidence artifact (GITHUB token) and save plan.json;
//   publish --out DIR [--write]               file or comment on deduplicated
//       intent-hq/intent issues (MONOREPO_ISSUES_TOKEN). Without --write it is a
//       dry run. Every input, the plan and partial receipts stay in DIR so a
//       failed reporter can be rerun; occurrence markers prevent duplicate writes.

function save(directory, name, value) {
  writeFileSync(join(directory, name), `${JSON.stringify(value, null, 2)}\n`);
}
function summary(directory, text, env) {
  writeFileSync(join(directory, 'summary.md'), `${text}\n`);
  if (env.GITHUB_STEP_SUMMARY) appendFileSync(env.GITHUB_STEP_SUMMARY, `${text}\n`);
}
function evidenceGap(directory, expected, observed, reason) {
  const message = `${reason}: requested attempt ${expected.run_attempt}, observed attempt ${observed.run_attempt}. The requested completion cannot be established from the available evidence`;
  save(directory, 'evidence-gap.json', { reason: message, expected, observed });
  throw new Error(message);
}
function validateCompletion(run, env, directory) {
  const event = readJson(env.GITHUB_EVENT_PATH);
  save(directory, 'source-event.json', event);
  if (
    event.action !== 'completed' ||
    !trustedRun(event.workflow_run) ||
    event.workflow_run.status !== 'completed' ||
    !Number.isSafeInteger(event.workflow_run.run_attempt) ||
    event.workflow_run.run_attempt < 1 ||
    event.workflow_run.id !== run.id ||
    event.workflow_run.head_sha !== run.head_sha
  )
    throw new Error('Completion event does not match the report');
  if (event.workflow_run.run_attempt !== run.run_attempt)
    evidenceGap(directory, event.workflow_run, run, 'Completion event attempt mismatch');
}
function writeScope(plan, env, directory) {
  // Fail loudly: a missing secret must never look like a quiet night.
  if (!env.GH_TOKEN)
    throw new Error(
      'MONOREPO_ISSUES_TOKEN is not set: issue writes for intent-hq/intent need it (repository secret)',
    );
  if (
    env.GITHUB_EVENT_NAME !== 'workflow_run' ||
    env.GITHUB_REPOSITORY !== SOURCE_REPO ||
    env.GITHUB_REF !== 'refs/heads/main' ||
    !env.GH_TOKEN ||
    !trustedRun(plan.run)
  ) {
    throw new Error(
      'Issue writes require the trusted main workflow_run job and MONOREPO_ISSUES_TOKEN',
    );
  }
  validateCompletion(plan.run, env, directory);
}

export function main(argv, { env = process.env, createClient = githubClient } = {}) {
  let directory;
  try {
    const [command, ...args] = argv;
    let runId;
    let runAttempt;
    let write = false;
    let historical = false;
    for (let i = 0; i < args.length; i += 1) {
      if (args[i] === '--out' && args[i + 1]) directory = resolve(args[++i]);
      else if (args[i] === '--run' && /^\d+$/.test(args[i + 1] ?? '')) runId = args[++i];
      else if (args[i] === '--attempt' && /^[1-9]\d*$/.test(args[i + 1] ?? ''))
        runAttempt = Number(args[++i]);
      else if (args[i] === '--write') write = true;
      else if (args[i] === '--historical') historical = true;
      else throw new Error(`Unknown or incomplete option: ${args[i]}`);
    }
    if (
      !directory ||
      !['collect', 'publish'].includes(command) ||
      (command === 'collect' && (!runId || write)) ||
      (runAttempt !== undefined && (command !== 'collect' || !Number.isSafeInteger(runAttempt)))
    ) {
      throw new Error(
        'Usage: nightly-payload-failures.mjs collect --run ID [--attempt N] --out DIR [--historical] | publish --out DIR [--write]',
      );
    }
    mkdirSync(directory, { recursive: true });
    const client = createClient({ directory });
    if (command === 'collect') {
      const run = client.run(runId);
      save(directory, 'run.json', run);
      if (
        !Number.isSafeInteger(run.id) ||
        String(run.id) !== runId ||
        !Number.isSafeInteger(run.run_attempt) ||
        run.run_attempt < 1 ||
        !/^[a-f0-9]{40}$/.test(run.head_sha) ||
        !Number.isFinite(Date.parse(run.updated_at)) ||
        run.status !== 'completed'
      )
        throw new Error('Run is incomplete or has invalid metadata');
      if (!trustedRun(run) && !historical)
        throw new Error('Run is outside trusted nightly/main manual scope');
      if (!historical && env.GITHUB_EVENT_NAME === 'workflow_run')
        validateCompletion(run, env, directory);
      if (runAttempt !== undefined && run.run_attempt !== runAttempt)
        evidenceGap(
          directory,
          { id: run.id, run_attempt: runAttempt },
          run,
          'Requested source attempt mismatch',
        );
      const jobs = client.jobs(runId); // Fail closed on unreadable history, never use artifact age alone.
      save(directory, 'jobs.json', jobs);
      const artifacts = client.artifacts(runId);
      save(directory, 'artifacts.json', artifacts);
      const documents = {};
      const matches = artifacts.filter((a) => a.name === ARTIFACT && !a.expired);
      if (matches.length === 1)
        documents[ARTIFACT] = client.archive(
          matches[0],
          { outcome: 'outcome.json', log: 'test.log', report: 'size-report.txt' },
          ['outcome'],
        );
      const release = client.latestRelease();
      save(directory, 'release.json', release);
      save(directory, 'documents.json', documents);
      const latest = client.run(runId);
      save(directory, 'latest-run.json', latest);
      if (
        latest.run_attempt !== run.run_attempt ||
        latest.status !== 'completed' ||
        latest.head_sha !== run.head_sha
      )
        evidenceGap(directory, run, latest, 'Source run changed during collection');
      const plan = analyzeRun({ run, jobs, artifacts, documents, release });
      plan.historical = historical;
      save(directory, 'plan.json', plan);
      summary(
        directory,
        `Payload sweep report for [run ${run.id}](https://github.com/${SOURCE_REPO}/actions/runs/${run.id}), attempt ${run.run_attempt}.\n\n` +
          `${plan.items.length} issue candidates. ${plan.incidents.length} evidence gaps. Latest released intentd: ${safeText(release.tag)}.\n\n` +
          (historical
            ? 'Read-only historical evidence: no issue writes are authorized.\n\n'
            : '') +
          plan.incidents.map((s) => `- ${safeText(s)}`).join('\n'),
        env,
      );
      return 0;
    }
    const plan = readJson(join(directory, 'plan.json'));
    if (write) {
      if (plan.historical) throw new Error('Historical diagnostics cannot write issues');
      writeScope(plan, env, directory);
    }
    const results = synchronizeIssues(plan, client, {
      write,
      checkpoint: (receipts) => save(directory, 'receipts.json', receipts),
    });
    save(directory, 'receipts.json', results);
    summary(
      directory,
      `${write ? 'Reported' : 'Dry run for'} [payload sweep run ${plan.run.id}, attempt ${plan.run.run_attempt}](https://github.com/${SOURCE_REPO}/actions/runs/${plan.run.id}/attempts/${plan.run.run_attempt}).\n\n` +
        (results.length
          ? results
              .map((r) => `- ${r.url ? `[Issue ${r.issue}](${r.url})` : r.key}: ${r.action}`)
              .join('\n')
          : 'The sweep evidence was validated; no failures.') +
        `\n\nSize report and test log: [source artifacts](https://github.com/${SOURCE_REPO}/actions/runs/${plan.run.id}#artifacts).\n\n` +
        (plan.incidents.length
          ? `CI evidence is incomplete; see the infrastructure issue and saved plan.\n${plan.incidents.map((s) => `- ${safeText(s)}`).join('\n')}`
          : ''),
      env,
    );
    return 0;
  } catch (error) {
    if (directory) {
      mkdirSync(directory, { recursive: true });
      save(directory, 'error.json', {
        error: error.message,
        retry:
          'Rerun the failed reporter after correcting the cause; existing occurrence markers prevent duplicate writes.',
      });
      const receipts = (() => {
        try {
          return readJson(join(directory, 'receipts.json'));
        } catch {
          return [];
        }
      })();
      summary(
        directory,
        `Payload sweep failure reporting failed: ${safeText(error.message)}.\n\nInputs, plan and partial receipts are retained for retry.\n\n` +
          receipts
            .filter((r) => r.url)
            .map((r) => `- [Issue ${r.issue}](${r.url}): ${r.action}`)
            .join('\n'),
        env,
      );
    }
    process.stderr.write(`Payload sweep failure reporting failed: ${error.message}\n`);
    return 1;
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  process.exitCode = main(process.argv.slice(2));
