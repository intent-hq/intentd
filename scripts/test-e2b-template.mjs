#!/usr/bin/env node
// Tests for the E2B intent-node template (packaging/e2b) and its workflow.
// Run: ./scripts/test-e2b-template.mjs (node:test, offline, no installs).
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import {
  TEMPLATE_NAME,
  archiveUrl,
  defineTemplate,
  installIntentdCommand,
  parseSha256,
  parseVersion,
  plan,
  publishTemplate,
  redact,
  smokeChecks,
  verifySmoke,
} from '../packaging/e2b/intent-node.mjs';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const SHA = 'ed397e7a366d37052e71e5bb00c2ebafeb5a72ea9ed475229c4f59dc897d8f21';

function recorder() {
  const calls = [];
  const builder = new Proxy({}, {
    get(_, name) {
      return (...args) => {
        calls.push([name, ...args]);
        return builder;
      };
    },
  });
  return { calls, Template: () => builder };
}

describe('parseVersion', () => {
  it('accepts plain and v-prefixed release versions', () => {
    assert.equal(parseVersion('0.11.139'), '0.11.139');
    assert.equal(parseVersion('v0.11.139'), '0.11.139');
    assert.equal(parseVersion('1.2.3-alpha.1'), '1.2.3-alpha.1');
  });
  it('rejects anything that is not a version, so nothing reaches the shell', () => {
    for (const bad of ['', 'main', 'sitter-v0.1.0', '0.11', '0.11.139;rm -rf /', '0.11.139 x', '$(id)']) {
      assert.throws(() => parseVersion(bad), /not an intentd release version/, bad);
    }
  });
});

describe('parseSha256', () => {
  it('reads the checksum from a dist sidecar', () => {
    assert.equal(parseSha256(`${SHA} *intentd-x86_64-unknown-linux-musl.tar.xz\n`), SHA);
    assert.equal(parseSha256(SHA.toUpperCase()), SHA);
  });
  it('rejects a malformed sidecar', () => {
    for (const bad of ['', 'Not Found', SHA.slice(1), `${SHA}0`]) {
      assert.throws(() => parseSha256(bad), /SHA-256/);
    }
  });
});

describe('plan', () => {
  it('publishes version and latest after a successful release run', () => {
    assert.deepEqual(plan({ EVENT_NAME: 'workflow_run', RUN_CONCLUSION: 'success', RUN_HEAD_BRANCH: 'v0.11.139' }), {
      version: '0.11.139', tags: ['0.11.139', 'latest'], publish: true,
    });
  });
  it('skips failed release runs and non-release refs', () => {
    assert.match(plan({ EVENT_NAME: 'workflow_run', RUN_CONCLUSION: 'failure', RUN_HEAD_BRANCH: 'v0.11.139' }).skip, /failure/);
    assert.match(plan({ EVENT_NAME: 'workflow_run', RUN_CONCLUSION: 'success', RUN_HEAD_BRANCH: 'main' }).skip, /not a release tag/);
  });
  it('moves latest on a manual run only when asked', () => {
    assert.deepEqual(plan({ EVENT_NAME: 'workflow_dispatch', INPUT_VERSION: '0.11.138', INPUT_LATEST: 'false' }).tags, ['0.11.138']);
    assert.deepEqual(plan({ EVENT_NAME: 'workflow_dispatch', INPUT_VERSION: 'v0.11.138', INPUT_LATEST: 'true' }).tags, ['0.11.138', 'latest']);
    assert.throws(() => plan({ EVENT_NAME: 'workflow_dispatch', INPUT_VERSION: 'nope' }), /not an intentd release version/);
  });
  it('builds a private pr tag from the newest release on a pull request', () => {
    assert.deepEqual(plan({ EVENT_NAME: 'pull_request', PR_NUMBER: '42', LATEST_RELEASE_TAG: 'v0.11.139' }), {
      version: '0.11.139', tags: ['pr-42'], publish: false,
    });
    assert.throws(() => plan({ EVENT_NAME: 'pull_request', PR_NUMBER: '4x', LATEST_RELEASE_TAG: 'v0.11.139' }), /PR number/);
  });
  it('refuses unknown events', () => {
    assert.throws(() => plan({ EVENT_NAME: 'push' }), /unsupported event/);
  });
});

describe('defineTemplate', () => {
  it('installs the pinned intentd archive with its checksum', () => {
    const command = installIntentdCommand('0.11.139', SHA);
    assert.ok(command.includes(archiveUrl('0.11.139')));
    assert.ok(command.includes(`${SHA}  `));
    assert.match(command, /sha256sum -c/);
    assert.equal(archiveUrl('0.11.139'),
      'https://github.com/intent-hq/intentd-releases/releases/download/v0.11.139/intentd-x86_64-unknown-linux-musl.tar.xz');
  });
  it('re-validates its inputs', () => {
    assert.throws(() => installIntentdCommand('0.1.0;id', SHA), /not an intentd release version/);
    assert.throws(() => installIntentdCommand('0.1.0', 'abc'), /SHA-256/);
  });
  it('never sets a start or ready command, so no daemon identity is snapshotted', () => {
    const { calls, Template } = recorder();
    defineTemplate(Template, { version: '0.11.139', sha256: SHA });
    const names = calls.map(([name]) => name);
    assert.ok(names.includes('fromImage'));
    assert.ok(!names.some(name => /start|ready/i.test(name)), names.join(','));
    const commands = calls.filter(([name]) => name === 'runCmd').map(([, command]) => command).join('\n');
    assert.doesNotMatch(commands, /intentd (serve|start)|executor\.mint/);
    assert.match(commands, /@anthropic-ai\/claude-code@\d/);
    assert.match(commands, /@openai\/codex@\d/);
    assert.match(commands, /pnpm@\d/);
  });
});

describe('smoke', () => {
  const outputs = { version: 'intentd 0.11.139\n', idle: '' };
  it('passes when the version matches and no intentd runs', () => {
    assert.deepEqual(verifySmoke('0.11.139', outputs), []);
  });
  it('reports a version mismatch and a running daemon', () => {
    const problems = verifySmoke('0.11.139', { version: 'intentd 0.11.138\n', idle: '123\n' });
    assert.equal(problems.length, 2);
    assert.match(problems.join('\n'), /0\.11\.138/);
    assert.match(problems.join('\n'), /running/);
  });
  it('checks every tool the template promises', () => {
    const tools = smokeChecks().tools.join(' ');
    for (const tool of ['intentd', 'tailcat', 'git', 'git-lfs', 'gh', 'rg', 'node', 'pnpm', 'python3', 'cc', 'make', 'claude', 'codex']) {
      assert.match(tools, new RegExp(`(^| )${tool}( |$)`), tool);
    }
  });
});

describe('secrets', () => {
  it('redacts the API key from any message', () => {
    assert.equal(redact('bad key e2b_abc123 here e2b_abc123', 'e2b_abc123'), 'bad key [redacted] here [redacted]');
    assert.equal(redact('nothing', ''), 'nothing');
  });
  it('publishes with the key in a header and keeps it out of errors', async () => {
    const seen = [];
    const fetchOk = async (url, init) => {
      seen.push([url, init]);
      return { ok: true, status: 200, json: async () => ({ names: ['intent-hq/intent-node'] }) };
    };
    assert.deepEqual(await publishTemplate({ templateId: 'tpl1', apiKey: 'e2b_secret', fetch: fetchOk }), ['intent-hq/intent-node']);
    assert.equal(seen[0][0], 'https://api.e2b.app/v2/templates/tpl1');
    assert.equal(seen[0][1].method, 'PATCH');
    assert.equal(seen[0][1].headers['X-API-Key'], 'e2b_secret');
    assert.deepEqual(JSON.parse(seen[0][1].body), { public: true });

    const fetchFail = async () => ({ ok: false, status: 403, text: async () => 'key e2b_secret is not allowed' });
    await assert.rejects(publishTemplate({ templateId: 'tpl1', apiKey: 'e2b_secret', fetch: fetchFail }), error => {
      assert.match(error.message, /403/);
      assert.doesNotMatch(error.message, /e2b_secret/);
      return true;
    });
  });
});

describe('workflow contract', () => {
  const workflow = readFileSync(join(ROOT, '.github/workflows/e2b-template.yml'), 'utf8');
  it('runs after the Release workflow and by hand', () => {
    assert.match(workflow, /workflow_run:\n\s+workflows: \[Release\]\n\s+types: \[completed\]/);
    assert.match(workflow, /workflow_dispatch:/);
    assert.equal(JSON.parse(readFileSync(join(ROOT, 'packaging/e2b/package.json'), 'utf8')).name, 'intent-node-e2b-template');
    assert.match(readFileSync(join(ROOT, '.github/workflows/v-release.yml'), 'utf8'), /^name: Release$/m);
  });
  it('is fail-soft without the E2B_API_KEY secret', () => {
    assert.match(workflow, /HAS_KEY: \$\{\{ secrets\.E2B_API_KEY != '' \}\}/);
    assert.match(workflow, /::warning::E2B_API_KEY secret is not set/);
  });
  it('gives the key only to the build and smoke steps', () => {
    assert.equal(workflow.match(/E2B_API_KEY: \$\{\{ secrets\.E2B_API_KEY \}\}/g).length, 2);
    assert.doesNotMatch(workflow.split('\n    steps:\n')[0], /secrets\.E2B_API_KEY/);
    assert.match(workflow, /npm ci --ignore-scripts/);
  });
  it('skips sitter releases when it picks the newest daemon release', () => {
    assert.match(workflow, /select\(\.tagName \| test\("\^v\[0-9\]\+/);
  });
  it('queues release builds instead of dropping them', () => {
    assert.match(workflow, /cancel-in-progress: false\n\s+queue: max/);
  });
  it('builds the template it then smoke-tests', () => {
    assert.equal(TEMPLATE_NAME, 'intent-node');
    assert.match(workflow, /node build\.mjs/);
    assert.match(workflow, /node smoke\.mjs/);
  });
});
