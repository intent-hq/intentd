#!/usr/bin/env node
// Tests for the E2B intent-node template (packaging/e2b) and its workflow.
// Run: ./scripts/test-e2b-template.mjs (node:test, offline, no installs).
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import {
  DEFAULT_SIZE,
  MANIFEST_ASSET,
  SIZES,
  archiveUrl,
  builtSizes,
  defineTemplate,
  enabledSizes,
  installIntentdCommand,
  isResourceLimitRefusal,
  manifestFor,
  parseSha256,
  parseSize,
  parseVersion,
  plan,
  publishTemplate,
  redact,
  smokeChecks,
  templateName,
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

describe('sizes', () => {
  it('offers 2x4, 4x8 (default), 8x16 and 16x32, one template each', () => {
    assert.deepEqual(SIZES.map(size => [size.id, size.cpuCount, size.memoryMB]), [
      ['2x4', 2, 4096], ['4x8', 4, 8192], ['8x16', 8, 16384], ['16x32', 16, 32768],
    ]);
    assert.equal(DEFAULT_SIZE, '4x8');
    assert.deepEqual(SIZES.map(size => templateName(size.id)),
      ['intent-node-2x4', 'intent-node-4x8', 'intent-node-8x16', 'intent-node-16x32']);
  });
  it('rejects unknown sizes', () => {
    assert.equal(parseSize('16x32').memoryMB, 32768);
    for (const bad of ['', '32x64', '4X8', '4x8;id']) assert.throws(() => parseSize(bad), /unknown template size/, bad);
  });
  it('builds every size unless E2B_TEMPLATE_SIZES narrows the list', () => {
    assert.deepEqual(enabledSizes(undefined), ['2x4', '4x8', '8x16', '16x32']);
    assert.deepEqual(enabledSizes(''), ['2x4', '4x8', '8x16', '16x32']);
    assert.deepEqual(enabledSizes(' 2x4, 4x8 '), ['2x4', '4x8']);
    assert.throws(() => enabledSizes('2x4,32x64'), /unknown template size/);
  });
  it('treats an E2B refusal over project CPU or memory limits as a skip', () => {
    for (const message of [
      '400: CPU count exceeds team limits (8)',
      '400: Memory exceeds team limits (8192 MiB)',
      '400: CPU count exceeds project limits (8)',
      '403: requested vCPU 16 is above the maximum allowed for your team',
      '400: memoryMB must be at most 8192',
    ]) assert.equal(isResourceLimitRefusal(new Error(message)), true, message);
  });
  it('fails the build on any other error', () => {
    for (const message of [
      'Unauthorized, please check your credentials.',
      'Rate limit exceeded, please try again later',
      '500: internal error while allocating memory',
      '400: invalid template name',
      'Build failed: npm ERR! JavaScript heap out of memory, CPU limit',
    ]) assert.equal(isResourceLimitRefusal(new Error(message)), false, message);
    assert.equal(isResourceLimitRefusal(undefined), false);
  });
});

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
  const outputs = { version: 'intentd 0.11.139\n', idle: '', cpus: '4\n', memKb: '8000000\n' };
  it('passes when the version and size match and no intentd runs', () => {
    assert.deepEqual(verifySmoke('0.11.139', '4x8', outputs), []);
  });
  it('reports a version mismatch and a running daemon', () => {
    const problems = verifySmoke('0.11.139', '4x8', { ...outputs, version: 'intentd 0.11.138\n', idle: '123\n' });
    assert.equal(problems.length, 2);
    assert.match(problems.join('\n'), /0\.11\.138/);
    assert.match(problems.join('\n'), /running/);
  });
  it('reports a sandbox smaller than its size', () => {
    const problems = verifySmoke('0.11.139', '8x16', outputs);
    assert.equal(problems.length, 2);
    assert.match(problems.join('\n'), /4 vCPU, expected 8/);
    assert.match(problems.join('\n'), /MiB of RAM, expected about 16384/);
  });
  it('checks every tool the template promises', () => {
    const tools = smokeChecks().tools.join(' ');
    for (const tool of ['intentd', 'tailcat', 'git', 'git-lfs', 'gh', 'rg', 'node', 'pnpm', 'python3', 'cc', 'make', 'claude', 'codex']) {
      assert.match(tools, new RegExp(`(^| )${tool}( |$)`), tool);
    }
  });
});

describe('manifest', () => {
  it('lists the built sizes in size order for one project and version', () => {
    assert.equal(MANIFEST_ASSET, 'e2b-templates.json');
    assert.deepEqual(manifestFor({ project: 'intent', version: 'v0.11.147', sizes: ['4x8', '2x4'] }), {
      schema: 1, project: 'intent', version: '0.11.147', sizes: ['2x4', '4x8'],
    });
    assert.deepEqual(manifestFor({ project: 'intent', version: '0.11.147', sizes: [] }).sizes, []);
  });
  it('refuses a bad project, version or size', () => {
    assert.throws(() => manifestFor({ project: 'Intent/x', version: '0.11.147', sizes: [] }), /project slug/);
    assert.throws(() => manifestFor({ project: 'intent', version: 'main', sizes: [] }), /not an intentd release version/);
    assert.throws(() => manifestFor({ project: 'intent', version: '0.11.147', sizes: ['3x6'] }), /unknown template size/);
  });
  it('checks each size tag of the version with the publishing key', async () => {
    const asked = [];
    const sizes = await builtSizes({
      version: '0.11.147',
      exists: async name => { asked.push(name); return !name.includes('8x16') && !name.includes('16x32'); },
    });
    assert.deepEqual(sizes, ['2x4', '4x8']);
    assert.deepEqual(asked, ['intent-node-2x4:0.11.147', 'intent-node-4x8:0.11.147', 'intent-node-8x16:0.11.147', 'intent-node-16x32:0.11.147']);
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
  const build = readFileSync(join(ROOT, '.github/workflows/e2b-template-build.yml'), 'utf8');
  const job = name => workflow.split(`\n  ${name}:\n`)[1].split(/\n  [\w-]+:\n/)[0];
  it('runs after the Release workflow and by hand', () => {
    assert.match(workflow, /workflow_run:\n\s+workflows: \[Release\]\n\s+types: \[completed\]/);
    assert.match(workflow, /workflow_dispatch:/);
    assert.equal(JSON.parse(readFileSync(join(ROOT, 'packaging/e2b/package.json'), 'utf8')).name, 'intent-node-e2b-template');
    assert.match(readFileSync(join(ROOT, '.github/workflows/v-release.yml'), 'utf8'), /^name: Release$/m);
  });
  it('is fail-soft without the E2B_API_KEY secret', () => {
    assert.match(build, /E2B_API_KEY:\n\s+required: false/);
    assert.match(build, /HAS_KEY: \$\{\{ secrets\.E2B_API_KEY != '' \}\}/);
    assert.match(build, /::warning::E2B_API_KEY secret is not set/);
  });
  it('gives the key only to the build, smoke and manifest steps', () => {
    assert.equal(build.match(/E2B_API_KEY: \$\{\{ secrets\.E2B_API_KEY \}\}/g).length, 3);
    assert.doesNotMatch(build.split('\n    steps:\n')[0], /secrets\.E2B_API_KEY \}\}/);
    assert.match(build, /npm ci --ignore-scripts/);
  });
  it('skips sitter releases when it picks the newest daemon release', () => {
    assert.match(build, /select\(\.tagName \| test\("\^v\[0-9\]\+/);
  });
  it('queues release builds and cancels superseded PR builds', () => {
    assert.match(job('release'), /if: github\.event_name != 'pull_request'/);
    assert.match(job('release'), /cancel-in-progress: false\n\s+queue: max/);
    assert.match(job('pull-request'), /head\.repo\.full_name == github\.repository/);
    assert.match(job('pull-request'), /cancel-in-progress: true/);
    assert.doesNotMatch(job('pull-request'), /queue:/);
    for (const name of ['release', 'pull-request']) assert.match(job(name), /uses: \.\/\.github\/workflows\/e2b-template-build\.yml/);
  });
  it('builds and smoke-tests every size in parallel legs', () => {
    assert.match(build, /fail-fast: false\n\s+matrix:\n\s+size: \['2x4', '4x8', '8x16', '16x32'\]/);
    assert.match(build, /SIZE: \$\{\{ matrix\.size \}\}/);
    assert.match(build, /ENABLED_SIZES: \$\{\{ vars\.E2B_TEMPLATE_SIZES \}\}/);
    assert.match(build, /node build\.mjs/);
    assert.match(build, /node smoke\.mjs "\$REF" "\$VERSION" "\$SIZE"/);
  });
  it('attaches the manifest to the release after release and manual builds only', () => {
    const manifest = build.split('\n  manifest:\n')[1];
    assert.match(manifest, /needs: template/);
    assert.match(manifest, /if: \$\{\{ !cancelled\(\) && github\.event_name != 'pull_request' \}\}/);
    assert.match(manifest, /node manifest\.mjs/);
    assert.match(manifest, /gh release upload "v\$VERSION" e2b-templates\.json --repo intent-hq\/intentd-releases --clobber/);
    assert.match(manifest, /GH_TOKEN: \$\{\{ secrets\.INTENTD_RELEASES_TOKEN \}\}/);
    assert.match(build, /template:\n\s+if: \$\{\{ !inputs\.manifest_only \}\}/);
    assert.match(job('release'), /manifest_only: \$\{\{ inputs\.manifest_only \|\| false \}\}/);
    assert.match(job('release'), /INTENTD_RELEASES_TOKEN: \$\{\{ secrets\.INTENTD_RELEASES_TOKEN \}\}/);
    assert.doesNotMatch(job('pull-request'), /INTENTD_RELEASES_TOKEN/);
  });
});
