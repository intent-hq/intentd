#!/usr/bin/env node
// Tests for the E2B intent-node template (packaging/e2b) and its workflow.
// Run: ./scripts/test-e2b-template.mjs (node:test, offline, no installs).
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import { compareLayers, imageLayers, parseRef, platformDigest, refDigest, report } from '../packaging/e2b/layers.mjs';
import { publish, vcrConfig, vcrTarget, vercelToken, waitReady } from '../packaging/e2b/vcr.mjs';
import {
  APT_PACKAGES,
  BASE_IMAGE,
  baseImage,
  DEFAULT_SIZE,
  GH,
  IMAGE_REPO,
  MANIFEST_ASSET,
  SIZES,
  archiveUrl,
  builtSizes,
  defineTemplate,
  addUserCommand,
  dockerfile,
  enabledSizes,
  imagePlan,
  installGhCommand,
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
  verifyImageSmoke,
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
  it('installs a pinned gh release instead of the old Debian package', () => {
    assert.match(GH.version, /^\d+\.\d+\.\d+$/);
    assert.match(GH.sha256, /^[0-9a-f]{64}$/);
    const command = installGhCommand();
    assert.ok(command.includes(`https://github.com/cli/cli/releases/download/v${GH.version}/gh_${GH.version}_linux_amd64.tar.gz`));
    assert.ok(command.includes(`${GH.sha256}  `));
    assert.match(command, /sha256sum -c/);
    assert.ok(!APT_PACKAGES.includes('gh'));
    const { calls, Template } = recorder();
    defineTemplate(Template, { version: '0.11.139', sha256: SHA });
    assert.ok(calls.some(([name, cmd, opts]) => name === 'runCmd' && cmd === command && opts?.user === 'root'));
  });
});

describe('container image', () => {
  it('publishes ghcr.io/intent-hq/intent-node with the template tags', () => {
    assert.equal(IMAGE_REPO, 'ghcr.io/intent-hq/intent-node');
    assert.deepEqual(imagePlan({ EVENT_NAME: 'workflow_run', RUN_CONCLUSION: 'success', RUN_HEAD_BRANCH: 'v0.11.139' }), {
      version: '0.11.139', tags: ['ghcr.io/intent-hq/intent-node:0.11.139', 'ghcr.io/intent-hq/intent-node:latest'], push: true,
    });
    assert.deepEqual(imagePlan({ EVENT_NAME: 'workflow_dispatch', INPUT_VERSION: '0.11.138', INPUT_LATEST: 'false' }).tags,
      ['ghcr.io/intent-hq/intent-node:0.11.138']);
  });
  it('builds a pull request image without pushing it, with the previous release as its baseline', () => {
    const env = { EVENT_NAME: 'pull_request', PR_NUMBER: '42', LATEST_RELEASE_TAG: 'v0.11.139', PREVIOUS_RELEASE_TAG: 'v0.11.138' };
    assert.deepEqual(imagePlan(env), {
      version: '0.11.139', tags: ['ghcr.io/intent-hq/intent-node:pr-42'], push: false, baseline: '0.11.138',
    });
    assert.throws(() => imagePlan({ ...env, PREVIOUS_RELEASE_TAG: '' }), /not an intentd release version/);
    assert.throws(() => imagePlan({ ...env, PREVIOUS_RELEASE_TAG: 'v0.11.139' }), /is the newest release/);
    assert.equal(imagePlan({ EVENT_NAME: 'workflow_dispatch', INPUT_VERSION: '0.11.138', PREVIOUS_RELEASE_TAG: 'v0.11.137' }).baseline, undefined);
  });
  it('passes skips through', () => {
    assert.match(imagePlan({ EVENT_NAME: 'workflow_run', RUN_CONCLUSION: 'failure', RUN_HEAD_BRANCH: 'v0.11.139' }).skip, /failure/);
  });
  it('runs the same steps as the E2B template, as root, in order', () => {
    const text = dockerfile({ version: '0.11.139', sha256: SHA });
    const lines = text.trim().split('\n');
    assert.equal(lines[0], `FROM ${BASE_IMAGE}`);
    const runs = lines.filter(line => line.startsWith('RUN '));
    assert.equal(runs[0], `RUN ${addUserCommand()}`);
    const apt = runs[1];
    assert.match(apt, /apt-get update/);
    for (const pkg of APT_PACKAGES) assert.match(apt, new RegExp(` ${pkg.replace(/[.+]/g, '\\$&')}( |$)`), pkg);
    assert.match(apt, /rm -rf \/var\/lib\/apt\/lists/);
    assert.doesNotMatch(apt, /no-install-recommends/, 'E2B aptInstall keeps recommended packages');
    const { calls, Template } = recorder();
    defineTemplate(Template, { version: '0.11.139', sha256: SHA });
    const commands = calls.filter(([name]) => name === 'runCmd').map(([, command]) => command);
    assert.deepEqual(runs.slice(2).map(line => line.slice(4)), commands);
    const firstUser = lines.findIndex(line => line.startsWith('USER '));
    assert.ok(firstUser > lines.indexOf(runs.at(-1)), 'every template step runs as root');
    assert.ok(text.includes(archiveUrl('0.11.139')));
  });
  it('runs containers as the unprivileged user, with its home where the volume mounts', () => {
    const lines = dockerfile({ version: '0.11.139', sha256: SHA }).trim().split('\n');
    assert.ok(lines.includes(`RUN ${addUserCommand()}`));
    assert.match(addUserCommand(), /usermod .*-l user .*node/);
    assert.match(addUserCommand(), /-d \/home\/user/);
    assert.match(addUserCommand(), /groupmod -n user node/);
    assert.deepEqual(lines.filter(line => line.startsWith('USER ')), ['USER user']);
    assert.ok(lines.indexOf('USER user') > lines.indexOf(`RUN ${addUserCommand()}`));
    assert.ok(lines.includes('WORKDIR /home/user'));
  });
  it('pins the base by digest, so its layers and the cache above them stay put', () => {
    assert.match(BASE_IMAGE, /^node:24-bookworm@sha256:[0-9a-f]{64}$/);
    assert.throws(() => baseImage('FROM node:24-bookworm\n'), /pinned by digest/);
    assert.throws(() => baseImage(`FROM ${BASE_IMAGE}\nFROM ${BASE_IMAGE}\n`), /one FROM line/);
    const dependabot = readFileSync(join(ROOT, '.github/dependabot.yml'), 'utf8');
    assert.match(dependabot, /package-ecosystem: "docker"\n\s+directory: "\/packaging\/e2b"/);
    const { calls, Template } = recorder();
    defineTemplate(Template, { version: '0.11.139', sha256: SHA });
    assert.deepEqual(calls[0], ['fromImage', BASE_IMAGE]);
  });
  it('orders layers from least to most often changed, intentd last', () => {
    const a = dockerfile({ version: '0.11.138', sha256: SHA }).trim().split('\n');
    const b = dockerfile({ version: '0.11.139', sha256: 'f'.repeat(64) }).trim().split('\n');
    const differ = a.map((line, i) => i).filter(i => a[i] !== b[i]);
    const lastRun = a.findLastIndex(line => line.startsWith('RUN '));
    assert.deepEqual(differ.filter(i => i <= lastRun), [lastRun], 'only the last RUN depends on the intentd version');
    assert.ok(a[lastRun].includes(archiveUrl('0.11.138')));
    for (const i of differ) assert.ok(i === lastRun || !/^(RUN|COPY|ADD) /.test(a[i]), a[i]);
    const runs = a.filter(line => line.startsWith('RUN '));
    assert.ok(runs.findIndex(line => line.includes('npm install')) > runs.findIndex(line => line.includes('apt-get')));
  });
  it('leaves the E2B template without the user step', () => {
    const { calls, Template } = recorder();
    defineTemplate(Template, { version: '0.11.139', sha256: SHA });
    assert.ok(!calls.some(([, command]) => String(command).includes('usermod')));
  });
  it('labels the image with its source and version', () => {
    const text = dockerfile({ version: '0.11.139', sha256: SHA });
    assert.match(text, /^LABEL org\.opencontainers\.image\.source="https:\/\/github\.com\/intent-hq\/intentd"$/m);
    assert.match(text, /^LABEL org\.opencontainers\.image\.version="0\.11\.139"$/m);
  });
  it('idles instead of starting intentd, so the backend starts it with its own identity', () => {
    const text = dockerfile({ version: '0.11.139', sha256: SHA });
    assert.match(text, /^CMD \["sleep", "infinity"\]$/m);
    assert.doesNotMatch(text, /ENTRYPOINT|intentd (serve|start)/);
  });
  it('re-validates its inputs', () => {
    assert.throws(() => dockerfile({ version: '0.1.0\nRUN id', sha256: SHA }), /not an intentd release version/);
    assert.throws(() => dockerfile({ version: '0.1.0', sha256: 'abc' }), /SHA-256/);
  });
  it('smoke-checks the version, gh and an idle default command', () => {
    const outputs = { version: 'intentd 0.11.139\n', idle: '', gh: `gh version ${GH.version} (2026-10-26)\n`, user: 'user\n/home/user\n' };
    assert.deepEqual(verifyImageSmoke('0.11.139', outputs), []);
    const problems = verifyImageSmoke('0.11.139', { ...outputs, version: 'intentd 0.11.138\n', idle: '7\n', gh: 'gh version 2.23.0\n' });
    assert.equal(problems.length, 3);
    assert.match(problems.join('\n'), /0\.11\.138/);
    assert.match(problems.join('\n'), /running/);
    assert.match(problems.join('\n'), /2\.23\.0/);
  });
  it('smoke-checks that the container runs as user', () => {
    const outputs = { version: 'intentd 0.11.139\n', idle: '', gh: `gh version ${GH.version} (2026-10-26)\n` };
    assert.equal(smokeChecks().user, 'id -un; echo "$HOME"');
    assert.deepEqual(verifyImageSmoke('0.11.139', { ...outputs, user: 'user\n/home/user\n' }), []);
    const problems = verifyImageSmoke('0.11.139', { ...outputs, user: 'root\n/root\n' });
    assert.equal(problems.length, 1);
    assert.match(problems[0], /root/);
    assert.match(verifyImageSmoke('0.11.139', outputs).join('\n'), /runs as/, 'the image smoke must collect the user');
  });
});

describe('smoke', () => {
  const outputs = {
    version: 'intentd 0.11.139\n', idle: '', cpus: '4\n', memKb: '8000000\n',
    gh: `gh version ${GH.version} (2026-10-26)\nhttps://github.com/cli/cli/releases/tag/v${GH.version}\n`,
  };
  it('passes when the version and size match and no intentd runs', () => {
    assert.deepEqual(verifySmoke('0.11.139', '4x8', outputs), []);
  });
  it('reports a version mismatch and a running daemon', () => {
    const problems = verifySmoke('0.11.139', '4x8', { ...outputs, version: 'intentd 0.11.138\n', idle: '123\n' });
    assert.equal(problems.length, 2);
    assert.match(problems.join('\n'), /0\.11\.138/);
    assert.match(problems.join('\n'), /running/);
  });
  it('reports a stale gh', () => {
    const problems = verifySmoke('0.11.139', '4x8', { ...outputs, gh: 'gh version 2.23.0 (2023-02-14)\n' });
    assert.deepEqual(problems, [`gh --version printed "gh version 2.23.0 (2023-02-14)", expected "gh version ${GH.version}"`]);
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

describe('image workflow contract', () => {
  const workflow = readFileSync(join(ROOT, '.github/workflows/intent-node-image.yml'), 'utf8');
  const build = readFileSync(join(ROOT, '.github/workflows/intent-node-image-build.yml'), 'utf8');
  const steps = build.split('\n    steps:\n')[1];
  const job = name => workflow.split(`\n  ${name}:\n`)[1].split(/\n  [\w-]+:\n/)[0];
  it('runs after the Release workflow, by hand and on PRs that touch it', () => {
    assert.match(workflow, /workflow_run:\n\s+workflows: \[Release\]\n\s+types: \[completed\]/);
    assert.match(workflow, /workflow_dispatch:\n\s+inputs:\n\s+version:/);
    assert.match(workflow, /pull_request:\n\s+paths:\n\s+- packaging\/e2b\/\*\*\n\s+- \.github\/workflows\/intent-node-image\.yml\n\s+- \.github\/workflows\/intent-node-image-build\.yml/);
  });
  it('builds linux/amd64 and smoke-tests before it pushes', () => {
    assert.match(steps, /node image\.mjs/);
    assert.match(steps, /docker buildx build --platform linux\/amd64 --pull --cache-to type=inline "\$\{args\[@\]\}"/);
    assert.match(steps, /args\+=\(--output "type=docker,name=\$REF"\)/);
    assert.ok(steps.indexOf('node smoke-image.mjs') < steps.indexOf('--push'));
    assert.doesNotMatch(steps, /docker (build|push) /, 'buildx keeps cached layers as the same blobs');
  });
  it('reuses the previous image as its layer cache', () => {
    assert.match(steps, /uses: docker\/setup-buildx-action@v\d+\n.*\n\s+with:\n\s+driver-opts: network=host/);
    assert.match(steps, /--cache-from "type=registry,ref=ghcr\.io\/intent-hq\/intent-node:latest"/);
    assert.match(steps, /--cache-to type=inline --push/);
  });
  it('fails a PR whose recipe would add more than the intentd layer per release', () => {
    assert.match(build, /LAYER_BUDGET_MB: \d+/);
    assert.match(build, /image: \$\{\{ github\.event_name == 'pull_request' && 'registry:2' \|\| '' \}\}/);
    assert.match(steps, /--file Dockerfile\.baseline/);
    assert.match(steps, /--cache-from "type=registry,ref=\$LOCAL_IMAGE:baseline,registry\.insecure=true"/);
    assert.ok(steps.indexOf('docker buildx prune --all --force') < steps.indexOf('Only the baseline as cache'), 'the PR build only has the registry cache');
    const pr = steps.split('if [ -n "$PR_NUMBER" ]; then')[1].split('else')[0];
    assert.doesNotMatch(pr, /args\+=\(--tag/, 'a tag would rename the local registry export');
    assert.match(steps, /node layers\.mjs "\$LOCAL_IMAGE:baseline" "\$LOCAL_IMAGE:pr-\$PR_NUMBER" --budget-mb "\$LAYER_BUDGET_MB"/);
    assert.match(steps, /PREVIOUS_RELEASE_TAG: \$\{\{ steps\.latest\.outputs\.previous \}\}/);
  });
  it('grants packages: write only to release and manual runs, which alone push', () => {
    assert.match(job('release'), /permissions:\n\s+contents: read\n\s+packages: write/);
    assert.doesNotMatch(job('pull-request'), /packages: write/);
    assert.match(steps, /if: steps\.plan\.outputs\.push == 'true'/);
  });
  it('is fail-soft when GitHub refuses the push', () => {
    assert.match(steps, /::warning::/);
    assert.match(steps, /denied\|unauthorized\|403/);
  });
  it('queues release runs and cancels superseded PR runs', () => {
    assert.match(job('release'), /if: github\.event_name != 'pull_request'/);
    assert.match(job('release'), /cancel-in-progress: false\n\s+queue: max/);
    assert.match(job('pull-request'), /cancel-in-progress: true/);
    for (const name of ['release', 'pull-request']) assert.match(job(name), /uses: \.\/\.github\/workflows\/intent-node-image-build\.yml/);
  });
});

describe('layers', () => {
  const digest = c => `sha256:${c.repeat(64)}`;
  function registry(images) {
    const calls = [];
    const fetch = async (url, { headers = {} } = {}) => {
      calls.push([url, headers]);
      if (url.endsWith('/token?scope=repository:intent-hq/intent-node:pull')) return Response.json({ token: 't' });
      const path = new URL(url).pathname.replace('/v2/intent-hq/intent-node', '').replace('/v2/intent-node', '');
      const body = images[path];
      if (!body) return new Response('', { status: 404 });
      return Response.json(body, { headers: { 'docker-content-digest': digest('9') } });
    };
    return { calls, fetch };
  }
  const config = steps => ({
    history: steps.flatMap(step => [{ created_by: 'LABEL x', empty_layer: true }, { created_by: step }]),
  });
  it('parses tag and digest references', () => {
    assert.deepEqual(parseRef('ghcr.io/intent-hq/intent-node:0.11.1'), { registry: 'ghcr.io', repository: 'intent-hq/intent-node', reference: '0.11.1' });
    assert.deepEqual(parseRef(`localhost:5000/intent-node@${digest('a')}`), { registry: 'localhost:5000', repository: 'intent-node', reference: digest('a') });
    assert.throws(() => parseRef('intent-node'), /not a registry/);
    assert.throws(() => parseRef('ghcr.io/intent-hq/intent-node'), /not a registry/);
  });
  it('reads the amd64 image of an index, pairing each blob with its step', async () => {
    const { calls, fetch } = registry({
      '/manifests/latest': { manifests: [
        { digest: digest('1'), platform: { os: 'linux', architecture: 'arm64' } },
        { digest: digest('2'), platform: { os: 'linux', architecture: 'amd64' } },
      ] },
      [`/manifests/${digest('2')}`]: { config: { digest: digest('3') }, layers: [{ digest: digest('a'), size: 10 }, { digest: digest('b'), size: 20 }] },
      [`/blobs/${digest('3')}`]: config(['RUN apt', 'RUN intentd']),
    });
    assert.deepEqual(await imageLayers('ghcr.io/intent-hq/intent-node:latest', fetch), [
      { digest: digest('a'), size: 10, step: 'RUN apt' },
      { digest: digest('b'), size: 20, step: 'RUN intentd' },
    ]);
    assert.ok(calls.slice(1).every(([url, headers]) => url.startsWith('https://ghcr.io/') && headers.Authorization === 'Bearer t'));
  });
  it('talks plain HTTP without a token to a localhost registry', async () => {
    const { calls, fetch } = registry({
      '/manifests/pr-1': { config: { digest: digest('3') }, layers: [{ digest: digest('a'), size: 5 }] },
      [`/blobs/${digest('3')}`]: config(['RUN a']),
    });
    await imageLayers('localhost:5000/intent-node:pr-1', fetch);
    assert.ok(calls.every(([url, headers]) => url.startsWith('http://localhost:5000/v2/') && !headers.Authorization));
  });
  it('refuses a config whose history does not match the layers', async () => {
    const { fetch } = registry({
      '/manifests/x': { config: { digest: digest('3') }, layers: [{ digest: digest('a'), size: 5 }] },
      [`/blobs/${digest('3')}`]: config(['RUN a', 'RUN b']),
    });
    await assert.rejects(imageLayers('localhost:5000/intent-node:x', fetch), /1 layers but 2 layer steps/);
  });
  it('resolves a tag to a digest reference', async () => {
    const { fetch } = registry({ '/manifests/latest': { manifests: [] } });
    assert.equal(await refDigest('ghcr.io/intent-hq/intent-node:latest', fetch), `ghcr.io/intent-hq/intent-node@${digest('9')}`);
  });
  it('counts only the new image blobs the old one lacks', () => {
    const older = [{ digest: 'a', size: 400, step: '' }, { digest: 'b', size: 300, step: '' }, { digest: 'c', size: 70, step: '' }];
    const newer = [{ digest: 'a', size: 400, step: '' }, { digest: 'b', size: 300, step: '' }, { digest: 'd', size: 72, step: '/bin/sh -c tar | x' }];
    const result = compareLayers(older, newer);
    assert.equal(result.total, 772);
    assert.equal(result.changed, 72);
    assert.deepEqual(result.layers.map(layer => layer.shared), [true, true, false]);
    const text = report('old', 'new', result);
    assert.match(text, /0\.0 MB of 0\.0 MB compressed; 1 of 3 layers/);
    assert.match(text, /\| 3 \| 0\.0 MB \| yes \| `tar \\\| x` \|/);
  });
});

describe('vercel registry', () => {
  const digest = c => `sha256:${c.repeat(64)}`;
  const AMD64 = digest('a');
  const IMAGE = 'vcr.vercel.com/sutterhill/intent-images/intent-node';
  const ENV = { VERCEL_TEAM_ID: 'team_abc', VCR_INTENT_NODE_IMAGE: IMAGE, VERCEL_TOKEN: 'vcp_secret' };
  const tagBody = (status, manifestDigest = AMD64) => ({ tag: { tag: '0.11.250', imageId: 'image_1', kind: 'manifest', manifestDigest, status } });

  // GHCR (anonymous) plus the Vercel API. `tags` is the sequence of answers to
  // the tag lookups: a body, or a number for an HTTP error status.
  function vercel({ tags = [], repository = { public: true }, exchange } = {}) {
    const calls = [];
    const fetch = async (url, init = {}) => {
      const method = init.method ?? 'GET';
      calls.push({ method, url: String(url), init });
      const { host, pathname } = new URL(url);
      if (host === 'ghcr.io' && pathname === '/token') return Response.json({ token: 't' });
      if (host === 'ghcr.io' && pathname === '/v2/intent-hq/intent-node/manifests/0.11.250') {
        return Response.json({ manifests: [
          { digest: digest('b'), platform: { os: 'unknown', architecture: 'unknown' } },
          { digest: AMD64, platform: { os: 'linux', architecture: 'amd64' } },
        ] }, { headers: { 'docker-content-digest': digest('c') } });
      }
      if (host === 'oidc.example') return Response.json({ value: 'github-oidc' });
      if (pathname === '/login/oauth/token') return exchange ? exchange(init) : Response.json({ access_token: 'vca_oidc' });
      if (pathname === '/login/oauth/token/revoke') return new Response('');
      if (pathname === '/v1/vcr/repository/intent-node') return Response.json({ repository: { name: 'intent-node', ...repository } });
      if (pathname === '/v1/vcr/repository/intent-node/tags/0.11.250') {
        const next = tags.length > 1 ? tags.shift() : tags[0];
        if (typeof next === 'number') return new Response('nope', { status: next });
        return Response.json(next);
      }
      return new Response('', { status: 404 });
    };
    return { calls, fetch };
  }
  function harness(options) {
    const api = vercel(options);
    const runs = [];
    const logs = [];
    let clock = 0;
    return {
      ...api,
      runs,
      logs,
      deps: {
        fetch: api.fetch,
        run: (command, args, { input } = {}) => {
          const at = args.indexOf('--authfile');
          runs.push({ command, args, input, auth: at >= 0 ? JSON.parse(readFileSync(args[at + 1], 'utf8')) : undefined });
        },
        sleep: async ms => { clock += ms; },
        now: () => clock,
        log: line => logs.push(line),
        timeoutMs: 60_000,
        intervalMs: 10_000,
      },
    };
  }

  it('reads the target from repository variables and is fail-soft without them', () => {
    assert.deepEqual(vcrConfig(ENV), {
      team: 'team_abc', image: IMAGE, registry: 'vcr.vercel.com', teamSlug: 'sutterhill', project: 'intent-images', repository: 'intent-node',
    });
    assert.match(vcrConfig({}).skip, /VERCEL_TEAM_ID and VCR_INTENT_NODE_IMAGE repository variables are not set/);
    assert.match(vcrConfig({ VCR_INTENT_NODE_IMAGE: IMAGE }).skip, /VERCEL_TEAM_ID/);
    assert.throws(() => vcrConfig({ ...ENV, VERCEL_TEAM_ID: 'sutterhill' }), /team ID/);
    assert.throws(() => vcrConfig({ ...ENV, VCR_INTENT_NODE_IMAGE: `${IMAGE}:latest` }), /without a tag/);
    assert.throws(() => vcrConfig({ ...ENV, VCR_INTENT_NODE_IMAGE: 'ghcr.io/a/b/c' }), /vcr\.vercel\.com/);
  });
  it('tags only the exact version, never latest, alpha or beta', () => {
    const config = vcrConfig(ENV);
    assert.equal(vcrTarget(config, 'v0.11.250'), `${IMAGE}:0.11.250`);
    for (const tag of ['latest', 'alpha', 'beta', '', '0.11']) assert.throws(() => vcrTarget(config, tag));
  });
  it('resolves the linux/amd64 image of the GHCR index, not the index', async () => {
    const { fetch } = vercel();
    assert.equal(await platformDigest('ghcr.io/intent-hq/intent-node:0.11.250', fetch), AMD64);
  });
  it('uses VERCEL_TOKEN, else exchanges the GitHub OIDC token, else refuses', async () => {
    const { fetch, calls } = vercel();
    assert.deepEqual(await vercelToken(ENV, 'team_abc', fetch), { token: 'vcp_secret', oidc: false });
    const oidcEnv = { ACTIONS_ID_TOKEN_REQUEST_URL: 'https://oidc.example/token?x=1', ACTIONS_ID_TOKEN_REQUEST_TOKEN: 'req' };
    assert.deepEqual(await vercelToken(oidcEnv, 'team_abc', fetch), { token: 'vca_oidc', oidc: true });
    assert.equal(calls[0].init.headers.Authorization, 'Bearer req');
    const body = new URLSearchParams(calls[1].init.body);
    assert.equal(body.get('subject_token'), 'github-oidc');
    assert.equal(body.get('team_id_or_slug'), 'team_abc');
    await assert.rejects(vercelToken({}, 'team_abc', fetch), /id-token: write/);
  });
  it('keeps the token out of exchange errors', async () => {
    const { fetch } = vercel({ exchange: () => new Response('no policy for github-oidc', { status: 403 }) });
    const oidcEnv = { ACTIONS_ID_TOKEN_REQUEST_URL: 'https://oidc.example/token', ACTIONS_ID_TOKEN_REQUEST_TOKEN: 'req' };
    await assert.rejects(vercelToken(oidcEnv, 'team_abc', fetch), error => {
      assert.match(error.message, /HTTP 403/);
      assert.match(error.message, /OIDC policy/);
      assert.doesNotMatch(error.message, /github-oidc/);
      return true;
    });
  });

  it('warns and succeeds without the credential', async () => {
    const h = harness();
    assert.deepEqual(await publish({ env: {}, version: '0.11.250', ...h.deps }), { skipped: true });
    assert.match(h.logs.join('\n'), /::warning::.*VERCEL_TEAM_ID/);
    assert.equal(h.runs.length, 0);
    assert.equal(h.calls.length, 0);
  });
  it('warns and succeeds when Vercel refuses the OIDC exchange (no policy yet)', async () => {
    const h = harness({ exchange: () => new Response('no matching policy', { status: 403 }) });
    const env = { VERCEL_TEAM_ID: 'team_abc', VCR_INTENT_NODE_IMAGE: IMAGE, ACTIONS_ID_TOKEN_REQUEST_URL: 'https://oidc.example/t', ACTIONS_ID_TOKEN_REQUEST_TOKEN: 'r' };
    assert.deepEqual(await publish({ env, version: '0.11.250', ...h.deps }), { skipped: true });
    assert.match(h.logs.join('\n'), /::warning::.*OIDC policy/);
    assert.equal(h.runs.length, 0);
    const transient = harness({ exchange: () => new Response('', { status: 503 }) });
    await assert.rejects(publish({ env, version: '0.11.250', ...transient.deps }), /HTTP 503/);
  });
  it('fails when an explicit VERCEL_TOKEN is refused', async () => {
    const h = harness({ tags: [401] });
    await assert.rejects(publish({ env: ENV, version: '0.11.250', ...h.deps }), /HTTP 401/);
  });
  it('copies the amd64 image by digest to the version tag, then waits for Ready', async () => {
    const h = harness({ tags: [404, tagBody('preparing'), tagBody('preparing'), tagBody('ready')] });
    const result = await publish({ env: ENV, version: '0.11.250', ...h.deps });
    assert.deepEqual(result, { target: `${IMAGE}:0.11.250`, digest: AMD64, copied: true });
    // No skopeo login: it checks the credential on /v2/, which a project-scoped
    // token cannot read (HTTP 404); the copy only touches the repository.
    assert.equal(h.runs.length, 1);
    const [copy] = h.runs;
    assert.equal(copy.command, 'skopeo');
    assert.ok(!copy.args.some(arg => arg.includes('vcp_secret')));
    assert.deepEqual(copy.auth, { auths: { 'vcr.vercel.com': { auth: Buffer.from('team_abc:vcp_secret').toString('base64') } } });
    assert.deepEqual(copy.args.slice(-3), ['--preserve-digests', `docker://ghcr.io/intent-hq/intent-node@${AMD64}`, `docker://${IMAGE}:0.11.250`]);
    const api = h.calls.filter(call => call.url.startsWith('https://api.vercel.com/v1/'));
    assert.ok(api.every(call => call.init.headers.Authorization === 'Bearer vcp_secret'));
    assert.match(api[0].url, /projectId=intent-images&teamId=team_abc/);
  });
  it('skips the copy when the tag already holds the same image (re-runs, backfills)', async () => {
    const h = harness({ tags: [tagBody('ready')] });
    assert.equal((await publish({ env: ENV, version: '0.11.250', ...h.deps })).copied, false);
    assert.equal(h.runs.length, 0);
  });
  it('keeps a version tag that holds an earlier build of the version, so a re-run succeeds', async () => {
    const h = harness({ tags: [tagBody('preparing', digest('d')), tagBody('ready', digest('d'))] });
    assert.deepEqual(await publish({ env: ENV, version: '0.11.250', ...h.deps }), { target: `${IMAGE}:0.11.250`, digest: digest('d'), copied: false });
    assert.equal(h.runs.length, 0);
    assert.match(h.logs.join('\n'), /::warning::.*earlier build.*never moved/);
  });
  it('fails with the recovery steps when the kept image is not usable', async () => {
    const h = harness({ tags: [tagBody('errored', digest('d'))] });
    await assert.rejects(publish({ env: ENV, version: '0.11.250', ...h.deps }), /errored.*vercel vcr image delete/s);
    assert.equal(h.runs.length, 0);
  });
  it('retries transient errors on the first tag lookup', async () => {
    const h = harness({ tags: [503, 429, 404, tagBody('ready')] });
    assert.equal((await publish({ env: ENV, version: '0.11.250', ...h.deps })).copied, true);
  });
  it('fails when the pushed digest differs from the GHCR amd64 digest', async () => {
    const h = harness({ tags: [404, tagBody('ready', digest('d'))] });
    await assert.rejects(publish({ env: ENV, version: '0.11.250', ...h.deps }), /digest .* does not match/);
  });
  for (const status of ['errored', 'unoptimized', null]) {
    it(`fails when VCR reports ${status}`, async () => {
      const h = harness({ tags: [404, tagBody('preparing'), tagBody(status)] });
      await assert.rejects(publish({ env: ENV, version: '0.11.250', ...h.deps }), /not usable by Vercel Sandbox/);
    });
  }
  it('fails when the image is not Ready in time, and on refused API calls', async () => {
    const h = harness({ tags: [404, tagBody('preparing')] });
    await assert.rejects(publish({ env: ENV, version: '0.11.250', ...h.deps }), /not Ready after 60s.*preparing/);
    const refused = harness({ tags: [404, 403] });
    await assert.rejects(publish({ env: ENV, version: '0.11.250', ...refused.deps }), /HTTP 403/);
  });
  it('retries transient API errors while it waits', async () => {
    const config = vcrConfig(ENV);
    const h = harness({ tags: [503, 429, tagBody('ready')] });
    assert.equal((await waitReady({ config, tag: '0.11.250', digest: AMD64, token: 't', ...h.deps })).status, 'ready');
  });
  it('warns, without changing it, when the repository is private', async () => {
    const h = harness({ tags: [tagBody('ready')], repository: { public: false } });
    await publish({ env: ENV, version: '0.11.250', ...h.deps });
    assert.match(h.logs.join('\n'), /::warning::.*not public/);
    assert.ok(h.calls.every(call => (call.init.method ?? 'GET') === 'GET' || !call.url.includes('/v1/vcr/')));
  });
  it('revokes an OIDC token when it is done, even after a failure', async () => {
    const h = harness({ tags: [tagBody('errored', digest('d'))] });
    const env = { VERCEL_TEAM_ID: 'team_abc', VCR_INTENT_NODE_IMAGE: IMAGE, ACTIONS_ID_TOKEN_REQUEST_URL: 'https://oidc.example/t', ACTIONS_ID_TOKEN_REQUEST_TOKEN: 'r' };
    await assert.rejects(publish({ env, version: '0.11.250', ...h.deps }));
    assert.ok(h.calls.some(call => call.url.endsWith('/login/oauth/token/revoke')));
  });
});

describe('vercel registry workflow contract', () => {
  const workflow = readFileSync(join(ROOT, '.github/workflows/intent-node-image.yml'), 'utf8');
  const build = readFileSync(join(ROOT, '.github/workflows/intent-node-image-build.yml'), 'utf8');
  const steps = build.split('\n    steps:\n')[1];
  const job = name => workflow.split(`\n  ${name}:\n`)[1].split(/\n  [\w-]+:\n/)[0];
  const step = steps.split('- name: Copy the image to the Vercel registry')[1] ?? '';
  it('copies only after GHCR accepted the push', () => {
    assert.match(steps, /- name: Push the image\n\s+id: push/);
    assert.match(steps, /echo "pushed=true" >> "\$GITHUB_OUTPUT"/);
    assert.match(step, /if: steps\.push\.outputs\.pushed == 'true'/);
    assert.ok(steps.indexOf('Push the image') < steps.indexOf('Copy the image to the Vercel registry'));
    assert.match(step, /node vcr\.mjs "\$VERSION"/);
  });
  it('installs skopeo only when the copy is configured, so the warning path cannot fail', () => {
    assert.match(step, /if \[ -n "\$VERCEL_TEAM_ID" \] && \[ -n "\$VCR_INTENT_NODE_IMAGE" \] && ! command -v skopeo/);
    assert.ok(step.indexOf('apt-get install') < step.indexOf('node vcr.mjs'));
  });
  it('passes the credential through env only, from variables and an optional secret', () => {
    assert.match(step, /VERCEL_TEAM_ID: \$\{\{ vars\.VERCEL_TEAM_ID \}\}/);
    assert.match(step, /VCR_INTENT_NODE_IMAGE: \$\{\{ vars\.VCR_INTENT_NODE_IMAGE \}\}/);
    assert.match(step, /VERCEL_TOKEN: \$\{\{ secrets\.VERCEL_TOKEN \}\}/);
    assert.match(build, /secrets:\n\s+VERCEL_TOKEN:\n\s+required: false/);
    assert.equal(build.match(/secrets\.VERCEL_TOKEN/g).length, 1);
  });
  it('gives only release and manual runs the OIDC grant and the token', () => {
    assert.match(job('release'), /permissions:\n\s+contents: read\n\s+packages: write\n\s+id-token: write/);
    assert.match(job('release'), /VERCEL_TOKEN: \$\{\{ secrets\.VERCEL_TOKEN \}\}/);
    assert.doesNotMatch(job('pull-request'), /id-token|VERCEL_TOKEN/);
  });
});
