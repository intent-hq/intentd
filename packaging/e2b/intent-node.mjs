// The intent-node E2B templates: a Debian sandbox image with intentd, its
// tailcat sidecar, git tooling, Node LTS and the provider CLIs, built once per
// sandbox size (E2B fixes vCPU and RAM at build time). The backend starts
// sandboxes from intent-node-<size>:<intentd version> and starts intentd
// itself after create, so the template has no start or ready command: E2B
// snapshots the start command's processes, which would give every sandbox the
// same daemon id, TLS key and tailcat key.
//
// This module has no dependencies so scripts/test-e2b-template.mjs can run it
// offline; build.mjs and smoke.mjs pass in the e2b SDK.

export const TEMPLATE_PREFIX = 'intent-node';
export const BASE_IMAGE = 'node:24-bookworm';
export const ARCHIVE_TARGET = 'x86_64-unknown-linux-musl';
export const RELEASES_REPO = 'intent-hq/intentd-releases';
export const INSTALL_DIR = '/opt/intentd';
// E2B Hobby caps sandboxes at 8 vCPU / 8 GiB; Pro goes higher only after E2B
// support raises the project's limits. So 8x16 and 16x32 need Pro with raised
// limits, for the publishing project to build them and for a user to start them.
// A build E2B refuses over those limits is skipped with a warning (see
// isResourceLimitRefusal); E2B_TEMPLATE_SIZES narrows what gets built.
export const SIZES = [
  { id: '2x4', cpuCount: 2, memoryMB: 4096 },
  { id: '4x8', cpuCount: 4, memoryMB: 8192 },
  { id: '8x16', cpuCount: 8, memoryMB: 16384 },
  { id: '16x32', cpuCount: 16, memoryMB: 32768 },
];
export const DEFAULT_SIZE = '4x8';
export const API_URL = 'https://api.e2b.app';

// Bump these by hand; each change rebuilds the npm layer.
export const NPM_GLOBALS = {
  pnpm: '12.18.0',
  '@anthropic-ai/claude-code': '2.1.318',
  '@openai/codex': '0.167.0',
};

export const APT_PACKAGES = [
  'build-essential',
  'ca-certificates',
  'curl',
  'gh',
  'git',
  'git-lfs',
  'procps',
  'python3',
  'python3-pip',
  'python3-venv',
  'ripgrep',
  'xz-utils',
];

export function parseSize(id) {
  const size = SIZES.find(candidate => candidate.id === id);
  if (!size) throw new Error(`unknown template size ${JSON.stringify(id)} (known: ${SIZES.map(s => s.id).join(', ')})`);
  return size;
}

export function templateName(sizeId) {
  return `${TEMPLATE_PREFIX}-${parseSize(sizeId).id}`;
}

// Sizes to build: a comma list (the E2B_TEMPLATE_SIZES variable), or all.
export function enabledSizes(list) {
  const ids = String(list ?? '').split(',').map(id => id.trim()).filter(Boolean);
  return ids.length ? ids.map(id => parseSize(id).id) : SIZES.map(size => size.id);
}

// True when E2B refused a build request because the size is over the project's
// vCPU or memory limits. The SDK reports API errors as "<status>: <message>".
export function isResourceLimitRefusal(error) {
  const message = String(error?.message ?? '');
  return /^(400|403|422): /.test(message)
    && /\b(v?cpu|memory(mb)?|ram)\b/i.test(message)
    && /(exceed|limit|maximum|at most|above)/i.test(message);
}

const VERSION = /^v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z.]+)?)$/;

export function parseVersion(value) {
  const match = VERSION.exec(String(value ?? '').trim());
  if (!match) throw new Error(`${JSON.stringify(value)} is not an intentd release version`);
  return match[1];
}

export function parseSha256(text) {
  const digest = String(text ?? '').trim().split(/\s+/)[0] ?? '';
  if (!/^[0-9a-fA-F]{64}$/.test(digest)) throw new Error('the archive .sha256 sidecar does not hold a SHA-256 digest');
  return digest.toLowerCase();
}

export function archiveUrl(version) {
  const v = parseVersion(version);
  return `https://github.com/${RELEASES_REPO}/releases/download/v${v}/intentd-${ARCHIVE_TARGET}.tar.xz`;
}

// What to build for one workflow event. Returns { skip } or
// { version, tags, publish }; throws on input that must fail the run.
export function plan(env) {
  switch (env.EVENT_NAME) {
    case 'workflow_run': {
      if (env.RUN_CONCLUSION !== 'success') return { skip: `the Release run ended with ${env.RUN_CONCLUSION}` };
      let version;
      try {
        version = parseVersion(env.RUN_HEAD_BRANCH);
      } catch {
        return { skip: `${JSON.stringify(env.RUN_HEAD_BRANCH)} is not a release tag` };
      }
      return { version, tags: [version, 'latest'], publish: true };
    }
    case 'workflow_dispatch': {
      const version = parseVersion(env.INPUT_VERSION);
      return { version, tags: env.INPUT_LATEST === 'true' ? [version, 'latest'] : [version], publish: true };
    }
    case 'pull_request': {
      if (!/^\d+$/.test(env.PR_NUMBER ?? '')) throw new Error(`bad PR number ${JSON.stringify(env.PR_NUMBER)}`);
      return { version: parseVersion(env.LATEST_RELEASE_TAG), tags: [`pr-${env.PR_NUMBER}`], publish: false };
    }
    default:
      throw new Error(`unsupported event ${JSON.stringify(env.EVENT_NAME)}`);
  }
}

export function installIntentdCommand(version, sha256) {
  const url = archiveUrl(version);
  const digest = parseSha256(sha256);
  return [
    'tmp=$(mktemp -d)',
    `curl --fail --silent --show-error --location --retry 3 --output "$tmp/intentd.tar.xz" '${url}'`,
    `echo '${digest}  '"$tmp/intentd.tar.xz" | sha256sum -c -`,
    `mkdir -p ${INSTALL_DIR}`,
    `tar -xJf "$tmp/intentd.tar.xz" -C ${INSTALL_DIR} --strip-components=1`,
    `ln -sf ${INSTALL_DIR}/intentd /usr/local/bin/intentd`,
    `ln -sf ${INSTALL_DIR}/libexec/tailcat /usr/local/bin/tailcat`,
    'rm -rf "$tmp"',
    'intentd --version',
  ].join(' && ');
}

export function npmGlobalsCommand() {
  const specs = Object.entries(NPM_GLOBALS).map(([name, version]) => `${name}@${version}`);
  return `npm install --global --no-fund --no-audit ${specs.join(' ')} && npm cache clean --force`;
}

export function defineTemplate(Template, { version, sha256 }) {
  return Template()
    .fromImage(BASE_IMAGE)
    .aptInstall(APT_PACKAGES)
    .runCmd('git lfs install --system', { user: 'root' })
    .runCmd(npmGlobalsCommand(), { user: 'root' })
    .runCmd(installIntentdCommand(version, sha256), { user: 'root' });
}

export function smokeChecks() {
  return {
    tools: ['intentd', 'tailcat', 'git', 'git-lfs', 'gh', 'rg', 'node', 'pnpm', 'python3', 'cc', 'make', 'claude', 'codex'],
    version: 'intentd --version',
    idle: 'pgrep -x intentd || true',
    cpus: 'nproc',
    memKb: "awk '/^MemTotal:/ { print $2 }' /proc/meminfo",
  };
}

// The guest kernel keeps some RAM for itself, so MemTotal is a bit under the
// allocation; 85% still tells the sizes apart.
export function verifySmoke(version, sizeId, outputs) {
  const size = parseSize(sizeId);
  const problems = [];
  const reported = outputs.version.trim();
  if (reported !== `intentd ${version}`) problems.push(`intentd --version printed ${JSON.stringify(reported)}, expected "intentd ${version}"`);
  const cpus = Number(outputs.cpus.trim());
  if (cpus !== size.cpuCount) problems.push(`the sandbox has ${cpus} vCPU, expected ${size.cpuCount}`);
  const memMiB = Math.round(Number(outputs.memKb.trim()) / 1024);
  if (!(memMiB >= size.memoryMB * 0.85)) problems.push(`the sandbox has ${memMiB} MiB of RAM, expected about ${size.memoryMB}`);
  if (outputs.idle.trim()) problems.push(`intentd is already running in a fresh sandbox (pid ${outputs.idle.trim()}); the template must not start it`);
  return problems;
}

export function redact(text, secret) {
  const value = String(text);
  return secret ? value.split(secret).join('[redacted]') : value;
}

// Makes the template visible to every E2B project as <project-slug>/<name>.
// Returns the names E2B reports for it.
export async function publishTemplate({ templateId, apiKey, fetch = globalThis.fetch, apiUrl = API_URL }) {
  const response = await fetch(`${apiUrl}/v2/templates/${encodeURIComponent(templateId)}`, {
    method: 'PATCH',
    headers: { 'X-API-Key': apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify({ public: true }),
  });
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(redact(`publishing template ${templateId} failed: HTTP ${response.status} ${body}`.trim(), apiKey));
  }
  const { names = [] } = await response.json();
  return names;
}
