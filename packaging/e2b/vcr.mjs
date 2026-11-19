#!/usr/bin/env node
// Copies a pushed ghcr.io/intent-hq/intent-node:<version> to Vercel Container
// Registry (VCR), so Vercel Sandbox can start from it, and waits until VCR
// reports it Ready. Run by intent-node-image-build.yml after the GHCR push.
// Usage: node vcr.mjs <version>
//
// Env: VERCEL_TEAM_ID (team_...), VCR_INTENT_NODE_IMAGE (vcr.vercel.com/<team>/<project>/<repo>),
// and VERCEL_TOKEN or the job's GitHub OIDC grant (id-token: write) for a
// Vercel OIDC policy with VCR read-write access. Without the variables, or
// while Vercel refuses the OIDC exchange (no policy yet), it warns and succeeds.
//
// Only the exact version tag is written, never latest/alpha/beta, and an
// existing version tag is never moved or deleted: sandboxes pin by digest, so
// a version already on VCR keeps its first build, which must become Ready.
import { execFileSync } from 'node:child_process';
import { appendFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { platformDigest } from './layers.mjs';
import { IMAGE_REPO, parseVersion, redact } from './intent-node.mjs';

export const VERCEL_API = 'https://api.vercel.com';
// The Vercel OAuth app that vercel/vcr-action/login exchanges GitHub OIDC tokens with.
export const VCR_APP_ID = 'cl_inrfNy8noLlhRrGbPEm0z47woXNcJVZ0';
const RESERVED_TAGS = new Set(['latest', 'alpha', 'beta']);
const IMAGE = /^(vcr\.vercel\.com)\/([a-z0-9][a-z0-9-]*)\/([a-z0-9][a-z0-9._-]*)\/([a-z0-9][a-z0-9._-]*)$/;

export function vcrConfig(env) {
  const missing = ['VERCEL_TEAM_ID', 'VCR_INTENT_NODE_IMAGE'].filter(name => !env[name]);
  if (missing.length) return { skip: `the ${missing.join(' and ')} repository variable${missing.length > 1 ? 's are' : ' is'} not set` };
  if (!/^team_\w+$/.test(env.VERCEL_TEAM_ID)) throw new Error(`VERCEL_TEAM_ID ${JSON.stringify(env.VERCEL_TEAM_ID)} is not a Vercel team ID (team_...)`);
  const match = IMAGE.exec(env.VCR_INTENT_NODE_IMAGE);
  if (!match) throw new Error(`VCR_INTENT_NODE_IMAGE ${JSON.stringify(env.VCR_INTENT_NODE_IMAGE)} must be vcr.vercel.com/<team>/<project>/<repository>, without a tag`);
  const [image, registry, teamSlug, project, repository] = match;
  return { team: env.VERCEL_TEAM_ID, image, registry, teamSlug, project, repository };
}

export function vcrTarget(config, version) {
  const tag = parseVersion(version);
  if (RESERVED_TAGS.has(tag)) throw new Error(`refusing to write the ${tag} tag to VCR`);
  return `${config.image}:${tag}`;
}

// A Vercel access token: VERCEL_TOKEN, else the job's GitHub OIDC token
// exchanged for a short-lived one (as vercel/vcr-action/login does).
export async function vercelToken(env, team, fetch = globalThis.fetch) {
  if (env.VERCEL_TOKEN) return { token: env.VERCEL_TOKEN, oidc: false };
  if (!env.ACTIONS_ID_TOKEN_REQUEST_URL || !env.ACTIONS_ID_TOKEN_REQUEST_TOKEN) {
    throw new Error('no Vercel credential: set the VERCEL_TOKEN secret or give the job id-token: write');
  }
  const idResponse = await fetch(env.ACTIONS_ID_TOKEN_REQUEST_URL, { headers: { Authorization: `Bearer ${env.ACTIONS_ID_TOKEN_REQUEST_TOKEN}` } });
  if (!idResponse.ok) throw new Error(`requesting the GitHub OIDC token failed: HTTP ${idResponse.status}`);
  const { value: idToken } = await idResponse.json();
  const response = await fetch(`${VERCEL_API}/login/oauth/token`, {
    method: 'POST',
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:token-exchange',
      client_id: VCR_APP_ID,
      subject_token_type: 'urn:ietf:params:oauth:token-type:id_token',
      requested_token_type: 'urn:ietf:params:oauth:token-type:access_token',
      team_id_or_slug: team,
      subject_token: idToken,
    }),
  });
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    const refused = response.status >= 400 && response.status < 500 && response.status !== 408 && response.status !== 429;
    throw Object.assign(new Error(redact(`Vercel OIDC token exchange failed: HTTP ${response.status} ${body}. Check the team's VCR OIDC policy matches intent-hq/intentd and grants read-write access to the project`, idToken)), { refused });
  }
  const { access_token: token } = await response.json();
  if (!token) throw new Error('Vercel OIDC token exchange returned no access_token');
  return { token, oidc: true };
}

async function revokeToken(token, fetch) {
  const response = await fetch(`${VERCEL_API}/login/oauth/token/revoke`, {
    method: 'POST',
    body: new URLSearchParams({ client_id: VCR_APP_ID, token, token_type_hint: 'access_token' }),
  }).catch(() => undefined);
  return response?.ok ?? false;
}

async function api(config, path, token, fetch) {
  const url = `${VERCEL_API}/v1/vcr/repository/${encodeURIComponent(config.repository)}${path}?projectId=${encodeURIComponent(config.project)}&teamId=${encodeURIComponent(config.team)}`;
  const response = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (response.ok) return { body: await response.json() };
  await response.body?.cancel?.();
  return { status: response.status };
}

// The tag's image, null when VCR does not have it, or { retry } on a transient error.
async function getTag(config, tag, token, fetch) {
  const { body, status } = await api(config, `/tags/${encodeURIComponent(tag)}`, token, fetch);
  if (body) return body.tag;
  if (status === 404) return null;
  if (status === 408 || status === 429 || status >= 500) return { retry: `HTTP ${status}` };
  throw new Error(`VCR tag lookup for ${config.repository}:${tag} failed: HTTP ${status}`);
}

// Polls the tag until VCR has optimized the expected image for Sandbox.
export async function waitReady({ config, tag, digest, token, fetch, sleep, now, log = () => {}, timeoutMs, intervalMs }) {
  const deadline = now() + timeoutMs;
  let last = 'not checked';
  for (;;) {
    const image = await getTag(config, tag, token, fetch);
    if (image && !image.retry) {
      if (image.manifestDigest !== digest) throw new Error(`VCR ${config.repository}:${tag} digest ${image.manifestDigest} does not match the GHCR linux/amd64 digest ${digest}`);
      if (image.status === 'ready') return image;
      if (image.status !== 'preparing') {
        throw new Error(`VCR ${config.repository}:${tag} is ${image.status} (kind ${image.kind}): not usable by Vercel Sandbox. `
          + `Delete it (vercel vcr image delete ${config.repository} ${image.imageId} --project ${config.project}), then re-run`);
      }
      last = image.status;
    } else {
      last = image?.retry ?? 'not found';
    }
    if (now() >= deadline) throw new Error(`VCR ${config.repository}:${tag} is not Ready after ${timeoutMs / 1000}s (last: ${last})`);
    log(`${config.repository}:${tag}: ${last}; waiting for Ready`);
    await sleep(intervalMs);
  }
}

export async function publish({
  env, version, fetch = globalThis.fetch, run = runCommand, sleep = ms => new Promise(r => setTimeout(r, ms)),
  now = Date.now, log = console.log, timeoutMs = 1_200_000, intervalMs = 10_000,
}) {
  const config = vcrConfig(env);
  if (config.skip) {
    log(`::warning::Not copying intent-node to Vercel Container Registry: ${config.skip}. See docs/RELEASING.md in intent-hq/intent.`);
    return { skipped: true };
  }
  const target = vcrTarget(config, version);
  const tag = target.slice(config.image.length + 1);
  const digest = await platformDigest(`${IMAGE_REPO}:${tag}`, fetch);
  // Until the team has a VCR OIDC policy, Vercel refuses the exchange: warn,
  // like the unconfigured case. An explicit VERCEL_TOKEN that fails is fatal.
  let auth;
  try {
    auth = await vercelToken(env, config.team, fetch);
  } catch (error) {
    if (!error.refused) throw error;
    log(`::warning::Not copying intent-node to Vercel Container Registry: ${error.message}. See docs/RELEASING.md in intent-hq/intent.`);
    return { skipped: true };
  }
  const { token, oidc } = auth;
  if (oidc) log(`::add-mask::${token}`);
  const authDir = mkdtempSync(join(tmpdir(), 'vcr-auth-'));
  try {
    let existing = await getTag(config, tag, token, fetch);
    for (let attempt = 1; existing?.retry && attempt < 5; attempt++) {
      await sleep(intervalMs);
      existing = await getTag(config, tag, token, fetch);
    }
    if (existing?.retry) throw new Error(`VCR tag lookup failed: ${existing.retry}`);
    // A re-run rebuilds the GHCR image with a new digest; the version's
    // earlier build stays on VCR, since tags are never moved.
    const expected = existing?.manifestDigest ?? digest;
    if (existing && expected !== digest) {
      log(`::warning::${target} already holds an earlier build of ${tag} (${expected}); GHCR now holds ${digest}. Keeping it: VCR version tags are never moved.`);
    } else if (!existing) {
      const authfile = join(authDir, 'auth.json');
      run('skopeo', ['login', '--authfile', authfile, '--username', config.team, '--password-stdin', config.registry], { input: token });
      run('skopeo', ['copy', '--authfile', authfile, '--retry-times', '3', '--preserve-digests', `docker://${IMAGE_REPO}@${digest}`, `docker://${target}`]);
      log(`Copied ${IMAGE_REPO}@${digest} to ${target}`);
    } else {
      log(`${target} already holds ${digest}`);
    }
    await waitReady({ config, tag, digest: expected, token, fetch, sleep, now, log, timeoutMs, intervalMs });
    log(`${target} is Ready (${expected})`);
    const { body } = await api(config, '', token, fetch);
    if (body?.repository?.public !== true) {
      log(`::warning::The VCR repository ${config.image} is not public, so other Vercel teams cannot start sandboxes from it. Make it public with vercel vcr update ${config.repository} --public.`);
    }
    return { target, digest: expected, copied: !existing };
  } finally {
    rmSync(authDir, { recursive: true, force: true });
    if (oidc && !(await revokeToken(token, fetch))) log('::warning::Could not revoke the Vercel access token; it expires on its own.');
  }
}

function runCommand(command, args, { input } = {}) {
  execFileSync(command, args, { input, stdio: [input === undefined ? 'ignore' : 'pipe', 'inherit', 'inherit'] });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const [version] = process.argv.slice(2);
  publish({ env: process.env, version })
    .then(result => {
      if (process.env.GITHUB_STEP_SUMMARY && result.target) {
        appendFileSync(process.env.GITHUB_STEP_SUMMARY, `- \`${result.target}\` (Vercel, Ready, ${result.digest})\n`);
      }
    })
    .catch(error => {
      console.error(`::error::${redact(error?.message ?? error, process.env.VERCEL_TOKEN)}`);
      process.exit(1);
    });
}
