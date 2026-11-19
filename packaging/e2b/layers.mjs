#!/usr/bin/env node
// Compares the layers of two images in registries: how many MB (compressed)
// of the new image are layers the old image does not have, which is what a
// host holding the old image pulls for the new one.
// Usage:
//   node layers.mjs <old ref> <new ref> [--budget-mb N]   compare; fail over N MB
//   node layers.mjs --digest <ref>                         print the ref's digest
// Refs like ghcr.io/intent-hq/intent-node:0.11.224, repo@sha256:..., or
// localhost:5000/intent-node:pr-1. Anonymous pulls only; plain HTTP for
// localhost registries. Writes the table to $GITHUB_STEP_SUMMARY when set.
import { appendFileSync } from 'node:fs';

const MANIFEST_TYPES = [
  'application/vnd.oci.image.index.v1+json',
  'application/vnd.docker.distribution.manifest.list.v2+json',
  'application/vnd.oci.image.manifest.v1+json',
  'application/vnd.docker.distribution.manifest.v2+json',
].join(', ');

export function parseRef(ref) {
  const match = /^([^/]+)\/(.+?)(?::([\w][\w.-]*))?(?:@(sha256:[0-9a-f]{64}))?$/.exec(String(ref));
  if (!match || !(match[3] || match[4])) throw new Error(`${JSON.stringify(ref)} is not a registry/repository:tag reference`);
  return { registry: match[1], repository: match[2], reference: match[4] ?? match[3] };
}

async function client({ registry, repository }, fetch) {
  const local = /^(localhost|127\.0\.0\.1)(:\d+)?$/.test(registry);
  const base = `${local ? 'http' : 'https'}://${registry}/v2/${repository}`;
  let auth = {};
  if (!local) {
    const response = await fetch(`https://${registry}/token?scope=repository:${repository}:pull`);
    if (!response.ok) throw new Error(`anonymous token for ${registry}/${repository} failed: HTTP ${response.status}`);
    auth = { Authorization: `Bearer ${(await response.json()).token}` };
  }
  return async (path, accept) => {
    const response = await fetch(`${base}${path}`, { headers: { ...auth, ...(accept ? { Accept: accept } : {}) } });
    if (!response.ok) throw new Error(`GET ${base}${path} failed: HTTP ${response.status}`);
    return Object.assign(await response.json(), { digest: response.headers.get('docker-content-digest') });
  };
}

// The digest a tag points at now, so a later comparison is not moved by a push.
export async function refDigest(ref, fetch = globalThis.fetch) {
  const parsed = parseRef(ref);
  const { digest } = await (await client(parsed, fetch))(`/manifests/${parsed.reference}`, MANIFEST_TYPES);
  if (!/^sha256:[0-9a-f]{64}$/.test(digest ?? '')) throw new Error(`${ref}: the registry sent no digest`);
  return `${parsed.registry}/${parsed.repository}@${digest}`;
}

// The digest of the linux/amd64 image itself: the index entry when the ref is
// an index (buildx adds an attestation), else the ref's own manifest.
export async function platformDigest(ref, fetch = globalThis.fetch) {
  const parsed = parseRef(ref);
  const manifest = await (await client(parsed, fetch))(`/manifests/${parsed.reference}`, MANIFEST_TYPES);
  const digest = manifest.manifests
    ? manifest.manifests.find(m => m.platform?.os === 'linux' && m.platform?.architecture === 'amd64')?.digest
    : manifest.digest;
  if (!/^sha256:[0-9a-f]{64}$/.test(digest ?? '')) throw new Error(`${ref} has no linux/amd64 image digest`);
  return digest;
}

// The linux/amd64 image's layers: compressed blob digest and size, and the
// step that made each. Hosts pull by blob digest, so that is what compares.
export async function imageLayers(ref, fetch = globalThis.fetch) {
  const parsed = parseRef(ref);
  const get = await client(parsed, fetch);
  let manifest = await get(`/manifests/${parsed.reference}`, MANIFEST_TYPES);
  if (manifest.manifests) {
    const entry = manifest.manifests.find(m => m.platform?.os === 'linux' && m.platform?.architecture === 'amd64');
    if (!entry) throw new Error(`${ref} has no linux/amd64 image`);
    manifest = await get(`/manifests/${entry.digest}`, MANIFEST_TYPES);
  }
  const config = await get(`/blobs/${manifest.config.digest}`);
  const steps = (config.history ?? []).filter(step => !step.empty_layer).map(step => step.created_by ?? '');
  if (steps.length !== manifest.layers.length) throw new Error(`${ref}: ${manifest.layers.length} layers but ${steps.length} layer steps`);
  return manifest.layers.map((layer, i) => ({ digest: layer.digest, size: layer.size, step: steps[i] }));
}

export function compareLayers(older, newer) {
  const known = new Set(older.map(layer => layer.digest));
  const layers = newer.map(layer => ({ ...layer, shared: known.has(layer.digest) }));
  const sum = list => list.reduce((total, layer) => total + layer.size, 0);
  return { layers, total: sum(layers), changed: sum(layers.filter(layer => !layer.shared)) };
}

const mb = bytes => `${(bytes / 1e6).toFixed(1)} MB`;

export function report(oldRef, newRef, { layers, total, changed }) {
  const step = text => text.replace(/^\/bin\/sh -c (#\(nop\) )?/, '').replace(/\s+/g, ' ').slice(0, 70).replace(/\|/g, '\\|');
  return [
    `### Layers of ${newRef} not in ${oldRef}`,
    '',
    `${mb(changed)} of ${mb(total)} compressed; ${layers.filter(l => !l.shared).length} of ${layers.length} layers.`,
    '',
    '| Layer | Size | New | Step |',
    '|---|---|---|---|',
    ...layers.map((layer, i) => `| ${i + 1} | ${mb(layer.size)} | ${layer.shared ? '' : 'yes'} | \`${step(layer.step)}\` |`),
    '',
  ].join('\n');
}

async function main(args) {
  if (args[0] === '--digest' && args.length === 2) {
    console.log(await refDigest(args[1]));
    return;
  }
  const [oldRef, newRef, flag, budget] = args;
  const budgetMB = flag === '--budget-mb' ? Number(budget) : undefined;
  if (!oldRef || !newRef || (flag && !(budgetMB >= 0))) throw new Error('usage: layers.mjs <old ref> <new ref> [--budget-mb N] | --digest <ref>');
  const [older, newer] = await Promise.all([imageLayers(oldRef), imageLayers(newRef)]);
  const result = compareLayers(older, newer);
  const text = report(oldRef, newRef, result);
  console.log(text);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${text}\n`);
  if (budgetMB !== undefined && result.changed > budgetMB * 1e6) {
    console.error(`::error::${mb(result.changed)} of new layers is over the ${budgetMB} MB budget: a release should only add the intentd layer`);
    process.exit(1);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main(process.argv.slice(2)).catch(error => {
    console.error(`::error::${error?.stack ?? error}`);
    process.exit(1);
  });
}
