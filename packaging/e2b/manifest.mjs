#!/usr/bin/env node
// Writes the release's template manifest (e2b-templates.json): the project and
// the sizes whose intent-node-<size>:<version> tag exists, checked with the
// publishing project's key after the builds. The workflow attaches it to the
// intentd-releases release, where backends read it without a key.
// Env: E2B_API_KEY, PROJECT, plus the plan inputs (EVENT_NAME, RUN_*,
// INPUT_VERSION). Writes skip/version/path to $GITHUB_OUTPUT.
import { writeFileSync } from 'node:fs';
import { Template } from 'e2b';
import { MANIFEST_ASSET, builtSizes, manifestFor, output, plan, redact } from './intent-node.mjs';

async function main() {
  const apiKey = process.env.E2B_API_KEY;
  if (!apiKey) throw new Error('E2B_API_KEY is not set');
  const target = plan(process.env);
  if (target.skip || !target.publish) {
    console.log(`::notice::No template manifest: ${target.skip ?? 'not a release build'}`);
    output({ skip: 'true' });
    return;
  }
  const sizes = await builtSizes({
    version: target.version,
    exists: name => Template.exists(name, { apiKey }),
  });
  const manifest = manifestFor({ project: process.env.PROJECT, version: target.version, sizes });
  writeFileSync(MANIFEST_ASSET, `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(`Template manifest for intentd ${manifest.version}: ${manifest.project}, sizes ${manifest.sizes.join(', ') || 'none'}`);
  output({ skip: 'false', version: manifest.version, path: MANIFEST_ASSET });
}

main().catch(error => {
  console.error(`::error::${redact(error?.stack ?? error, process.env.E2B_API_KEY)}`);
  process.exit(1);
});
