#!/usr/bin/env node
// Plans the intent-node container image for one workflow event (imagePlan in
// intent-node.mjs) and writes its Dockerfile, the E2B template's steps.
// Env: the plan inputs (EVENT_NAME, RUN_*, INPUT_*, PR_NUMBER, LATEST_RELEASE_TAG).
// Writes skip/version/ref/tags/push to $GITHUB_OUTPUT.
import { writeFileSync } from 'node:fs';
import { dockerfile, fetchSha256, imagePlan, output } from './intent-node.mjs';

async function main() {
  const target = imagePlan(process.env);
  if (target.skip) {
    console.log(`::notice::Skipping the intent-node image: ${target.skip}`);
    output({ skip: 'true' });
    return;
  }
  const sha256 = await fetchSha256(target.version);
  writeFileSync('Dockerfile', dockerfile({ version: target.version, sha256 }));
  console.log(`Wrote the Dockerfile for intentd ${target.version}, tags ${target.tags.join(', ')}`);
  output({ skip: 'false', version: target.version, ref: target.tags[0], tags: target.tags.join(' '), push: String(target.push) });
}

main().catch(error => {
  console.error(`::error::${error?.stack ?? error}`);
  process.exit(1);
});
