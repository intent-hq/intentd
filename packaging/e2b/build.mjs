#!/usr/bin/env node
// Builds intent-node:<tag> on E2B for one workflow event (see plan() in
// intent-node.mjs) and, for release builds, makes the template public.
// Env: E2B_API_KEY plus the plan inputs (EVENT_NAME, RUN_*, INPUT_*, PR_NUMBER,
// LATEST_RELEASE_TAG). Writes skip/version/ref/names to $GITHUB_OUTPUT.
import { appendFileSync } from 'node:fs';
import { Template, defaultBuildLogger } from 'e2b';
import {
  BUILD_RESOURCES,
  TEMPLATE_NAME,
  archiveUrl,
  defineTemplate,
  parseSha256,
  plan,
  publishTemplate,
  redact,
} from './intent-node.mjs';

function output(values) {
  const lines = Object.entries(values).map(([key, value]) => `${key}=${value}`).join('\n');
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${lines}\n`);
  console.log(lines);
}

async function fetchSha256(version) {
  const url = `${archiveUrl(version)}.sha256`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`fetching ${url} failed: HTTP ${response.status}`);
  return parseSha256(await response.text());
}

async function main() {
  const apiKey = process.env.E2B_API_KEY;
  if (!apiKey) throw new Error('E2B_API_KEY is not set');
  const target = plan(process.env);
  if (target.skip) {
    console.log(`::notice::Skipping the intent-node template: ${target.skip}`);
    output({ skip: 'true' });
    return;
  }
  const sha256 = await fetchSha256(target.version);
  console.log(`Building ${TEMPLATE_NAME} for intentd ${target.version} with tags ${target.tags.join(', ')}`);
  const info = await Template.build(defineTemplate(Template, { version: target.version, sha256 }), TEMPLATE_NAME, {
    ...BUILD_RESOURCES,
    tags: target.tags,
    apiKey,
    onBuildLogs: defaultBuildLogger(),
  });
  console.log(`Built template ${info.templateId} build ${info.buildId}`);
  const names = target.publish ? await publishTemplate({ templateId: info.templateId, apiKey }) : [info.name];
  output({ skip: 'false', version: target.version, ref: `${TEMPLATE_NAME}:${target.tags[0]}`, names: names.join(',') });
}

main().catch(error => {
  console.error(`::error::${redact(error?.stack ?? error, process.env.E2B_API_KEY)}`);
  process.exit(1);
});
