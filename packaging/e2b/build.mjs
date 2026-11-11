#!/usr/bin/env node
// Builds intent-node-<size>:<tag> on E2B for one workflow event (see plan() in
// intent-node.mjs) and, for release builds, makes the template public.
// Env: E2B_API_KEY, SIZE, ENABLED_SIZES (optional comma list) plus the plan
// inputs (EVENT_NAME, RUN_*, INPUT_*, PR_NUMBER, LATEST_RELEASE_TAG).
// Writes skip/version/ref/names to $GITHUB_OUTPUT.
import { Template, defaultBuildLogger } from 'e2b';
import {
  defineTemplate,
  enabledSizes,
  fetchSha256,
  output,
  isResourceLimitRefusal,
  parseSize,
  plan,
  publishTemplate,
  redact,
  templateName,
} from './intent-node.mjs';

async function main() {
  const apiKey = process.env.E2B_API_KEY;
  if (!apiKey) throw new Error('E2B_API_KEY is not set');
  const size = parseSize(process.env.SIZE);
  if (!enabledSizes(process.env.ENABLED_SIZES).includes(size.id)) {
    console.log(`::notice::Skipping ${templateName(size.id)}: not in E2B_TEMPLATE_SIZES`);
    output({ skip: 'true' });
    return;
  }
  const target = plan(process.env);
  if (target.skip) {
    console.log(`::notice::Skipping the intent-node template: ${target.skip}`);
    output({ skip: 'true' });
    return;
  }
  const sha256 = await fetchSha256(target.version);
  const name = templateName(size.id);
  console.log(`Building ${name} (${size.cpuCount} vCPU, ${size.memoryMB} MiB) for intentd ${target.version} with tags ${target.tags.join(', ')}`);
  let info;
  try {
    info = await Template.build(defineTemplate(Template, { version: target.version, sha256 }), name, {
      cpuCount: size.cpuCount,
      memoryMB: size.memoryMB,
      tags: target.tags,
      apiKey,
      onBuildLogs: defaultBuildLogger(),
    });
  } catch (error) {
    if (!isResourceLimitRefusal(error)) throw error;
    // Fail-soft: the publishing E2B project's limits are below this size, so no
    // tag exists for this version and the backend does not offer the size.
    console.log(`::warning::E2B refused ${name} (${size.cpuCount} vCPU, ${size.memoryMB} MiB) over the project's limits; skipping it. ${redact(error.message, apiKey)}`);
    output({ skip: 'true', refused: 'true' });
    return;
  }
  console.log(`Built template ${info.templateId} build ${info.buildId}`);
  const names = target.publish ? await publishTemplate({ templateId: info.templateId, apiKey }) : [info.name];
  output({ skip: 'false', version: target.version, ref: `${name}:${target.tags[0]}`, names: names.join(',') });
}

main().catch(error => {
  console.error(`::error::${redact(error?.stack ?? error, process.env.E2B_API_KEY)}`);
  process.exit(1);
});
