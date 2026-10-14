'use strict';
const fs = require('node:fs');
const path = require('node:path');

async function publish(client, root = process.env.NATIVE_ARTIFACT_ROOT || path.join(process.env.RUNNER_TEMP, 'native-artifacts')) {
  const plan = JSON.parse(fs.readFileSync(path.join(root, 'publication-plan.json'), 'utf8'));
  if (!plan.length || !plan[0].name.startsWith('native-v3-index-')) throw new Error('missing publication index');
  const receipts = [];
  try {
    // The immutable index comes first. Interrupted later uploads remain misses;
    // every finalized variant is discoverable without waiting for the whole set.
    for (const item of plan) {
      const started = performance.now();
      const result = await client.uploadArtifact(item.name, item.files, item.root, {compressionLevel: 0, retentionDays: 7});
      if (!result.id || !result.digest) throw new Error('publication did not return an authenticated artifact');
      receipts.push({name: item.name, ...result, upload_seconds: (performance.now() - started) / 1000});
    }
  } finally {
    fs.writeFileSync(path.join(root, 'upload-receipts.json'), JSON.stringify(receipts, null, 2) + '\n');
  }
  return receipts;
}
module.exports = {publish};
