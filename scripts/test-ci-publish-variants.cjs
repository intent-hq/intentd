'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const {publish} = require('./ci-publish-variants.cjs');

for (const fails of [false, true]) {
  test(`index precedes independent variants; partial publication ${fails}`, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'variant-publish-'));
    try {
      const plan = ['native-v3-index-linux-sha-1', 'native-v3-variant-a-1', 'native-v3-variant-b-1'].map(name => ({name, root, files: [path.join(root, name)]}));
      fs.writeFileSync(path.join(root, 'publication-plan.json'), JSON.stringify(plan));
      const seen = [];
      const client = {async uploadArtifact(name, files, base, options) {
        seen.push(name);
        assert.deepEqual(files, plan[seen.length-1].files);
        assert.equal(base, root);
        assert.deepEqual(options, {compressionLevel: 0, retentionDays: 7});
        if (fails && seen.length === 3) throw new Error('interrupted upload');
        return {id: seen.length, size: 100, digest: 'a'.repeat(64)};
      }};
      if (fails) await assert.rejects(publish(client, root), /interrupted upload/);
      else assert.equal((await publish(client, root)).length, 3);
      assert.deepEqual(seen, plan.map(item => item.name));
      const receipts = JSON.parse(fs.readFileSync(path.join(root, 'upload-receipts.json')));
      assert.equal(receipts.length, fails ? 2 : 3);
      assert.ok(receipts.every(item => item.upload_seconds >= 0));
    } finally { fs.rmSync(root, {recursive: true, force: true}); }
  });
}

require('node:test')('artifact SDK and transitive dependencies have reproducible integrity pins', () => {
  const action = path.join(__dirname, '../.github/actions/publish-variants');
  const manifest = JSON.parse(fs.readFileSync(path.join(action, 'package.json')));
  const lock = JSON.parse(fs.readFileSync(path.join(action, 'package-lock.json')));
  assert.equal(manifest.dependencies['@actions/artifact'], '6.3.1');
  assert.equal(lock.packages['node_modules/@actions/artifact'].version, '6.3.1');
  for (const [name, entry] of Object.entries(lock.packages)) {
    if (name) assert.match(entry.integrity, /^sha512-/);
  }
});
