// Verification artifact only. Executes unchanged legacy client functions; no provider calls.
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

// Usage: node this-file.mjs /absolute/frontend-root /actual/rust-projection.json
const frontend = pathToFileURL(`${process.argv[2]}/`);
const require = createRequire(new URL('package.json', frontend));
const ts = require('typescript');

function loadFunctions(path, names) {
  const url = new URL(path, frontend);
  const file = ts.createSourceFile(fileURLToPath(url), readFileSync(url, 'utf8'), ts.ScriptTarget.Latest, true);
  const selected = [];
  const visit = (node) => {
    if (ts.isFunctionDeclaration(node) && names.includes(node.name?.text)) {
      selected.push(node.getText(file).replace(/^export\s+/, ''));
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  if (selected.length !== names.length) throw new Error('Production function selection changed');
  return ts.transpileModule(selected.join('\n'), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  }).outputText;
}

const mergeSource = loadFunctions('src/lib/components/workspace/sidebar/sidebar-changes-utils.ts', [
  'mergeMonitoredPRs', 'monitorDisplayStatus', 'shortRepoDisplay',
]);
const merge = new Function(`${mergeSource}\nreturn mergeMonitoredPRs;`)();
const projected = JSON.parse(readFileSync(process.argv[3], 'utf8'));
const formatterSource = loadFunctions('src/shared/i18n/formatters.ts', ['formatInteger', 'numberFormat']);
const formatInteger = new Function(`const numberFormats = new Map(); const getLocale = () => "en";\n${formatterSource}\nreturn formatInteger;`)();

// The unchanged consumer remains unsafe on raw qualified data. This oracle must
// not start passing by changing production UI or weakening these assertions.
const raw = projected.qualified.list;
assert.equal(merge([], raw.slice(0, 2), 'Owner/Repo').length, 1);
assert.equal(formatInteger(raw[1].lastSnapshot.approvals.have), '0');
assert.equal(merge([], projected.qualified['missing-url'], 'Owner/Repo')[0].url,
  'https://github.com/Owner/Repo/pull/17');

// Feed actual Rust policy output to those same production functions.
const safe = projected.legacy;
const rows = merge([], safe.list, 'Owner/Repo');
assert.equal(rows.length, 1);
assert.equal(rows[0].url, 'https://github.com/Owner/Repo/pull/17');
assert.equal(rows[0].monitorSnapshot.state, 'open');
assert.equal(formatInteger(rows[0].monitorSnapshot.approvals.have), '1');
assert.equal(safe['missing-url'].length, 0);
for (const surface of ['events', 'waiting', 'refusal', 'registration', 'snapshot', 'wake', 'replay']) {
  assert.equal(safe[surface].length, 1, surface);
  assert(!JSON.stringify(safe[surface]).includes('gitlab'), surface);
}
for (const surface of ['wake', 'replay']) {
  assert.equal(safe[surface][0].metadata.type, 'pr_monitor_wake');
  assert.equal(safe[surface][0].metadata.prNumber, 17);
  assert.equal(safe[surface][0].body, 'GitHub changed');
}
console.log('PASS: raw mixed identity/null/missing URL remain unsafe; Rust-projected legacy surfaces preserve ordinary GitHub without qualified leakage.');
