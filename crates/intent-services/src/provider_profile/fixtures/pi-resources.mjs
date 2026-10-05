// Run explicitly with the official @earendil-works/pi-coding-agent@0.81.0
// package directory as argv[2]. No real credentials or model/network calls.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const runtime = path.resolve(process.argv[2]);
assert.equal(JSON.parse(fs.readFileSync(path.join(runtime, 'package.json'))).version, '0.81.0');
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'intent-pi-resources-'));
const home = path.join(root, 'home');
const cwd = path.join(root, 'repository', 'nested');
const native = path.join(home, '.pi', 'agent');
const owned = path.join(root, 'owned');
const marker = path.join(root, 'ambient-executed');
const approvedMarker = path.join(root, 'approved-executed');
function put(file, contents) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, contents);
}
function skill(dir, name) {
  put(path.join(dir, name, 'SKILL.md'), `---\nname: ${name}\ndescription: Fixture sentinel\n---\nFixture only.\n`);
}
try {
  for (const key of Object.keys(process.env)) if (!['PATH', 'SystemRoot'].includes(key)) delete process.env[key];
  Object.assign(process.env, { HOME: home, USERPROFILE: home, PI_OFFLINE: '1', PI_SKIP_VERSION_CHECK: '1', NODE_DISABLE_COMPILE_CACHE: '1' });
  fs.mkdirSync(cwd, { recursive: true });
  fs.mkdirSync(owned, { recursive: true });
  skill(path.join(native, 'skills'), 'ambient-home');
  skill(path.join(home, '.agents', 'skills'), 'ambient-compatible-home');
  skill(path.join(cwd, '.pi', 'skills'), 'ambient-project');
  skill(path.join(root, 'repository', '.agents', 'skills'), 'ambient-ancestor');
  skill(path.join(cwd, '.agents', 'skills'), 'ambient-compatible-project');
  const extension = `import fs from 'node:fs'; export default function () { fs.writeFileSync(${JSON.stringify(marker)}, 'executed'); }`;
  put(path.join(native, 'extensions', 'ambient.mjs'), extension);
  put(path.join(cwd, '.pi', 'extensions', 'ambient.mjs'), extension);
  put(path.join(cwd, '.pi', 'settings.json'), JSON.stringify({extensions:['./extensions/ambient.mjs'], packages:['./extensions/ambient.mjs'], skills:['./skills']}));
  put(path.join(owned, 'settings.json'), JSON.stringify({packages:[], extensions:[], skills:[], prompts:[]}));
  const { DefaultResourceLoader } = await import(pathToFileURL(path.join(runtime, 'dist/core/resource-loader.js')));
  const { SettingsManager } = await import(pathToFileURL(path.join(runtime, 'dist/core/settings-manager.js')));
  const { builtInExtensions } = await import(pathToFileURL(path.join(runtime, 'dist/extensions/index.js')));
  const ambient = new DefaultResourceLoader({cwd, agentDir:native, extensionFactories:builtInExtensions});
  await ambient.reload();
  assert.ok(ambient.getSkills().skills.some(s=>s.name === 'ambient-home'));
  assert.ok(ambient.getSkills().skills.some(s=>s.name === 'ambient-project'));
  assert.ok(fs.existsSync(marker), 'baseline must execute the harmless native extension');
  fs.unlinkSync(marker);
  const settings = SettingsManager.create(cwd, owned, {projectTrusted:false});
  const isolated = new DefaultResourceLoader({cwd, agentDir:owned, settingsManager:settings, noSkills:true, noExtensions:true, noPromptTemplates:true, noThemes:true, extensionFactories:builtInExtensions});
  for (let i=0; i<2; i++) {
    await isolated.reload({resolveProjectTrust: async()=>false});
    assert.deepEqual(settings.getProjectSettings(), {}, 'project package settings must be excluded before resolution');
    assert.deepEqual(isolated.getSkills().skills, []);
    assert.deepEqual(isolated.getPrompts().prompts, []);
    assert.ok(isolated.getExtensions().extensions.every(e=>e.path.startsWith('<inline:')));
    assert.ok(!fs.existsSync(marker), 'ambient extension must never execute');
  }
  const approvedPath = path.join(owned, 'approved.mjs');
  put(approvedPath, `import fs from 'node:fs'; export default function () { fs.writeFileSync(${JSON.stringify(approvedMarker)}, 'executed'); }`);
  const interactive = new DefaultResourceLoader({cwd, agentDir:owned, settingsManager:settings, noSkills:true, noExtensions:true, noPromptTemplates:true, noThemes:true, extensionFactories:builtInExtensions, additionalExtensionPaths:[approvedPath]});
  await interactive.reload({resolveProjectTrust:async()=>false});
  assert.ok(fs.existsSync(approvedMarker), 'explicit owned extension remains usable');
  assert.ok(!fs.existsSync(marker));
  assert.deepEqual(interactive.getSkills().skills, []);
  console.log('PASS Pi 0.81.0 actual loader: baseline exposure, owned extension, zero skills, ambient exclusion, reload');
} finally { fs.rmSync(root, {recursive:true, force:true}); }
