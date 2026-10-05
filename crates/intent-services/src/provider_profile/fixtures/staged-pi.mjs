// Consume generated CLI arguments with the pinned parser/resource loader.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
const runtime=path.resolve(process.argv[2]);
assert.equal(JSON.parse(fs.readFileSync(path.join(runtime,'package.json'))).version,'0.81.0');
const supplied=JSON.parse(fs.readFileSync(process.argv[3],'utf8'));
const root=fs.mkdtempSync(path.join(os.tmpdir(),'intent-staged-pi-'));
const home=path.join(root,'home'),cwd=path.join(root,'repo');
for(const p of [home,cwd,path.join(cwd,'.pi')])fs.mkdirSync(p,{recursive:true});
for(const key of Object.keys(process.env))if(key!=='PATH')delete process.env[key];
Object.assign(process.env,{HOME:home,USERPROFILE:home,PI_SKIP_VERSION_CHECK:'1',...supplied.environment});
for(const file of ['AGENTS.md','.pi/SYSTEM.md','.pi/APPEND_SYSTEM.md'])fs.writeFileSync(path.join(cwd,file),'AMBIENT-CONTEXT');
try{
 const {parseArgs}=await import(pathToFileURL(path.join(runtime,'dist/cli/args.js')));
 const {DefaultResourceLoader}=await import(pathToFileURL(path.join(runtime,'dist/core/resource-loader.js')));
 const {SettingsManager}=await import(pathToFileURL(path.join(runtime,'dist/core/settings-manager.js')));
 const parsed=parseArgs(supplied.runtime_args);
 assert.equal(parsed.noContextFiles,true);assert.equal(parsed.systemPrompt,'');assert.equal(parsed.noSkills,true);assert.equal(parsed.noExtensions,true);
 const baseline=new DefaultResourceLoader({cwd,agentDir:supplied.directory});await baseline.reload();
 assert(baseline.getAgentsFiles().agentsFiles.some(f=>f.content.includes('AMBIENT-CONTEXT')));
 assert.equal(baseline.getSystemPrompt(),'AMBIENT-CONTEXT');
 const loader=new DefaultResourceLoader({cwd,agentDir:supplied.directory,
  settingsManager:SettingsManager.create(cwd,supplied.directory,{projectTrusted:false}),
  noContextFiles:parsed.noContextFiles,noSkills:parsed.noSkills,noExtensions:parsed.noExtensions,
  noPromptTemplates:parsed.noPromptTemplates,noThemes:parsed.noThemes,
  systemPrompt:parsed.systemPrompt,appendSystemPrompt:parsed.appendSystemPrompt});
 for(let i=0;i<2;i++){
  await loader.reload({resolveProjectTrust:async()=>false});
  assert.deepEqual(loader.getAgentsFiles().agentsFiles,[]);assert(!loader.getSystemPrompt());
  assert.deepEqual(loader.getAppendSystemPrompt(),['OWNED-STAGED-INSTRUCTIONS']);
  assert.deepEqual(loader.getSkills().skills,[]);
 }
 console.log('PASS staged Pi explicit instructions and native context suppression through reload');
}finally{fs.rmSync(root,{recursive:true,force:true});}
