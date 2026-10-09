import test from 'node:test';import assert from 'node:assert/strict';import {mkdtempSync,writeFileSync,rmSync,mkdirSync}from'node:fs';import{join}from'node:path';import{tmpdir}from'node:os';import{validate}from'./upload-policy.mjs';
const sample=()=>({stage:'provenance',outcome:'FAILED',errorHResult:0,behavioralInvocations:0,nodeSha256:'a'.repeat(64),platform:'win32',nodeVersion:'24.21.0',logs:[],rawFailureOutputUploaded:false,qualification:'Setup-only. Failed setup output may be incomplete; no Windows behavioral or historical cause acceptance.'});
function control(change,accepted=false){const root=mkdtempSync(join(tmpdir(),'pi-upload-control-'));try{const s=sample();change(s,root);writeFileSync(join(root,'setup-summary.json'),JSON.stringify(s));if(accepted)assert.doesNotThrow(()=>validate(root,[]));else assert.throws(()=>validate(root,[]));}finally{rmSync(root,{recursive:true,force:true});}}
test('accept fixed setup-failure envelope',()=>control(()=>{},true));
test('reject arbitrary secret field',()=>control(s=>s.token='SYNTHETIC_SECRET'));
test('reject raw exception field',()=>control(s=>s.error='SYNTHETIC_PRIVATE_ERROR'));
test('reject unexpected raw log upload',()=>control((_s,r)=>writeFileSync(join(r,'npm.log'),'SYNTHETIC_SECRET')));
test('reject injected arbitrary TAP diagnostic',()=>control((_s,r)=>writeFileSync(join(r,'controls.tap'),'# private=SYNTHETIC_SECRET\n')));
test('reject upload file exceeding cap',()=>control((_s,r)=>writeFileSync(join(r,'controls.tap'),Buffer.alloc(1048577,65))));
test('reject nested upload directory',()=>control((_s,r)=>mkdirSync(join(r,'credentials'))));
test('reject behavioral credit in setup summary',()=>control(s=>s.behavioralInvocations=1));
import{readFileSync,unlinkSync}from'node:fs';import{complete}from'./upload-success-fixture.mjs';
const expected=JSON.parse(readFileSync(new URL('./payload/diagnostic/expected-tests.json',import.meta.url),'utf8'));
function mutate(root,name,fn){const path=join(root,name),x=JSON.parse(readFileSync(path,'utf8'));fn(x);writeFileSync(path,JSON.stringify(x));}
function successControl(change,accepted=false){const root=mkdtempSync(join(tmpdir(),'pi-upload-full-control-'));try{complete(root,expected);change(root);if(accepted)assert.doesNotThrow(()=>validate(root,expected));else assert.throws(()=>validate(root,expected));}finally{rmSync(root,{recursive:true,force:true});}}
test('accept complete reconciled success with legitimate synthetic failures',()=>successControl(()=>{},true));
test('reject PASS summary without required evidence',()=>control(s=>{s.outcome='PASS';s.stage='setup-complete';}));
test('reject missing native case evidence',()=>successControl(r=>unlinkSync(join(r,'native-native-receipts.json'))));
test('reject duplicate native case identity',()=>successControl(r=>mutate(r,'native-native-receipts.json',x=>x[1]=x[0])));
test('reject native unresolved active job',()=>successControl(r=>mutate(r,'native-native-receipts.json',x=>x[0].receipt.activeZero=false)));
test('reject resumed assignment-refusal receipt',()=>successControl(r=>mutate(r,'native-native-receipts.json',x=>x[1].receipt.resumed=true)));
test('reject changed original nonzero control outcome',()=>successControl(r=>mutate(r,'native-native-receipts.json',x=>x[2].receipt.originalExit=0)));
test('reject truncated accepted46 TAP',()=>successControl(r=>writeFileSync(join(r,'controls.tap'),'TAP version 13\n')));
test('reject duplicated accepted46 TAP result',()=>successControl(r=>writeFileSync(join(r,'controls.tap'),readFileSync(join(r,'controls.tap'),'utf8')+'ok 47 - '+expected[0]+'\n')));
test('reject absent policy execution receipt',()=>successControl(r=>unlinkSync(join(r,'policy-receipt.json'))));
test('reject wrong policy TAP count',()=>successControl(r=>writeFileSync(join(r,'policy-controls.tap'),readFileSync(join(r,'policy-controls.tap'),'utf8').replace(/# pass [0-9]+/,'# pass 0'))));
test('reject incomplete installer receipt set',()=>successControl(r=>mutate(r,'install-receipts.json',x=>x.pop())));
test('reject tool Node digest inconsistent with summary',()=>successControl(r=>mutate(r,'tool-provenance.json',x=>x.before.node.sha256='b'.repeat(64))));
test('reject unverified post-use tool equality',()=>successControl(r=>mutate(r,'tool-provenance.json',x=>x.afterEqual=false)));
test('reject duplicate dependency group',()=>successControl(r=>mutate(r,'dependency-result.json',x=>x.packages[1]=x.packages[0])));
test('reject require route substituted for import',()=>successControl(r=>mutate(r,'dependency-result.json',x=>x.resolution[0].condition='require')));
test('reject missing native import resolution proof',()=>successControl(r=>unlinkSync(join(r,'resolution-control-result.json'))));
test('reject permissive unresolved-ownership latch evidence',()=>successControl(r=>mutate(r,'latch-result.json',x=>x[3].nextLaunchPermitted=true)));
