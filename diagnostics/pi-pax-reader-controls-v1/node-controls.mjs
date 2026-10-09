import assert from 'node:assert/strict';
import {createReaderLoader,readNativeTar} from './reader-loader.mjs';
import * as fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {gzipSync,gunzipSync} from 'node:zlib';
import {fileURLToPath} from 'node:url';
const packet=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];
const read=n=>fs.readFileSync(path.join(packet,n),'utf8');
const old=read('original-accept-dependencies.mjs'),marked=read('accept-dependencies.mjs');
const delta=JSON.parse(read('acceptance-delta.json')),sd=JSON.parse(read('setup-delta.json'));
const schema=JSON.parse(read('schema.json')),expected=JSON.parse(read('expected-controls.json'));
const rows=[],knownFiles=new Set();let caseIndex=0,assertion=0,created=false,cleaned=false;
function need(ok,id){assertion=id;if(!ok)throw new Error('dependency_control_'+id);}
function reject(fn,id){let thrown=false,error;try{fn();}catch(e){thrown=true;error=e;}need(thrown&&error instanceof Error&&error.message==='dependency_control_'+id,90);}
const sha=b=>createHash('sha256').update(b).digest('hex');
const canonical=x=>JSON.stringify(x);
async function group(i,fn){caseIndex=i;await fn();rows.push({name:expected[i-1],outcome:'PASS'});}
function once(source,a,b){const start=source.indexOf(a),end=source.indexOf(b,start);need(start>=0&&end>start&&source.indexOf(a,start+1)<0,1);return source.slice(start,end);}
function extract(source){return {helper:once(source,'// BEGIN FIXED','// END FIXED'),tar:once(source,'async function tarFor(', '\nfunction inventory('),catch:source.slice(source.lastIndexOf('} catch(dependencyFailureError) {'))};}
const originals=[extract(old),extract(marked)];
function header({name='package/file',type='0',data=Buffer.from('fixture'),size=null,prefix='',magic='ustar\0',version='00',checksumBad=false,linkname=''}={}){
 const b=Buffer.alloc(512);b.write(name,0,100,'utf8');b.write('0000644\0',100,8,'ascii');
 b.write(size??(data.length.toString(8).padStart(11,'0')+'\0'),124,12,'ascii');b[156]=type.charCodeAt(0);
 b.write(linkname,157,100,'utf8');b.write(prefix,345,155,'utf8');b.write(magic,257,6,'ascii');b.write(version,263,2,'ascii');b.fill(32,148,156);
 let sum=0;for(const x of b)sum+=x;b.write(sum.toString(8).padStart(6,'0')+'\0 ',148,8,'ascii');if(checksumBad)b[148]=b[148]===48?49:48;
 return Buffer.concat([b,data,Buffer.alloc((512-data.length%512)%512)]);
}
function archive(entries,tail=Buffer.alloc(1024)){return gzipSync(Buffer.concat([...entries,tail]));}
const empty=Buffer.alloc(0),rootHeader=()=>header({name:'package',type:'5',data:empty});
const good=archive([header({name:'package/dir/',type:'5',data:empty}),header({name:'package/dir/file'})]);
const AsyncFunction=Object.getPrototypeOf(async function(){}).constructor;
// Exact function bodies/helper/outer catch; only providers/observers are supplied. No package body is read or imported.
function fixture(which,options={}){
 const originalEx=originals[which],ex={...originalEx,tar:options.mutate?options.mutate(originalEx.tar):originalEx.tar},ledger=[],marks=[],writes=[],observer={present:false,value:undefined},state={gz:good,writeFault:false,markFault:false,includeFault:false,outsideFault:false,argumentFault:false};
 const injected=Object.hasOwn(options,'injected')?options.injected:Object.assign(new Error('private-fixture'),{code:'EACCES'});
 const remember=e=>{observer.present=true;observer.value=e;};
 function call(method,args){ledger.push(['assert',method,args]);try{if(method==='call'&&((options.assertFault&&args[1]==='invalid exact package root directory')||args[1]===options.assertFaultMessage&&options.assertFaultMessage!==undefined))throw injected;if(options.skipChecksum&&method==='equal'&&args[2]==='tar checksum')return;return method==='call'?assert(...args):assert[method](...args);}catch(e){remember(e);if(options.replaceError)throw new Error('private-replacement');if(options.changedSemantics)e.code='OTHER';throw e;}}
 const observed=(...args)=>call('call',args);observed.equal=(...args)=>call('equal',args);
 class ObservedMap extends Map {
  constructor(...args){super(...args);ledger.push(['map-new']);}
  has(k){ledger.push(['map-has',k]);if(state.argumentFault&&k==='file'){remember(injected);throw injected;}return super.has(k);}
  set(k,v){ledger.push(['map-set',k]);if(state.outsideFault&&k==='file'){remember(injected);throw injected;}return super.set(k,v);}
 }
 function write(p,b,o){need(p===path.join(root,'dependency-failure.json'),2);writes.push({b,o});if(state.writeFault)throw new Error('private-writer');if(options.realWrite){fs.writeFileSync(p,b,o);knownFiles.add(p);}}
 const load=new Function('assert','createHash','gunzipSync','fetch','AbortSignal','Map','writeFileSync','join','root','markObserver',"'use strict';\n"+ex.helper+"\nconst digest=b=>createHash('sha256').update(b).digest('hex');let downloaded=0,totalInflated=0;const cache=new Map();\n"+ex.tar+"\nconst originalMark=MarkDependencyFailureStage;MarkDependencyFailureStage=(...args)=>{markObserver(args);return originalMark(...(markObserver.remap&&args[0]==='tar-a01-checksum'?['tar-a02-octal-size']:args));};dependencyFailureRoot=root;\nreturn {tarFor,state(){return [dependencyFailureStage,dependencyFailureGroup];},write:WriteDependencyFailureMetadata,includes(v){if(v){DEPENDENCY_FAILURE_STAGES.includes=function(stage){if(stage.startsWith('tar-a'))throw new Error('private-includes');return Array.prototype.includes.call(this,stage);};}else delete DEPENDENCY_FAILURE_STAGES.includes;}};");
 const markObserver=args=>{marks.push(args);if(state.markFault&&args[0].startsWith('tar-a'))throw new Error('private-mark');};markObserver.remap=!!options.wrongId;
 const api=load(observed,createHash,(b,o)=>{ledger.push(['inflate',o.maxOutputLength]);return gunzipSync(b,o);},async(u,o)=>{ledger.push(['fetch',u.href,o.redirect]);return {status:200,body:[state.gz]};},{timeout(ms){ledger.push(['timeout',ms]);return 'fixture-only';}},ObservedMap,write,path.join,root,markObserver);
 const outer=new AsyncFunction('tarFor','url','sri','WriteDependencyFailureMetadata',"'use strict';\ntry {return await tarFor(url,sri);\n"+ex.catch);
 let seq=0;
 async function run(gz=good,opts={}){
  state.gz=gz;state.writeFault=!!opts.writeFault;state.markFault=!!opts.markFault;state.argumentFault=!!opts.argumentFault;state.outsideFault=!!opts.outsideFault;api.includes(!!opts.includeFault);
  ledger.length=0;marks.length=0;writes.length=0;observer.present=false;observer.value=undefined;
  const sri=opts.sri??('sha512-'+createHash('sha512').update(gz).digest('base64'));let thrown=false,value,result;
  try{result=await outer(api.tarFor,opts.url??('https://registry.npmjs.org/fixture/'+(++seq)+'.tgz'),sri,api.write);}catch(e){thrown=true;value=e;}finally{api.includes(false);}
  return {thrown,value,result,ledger:structuredClone(ledger),marks:structuredClone(marks),writes:[...writes],observed:observer.present,boundary:observer.value,stage:api.state()[0]};
 }
 return {run,injected,api};
}
function semantic(e){return {code:e?.code,operator:e?.operator,actual:e?.actual,expected:e?.expected,name:e?.name,generatedMessage:e?.generatedMessage};}
function compare(a,b){need(a.thrown===b.thrown,3);need(canonical(a.ledger)===canonical(b.ledger),4);if(a.thrown){need(a.observed&&b.observed&&Object.is(a.value,a.boundary)&&Object.is(b.value,b.boundary),5);need(canonical(semantic(a.value))===canonical(semantic(b.value)),6);}else{need(canonical([...a.result.files])===canonical([...b.result.files])&&a.result.tarSha===b.result.tarSha&&a.result.integrity===b.result.integrity&&a.result.url===b.result.url,7);need(a.writes.length===0&&b.writes.length===0&&canonical(a.marks)===canonical(b.marks),8);}}
function checkFailure(r,stage){need(r.thrown&&r.observed&&Object.is(r.value,r.boundary),9);need(r.writes.length===1&&r.writes[0].o.flag==='wx'&&Buffer.byteLength(r.writes[0].b)<=512,10);const v=JSON.parse(r.writes[0].b);need(Object.keys(v).sort().join('|')==='behavioralInvocations|code|group|outcome|schema|stage'&&v.schema==='dependency-failure-v1'&&v.outcome==='FAILED'&&v.behavioralInvocations===0,11);need(v.stage===stage&&r.stage===stage,12);need(!r.writes[0].b.includes('private-'),13);return v;}
function boundMarks(r){return r.marks.filter(a=>a[0].startsWith('tar-a'));}
function mutateOnce(source,a,b){need(source.split(a).length===2,24);return source.replace(a,b);}
function nativeFailure(r,stage,message){checkFailure(r,stage);need(r.value instanceof assert.AssertionError&&r.value.code==='ERR_ASSERTION',30);if(message!==undefined)need(r.value.message===message,31);}
function bindArchive(r,gz){need(!r.thrown&&r.result.tarSha===sha(gz)&&r.result.integrity==='sha512-'+createHash('sha512').update(gz).digest('base64')&&r.result.url.startsWith('https://registry.npmjs.org/fixture/'),32);}
function sameFiles(a,b){need(!a.thrown&&!b.thrown&&canonical([...a.result.files])===canonical([...b.result.files]),33);}
function ioLedger(r){return r.ledger.filter(e=>['fetch','timeout','inflate'].includes(e[0]));}
function paxRecord(key,value){
 const tail=Buffer.concat([Buffer.from(key+'='),Buffer.isBuffer(value)?value:Buffer.from(value),Buffer.from('\n')]);
 let n=tail.length+2;for(let i=0;i<8;i++){const next=tail.length+String(n).length+1;if(next===n)return Buffer.concat([Buffer.from(n+' '),tail]);n=next;}throw new Error('fixture_length');
}
const bodyOf=records=>Buffer.concat(records.map(([k,v])=>paxRecord(k,v)));
function pax(records=[['path','package/effective']],options={}){return header({name:'PaxHeader/file',type:'x',data:bodyOf(records),...options});}
function paxArchive(records,member={},rest=[]){return archive([pax(records),header({name:'package/raw',data:Buffer.from('body'),...member}),...rest]);}
function paxFailure(r,message){nativeFailure(r,'tar-parse',message);const last=r.ledger.filter(e=>e[0]==='assert').at(-1);need(last?.[1]==='call'&&last[2].length===2&&last[2][1]===message&&!last[2][0]&&r.value.operator==='=='&&Object.is(r.value.actual,last[2][0])&&r.value.expected===true&&r.value.generatedMessage===false,65);}
function expectFiles(r,gz,entries){bindArchive(r,gz);need(canonical([...r.result.files])===canonical(entries.map(([name,data])=>[name,{sha256:sha(data),bytes:data.length}])),66);}
const recordSource=once(originals[1].tar,' function paxPath(','\n for(let offset');
function recordModel(body,options={}){
 const events=[];let boundary,seen=false,thrown=false,value,result;
 const injected=Object.hasOwn(options,'injected')?options.injected:new Error('private-pax');
 const observed=(...args)=>{events.push(args);try{if(options.fault)throw injected;return assert(...args);}catch(e){seen=true;boundary=e;throw e;}};
 const fn=new Function('assert','Buffer',"'use strict';"+recordSource+';return paxRecords;')(observed,Buffer);
 try{result=fn(body);}catch(e){thrown=true;value=e;}return {events,seen,boundary,thrown,value,result,injected};
}
function recordRefusal(body,message){const r=recordModel(body);need(r.thrown&&r.seen&&Object.is(r.value,r.boundary)&&r.value instanceof assert.AssertionError&&r.value.message===message,67);return r;}
function capModel(length,paxBytes,paxHeaders){
 // Actual cap expression, scalar model only: no claim of native large allocation or elapsed time.
 const expression=once(originals[1].tar,"assert(length>0&&length<=1048576",",'pax metadata cap');").slice(7);
 return new Function('length','paxBytes','paxHeaders','return ('+expression+');')(length,paxBytes,paxHeaders);
}

// Only the hash-bound tar reader closure executes as third-party code. Application package entry count remains zero.
let loadedReader,Parser,readerReceipt=null,readerComparisons=0,readerRefusals=0;
const readerEvidence=[];
async function nativeFixture(label,gz,compressed=false){
 need(/^[a-z0-9-]{1,48}$/.test(label)&&readerEvidence.length<64,78);
 const input=compressed?gz:gunzipSync(gz,{maxOutputLength:131072});
 const result=await readNativeTar(Parser,input);
 readerEvidence.push({label,compressed,inputSha256:sha(input),compressedSha256:sha(gz),result:structuredClone(result)});
 return result;
}
function expectNative(n,expected){
 const rows=expected.map(([name,type,data])=>({path:name,type,size:data.length,bytes:data.length,sha256:sha(data)}));
 need(n.ok===true&&n.code==='end'&&n.entries===rows.length&&n.ended===rows.length&&n.total===rows.reduce((s,r)=>s+r.bytes,0)&&canonical(n.rows)===canonical(rows),88);
}
async function differential(label,gz,entries,directories=[]){
 const candidate=await fixture(1).run(gz);expectFiles(candidate,gz,entries);
 const expected=[...directories.map(n=>[n,'Directory',empty]),...entries.map(([n,b])=>['package/'+n,'File',b])];
 expectNative(await nativeFixture(label+'-raw',gz),expected);
 expectNative(await nativeFixture(label+'-gzip',gz,true),expected);readerComparisons++;
}

async function main(){
 need(process.platform==='win32'&&process.version==='v24.21.0'&&path.isAbsolute(root)&&!fs.existsSync(root),18);fs.mkdirSync(root);created=true;
 await group(1,async()=>{
  let reads=0;const readBound=n=>{reads++;return fs.readFileSync(path.join(packet,n));};
  const createdLoader=createReaderLoader(packet,readBound);need(reads===12&&createdLoader.loaded().length===0,80);
  function loaderRefuse(readFn,code){let error;try{createReaderLoader(packet,readFn);}catch(e){error=e;}need(error instanceof Error&&error.message===code,81);}
  loaderRefuse(n=>n==='reader-manifest.json'?Buffer.from('{}'):readBound(n),'reader_loader_manifest_hash');
  const manifest=JSON.parse(read('reader-manifest.json')),files=Object.values(manifest.modules).map(m=>m.file);
  for(const file of [files[0],files.at(-1)])loaderRefuse(n=>n===file?Buffer.from('bad'):readBound(n),'reader_loader_source_hash');
  const injected=new Error('fixed-read-fault');let caught;try{createReaderLoader(packet,n=>{if(n===files[0])throw injected;return readBound(n);});}catch(e){caught=e;}need(caught===injected,82);
  let refused;try{createdLoader.resolve(createdLoader.entry,'fs');}catch(e){refused=e;}need(refused?.message==='reader_loader_specifier'&&createdLoader.loaded().length===0,83);
  loadedReader=createdLoader;const exports=loadedReader.load(loadedReader.entry);Parser=exports.Parser;
  need(typeof Parser==='function'&&loadedReader.load(loadedReader.entry)===exports,84);
  need(loadedReader.loaded().length===11&&new Set(loadedReader.loaded()).size===11&&canonical(loadedReader.loaded().slice().sort())===canonical(Object.keys(manifest.modules).sort()),85);
  need(canonical(loadedReader.sourceHashes())===canonical(Object.fromEntries(Object.entries(manifest.modules).map(([id,m])=>[id,m.sha256]))),86);
  const original=await fixture(0).run(good),candidate=await fixture(1).run(good);compare(original,candidate);
  await differential('ordinary',good,[['dir/file',Buffer.from('fixture')]],['package/dir/']);
 });
 await group(2,async()=>{
  for(const [id,name]of [['ascii','package/new'],['utf8','package/café-😀'],['long','package/'+('a'.repeat(160))]]){
   const gz=paxArchive([['path',name],['mtime','-1.25'],['linkpath','']],{},[header({name:'package/after',data:Buffer.from('next')})]);
   await differential(id,gz,[[name.slice(8),Buffer.from('body')],['after',Buffer.from('next')]]);
  }
  const gz=paxArchive([['path','package/replaced']]);await differential('raw-ustar-prefix',archive([pax([['path','package/replaced']]),header({name:'raw',prefix:'package',data:Buffer.from('body')})]),[['replaced',Buffer.from('body')]]);
  for(const [i,value]of ['outside/file','package/../escape','package/C:bad','package/con'].entries()){
   const bad=paxArchive([['path',value]]),r=await fixture(1).run(bad);paxFailure(r,value==='package/con'?'pax effective Windows name':'pax effective path');
   const n=await nativeFixture('security-'+i,bad);expectNative(n,[[value,'File',Buffer.from('body')]]);readerRefusals++;
  }
  const backslash=paxArchive([['path','package/dir\\file']]);paxFailure(await fixture(1).run(backslash),'pax effective path');expectNative(await nativeFixture('windows-backslash',backslash),[['package/dir/file','File',Buffer.from('body')]]);readerRefusals++;
  const raw=paxArchive([['path','package/good']],{name:'outside/raw'});nativeFailure(await fixture(1).run(raw),'tar-a04-form-file');expectNative(await nativeFixture('raw-path-refusal',raw),[['package/good','File',Buffer.from('body')]]);readerRefusals++;
 });
 await group(3,async()=>{
  for(const size of [0,1,511,512,513]){
   const data=Buffer.alloc(size,97),gz=paxArchive([['path','package/sized'],['size',String(size)]],{data,size:'00000000000\0'},[header({name:'package/after',data:Buffer.from('next')})]);
   await differential('size-'+size,gz,[['sized',data],['after',Buffer.from('next')]]);
  }
  const shrink=paxArchive([['size','1']],{data:Buffer.from('x'),size:'00000000400\0'},[header({name:'package/tail',data:Buffer.from('z')})]);
  await differential('size-shrink',shrink,[['raw',Buffer.from('x')],['tail',Buffer.from('z')]]);
  const sizeGz=paxArchive([['size','513']],{data:Buffer.alloc(513,97),size:'00000000000\0'});
  const mutant=await fixture(1,{mutate:s=>mutateOnce(s,'contentLength=pendingPax.size','contentLength=length')}).run(sizeGz);
  let rejected=false;try{expectFiles(mutant,sizeGz,[['raw',Buffer.alloc(513,97)]]);}catch(e){rejected=e.message==='dependency_control_32'||e.message==='dependency_control_66';}need(rejected,87);
 });
 await group(4,async()=>{
  const malformed=archive([pax([],{data:Buffer.from('99 path=package/ignored\n')}),header({name:'package/raw',data:Buffer.from('body')})]);
  paxFailure(await fixture(1).run(malformed),'pax record extent');expectNative(await nativeFixture('bad-length-ignored',malformed),[['package/raw','File',Buffer.from('body')]]);readerRefusals++;
  const duplicate=paxArchive([['path','package/first'],['path','package/last']]);
  paxFailure(await fixture(1).run(duplicate),'pax record key');expectNative(await nativeFixture('duplicate-last-wins',duplicate),[['package/last','File',Buffer.from('body')]]);readerRefusals++;
  const unknown=paxArchive([['unknown','fixed']]);paxFailure(await fixture(1).run(unknown),'pax record key');expectNative(await nativeFixture('unknown-ignored',unknown),[['package/raw','File',Buffer.from('body')]]);readerRefusals++;
  const badUtf=paxArchive([['path',Buffer.from([112,97,99,107,97,103,101,47,255])]]);paxFailure(await fixture(1).run(badUtf),'pax path encoding');
  // This is a candidate-only malformed UTF8 refusal; no replacement-decoding equivalence is claimed.
  const correct=paxArchive([['path','package/byte-😀'],['size','4']]);await differential('byte-framing',correct,[['byte-😀',Buffer.from('body')]]);
 });
 await group(5,async()=>{
  const repeated=archive([pax([['path','package/one']]),header({name:'package/raw',data:Buffer.from('1')}),header({name:'package/plain',data:Buffer.from('2')}),pax([['path','package/three']]),header({name:'package/raw',data:Buffer.from('3')})]);
  await differential('single-use-reset',repeated,[['one',Buffer.from('1')],['plain',Buffer.from('2')],['three',Buffer.from('3')]]);
  const chain=archive([pax([['path','package/one']]),pax([['path','package/two']]),header({name:'package/raw',data:Buffer.from('x')})]);
  paxFailure(await fixture(1).run(chain),'pax regular target');expectNative(await nativeFixture('chained-last-path',chain),[['package/two','File',Buffer.from('x')]]);readerRefusals++;
  const global=archive([pax([['path','package/ignored']],{type:'g'}),header({name:'package/raw',data:Buffer.from('x')})]);
  nativeFailure(await fixture(1).run(global),'tar-a04-form-pax-g');expectNative(await nativeFixture('global-path-ignored',global),[['package/raw','File',Buffer.from('x')]]);readerRefusals++;
  const directory=archive([pax([['path','package/dir']]),header({name:'package/raw',type:'5',data:empty}),header({name:'package/after',data:Buffer.from('x')})]);
  paxFailure(await fixture(1).run(directory),'pax regular target');expectNative(await nativeFixture('directory-target',directory),[['package/dir','Directory',empty],['package/after','File',Buffer.from('x')]]);readerRefusals++;
  const collision=archive([pax([['path','package/same']]),header({name:'package/raw',data:Buffer.from('1')}),header({name:'package/same',data:Buffer.from('2')})]);
  nativeFailure(await fixture(1).run(collision),'tar-a06-file-unique');expectNative(await nativeFixture('duplicate-members',collision),[['package/same','File',Buffer.from('1')],['package/same','File',Buffer.from('2')]]);readerRefusals++;
  const orphan=archive([pax([['path','package/unused']])]);paxFailure(await fixture(1).run(orphan),'pax orphan');
 });
 await group(6,async()=>{
  const gz=paxArchive([['path','package/effective']]),n=await nativeFixture('oracle-base',gz);expectNative(n,[['package/effective','File',Buffer.from('body')]]);
  for(const field of ['path','type','size','bytes','sha256']){const m=structuredClone(n);m.rows[0][field]=typeof m.rows[0][field]==='number'?m.rows[0][field]+1:'changed';reject(()=>expectNative(m,[['package/effective','File',Buffer.from('body')]]),88);}
  const missing=structuredClone(n);missing.rows=[];reject(()=>expectNative(missing,[['package/effective','File',Buffer.from('body')]]),88);
  const extra=structuredClone(n);extra.rows.push(extra.rows[0]);reject(()=>expectNative(extra,[['package/effective','File',Buffer.from('body')]]),88);
  const orderGz=archive([header({name:'package/a',data:Buffer.from('1')}),header({name:'package/b',data:Buffer.from('2')})]);const order=await nativeFixture('order-oracle',orderGz);expectNative(order,[['package/a','File',Buffer.from('1')],['package/b','File',Buffer.from('2')]]);order.rows.reverse();reject(()=>expectNative(order,[['package/a','File',Buffer.from('1')],['package/b','File',Buffer.from('2')]]),88);
  const badChecksum=archive([header({checksumBad:true})]);const failed=await nativeFixture('checksum-refused',badChecksum);need(!failed.ok&&failed.code==='error',89);const r=await fixture(1).run(badChecksum);nativeFailure(r,'tar-a01-checksum');
  const checksumEvents=r.ledger.filter(x=>x[0]==='assert'&&x[1]==='equal'&&x[2][2]==='tar checksum');need(checksumEvents.length===1&&r.value.operator==='strictEqual'&&Object.is(r.value.actual,checksumEvents[0][2][0])&&Object.is(r.value.expected,checksumEvents[0][2][1]),91);
  for(const injected of [new Error('private-injected'),'private-string',null]){const f=fixture(1,{assertFaultMessage:'pax record key',injected}),r=await f.run(paxArchive([['unknown','x']]),{writeFault:true});need(r.thrown&&Object.is(r.value,injected)&&Object.is(r.boundary,injected),92);}
  const f=fixture(1),url='https://registry.npmjs.org/fixture/fresh.tgz';const failedCandidate=await f.run(gz,{url,sri:'sha512-AAAA'});need(failedCandidate.thrown,93);const recovered=await f.run(gz,{url});expectFiles(recovered,gz,[['effective',Buffer.from('body')]]);need(ioLedger(recovered).some(x=>x[0]==='fetch'),94);
  const badPath=await fixture(1,{mutate:s=>mutateOnce(s,'rel=paxPath(pendingPax.path)','rel=rel')}).run(gz);reject(()=>expectFiles(badPath,gz,[['effective',Buffer.from('body')]]),66);
  need(readerComparisons===13&&readerRefusals===13&&readerEvidence.length===42,95);
  readerReceipt={manifestSha256:sha(fs.readFileSync(path.join(packet,'reader-manifest.json'))),modules:loadedReader.loaded().length,calls:readerEvidence.length,comparisons:readerComparisons,refusals:readerRefusals,evidenceSha256:sha(Buffer.from(canonical(readerEvidence)))};
  need(readerReceipt.modules===11&&readerReceipt.calls===42,96);
 });

}
let outcome='FAILED';try{await main();outcome='PASS';}catch{}finally{try{if(created){for(const f of fs.readdirSync(root)){const full=path.join(root,f),st=fs.lstatSync(full);need(knownFiles.has(full)&&st.isFile()&&!st.isSymbolicLink(),47);}for(const f of knownFiles)fs.unlinkSync(f);fs.rmdirSync(root);cleaned=!fs.existsSync(root);}}catch{outcome='FAILED';}}
if(!created||!cleaned||rows.length!==6||readerReceipt===null)outcome='FAILED';
process.stdout.write(JSON.stringify({schema:'dependency-js-controls-v1',reader:readerReceipt,outcome,caseIndex,assertion,created,cleaned,results:outcome==='PASS'?rows:[],completed:rows.map(x=>x.name),setupInvocations:0,packageEntryExecutions:0,privateLogReads:0}));if(outcome!=='PASS')process.exitCode=1;
