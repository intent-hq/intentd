import assert from 'node:assert/strict';
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
const mapping=JSON.parse(read('assertion-map.json'));
const rows=[],knownFiles=new Set();let caseIndex=0,assertion=0,created=false,cleaned=false;
function need(ok,id){assertion=id;if(!ok)throw new Error('dependency_control_'+id);}
function reject(fn,id){let thrown=false,error;try{fn();}catch(e){thrown=true;error=e;}need(thrown&&error instanceof Error&&error.message==='dependency_control_'+id,90);}
const sha=b=>createHash('sha256').update(b).digest('hex');
const canonical=x=>JSON.stringify(x);
async function group(i,fn){caseIndex=i;await fn();rows.push({name:expected[i-1],outcome:'PASS'});}
function once(source,a,b){const start=source.indexOf(a),end=source.indexOf(b,start);need(start>=0&&end>start&&source.indexOf(a,start+1)<0,1);return source.slice(start,end);}
function extract(source){return {helper:once(source,'// BEGIN FIXED','// END FIXED'),tar:once(source,'async function tarFor(', '\nfunction inventory('),catch:source.slice(source.lastIndexOf('} catch(dependencyFailureError) {'))};}
const originals=[extract(old),extract(marked)];
function header({name='package/file',type='0',data=Buffer.from('fixture'),size=null,checksumBad=false}={}){
 const b=Buffer.alloc(512);b.write(name,0,100,'utf8');b.write('0000644\0',100,8,'ascii');
 b.write(size??(data.length.toString(8).padStart(11,'0')+'\0'),124,12,'ascii');b[156]=type.charCodeAt(0);b.fill(32,148,156);
 let sum=0;for(const x of b)sum+=x;b.write(sum.toString(8).padStart(6,'0')+'\0 ',148,8,'ascii');if(checksumBad)b[148]=b[148]===48?49:48;
 return Buffer.concat([b,data,Buffer.alloc((512-data.length%512)%512)]);
}
function archive(entries){return gzipSync(Buffer.concat([...entries,Buffer.alloc(1024)]));}
const good=archive([header({name:'package/dir/',type:'5',data:Buffer.alloc(0)}),header({name:'package/dir/file'})]);
const stages=delta.changes.slice(0,9).map(c=>c.new.match(/MarkDependencyFailureStage\('([^']+)'/)[1]);
const samples=[archive([header({checksumBad:true})]),archive([header({size:'00000000008\0'})]),archive([header({size:'00000010000\0'})]),archive([header({name:'outside/file'})]),archive([header({name:'package/CON'})]),archive([header(),header()]),null,archive([header({type:'2'})]),archive([])];
const AsyncFunction=Object.getPrototypeOf(async function(){}).constructor;
// Exact function bodies/helper/outer catch; only providers/observers are supplied. No package body is read or imported.
function fixture(which,options={}){
 const ex=originals[which],ledger=[],marks=[],writes=[],observer={present:false,value:undefined},state={gz:good,writeFault:false,markFault:false,includeFault:false,outsideFault:false,argumentFault:false};
 const injected=Object.hasOwn(options,'injected')?options.injected:Object.assign(new Error('private-fixture'),{code:'EACCES'});
 const remember=e=>{observer.present=true;observer.value=e;};
 function call(method,args){ledger.push(['assert',method,args]);try{if(options.skipChecksum&&method==='equal'&&args[2]==='tar checksum')return;return method==='call'?assert(...args):assert[method](...args);}catch(e){remember(e);if(options.replaceError)throw new Error('private-replacement');if(options.changedSemantics)e.code='OTHER';throw e;}}
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
  const sri='sha512-'+createHash('sha512').update(gz).digest('base64');let thrown=false,value,result;
  try{result=await outer(api.tarFor,'https://registry.npmjs.org/fixture/'+(++seq)+'.tgz',sri,api.write);}catch(e){thrown=true;value=e;}finally{api.includes(false);}
  return {thrown,value,result,ledger:structuredClone(ledger),marks:structuredClone(marks),writes:[...writes],observed:observer.present,boundary:observer.value,stage:api.state()[0]};
 }
 return {run,injected,api};
}
function semantic(e){return {code:e?.code,operator:e?.operator,actual:e?.actual,expected:e?.expected,message:e?.message};}
function compare(a,b){need(a.thrown===b.thrown,3);need(canonical(a.ledger)===canonical(b.ledger),4);if(a.thrown){need(a.observed&&b.observed&&Object.is(a.value,a.boundary)&&Object.is(b.value,b.boundary),5);need(canonical(semantic(a.value))===canonical(semantic(b.value)),6);}else{need(canonical([...a.result.files])===canonical([...b.result.files])&&a.result.tarSha===b.result.tarSha&&a.result.integrity===b.result.integrity&&a.result.url===b.result.url,7);need(a.writes.length===0&&b.writes.length===0&&canonical(a.marks)===canonical(b.marks),8);}}
function checkFailure(r,stage){need(r.thrown&&r.observed&&Object.is(r.value,r.boundary),9);need(r.writes.length===1&&r.writes[0].o.flag==='wx'&&Buffer.byteLength(r.writes[0].b)<=512,10);const v=JSON.parse(r.writes[0].b);need(Object.keys(v).sort().join('|')==='behavioralInvocations|code|group|outcome|schema|stage'&&v.schema==='dependency-failure-v1'&&v.outcome==='FAILED'&&v.behavioralInvocations===0,11);need(v.stage===stage&&r.stage===stage,12);need(!r.writes[0].b.includes('private-'),13);return v;}
function boundMarks(r){return r.marks.filter(a=>a[0].startsWith('tar-a'));}
// A07 is solely a source-bound scalar model; it never fabricates 100001 native Map entries.
function countModel(which,count,{markerFault=false,replacement=false}={}){
 const c=delta.changes[6],statement=which?c.new:c.old;const events=[],seen={present:false,value:null};
 const observed=v=>{events.push(['assert',v]);try{assert(v);}catch(e){seen.present=true;seen.value=e;throw e;}};
 let threw=false,value;try{new Function('assert','count','MarkDependencyFailureStage',"'use strict';"+statement)(observed,count,s=>{events.push(['mark',s]);if(markerFault)throw new Error('marker');});}catch(e){threw=true;value=replacement?new Error('replacement'):e;}
 return {threw,value,seen,events};
}
function checkCount(r,count,marked){need(r.threw===(count>100000),14);need(canonical(r.events.filter(x=>x[0]==='assert'))===canonical([['assert',count<=100000]]),15);if(r.threw){need(r.seen.present&&Object.is(r.value,r.seen.value)&&r.value.code==='ERR_ASSERTION',16);need(canonical(r.events.filter(x=>x[0]==='mark'))===canonical(marked?[['mark',stages[6]]]:[]),17);}}
async function main(){
 need(process.platform==='win32'&&process.version==='v24.21.0'&&path.isAbsolute(root)&&!fs.existsSync(root),18);fs.mkdirSync(root);created=true;
 await group(1,async()=>{
  need(sha(old)===delta.original&&sha(marked)===delta.candidate,19);let rev=marked;for(const c of [...delta.changes].reverse()){need(rev.split(c.new).length===2,20);rev=rev.replace(c.new,c.old);}need(rev===old&&delta.changes.length===10,21);
  let ps=read('setup-only.ps1');for(const c of [...sd.changes].reverse()){need(ps.split(c.new).length===2,22);ps=ps.replace(c.new,c.old);}need(ps===read('original-setup-only.ps1')&&sd.changes.length===1,23);
  for(let i=0;i<9;i++){const c=delta.changes[i],original=i===7?c.old.slice(5):c.old;need(marked.split(original).length===2&&old.split(original).length===2,24);}
  need(originals[0].catch===originals[1].catch&&originals[0].catch.includes('throw dependencyFailureError;'),25);
  need(mapping.length===9&&mapping.every((m,i)=>m.id===delta.changes[i].id&&m.stageValue===stages[i]&&m.originalStatement===(i===7?delta.changes[i].old.slice(5):delta.changes[i].old)),41);
  const a=await fixture(0).run(),b=await fixture(1).run();compare(a,b);need(!a.thrown&&!b.thrown&&b.result.files.size===1&&boundMarks(b).length===0,26);
  reject(()=>compare(a,{...b,ledger:[...b.ledger,['extra-query']]}),4);
 });
 await group(2,async()=>{
  for(let i=0;i<9;i++){if(i===6)continue;const a=await fixture(0).run(samples[i]),b=await fixture(1).run(samples[i]);compare(a,b);need(a.thrown&&a.value.code==='ERR_ASSERTION'&&b.value.code==='ERR_ASSERTION',27);checkFailure(a,'tar-parse');checkFailure(b,stages[i]);need(canonical(boundMarks(b))===canonical([[stages[i]]])&&boundMarks(a).length===0,28);
   reject(()=>checkFailure({...b,stage:stages[(i+1)%9]},stages[i]),12);
   reject(()=>checkFailure({...b,value:new Error('replacement')},stages[i]),9);
   reject(()=>checkFailure({...b,observed:false},stages[i]),9);
   reject(()=>checkFailure({...b,thrown:false},stages[i]),9);
  }
  const normal=await fixture(0).run(samples[0]);
  const skipped=await fixture(1,{skipChecksum:true}).run(samples[0]);reject(()=>checkFailure(skipped,stages[0]),9);
  const wrong=await fixture(1,{wrongId:true}).run(samples[0]);reject(()=>checkFailure(wrong,stages[0]),12);
  const replaced=await fixture(1,{replaceError:true}).run(samples[0]);reject(()=>checkFailure(replaced,stages[0]),9);
  const changed=await fixture(1,{changedSemantics:true}).run(samples[0]);reject(()=>compare(normal,changed),6);
  for(const n of [0,99999,100000,100001]){checkCount(countModel(0,n),n,false);checkCount(countModel(1,n),n,true);}checkCount(countModel(1,100001,{markerFault:true}),100001,true);reject(()=>checkCount(countModel(1,100001,{replacement:true}),100001,true),16);
 });
 await group(3,async()=>{
  for(const opts of [{argumentFault:true},{outsideFault:true}]){for(const injected of [Object.assign(new Error('private-provider'),{code:'EACCES'}),'private-string',null]){
   // Explicit values, including null, are captured by the same observing provider in both variants.
   const aF=fixture(0,{injected}),bF=fixture(1,{injected});need(Object.is(aF.injected,injected)&&Object.is(bF.injected,injected),29);
   const a=await aF.run(archive([header()]),opts),b=await bF.run(archive([header()]),opts);compare(a,b);need(Object.is(a.value,injected)&&Object.is(b.value,injected),30);checkFailure(a,'tar-parse');const v=checkFailure(b,opts.argumentFault?stages[5]:'tar-parse');need(v.code===(injected&&typeof injected==='object'?'EACCES':'OTHER'),31);need(boundMarks(b).length===(opts.argumentFault?1:0),32);
  }}
 });
 await group(4,async()=>{
  const aF=fixture(0),bF=fixture(1);
  for(const opts of [{},{markFault:true},{includeFault:true}]){compare(await aF.run(),await bF.run());const a=await aF.run(samples[0]),b=await bF.run(samples[0],opts);compare(a,b);checkFailure(b,opts.markFault||opts.includeFault?'tar-parse':stages[0]);need(boundMarks(b).length===1,33);compare(await aF.run(),await bF.run());}
  const a=await fixture(0).run(samples[0]),b=await fixture(1).run(samples[0],{writeFault:true});compare(a,b);need(b.writes.length===1&&boundMarks(b).length===1,34);
 });
 await group(5,async()=>{
  need(schema.stages.includes('unknown')&&stages.every(s=>schema.stages.includes(s)),35);
  for(let i=0;i<9;i++){if(i===6)continue;const b=await fixture(1).run(samples[i]);const v=checkFailure(b,stages[i]);need(v.code==='ERR_ASSERTION'&&v.group==='none'&&Buffer.byteLength('DEPENDENCY_FAILURE_V1 '+b.writes[0].b)<=768,36);}
  // Native PowerShell relay, all nine enums, unknown, cap and duplicate-token behavior are required by corresponding PS groups.
  need(stages.length===9&&new Set(stages).size===9,37);
 });
 await group(6,async()=>{
  const f=fixture(1,{realWrite:true});const first=await f.run(samples[0]);checkFailure(first,stages[0]);const filename=path.join(root,'dependency-failure.json'),bytes=fs.readFileSync(filename);need(bytes.length<=512&&bytes.equals(Buffer.from(first.writes[0].b)),38);
  const second=await f.run(samples[1]);checkFailure(second,stages[1]);need(fs.readFileSync(filename).equals(bytes)&&second.writes.length===1&&Object.is(second.value,second.boundary),39);
  fs.unlinkSync(filename);knownFiles.delete(filename);
  const absent=await f.run(samples[2],{writeFault:true});checkFailure(absent,stages[2]);need(!fs.existsSync(filename),40);
 });
}
let outcome='FAILED';try{await main();outcome='PASS';}catch{}finally{try{if(created){for(const f of fs.readdirSync(root)){const full=path.join(root,f),st=fs.lstatSync(full);need(knownFiles.has(full)&&st.isFile()&&!st.isSymbolicLink(),47);}for(const f of knownFiles)fs.unlinkSync(f);fs.rmdirSync(root);cleaned=!fs.existsSync(root);}}catch{outcome='FAILED';}}
if(!created||!cleaned||rows.length!==6)outcome='FAILED';
process.stdout.write(JSON.stringify({schema:'dependency-js-controls-v1',outcome,caseIndex,assertion,created,cleaned,results:outcome==='PASS'?rows:[],completed:rows.map(x=>x.name),setupInvocations:0,packageEntryExecutions:0,privateLogReads:0}));if(outcome!=='PASS')process.exitCode=1;
