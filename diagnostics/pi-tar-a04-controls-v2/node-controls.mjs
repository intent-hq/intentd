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
function semantic(e){return {code:e?.code,operator:e?.operator,actual:e?.actual,expected:e?.expected,name:e?.name,generatedMessage:e?.generatedMessage};}
function compare(a,b){need(a.thrown===b.thrown,3);need(canonical(a.ledger)===canonical(b.ledger),4);if(a.thrown){need(a.observed&&b.observed&&Object.is(a.value,a.boundary)&&Object.is(b.value,b.boundary),5);need(canonical(semantic(a.value))===canonical(semantic(b.value)),6);}else{need(canonical([...a.result.files])===canonical([...b.result.files])&&a.result.tarSha===b.result.tarSha&&a.result.integrity===b.result.integrity&&a.result.url===b.result.url,7);need(a.writes.length===0&&b.writes.length===0&&canonical(a.marks)===canonical(b.marks),8);}}
function checkFailure(r,stage){need(r.thrown&&r.observed&&Object.is(r.value,r.boundary),9);need(r.writes.length===1&&r.writes[0].o.flag==='wx'&&Buffer.byteLength(r.writes[0].b)<=512,10);const v=JSON.parse(r.writes[0].b);need(Object.keys(v).sort().join('|')==='behavioralInvocations|code|group|outcome|schema|stage'&&v.schema==='dependency-failure-v1'&&v.outcome==='FAILED'&&v.behavioralInvocations===0,11);need(v.stage===stage&&r.stage===stage,12);need(!r.writes[0].b.includes('private-'),13);return v;}
function boundMarks(r){return r.marks.filter(a=>a[0].startsWith('tar-a'));}
// Exact A04 block plus exact metadata helper/outercatch; providers are explicit fixtures.
const parts=['prefix','backslash','traversal','colon'];
const blocks=delta.changes[0];
function blockFixture(which,{mutate=null,realWrite=false}={}){
 const ex=originals[which],events=[],marks=[],writes=[];
 const state={options:{},observed:false,boundary:undefined};
 const remember=e=>{state.observed=true;state.boundary=e;};
 const markObserver=args=>{marks.push(args);if(state.options.markerFault&&args[0].startsWith('tar-a04'))throw state.options.fault;};
 function write(p,b,o){need(p===path.join(root,'dependency-failure.json'),2);writes.push({b,o});if(state.options.writerFault)throw state.options.fault;if(realWrite){fs.writeFileSync(p,b,o);knownFiles.add(p);}}
 let body=which?blocks.new:blocks.old;if(mutate)body=mutate(body);
 const loader=new Function('writeFileSync','join','root','markObserver',"'use strict';\n"+ex.helper+"\nconst originalMark=MarkDependencyFailureStage;MarkDependencyFailureStage=(...args)=>{markObserver(args);return originalMark(...args);};dependencyFailureRoot=root;\nreturn {invoke(name,assert){"+body+"},before(name){"+body+";let assert;},state(){return dependencyFailureStage;},write:WriteDependencyFailureMetadata,begin(){MarkDependencyFailureStage('tar-parse');},fault(v){if(v)DEPENDENCY_FAILURE_STAGES.includes=function(s){if(s.startsWith('tar-a04'))throw new Error('private-includes');return Array.prototype.includes.call(this,s);};else delete DEPENDENCY_FAILURE_STAGES.includes;}};");
 const api=loader(write,path.join,root,markObserver);
 const outer=new Function('invoke','name','assert','WriteDependencyFailureMetadata',"'use strict';try {return invoke(name,assert);\n"+ex.catch);
 function run(options={}){
  state.options={fault:new Error('private-fault'),...options};events.length=0;marks.length=0;writes.length=0;state.observed=false;state.boundary=undefined;
  const o=state.options;api.begin();api.fault(!!o.includesFault);let includeGets=0;
  const v=o.values??[true,false,false,false];let name;
  const fail=site=>{if(o.faultAt===site){remember(o.fault);throw o.fault;}};
  function method(receiver,methodName,site,implementation){return function(...args){events.push(['call',site,this===receiver,args]);fail(site+':call');if(site==='includes'&&args[0]===':')fail('includes-colon:call');return implementation(...args);};}
  const segments={};Object.defineProperty(segments,'includes',{get(){events.push(['get','segment-includes']);fail('segment-includes:get');return method(segments,'includes','segment-includes',()=>v[2]);}});
  name={};for(const key of ['startsWith','includes','split'])Object.defineProperty(name,key,{get(){events.push(['get',key]);fail(key+':get');if(key==='includes'&&++includeGets===2)fail('includes-colon:get');return method(name,key,key,(arg)=>{if(key==='startsWith')return v[0];if(key==='split')return segments;return arg==='\\'?v[1]:v[3];});}});
  if(Object.hasOwn(o,'name'))name=o.name;
  const seenAssert=(...args)=>{events.push(['assert',args.length,args[0]]);try{if(o.assertFault)throw o.fault;return assert(...args);}catch(e){remember(e);if(o.changedSemantics&&e&&typeof e==='object')e.code='OTHER';if(o.replaceError)throw new Error('private-replacement');throw e;}};
  let thrown=false,value,result;
  try{result=outer(o.before?api.before:api.invoke,name,o.direct?assert:seenAssert,api.write);}catch(e){thrown=true;value=e;}finally{api.fault(false);}
  return {thrown,value,result,events:[...events],marks:structuredClone(marks),writes:[...writes],observed:state.observed,boundary:state.boundary,stage:api.state()};
 }
 return {run};
}
function errorSemantics(e){return {name:e?.name,code:e?.code,operator:e?.operator,actual:e?.actual,expected:e?.expected,generatedMessage:e?.generatedMessage};}
function equalEvents(a,b){need(a.length===b.length,50);for(let i=0;i<a.length;i++){need(a[i][0]===b[i][0]&&a[i][1]===b[i][1],50);if(a[i][0]==='assert')need(Object.is(a[i][2],b[i][2]),50);else need(canonical(a[i])===canonical(b[i]),50);}}
function pair(a,b){need(a.thrown===b.thrown,51);equalEvents(a.events,b.events);if(a.thrown){need(a.observed&&b.observed&&Object.is(a.value,a.boundary)&&Object.is(b.value,b.boundary),52);need(canonical(errorSemantics(a.value))===canonical(errorSemantics(b.value)),53);}else need(a.result===undefined&&b.result===undefined&&a.writes.length===0&&b.writes.length===0&&canonical(a.marks)===canonical(b.marks),54);}
function metadata(r,stage){need(r.thrown&&r.writes.length===1&&r.writes[0].o.flag==='wx'&&Buffer.byteLength(r.writes[0].b)<=512,55);const v=JSON.parse(r.writes[0].b);need(Object.keys(v).sort().join('|')==='behavioralInvocations|code|group|outcome|schema|stage'&&v.schema==='dependency-failure-v1'&&v.outcome==='FAILED'&&v.behavioralInvocations===0,56);need(v.stage===stage&&r.stage===stage,57);need(!r.writes[0].b.includes('private-')&&Buffer.byteLength('DEPENDENCY_FAILURE_V1 '+r.writes[0].b)<=768,58);return v;}
function expectedEvents(stop=3,last){if(arguments.length<2)last=false;const e=[['get','startsWith'],['call','startsWith',true,['package/']]];if(stop>=1)e.push(['get','includes'],['call','includes',true,['\\']]);if(stop>=2)e.push(['get','split'],['call','split',true,['/']],['get','segment-includes'],['call','segment-includes',true,['..']]);if(stop>=3)e.push(['get','includes'],['call','includes',true,[':']]);e.push(['assert',1,last]);return e;}
function negative(fn,id){reject(fn,id);}
function nativeError(r){need(r.thrown&&r.value instanceof assert.AssertionError&&r.value.code==='ERR_ASSERTION'&&r.value.operator==='=='&&r.value.actual===false&&r.value.expected===true&&r.value.generatedMessage===true,59);}
async function main(){
 need(process.platform==='win32'&&process.version==='v24.21.0'&&path.isAbsolute(root)&&!fs.existsSync(root),18);fs.mkdirSync(root);created=true;
 await group(1,async()=>{
  const oracleDelta=JSON.parse(read('oracle-delta.json')),failedOracle=read('original-oracle-node-controls.mjs'),thisOracle=read('node-controls.mjs');
  need(sha(failedOracle)===oracleDelta.original&&sha(thisOracle)===oracleDelta.candidate,71);
  let oracleReverse=thisOracle;for(const c of [...oracleDelta.changes].reverse()){need(oracleReverse.split(c.new).length===2,72);oracleReverse=oracleReverse.replace(c.new,c.old);}
  need(oracleReverse===failedOracle&&oracleDelta.changes.length===3,73);
  need(sha(old)===delta.original&&sha(marked)===delta.candidate&&delta.changes.length===2,19);let rev=marked;for(const c of [...delta.changes].reverse()){need(rev.split(c.new).length===2,20);rev=rev.replace(c.new,c.old);}need(rev===old,21);
  let ps=read('setup-only.ps1');for(const c of [...sd.changes].reverse()){need(ps.split(c.new).length===2,22);ps=ps.replace(c.new,c.old);}need(ps===read('original-setup-only.ps1')&&sd.changes.length===1,23);need(originals[0].catch===originals[1].catch,25);
  for(const operand of delta.targetOperandOrderAndOccurrences)need(blocks.old.split(operand.operand).length===2&&blocks.new.split(operand.operand).length===2,24);
  const a=blockFixture(0).run(),b=blockFixture(1).run();pair(a,b);equalEvents(b.events,expectedEvents(3,true));
  for(const name of ['package/file','package/dir/file','package/a..b'])pair(blockFixture(0).run({name}),blockFixture(1).run({name}));
  const f0=fixture(0),f1=fixture(1);const x=await f0.run(),y=await f1.run();compare(x,y);need(!x.thrown&&y.result.files.size===1&&boundMarks(y).length===0,26);
  negative(()=>pair(a,{...b,events:[...b.events,['extra']]}),50);
 });
 await group(2,async()=>{
  const paths=['outside/file','package/a\\b','package/a/../b','package/a:b'];
  for(let i=0;i<4;i++){
   const values=[true,false,false,false];values[i]=i===0?false:true;
   const a=blockFixture(0).run({values}),b=blockFixture(1).run({values});pair(a,b);equalEvents(b.events,expectedEvents(i,false));metadata(a,'tar-a04-package-path');metadata(b,'tar-a04-'+parts[i]+'-false');nativeError(b);
   for(const which of [0,1]){const r=blockFixture(which).run({name:paths[i],direct:true});nativeError(r);metadata(r,which?'tar-a04-'+parts[i]+'-false':'tar-a04-package-path');}
   const x=await fixture(0).run(archive([header({name:paths[i]})])),y=await fixture(1).run(archive([header({name:paths[i]})]));compare(x,y);checkFailure(x,'tar-a04-package-path');checkFailure(y,'tar-a04-'+parts[i]+'-false');
  }
  const multi=blockFixture(1).run({values:[false,true,true,true]});equalEvents(multi.events,expectedEvents(0,false));metadata(multi,'tar-a04-prefix-false');
  const a=blockFixture(0).run({values:[false,false,false,false]});
  const skipped=blockFixture(1,{mutate:s=>s.replace("name.startsWith('package/')",'true')}).run({values:[false,false,false,false]});negative(()=>pair(a,skipped),51);
  const twice=blockFixture(1,{mutate:s=>s.replace("name.startsWith('package/')","(name.startsWith('package/'),name.startsWith('package/'))")}).run({values:[false,false,false,false]});negative(()=>pair(a,twice),50);
  const reordered=blockFixture(1,{mutate:s=>s.replace("name.startsWith('package/')","(!name.includes(':'),name.startsWith('package/'))")}).run({values:[false,false,false,false]});negative(()=>pair(a,reordered),50);
  const wrong=blockFixture(1,{mutate:s=>s.replace("dependencyA04Part='prefix'","dependencyA04Part='colon'")}).run({values:[false,false,false,false]});negative(()=>metadata(wrong,'tar-a04-prefix-false'),57);
  const replaced=blockFixture(1).run({values:[false,false,false,false],replaceError:true});negative(()=>pair(a,replaced),52);
  negative(()=>pair(a,{...a,observed:false}),52);
  const changed=blockFixture(1).run({values:[false,false,false,false],changedSemantics:true});negative(()=>pair(a,changed),53);
 });
 await group(3,async()=>{
  // Exact helpers extracted as source; native parameter semantics are executed only in a selected run.
  const failedOracle=read('original-oracle-node-controls.mjs');
  function oracleHelper(source){const begin=source.indexOf('function expectedEvents('),end=source.indexOf('\nfunction negative(',begin);need(begin>=0&&end>begin,74);return new Function(source.slice(begin,end)+';return expectedEvents;')();}
  const beforeOracle=oracleHelper(failedOracle),afterOracle=oracleHelper(read('node-controls.mjs'));
  const manualUndefined=[['get','startsWith'],['call','startsWith',true,['package/']],['assert',1,undefined]];
  const manualFalse=[['get','startsWith'],['call','startsWith',true,['package/']],['assert',1,false]];
  equalEvents(beforeOracle(0),manualFalse);equalEvents(afterOracle(0),manualFalse);
  equalEvents(beforeOracle(0,undefined),manualFalse);
  negative(()=>equalEvents(beforeOracle(0,undefined),manualUndefined),50);
  equalEvents(afterOracle(0,undefined),manualUndefined);
  equalEvents(beforeOracle(),beforeOracle(3,false));equalEvents(afterOracle(),afterOracle(3,false));
  equalEvents(beforeOracle(undefined),beforeOracle(3,false));equalEvents(afterOracle(undefined),afterOracle(3,false));
  const sharedObject={},sharedSymbol=Symbol('private-identity');
  for(const value of [false,true,0,-0,'',null,undefined,NaN,sharedObject,sharedSymbol]){
   const actual=afterOracle(0,value);need(actual.length===3&&actual[2][0]==='assert'&&actual[2][1]===1&&Object.is(actual[2][2],value),75);
   equalEvents(actual,[['get','startsWith'],['call','startsWith',true,['package/']],['assert',1,value]]);
   equalEvents(afterOracle(0,value,'ignored-extra'),actual);
  }
  // Exact A50 failures, with all preceding event/type/count checks held valid for value negatives.
  negative(()=>equalEvents(afterOracle(0,undefined),afterOracle(0,false)),50);
  negative(()=>equalEvents(afterOracle(0,false),afterOracle(0,undefined)),50);
  negative(()=>equalEvents(afterOracle(0,+0),afterOracle(0,-0)),50);
  negative(()=>equalEvents(afterOracle(0,sharedObject),afterOracle(0,{})),50);
  negative(()=>equalEvents(afterOracle(0,sharedSymbol),afterOracle(0,Symbol('private-identity'))),50);
  const baseline=afterOracle(0,false);
  negative(()=>equalEvents(baseline,baseline.slice(0,-1)),50);
  negative(()=>equalEvents(baseline,[baseline[1],baseline[0],baseline[2]]),50);
  negative(()=>equalEvents(baseline,[['get','includes'],baseline[1],baseline[2]]),50);
  negative(()=>equalEvents(baseline,[baseline[0],['call','startsWith',false,['package/']],baseline[2]]),50);
  negative(()=>equalEvents(baseline,[baseline[0],['call','startsWith',true,['wrong/']],baseline[2]]),50);
  for(const args of [[0],[0,undefined],[0,false],[0,undefined],[0]]){
   const expected=args.length<2?manualFalse:args[1]===undefined?manualUndefined:manualFalse;
   equalEvents(afterOracle(...args),expected);
  }

  const sites=[['startsWith:get','prefix'],['startsWith:call','prefix'],['includes:get','backslash'],['includes:call','backslash'],['split:get','traversal'],['split:call','traversal'],['segment-includes:get','traversal'],['segment-includes:call','traversal'],['includes-colon:get','colon'],['includes-colon:call','colon']];
  for(const [faultAt,part] of sites)for(const fault of [Object.assign(new Error('private-injected'),{code:'EACCES'}),'private-string',null]){
   const options={faultAt,fault};const a=blockFixture(0).run(options),b=blockFixture(1).run(options);pair(a,b);need(Object.is(a.value,fault)&&Object.is(b.value,fault),60);need(!b.events.some(e=>e[0]==='assert'),61);metadata(b,'tar-a04-'+part+'-eval');
  }
  const fault=new Error('private-assert');const a=blockFixture(0).run({assertFault:true,fault}),b=blockFixture(1).run({assertFault:true,fault});pair(a,b);need(Object.is(b.value,fault),60);equalEvents(b.events,expectedEvents(3,true));metadata(b,'tar-a04-colon-true');
  for(const which of [0,1]){const r=blockFixture(which).run({before:true});need(r.thrown&&r.value instanceof ReferenceError&&r.events.length===0,62);metadata(r,'tar-a04-package-path');}
  for(const value of [0,'',null,undefined,NaN]){const options={values:[value,false,false,false],assertFault:true,fault};const x=blockFixture(0).run(options),y=blockFixture(1).run(options);pair(x,y);equalEvents(y.events,expectedEvents(0,value));metadata(y,'tar-a04-prefix-other');}
  const hostile=new Proxy({}, {get(){throw new Error('private-coercion');}});const x=blockFixture(0).run({values:[hostile,false,false,false]}),y=blockFixture(1).run({values:[hostile,false,false,false]});pair(x,y);equalEvents(y.events,expectedEvents(3,true));
  for(let i=1;i<4;i++)for(const value of [0,'',null,undefined,{},'private-truthy']){const values=[true,false,false,false];values[i]=value;const r=blockFixture(1).run({values});if(value){metadata(r,'tar-a04-'+parts[i]+'-false');}else need(!r.thrown,63);need(!r.marks.some(m=>m[0].endsWith('-other')),64);}
  const normal=blockFixture(0).run({values:[0,false,false,false],assertFault:true,fault});const normalized=blockFixture(1,{mutate:s=>s.replace("name.startsWith('package/')","!!name.startsWith('package/')")}).run({values:[0,false,false,false],assertFault:true,fault});negative(()=>pair(normal,normalized),50);
 });
 await group(4,async()=>{
  const a=blockFixture(0),b=blockFixture(1);
  for(const options of [{values:[false,false,false,false]},{values:[true,false,false,true]},{before:true},{}]){const x=a.run(options),y=b.run(options);if(options.before){need(y.events.length===0,62);metadata(y,'tar-a04-package-path');}else pair(x,y);if(options.values)metadata(y,options.values[0]?'tar-a04-colon-false':'tar-a04-prefix-false');}
  for(const key of ['markerFault','includesFault','writerFault']){const options={values:[false,false,false,false],[key]:true};const x=a.run(options),y=b.run(options);pair(x,y);metadata(y,key==='writerFault'?'tar-a04-prefix-false':'tar-parse');need(y.marks.filter(m=>m[0].startsWith('tar-a04')).length===1,65);pair(a.run(),b.run());}
  const f0=fixture(0),f1=fixture(1);for(const name of ['outside/a','package/good','package/a:b','package/good2']){const gz=archive([header({name})]);const x=await f0.run(gz),y=await f1.run(gz);compare(x,y);if(y.thrown)checkFailure(y,name.startsWith('outside')?'tar-a04-prefix-false':'tar-a04-colon-false');}
  const multi=archive([header({name:'package/first'}),header({name:'package/second:bad'})]);const r=await f1.run(multi);checkFailure(r,'tar-a04-colon-false');
 });
 await group(5,async()=>{
  const stages=parts.flatMap(p=>['eval','false','true','other'].map(k=>'tar-a04-'+p+'-'+k));need(stages.length===16&&stages.every(s=>schema.stages.includes(s)),66);
  const r=blockFixture(1).run({values:[false,false,false,false]});metadata(r,'tar-a04-prefix-false');negative(()=>metadata({...r,stage:'tar-a04-colon-false'},'tar-a04-prefix-false'),57);
  // All sixteen relay labels tested by PS are schema admissibility only; negated operands never natively return other.
  const evil=new Proxy({}, {getOwnPropertyDescriptor(){throw new Error('private-descriptor');}});const f=blockFixture(1);const x=f.run({faultAt:'startsWith:get',fault:evil});need(Object.is(x.value,evil),60);need(metadata(x,'tar-a04-prefix-eval').code==='OTHER',67);
 });
 await group(6,async()=>{
  const f=blockFixture(1,{realWrite:true});const first=f.run({values:[false,false,false,false]});metadata(first,'tar-a04-prefix-false');const filename=path.join(root,'dependency-failure.json'),bytes=fs.readFileSync(filename);need(bytes.equals(Buffer.from(first.writes[0].b)),68);
  const second=f.run({values:[true,false,false,true]});metadata(second,'tar-a04-colon-false');need(fs.readFileSync(filename).equals(bytes)&&Object.is(second.value,second.boundary),69);fs.unlinkSync(filename);knownFiles.delete(filename);
  const fault=f.run({values:[false,false,false,false],writerFault:true});metadata(fault,'tar-a04-prefix-false');need(Object.is(fault.value,fault.boundary)&&!fs.existsSync(filename),70);
 });
}

let outcome='FAILED';try{await main();outcome='PASS';}catch{}finally{try{if(created){for(const f of fs.readdirSync(root)){const full=path.join(root,f),st=fs.lstatSync(full);need(knownFiles.has(full)&&st.isFile()&&!st.isSymbolicLink(),47);}for(const f of knownFiles)fs.unlinkSync(f);fs.rmdirSync(root);cleaned=!fs.existsSync(root);}}catch{outcome='FAILED';}}
if(!created||!cleaned||rows.length!==6)outcome='FAILED';
process.stdout.write(JSON.stringify({schema:'dependency-js-controls-v1',outcome,caseIndex,assertion,created,cleaned,results:outcome==='PASS'?rows:[],completed:rows.map(x=>x.name),setupInvocations:0,packageEntryExecutions:0,privateLogReads:0}));if(outcome!=='PASS')process.exitCode=1;
