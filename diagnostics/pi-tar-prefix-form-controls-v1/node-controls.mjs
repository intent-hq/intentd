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
// Exact complete original/candidate A04 blocks. Only the synchronous fixture observes String.prototype.
function a04(source){const begin=source.indexOf("{let dependencyA04Part='unavailable'");const end=source.indexOf('}}const rel=name.slice(8);',begin);need(begin>=0&&end>begin&&source.indexOf("{let dependencyA04Part='unavailable'",begin+1)<0,24);return source.slice(begin,end+2);}
const blocks=[a04(old),a04(marked)];
function blockFixture(which,{mutate=null,realWrite=false}={}){
 const ex=originals[which],events=[],marks=[],writes=[],state={options:{},observed:false,boundary:undefined};
 const remember=e=>{state.observed=true;state.boundary=e;};
 const markObserver=args=>{marks.push(args);if(state.options.markerFault&&args[0].startsWith('tar-a04'))throw state.options.fault;};
 function write(p,b,o){need(p===path.join(root,'dependency-failure.json'),2);writes.push({b,o});if(state.options.writerFault)throw state.options.fault;if(realWrite){fs.writeFileSync(p,b,o);knownFiles.add(p);}}
 let body=blocks[which];if(mutate)body=mutate(body);
 const loader=new Function('writeFileSync','join','root','markObserver',"'use strict';\n"+ex.helper+"\nconst originalMark=MarkDependencyFailureStage;MarkDependencyFailureStage=(...args)=>{markObserver(args);return originalMark(...args);};dependencyFailureRoot=root;\nreturn {invoke(name,type,assert){"+body+"},before(name,type){"+body+";let assert;},state(){return dependencyFailureStage;},write:WriteDependencyFailureMetadata,begin(){MarkDependencyFailureStage('tar-parse');},fault(v){if(v)DEPENDENCY_FAILURE_STAGES.includes=function(s){if(s.startsWith('tar-a04'))throw new Error('private-includes');return Array.prototype.includes.call(this,s);};else delete DEPENDENCY_FAILURE_STAGES.includes;}};");
 const api=loader(write,path.join,root,markObserver);
 const outer=new Function('invoke','name','type','assert','WriteDependencyFailureMetadata',"'use strict';try {return invoke(name,type,assert);\n"+ex.catch);
 function run(options={}){
  const o=state.options={name:'outside/file',type:'0',fault:new Error('private-fault'),...options};events.length=marks.length=writes.length=0;state.observed=false;state.boundary=undefined;api.begin();api.fault(!!o.includesFault);
  const saved=Object.getOwnPropertyDescriptor(String.prototype,'startsWith');need(saved&&typeof saved.value==='function'&&saved.configurable,71);let gets=0,restored=false;
  const get=function(){if(this!==o.name)return saved.value;gets++;events.push(['get','startsWith']);if((o.diagFault==='get'&&gets===2)||(o.prefixFault==='get'&&gets===1)){if(gets===1)remember(o.fault);throw o.fault;}return function(...args){events.push(['call','startsWith',this===o.name,args]);const diagnostic=args[0]==='PaxHeader/';if((diagnostic&&o.diagFault==='call')||(!diagnostic&&o.prefixFault==='call')){if(!diagnostic)remember(o.fault);throw o.fault;}if(diagnostic&&Object.hasOwn(o,'diagValue'))return o.diagValue;if(!diagnostic&&Object.hasOwn(o,'prefixValue'))return o.prefixValue;return Reflect.apply(saved.value,this,args);};};
  const seenAssert=(...args)=>{events.push(['assert',args.length,args[0]]);try{if(o.assertFault)throw o.fault;return assert(...args);}catch(e){remember(e);if(o.changedSemantics&&e&&typeof e==='object')e.code='OTHER';if(o.replaceError)throw new Error('private-replacement');throw e;}};
  let thrown=false,value,result;
  try{Object.defineProperty(String.prototype,'startsWith',{get,configurable:true,enumerable:saved.enumerable});result=outer(o.before?api.before:api.invoke,o.name,o.type,seenAssert,api.write);}catch(e){thrown=true;value=e;}finally{Object.defineProperty(String.prototype,'startsWith',saved);api.fault(false);const after=Object.getOwnPropertyDescriptor(String.prototype,'startsWith');restored=after.value===saved.value&&after.get===saved.get&&after.set===saved.set&&after.configurable===saved.configurable&&after.enumerable===saved.enumerable&&after.writable===saved.writable;}
  need(restored,72);return {thrown,value,result,events:[...events],marks:structuredClone(marks),writes:[...writes],observed:state.observed,boundary:state.boundary,stage:api.state(),restored};
 }
 return {run};
}
function errorSemantics(e){return {name:e?.name,code:e?.code,operator:e?.operator,actual:e?.actual,expected:e?.expected,generatedMessage:e?.generatedMessage};}
function equalEvents(a,b){need(a.length===b.length,50);for(let i=0;i<a.length;i++){need(a[i][0]===b[i][0]&&a[i][1]===b[i][1],50);if(a[i][0]==='assert')need(Object.is(a[i][2],b[i][2]),50);else need(canonical(a[i])===canonical(b[i]),50);}}
function pair(a,b){need(a.thrown===b.thrown,51);need(a.restored&&b.restored,72);if(a.thrown){need(a.observed&&b.observed&&Object.is(a.value,a.boundary)&&Object.is(b.value,b.boundary),52);need(canonical(errorSemantics(a.value))===canonical(errorSemantics(b.value)),53);}else need(a.result===undefined&&b.result===undefined&&a.writes.length===0&&b.writes.length===0&&canonical(a.marks)===canonical(b.marks),54);}
function metadata(r,stage){need(r.thrown&&r.writes.length===1&&r.writes[0].o.flag==='wx'&&Buffer.byteLength(r.writes[0].b)<=512,55);const v=JSON.parse(r.writes[0].b);need(Object.keys(v).sort().join('|')==='behavioralInvocations|code|group|outcome|schema|stage'&&v.schema==='dependency-failure-v1'&&v.outcome==='FAILED'&&v.behavioralInvocations===0,56);need(v.stage===stage&&r.stage===stage,57);need(!r.writes[0].b.includes('private-')&&Buffer.byteLength('DEPENDENCY_FAILURE_V1 '+r.writes[0].b)<=768,58);return v;}
function expectedEvents(extra=false,value){if(arguments.length<2)value=false;return [['get','startsWith'],['call','startsWith',true,['package/']],['assert',1,value],...(extra?[['get','startsWith'],['call','startsWith',true,['PaxHeader/']]]:[])];}
function nativeError(r){need(r.thrown&&r.value instanceof assert.AssertionError&&r.value.code==='ERR_ASSERTION'&&r.value.operator==='=='&&r.value.actual===false&&r.value.expected===true&&r.value.generatedMessage===true,59);}
const cases=[
 ['package','5','root-dir',false],['package','x','root-other',false],
 ['./package','5','dot-dir',false],['./package/','g','dot-other',false],
 ['PaxHeader/file','x','pax-x',true],['PaxHeader/file','g','pax-g',true],['outside/file','x','pax-other',true],
 ['outside/file','L','gnu-L',false],['outside/file','K','gnu-K',false],['outside/','5','dir',false],['outside/file','0','file',false],['outside/file','2','other',false]
];
async function main(){
 need(process.platform==='win32'&&process.version==='v24.21.0'&&path.isAbsolute(root)&&!fs.existsSync(root),18);fs.mkdirSync(root);created=true;
 await group(1,async()=>{
  need(sha(old)===delta.original&&sha(marked)===delta.candidate&&delta.changes.length===2,19);let rev=marked;for(const c of [...delta.changes].reverse()){need(rev.split(c.new).length===2,20);rev=rev.replace(c.new,c.old);}need(rev===old,21);
  let ps=read('setup-only.ps1');for(const c of [...sd.changes].reverse()){need(ps.split(c.new).length===2,22);ps=ps.replace(c.new,c.old);}need(ps===read('original-setup-only.ps1')&&sd.changes.length===1,23);need(originals[0].catch===originals[1].catch,25);
  need(blocks[0].slice(0,blocks[0].indexOf('} catch(dependencyTarError)'))===blocks[1].slice(0,blocks[1].indexOf('} catch(dependencyTarError)')),24);
  for(const name of ['package/file','package/dir/file','package/a..b','package/']){const x=blockFixture(0).run({name,type:'5'}),y=blockFixture(1).run({name,type:'5'});pair(x,y);equalEvents(x.events,y.events);equalEvents(y.events,expectedEvents(false,true));}
  const x=await fixture(0).run(),y=await fixture(1).run();compare(x,y);need(!y.thrown&&y.result.files.size===1&&boundMarks(y).length===0,26);
  const a=blockFixture(1).run();reject(()=>equalEvents(a.events,[...a.events,['extra']]),50);
 });
 await group(2,async()=>{
  for(const [name,type,kind,extra]of cases){
   const x=blockFixture(0).run({name,type}),y=blockFixture(1).run({name,type});pair(x,y);equalEvents(x.events,expectedEvents());equalEvents(y.events,expectedEvents(extra));nativeError(y);metadata(x,'tar-a04-prefix-false');metadata(y,'tar-a04-form-'+kind);
   const gz=archive([header({name,type,data:Buffer.alloc(0)})]);const a=await fixture(0).run(gz),b=await fixture(1).run(gz);compare(a,b);checkFailure(a,'tar-a04-prefix-false');checkFailure(b,'tar-a04-form-'+kind);
  }
  for(const [name,type,kind,extra]of [['package','g','root-other',false],['./package/','5','dot-dir',false],['PaxHeader/file','0','file',false],['PaxHeader/','x','pax-x',true],['not-pax','g','pax-other',true],['','5','dir',false]]){const y=blockFixture(1).run({name,type});metadata(y,'tar-a04-form-'+kind);equalEvents(y.events,expectedEvents(extra));}
  // NUL header type normalizes to 0 in exact tarFor; this is not raw type disclosure.
  const nul=await fixture(1).run(archive([header({name:'outside/file',type:'\0'})]));checkFailure(nul,'tar-a04-form-file');
  const r=blockFixture(1).run({name:'package',type:'x'});reject(()=>metadata(r,'tar-a04-form-pax-x'),57);
 });
 await group(3,async()=>{
  const opts={name:'PaxHeader/file',type:'x'};
  for(const diagFault of ['get','call'])for(const fault of [new Error('private-diag'),'private-string',null]){const a=blockFixture(0).run(opts),b=blockFixture(1).run({...opts,diagFault,fault});pair(a,b);metadata(b,'tar-a04-prefix-false');equalEvents(b.events,diagFault==='get'?expectedEvents().concat([['get','startsWith']]):expectedEvents(true));need(b.marks.filter(m=>m[0].startsWith('tar-a04')).length===1,65);}
  // The new diagnostic ternary uses truthiness; original predicate false gate stays strict.
  const hostile=new Proxy({}, {get(){throw new Error('private-no-coercion');}});
  for(const value of [true,false,0,-0,'',null,undefined,NaN,1,'private-truthy',hostile]){const b=blockFixture(1).run({...opts,diagValue:value});metadata(b,value?'tar-a04-form-pax-x':'tar-a04-form-pax-other');equalEvents(b.events,expectedEvents(true));nativeError(b);}
  const a=blockFixture(1).run(opts);
  const doubled=blockFixture(1,{mutate:s=>s.replace("name.startsWith('PaxHeader/')","(name.startsWith('PaxHeader/'),name.startsWith('PaxHeader/'))")}).run(opts);reject(()=>equalEvents(doubled.events,expectedEvents(true)),50);
  const skipped=blockFixture(1,{mutate:s=>s.replace("name.startsWith('PaxHeader/')",'true')}).run(opts);reject(()=>equalEvents(skipped.events,expectedEvents(true)),50);
  const wrongArg=blockFixture(1,{mutate:s=>s.replace("name.startsWith('PaxHeader/')","name.startsWith('wrong/')")}).run(opts);reject(()=>equalEvents(wrongArg.events,expectedEvents(true)),50);
  const wrongReceiver=blockFixture(1,{mutate:s=>s.replace("name.startsWith('PaxHeader/')","name.startsWith.call('other','PaxHeader/')")}).run(opts);reject(()=>equalEvents(wrongReceiver.events,expectedEvents(true)),50);
  const reordered=blockFixture(1,{mutate:s=>s.replace("name==='package'","(name.startsWith('PaxHeader/'),name==='package')")}).run(opts);reject(()=>equalEvents(reordered.events,expectedEvents(true)),50);
  const wrong=blockFixture(1,{mutate:s=>s.replace("'tar-a04-form-pax-x'","'tar-a04-form-pax-g'")}).run(opts);reject(()=>metadata(wrong,'tar-a04-form-pax-x'),57);
  reject(()=>pair(a,{...a,observed:false}),52);const replaced=blockFixture(1).run({...opts,replaceError:true});reject(()=>pair(a,replaced),52);
  const changed=blockFixture(1).run({...opts,changedSemantics:true});reject(()=>pair(a,changed),53);
 });
 await group(4,async()=>{
  const f=blockFixture(1);
  for(const [name,type,stage,extra]of [['package','5','tar-a04-form-root-dir',false],['PaxHeader/a','g','tar-a04-form-pax-g',true],['outside','0','tar-a04-form-file',false],['package/a:b','0','tar-a04-colon-false',false],['package/good','0',null,false]]){const r=f.run({name,type});if(stage)metadata(r,stage);else need(!r.thrown&&r.writes.length===0,63);equalEvents(r.events,expectedEvents(extra,stage===null));}
  for(const type of [undefined,null,'','xx',5,new String('x')]){const r=f.run({name:'PaxHeader/file',type});metadata(r,'tar-a04-prefix-false');equalEvents(r.events,expectedEvents());}
  const objectName={startsWith(){return false;}};metadata(f.run({name:objectName,type:'x'}),'tar-a04-prefix-false');
  const fault=new Error('private-assert');for(const prefixValue of [undefined,0,'',null,NaN]){const r=f.run({name:'PaxHeader/file',type:'x',prefixValue,assertFault:true,fault});metadata(r,'tar-a04-prefix-other');equalEvents(r.events,expectedEvents(false,prefixValue));need(Object.is(r.value,fault),60);}
  for(const prefixFault of ['get','call']){const r=f.run({name:'PaxHeader/file',type:'x',prefixFault,fault});metadata(r,'tar-a04-prefix-eval');need(Object.is(r.value,fault)&&!r.events.some(e=>e[0]==='assert'),60);}
  const before=f.run({name:'PaxHeader/file',type:'x',before:true});need(before.value instanceof ReferenceError&&before.events.length===0,62);metadata(before,'tar-a04-package-path');
  const after=f.run({name:'package/good',type:'x',assertFault:true,fault});metadata(after,'tar-a04-colon-true');need(Object.is(after.value,fault),60);equalEvents(after.events,expectedEvents(false,true));
  for(const key of ['markerFault','includesFault','writerFault']){const x=f.run({name:'PaxHeader/a',type:'x',[key]:true});metadata(x,key==='writerFault'?'tar-a04-form-pax-x':'tar-parse');need(x.observed&&Object.is(x.value,x.boundary),52);need(x.marks.filter(m=>m[0].startsWith('tar-a04')).length===1,65);need(!f.run({name:'package/good'}).thrown,63);}
 });
 await group(5,async()=>{
  const stages=cases.map(x=>'tar-a04-form-'+x[2]);need(stages.length===12&&new Set(stages).size===12&&stages.every(s=>schema.stages.includes(s)),66);
  const r=blockFixture(1).run();metadata(r,'tar-a04-form-file');reject(()=>metadata({...r,stage:'tar-a04-form-dir'},'tar-a04-form-file'),57);
  const evil=new Proxy({}, {getOwnPropertyDescriptor(){throw new Error('private-descriptor');}});const e=blockFixture(1).run({name:'outside',type:'0',assertFault:true,fault:evil});need(Object.is(e.value,evil),60);need(metadata(e,'tar-a04-form-file').code==='OTHER',67);
  // Categories are admissibility-only; neither a matching root nor metadata spelling is accepted.
  for(const [name,type]of [['package','5'],['PaxHeader/a','x'],['package/a','x'],['package/a','L'],['package/a','2']]){const x=await fixture(0).run(archive([header({name,type})])),y=await fixture(1).run(archive([header({name,type})]));compare(x,y);need(y.thrown,51);}
 });
 await group(6,async()=>{
  const f=blockFixture(1,{realWrite:true});const first=f.run({name:'package',type:'5'});metadata(first,'tar-a04-form-root-dir');const filename=path.join(root,'dependency-failure.json'),bytes=fs.readFileSync(filename);need(bytes.equals(Buffer.from(first.writes[0].b)),68);
  const second=f.run({name:'PaxHeader/a',type:'x'});metadata(second,'tar-a04-form-pax-x');need(fs.readFileSync(filename).equals(bytes)&&Object.is(second.value,second.boundary),69);fs.unlinkSync(filename);knownFiles.delete(filename);
  const fault=f.run({name:'outside',type:'0',writerFault:true});metadata(fault,'tar-a04-form-file');need(Object.is(fault.value,fault.boundary)&&!fs.existsSync(filename),70);
  for(const [name,stage]of [['package/a\\b','tar-a04-backslash-false'],['package/a/../b','tar-a04-traversal-false'],['package/a:b','tar-a04-colon-false'],['outside/../a:b','tar-a04-form-file']]){const a=await fixture(0).run(archive([header({name})])),b=await fixture(1).run(archive([header({name})]));compare(a,b);checkFailure(b,stage);}
  const normal=blockFixture(0).run({name:'outside'});const skip=blockFixture(1,{mutate:s=>s.replace("name.startsWith('package/')",'true')}).run({name:'outside'});reject(()=>pair(normal,skip),51);
  const normalized=blockFixture(1,{mutate:s=>s.replace("name.startsWith('package/')","(name==='package'||name.startsWith('package/'))")}).run({name:'package',type:'5'});reject(()=>pair(blockFixture(0).run({name:'package',type:'5'}),normalized),51);
 });
}

let outcome='FAILED';try{await main();outcome='PASS';}catch{}finally{try{if(created){for(const f of fs.readdirSync(root)){const full=path.join(root,f),st=fs.lstatSync(full);need(knownFiles.has(full)&&st.isFile()&&!st.isSymbolicLink(),47);}for(const f of knownFiles)fs.unlinkSync(f);fs.rmdirSync(root);cleaned=!fs.existsSync(root);}}catch{outcome='FAILED';}}
if(!created||!cleaned||rows.length!==6)outcome='FAILED';
process.stdout.write(JSON.stringify({schema:'dependency-js-controls-v1',outcome,caseIndex,assertion,created,cleaned,results:outcome==='PASS'?rows:[],completed:rows.map(x=>x.name),setupInvocations:0,packageEntryExecutions:0,privateLogReads:0}));if(outcome!=='PASS')process.exitCode=1;
