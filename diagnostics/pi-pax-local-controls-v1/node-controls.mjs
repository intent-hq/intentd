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
async function main(){
 need(process.platform==='win32'&&process.version==='v24.21.0'&&path.isAbsolute(root)&&!fs.existsSync(root),18);fs.mkdirSync(root);created=true;
 await group(1,async()=>{
  need(sha(old)===delta.before&&sha(marked)===delta.after,19);let reversed=marked;
  for(const c of [...delta.changes].reverse()){need(reversed.split(c.new).length===2,20);reversed=reversed.replace(c.new,c.old);}need(reversed===old,20);
  need(read('setup-only.ps1')===read('original-setup-only.ps1')&&sd.changes.length===0,21);
  need(originals[0].helper===originals[1].helper&&originals[0].catch===originals[1].catch,22);
  const records=[['path','package/é-file'],['size','4'],['mtime','-1.25'],['linkpath','']];
  const gz=paxArchive(records),r=await fixture(1).run(gz);expectFiles(r,gz,[['é-file',Buffer.from('body')]]);need(r.writes.length===0,68);
  const decoded=recordModel(bodyOf(records));need(!decoded.thrown&&decoded.result.path==='package/é-file'&&decoded.result.size===4&&decoded.result.mtime==='-1.25'&&decoded.result.linkpath==='',69);
  for(const key of ['unknown','uid','hdrcharset','GNU.sparse.map'])recordRefusal(bodyOf([[key,'1']]),'pax record key');
  recordRefusal(bodyOf([['size','0'],['size','1']]),'pax record key');
  for(const b of [Buffer.from('01 x=a\n'),Buffer.from('0 x=a\n'),Buffer.from('-1 x=a\n')])recordRefusal(b,'pax record length');
  for(const b of [Buffer.from('99 size=0\n'),Buffer.from('9 size=0X')])recordRefusal(b,'pax record extent');
  recordRefusal(Buffer.from('7 xxxx\n'),'pax record key');recordRefusal(empty,'pax records');
  for(const b of [Buffer.from('a\nb'),Buffer.from('a\rb'),Buffer.from([0])])recordRefusal(bodyOf([['path',b]]),'pax record value');
  recordRefusal(bodyOf([['path',Buffer.from([0xc0,0xaf])]]),'pax path encoding');recordRefusal(bodyOf([['path','\uFEFFpackage/a']]),'pax path encoding');
  recordRefusal(Buffer.concat([bodyOf([['size','0']]),Buffer.from('x')]),'pax record length');
  for(const v of ['', '-1','+1','01','1e1','0.5','33554433','9007199254740992'])recordRefusal(bodyOf([['size',v]]),'pax size');
  for(const v of ['', 'NaN','1e2','+1','01','8640000000001','1'.repeat(65)])recordRefusal(bodyOf([['mtime',v]]),'pax mtime');
  recordRefusal(bodyOf([['linkpath','target']]),'pax linkpath');recordRefusal(bodyOf([['path','']]),'pax path bytes');
  const noLf=archive([pax([], {data:Buffer.from('9 size=0X')}),header({data:empty})]);paxFailure(await fixture(1).run(noLf),'pax record extent');
  const skipLf=await fixture(1,{mutate:s=>mutateOnce(s,'body[end-1]===10','true')}).run(noLf);reject(()=>paxFailure(skipLf,'pax record extent'),9);
  const invalidUtf=paxArchive([['path',Buffer.concat([Buffer.from('package/'),Buffer.from([0xc0,0xaf])])]]);paxFailure(await fixture(1).run(invalidUtf),'pax path encoding');
  const skipUtf=await fixture(1,{mutate:s=>mutateOnce(s,"Buffer.from(value,'utf8').equals(bytes)",'true')}).run(invalidUtf);reject(()=>paxFailure(skipUtf,'pax path encoding'),9);
  paxFailure(await fixture(1).run(archive([pax([]),header()])),'pax metadata cap');
  const malformed=paxArchive([['unknown','1']]);paxFailure(await fixture(1).run(malformed),'pax record key');
  const noKey=await fixture(1,{mutate:s=>mutateOnce(s,"['path','size','mtime','linkpath'].includes(key)",'true')}).run(paxArchive([['unknown','']]));reject(()=>paxFailure(noKey,'pax record key'),9);
 });
 await group(2,async()=>{
  for(const name of ['outside/a','/package/a','package/../x','package/a\\b','package/a:b'])paxFailure(await fixture(1).run(paxArchive([['path',name]])),'pax effective path');
  for(const name of ['package/','package/a/','package/CON','package/a.','package/a '])paxFailure(await fixture(1).run(paxArchive([['path',name]])),'pax effective Windows name');
  for(const [name,stage]of [['outside/a','tar-a04-form-file'],['package/../x','tar-a04-traversal-false'],['package/a\\b','tar-a04-backslash-false'],['package/a:b','tar-a04-colon-false'],['package/CON','tar-a05-windows-name']])nativeFailure(await fixture(1).run(paxArchive([['path','package/safe']],{name})),stage);
  paxFailure(await fixture(1).run(paxArchive([['path','package/safe']],{name:'package/raw/'})),'pax regular target');
  paxFailure(await fixture(1).run(paxArchive([['path','package/safe']],{linkname:'target'})),'pax regular target');
  const unsafe=paxArchive([['path','outside/a']]);const skipPath=await fixture(1,{mutate:s=>mutateOnce(s,"value.startsWith('package/')",'true')}).run(unsafe);reject(()=>paxFailure(skipPath,'pax effective path'),9);
  const unsafeRaw=paxArchive([['path','package/safe']],{name:'outside/a'});const skipRaw=await fixture(1,{mutate:s=>mutateOnce(s,"name.startsWith('package/')",'true')}).run(unsafeRaw);reject(()=>nativeFailure(skipRaw,'tar-a04-form-file'),9);
  const data=Buffer.from('body'),gz=paxArchive([['path','package/new']]),r=await fixture(1).run(gz);expectFiles(r,gz,[['new',data]]);
  const ignore=await fixture(1,{mutate:s=>mutateOnce(s,"if(Object.hasOwn(pendingPax,'path'))rel=paxPath(pendingPax.path);",'')}).run(gz);reject(()=>expectFiles(ignore,gz,[['new',data]]),66);
  const duplicate=paxArchive([['path','package/existing']],{},[header({name:'package/existing'})]);nativeFailure(await fixture(1).run(duplicate),'tar-a06-file-unique');
  for(const name of ['PaxHeader/','PaxHeader/a/b','PaxHeader/..','PaxHeader/a\\b','PaxHeader/a:b','PaxHeader/CON','PaxHeader/a.','Other/file'])paxFailure(await fixture(1).run(archive([pax([['size','4']],{name}),header()])),'pax carrier path');
  for(const opts of [{prefix:'bad'},{magic:'bad'},{version:'01'},{linkname:'bad'}])paxFailure(await fixture(1).run(archive([pax([['size','4']],opts),header()])),'pax carrier header');
  const badPadding=pax();badPadding[badPadding.length-1]=1;paxFailure(await fixture(1).run(archive([badPadding,header()])),'pax metadata padding');
  const unpadded=archive([badPadding,header()]);const skipPadding=await fixture(1,{mutate:s=>mutateOnce(s,'tar.subarray(offset+512+length,next).every(x=>x===0)','true')}).run(unpadded);reject(()=>paxFailure(skipPadding,'pax metadata padding'),9);
 });
 await group(3,async()=>{
  for(const [raw,n]of [[0,513],[513,1],[4,0],[0,1],[1,512]]){
   const data=Buffer.alloc(n,97),size=raw.toString(8).padStart(11,'0')+'\0',tail=header({name:'package/after',data:Buffer.from('end')});
   const gz=paxArchive([['path','package/changed'],['size',String(n)]],{size,data},[tail]);expectFiles(await fixture(1).run(gz),gz,[['changed',data],['after',Buffer.from('end')]]);
  }
  const data=Buffer.alloc(513,97),gz=paxArchive([['size','513']],{size:'00000000000\0',data},[header({name:'package/after'})]);
  const ignored=await fixture(1,{mutate:s=>mutateOnce(s,"if(Object.hasOwn(pendingPax,'size'))contentLength=pendingPax.size;",'')}).run(gz);reject(()=>bindArchive(ignored,gz),32);
  const short=gzipSync(Buffer.concat([pax([['size','513']]),header({data:empty}).subarray(0,512),Buffer.alloc(1)]));paxFailure(await fixture(1).run(short),'pax carrier header');
  const extent=archive([pax([['size','8192']]),header({data:empty})]);paxFailure(await fixture(1).run(extent),'pax effective extent');
  // Raw size refusal remains mandatory even if effective size is smaller.
  nativeFailure(await fixture(1).run(paxArchive([['size','0']],{size:'00200000001\0',data:empty})),'tar-a03-size-extent');
  const missingPad=gzipSync(Buffer.concat([pax([['size','1']]),header({data:empty}).subarray(0,512),Buffer.from('a')]));paxFailure(await fixture(1).run(missingPad),'pax carrier header');
  const noApply=await fixture(1,{mutate:s=>mutateOnce(s,'bytes:contentLength','bytes:length')}).run(gz);reject(()=>expectFiles(noApply,gz,[['raw',data],['after',Buffer.from('fixture')]]),66);
 });
 await group(4,async()=>{
  for(const entry of [rootHeader(),header({type:'5',data:empty}),pax(),header({type:'g',data:empty}),header({type:'L',data:empty}),header({type:'K',data:empty}),header({type:'2',data:empty})])paxFailure(await fixture(1).run(archive([pax(),entry])),'pax regular target');
  for(const type of ['g','L','K','X','S','2','1']){const gz=archive([header({name:'package/meta',type,data:empty}),header()]);compare(await fixture(0).run(gz),await fixture(1).run(gz));nativeFailure(await fixture(1).run(gz),'tar-a08-entry-type');}
  for(const tail of [Buffer.alloc(1024),empty])paxFailure(await fixture(1).run(archive([pax()],tail)),'pax orphan');
  const unfinished=gzipSync(Buffer.concat([pax(),header().subarray(0,511)]));paxFailure(await fixture(1).run(unfinished),'pax carrier header');
  const data=Buffer.from('body'),gz=paxArchive([['path','package/once']],{},[header({name:'package/raw'})]);expectFiles(await fixture(1).run(gz),gz,[['once',data],['raw',Buffer.from('fixture')]]);
  const stale=await fixture(1,{mutate:s=>mutateOnce(s,"assert(count+1<=100000,'pax file count');pendingPax=null;","assert(count+1<=100000,'pax file count');")}).run(gz);reject(()=>bindArchive(stale,gz),32);
  const cached=fixture(1),bad=archive([pax()]);const one=await cached.run(bad,{url:'https://registry.npmjs.org/fixture/same.tgz'}),two=await cached.run(bad,{url:'https://registry.npmjs.org/fixture/same.tgz'});paxFailure(one,'pax orphan');paxFailure(two,'pax orphan');need(ioLedger(one).length===3&&ioLedger(two).length===3,70);
 });
 await group(5,async()=>{
  for(const entries of [[header()],[rootHeader(),header()],[header(),rootHeader()],[rootHeader(),rootHeader(),header()],[header({name:'package/',type:'5',data:empty}),header()]]){const gz=archive(entries);compare(await fixture(0).run(gz),await fixture(1).run(gz));}
  const plain=archive([header({name:'package/effective',data:Buffer.from('body')})]),extended=paxArchive([['path','package/effective']]);const a=await fixture(0).run(plain),b=await fixture(1).run(extended);sameFiles(a,b);bindArchive(a,plain);bindArchive(b,extended);need(a.result.tarSha!==b.result.tarSha&&a.result.integrity!==b.result.integrity,71);
  for(const entry of [header({checksumBad:true}),header({size:'00000000008\0'}),header({name:'package/../x'}),header({name:'package/CON'}),header({name:'package',type:'5',data:Buffer.from('x')})]){const gz=archive([entry,header()]);compare(await fixture(0).run(gz),await fixture(1).run(gz));}
  const corrupt=pax([['size','4']],{checksumBad:true});nativeFailure(await fixture(1).run(archive([corrupt,header()])),'tar-a01-checksum');
  const gz=paxArchive([['path','package/file']]),mismatch=await fixture(1).run(gz,{sri:'sha512-'+Buffer.alloc(64).toString('base64')});nativeFailure(mismatch,'tar-integrity');need(!mismatch.ledger.some(e=>e[0]==='inflate'),58);
  const f=fixture(1),x=await f.run(gz,{url:'https://registry.npmjs.org/fixture/cache.tgz'}),y=await f.run(gz,{url:'https://registry.npmjs.org/fixture/cache.tgz'});bindArchive(x,gz);bindArchive(y,gz);sameFiles(x,y);need(ioLedger(x).length===3&&ioLedger(y).length===0,57);
  // All dependency checks after tarFor are byte exact except the truthful result description.
  const before=old.slice(old.indexOf('\nfunction inventory(')),after=marked.slice(marked.indexOf('\nfunction inventory('));const description=delta.changes.at(-1);need(after.replace(description.new,description.old)===before,72);
 });
 await group(6,async()=>{
  // Exact source arithmetic models supplement tiny native Buffer fixtures; no pinned-reader import or native-reader differential credit.
  for(const [length,bytes,headers,want]of [[1,0,0,true],[1048576,7340032,1023,true],[0,0,0,false],[1048577,0,0,false],[1,8388608,0,false],[1,0,1024,false]])need(capModel(length,bytes,headers)===want,73);
  const maxPath='package/'+('a'.repeat(4088));need(Buffer.byteLength(maxPath)===4096&&!recordModel(bodyOf([['path',maxPath]])).thrown,74);recordRefusal(bodyOf([['path',maxPath+'a']]),'pax path bytes');
  for(const n of [9,10,99,100,999]){const value='package/'+('a'.repeat(n)),r=recordModel(bodyOf([['path',value]]));need(!r.thrown&&r.result.path===value,75);}
  for(const injected of [new Error('private-original'),'private-string',null]){const r=recordModel(bodyOf([['size','0']]),{fault:true,injected});need(r.thrown&&r.seen&&Object.is(r.value,injected)&&Object.is(r.boundary,injected),76);}
  const bad=paxArchive([['unknown','1']]);
  for(const injected of [new Error('private-original'),'private-string',null]){const x=await fixture(1,{assertFaultMessage:'pax record key',injected}).run(bad);need(x.thrown&&x.observed&&Object.is(x.value,injected)&&Object.is(x.boundary,injected),59);checkFailure(x,'tar-parse');}
  const real=fixture(1,{realWrite:true});const first=await real.run(bad);paxFailure(first,'pax record key');const filename=path.join(root,'dependency-failure.json'),bytes=fs.readFileSync(filename);need(bytes.equals(Buffer.from(first.writes[0].b)),60);
  const second=await real.run(archive([header({name:'outside/file'})]));nativeFailure(second,'tar-a04-form-file');need(fs.readFileSync(filename).equals(bytes),61);fs.unlinkSync(filename);knownFiles.delete(filename);
  const writer=await real.run(bad,{writeFault:true});paxFailure(writer,'pax record key');need(!fs.existsSync(filename),62);
  const gz=paxArchive([['size','4']]);const success=await real.run(gz);bindArchive(success,gz);need(success.writes.length===0&&!fs.existsSync(filename),63);
  const altered=await fixture(1,{replaceError:true}).run(bad);reject(()=>paxFailure(altered,'pax record key'),9);
  reject(()=>expectFiles({...success,result:{...success.result,files:new Map()}},gz,[['raw',Buffer.from('body')]]),66);
  need(originals[0].helper===originals[1].helper&&originals[0].catch===originals[1].catch,77);
 });
}
let outcome='FAILED';try{await main();outcome='PASS';}catch{}finally{try{if(created){for(const f of fs.readdirSync(root)){const full=path.join(root,f),st=fs.lstatSync(full);need(knownFiles.has(full)&&st.isFile()&&!st.isSymbolicLink(),47);}for(const f of knownFiles)fs.unlinkSync(f);fs.rmdirSync(root);cleaned=!fs.existsSync(root);}}catch{outcome='FAILED';}}
if(!created||!cleaned||rows.length!==6)outcome='FAILED';
process.stdout.write(JSON.stringify({schema:'dependency-js-controls-v1',outcome,caseIndex,assertion,created,cleaned,results:outcome==='PASS'?rows:[],completed:rows.map(x=>x.name),setupInvocations:0,packageEntryExecutions:0,privateLogReads:0}));if(outcome!=='PASS')process.exitCode=1;
