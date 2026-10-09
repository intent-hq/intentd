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
function header({name='package/file',type='0',data=Buffer.from('fixture'),size=null,prefix='',magic='ustar\0',version='00',checksumBad=false}={}){
 const b=Buffer.alloc(512);b.write(name,0,100,'utf8');b.write('0000644\0',100,8,'ascii');
 b.write(size??(data.length.toString(8).padStart(11,'0')+'\0'),124,12,'ascii');b[156]=type.charCodeAt(0);
 b.write(prefix,345,155,'utf8');b.write(magic,257,6,'ascii');b.write(version,263,2,'ascii');b.fill(32,148,156);
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
 function call(method,args){ledger.push(['assert',method,args]);try{if(options.assertFault&&method==='call'&&args[1]==='invalid exact package root directory')throw injected;if(options.skipChecksum&&method==='equal'&&args[2]==='tar checksum')return;return method==='call'?assert(...args):assert[method](...args);}catch(e){remember(e);if(options.replaceError)throw new Error('private-replacement');if(options.changedSemantics)e.code='OTHER';throw e;}}
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
const insertion=delta.insertedSource;
need(typeof insertion==='string'&&marked.split(insertion).length===2,24);
function mutateOnce(source,a,b){need(source.split(a).length===2,24);return source.replace(a,b);}
function nativeFailure(r,stage,message){checkFailure(r,stage);need(r.value instanceof assert.AssertionError&&r.value.code==='ERR_ASSERTION',30);if(message!==undefined)need(r.value.message===message,31);}
// Check native strictEqual semantics and the original argument ledger; rendered diff text is source/runtime dependent.
function checksumFailure(r,block){
 nativeFailure(r,'tar-a01-checksum');
 let sum=0;for(let i=0;i<512;i++)sum+=(i>=148&&i<156)?32:block[i];
 const checksum=parseInt(block.subarray(148,156).toString('utf8').replace(/\0.*$/s,'').trim(),8);
 const calls=r.ledger.filter(e=>e[0]==='assert'),last=calls.at(-1),checksumCalls=calls.filter(e=>e[2][2]==='tar checksum');
 need(Number.isSafeInteger(sum)&&Number.isSafeInteger(checksum)&&sum!==checksum&&canonical(boundMarks(r))===canonical([['tar-a01-checksum']])&&checksumCalls.length===1&&last?.[1]==='equal'&&last[2].length===3&&Object.is(last[2][0],sum)&&Object.is(last[2][1],checksum)&&last[2][2]==='tar checksum'&&r.value.operator==='strictEqual'&&Object.is(r.value.actual,sum)&&Object.is(r.value.expected,checksum)&&r.value.generatedMessage===false,31);
}
function bindArchive(r,gz){need(!r.thrown&&r.result.tarSha===sha(gz)&&r.result.integrity==='sha512-'+createHash('sha512').update(gz).digest('base64')&&r.result.url.startsWith('https://registry.npmjs.org/fixture/'),32);}
function sameFiles(a,b){need(!a.thrown&&!b.thrown&&canonical([...a.result.files])===canonical([...b.result.files]),33);}
function ioLedger(r){return r.ledger.filter(e=>['fetch','timeout','inflate'].includes(e[0]));}
function noRootMap(r){need(!r.ledger.some(e=>(e[0]==='map-set'||e[0]==='map-has')&&['','package','package/'].includes(e[1])),34);}
function rootRefusal(r){nativeFailure(r,'tar-parse','invalid exact package root directory');}
function queryModel({body=insertion,opts={}}={}){
 // Exact inserted branch in a bounded one-iteration shell. Nonempty prefix is an injected model only.
 const events=[],token=Object.hasOwn(opts,'fault')?opts.fault:new Error('private-field');let called=0,seen=false,boundary;
 function field(...args){events.push(['field',this===undefined,args]);called++;if(opts.fieldFault===called){seen=true;boundary=token;throw token;}return args[0]===257?(opts.magic??'ustar'):(opts.version??'00');}
 function observed(...args){events.push(['assert',this===undefined,args]);try{if(opts.assertFault)throw token;return assert(...args);}catch(e){seen=true;boundary=e;throw e;}}
 const invoke=new Function('name','type','length','prefix','tar','field','assert',"'use strict';let offset=0,turn=0;for(;offset+512<=tar.length&&turn<1;turn++){"+body+"return {offset,ordinary:true};}return {offset,ordinary:false};");
 let thrown=false,value,result;try{result=invoke(opts.name??'package',opts.type??'5',opts.length??0,opts.prefix??'',{length:opts.tarLength??1024},field,observed);}catch(e){thrown=true;value=e;}
 return {events,thrown,value,result,seen,boundary,token};
}
function eventEqual(actual,expected){need(canonical(actual)===canonical(expected),50);}
const validEvents=[['field',true,[257,263]],['field',true,[263,265]],['assert',true,[true,'invalid exact package root directory']]];
function modelIdentity(r){need(r.thrown&&r.seen&&Object.is(r.value,r.boundary)&&Object.is(r.value,r.token),51);}
async function main(){
 need(process.platform==='win32'&&process.version==='v24.21.0'&&path.isAbsolute(root)&&!fs.existsSync(root),18);fs.mkdirSync(root);created=true;
 await group(1,async()=>{
  need(sha(old)===delta.before&&sha(marked)===delta.after&&delta.changes.length===1,19);
  const c=delta.changes[0];need(marked.split(c.new).length===2&&marked.replace(c.new,c.old)===old&&marked.replace(insertion,'')===old,20);
  need(read('setup-only.ps1')===read('original-setup-only.ps1')&&sd.changes.length===0,21);
  need(originals[0].helper===originals[1].helper&&originals[0].catch===originals[1].catch,22);
  const first=header({name:'package/a',data:Buffer.from('alpha')}),last=header({name:'package/z',data:Buffer.from('omega')});
  const plain=archive([first,last]),a=await fixture(0).run(plain),b=await fixture(1).run(plain);compare(a,b);bindArchive(a,plain);bindArchive(b,plain);
  for(const entries of [[rootHeader(),first,last],[first,rootHeader(),last],[first,last,rootHeader()],[rootHeader(),rootHeader(),first,rootHeader(),last]]){
   const gz=archive(entries),x=await fixture(0).run(gz),y=await fixture(1).run(gz);nativeFailure(x,'tar-a04-form-root-dir');bindArchive(y,gz);sameFiles(a,y);noRootMap(y);
   need(y.result.tarSha!==a.result.tarSha&&y.result.integrity!==a.result.integrity,23);
   need(canonical(ioLedger(a))===canonical(ioLedger(y)),25);
  }
  for(const entries of [[rootHeader()],[rootHeader(),rootHeader()]])nativeFailure(await fixture(1).run(archive(entries)),'tar-a09-nonempty');
  const slash=archive([header({name:'package/',type:'5',data:empty}),first]);compare(await fixture(0).run(slash),await fixture(1).run(slash));
  reject(()=>sameFiles(a,{...b,result:{...b.result,files:new Map()}}),33);
 });
 await group(2,async()=>{
  const validFile=header({name:'package/file'});
  for(const name of ['Package','packageX','package.','package ','./package','/package','package\\','package:','package/../bad','ｐackage']){
   const gz=archive([header({name,type:'5',data:empty}),validFile]);const a=await fixture(0).run(gz),b=await fixture(1).run(gz);compare(a,b);need(a.thrown&&b.thrown,26);
  }
  for(const type of ['0','\0','1','2','x','g','L','K','9']){
   const gz=archive([header({name:'package',type,data:empty}),validFile]);const a=await fixture(0).run(gz),b=await fixture(1).run(gz);compare(a,b);nativeFailure(b,'tar-a04-form-root-other');
  }
  // Natural nonempty prefix composes prefix/package and reaches original path refusal, not newroot branch.
  const prefixed=archive([header({name:'package',prefix:'outside',type:'5',data:empty}),validFile]);const a=await fixture(0).run(prefixed),b=await fixture(1).run(prefixed);compare(a,b);nativeFailure(b,'tar-a04-form-dir');
  const wrongType=archive([header({name:'package',type:'0',data:empty}),validFile]);const mutant=await fixture(1,{mutate:s=>mutateOnce(s,"name==='package'&&type==='5'","name==='package'")}).run(wrongType);reject(()=>nativeFailure(mutant,'tar-a04-form-root-other'),9);
  const dot=archive([header({name:'./package',type:'5',data:empty}),validFile]);const relaxed=await fixture(1,{mutate:s=>mutateOnce(s,"name==='package'&&type==='5'","(name==='package'||name==='./package')&&type==='5'")}).run(dot);reject(()=>nativeFailure(relaxed,'tar-a04-form-dot-dir'),9);
 });
 await group(3,async()=>{
  const file=header({name:'package/file'});
  for(const length of [1,511,512]){const gz=archive([header({name:'package',type:'5',data:Buffer.alloc(length)}),file]);rootRefusal(await fixture(1).run(gz));}
  for(const size of ['00000000008\0','\x80'+String.fromCharCode(0).repeat(11)])nativeFailure(await fixture(1).run(archive([header({name:'package',type:'5',data:empty,size}),file])),'tar-a02-octal-size');
  nativeFailure(await fixture(1).run(archive([header({name:'package',type:'5',data:empty,size:'00200000001\0'}),file])),'tar-a03-size-extent');
  nativeFailure(await fixture(1).run(archive([header({name:'package',type:'5',data:empty,size:'00000010000\0'})])),'tar-a03-size-extent');
  const checksumHeader=header({name:'package',type:'5',data:empty,checksumBad:true});
  const checksumArchive=archive([checksumHeader,file]),checksumResult=await fixture(1).run(checksumArchive);
  compare(await fixture(0).run(checksumArchive),checksumResult);checksumFailure(checksumResult,checksumHeader);
  // Native checksum semantics and exact original argument ledger; do not compare rendered diff message to its argument.
  const assertIndex=checksumResult.ledger.map(e=>e[0]).lastIndexOf('assert');
  for(const kind of ['missing','extra','method','arity','message','actual','expected']){
   const ledger=structuredClone(checksumResult.ledger),event=ledger[assertIndex];
   if(kind==='missing')ledger.splice(assertIndex,1);
   if(kind==='extra')ledger.push(structuredClone(event));
   if(kind==='method')event[1]='call';
   if(kind==='arity')event[2].push('extra');
   if(kind==='message')event[2][2]='unrelated assertion';
   if(kind==='actual')event[2][0]+=1;
   if(kind==='expected')event[2][1]+=1;
   reject(()=>checksumFailure({...checksumResult,ledger},checksumHeader),31);
  }
  // Change one native field while retaining class/code, stage, ledger and outward-observer identity preconditions.
  for(const fields of [{operator:'=='},{actual:checksumResult.value.actual+1},{expected:checksumResult.value.expected+1},{generatedMessage:true}]){
   const error=Object.defineProperties(Object.create(Object.getPrototypeOf(checksumResult.value)),Object.getOwnPropertyDescriptors(checksumResult.value));Object.assign(error,fields);
   reject(()=>checksumFailure({...checksumResult,value:error,boundary:error},checksumHeader),31);
  }
  reject(()=>checksumFailure({...checksumResult,marks:[['tar-a02-octal-size']]},checksumHeader),31);
  reject(()=>checksumFailure({...checksumResult,stage:'tar-a02-octal-size'},checksumHeader),12);
  reject(()=>checksumFailure({...checksumResult,value:new Error('private-replacement')},checksumHeader),9);
  const unrelated=Object.assign(new Error('private-unrelated'),{code:'ERR_ASSERTION',operator:'strictEqual',actual:checksumResult.value.actual,expected:checksumResult.value.expected,generatedMessage:false});
  reject(()=>checksumFailure({...checksumResult,value:unrelated,boundary:unrelated},checksumHeader),30);
  for(const [magic,version]of [['',''],['ustar ',' '],['xxxxx','00'],['ustar\0','01']])rootRefusal(await fixture(1).run(archive([header({name:'package',type:'5',data:empty,magic,version}),file])));
  // Fullarchive modulo constraint tested with valid nonemptyfiles, not incidental empty-map failure.
  for(const n of [1,511])rootRefusal(await fixture(1).run(archive([rootHeader(),file],Buffer.alloc(n))));
  nativeFailure(await fixture(1).run(archive([],rootHeader().subarray(0,511))),'tar-a09-nonempty');
  const size1=archive([header({name:'package',type:'5',data:Buffer.alloc(1)}),file]);
  const noSize=await fixture(1,{mutate:s=>mutateOnce(s,'length===0&&tar.length','true&&tar.length')}).run(size1);reject(()=>rootRefusal(noSize),12);
  const misaligned=archive([rootHeader(),file],Buffer.alloc(1));const noAlign=await fixture(1,{mutate:s=>mutateOnce(s,'tar.length%512===0','true')}).run(misaligned);reject(()=>rootRefusal(noAlign),9);
  const badMagic=archive([header({name:'package',type:'5',data:empty,magic:'xxxxx'}),file]);const noMagic=await fixture(1,{mutate:s=>mutateOnce(s,"field(257,263)==='ustar'",'true')}).run(badMagic);reject(()=>rootRefusal(noMagic),9);
  const badVersion=archive([header({name:'package',type:'5',data:empty,version:'01'}),file]);const noVersion=await fixture(1,{mutate:s=>mutateOnce(s,"field(263,265)==='00'",'true')}).run(badVersion);reject(()=>rootRefusal(noVersion),9);
  // Original checksum guard must not be moved behind exception.
  const unchecked=await fixture(1,{skipChecksum:true}).run(archive([header({name:'package',type:'5',data:empty,checksumBad:true}),file]));reject(()=>nativeFailure(unchecked,'tar-a01-checksum'),9);
 });
 await group(4,async()=>{
  const file=header({name:'package/file'});
  for(const [entry,stage]of [[header({name:'package/bad',checksumBad:true}),'tar-a01-checksum'],[header({name:'package/a/../b'}),'tar-a04-traversal-false'],[header({name:'package/a\\b'}),'tar-a04-backslash-false'],[header({name:'package/a:b'}),'tar-a04-colon-false'],[header({name:'package/CON'}),'tar-a05-windows-name'],[header({name:'package/meta',type:'x',data:empty}),'tar-a08-entry-type'],[header({name:'PaxHeader/file',type:'x',data:empty}),'tar-a04-form-pax-x']])nativeFailure(await fixture(1).run(archive([rootHeader(),entry])),stage);
  nativeFailure(await fixture(1).run(archive([file,rootHeader(),file])),'tar-a06-file-unique');
  rootRefusal(await fixture(1).run(archive([file,rootHeader()],file.subarray(0,511))));
  const goodRoot=archive([rootHeader(),file]),a=await fixture(1).run(goodRoot);bindArchive(a,goodRoot);
  // 1024 skips the next header; this is a genuine wrong-stride mutant and must not match the files map.
  const jump=await fixture(1,{mutate:s=>mutateOnce(s,'offset+=512;continue;','offset+=1024;continue;')}).run(goodRoot);reject(()=>sameFiles(a,jump),33);
  // Equivalent for admitted length0, explicitly accepted as equivalent rather than advertised as a killed mutant.
  const equivalent=await fixture(1,{mutate:s=>mutateOnce(s,'offset+=512;continue;','offset+=512+Math.ceil(length/512)*512;continue;')}).run(goodRoot);compare(a,equivalent);bindArchive(equivalent,goodRoot);
  const mapped=await fixture(1,{mutate:s=>mutateOnce(s,'offset+=512;continue;',"files.set('package',{sha256:'private-invalid',bytes:0});offset+=512;continue;")}).run(goodRoot);reject(()=>sameFiles(a,mapped),33);reject(()=>noRootMap(mapped),34);
  // Zero-stride mutant is evaluated only in bounded one-iteration model to avoid a hanging real parser.
  const zero=queryModel({body:mutateOnce(insertion,'offset+=512;continue;','offset+=0;continue;')});reject(()=>need(!zero.thrown&&zero.result.offset===512,52),52);
 });
 await group(5,async()=>{
  const valid=queryModel();eventEqual(valid.events,validEvents);need(!valid.thrown&&valid.result.offset===512&&!valid.result.ordinary,52);
  for(const opts of [{name:'package/file'},{type:'0'},{name:'./package'}]){const r=queryModel({opts});need(!r.thrown&&r.result.ordinary&&r.result.offset===0,53);eventEqual(r.events,[]);}
  for(const opts of [{length:1},{tarLength:513},{prefix:'outside'}]){const r=queryModel({opts});need(r.thrown&&r.value instanceof assert.AssertionError,54);eventEqual(r.events,[['assert',true,[false,'invalid exact package root directory']]]);}
  // Nonempty prefix with exactroot is deliberately a synthetic impossible-native composition; no native mutant claim.
  const injected=queryModel({opts:{prefix:'outside'},body:mutateOnce(insertion,"prefix===''",'true')});need(!injected.thrown&&injected.result.offset===512,55);
  eventEqual(queryModel({opts:{magic:'wrong'}}).events,[['field',true,[257,263]],['assert',true,[false,'invalid exact package root directory']]]);
  for(const fieldFault of [1,2])for(const fault of [new Error('private-field'),'private-string',null]){const r=queryModel({opts:{fieldFault,fault}});modelIdentity(r);need(r.events.length===fieldFault&&!r.events.some(e=>e[0]==='assert'),56);}
  for(const fault of [new Error('private-assert'),'private-string',null])modelIdentity(queryModel({opts:{assertFault:true,fault}}));
  for(const [from,to]of [["field(257,263)","(field(257,263),field(257,263))"],["field(257,263)","'ustar'"],["field(257,263)","field(258,263)"],["field(257,263)","field.call({},257,263)"]]){
   const r=queryModel({body:mutateOnce(insertion,from,to)});reject(()=>eventEqual(r.events,validEvents),50);
  }
  reject(()=>modelIdentity({...queryModel({opts:{fieldFault:1}}),seen:false}),51);
  const faulty=queryModel({opts:{fieldFault:1}});reject(()=>modelIdentity({...faulty,value:new Error('private-replacement')}),51);
 });
 await group(6,async()=>{
  const file=header({name:'package/file'}),gz=archive([rootHeader(),file]);
  const f=fixture(1);const a=await f.run(gz,{url:'https://registry.npmjs.org/fixture/cache.tgz'}),b=await f.run(gz,{url:'https://registry.npmjs.org/fixture/cache.tgz'});bindArchive(a,gz);bindArchive(b,gz);sameFiles(a,b);need(ioLedger(a).length===3&&ioLedger(b).length===0,57);
  const mismatch=await fixture(1).run(gz,{sri:'sha512-'+Buffer.alloc(64).toString('base64')});nativeFailure(mismatch,'tar-integrity');need(!mismatch.ledger.some(e=>e[0]==='inflate'),58);
  // Direct injected values at new assert pass through exact full tarFor plus original outercatch unchanged.
  for(const injected of [new Error('private-original'),'private-string',null]){const x=await fixture(1,{assertFault:true,injected}).run(gz);need(x.thrown&&x.observed&&Object.is(x.value,injected)&&Object.is(x.boundary,injected),59);checkFailure(x,'tar-parse');}
  const bad=archive([header({name:'package',type:'5',data:empty,version:'01'}),file]);
  const real=fixture(1,{realWrite:true});const first=await real.run(bad);rootRefusal(first);const filename=path.join(root,'dependency-failure.json'),bytes=fs.readFileSync(filename);need(bytes.equals(Buffer.from(first.writes[0].b)),60);
  const next=await real.run(archive([header({name:'outside/file'})]));nativeFailure(next,'tar-a04-form-file');need(fs.readFileSync(filename).equals(bytes),61);fs.unlinkSync(filename);knownFiles.delete(filename);
  const writer=await real.run(bad,{writeFault:true});rootRefusal(writer);need(Object.is(writer.value,writer.boundary)&&!fs.existsSync(filename),62);
  const fresh=await real.run(gz);bindArchive(fresh,gz);need(fresh.writes.length===0&&!fs.existsSync(filename),63);
  const altered=await fixture(1,{replaceError:true}).run(bad);reject(()=>rootRefusal(altered),9);
  // No original checks or body after tarFor are rewritten, including file count and installed-byte inventory.
  need(marked.replace(insertion,'')===old&&marked.slice(marked.indexOf('\nfunction inventory('))===old.slice(old.indexOf('\nfunction inventory(')),64);
 });
}
let outcome='FAILED';try{await main();outcome='PASS';}catch{}finally{try{if(created){for(const f of fs.readdirSync(root)){const full=path.join(root,f),st=fs.lstatSync(full);need(knownFiles.has(full)&&st.isFile()&&!st.isSymbolicLink(),47);}for(const f of knownFiles)fs.unlinkSync(f);fs.rmdirSync(root);cleaned=!fs.existsSync(root);}}catch{outcome='FAILED';}}
if(!created||!cleaned||rows.length!==6)outcome='FAILED';
process.stdout.write(JSON.stringify({schema:'dependency-js-controls-v1',outcome,caseIndex,assertion,created,cleaned,results:outcome==='PASS'?rows:[],completed:rows.map(x=>x.name),setupInvocations:0,packageEntryExecutions:0,privateLogReads:0}));if(outcome!=='PASS')process.exitCode=1;
