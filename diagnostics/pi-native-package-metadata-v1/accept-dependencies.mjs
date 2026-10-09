// Artifact-only setup acceptance; no package entry point is imported or executed.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,mkdirSync,readdirSync,lstatSync,realpathSync,existsSync,copyFileSync,unlinkSync} from 'node:fs';
import {resolve,join,relative,dirname,isAbsolute,sep} from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {gunzipSync} from 'node:zlib';
// BEGIN FIXED DEPENDENCY FAILURE METADATA
let dependencyFailureStage='argument-resolution',dependencyFailureGroup='none',dependencyFailureRoot=null;
function MarkDependencyFailureStage(stage,group=null) {
 try {
  if(DEPENDENCY_FAILURE_STAGES.includes(stage))dependencyFailureStage=stage;
  if(group!==null&&['none','adapter','pi'].includes(group))dependencyFailureGroup=group;
 } catch {}
}
const DEPENDENCY_FAILURE_STAGES=["argument-resolution","platform","node-version","expected-lock-read","installed-lock-read","package-keys","package-path","package-metadata","integrity-attribution","tar-cache-key","tar-url-policy","tar-fetch","tar-response","tar-stream","tar-integrity","tar-inflate","tar-parse","installed-tar-bytes","installed-inventory","generated-files","mirror-absence","mirror-copy","mirror-inventory","entry-pin","resolution-preinventory","resolution-helper","resolution-child","resolution-output","resolution-identities","resolution-targets","resolution-postinventory","mirror-unmodified","result-write","tar-a01-checksum","tar-a02-octal-size","tar-a03-size-extent","tar-a04-package-path","tar-a05-windows-name","tar-a06-file-unique","tar-a07-file-count","tar-a08-entry-type","tar-a09-nonempty","tar-a04-prefix-eval","tar-a04-prefix-false","tar-a04-prefix-true","tar-a04-prefix-other","tar-a04-backslash-eval","tar-a04-backslash-false","tar-a04-backslash-true","tar-a04-backslash-other","tar-a04-traversal-eval","tar-a04-traversal-false","tar-a04-traversal-true","tar-a04-traversal-other","tar-a04-colon-eval","tar-a04-colon-false","tar-a04-colon-true","tar-a04-colon-other","tar-a04-form-root-dir","tar-a04-form-root-other","tar-a04-form-dot-dir","tar-a04-form-dot-other","tar-a04-form-pax-x","tar-a04-form-pax-g","tar-a04-form-pax-other","tar-a04-form-gnu-L","tar-a04-form-gnu-K","tar-a04-form-dir","tar-a04-form-file","tar-a04-form-other","pm-no-actual","pm-no-expected","pm-shape","pm-identity","pm-integrity","pm-graph","pm-platform","pm-lifecycle","pm-descriptive","pm-other","pm-multiple","pm-equal-call","pm-unclassified"];
const DEPENDENCY_FAILURE_CODES=["ERR_ASSERTION","ENOENT","ENOTDIR","EACCES","EPERM","EEXIST","ERR_INVALID_ARG_TYPE","ERR_INVALID_ARG_VALUE","ERR_OUT_OF_RANGE","ABORT_ERR","ETIMEDOUT","ERR_BUFFER_TOO_LARGE","Z_DATA_ERROR","OTHER"];
function WriteDependencyFailureMetadata(error) {
 try {
  if(typeof dependencyFailureRoot!=='string')return;
  let code='OTHER';
  try {const descriptor=Object.getOwnPropertyDescriptor(error,'code');if(descriptor&&typeof descriptor.value==='string'&&DEPENDENCY_FAILURE_CODES.includes(descriptor.value))code=descriptor.value;} catch {}
  const record={schema:'dependency-failure-v1',stage:DEPENDENCY_FAILURE_STAGES.includes(dependencyFailureStage)?dependencyFailureStage:'unknown',group:['none','adapter','pi'].includes(dependencyFailureGroup)?dependencyFailureGroup:'unknown',code,outcome:'FAILED',behavioralInvocations:0};
  const text=JSON.stringify(record);
  if(text.length<=512)writeFileSync(join(dependencyFailureRoot,'dependency-failure.json'),text,{flag:'wx'});
 } catch {} // Metadata/file/writer failure cannot replace the original thrown value.
}
// BEGIN PACKAGE METADATA FAILURE CLASSIFIER
function ClassifyPackageMetadataFailure(actualReady,expectedReady,actualText,expectedText) {
 try {
  if(!actualReady)return 'pm-no-actual';
  if(!expectedReady)return 'pm-no-expected';
  if(typeof actualText!=='string'||typeof expectedText!=='string'||Buffer.byteLength(actualText,'utf8')>16384||Buffer.byteLength(expectedText,'utf8')>16384)return 'pm-unclassified';
  if(actualText===expectedText)return 'pm-equal-call';
  const actual=JSON.parse(actualText),expected=JSON.parse(expectedText);
  if(actual===null||expected===null||typeof actual!=='object'||typeof expected!=='object'||Array.isArray(actual)||Array.isArray(expected))return 'pm-shape';
  const keys=[...new Set([...Object.keys(actual),...Object.keys(expected)])];
  if(keys.length>64)return 'pm-unclassified';
  const groups=new Set();
  for(const key of keys){
   if(Object.hasOwn(actual,key)===Object.hasOwn(expected,key)&&JSON.stringify(actual[key])===JSON.stringify(expected[key]))continue;
   let group='other';
   if(['version','resolved'].includes(key))group='identity';
   else if(key==='integrity')group='integrity';
   else if(['dependencies','optionalDependencies','peerDependencies','peerDependenciesMeta'].includes(key))group='graph';
   else if(['optional','dev','peer','devOptional','cpu','os','libc','engines'].includes(key))group='platform';
   else if(['hasInstallScript','hasShrinkwrap','bin'].includes(key))group='lifecycle';
   else if(['license','funding','deprecated'].includes(key))group='descriptive';
   groups.add(group);
  }
  if(groups.size>1)return 'pm-multiple';
  if(groups.size===1)return 'pm-'+groups.values().next().value;
  // Parsed equivalence does not establish original assertion-argument equality.
  return 'pm-unclassified';
 } catch {return 'pm-unclassified';}
}
// END PACKAGE METADATA FAILURE CLASSIFIER
// END FIXED DEPENDENCY FAILURE METADATA
try {
const [packet,work,evidence]=process.argv.slice(2).map(x=>resolve(x));
try {dependencyFailureRoot=evidence;} catch {}
MarkDependencyFailureStage('platform');assert.equal(process.platform,'win32');MarkDependencyFailureStage('node-version');assert.equal(process.version,'v24.21.0');
const digest=b=>createHash('sha256').update(b).digest('hex');
const inside=(base,path)=>{const rel=relative(base,path);return rel!==''&&!rel.startsWith('..'+sep)&&rel!=='..'&&!isAbsolute(rel);};
const stable=x=>x===null||typeof x!=='object'?JSON.stringify(x):Array.isArray(x)?'['+x.map(stable).join(',')+']':'{'+Object.keys(x).sort().map(k=>JSON.stringify(k)+':'+stable(x[k])).join(',')+'}';
const outputs=[];let downloaded=0,totalInflated=0;const cache=new Map();
function integrityFor(records,path){const v=records[path];if(v.integrity)return v.integrity;const candidates=Object.entries(records).filter(([k,x])=>k!==path&&x.integrity&&x.resolved===v.resolved&&stable({...x,integrity:undefined})===stable({...v,integrity:undefined}));assert.equal(candidates.length,1,'missing integrity is not uniquely attributed');return candidates[0][1].integrity;}
async function tarFor(url,sri){MarkDependencyFailureStage('tar-cache-key');const key=url+'\n'+sri;if(cache.has(key))return cache.get(key);MarkDependencyFailureStage('tar-url-policy');const u=new URL(url);assert.equal(u.protocol,'https:');assert.equal(u.hostname,'registry.npmjs.org');assert(!u.username&&!u.password&&!u.search);assert(/^sha512-[A-Za-z0-9+/]+=*$/.test(sri));MarkDependencyFailureStage('tar-fetch');const response=await fetch(u,{redirect:'error',signal:AbortSignal.timeout(15000)});MarkDependencyFailureStage('tar-response');assert.equal(response.status,200);MarkDependencyFailureStage('tar-stream');const chunks=[];let size=0;for await(const x of response.body){size+=x.length;downloaded+=x.length;assert(size<=33554432&&downloaded<=536870912,'tarball download cap');chunks.push(x);}MarkDependencyFailureStage('tar-integrity');const compressed=Buffer.concat(chunks);assert.equal('sha512-'+createHash('sha512').update(compressed).digest('base64'),sri);MarkDependencyFailureStage('tar-inflate');const tar=gunzipSync(compressed,{maxOutputLength:268435456});totalInflated+=tar.length;assert(totalInflated<=2147483648,'aggregate tar cap');MarkDependencyFailureStage('tar-parse');const files=new Map();let count=0;
 // Strict, bounded local PAX subset. Metadata is interpreted, never silently skipped.
 let pendingPax=null,paxBytes=0,paxHeaders=0;
 function paxPath(value){
  assert(value.startsWith('package/')&&!value.includes('\\')&&!value.split('/').includes('..')&&!value.includes(':'),'pax effective path');
  const rel=value.slice(8);
  assert(rel&&!value.endsWith('/')&&rel.split('/').filter(Boolean).every(x=>!/[ .]$/.test(x)&&! /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(x)),'pax effective Windows name');
  return rel;
 }
 function paxRecords(body){
  const values=Object.create(null);let pos=0,records=0;
  while(pos<body.length){
   const space=body.indexOf(32,pos);
   assert(space>pos&&space-pos<=7,'pax record length');
   const digits=body.subarray(pos,space).toString('latin1');
   assert(/^[1-9][0-9]*$/.test(digits),'pax record length');
   const n=Number(digits),end=pos+n;
   assert(Number.isSafeInteger(n)&&end<=body.length&&end>space+3&&body[end-1]===10,'pax record extent');
   const equal=body.indexOf(61,space+1);
   assert(equal>space+1&&equal<end-1,'pax record key');
   const key=body.subarray(space+1,equal).toString('latin1');
   assert(['path','size','mtime','linkpath'].includes(key)&&!Object.hasOwn(values,key)&&++records<=4,'pax record key');
   const bytes=body.subarray(equal+1,end-1);
   assert(!bytes.includes(0)&&!bytes.includes(10)&&!bytes.includes(13),'pax record value');
   if(key==='path'){
    assert(bytes.length>0&&bytes.length<=4096,'pax path bytes');
    const value=bytes.toString('utf8');
    assert(Buffer.from(value,'utf8').equals(bytes)&&!value.startsWith('\uFEFF'),'pax path encoding');
    paxPath(value);values.path=value;
   }else if(key==='size'){
    const value=bytes.toString('latin1');
    assert(/^(0|[1-9][0-9]*)$/.test(value)&&Number.isSafeInteger(Number(value))&&Number(value)<=33554432,'pax size');
    values.size=Number(value);
   }else if(key==='mtime'){
    const value=bytes.toString('latin1');
    assert(bytes.length>0&&bytes.length<=64&&/^-?(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(value)&&Number.isFinite(Number(value))&&Math.abs(Number(value))<=8640000000000,'pax mtime');
    values.mtime=value;
   }else {assert(bytes.length===0,'pax linkpath');values.linkpath='';}
   pos=end;
  }
  assert(records>0&&pos===body.length,'pax records');return values;
 }

 for(let offset=0;offset+512<=tar.length;){const block=tar.subarray(offset,offset+512);if(block.every(x=>x===0)){if(pendingPax!==null)assert(false,'pax orphan');break;}const field=(a,b)=>block.subarray(a,b).toString('utf8').replace(/\0.*$/s,'');const checksum=parseInt(field(148,156).trim(),8);let sum=0;for(let i=0;i<512;i++)sum+=(i>=148&&i<156)?32:block[i];try {assert.equal(sum,checksum,'tar checksum');} catch(dependencyTarError) {try {MarkDependencyFailureStage('tar-a01-checksum');} catch {} throw dependencyTarError;}const prefix=field(345,500),name=(prefix?prefix+'/':'')+field(0,100),type=String.fromCharCode(block[156]||48),sizeText=field(124,136).trim();try {assert(/^[0-7]+$/.test(sizeText));} catch(dependencyTarError) {try {MarkDependencyFailureStage('tar-a02-octal-size');} catch {} throw dependencyTarError;}const length=parseInt(sizeText,8);try {assert(length<=33554432&&offset+512+length<=tar.length);} catch(dependencyTarError) {try {MarkDependencyFailureStage('tar-a03-size-extent');} catch {} throw dependencyTarError;}if(pendingPax!==null)assert(type==='0','pax regular target');
 if(type==='x'){
  assert(tar.length%512===0&&prefix===''&&field(257,263)==='ustar'&&field(263,265)==='00'&&field(157,257)==='','pax carrier header');
  const carrier=name.slice(10);
  assert(name.startsWith('PaxHeader/')&&carrier!==''&&carrier!=='.'&&carrier!=='..'&&!/[\\/:\x00]/.test(carrier)&&!/[ .]$/.test(carrier)&&! /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(carrier),'pax carrier path');
  assert(length>0&&length<=1048576&&paxBytes+length<=8388608&&paxHeaders+1<=1024,'pax metadata cap');
  const next=offset+512+Math.ceil(length/512)*512;
  assert(next<=tar.length&&tar.subarray(offset+512+length,next).every(x=>x===0),'pax metadata padding');
  const values=paxRecords(tar.subarray(offset+512,offset+512+length));
  pendingPax=values;paxBytes+=length;paxHeaders++;offset=next;continue;
 }
 if(name==='package'&&type==='5'){assert(length===0&&tar.length%512===0&&prefix===''&&field(257,263)==='ustar'&&field(263,265)==='00','invalid exact package root directory');offset+=512;continue;}{let dependencyA04Part='unavailable',dependencyA04Returned=false,dependencyA04Value;
try {assert((dependencyA04Part='prefix',dependencyA04Returned=false,dependencyA04Value=name.startsWith('package/'),dependencyA04Returned=true,dependencyA04Value)&&(dependencyA04Part='backslash',dependencyA04Returned=false,dependencyA04Value=!name.includes('\\'),dependencyA04Returned=true,dependencyA04Value)&&(dependencyA04Part='traversal',dependencyA04Returned=false,dependencyA04Value=!name.split('/').includes('..'),dependencyA04Returned=true,dependencyA04Value)&&(dependencyA04Part='colon',dependencyA04Returned=false,dependencyA04Value=!name.includes(':'),dependencyA04Returned=true,dependencyA04Value));} catch(dependencyTarError) {
 try {
  const dependencyA04Kind=!dependencyA04Returned?'eval':typeof dependencyA04Value==='boolean'?(dependencyA04Value?'true':'false'):'other';
  let dependencyA04Stage='tar-a04-package-path';
  if(dependencyA04Part==='prefix')dependencyA04Stage='tar-a04-prefix-'+dependencyA04Kind;
  else if(dependencyA04Part==='backslash')dependencyA04Stage='tar-a04-backslash-'+dependencyA04Kind;
  else if(dependencyA04Part==='traversal')dependencyA04Stage='tar-a04-traversal-'+dependencyA04Kind;
  else if(dependencyA04Part==='colon')dependencyA04Stage='tar-a04-colon-'+dependencyA04Kind;
  if(dependencyA04Part==='prefix'&&dependencyA04Returned&&dependencyA04Value===false){
   try {
    if(typeof name==='string'&&typeof type==='string'&&type.length===1){
     if(name==='package')dependencyA04Stage=type==='5'?'tar-a04-form-root-dir':'tar-a04-form-root-other';
     else if(name==='./package'||name==='./package/')dependencyA04Stage=type==='5'?'tar-a04-form-dot-dir':'tar-a04-form-dot-other';
     else if(type==='x'||type==='g')dependencyA04Stage=name.startsWith('PaxHeader/')?(type==='x'?'tar-a04-form-pax-x':'tar-a04-form-pax-g'):'tar-a04-form-pax-other';
     else if(type==='L')dependencyA04Stage='tar-a04-form-gnu-L';
     else if(type==='K')dependencyA04Stage='tar-a04-form-gnu-K';
     else if(type==='5')dependencyA04Stage='tar-a04-form-dir';
     else if(type==='0')dependencyA04Stage='tar-a04-form-file';
     else dependencyA04Stage='tar-a04-form-other';
    }
   } catch {}
  }
  MarkDependencyFailureStage(dependencyA04Stage);
 } catch {}
 throw dependencyTarError;
}}let rel=name.slice(8),contentLength=length;try {assert(rel.split('/').filter(Boolean).every(x=>!/[ .]$/.test(x)&&! /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(x)),'unsafe Windows tar name');} catch(dependencyTarError) {try {MarkDependencyFailureStage('tar-a05-windows-name');} catch {} throw dependencyTarError;}if(pendingPax!==null){
  assert(!name.endsWith('/')&&rel!==''&&field(157,257)==='','pax regular target');
  if(Object.hasOwn(pendingPax,'path'))rel=paxPath(pendingPax.path);
  if(Object.hasOwn(pendingPax,'size'))contentLength=pendingPax.size;
  assert(contentLength<=33554432&&offset+512+contentLength<=tar.length&&offset+512+Math.ceil(contentLength/512)*512<=tar.length,'pax effective extent');
  assert(count+1<=100000,'pax file count');pendingPax=null;
 }
 if(type==='0'){try {assert(rel&&!files.has(rel));} catch(dependencyTarError) {try {MarkDependencyFailureStage('tar-a06-file-unique');} catch {} throw dependencyTarError;}files.set(rel,{sha256:digest(tar.subarray(offset+512,offset+512+contentLength)),bytes:contentLength});count++;try {assert(count<=100000);} catch(dependencyTarError) {try {MarkDependencyFailureStage('tar-a07-file-count');} catch {} throw dependencyTarError;}}else {try {assert(type==='5','unsupported tar entry: fail closed, no automatic extraction');} catch(dependencyTarError) {try {MarkDependencyFailureStage('tar-a08-entry-type');} catch {} throw dependencyTarError;}}offset+=512+Math.ceil(contentLength/512)*512;
 }if(pendingPax!==null)assert(false,'pax orphan');try {assert(files.size>0);} catch(dependencyTarError) {try {MarkDependencyFailureStage('tar-a09-nonempty');} catch {} throw dependencyTarError;}const result={files,tarSha:digest(compressed),integrity:sri,url};cache.set(key,result);return result;}
function inventory(base){const files={};let bytes=0,count=0;function walk(dir){for(const entry of readdirSync(dir,{withFileTypes:true})){const f=join(dir,entry.name),st=lstatSync(f);assert(!st.isSymbolicLink(),'unreviewed junction/symlink');if(st.isDirectory())walk(f);else{assert(st.isFile()&&st.size<=33554432);bytes+=st.size;count++;assert(bytes<=2147483648&&count<=100000);files[relative(base,f).split(sep).join('/')]=digest(readFileSync(f));}}}walk(base);return files;}
for(const [lockName,prefix] of [['package-lock.json','.pi-acp-test'],['pi-package-lock.json','.pi-runtime-test']]){
 MarkDependencyFailureStage('expected-lock-read',lockName==='package-lock.json'?'adapter':'pi');const base=join(work,prefix);const expected=JSON.parse(readFileSync(join(packet,lockName),'utf8')).packages;MarkDependencyFailureStage('installed-lock-read');const actual=JSON.parse(readFileSync(join(base,'node_modules/.package-lock.json'),'utf8')).packages;MarkDependencyFailureStage('package-keys');assert.deepEqual(Object.keys(actual).sort(),Object.keys(expected).sort(),'no omitted optional/platform package accepted');const tarFiles={};const attribution=[];
 for(const name of Object.keys(expected).sort()){MarkDependencyFailureStage('package-path');assert(name.startsWith('node_modules/')&&!name.split('/').includes('..')&&!name.includes('\\'));const e=expected[name],a=actual[name];MarkDependencyFailureStage('package-metadata');{let metadataActualReady=false,metadataExpectedReady=false,metadataActualText,metadataExpectedText;try{assert.equal((metadataActualText=stable(a),metadataActualReady=true,metadataActualText),(metadataExpectedText=stable(e),metadataExpectedReady=true,metadataExpectedText),'metadata difference, including optional/platform flags');}catch(metadataError){try{MarkDependencyFailureStage(ClassifyPackageMetadataFailure(metadataActualReady,metadataExpectedReady,metadataActualText,metadataExpectedText));}catch{}throw metadataError;}}MarkDependencyFailureStage('integrity-attribution');const sri=integrityFor(expected,name);const tar=await tarFor(e.resolved,sri);MarkDependencyFailureStage('installed-tar-bytes');attribution.push({packagePath:name,version:e.version,resolved:e.resolved,integrity:sri,integrityInHistoricalRecord:!!e.integrity,tarSha256:tar.tarSha});for(const [n,v]of tar.files){const rel=name+'/'+n,f=join(base,...rel.split('/'));assert(inside(base,f));assert(lstatSync(f).isFile()&&!lstatSync(f).isSymbolicLink());assert.equal(digest(readFileSync(f)),v.sha256,'installed bytes differ from tar');if(tarFiles[rel])assert.equal(tarFiles[rel],v.sha256);tarFiles[rel]=v.sha256;}}
 MarkDependencyFailureStage('installed-inventory');const all=inventory(base);MarkDependencyFailureStage('generated-files');const extra=Object.keys(all).filter(n=>!tarFiles[n]);for(const n of extra){assert(n==='node_modules/.package-lock.json'||/(^|\/)node_modules\/\.bin\/[^/]+$/.test(n),'unattributed installed file');}outputs.push({prefix,packages:attribution,fileInventory:all,generatedFiles:extra,qualification:'Generated .bin shims hashed but not executed; no optional/platform exclusion permitted. Tar-attributed bytes are current install evidence, not a historical installed tree.'});
}
MarkDependencyFailureStage('mirror-absence','adapter');const original=join(work,'.pi-acp-test');const mirror=join(work,'.pi-acp-mirror');assert(!existsSync(mirror));
function copyTree(from,to){mkdirSync(to,{recursive:false});for(const e of readdirSync(from,{withFileTypes:true})){const a=join(from,e.name),b=join(to,e.name);assert(!lstatSync(a).isSymbolicLink());if(e.isDirectory())copyTree(a,b);else copyFileSync(a,b,1);}}
MarkDependencyFailureStage('mirror-copy');copyTree(original,mirror);MarkDependencyFailureStage('mirror-inventory');assert.deepEqual(inventory(original),inventory(mirror));
MarkDependencyFailureStage('entry-pin');const entryRel='node_modules/pi-acp/dist/index.js',source=join(original,entryRel),marked=join(mirror,entryRel);assert.equal(digest(readFileSync(source)),'324aeb8bba1228937e16b1326fb3e014e2a625b2dea549dbcd49b006cd5df6a2');
MarkDependencyFailureStage('resolution-preinventory');const originalBefore=inventory(original),mirrorBefore=inventory(mirror);
function resolveEntries(entry){MarkDependencyFailureStage('resolution-helper');const helper=join(dirname(entry),'.pi-setup-resolution.mjs');assert(!existsSync(helper));const bytes=readFileSync(join(packet,'resolve-entry.mjs'));writeFileSync(helper,bytes,{flag:'wx'});try{MarkDependencyFailureStage('resolution-child');const run=spawnSync(process.execPath,[helper],{timeout:5000,maxBuffer:8192});MarkDependencyFailureStage('resolution-output');assert.equal(run.status,0);assert.equal(run.stderr.length,0);return JSON.parse(run.stdout.toString());}finally{unlinkSync(helper);}}
const a=resolveEntries(source),b=resolveEntries(marked);MarkDependencyFailureStage('resolution-identities');assert.deepEqual(a.map(x=>x.name),['@agentclientprotocol/sdk','cross-spawn']);assert.deepEqual(b.map(x=>x.name),a.map(x=>x.name));
MarkDependencyFailureStage('resolution-targets');const resolution=[];for(let i=0;i<a.length;i++){for(const condition of ['import','require']){const left=realpathSync(a[i][condition+'Path']),right=realpathSync(b[i][condition+'Path']);assert(inside(original,left)&&inside(mirror,right));assert.equal(relative(original,left),relative(mirror,right));assert.equal(digest(readFileSync(left)),digest(readFileSync(right)));resolution.push({name:a[i].name,condition,relativePath:relative(original,left),sha256:digest(readFileSync(left))});}}
MarkDependencyFailureStage('resolution-postinventory');assert.deepEqual(inventory(original),originalBefore);assert.deepEqual(inventory(mirror),mirrorBefore);
// Setup only: the mirror is verified but deliberately NOT overlaid or launched.
MarkDependencyFailureStage('mirror-unmodified');assert.equal(digest(readFileSync(marked)),digest(readFileSync(source)));
MarkDependencyFailureStage('result-write');writeFileSync(join(evidence,'dependency-result.json'),JSON.stringify({stage:'setup',outcome:'PASS',behavioralInvocations:0,downloadsBytes:downloaded,packages:outputs,resolution,mirrorUnmodified:true,optionalExclusions:[],tarParser:'Regular files/directories and bounded single local PAX regular-file overrides; unsupported archive features stop setup.'},null,2),{flag:'wx'});
} catch(dependencyFailureError) {
 try {WriteDependencyFailureMetadata(dependencyFailureError);} catch {}
 throw dependencyFailureError;
}
