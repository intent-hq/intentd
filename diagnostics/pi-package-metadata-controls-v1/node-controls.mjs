import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const packet=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];
let caseIndex=0,assertion=0,created=false,cleaned=false;
const rows=[],knownFiles=new Set();
function need(ok,id){assertion=id;if(!ok)throw new Error('dependency_control_'+id);}
const read=n=>fs.readFileSync(path.join(packet,n),'utf8');
const old=read('original-accept-dependencies.mjs'),marked=read('accept-dependencies.mjs'),delta=JSON.parse(read('acceptance-delta.json')),schema=JSON.parse(read('schema.json')),expected=JSON.parse(read('expected-controls.json')),inv=JSON.parse(read('invariants.json'));
const sha=b=>createHash('sha256').update(b).digest('hex');
function extract(text,start,end){need(text.split(start).length===2&&text.split(end).length===2,1);const i=text.indexOf(start),j=text.indexOf(end,i)+end.length;need(j>i,1);return text.slice(i,j);}
const fixed=extract(marked,'// BEGIN FIXED DEPENDENCY FAILURE METADATA','// END FIXED DEPENDENCY FAILURE METADATA');
const classifierText=extract(marked,'// BEGIN PACKAGE METADATA FAILURE CLASSIFIER','// END PACKAGE METADATA FAILURE CLASSIFIER');
const stableText=marked.split('\n').find(x=>x.startsWith('const stable='));need(stableText===old.split('\n').find(x=>x.startsWith('const stable=')),2);
const nativeStable=new Function(stableText+';return stable;')();
const classifier=new Function(classifierText+';return ClassifyPackageMetadataFailure;')();
const oldStatement=inv.originalStatement,newStatement=delta.changes[2].new;
need(old.split(oldStatement).length===2&&marked.split(newStatement).length===2,3);
const original=new Function('assert','stable','a','e','MarkDependencyFailureStage','ClassifyPackageMetadataFailure',"'use strict';"+oldStatement);
const candidate=new Function('assert','stable','a','e','MarkDependencyFailureStage','ClassifyPackageMetadataFailure',"'use strict';"+newStatement);
function reject(fn,id){let caught=false,value;try{fn();}catch(e){caught=true;value=e;}need(caught&&value instanceof Error&&value.message==='dependency_control_'+id,90);}
async function group(i,fn){caseIndex=i;await fn();rows.push({name:expected[i-1],outcome:'PASS'});}
function equalEvents(a,b){need(a.length===b.length,10);for(let i=0;i<a.length;i++){need(a[i].length===b[i].length,10);for(let j=0;j<a[i].length;j++)need(Object.is(a[i][j],b[i][j]),10);}}
function run(fn,opts={}){
 const events=[],marks=[],a=opts.a??{version:'a'},e=opts.e??{version:'b'};let caught=false,value,phase=0;
 const eq=function(...args){events.push(['call',this,...args]);if(opts.fault==='call')throw opts.error;return assert.equal(...args);};
 const provider={};Object.defineProperty(provider,'equal',{get(){events.push(['get']);if(opts.fault==='get')throw opts.error;return eq;}});
 const stable=x=>{phase++;events.push(['stable',phase,x]);if(opts.fault==='actual'&&phase===1||opts.fault==='expected'&&phase===2)throw opts.error;return opts.texts?opts.texts[phase-1]:nativeStable(x);};
 const mark=x=>{marks.push(x);if(opts.markerFault)throw opts.error;};
 const classify=opts.classifier??classifier;
 try{fn(provider,stable,a,e,mark,classify);}catch(err){caught=true;value=err;}
 return {events,marks,caught,value,a,e,provider};
}
function normalizedEvents(r){return r.events.map(row=>row.map(v=>v===r.provider?'receiver':v===r.a?'actual-object':v===r.e?'expected-object':v));}
function samePair(opts){const a=run(original,opts),b=run(candidate,opts);equalEvents(normalizedEvents(a),normalizedEvents(b));need(a.caught===b.caught,11);if(opts.fault)need(Object.is(a.value,opts.error)&&Object.is(b.value,opts.error),12);else if(a.caught){need(a.value.code==='ERR_ASSERTION'&&b.value.code==='ERR_ASSERTION'&&a.value.operator===b.value.operator&&Object.is(a.value.actual,b.value.actual)&&Object.is(a.value.expected,b.value.expected),13);}return [a,b];}
function stage(opts,want){const r=run(candidate,opts);need(r.caught&&r.marks.length===1&&r.marks[0]===want,14);return r;}
function checkCategory(a,e,want){const x=classifier(true,true,a,e);need(x===want,15);return x;}
function helper(options={}){
 const writes=[];const state=new Function('writeFileSync','join',"'use strict';"+fixed+";return {mark:MarkDependencyFailureStage,write:WriteDependencyFailureMetadata,state(){return [dependencyFailureStage,dependencyFailureGroup];},setRoot(v){dependencyFailureRoot=v;},faultMark(){const saved=DEPENDENCY_FAILURE_STAGES.includes;DEPENDENCY_FAILURE_STAGES.includes=()=>{throw new Error('fixed-fault');};try{MarkDependencyFailureStage('pm-identity','pi');}finally{DEPENDENCY_FAILURE_STAGES.includes=saved;}}};")((p,b,o)=>{writes.push({p,b,o});if(options.fault)throw options.fault;if(options.real){fs.writeFileSync(p,b,o);knownFiles.add(p);}},path.join);state.setRoot(root);return {state,writes};
}
function record(h,error){h.state.write(error);need(h.writes.length>0,16);const w=h.writes.at(-1);need(Buffer.byteLength(w.b)<=512&&w.o.flag==='wx',17);const x=JSON.parse(w.b);need(Object.keys(x).sort().join('|')==='behavioralInvocations|code|group|outcome|schema|stage',18);need(!w.b.includes('secret')&&x.outcome==='FAILED'&&x.behavioralInvocations===0,19);return x;}
async function main(){
 need(process.platform==='win32'&&process.version==='v24.21.0'&&path.isAbsolute(root)&&!fs.existsSync(root),20);fs.mkdirSync(root);created=true;
 await group(1,()=>{
  need(sha(Buffer.from(old))===delta.original&&sha(Buffer.from(marked))===delta.candidate,21);let back=marked;
  for(const c of [...delta.changes].reverse()){need(back.split(c.new).length===2,22);back=back.replace(c.new,c.old);}need(back===old,23);
  let forward=old;for(const c of delta.changes){need(forward.split(c.old).length===2,24);forward=forward.replace(c.old,c.new);}need(forward===marked,25);
  const good=run(candidate,{a:{b:[1,2],a:true},e:{a:true,b:[1,2]}});need(!good.caught&&good.marks.length===0,26);
  samePair({a:{z:{y:1,x:null},a:[true,false]},e:{a:[true,false],z:{x:null,y:1}}});
  const pair=samePair({a:{version:'a'},e:{version:'b'}});need(pair[1].marks[0]==='pm-identity',27);
  const events=normalizedEvents(pair[1]);need(events.length===4&&events[0][0]==='get'&&events[1][0]==='stable'&&events[1][1]===1&&events[2][0]==='stable'&&events[2][1]===2&&events[3][0]==='call'&&events[3][1]==='receiver'&&events[3][4]==='metadata difference, including optional/platform flags',28);
 });
 await group(2,()=>{
  for(const fault of ['get','actual','expected','call'])for(const error of [new Error('secret-error'),'secret-string',null]){const [a,b]=samePair({fault,error});need(b.marks.length===1&&b.marks[0]===({get:'pm-no-actual',actual:'pm-no-actual',expected:'pm-no-expected',call:'pm-identity'})[fault],29);const x=run(candidate,{fault,error,markerFault:true});need(x.caught&&Object.is(x.value,error),30);}
  const eq=stage({texts:['{"x":1}','{"x":1}'],fault:'call',error:null},'pm-equal-call');need(eq.caught&&eq.value===null,31);
  stage({texts:['{"x":1}','{"x":1.0}']},'pm-unclassified');
  stage({texts:['{"a":1,"b":2}','{"b":2,"a":1}']},'pm-unclassified');
  // A source mutant that equates detached values must be detected by C1.
  const mutant=classifierText.replace("return 'pm-unclassified';\n } catch", "return 'pm-equal-call';\n } catch");need(mutant!==classifierText,32);const bad=new Function(mutant+';return ClassifyPackageMetadataFailure;')();
  reject(()=>need(bad(true,true,'{"x":1}','{"x":1.0}')==='pm-unclassified',33),33);
  const r=run(candidate,{fault:'call',error:'same',texts:['{}','{}']});const events=normalizedEvents(r);
  reject(()=>equalEvents(events,events.slice(1)),10);const reordered=events.slice();[reordered[1],reordered[2]]=[reordered[2],reordered[1]];reject(()=>equalEvents(events,reordered),10);
  const wrongReceiver=events.map(x=>x.slice());wrongReceiver[3][1]='wrong';reject(()=>equalEvents(events,wrongReceiver),10);
  reject(()=>need(Object.is(r.value,new Error('same')),12),12);
  const twice=newStatement.replace('metadataActualText=stable(a)','metadataActualText=(stable(a),stable(a))');need(twice!==newStatement,59);const tf=new Function('assert','stable','a','e','MarkDependencyFailureStage','ClassifyPackageMetadataFailure',"'use strict';"+twice);const tr=run(tf,{fault:'call',error:'same',texts:['{}','{}','{}']});reject(()=>equalEvents(events,normalizedEvents(tr)),10);
 });
 await group(3,()=>{
  const fields={identity:['version','resolved'],integrity:['integrity'],graph:['dependencies','optionalDependencies','peerDependencies','peerDependenciesMeta'],platform:['optional','dev','peer','devOptional','cpu','os','libc','engines'],lifecycle:['hasInstallScript','hasShrinkwrap','bin'],descriptive:['license','funding','deprecated'],other:['secret-unknown','__proto__']};
  for(const [g,ks]of Object.entries(fields))for(const k of ks){checkCategory(JSON.stringify({[k]:1}),'{}','pm-'+g);checkCategory(JSON.stringify({[k]:1}),JSON.stringify({[k]:2}),'pm-'+g);}
  checkCategory('{"version":1,"resolved":1}','{"version":2,"resolved":2}','pm-identity');checkCategory('{"version":1,"integrity":1}','{"version":2,"integrity":2}','pm-multiple');
  for(const [a,e]of [[{x:null},{}],[{x:false},{x:0}],[{x:0},{x:''}],[{x:[1,2]},{x:[2,1]}],[{x:{a:1}},{x:{a:2}}]]){samePair({a,e});checkCategory(nativeStable(a),nativeStable(e),'pm-other');}
  const bad=classifierText.replace("group='integrity'","group='identity'");need(bad!==classifierText,34);const f=new Function(bad+';return ClassifyPackageMetadataFailure;')();reject(()=>need(f(true,true,'{"integrity":1}','{"integrity":2}')==='pm-integrity',35),35);
  const m=classifierText.replace("if(groups.size>1)return 'pm-multiple';","if(groups.size>1)return 'pm-identity';");need(m!==classifierText,36);const mf=new Function(m+';return ClassifyPackageMetadataFailure;')();reject(()=>need(mf(true,true,'{"version":1,"integrity":1}','{}')==='pm-multiple',37),37);
 });
 await group(4,()=>{
  const big=JSON.stringify({x:'x'.repeat(16376)});need(Buffer.byteLength(big)===16384,38);checkCategory(big,'{"x":""}','pm-other');checkCategory(big+' ','{"x":""}','pm-unclassified');
  const utf=JSON.stringify({x:'é'.repeat(8188)});need(Buffer.byteLength(utf)===16384&&utf.length<16384,39);checkCategory(utf,'{}','pm-other');checkCategory(utf+' ','{}','pm-unclassified');
  const a=Object.fromEntries(Array.from({length:64},(_,i)=>['key'+i,i]));checkCategory(JSON.stringify(a),'{}','pm-other');a.extra=1;checkCategory(JSON.stringify(a),'{}','pm-unclassified');
  for(const [x,y,want]of [['{','{}','pm-unclassified'],['null','{}','pm-shape'],['[]','{}','pm-shape'],['1','2','pm-shape']])checkCategory(x,y,want);
  need(classifier(true,true,undefined,'{}')==='pm-unclassified',40);
  const wrongCap=classifierText.replace('keys.length>64','keys.length>65');need(wrongCap!==classifierText,41);const f=new Function(wrongCap+';return ClassifyPackageMetadataFailure;')();reject(()=>need(f(true,true,JSON.stringify(a),'{}')==='pm-unclassified',42),42);
  let queries=0;const originalRecord={get version(){queries++;return 'a';}};const r=run(candidate,{a:originalRecord,e:{version:'b'}});need(r.caught&&queries===1&&r.marks[0]==='pm-identity',43);
 });
 await group(5,()=>{
  const injected=Object.freeze({secret:'identity'});
  for(const opts of [{a:{version:1},e:{version:2}},{a:{optional:true},e:{optional:false}},{texts:['{"x":1}','{"x":1.0}']},{texts:['{}','{}'],fault:'call',error:injected},{a:{x:1},e:{x:1}}]){samePair(opts);}
  for(const [opts,want]of [[{a:{version:1},e:{version:2}},'pm-identity'],[{a:{optional:true},e:{optional:false}},'pm-platform'],[{texts:['{\"x\":1}','{\"x\":1.0}']},'pm-unclassified'],[{texts:['{}','{}'],fault:'call',error:injected},'pm-equal-call']])stage(opts,want);
  const throwing=()=>{throw new Error('secret-classifier');};const x=run(candidate,{fault:'call',error:injected,classifier:throwing});need(x.caught&&x.value===injected&&x.marks.length===0,44);
  const h=helper();h.state.mark('package-metadata','pi');const before=h.state.state();h.state.faultMark();need(JSON.stringify(h.state.state())===JSON.stringify(before),45);
  let gets=0;const err={};Object.defineProperty(err,'code',{get(){gets++;throw new Error('secret');}});need(record(h,err).code==='OTHER'&&gets===0,46);
  const nativeDescriptor=Object.getOwnPropertyDescriptor(JSON,'parse');try{Object.defineProperty(JSON,'parse',{...nativeDescriptor,value(){throw new Error('fixed-parser-fault');}});need(classifier(true,true,'{"x":1}','{}')==='pm-unclassified',47);}finally{Object.defineProperty(JSON,'parse',nativeDescriptor);}need(Object.getOwnPropertyDescriptor(JSON,'parse').value===nativeDescriptor.value,48);
  const saved=Object.getOwnPropertyDescriptor(Buffer,'byteLength');try{Object.defineProperty(Buffer,'byteLength',{...saved,value(){throw new Error('fixed-size-fault');}});need(classifier(true,true,'{}','{}')==='pm-unclassified',49);}finally{Object.defineProperty(Buffer,'byteLength',saved);}need(Object.getOwnPropertyDescriptor(Buffer,'byteLength').value===saved.value,50);
  const mutant=newStatement.replace('throw metadataError;','throw new Error("replacement");');need(mutant!==newStatement,51);const f=new Function('assert','stable','a','e','MarkDependencyFailureStage','ClassifyPackageMetadataFailure',"'use strict';"+mutant);const bad=run(f,{fault:'call',error:injected});reject(()=>need(bad.value===injected,52),52);
 });
 await group(6,()=>{
  for(const label of inv.labels){const h=helper();h.state.mark(label,'pi');const rec=record(h,{code:'ERR_ASSERTION',secret:'never-export'});need(rec.stage===label&&rec.group==='pi',53);}
  const h=helper({real:true});h.state.mark('pm-identity','pi');record(h,{code:'ERR_ASSERTION'});const file=path.join(root,'dependency-failure.json'),bytes=fs.readFileSync(file);need(bytes.length<=512&&JSON.parse(bytes).stage==='pm-identity',54);h.state.mark('pm-platform','adapter');h.state.write({code:'OTHER'});need(fs.readFileSync(file).equals(bytes)&&h.writes.length===2,55);
  const fail=helper({fault:injectedFault});fail.state.mark('pm-unclassified','pi');record(fail,{code:'ERR_ASSERTION'});need(fail.writes.length===1,56);
  need(schema.stages.includes('pm-equal-call')&&schema.stages.includes('pm-unclassified')&&rows.length===5&&expected.length===6,57);
 });
}
const injectedFault=new Error('writer-fault');
let outcome='FAILED';try{await main();outcome='PASS';}catch{}finally{try{if(created){for(const f of fs.readdirSync(root)){const full=path.join(root,f),st=fs.lstatSync(full);need(knownFiles.has(full)&&st.isFile()&&!st.isSymbolicLink(),58);}for(const f of knownFiles)fs.unlinkSync(f);fs.rmdirSync(root);cleaned=!fs.existsSync(root);}}catch{outcome='FAILED';}}
if(!created||!cleaned||rows.length!==6)outcome='FAILED';
const output={schema:'dependency-js-controls-v1',outcome,caseIndex,assertion,created,cleaned,results:outcome==='PASS'?rows:[],completed:rows.map(x=>x.name),setupInvocations:0,packageEntryExecutions:0,privateLogReads:0};
process.stdout.write(JSON.stringify(output));if(outcome!=='PASS')process.exitCode=1;
