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
const classifier=new Function(classifierText+'\n;return ClassifyPackageMetadataFailure;')();
const maskSource=classifierText;const encodeGroups=new Function(maskSource+'\n;return EncodePackageMetadataGroups;')();
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
 const writes=[];const state=new Function('writeFileSync','join',"'use strict';"+fixed+"\n;return {mark:MarkDependencyFailureStage,write:WriteDependencyFailureMetadata,state(){return [dependencyFailureStage,dependencyFailureGroup];},setRoot(v){dependencyFailureRoot=v;},faultMark(){const saved=DEPENDENCY_FAILURE_STAGES.includes;DEPENDENCY_FAILURE_STAGES.includes=()=>{throw new Error('fixed-fault');};try{MarkDependencyFailureStage('pm-identity','pi');}finally{DEPENDENCY_FAILURE_STAGES.includes=saved;}}};")((p,b,o)=>{writes.push({p,b,o});if(options.fault)throw options.fault;if(options.real){fs.writeFileSync(p,b,o);knownFiles.add(p);}},path.join);state.setRoot(root);return {state,writes};
}
function record(h,error){h.state.write(error);need(h.writes.length>0,16);const w=h.writes.at(-1);need(Buffer.byteLength(w.b)<=512&&w.o.flag==='wx',17);const x=JSON.parse(w.b);need(Object.keys(x).sort().join('|')==='behavioralInvocations|code|group|outcome|schema|stage',18);need(!w.b.includes('secret')&&x.outcome==='FAILED'&&x.behavioralInvocations===0,19);return x;}
async function main(){
 need(process.platform==='win32'&&process.version==='v24.21.0'&&path.isAbsolute(root)&&!fs.existsSync(root),20);fs.mkdirSync(root);created=true;
 await group(1,()=>{
  need(sha(Buffer.from(old))===delta.original&&sha(Buffer.from(marked))===delta.candidate,21);let back=marked;
  for(const c of [...delta.changes].reverse()){need(back.split(c.new).length===2,22);back=back.replace(c.new,c.old);}need(back===old,23);
  let forward=old;for(const c of delta.changes){need(forward.split(c.old).length===2,24);forward=forward.replace(c.old,c.new);}need(forward===marked,25);
  // Bind separator sensitivities to the exact five classifier factory expressions.
  const self=read('node-controls.mjs');
  const factoryBodies=[['classifierText',classifierText],['mutant',classifierText.replace("return 'pm-unclassified';\n } catch", "return 'pm-equal-call';\n } catch")],['bad',classifierText.replace("group='integrity'","group='identity'")],['m',classifierText.replace("if(groups.size>1){const stage=EncodePackageMetadataGroups(groups);if(stage==='pm-set-07')return versionContribution?(versionPartition??'pm-07-v-yes'):'pm-07-v-no';return stage;}","if(groups.size>1)return 'pm-identity';")],['wrongCap',classifierText.replace('keys.length>64','keys.length>65')]];
  for(const [index,[parameter,body]]of factoryBodies.entries()){
   const guardId=61+index;
   const expression='new Function('+parameter+"+'\\n;return ClassifyPackageMetadataFailure;')()";
   need(self.split(expression).length===2&&(parameter==='classifierText'||body!==classifierText),60);
   const built=new Function(parameter,'return '+expression)(body);need(typeof built==='function',guardId);
   const missingSeparator=expression.replace('\\n;return',';return');need(missingSeparator!==expression,67);
   const missing=new Function(parameter,'return '+missingSeparator)(body);need(missing===undefined,68);
   reject(()=>need(typeof missing==='function',guardId),guardId);
  }
  // Bind the sixth sensitivity to the actual helper factory, before its provider invocation.
  const helperStart='new '+'Function('+"'writeFileSync','join',",helperEnd=')('+'(p,b,o)=>';
  need(self.split(helperStart).length===2&&self.split(helperEnd).length===2,69);
  const helperAt=self.indexOf(helperStart),helperTo=self.indexOf(helperEnd,helperAt)+1;
  const helperExpression=self.slice(helperAt,helperTo);need(helperTo>helperAt&&helperExpression.split('\\n;return').length===2,69);
  let factoryWrites=0;const writer=()=>{factoryWrites++;};
  const helperExport=new Function('fixed','return '+helperExpression)(fixed)(writer,path.join);
  const helperShape=v=>v!==null&&typeof v==='object'&&Object.keys(v).sort().join('|')==='faultMark|mark|setRoot|state|write'&&Object.values(v).every(x=>typeof x==='function');
  need(helperShape(helperExport)&&factoryWrites===0,66);
  const missingHelperExpression=helperExpression.replace('\\n;return',';return');need(missingHelperExpression!==helperExpression,67);
  const missingHelper=new Function('fixed','return '+missingHelperExpression)(fixed)(writer,path.join);need(missingHelper===undefined&&factoryWrites===0,68);
  reject(()=>need(helperShape(missingHelper),66),66);
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
  const mutant=classifierText.replace("return 'pm-unclassified';\n } catch", "return 'pm-equal-call';\n } catch");need(mutant!==classifierText,32);const bad=new Function(mutant+'\n;return ClassifyPackageMetadataFailure;')();
  reject(()=>need(bad(true,true,'{"x":1}','{"x":1.0}')==='pm-unclassified',33),33);
  const r=run(candidate,{fault:'call',error:'same',texts:['{}','{}']});const events=normalizedEvents(r);
  reject(()=>equalEvents(events,events.slice(1)),10);const reordered=events.slice();[reordered[1],reordered[2]]=[reordered[2],reordered[1]];reject(()=>equalEvents(events,reordered),10);
  const wrongReceiver=events.map(x=>x.slice());wrongReceiver[3][1]='wrong';reject(()=>equalEvents(events,wrongReceiver),10);
  reject(()=>need(Object.is(r.value,new Error('same')),12),12);
  const twice=newStatement.replace('metadataActualText=stable(a)','metadataActualText=(stable(a),stable(a))');need(twice!==newStatement,59);const tf=new Function('assert','stable','a','e','MarkDependencyFailureStage','ClassifyPackageMetadataFailure',"'use strict';"+twice);const tr=run(tf,{fault:'call',error:'same',texts:['{}','{}','{}']});reject(()=>equalEvents(events,normalizedEvents(tr)),10);
 });
 await group(3,()=>{
   // Fixed partition of positive current07 only; no actual values leave the fixture.
   const baseExpected={version:'expected',integrity:'j',dependencies:{x:'2'}};
   const partitionCases=[
    [{integrity:'i',dependencies:{x:'1'}},baseExpected,'pm-07-v-absent'],
    ...[null,false,0,1,[],{},['a'],{x:'a'}].map(version=>[{version,integrity:'i',dependencies:{x:'1'}},baseExpected,'pm-07-v-nonstr']),
    ...['actual','', 'é'].map(version=>[{version,integrity:'i',dependencies:{x:'1'}},baseExpected,'pm-07-v-string']),
    [{version:'same',resolved:'a',integrity:'i',dependencies:{x:'1'}},{version:'same',resolved:'b',integrity:'j',dependencies:{x:'2'}},'pm-07-v-no'],
    [{version:'a',integrity:'i',dependencies:{x:'1'}},{integrity:'j',dependencies:{x:'2'}},'pm-07-v-yes'],
    ...[null,false,0,{},[]].map(version=>[{version:'a',integrity:'i',dependencies:{x:'1'}},{version,integrity:'j',dependencies:{x:'2'}},'pm-07-v-yes'])
   ];
   for(const [a,e,want]of partitionCases){checkCategory(nativeStable(a),nativeStable(e),want);const pair=samePair({a,e});need(pair[1].marks[0]===want,86);}
   // Absent branch uses expected invariant, not a forbidden short-circuit value read.
   // Arbitrary absent/nonstring expected inputs are outside that source invariant.
   const pActual=JSON.stringify(partitionCases[0][0]),pExpected=JSON.stringify(baseExpected);
   const presentActual=JSON.stringify({version:1,integrity:'i',dependencies:{x:'1'}});
   const partitionMutants=[
    ["actualOwn===false&&expectedOwn===true","expectedOwn===false&&actualOwn===true",pActual,pExpected,'pm-07-v-absent'],
    ["typeof expectedValue==='string'","true",JSON.stringify({version:'a',integrity:'i',dependencies:{x:'1'}}),JSON.stringify({version:1,integrity:'j',dependencies:{x:'2'}}),'pm-07-v-yes'],
    ["typeof actualValue==='string'?'pm-07-v-string':'pm-07-v-nonstr'","typeof actualValue==='string'?'pm-07-v-nonstr':'pm-07-v-string'",presentActual,pExpected,'pm-07-v-nonstr']
   ];
   for(const [from,to,a,e,want]of partitionMutants){need(classifierText.split(from).length===2,91);const text=classifierText.replace(from,to);const fn=new Function(text+'\n;return ClassifyPackageMetadataFailure;')();reject(()=>need(fn(true,true,a,e)===want,86),86);}
   // Same current07 pair: presence/type/value contribution, not version-string drift.
   const versionCases=[
    [{version:'a',integrity:'i',dependencies:{x:'1'}},{version:'b',integrity:'j',dependencies:{x:'2'}},'pm-07-v-string'],
    [{version:'v',resolved:'a',integrity:'i',dependencies:{x:'1'}},{version:'v',resolved:'b',integrity:'j',dependencies:{x:'2'}},'pm-07-v-no'],
    [{version:'a',resolved:'a',integrity:'i',dependencies:{x:'1'}},{version:'b',resolved:'b',integrity:'j',dependencies:{x:'2'}},'pm-07-v-string'],
    [{version:null,integrity:'i',dependencies:{x:'1'}},{integrity:'j',dependencies:{x:'2'}},'pm-07-v-yes'],
    [{version:1,integrity:'i',dependencies:{x:'1'}},{version:'1',integrity:'j',dependencies:{x:'2'}},'pm-07-v-nonstr']
   ];
   for(const [a,e,want]of versionCases){checkCategory(nativeStable(a),nativeStable(e),want);const pair=samePair({a,e});need(pair[1].marks.length===1&&pair[1].marks[0]===want,78);}
   const posA='{"version":"a","integrity":"i","dependencies":{"x":"1"}}',posE='{"version":"b","integrity":"j","dependencies":{"x":"2"}}';
   const negA='{"version":"v","resolved":"a","integrity":"i","dependencies":{"x":"1"}}',negE='{"version":"v","resolved":"b","integrity":"j","dependencies":{"x":"2"}}';
   for(const [from,to,input,want,id]of [
    ["if(key==='version')versionContribution=true;","if(key==='version')versionContribution=false;",[posA,posE],'pm-07-v-string',78],
    ["let versionContribution=false;","let versionContribution=true;",[negA,negE],'pm-07-v-no',79],
    ["if(key==='version')versionContribution=true;","if(key==='resolved')versionContribution=true;",[posA,posE],'pm-07-v-string',78]
   ]){need(classifierText.split(from).length===2,80);const body=classifierText.replace(from,to);const mutated=new Function(body+'\n;return ClassifyPackageMetadataFailure;')();reject(()=>need(mutated(true,true,...input)===want,id),id);}
   // Removing the current07 gate must fail for the existing03 baseline.
   const withoutGate=classifierText.replace("if(stage==='pm-set-07')",'if(true)');need(withoutGate!==classifierText,80);const unguarded=new Function(withoutGate+'\n;return ClassifyPackageMetadataFailure;')();reject(()=>need(unguarded(true,true,'{"version":1,"integrity":1}','{}')==='pm-set-03',81),81);
  // Exhaustive fixed seven-category sets, with independent numeric mask oracle.
  const representatives=['version','integrity','dependencies','optional','bin','license','secret-unknown'];
  const groupNames=['identity','integrity','graph','platform','lifecycle','descriptive','other'];
  const wantLabels=[];
  for(let mask=0;mask<128;mask++){
   const selected=groupNames.filter((g,i)=>mask&(1<<i));if(selected.length<2)continue;
   const actual=Object.fromEntries(representatives.filter((k,i)=>mask&(1<<i)).map(k=>[k,1]));
   const maskWant='pm-set-'+mask.toString(16).padStart(2,'0');wantLabels.push(maskWant);const want=mask===7?'pm-07-v-yes':maskWant;
   checkCategory(nativeStable(actual),'{}',want);
   const reversed=Object.fromEntries(Object.entries(actual).reverse());checkCategory(JSON.stringify(reversed),'{}',want);
   need(encodeGroups(new Set(selected))===maskWant,72);
   const pair=samePair({a:actual,e:{}});need(pair[1].marks.length===1&&pair[1].marks[0]===want,72);
  }
  need(wantLabels.length===120&&JSON.stringify(wantLabels)===JSON.stringify(inv.maskLabels),71);
  const swappedBits=classifierText.replace('mask|=1<<index','mask|=1<<(6-index)');need(swappedBits!==classifierText,70);
  const swappedClassifier=new Function(swappedBits+'\n;return ClassifyPackageMetadataFailure;')();
  reject(()=>need(swappedClassifier(true,true,'{"version":1,"integrity":1}','{}')==='pm-set-03',72),72);
  const fields={identity:['version','resolved'],integrity:['integrity'],graph:['dependencies','optionalDependencies','peerDependencies','peerDependenciesMeta'],platform:['optional','dev','peer','devOptional','cpu','os','libc','engines'],lifecycle:['hasInstallScript','hasShrinkwrap','bin'],descriptive:['license','funding','deprecated'],other:['secret-unknown','__proto__']};
  for(const [g,ks]of Object.entries(fields))for(const k of ks){checkCategory(JSON.stringify({[k]:1}),'{}','pm-'+g);checkCategory(JSON.stringify({[k]:1}),JSON.stringify({[k]:2}),'pm-'+g);}
  checkCategory('{"version":1,"resolved":1}','{"version":2,"resolved":2}','pm-identity');checkCategory('{"version":1,"integrity":1}','{"version":2,"integrity":2}','pm-set-03');
  for(const [a,e]of [[{x:null},{}],[{x:false},{x:0}],[{x:0},{x:''}],[{x:[1,2]},{x:[2,1]}],[{x:{a:1}},{x:{a:2}}]]){samePair({a,e});checkCategory(nativeStable(a),nativeStable(e),'pm-other');}
  const bad=classifierText.replace("group='integrity'","group='identity'");need(bad!==classifierText,34);const f=new Function(bad+'\n;return ClassifyPackageMetadataFailure;')();reject(()=>need(f(true,true,'{"integrity":1}','{"integrity":2}')==='pm-integrity',35),35);
  const m=classifierText.replace("if(groups.size>1){const stage=EncodePackageMetadataGroups(groups);if(stage==='pm-set-07')return versionContribution?(versionPartition??'pm-07-v-yes'):'pm-07-v-no';return stage;}","if(groups.size>1)return 'pm-identity';");need(m!==classifierText,36);const mf=new Function(m+'\n;return ClassifyPackageMetadataFailure;')();reject(()=>need(mf(true,true,'{"version":1,"integrity":1}','{}')==='pm-set-03',37),37);
 });
 await group(4,()=>{
   // Exact predecessor vs candidate observations, including lookup receivers and short circuits.
   let partitionBefore=marked;for(const c of [...inv.partitionChanges].reverse()){need(partitionBefore.split(c.new).length===2,91);partitionBefore=partitionBefore.replace(c.new,c.old);}need(sha(Buffer.from(partitionBefore))===inv.partitionPredecessor,91);
   const partitionBeforeText=extract(partitionBefore,'// BEGIN PACKAGE METADATA FAILURE CLASSIFIER','// END PACKAGE METADATA FAILURE CLASSIFIER');
   function partitionObserve(text,a,e,{fault='',nonboolean=false}={}){
    const events=[];let parsed=0,hasGets=0,hasCalls=0,stringGets=0,stringCalls=0;
    const tags=new WeakMap();const token=v=>v===null?'null':typeof v==='object'?'structured':typeof v+':'+String(v);
    const fail=point=>{if(fault===point)throw new Error('partition-provider-'+point);};
    const object={keys(value){events.push(['keys',tags.get(value),this===object]);return Object.keys(value);},get hasOwn(){const n=++hasGets;events.push(['has-get',n]);fail('has-get-'+n);return function(value,key){const m=++hasCalls;events.push(['has-call',m,tags.get(value),key,this===object]);fail('has-call-'+m);return nonboolean&&key==='version'?'nonboolean':Object.hasOwn(value,key);};}};
    const json={parse(value){const n=++parsed;events.push(['parse',n,this===json]);fail('parse-'+n);const raw=JSON.parse(value);const tag=n===1?'actual':'expected';const proxy=new Proxy(raw,{ownKeys(t){events.push(['ownKeys',tag]);return Reflect.ownKeys(t);},getOwnPropertyDescriptor(t,k){events.push(['descriptor',tag,String(k)]);fail('descriptor-'+tag+'-'+String(k));return Reflect.getOwnPropertyDescriptor(t,k);},get(t,k,recv){events.push(['value',tag,String(k),recv===proxy]);fail('value-'+tag+'-'+String(k));return Reflect.get(t,k,recv);}});tags.set(proxy,tag);return proxy;},get stringify(){const n=++stringGets;events.push(['stringify-get',n]);fail('stringify-get-'+n);return function(value){const m=++stringCalls;events.push(['stringify-call',m,this===json,token(value)]);fail('stringify-call-'+m);return JSON.stringify(value);};}};
    const fn=new Function('Object','JSON',text+'\n;return ClassifyPackageMetadataFailure;')(object,json);
    return {stage:fn(true,true,a,e),events,hasGets,hasCalls,stringGets,stringCalls};
   }
   const pa='{"version":"actual","integrity":"i","dependencies":{"x":"1"}}',pe='{"version":"expected","integrity":"j","dependencies":{"x":"2"}}',missing='{"integrity":"i","dependencies":{"x":"1"}}';
   const scenarios=[[pa,pe,'pm-07-v-string'],[missing,pe,'pm-07-v-absent'],['{"version":null,"integrity":"i","dependencies":{"x":"1"}}',pe,'pm-07-v-nonstr'],[pa,'{"integrity":"j","dependencies":{"x":"2"}}','pm-07-v-yes'],[pa,'{"version":null,"integrity":"j","dependencies":{"x":"2"}}','pm-07-v-yes']];
   for(const [a,e,want]of scenarios){const oldObs=partitionObserve(partitionBeforeText,a,e),newObs=partitionObserve(classifierText,a,e);equalEvents(oldObs.events,newObs.events);need(oldObs.stage==='pm-07-v-yes'&&newObs.stage===want,86);}
   const full=partitionObserve(classifierText,pa,pe);need(full.hasGets===6&&full.hasCalls===6&&full.stringGets===6&&full.stringCalls===6,89);
   const v=full.events.filter(x=>x[0]==='has-call'&&x[3]==='version');equalEvents(v,[['has-call',1,'actual','version',true],['has-call',2,'expected','version',true]]);
   const valueAt=full.events.findIndex(x=>x[0]==='value'&&x[1]==='actual'&&x[2]==='version');
   equalEvents(full.events.slice(valueAt-1,valueAt+5),[['stringify-get',1],['value','actual','version',true],['stringify-call',1,true,'string:actual'],['stringify-get',2],['value','expected','version',true],['stringify-call',2,true,'string:expected']]);
   for(const [a,e]of [[missing,pe],[pa,'{"integrity":"j","dependencies":{"x":"2"}}']]){
    const o=partitionObserve(classifierText,a,e);need(o.events.filter(x=>x[0]==='value'&&x[2]==='version').length===0&&o.stringCalls===4,89);
    for(const fault of ['value-actual-version','value-expected-version']){const q=partitionObserve(classifierText,a,e,{fault});equalEvents(q.events,o.events);need(q.stage===o.stage,89);}
   }
   for(const fault of ['parse-1','parse-2','has-get-1','has-get-2','has-call-1','has-call-2','descriptor-actual-version','descriptor-expected-version','value-actual-version','value-expected-version','stringify-get-1','stringify-get-2','stringify-call-1','stringify-call-2','value-actual-integrity','stringify-call-3']){
    const before=partitionObserve(partitionBeforeText,pa,pe,{fault}),after=partitionObserve(classifierText,pa,pe,{fault});equalEvents(before.events,after.events);need(before.stage==='pm-unclassified'&&after.stage==='pm-unclassified',87);
   }
   const invalidOwn=partitionObserve(classifierText,pa,pe,{nonboolean:true});need(invalidOwn.stage==='pm-07-v-yes',87);
   // Each mutation must fail its intended exact oracle; unrelated throws cannot pass.
   const extraProperty=classifierText.replace("if(key==='version')versionContribution=true;","if(key==='version'){expected[key];versionContribution=true;}");need(extraProperty!==classifierText,91);reject(()=>equalEvents(partitionObserve(partitionBeforeText,missing,pe).events,partitionObserve(extraProperty,missing,pe).events),10);
   const reversedOwn=classifierText.replace('(actualOwn=Object.hasOwn(actual,key))===(expectedOwn=Object.hasOwn(expected,key))','(expectedOwn=Object.hasOwn(expected,key))===(actualOwn=Object.hasOwn(actual,key))');need(reversedOwn!==classifierText,91);reject(()=>equalEvents(full.events,partitionObserve(reversedOwn,pa,pe).events),10);
   const lostReceiver=classifierText.replace('actualOwn=Object.hasOwn(actual,key)','actualOwn=(0,Object.hasOwn)(actual,key)');need(lostReceiver!==classifierText,91);reject(()=>equalEvents(full.events,partitionObserve(lostReceiver,pa,pe).events),10);
   const earlyPartition=classifierText.replace("if(key==='version')versionContribution=true;","if(key==='version')return 'pm-07-v-string';");need(earlyPartition!==classifierText,91);reject(()=>need(partitionObserve(earlyPartition,pa,pe,{fault:'stringify-call-3'}).stage==='pm-unclassified',87),87);
   const badUnavailable=classifierText.replace("} catch {return 'pm-unclassified';}","} catch {return 'pm-07-v-absent';}");need(badUnavailable!==classifierText,91);reject(()=>need(partitionObserve(badUnavailable,pa,pe,{fault:'parse-1'}).stage==='pm-unclassified',87),87);
   // Compare actual predecessor/candidate classifier observations on detached providers.
   let predecessor=marked;for(const change of [...inv.versionChanges].reverse()){need(predecessor.split(change.new).length===2,80);predecessor=predecessor.replace(change.new,change.old);}need(sha(Buffer.from(predecessor))===inv.versionPredecessor,80);
   const predecessorText=extract(predecessor,'// BEGIN PACKAGE METADATA FAILURE CLASSIFIER','// END PACKAGE METADATA FAILURE CLASSIFIER');
   function observeClassifier(text,a,e,fault=0){
    const events=[];let parseCalls=0,stringifyCalls=0;
    const json={parse(value){parseCalls++;events.push(['parse',parseCalls,value]);const parsed=JSON.parse(value);if(parseCalls===fault)throw new Error('parse-provider');if(parsed===null||typeof parsed!=='object')return parsed;return new Proxy(parsed,{ownKeys(target){events.push(['keys',parseCalls]);return Reflect.ownKeys(target);},getOwnPropertyDescriptor(target,key){events.push(['own',String(key)]);return Reflect.getOwnPropertyDescriptor(target,key);},get(target,key,receiver){events.push(['value',String(key)]);return Reflect.get(target,key,receiver);}});},stringify(value){stringifyCalls++;events.push(['stringify',stringifyCalls,JSON.stringify(value)]);if(fault===3&&stringifyCalls===3)throw new Error('late-provider');return JSON.stringify(value);}};
    const fn=new Function('JSON',text+'\n;return ClassifyPackageMetadataFailure;')(json);const stage=fn(true,true,a,e);return {events,stage,parseCalls,stringifyCalls};
   }
   const pairText=['{"version":"a","integrity":"i","dependencies":{"x":"1"}}','{"version":"b","integrity":"j","dependencies":{"x":"2"}}'];
   const priorSeen=observeClassifier(predecessorText,...pairText),newSeen=observeClassifier(classifierText,...pairText);equalEvents(priorSeen.events,newSeen.events);need(priorSeen.stage==='pm-set-07'&&newSeen.stage==='pm-07-v-string'&&newSeen.parseCalls===2&&newSeen.stringifyCalls===6,82);
   for(const fault of [1,2,3]){const before=observeClassifier(predecessorText,...pairText,fault),after=observeClassifier(classifierText,...pairText,fault);equalEvents(before.events,after.events);need(before.stage==='pm-unclassified'&&after.stage==='pm-unclassified',81);}
   // A recomparison mutation must fail the exact observation ledger.
   const extraRead=classifierText.replace("if(key==='version')versionContribution=true;","if(key==='version'){JSON.stringify(actual[key]);versionContribution=true;}");need(extraRead!==classifierText,80);const observedExtra=observeClassifier(extraRead,...pairText);reject(()=>equalEvents(priorSeen.events,observedExtra.events),10);
   for(const [ar,er,a,e,want]of [
    [false,true,pairText[0],pairText[1],'pm-no-actual'],[true,false,pairText[0],pairText[1],'pm-no-expected'],
    [true,true,'{}','{}','pm-equal-call'],[true,true,'{"x":1}','{"x":1.0}','pm-unclassified'],
    [true,true,'{','{}','pm-unclassified'],[true,true,'null','{}','pm-shape'],
    [true,true,JSON.stringify({version:'x'.repeat(16384)}),'{}','pm-unclassified'],
    [true,true,JSON.stringify(Object.fromEntries(Array.from({length:65},(_,i)=>['k'+i,i]))),'{}','pm-unclassified'],
    [true,true,'{"version":1}','{}','pm-identity']
   ])need(classifier(ar,er,a,e)===want,81);
   // An early-return mutation must be refused after a late provider failure.
   const early=classifierText.replace("if(key==='version')versionContribution=true;","if(key==='version')return 'pm-07-v-yes';");need(early!==classifierText,80);reject(()=>need(observeClassifier(early,...pairText,3).stage==='pm-unclassified',81),81);
  // Native Set boundary and controlled size/has providers; never original metadata objects.
  const names=['identity','integrity','graph','platform','lifecycle','descriptive','other'];
  function groupProvider(selected,{size=selected.length,fault=-1,nonboolean=-1}={}){
   const events=[];const object={get size(){events.push(['size']);return size;},get has(){const index=events.filter(x=>x[0]==='get-has').length;events.push(['get-has',index]);if(index===fault)throw new Error('set-get-fault');return function(name){events.push(['has',index,this===object,name]);if(index===nonboolean)return 'truthy';return selected.includes(name);};}};
   return {object,events};
  }
  const expectedSetEvents=()=>[['size'],...names.flatMap((name,index)=>[['get-has',index],['has',index,true,name]])];
  const positive=groupProvider(['identity','integrity']);need(encodeGroups(positive.object)==='pm-set-03',72);equalEvents(positive.events,expectedSetEvents());
  need(encodeGroups(new Set(names))==='pm-set-7f',72);
  for(const size of [0,1,8,-1,2.5,null,true,'2',undefined]){
   const x=groupProvider(['identity','integrity'],{size});
   // Explicit undefined must not be converted to the omitted default size.
   if(size===undefined)Object.defineProperty(x.object,'size',{get(){x.events.push(['size']);return undefined;}});
   need(encodeGroups(x.object)==='pm-multiple',74);equalEvents(x.events,[['size']]);
  }
  need(encodeGroups(null)==='pm-multiple',74);
  need(encodeGroups(new Set(['identity','integrity','unknown']))==='pm-multiple',74);
  const duplicateModel=groupProvider(['identity','integrity'],{size:3});need(encodeGroups(duplicateModel.object)==='pm-multiple',74);equalEvents(duplicateModel.events,expectedSetEvents());
  for(let index=0;index<7;index++){
   const fault=groupProvider(['identity','integrity'],{fault:index});need(encodeGroups(fault.object)==='pm-multiple',74);
   equalEvents(fault.events,expectedSetEvents().slice(0,1+index*2+1));
   const typed=groupProvider(['identity','integrity'],{nonboolean:index});need(encodeGroups(typed.object)==='pm-multiple',75);
   equalEvents(typed.events,expectedSetEvents().slice(0,1+(index+1)*2));
  }
  const extraQuery=classifierText.replace('const size=groups.size;','groups.has(\'identity\');const size=groups.size;');need(extraQuery!==classifierText,70);
  const extraEncoder=new Function(extraQuery+'\n;return EncodePackageMetadataGroups;')();const q=groupProvider(['identity','integrity']);need(extraEncoder(q.object)==='pm-set-03',72);
  reject(()=>equalEvents(q.events,expectedSetEvents()),10);
  const wrongFallback=classifierText.replace("catch {return 'pm-multiple';}","catch {return 'pm-set-03';}");need(wrongFallback!==classifierText,70);
  const faultEncoder=new Function(wrongFallback+'\n;return EncodePackageMetadataGroups;')();
  reject(()=>need(faultEncoder(null)==='pm-multiple',74),74);
  const big=JSON.stringify({x:'x'.repeat(16376)});need(Buffer.byteLength(big)===16384,38);checkCategory(big,'{"x":""}','pm-other');checkCategory(big+' ','{"x":""}','pm-unclassified');
  const utf=JSON.stringify({x:'é'.repeat(8188)});need(Buffer.byteLength(utf)===16384&&utf.length<16384,39);checkCategory(utf,'{}','pm-other');checkCategory(utf+' ','{}','pm-unclassified');
  const a=Object.fromEntries(Array.from({length:64},(_,i)=>['key'+i,i]));checkCategory(JSON.stringify(a),'{}','pm-other');a.extra=1;checkCategory(JSON.stringify(a),'{}','pm-unclassified');
  for(const [x,y,want]of [['{','{}','pm-unclassified'],['null','{}','pm-shape'],['[]','{}','pm-shape'],['1','2','pm-shape']])checkCategory(x,y,want);
  need(classifier(true,true,undefined,'{}')==='pm-unclassified',40);
  const wrongCap=classifierText.replace('keys.length>64','keys.length>65');need(wrongCap!==classifierText,41);const f=new Function(wrongCap+'\n;return ClassifyPackageMetadataFailure;')();reject(()=>need(f(true,true,JSON.stringify(a),'{}')==='pm-unclassified',42),42);
  let queries=0;const originalRecord={get version(){queries++;return 'a';}};const r=run(candidate,{a:originalRecord,e:{version:'b'}});need(r.caught&&queries===1&&r.marks[0]==='pm-identity',43);
 });
 await group(5,()=>{
   const partitionExpected={version:'expected',integrity:'j',dependencies:{x:'2'}},partitionMissing={integrity:'i',dependencies:{x:'1'}},partitionTyped={version:null,integrity:'i',dependencies:{x:'1'}},partitionString={version:'actual',integrity:'i',dependencies:{x:'1'}};
   const unsupportedExpected={integrity:'j',dependencies:{x:'2'}};
   for(const [a,e,want]of [[partitionMissing,partitionExpected,'pm-07-v-absent'],[partitionTyped,partitionExpected,'pm-07-v-nonstr'],[partitionString,partitionExpected,'pm-07-v-string'],[partitionString,unsupportedExpected,'pm-07-v-yes'],[{version:1},{version:2},'pm-identity'],[partitionMissing,partitionExpected,'pm-07-v-absent'],[partitionString,partitionString,null]]){const pair=samePair({a,e});need(want===null?pair[1].marks.length===0:pair[1].marks[0]===want,88);}
   const freshPartition='let versionPartition=null;';need(classifierText.split(freshPartition).length===2,91);const hoisted=freshPartition+'\n'+classifierText.replace(freshPartition,'');const stalePartition=new Function(hoisted+'\n;return ClassifyPackageMetadataFailure;')();
   need(stalePartition(true,true,nativeStable(partitionMissing),nativeStable(partitionExpected))==='pm-07-v-absent',88);
   reject(()=>need(stalePartition(true,true,nativeStable(partitionString),nativeStable(unsupportedExpected))==='pm-07-v-yes',88),88);
   for(const a of [partitionMissing,partitionTyped,partitionString])for(const error of [new Error('partition-fault'),'same-partition',null]){
    const pair=samePair({a,e:partitionExpected,fault:'call',error});need(Object.is(pair[1].value,error),84);
    const markedFault=run(candidate,{a,e:partitionExpected,fault:'call',error,markerFault:true});need(markedFault.caught&&Object.is(markedFault.value,error),84);
   }
   const savedPartitionParser=Object.getOwnPropertyDescriptor(JSON,'parse');try{Object.defineProperty(JSON,'parse',{...savedPartitionParser,value(){throw new Error('partition-parse-fault');}});need(classifier(true,true,nativeStable(partitionMissing),nativeStable(partitionExpected))==='pm-unclassified',87);}finally{Object.defineProperty(JSON,'parse',savedPartitionParser);}
   need(Object.getOwnPropertyDescriptor(JSON,'parse').value===savedPartitionParser.value,85);checkCategory(nativeStable(partitionTyped),nativeStable(partitionExpected),'pm-07-v-nonstr');
   const yesA={version:'a',integrity:'i',dependencies:{x:'1'}},yesE={version:'b',integrity:'j',dependencies:{x:'2'}};
   const noA={version:'v',resolved:'a',integrity:'i',dependencies:{x:'1'}},noE={version:'v',resolved:'b',integrity:'j',dependencies:{x:'2'}};
   for(const [a,e,want]of [[yesA,yesE,'pm-07-v-string'],[noA,noE,'pm-07-v-no'],[{version:'a'},{version:'a'},null],[yesA,yesE,'pm-07-v-string'],[{x:1},{},'pm-other'],[noA,noE,'pm-07-v-no']]){const pair=samePair({a,e});need(want===null?pair[1].marks.length===0:pair[1].marks[0]===want,83);}
   const declaration='let versionContribution=false;';need(classifierText.split(declaration).length===2,80);const stale=declaration+'\n'+classifierText.replace(declaration,'');const staleFn=new Function(stale+'\n;return ClassifyPackageMetadataFailure;')();need(staleFn(true,true,nativeStable(yesA),nativeStable(yesE))==='pm-07-v-string',83);reject(()=>need(staleFn(true,true,nativeStable(noA),nativeStable(noE))==='pm-07-v-no',83),83);
   for(const [a,e,want]of [[yesA,yesE,'pm-07-v-string'],[noA,noE,'pm-07-v-no']])for(const error of [new Error('version-provider'),'same-version',null]){
    const pair=samePair({a,e,fault:'call',error});need(pair[1].marks[0]===want&&Object.is(pair[1].value,error),84);
    const failedMark=run(candidate,{a,e,fault:'call',error,markerFault:true});need(failedMark.caught&&Object.is(failedMark.value,error),84);
   }
   const versionSetDescriptor=Object.getOwnPropertyDescriptor(Set.prototype,'has');
   try{Object.defineProperty(Set.prototype,'has',{...versionSetDescriptor,value(){throw new Error('version-encoder-fault');}});checkCategory(nativeStable(noA),nativeStable(noE),'pm-multiple');}finally{Object.defineProperty(Set.prototype,'has',versionSetDescriptor);}
   need(Object.getOwnPropertyDescriptor(Set.prototype,'has').value===versionSetDescriptor.value,85);checkCategory(nativeStable(noA),nativeStable(noE),'pm-07-v-no');
  for(const [a,e,want]of [[{version:1,integrity:1},{},'pm-set-03'],[{optional:true,license:'x'},{},'pm-set-28'],[{version:1},{version:2},'pm-identity'],[{version:1},{version:1},null]]){
   const pair=samePair({a,e});need(want===null?pair[1].marks.length===0:pair[1].marks.length===1&&pair[1].marks[0]===want,76);
  }
  for(const fault of ['get','actual','expected','call'])for(const error of [new Error('mask-error'),'same-mask',null]){
   const pair=samePair({a:{version:1,integrity:1},e:{},fault,error});
   need(pair[1].marks[0]===({get:'pm-no-actual',actual:'pm-no-actual',expected:'pm-no-expected',call:'pm-set-03'})[fault],76);
   const m=run(candidate,{a:{version:1,integrity:1},e:{},fault,error,markerFault:true});need(m.caught&&Object.is(m.value,error),12);
  }
  const setDescriptor=Object.getOwnPropertyDescriptor(Set.prototype,'has');
  try{
   Object.defineProperty(Set.prototype,'has',{...setDescriptor,value(){throw new Error('mask-has-fault');}});
   checkCategory('{"version":1,"integrity":1}','{}','pm-multiple');
  }finally{Object.defineProperty(Set.prototype,'has',setDescriptor);}
  need(Object.getOwnPropertyDescriptor(Set.prototype,'has').value===setDescriptor.value,77);
  checkCategory('{"version":1,"integrity":1}','{}','pm-set-03');
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
