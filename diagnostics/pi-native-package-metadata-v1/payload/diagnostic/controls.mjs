import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync,openSync,closeSync,writeSync,mkdtempSync,rmSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { PassThrough,Writable } from 'node:stream';
import * as readline from 'node:readline';
import { fileURLToPath } from 'node:url';
import { dirname,join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { createDiagnostic,SOURCE } from './collector.mjs';
import { parseFrames } from './parser.mjs';
const here=dirname(fileURLToPath(import.meta.url));
const original=readFileSync(join(here,'../pinned-sources/pi-acp/package/dist/index.js'),'utf8');
assert.equal(createHash('sha256').update(original).digest('hex'),SOURCE);
const marked=readFileSync(join(here,'index.instrumented.mjs'),'utf8');
function extract(s,start,end){const i=s.indexOf(start),j=s.indexOf(end,i);assert(i>=0&&j>i);return s.slice(i,j);}
function deferred(){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};}
const tick=()=>new Promise(resolve=>setImmediate(resolve));
function harness(s,diagnosticWriter){
 const updates=[],commands=[],frames=[];let uuid=0;
 const diag=createDiagnostic({fd:3,caseName:'absolute',adapter:1,writer:diagnosticWriter||((_fd,b)=>{frames.push(Buffer.from(b));return b.length;})});
 const ctx=vm.createContext({DIAG:diag,crypto:{randomUUID:()=>`request-${++uuid}`},readline,setTimeout,clearTimeout,console,
  expandSlashCommand:m=>m,SESSION_STATS_TIMEOUT_MS:1000,toUsageUpdate:()=>null,promptToPiMessage:p=>({message:p[0].text,images:[]}),
  RequestError:{authRequired:(_x,m)=>new Error(m)},getAuthMethods:()=>[]});
 const code=extract(s,'var PiRpcProcess =','// src/acp/auth-required.ts')+extract(s,'function maybeAuthRequiredError','// src/acp/session-store.ts')+extract(s,'var PiAcpSession =','// src/acp/pi-sessions.ts')+extract(s,'var PiAcpAgent =','// src/index.ts')+'\n({PiRpcProcess,PiAcpSession,PiAcpAgent})';
 const C=vm.runInContext(code,ctx,{timeout:1000});
 const child=new EventEmitter();child.stdout=new PassThrough();child.stdin=new Writable({write(chunk,_enc,cb){commands.push(JSON.parse(chunk.toString()));cb();}});
 const proc=new C.PiRpcProcess(child);
 const session=new C.PiAcpSession({sessionId:'synthetic-session',cwd:'.',mcpServers:[],proc,conn:{sessionUpdate:x=>{updates.push(JSON.parse(JSON.stringify(x)));return Promise.resolve();}},fileCommands:[]});
 // Fixture stubs avoid unrelated stats RPC. Both variants use the same controlled await.
 session.publishContextUsage=async()=>{};
 const agent=Object.create(C.PiAcpAgent.prototype);agent.restoreSession=async()=>session;
 const call=message=>agent.prompt({sessionId:'synthetic-session',prompt:[{type:'text',text:message}]});
 const response=(index,success=true,error)=>child.stdout.write(JSON.stringify({type:'response',id:commands[index].id,success,error})+'\n');
 const close=()=>{child.stdout.end();child.stdin.end();};
 return {call,response,proc,session,child,updates,commands,frames,diag,close};
}
async function scenario(s,kind,diagnosticWriter){
 const h=harness(s,diagnosticWriter),settled=[];let first,second;
 if(kind==='write-error')h.proc.writeLine=()=>Promise.reject(new Error('synthetic write failure'));
 try{
  first=h.call(kind==='queued'||kind.startsWith('late')?'first:created':'first:cancel');first.then(x=>settled.push(['first',x]),e=>settled.push(['first-error',e.message]));await tick();
  if(kind==='queued'||kind.startsWith('late')){second=h.call('first:cancel');second.then(x=>settled.push(['second',x]),e=>settled.push(['second-error',e.message]));await tick();}
  if(kind==='success'){h.response(0);await tick();h.session.handlePiEvent({type:'agent_settled'});await first;}
  else if(kind==='cancel'){h.response(0);await tick();const cancellation=h.session.cancel();await tick();h.response(1);await cancellation;h.session.handlePiEvent({type:'agent_settled'});await first;}
  else if(kind==='rejection'){h.response(0,false,'synthetic non-auth error');await first;}
  else if(kind==='write-error'){await first;}
  else if(kind==='auth'){h.response(0,false,'missing API key');await assert.rejects(first);}
  else if(kind==='exit'){h.child.emit('exit',7,null);await first;}
  else if(kind==='child-error'){h.child.emit('error',new Error('synthetic spawn error'));await first;}
  else if(kind==='queued'){h.response(0);await tick();h.session.handlePiEvent({type:'agent_settled'});await first;await tick();h.response(1);await tick();h.session.handlePiEvent({type:'agent_settled'});await second;}
  else if(kind==='late-reject'){
   h.session.startTurn(h.session.turnQueue.shift());await tick();h.response(1);h.response(0,false,'synthetic late rejection');await second;
  }else if(kind==='late-settle'){
   const barrier=deferred();h.session.publishContextUsage=()=>barrier.promise;
   h.response(0);await tick();h.session.handlePiEvent({type:'agent_settled'});
   h.session.startTurn(h.session.turnQueue.shift());await tick();h.response(1);await tick();barrier.resolve();await second;
  }
  await tick();
  return {observable:JSON.parse(JSON.stringify({settled,updates:h.updates,commands:h.commands})),capture:Buffer.concat(h.frames),diagnostic:h.diag.inspect()};
 }finally{h.close();}
}
for(const kind of ['success','cancel','rejection','write-error','auth','exit','child-error','queued','late-reject','late-settle']){
 test('original vs overlay '+kind,async()=>{
  const a=await scenario(original,kind),b=await scenario(marked,kind);assert.deepEqual(b.observable,a.observable);
  if(kind==='auth'){assert.equal(b.capture.length,0);return;}
  const result=parseFrames(b.capture,{caseName:'absolute',adapter:1});assert.equal(result.complete,true,JSON.stringify(result));
  if(kind.startsWith('late'))assert.notEqual(result.entryTurn,result.turn);
  if(kind==='rejection'||kind==='write-error'||kind==='exit'||kind==='child-error'||kind==='late-reject')assert.equal(result.route,'catch_resolve');
 });
}
function minimal(writer){
 const frames=[];const d=createDiagnostic({fd:3,caseName:'absolute',adapter:1,writer:writer||((_fd,b)=>{frames.push(Buffer.from(b));return b.length;})});
 const proc={},session={proc,pendingTurn:null,cancelRequested:false},call=d.call(),queued={},pending={};
 d.turn(session,queued,call,'first:cancel');session.pendingTurn=pending;const ctx=d.start(session,queued,pending);d.request(proc,'raw-secret',ctx,'prompt');d.response(proc,'raw-secret',{success:true});d.settled(session);const entry=d.settleEntry(session);d.settleResolve(entry,session,'end_turn');d.acpReturn(call,'end_turn','end_turn');d.flush(call);return {d,call,frames,proc,session};
}
const decode=b=>parseFrames(b,{caseName:'absolute',adapter:1});
const encoded=f=>{const body=JSON.stringify(f);return Buffer.from(`PTD2 ${Buffer.byteLength(body)}\n${body}\n`);};
function frame(b){const nl=b.indexOf(10);return JSON.parse(b.subarray(nl+1,b.length-1));}
test('frame complete and source secret omission',()=>{const h=minimal(),b=Buffer.concat(h.frames);assert(decode(b).complete);assert(!b.includes('raw-secret'));assert(b.length<=65536);});
for(const fault of ['missing','duplicate','truncated','wrong-case','wrong-source','sequence','required-event','overflow','unknown-field','bad-link'])test('parser rejects '+fault,()=>{
 let b=Buffer.concat(minimal().frames),f=frame(b);
 if(fault==='missing')b=Buffer.alloc(0);else if(fault==='duplicate')b=Buffer.concat([b,b]);else if(fault==='truncated')b=b.subarray(0,-1);
 else{if(fault==='wrong-case')f.case='bare';if(fault==='wrong-source')f.source='0'.repeat(64);if(fault==='sequence')f.events[0][0]=2;if(fault==='required-event')f.events=f.events.filter(e=>e[1]!=='request');if(fault==='overflow')f.flags.overflow=true;if(fault==='unknown-field')f.token='do-not-emit';if(fault==='bad-link')f.events.find(e=>e[1]==='response')[4]+=1;b=encoded(f);}
 assert(!decode(b).complete);
});
test('event and map caps are explicit',()=>{const d=createDiagnostic();for(let i=0;i<1000;i++)d.call();const r=d.inspect();assert(r.flags.overflow);assert(r.events.length<=192);assert(r.count<=256);});
test('error properties do not invoke getters or stringify arbitrary objects',()=>{let touched=0;const x={get message(){touched++;throw Error('getter');},toString(){touched++;throw Error('stringify');}};const d=createDiagnostic();d.rejected({a:1,s:1,t:1,n:1,p:1,r:1,c:1},x);assert.equal(touched,0);const e=new Error();Object.defineProperty(e,'message',{get(){touched++;throw Error('getter');}});d.rejected({a:1,s:1,t:1,n:1,p:1,r:1,c:1},e);assert.equal(touched,0);});
test('error text bounded and omitted',()=>{const d=createDiagnostic();d.rejected({a:1,s:1,t:1,n:1,p:1,r:1,c:1},new Error('SECRET'.repeat(10000)));const r=JSON.stringify(d.inspect());assert(!r.includes('SECRET'));assert(r.length<2048);});
test('short and failed descriptor writes stay diagnostic-only',()=>{for(const writer of [()=>0,()=>{throw Error('write failure');}]){const h=minimal(writer);assert(h.d.inspect().flags.writeError);}});
test('actual inherited descriptor capture and closed descriptor control',()=>{
 const dir=mkdtempSync(join(here,'descriptor-control-'));try{
  const file=join(dir,'capture');const fd=openSync(file,'wx',0o600);
  try{const r=spawnSync(process.execPath,['--input-type=module','-e',"import{writeSync}from'node:fs';writeSync(3,Buffer.from('descriptor-control\\n'));"],{stdio:['ignore','pipe','pipe',fd],timeout:5000,maxBuffer:8192});assert.equal(r.status,0);assert.equal(readFileSync(file,'utf8'),'descriptor-control\n');}finally{closeSync(fd);}
  const r=spawnSync(process.execPath,['--input-type=module','-e',"import{writeSync}from'node:fs';let caught=false;try{writeSync(99,'x')}catch{caught=true}if(!caught)process.exitCode=1;"],{timeout:5000,maxBuffer:8192});assert.equal(r.status,0);
 }finally{rmSync(dir,{recursive:true,force:true});}
});

for(const mode of ['short','throw'])for(const kind of ['success','rejection'])test('overlay diagnostic writer '+mode+' preserves '+kind,async()=>{
 const writer=mode==='short'?()=>0:()=>{throw Error('synthetic diagnostic write failure');};
 const a=await scenario(original,kind),b=await scenario(marked,kind,writer);
 assert.deepEqual(b.observable,a.observable);assert.equal(b.diagnostic.flags.writeError,true);
 assert.equal(decode(b.capture).complete,false);
});
const renumber=f=>{f.events.forEach((e,i)=>e[0]=i+1);return f;};
const move=(f,kind,beforeKind,after=false)=>{
 const at=f.events.findIndex(e=>e[1]===kind);const [row]=f.events.splice(at,1);
 const target=f.events.findIndex(e=>e[1]===beforeKind);f.events.splice(target+(after?1:0),0,row);renumber(f);
};
for(const fault of ['terminal-before-request','terminal-after-return','route-before-start','response-session','response-process','route-session','route-process','invalid-exit-code','invalid-exit-signal','invalid-other-return','wrong-origin-session','unknown-settle-entry'])test('strict graph rejects '+fault,()=>{
 const f=frame(Buffer.concat(minimal().frames));
 if(fault==='terminal-before-request')move(f,'response','request');
 if(fault==='terminal-after-return')move(f,'response','acp_return',true);
 if(fault==='route-before-start')move(f,'settle_resolve','start');
 if(fault==='response-session')f.events.find(e=>e[1]==='response')[3]+=50;
 if(fault==='response-process')f.events.find(e=>e[1]==='response')[6]+=50;
 if(fault==='route-session')f.events.find(e=>e[1]==='settle_resolve')[3]+=50;
 if(fault==='route-process')f.events.find(e=>e[1]==='settle_resolve')[6]+=50;
 if(fault==='invalid-exit-code'||fault==='invalid-exit-signal'){
  const p=f.events.find(e=>e[1]==='request')[6];f.events.push([0,'process_exit',0,0,0,0,p,0,0,fault==='invalid-exit-code'?'1':0,fault==='invalid-exit-signal'?'SIGBOGUS':'other']);renumber(f);
 }
 if(fault==='invalid-other-return'){
  const e=[...f.events.find(e=>e[1]==='acp_return')];e[8]=0;e[9]=99;f.events.unshift(e);renumber(f);
 }
 if(fault==='wrong-origin-session')f.events.find(e=>e[1]==='settle_entry')[3]+=50;
 if(fault==='unknown-settle-entry'){
  for(const e of f.events.filter(e=>['settled','settle_entry'].includes(e[1])))for(let i=2;i<=8;i++)e[i]=0;
  const e=f.events.find(e=>e[1]==='settle_resolve');e[9]=e[10]=e[11]=0;
 }
 assert.equal(decode(encoded(f)).complete,false,fault);
});
test('valid target terminal after settle before return is not blanket rejected',()=>{
 const f=frame(Buffer.concat(minimal().frames));move(f,'response','settle_resolve',true);
 assert.equal(decode(encoded(f)).complete,true);
});
for(const fault of ['origin-terminal-after-rejection','origin-request-process','origin-rejection-session'])test('late catch rejects '+fault,async()=>{
 const observed=await scenario(marked,'late-reject');const f=frame(observed.capture);
 const rejection=f.events.find(e=>e[1]==='prompt_reject');
 if(fault==='origin-terminal-after-rejection'){
  const at=f.events.findIndex(e=>['response','request_reject'].includes(e[1])&&e[4]===rejection[4]);const [terminal]=f.events.splice(at,1);
  f.events.splice(f.events.indexOf(rejection)+1,0,terminal);renumber(f);
 }
 if(fault==='origin-request-process')f.events.find(e=>e[1]==='request'&&e[4]===rejection[4])[6]+=50;
 if(fault==='origin-rejection-session')rejection[3]+=50;
 assert.equal(decode(encoded(f)).complete,false,fault);
});
