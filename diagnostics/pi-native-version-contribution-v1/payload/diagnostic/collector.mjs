import { createHash } from 'node:crypto';
import { types } from 'node:util';
import { writeSync } from 'node:fs';
export const SOURCE = '324aeb8bba1228937e16b1326fb3e014e2a625b2dea549dbcd49b006cd5df6a2';
export function createDiagnostic({ fd = -1, caseName = 'unknown', adapter = 0, writer = writeSync } = {}) {
  const events = [], ids = new WeakMap(), contexts = new WeakMap(), requests = new WeakMap(), calls = new Map();
  let count = 0, seq = 0, reqCount = 0, snapshots = 0, bytes = 0;
  const flags = { overflow: false, captureError: false, writeError: false, unknownLink: false };
  const zero = () => ({ a:0,s:0,t:0,n:0,p:0,r:0,c:0 });
  function id(obj) {
    if (!obj || (typeof obj !== 'object' && typeof obj !== 'function')) return 0;
    if (ids.has(obj)) return ids.get(obj);
    if (count >= 256) { flags.overflow = true; return 0; }
    ids.set(obj, ++count); return count;
  }
  function emit(kind, ctx = zero(), extra = []) {
    if (seq >= 192) { flags.overflow = true; return; }
    const row = [++seq,kind,ctx.a,ctx.s,ctx.t,ctx.n,ctx.p,ctx.r,ctx.c,...extra];
    const text = JSON.stringify(row);
    if (Buffer.byteLength(text) > 256) { flags.overflow = true; return; }
    events.push(row);
  }
  function errorFields(err) {
    if (!types.isNativeError(err)) return ['unknown',0,''];
    const d = Object.getOwnPropertyDescriptor(err,'message');
    if (!d || !Object.hasOwn(d,'value') || typeof d.value !== 'string') return ['unknown',0,''];
    const v=d.value.slice(0,512), length=Math.min(d.value.length,2147483647);
    let category='unknown';
    if (v.startsWith('pi process exited (code=')) category='process_exit';
    else if(v.startsWith('pi prompt failed:')) category='pi_prompt_failed';
    else if(v.includes('already processing')) category='already_processing';
    else if(v.includes('No model')) category='no_model';
    return [category,length,createHash('sha256').update(v).digest('hex')];
  }
  function context(obj) { const c=contexts.get(obj); if(!c) flags.unknownLink=true; return c || zero(); }
  const impl = {
    call() { const c=zero();c.a=id(c);if(c.a)calls.set(c.a,c);emit('call',c);return c; },
    turn(session,queued,call,message) {
      const c={...call,s:id(session),t:id(queued),n:0,p:id(session.proc),r:0,c:message==='first:cancel'?1:0};
      if(calls.has(c.a))Object.assign(calls.get(c.a),c);contexts.set(queued,c);emit('turn',c);return c;
    },
    start(session,queued,pending) { const c={...context(queued),n:id(pending)};if(calls.has(c.a))Object.assign(calls.get(c.a),c);contexts.set(pending,c);emit('start',c);return c; },
    current(pending) { return pending ? context(pending) : zero(); },
    request(proc,rawId,ctx,kind) {
      if(reqCount>=128){flags.overflow=true;return zero();}
      const c={...(ctx||zero()),p:id(proc),r:++reqCount};
      let m=requests.get(proc);if(!m){m=new Map();requests.set(proc,m);}m.set(rawId,c);
      if(kind==='prompt'){if(calls.has(c.a))Object.assign(calls.get(c.a),c);emit('request',c)};return c;
    },
    response(proc,rawId,msg) { const c=requests.get(proc)?.get(rawId);if(c?.t)emit('response',c,[msg.success===true?1:0]); },
    requestTimeout(ctx) { if(ctx.t)emit('request_reject',ctx,['timeout',0,'']); },
    requestReject(ctx,err) { if(ctx.t)emit('request_reject',ctx,errorFields(err)); },
    process(proc,kind,code,signal) { emit(kind,{...zero(),p:id(proc)},[Number.isInteger(code)?code:null,['SIGTERM','SIGKILL','SIGINT'].includes(signal)?signal:'other']); },
    processError(proc,err) { emit('process_error',{...zero(),p:id(proc)},errorFields(err)); },
    rejected(ctx,err) { emit('prompt_reject',ctx,errorFields(err)); },
    settled(session) { emit('settled',impl.current(session.pendingTurn)); },
    settleEntry(session) { const c=impl.current(session.pendingTurn);emit('settle_entry',c);return c; },
    settleResolve(entry,session,reason) { const current=impl.current(session.pendingTurn);emit('settle_resolve',current,[entry.a,entry.t,entry.n,reason==='cancelled'?1:0]); },
    catchResolve(origin,session,auth) { const current=impl.current(session.pendingTurn);emit('catch_resolve',current,[origin.a,origin.t,origin.n,auth?1:0,session.cancelRequested?1:0]); },
    acpReturn(call,result,stopReason) { emit('acp_return',call,[['error','end_turn','cancelled'].indexOf(result),['end_turn','cancelled'].indexOf(stopReason)]); },
    flush(call) {
      if (!call.c || snapshots>=2) return;
      const body=JSON.stringify({v:2,source:SOURCE,case:caseName,adapter,snapshot:++snapshots,final:true,flags:{...flags},events});
      const frame=Buffer.from('PTD2 '+Buffer.byteLength(body)+'\n'+body+'\n');
      if(frame.length>65536 || bytes+frame.length>131072){flags.overflow=true;return;}
      bytes+=frame.length;
      if(fd<0){flags.writeError=true;return;}
      const written=writer(fd,frame,0,frame.length);if(written!==frame.length)flags.writeError=true;
    },
    inspect() { return {events:events.map(x=>[...x]),flags:{...flags},count,reqCount,snapshots,bytes}; }
  };
  const api={};for(const [name,fn] of Object.entries(impl))api[name]=(...args)=>{try{return fn(...args);}catch{flags.captureError=true;if(name==='flush')flags.writeError=true;return zero();}};
  return Object.freeze(api);
}
export const DIAG = createDiagnostic({ fd:process.env.INTENT_PI_DIAG_FD==='3'?3:-1,caseName:['bare','absolute'].includes(process.env.INTENT_PI_DIAG_CASE)?process.env.INTENT_PI_DIAG_CASE:'unknown',adapter:['1','2'].includes(process.env.INTENT_PI_DIAG_ADAPTER)?Number(process.env.INTENT_PI_DIAG_ADAPTER):0 });
