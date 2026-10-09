import { SOURCE } from './collector.mjs';
const LENGTHS={call:9,turn:9,start:9,request:9,response:10,request_reject:12,process_exit:11,process_error:12,prompt_reject:12,settled:9,settle_entry:9,settle_resolve:13,catch_resolve:14,acp_return:11};
const integer=(x,min,max)=>Number.isSafeInteger(x)&&x>=min&&x<=max;
const bit=x=>x===0||x===1;
const require=(value,label)=>{if(!value)throw Error(label);};
const same=(a,b,indices)=>indices.every(i=>a[i]===b[i]);
const unique=(rows,label)=>{require(rows.length===1,label);return rows[0];};
export function parseFrames(buffer,{caseName,adapter}) {
 const errors=[],frames=[];let offset=0;
 try {
  require(Buffer.isBuffer(buffer)&&buffer.length<=131072,'capture-cap');
  require(['bare','absolute'].includes(caseName)&&[1,2].includes(adapter),'expected-identity');
  while(offset<buffer.length){
   const nl=buffer.indexOf(10,offset);require(nl>=0&&nl-offset<=12,'header');
   const h=buffer.subarray(offset,nl).toString('ascii');require(/^PTD2 [1-9][0-9]{0,4}$/.test(h),'header');
   const length=Number(h.slice(5)),end=nl+1+length;
   require(end<buffer.length&&buffer[end]===10&&end+1-offset<=65536,'truncated-or-frame-cap');
   const raw=buffer.subarray(nl+1,end);require(raw.equals(Buffer.from(raw.toString('utf8'))),'invalid-utf8');
   const f=JSON.parse(raw.toString('utf8'));frames.push(f);offset=end+1;
   require(frames.length<=2&&f.v===2&&f.source===SOURCE&&f.case===caseName&&f.adapter===adapter&&f.snapshot===frames.length&&f.final===true,'identity-or-sequence');
   require(JSON.stringify(Object.keys(f).sort())===JSON.stringify(['adapter','case','events','final','flags','snapshot','source','v']),'envelope-fields');
   require(f.flags&&JSON.stringify(Object.keys(f.flags).sort())===JSON.stringify(['captureError','overflow','unknownLink','writeError']),'flags');
   require(Object.values(f.flags).every(v=>v===false),'capture-incomplete');
   require(Array.isArray(f.events)&&f.events.length<=192,'event-count');
   for(let i=0;i<f.events.length;i++){
    const e=f.events[i],kind=e?.[1];
    require(Array.isArray(e)&&e[0]===i+1&&Object.hasOwn(LENGTHS,kind)&&e.length===LENGTHS[kind]&&Buffer.byteLength(JSON.stringify(e))<=256,'event-shape');
    require(e.slice(2,7).every(x=>integer(x,0,256))&&integer(e[7],0,128)&&bit(e[8]),'event-identities');
    if(kind==='call')require(e[2]>0&&e.slice(3,9).every(x=>x===0),'call-fields');
    if(kind==='turn')require([2,3,4,6].every(j=>e[j]>0)&&e[5]===0&&e[7]===0,'turn-fields');
    if(kind==='start'||kind==='prompt_reject')require(e.slice(2,7).every(x=>x>0)&&e[7]===0,'start-context');
    if(['request','response','request_reject'].includes(kind))require(e.slice(2,8).every(x=>x>0),'request-context');
    if(['settled','settle_entry','settle_resolve','catch_resolve'].includes(kind))require((e.slice(2,7).every(x=>x>0)&&e[7]===0)||(e.slice(2,9).every(x=>x===0)),'pending-context');
    if(kind==='response')require(bit(e[9]),'response-flag');
    if(['prompt_reject','request_reject','process_error'].includes(kind))require(['unknown','process_exit','pi_prompt_failed','already_processing','no_model','timeout'].includes(e[9])&&integer(e[10],0,2147483647)&&typeof e[11]==='string'&&/^(?:[a-f0-9]{64})?$/.test(e[11]),'error-fields');
    if(['process_exit','process_error'].includes(kind))require(e[6]>0&&[2,3,4,5,7,8].every(j=>e[j]===0),'process-context');
    if(kind==='process_exit')require((e[9]===null||integer(e[9],-2147483648,2147483647))&&['SIGTERM','SIGKILL','SIGINT','other'].includes(e[10]),'exit-fields');
    if(kind==='settle_resolve')require(e.slice(9,12).every(x=>integer(x,0,256))&&bit(e[12]),'settle-fields');
    if(kind==='catch_resolve')require(e.slice(9,12).every(x=>integer(x,1,256))&&e.slice(12,14).every(bit),'catch-fields');
    if(kind==='acp_return')require([2,3,4,6].every(j=>e[j]>0)&&[0,1,2].includes(e[9])&&[0,1].includes(e[10])&&((e[9]===2&&e[10]===1)||(e[9]===1&&e[10]===0)||(e[9]===0)),'return-fields');
   }
  }
  require(frames.length===1,'missing-or-duplicate-final');
  const events=frames[0].events;
  const find=kind=>events.filter(e=>e[1]===kind);
  const callOf=e=>unique(find('call').filter(x=>x[2]===e[2]),'call-link');
  const turnOf=e=>unique(find('turn').filter(x=>same(x,e,[2,3,4,6,8])),'turn-link');
  const startOf=e=>unique(find('start').filter(x=>same(x,e,[2,3,4,5,6,8])),'start-link');
  const requestOf=e=>unique(find('request').filter(x=>same(x,e,[2,3,4,5,6,7,8])),'request-link');
  const requestForStart=e=>unique(find('request').filter(x=>same(x,e,[2,3,4,5,6,8])),'turn-request-link');
  const terminalFor=r=>unique(events.filter(e=>['response','request_reject'].includes(e[1])&&same(e,r,[2,3,4,5,6,7,8])),'request-terminal-count');
  const originFor=e=>unique(find('start').filter(x=>x[2]===e[9]&&x[4]===e[10]&&x[5]===e[11]),'origin-start-link');
  require(new Set(find('call').map(e=>e[2])).size===find('call').length,'duplicate-call');
  require(new Set(find('turn').map(e=>e[4])).size===find('turn').length&&new Set(find('turn').map(e=>e[2])).size===find('turn').length,'duplicate-turn');
  require(new Set(find('start').map(e=>e[5])).size===find('start').length&&new Set(find('start').map(e=>e[4])).size===find('start').length,'duplicate-start');
  require(new Set(find('request').map(e=>e[7])).size===find('request').length,'duplicate-request');
  for(const e of events){
   const k=e[1];
   if(k==='turn')require(callOf(e)[0]<e[0],'call-turn-order');
   if(k==='start')require(turnOf(e)[0]<e[0],'turn-start-order');
   if(k==='request')require(startOf(e)[0]<e[0],'start-request-order');
   if(k==='response'||k==='request_reject'){
    const r=requestOf(e);require(r[0]<e[0],'request-terminal-order');
    require(terminalFor(r)===e,'duplicate-terminal');
   }
   if(k==='prompt_reject'){
    const start=startOf(e),r=requestForStart(start),terminal=terminalFor(r);
    require(start[0]<r[0]&&r[0]<terminal[0]&&terminal[0]<e[0],'rejection-order');
    require(terminal[1]==='request_reject'||terminal[9]===0,'rejection-from-success');
   }
   if(['settled','settle_entry','settle_resolve','catch_resolve'].includes(k)&&e[2]!==0)require(startOf(e)[0]<e[0],'pending-event-order');
   if(k==='settle_entry'){
    const previous=events[e[0]-2];require(previous?.[1]==='settled'&&same(previous,e,[2,3,4,5,6,7,8]),'settled-entry-order');
   }
   if(['catch_resolve','settle_resolve'].includes(k)&&e[2]!==0)require(requestForStart(startOf(e))[0]<e[0],'route-request-order');
   if(k==='catch_resolve'){
    const origin=originFor(e),rejection=unique(find('prompt_reject').filter(x=>same(x,origin,[2,3,4,5,6,8])),'origin-rejection-link');
    require(rejection[0]<e[0],'catch-order');
    if(e[2]!==0)require(same(origin,e,[3,6]),'cross-session-or-process-catch');
   }
   if(k==='settle_resolve'){
    const entry=unique(find('settle_entry').filter(x=>x[2]===e[9]&&x[4]===e[10]&&x[5]===e[11]),'entry-link');
    require(entry[0]<e[0],'settle-order');
    if(e[2]!==0&&entry[2]!==0)require(same(entry,e,[3,6]),'cross-session-or-process-settle');
   }
   if(k==='acp_return'){
    require(turnOf(e)[0]<e[0],'turn-return-order');
    if(e[5]===0||e[7]===0)require(e[5]===0&&e[7]===0&&e[9]===2&&e[10]===1,'queued-return');
    else{
     const r=requestOf(e),terminal=terminalFor(r);
     require(r[0]<terminal[0]&&terminal[0]<e[0],'terminal-return-order');
    }
   }
  }
  const end=unique(find('acp_return').filter(e=>e[8]===1),'first-cancel-return-count');
  require(end.slice(2,8).every(x=>x>0),'unlinked-return');
  const request=requestOf(end),terminal=terminalFor(request),start=startOf(end);
  require(request[0]<terminal[0]&&terminal[0]<end[0],'target-terminal-order');
  const route=unique(events.filter(e=>['catch_resolve','settle_resolve'].includes(e[1])&&same(e,end,[2,3,4,5,6,8])&&e[0]<end[0]),'missing-or-competing-resolution-attempts');
  require(start[0]<request[0]&&request[0]<route[0],'route-creation-order');
  if(route[1]==='catch_resolve')require(route[12]===0&&end[9]===(route[13]===1?2:0)&&(route[13]===0||end[10]===1),'catch-result');
  else require(route.slice(9,12).every(x=>x>0)&&end[9]===(route[12]===1?2:1)&&end[10]===route[12],'settle-result');
  return {complete:true,route:route[1],call:end[2],session:end[3],turn:end[4],pending:end[5],process:end[6],request:end[7],entryTurn:route[10],entryPending:route[11],errors:[],qualification:'Observed attempt and process-local callback partial order only; target terminal need not precede settle. No historical attribution.'};
 }catch(e){errors.push(typeof e.message==='string'&&/^[a-z-]{1,64}$/.test(e.message)?e.message:'parse-error');return {complete:false,route:null,errors,frameCount:frames.length};}
}
