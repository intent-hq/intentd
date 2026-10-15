import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {createServer} from 'node:http';
import {pathToFileURL} from 'node:url';
import {mkdirSync,readFileSync,readdirSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {PassThrough,Readable,Writable} from 'node:stream';
import {createInterface} from 'node:readline';
const [installed,root,mode,owner]=process.argv.slice(2),runtime=join(installed,'runtime');
mkdirSync(root,{recursive:true,mode:0o700});const work=join(root,'work');mkdirSync(work,{recursive:true,mode:0o700});
const imp=p=>import(pathToFileURL(join(runtime,p)));
const {query}=await imp('node_modules/@anthropic-ai/claude-agent-sdk/sdk.mjs');
const {ReviewMonitorJournal}=await imp('dist/review-monitoring-journal.js');
const {ReviewProcessAllocation}=await imp('dist/review-monitoring-process.js');
const {Pushable}=await imp('dist/utils.js');
const {claudeCliPath,promptToClaude,runAcp}=await imp('dist/acp-agent.js');
const {monitorPayloadDigest}=await imp('dist/review-monitoring.js');
const {ndJsonStream}=await imp('node_modules/@agentclientprotocol/sdk/dist/acp.js');
const journal=new ReviewMonitorJournal(join(root,'journal'),true);
if(mode==='seed'){const session=journal.enrollNative(owner);writeFileSync(join(root,'seed.json'),JSON.stringify({session,owner}));journal.close();process.exit(0);}
const {session}=JSON.parse(readFileSync(join(root,'seed.json'),'utf8'));
const inputs=JSON.parse(readFileSync(join(root,'store-input.json'),'utf8'));
const native=journal.nativeDirectory(session,owner);
const calls=[],messages=[];let allocation,q,stderr='';const input=new Pushable();
const server=createServer(async(req,res)=>{
 let body='';for await(const b of req)body+=b;
 if(req.url?.includes('/messages/count_tokens')){res.setHeader('content-type','application/json');res.end(JSON.stringify({input_tokens:1}));return;}
 if(!req.url?.startsWith('/v1/messages')){res.writeHead(404);res.end();return;}
 const request=JSON.parse(body);calls.push(request);writeFileSync(join(root,'producer-model.json'),JSON.stringify(calls,null,2));
 const hasAgent=request.tools?.some(t=>t.name==='Agent');
 const used=request.messages.some(m=>Array.isArray(m.content)&&m.content.some(b=>b.type==='tool_use'&&b.name==='Agent'));
 const tool=hasAgent&&!used;
 const content=tool?{type:'tool_use',id:'native_child_probe',name:'Agent',input:{description:'Check retained review context',prompt:'Return exactly CHILD_CONTEXT_OBSERVED. Do not use tools.',subagent_type:'closure-probe'}}:{type:'text',text:hasAgent?'PARENT_CONTEXT_OBSERVED':'CHILD_CONTEXT_OBSERVED'};
 const message={id:'msg_'+calls.length,type:'message',role:'assistant',model:request.model,content:[content],stop_reason:tool?'tool_use':'end_turn',stop_sequence:null,usage:{input_tokens:10,output_tokens:5}};
 if(request.stream){
 res.writeHead(200,{'content-type':'text/event-stream'});
 const events=[['message_start',{type:'message_start',message:{...message,content:[],stop_reason:null}}],['content_block_start',{type:'content_block_start',index:0,content_block:tool?{...content,input:{}}:{type:'text',text:''}}],['content_block_delta',{type:'content_block_delta',index:0,delta:tool?{type:'input_json_delta',partial_json:JSON.stringify(content.input)}:{type:'text_delta',text:content.text}}],['content_block_stop',{type:'content_block_stop',index:0}],['message_delta',{type:'message_delta',delta:{stop_reason:message.stop_reason,stop_sequence:null},usage:{output_tokens:5}}],['message_stop',{type:'message_stop'}]];
 for(const [name,data]of events)res.write(`event: ${name}\ndata: ${JSON.stringify(data)}\n\n`);res.end();
 }else{res.setHeader('content-type','application/json');res.end(JSON.stringify(message));}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const waiters=new Map();let loop, loopError, peer;let retiring=false;
const timer=setTimeout(()=>{console.error('native child deadline');q?.close();if(allocation)void allocation.retire();},60000);
try{
 q=query({prompt:input,options:{sessionId:session,cwd:work,tools:['Agent'],settingSources:[],agents:{'closure-probe':{description:'Controlled child context probe',prompt:'You are a controlled child. Return CHILD_CONTEXT_OBSERVED.',tools:[]}},systemPrompt:'Controlled local parent. Delegate once to closure-probe then finish.',model:'claude-sonnet-4-5',pathToClaudeCodeExecutable:await claudeCliPath(),canUseTool:async(_name,value)=>({behavior:'allow',updatedInput:value}),env:{PATH:process.env.PATH,NODE_OPTIONS:'',HOME:root,TMPDIR:'/run/intent-review/tmp',CLAUDE_CONFIG_DIR:'/run/intent-review/config',ANTHROPIC_BASE_URL:`http://127.0.0.1:${server.address().port}`,ANTHROPIC_API_KEY:'intent-local-test-only',CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1',DISABLE_TELEMETRY:'1',DISABLE_AUTOUPDATER:'1'},spawnClaudeCodeProcess(options){allocation=new ReviewProcessAllocation(options,[work],native.assertCurrent,native.openForSpawn(),join(installed,'bin/review-containment'));allocation.process.stderr.on('data',b=>{stderr+=b});return allocation.process;}}});
 await q.initializationResult();
 loop=(async()=>{for await(const m of q){messages.push(m);if(m.type==='result'){const ids=m.user_message_uuids??[m.user_message_uuid];for(const id of ids){const resolve=waiters.get(id);if(resolve){waiters.delete(id);resolve(m);}}}}})().catch(error=>{loopError=error;if(!retiring)throw error;});
 const correlations=[];
 for(const params of inputs){
  const {messageId:id,storeEnvelopeDigest}=params._meta.reviewMonitoringMessage;
  correlations.push({messageId:id,storeEnvelopeDigest,acpPayloadDigest:params._meta.reviewMonitoringMessage.acpPayloadDigest});
  const converted=promptToClaude(params,journal.prepareInput(session,owner,params));const result=new Promise(r=>waiters.set(converted.uuid,r));input.push(converted);const done=await result;assert.equal(done.is_error,false,JSON.stringify(done));
 }
 const compactUuid=randomUUID();const compactDone=new Promise(r=>waiters.set(compactUuid,r));
 input.push({type:'user',uuid:compactUuid,message:{role:'user',content:'/compact Preserve only a short summary without quoting original review payloads.'},parent_tool_use_id:null,session_id:session});
 const compactResult=await compactDone;
 writeFileSync(join(root,'compact-result.json'),JSON.stringify({compactResult,messages},null,2));
 assert.equal(compactResult.is_error,false,JSON.stringify(compactResult));
 assert.ok(messages.some(m=>m.type==='system'&&m.subtype==='compact_boundary'),'real native compaction boundary required');
 retiring=true;await allocation.retire();q.close();input.end();await loop;
 if(loopError)assert.match(String(loopError),/terminated by signal SIGKILL/);
 const files=[];function walk(dir){for(const e of readdirSync(dir,{withFileTypes:true})){const p=join(dir,e.name);if(e.isDirectory())walk(p);else if(p.endsWith('.jsonl'))files.push({path:p.slice(native.root.length+1),raw:readFileSync(p).toString('base64'),rows:readFileSync(p,'utf8').split('\n').filter(Boolean).map(JSON.parse)});}}walk(native.root);
 writeFileSync(join(root,'native-capture.json'),JSON.stringify({session,correlations,files,messages,calls: calls.length,retirementIteratorError:loopError?String(loopError):null},null,2));
 assert.ok(files.some(f=>f.path.includes('/subagents/')),'native Agent must create actual child transcript');
 assert.ok(calls.some(c=>!c.tools?.some(t=>t.name==='Agent')),'actual child must reach controlled model');
 const snapshot=journal.snapshot(session,owner,{});assert.deepEqual(snapshot.monitorMessages,correlations);

 console.log('NATIVE_STORE_CHILD_COMPACT_PASS',JSON.stringify({session,correlations,modelCalls:calls.length}));
}finally{clearTimeout(timer);input.end();q?.close();if(allocation)await allocation.retire();if(loop)await loop.catch(()=>{});writeFileSync(join(root,'stderr.log'),stderr);journal.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
