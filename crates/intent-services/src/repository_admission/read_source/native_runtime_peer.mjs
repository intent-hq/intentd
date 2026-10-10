import {createServer} from 'node:http';
import {pathToFileURL} from 'node:url';
import {writeFileSync} from 'node:fs';
import {join} from 'node:path';
const source=process.argv[2],root=process.argv[3],work=root;
const {runAcp}=await import(pathToFileURL(join(source,'dist/acp-agent.js')));
const {ReviewMonitorJournal}=await import(pathToFileURL(join(source,'dist/review-monitoring-journal.js')));
const journal=new ReviewMonitorJournal(join(root,'journal'),true);
 const calls=[];
 const server=createServer(async(req,res)=>{
  let body='';for await(const chunk of req)body+=chunk;
  if(req.url?.includes('/messages/count_tokens')){res.setHeader('content-type','application/json');res.end(JSON.stringify({input_tokens:1}));return;}
  if(!req.url?.startsWith('/v1/messages')){res.writeHead(404);res.end();return;}
  const request=JSON.parse(body);calls.push(request);writeFileSync(join(root,"model.json"),JSON.stringify(calls));
  const message={id:'msg_local_fixture',type:'message',role:'assistant',model:request.model,content:[],stop_reason:null,stop_sequence:null,usage:{input_tokens:1,output_tokens:1}};
  if(request.stream){
   res.writeHead(200,{'content-type':'text/event-stream'});
   const events=[['message_start',{type:'message_start',message}],['content_block_start',{type:'content_block_start',index:0,content_block:{type:'text',text:''}}],['content_block_delta',{type:'content_block_delta',index:0,delta:{type:'text_delta',text:'Controlled native result.'}}],['content_block_stop',{type:'content_block_stop',index:0}],['message_delta',{type:'message_delta',delta:{stop_reason:'end_turn',stop_sequence:null},usage:{output_tokens:1}}],['message_stop',{type:'message_stop'}]];
   for(const [name,data] of events)res.write(`event: ${name}\ndata: ${JSON.stringify(data)}\n\n`);res.end();
  }else{res.setHeader('content-type','application/json');res.end(JSON.stringify({...message,content:[{type:'text',text:'Controlled native result.'}],stop_reason:'end_turn'}));}
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));

const environment={PATH:process.env.PATH,HOME:root,TMPDIR:work,NODE_OPTIONS:'',ANTHROPIC_BASE_URL:`http://127.0.0.1:${server.address().port}`,ANTHROPIC_API_KEY:'intent-test-only',CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1',DISABLE_TELEMETRY:'1',DISABLE_AUTOUPDATER:'1'};
Object.assign(process.env,environment);
const logger={log:console.error,error:console.error,warn:console.error,debug:console.error};
const {agent,connection}=runAcp(logger,undefined,{journal,environment,containmentExecutable:process.argv[4]});
connection.closed.then(async()=>{await agent.dispose();journal.close();server.closeAllConnections();server.close();});
