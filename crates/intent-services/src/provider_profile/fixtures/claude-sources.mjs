import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
import {spawn,spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
const repo=path.resolve(process.argv[4]);
const testName=process.argv[5];
const modules=path.resolve(process.argv[2]);
const cli=path.join(modules,'@anthropic-ai/claude-agent-sdk-linux-x64/claude');
const harness=path.resolve(process.argv[3]);
const root=await fs.mkdtemp(path.join(os.tmpdir(),'intent-verify-acquisition-'));
const home=path.join(root,'home'), config=path.join(home,'.claude'), admin=path.join(root,'admin'), main=path.join(root,'main'), state=path.join(root,'state'), bin=path.join(root,'bin');
for (const dir of [config,admin,state,bin,path.join(root,'tmp')]) await fs.mkdir(dir,{recursive:true,mode:0o700});
await fs.symlink(cli,path.join(bin,'claude'));
const env={PATH:bin+':/usr/bin:/bin',HOME:home,USERPROFILE:home,SHELL:'/bin/false',TMPDIR:path.join(root,'tmp'),CLAUDE_CONFIG_DIR:config,ANTHROPIC_API_KEY:'synthetic-review-key',CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1',DISABLE_AUTOUPDATER:'1'};
const namespace=['--unshare-user','--die-with-parent','--ro-bind','/','/','--dev','/dev','--proc','/proc','--bind',root,root,'--tmpfs','/etc','--ro-bind','/etc/passwd','/etc/passwd','--ro-bind','/etc/group','/etc/group','--ro-bind',admin,'/etc/claude-code','--'];
const git=spawnSync('git',['clone','--shared','--no-checkout','-q',repo,main],{env,encoding:'utf8'});assert.equal(git.status,0,git.stderr);
const subdir=path.join(main,'nested'), worktree=path.join(root,'worktree');
await fs.mkdir(subdir);
const wt=spawnSync('git',['-C',main,'worktree','add','--detach','--no-checkout',worktree,'HEAD'],{env,encoding:'utf8'});assert.equal(wt.status,0,wt.stderr);
await fs.mkdir(path.join(main,'.claude'));
await fs.writeFile(path.join(config,'settings.json'),JSON.stringify({model:'claude-sonnet-4-6'}));
const settings=path.join(main,'.claude/settings.local.json');
const seen=[];
const server=http.createServer(async(req,res)=>{
 let raw='';for await(const data of req)raw+=data;
 if(!req.url.startsWith('/v1/messages')||req.url.includes('count_tokens')) {res.writeHead(200,{'content-type':'application/json'});res.end('{"input_tokens":1}');return;}
 const body=JSON.parse(raw);seen.push(body.model);
 res.writeHead(200,{'content-type':'text/event-stream'});
 const events=[['message_start',{type:'message_start',message:{id:'msg_fixture',type:'message',role:'assistant',model:body.model,content:[],stop_reason:null,stop_sequence:null,usage:{input_tokens:1,output_tokens:0}}}],['content_block_start',{type:'content_block_start',index:0,content_block:{type:'text',text:''}}],['content_block_delta',{type:'content_block_delta',index:0,delta:{type:'text_delta',text:'Fixture complete.'}}],['content_block_stop',{type:'content_block_stop',index:0}],['message_delta',{type:'message_delta',delta:{stop_reason:'end_turn',stop_sequence:null},usage:{output_tokens:1}}],['message_stop',{type:'message_stop'}]];
 for(const [event,data]of events)res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);res.end();
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
env.ANTHROPIC_BASE_URL=`http://127.0.0.1:${server.address().port}`;
for (const key of Object.keys(process.env))delete process.env[key];Object.assign(process.env,env);
const {query}=await import(pathToFileURL(path.join(modules,'@anthropic-ai/claude-agent-sdk/sdk.mjs')));
const legacy=path.join(worktree,'.claude/settings.local.json');
await fs.mkdir(path.dirname(legacy));
function acquire(cwd,mutation) {
 return spawnSync('bwrap',[...namespace,harness,'--exact',testName,'--ignored','--nocapture'],{cwd,env:{...env,INTENT_ACQUISITION_SOURCE_CHILD:'1',INTENT_ACQUISITION_STATE:state,...(mutation?{INTENT_ACQUISITION_MUTATION:mutation}:{})},encoding:'utf8',timeout:15000});
}
async function resetAuth(){await fs.rm(home,{recursive:true,force:true});await fs.mkdir(config,{recursive:true});await fs.writeFile(path.join(config,'settings.json'),JSON.stringify({model:'claude-sonnet-4-6'}));}
try {
 for(const [label,cwd] of [['root',main],['subdirectory',subdir],['linked worktree',worktree],['legacy local',worktree]]) {
  await fs.writeFile(settings,'{}');await fs.rm(legacy,{force:true});await resetAuth();
  const positive=acquire(cwd);
  assert.equal(positive.status,0,positive.stderr);assert(positive.stdout.includes('BUILT:claude-sonnet-4-6'),label+' empty sources must remain usable: '+positive.stdout);
  const selected=label==='legacy local'?legacy:settings;
  await fs.writeFile(selected,JSON.stringify({model:'claude-opus-4-6'}));await resetAuth();
  const got=acquire(cwd);
  assert.equal(got.status,0,got.stderr);assert(got.stdout.includes('DEFERRED:AuthProjection'),label+' must acquire the native model source: '+got.stdout);
  console.log(`PASS ${label}: public acquisition defers unsupported native local model`);
  let closed;const before=seen.length;
  const q=query({prompt:'Return the fixture text.',options:{cwd,env,pathToClaudeCodeExecutable:cli,settingSources:['user','project','local'],spawnClaudeCodeProcess:o=>{
   const child=spawn('bwrap',[...namespace,o.command,...o.args],{cwd:o.cwd,env:o.env,stdio:['pipe','pipe','pipe'],signal:o.signal});closed=new Promise(resolve=>child.once('close',resolve));return child;
  }}});
  const timer=setTimeout(()=>q.close(),15000);
  try{let success=false;for await(const event of q){if(event.type==='result')success=event.subtype==='success';}assert(success,label+' native baseline did not complete');}
  finally{clearTimeout(timer);q.close();await closed;}
  assert(seen.length>before);assert.equal(seen.at(-1),'claude-opus-4-6');
  console.log(`PASS ${label}: native loads expected local model`);
 }
 for(const [label,cwd,selected] of [['root',main,settings],['subdirectory',subdir,settings],['linked worktree',worktree,settings],['legacy local',worktree,legacy]]) {
  await resetAuth();await fs.writeFile(settings,'{}');await fs.rm(legacy,{force:true});
  const changed=acquire(cwd,selected);
  assert.equal(changed.status,0,changed.stderr);assert(changed.stdout.includes('READY'));assert(changed.stdout.includes('BUILD_FAILED'),label+' mutation must invalidate acquisition: '+changed.stdout);
  console.log(`PASS ${label}: post-acquisition source mutation fails build`);
 }
 console.log('PASS Claude native/public canonical source parity');
}finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await fs.rm(root,{recursive:true,force:true});}
