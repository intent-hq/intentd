// Generated native profile against a loopback fake model API. Synthetic keys only.
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
const modules = path.resolve(process.argv[2]);
const supplied = JSON.parse(await fs.readFile(process.argv[3], 'utf8'));
const sdk = path.join(modules, '@anthropic-ai/claude-agent-sdk');
assert.equal(JSON.parse(await fs.readFile(path.join(sdk, 'package.json'))).version, '0.3.280');
const cli = path.join(modules, '@anthropic-ai/claude-agent-sdk-linux-x64/claude');
const root = await fs.mkdtemp(path.join(os.tmpdir(), 'intent-staged-claude-'));
const home = path.join(root, 'home'), cwd = path.join(root, 'repo'), admin = path.join(root, 'admin');
for (const p of [home, cwd, admin, path.join(cwd, '.claude/skills/ambient')]) await fs.mkdir(p, { recursive: true });
await fs.writeFile(path.join(cwd, 'CLAUDE.md'), 'AMBIENT-INSTRUCTIONS-MUST-NOT-LOAD');
await fs.writeFile(path.join(cwd, '.claude/skills/ambient/SKILL.md'), '---\nname: ambient\ndescription: AMBIENT-SKILL-MUST-NOT-LOAD\n---\nDo not load.');
const marker = path.join(root, 'ambient-mcp');
await fs.writeFile(path.join(cwd, '.mcp.json'), JSON.stringify({mcpServers:{ambient:{command:process.execPath,args:['-e',`require('fs').writeFileSync(${JSON.stringify(marker)},'started')`]}}}));
const env = { PATH: process.env.PATH, HOME: home, USERPROFILE: home, ...supplied.environment,
  CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1', DISABLE_AUTOUPDATER:'1' };
for (const key of Object.keys(process.env)) delete process.env[key];
Object.assign(process.env, env);
const { query } = await import(pathToFileURL(path.join(sdk, 'sdk.mjs')));
const requests=[];
const server=http.createServer(async (req,res)=>{
  let text='';for await(const chunk of req) text+=chunk;
  if (!req.url.startsWith('/v1/messages') || req.url.includes('count_tokens')) {res.writeHead(200,{'content-type':'application/json'});res.end('{"input_tokens":1}');return;}
  requests.push({headers:req.headers,body:JSON.parse(text)});
  res.writeHead(200,{'content-type':'text/event-stream'});
  const events=[['message_start',{type:'message_start',message:{id:'msg_fixture',type:'message',role:'assistant',model:'claude-sonnet-4-6',content:[],stop_reason:null,stop_sequence:null,usage:{input_tokens:1,output_tokens:0}}}],
    ['content_block_start',{type:'content_block_start',index:0,content_block:{type:'text',text:''}}],
    ['content_block_delta',{type:'content_block_delta',index:0,delta:{type:'text_delta',text:'Synthetic fixture complete.'}}],
    ['content_block_stop',{type:'content_block_stop',index:0}],
    ['message_delta',{type:'message_delta',delta:{stop_reason:'end_turn',stop_sequence:null},usage:{output_tokens:1}}],['message_stop',{type:'message_stop'}]];
  for(const [event,data] of events)res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);res.end();
});
await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(Number(new URL(env.ANTHROPIC_BASE_URL).port),'127.0.0.1',resolve);});
const namespace=['--unshare-user','--die-with-parent','--ro-bind','/','/','--dev','/dev','--proc','/proc',
 '--bind',root,root,'--bind',supplied.directory,supplied.directory,'--tmpfs','/etc',
 '--ro-bind','/etc/passwd','/etc/passwd','--ro-bind','/etc/group','/etc/group','--ro-bind',admin,'/etc/claude-code','--'];
try {
  for (const mode of ['native','sdk']) {
    let closed;
    const q=query({prompt:'Reply with the fixture response.',options:{cwd,env,pathToClaudeCodeExecutable:cli,
      ...(mode==='sdk'?{...supplied.session_meta.claudeCode.options,systemPrompt:supplied.session_meta.systemPrompt}:{}),
      spawnClaudeCodeProcess:o=>{const child=spawn('bwrap',[...namespace,o.command,...o.args,...(mode==='native'?supplied.runtime_args:[])],{cwd:o.cwd,env:o.env,stdio:['pipe','pipe','pipe'],signal:o.signal});closed=new Promise(resolve=>child.once('close',resolve));return child;},
    }});
    const timer=setTimeout(()=>q.close(),20000);
    try { let success=false;for await(const event of q){if(event.type==='result')success=event.subtype==='success';} assert(success,`${mode} did not complete`); }
    finally{clearTimeout(timer);q.close();await closed;}
  }
  assert(requests.length>=2);
  for(const request of requests){
    assert.equal(request.headers['x-api-key'],env.ANTHROPIC_API_KEY);
    assert.equal(request.body.model,'claude-sonnet-4-6');
    const system=JSON.stringify(request.body.system);
    assert(system.includes('OWNED-STAGED-INSTRUCTIONS'));
    assert(!system.includes('AMBIENT-INSTRUCTIONS-MUST-NOT-LOAD'));
    assert(!system.includes('AMBIENT-SKILL-MUST-NOT-LOAD'));
    assert(!JSON.stringify(request.body.tools).includes('mcp__'));
  }
  assert.equal(await fs.stat(marker).catch(()=>null),null);
  console.log('PASS staged Claude native/SDK synthetic auth, model and instructions');
} finally {server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await fs.rm(root,{recursive:true,force:true});}
