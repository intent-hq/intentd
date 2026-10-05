// Drives the actual Rust service callers and pinned npx ACP/native runtime.
// Only synthetic credentials and a loopback model endpoint are used.
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createRequire} from 'node:module';

const [modules, harness, testName] = process.argv.slice(2);
const cli = path.join(modules, '@anthropic-ai/claude-agent-sdk-linux-x64/claude');
assert.equal(JSON.parse(await fs.readFile(path.join(modules, '@agentclientprotocol/claude-agent-acp/package.json'))).version, '0.81.1');
assert.equal(JSON.parse(await fs.readFile(path.join(modules, '@anthropic-ai/claude-agent-sdk/package.json'))).version, '0.3.280');
const root = await fs.mkdtemp(path.join(os.tmpdir(), 'intent-ephemeral-callers-'));
const home = path.join(root, 'home'), cwd = path.join(root, 'repo'), temp = path.join(root, 'tmp');
const admin = path.join(root, 'admin'), bin = path.join(root, 'bin'), cache = path.join(root, 'npm');
for (const dir of [home, cwd, temp, admin, bin, cache]) await fs.mkdir(dir, {mode:0o700});
await fs.symlink(cli, path.join(bin, 'claude'));
// Reuse the exact installed package from npm's cache without a registry fetch.
const packageRoot = path.dirname(modules), cacheEntry = path.join(cache, '_npx', path.basename(packageRoot));
await fs.mkdir(cacheEntry, {recursive:true});
for (const file of ['package.json', 'package-lock.json']) await fs.copyFile(path.join(packageRoot, file), path.join(cacheEntry, file));
await fs.symlink(modules, path.join(cacheEntry, 'node_modules'));
const npmRoot=path.resolve(path.dirname(await fs.realpath('/usr/bin/npx')),'..');
const cacache=createRequire(import.meta.url)(path.join(npmRoot,'node_modules/cacache'));
const manifestKey='make-fetch-happen:request-cache:https://registry.npmjs.org/@agentclientprotocol%2fclaude-agent-acp';
const manifest=await cacache.get(path.resolve(modules,'../../..','_cacache'),manifestKey);
await cacache.put(path.join(cache,'_cacache'),manifestKey,manifest.data,{metadata:manifest.metadata});
const marker = path.join(root, 'ambient-started');
const mcp = {command:'/usr/bin/node',args:['-e',`require('fs').writeFileSync(${JSON.stringify(marker)}, 'started')`]};
const write = async (p, text) => {await fs.mkdir(path.dirname(p), {recursive:true}); await fs.writeFile(p,text);};
async function resetAmbient() {
  await fs.rm(home, {recursive:true, force:true});
  await fs.mkdir(home, {mode:0o700});
  await write(path.join(home, '.claude/settings.json'), JSON.stringify({model:'claude-sonnet-4-6',enableAllProjectMcpServers:true}));
  await write(path.join(home, '.claude.json'), JSON.stringify({mcpServers:{ambientHome:mcp}}));
  for (const dir of [home, cwd, temp]) {
    await write(path.join(dir, '.claude/skills/ambient/SKILL.md'), '---\nname: ambient\ndescription: AMBIENT-EPHEMERAL-SKILL-MUST-NOT-LOAD\n---\nNever load.');
    if (dir !== home) {
      await write(path.join(dir, '.mcp.json'), JSON.stringify({mcpServers:{ambientProject:mcp}}));
      await write(path.join(dir, 'CLAUDE.md'), 'AMBIENT-EPHEMERAL-INSTRUCTIONS-MUST-NOT-LOAD');
    }
  }
  await fs.rm(path.join(cwd,'request-started'), {force:true});
}
let mode;
const requests=[];
const server=http.createServer(async(req,res)=>{
  let raw='';for await(const chunk of req)raw+=chunk;
  if(!req.url.startsWith('/v1/messages')||req.url.includes('count_tokens')) {res.writeHead(200,{'content-type':'application/json'});res.end('{"input_tokens":1}');return;}
  const body=JSON.parse(raw);requests.push({mode,body,headers:req.headers});
  await fs.writeFile(path.join(cwd,'request-started'),'ready');
  if(mode==='timeout'||mode==='cancel')return;
  if(mode==='error'){res.writeHead(401,{'content-type':'application/json'});res.end('{"type":"error","error":{"type":"authentication_error","message":"synthetic failure"}}');return;}
  res.writeHead(200,{'content-type':'text/event-stream'});
  const events=[['message_start',{type:'message_start',message:{id:'msg_fixture',type:'message',role:'assistant',model:body.model,content:[],stop_reason:null,stop_sequence:null,usage:{input_tokens:1,output_tokens:0}}}],['content_block_start',{type:'content_block_start',index:0,content_block:{type:'text',text:''}}],['content_block_delta',{type:'content_block_delta',index:0,delta:{type:'text_delta',text:'OK'}}],['content_block_stop',{type:'content_block_stop',index:0}],['message_delta',{type:'message_delta',delta:{stop_reason:'end_turn',stop_sequence:null},usage:{output_tokens:1}}],['message_stop',{type:'message_stop'}]];
  for(const[event,data]of events)res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);res.end();
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const env={PATH:bin+':/usr/bin:/bin', HOME:home, USERPROFILE:home, SHELL:'/bin/false', TMPDIR:temp,
  CLAUDE_CONFIG_DIR:path.join(home,'.claude'),ANTHROPIC_API_KEY:'synthetic-ephemeral-key', ANTHROPIC_BASE_URL:`http://127.0.0.1:${server.address().port}`,
  CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1',DISABLE_AUTOUPDATER:'1',npm_config_cache:cache,npm_config_offline:'true',npm_config_update_notifier:'false'};
const namespace=['--unshare-user','--die-with-parent','--ro-bind','/','/','--dev','/dev','--proc','/proc','--bind',root,root,'--tmpfs','/etc','--ro-bind','/etc/passwd','/etc/passwd','--ro-bind','/etc/group','/etc/group','--ro-bind',admin,'/etc/claude-code','--'];
async function run() {
  const child=spawn('bwrap',[...namespace,harness,'--exact',testName,'--ignored','--nocapture'],{cwd,env:{...env,INTENT_EPHEMERAL_CHILD:mode},stdio:['ignore','pipe','pipe']});
  let stdout='',stderr='';child.stdout.on('data',data=>stdout+=data);child.stderr.on('data',data=>stderr+=data);
  const timer=setTimeout(()=>child.kill('SIGKILL'),70000);
  try {
    const code=await new Promise(resolve=>child.once('close',resolve));
    if(code!==0) {
      for(const name of await fs.readdir(path.join(cache,'_logs')).catch(()=>[])) {
        stderr+='\nNPM startup: '+(await fs.readFile(path.join(cache,'_logs',name),'utf8')).slice(-6000);
      }
    }
    assert.equal(code,0,`${mode}: ${stdout}\n${stderr}`);assert(stdout.includes(`PASS ephemeral caller ${mode}`));
  }
  finally{clearTimeout(timer);}
}
try {
  for(mode of ['inventory','inventory-deferred','completion','provider-test','models','error','timeout','cancel']) {
    await resetAmbient();
    const unsupported = path.join(cwd,'.claude/settings.local.json');
    if(mode==='inventory-deferred')await write(unsupported,JSON.stringify({model:'claude-opus-4-6'}));
    else await fs.rm(unsupported,{force:true});
    await run();server.closeAllConnections();
    assert.equal(await fs.stat(marker).catch(()=>null),null,'ambient MCP started');
    const contents=await fs.readdir(temp);
    assert(!contents.some(name=>name.startsWith('intent-ephemeral-profile-')||name.startsWith('intentd-npx-')),'ephemeral directory survived cleanup: '+contents.join(','));
    console.log('PASS actual ephemeral '+mode);
  }
  assert(requests.some(r=>r.mode==='completion'));assert(requests.some(r=>r.mode==='provider-test'));
  // Native Claude also sends tool-free warmup and session-title requests.
  // Every request must be isolated; the utility prompt belongs to the user turn.
  const completions=requests.filter(r=>r.mode==='completion'&&r.body.messages?.some(message=>
    message.role==='user'&&(message.content==='Reply OK'||Array.isArray(message.content)&&message.content.some(block=>block.type==='text'&&block.text==='Reply OK'))));
  assert(completions.length>0,'completion user turn never reached the model');
  for(const {body} of completions)assert(JSON.stringify(body.system).includes('EPHEMERAL-UTILITY-INSTRUCTIONS'),`system=${JSON.stringify(body.system)} messages=${JSON.stringify(body.messages)}`);
  for(const {body,headers,mode} of requests) {
    assert.equal(headers['x-api-key'],'synthetic-ephemeral-key');
    assert.equal(body.model,'claude-sonnet-4-6');
    assert.equal((body.tools??[]).length,0,'managed ephemeral tools must be empty');
    const serialized=JSON.stringify(body);
    assert(!serialized.includes('AMBIENT-EPHEMERAL'),'ambient skill/instructions reached the model');
  }
  console.log('PASS ephemeral inventory and callers');
} finally {server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await fs.rm(root,{recursive:true,force:true});}
