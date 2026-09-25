// Deterministic ACP/app-server fixture. Its generated preamble supplies role
// and a fixture directory; it never reaches a network or package manager.
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const config = JSON.parse(fs.readFileSync(path.join(fixture, 'fixture.json'), 'utf8'));
if (process.argv.includes('--version')) {
  console.log(role === 'acp' ? 'codex-acp 1.13.1' : 'codex-cli 0.333.4');
  process.exit(0);
}
const home = process.env.CODEX_HOME;
const record = event => fs.appendFileSync(path.join(fixture, 'events.jsonl'), JSON.stringify({role,...event})+'\n');
const failure = () => { record({isolationFailure:true}); process.exit(90); };
const seed = fs.readFileSync(path.join(home, 'config.toml'), 'utf8');
if (seed.includes('mcp_servers')) {
  fs.writeFileSync(path.join(fixture, 'mcp-launched'), 'bad');
  failure();
}
const isolation = {cwd:fs.realpathSync(process.cwd())===fs.realpathSync(home),home:process.env.HOME===home,
  profile:process.env.USERPROFILE===home,xdg:process.env.XDG_CONFIG_HOME===home,
  config:!process.env.CODEX_CONFIG,
  preload:!process.env.NODE_OPTIONS?.includes('intentd-inherited-preload-canary'),
  entry:!process.env.INTENT_CODEX_ENTRY,cache:!fs.existsSync(path.join(home,'models_cache.json'))};
if (!Object.values(isolation).every(Boolean)) { record({isolation}); failure(); }
if (role === 'raw' && (process.argv[2] !== 'app-server' || process.argv.length !== 3)) failure();
const authFile = path.join(home, 'auth.json');
record({started:true,pid:process.pid,home,auth:fs.existsSync(authFile),
  private:process.platform==='win32' || ((fs.statSync(home).mode & 0o077)===0
    && (!fs.existsSync(authFile) || (fs.statSync(authFile).mode & 0o077)===0)),
  env:Object.keys(process.env),codexPath:process.env.CODEX_PATH || null});
fs.writeFileSync(path.join(home, 'models_cache.json'), 'fixture-only');
const mode = config[role] || 'ok';
if (config.keepAlive) setInterval(() => {}, 1000);
const send = value => process.stdout.write(JSON.stringify(value)+'\n');
const error = (id,code) => send({id,error:{code,message:'credential-canary account-canary user@example.invalid',data:{access_token:'credential-canary'}}});
process.stderr.write('credential-canary user@example.invalid\n');
if (mode === 'exit') process.exit(7);
if (mode === 'stdoutFlood') { process.stdout.write('x'.repeat(1024*1024+1)); }
if (mode === 'stderrFlood') { process.stderr.write('x'.repeat(1024*1024+1)); }
if (mode === 'malformed') process.stdout.write('{not json}\n');
readline.createInterface({input:process.stdin}).on('line', line => {
  const message = JSON.parse(line);
  record({method:message.method,params:message.params});
  const allowed = role==='acp' ? ['initialize','session/new'] : ['initialize','initialized','account/read','model/list'];
  if (!allowed.includes(message.method)) { record({unexpectedMethod:message.method}); process.exit(91); }
  if (mode==='timeout' || mode==='stdoutFlood' || mode==='stderrFlood' || mode==='malformed') return;
  if (message.method==='initialized') return;
  if (message.method==='initialize') {
    if (mode==='unsupported') return error(message.id,-32601);
    if (mode==='peerRequest') return send({id:'peer',method:'fs/read_text_file',params:{path:'ignored'}});
    return send({id:message.id,result:role==='acp'?{protocolVersion:1,agentCapabilities:{},authMethods:[]}:{userAgent:'account-canary'}});
  }
  if (message.method==='session/new') {
    if (JSON.stringify(message.params.mcpServers)!=='[]' || message.params.cwd!==home) return failure();
    if (mode==='auth') return error(message.id,-32000);
    const result = config.session || {sessionId:'session-canary',models:{availableModels:[
      {modelId:config.model || 'fixture-model',name:'credential-canary'},
      {modelId:'fixture-model-high',name:'account-canary'}]},
      configOptions:[{id:'model',options:[{value:config.model || 'fixture-model',name:'user@example.invalid'}]}]};
    if (mode==='notificationError' || mode==='notification') {
      send({method:'session/update',params:{sessionId:'unrelated-session',update:{models:{availableModels:[{modelId:'unrelated-model'}]}}}});
      send({method:'session/update',params:{update:{models:{availableModels:[{modelId:'notification-model'}]}}}});
      if (mode==='notificationError') return error(message.id,-32000);
      return send({id:message.id,result:{sessionId:'session-canary'}});
    }
    return send({id:message.id,result});
  }
  if (message.method==='account/read') {
    if (message.params.refreshToken!==false) return failure();
    return send({id:message.id,result:{requiresOpenaiAuth:true,
      account: mode==='auth' ? null : {type:'chatgpt',email:'user@example.invalid',accountId:'account-canary'}}});
  }
  if (message.method==='model/list') {
    if (message.params.includeHidden!==true || message.params.limit!==100) return failure();
    if (mode==='rpcError') return error(message.id,-32000);
    if (mode==='repeatCursor') return send({id:message.id,result:{data:[],nextCursor:'private-cursor'}});
    if (mode==='pages') return send({id:message.id,result:{data:[],nextCursor:String(Number(message.params.cursor || 0)+1)}});
    const pages = config.pages || [{data:[{id:config.model || 'fixture-model',model:'fixture-alias',displayName:'credential-canary',hidden:false}],nextCursor:'private-cursor'},
      {data:[{id:'hidden-model',model:'hidden-model',hidden:true}],nextCursor:null}];
    const page = message.params.cursor===null ? 0 : 1;
    return send({id:message.id,result:pages[page]});
  }
});
