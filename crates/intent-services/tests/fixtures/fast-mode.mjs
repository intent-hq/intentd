// Isolated ACP provider with observable native configuration at prompt time.
// The harness prepends fixture constants; no provider credentials or files.
import readline from 'node:readline';
import fs from 'node:fs';
let model = 'supported';
let effort = 'medium';
let fast = true; // Native config / cold resume starts enabled.
let sid = 'fast-session';
let controls = [];
let held = null;
const fastId = provider === 'claude-code' ? 'fast' : 'fast-mode';
const select = (id, currentValue, values, category) => ({
  id, name: id, type: 'select', currentValue,
  options: values.map(value => ({value, name: value})), ...(category ? {category} : {})
});
const options = () => [
  select('model', model, ['supported', 'unsupported'], 'model'),
  select('effort', effort, ['medium', 'high'], 'thought_level'),
  ...(provider === 'claude-code' && model === 'unsupported' ? [] :
    [select(fastId, fast ? 'on' : 'off', ['on', 'off'])])
];
const send = value => process.stdout.write(JSON.stringify(value) + '\n');
const result = (id, value) => send({jsonrpc:'2.0', id, result:value});
const record = value => fs.appendFileSync(logPath, JSON.stringify(value) + '\n');
readline.createInterface({input:process.stdin, terminal:false}).on('line', line => {
  const msg = JSON.parse(line);
  const p = msg.params ?? {};
  if (!msg.method) return;
  record({method:msg.method, params:p, pid:process.pid});
  switch (msg.method) {
    case 'initialize':
      return result(msg.id, {protocolVersion:1, agentCapabilities:{loadSession:true}});
    case 'session/new':
    case 'session/load':
      sid = p.sessionId ?? sid;
      return result(msg.id, {sessionId:sid, configOptions:options()});
    case 'session/set_mode': return result(msg.id, {});
    case 'session/set_config_option':
      if (p.configId === 'model') model = p.value;
      else if (p.configId === 'effort') effort = p.value;
      else if (p.configId === fastId && options().some(o => o.id === fastId)) {
        if (failOff && p.value === 'off') return send({jsonrpc:'2.0', id:msg.id,
          error:{code:-32602, message:'inherited Fast tier could not be cleared'}});
        fast = p.value === 'on';
        controls.push(p.value);
      } else return send({jsonrpc:'2.0', id:msg.id, error:{code:-32602, message:'unsupported option'}});
      return result(msg.id, {configOptions:options()});
    case 'session/prompt': {
      const state = {model, effort, pid:process.pid, sessionId:sid, controls,
        fastMode: fast && model !== 'unsupported',
        serviceTier: fast && model !== 'unsupported' ? 'fast' : null};
      record({native:state});
      if (p.prompt?.[0]?.text === 'hold') {
        held = {id:msg.id, state};
        send({jsonrpc:'2.0', method:'fixture/held', params:state});
        return;
      }
      send({jsonrpc:'2.0', method:'session/update', params:{sessionId:sid,
        update:{sessionUpdate:'agent_message_chunk', content:{type:'text', text:JSON.stringify(state)}}}});
      return result(msg.id, {stopReason:'end_turn', native:state});
    }
    case 'fixture/release':
      result(held.id, {stopReason:'end_turn', native:held.state});
      held = null;
      return result(msg.id, {});
    default: if (msg.id !== undefined) return result(msg.id, {});
  }
});
