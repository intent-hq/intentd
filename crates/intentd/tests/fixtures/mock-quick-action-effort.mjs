// Deterministic one-shot ACP fixture. Every incoming request is logged before
// it is answered, so failure tests can prove no prompt was sent.
import fs from 'node:fs';
import readline from 'node:readline';
const behavior = JSON.parse(process.env.MOCK_EFFORT_BEHAVIOR || '{}');
let effort = 'medium';
let model = 'opening';
const selector = (values) => [{
  id: 'adapter-thinking', name: 'Thinking', category: 'thought_level',
  type: 'select', currentValue: effort,
  options: values.map(value => ({value, name: value})),
}];
const send = o => process.stdout.write(JSON.stringify(o) + '\n');
const result = (id, value) => send({jsonrpc:'2.0', id, result:value});
const error = id => send({jsonrpc:'2.0', id, error:{code:-32602,message:'rejected'}});
readline.createInterface({input:process.stdin, terminal:false}).on('line', line => {
  const msg = JSON.parse(line);
  if (!msg.method) return;
  if (process.env.MOCK_EFFORT_LOG) fs.appendFileSync(process.env.MOCK_EFFORT_LOG, JSON.stringify(msg) + '\n');
  if (msg.method === 'initialize') return result(msg.id, {protocolVersion:1,agentCapabilities:{}});
  if (msg.method === 'session/new') return result(msg.id, {
    sessionId:'one-shot', configOptions: selector(behavior.openingValues ?? ['low','medium','high']),
  });
  if (msg.method === 'session/set_config_option') {
    if (msg.params.configId === 'model') {
      if (behavior.rejectModel) return error(msg.id);
      model = msg.params.value;
      if ('modelOptions' in behavior) return result(msg.id, {configOptions:behavior.modelOptions});
      if (behavior.modelValues) return result(msg.id, {configOptions:selector(behavior.modelValues)});
      return result(msg.id, {});
    }
    if (msg.params.configId !== 'adapter-thinking' || behavior.rejectEffort) return error(msg.id);
    effort = behavior.echoEffort ?? msg.params.value;
    return result(msg.id, {configOptions:selector(behavior.modelValues ?? ['low','medium','high'])});
  }
  if (msg.method === 'session/prompt') {
    send({jsonrpc:'2.0',method:'session/update',params:{sessionId:'one-shot',update:{
      sessionUpdate:'agent_message_chunk',content:{type:'text',text:behavior.response ?? JSON.stringify({model,effort})},
    }}});
    return result(msg.id, {stopReason:'end_turn'});
  }
});
