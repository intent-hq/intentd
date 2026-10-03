import {createInterface} from 'node:readline';
import {appendFileSync} from 'node:fs';

if (process.argv[2] !== 'app-server') throw new Error('Expected app-server');
const model = process.argv[3];
const send = message => process.stdout.write(JSON.stringify(message) + '\n');
for await (const line of createInterface({input: process.stdin})) {
  const message = JSON.parse(line);
  if (process.env.MOCK_CODEX_REPORT) appendFileSync(process.env.MOCK_CODEX_REPORT, JSON.stringify({model, method: message.method, params: message.params}) + '\n');
  if (message.id === undefined) continue;
  let result;
  switch (message.method) {
    case 'initialize': result = {userAgent: 'fixture', codexHome: process.env.CODEX_HOME}; break;
    case 'account/read': result = {account: {type: 'apiKey'}, requiresOpenaiAuth: true}; break;
    case 'config/read': result = {config: {model, model_provider: 'openai'}, origins: {}, layers: []}; break;
    case 'skills/list': result = {data: []}; break;
    case 'skills/extraRoots/set': result = {}; break;
    case 'model/list': result = {data: [{id: model, model, displayName: model, description: 'Host fixture',
      hidden: false, isDefault: true, inputModalities: ['text'], supportsPersonality: false,
      supportedReasoningEfforts: [{reasoningEffort: 'medium', description: 'Medium'}], defaultReasoningEffort: 'medium'}], nextCursor: null}; break;
    case 'thread/start': result = {thread: {id: 'fixture-thread', turns: []}, model, modelProvider: 'openai', reasoningEffort: 'medium', serviceTier: null}; break;
    case 'account/rateLimits/read': result = {rateLimits: {}, rateLimitsByLimitId: {}}; break;
    case 'thread/goal/get': result = {goal: null}; break;
    case 'thread/settings/update': result = {}; break;
    case 'mcpServerStatus/list': result = {data: [], nextCursor: null}; break;
    case 'turn/start': {
      result = {turn: {id: 'fixture-turn', items: [], status: 'inProgress', error: null}};
      break;
    }
    default:
      send({id: message.id, error: {code: -32601, message: `Unexpected method ${message.method}`}});
      continue;
  }
  send({id: message.id, result});
  if (message.method === 'turn/start') {
    send({method: 'turn/started', params: {threadId: 'fixture-thread', turn: result.turn}});
    send({method: 'item/agentMessage/delta', params: {threadId: 'fixture-thread', turnId: 'fixture-turn', itemId: 'reply', delta: `reply from ${model}`}});
    send({method: 'turn/completed', params: {threadId: 'fixture-thread', turn: {...result.turn, status: 'completed'}}});
  }
}
