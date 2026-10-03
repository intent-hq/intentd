# Installed CLI protocol double, never an ACP adapter. The Rust harness prepends
# an absolute Python -S shebang and fixed PROVIDER, GENERATION, LOG constants.
# Each replacement changes only this executable; the ACP adapter stays untouched.
import json
import os
import sys
import uuid


def log(kind, **fields):
    with open(LOG, 'a', encoding='utf-8') as out:
        out.write(json.dumps(dict(kind=kind, provider=PROVIDER, generation=GENERATION,
                                  pid=os.getpid(), **fields)) + '\n')


def send(value):
    print(json.dumps(value), flush=True)


if '--version' in sys.argv:
    print('fixture-cli ' + str(GENERATION))
    sys.exit(0)

log('launch', args=sys.argv[1:],
    selected=os.environ.get('CODEX_PATH' if PROVIDER == 'codex' else 'CLAUDE_CODE_EXECUTABLE'),
    policy=os.environ.get('CODEX_CONFIG'),
    marker=os.environ.get('CODEX_FIXTURE_MARKER' if PROVIDER == 'codex' else 'CLAUDE_FIXTURE_MARKER'),
    proxy=os.environ.get('HTTPS_PROXY'),
    home=os.environ.get('CODEX_HOME' if PROVIDER == 'codex' else 'CLAUDE_CONFIG_DIR'))
models = ['fixture-base'] + (['fixture-added'] if GENERATION >= 2 else [])
model = models[0]
session = str(uuid.uuid4())
ephemeral_threads = set()
for line in sys.stdin:
    request = json.loads(line)
    if PROVIDER == 'codex':
        method = request.get('method')
        params = request.get('params') or {}
        log('request', method=method, params=params)
        if 'id' not in request:
            continue
        result = {}
        if method == 'initialize':
            result = dict(userAgent='controlled-cli', codexHome=os.environ.get('CODEX_HOME'))
        elif method == 'account/read':
            result = dict(account={'type': 'apiKey'}, requiresOpenaiAuth=False)
        elif method == 'config/read':
            result = dict(config={}, layers=[])
        elif method == 'account/rateLimits/read':
            result = dict(rateLimits={}, rateLimitsByLimitId={})
        elif method == 'thread/goal/get':
            result = dict(goal=None)
        elif method == 'model/list':
            result = dict(data=[dict(id=m, model=m, displayName=m, description=m,
                                    isDefault=i == 0, hidden=False, inputModalities=['text'],
                                    defaultReasoningEffort='medium',
                                    supportedReasoningEfforts=[dict(reasoningEffort=e, description=e)
                                                               for e in ['medium', 'high']])
                                for i, m in enumerate(models)], nextCursor=None)
        elif method in ['thread/start', 'thread/resume']:
            session = params.get('threadId', str(uuid.uuid4()))
            if params.get('ephemeral'):
                ephemeral_threads.add(session)
            result = dict(thread=dict(id=session, turns=[], historyMode='legacy'), model=model,
                          modelProvider='openai', reasoningEffort='medium')
        elif method in ['skills/list', 'mcpServerStatus/list', 'thread/items/list', 'thread/turns/list']:
            result = dict(data=[], nextCursor=None)
        elif method == 'turn/start':
            session = params['threadId']
            if session in ephemeral_threads:
                # Vendored ACP independently tries an unadvertised title model after
                # a real turn. Reject only that auxiliary request without killing
                # the installed CLI or weakening selected-model assertions.
                assert params.get('outputSchema'), params
                log('auxiliary_prompt', model=params['model'], session=session)
                send(dict(id=request['id'], error=dict(code=-32602, message='fixture title model unavailable')))
                continue
            model = params['model']
            assert model in models, model
            log('prompt', model=model, effort=params.get('effort'), session=session)
            turn = dict(id=str(uuid.uuid4()), items=[], status='inProgress', error=None)
            send(dict(id=request['id'], result=dict(turn=turn)))
            send(dict(method='turn/started', params=dict(threadId=session, turn=turn)))
            send(dict(method='item/agentMessage/delta', params=dict(threadId=session, turnId=turn['id'],
                                                                 itemId='reply', delta='fixture reply ' + model)))
            turn['status'] = 'completed'
            send(dict(method='turn/completed', params=dict(threadId=session, turn=turn)))
            continue
        elif method not in ['thread/name/set', 'thread/settings/update', 'thread/unsubscribe',
                            'skills/extraRoots/set', 'mcpServer/reload']:
            raise AssertionError('unexpected Codex request: ' + method)
        send(dict(id=request['id'], result=result))
        if method in ['thread/start', 'thread/resume']:
            for name in params.get('config', {}).get('mcp_servers', {}):
                send(dict(method='mcpServer/startupStatus/updated',
                          params=dict(name=name, status='ready', error=None)))
    else:
        if request['type'] == 'control_request':
            control = request['request']
            subtype = control['subtype']
            log('request', method=subtype, params=control)
            result = {}
            if subtype == 'initialize':
                result = dict(models=[dict(value=m, displayName=m, description=m) for m in models],
                              commands=[], account={'tokenSource': 'apiKey'},
                              output_style='default', available_output_styles=['default'])
            elif subtype == 'set_model':
                model = control['model']
                assert model in models, model
            elif subtype == 'mcp_status':
                result = dict(mcpServers=[])
            elif subtype not in ['set_permission_mode', 'set_max_thinking_tokens', 'apply_flag_settings',
                                 'mcp_set_servers', 'interrupt', 'rewind_files']:
                raise AssertionError('unexpected Claude control: ' + subtype)
            send(dict(type='control_response', response=dict(subtype='success',
                      request_id=request['request_id'], response=result)))
        elif request['type'] == 'user':
            log('prompt', model=model, session=session)
            send(dict(type='assistant', session_id=session, uuid=str(uuid.uuid4()),
                      message=dict(id='message', type='message', role='assistant', model=model,
                                   content=[dict(type='text', text='fixture reply ' + model)],
                                   stop_reason='end_turn', stop_sequence=None,
                                   usage=dict(input_tokens=1, output_tokens=1))))
            send(dict(type='result', subtype='success', is_error=False, result='fixture reply ' + model,
                      session_id=session, uuid=str(uuid.uuid4()), duration_ms=1, duration_api_ms=0,
                      num_turns=1, total_cost_usd=0, usage=dict(input_tokens=1, output_tokens=1)))
        else:
            raise AssertionError('unexpected Claude frame: ' + request['type'])
