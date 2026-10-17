import asyncio
from types import SimpleNamespace

import pytest

from launchkit.guide_models import starter_state
from launchkit.guide_operations import GuideOperations
from launchkit.guide_store import GuideStore
from launchkit.server import GuideResponse


@pytest.mark.asyncio
@pytest.mark.parametrize('typed', [True, False])
async def test_missing_publication_category_is_typed_and_does_not_copy_private_failure_text(tmp_path, typed):
    from launchkit.guide_models import GuidePublicationMissing
    store = GuideStore(tmp_path)
    initial = store.create(starter_state({}))
    private = 'publication_missing token=private-secret /private/source/folder'
    failure = GuidePublicationMissing(private) if typed else RuntimeError(private)

    async def get(identity):
        return store.get(identity)

    async def respond(identity, value):
        raise failure

    service = SimpleNamespace(store=store, get=get, respond=respond)
    operations = GuideOperations(wait_seconds=1)
    value = GuideResponse(version=initial['version'], action='chat', text='Review my current progress.')
    with pytest.raises(type(failure)):
        await operations.submit(service, initial['id'], value)
    saved = await operations.read(service, initial['id'])
    assert saved['version'] == initial['version'] and saved['messages'] == initial['messages']
    assert saved['operation']['status'] == 'failed'
    assert saved['operation']['request'] == value.model_dump(mode='json')
    assert 'private-secret' not in str(saved) and '/private/source/folder' not in str(saved)
    if typed:
        assert saved['operation']['failure'] == {'phase': 'publication', 'category': 'publication_missing'}
        assert 'did not save' in saved['operation']['detail']
    else:
        assert 'failure' not in saved['operation']
        assert 'publication_missing' not in saved['operation']['detail']


@pytest.mark.asyncio
async def test_late_response_is_committed_once_after_http_deadline_and_reload(tmp_path):
    store = GuideStore(tmp_path)
    initial = store.create(starter_state({}))
    release = asyncio.Event()
    sends = []
    async def get(identity):
        return store.get(identity)
    async def respond(identity, value):
        sends.append(value.text)
        await release.wait()
        state = store.get(identity)
        state['summary'] = 'The late completed result'
        return store.save(state, value.version)
    service = SimpleNamespace(store=store, get=get, respond=respond)
    operations = GuideOperations(wait_seconds=0.01)
    request = GuideResponse(version=initial['version'], text='One user request')
    pending, deferred = await operations.submit(service, initial['id'], request)
    assert deferred and pending['operation']['status'] == 'running'
    assert (await operations.read(service, initial['id']))['operation']['request']['text'] == request.text
    duplicate, deferred = await operations.submit(service, initial['id'], request)
    assert deferred and len(sends) == 1
    release.set()
    await asyncio.gather(*list(operations.tasks.values()))
    reloaded = await operations.read(service, initial['id'])
    assert reloaded['version'] == initial['version'] + 1
    assert reloaded['summary'] == 'The late completed result'
    result, deferred = await operations.submit(service, initial['id'], request)
    assert not deferred and result['version'] == reloaded['version'] and len(sends) == 1


@pytest.mark.asyncio
async def test_server_restart_retains_submitted_intent_without_automatic_resend(tmp_path):
    store = GuideStore(tmp_path)
    state = store.create(starter_state({}))
    store.save_operation(state['id'], {'status': 'running', 'version': state['version'], 'request': {'text': 'Preserve this answer'}})
    async def get(identity): return store.get(identity)
    recovered = await GuideOperations().read(SimpleNamespace(store=store, get=get), state['id'])
    assert recovered['operation']['status'] == 'interrupted'
    assert recovered['operation']['request']['text'] == 'Preserve this answer'
    assert recovered['version'] == state['version']


def test_history_retains_multiple_named_setups(tmp_path):
    store = GuideStore(tmp_path)
    first = store.create(starter_state({}))
    second = store.create(starter_state({}))
    assert {row['id'] for row in store.list()} == {first['id'], second['id']}


@pytest.mark.asyncio
async def test_native_transport_retries_recover_same_request_without_new_admission(tmp_path, monkeypatch):
    import httpx
    from launchkit.architect import ArchitectService
    from launchkit.blueprint import starter_blueprint
    store = GuideStore(tmp_path / 'guides')
    state = store.create(starter_state({}))
    architect = ArchitectService(tmp_path / 'runtime')
    sends, reads = [], []
    class Agent:
        async def inspect(self): return SimpleNamespace(output_preview='Recovered native result')
    architect.runtime = SimpleNamespace(rust_http_base_url='http://native.test', agent=lambda _: Agent())
    async def start(): return architect.status()
    monkeypatch.setattr(architect, 'start', start)
    class Client:
        def __init__(self, *args, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def post(self, url, **kwargs):
            sends.append(kwargs['json']['idempotency_key'])
            if len(sends) == 1: raise httpx.ReadTimeout('Acceptance response lost')
            return httpx.Response(200, request=httpx.Request('POST', url), json={'cursor':1, 'interaction_id':'same-admitted-turn'})
        async def get(self, url, **kwargs):
            reads.append(kwargs['params'])
            if len(reads) == 1: raise httpx.ReadTimeout('Timeline temporarily unavailable')
            return httpx.Response(200, request=httpx.Request('GET', url), json={'frames':[{
                'cursor':2, 'kind':'interaction_complete', 'identity':'architect:main',
                'interaction_id':'same-admitted-turn', 'source':{'kind':'console_event'},
                'payload':{'result':'Recovered native result', 'source_event_type':'run_completed'}}]})
    monkeypatch.setattr(httpx, 'AsyncClient', Client)
    async def get(identity): return store.get(identity)
    async def respond(identity, value):
        result = await architect.message(value.text, starter_blueprint().model_dump(mode='json'))
        saved = store.get(identity)
        saved['summary'] = result['message']
        return store.save(saved, value.version)
    service = SimpleNamespace(store=store, get=get, respond=respond)
    operations = GuideOperations(wait_seconds=0.01)
    value = GuideResponse(version=1, action='chat', text='One change')
    saved, pending = await operations.submit(service, state['id'], value)
    assert pending and saved['operation']['status'] == 'running'
    await asyncio.wait_for(operations.tasks[state['id']], timeout=4)
    saved = await operations.read(service, state['id'])
    assert saved['version'] == 2 and saved['summary'] == 'Recovered native result'
    assert saved['operation']['status'] == 'completed'
    assert len(sends) == 2 and len(set(sends)) == 1
    assert len(reads) == 2
    await operations.submit(service, state['id'], value)
    assert len(sends) == 2


@pytest.mark.asyncio
async def test_poll_never_pairs_a_completed_receipt_with_the_previous_response(tmp_path):
    store = GuideStore(tmp_path)
    state = store.create(starter_state({}))
    identity = state['id']
    receipt = {'request_id': 'first', 'version': 1, 'status': 'running', 'request': {'text': 'Original answer'}}
    store.save_operation(identity, receipt)
    first = True
    async def get(identity):
        nonlocal first
        snapshot = store.get(identity)
        if first:
            first = False
            store.save({**snapshot, 'summary': 'Completed during poll'}, 1)
            store.save_operation(identity, {**receipt, 'status': 'completed'})
        return snapshot
    view = await GuideOperations().read(SimpleNamespace(store=store, get=get), identity)
    assert view['operation']['status'] == 'completed'
    assert view['version'] == 2 and view['summary'] == 'Completed during poll'


@pytest.mark.asyncio
async def test_retry_completion_shortcut_returns_the_committed_response(tmp_path):
    from launchkit.projects import fingerprint
    store = GuideStore(tmp_path)
    state = store.create(starter_state({}))
    identity = state['id']
    value = GuideResponse(version=1, text='Original answer')
    receipt = {'request_id': fingerprint({'guide_id': identity, **value.model_dump(mode='json')}), 'version': 1,
               'status': 'running', 'request': value.model_dump(mode='json')}
    store.save_operation(identity, receipt)
    first = True
    async def get(identity):
        nonlocal first
        snapshot = store.get(identity)
        if first:
            first = False
            store.save({**snapshot, 'summary': 'Completed while retrying'}, 1)
            store.save_operation(identity, {**receipt, 'status': 'completed'})
        return snapshot
    async def respond(*args):
        pytest.fail('Completed request must not be admitted again')
    view, pending = await GuideOperations().submit(SimpleNamespace(store=store, get=get, respond=respond), identity, value)
    assert not pending
    assert view['version'] == 2 and view['summary'] == 'Completed while retrying'
