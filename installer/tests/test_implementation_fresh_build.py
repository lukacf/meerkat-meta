"""Fresh build admission uses real owners and inert native/build boundaries.

These are source admission regressions, not installed or provider execution.
"""
import asyncio
import json
import threading

import pytest

from launchkit.implementation import update_receipt
from launchkit.implementation_workspace import ImplementationWorkspace
from launchkit.projects import DraftConflict
from test_local_application_registry import (
    registry, product, no_product_execution, no_processes, new_guide, build,
    workspace, host_receipt,
)
from test_retained_ordinary_application import newer_target, upgraded_registry
from test_implementation_stop_route import stop_api


async def incomplete(f):
    saved = new_guide(f)
    first = await build(f, saved, 'Old repair request must stay with the old build.')
    old = workspace(f.implementation, first)
    old.write_file('application/unfinished.txt', 'Retain this only in the previous build.\n')
    check = host_receipt(old)
    task = f.implementation.tasks.pop(first['build_id'])
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    directory = f.implementation._directory(saved['id'])
    update_receipt(directory, status='interrupted', receipt=check, checks=check['checks'],
        ready_to_package=False, cleanup_pending=False, cleanup_in_progress=False,
        worker_stop_confirmed=True)
    return saved, first, old, directory


def successor(f, target, monkeypatch):
    implementation, deployment, owner = upgraded_registry(f, target, ())
    monkeypatch.setattr(implementation, '_run', f.implementation._run)
    return implementation, owner


async def test_fresh_build_selects_new_runtime_without_copying_source_proof_or_notes(registry, newer_target, monkeypatch):
    f = registry
    saved, first, old, directory = await incomplete(f)
    original = {name: (directory / name).read_bytes() for name in ['input.json', 'receipt.json']}
    digest = old.digest()
    implementation, owner = successor(f, newer_target, monkeypatch)
    options = owner.initial_build_options(saved['id'], saved['version'])
    try:
        with pytest.raises(DraftConflict, match='Preserve the selected product runtime'):
            await implementation.submit(saved['id'], saved['version'],
                expected_selected_build_id=first['build_id'], **options)
        result = await implementation.submit(saved['id'], saved['version'], start_fresh=True,
            expected_fresh_source_digest=digest, expected_selected_build_id=first['build_id'], **options)
        new = workspace(implementation, result)
        frozen = json.loads((implementation._directory(saved['id']) / 'input.json').read_text())
        assert frozen['fresh_build_predecessor'] == {'build_id': first['build_id'], 'source_digest': digest}
        assert frozen['runtime_candidate']['assembly']['sha256'] == newer_target.assembly.sha256
        assert frozen['runtime_candidate']['assembly']['sha256'] != f.target.assembly.sha256
        assert frozen['framework']['reason'] == 'explicit-fresh-build'
        assert frozen['previous_implementation'] is None and frozen['implementation_requests'] == []
        assert frozen['request'] == '' and frozen['blueprint'] == saved['proposal']
        assert not (new.project / 'application/unfinished.txt').exists()
        assert not result['ready_to_package'] and not result['acceptance_trusted']
        assert new.verification() is None
        assert old.digest() == digest
        for name, content in original.items():
            assert (directory / name).read_bytes() == content
        with pytest.raises(DraftConflict):
            await implementation.submit(saved['id'], saved['version'], start_fresh=True,
                expected_fresh_source_digest=digest, expected_selected_build_id=first['build_id'], **options)
        assert len(implementation.tasks) == 1
    finally:
        await implementation.shutdown()


@pytest.mark.parametrize('change', ['source', 'running', 'completed', 'cleanup', 'unconfirmed'])
async def test_fresh_build_refuses_stale_or_unsettled_predecessor(registry, newer_target, monkeypatch, change):
    f = registry
    saved, first, old, directory = await incomplete(f)
    digest = old.digest()
    if change == 'source':
        old.write_file('application/later.txt', 'Newer unsent work.\n')
    elif change in {'running', 'completed'}:
        update_receipt(directory, status=change)
    elif change == 'cleanup':
        update_receipt(directory, cleanup_pending=True)
    else:
        update_receipt(directory, worker_stop_confirmed=False)
    implementation, owner = successor(f, newer_target, monkeypatch)
    try:
        with pytest.raises(DraftConflict):
            await implementation.submit(saved['id'], saved['version'], start_fresh=True,
                expected_fresh_source_digest=digest, expected_selected_build_id=first['build_id'],
                **owner.initial_build_options(saved['id'], saved['version']))
        assert not implementation.tasks
        assert len(list(directory.parent.glob('*/receipt.json'))) == 1
    finally:
        await implementation.shutdown()


async def test_fresh_build_fences_source_changes_during_preparation(registry, newer_target, monkeypatch):
    f = registry
    saved, first, old, directory = await incomplete(f)
    digest = old.digest()
    implementation, owner = successor(f, newer_target, monkeypatch)
    entered, release = threading.Event(), threading.Event()
    original = ImplementationWorkspace.seed

    def held(new, *args, **kwargs):
        if kwargs.get('fresh_reason') == 'explicit-fresh-build':
            entered.set()
            assert release.wait(10)
        return original(new, *args, **kwargs)

    monkeypatch.setattr(ImplementationWorkspace, 'seed', held)
    task = asyncio.create_task(implementation.submit(saved['id'], saved['version'], start_fresh=True,
        expected_fresh_source_digest=digest, expected_selected_build_id=first['build_id'],
        **owner.initial_build_options(saved['id'], saved['version'])))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        old.write_file('application/later.txt', 'Changed during preparation.\n')
        release.set()
        with pytest.raises(DraftConflict, match='changed while preparing'):
            await task
        assert not implementation.tasks
        assert len(list(directory.parent.glob('*/receipt.json'))) == 1
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await implementation.shutdown()


def test_fresh_build_route_forwards_explicit_selection_without_repair_note(stop_api):
    client, implementation = stop_api
    implementation.submit.return_value = {'build_id': 'new', 'status': 'queued'}
    result = client.post('/api/guide/guide/implementation', json={'version': 3,
        'start_fresh': True, 'previous_build_id': 'old', 'previous_source_digest': 'a' * 64})
    assert result.status_code == 202
    implementation.submit.assert_awaited_once_with('guide', 3, '', expected_selected_build_id='old',
        refresh_framework=False, start_fresh=True, expected_fresh_source_digest='a' * 64)


@pytest.mark.parametrize('change', [
    {'start_fresh': 'true'}, {'previous_source_digest': None}, {'previous_build_id': None},
    {'request': 'A new repair note'}, {'refresh_framework': True}, {'preserve_design': True},
    {'start_fresh': False},
])
def test_fresh_build_route_refuses_ambiguous_or_mixed_requests(stop_api, change):
    client, implementation = stop_api
    body = {'version': 3, 'start_fresh': True, 'previous_build_id': 'old',
        'previous_source_digest': 'a' * 64, **change}
    result = client.post('/api/guide/guide/implementation', json=body)
    assert result.status_code == 422
    implementation.submit.assert_not_awaited()
