"""Refresh through the normal host-owned route with inert product artifacts.

Synthetic signed acceptance only exercises source custody. No application,
provider, SDK, native gateway, install or user credential is executed.
"""
from dataclasses import replace
import hashlib
import json
from unittest.mock import AsyncMock, Mock

import pytest

from launchkit import server
from launchkit.local_application_registry import LocalApplicationRegistry
from launchkit.projects import DraftConflict
from launchkit.runtime_selection import candidate_selection
from test_local_application_registry import registry, new_guide, build, accept_source_fixture, workspace
from test_meta_toolkit_preserved_refresh import products
from test_meta_toolkit_runtime_selection import product, no_product_execution
from test_implementation_runtime_refresh import accepted_route_status
from test_implementation_stop_route import stop_api


def inventory(root):
    return {str(p.relative_to(root)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mode & 0o777)
        for p in root.rglob('*') if p.is_file() and not p.name.endswith('.lock')}


async def test_normal_refresh_selects_new_trusted_host_product_and_preserves_accepted_application(
        registry, products, monkeypatch, tmp_path):
    f = registry
    saved = new_guide(f)
    first = await build(f, saved)
    old = await accept_source_fixture(f, first)
    directory = f.implementation._directory(saved['id'], first['build_id'])
    package, _ = await f.implementation.package(saved['id'], first['build_id'])
    retained_package = tmp_path / 'original-accepted-package.zip'
    retained_package.write_bytes(package)
    before = inventory(directory)
    old_files = {item.path: (item.as_bytes(), item.mode) for item in old.export_files(require_verified=False)}
    new_target = products[1].target
    assert candidate_selection(new_target) != candidate_selection(f.target)
    # Model the normal restarted launcher binding, not a mutation of the old owner.
    current_host = LocalApplicationRegistry(guide_store=f.store, implementation=f.implementation,
        deployment=f.service, runtime_target=new_target)
    f.implementation.prepare_workspace = current_host.prepare_workspace
    monkeypatch.setattr(server, 'implementation', f.implementation)
    monkeypatch.setattr(server, 'application_host', current_host)
    result = await server.build_implementation(saved['id'], server.ImplementationRequest(
        version=saved['version'], previous_build_id=first['build_id'],
        refresh_framework=True, preserve_design=True))
    assert result['build_id'] != first['build_id'] and not result['ready_to_package']
    successor = workspace(f.implementation, result)
    frozen = json.loads((f.implementation._directory(saved['id'], result['build_id']) / 'input.json').read_bytes())
    assert frozen['runtime_candidate'] == candidate_selection(new_target)
    assert frozen['framework']['runtime_candidate'] == candidate_selection(new_target)
    assert frozen['runtime_refresh_predecessor'] == {
        'build_id': first['build_id'], 'source_digest': old.digest()}
    for item in successor.export_files(require_verified=False):
        if item.path.startswith(('application/', 'config/')) or item.path == 'blueprint.json':
            assert (item.as_bytes(), item.mode) == old_files[item.path]
    assert successor.verification() is None
    assert inventory(directory) == before
    assert retained_package.read_bytes() == package
    # Native work and acceptance still belong to the new build, not its predecessor.
    assert not result['acceptance_trusted']
    assert candidate_selection(f.registry.runtime_target) == candidate_selection(f.target)


@pytest.mark.parametrize('defect', ['foreign-owner', 'sdk-family', 'platform', 'source-race'])
def test_refresh_route_refuses_invalid_current_host_selection_before_worker_admission(
        stop_api, products, monkeypatch, defect):
    client, endpoint = stop_api
    old_target, new_target = products[0].target, products[1].target
    endpoint.status = AsyncMock(return_value=accepted_route_status())
    endpoint.submit.return_value = {'build_id': 'new', 'status': 'queued'}
    endpoint.selected_runtime_target = Mock(return_value=old_target)
    owner = object.__new__(LocalApplicationRegistry)
    selection = {'runtime_target': new_target}
    if defect == 'foreign-owner':
        owner = Mock()
    elif defect == 'sdk-family':
        selection = {}
    elif defect == 'platform':
        selection = {'runtime_target': replace(new_target, target='linux-x86_64')}
    if defect != 'foreign-owner':
        owner.initial_build_options = Mock(return_value=selection,
            side_effect=DraftConflict('The trusted guide selection changed.') if defect == 'source-race' else None)
    monkeypatch.setattr(server, 'application_host', owner)
    response = client.post('/api/guide/selected-guide/implementation', json={
        'version': 8, 'previous_build_id': 'accepted-build', 'refresh_framework': True, 'preserve_design': True})
    assert response.status_code == 409
    endpoint.submit.assert_not_awaited()


@pytest.mark.parametrize('host_kind', ['registry', 'manual'])
def test_refresh_route_authenticates_predecessor_before_forwarding_current_trusted_target(
        stop_api, products, monkeypatch, host_kind):
    from launchkit.forge_application_host import ForgeApplicationHost
    client, endpoint = stop_api
    old_target, new_target = products[0].target, products[1].target
    endpoint.status = AsyncMock(return_value=accepted_route_status())
    endpoint.selected_runtime_target = Mock(return_value=old_target)
    endpoint.submit.return_value = {'build_id': 'new', 'status': 'queued'}
    owner = object.__new__(LocalApplicationRegistry if host_kind == 'registry' else ForgeApplicationHost)
    def select(*args):
        endpoint.selected_runtime_target.assert_called_once_with('selected-guide',
            build_id='accepted-build', source_digest='a' * 64)
        return {'runtime_target': new_target}
    owner.initial_build_options = Mock(side_effect=select)
    monkeypatch.setattr(server, 'application_host', owner)
    response = client.post('/api/guide/selected-guide/implementation', json={
        'version': 8, 'previous_build_id': 'accepted-build', 'refresh_framework': True, 'preserve_design': True})
    assert response.status_code == 202
    owner.initial_build_options.assert_called_once_with(*(
        ('selected-guide', 8) if host_kind == 'registry' else ('selected-guide',)))
    endpoint.submit.assert_awaited_once_with('selected-guide', 8, '',
        expected_selected_build_id='accepted-build', refresh_framework=True,
        preserve_design=True, runtime_target=new_target)
