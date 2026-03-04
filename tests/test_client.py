"""Tests for fabric_import.client"""

import json
import unittest
from unittest.mock import patch, MagicMock

from fabric_import.client import FabricClient


class TestFabricClient(unittest.TestCase):
    """Tests for FabricClient with mocked authenticator and network."""

    def _make_client(self):
        """Create a FabricClient with mocked dependencies."""
        mock_auth = MagicMock()
        mock_auth.get_headers.return_value = {
            'Authorization': 'Bearer test',
            'Content-Type': 'application/json',
        }
        # Patch settings to avoid needing env vars
        with patch('fabric_import.config.settings.get_settings') as mock_settings:
            s = MagicMock()
            s.fabric_api_base_url = 'https://api.fabric.microsoft.com/v1'
            s.fabric_workspace_id = 'ws-test'
            s.deployment_timeout = 30
            s.retry_attempts = 1
            s.retry_delay = 1
            mock_settings.return_value = s
            client = FabricClient(authenticator=mock_auth)
        return client

    @patch('fabric_import.client.urlopen')
    def test_get_request(self, mock_urlopen):
        client = self._make_client()
        # Force urllib path
        client._use_requests = False
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({'items': []}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = client.get('/workspaces')
        self.assertEqual(result, {'items': []})

    @patch('fabric_import.client.urlopen')
    def test_post_request(self, mock_urlopen):
        client = self._make_client()
        client._use_requests = False
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({'id': 'new-1'}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = client.post('/items', data={'name': 'test'})
        self.assertEqual(result['id'], 'new-1')

    def test_list_workspaces(self):
        client = self._make_client()
        client._request = MagicMock(return_value={'value': [{'id': 'ws-1'}]})
        result = client.list_workspaces()
        client._request.assert_called_with('GET', '/workspaces', params=None)
        self.assertEqual(len(result['value']), 1)

    def test_get_workspace(self):
        client = self._make_client()
        client._request = MagicMock(return_value={'id': 'ws-1', 'name': 'Test'})
        result = client.get_workspace('ws-1')
        client._request.assert_called_with('GET', '/workspaces/ws-1', params=None)

    def test_list_items_no_filter(self):
        client = self._make_client()
        client._request = MagicMock(return_value={'value': []})
        client.list_items('ws-1')
        client._request.assert_called_with(
            'GET', '/workspaces/ws-1/items', params={},
        )

    def test_list_items_with_type_filter(self):
        client = self._make_client()
        client._request = MagicMock(return_value={'value': []})
        client.list_items('ws-1', item_type='Lakehouse')
        client._request.assert_called_with(
            'GET', '/workspaces/ws-1/items',
            params={'$filter': "type eq 'Lakehouse'"},
        )

    def test_put_request(self):
        client = self._make_client()
        client._request = MagicMock(return_value={'id': '1'})
        client.put('/items/1', data={'name': 'updated'})
        client._request.assert_called_with(
            'PUT', '/items/1', data={'name': 'updated'},
        )

    def test_patch_request(self):
        client = self._make_client()
        client._request = MagicMock(return_value={'id': '1'})
        client.patch('/items/1', data={'name': 'patched'})
        client._request.assert_called_with(
            'PATCH', '/items/1', data={'name': 'patched'},
        )

    def test_delete_request(self):
        client = self._make_client()
        client._request = MagicMock(return_value={})
        client.delete('/items/1')
        client._request.assert_called_with('DELETE', '/items/1')


if __name__ == '__main__':
    unittest.main()
