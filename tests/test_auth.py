"""Tests for fabric_import.auth"""

import unittest
from unittest.mock import patch, MagicMock


class TestFabricAuthenticator(unittest.TestCase):
    """Tests for FabricAuthenticator — mocks azure-identity."""

    @patch('fabric_import.auth.ClientSecretCredential')
    @patch('fabric_import.auth.DefaultAzureCredential')
    @patch('fabric_import.config.settings.get_settings')
    def test_init_service_principal(self, mock_settings, mock_default, mock_csc):
        mock_settings.return_value = MagicMock(
            fabric_tenant_id='t-id',
            fabric_client_id='c-id',
            fabric_client_secret='c-secret',
        )
        mock_csc.return_value = MagicMock()

        from fabric_import.auth import FabricAuthenticator
        auth = FabricAuthenticator(use_managed_identity=False)
        mock_csc.assert_called_once_with(
            tenant_id='t-id', client_id='c-id', client_secret='c-secret',
        )

    @patch('fabric_import.auth.DefaultAzureCredential')
    @patch('fabric_import.config.settings.get_settings')
    def test_init_managed_identity(self, mock_settings, mock_default):
        mock_settings.return_value = MagicMock()
        mock_default.return_value = MagicMock()

        from fabric_import.auth import FabricAuthenticator
        auth = FabricAuthenticator(use_managed_identity=True)
        mock_default.assert_called_once()

    @patch('fabric_import.auth.ClientSecretCredential')
    @patch('fabric_import.auth.DefaultAzureCredential')
    @patch('fabric_import.config.settings.get_settings')
    def test_get_token(self, mock_settings, mock_default, mock_csc):
        mock_settings.return_value = MagicMock(
            fabric_tenant_id='t', fabric_client_id='c', fabric_client_secret='s',
        )
        mock_credential = MagicMock()
        mock_token = MagicMock()
        mock_token.token = 'test-token-123'
        mock_credential.get_token.return_value = mock_token
        mock_csc.return_value = mock_credential

        from fabric_import.auth import FabricAuthenticator
        auth = FabricAuthenticator(use_managed_identity=False)
        token = auth.get_token()
        self.assertEqual(token, 'test-token-123')

    @patch('fabric_import.auth.ClientSecretCredential')
    @patch('fabric_import.auth.DefaultAzureCredential')
    @patch('fabric_import.config.settings.get_settings')
    def test_get_headers(self, mock_settings, mock_default, mock_csc):
        mock_settings.return_value = MagicMock(
            fabric_tenant_id='t', fabric_client_id='c', fabric_client_secret='s',
        )
        mock_credential = MagicMock()
        mock_token = MagicMock()
        mock_token.token = 'bearer-token'
        mock_credential.get_token.return_value = mock_token
        mock_csc.return_value = mock_credential

        from fabric_import.auth import FabricAuthenticator
        auth = FabricAuthenticator(use_managed_identity=False)
        headers = auth.get_headers()
        self.assertEqual(headers['Authorization'], 'Bearer bearer-token')
        self.assertEqual(headers['Content-Type'], 'application/json')


if __name__ == '__main__':
    unittest.main()
