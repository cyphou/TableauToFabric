"""Tests for fabric_import.config (settings and environments)"""

import os
import unittest
from unittest.mock import patch

from fabric_import.config.environments import EnvironmentType, EnvironmentConfig


class TestEnvironmentType(unittest.TestCase):
    """Tests for EnvironmentType enum."""

    def test_values(self):
        self.assertEqual(EnvironmentType.DEVELOPMENT.value, 'development')
        self.assertEqual(EnvironmentType.STAGING.value, 'staging')
        self.assertEqual(EnvironmentType.PRODUCTION.value, 'production')

    def test_from_string(self):
        self.assertEqual(EnvironmentType('development'), EnvironmentType.DEVELOPMENT)
        self.assertEqual(EnvironmentType('staging'), EnvironmentType.STAGING)
        self.assertEqual(EnvironmentType('production'), EnvironmentType.PRODUCTION)

    def test_invalid_raises(self):
        with self.assertRaises(ValueError):
            EnvironmentType('invalid')


class TestEnvironmentConfig(unittest.TestCase):
    """Tests for EnvironmentConfig."""

    def test_development_defaults(self):
        cfg = EnvironmentConfig(EnvironmentType.DEVELOPMENT)
        self.assertEqual(cfg.log_level, 'DEBUG')
        self.assertEqual(cfg.retry_count, 1)
        self.assertEqual(cfg.timeout, 30)
        self.assertIn('fabric.microsoft.com', cfg.api_base)

    def test_staging_defaults(self):
        cfg = EnvironmentConfig(EnvironmentType.STAGING)
        self.assertEqual(cfg.log_level, 'INFO')
        self.assertEqual(cfg.retry_count, 3)
        self.assertEqual(cfg.timeout, 60)

    def test_production_defaults(self):
        cfg = EnvironmentConfig(EnvironmentType.PRODUCTION)
        self.assertEqual(cfg.log_level, 'WARNING')
        self.assertEqual(cfg.retry_count, 5)
        self.assertEqual(cfg.timeout, 120)

    def test_string_input(self):
        cfg = EnvironmentConfig('production')
        self.assertEqual(cfg.environment, EnvironmentType.PRODUCTION)

    def test_environment_stored(self):
        cfg = EnvironmentConfig(EnvironmentType.STAGING)
        self.assertEqual(cfg.environment, EnvironmentType.STAGING)


class TestFallbackSettings(unittest.TestCase):
    """Tests for _FallbackSettings and get_settings()."""

    def test_fallback_defaults(self):
        from fabric_import.config.settings import _FallbackSettings
        s = _FallbackSettings()
        self.assertIn('fabric.microsoft.com', s.FABRIC_API_BASE_URL)
        self.assertEqual(s.FABRIC_ENVIRONMENT, 'development')
        self.assertEqual(s.LOG_LEVEL, 'INFO')

    def test_lowercase_property_accessors(self):
        from fabric_import.config.settings import _FallbackSettings
        s = _FallbackSettings()
        self.assertIn('fabric.microsoft.com', s.fabric_api_base_url)
        self.assertEqual(s.fabric_environment, 'development')
        self.assertEqual(s.log_level, 'INFO')
        self.assertEqual(s.fabric_tenant_id, '')
        self.assertEqual(s.fabric_client_id, '')
        self.assertEqual(s.fabric_client_secret, '')
        self.assertEqual(s.fabric_workspace_id, '')

    def test_deployment_properties(self):
        from fabric_import.config.settings import _FallbackSettings
        s = _FallbackSettings()
        self.assertEqual(s.deployment_timeout, 30)
        self.assertEqual(s.retry_attempts, 3)
        self.assertEqual(s.retry_delay, 1)

    @patch.dict(os.environ, {
        'FABRIC_TENANT_ID': 'test-tenant',
        'FABRIC_WORKSPACE_ID': 'test-workspace',
    })
    def test_reads_env_vars(self):
        from fabric_import.config.settings import _FallbackSettings
        s = _FallbackSettings()
        self.assertEqual(s.FABRIC_TENANT_ID, 'test-tenant')
        self.assertEqual(s.FABRIC_WORKSPACE_ID, 'test-workspace')

    def test_get_settings_returns_object(self):
        # Reset singleton for test
        import fabric_import.config.settings as settings_mod
        settings_mod._settings_instance = None
        try:
            s = settings_mod.get_settings()
            self.assertIsNotNone(s)
            self.assertTrue(hasattr(s, 'FABRIC_API_BASE_URL') or
                            hasattr(s, 'fabric_api_base_url'))
        finally:
            settings_mod._settings_instance = None

    def test_get_settings_singleton(self):
        import fabric_import.config.settings as settings_mod
        settings_mod._settings_instance = None
        try:
            s1 = settings_mod.get_settings()
            s2 = settings_mod.get_settings()
            self.assertIs(s1, s2)
        finally:
            settings_mod._settings_instance = None


if __name__ == '__main__':
    unittest.main()
