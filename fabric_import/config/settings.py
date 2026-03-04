"""
Configuration settings for Fabric deployment.

Reads settings from environment variables or a .env file.
Supports optional pydantic-settings integration.
"""

import os
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fallback settings (no pydantic dependency required)
# ---------------------------------------------------------------------------

class _FallbackSettings:
    """Simple env-var-backed settings object."""

    def __init__(self):
        self.FABRIC_TENANT_ID = os.getenv('FABRIC_TENANT_ID', '')
        self.FABRIC_CLIENT_ID = os.getenv('FABRIC_CLIENT_ID', '')
        self.FABRIC_CLIENT_SECRET = os.getenv('FABRIC_CLIENT_SECRET', '')
        self.FABRIC_WORKSPACE_ID = os.getenv('FABRIC_WORKSPACE_ID', '')
        self.FABRIC_API_BASE_URL = os.getenv(
            'FABRIC_API_BASE_URL', 'https://api.fabric.microsoft.com/v1'
        )
        self.FABRIC_LAKEHOUSE_NAME = os.getenv('FABRIC_LAKEHOUSE_NAME', '')
        self.FABRIC_ENVIRONMENT = os.getenv('FABRIC_ENVIRONMENT', 'development')
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # Lowercase property accessors (match pydantic-settings convention)
    @property
    def fabric_tenant_id(self):
        return self.FABRIC_TENANT_ID

    @property
    def fabric_client_id(self):
        return self.FABRIC_CLIENT_ID

    @property
    def fabric_client_secret(self):
        return self.FABRIC_CLIENT_SECRET

    @property
    def fabric_workspace_id(self):
        return self.FABRIC_WORKSPACE_ID

    @property
    def fabric_api_base_url(self):
        return self.FABRIC_API_BASE_URL

    @property
    def fabric_lakehouse_name(self):
        return self.FABRIC_LAKEHOUSE_NAME

    @property
    def fabric_environment(self):
        return self.FABRIC_ENVIRONMENT

    @property
    def log_level(self):
        return self.LOG_LEVEL

    @property
    def deployment_timeout(self):
        return int(os.getenv('DEPLOYMENT_TIMEOUT', '30'))

    @property
    def retry_attempts(self):
        return int(os.getenv('RETRY_ATTEMPTS', '3'))

    @property
    def retry_delay(self):
        return int(os.getenv('RETRY_DELAY', '1'))


# ---------------------------------------------------------------------------
# Try pydantic-settings first; fall back gracefully
# ---------------------------------------------------------------------------

_settings_instance = None


def get_settings():
    """Return a singleton settings object."""
    global _settings_instance
    if _settings_instance is not None:
        return _settings_instance

    try:
        from pydantic_settings import BaseSettings

        class Settings(BaseSettings):
            FABRIC_TENANT_ID: str = ''
            FABRIC_CLIENT_ID: str = ''
            FABRIC_CLIENT_SECRET: str = ''
            FABRIC_WORKSPACE_ID: str = ''
            FABRIC_API_BASE_URL: str = 'https://api.fabric.microsoft.com/v1'
            FABRIC_LAKEHOUSE_NAME: str = ''
            FABRIC_ENVIRONMENT: str = 'development'
            LOG_LEVEL: str = 'INFO'
            DEPLOYMENT_TIMEOUT: int = 30
            RETRY_ATTEMPTS: int = 3
            RETRY_DELAY: int = 1

            # Lowercase aliases for consumer code compatibility
            @property
            def fabric_tenant_id(self):
                return self.FABRIC_TENANT_ID

            @property
            def fabric_client_id(self):
                return self.FABRIC_CLIENT_ID

            @property
            def fabric_client_secret(self):
                return self.FABRIC_CLIENT_SECRET

            @property
            def fabric_workspace_id(self):
                return self.FABRIC_WORKSPACE_ID

            @property
            def fabric_api_base_url(self):
                return self.FABRIC_API_BASE_URL

            @property
            def fabric_lakehouse_name(self):
                return self.FABRIC_LAKEHOUSE_NAME

            @property
            def deployment_timeout(self):
                return self.DEPLOYMENT_TIMEOUT

            @property
            def retry_attempts(self):
                return self.RETRY_ATTEMPTS

            @property
            def retry_delay(self):
                return self.RETRY_DELAY

            class Config:
                env_file = '.env'
                env_file_encoding = 'utf-8'

        _settings_instance = Settings()
    except ImportError:
        logger.debug('pydantic-settings not installed; using fallback env vars')
        _settings_instance = _FallbackSettings()

    return _settings_instance
