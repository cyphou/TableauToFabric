"""
Environment-specific configuration for Fabric deployments.
"""

from enum import Enum


class EnvironmentType(str, Enum):
    DEVELOPMENT = 'development'
    STAGING = 'staging'
    PRODUCTION = 'production'


class EnvironmentConfig:
    """Per-environment overrides."""

    _DEFAULTS = {
        EnvironmentType.DEVELOPMENT: {
            'api_base': 'https://api.fabric.microsoft.com/v1',
            'log_level': 'DEBUG',
            'retry_count': 1,
            'timeout': 30,
        },
        EnvironmentType.STAGING: {
            'api_base': 'https://api.fabric.microsoft.com/v1',
            'log_level': 'INFO',
            'retry_count': 3,
            'timeout': 60,
        },
        EnvironmentType.PRODUCTION: {
            'api_base': 'https://api.fabric.microsoft.com/v1',
            'log_level': 'WARNING',
            'retry_count': 5,
            'timeout': 120,
        },
    }

    def __init__(self, environment=EnvironmentType.DEVELOPMENT):
        if isinstance(environment, str):
            environment = EnvironmentType(environment)
        self.environment = environment
        self._cfg = self._DEFAULTS.get(environment, self._DEFAULTS[EnvironmentType.DEVELOPMENT])

    @property
    def api_base(self):
        return self._cfg['api_base']

    @property
    def log_level(self):
        return self._cfg['log_level']

    @property
    def retry_count(self):
        return self._cfg['retry_count']

    @property
    def timeout(self):
        return self._cfg['timeout']
