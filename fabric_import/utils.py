"""
Utility helpers for Fabric deployment and caching.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class DeploymentReport:
    """Track deployment results and generate a report."""

    def __init__(self):
        self.entries = []

    def add(self, artifact_name, artifact_type, status, details=None):
        self.entries.append({
            'artifact_name': artifact_name,
            'artifact_type': artifact_type,
            'status': status,
            'details': details or {},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        })

    def succeeded(self):
        return [e for e in self.entries if e['status'] == 'success']

    def failed(self):
        return [e for e in self.entries if e['status'] == 'failed']

    def summary(self):
        return {
            'total': len(self.entries),
            'succeeded': len(self.succeeded()),
            'failed': len(self.failed()),
            'entries': self.entries,
        }

    def save(self, output_path):
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.summary(), f, indent=2)
        logger.info(f'Deployment report saved: {output_path}')


class ArtifactCache:
    """Simple file-based cache for artifact metadata."""

    def __init__(self, cache_dir=None):
        self.cache_dir = Path(cache_dir or os.path.join(
            os.path.expanduser('~'), '.tableautofabric', 'cache'
        ))
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, workspace_id, artifact_name):
        safe = artifact_name.replace(' ', '_').replace('/', '_')
        return f'{workspace_id}_{safe}.json'

    def get(self, workspace_id, artifact_name):
        key = self._cache_key(workspace_id, artifact_name)
        path = self.cache_dir / key
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def set(self, workspace_id, artifact_name, data):
        key = self._cache_key(workspace_id, artifact_name)
        path = self.cache_dir / key
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

    def clear(self, workspace_id=None):
        pattern = f'{workspace_id}_*' if workspace_id else '*'
        for path in self.cache_dir.glob(pattern):
            path.unlink(missing_ok=True)
            logger.debug(f'Cache cleared: {path.name}')
