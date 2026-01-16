"""
Executable entrypoint for the metadata API.

Configure ClickHouse connection via environment variables:
  CH_HOST (default: localhost)
  CH_PORT (default: 8123)
  CH_USER (default: developer)
  CH_PASSWORD (default: developer)
  CH_SECURE (default: false)
  CH_VERIFY (default: false)
"""

from __future__ import annotations

import os

from .app import create_app
from .cluster_store import ClusterSettings, ClusterStore
from .services import ClickHouseMetadataService


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.lower() in {"1", "true", "yes", "on"}


def _build_store() -> ClusterStore:
    store = ClusterStore()
    settings = ClusterSettings(
        host=os.getenv("CH_HOST", "localhost"),
        port=int(os.getenv("CH_PORT", "8123")),
        user=os.getenv("CH_USER", "developer"),
        password=os.getenv("CH_PASSWORD", "developer"),
        secure=_env_bool("CH_SECURE", False),
        verify=_env_bool("CH_VERIFY", False),
        read_only=_env_bool("CH_READ_ONLY", False),
    )
    store.add_cluster("default", settings, make_active=True)
    return store


cluster_store = _build_store()
service = ClickHouseMetadataService(cluster_store)
app = create_app(service, cluster_store=cluster_store)
