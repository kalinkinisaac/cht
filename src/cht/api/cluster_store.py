from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from cht.cluster import Cluster

logger = logging.getLogger(__name__)


@dataclass
class ClusterSettings:
    host: str
    port: int
    user: str
    password: str
    secure: bool = False
    verify: bool = False
    read_only: bool = False


class ClusterStore:
    """
    In-memory registry for ClickHouse connections with active selection support.
    """

    def __init__(self) -> None:
        self._configs: Dict[str, ClusterSettings] = {}
        self._instances: Dict[str, Cluster] = {}
        self._active: Optional[str] = None

    def add_cluster(
        self,
        name: str,
        settings: ClusterSettings,
        *,
        make_active: bool = False,
    ) -> Cluster:
        logger.info(f"Adding cluster '{name}' with settings: {settings}")
        try:
            self._configs[name] = settings
            self._instances[name] = Cluster(
                name=name,
                host=settings.host,
                port=settings.port,
                user=settings.user,
                password=settings.password,
                read_only=settings.read_only,
                secure=settings.secure,
                verify=settings.verify,
            )
            if self._active is None or make_active:
                self._active = name
                logger.info(f"Set cluster '{name}' as active")
            logger.info(f"Successfully added cluster '{name}'")
            return self._instances[name]
        except Exception as e:
            logger.error(f"Failed to add cluster '{name}': {e}")
            raise

    def add_cluster_instance(
        self,
        name: str,
        settings: ClusterSettings,
        cluster: Cluster,
        *,
        make_active: bool = False,
    ) -> None:
        """Register an existing Cluster instance (useful for testing/mocking)."""
        self._configs[name] = settings
        self._instances[name] = cluster
        if self._active is None or make_active:
            self._active = name

    def list_clusters(self) -> List[dict]:
        logger.info(f"Listing {len(self._configs)} configured clusters")
        try:
            clusters = []
            for name, config in self._configs.items():
                clusters.append(
                    {
                        "name": name,
                        "host": config.host,
                        "port": config.port,
                        "user": config.user,
                        "secure": config.secure,
                        "verify": config.verify,
                        "read_only": config.read_only,
                        "active": name == self._active,
                    }
                )
            logger.info(f"Successfully listed clusters: {[c['name'] for c in clusters]}")
            return clusters
        except Exception as e:
            logger.error(f"Failed to list clusters: {e}")
            raise

    def set_active(self, name: str) -> None:
        logger.info(f"Setting active cluster to '{name}'")
        try:
            if name not in self._configs:
                logger.error(f"Cannot set active cluster - '{name}' is not registered")
                raise KeyError(f"Cluster '{name}' is not registered")
            self._active = name
            logger.info(f"Successfully set active cluster to '{name}'")
        except Exception as e:
            logger.error(f"Failed to set active cluster: {e}")
            raise

    def get_cluster(self, name: Optional[str] = None) -> Cluster:
        target = name or self._active
        logger.info(f"Getting cluster '{target}' (requested: '{name}', active: '{self._active}')")
        try:
            if not target:
                logger.error("No cluster configured and no specific cluster requested")
                raise RuntimeError("No cluster configured; add one via /clusters first")
            if target not in self._instances:
                logger.error(
                    f"Cluster '{target}' is not registered. Available: {list(self._instances.keys())}"
                )
                raise KeyError(f"Cluster '{target}' is not registered")
            logger.info(f"Successfully retrieved cluster '{target}'")
            return self._instances[target]
        except Exception as e:
            logger.error(f"Failed to get cluster '{target}': {e}")
            raise

    def delete_cluster(self, name: str) -> None:
        """
        Remove a cluster from the store.
        If it's the active cluster, set active to None.
        """
        logger.info(f"Deleting cluster '{name}'")
        try:
            if name not in self._configs:
                logger.error(f"Cannot delete cluster - '{name}' is not registered")
                raise KeyError(f"Cluster '{name}' is not registered")

            # Clean up the cluster instance and config
            if name in self._instances:
                # Close the client connection if it exists
                cluster = self._instances[name]
                if hasattr(cluster, "_client") and cluster._client:
                    cluster._client.close()
                del self._instances[name]

            del self._configs[name]

            # If this was the active cluster, clear active
            if self._active == name:
                self._active = None
                logger.info(f"Cleared active cluster since '{name}' was active")

            logger.info(f"Successfully deleted cluster '{name}'")
        except Exception as e:
            logger.error(f"Failed to delete cluster '{name}': {e}")
            raise

    def update_cluster(
        self,
        name: str,
        settings: ClusterSettings,
        *,
        make_active: bool = False,
    ) -> None:
        """
        Update an existing cluster's settings.
        """
        logger.info(f"Updating cluster '{name}' with settings: {settings}")
        try:
            if name not in self._configs:
                logger.error(f"Cannot update cluster - '{name}' is not registered")
                raise KeyError(f"Cluster '{name}' is not registered")

            # Close existing client connection if it exists
            if name in self._instances:
                cluster = self._instances[name]
                if hasattr(cluster, "_client") and cluster._client:
                    cluster._client.close()
                    cluster._client = None

            # Update config and create new instance
            self._configs[name] = settings
            self._instances[name] = Cluster(
                name=name,
                host=settings.host,
                port=settings.port,
                user=settings.user,
                password=settings.password,
                read_only=settings.read_only,
                secure=settings.secure,
                verify=settings.verify,
            )

            if make_active:
                self._active = name
                logger.info(f"Set updated cluster '{name}' as active")

            logger.info(f"Successfully updated cluster '{name}'")
        except Exception as e:
            logger.error(f"Failed to update cluster '{name}': {e}")
            raise
