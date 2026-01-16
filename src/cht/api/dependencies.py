from fastapi import Request

from .cluster_store import ClusterStore
from .services import MetadataService


def get_metadata_service(request: Request) -> MetadataService:
    """Retrieve the configured MetadataService from FastAPI app state."""
    service = getattr(request.app.state, "metadata_service", None)
    if service is None:
        raise RuntimeError("Metadata service is not configured")
    return service


def get_cluster_store(request: Request) -> ClusterStore:
    """Retrieve the cluster store from FastAPI app state."""
    store = getattr(request.app.state, "cluster_store", None)
    if store is None:
        raise RuntimeError("Cluster store is not configured")
    return store
