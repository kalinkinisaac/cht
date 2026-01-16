from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import JSONResponse

from ..cluster_store import ClusterSettings, ClusterStore
from ..dependencies import get_cluster_store
from ..schemas import ClusterConfig, ClusterInfo

router = APIRouter(prefix="/clusters", tags=["clusters"])


@router.get("", response_model=list[ClusterInfo])
def list_clusters(store: ClusterStore = Depends(get_cluster_store)) -> list[ClusterInfo]:
    """Return all configured clusters with active flag."""
    return store.list_clusters()


@router.post("", response_model=ClusterInfo, status_code=status.HTTP_201_CREATED)
def add_cluster(
    config: ClusterConfig, store: ClusterStore = Depends(get_cluster_store)
) -> ClusterInfo:
    """Add a new ClickHouse connection and optionally make it active."""
    settings = ClusterSettings(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        secure=config.secure,
        verify=config.verify,
        read_only=config.read_only,
    )
    store.add_cluster(config.name, settings, make_active=config.make_active)
    for info in store.list_clusters():
        if info["name"] == config.name:
            return info  # type: ignore[return-value]
    raise RuntimeError("Cluster was not registered correctly")


@router.post("/{name}/select", status_code=status.HTTP_204_NO_CONTENT)
def select_active_cluster(name: str, store: ClusterStore = Depends(get_cluster_store)) -> Response:
    """Mark a configured cluster as the active one used by metadata endpoints by default."""
    store.set_active(name)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{name}/test", response_model=dict)
def test_cluster_connection(name: str, store: ClusterStore = Depends(get_cluster_store)):
    """
    Ping a cluster and return a structured result without raising.
    """
    try:
        cluster = store.get_cluster(name)
        cluster.client.ping()
        return {"status": "ok"}
    except Exception as exc:  # pragma: no cover - connection errors are environment-specific
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"status": "error", "error": str(exc)},
        )


@router.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
def delete_cluster(name: str, store: ClusterStore = Depends(get_cluster_store)) -> Response:
    """Delete a cluster configuration."""
    store.delete_cluster(name)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put("/{name}", response_model=ClusterInfo)
def update_cluster(
    name: str, config: ClusterConfig, store: ClusterStore = Depends(get_cluster_store)
) -> ClusterInfo:
    """Update an existing cluster's configuration."""
    settings = ClusterSettings(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        secure=config.secure,
        verify=config.verify,
        read_only=config.read_only,
    )
    store.update_cluster(name, settings, make_active=config.make_active)
    for info in store.list_clusters():
        if info["name"] == name:
            return info  # type: ignore[return-value]
    raise RuntimeError("Cluster was not updated correctly")
