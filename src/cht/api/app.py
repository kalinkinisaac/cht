import logging
import traceback

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .cluster_store import ClusterStore
from .frontend import router as frontend_router
from .routers.clusters import router as clusters_router
from .routers.metadata import router as metadata_router
from .services import MetadataService

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("cht.api")


def create_app(
    metadata_service: MetadataService, cluster_store: ClusterStore | None = None
) -> FastAPI:
    """
    Build a FastAPI app for ClickHouse metadata operations.

    Args:
        metadata_service: Service instance that encapsulates access to ClickHouse metadata.
            A concrete implementation will wrap Cluster/Table and perform the actual queries.
        cluster_store: Registry of available ClickHouse clusters. A fresh store will be created if
            none is supplied.

    Raises:
        ValueError: When metadata_service is not provided.
    """
    if metadata_service is None:
        raise ValueError("metadata_service is required")

    app = FastAPI(title="CHT Metadata API")

    # Add CORS middleware for frontend
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled exception at {request.method} {request.url}: {exc}")
        logger.error(f"Exception details: {traceback.format_exc()}")

        if isinstance(exc, HTTPException):
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

        return JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error",
                "error": str(exc),
                "type": type(exc).__name__,
            },
        )

    # Request logging middleware
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        logger.info(f"Request: {request.method} {request.url}")
        try:
            response = await call_next(request)
            logger.info(f"Response: {request.method} {request.url} - {response.status_code}")
            return response
        except Exception as e:
            logger.error(f"Request failed: {request.method} {request.url} - {e}")
            raise

    app.state.metadata_service = metadata_service
    app.state.cluster_store = cluster_store or ClusterStore()
    app.include_router(clusters_router)
    app.include_router(metadata_router)
    app.include_router(frontend_router)

    logger.info("FastAPI app created successfully")
    return app
