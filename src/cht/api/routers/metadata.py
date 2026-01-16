import io
from datetime import datetime

from fastapi import APIRouter, Depends, Query, Response, status

from ..dependencies import get_metadata_service
from ..schemas import ColumnInfo, CommentUpdate, ExportRequest, TableSummary
from ..services import MetadataService

router = APIRouter(prefix="/databases", tags=["metadata"])


@router.get("", response_model=list[str])
def list_databases(
    cluster: str | None = Query(default=None, description="Cluster name to query"),
    service: MetadataService = Depends(get_metadata_service),
) -> list[str]:
    """List available ClickHouse databases."""
    return service.list_databases(cluster=cluster)


@router.get("/{database}/tables", response_model=list[TableSummary])
def list_tables(
    database: str,
    cluster: str | None = Query(default=None, description="Cluster name to query"),
    service: MetadataService = Depends(get_metadata_service),
) -> list[TableSummary]:
    """List tables in a database along with table comments."""
    return service.list_tables(database, cluster=cluster)


@router.get("/{database}/tables/{table}/columns", response_model=list[ColumnInfo])
def list_columns(
    database: str,
    table: str,
    cluster: str | None = Query(default=None, description="Cluster name to query"),
    service: MetadataService = Depends(get_metadata_service),
) -> list[ColumnInfo]:
    """List table columns including type and comment."""
    return service.list_columns(database, table, cluster=cluster)


@router.patch(
    "/{database}/tables/{table}/comment",
    status_code=status.HTTP_204_NO_CONTENT,
)
def update_table_comment(
    database: str,
    table: str,
    payload: CommentUpdate,
    cluster: str | None = Query(default=None, description="Cluster name to query"),
    service: MetadataService = Depends(get_metadata_service),
) -> Response:
    """Update the table-level comment."""
    service.update_table_comment(database, table, payload.comment, cluster=cluster)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{database}/tables/{table}/columns/{column}/comment",
    status_code=status.HTTP_204_NO_CONTENT,
)
def update_column_comment(
    database: str,
    table: str,
    column: str,
    payload: CommentUpdate,
    cluster: str | None = Query(default=None, description="Cluster name to query"),
    service: MetadataService = Depends(get_metadata_service),
) -> Response:
    """Update the comment for a specific column."""
    service.update_column_comment(database, table, column, payload.comment, cluster=cluster)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/export/excel")
def export_table_descriptions_to_excel(
    payload: ExportRequest,
    service: MetadataService = Depends(get_metadata_service),
) -> Response:
    """Export table descriptions for selected databases to Excel format.

    Creates one worksheet per table with columns: Column Name, Column Type, Comment.
    Each worksheet is named with the full table name (database.table).
    """
    excel_data = service.export_table_descriptions_to_excel(
        payload.databases, cluster=payload.cluster
    )

    # Generate filename with timestamp for uniqueness
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"table_descriptions_{timestamp}.xlsx"

    # Ensure filename is ASCII-safe for better cross-platform compatibility
    safe_filename = filename.encode("ascii", "ignore").decode("ascii")

    return Response(
        content=excel_data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_filename}"',
            "Content-Length": str(len(excel_data)),
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "Accept-Ranges": "bytes",  # Helps with download resumption
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
