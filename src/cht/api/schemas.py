from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class TableSummary(BaseModel):
    name: str
    comment: str | None = None

    model_config = ConfigDict(extra="forbid")


class ColumnInfo(BaseModel):
    name: str
    type: str
    comment: str | None = None

    model_config = ConfigDict(extra="forbid")


class CommentUpdate(BaseModel):
    comment: str

    model_config = ConfigDict(extra="forbid")


class ClusterConfig(BaseModel):
    name: str
    host: str
    port: int = 8123
    user: str = "default"
    password: str = ""
    secure: bool = False
    verify: bool = False
    read_only: bool = False
    make_active: bool = False

    model_config = ConfigDict(extra="forbid")


class ClusterInfo(BaseModel):
    name: str
    host: str
    port: int
    user: str
    secure: bool = False
    verify: bool = False
    read_only: bool = False
    active: bool = False


class ExportRequest(BaseModel):
    databases: list[str]
    cluster: str | None = None

    model_config = ConfigDict(extra="forbid")

    model_config = ConfigDict(extra="forbid")
