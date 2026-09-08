from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

OPAQUE_PATTERN = r"^[A-Za-z0-9._~-]{1,160}$"
SHA256_PATTERN = r"^[a-fA-F0-9]{64}$"


class SourceFormat(StrEnum):
    duckdb = "duckdb"
    sqlite = "sqlite"
    csv = "csv"


class SourceResolveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scope_id: str = Field(pattern=OPAQUE_PATTERN)
    source_ref: str = Field(pattern=OPAQUE_PATTERN)
    format: SourceFormat
    size: int = Field(ge=0)
    expected_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)


class SourceSummary(BaseModel):
    source_id: str
    format: SourceFormat
    filename: str
    size: int
    sha256: str
    schema_digest: str
    cache_status: Literal["created", "reused"]
    expires_at: str


class SourceResolveResponse(BaseModel):
    cached: bool
    source: SourceSummary | None = None


class ColumnSchema(BaseModel):
    name: str
    type: str
    nullable: bool


class TableSchema(BaseModel):
    name: str
    kind: Literal["table", "view"]
    columns: list[ColumnSchema]


class SchemaResponse(BaseModel):
    source_id: str
    format: SourceFormat
    schema_digest: str
    tables: list[TableSchema]


ParameterType = Literal[
    "null",
    "boolean",
    "integer",
    "float",
    "decimal",
    "string",
    "date",
    "time",
    "timestamp",
]


class TypedParameter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: ParameterType
    value: Any = None


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sql: str = Field(min_length=1, max_length=100_000)
    parameters: dict[str, TypedParameter] = Field(default_factory=dict)


class ResultColumn(BaseModel):
    name: str
    type: str


class QueryResponse(BaseModel):
    result_id: str
    columns: list[ResultColumn]
    row_count: int
    byte_count: int
    preview: list[list[Any]]
    source_sha256: str
    sql_sha256: str
    duration_ms: int
    cache_status: Literal["hit", "miss", "bypass"]
    execution: dict[str, Any] | None = None


class PurgeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_id: str | None = None
    scope_id: str | None = Field(default=None, pattern=OPAQUE_PATTERN)
    result_id: str | None = None
    dry_run: bool = True


class ErrorResponse(BaseModel):
    code: str
    message: str
    request_id: str


class CacheStatusResponse(BaseModel):
    source_count: int
    source_bytes: int
    result_count: int
    result_bytes: int
    hits: int
    misses: int
    bypasses: int
    evictions: int
    active_queries: int
