from __future__ import annotations

import logging
import secrets
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    File,
    Form,
    Header,
    Request,
    UploadFile,
)
from fastapi.responses import FileResponse, JSONResponse

from . import __version__
from .config import Settings
from .errors import AuthenticationError, WorkerError
from .models import (
    CacheStatusResponse,
    ErrorResponse,
    QueryRequest,
    QueryResponse,
    SchemaResponse,
    SourceFormat,
    SourceResolveRequest,
    SourceResolveResponse,
    SourceSummary,
)
from .policy import POLICY_VERSION
from .service import QueryService

logger = logging.getLogger("omero_data_query_worker")


def create_app(settings: Settings | None = None) -> FastAPI:
    selected = settings or Settings.from_env()
    service = QueryService(selected)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        service.prepare()
        yield

    application = FastAPI(
        title="OMERO.DataQueryWorker",
        version=__version__,
        description="Private bounded-query service for immutable tabular sources",
        lifespan=lifespan,
    )
    application.state.query_service = service

    @application.middleware("http")
    async def request_context(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        return response

    @application.exception_handler(WorkerError)
    async def worker_error(request: Request, exc: WorkerError) -> JSONResponse:
        request_id = getattr(request.state, "request_id", uuid.uuid4().hex)
        logger.warning(
            "request_failed request_id=%s code=%s path=%s",
            request_id,
            exc.code,
            request.url.path,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=ErrorResponse(
                code=exc.code,
                message=str(exc),
                request_id=request_id,
            ).model_dump(),
        )

    @application.get("/health/live", include_in_schema=False)
    def live() -> dict[str, str]:
        return {"status": "live"}

    @application.get("/health/ready", include_in_schema=False)
    def ready() -> dict[str, str]:
        return {"status": "ready"}

    def authenticate(authorization: Annotated[str | None, Header()] = None) -> None:
        scheme, _, value = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not secrets.compare_digest(value, selected.api_token):
            raise AuthenticationError("A valid worker bearer token is required")

    router = APIRouter(prefix="/v1", dependencies=[Depends(authenticate)])

    @router.get("/capabilities")
    def capabilities() -> dict[str, object]:
        return {
            "protocol": "omero-data-query-worker-v1",
            "worker_version": __version__,
            "policy_version": POLICY_VERSION,
            "formats": ["duckdb", "sqlite", "csv"],
            "csv_table": "data",
            "parameter_style": "$name",
            "limits": {
                "query_timeout_seconds": selected.query_timeout_seconds,
                "max_result_rows": selected.max_result_rows,
                "max_result_bytes": selected.max_result_bytes,
                "max_source_bytes": selected.max_source_bytes,
                "max_concurrent_queries": selected.max_concurrent_queries,
            },
        }

    @router.post("/sources/resolve", response_model=SourceResolveResponse)
    def resolve_source(payload: SourceResolveRequest) -> SourceResolveResponse:
        return service.resolve_source(payload)

    @router.post("/sources", response_model=SourceSummary, status_code=201)
    async def ingest_source(
        scope_id: Annotated[str, Form()],
        source_ref: Annotated[str, Form()],
        format: Annotated[SourceFormat, Form()],  # noqa: A002 - public wire name
        size: Annotated[int, Form(ge=0)],
        file: Annotated[UploadFile, File()],
        expected_sha256: Annotated[str | None, Form()] = None,
    ) -> SourceSummary:
        return await service.ingest_source(
            scope_id=scope_id,
            source_ref=source_ref,
            source_format=format,
            declared_size=size,
            expected_sha256=expected_sha256,
            upload=file,
        )

    @router.get("/sources/{source_id}/schema", response_model=SchemaResponse)
    def source_schema(source_id: str) -> SchemaResponse:
        return service.schema(source_id)

    @router.post("/sources/{source_id}/query", response_model=QueryResponse)
    def source_query(source_id: str, payload: QueryRequest) -> QueryResponse:
        return service.query(source_id, payload)

    @router.get("/results/{result_id}/download")
    def result_download(result_id: str) -> FileResponse:
        path, record = service.result_file(result_id)
        return FileResponse(
            path,
            media_type="text/csv; charset=utf-8",
            filename=f"{record.result_id}.csv",
        )

    @router.get("/cache/status", response_model=CacheStatusResponse)
    def cache_status() -> dict[str, int]:
        return service.cache.status()

    application.include_router(router)
    return application


app = create_app()
