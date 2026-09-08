from __future__ import annotations

import asyncio
import functools
import logging
import shutil
import tempfile
import threading
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Annotated, TypeVar

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
from starlette.concurrency import run_in_threadpool
from starlette.responses import PlainTextResponse

from . import __version__
from .config import Settings
from .engines import engine_versions
from .errors import WorkerError
from .models import (
    CacheStatusResponse,
    ErrorResponse,
    PurgeRequest,
    QueryRequest,
    QueryResponse,
    SchemaResponse,
    SourceFormat,
    SourceResolveRequest,
    SourceResolveResponse,
    SourceSummary,
)
from .operations import Credentials, volume_lock
from .operations import request_id as safe_request_id
from .policy import POLICY_VERSION
from .service import QueryService
from .upload import UploadAdmission

logger = logging.getLogger("omero_data_query_worker")
T = TypeVar("T")


def create_app(settings: Settings | None = None) -> FastAPI:
    selected = settings or Settings.from_env()
    service = QueryService(selected)
    credentials = Credentials(selected)

    def cleanup_loop() -> None:
        while not service.stopping.wait(selected.cleanup_interval_seconds):
            try:
                service.cache.cleanup_sources()
                service.cache.cleanup_results()
                service.cache.cleanup_temporary()
            except OSError:
                service.metrics.add("cleanup_failures_total")

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        with volume_lock(selected.cache_dir):
            service.prepare()
            credentials.tokens()
            cleaner = threading.Thread(target=cleanup_loop, daemon=True)
            cleaner.start()
            try:
                yield
            finally:
                service.stopping.set()
                cleaner.join(5)

    application = FastAPI(
        title="OMERO.DataQueryWorker",
        version=__version__,
        description="Private bounded-query service for immutable tabular sources",
        lifespan=lifespan,
    )
    application.state.query_service = service

    @application.middleware("http")
    async def request_context(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = safe_request_id(request.headers.get("X-Request-ID"))
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        return response

    @application.exception_handler(WorkerError)
    async def worker_error(request: Request, exc: WorkerError) -> JSONResponse:
        request_id = getattr(request.state, "request_id", safe_request_id(None))
        service.metrics.add(f"errors_{exc.code}_total")
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
    def ready() -> JSONResponse:
        try:
            with tempfile.TemporaryFile(dir=selected.tmp_dir) as probe:
                probe.write(b"ready")
                probe.flush()
            if (
                service.stopping.is_set()
                or not service.cache.has_capacity()
                or shutil.disk_usage(selected.cache_dir).free < 1024 * 1024
            ):
                raise OSError
        except OSError:
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return JSONResponse({"status": "ready"})

    def authenticate(authorization: Annotated[str | None, Header()] = None) -> None:
        credentials.authenticate(authorization)

    async def watch_disconnect(request: Request, cancelled: threading.Event) -> None:
        while not cancelled.is_set():
            if await request.is_disconnected():
                cancelled.set()
                return
            await asyncio.sleep(0.1)

    async def operation(request: Request, function: Callable[..., T]) -> T:
        cancelled = threading.Event()
        watcher = asyncio.create_task(watch_disconnect(request, cancelled))
        try:
            return await run_in_threadpool(functools.partial(function, cancelled=cancelled))
        finally:
            cancelled.set()
            watcher.cancel()
            with suppress(asyncio.CancelledError):
                await watcher

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
            "features": {"result_provenance_v1": True},
            "engine_versions": engine_versions(),
            "limits": {
                "query_timeout_seconds": selected.query_timeout_seconds,
                "max_result_rows": selected.max_result_rows,
                "max_result_bytes": selected.max_result_bytes,
                "max_source_bytes": selected.max_source_bytes,
                "max_concurrent_queries": selected.max_concurrent_queries,
                "max_concurrent_ingestions": selected.max_concurrent_ingestions,
                "ingestion_timeout_seconds": selected.ingestion_timeout_seconds,
            },
        }

    @router.post("/sources/resolve", response_model=SourceResolveResponse)
    def resolve_source(payload: SourceResolveRequest) -> SourceResolveResponse:
        return service.resolve_source(payload)

    @router.post("/sources", response_model=SourceSummary, status_code=201)
    async def ingest_source(
        request: Request,
        scope_id: Annotated[str, Form()],
        source_ref: Annotated[str, Form()],
        format: Annotated[SourceFormat, Form()],  # noqa: A002 - public wire name
        size: Annotated[int, Form(ge=0)],
        file: Annotated[UploadFile, File()],
        expected_sha256: Annotated[str | None, Form()] = None,
    ) -> SourceSummary:
        return await operation(
            request,
            functools.partial(
                service.ingest_source,
                scope_id=scope_id,
                source_ref=source_ref,
                source_format=format,
                declared_size=size,
                expected_sha256=expected_sha256,
                upload=file,
            ),
        )

    @router.get("/sources/{source_id}/schema", response_model=SchemaResponse)
    def source_schema(source_id: str) -> SchemaResponse:
        return service.schema(source_id)

    @router.post("/sources/{source_id}/query", response_model=QueryResponse)
    async def source_query(
        request: Request, source_id: str, payload: QueryRequest
    ) -> QueryResponse:
        return await operation(request, functools.partial(service.query, source_id, payload))

    @router.get("/results/{result_id}/download")
    def result_download(result_id: str) -> FileResponse:
        record = service.cache.acquire_result(result_id)
        try:
            return LeasedFileResponse(
                service.cache.result_path(result_id) / record.result_file,
                media_type="text/csv; charset=utf-8",
                filename=f"{record.result_id}.csv",
                release=lambda: service.cache.release(result_id),
            )
        except BaseException:
            service.cache.release(result_id)
            raise

    @router.get("/cache/status", response_model=CacheStatusResponse)
    def cache_status() -> dict[str, int]:
        return service.cache.status()

    @router.get("/metrics", response_class=PlainTextResponse)
    def metrics() -> str:
        return service.metrics.render(service.cache.status())

    @router.post("/cache/purge")
    def purge(payload: PurgeRequest) -> dict[str, object]:
        return service.cache.purge(**payload.model_dump())

    application.include_router(router)
    application.add_middleware(UploadAdmission, service=service, credentials=credentials)
    return application


class LeasedFileResponse(FileResponse):
    def __init__(
        self, path: Path, *, release: Callable[[], None], media_type: str, filename: str
    ) -> None:
        super().__init__(path, media_type=media_type, filename=filename)
        self.release = release

    async def __call__(self, scope, receive, send):  # type: ignore[no-untyped-def]
        try:
            await super().__call__(scope, receive, send)
        finally:
            self.release()


app = create_app()
