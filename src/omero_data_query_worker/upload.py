"""Admission and spool accounting before Starlette parses multipart uploads."""

from __future__ import annotations

import threading

from starlette.concurrency import run_in_threadpool
from starlette.datastructures import Headers
from starlette.formparsers import MultiPartException
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .errors import QueryCapacityExceeded, SourceTooLarge, WorkerError
from .operations import Credentials, request_id
from .service import QueryService


class UploadAdmission:
    def __init__(self, app: ASGIApp, service: QueryService, credentials: Credentials):
        self.app, self.service, self.credentials = app, service, credentials
        self.capacity = threading.BoundedSemaphore(service.settings.max_concurrent_ingestions)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] != "/v1/sources" or scope["method"] != "POST":
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        correlation = request_id(headers.get("X-Request-ID"))

        async def reject(exc: WorkerError) -> None:
            self.service.metrics.add(f"errors_{exc.code}_total")
            response = JSONResponse(
                {"code": exc.code, "message": str(exc), "request_id": correlation},
                status_code=exc.status_code,
                headers={"X-Request-ID": correlation},
            )
            await response(scope, receive, send)

        try:
            self.credentials.authenticate(headers.get("Authorization"))
            if not self.capacity.acquire(blocking=False):
                self.service.metrics.add("admission_rejections_total")
                raise QueryCapacityExceeded("All upload slots are busy; retry later")
        except WorkerError as exc:
            await reject(exc)
            return
        staging = None
        failure: WorkerError | None = None
        sent_failure = False
        received = 0
        try:
            staging = await run_in_threadpool(self.service.cache.staging_path, "source")

            async def bounded_receive() -> Message:
                nonlocal received, failure
                message = await receive()
                if message["type"] == "http.request":
                    received += len(message.get("body", b""))
                    try:
                        # Multipart fields/boundaries have a separate bounded allowance.
                        if received > self.service.settings.max_source_bytes + 1024 * 1024:
                            raise SourceTooLarge("Upload exceeds the source size limit")
                        await run_in_threadpool(self.service.cache.reserve, staging, received)
                    except WorkerError as exc:
                        failure = exc
                        # Starlette closes every partially parsed upload on this exception.
                        raise MultiPartException("Upload capacity exceeded") from exc
                return message

            async def bounded_send(message: Message) -> None:
                nonlocal sent_failure
                if failure:
                    if not sent_failure:
                        sent_failure = True
                        await reject(failure)
                else:
                    await send(message)

            await self.app(scope, bounded_receive, bounded_send)
        finally:
            if staging is not None:
                await run_in_threadpool(self.service.cache.discard_staging, staging)
            self.capacity.release()
