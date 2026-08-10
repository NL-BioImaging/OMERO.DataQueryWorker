from __future__ import annotations


class WorkerError(Exception):
    status_code = 400
    code = "worker_error"


class AuthenticationError(WorkerError):
    status_code = 401
    code = "authentication_required"


class SourceNotFound(WorkerError):
    status_code = 404
    code = "source_not_found"


class ResultNotFound(WorkerError):
    status_code = 404
    code = "result_not_found"


class InvalidSource(WorkerError):
    status_code = 422
    code = "invalid_source"


class IngestionTimedOut(WorkerError):
    status_code = 504
    code = "ingestion_timeout"


class SourceConflict(WorkerError):
    status_code = 409
    code = "source_conflict"


class SourceTooLarge(WorkerError):
    status_code = 413
    code = "source_too_large"


class InvalidQuery(WorkerError):
    status_code = 422
    code = "invalid_query"


class QueryLimitExceeded(WorkerError):
    status_code = 413
    code = "query_limit_exceeded"


class QueryTimedOut(WorkerError):
    status_code = 504
    code = "query_timeout"


class QueryCapacityExceeded(WorkerError):
    status_code = 429
    code = "query_capacity_exceeded"


class CacheCapacityExceeded(WorkerError):
    status_code = 507
    code = "cache_capacity_exceeded"


class QueryExecutionError(WorkerError):
    status_code = 422
    code = "query_execution_failed"
