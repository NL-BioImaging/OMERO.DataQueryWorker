from __future__ import annotations

import pytest

from omero_data_query_worker.errors import InvalidQuery
from omero_data_query_worker.models import SourceFormat
from omero_data_query_worker.policy import validate_query


def test_accepts_select_with_named_parameters() -> None:
    result = validate_query(
        "WITH chosen AS (SELECT * FROM data WHERE area >= $minimum) SELECT * FROM chosen",
        SourceFormat.csv,
        {"minimum"},
    )
    assert result.deterministic
    assert result.parameter_names == {"minimum"}


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM data",
        "UPDATE data SET area = 1",
        "ATTACH 'secret.duckdb' AS secret",
        "COPY data TO 'result.csv'",
        "PRAGMA version",
        "INSTALL httpfs",
        "LOAD httpfs",
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM '/etc/passwd.csv'",
        "SELECT load_extension('/tmp/evil')",
        "SELECT getenv('DQW_API_TOKEN')",
        "SELECT * FROM sqlite_scan('x.sqlite', 'data')",
        "SELECT 1; SELECT 2",
        "SELECT 'unterminated",
    ],
)
def test_rejects_unsafe_sql(sql: str) -> None:
    with pytest.raises(InvalidQuery):
        validate_query(sql, SourceFormat.duckdb, set())


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT random()",
        "SELECT current_timestamp",
        "SELECT now()",
        "SELECT uuid()",
    ],
)
def test_marks_volatile_queries_non_deterministic(sql: str) -> None:
    assert not validate_query(sql, SourceFormat.duckdb, set()).deterministic


def test_rejects_missing_and_unused_parameters() -> None:
    with pytest.raises(InvalidQuery, match="Missing parameters"):
        validate_query("SELECT $value", SourceFormat.duckdb, set())
    with pytest.raises(InvalidQuery, match="Unused parameters"):
        validate_query("SELECT 1", SourceFormat.duckdb, {"value"})
