from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from sqlglot import exp, parse
from sqlglot.errors import ParseError, TokenError

from .errors import InvalidQuery
from .models import SourceFormat

POLICY_VERSION = "query-policy-v1"

BLOCKED_NODE_TYPES = (
    exp.Alter,
    exp.Attach,
    exp.Command,
    exp.Copy,
    exp.Create,
    exp.Delete,
    exp.Drop,
    exp.Insert,
    exp.LoadData,
    exp.Merge,
    exp.Pragma,
    exp.Set,
    exp.Transaction,
    exp.Update,
    exp.Use,
)

BLOCKED_FUNCTIONS = {
    "GLOB",
    "GETENV",
    "HTTP_GET",
    "HTTP_POST",
    "LOAD_EXTENSION",
    "PARQUET_SCAN",
    "READ_BLOB",
    "READ_CSV",
    "READ_CSV_AUTO",
    "READ_JSON",
    "READ_JSON_AUTO",
    "READ_NDJSON",
    "READ_PARQUET",
    "READ_TEXT",
    "SQLITE_SCAN",
}

VOLATILE_FUNCTIONS = {
    "CHANGES",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRVAL",
    "GEN_RANDOM_UUID",
    "LAST_INSERT_ROWID",
    "NEXTVAL",
    "NOW",
    "RANDOM",
    "RAND",
    "TOTAL_CHANGES",
    "UUID",
    "UUIDV4",
    "UUIDV7",
}

VOLATILE_PATTERN = re.compile(
    r"\b(?:CURRENT_DATE|CURRENT_TIME|CURRENT_TIMESTAMP)\b|"
    r"\b(?:NOW|RANDOM|RAND|UUID|UUIDV4|UUIDV7|NEXTVAL|CURRVAL)\s*\(",
    re.IGNORECASE,
)
PARAMETER_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
PARAMETER_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class ValidatedQuery:
    original_sql: str
    canonical_sql: str
    sql_sha256: str
    deterministic: bool
    parameter_names: frozenset[str]


def _dialect(source_format: SourceFormat) -> str:
    return "sqlite" if source_format is SourceFormat.sqlite else "duckdb"


def _function_name(node: exp.Expression) -> str | None:
    if isinstance(node, exp.Anonymous):
        return node.name.upper()
    if isinstance(node, exp.Func):
        return str(node.sql_name()).upper()  # type: ignore[no-untyped-call]
    return None


def validate_query(
    sql: str,
    source_format: SourceFormat,
    supplied_parameters: set[str],
) -> ValidatedQuery:
    if "\x00" in sql:
        raise InvalidQuery("SQL must not contain NUL bytes")
    try:
        statements = parse(sql, read=_dialect(source_format))
    except (ParseError, TokenError) as exc:
        raise InvalidQuery(f"SQL could not be parsed: {exc}") from exc
    if len(statements) != 1 or statements[0] is None:
        raise InvalidQuery("Exactly one SQL statement is required")
    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.SetOperation)):
        raise InvalidQuery("Only SELECT or WITH ... SELECT queries are allowed")
    for node in tree.walk():
        if isinstance(node, BLOCKED_NODE_TYPES):
            raise InvalidQuery(f"SQL operation {node.key.upper()} is not allowed")
        function = _function_name(node)
        if function and (
            function in BLOCKED_FUNCTIONS
            or function.startswith("READ_")
            or function.endswith("_SCAN")
        ):
            raise InvalidQuery(f"SQL function {function} is not allowed")
        if isinstance(node, exp.Table):
            table_name = node.name
            if (
                "/" in table_name
                or "\\" in table_name
                or table_name.lower().endswith(
                    (".csv", ".duckdb", ".sqlite", ".sqlite3", ".parquet", ".json")
                )
            ):
                raise InvalidQuery("Filesystem table references are not allowed")

    canonical = tree.sql(dialect=_dialect(source_format), pretty=False, comments=False)
    parameter_names = frozenset(PARAMETER_PATTERN.findall(canonical))
    invalid_names = supplied_parameters - {
        name for name in supplied_parameters if PARAMETER_NAME_PATTERN.fullmatch(name)
    }
    if invalid_names:
        raise InvalidQuery("Parameter names must be SQL identifiers")
    missing = parameter_names - supplied_parameters
    extra = supplied_parameters - parameter_names
    if missing:
        raise InvalidQuery(f"Missing parameters: {', '.join(sorted(missing))}")
    if extra:
        raise InvalidQuery(f"Unused parameters: {', '.join(sorted(extra))}")

    volatile = VOLATILE_PATTERN.search(canonical) is not None
    if not volatile:
        volatile = any((_function_name(node) or "") in VOLATILE_FUNCTIONS for node in tree.walk())
    return ValidatedQuery(
        original_sql=sql,
        canonical_sql=canonical,
        sql_sha256=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        deterministic=not volatile,
        parameter_names=parameter_names,
    )
