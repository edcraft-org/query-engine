"""Query Engine - Chainable query builder for execution trace data."""

from importlib.metadata import version

__version__ = version("query-engine")

from query_engine.core import Query, QueryEngine
from query_engine.exceptions import (
    InvalidFieldError,
    InvalidOperatorError,
    QueryEngineError,
)

__all__ = [
    "__version__",
    "QueryEngine",
    "Query",
    "QueryEngineError",
    "InvalidOperatorError",
    "InvalidFieldError",
]
