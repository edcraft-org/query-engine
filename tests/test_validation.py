import pytest
from step_tracer.models import ExecutionContext

from query_engine import InvalidFieldError, InvalidOperatorError, QueryEngine
from query_engine.exceptions import QueryEngineError


def test_select_requires_at_least_one_field(
    execution_context: ExecutionContext,
) -> None:
    query = QueryEngine(execution_context).create_query()

    with pytest.raises(QueryEngineError, match="At least one field"):
        query.select()


def test_group_by_requires_field_or_aggregation(
    execution_context: ExecutionContext,
) -> None:
    query = QueryEngine(execution_context).create_query()

    with pytest.raises(QueryEngineError, match="At least one field"):
        query.group_by()

    with pytest.raises(QueryEngineError, match="At least one aggregation"):
        query.group_by("stmt_type").execute()


def test_limit_and_offset_validate_bounds(
    execution_context: ExecutionContext,
) -> None:
    query = QueryEngine(execution_context).create_query()

    with pytest.raises(QueryEngineError, match="Limit must be positive"):
        query.limit(0)

    with pytest.raises(QueryEngineError, match="Offset must be non-negative"):
        query.offset(-1)


def test_invalid_operator_raises_error(
    execution_context: ExecutionContext,
) -> None:
    query = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="execution_id", op="contains", value=1)
    )

    with pytest.raises(InvalidOperatorError, match="Unsupported operator: contains"):
        query.execute()


def test_invalid_field_raises_error(
    execution_context: ExecutionContext,
) -> None:
    query = QueryEngine(execution_context).create_query().select("missing")

    with pytest.raises(InvalidFieldError, match="Invalid field: missing"):
        query.execute()
