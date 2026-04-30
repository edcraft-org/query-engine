from step_tracer.models import ExecutionContext

from query_engine import QueryEngine


def test_execute_returns_trace_and_variables(
    execution_context: ExecutionContext,
) -> None:
    results = QueryEngine(execution_context).create_query().execute()

    assert [item.stmt_type for item in results] == [
        "function",
        "function",
        "function",
        "variable",
    ]


def test_where_filters_with_keyword_condition(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(name="fibonacci")
        .select("execution_id")
        .execute()
    )

    assert results == [1, 2]


def test_where_supports_or_conditions(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(("name", "==", "fibonacci"), ("name", "==", "total"))
        .select("name")
        .execute()
    )

    assert results == ["fibonacci", "fibonacci", "total"]


def test_where_supports_comparison_and_membership(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="execution_id", op="in", value={1, 3})
        .where(field="line_number", op=">=", value=10)
        .select("name")
        .execute()
    )

    assert results == ["fibonacci", "double"]
