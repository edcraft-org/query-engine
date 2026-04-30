from step_tracer.models import ExecutionContext

from query_engine import QueryEngine


def test_group_by_field_applies_aggregation(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .group_by("stmt_type")
        .agg(count=len)
        .execute()
    )

    assert results == [
        {"stmt_type": "function", "count": 3},
        {"stmt_type": "variable", "count": 1},
    ]


def test_group_by_alias_renames_group_field(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="stmt_type", op="==", value="function")
        .group_by(function_name="name")
        .agg(total=len)
        .execute()
    )

    assert results == [
        {"function_name": "fibonacci", "total": 2},
        {"function_name": "double", "total": 1},
    ]
