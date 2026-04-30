from step_tracer.models import ExecutionContext

from query_engine import QueryEngine


def test_select_single_field_returns_values(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="name", op="==", value="fibonacci")
        .select("return_value")
        .execute()
    )

    assert results == [1, 1]


def test_select_multiple_fields_returns_dicts(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="stmt_type", op="==", value="function")
        .limit(1)
        .select("name", "return_value")
        .execute()
    )

    assert results == [{"name": "fibonacci", "return_value": 1}]
