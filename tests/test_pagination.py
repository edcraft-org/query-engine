from step_tracer.models import ExecutionContext

from query_engine import QueryEngine


def test_order_offset_and_limit_page_results(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="stmt_type", op="==", value="function")
        .order_by("execution_id", is_ascending=False)
        .offset(1)
        .limit(1)
        .select("name")
        .execute()
    )

    assert results == ["fibonacci"]
