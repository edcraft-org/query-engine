from step_tracer.models import ExecutionContext

from query_engine import QueryEngine


def test_map_transforms_each_result(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="stmt_type", op="==", value="function")
        .map(lambda item: item.name.upper())
        .execute()
    )

    assert results == ["FIBONACCI", "FIBONACCI", "DOUBLE"]


def test_reduce_flattens_one_level(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="stmt_type", op="==", value="function")
        .select("arguments")
        .map(dict.values)
        .map(list)
        .reduce()
        .execute()
    )

    assert results == [1, 2, 4]


def test_distinct_removes_duplicate_values(
    execution_context: ExecutionContext,
) -> None:
    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="stmt_type", op="==", value="function")
        .select("return_value")
        .distinct()
        .execute()
    )

    assert results == [1, 8]
