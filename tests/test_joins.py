from dataclasses import dataclass

from step_tracer.models import ExecutionContext, FunctionCall

from query_engine import QueryEngine


@dataclass(frozen=True)
class FunctionMetadata:
    function_name: str
    category: str


def test_inner_join_combines_matching_items(
    execution_context: ExecutionContext,
) -> None:
    metadata = [
        FunctionMetadata(function_name="fibonacci", category="recursion"),
        FunctionMetadata(function_name="double", category="math"),
    ]

    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="stmt_type", op="==", value="function")
        .inner_join(
            other_items=metadata,
            conditions=lambda left, right: left.name == right.function_name,
            left_alias="execution",
            right_alias="metadata",
        )
        .select("execution.name", "metadata.category")
        .execute()
    )

    assert results == [
        {"execution.name": "fibonacci", "metadata.category": "recursion"},
        {"execution.name": "fibonacci", "metadata.category": "recursion"},
        {"execution.name": "double", "metadata.category": "math"},
    ]


def test_left_join_keeps_unmatched_left_items(
    execution_context: ExecutionContext,
) -> None:
    metadata = [
        FunctionMetadata(function_name="fibonacci", category="recursion"),
    ]

    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="stmt_type", op="==", value="function")
        .left_join(
            other_items=metadata,
            conditions=lambda left, right: left.name == right.function_name,
            left_alias="execution",
            right_alias="metadata",
        )
        .select("execution.name", "metadata.category")
        .execute()
    )

    assert results == [
        {"execution.name": "fibonacci", "metadata.category": "recursion"},
        {"execution.name": "fibonacci", "metadata.category": "recursion"},
        {"execution.name": "double", "metadata.category": None},
    ]


def test_full_outer_join_keeps_unmatched_items_from_both_sides(
    execution_context: ExecutionContext,
) -> None:
    metadata = [
        FunctionMetadata(function_name="fibonacci", category="recursion"),
        FunctionMetadata(function_name="missing", category="unknown"),
    ]

    results = (
        QueryEngine(execution_context)
        .create_query()
        .where(field="stmt_type", op="==", value="function")
        .full_outer_join(
            other_items=metadata,
            conditions=lambda left, right: (
                isinstance(left, FunctionCall)
                and left.name == right.function_name
            ),
            left_alias="execution",
            right_alias="metadata",
        )
        .select("execution.name", "metadata.category")
        .execute()
    )

    assert results == [
        {"execution.name": "fibonacci", "metadata.category": "recursion"},
        {"execution.name": "fibonacci", "metadata.category": "recursion"},
        {"execution.name": "double", "metadata.category": None},
        {"execution.name": None, "metadata.category": "unknown"},
    ]
