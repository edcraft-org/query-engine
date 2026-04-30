from collections.abc import Mapping

import pytest
from step_tracer.models import ExecutionContext


def _record_function_call(
    context: ExecutionContext,
    *,
    line_number: int,
    name: str,
    arguments: Mapping[str, object],
    return_value: object,
) -> None:
    function_call = context.create_function_call(
        line_number=line_number,
        func_name=name,
        func_full_name=name,
    )
    for arg_name, arg_value in arguments.items():
        function_call.add_arg(arg_name, arg_value)
    function_call.set_return_value(return_value)

    context.push_execution(function_call)
    context.pop_execution()


@pytest.fixture
def execution_context() -> ExecutionContext:
    """Return a small execution context built with real step-tracer models."""
    context = ExecutionContext()

    _record_function_call(
        context,
        line_number=10,
        name="fibonacci",
        arguments={"n": 1},
        return_value=1,
    )
    _record_function_call(
        context,
        line_number=12,
        name="fibonacci",
        arguments={"n": 2},
        return_value=1,
    )
    _record_function_call(
        context,
        line_number=20,
        name="double",
        arguments={"value": 4},
        return_value=8,
    )
    context.record_global_variable(
        name="total",
        value=2,
        access_path="total",
        line_number=14,
    )

    return context
