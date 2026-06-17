# Query Engine Guide
This document explains the code in greater detail.


# Core Classes
This section explains the classes in `core.py`.


## Query Engine
This is the entrypoint to work with the Query Engine. It takes in an `ExecutionContext` (produced from running the `StepTracer`).

The entire `ExecutionContext` is given to the Query Engine. Currently, only the `execution_trace` and `variables` of the `ExecutionContext` are used.

The Query Engine has a single method `create_query` to create a new query instance.

The `QueryEngine` class is like a wrapper around the `Query` class and enables us to create new queries from the same `ExecutionContext`.


## Query

| Attributes | Description |
| ---------- | ----------- |
| execution_context | The output from the `StepTracer` containing the information collected. |
| pipeline | The operations to be performed on the information from the `execution_context`. More specifically, the `execution_trace` and `variables`. |

The query is only evaluated when the `execute` method is called. Other methods are used to add the corresponding `PipelineStep` to the `pipeline`. This allows the query to only be evaluated when needed.


### Methods

`execute`: 

Extracts the `execution_trace` and `variables` from the `execution_context`.

Applies the pipeline by calling `apply` for each step in the `pipeline`.

---

The other methods are used to add the corresponding `PipelineStep` to the `pipeline`. Each `PipelineStep` is explained in greater detail in the [Pipeline Steps](#pipeline-steps) section. 

The methods modifies the `pipeline` attribute and returns the object itself. It does not create a new `Query` object.

In this section, we will cover the usage of the methods.

---

`where`: 

There are a few ways to apply the `where` condition. 

```python
# Single condition
query.where(field="stmt_type", op="==", value="function")
query.where(("stmt_type", "==", "function"))
query.where(stmt_type="function")

# OR logic
query.where(
    ("line_number", "<", 10),
    ("line_number", ">", 50)
)

# AND logic
(
    query
    .where(field="stmt_type", op="==", value="function")
    .where(field="name", op="==", value="fibonacci")
)
query.where(stmt_type="function", name="fibonacci")
```

The `where` method accepts different variations of arguments to handle both `AND` and `OR` conditions. 

If more than one tuple of `(field, operator, value)` are given, the tuples will be applied as `OR` logic. Conditions supplied as `field=...` and as keyword arugments are applied as `AND` logic.

**Example:**

```python
query.where(
    ("line_number", "<", 10),
    ("line_number", ">", 50),
    field="name",
    op="==",
    value="fibonacci",
    stmt_type="function",
)
```

The above query is logically equivalent to

```
(line_number < 10 OR line_number > 50)
AND name = 'fibonacci'
AND stmt_type = 'function'
```

---

`map`:

Applies a function to each item in the result.

```python
# Before: [VariableSnapshot(line_number=2, ...)]

query.map(lambda item: item.line_number - 1)

# After: [1]
```

---

`reduce`:

Flattens the list by one layer.

```python
code = """
lst = []
lst.append(1)
""" 

query = (
    query.group_by("stmt_type")
    .agg(line_numbers=lambda items: [item.line_number for item in items])
    .select("line_numbers")
)

# Before: [[3], [2, 3]]

query.reduce()

# After: [3, 2, 3]
```

Works on items with a mix of lists and elements.

```python
query = (
    query.group_by("stmt_type")
    .agg(line_numbers=lambda items: [item.line_number for item in items] if len(items) > 1 else items[0].line_number)
    .select("line_numbers")
)

# Before: [3, [2, 3]]

query.reduce()

# After: [3, 2, 3]
```

---

`select`:

Accepts a variable number of fields to select from the result.
* Selecting a single field returns a list of values
* Selecting multiple fields return a list of dictionaries

```python
# Select single field (returns list of values)
query.select("line_number")  # Returns [1, 2, 3, ...]

# Select multiple fields (returns list of dicts)
query.select("line_number", "stmt_type")  # Returns [{'line_number': 2, 'stmt_type': 'variable'}, ...]

query.select()  # Error: no field chosen
```

---

`distinct`:

Remove duplicates from result.

```python
code = """
x = 1
x += 1
"""

query.select("name")

# Before: ['x', 'x']

query.distinct()

# After: ['x']
```

---

`order_by`:

Order by the given field in ascending or descending order.

```python
code = """
x = 1
z = 3
y = 2
"""

query.order_by("value")
query.select("name")

# Without ordering: ['x', 'z', 'y']
# Ascending ordering: ['x', 'y', 'z']

query.order_by("value", is_ascending=False)

# Descending ordering: ['z', 'y', 'x']
```

Only one field can be passed into this function. To order by multiple fields, call the function multiple times. The most dominant ordering will be the according to the latest function call. This may be counterintuitive.

```python
code = """
x, z = 1, 3
y = 2
"""

query.order_by("value", is_ascending=False)
query.order_by("line_number")
query.select("name")

# Before ordering: ['x', 'z', 'y']
# After ordering: ['z', 'x', 'y']
# Ordered by line number
# For items with the same line number, order by their value in descending order
```

---

`group_by`:

Accepts a variable number of fields to group the results by. 

Keyword arugments can be used to renamed the field used to group the results by so that they can be references using the new name, such as used by the aggregation function.

```python
code = """
for i in range(2):
    for j in range(3):
        pass
"""

# Group by single field
query.where(
    ("stmt_type", "==", "loop_iteration"),
).group_by("loop_execution_id").agg(
    count=lambda items: len(items),
)
# Result: [
#     {'loop_execution_id': 1,  'count': 2},
#     {'loop_execution_id': 4,  'count': 3}, 
#     {'loop_execution_id': 10, 'count': 3}
# ]

# Group by multiple fields with aliases
query.group_by(
    parent_loop="loop_execution_id",
    iter_num="iteration_num"
).agg(
    total=lambda items: len(items)
)
# Result: [
#     {'parent_loop': 1, 'iter_num': 0, 'total': 1}, 
#     {'parent_loop': 4, 'iter_num': 0, 'total': 1},
#     ...]
```

---

`agg`: 

Applies the aggregation function on the grouped results. If no `group_by` was applied, the aggregration is applied on the entire result.

```python
query.agg(
    total=lambda items: len(items)
) # Applied on the entire result
```

---

`offset`: 

Returns the items, exlcuding the first `n` items, where `n = offset`.

---

`limit`: 

Returns the first `n` items, where `n = offset`.

---

`inner_join`:

Joins the current list of items with the given list of items.
The join operation returns a list of `JoinResult`. A `JoinResult` is an object that combines an item from the current list of items `left_item`, with an item from other_items `right_item`. `inner_join` only returns `JoinResult` that satifies the given `conditions`.

Arguments:
* `other_items`: Items to join with the current list of items
* `conditions`: Condition to join on. This is a function that takes `left_item` and `right_item`. `left_item` is an item from the current list of items. `right_item` is an item from `other_items`.
* `left_alias`: The name to access items from the current list of items in the `JoinResult`.
* `right_alias`: The name to access items from `other_items` in the `JoinResult`. 
These arguments are the same for other types of joins.

```python
code = """
x = 0
for i in range(1, 3):
    x += i

for i in range(2):
    x += 1
"""

query.where(stmt_type="loop").inner_join(
    other_items=exec_context.variables,
    conditions=lambda left, right: right.execution_id > left.execution_id
    and right.execution_id <= left.end_execution_id and right.name == "x",
    left_alias="left",
    right_alias="right",
)

query.select("right.value")  # access fields using the alias
```

---

`left_join`: 

Performs a left join.

For every item in the current list of items:
* if it matches something in `other_items`, return the matched pairs
* if it matches nothing, return the left item paired with `None`. However, this will lose information about the attributes of `other_items`.

---

`right_join`:

Performs a right join.

For every item in `other_items`:
* if it matches something in the current list of items, return the matched pairs
* if it matches nothing, return `None` paired with the right item

---

`full_outer_join`:

Performs a full outer join.

It returns:
* all matching pairs
* unmatched left items with `None` on the right
* unmatched right items with `None` on the left


## Pipeline Steps
This section explains the classes in `pipeline_steps.py`.


### QueryCondition

The condition used in `WHERE` operations. There is a set of operations available for where operations. This can be extended upon by adding new operation mappings of the string representing the operation to how the operation is performed.

`evaluate`:

Evaluates the condition against a given object.

The actual value of the field is obtained from the object using `get_field_value`. The field value is compared against the supplied value using the operator.


### PipelineStepBase

The base class for pipeline steps. All pipeline steps must implement the `apply` method which applies the step to a list of items.


### WhereStep

Filters the item list based on the given `conditions`. The item will be included in the result if at least one condition passes.


### SelectStep

Selects the `fields` from each item.
* One field selected: Returns a list of values. E.g. `[1, 2, 3]`
* Multiple fields selected: Returns a list of dictionaries. E.g.
    ```
    [
        {
            "line_number": 1,
            "value": 1,
        },
        {
            "line_number": 2,
            "value": 2,
        }
    ]
    ```

If the field does not exist, throws an `InvalidFieldError`.


### MapStep

Applies the `func` to every item.


### ReduceStep

Flattens the list by one layer. Works on items with a mix of lists and elements.


### DistinctStep

For hashable types, keep track of seen items using a set and discard seen items.
For non-hashable types, iterate through a list of seen items to check for uniqueness.
Order of items is preserved.


### OrderByStep

Sorts the `items` by the `field`. The value of the field is obtained using `get_field_value`.


### GroupByStep

Attributes:
* `group_fields`: a dictionary containing the `alias` to `field` mapping.
* `aggregations`: a dictionary containing the `field/alias` to aggregation function mapping

At least one aggregation must be made after grouping the items.

The items are first grouped by the `group_fields`. Then, for each group, use the aggregation function on the items.


### OffsetStep

Returns the items, exlcuding the first `n` items, where `n = offset`.


### LimitStep

Returns the first `n` items, where `n = offset`.


### JoinResult

Represents the result of join operations. A join can produce a result that contains values from multiple sources so `JoinResult` has `alias_to_items`, which is a dictionary mapping `alias` to `item`. All aliases need to be unique.

```
JoinResult(
    alias_to_items={
        'left': LoopExecution(execution_id=1, ...), 
        'right': VariableSnapshot(var_id=3, ...)
    }
)

JoinResult(
        'alias1': LoopExecution(execution_id=1, ...), 
        'alias2': LoopIteration(execution_id=2, ...), 
        'alias3': VariableSnapshot(var_id=3, ...)
)

JoinResult(
    alias_to_items={
        'left': LoopExecution(execution_id=1, ...), 
        'right': None
    }
)
```

**Code Improvements**: Since the class only has `alias_to_items`, consider removing this attribute and instead each `alias` is an attribute of the class.


### JoinStep

Base class for different types of joins. This class contains shared join logic so the specific join types only need to implement their matching behavior. Joins `items` with `other_items`. 

Attributes:
* `other_items`: list of items to join with
* `conditions`: A function taking in two items. The condition to join on.
* `left_alias`: The alias used for items on the left of the join.
* `right_alias`: The alias used for items on the right of the join.

Method:

`_create_joined_result`: create a `JoinResult` using `left_item` and `right_item`.
* If left_item is already a `JoinResult`, it reuses its aliases. Otherwise, it stores left_item under left_alias.
* Stores the right_item under right_alias.


### InnerJoinStep

Returns pairs where `conditions(left_item, right_item)` is `True`.


### LeftJoinStep

For every item in the current list of items:
* if it matches something in `other_items`, return the matched pairs
* if it matches nothing, return the left item paired with `None`. However, this will lose information about the attributes of `other_items`.


### RightJoinStep

For every item in `other_items`:
* if it matches something in the current list of items, return the matched pairs
* if it matches nothing, return `None` paired with the right item


### FullOuterJoinStep

It returns:
* all matching pairs
* unmatched left items with `None` on the right
* unmatched right items with `None` on the left


### PipelineStep

PipelineStep is a type alias for any one of the step classes that can live in the query pipeline.

All pipeline steps implement the `apply` method. We can exploring using of `PipelineStepBase` instead of having an additional type alias.


## Utils
This section explains the methods in `utils.py`.

`get_field_value`:

Retrieves a value from an object using dot-separated field path.

```python
get_field_value(join_result, "left.execution_id")
```

This allows the code to dynamically resolve fields based on a string.

This function splits the path into fields. E.g. `["left", "execution_id"]`.

It starts from the root object and walk through each field to resolve one level at a time. It handles different cases of objects:

* `JoinResult`: Tries to get the value of the field. If the field is `None` or the field is not found, no error is thrown and `None` is returned to handle outer joins. For outer joins, even if there are no matches, it returns `None`, similar to SQL behaviour. It is possible for the field to be not found because the unmatched pairs will be paired with `None`, which does not preserve information about the original attribute names.

* `dict`: Accesses the field if it exists

* normal python object: checks if the object contains the field and returns the value

If the field is not found, an `InvalidFieldError` is thrown (except for `JoinResult`).

**Code Improvement**: Instead of having this class, in each object (e.g. `JoinResult`, `StatementExecution`), implement a `get` method to handle field lookup.


## Run Query

Here's a code snippet to try out the Query Engine:

```python
from step_tracer import StepTracer

from query_engine.core import QueryEngine

code = """
lst = []
lst.append(1)
"""  # modify this with your own code

tracer = StepTracer()

transformed_code = tracer.transform_code(code)
exec_context = tracer.execute_transformed_code(transformed_code)

query_engine = QueryEngine(exec_context)

query = query_engine.create_query()

query = query.where(name="lst")  # modify this with your own query

query_results = query.execute()
print(query_results)
```
