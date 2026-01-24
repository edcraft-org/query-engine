# Query Engine

A chainable query builder for execution trace data. Query Engine provides a SQL-like interface for filtering, transforming, and analyzing execution traces from [step-tracer](https://github.com/edcraft-org/step-tracer).

## Features

- **Chainable API**: Build complex queries with method chaining
- **Filtering**: Filter data with `where()` conditions supporting multiple operators
- **Transformations**: Transform data with `map()`, `reduce()`, `select()`, and `distinct()`
- **Aggregations**: Group and aggregate data with `group_by()` and `agg()`
- **Joins**: Perform inner, left, right, and full outer joins
- **Sorting & Pagination**: Sort with `order_by()`, paginate with `limit()` and `offset()`

## Installation

## Installation

### From GitHub

Using uv:

```bash
uv add git+https://github.com/edcraft-org/query-engine.git
```

Using pip:

```bash
pip install git+https://github.com/edcraft-org/query-engine.git
```

For a specific branch, tag, or commit:

```bash
uv add git+https://github.com/edcraft-org/query-engine.git@branch-name
```

### From Local Source

Using uv:

```bash
uv add /path/to/query-engine
```

Or in editable mode for development:

```bash
uv pip install -e /path/to/query-engine
```

Using pip:

```bash
pip install /path/to/query-engine
```

Or in editable mode for development:

```bash
pip install -e /path/to/query-engine
```


## Quick Start

```python
from query_engine import QueryEngine

# Create a query engine with your execution context 
engine = QueryEngine(
    exec_ctx # from executing step tracer
)

# Build and execute a query
results = (
    engine.create_query()
    .where(field="stmt_type", op="==", value="function")
    .where(field="name", op="==", value="fibonacci")
    .order_by(field="execution_id", is_ascending=True)
    .select("arguments", "return_value")
    .limit(5)
    .execute()
)
```

## Usage Examples

### Filtering Data

```python
# Single condition
query.where(field="stmt_type", op="==", value="function")

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
```

**Supported operators**: `==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `not_in`

### Selecting Fields

```python
# Select single field (returns list of values)
query.select("line_number")

# Select multiple fields (returns list of dicts)
query.select("line_number", "code", "status")
```

### Transforming Data

```python
# Map: Apply a function to each item
query.map(lambda item: item.line_number - 1)

# Reduce: Flatten nested lists
query.reduce()

# Distinct: Remove duplicates
query.distinct()
```

### Grouping and Aggregation

```python
# Group by single field
query.group_by("loop_execution_id").agg(
    count=lambda items: len(items),
)

# Group by multiple fields with aliases
query.group_by(
    parent_loop="loop_execution_id",
    iter_num="iteration_num"
).agg(
    total=lambda items: len(items)
)
```

### Sorting and Pagination

```python
# Sort ascending
query.order_by("execution_id")

# Sort descending
query.order_by("execution_id", is_ascending=False)

# Pagination
query.offset(20).limit(10)  # Get items 20-30
```

### Joins

```python
# Inner join
query.inner_join(
    other_items=other_data,
    conditions=lambda left, right: left.loop_execution_id == right.execution_id,
    left_alias="loop_iteration",
    right_alias="parent_loop_execution"
)

# Access joined data
for result in query.execute():
    loop_iteration_data = result.get("loop_iteration")
    parent_loop_execution_data = result.get("parent_loop_execution")
```

**Available joins**: `inner_join()`, `left_join()`, `right_join()`, `full_outer_join()`


## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/edcraft-org/query-engine.git
cd query-engine

# Install dependencies
make install
```

### Running Tests

```bash
make test
```

### Code Quality

```bash
# Run linter
make lint

# Run type checker
make type-check

# Run all checks
make all-checks
```

### Project Structure

```
query-engine/
├── src/query_engine/
│   ├── __init__.py           # Public API exports
│   ├── core.py               # Query and QueryEngine classes
│   ├── pipeline_steps.py     # Pipeline step implementations
│   ├── exceptions.py         # Custom exceptions
│   └── utils.py              # Utility functions
├── tests/                    # Test suite
├── pyproject.toml            # Project configuration
├── Makefile                  # Development commands
└── README.md                 # This file
```

## API Reference

### QueryEngine

- `create_query()` - Create a new query instance

### Query Methods

**Filtering:**
- `where(*conditions, field=None, op="==", value=None, **kwargs)` - Filter items

**Selection:**
- `select(*fields)` - Select specific fields

**Transformation:**
- `map(func)` - Apply function to each item
- `reduce()` - Flatten nested lists
- `distinct()` - Remove duplicates

**Aggregation:**
- `group_by(*fields, **kwargs)` - Group items by fields
- `agg(**aggregations)` - Apply aggregation functions

**Sorting:**
- `order_by(field, is_ascending=True)` - Sort items

**Pagination:**
- `offset(offset)` - Skip items
- `limit(limit)` - Limit number of items

**Joins:**
- `inner_join(other_items, conditions, left_alias, right_alias)` - Inner join
- `left_join(other_items, conditions, left_alias, right_alias)` - Left join
- `right_join(other_items, conditions, left_alias, right_alias)` - Right join
- `full_outer_join(other_items, conditions, left_alias, right_alias)` - Full outer join

**Execution:**
- `execute()` - Execute the query and return results

## Requirements

- Python 3.12 or higher
- step-tracer 0.1.0 or higher

## License

MIT License - see [LICENSE](LICENSE) file for details.
