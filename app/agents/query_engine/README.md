# Query Engine

Multi-datasource query generation and execution engine for DataFlow.

## Quick Start

```python
from app.agents.query_engine import QueryEngineFactory
from app.agents.utils.database_connection_schema import DatabaseType

# Create query engine for PostgreSQL
generator, executor = QueryEngineFactory.create_query_engine(
    DatabaseType.POSTGRESQL
)

# Generate query
result = generator.generate_query(
    "Show top 10 customers by revenue",
    schema_context={
        "tables": [
            {
                "name": "customers",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "varchar"},
                    {"name": "revenue", "type": "decimal"}
                ]
            }
        ]
    }
)

print(f"Generated: {result.query}")
print(f"Confidence: {result.confidence_score}")

# Execute query
execution = executor.execute_query(
    result.query,
    connection_config={
        'db_type': 'postgresql',
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'username': 'user',
        'password': 'pass'
    }
)

print(f"Rows: {execution.rows_returned}")
for row in execution.data:
    print(row)
```

## Supported Databases

### SQL (via SQLAlchemy)
- PostgreSQL
- MySQL
- SQLite
- Oracle
- SQL Server

### NoSQL
- MongoDB

## Architecture

```
interfaces/          - Abstract base classes
    query_generator_interface.py
    query_executor_interface.py

generators/          - Query generation implementations
    sql_query_generator.py
    mongodb_query_generator.py

executors/          - Query execution implementations
    sql_query_executor.py
    mongodb_query_executor.py

query_factory.py    - Factory classes for creating generators/executors
```

## Key Classes

### QueryEngineFactory
Main entry point for creating query engines.

```python
generator, executor = QueryEngineFactory.create_query_engine(db_type)
```

### QueryGeneratorInterface
Abstract interface for query generators.

**Methods:**
- `generate_query()` - Convert natural language to query
- `validate_query()` - Validate query syntax
- `explain_query()` - Get human-readable explanation
- `get_supported_query_language()` - Get query language type

### QueryExecutorInterface
Abstract interface for query executors.

**Methods:**
- `execute_query()` - Execute query and return results
- `execute_query_streaming()` - Stream large result sets
- `explain_execution_plan()` - Get execution plan
- `test_connection()` - Test database connectivity
- `close_connection()` - Close active connections

## Configuration

Set environment variables:

```bash
OPENAI_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini
```

## Examples

See `/QUERY_ENGINE_ARCHITECTURE.md` for comprehensive examples.

## Testing

```python
def test_query_generation():
    from app.agents.query_engine import QueryEngineFactory
    from app.agents.utils.database_connection_schema import DatabaseType
    
    generator, _ = QueryEngineFactory.create_query_engine(
        DatabaseType.POSTGRESQL
    )
    
    result = generator.generate_query(
        "Count total users",
        {"tables": [{"name": "users", "columns": []}]}
    )
    
    assert "COUNT" in result.query
    assert result.confidence_score > 0.5
```

## Adding New Data Sources

1. Create generator in `generators/`
2. Create executor in `executors/`
3. Update `query_factory.py`
4. Add database type to `DatabaseType` enum

See `/QUERY_ENGINE_ARCHITECTURE.md` section "Adding New Data Sources" for details.
