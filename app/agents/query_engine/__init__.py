"""
Query Engine Package

Multi-datasource query generation and execution system.

Supports:
- SQL databases (PostgreSQL, MySQL, SQLite, Oracle, SQL Server) via SQLAlchemy
- MongoDB (aggregation pipelines)
- Extensible for other data sources

Usage:
    from app.agents.query_engine import QueryEngineFactory
    from app.agents.utils.database_connection_schema import DatabaseType
    
    # Create generator and executor
    generator, executor = QueryEngineFactory.create_query_engine(
        DatabaseType.POSTGRESQL
    )
    
    # Generate query from natural language
    result = generator.generate_query(
        "Show top 10 customers by revenue",
        schema_context
    )
    
    # Execute the generated query
    execution_result = executor.execute_query(
        result.query,
        connection_config
    )
    
    # Access results
    for row in execution_result.data:
        print(row)
"""
from .query_factory import (
    QueryGeneratorFactory,
    QueryExecutorFactory,
    QueryEngineFactory
)

__all__ = [
    'QueryGeneratorFactory',
    'QueryExecutorFactory',
    'QueryEngineFactory'
]
