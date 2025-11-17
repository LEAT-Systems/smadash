"""Example: SQL query generation and execution with PostgreSQL."""
import os
from dotenv import load_dotenv

from app.agents.query_engine import QueryEngineFactory
from app.agents.utils.database_connection_schema import DatabaseType

# Load environment variables
load_dotenv()


def main():
    """Demonstrate SQL query generation and execution."""
    
    print("=" * 60)
    print("SQL Query Engine Example - PostgreSQL")
    print("=" * 60)
    
    # 1. Create query engine for PostgreSQL
    print("\n1. Creating query engine...")
    generator, executor = QueryEngineFactory.create_query_engine(
        DatabaseType.POSTGRESQL
    )
    print(f"✓ Generator: {generator.__class__.__name__}")
    print(f"✓ Executor: {executor.__class__.__name__}")
    
    # 2. Define database schema
    print("\n2. Setting up schema context...")
    schema_context = {
        "tables": [
            {
                "name": "customers",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "varchar"},
                    {"name": "email", "type": "varchar"},
                    {"name": "revenue", "type": "decimal"},
                    {"name": "created_at", "type": "timestamp"}
                ]
            },
            {
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "customer_id", "type": "int"},
                    {"name": "total", "type": "decimal"},
                    {"name": "order_date", "type": "date"}
                ]
            }
        ]
    }
    print("✓ Schema loaded: customers, orders tables")
    
    # 3. Generate queries from natural language
    queries = [
        "Show top 10 customers by revenue",
        "Count total orders",
        "Calculate average order value by month",
        "Find customers who made orders in the last 30 days"
    ]
    
    print("\n3. Generating SQL queries...")
    for i, nl_query in enumerate(queries, 1):
        print(f"\n   Query {i}: {nl_query}")
        print("   " + "-" * 50)
        
        result = generator.generate_query(
            natural_language_query=nl_query,
            schema_context=schema_context
        )
        
        print(f"   SQL: {result.query}")
        print(f"   Type: {result.query_type}")
        print(f"   Tables: {', '.join(result.tables_or_collections)}")
        print(f"   Confidence: {result.confidence_score:.2f}")
        print(f"   Explanation: {result.explanation}")
        
        if result.warnings:
            print(f"   ⚠ Warnings: {', '.join(result.warnings)}")
    
    # 4. Execute a query (if connection available)
    print("\n4. Query Execution Demo")
    print("   " + "-" * 50)
    
    # Check if database connection is configured
    if os.getenv('DB_HOST'):
        connection_config = {
            'db_type': 'postgresql',
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'mydb'),
            'username': os.getenv('DB_USER', 'user'),
            'password': os.getenv('DB_PASSWORD', 'password')
        }
        
        # Test connection
        print("   Testing connection...")
        if executor.test_connection(connection_config):
            print("   ✓ Connection successful")
            
            # Generate and execute a simple query
            result = generator.generate_query(
                "SELECT 1 as test_column",
                schema_context
            )
            
            print(f"\n   Executing: {result.query}")
            execution = executor.execute_query(
                query=result.query,
                connection_config=connection_config,
                use_cache=True,
                cache_ttl_seconds=60
            )
            
            print(f"   Status: {execution.status.value}")
            print(f"   Rows returned: {execution.rows_returned}")
            print(f"   Execution time: {execution.execution_time_ms:.2f}ms")
            print(f"   From cache: {execution.from_cache}")
            
            if execution.data:
                print(f"\n   Results:")
                for row in execution.data:
                    print(f"   {row}")
        else:
            print("   ✗ Connection failed")
    else:
        print("   ℹ No database connection configured")
        print("   Set DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD to test execution")
    
    # 5. Query validation
    print("\n5. Query Validation Demo")
    print("   " + "-" * 50)
    
    test_queries = [
        ("SELECT * FROM customers LIMIT 10", "Valid query"),
        ("DROP TABLE customers", "Dangerous operation"),
        ("SELECT * FROM nonexistent_table", "Invalid table")
    ]
    
    for sql, description in test_queries:
        print(f"\n   Testing: {description}")
        print(f"   SQL: {sql}")
        
        validation = generator.validate_query(sql, schema_context)
        
        if validation['valid']:
            print(f"   ✓ Valid")
        else:
            print(f"   ✗ Invalid")
            if validation['errors']:
                print(f"   Errors: {', '.join(validation['errors'])}")
        
        if validation.get('warnings'):
            print(f"   ⚠ Warnings: {', '.join(validation['warnings'])}")
    
    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
