"""Example: MongoDB query generation and execution."""
import os
from dotenv import load_dotenv

from app.agents.query_engine import QueryEngineFactory
from app.agents.utils.database_connection_schema import DatabaseType

# Load environment variables
load_dotenv()


def main():
    """Demonstrate MongoDB query generation and execution."""
    
    print("=" * 60)
    print("MongoDB Query Engine Example")
    print("=" * 60)
    
    # 1. Create query engine for MongoDB
    print("\n1. Creating query engine...")
    generator, executor = QueryEngineFactory.create_query_engine(
        DatabaseType.MONGODB
    )
    print(f"✓ Generator: {generator.__class__.__name__}")
    print(f"✓ Executor: {executor.__class__.__name__}")
    
    # 2. Define MongoDB schema
    print("\n2. Setting up schema context...")
    schema_context = {
        "collections": [
            {
                "name": "orders",
                "fields": [
                    {"name": "_id", "type": "ObjectId"},
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "total", "type": "decimal"},
                    {"name": "status", "type": "string"},
                    {"name": "created_at", "type": "date"}
                ]
            },
            {
                "name": "products",
                "fields": [
                    {"name": "_id", "type": "ObjectId"},
                    {"name": "name", "type": "string"},
                    {"name": "category", "type": "string"},
                    {"name": "price", "type": "decimal"},
                    {"name": "stock", "type": "int"}
                ]
            }
        ]
    }
    print("✓ Schema loaded: orders, products collections")
    
    # 3. Generate MongoDB aggregation pipelines
    queries = [
        "Count orders by status",
        "Calculate total revenue by customer",
        "Find top 10 products by sales",
        "Get average order value by month"
    ]
    
    print("\n3. Generating MongoDB aggregation pipelines...")
    for i, nl_query in enumerate(queries, 1):
        print(f"\n   Query {i}: {nl_query}")
        print("   " + "-" * 50)
        
        result = generator.generate_query(
            natural_language_query=nl_query,
            schema_context=schema_context
        )
        
        print(f"   Pipeline: {result.query}")
        print(f"   Type: {result.query_type}")
        print(f"   Collections: {', '.join(result.tables_or_collections)}")
        print(f"   Confidence: {result.confidence_score:.2f}")
        print(f"   Explanation: {result.explanation}")
        
        if result.warnings:
            print(f"   ⚠ Warnings: {', '.join(result.warnings)}")
    
    # 4. Execute a query (if MongoDB connection available)
    print("\n4. Query Execution Demo")
    print("   " + "-" * 50)
    
    if os.getenv('MONGO_HOST'):
        connection_config = {
            'db_type': 'mongodb',
            'host': os.getenv('MONGO_HOST', 'localhost'),
            'port': int(os.getenv('MONGO_PORT', 27017)),
            'database': os.getenv('MONGO_DB', 'mydb'),
            'collection': 'orders',
            'username': os.getenv('MONGO_USER'),
            'password': os.getenv('MONGO_PASSWORD')
        }
        
        # Test connection
        print("   Testing connection...")
        if executor.test_connection(connection_config):
            print("   ✓ Connection successful")
            
            # Generate and execute a simple query
            result = generator.generate_query(
                "Count all orders",
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
            print(f"   Documents returned: {execution.rows_returned}")
            print(f"   Execution time: {execution.execution_time_ms:.2f}ms")
            print(f"   From cache: {execution.from_cache}")
            
            if execution.data:
                print(f"\n   Results:")
                for doc in execution.data:
                    print(f"   {doc}")
        else:
            print("   ✗ Connection failed")
    else:
        print("   ℹ No MongoDB connection configured")
        print("   Set MONGO_HOST, MONGO_PORT, MONGO_DB to test execution")
    
    # 5. Pipeline validation
    print("\n5. Pipeline Validation Demo")
    print("   " + "-" * 50)
    
    test_pipelines = [
        ('[{"$match": {"status": "completed"}}, {"$count": "total"}]', "Valid pipeline"),
        ('{"invalid": "json"}', "Invalid JSON"),
        ('[{"$unknown_stage": {}}]', "Unknown stage")
    ]
    
    for pipeline, description in test_pipelines:
        print(f"\n   Testing: {description}")
        print(f"   Pipeline: {pipeline}")
        
        validation = generator.validate_query(pipeline, schema_context)
        
        if validation['valid']:
            print(f"   ✓ Valid")
        else:
            print(f"   ✗ Invalid")
            if validation['errors']:
                print(f"   Errors: {', '.join(validation['errors'])}")
        
        if validation.get('warnings'):
            print(f"   ⚠ Warnings: {', '.join(validation['warnings'])}")
    
    # 6. Query explanation
    print("\n6. Query Explanation Demo")
    print("   " + "-" * 50)
    
    pipeline = '[{"$match": {"status": "completed"}}, {"$group": {"_id": "$customer_id", "total": {"$sum": "$total"}}}, {"$sort": {"total": -1}}, {"$limit": 10}]'
    
    print(f"   Pipeline: {pipeline}")
    explanation = generator.explain_query(pipeline)
    print(f"\n   Explanation: {explanation}")
    
    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
