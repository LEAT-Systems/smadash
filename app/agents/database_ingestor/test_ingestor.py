from interfaces import ConnectionConfig
from app.agents.utils import DatabaseType
from sql_alchemy import DatabaseIngestorFactory
from sql_alchemy import DatabaseIngestionPipeline

def main():
    # Configure databases using SQLAlchemy
    source_config = ConnectionConfig(
        host="localhost",
        port=5433,
        database="articles_db",
        username="postgres",
        password="_SolidLea_01",
        db_type=DatabaseType.POSTGRESQL,
        additional_params={
            'charset': 'utf8mb4',
            'pool_recycle': 3600
        }
    )

    target_config = ConnectionConfig(
        host="localhost",
        port=5433,
        database="smadash",
        username="postgres",
        password="_SolidLea_01",
        db_type=DatabaseType.POSTGRESQL,
        additional_params={
            'sslmode': 'prefer'
        }
    )

    # Test individual ingestor
    factory = DatabaseIngestorFactory()
    ingestor = factory.create_ingestor(DatabaseType.POSTGRESQL)

    if ingestor.test_connection(source_config):
        print("✅ Connection test successful")

        # Connect and discover schema
        if ingestor.connect(source_config):
            print("🔍 Discovering schema...")

            tables = ingestor.discover_tables()
            print(f"Found {len(tables)} tables:")

            for table in tables[:5]:  # Show first 5 tables
                print(f"  - {table.name}: {table.row_count} rows, {len(table.columns)} columns")

                # Show some sample data
                sample_data = ingestor.extract_data(table.name, batch_size=3)
                if sample_data:
                    print(f"    Sample: {list(sample_data[0].keys())}")

            ingestor.disconnect()
    else:
        print("❌ Connection test failed")
        return

    # Use full pipeline
    pipeline = DatabaseIngestionPipeline()

    # Create execution plan
    print("\n📋 Creating ingestion plan...")
    plan = pipeline.create_ingestion_plan(
        source_config=source_config,
        target_config=target_config,
        # table_filters=['users', 'orders']  # Optional filter
    )

    print(f"Plan created:")
    print(f"  - Tables to process: {plan['total_tables']}")
    print(f"  - Total rows: {plan['total_rows']:,}")
    print(f"  - Estimated time: {plan['total_estimated_time_minutes']:.1f} minutes")
    print(f"  - Normalization rules: {len(plan['normalization_rules'])}")

    # Progress callback
    def progress_callback(status):
        pct = status['current_progress_pct']
        current_table = status.get('current_table', 'N/A')
        print(f"Progress: {pct:.1f}% - Processing: {current_table}")

    # Execute ingestion
    print("\n🚀 Starting ingestion...")
    result, schema = pipeline.execute_ingestion(plan, progress_callback)

    print(f"\n✅ Ingestion completed!")
    print(f"  - Status: {result['status']}")
    print(f"  - Tables processed: {result['tables_processed']}/{result['total_tables']}")
    print(f"  - Rows processed: {result['statistics']['total_rows_processed']:,}")
    print(f"  - Processing rate: {result['statistics']['processing_rate_rows_per_second']:.1f} rows/sec")
    print(schema)
    if result['errors']:
        print(f"  - Errors: {len(result['errors'])}")
        for error in result['errors'][:3]:  # Show first 3 errors
            print(f"    • {error}")

if __name__ == "__main__":
    main()