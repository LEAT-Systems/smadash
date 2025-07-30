import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from app.agents.database_ingestor.interfaces import DatabaseIngestionPipelineInterface, ConnectionConfig
from .ingestor_factory import DatabaseIngestorFactory
from app.agents.utils.database_normalizer import DataNormalizer
from app.agents.utils.database_connection_schema import DatabaseType

class DatabaseIngestionPipeline(DatabaseIngestionPipelineInterface):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.active_executions = {}
        self.normalizer = DataNormalizer()
        self.factory = DatabaseIngestorFactory()

    def create_ingestion_plan(self, source_config: ConnectionConfig,
                              target_config: ConnectionConfig,
                              table_filters: Optional[List[str]] = None) -> Dict[str, Any]:

        plan_id = str(uuid.uuid4())

        # Create ingestor using factory
        source_ingestor = self.factory.create_ingestor(source_config.db_type)

        # Connect and discover schema
        if not source_ingestor.connect(source_config):
            raise RuntimeError("Failed to connect to source database")

        try:
            all_tables = source_ingestor.discover_tables()

            # Filter tables if specified
            if table_filters:
                tables_to_process = [t for t in all_tables if t.name in table_filters]
            else:
                tables_to_process = all_tables

            # Analyze normalization needs
            normalization_rules = self.normalizer.analyze_normalization_needs(tables_to_process)

            plan = {
                'plan_id': plan_id,
                'created_at': datetime.now().isoformat(),
                'source_config': {
                    'db_type': source_config.db_type.value,
                    'host': source_config.host,
                    'port': source_config.port,
                    'database': source_config.database,
                    'username': source_config.username,
                    'password': source_config.password
                    # Note: password not included in plan for security
                },
                'target_config': {
                    'db_type': target_config.db_type.value,
                    'host': target_config.host,
                    'port': target_config.port,
                    'database': target_config.database,
                    'username': target_config.username,
                    'password': source_config.password
                },
                'tables_to_process': [
                    {
                        'name': table.name,
                        'schema': table.schema,
                        'row_count': table.row_count,
                        'column_count': len(table.columns),
                        'has_primary_key': len(table.primary_keys) > 0,
                        'has_foreign_keys': len(table.foreign_keys) > 0,
                        'estimated_time_minutes': self._estimate_processing_time(table.row_count)
                    }
                    for table in tables_to_process
                ],
                'normalization_rules': [
                    {
                        'table_name': rule.table_name,
                        'column_name': rule.column_name,
                        'rule_type': rule.rule_type,
                        'parameters': rule.parameters
                    }
                    for rule in normalization_rules
                ],
                'total_estimated_time_minutes': sum(
                    self._estimate_processing_time(table.row_count)
                    for table in tables_to_process
                ),
                'total_tables': len(tables_to_process),
                'total_rows': sum(table.row_count for table in tables_to_process)
            }

            return plan

        finally:
            source_ingestor.disconnect()

    def execute_ingestion(self, plan: Dict[str, Any],
                          progress_callback: Optional[callable] = None) -> tuple[Dict[str, Any], Dict[str, Any]]:

        execution_id = str(uuid.uuid4())
        start_time = datetime.now()

        execution_status = {
            'execution_id': execution_id,
            'plan_id': plan['plan_id'],
            'status': 'running',
            'start_time': start_time.isoformat(),
            'tables_processed': 0,
            'total_tables': len(plan['tables_to_process']),
            'current_table': None,
            'current_progress_pct': 0.0,
            'errors': [],
            'warnings': [],
            'statistics': {
                'total_rows_processed': 0,
                'total_rows_inserted': 0,
                'total_rows_failed': 0,
                'processing_rate_rows_per_second': 0,
                'tables_completed': [],
                'tables_failed': []
            }
        }

        self.active_executions[execution_id] = execution_status

        try:
            # Reconstruct connection configs (you'd need to store passwords securely)
            source_config = self._reconstruct_connection_config(plan['source_config'])
            target_config = self._reconstruct_connection_config(plan['target_config'])

            # Create ingestors
            source_ingestor = self.factory.create_ingestor(source_config.db_type)
            target_ingestor = self.factory.create_ingestor(target_config.db_type)

            # Connect to databases
            if not source_ingestor.connect(source_config):
                raise RuntimeError("Failed to connect to source database")

            if not target_ingestor.connect(target_config):
                raise RuntimeError("Failed to connect to target database")
            schema_summary = self._extract_schema_for_llm(source_ingestor, source_config)

            try:
                # Process each table
                for i, table_info in enumerate(plan['tables_to_process']):
                    execution_status['current_table'] = table_info['name']
                    execution_status['current_progress_pct'] = (i / len(plan['tables_to_process'])) * 100

                    if progress_callback:
                        progress_callback(execution_status)

                    try:
                        # Process table data
                        table_stats = self._process_table(
                            source_ingestor=source_ingestor,
                            target_ingestor=target_ingestor,
                            table_info=table_info,
                            normalization_rules=plan['normalization_rules'],
                            execution_status=execution_status
                        )

                        # Update statistics
                        execution_status['tables_processed'] += 1
                        execution_status['statistics']['total_rows_processed'] += table_stats['rows_processed']
                        execution_status['statistics']['total_rows_inserted'] += table_stats['rows_inserted']
                        execution_status['statistics']['total_rows_failed'] += table_stats['rows_failed']
                        execution_status['statistics']['tables_completed'].append({
                            'table_name': table_info['name'],
                            'rows_processed': table_stats['rows_processed'],
                            'processing_time_seconds': table_stats['processing_time']
                        })

                    except Exception as table_error:
                        error_msg = f"Failed to process table {table_info['name']}: {str(table_error)}"
                        execution_status['errors'].append(error_msg)
                        execution_status['statistics']['tables_failed'].append({
                            'table_name': table_info['name'],
                            'error': str(table_error)
                        })
                        self.logger.error(error_msg)

                        # Continue with next table instead of failing entire ingestion
                        continue

                    if progress_callback:
                        progress_callback(execution_status)

                # Mark as completed
                execution_status['status'] = 'completed' if not execution_status['errors'] else 'completed_with_errors'
                execution_status['end_time'] = datetime.now().isoformat()
                execution_status['current_progress_pct'] = 100.0

                # Calculate final statistics
                total_time = (datetime.now() - start_time).total_seconds()
                execution_status['statistics']['total_processing_time_seconds'] = total_time
                execution_status['statistics']['processing_rate_rows_per_second'] = (
                    execution_status['statistics']['total_rows_processed'] / total_time
                    if total_time > 0 else 0
                )

            finally:
                source_ingestor.disconnect()
                target_ingestor.disconnect()

        except Exception as e:
            execution_status['status'] = 'failed'
            execution_status['end_time'] = datetime.now().isoformat()
            execution_status['errors'].append(str(e))
            self.logger.error(f"Ingestion failed: {e}")

        return execution_status, schema_summary

    def _process_table(self, source_ingestor, target_ingestor, table_info: Dict[str, Any],
                       normalization_rules: List[Dict[str, Any]], execution_status: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single table."""

        table_start_time = datetime.now()
        table_name = table_info['name']

        # Get table metadata
        table_metadata = source_ingestor.get_table_metadata(table_name)
        # Validate data integrity
        validation_results = source_ingestor.validate_data_integrity(table_name)
        if validation_results['errors']:
            execution_status['warnings'].extend([
                f"Table {table_name}: {error}" for error in validation_results['errors']
            ])

        # Extract and process data in batches
        batch_size = 1000
        rows_processed = 0
        rows_inserted = 0
        rows_failed = 0

        # Get normalization rules for this table
        table_rules = [
            rule for rule in normalization_rules
            if rule['table_name'] == table_name
        ]

        try:
            # Process data in streaming fashion
            for batch_data in self._get_data_batches(source_ingestor, table_name, batch_size):
                if not batch_data:
                    break

                # Apply normalization if rules exist
                if table_rules:
                    normalized_data = self.normalizer.apply_business_rules(batch_data, table_rules)
                else:
                    normalized_data = batch_data

                # Insert into target database (mock implementation)
                batch_inserted = self._insert_batch_to_target(
                    target_ingestor, table_name, normalized_data, table_metadata
                )

                rows_processed += len(batch_data)
                rows_inserted += batch_inserted
                rows_failed += len(batch_data) - batch_inserted

        except Exception as e:
            self.logger.error(f"Error processing table {table_name}: {e}")
            raise

        processing_time = (datetime.now() - table_start_time).total_seconds()

        return {
            'rows_processed': rows_processed,
            'rows_inserted': rows_inserted,
            'rows_failed': rows_failed,
            'processing_time': processing_time
        }

    def _get_data_batches(self, ingestor, table_name: str, batch_size: int):
        """Get data in batches from source."""
        batch = []
        for row in ingestor.extract_data_streaming(table_name, batch_size=batch_size):
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []

        # Yield remaining rows
        if batch:
            yield batch

    def _insert_batch_to_target(self, target_ingestor, table_name: str,
                                data: List[Dict[str, Any]], metadata) -> int:
        """Insert batch data to target database."""
        # This is a mock implementation
        # In reality, you'd use the target ingestor to insert data
        return len(data)  # Assume all rows inserted successfully

    def _reconstruct_connection_config(self, config_dict: Dict[str, Any]) -> ConnectionConfig:
        """Reconstruct ConnectionConfig from dictionary."""
        # Note: In a real implementation, you'd need to securely store and retrieve passwords
        return ConnectionConfig(
            host=config_dict['host'],
            port=config_dict['port'],
            database=config_dict['database'],
            username=config_dict['username'],
            password=config_dict['password'],  # Would need to be retrieved securely
            db_type=DatabaseType(config_dict['db_type'])
        )

    def rollback_ingestion(self, execution_id: str) -> bool:
        # Implementation would depend on your rollback strategy
        return super().rollback_ingestion(execution_id)

    def get_ingestion_status(self, execution_id: str) -> Dict[str, Any]:
        return self.active_executions.get(execution_id, {
            'error': 'Execution not found'
        })

    def _estimate_processing_time(self, row_count: int) -> float:
        # Simple estimation: 1000 rows per minute
        return max(1.0, row_count / 1000.0)

    def _extract_schema_for_llm(self, ingestor, source_config) -> dict:
        """Extract comprehensive schema information for LLM consumption."""
        try:

            # Get complete schema information
            tables = ingestor.discover_tables()

            # Structure for LLM consumption
            schema_info = {
                "database_name": source_config.database,
                "database_type": source_config.db_type.value,
                "tables": [],
                "relationships": [],
                "table_count": len(tables),
                "generated_at": datetime.now().isoformat()
            }

            # Process each table
            for table in tables:
                table_info = {
                    "name": table.name,
                    "schema": table.schema,
                    "row_count": table.row_count,
                    "columns": [
                        {
                            "name": col["name"],
                            "type": col["type"],
                            "nullable": col["nullable"],
                            "primary_key": col["name"] in table.primary_keys,
                            "default": col.get("default")
                        }
                        for col in table.columns
                    ],
                    "primary_keys": table.primary_keys,
                    "foreign_keys": table.foreign_keys,
                    "indexes": table.indexes
                }
                schema_info["tables"].append(table_info)

                # Extract relationships
                for fk in table.foreign_keys:
                    relationship = {
                        "from_table": table.name,
                        "from_column": fk["constrained_columns"][0],
                        "to_table": fk["referred_table"],
                        "to_column": fk["referred_columns"][0],
                        "relationship_type": "foreign_key"
                    }
                    schema_info["relationships"].append(relationship)

            return schema_info
        except Exception as e:
            self.logger.error(f"Failed to extract schema information: {e}")
            return {}
        #
        # finally:
        #     ingestor.disconnect()