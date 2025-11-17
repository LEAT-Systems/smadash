"""Service for background schema ingestion."""
import uuid
import logging
from typing import List, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session

from app.api.db.models import DataSource, DataSourceSchema
from app.agents.database_ingestor.ingestor_factory import DatabaseIngestorFactory
from app.agents.utils.database_connection_schema import ConnectionConfig


logger = logging.getLogger(__name__)


class SchemaIngestionService:
    """Service for ingesting database schemas."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def ingest_schema(self, datasource_id: str, connection_config: ConnectionConfig) -> Dict[str, Any]:
        """Ingest schema from a data source."""
        logger.info(f"Starting schema ingestion for datasource: {datasource_id}")
        
        try:
            # Get appropriate ingestor
            ingestor = DatabaseIngestorFactory.create_ingestor(connection_config.db_type)
            
            # Connect to database
            connected = ingestor.connect(connection_config)
            if not connected:
                raise Exception("Failed to connect to database")
            
            # Discover tables
            tables_metadata = ingestor.discover_tables()
            logger.info(f"Discovered {len(tables_metadata)} tables")
            
            # Delete existing schema for this datasource
            self.db.query(DataSourceSchema).filter(
                DataSourceSchema.datasource_id == datasource_id
            ).delete()
            
            # Save schema for each table
            tables_saved = []
            for table_meta in tables_metadata:
                schema_record = DataSourceSchema(
                    id=str(uuid.uuid4()),
                    datasource_id=datasource_id,
                    table_name=table_meta.name,
                    schema_name=table_meta.schema,
                    row_count=table_meta.row_count,
                    columns=self._serialize_columns(table_meta.columns),
                    primary_keys=table_meta.primary_keys,
                    foreign_keys=table_meta.foreign_keys,
                    indexes=table_meta.indexes
                )
                self.db.add(schema_record)
                tables_saved.append(table_meta.name)
            
            # Commit all schema records
            self.db.commit()
            
            # Disconnect from database
            ingestor.disconnect()
            
            logger.info(f"Schema ingestion completed for datasource: {datasource_id}")
            
            return {
                'success': True,
                'tables_ingested': len(tables_saved),
                'table_names': tables_saved,
                'ingested_at': datetime.utcnow()
            }
        
        except Exception as e:
            logger.error(f"Schema ingestion failed for datasource {datasource_id}: {str(e)}")
            self.db.rollback()
            return {
                'success': False,
                'error': str(e),
                'tables_ingested': 0
            }
    
    def _serialize_columns(self, columns: List[Any]) -> List[Dict[str, Any]]:
        """Serialize column metadata for JSON storage."""
        serialized = []
        for col in columns:
            # Handle both dict and object column metadata
            if isinstance(col, dict):
                serialized.append(col)
            else:
                # If it's an object with attributes
                serialized.append({
                    'name': getattr(col, 'name', ''),
                    'data_type': getattr(col, 'data_type', ''),
                    'nullable': getattr(col, 'nullable', True),
                    'default_value': getattr(col, 'default_value', None),
                    'auto_increment': getattr(col, 'auto_increment', False)
                })
        return serialized
    
    def get_schema(self, datasource_id: str) -> List[DataSourceSchema]:
        """Get schema for a data source."""
        return self.db.query(DataSourceSchema).filter(
            DataSourceSchema.datasource_id == datasource_id
        ).all()
    
    def get_table_schema(self, datasource_id: str, table_name: str) -> DataSourceSchema:
        """Get schema for a specific table."""
        return self.db.query(DataSourceSchema).filter(
            DataSourceSchema.datasource_id == datasource_id,
            DataSourceSchema.table_name == table_name
        ).first()
    
    def get_schema_context(self, datasource_id: str) -> str:
        """Get formatted schema context for LLM."""
        schemas = self.get_schema(datasource_id)
        
        if not schemas:
            return "No schema available for this data source."
        
        context_lines = ["Database Schema:\n"]
        
        for schema in schemas:
            table_name = schema.table_name
            context_lines.append(f"\nTable: {table_name}")
            context_lines.append(f"Rows: {schema.row_count}")
            
            # Add columns
            context_lines.append("Columns:")
            for col in schema.columns:
                nullable = " (nullable)" if col.get('nullable') else " (required)"
                pk = " [PRIMARY KEY]" if col.get('name') in (schema.primary_keys or []) else ""
                context_lines.append(
                    f"  - {col.get('name')}: {col.get('data_type')}{nullable}{pk}"
                )
            
            # Add foreign keys
            if schema.foreign_keys:
                context_lines.append("Foreign Keys:")
                for fk in schema.foreign_keys:
                    context_lines.append(
                        f"  - {fk.get('constrained_columns')} -> "
                        f"{fk.get('referred_table')}.{fk.get('referred_columns')}"
                    )
            
            # Add indexes
            if schema.indexes:
                context_lines.append("Indexes:")
                for idx in schema.indexes:
                    unique = " (unique)" if idx.get('unique') else ""
                    context_lines.append(
                        f"  - {idx.get('name')}: {idx.get('column_names')}{unique}"
                    )
        
        return "\n".join(context_lines)
