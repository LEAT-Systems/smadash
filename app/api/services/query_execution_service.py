"""Query execution service."""
import uuid
import time
import logging
import hashlib
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from app.api.db.models import QueryExecution, QueryStatusEnum, GeneratedQuery
from app.api.models.query import QueryExecuteRequest, QueryExecuteResponse
from app.agents.database_ingestor.ingestor_factory import DatabaseIngestorFactory
from app.agents.utils.database_connection_schema import ConnectionConfig


logger = logging.getLogger(__name__)


class QueryExecutionService:
    """Service for executing SQL queries against data sources."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def execute_query(
        self,
        request: QueryExecuteRequest,
        connection_config: ConnectionConfig
    ) -> QueryExecuteResponse:
        """Execute SQL query against data source."""
        logger.info(f"Executing query for datasource: {request.datasource_id}")
        
        execution_id = str(uuid.uuid4())
        start_time = time.time()
        
        # Check cache first
        if request.use_cache:
            cached_result = self._get_cached_result(request.sql_query, request.datasource_id)
            if cached_result:
                logger.info("Returning cached query result")
                return self._create_response_from_cache(cached_result, execution_id)
        
        try:
            # Get appropriate ingestor
            ingestor = DatabaseIngestorFactory.create_ingestor(connection_config.db_type)
            
            # Connect to database
            connected = ingestor.connect(connection_config)
            if not connected:
                raise Exception("Failed to connect to database")
            
            # Execute query (use extract_data method with custom SQL)
            # For now, we'll use a simple approach - in production, you'd want more control
            result_data = self._execute_sql(ingestor, request.sql_query, connection_config)
            
            # Disconnect
            ingestor.disconnect()
            
            execution_time_ms = (time.time() - start_time) * 1000
            
            # Extract columns from result
            columns = []
            if result_data and len(result_data) > 0:
                first_row = result_data[0]
                columns = [{'name': col, 'type': self._infer_type(first_row[col])} for col in first_row.keys()]
            
            # Create execution record
            cache_key = self._generate_cache_key(request.sql_query, request.datasource_id) if request.use_cache else None
            cache_expires_at = datetime.utcnow() + timedelta(seconds=request.cache_ttl_seconds) if request.use_cache else None
            
            execution = QueryExecution(
                id=execution_id,
                query_id=request.query_id,
                datasource_id=request.datasource_id,
                sql_query=request.sql_query,
                status=QueryStatusEnum.COMPLETED,
                rows_returned=len(result_data),
                execution_time_ms=execution_time_ms,
                result_data=result_data[:1000] if len(result_data) > 1000 else result_data,  # Cache first 1000 rows
                columns=columns,
                from_cache=False,
                cache_key=cache_key,
                cache_expires_at=cache_expires_at,
                user_id=request.user_id,
                organization_id=request.organization_id,
                canvas_id=request.canvas_id,
                dashboard_id=request.dashboard_id,
                component_id=request.component_id,
                executed_at=datetime.utcnow()
            )
            
            self.db.add(execution)
            self.db.commit()
            self.db.refresh(execution)
            
            return QueryExecuteResponse(
                execution_id=execution_id,
                query_id=request.query_id,
                status=QueryStatusEnum.COMPLETED,
                rows_returned=len(result_data),
                execution_time_ms=execution_time_ms,
                data=result_data,
                columns=columns,
                from_cache=False,
                executed_at=datetime.utcnow()
            )
        
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            
            # Record failed execution
            execution = QueryExecution(
                id=execution_id,
                query_id=request.query_id,
                datasource_id=request.datasource_id,
                sql_query=request.sql_query,
                status=QueryStatusEnum.FAILED,
                error_message=str(e),
                user_id=request.user_id,
                organization_id=request.organization_id,
                canvas_id=request.canvas_id,
                dashboard_id=request.dashboard_id,
                component_id=request.component_id,
                executed_at=datetime.utcnow()
            )
            
            self.db.add(execution)
            self.db.commit()
            
            raise
    
    def _execute_sql(self, ingestor, sql_query: str, connection_config: ConnectionConfig) -> List[Dict[str, Any]]:
        """Execute SQL query using the ingestor."""
        try:
            # Use SQLAlchemy engine to execute custom SQL
            from sqlalchemy import text
            
            # Execute query directly
            result = ingestor.session.execute(text(sql_query))
            
            # Convert to list of dictionaries
            rows = []
            for row in result:
                row_dict = dict(row._mapping)
                # Convert datetime objects to ISO format strings
                for key, value in row_dict.items():
                    if isinstance(value, datetime):
                        row_dict[key] = value.isoformat()
                rows.append(row_dict)
            
            return rows
        
        except Exception as e:
            logger.error(f"SQL execution error: {str(e)}")
            raise
    
    def _generate_cache_key(self, sql_query: str, datasource_id: str) -> str:
        """Generate cache key for query."""
        content = f"{datasource_id}:{sql_query}".encode()
        return hashlib.sha256(content).hexdigest()
    
    def _get_cached_result(self, sql_query: str, datasource_id: str) -> Optional[QueryExecution]:
        """Get cached query result if available."""
        cache_key = self._generate_cache_key(sql_query, datasource_id)
        
        # Find non-expired cached result
        cached = self.db.query(QueryExecution).filter(
            QueryExecution.cache_key == cache_key,
            QueryExecution.status == QueryStatusEnum.COMPLETED,
            QueryExecution.cache_expires_at > datetime.utcnow()
        ).first()
        
        return cached
    
    def _create_response_from_cache(self, cached: QueryExecution, execution_id: str) -> QueryExecuteResponse:
        """Create response from cached execution."""
        return QueryExecuteResponse(
            execution_id=execution_id,
            query_id=cached.query_id,
            status=QueryStatusEnum.CACHED,
            rows_returned=cached.rows_returned,
            execution_time_ms=0,  # Instant from cache
            data=cached.result_data,
            columns=cached.columns,
            from_cache=True,
            cached_at=cached.executed_at,
            executed_at=datetime.utcnow()
        )
    
    def _infer_type(self, value: Any) -> str:
        """Infer column type from value."""
        if value is None:
            return 'string'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            return 'string'
        elif isinstance(value, (datetime, str)):
            # Check if string looks like datetime
            if isinstance(value, str) and ('T' in value or '-' in value):
                return 'datetime'
            return 'string'
        else:
            return 'string'
    
    def get_execution(self, execution_id: str) -> Optional[QueryExecution]:
        """Get execution by ID."""
        return self.db.query(QueryExecution).filter(QueryExecution.id == execution_id).first()
    
    def get_executions_by_query(self, query_id: str) -> List[QueryExecution]:
        """Get all executions for a query."""
        return self.db.query(QueryExecution).filter(
            QueryExecution.query_id == query_id
        ).order_by(QueryExecution.executed_at.desc()).all()
    
    def get_executions_by_user(self, user_id: str, limit: int = 50) -> List[QueryExecution]:
        """Get executions for a user."""
        return self.db.query(QueryExecution).filter(
            QueryExecution.user_id == user_id
        ).order_by(QueryExecution.executed_at.desc()).limit(limit).all()
