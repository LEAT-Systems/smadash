"""Query execution service using multi-datasource query engine."""
import uuid
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session

from app.api.db.models import GeneratedQuery, DataSource, QueryExecution
from app.api.models.query import QueryExecuteRequest, QueryExecuteResponse, QueryStatus
from app.agents.query_engine import QueryEngineFactory
from app.agents.utils.database_connection_schema import DatabaseType, ConnectionConfig


logger = logging.getLogger(__name__)


class QueryExecutorService:
    """
    Service for executing queries on different data sources using multi-datasource query engine.
    
    Supports SQL (PostgreSQL, MySQL, SQLite, Oracle, SQL Server) and MongoDB.
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def execute_query(
        self,
        request: QueryExecuteRequest
    ) -> QueryExecuteResponse:
        """
        Execute a query against a data source.
        
        Args:
            request: Query execution request
            
        Returns:
            QueryExecuteResponse with execution results
        """
        logger.info(f"Executing query for datasource: {request.datasource_id}")
        
        try:
            # Get datasource
            datasource = self.db.query(DataSource).filter(
                DataSource.id == request.datasource_id
            ).first()
            
            if not datasource:
                raise ValueError(f"Datasource not found: {request.datasource_id}")
            
            # Convert datasource db_type to DatabaseType enum
            db_type = self._convert_to_database_type(datasource.db_type.value)
            
            # Create query executor for this database type
            _, executor = QueryEngineFactory.create_query_engine(db_type)
            
            logger.info(f"Using {executor.__class__.__name__} for {db_type.value}")
            
            # Build connection config
            connection_config = self._build_connection_config(datasource)
            
            # Execute query
            result = executor.execute_query(
                query=request.sql_query,
                connection_config=connection_config,
                use_cache=request.use_cache,
                cache_ttl_seconds=request.cache_ttl_seconds
            )
            
            # Create execution record
            execution_id = result.execution_id
            query_execution = QueryExecution(
                id=execution_id,
                query_id=request.query_id,
                datasource_id=request.datasource_id,
                sql_query=request.sql_query,
                status=result.status.value,
                rows_returned=result.rows_returned,
                execution_time_ms=result.execution_time_ms,
                from_cache=result.from_cache,
                cached_at=result.cached_at,
                error_message=result.error_message,
                user_id=request.user_id,
                organization_id=request.organization_id,
                canvas_id=request.canvas_id,
                dashboard_id=request.dashboard_id,
                component_id=request.component_id,
                executed_at=datetime.utcnow()
            )
            
            self.db.add(query_execution)
            self.db.commit()
            
            logger.info(f"Query executed. Rows: {result.rows_returned}, Time: {result.execution_time_ms:.2f}ms")
            
            return QueryExecuteResponse(
                execution_id=execution_id,
                query_id=request.query_id,
                status=QueryStatus(result.status.value),
                rows_returned=result.rows_returned,
                execution_time_ms=result.execution_time_ms,
                data=result.data,
                columns=result.columns,
                from_cache=result.from_cache,
                cached_at=result.cached_at,
                executed_at=datetime.utcnow()
            )
        
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_streaming_query(
        self,
        request: QueryExecuteRequest
    ):
        """
        Execute query with streaming results for large datasets.
        
        Args:
            request: Query execution request
            
        Yields:
            Individual rows of data
        """
        try:
            # Get datasource
            datasource = self.db.query(DataSource).filter(
                DataSource.id == request.datasource_id
            ).first()
            
            if not datasource:
                raise ValueError(f"Datasource not found: {request.datasource_id}")
            
            # Convert datasource db_type to DatabaseType enum
            db_type = self._convert_to_database_type(datasource.db_type.value)
            
            # Create query executor
            _, executor = QueryEngineFactory.create_query_engine(db_type)
            
            # Build connection config
            connection_config = self._build_connection_config(datasource)
            
            logger.info(f"Executing streaming query on {db_type.value}")
            
            # Execute with streaming
            for row in executor.execute_query_streaming(
                query=request.sql_query,
                connection_config=connection_config,
                batch_size=1000
            ):
                yield row
        
        except Exception as e:
            logger.error(f"Streaming query execution failed: {str(e)}")
            raise
    
    def explain_query(
        self,
        datasource_id: str,
        query: str
    ) -> Dict[str, Any]:
        """
        Get execution plan for a query without executing it.
        
        Args:
            datasource_id: ID of the datasource
            query: Query string to explain
            
        Returns:
            Execution plan details
        """
        try:
            # Get datasource
            datasource = self.db.query(DataSource).filter(
                DataSource.id == datasource_id
            ).first()
            
            if not datasource:
                raise ValueError(f"Datasource not found: {datasource_id}")
            
            # Convert datasource db_type to DatabaseType enum
            db_type = self._convert_to_database_type(datasource.db_type.value)
            
            # Create query executor
            _, executor = QueryEngineFactory.create_query_engine(db_type)
            
            # Build connection config
            connection_config = self._build_connection_config(datasource)
            
            # Get execution plan
            plan = executor.explain_execution_plan(query, connection_config)
            
            return plan
        
        except Exception as e:
            logger.error(f"Query explain failed: {str(e)}")
            raise
    
    def _convert_to_database_type(self, db_type_str: str) -> DatabaseType:
        """Convert string database type to DatabaseType enum."""
        mapping = {
            'mysql': DatabaseType.MYSQL,
            'postgresql': DatabaseType.POSTGRESQL,
            'sqlite': DatabaseType.SQLITE,
            'oracle': DatabaseType.ORACLE,
            'sqlserver': DatabaseType.SQLSERVER,
            'mongodb': DatabaseType.MONGODB,
        }
        db_type_lower = db_type_str.lower()
        if db_type_lower not in mapping:
            raise ValueError(f"Unsupported database type: {db_type_str}")
        return mapping[db_type_lower]
    
    def _build_connection_config(self, datasource: DataSource) -> Dict[str, Any]:
        """Build connection config from datasource."""
        from app.api.services.datasource_service import DataSourceService
        
        # Use datasource service to get connection config with decrypted password
        datasource_service = DataSourceService(self.db)
        connection_config = datasource_service.get_connection_config(datasource.id)
        
        if not connection_config:
            raise ValueError(f"Failed to build connection config for datasource: {datasource.id}")
        
        # Convert ConnectionConfig to dict
        return {
            'db_type': connection_config.db_type.value,
            'host': connection_config.host,
            'port': connection_config.port,
            'database': connection_config.database,
            'username': connection_config.username,
            'password': connection_config.password,
            'additional_params': connection_config.additional_params or {}
        }
