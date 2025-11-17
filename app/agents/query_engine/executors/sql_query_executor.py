"""SQL query executor using SQLAlchemy for multiple SQL databases."""
import time
import uuid
import hashlib
import json
import logging
from typing import Dict, Any, List, Optional, Generator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.agents.query_engine.interfaces import QueryExecutorInterface
from app.agents.query_engine.interfaces.query_executor_interface import (
    QueryExecutionResult, ExecutionStatus
)


logger = logging.getLogger(__name__)


class QueryCache:
    """Simple in-memory cache for query results."""
    
    def __init__(self):
        self.cache: Dict[str, Dict[str, Any]] = {}
    
    def get(self, query_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached result if not expired."""
        if query_hash in self.cache:
            cached = self.cache[query_hash]
            if datetime.utcnow() < cached['expires_at']:
                return cached
            else:
                # Expired, remove from cache
                del self.cache[query_hash]
        return None
    
    def set(self, query_hash: str, data: Any, ttl_seconds: int):
        """Cache query result."""
        self.cache[query_hash] = {
            'data': data,
            'cached_at': datetime.utcnow(),
            'expires_at': datetime.utcnow() + timedelta(seconds=ttl_seconds)
        }
    
    def clear(self):
        """Clear all cached results."""
        self.cache.clear()
    
    @staticmethod
    def hash_query(query: str, connection_str: str) -> str:
        """Generate hash for query caching."""
        content = f"{query}:{connection_str}"
        return hashlib.sha256(content.encode()).hexdigest()


class SQLQueryExecutor(QueryExecutorInterface):
    """
    Executes SQL queries using SQLAlchemy.
    
    Supports: PostgreSQL, MySQL, SQLite, Oracle, SQL Server, and other SQLAlchemy-compatible databases.
    """
    
    def __init__(self):
        """Initialize SQL query executor."""
        self.engine: Optional[Engine] = None
        self.connection_string: Optional[str] = None
        self.cache = QueryCache()
    
    def execute_query(
        self,
        query: str,
        connection_config: Dict[str, Any],
        use_cache: bool = True,
        cache_ttl_seconds: int = 300
    ) -> QueryExecutionResult:
        """Execute SQL query and return results."""
        execution_id = str(uuid.uuid4())
        start_time = time.time()
        
        try:
            # Build connection string
            conn_str = self._build_connection_string(connection_config)
            
            # Check cache
            if use_cache:
                query_hash = QueryCache.hash_query(query, conn_str)
                cached_result = self.cache.get(query_hash)
                if cached_result:
                    logger.info(f"Cache hit for query: {query[:50]}...")
                    return QueryExecutionResult(
                        execution_id=execution_id,
                        status=ExecutionStatus.CACHED,
                        data=cached_result['data']['rows'],
                        columns=cached_result['data']['columns'],
                        rows_returned=len(cached_result['data']['rows']),
                        execution_time_ms=(time.time() - start_time) * 1000,
                        from_cache=True,
                        cached_at=cached_result['cached_at']
                    )
            
            # Create engine if needed
            if not self.engine or self.connection_string != conn_str:
                self._create_engine(conn_str)
            
            # Execute query
            logger.info(f"Executing SQL query: {query[:100]}...")
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                
                # Fetch all results
                rows = []
                for row in result:
                    rows.append(dict(row._mapping))
                
                # Get column metadata
                columns = [
                    {"name": col, "type": str(type(rows[0][col]).__name__) if rows else "unknown"}
                    for col in result.keys()
                ]
                
                execution_time_ms = (time.time() - start_time) * 1000
                
                # Cache results
                if use_cache:
                    self.cache.set(
                        query_hash,
                        {'rows': rows, 'columns': columns},
                        cache_ttl_seconds
                    )
                
                logger.info(f"Query executed successfully. Rows: {len(rows)}, Time: {execution_time_ms:.2f}ms")
                
                return QueryExecutionResult(
                    execution_id=execution_id,
                    status=ExecutionStatus.COMPLETED,
                    data=rows,
                    columns=columns,
                    rows_returned=len(rows),
                    execution_time_ms=execution_time_ms,
                    from_cache=False
                )
        
        except SQLAlchemyError as e:
            logger.error(f"SQL execution error: {str(e)}")
            return QueryExecutionResult(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                data=[],
                columns=[],
                rows_returned=0,
                execution_time_ms=(time.time() - start_time) * 1000,
                from_cache=False,
                error_message=f"SQL error: {str(e)}"
            )
        
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            return QueryExecutionResult(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                data=[],
                columns=[],
                rows_returned=0,
                execution_time_ms=(time.time() - start_time) * 1000,
                from_cache=False,
                error_message=str(e)
            )
    
    def execute_query_streaming(
        self,
        query: str,
        connection_config: Dict[str, Any],
        batch_size: int = 1000
    ) -> Generator[Dict[str, Any], None, None]:
        """Execute query with streaming results."""
        try:
            # Build connection string
            conn_str = self._build_connection_string(connection_config)
            
            # Create engine if needed
            if not self.engine or self.connection_string != conn_str:
                self._create_engine(conn_str)
            
            logger.info(f"Executing streaming query: {query[:100]}...")
            
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                
                # Stream results
                for row in result:
                    yield dict(row._mapping)
        
        except Exception as e:
            logger.error(f"Streaming query error: {str(e)}")
            raise
    
    def explain_execution_plan(
        self,
        query: str,
        connection_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get query execution plan."""
        try:
            # Build connection string
            conn_str = self._build_connection_string(connection_config)
            
            # Create engine if needed
            if not self.engine or self.connection_string != conn_str:
                self._create_engine(conn_str)
            
            # Get database type to use appropriate EXPLAIN syntax
            db_type = connection_config.get('db_type', 'postgresql').lower()
            
            if db_type == 'postgresql':
                explain_query = f"EXPLAIN (FORMAT JSON) {query}"
            elif db_type == 'mysql':
                explain_query = f"EXPLAIN FORMAT=JSON {query}"
            elif db_type == 'sqlite':
                explain_query = f"EXPLAIN QUERY PLAN {query}"
            else:
                explain_query = f"EXPLAIN {query}"
            
            with self.engine.connect() as connection:
                result = connection.execute(text(explain_query))
                rows = [dict(row._mapping) for row in result]
                
                return {
                    "execution_plan": rows,
                    "db_type": db_type,
                    "raw_explain": str(rows)
                }
        
        except Exception as e:
            logger.error(f"Explain plan error: {str(e)}")
            return {
                "error": str(e),
                "execution_plan": []
            }
    
    def test_connection(self, connection_config: Dict[str, Any]) -> bool:
        """Test database connection."""
        try:
            conn_str = self._build_connection_string(connection_config)
            test_engine = create_engine(
                conn_str,
                pool_pre_ping=True,
                connect_args={"connect_timeout": 5}
            )
            
            with test_engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            
            test_engine.dispose()
            return True
        
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def close_connection(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.connection_string = None
            logger.info("Database connection closed")
    
    def _create_engine(self, connection_string: str):
        """Create SQLAlchemy engine."""
        try:
            # Close existing engine
            if self.engine:
                self.engine.dispose()
            
            # Create new engine
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                echo=False
            )
            self.connection_string = connection_string
            logger.info("Database engine created successfully")
        
        except Exception as e:
            logger.error(f"Failed to create engine: {str(e)}")
            raise
    
    def _build_connection_string(self, config: Dict[str, Any]) -> str:
        """Build SQLAlchemy connection string from config."""
        db_type = config.get('db_type', 'postgresql').lower()
        host = config.get('host', 'localhost')
        port = config.get('port')
        database = config.get('database')
        username = config.get('username')
        password = config.get('password')
        
        # Map database types to SQLAlchemy dialects
        dialect_map = {
            'postgresql': 'postgresql',
            'mysql': 'mysql+pymysql',
            'sqlite': 'sqlite',
            'oracle': 'oracle+cx_oracle',
            'sqlserver': 'mssql+pyodbc',
            'mssql': 'mssql+pyodbc'
        }
        
        dialect = dialect_map.get(db_type, 'postgresql')
        
        # Handle SQLite (file-based)
        if db_type == 'sqlite':
            if database:
                return f"sqlite:///{database}"
            else:
                return "sqlite:///:memory:"
        
        # Build connection string for other databases
        if username and password:
            auth = f"{username}:{password}"
        elif username:
            auth = username
        else:
            auth = ""
        
        if port:
            host_port = f"{host}:{port}"
        else:
            host_port = host
        
        if auth:
            conn_str = f"{dialect}://{auth}@{host_port}/{database}"
        else:
            conn_str = f"{dialect}://{host_port}/{database}"
        
        # Add additional parameters
        additional_params = config.get('additional_params', {})
        if additional_params:
            params = "&".join([f"{k}={v}" for k, v in additional_params.items()])
            conn_str += f"?{params}"
        
        return conn_str
