"""MongoDB query executor for aggregation pipelines and queries."""
import time
import uuid
import hashlib
import json
import logging
from typing import Dict, Any, List, Optional, Generator
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from app.agents.query_engine.interfaces import QueryExecutorInterface
from app.agents.query_engine.interfaces.query_executor_interface import (
    QueryExecutionResult, ExecutionStatus
)


logger = logging.getLogger(__name__)


class MongoDBQueryCache:
    """Simple in-memory cache for MongoDB query results."""
    
    def __init__(self):
        self.cache: Dict[str, Dict[str, Any]] = {}
    
    def get(self, query_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached result if not expired."""
        if query_hash in self.cache:
            cached = self.cache[query_hash]
            if datetime.utcnow() < cached['expires_at']:
                return cached
            else:
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


class MongoDBQueryExecutor(QueryExecutorInterface):
    """
    Executes MongoDB aggregation pipelines and queries.
    
    Supports MongoDB aggregation framework for complex queries.
    """
    
    def __init__(self):
        """Initialize MongoDB query executor."""
        self.client: Optional[MongoClient] = None
        self.connection_string: Optional[str] = None
        self.cache = MongoDBQueryCache()
    
    def execute_query(
        self,
        query: str,
        connection_config: Dict[str, Any],
        use_cache: bool = True,
        cache_ttl_seconds: int = 300
    ) -> QueryExecutionResult:
        """Execute MongoDB aggregation pipeline and return results."""
        execution_id = str(uuid.uuid4())
        start_time = time.time()
        
        try:
            # Parse query as JSON (aggregation pipeline)
            pipeline = json.loads(query)
            if not isinstance(pipeline, list):
                raise ValueError("MongoDB query must be a JSON array (aggregation pipeline)")
            
            # Build connection string
            conn_str = self._build_connection_string(connection_config)
            collection_name = connection_config.get('collection')
            if not collection_name:
                raise ValueError("Collection name must be specified in connection_config")
            
            # Check cache
            if use_cache:
                query_hash = MongoDBQueryCache.hash_query(query, conn_str + collection_name)
                cached_result = self.cache.get(query_hash)
                if cached_result:
                    logger.info(f"Cache hit for MongoDB query on collection: {collection_name}")
                    return QueryExecutionResult(
                        execution_id=execution_id,
                        status=ExecutionStatus.CACHED,
                        data=cached_result['data']['documents'],
                        columns=cached_result['data']['columns'],
                        rows_returned=len(cached_result['data']['documents']),
                        execution_time_ms=(time.time() - start_time) * 1000,
                        from_cache=True,
                        cached_at=cached_result['cached_at']
                    )
            
            # Create client if needed
            if not self.client or self.connection_string != conn_str:
                self._create_client(conn_str)
            
            # Get database and collection
            database_name = connection_config.get('database')
            db = self.client[database_name]
            collection = db[collection_name]
            
            # Execute aggregation pipeline
            logger.info(f"Executing MongoDB pipeline on {database_name}.{collection_name}")
            cursor = collection.aggregate(pipeline)
            
            # Fetch all results
            documents = list(cursor)
            
            # Convert ObjectId and other MongoDB types to JSON-serializable formats
            documents = self._serialize_documents(documents)
            
            # Get column names from first document
            columns = []
            if documents:
                columns = [
                    {"name": key, "type": type(value).__name__}
                    for key, value in documents[0].items()
                ]
            
            execution_time_ms = (time.time() - start_time) * 1000
            
            # Cache results
            if use_cache:
                self.cache.set(
                    query_hash,
                    {'documents': documents, 'columns': columns},
                    cache_ttl_seconds
                )
            
            logger.info(f"MongoDB query executed successfully. Documents: {len(documents)}, Time: {execution_time_ms:.2f}ms")
            
            return QueryExecutionResult(
                execution_id=execution_id,
                status=ExecutionStatus.COMPLETED,
                data=documents,
                columns=columns,
                rows_returned=len(documents),
                execution_time_ms=execution_time_ms,
                from_cache=False
            )
        
        except PyMongoError as e:
            logger.error(f"MongoDB execution error: {str(e)}")
            return QueryExecutionResult(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                data=[],
                columns=[],
                rows_returned=0,
                execution_time_ms=(time.time() - start_time) * 1000,
                from_cache=False,
                error_message=f"MongoDB error: {str(e)}"
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
        """Execute MongoDB query with streaming results."""
        try:
            # Parse query as JSON
            pipeline = json.loads(query)
            if not isinstance(pipeline, list):
                raise ValueError("MongoDB query must be a JSON array")
            
            # Build connection string
            conn_str = self._build_connection_string(connection_config)
            collection_name = connection_config.get('collection')
            if not collection_name:
                raise ValueError("Collection name must be specified")
            
            # Create client if needed
            if not self.client or self.connection_string != conn_str:
                self._create_client(conn_str)
            
            # Get database and collection
            database_name = connection_config.get('database')
            db = self.client[database_name]
            collection = db[collection_name]
            
            logger.info(f"Executing streaming MongoDB query on {database_name}.{collection_name}")
            
            # Execute aggregation pipeline with cursor
            cursor = collection.aggregate(pipeline, batchSize=batch_size)
            
            # Stream results
            for document in cursor:
                yield self._serialize_document(document)
        
        except Exception as e:
            logger.error(f"Streaming query error: {str(e)}")
            raise
    
    def explain_execution_plan(
        self,
        query: str,
        connection_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get MongoDB query execution plan."""
        try:
            # Parse query
            pipeline = json.loads(query)
            if not isinstance(pipeline, list):
                raise ValueError("MongoDB query must be a JSON array")
            
            # Build connection string
            conn_str = self._build_connection_string(connection_config)
            collection_name = connection_config.get('collection')
            
            # Create client if needed
            if not self.client or self.connection_string != conn_str:
                self._create_client(conn_str)
            
            # Get database and collection
            database_name = connection_config.get('database')
            db = self.client[database_name]
            collection = db[collection_name]
            
            # Get explain plan
            explain_result = db.command('aggregate', collection_name, pipeline=pipeline, explain=True)
            
            return {
                "execution_plan": explain_result,
                "database": database_name,
                "collection": collection_name
            }
        
        except Exception as e:
            logger.error(f"Explain plan error: {str(e)}")
            return {
                "error": str(e),
                "execution_plan": {}
            }
    
    def test_connection(self, connection_config: Dict[str, Any]) -> bool:
        """Test MongoDB connection."""
        try:
            conn_str = self._build_connection_string(connection_config)
            test_client = MongoClient(
                conn_str,
                serverSelectionTimeoutMS=5000
            )
            
            # Test connection by running a simple command
            test_client.admin.command('ping')
            test_client.close()
            return True
        
        except Exception as e:
            logger.error(f"MongoDB connection test failed: {str(e)}")
            return False
    
    def close_connection(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.connection_string = None
            logger.info("MongoDB connection closed")
    
    def _create_client(self, connection_string: str):
        """Create MongoDB client."""
        try:
            # Close existing client
            if self.client:
                self.client.close()
            
            # Create new client
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                socketTimeoutMS=30000
            )
            self.connection_string = connection_string
            logger.info("MongoDB client created successfully")
        
        except Exception as e:
            logger.error(f"Failed to create MongoDB client: {str(e)}")
            raise
    
    def _build_connection_string(self, config: Dict[str, Any]) -> str:
        """Build MongoDB connection string from config."""
        host = config.get('host', 'localhost')
        port = config.get('port', 27017)
        username = config.get('username')
        password = config.get('password')
        database = config.get('database', 'admin')
        
        # Build connection string
        if username and password:
            conn_str = f"mongodb://{username}:{password}@{host}:{port}/{database}"
        else:
            conn_str = f"mongodb://{host}:{port}/{database}"
        
        # Add additional parameters
        additional_params = config.get('additional_params', {})
        if additional_params:
            params = "&".join([f"{k}={v}" for k, v in additional_params.items()])
            conn_str += f"?{params}"
        
        return conn_str
    
    def _serialize_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize MongoDB document to JSON-compatible format."""
        from bson import ObjectId
        from datetime import datetime
        
        serialized = {}
        for key, value in document.items():
            if isinstance(value, ObjectId):
                serialized[key] = str(value)
            elif isinstance(value, datetime):
                serialized[key] = value.isoformat()
            elif isinstance(value, dict):
                serialized[key] = self._serialize_document(value)
            elif isinstance(value, list):
                serialized[key] = [
                    self._serialize_document(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                serialized[key] = value
        
        return serialized
    
    def _serialize_documents(self, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Serialize list of MongoDB documents."""
        return [self._serialize_document(doc) for doc in documents]
