"""Interface for query executors across different data sources."""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Generator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class ExecutionStatus(Enum):
    """Query execution status."""
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    CACHED = "cached"


@dataclass
class QueryExecutionResult:
    """Container for query execution results."""
    execution_id: str
    status: ExecutionStatus
    data: List[Dict[str, Any]]
    columns: List[Dict[str, str]]  # [{"name": "col1", "type": "int"}, ...]
    rows_returned: int
    execution_time_ms: float
    from_cache: bool = False
    cached_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class QueryExecutorInterface(ABC):
    """
    Abstract interface for query executors.
    
    Implementations execute database-specific queries and return normalized results.
    """
    
    @abstractmethod
    def execute_query(
        self,
        query: str,
        connection_config: Dict[str, Any],
        use_cache: bool = True,
        cache_ttl_seconds: int = 300
    ) -> QueryExecutionResult:
        """
        Execute a query against the data source.
        
        Args:
            query: The query string to execute
            connection_config: Database connection configuration
            use_cache: Whether to use cached results if available
            cache_ttl_seconds: Cache time-to-live in seconds
            
        Returns:
            QueryExecutionResult: Execution results with data and metadata
        """
        pass
    
    @abstractmethod
    def execute_query_streaming(
        self,
        query: str,
        connection_config: Dict[str, Any],
        batch_size: int = 1000
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Execute query with streaming results for large datasets.
        
        Args:
            query: The query string to execute
            connection_config: Database connection configuration
            batch_size: Number of rows per batch
            
        Yields:
            Individual row data
        """
        pass
    
    @abstractmethod
    def explain_execution_plan(
        self,
        query: str,
        connection_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get query execution plan without executing the query.
        
        Args:
            query: The query string to analyze
            connection_config: Database connection configuration
            
        Returns:
            Dict with execution plan details
        """
        pass
    
    @abstractmethod
    def test_connection(
        self,
        connection_config: Dict[str, Any]
    ) -> bool:
        """
        Test database connection.
        
        Args:
            connection_config: Database connection configuration
            
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def close_connection(self) -> None:
        """Close active database connection."""
        pass
