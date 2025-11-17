"""Interface for query generators across different data sources."""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum


class QueryLanguage(Enum):
    """Supported query languages."""
    SQL = "sql"
    MONGODB_QUERY = "mongodb_query"
    ELASTICSEARCH_DSL = "elasticsearch_dsl"
    GRAPHQL = "graphql"


@dataclass
class GeneratedQuery:
    """Container for generated query results."""
    query: str  # The actual query string (SQL, MongoDB aggregation pipeline, etc.)
    query_language: QueryLanguage
    query_type: str  # 'select', 'aggregate', 'join', 'analysis'
    tables_or_collections: List[str]
    explanation: str
    confidence_score: float
    warnings: List[str]
    estimated_rows: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


class QueryGeneratorInterface(ABC):
    """
    Abstract interface for query generators.
    
    Implementations convert natural language queries into database-specific
    query languages (SQL, MongoDB aggregation pipelines, etc.)
    """
    
    @abstractmethod
    def generate_query(
        self,
        natural_language_query: str,
        schema_context: Dict[str, Any],
        additional_context: Optional[Dict[str, Any]] = None
    ) -> GeneratedQuery:
        """
        Generate a database-specific query from natural language.
        
        Args:
            natural_language_query: User's natural language request
            schema_context: Database schema information (tables, collections, fields)
            additional_context: Additional context like user preferences, filters, etc.
            
        Returns:
            GeneratedQuery: Container with generated query and metadata
        """
        pass
    
    @abstractmethod
    def validate_query(
        self,
        query: str,
        schema_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate a query against the schema.
        
        Args:
            query: The query string to validate
            schema_context: Database schema information
            
        Returns:
            Dict with validation results (valid: bool, errors: List[str], warnings: List[str])
        """
        pass
    
    @abstractmethod
    def explain_query(
        self,
        query: str
    ) -> str:
        """
        Generate human-readable explanation of what the query does.
        
        Args:
            query: The query string to explain
            
        Returns:
            Human-readable explanation
        """
        pass
    
    @abstractmethod
    def get_supported_query_language(self) -> QueryLanguage:
        """
        Get the query language this generator produces.
        
        Returns:
            QueryLanguage enum value
        """
        pass
