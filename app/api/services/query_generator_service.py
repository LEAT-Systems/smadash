"""Query generation service using multi-datasource query engine."""
import uuid
import time
import logging
import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session

from app.api.db.models import GeneratedQuery, DataSource
from app.api.models.query import QueryGenerateRequest, QueryGenerateResponse, QueryType
from app.agents.query_engine import QueryEngineFactory
from app.agents.utils.database_connection_schema import DatabaseType


logger = logging.getLogger(__name__)


class QueryGeneratorService:
    """
    Service for generating queries from natural language using multi-datasource query engine.
    
    Supports SQL (PostgreSQL, MySQL, SQLite, Oracle, SQL Server) and MongoDB.
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.model = os.getenv('LLM_MODEL', 'gpt-4o-mini')
    
    def generate_query(
        self, 
        request: QueryGenerateRequest,
        schema_context: Dict[str, Any]
    ) -> QueryGenerateResponse:
        """
        Generate database-specific query from natural language.
        
        Args:
            request: Query generation request with datasource info
            schema_context: Database schema information (dict or string)
            
        Returns:
            QueryGenerateResponse with generated query
        """
        logger.info(f"Generating query for: {request.natural_language_query}")
        
        start_time = time.time()
        
        try:
            # Get datasource to determine database type
            datasource = self.db.query(DataSource).filter(
                DataSource.id == request.datasource_id
            ).first()
            
            if not datasource:
                raise ValueError(f"Datasource not found: {request.datasource_id}")
            
            # Convert datasource db_type to DatabaseType enum
            db_type = self._convert_to_database_type(datasource.db_type.value)
            
            # Create query generator for this database type
            generator, _ = QueryEngineFactory.create_query_engine(
                db_type=db_type,
                model=self.model
            )
            
            logger.info(f"Using {generator.__class__.__name__} for {db_type.value}")
            
            # Generate query using the appropriate generator
            generated = generator.generate_query(
                natural_language_query=request.natural_language_query,
                schema_context=schema_context,
                additional_context=request.additional_context
            )
            
            # Determine query type
            query_type = self._map_query_type(generated.query_type)
            
            # Create query record in database
            query_id = str(uuid.uuid4())
            generated_query = GeneratedQuery(
                id=query_id,
                datasource_id=request.datasource_id,
                natural_language_query=request.natural_language_query,
                generated_sql=generated.query,
                query_type=query_type,
                tables_used=generated.tables_or_collections,
                estimated_rows=generated.estimated_rows,
                confidence_score=generated.confidence_score,
                explanation=generated.explanation,
                warnings=generated.warnings,
                user_id=request.user_id,
                organization_id=request.organization_id,
                canvas_id=request.canvas_id,
                dashboard_id=request.dashboard_id,
                component_id=request.component_id,
                llm_model=self.model,
                llm_tokens_used=None,  # Could extract from metadata if needed
                llm_response_time_ms=(time.time() - start_time) * 1000
            )
            
            self.db.add(generated_query)
            self.db.commit()
            self.db.refresh(generated_query)
            
            logger.info(f"Query generated successfully. ID: {query_id}, Type: {query_type}")
            
            return QueryGenerateResponse(
                query_id=query_id,
                natural_language_query=request.natural_language_query,
                generated_sql=generated.query,
                query_type=query_type,
                tables_used=generated.tables_or_collections,
                estimated_rows=generated.estimated_rows,
                explanation=generated.explanation,
                confidence_score=generated.confidence_score,
                warnings=generated.warnings,
                created_at=datetime.utcnow()
            )
        
        except Exception as e:
            logger.error(f"Query generation failed: {str(e)}")
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
    
    def _map_query_type(self, query_type_str: str) -> str:
        """Map query engine query type to QueryType enum value."""
        # Map from query engine types to API model types
        mapping = {
            'select': 'select',
            'aggregate': 'aggregate',
            'join': 'join',
            'analysis': 'analysis',
            'filter': 'select'  # MongoDB filter becomes select
        }
        return mapping.get(query_type_str.lower(), 'analysis')
    
    def get_query(self, query_id: str) -> Optional[GeneratedQuery]:
        """Get generated query by ID."""
        return self.db.query(GeneratedQuery).filter(GeneratedQuery.id == query_id).first()
    
    def get_queries_by_user(self, user_id: str, limit: int = 50) -> List[GeneratedQuery]:
        """Get queries for a user."""
        return self.db.query(GeneratedQuery).filter(
            GeneratedQuery.user_id == user_id
        ).order_by(GeneratedQuery.created_at.desc()).limit(limit).all()
    
    def get_queries_by_canvas(self, canvas_id: str) -> List[GeneratedQuery]:
        """Get queries for a canvas."""
        return self.db.query(GeneratedQuery).filter(
            GeneratedQuery.canvas_id == canvas_id
        ).order_by(GeneratedQuery.created_at.desc()).all()