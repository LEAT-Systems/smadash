"""API routes for query generation and execution."""
from fastapi import APIRouter, Depends, HTTPException, Query as QueryParam
from sqlalchemy.orm import Session
from typing import List, Optional
import logging

from app.api.db.database import get_db
from app.api.models.query import (
    QueryGenerateRequest,
    QueryGenerateResponse,
    QueryExecuteRequest,
    QueryExecuteResponse,
    QueryHistoryResponse,
    QueryValidateRequest,
    QueryValidateResponse
)
from app.api.services.datasource_service import DataSourceService
from app.api.services.schema_ingestion_service import SchemaIngestionService
from app.api.services.query_generator_service import QueryGeneratorService
from app.api.services.query_execution_service import QueryExecutionService
from app.api.utils.dependencies import get_current_user


router = APIRouter(prefix="/queries", tags=["queries"])
logger = logging.getLogger(__name__)


@router.post("/generate", response_model=QueryGenerateResponse)
async def generate_query(
    request: QueryGenerateRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Generate SQL query from natural language.
    
    Flow:
    1. Get datasource schema from database
    2. Build context with table/column information
    3. Call LLM with natural language query + schema context
    4. Parse and validate generated SQL
    5. Save query to database
    6. Return generated query
    """
    try:
        # Verify datasource exists
        datasource_service = DataSourceService(db)
        datasource = datasource_service.get_datasource(request.datasource_id)
        
        if not datasource:
            raise HTTPException(status_code=404, detail="Datasource not found")
        
        if not datasource.schema_ingested:
            raise HTTPException(
                status_code=400,
                detail="Schema not yet ingested for this datasource. Please wait for ingestion to complete."
            )
        
        # Get schema context
        schema_service = SchemaIngestionService(db)
        schema_context = schema_service.get_schema_context(request.datasource_id)
        
        # Generate query using LLM
        query_service = QueryGeneratorService(db)
        result = query_service.generate_query(request, schema_context)
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query generation failed: {str(e)}")


@router.post("/execute", response_model=QueryExecuteResponse)
async def execute_query(
    request: QueryExecuteRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Execute SQL query against datasource.
    
    Flow:
    1. Get datasource connection config
    2. Check cache for recent identical query
    3. If not cached, execute query using ingestor
    4. Save results to cache (limited to first 1000 rows)
    5. Save execution metadata
    6. Return results
    """
    try:
        # Get datasource and connection config
        datasource_service = DataSourceService(db)
        connection_config = datasource_service.get_connection_config(request.datasource_id)
        
        if not connection_config:
            raise HTTPException(status_code=404, detail="Datasource not found")
        
        # Execute query
        execution_service = QueryExecutionService(db)
        result = execution_service.execute_query(request, connection_config)
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")


@router.post("/generate-and-execute", response_model=dict)
async def generate_and_execute_query(
    request: QueryGenerateRequest,
    use_cache: bool = True,
    cache_ttl_seconds: int = 300,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Generate SQL from natural language and immediately execute it.
    
    This is a convenience endpoint that combines generate + execute.
    """
    try:
        # Generate query
        datasource_service = DataSourceService(db)
        datasource = datasource_service.get_datasource(request.datasource_id)
        
        if not datasource:
            raise HTTPException(status_code=404, detail="Datasource not found")
        
        if not datasource.schema_ingested:
            raise HTTPException(
                status_code=400,
                detail="Schema not yet ingested for this datasource."
            )
        
        schema_service = SchemaIngestionService(db)
        schema_context = schema_service.get_schema_context(request.datasource_id)
        
        query_service = QueryGeneratorService(db)
        generated = query_service.generate_query(request, schema_context)
        
        # Execute query
        connection_config = datasource_service.get_connection_config(request.datasource_id)
        
        execute_request = QueryExecuteRequest(
            query_id=generated.query_id,
            datasource_id=request.datasource_id,
            sql_query=generated.generated_sql,
            user_id=request.user_id,
            organization_id=request.organization_id,
            canvas_id=request.canvas_id,
            dashboard_id=request.dashboard_id,
            component_id=request.component_id,
            use_cache=use_cache,
            cache_ttl_seconds=cache_ttl_seconds
        )
        
        execution_service = QueryExecutionService(db)
        executed = execution_service.execute_query(execute_request, connection_config)
        
        return {
            "generated_query": generated,
            "execution_result": executed
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Generate and execute failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", response_model=List[QueryHistoryResponse])
async def get_query_history(
    user_id: Optional[str] = None,
    canvas_id: Optional[str] = None,
    dashboard_id: Optional[str] = None,
    limit: int = QueryParam(50, le=200),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get query history for user, canvas, or dashboard."""
    try:
        query_service = QueryGeneratorService(db)
        
        if canvas_id:
            queries = query_service.get_queries_by_canvas(canvas_id)
        elif user_id:
            queries = query_service.get_queries_by_user(user_id, limit)
        else:
            # Default to current user
            queries = query_service.get_queries_by_user(current_user.get('id'), limit)
        
        # Convert to response format
        results = []
        for query in queries:
            # Get latest execution
            execution_service = QueryExecutionService(db)
            executions = execution_service.get_executions_by_query(query.id)
            latest_execution = executions[0] if executions else None
            
            results.append(QueryHistoryResponse(
                id=query.id,
                natural_language_query=query.natural_language_query,
                generated_sql=query.generated_sql,
                query_type=query.query_type,
                datasource_id=query.datasource_id,
                datasource_name=query.datasource.name if query.datasource else "Unknown",
                status=latest_execution.status if latest_execution else "pending",
                rows_returned=latest_execution.rows_returned if latest_execution else None,
                execution_time_ms=latest_execution.execution_time_ms if latest_execution else None,
                canvas_id=query.canvas_id,
                dashboard_id=query.dashboard_id,
                component_id=query.component_id,
                created_at=query.created_at,
                executed_at=latest_execution.executed_at if latest_execution else None
            ))
        
        return results
    
    except Exception as e:
        logger.error(f"Failed to get query history: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get query history")


@router.get("/{query_id}", response_model=QueryGenerateResponse)
async def get_query(
    query_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get a specific generated query."""
    query_service = QueryGeneratorService(db)
    query = query_service.get_query(query_id)
    
    if not query:
        raise HTTPException(status_code=404, detail="Query not found")
    
    return QueryGenerateResponse(
        query_id=query.id,
        natural_language_query=query.natural_language_query,
        generated_sql=query.generated_sql,
        query_type=query.query_type,
        tables_used=query.tables_used or [],
        estimated_rows=query.estimated_rows,
        explanation=query.explanation,
        confidence_score=query.confidence_score,
        warnings=query.warnings or [],
        created_at=query.created_at
    )


@router.get("/{query_id}/executions", response_model=List[QueryExecuteResponse])
async def get_query_executions(
    query_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all executions for a query."""
    execution_service = QueryExecutionService(db)
    executions = execution_service.get_executions_by_query(query_id)
    
    results = []
    for execution in executions:
        results.append(QueryExecuteResponse(
            execution_id=execution.id,
            query_id=execution.query_id,
            status=execution.status,
            rows_returned=execution.rows_returned or 0,
            execution_time_ms=execution.execution_time_ms or 0,
            data=execution.result_data or [],
            columns=execution.columns or [],
            from_cache=execution.from_cache,
            cached_at=execution.executed_at if execution.from_cache else None,
            executed_at=execution.executed_at
        ))
    
    return results
