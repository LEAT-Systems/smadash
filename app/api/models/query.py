"""Query generation and execution models."""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


class QueryStatus(str, Enum):
    """Query execution status."""
    PENDING = "pending"
    GENERATING = "generating"
    GENERATED = "generated"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    CACHED = "cached"


class QueryType(str, Enum):
    """Type of query."""
    SELECT = "select"
    AGGREGATE = "aggregate"
    JOIN = "join"
    ANALYSIS = "analysis"


class QueryGenerateRequest(BaseModel):
    """Request model for generating query from natural language."""
    datasource_id: str
    natural_language_query: str = Field(..., min_length=3, max_length=1000)
    canvas_id: Optional[str] = None
    dashboard_id: Optional[str] = None
    component_id: Optional[str] = None
    user_id: str
    organization_id: str
    additional_context: Optional[Dict[str, Any]] = None


class QueryGenerateResponse(BaseModel):
    """Response model for generated query."""
    query_id: str
    natural_language_query: str
    generated_sql: str
    query_type: QueryType
    tables_used: List[str]
    estimated_rows: Optional[int] = None
    explanation: Optional[str] = None
    confidence_score: Optional[float] = None
    warnings: Optional[List[str]] = None
    created_at: datetime


class QueryExecuteRequest(BaseModel):
    """Request model for executing a query."""
    query_id: Optional[str] = None
    datasource_id: str
    sql_query: str
    user_id: str
    organization_id: str
    canvas_id: Optional[str] = None
    dashboard_id: Optional[str] = None
    component_id: Optional[str] = None
    use_cache: Optional[bool] = True
    cache_ttl_seconds: Optional[int] = 300


class QueryExecuteResponse(BaseModel):
    """Response model for query execution."""
    execution_id: str
    query_id: Optional[str]
    status: QueryStatus
    rows_returned: int
    execution_time_ms: float
    data: List[Dict[str, Any]]
    columns: List[Dict[str, str]]
    from_cache: bool
    cached_at: Optional[datetime] = None
    executed_at: datetime


class QueryHistoryResponse(BaseModel):
    """Response model for query history."""
    id: str
    natural_language_query: str
    generated_sql: str
    query_type: QueryType
    datasource_id: str
    datasource_name: str
    status: QueryStatus
    rows_returned: Optional[int]
    execution_time_ms: Optional[float]
    canvas_id: Optional[str]
    dashboard_id: Optional[str]
    component_id: Optional[str]
    created_at: datetime
    executed_at: Optional[datetime]


class QueryValidateRequest(BaseModel):
    """Request model for validating a query."""
    datasource_id: str
    sql_query: str
    
    
class QueryValidateResponse(BaseModel):
    """Response model for query validation."""
    valid: bool
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None
    tables_referenced: List[str]
    estimated_cost: Optional[str] = None
