from pydantic import BaseModel
from typing import List, Optional

class DatabaseConnectionCreate(BaseModel):
    db_type: str
    connection_string: str

class DatabaseConnection(BaseModel):
    id: str
    user_id: str
    db_type: str
    connection_string: str
    schema: Optional[dict] = None

class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    results: List[dict]
    suggested_visualization: Optional[str] = None
    success: bool
    message: Optional[str]