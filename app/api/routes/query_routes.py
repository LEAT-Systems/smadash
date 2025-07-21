from fastapi import APIRouter, Depends, HTTPException
from ..models import QueryRequest, QueryResponse
from app.api.services.auth_service import get_current_user
from app.api.db.storage import database_connections_db
from app.api.services.query_generator_service import generate_sql
from sqlalchemy import create_engine

router = APIRouter()

@router.post("/query")
def query_database(query_req: QueryRequest, current_user: dict = Depends(get_current_user)):
    user_connections = [conn for conn in database_connections_db.values() if conn["user_id"] == current_user["email"]]
    if not user_connections:
        raise HTTPException(status_code=404, detail="No database connected")
    db_connection = user_connections[0]
    schema = db_connection["schema"]
    sql_query = generate_sql(schema, query_req.query)
    engine = create_engine(db_connection["connection_string"])
    with engine.connect() as connection:
        try:
            result = connection.execute(sql_query)
            rows = result.fetchall()
            columns = result.keys()
            results = [dict(zip(columns, row)) for row in rows]
            suggested_visualization = "table" if len(results) > 1 else "text" if len(results) == 1 else None
            return QueryResponse(results=results, suggested_visualization=suggested_visualization, success=True, message=None)
        except Exception as e:
            return QueryResponse(results=[], suggested_visualization=None, success=False, message=str(e))