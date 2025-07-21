from fastapi import APIRouter, Depends, HTTPException
from ..models import DatabaseConnectionCreate
from ..auth.dependencies import get_current_user
from ..database import pull_schema
from ..storage import database_connections_db
import uuid

router = APIRouter()

@router.post("/connect-database")
def connect_database(db_connection: DatabaseConnectionCreate, current_user: dict = Depends(get_current_user)):
    connection_id = str(uuid.uuid4())
    db_connection_dict = db_connection.dict()
    db_connection_dict["user_id"] = current_user["email"]
    db_connection_dict["id"] = connection_id
    schema = pull_schema(db_connection.db_type, db_connection.connection_string)
    db_connection_dict["schema"] = schema
    database_connections_db[connection_id] = db_connection_dict
    return {"message": "Database connected successfully", "connection_id": connection_id}