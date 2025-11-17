"""Data source models for database connections."""
from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


class DatabaseType(str, Enum):
    """Supported database types."""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    MONGODB = "mongodb"


class DataSourceStatus(str, Enum):
    """Data source status."""
    PENDING = "pending"
    ACTIVE = "active"
    ERROR = "error"
    DISABLED = "disabled"


class DataSourceConnectionTest(BaseModel):
    """Request model for testing database connection."""
    db_type: DatabaseType
    host: str
    port: Optional[int] = None
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    ssl_enabled: Optional[bool] = False
    additional_params: Optional[Dict[str, Any]] = None

    @validator('port', pre=True, always=True)
    def set_default_port(cls, v, values):
        """Set default port based on database type."""
        if v is not None:
            return v
        
        db_type = values.get('db_type')
        default_ports = {
            DatabaseType.MYSQL: 3306,
            DatabaseType.POSTGRESQL: 5432,
            DatabaseType.MONGODB: 27017,
            DatabaseType.ORACLE: 1521,
            DatabaseType.SQLSERVER: 1433,
        }
        return default_ports.get(db_type)


class DataSourceCreate(BaseModel):
    """Request model for creating a new data source."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    db_type: DatabaseType
    host: str
    port: Optional[int] = None
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    ssl_enabled: Optional[bool] = False
    additional_params: Optional[Dict[str, Any]] = None
    organization_id: str
    auto_ingest_schema: Optional[bool] = True

    @validator('port', pre=True, always=True)
    def set_default_port(cls, v, values):
        """Set default port based on database type."""
        if v is not None:
            return v
        
        db_type = values.get('db_type')
        default_ports = {
            DatabaseType.MYSQL: 3306,
            DatabaseType.POSTGRESQL: 5432,
            DatabaseType.MONGODB: 27017,
            DatabaseType.ORACLE: 1521,
            DatabaseType.SQLSERVER: 1433,
        }
        return default_ports.get(db_type)


class DataSourceUpdate(BaseModel):
    """Request model for updating a data source."""
    name: Optional[str] = None
    description: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    ssl_enabled: Optional[bool] = None
    additional_params: Optional[Dict[str, Any]] = None
    status: Optional[DataSourceStatus] = None


class DataSourceResponse(BaseModel):
    """Response model for data source."""
    id: str
    name: str
    description: Optional[str]
    db_type: DatabaseType
    host: str
    port: int
    database: str
    username: Optional[str]
    ssl_enabled: bool
    status: DataSourceStatus
    organization_id: str
    schema_ingested: bool
    schema_ingested_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class TableSchemaResponse(BaseModel):
    """Response model for table schema."""
    name: str
    schema: Optional[str]
    row_count: int
    columns: List[Dict[str, Any]]
    primary_keys: List[str]
    foreign_keys: List[Dict[str, Any]]
    indexes: List[Dict[str, Any]]


class DataSourceSchemaResponse(BaseModel):
    """Response model for complete data source schema."""
    datasource_id: str
    tables: List[TableSchemaResponse]
    relationships: List[Dict[str, Any]]
    ingested_at: datetime


class ConnectionTestResponse(BaseModel):
    """Response model for connection test."""
    success: bool
    message: str
    db_type: DatabaseType
    connection_time_ms: Optional[float] = None
    error_details: Optional[str] = None
