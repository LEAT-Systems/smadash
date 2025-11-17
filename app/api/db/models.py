"""SQLAlchemy database models for ingestor service."""
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, JSON, Float, Enum as SQLEnum, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
import enum

Base = declarative_base()


class DatabaseTypeEnum(str, enum.Enum):
    """Database types."""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    MONGODB = "mongodb"


class DataSourceStatusEnum(str, enum.Enum):
    """Data source status."""
    PENDING = "pending"
    ACTIVE = "active"
    ERROR = "error"
    DISABLED = "disabled"


class QueryStatusEnum(str, enum.Enum):
    """Query status."""
    PENDING = "pending"
    GENERATING = "generating"
    GENERATED = "generated"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    CACHED = "cached"


class DataSource(Base):
    """Data source model for storing database connections."""
    __tablename__ = "datasources"

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    db_type = Column(SQLEnum(DatabaseTypeEnum), nullable=False)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    database = Column(String(255), nullable=False)
    username = Column(String(255), nullable=True)
    password_encrypted = Column(Text, nullable=True)  # Encrypted password
    ssl_enabled = Column(Boolean, default=False)
    additional_params = Column(JSON, nullable=True)
    
    # Metadata
    status = Column(SQLEnum(DataSourceStatusEnum), default=DataSourceStatusEnum.PENDING)
    organization_id = Column(String(36), nullable=False, index=True)
    
    # Schema ingestion tracking
    schema_ingested = Column(Boolean, default=False)
    schema_ingested_at = Column(DateTime, nullable=True)
    schema_ingestion_error = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    schemas = relationship("DataSourceSchema", back_populates="datasource", cascade="all, delete-orphan")
    queries = relationship("GeneratedQuery", back_populates="datasource")
    executions = relationship("QueryExecution", back_populates="datasource")

    __table_args__ = (
        Index('idx_datasource_org', 'organization_id'),
        Index('idx_datasource_status', 'status'),
    )


class DataSourceSchema(Base):
    """Schema information for data sources."""
    __tablename__ = "datasource_schemas"

    id = Column(String(36), primary_key=True)
    datasource_id = Column(String(36), ForeignKey('datasources.id', ondelete='CASCADE'), nullable=False)
    
    # Table information
    table_name = Column(String(255), nullable=False)
    schema_name = Column(String(255), nullable=True)
    row_count = Column(Integer, default=0)
    
    # Schema details (stored as JSON)
    columns = Column(JSON, nullable=False)  # List of column metadata
    primary_keys = Column(JSON, nullable=True)  # List of primary key columns
    foreign_keys = Column(JSON, nullable=True)  # List of foreign key constraints
    indexes = Column(JSON, nullable=True)  # List of indexes
    
    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    datasource = relationship("DataSource", back_populates="schemas")

    __table_args__ = (
        Index('idx_schema_datasource', 'datasource_id'),
        Index('idx_schema_table', 'datasource_id', 'table_name'),
    )


class GeneratedQuery(Base):
    """Generated queries from natural language."""
    __tablename__ = "generated_queries"

    id = Column(String(36), primary_key=True)
    datasource_id = Column(String(36), ForeignKey('datasources.id'), nullable=False)
    
    # Query information
    natural_language_query = Column(Text, nullable=False)
    generated_sql = Column(Text, nullable=False)
    query_type = Column(String(50), nullable=True)  # select, aggregate, join, analysis
    
    # Tables and analysis
    tables_used = Column(JSON, nullable=True)  # List of tables referenced
    estimated_rows = Column(Integer, nullable=True)
    confidence_score = Column(Float, nullable=True)
    explanation = Column(Text, nullable=True)
    warnings = Column(JSON, nullable=True)
    
    # Context
    user_id = Column(String(36), nullable=False, index=True)
    organization_id = Column(String(36), nullable=False, index=True)
    canvas_id = Column(String(36), nullable=True, index=True)
    dashboard_id = Column(String(36), nullable=True, index=True)
    component_id = Column(String(36), nullable=True, index=True)
    
    # LLM metadata
    llm_model = Column(String(100), nullable=True)
    llm_tokens_used = Column(Integer, nullable=True)
    llm_response_time_ms = Column(Float, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    datasource = relationship("DataSource", back_populates="queries")
    executions = relationship("QueryExecution", back_populates="query")

    __table_args__ = (
        Index('idx_query_user', 'user_id'),
        Index('idx_query_org', 'organization_id'),
        Index('idx_query_canvas', 'canvas_id'),
        Index('idx_query_dashboard', 'dashboard_id'),
        Index('idx_query_component', 'component_id'),
    )


class QueryExecution(Base):
    """Query execution history and results."""
    __tablename__ = "query_executions"

    id = Column(String(36), primary_key=True)
    query_id = Column(String(36), ForeignKey('generated_queries.id'), nullable=True)
    datasource_id = Column(String(36), ForeignKey('datasources.id'), nullable=False)
    
    # Execution details
    sql_query = Column(Text, nullable=False)
    status = Column(SQLEnum(QueryStatusEnum), default=QueryStatusEnum.PENDING)
    
    # Results
    rows_returned = Column(Integer, nullable=True)
    execution_time_ms = Column(Float, nullable=True)
    result_data = Column(JSON, nullable=True)  # Cached results (limited)
    columns = Column(JSON, nullable=True)  # Column metadata
    
    # Caching
    from_cache = Column(Boolean, default=False)
    cache_key = Column(String(64), nullable=True, index=True)
    cache_expires_at = Column(DateTime, nullable=True)
    
    # Error handling
    error_message = Column(Text, nullable=True)
    error_code = Column(String(50), nullable=True)
    
    # Context
    user_id = Column(String(36), nullable=False, index=True)
    organization_id = Column(String(36), nullable=False, index=True)
    canvas_id = Column(String(36), nullable=True)
    dashboard_id = Column(String(36), nullable=True)
    component_id = Column(String(36), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    executed_at = Column(DateTime, nullable=True)
    
    # Relationships
    query = relationship("GeneratedQuery", back_populates="executions")
    datasource = relationship("DataSource", back_populates="executions")

    __table_args__ = (
        Index('idx_execution_query', 'query_id'),
        Index('idx_execution_datasource', 'datasource_id'),
        Index('idx_execution_user', 'user_id'),
        Index('idx_execution_org', 'organization_id'),
        Index('idx_execution_cache', 'cache_key', 'cache_expires_at'),
        Index('idx_execution_status', 'status'),
    )
