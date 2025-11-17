"""API routes for data source management."""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List
import logging

from app.api.db.database import get_db
from app.api.models.datasource import (
    DataSourceConnectionTest,
    DataSourceCreate,
    DataSourceUpdate,
    DataSourceResponse,
    DataSourceSchemaResponse,
    ConnectionTestResponse,
    TableSchemaResponse
)
from app.api.services.datasource_service import DataSourceService
from app.api.services.schema_ingestion_service import SchemaIngestionService
from app.api.utils.dependencies import get_current_user


router = APIRouter(prefix="/datasources", tags=["datasources"])
logger = logging.getLogger(__name__)


@router.post("/test-connection", response_model=ConnectionTestResponse)
async def test_datasource_connection(
    test_request: DataSourceConnectionTest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Test database connection without saving."""
    service = DataSourceService(db)
    return service.test_connection(test_request)


@router.post("", response_model=DataSourceResponse, status_code=201)
async def create_datasource(
    datasource: DataSourceCreate,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Create a new data source.
    
    This endpoint:
    1. Validates the connection
    2. Encrypts and saves credentials
    3. Triggers background schema ingestion (if auto_ingest_schema=True)
    """
    try:
        service = DataSourceService(db)
        
        # Create datasource
        db_datasource = service.create_datasource(datasource)
        
        # Trigger background schema ingestion if requested
        if datasource.auto_ingest_schema:
            background_tasks.add_task(
                ingest_schema_background,
                datasource_id=db_datasource.id,
                organization_id=datasource.organization_id
            )
        
        return db_datasource
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create datasource: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create datasource")


@router.get("", response_model=List[DataSourceResponse])
async def get_datasources(
    organization_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all data sources for an organization."""
    service = DataSourceService(db)
    return service.get_datasources_by_organization(organization_id)


@router.get("/{datasource_id}", response_model=DataSourceResponse)
async def get_datasource(
    datasource_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get a specific data source."""
    service = DataSourceService(db)
    datasource = service.get_datasource(datasource_id)
    
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    
    return datasource


@router.put("/{datasource_id}", response_model=DataSourceResponse)
async def update_datasource(
    datasource_id: str,
    update_data: DataSourceUpdate,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update a data source."""
    service = DataSourceService(db)
    datasource = service.update_datasource(datasource_id, update_data)
    
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    
    return datasource


@router.delete("/{datasource_id}", status_code=204)
async def delete_datasource(
    datasource_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete a data source."""
    service = DataSourceService(db)
    success = service.delete_datasource(datasource_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="Datasource not found")


@router.post("/{datasource_id}/ingest-schema", status_code=202)
async def trigger_schema_ingestion(
    datasource_id: str,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Trigger schema ingestion for a data source."""
    service = DataSourceService(db)
    datasource = service.get_datasource(datasource_id)
    
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    
    # Trigger background ingestion
    background_tasks.add_task(
        ingest_schema_background,
        datasource_id=datasource_id,
        organization_id=datasource.organization_id
    )
    
    return {"message": "Schema ingestion started", "datasource_id": datasource_id}


@router.get("/{datasource_id}/schema", response_model=List[TableSchemaResponse])
async def get_datasource_schema(
    datasource_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get schema for a data source."""
    # Verify datasource exists
    datasource_service = DataSourceService(db)
    datasource = datasource_service.get_datasource(datasource_id)
    
    if not datasource:
        raise HTTPException(status_code=404, detail="Datasource not found")
    
    # Get schema
    schema_service = SchemaIngestionService(db)
    schemas = schema_service.get_schema(datasource_id)
    
    # Convert to response format
    return [
        TableSchemaResponse(
            name=schema.table_name,
            schema=schema.schema_name,
            row_count=schema.row_count,
            columns=schema.columns,
            primary_keys=schema.primary_keys or [],
            foreign_keys=schema.foreign_keys or [],
            indexes=schema.indexes or []
        )
        for schema in schemas
    ]


def ingest_schema_background(datasource_id: str, organization_id: str):
    """Background task for schema ingestion."""
    from app.api.db.database import SessionLocal
    
    db = SessionLocal()
    try:
        logger.info(f"Starting background schema ingestion for datasource: {datasource_id}")
        
        # Get connection config
        datasource_service = DataSourceService(db)
        connection_config = datasource_service.get_connection_config(datasource_id)
        
        if not connection_config:
            logger.error(f"Could not get connection config for datasource: {datasource_id}")
            return
        
        # Ingest schema
        schema_service = SchemaIngestionService(db)
        result = schema_service.ingest_schema(datasource_id, connection_config)
        
        # Update datasource status
        datasource_service.mark_schema_ingested(
            datasource_id,
            success=result['success'],
            error=result.get('error')
        )
        
        if result['success']:
            logger.info(f"Schema ingestion completed: {result['tables_ingested']} tables")
        else:
            logger.error(f"Schema ingestion failed: {result.get('error')}")
    
    except Exception as e:
        logger.error(f"Background schema ingestion failed: {str(e)}")
        # Mark as failed
        datasource_service = DataSourceService(db)
        datasource_service.mark_schema_ingested(datasource_id, success=False, error=str(e))
    
    finally:
        db.close()
