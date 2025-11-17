"""Data source service for managing database connections."""
import time
import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from cryptography.fernet import Fernet
import os

from app.api.db.models import DataSource, DataSourceStatusEnum, DatabaseTypeEnum
from app.api.models.datasource import (
    DataSourceCreate, 
    DataSourceUpdate, 
    DataSourceConnectionTest,
    ConnectionTestResponse
)
from app.agents.database_ingestor.ingestor_factory import DatabaseIngestorFactory
from app.agents.utils.database_connection_schema import ConnectionConfig, DatabaseType


class DataSourceService:
    """Service for managing data sources."""
    
    def __init__(self, db: Session):
        self.db = db
        # Get encryption key from environment or generate one
        self.encryption_key = os.getenv('DATASOURCE_ENCRYPTION_KEY', Fernet.generate_key())
        if isinstance(self.encryption_key, str):
            self.encryption_key = self.encryption_key.encode()
        self.cipher = Fernet(self.encryption_key)
    
    def _encrypt_password(self, password: str) -> str:
        """Encrypt password for storage."""
        if not password:
            return None
        return self.cipher.encrypt(password.encode()).decode()
    
    def _decrypt_password(self, encrypted_password: str) -> str:
        """Decrypt password from storage."""
        if not encrypted_password:
            return None
        return self.cipher.decrypt(encrypted_password.encode()).decode()
    
    def _convert_db_type_to_enum(self, db_type: str) -> DatabaseType:
        """Convert string db_type to DatabaseType enum."""
        mapping = {
            'mysql': DatabaseType.MYSQL,
            'postgresql': DatabaseType.POSTGRESQL,
            'sqlite': DatabaseType.SQLITE,
            'oracle': DatabaseType.ORACLE,
            'sqlserver': DatabaseType.SQLSERVER,
            'mongodb': DatabaseType.MONGODB,
        }
        return mapping.get(db_type.lower(), DatabaseType.POSTGRESQL)
    
    def test_connection(self, test_request: DataSourceConnectionTest) -> ConnectionTestResponse:
        """Test database connection."""
        start_time = time.time()
        
        try:
            # Create connection config
            config = ConnectionConfig(
                db_type=self._convert_db_type_to_enum(test_request.db_type.value),
                host=test_request.host,
                port=test_request.port,
                database=test_request.database,
                username=test_request.username,
                password=test_request.password,
                additional_params=test_request.additional_params or {}
            )
            
            # Get appropriate ingestor
            ingestor = DatabaseIngestorFactory.create_ingestor(config.db_type)
            
            # Test connection
            success = ingestor.test_connection(config)
            connection_time_ms = (time.time() - start_time) * 1000
            
            if success:
                return ConnectionTestResponse(
                    success=True,
                    message="Connection successful",
                    db_type=test_request.db_type,
                    connection_time_ms=connection_time_ms
                )
            else:
                return ConnectionTestResponse(
                    success=False,
                    message="Connection failed",
                    db_type=test_request.db_type,
                    connection_time_ms=connection_time_ms,
                    error_details="Unable to establish connection to database"
                )
        
        except Exception as e:
            connection_time_ms = (time.time() - start_time) * 1000
            return ConnectionTestResponse(
                success=False,
                message="Connection test failed",
                db_type=test_request.db_type,
                connection_time_ms=connection_time_ms,
                error_details=str(e)
            )
    
    def create_datasource(self, datasource: DataSourceCreate) -> DataSource:
        """Create a new data source."""
        # First test the connection
        test_request = DataSourceConnectionTest(
            db_type=datasource.db_type,
            host=datasource.host,
            port=datasource.port,
            database=datasource.database,
            username=datasource.username,
            password=datasource.password,
            ssl_enabled=datasource.ssl_enabled,
            additional_params=datasource.additional_params
        )
        
        test_result = self.test_connection(test_request)
        if not test_result.success:
            raise ValueError(f"Connection test failed: {test_result.error_details}")
        
        # Create data source record
        db_datasource = DataSource(
            id=str(uuid.uuid4()),
            name=datasource.name,
            description=datasource.description,
            db_type=DatabaseTypeEnum[datasource.db_type.value.upper()],
            host=datasource.host,
            port=datasource.port,
            database=datasource.database,
            username=datasource.username,
            password_encrypted=self._encrypt_password(datasource.password),
            ssl_enabled=datasource.ssl_enabled,
            additional_params=datasource.additional_params,
            organization_id=datasource.organization_id,
            status=DataSourceStatusEnum.ACTIVE,
            schema_ingested=False
        )
        
        self.db.add(db_datasource)
        self.db.commit()
        self.db.refresh(db_datasource)
        
        return db_datasource
    
    def get_datasource(self, datasource_id: str) -> Optional[DataSource]:
        """Get data source by ID."""
        return self.db.query(DataSource).filter(DataSource.id == datasource_id).first()
    
    def get_datasources_by_organization(self, organization_id: str) -> List[DataSource]:
        """Get all data sources for an organization."""
        return self.db.query(DataSource).filter(
            DataSource.organization_id == organization_id
        ).order_by(DataSource.created_at.desc()).all()
    
    def update_datasource(self, datasource_id: str, update_data: DataSourceUpdate) -> Optional[DataSource]:
        """Update a data source."""
        datasource = self.get_datasource(datasource_id)
        if not datasource:
            return None
        
        # Update fields
        update_dict = update_data.dict(exclude_unset=True)
        
        # Handle password encryption
        if 'password' in update_dict:
            update_dict['password_encrypted'] = self._encrypt_password(update_dict.pop('password'))
        
        # Handle status enum
        if 'status' in update_dict:
            update_dict['status'] = DataSourceStatusEnum[update_dict['status'].upper()]
        
        for key, value in update_dict.items():
            setattr(datasource, key, value)
        
        datasource.updated_at = datetime.utcnow()
        self.db.commit()
        self.db.refresh(datasource)
        
        return datasource
    
    def delete_datasource(self, datasource_id: str) -> bool:
        """Delete a data source."""
        datasource = self.get_datasource(datasource_id)
        if not datasource:
            return False
        
        self.db.delete(datasource)
        self.db.commit()
        return True
    
    def get_connection_config(self, datasource_id: str) -> Optional[ConnectionConfig]:
        """Get connection config for a data source."""
        datasource = self.get_datasource(datasource_id)
        if not datasource:
            return None
        
        return ConnectionConfig(
            db_type=self._convert_db_type_to_enum(datasource.db_type.value),
            host=datasource.host,
            port=datasource.port,
            database=datasource.database,
            username=datasource.username,
            password=self._decrypt_password(datasource.password_encrypted),
            additional_params=datasource.additional_params or {}
        )
    
    def mark_schema_ingested(self, datasource_id: str, success: bool = True, error: str = None):
        """Mark schema as ingested for a data source."""
        datasource = self.get_datasource(datasource_id)
        if datasource:
            datasource.schema_ingested = success
            datasource.schema_ingested_at = datetime.utcnow() if success else None
            datasource.schema_ingestion_error = error
            self.db.commit()
