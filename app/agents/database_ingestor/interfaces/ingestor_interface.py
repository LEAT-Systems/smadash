from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from app.agents.schemas import ConnectionConfig, TableMetadata, NormalizationRule


class DatabaseIngestorInterface(ABC):

    @abstractmethod
    def connect(self, config: ConnectionConfig) -> bool:
        """
        Establish connection to the source database.
        
        Args:
            config: Database connection configuration
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    def test_connection(self, config: ConnectionConfig) -> bool:
        """
        Test database connection without establishing persistent connection.
        
        Args:
            config: Database connection configuration
            
        Returns:
            bool: True if connection test successful, False otherwise
        """
        pass

    @abstractmethod
    def discover_schema(self) -> List[TableMetadata]:
        """
        Discover and extract metadata from all tables in the database.
        
        Returns:
            List[TableMetadata]: List of table metadata objects
        """
        pass

    @abstractmethod
    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        """
        Get metadata for a specific table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (if applicable)
            
        Returns:
            TableMetadata: Table metadata object
        """
        pass

    @abstractmethod
    def extract_data(self, table_name: str, schema: Optional[str] = None,
                     batch_size: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Extract data from a specific table.
        
        Args:
            table_name: Name of the table to extract from
            schema: Schema name (if applicable)
            batch_size: Number of rows to extract per batch
            offset: Starting row offset
            
        Returns:
            List[Dict[str, Any]]: List of row dictionaries
        """
        pass

    @abstractmethod
    def extract_data_streaming(self, table_name: str, schema: Optional[str] = None,
                               batch_size: int = 1000):
        """
        Extract data from a table using streaming/generator approach.
        
        Args:
            table_name: Name of the table to extract from
            schema: Schema name (if applicable)
            batch_size: Number of rows per batch
            
        Yields:
            Dict[str, Any]: Individual row data
        """
        pass

    @abstractmethod
    def validate_data_integrity(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate data integrity for a table (constraints, null values, data types).
        
        Args:
            table_name: Name of the table to validate
            schema: Schema name (if applicable)
            
        Returns:
            Dict[str, Any]: Validation results including errors and warnings
        """
        pass

