from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from app.agents.utils import ConnectionConfig


class DatabaseIngestionPipelineInterface(ABC):

    @abstractmethod
    def create_ingestion_plan(self, source_config: ConnectionConfig,
                              target_config: ConnectionConfig,
                              table_filters: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Create an execution plan for database ingestion.

        Args:
            source_config: Source database configuration
            target_config: Target database configuration
            table_filters: Optional list of table names to include

        Returns:
            Dict[str, Any]: Ingestion execution plan
        """
        pass

    @abstractmethod
    def execute_ingestion(self, plan: Dict[str, Any],
                          progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """
        Execute the database ingestion according to the plan.

        Args:
            plan: Ingestion execution plan
            progress_callback: Optional callback for progress updates

        Returns:
            Dict[str, Any]: Ingestion results and statistics
        """
        pass

    @abstractmethod
    def rollback_ingestion(self, execution_id: str) -> bool:
        """
        Rollback a failed or incomplete ingestion.

        Args:
            execution_id: Identifier of the ingestion to rollback

        Returns:
            bool: True if rollback successful, False otherwise
        """
        pass

    @abstractmethod
    def get_ingestion_status(self, execution_id: str) -> Dict[str, Any]:
        """
        Get the status of an ongoing or completed ingestion.

        Args:
            execution_id: Identifier of the ingestion

        Returns:
            Dict[str, Any]: Status information including progress and errors
        """
        pass