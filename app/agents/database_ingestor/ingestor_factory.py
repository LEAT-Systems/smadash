from app.agents.database_ingestor.interfaces import  DatabaseIngestorInterface
from app.agents.utils.database_connection_schema import DatabaseType
from app.agents.database_ingestor.sql_alchemy.ingestor import SQLAlchemyIngestor
from app.agents.database_ingestor.mongo_client.ingestor import MongoDBIngestor


class DatabaseIngestorFactory:
    """Factory class for creating database ingestors."""

    @staticmethod
    def create_ingestor(db_type: DatabaseType) -> DatabaseIngestorInterface:
        """Create appropriate ingestor based on database type."""

        # For SQL databases, use SQLAlchemy ingestor
        if db_type in [
            DatabaseType.MYSQL,
            DatabaseType.POSTGRESQL,
            DatabaseType.SQLITE,
            DatabaseType.ORACLE,
            DatabaseType.SQLSERVER
        ]:
            return SQLAlchemyIngestor()

        # For NoSQL databases, you would create specific ingestors
        elif db_type == DatabaseType.MONGODB:
            return MongoDBIngestor()  # To be implemented

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    @staticmethod
    def get_supported_databases() -> list[DatabaseType]:
        """Get list of supported database types."""
        return [
            DatabaseType.MYSQL,
            DatabaseType.POSTGRESQL,
            DatabaseType.SQLITE,
            DatabaseType.ORACLE,
            DatabaseType.SQLSERVER
        ]