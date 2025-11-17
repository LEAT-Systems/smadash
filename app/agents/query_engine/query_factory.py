"""Factory classes for creating query generators and executors based on database type."""
import logging
from typing import Optional

from app.agents.utils.database_connection_schema import DatabaseType
from app.agents.query_engine.interfaces import QueryGeneratorInterface, QueryExecutorInterface
from app.agents.query_engine.generators import SQLQueryGenerator, MongoDBQueryGenerator
from app.agents.query_engine.executors import SQLQueryExecutor, MongoDBQueryExecutor


logger = logging.getLogger(__name__)


class QueryGeneratorFactory:
    """
    Factory for creating appropriate query generators based on database type.
    
    Supported databases:
    - SQL: PostgreSQL, MySQL, SQLite, Oracle, SQL Server (via SQLAlchemy)
    - NoSQL: MongoDB
    """
    
    @staticmethod
    def create_generator(
        db_type: DatabaseType,
        dialect: Optional[str] = None,
        model: str = "gpt-4o-mini"
    ) -> QueryGeneratorInterface:
        """
        Create appropriate query generator based on database type.
        
        Args:
            db_type: Type of database (from DatabaseType enum)
            dialect: SQL dialect for SQL databases (postgresql, mysql, sqlite, oracle, mssql)
            model: LLM model to use for query generation
            
        Returns:
            QueryGeneratorInterface implementation
            
        Raises:
            ValueError: If database type is not supported
        """
        # SQL databases - use SQLAlchemy-based generator
        if db_type in [
            DatabaseType.MYSQL,
            DatabaseType.POSTGRESQL,
            DatabaseType.SQLITE,
            DatabaseType.ORACLE,
            DatabaseType.SQLSERVER
        ]:
            # Map DatabaseType to SQL dialect
            dialect_map = {
                DatabaseType.POSTGRESQL: 'postgresql',
                DatabaseType.MYSQL: 'mysql',
                DatabaseType.SQLITE: 'sqlite',
                DatabaseType.ORACLE: 'oracle',
                DatabaseType.SQLSERVER: 'mssql'
            }
            
            sql_dialect = dialect or dialect_map.get(db_type, 'postgresql')
            logger.info(f"Creating SQL query generator for {sql_dialect}")
            return SQLQueryGenerator(dialect=sql_dialect, model=model)
        
        # NoSQL databases
        elif db_type == DatabaseType.MONGODB:
            logger.info("Creating MongoDB query generator")
            return MongoDBQueryGenerator(model=model)
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    @staticmethod
    def get_supported_databases() -> list:
        """
        Get list of supported database types.
        
        Returns:
            List of DatabaseType enums
        """
        return [
            DatabaseType.POSTGRESQL,
            DatabaseType.MYSQL,
            DatabaseType.SQLITE,
            DatabaseType.ORACLE,
            DatabaseType.SQLSERVER,
            DatabaseType.MONGODB
        ]


class QueryExecutorFactory:
    """
    Factory for creating appropriate query executors based on database type.
    
    Supported databases:
    - SQL: PostgreSQL, MySQL, SQLite, Oracle, SQL Server (via SQLAlchemy)
    - NoSQL: MongoDB
    """
    
    @staticmethod
    def create_executor(db_type: DatabaseType) -> QueryExecutorInterface:
        """
        Create appropriate query executor based on database type.
        
        Args:
            db_type: Type of database (from DatabaseType enum)
            
        Returns:
            QueryExecutorInterface implementation
            
        Raises:
            ValueError: If database type is not supported
        """
        # SQL databases - use SQLAlchemy-based executor
        if db_type in [
            DatabaseType.MYSQL,
            DatabaseType.POSTGRESQL,
            DatabaseType.SQLITE,
            DatabaseType.ORACLE,
            DatabaseType.SQLSERVER
        ]:
            logger.info(f"Creating SQL query executor for {db_type.value}")
            return SQLQueryExecutor()
        
        # NoSQL databases
        elif db_type == DatabaseType.MONGODB:
            logger.info("Creating MongoDB query executor")
            return MongoDBQueryExecutor()
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    @staticmethod
    def get_supported_databases() -> list:
        """
        Get list of supported database types.
        
        Returns:
            List of DatabaseType enums
        """
        return [
            DatabaseType.POSTGRESQL,
            DatabaseType.MYSQL,
            DatabaseType.SQLITE,
            DatabaseType.ORACLE,
            DatabaseType.SQLSERVER,
            DatabaseType.MONGODB
        ]


class QueryEngineFactory:
    """
    Unified factory for creating both generator and executor for a database type.
    
    This is the recommended way to get query engine components.
    """
    
    @staticmethod
    def create_query_engine(
        db_type: DatabaseType,
        dialect: Optional[str] = None,
        model: str = "gpt-4o-mini"
    ) -> tuple[QueryGeneratorInterface, QueryExecutorInterface]:
        """
        Create both query generator and executor for a database type.
        
        Args:
            db_type: Type of database
            dialect: SQL dialect (for SQL databases)
            model: LLM model for query generation
            
        Returns:
            Tuple of (generator, executor)
            
        Example:
            >>> from app.agents.utils.database_connection_schema import DatabaseType
            >>> generator, executor = QueryEngineFactory.create_query_engine(
            ...     DatabaseType.POSTGRESQL
            ... )
            >>> # Generate query
            >>> result = generator.generate_query("Show top 10 customers", schema)
            >>> # Execute query
            >>> data = executor.execute_query(result.query, connection_config)
        """
        generator = QueryGeneratorFactory.create_generator(db_type, dialect, model)
        executor = QueryExecutorFactory.create_executor(db_type)
        
        logger.info(f"Created query engine for {db_type.value}")
        return generator, executor
    
    @staticmethod
    def get_supported_databases() -> list:
        """Get list of all supported database types."""
        return QueryGeneratorFactory.get_supported_databases()
