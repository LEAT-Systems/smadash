from sqlalchemy import create_engine, MetaData, Table, inspect, text, select, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List, Optional, Any, Generator
import logging
from datetime import datetime
from app.agents.database_ingestor.interfaces import DatabaseIngestorInterface, ConnectionConfig, TableMetadata
from app.agents.schemas.database_connection_schema import DatabaseType

class SQLAlchemyIngestor(DatabaseIngestorInterface):
    def __init__(self):
        self.engine: Optional[Engine] = None
        self.session: Optional[Session] = None
        self.metadata: Optional[MetaData] = None
        self.logger = logging.getLogger(__name__)
        self.SessionLocal = None

    def connect(self, config: ConnectionConfig) -> bool:
        try:
            # Build connection URL based on database type
            connection_url = self._build_connection_url(config)

            # Create engine with connection pooling
            self.engine = create_engine(
                connection_url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                echo=False
            )

            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )

            # Create session
            self.session = self.SessionLocal()

            # Create metadata instance
            self.metadata = MetaData()

            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            self.logger.info(f"Connected to {config.db_type.value} database: {config.database}")
            return True

        except SQLAlchemyError as e:
            self.logger.error(f"Failed to connect to {config.db_type.value}: {e}")
            return False

    def disconnect(self) -> None:
        if self.session:
            self.session.close()
            self.session = None

        if self.engine:
            self.engine.dispose()
            self.engine = None

        self.metadata = None
        self.SessionLocal = None
        self.logger.info("Disconnected from database")

    def test_connection(self, config: ConnectionConfig) -> bool:
        try:
            connection_url = self._build_connection_url(config)
            test_engine = create_engine(connection_url)

            with test_engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            test_engine.dispose()
            return True

        except SQLAlchemyError:
            return False

    def discover_schema(self) -> List[TableMetadata]:
        if not self.engine:
            raise RuntimeError("Not connected to database")

        tables = []
        inspector = inspect(self.engine)

        # Get all table names
        table_names = inspector.get_table_names()

        # Include views if supported
        try:
            view_names = inspector.get_view_names()
            table_names.extend(view_names)
        except NotImplementedError:
            # Some databases don't support views inspection
            pass

        for table_name in table_names:
            try:
                metadata = self.get_table_metadata(table_name)
                tables.append(metadata)
            except Exception as e:
                self.logger.warning(f"Failed to get metadata for table {table_name}: {e}")
                continue

        return tables

    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        if not self.engine:
            raise RuntimeError("Not connected to database")

        inspector = inspect(self.engine)

        # Get column information
        columns = []
        column_info_list = inspector.get_columns(table_name, schema=schema)

        for col_info in column_info_list:
            column_data = {
                'name': col_info['name'],
                'type': str(col_info['type']),
                'nullable': col_info['nullable'],
                'default': col_info['default'],
                'autoincrement': col_info.get('autoincrement', False),
                'comment': col_info.get('comment', '')
            }
            columns.append(column_data)

        # Get primary keys
        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
        primary_keys = pk_constraint.get('constrained_columns', []) if pk_constraint else []

        # Get foreign keys
        foreign_keys = []
        fk_constraints = inspector.get_foreign_keys(table_name, schema=schema)

        for fk in fk_constraints:
            foreign_key_info = {
                'name': fk.get('name'),
                'constrained_columns': fk['constrained_columns'],
                'referred_table': fk['referred_table'],
                'referred_columns': fk['referred_columns'],
                'referred_schema': fk.get('referred_schema')
            }
            foreign_keys.append(foreign_key_info)

        # Get indexes
        indexes = []
        index_info_list = inspector.get_indexes(table_name, schema=schema)

        for idx_info in index_info_list:
            index_data = {
                'name': idx_info['name'],
                'column_names': idx_info['column_names'],
                'unique': idx_info['unique'],
                'type': idx_info.get('type', 'btree')
            }
            indexes.append(index_data)

        # Get row count
        row_count = self._get_table_row_count(table_name, schema)

        return TableMetadata(
            name=table_name,
            schema=schema,
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            indexes=indexes,
            row_count=row_count
        )

    def extract_data(self, table_name: str, schema: Optional[str] = None,
                     batch_size: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        if not self.session:
            raise RuntimeError("Not connected to database")

        try:
            # Reflect the table
            table = Table(table_name, self.metadata, schema=schema, autoload_with=self.engine)

            # Build query with limit and offset
            query = select(table).limit(batch_size).offset(offset)

            # Execute query
            result = self.session.execute(query)

            # Convert to list of dictionaries
            rows = []
            for row in result:
                row_dict = dict(row._mapping)
                # Convert datetime objects to strings for JSON serialization
                for key, value in row_dict.items():
                    if isinstance(value, datetime):
                        row_dict[key] = value.isoformat()
                rows.append(row_dict)

            return rows

        except SQLAlchemyError as e:
            self.logger.error(f"Error extracting data from {table_name}: {e}")
            raise

    def extract_data_streaming(self, table_name: str, schema: Optional[str] = None,
                               batch_size: int = 1000) -> Generator[Dict[str, Any], None, None]:
        if not self.session:
            raise RuntimeError("Not connected to database")

        try:
            # Reflect the table
            table = Table(table_name, self.metadata, schema=schema, autoload_with=self.engine)

            offset = 0
            while True:
                # Build query with limit and offset
                query = select(table).limit(batch_size).offset(offset)

                # Execute query
                result = self.session.execute(query)
                rows = result.fetchall()

                if not rows:
                    break

                for row in rows:
                    row_dict = dict(row._mapping)
                    # Convert datetime objects to strings for JSON serialization
                    for key, value in row_dict.items():
                        if isinstance(value, datetime):
                            row_dict[key] = value.isoformat()
                    yield row_dict

                offset += batch_size

        except SQLAlchemyError as e:
            self.logger.error(f"Error streaming data from {table_name}: {e}")
            raise

    def validate_data_integrity(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        if not self.session:
            raise RuntimeError("Not connected to database")

        validation_results = {
            'table_name': table_name,
            'schema': schema,
            'errors': [],
            'warnings': [],
            'statistics': {}
        }

        try:
            # Reflect the table
            table = Table(table_name, self.metadata, schema=schema, autoload_with=self.engine)

            # Check for null values in non-nullable columns
            for column in table.columns:
                if not column.nullable:
                    null_count_query = select(func.count()).where(column.is_(None))
                    result = self.session.execute(null_count_query)
                    null_count = result.scalar()

                    if null_count > 0:
                        validation_results['errors'].append(
                            f"Column {column.name} has {null_count} null values but is defined as NOT NULL"
                        )

            # Get table statistics
            total_rows = self.session.execute(select(func.count()).select_from(table)).scalar()
            validation_results['statistics']['total_rows'] = total_rows

            # Check for duplicate primary keys (if any)
            if table.primary_key.columns:
                pk_columns = list(table.primary_key.columns)
                duplicate_query = (
                    select(*pk_columns, func.count().label('count'))
                    .group_by(*pk_columns)
                    .having(func.count() > 1)
                )

                duplicates = self.session.execute(duplicate_query).fetchall()
                if duplicates:
                    validation_results['errors'].append(
                        f"Found {len(duplicates)} duplicate primary key values"
                    )

        except SQLAlchemyError as e:
            validation_results['errors'].append(f"Error during validation: {str(e)}")

        return validation_results

    def _build_connection_url(self, config: ConnectionConfig) -> str:
        """Build SQLAlchemy connection URL from config."""

        # Database-specific URL patterns
        url_patterns = {
            DatabaseType.MYSQL: "mysql+pymysql://{username}:{password}@{host}:{port}/{database}",
            DatabaseType.POSTGRESQL: "postgresql://{username}:{password}@{host}:{port}/{database}",
            DatabaseType.SQLITE: "sqlite:///{database}",
            DatabaseType.ORACLE: "oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}",
            DatabaseType.SQLSERVER: "mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        }

        if config.db_type not in url_patterns:
            raise ValueError(f"Unsupported database type: {config.db_type}")

        # Handle SQLite special case (no host/port/username/password)
        if config.db_type == DatabaseType.SQLITE:
            return url_patterns[config.db_type].format(database=config.database)

        return url_patterns[config.db_type].format(
            username=config.username,
            password=config.password,
            host=config.host,
            port=config.port,
            database=config.database
        )

    def _get_table_row_count(self, table_name: str, schema: Optional[str] = None) -> int:
        """Get row count for a table."""
        try:
            table = Table(table_name, self.metadata, schema=schema, autoload_with=self.engine)
            count_query = select(func.count()).select_from(table)
            result = self.session.execute(count_query)
            return result.scalar() or 0
        except SQLAlchemyError as e:
            self.logger.warning(f"Could not get row count for {table_name}: {e}")
            return 0