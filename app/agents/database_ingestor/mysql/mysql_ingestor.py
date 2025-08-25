import mysql.connector
from typing import Dict, List, Optional, Any, Generator
import logging
from app.agents.database_ingestor.interfaces import DatabaseIngestorInterface, ConnectionConfig, TableMetadata, DatabaseType

class MySQLIngestor(DatabaseIngestorInterface):
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.logger = logging.getLogger(__name__)

    def connect(self, config: ConnectionConfig) -> bool:
        try:
            self.connection = mysql.connector.connect
            self.cursor = self.connection.cursor(dictionary=True)
            self.logger.info(f"Connected to MySQL database: {config.database}")
            return True
        except mysql.connector.Error as e:
            self.logger.error(f"Failed to connect to MySQL: {e}")
            return False

    def disconnect(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("Disconnected from MySQL database")

    def test_connection(self, config: ConnectionConfig) -> bool:
        try:
            test_conn = mysql.connector.connect
            test_conn.close()
            return True
        except mysql.connector.Error:
            return False

    def discover_schema(self) -> List[TableMetadata]:
        if not self.cursor:
            raise RuntimeError("Not connected to database")

        tables = []

        # Get all tables
        self.cursor.execute("SHOW TABLES")
        table_names = [row[list(row.keys())[0]] for row in self.cursor.fetchall()]

        for table_name in table_names:
            metadata = self.get_table_metadata(table_name)
            tables.append(metadata)

        return tables

    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        if not self.cursor:
            raise RuntimeError("Not connected to database")

        # Get column information
        self.cursor.execute(f"DESCRIBE {table_name}")
        columns = []
        primary_keys = []

        for row in self.cursor.fetchall():
            column_info = {
                'name': row['Field'],
                'type': row['Type'],
                'nullable': row['Null'] == 'YES',
                'default': row['Default'],
                'extra': row['Extra']
            }
            columns.append(column_info)

            if row['Key'] == 'PRI':
                primary_keys.append(row['Field'])

        # Get foreign keys
        foreign_keys = self._get_foreign_keys(table_name)

        # Get indexes
        indexes = self._get_indexes(table_name)

        # Get row count
        self.cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        row_count = self.cursor.fetchone()['count']

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
        if not self.cursor:
            raise RuntimeError("Not connected to database")

        query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def extract_data_streaming(self, table_name: str, schema: Optional[str] = None,
                               batch_size: int = 1000) -> Generator[Dict[str, Any], None, None]:
        if not self.cursor:
            raise RuntimeError("Not connected to database")

        offset = 0
        while True:
            query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            if not rows:
                break

            for row in rows:
                yield row

            offset += batch_size

    def validate_data_integrity(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        validation_results = {
            'table_name': table_name,
            'errors': [],
            'warnings': [],
            'statistics': {}
        }

        # Check for null values in non-nullable columns
        metadata = self.get_table_metadata(table_name, schema)

        for column in metadata.columns:
            if not column['nullable']:
                query = f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {column['name']} IS NULL"
                self.cursor.execute(query)
                null_count = self.cursor.fetchone()['null_count']

                if null_count > 0:
                    validation_results['errors'].append(
                        f"Column {column['name']} has {null_count} null values but is defined as NOT NULL"
                    )

        return validation_results

    def _get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        query = """
                SELECT
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = %s AND REFERENCED_TABLE_NAME IS NOT NULL \
                """
        self.cursor.execute(query, (table_name,))
        return [dict(row) for row in self.cursor.fetchall()]

    def _get_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        self.cursor.execute(f"SHOW INDEX FROM {table_name}")
        return [dict(row) for row in self.cursor.fetchall()]