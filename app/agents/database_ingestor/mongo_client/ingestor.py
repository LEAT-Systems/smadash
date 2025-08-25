from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError
from typing import Dict, List, Optional, Any, Generator, Set
import logging
from datetime import datetime
from bson import ObjectId
from collections import defaultdict, Counter
from app.agents.database_ingestor.interfaces import DatabaseIngestorInterface, ConnectionConfig, TableMetadata
from app.agents.utils.database_connection_schema import DatabaseType, ColumnMetadata


class MongoDBIngestor(DatabaseIngestorInterface):
    def __init__(self):
        self.client: Optional[MongoClient] = None
        self.database = None
        self.logger = logging.getLogger(__name__)
        self.db_name: Optional[str] = None

    def connect(self, config: ConnectionConfig) -> bool:
        try:
            # Build MongoDB connection string
            connection_string = self._build_connection_string(config)

            # Create MongoDB client
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=10000,  # 10 second connection timeout
                socketTimeoutMS=30000,  # 30 second socket timeout
                maxPoolSize=10,
                minPoolSize=1,
                maxIdleTimeMS=30000
            )

            # Test connection
            self.client.admin.command('ping')

            # Set database
            self.db_name = config.database if config.database is not None else self.client.get_default_database().name
            config.database = self.db_name
            self.database = self.client.get_database(self.db_name)

            self.logger.info(f"Connected to MongoDB database: {config.database}")
            return True

        except (ConnectionFailure, ServerSelectionTimeoutError, PyMongoError) as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False

    def disconnect(self) -> None:
        if self.client:
            self.client.close()
            self.client = None
            self.database = None
            self.db_name = None

        self.logger.info("Disconnected from MongoDB")

    def test_connection(self, config: ConnectionConfig) -> bool:
        try:
            connection_string = self._build_connection_string(config)
            test_client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000
            )

            # Test connection
            test_client.admin.command('ping')
            test_client.close()
            return True

        except (ConnectionFailure, ServerSelectionTimeoutError, PyMongoError):
            return False

    def discover_tables(self) -> List[TableMetadata]:
        """Discover collections (MongoDB equivalent of tables)."""
        if self.database is None:
            raise RuntimeError("Not connected to database")

        collections = []

        try:
            # Get all collection names
            collection_names = self.database.list_collection_names()

            for collection_name in collection_names:
                try:
                    metadata = self.get_table_metadata(collection_name)
                    collections.append(metadata)
                except Exception as e:
                    self.logger.warning(f"Failed to get metadata for collection {collection_name}: {e}")
                    continue

            return collections

        except PyMongoError as e:
            self.logger.error(f"Error discovering collections: {e}")
            raise

    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        """Get metadata for a MongoDB collection (table equivalent)."""
        if self.database is None:
            raise RuntimeError("Not connected to database")

        try:
            collection = self.database[table_name]

            # Get document count
            row_count = collection.count_documents({})

            # Analyze schema by sampling documents
            columns = self._analyze_collection_schema(collection)

            # MongoDB doesn't have traditional primary keys, foreign keys, or indexes in the same way
            # But we can get index information
            indexes = self._get_collection_indexes(collection)

            # MongoDB uses _id as the default primary key
            primary_keys = ['_id']

            # MongoDB doesn't have foreign keys in the traditional sense
            foreign_keys = []

            return TableMetadata(
                name=table_name,
                schema=self.db_name,  # Use database name as schema
                columns=columns,
                primary_keys=primary_keys,
                foreign_keys=foreign_keys,
                indexes=indexes,
                row_count=row_count
            )

        except PyMongoError as e:
            self.logger.error(f"Error getting metadata for collection {table_name}: {e}")
            raise

    def extract_data(self, table_name: str, schema: Optional[str] = None,
                     batch_size: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        """Extract data from MongoDB collection with pagination."""
        if self.database is None:
            raise RuntimeError("Not connected to database")

        try:
            collection = self.database[table_name]

            # Use skip and limit for pagination
            cursor = collection.find().skip(offset).limit(batch_size)

            documents = []
            for doc in cursor:
                # Convert ObjectId and other BSON types to JSON serializable formats
                serialized_doc = self._serialize_document(doc)
                documents.append(serialized_doc)

            return documents

        except PyMongoError as e:
            self.logger.error(f"Error extracting data from collection {table_name}: {e}")
            raise

    def extract_data_streaming(self, table_name: str, schema: Optional[str] = None,
                               batch_size: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """Stream data from MongoDB collection."""
        if self.database is None:
            raise RuntimeError("Not connected to database")

        try:
            collection = self.database[table_name]

            # Use cursor with batch size
            cursor = collection.find().batch_size(batch_size)

            for doc in cursor:
                # Convert ObjectId and other BSON types to JSON serializable formats
                serialized_doc = self._serialize_document(doc)
                yield serialized_doc

        except PyMongoError as e:
            self.logger.error(f"Error streaming data from collection {table_name}: {e}")
            raise

    def validate_data_integrity(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """Validate MongoDB collection data integrity."""
        if self.database is None:
            raise RuntimeError("Not connected to database")

        validation_results = {
            'table_name': table_name,
            'schema': schema,
            'errors': [],
            'warnings': [],
            'statistics': {}
        }

        try:
            collection = self.database[table_name]

            # Get basic statistics
            total_docs = collection.count_documents({})
            validation_results['statistics']['total_rows'] = total_docs

            if total_docs == 0:
                validation_results['warnings'].append("Collection is empty")
                return validation_results

            # Check for duplicate _id values (shouldn't happen, but good to verify)
            pipeline = [
                {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": 1}}}
            ]

            duplicates = list(collection.aggregate(pipeline))
            if duplicates:
                validation_results['errors'].append(
                    f"Found {len(duplicates)} duplicate _id values (this should not happen)"
                )

            # Sample documents to check for common issues
            sample_size = min(1000, total_docs)
            sample_docs = list(collection.aggregate([{"$sample": {"size": sample_size}}]))

            # Check for missing _id fields
            missing_id_count = 0
            field_types = defaultdict(Counter)

            for doc in sample_docs:
                if '_id' not in doc:
                    missing_id_count += 1

                # Analyze field types for consistency
                for field, value in doc.items():
                    field_types[field][type(value).__name__] += 1

            if missing_id_count > 0:
                validation_results['errors'].append(
                    f"Found {missing_id_count} documents without _id field"
                )

            # Check for inconsistent field types
            for field, type_counts in field_types.items():
                if len(type_counts) > 1:
                    validation_results['warnings'].append(
                        f"Field '{field}' has inconsistent types: {dict(type_counts)}"
                    )

            # Get collection statistics
            stats = self.database.command("collStats", table_name)
            validation_results['statistics'].update({
                'size_bytes': stats.get('size', 0),
                'storage_size_bytes': stats.get('storageSize', 0),
                'index_count': stats.get('nindexes', 0),
                'average_document_size': stats.get('avgObjSize', 0)
            })

        except PyMongoError as e:
            validation_results['errors'].append(f"Error during validation: {str(e)}")

        return validation_results

    def _build_connection_string(self, config: ConnectionConfig) -> str:
        """Build MongoDB connection string from config."""

        if config.db_type != DatabaseType.MONGODB:
            raise ValueError(f"Expected MongoDB type, got: {config.db_type}")

        # Handle different MongoDB connection patterns
        if config.username and config.password:
            if config.port and config.port != 27017:  # Non-default port
                connection_string = f"mongodb://{config.username}:{config.password}@{config.host}:{config.port}/"
            else:
                connection_string = f"mongodb://{config.username}:{config.password}@{config.host}/"
        else:
            if config.port and config.port != 27017:  # Non-default port
                connection_string = f"mongodb://{config.host}:{config.port}/"
            else:
                connection_string = {config.host}

        # Add additional connection options if needed
        connection_options = []

        # You can add more options here based on your needs
        # For example: SSL, replica sets, etc.

        if connection_options:
            connection_string += "?" + "&".join(connection_options)

        return connection_string

    def _analyze_collection_schema(self, collection, sample_size: int = 1000) -> List[ColumnMetadata]:
        """Analyze MongoDB collection schema by sampling documents."""

        # Get total document count
        total_docs = collection.count_documents({})

        if total_docs == 0:
            return []

        # Sample documents to analyze schema
        actual_sample_size = min(sample_size, total_docs)
        pipeline = [{"$sample": {"size": actual_sample_size}}]
        sample_docs = list(collection.aggregate(pipeline))

        # Analyze fields across all sampled documents
        field_info = defaultdict(lambda: {
            'types': Counter(),
            'null_count': 0,
            'total_count': 0,
            'sample_values': []
        })

        for doc in sample_docs:
            doc_fields = set(doc.keys())

            # Track all fields that appear in documents
            for field in doc_fields:
                value = doc[field]
                field_info[field]['total_count'] += 1

                if value is None:
                    field_info[field]['null_count'] += 1
                    field_info[field]['types']['null'] += 1
                else:
                    value_type = self._get_mongodb_type_name(value)
                    field_info[field]['types'][value_type] += 1

                    # Store sample values for analysis
                    if len(field_info[field]['sample_values']) < 5:
                        field_info[field]['sample_values'].append(value)

        # Create ColumnMetadata objects
        columns = []
        for field_name, info in field_info.items():
            # Determine the most common type
            most_common_type = info['types'].most_common(1)[0][0] if info['types'] else 'unknown'

            # Calculate if field is nullable (appears as null or missing in some docs)
            nullable = info['null_count'] > 0 or info['total_count'] < actual_sample_size

            column_metadata = ColumnMetadata()
            column_metadata.name = field_name
            column_metadata.data_type = most_common_type
            column_metadata.nullable = nullable
            column_metadata.default_value = None  # MongoDB doesn't have default values
            column_metadata.auto_increment = field_name == '_id'  # _id is auto-generated

            columns.append(column_metadata)

        return columns

    def _get_collection_indexes(self, collection) -> List[Dict[str, Any]]:
        """Get index information for a MongoDB collection."""
        indexes = []

        try:
            index_info = collection.list_indexes()

            for index in index_info:
                index_data = {
                    'name': index.get('name', ''),
                    'column_names': list(index.get('key', {}).keys()),
                    'unique': index.get('unique', False),
                    'type': 'btree'  # MongoDB uses B-tree indexes by default
                }

                # Add additional index properties if available
                if 'sparse' in index:
                    index_data['sparse'] = index['sparse']
                if 'background' in index:
                    index_data['background'] = index['background']

                indexes.append(index_data)

        except PyMongoError as e:
            self.logger.warning(f"Could not retrieve index information: {e}")

        return indexes

    def _get_mongodb_type_name(self, value: Any) -> str:
        """Get MongoDB-specific type name for a value."""
        if isinstance(value, ObjectId):
            return 'ObjectId'
        elif isinstance(value, datetime):
            return 'Date'
        elif isinstance(value, dict):
            return 'Object'
        elif isinstance(value, list):
            return 'Array'
        elif isinstance(value, str):
            return 'String'
        elif isinstance(value, int):
            return 'Int32'
        elif isinstance(value, float):
            return 'Double'
        elif isinstance(value, bool):
            return 'Boolean'
        elif value is None:
            return 'null'
        else:
            return type(value).__name__

    def _serialize_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize MongoDB document for JSON compatibility."""
        serialized = {}

        for key, value in doc.items():
            if isinstance(value, ObjectId):
                serialized[key] = str(value)
            elif isinstance(value, datetime):
                serialized[key] = value.isoformat()
            elif isinstance(value, dict):
                serialized[key] = self._serialize_document(value)
            elif isinstance(value, list):
                serialized[key] = [
                    self._serialize_document(item) if isinstance(item, dict)
                    else str(item) if isinstance(item, ObjectId)
                    else item.isoformat() if isinstance(item, datetime)
                    else item
                    for item in value
                ]
            else:
                serialized[key] = value

        return serialized