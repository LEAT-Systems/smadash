from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List, Any


class DatabaseType(Enum):
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    MONGODB = "mongodb"

@dataclass
class ConnectionConfig:
    host: str
    db_type: DatabaseType
    port: int = None
    database: str = None
    username: str = None
    password: str = None
    additional_params: Optional[Dict[str, Any]] = None

@dataclass
class TableMetadata:
    name: str
    schema: Optional[str]
    columns: List[Dict[str, Any]]
    primary_keys: List[str]
    foreign_keys: List[Dict[str, Any]]
    indexes: List[Dict[str, Any]]
    row_count: int

@dataclass
class NormalizationRule:
    table_name: str
    column_name: str
    rule_type: str  # e.g., 'data_type_conversion', 'null_handling', 'constraint_validation'
    parameters: Dict[str, Any]

# @dataclass
class ColumnMetadata:
    name: str
    data_type: str
    nullable: bool
    default_value: Optional[str]
    auto_increment: bool
