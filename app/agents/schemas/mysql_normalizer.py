from typing import Dict, List, Any
import logging
from datetime import datetime
from app.agents.database_ingestor.interfaces import DatabaseNormalizerInterface, TableMetadata, NormalizationRule

class DataNormalizer(DatabaseNormalizerInterface):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.type_mappings = {
            'mysql_to_postgresql': {
                'INT': 'INTEGER',
                'VARCHAR': 'VARCHAR',
                'TEXT': 'TEXT',
                'DATETIME': 'TIMESTAMP',
                'TINYINT(1)': 'BOOLEAN'
            }
        }

    def analyze_normalization_needs(self, metadata: List[TableMetadata]) -> List[NormalizationRule]:
        rules = []

        for table in metadata:
            for column in table.columns:
                # Check for data type normalization needs
                if self._needs_type_conversion(column):
                    rules.append(NormalizationRule(
                        table_name=table.name,
                        column_name=column['name'],
                        rule_type='data_type_conversion',
                        parameters={
                            'source_type': column['type'],
                            'target_type': self._get_target_type(column['type'])
                        }
                    ))

                # Check for null handling needs
                if column['nullable'] and self._has_business_rule_for_nulls(table.name, column['name']):
                    rules.append(NormalizationRule(
                        table_name=table.name,
                        column_name=column['name'],
                        rule_type='null_handling',
                        parameters={
                            'strategy': 'default_value',
                            'default': self._get_default_for_column(column)
                        }
                    ))

        return rules

    def normalize_data_types(self, data: List[Dict[str, Any]],
                             source_metadata: TableMetadata,
                             target_type_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        normalized_data = []

        for row in data:
            normalized_row = {}

            for column in source_metadata.columns:
                column_name = column['name']
                column_type = column['type']
                value = row.get(column_name)

                if value is not None:
                    normalized_value = self._convert_value(value, column_type, target_type_mapping.get(column_type))
                    normalized_row[column_name] = normalized_value
                else:
                    normalized_row[column_name] = value

            normalized_data.append(normalized_row)

        return normalized_data

    def handle_null_values(self, data: List[Dict[str, Any]],
                           null_handling_rules: Dict[str, str]) -> List[Dict[str, Any]]:
        processed_data = []

        for row in data:
            processed_row = {}

            for column_name, value in row.items():
                if value is None and column_name in null_handling_rules:
                    strategy = null_handling_rules[column_name]

                    if strategy == 'remove_row':
                        processed_row = None
                        break
                    elif strategy == 'default_value':
                        processed_row[column_name] = self._get_default_value(column_name)
                    elif strategy == 'empty_string':
                        processed_row[column_name] = ''
                    elif strategy == 'zero':
                        processed_row[column_name] = 0
                else:
                    processed_row[column_name] = value

            if processed_row is not None:
                processed_data.append(processed_row)

        return processed_data

    def validate_constraints(self, data: List[Dict[str, Any]],
                             constraints: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        validation_errors = {
            'primary_key_violations': [],
            'foreign_key_violations': [],
            'check_constraint_violations': [],
            'unique_constraint_violations': []
        }

        for constraint in constraints:
            constraint_type = constraint['type']

            if constraint_type == 'PRIMARY_KEY':
                errors = self._validate_primary_key(data, constraint)
                validation_errors['primary_key_violations'].extend(errors)

            elif constraint_type == 'FOREIGN_KEY':
                errors = self._validate_foreign_key(data, constraint)
                validation_errors['foreign_key_violations'].extend(errors)

            elif constraint_type == 'UNIQUE':
                errors = self._validate_unique_constraint(data, constraint)
                validation_errors['unique_constraint_violations'].extend(errors)

        return validation_errors

    def normalize_encoding(self, data: List[Dict[str, Any]],
                           target_encoding: str = 'utf-8') -> List[Dict[str, Any]]:
        normalized_data = []

        for row in data:
            normalized_row = {}

            for column_name, value in row.items():
                if isinstance(value, str):
                    try:
                        # Try to encode/decode to normalize encoding
                        normalized_value = value.encode(target_encoding, errors='ignore').decode(target_encoding)
                        normalized_row[column_name] = normalized_value
                    except (UnicodeEncodeError, UnicodeDecodeError) as e:
                        self.logger.warning(f"Encoding issue in column {column_name}: {e}")
                        normalized_row[column_name] = value
                else:
                    normalized_row[column_name] = value

            normalized_data.append(normalized_row)

        return normalized_data

    def apply_business_rules(self, data: List[Dict[str, Any]],
                             rules: List[NormalizationRule]) -> List[Dict[str, Any]]:
        processed_data = data.copy()

        for rule in rules:
            if rule.rule_type == 'data_transformation':
                processed_data = self._apply_transformation_rule(processed_data, rule)
            elif rule.rule_type == 'data_validation':
                processed_data = self._apply_validation_rule(processed_data, rule)
            elif rule.rule_type == 'data_cleansing':
                processed_data = self._apply_cleansing_rule(processed_data, rule)

        return processed_data

    def generate_normalization_report(self, original_data: List[Dict[str, Any]],
                                      normalized_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        report = {
            'timestamp': datetime.now().isoformat(),
            'original_record_count': len(original_data),
            'normalized_record_count': len(normalized_data),
            'records_removed': len(original_data) - len(normalized_data),
            'columns_processed': set(),
            'transformations_applied': [],
            'data_quality_metrics': {}
        }

        # Analyze changes
        if original_data and normalized_data:
            original_columns = set(original_data[0].keys()) if original_data else set()
            normalized_columns = set(normalized_data[0].keys()) if normalized_data else set()

            report['columns_processed'] = list(original_columns.union(normalized_columns))
            report['columns_added'] = list(normalized_columns - original_columns)
            report['columns_removed'] = list(original_columns - normalized_columns)

        return report

    # Helper methods
    def _needs_type_conversion(self, column: Dict[str, Any]) -> bool:
        # Implementation logic for determining if type conversion is needed
        return column['type'].upper() in ['TINYINT(1)', 'DATETIME']

    def _get_target_type(self, source_type: str) -> str:
        mapping = self.type_mappings.get('mysql_to_postgresql', {})
        return mapping.get(source_type.upper(), source_type)

    def _convert_value(self, value: Any, source_type: str, target_type: str) -> Any:
        if target_type == 'BOOLEAN' and source_type == 'TINYINT(1)':
            return bool(value)
        elif target_type == 'TIMESTAMP' and source_type == 'DATETIME':
            return value  # Usually handled by the database driver
        return value

    def _validate_primary_key(self, data: List[Dict[str, Any]], constraint: Dict[str, Any]) -> List[str]:
        # Implementation for primary key validation
        errors = []
        pk_columns = constraint['columns']
        seen_keys = set()

        for i, row in enumerate(data):
            key_values = tuple(row.get(col) for col in pk_columns)

            if None in key_values:
                errors.append(f"Row {i}: Primary key contains null values")
            elif key_values in seen_keys:
                errors.append(f"Row {i}: Duplicate primary key values: {key_values}")
            else:
                seen_keys.add(key_values)

        return errors

    def _get_default_value(self, column_name: str) -> Any:
        # Business logic for default values
        default_mappings = {
            'created_at': datetime.now(),
            'status': 'active',
            'is_deleted': False
        }
        return default_mappings.get(column_name, '')