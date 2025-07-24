from abc import abstractmethod, ABC
from typing import List, Dict, Any

from app.agents.schemas import TableMetadata, NormalizationRule


class DatabaseNormalizerInterface(ABC):

    @abstractmethod
    def analyze_normalization_needs(self, metadata: List[TableMetadata]) -> List[NormalizationRule]:
        """
        Analyze table metadata to identify normalization requirements.

        Args:
            metadata: List of table metadata objects

        Returns:
            List[NormalizationRule]: List of normalization rules to apply
        """
        pass

    @abstractmethod
    def normalize_data_types(self, data: List[Dict[str, Any]],
                             source_metadata: TableMetadata,
                             target_type_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Normalize data types according to target database requirements.

        Args:
            data: Raw data to normalize
            source_metadata: Source table metadata
            target_type_mapping: Mapping of source types to target types

        Returns:
            List[Dict[str, Any]]: Normalized data
        """
        pass

    @abstractmethod
    def handle_null_values(self, data: List[Dict[str, Any]],
                           null_handling_rules: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Handle null values according to specified rules.

        Args:
            data: Data containing potential null values
            null_handling_rules: Rules for handling nulls per column

        Returns:
            List[Dict[str, Any]]: Data with null values handled
        """
        pass

    @abstractmethod
    def validate_constraints(self, data: List[Dict[str, Any]],
                             constraints: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Validate data against database constraints.

        Args:
            data: Data to validate
            constraints: List of constraint definitions

        Returns:
            Dict[str, List[str]]: Validation errors grouped by constraint type
        """
        pass

    @abstractmethod
    def normalize_encoding(self, data: List[Dict[str, Any]],
                           target_encoding: str = 'utf-8') -> List[Dict[str, Any]]:
        """
        Normalize character encoding for text data.

        Args:
            data: Data with potential encoding issues
            target_encoding: Target character encoding

        Returns:
            List[Dict[str, Any]]: Data with normalized encoding
        """
        pass

    @abstractmethod
    def apply_business_rules(self, data: List[Dict[str, Any]],
                             rules: List[NormalizationRule]) -> List[Dict[str, Any]]:
        """
        Apply custom business rules for data normalization.

        Args:
            data: Data to process
            rules: List of business rules to apply

        Returns:
            List[Dict[str, Any]]: Data after applying business rules
        """
        pass

    @abstractmethod
    def generate_normalization_report(self, original_data: List[Dict[str, Any]],
                                      normalized_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate a report comparing original and normalized data.

        Args:
            original_data: Original data before normalization
            normalized_data: Data after normalization

        Returns:
            Dict[str, Any]: Normalization report with statistics and changes
        """
        pass

