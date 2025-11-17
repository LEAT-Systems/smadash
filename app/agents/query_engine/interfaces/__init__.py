"""Query engine interfaces for multi-datasource support."""
from .query_generator_interface import QueryGeneratorInterface
from .query_executor_interface import QueryExecutorInterface

__all__ = ['QueryGeneratorInterface', 'QueryExecutorInterface']
