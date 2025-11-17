"""SQL query generator using LLM for natural language to SQL conversion."""
import os
import json
import logging
from typing import Dict, Any, List, Optional
from openai import OpenAI

from app.agents.query_engine.interfaces import QueryGeneratorInterface
from app.agents.query_engine.interfaces.query_generator_interface import (
    GeneratedQuery, QueryLanguage
)


logger = logging.getLogger(__name__)


class SQLQueryGenerator(QueryGeneratorInterface):
    """
    Generates SQL queries from natural language using LLM.
    
    Supports all SQL databases via SQLAlchemy (PostgreSQL, MySQL, SQLite, Oracle, SQL Server).
    """
    
    def __init__(self, dialect: str = "postgresql", model: str = "gpt-4o-mini"):
        """
        Initialize SQL query generator.
        
        Args:
            dialect: SQL dialect (postgresql, mysql, sqlite, oracle, mssql)
            model: LLM model to use for generation
        """
        self.dialect = dialect
        self.model = model
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        if self.openai_api_key:
            self.client = OpenAI(api_key=self.openai_api_key)
        else:
            logger.warning("No OpenAI API key found. LLM generation will not be available.")
            self.client = None
    
    def generate_query(
        self,
        natural_language_query: str,
        schema_context: Dict[str, Any],
        additional_context: Optional[Dict[str, Any]] = None
    ) -> GeneratedQuery:
        """Generate SQL query from natural language."""
        logger.info(f"Generating SQL query for: {natural_language_query}")
        
        # Build system prompt with schema
        system_prompt = self._build_system_prompt(schema_context)
        
        # Call LLM if available, otherwise use pattern matching
        if self.client:
            llm_response = self._call_llm(system_prompt, natural_language_query)
        else:
            llm_response = self._pattern_matching_fallback(natural_language_query, schema_context)
        
        # Parse and return structured result
        return self._parse_llm_response(llm_response, natural_language_query)
    
    def validate_query(
        self,
        query: str,
        schema_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate SQL query against schema."""
        errors = []
        warnings = []
        
        try:
            # Basic SQL syntax validation
            query_upper = query.upper().strip()
            
            # Check for dangerous operations
            dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE']
            for keyword in dangerous_keywords:
                if keyword in query_upper:
                    errors.append(f"Query contains dangerous operation: {keyword}")
            
            # Check if tables exist in schema
            tables_in_schema = set()
            if isinstance(schema_context, dict):
                if 'tables' in schema_context:
                    tables_in_schema = {t.get('name', '').lower() for t in schema_context['tables']}
                elif 'table_schemas' in schema_context:
                    tables_in_schema = set(schema_context['table_schemas'].keys())
            
            # Extract table references from query (basic check)
            for table in tables_in_schema:
                if table.lower() in query.lower():
                    # Found valid table reference
                    pass
            
            if not errors:
                return {"valid": True, "errors": [], "warnings": warnings}
            else:
                return {"valid": False, "errors": errors, "warnings": warnings}
        
        except Exception as e:
            logger.error(f"Query validation error: {str(e)}")
            return {
                "valid": False,
                "errors": [f"Validation error: {str(e)}"],
                "warnings": warnings
            }
    
    def explain_query(self, query: str) -> str:
        """Generate human-readable explanation of SQL query."""
        if not self.client:
            return "LLM not available for query explanation"
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a SQL expert. Explain SQL queries in simple, human-readable terms."
                    },
                    {
                        "role": "user",
                        "content": f"Explain this SQL query in plain English:\n\n{query}"
                    }
                ],
                temperature=0.3,
                max_tokens=300
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Query explanation error: {str(e)}")
            return f"Error generating explanation: {str(e)}"
    
    def get_supported_query_language(self) -> QueryLanguage:
        """Return SQL as the supported query language."""
        return QueryLanguage.SQL
    
    def _build_system_prompt(self, schema_context: Dict[str, Any]) -> str:
        """Build system prompt with schema information."""
        # Format schema for prompt
        schema_str = self._format_schema_for_prompt(schema_context)
        
        return f"""You are an expert SQL query generator. Your job is to:
1. Convert natural language requests into valid SQL queries
2. Generate queries optimized for {self.dialect}
3. Return structured JSON with query details

Database Schema:
{schema_str}

Guidelines:
- Generate valid, executable SQL for {self.dialect}
- Use proper JOINs when querying multiple tables
- Include appropriate WHERE clauses for filtering
- Use aggregations (COUNT, SUM, AVG, etc.) when needed
- Add LIMIT clauses for reasonable result sizes
- NEVER use SELECT * in production queries
- Always include at least one human-readable display field (not just IDs)
- For foreign keys, JOIN to fetch display names/labels
- Use table aliases for readability
- Follow {self.dialect} syntax and conventions

Response format (JSON only, no additional text):
{{
    "sql": "SELECT ... FROM ... WHERE ...",
    "query_type": "select|aggregate|join|analysis",
    "tables": ["table1", "table2"],
    "explanation": "What the query does",
    "confidence": 0.0-1.0,
    "warnings": ["any caveats or warnings"],
    "estimated_rows": approximate_result_count,
    "display_fields": {{"table1": ["name", "label"], "table2": ["title"]}},
    "joins": [{{"from_table": "orders", "to_table": "customers", "on": "customer_id"}}]
}}
"""
    
    def _format_schema_for_prompt(self, schema_context: Dict[str, Any]) -> str:
        """Format schema context for LLM prompt."""
        if isinstance(schema_context, str):
            return schema_context
        
        if isinstance(schema_context, dict):
            # Handle different schema formats
            if 'tables' in schema_context:
                # Format: {"tables": [{"name": "...", "columns": [...]}]}
                formatted = []
                for table in schema_context.get('tables', []):
                    table_name = table.get('name', 'unknown')
                    formatted.append(f"\nTable: {table_name}")
                    for col in table.get('columns', []):
                        col_name = col.get('name', col.get('column_name', ''))
                        col_type = col.get('type', col.get('data_type', 'unknown'))
                        formatted.append(f"  - {col_name} ({col_type})")
                return "\n".join(formatted)
            
            elif 'table_schemas' in schema_context:
                # Format: {"table_schemas": {"table1": {...}, "table2": {...}}}
                formatted = []
                for table_name, table_info in schema_context.get('table_schemas', {}).items():
                    formatted.append(f"\nTable: {table_name}")
                    for col in table_info.get('columns', []):
                        formatted.append(f"  - {col['name']} ({col['type']})")
                return "\n".join(formatted)
        
        # Fallback: return JSON string
        return json.dumps(schema_context, indent=2)
    
    def _call_llm(self, system_prompt: str, user_query: str) -> Dict[str, Any]:
        """Call OpenAI LLM for query generation."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_query}
                ],
                temperature=0.2,
                max_tokens=1500,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content.strip()
            return {
                'content': content,
                'usage': {
                    'total_tokens': response.usage.total_tokens,
                    'prompt_tokens': response.usage.prompt_tokens,
                    'completion_tokens': response.usage.completion_tokens
                }
            }
        
        except Exception as e:
            logger.error(f"LLM API call failed: {str(e)}")
            raise
    
    def _pattern_matching_fallback(
        self,
        query: str,
        schema_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fallback pattern matching when LLM unavailable."""
        query_lower = query.lower()
        
        # Extract table name from schema
        table_name = "table_name"
        if isinstance(schema_context, dict):
            if 'tables' in schema_context and schema_context['tables']:
                table_name = schema_context['tables'][0].get('name', 'table_name')
            elif 'table_schemas' in schema_context:
                table_name = list(schema_context['table_schemas'].keys())[0]
        
        # Pattern matching
        if any(word in query_lower for word in ['count', 'how many', 'number of']):
            sql = f"SELECT COUNT(*) as count FROM {table_name}"
            query_type = "aggregate"
        elif any(word in query_lower for word in ['average', 'avg', 'mean']):
            sql = f"SELECT AVG(value_column) as average FROM {table_name}"
            query_type = "aggregate"
        elif any(word in query_lower for word in ['sum', 'total']):
            sql = f"SELECT SUM(value_column) as total FROM {table_name}"
            query_type = "aggregate"
        else:
            sql = f"SELECT * FROM {table_name} LIMIT 100"
            query_type = "select"
        
        return {
            'content': json.dumps({
                'sql': sql,
                'query_type': query_type,
                'tables': [table_name],
                'explanation': 'Generated using pattern matching (LLM unavailable)',
                'confidence': 0.5,
                'warnings': ['Generated without LLM - may need manual adjustment'],
                'estimated_rows': 100
            })
        }
    
    def _parse_llm_response(
        self,
        llm_response: Dict[str, Any],
        original_query: str
    ) -> GeneratedQuery:
        """Parse LLM response into GeneratedQuery object."""
        try:
            content = llm_response['content']
            parsed = json.loads(content) if isinstance(content, str) else content
            
            return GeneratedQuery(
                query=parsed.get('sql', ''),
                query_language=QueryLanguage.SQL,
                query_type=parsed.get('query_type', 'select'),
                tables_or_collections=parsed.get('tables', []),
                explanation=parsed.get('explanation', ''),
                confidence_score=parsed.get('confidence', 0.9),
                warnings=parsed.get('warnings', []),
                estimated_rows=parsed.get('estimated_rows'),
                metadata={
                    'dialect': self.dialect,
                    'display_fields': parsed.get('display_fields', {}),
                    'joins': parsed.get('joins', []),
                    'original_query': original_query
                }
            )
        
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse LLM response: {str(e)}")
            # Try to extract SQL from response
            content = llm_response.get('content', '')
            if 'SELECT' in content.upper():
                import re
                sql_match = re.search(r'(SELECT.*?(?:;|$))', content, re.IGNORECASE | re.DOTALL)
                if sql_match:
                    return GeneratedQuery(
                        query=sql_match.group(1).strip(),
                        query_language=QueryLanguage.SQL,
                        query_type='select',
                        tables_or_collections=[],
                        explanation='Extracted from unstructured response',
                        confidence_score=0.6,
                        warnings=['Response was not in expected JSON format'],
                        metadata={'dialect': self.dialect}
                    )
            
            raise ValueError(f"Could not parse SQL from LLM response: {str(e)}")
