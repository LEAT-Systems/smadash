"""MongoDB query generator using LLM for natural language to MongoDB aggregation pipeline conversion."""
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


class MongoDBQueryGenerator(QueryGeneratorInterface):
    """
    Generates MongoDB aggregation pipelines from natural language using LLM.
    
    Converts natural language queries into MongoDB aggregation pipeline syntax.
    """
    
    def __init__(self, model: str = "gpt-4o-mini"):
        """
        Initialize MongoDB query generator.
        
        Args:
            model: LLM model to use for generation
        """
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
        """Generate MongoDB aggregation pipeline from natural language."""
        logger.info(f"Generating MongoDB query for: {natural_language_query}")
        
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
        """Validate MongoDB aggregation pipeline."""
        errors = []
        warnings = []
        
        try:
            # Parse as JSON array (aggregation pipeline)
            pipeline = json.loads(query)
            
            if not isinstance(pipeline, list):
                errors.append("MongoDB pipeline must be a JSON array")
                return {"valid": False, "errors": errors, "warnings": warnings}
            
            # Check for valid stages
            valid_stages = [
                '$match', '$group', '$sort', '$limit', '$skip', '$project',
                '$lookup', '$unwind', '$addFields', '$count', '$facet', '$sample'
            ]
            
            for stage in pipeline:
                if not isinstance(stage, dict):
                    errors.append(f"Invalid stage format: {stage}")
                    continue
                
                stage_name = list(stage.keys())[0] if stage else None
                if stage_name and stage_name not in valid_stages:
                    warnings.append(f"Unknown or uncommon stage: {stage_name}")
            
            if not errors:
                return {"valid": True, "errors": [], "warnings": warnings}
            else:
                return {"valid": False, "errors": errors, "warnings": warnings}
        
        except json.JSONDecodeError as e:
            return {
                "valid": False,
                "errors": [f"Invalid JSON: {str(e)}"],
                "warnings": warnings
            }
        except Exception as e:
            return {
                "valid": False,
                "errors": [f"Validation error: {str(e)}"],
                "warnings": warnings
            }
    
    def explain_query(self, query: str) -> str:
        """Generate human-readable explanation of MongoDB query."""
        if not self.client:
            return "LLM not available for query explanation"
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a MongoDB expert. Explain MongoDB aggregation pipelines in simple, human-readable terms."
                    },
                    {
                        "role": "user",
                        "content": f"Explain this MongoDB aggregation pipeline in plain English:\n\n{query}"
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
        """Return MongoDB query language."""
        return QueryLanguage.MONGODB_QUERY
    
    def _build_system_prompt(self, schema_context: Dict[str, Any]) -> str:
        """Build system prompt with schema information."""
        # Format schema for prompt
        schema_str = self._format_schema_for_prompt(schema_context)
        
        return f"""You are an expert MongoDB query generator. Your job is to:
1. Convert natural language requests into MongoDB aggregation pipelines
2. Generate efficient, optimized pipelines
3. Return structured JSON with pipeline details

MongoDB Collections Schema:
{schema_str}

Guidelines:
- Generate valid MongoDB aggregation pipeline (JSON array)
- Use appropriate stages: $match, $group, $project, $sort, $limit, $lookup
- For filtering: use $match stage
- For aggregations: use $group with accumulators ($sum, $avg, $count, etc.)
- For joins: use $lookup to reference other collections
- For shaping output: use $project
- Always limit results with $limit for large datasets
- Use indexes efficiently (put $match early in pipeline)
- Use field projection to reduce data transfer

Response format (JSON only, no additional text):
{{
    "pipeline": [
        {{"$match": {{"field": "value"}}}},
        {{"$group": {{"_id": "$field", "count": {{"$sum": 1}}}}}},
        {{"$sort": {{"count": -1}}}},
        {{"$limit": 100}}
    ],
    "query_type": "aggregate|filter|lookup|analysis",
    "collections": ["collection1", "collection2"],
    "explanation": "What the pipeline does",
    "confidence": 0.0-1.0,
    "warnings": ["any caveats or warnings"],
    "estimated_documents": approximate_result_count,
    "indexes_used": ["field1", "field2"]
}}
"""
    
    def _format_schema_for_prompt(self, schema_context: Dict[str, Any]) -> str:
        """Format schema context for LLM prompt."""
        if isinstance(schema_context, str):
            return schema_context
        
        if isinstance(schema_context, dict):
            # Handle different schema formats
            if 'collections' in schema_context:
                # Format: {"collections": [{"name": "...", "fields": [...]}]}
                formatted = []
                for collection in schema_context.get('collections', []):
                    coll_name = collection.get('name', 'unknown')
                    formatted.append(f"\nCollection: {coll_name}")
                    for field in collection.get('fields', []):
                        field_name = field.get('name', field.get('field', ''))
                        field_type = field.get('type', 'unknown')
                        formatted.append(f"  - {field_name}: {field_type}")
                return "\n".join(formatted)
            
            elif 'collection_schemas' in schema_context:
                # Format: {"collection_schemas": {"coll1": {...}, "coll2": {...}}}
                formatted = []
                for coll_name, coll_info in schema_context.get('collection_schemas', {}).items():
                    formatted.append(f"\nCollection: {coll_name}")
                    for field in coll_info.get('fields', []):
                        formatted.append(f"  - {field['name']}: {field['type']}")
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
        
        # Extract collection name from schema
        collection_name = "collection"
        if isinstance(schema_context, dict):
            if 'collections' in schema_context and schema_context['collections']:
                collection_name = schema_context['collections'][0].get('name', 'collection')
            elif 'collection_schemas' in schema_context:
                collection_name = list(schema_context['collection_schemas'].keys())[0]
        
        # Pattern matching
        if any(word in query_lower for word in ['count', 'how many', 'number of']):
            pipeline = [
                {"$count": "total"}
            ]
            query_type = "aggregate"
        elif any(word in query_lower for word in ['average', 'avg', 'mean']):
            pipeline = [
                {"$group": {"_id": None, "average": {"$avg": "$value"}}},
                {"$project": {"_id": 0, "average": 1}}
            ]
            query_type = "aggregate"
        elif any(word in query_lower for word in ['sum', 'total']):
            pipeline = [
                {"$group": {"_id": None, "total": {"$sum": "$value"}}},
                {"$project": {"_id": 0, "total": 1}}
            ]
            query_type = "aggregate"
        elif any(word in query_lower for word in ['group', 'by']):
            pipeline = [
                {"$group": {"_id": "$category", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 100}
            ]
            query_type = "aggregate"
        else:
            pipeline = [
                {"$limit": 100}
            ]
            query_type = "filter"
        
        return {
            'content': json.dumps({
                'pipeline': pipeline,
                'query_type': query_type,
                'collections': [collection_name],
                'explanation': 'Generated using pattern matching (LLM unavailable)',
                'confidence': 0.5,
                'warnings': ['Generated without LLM - may need manual adjustment'],
                'estimated_documents': 100
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
            
            # Convert pipeline to JSON string for storage
            pipeline_str = json.dumps(parsed.get('pipeline', []), indent=2)
            
            return GeneratedQuery(
                query=pipeline_str,
                query_language=QueryLanguage.MONGODB_QUERY,
                query_type=parsed.get('query_type', 'aggregate'),
                tables_or_collections=parsed.get('collections', []),
                explanation=parsed.get('explanation', ''),
                confidence_score=parsed.get('confidence', 0.9),
                warnings=parsed.get('warnings', []),
                estimated_rows=parsed.get('estimated_documents'),
                metadata={
                    'indexes_used': parsed.get('indexes_used', []),
                    'original_query': original_query
                }
            )
        
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse LLM response: {str(e)}")
            raise ValueError(f"Could not parse MongoDB pipeline from LLM response: {str(e)}")
