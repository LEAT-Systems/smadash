-- PostgreSQL initialization script for Query Engine Database
-- Creates tables for datasources, schemas, and generated queries

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create datasources table
CREATE TABLE IF NOT EXISTS datasources (
    id VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    db_type VARCHAR(50) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    database VARCHAR(255) NOT NULL,
    username VARCHAR(255),
    password_encrypted TEXT,
    ssl_enabled BOOLEAN DEFAULT FALSE,
    additional_params JSONB,
    status VARCHAR(50) DEFAULT 'pending',
    organization_id VARCHAR(36) NOT NULL,
    schema_ingested BOOLEAN DEFAULT FALSE,
    schema_ingested_at TIMESTAMP,
    schema_ingestion_error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create datasource_schemas table
CREATE TABLE IF NOT EXISTS datasource_schemas (
    id VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    datasource_id VARCHAR(36) NOT NULL REFERENCES datasources(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255),
    row_count INTEGER DEFAULT 0,
    columns JSONB NOT NULL,
    primary_keys JSONB,
    foreign_keys JSONB,
    indexes JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create generated_queries table
CREATE TABLE IF NOT EXISTS generated_queries (
    id VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    datasource_id VARCHAR(36) NOT NULL REFERENCES datasources(id),
    natural_language_query TEXT NOT NULL,
    generated_sql TEXT NOT NULL,
    query_type VARCHAR(50),
    tables_used JSONB,
    estimated_rows INTEGER,
    confidence_score FLOAT,
    explanation TEXT,
    warnings JSONB,
    user_id VARCHAR(36) NOT NULL,
    organization_id VARCHAR(36) NOT NULL,
    canvas_id VARCHAR(36),
    dashboard_id VARCHAR(36),
    component_id VARCHAR(36),
    llm_model VARCHAR(100),
    llm_tokens_used INTEGER,
    llm_response_time_ms FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create query_executions table
CREATE TABLE IF NOT EXISTS query_executions (
    id VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    query_id VARCHAR(36) REFERENCES generated_queries(id),
    datasource_id VARCHAR(36) NOT NULL REFERENCES datasources(id),
    sql_query TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    rows_returned INTEGER DEFAULT 0,
    execution_time_ms FLOAT,
    from_cache BOOLEAN DEFAULT FALSE,
    cached_at TIMESTAMP,
    error_message TEXT,
    user_id VARCHAR(36) NOT NULL,
    organization_id VARCHAR(36) NOT NULL,
    canvas_id VARCHAR(36),
    dashboard_id VARCHAR(36),
    component_id VARCHAR(36),
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_datasource_org ON datasources(organization_id);
CREATE INDEX IF NOT EXISTS idx_datasource_status ON datasources(status);
CREATE INDEX IF NOT EXISTS idx_schema_datasource ON datasource_schemas(datasource_id);
CREATE INDEX IF NOT EXISTS idx_schema_table ON datasource_schemas(datasource_id, table_name);
CREATE INDEX IF NOT EXISTS idx_query_datasource ON generated_queries(datasource_id);
CREATE INDEX IF NOT EXISTS idx_query_user ON generated_queries(user_id);
CREATE INDEX IF NOT EXISTS idx_query_org ON generated_queries(organization_id);
CREATE INDEX IF NOT EXISTS idx_query_canvas ON generated_queries(canvas_id);
CREATE INDEX IF NOT EXISTS idx_execution_query ON query_executions(query_id);
CREATE INDEX IF NOT EXISTS idx_execution_user ON query_executions(user_id);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_datasources_updated_at BEFORE UPDATE ON datasources
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_datasource_schemas_updated_at BEFORE UPDATE ON datasource_schemas
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_generated_queries_updated_at BEFORE UPDATE ON generated_queries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert sample datasource for testing (connects to the MongoDB container)
INSERT INTO datasources (
    id,
    name,
    description,
    db_type,
    host,
    port,
    database,
    username,
    password_encrypted,
    status,
    organization_id,
    schema_ingested
) VALUES (
    'sample-mongodb-001',
    'Test MongoDB',
    'Sample MongoDB datasource for testing multi-datasource queries',
    'mongodb',
    'mongodb',
    27017,
    'test_db',
    'admin',
    'admin_password',
    'active',
    'default-org-001',
    FALSE
) ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE datasources IS 'Stores datasource connection information';
COMMENT ON TABLE datasource_schemas IS 'Stores ingested database schema metadata';
COMMENT ON TABLE generated_queries IS 'Stores LLM-generated queries from natural language';
COMMENT ON TABLE query_executions IS 'Stores query execution history and results';
