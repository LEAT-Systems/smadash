# Modular FastAPI Project

A modular FastAPI application with clean separation of concerns.

## Project Structure

```
.
├── app/                     # Application package
│   ├── __init__.py         # Init file
│   ├── main.py             # FastAPI application entry point
│   ├── api/                # API package
│   │   ├── __init__.py     
│   │   ├── dependencies.py # API dependencies
│   │   ├── routes/        # API route modules
│   │   │   └── __init__.py
│   │   ├── db/           # Database related code
│   │   │   ├── __init__.py
│   │   │   ├── base.py   # Base DB setup
│   │   │   └── session.py # DB session management
│   │   ├── models/       # Database models
│   │   │   └── __init__.py
│   │   ├── schemas/      # Pydantic schemas for request/response
│   │   │   └── __init__.py
│   │   └── services/     # Business logic services
│   │       ├── __init__.py
│   │       └── item_service.py
│   ├── agents/             # Langchain agents code
│   │   ├── __init__.py
└── requirements.txt        # Project dependencies
```
```
    graph TD
      %%=== Onboarding Phase ===%%
      subgraph Onboarding
        A1[🔍 Schema Ingestor]
        A2[🔄 Schema Normalizer]
        A3[📚 Central Schema Store] -> MongoDB
      end
      A1 --> A2
      A2 --> A3
    
      %%=== Query Intake & Planning ===%%
      subgraph QueryFlow
        U[💬 User NL Query] 
        QP[🤖 Query Parser (LLM)]
        PL[🧩 Query Planner]
        DR[🚦 DB Adapter Router]
        XE[⚡ Query Executor]
        CA[🗄️ Cache Layer]
      end
      U --> QP
      A3 --> QP
      QP --> PL
      PL --> DR
      DR --> XE
      XE --> CA
      CA --> PL       %% cache hit shortcut
      XE --> RS[📊 Raw Result Set]
    
      %%=== Post-Processing & Visualization ===%%
      subgraph PostProcess
        RF[📝 Result Formatter (LLM)]
        VR[🎨 Viz Recommender (LLM)]
        RN[🖥️ Renderer]
      end
      RS --> RF
      RS --> VR
      RF --> RN
      VR --> RN
    
      %%=== Shared Services ===%%
      subgraph Services
        LM[🔍 Logging & Metrics]
        EH[🚨 Error Handler]
        AC[🔐 Auth & Access Control]
      end
    
      %% service hooks
      Onboarding & QueryFlow & PostProcess --> LM
      Onboarding & QueryFlow & PostProcess --> EH
      U & XE & RN --> AC
    
      %% annotate adapters
      subgraph DR internal
        DR1[SQL Adapter]
        DR2[NoSQL Adapter]
        DR3[GraphDB Adapter]
      end
      DR --> DR1
      DR --> DR2
      DR --> DR3
      
    Agentic Dashboard: End-to-End LangGraph Architecture
Below is a complete LangGraph design capturing every scenario—from onboarding a brand-new database through multi-DB query execution to LLM-driven formatting and visualization recommendations.
graph TD
  %%=== Onboarding Phase ===%%
  subgraph Onboarding
    A1[🔍 Schema Ingestor]
    A2[🔄 Schema Normalizer]
    A3[📚 Central Schema Store]
  end
  A1 --> A2
  A2 --> A3

  %%=== Query Intake & Planning ===%%
  subgraph QueryFlow
    U[💬 User NL Query] 
    QP[🤖 Query Parser (LLM)]
    PL[🧩 Query Planner]
    DR[🚦 DB Adapter Router]
    XE[⚡ Query Executor]
    CA[🗄️ Cache Layer]
  end
  U --> QP
  A3 --> QP
  QP --> PL
  PL --> DR
  DR --> XE
  XE --> CA
  CA --> PL       %% cache hit shortcut
  XE --> RS[📊 Raw Result Set]

  %%=== Post-Processing & Visualization ===%%
  subgraph PostProcess
    RF[📝 Result Formatter (LLM)]
    VR[🎨 Viz Recommender (LLM)]
    RN[🖥️ Renderer]
  end
  RS --> RF
  RS --> VR
  RF --> RN
  VR --> RN

  %%=== Shared Services ===%%
  subgraph Services
    LM[🔍 Logging & Metrics]
    EH[🚨 Error Handler]
    AC[🔐 Auth & Access Control]
  end

  %% service hooks
  Onboarding & QueryFlow & PostProcess --> LM
  Onboarding & QueryFlow & PostProcess --> EH
  U & XE & RN --> AC

  %% annotate adapters
  subgraph DR internal
    DR1[SQL Adapter]
    DR2[NoSQL Adapter]
    DR3[GraphDB Adapter]
  end
  DR --> DR1
  DR --> DR2
  DR --> DR3



Component Responsibilities
1. Schema Ingestion & Storage
- Schema Ingestor: Connects to any database type; introspects tables/collections, indexes, relationships.
- Schema Normalizer: Converts diverse DB metadata into a unified JSON graph.
- Central Schema Store: Persists normalized schemas for rapid lookup during query planning.
2. Query Intake & Planning
- User NL Query: Free-form text or voice input.
- Query Parser (LLM): Uses schema context to translate NL into an abstract query plan or AST.
- Query Planner: Optimizes the AST—injects projections, filters, joins; considers cost estimates.
- DB Adapter Router: Routes planned queries to:
    - SQL Adapter (Postgres, MySQL, Oracle…)
    - NoSQL Adapter (MongoDB, DynamoDB…)
    - GraphDB Adapter (Neo4j, JanusGraph…)
- Query Executor: Runs the adapter-specific query, streams results.
- Cache Layer: Caches frequent queries and sub-results for ultra-fast responses.
3. Result Formatting & Visualization
- Result Formatter (LLM): Summarizes or restructures raw rows/documents into JSON structures tailored for visualization.
- Viz Recommender (LLM): Chooses optimal display (bar, line, pie, table, heatmap) based on data characteristics and user preferences.
- Renderer: Renders chosen chart/table in the dashboard front end, supports dynamic drill-downs.
4. Shared Services
- Logging & Metrics: Tracks node execution times, LLM costs, error rates, query patterns.
- Error Handler: Catches failures (parsing, execution, LLM timeouts) and triggers fallback flows or user prompts.
- Auth & Access Control: Enforces data-level security; multi-tenant isolation; row-/column-level permissions.

How All Scenarios Are Handled
- New Database Onboarding: Full introspection → stored schema → available for immediate queries.
- Hybrid Environments: Router picks correct adapter per query; normalized schema bridges SQL/NoSQL/Graph.
- Cache Hits: Planner checks Cache Layer before sending to executor.
- Complex Joins & Aggregations: Planner injects best join strategies; adapter uses native index hints.
- Fallbacks: On parsing or execution error, Error Handler invites user to rephrase or simplifies the query.


```
## Setup

1. Create a virtual environment:
```bash
python -m venv .venv
```

2. Activate the virtual environment:
```bash
# On Windows
.venv\Scripts\activate
# On Unix or MacOS
source .venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Change directory
```bash
cd app
```
## Running the Application
```bash
uvicorn app.main:app --reload
```

The API will be available at http://localhost:8000

API documentation is available at:
- http://localhost:8000/docs (Swagger UI)
- http://localhost:8000/redoc (ReDoc)
