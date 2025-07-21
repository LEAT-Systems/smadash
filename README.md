# Modular FastAPI Project

A modular FastAPI application with clean separation of concerns.

## Project Structure

```
.
├── app/                  # Application package
│   ├── __init__.py      # Init file
│   ├── main.py          # FastAPI application entry point
│   ├── api/             # API routes package
│   │   ├── __init__.py  
│   │   ├── dependencies.py  # API dependencies
│   │   └── routes/      # API route modules
│   ├── core/            # Core application code
│   │   ├── __init__.py
│   │   ├── config.py    # Application configuration
│   │   └── security.py  # Security utilities
│   ├── db/              # Database related code
│   │   ├── __init__.py
│   │   ├── base.py      # Base DB setup
│   │   └── session.py   # DB session management
│   ├── models/          # Database models
│   │   └── __init__.py
│   ├── schemas/         # Pydantic schemas for request/response
│   │   └── __init__.py
│   └── services/        # Business logic services
│       └── __init__.py
└── requirements.txt     # Project dependencies
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

## Running the Application

```bash
uvicorn app.main:app --reload
```

The API will be available at http://localhost:8000

API documentation is available at:
- http://localhost:8000/docs (Swagger UI)
- http://localhost:8000/redoc (ReDoc)
