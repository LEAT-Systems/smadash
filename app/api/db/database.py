from ..models import *
from .base import Base
from .session import engine, SessionLocal

def create_tables():
    """Create all database tables."""
    Base.metadata.create_all(bind=engine)

def instantiate_db():
    """Initialize database tables."""
    create_tables()
    return True

def get_db():
    """
    Dependency function to get database session.
    Used by FastAPI to inject database sessions into route handlers.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()