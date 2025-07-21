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