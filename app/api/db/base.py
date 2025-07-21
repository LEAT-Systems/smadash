from sqlalchemy.ext.declarative import declarative_base, declared_attr


class CustomBase:
    """Custom base class for SQLAlchemy models."""

    # Generate __tablename__ automatically
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    # Add common columns/methods here
    # id = Column(Integer, primary_key=True, index=True)
    # created_at = Column(DateTime, default=datetime.utcnow)
    # updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


Base = declarative_base(cls=CustomBase)
