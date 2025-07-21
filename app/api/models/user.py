from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import relationship

from ..db.base import Base


class User(Base):
    """User model."""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    username = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    is_superuser = Column(Boolean, default=False)

    # Relationships
    # items = relationship("Item", back_populates="owner", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User {self.email}>"
