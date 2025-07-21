from typing import List, Optional

from sqlalchemy.orm import Session

from ..utils.security import get_password_hash
from ..models.user import User
from ..schemas.user import UserCreate, UserUpdate


class UserService:
    """User service for business logic operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_user(self, user_id: int) -> Optional[User]:
        """Get user by ID."""
        return self.db.query(User).filter(User.id == user_id).first()

    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        return self.db.query(User).filter(User.email == email).first()

    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        return self.db.query(User).filter(User.username == username).first()

    def get_users(self, skip: int = 0, limit: int = 100) -> List[User]:
        """Get all users."""
        return self.db.query(User).offset(skip).limit(limit).all()

    def create_user(self, user_data: UserCreate) -> User:
        """Create a new user."""
        # Hash password
        hashed_password = get_password_hash(user_data.password)

        # Create user object
        db_user = User(
            email=user_data.email,
            username=user_data.username,
            hashed_password=hashed_password,
            is_active=True,
            is_superuser=False,
        )

        # Save to database
        self.db.add(db_user)
        self.db.commit()
        self.db.refresh(db_user)

        return db_user

    def update_user(self, user_id: int, user_data: UserUpdate) -> User:
        """Update user."""
        # Get user
        db_user = self.get_user(user_id=user_id)

        # Update user data
        update_data = user_data.dict(exclude_unset=True)

        # Hash password if provided
        if "password" in update_data:
            update_data["hashed_password"] = get_password_hash(update_data.pop("password"))

        # Update user attributes
        for field, value in update_data.items():
            setattr(db_user, field, value)

        # Save to database
        self.db.commit()
        self.db.refresh(db_user)

        return db_user

    def delete_user(self, user_id: int) -> None:
        """Delete user."""
        # Get user
        db_user = self.get_user(user_id=user_id)

        # Delete user
        self.db.delete(db_user)
        self.db.commit()
