from typing import Generator

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from app.api.db import SessionLocal
from app.api.services.user_service import UserService

# OAuth2 token URL
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"/api/v1/auth/login")

def get_db() -> Generator:
    """Get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(token: str = Depends(oauth2_scheme)):
    """Get current user from token."""
    # This is a placeholder. In a real application, you would verify the token
    # and get the user from the database.
    if token == "invalid_token":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # Return a mock user for now
    return {"id": 1, "username": "user", "email": "user@example.com"}

def get_user_service(db=Depends(get_db)):
    """Get user service."""
    return UserService(db)
