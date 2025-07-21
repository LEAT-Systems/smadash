from typing import List

from fastapi import APIRouter, Depends, HTTPException, status

from ..utils.dependencies import get_current_user, get_user_service
from ..schemas.user import User, UserCreate, UserUpdate
from ..services.user_service import UserService

router = APIRouter(tags=["users"], prefix="/users")


@router.get("/", response_model=List[User])
def get_users(skip: int = 0, limit: int = 100, user_service: UserService = Depends(get_user_service)):
    """Get all users."""
    return user_service.get_users(skip=skip, limit=limit)


@router.post("/", response_model=User, status_code=status.HTTP_201_CREATED)
def create_user(user_data: UserCreate, user_service: UserService = Depends(get_user_service)):
    """Create a new user."""
    user = user_service.get_user_by_email(email=user_data.email)
    if user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )
    return user_service.create_user(user_data=user_data)


@router.get("/me", response_model=User)
def get_current_user_info(current_user=Depends(get_current_user)):
    """Get current user info."""
    return current_user


@router.get("/{user_id}", response_model=User)
def get_user(user_id: int, user_service: UserService = Depends(get_user_service)):
    """Get user by ID."""
    user = user_service.get_user(user_id=user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return user


@router.put("/{user_id}", response_model=User)
def update_user(
    user_id: int,
    user_data: UserUpdate,
    current_user=Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
):
    """Update user."""
    # Check if the user is updating their own profile
    if current_user["id"] != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions",
        )
    user = user_service.get_user(user_id=user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return user_service.update_user(user_id=user_id, user_data=user_data)


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(
    user_id: int,
    current_user=Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
):
    """Delete user."""
    # Only allow users to delete their own account (or implement admin role check)
    if current_user["id"] != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions",
        )
    user = user_service.get_user(user_id=user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    user_service.delete_user(user_id=user_id)
