from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field

from app.api.schemas.item import Item


# Shared properties
class UserBase(BaseModel):
    """Shared User properties."""
    email: Optional[EmailStr] = None
    username: Optional[str] = None
    is_active: Optional[bool] = True


# Properties to receive via API on creation
class UserCreate(UserBase):
    """User creation schema."""
    email: EmailStr
    username: str
    password: str = Field(..., min_length=8)


# Properties to receive via API on update
class UserUpdate(UserBase):
    """User update schema."""
    password: Optional[str] = Field(None, min_length=8)


# Properties to return via API
class User(UserBase):
    """User response schema."""
    id: int
    is_active: bool
    is_superuser: bool
    items: List[Item] = []

    class Config:
        orm_mode = True
