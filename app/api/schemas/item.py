from typing import Optional

from pydantic import BaseModel


# Shared properties
class ItemBase(BaseModel):
    """Shared Item properties."""
    title: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = True


# Properties to receive via API on creation
class ItemCreate(ItemBase):
    """Item creation schema."""
    title: str


# Properties to receive via API on update
class ItemUpdate(ItemBase):
    """Item update schema."""
    pass


# Properties to return via API
class Item(ItemBase):
    """Item response schema."""
    id: int
    owner_id: int
    title: str

    class Config:
        orm_mode = True
