from typing import List

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.dependencies import get_current_user
from app.api.schemas.item import Item, ItemCreate, ItemUpdate
from app.api.services.item_service import ItemService
from app.api.db import SessionLocal

router = APIRouter(tags=["items"], prefix="/items")


def get_item_service():
    """Get item service."""
    db = SessionLocal()
    try:
        yield ItemService(db)
    finally:
        db.close()


@router.get("/", response_model=List[Item])
def get_items(
    skip: int = 0,
    limit: int = 100,
    item_service: ItemService = Depends(get_item_service),
):
    """Get all items."""
    return item_service.get_items(skip=skip, limit=limit)


@router.post("/", response_model=Item, status_code=status.HTTP_201_CREATED)
def create_item(
    item_data: ItemCreate,
    current_user=Depends(get_current_user),
    item_service: ItemService = Depends(get_item_service),
):
    """Create a new item."""
    return item_service.create_item(item_data=item_data, user_id=current_user["id"])


@router.get("/{item_id}", response_model=Item)
def get_item(
    item_id: int,
    item_service: ItemService = Depends(get_item_service),
):
    """Get item by ID."""
    item = item_service.get_item(item_id=item_id)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found",
        )
    return item


@router.put("/{item_id}", response_model=Item)
def update_item(
    item_id: int,
    item_data: ItemUpdate,
    current_user=Depends(get_current_user),
    item_service: ItemService = Depends(get_item_service),
):
    """Update item."""
    item = item_service.get_item(item_id=item_id)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found",
        )
    # Check if the current user is the owner of the item
    if item.owner_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions",
        )
    return item_service.update_item(item_id=item_id, item_data=item_data)


@router.delete("/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_item(
    item_id: int,
    current_user=Depends(get_current_user),
    item_service: ItemService = Depends(get_item_service),
):
    """Delete item."""
    item = item_service.get_item(item_id=item_id)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found",
        )
    # Check if the current user is the owner of the item
    if item.owner_id != current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions",
        )
    item_service.delete_item(item_id=item_id)
