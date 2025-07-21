# from typing import List, Optional
#
# from sqlalchemy.orm import Session
#
# from ..models.item import Item
# from ..schemas.item import ItemCreate, ItemUpdate
#
#
# class ItemService:
#     """Item service for business logic operations."""
#
#     def __init__(self, db: Session):
#         self.db = db
#
#     def get_item(self, item_id: int) -> Optional[Item]:
#         """Get item by ID."""
#         return self.db.query(Item).filter(Item.id == item_id).first()
#
#     def get_items(self, skip: int = 0, limit: int = 100) -> List[Item]:
#         """Get all items."""
#         return self.db.query(Item).offset(skip).limit(limit).all()
#
#     def get_user_items(self, user_id: int, skip: int = 0, limit: int = 100) -> List[Item]:
#         """Get items for a specific user."""
#         return (
#             self.db.query(Item)
#             .filter(Item.owner_id == user_id)
#             .offset(skip)
#             .limit(limit)
#             .all()
#         )
#
#     def create_item(self, item_data: ItemCreate, user_id: int) -> Item:
#         """Create a new item."""
#         # Create item object
#         db_item = Item(
#             **item_data.dict(),
#             owner_id=user_id,
#         )
#
#         # Save to database
#         self.db.add(db_item)
#         self.db.commit()
#         self.db.refresh(db_item)
#
#         return db_item
#
#     def update_item(self, item_id: int, item_data: ItemUpdate) -> Item:
#         """Update item."""
#         # Get item
#         db_item = self.get_item(item_id=item_id)
#
#         # Update item data
#         update_data = item_data.dict(exclude_unset=True)
#
#         # Update item attributes
#         for field, value in update_data.items():
#             setattr(db_item, field, value)
#
#         # Save to database
#         self.db.commit()
#         self.db.refresh(db_item)
#
#         return db_item
#
#     def delete_item(self, item_id: int) -> None:
#         """Delete item."""
#         # Get item
#         db_item = self.get_item(item_id=item_id)
#
#         # Delete item
#         self.db.delete(db_item)
#         self.db.commit()
