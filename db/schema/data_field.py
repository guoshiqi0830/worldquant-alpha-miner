from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class DataFieldBase(BaseModel):
    field_name: str
    description: Optional[str] = None
    dataset_id: Optional[str] = None
    dataset_name: Optional[str] = None
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    subcategory_id: Optional[str] = None
    subcategory_name: Optional[str] = None
    region: Optional[str] = None
    delay: Optional[int] = None
    universe: Optional[str] = None
    type: Optional[str] = None
    coverage: Optional[float] = None
    user_count: Optional[int] = None
    alpha_count: Optional[int] = None


class DataField(DataFieldBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
