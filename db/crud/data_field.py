from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, text
from typing import Optional, List
from db.schema.data_field import DataFieldBase, DataFieldBase
from db.model.data_field import DataField


def create_data_field(db: Session, data_field: DataFieldBase):
    db_data_field = DataField(**data_field.dict())
    db.add(db_data_field)
    db.commit()
    db.refresh(db_data_field)
    return db_data_field

def upsert_data_field(db: Session, data_field: DataFieldBase):
    db_data_field_list = get_data_fields_by_criteria(
        db,
        data_field.field_name, 
        data_field.dataset_id,
        data_field.category_id,
        data_field.subcategory_id,
        data_field.region,
        data_field.delay,
        data_field.universe,
        data_field.type
    )
    if db_data_field_list:
        db_data_field = db_data_field_list[0]
        update_data = data_field.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_data_field, key, value)
        
        db.commit()
        db.refresh(db_data_field)
    else:
        create_data_field(db, data_field)
    

def get_data_fields_by_criteria(
    db: Session,
    field_name: Optional[str] = None,
    dataset_id: Optional[str] = None,
    category_id: Optional[str] = None,
    subcategory_id: Optional[str] = None,
    region: Optional[str] = None,
    delay: Optional[int] = None,
    universe: Optional[str] = None,
    type: Optional[str] = None,
    skip: int = 0,
    limit: int = 1000,
    order_by: Optional[str] = ''
) -> List[DataField]:
    query = db.query(DataField)
    
    filters = []
    if field_name:
        filters.append(DataField.field_name == field_name)
    if dataset_id:
        filters.append(DataField.dataset_id == dataset_id)
    if category_id:
        filters.append(DataField.category_id == category_id)
    if subcategory_id:
        filters.append(DataField.subcategory_id == subcategory_id)
    if region:
        filters.append(DataField.region == region)
    if delay is not None:
        filters.append(DataField.delay == delay)
    if universe:
        filters.append(DataField.universe == universe)
    if type:
        filters.append(DataField.type == type)
    
    if filters:
        query = query.filter(and_(*filters))
    
    return query.order_by(text(order_by)).offset(skip).limit(limit).all()
