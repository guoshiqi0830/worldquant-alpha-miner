from sqlalchemy import Column, Integer, String, Text, Numeric, TIMESTAMP
from sqlalchemy.sql import func
from db.database import Base

class DataField(Base):
    __tablename__ = "data_field"

    id = Column(Integer, primary_key=True, autoincrement=True)
    field_name = Column(String(100))
    description = Column(Text)
    dataset_id = Column(String(10))
    dataset_name = Column(String(100))
    category_id = Column(String(10))
    category_name = Column(String(100))
    subcategory_id = Column(String(10))
    subcategory_name = Column(String(100))
    region = Column(String(10))
    delay = Column(Integer)
    universe = Column(String(20))
    type = Column(String(20))
    coverage = Column(Numeric)
    user_count = Column(Integer)
    alpha_count = Column(Integer)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
