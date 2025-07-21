from sqlalchemy.orm import Session
from typing import Optional, List
from sqlalchemy import or_
from db.schema.alpha import AlphaBase
from db.model.alpha import Alpha


def create_alpha(db: Session, alpha: AlphaBase) -> Alpha:
    db_alpha = Alpha(**alpha.model_dump())
    db.add(db_alpha)
    db.commit()
    db.refresh(db_alpha)
    return db_alpha

def get_alphas(
    db: Session, 
    skip: int = 0, 
    limit: int = 10000,
    status: Optional[list[str]] = []
) -> List[Alpha]:
    query = db.query(Alpha)
    if status:
        query = query.filter(
            or_(
                *[ Alpha.status == s for s in status ]
            )
        )
    return query.offset(skip).limit(limit).all()

def upsert_alpha(
    db: Session,
    alpha: AlphaBase,
    refresh_status: bool = True
) -> Optional[Alpha]:
    db_alpha = db.query(Alpha).filter(Alpha.alpha_id == alpha.alpha_id).first()
    if db_alpha:
        update_data = alpha.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            # don't set status field
            if not refresh_status and key == 'status':
                continue
            setattr(db_alpha, key, value)
        
        db.commit()
        db.refresh(db_alpha)
    else:
        create_alpha(db, alpha)
    return db_alpha

def delete_alpha(db: Session, simulation_id: int) -> bool:
    db_alpha = db.query(Alpha).filter(Alpha.id == simulation_id).first()
    if db_alpha:
        db.delete(db_alpha)
        db.commit()
        return True
    return False

def get_alpha_by_alpha_id(db: Session, alpha_id: str) -> List[Alpha]:
    return db.query(Alpha).filter(Alpha.alpha_id == alpha_id).all()

