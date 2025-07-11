from sqlalchemy.orm import Session
from typing import Optional, List
from sqlalchemy import or_, and_
from db.schema.simulation import SimulationCreate, SimulationUpdate
from db.model.simulation import Simulation


def create_simulation(db: Session, simulation: SimulationCreate) -> Simulation:
    db_simulation = Simulation(**simulation.model_dump())
    db.add(db_simulation)
    db.commit()
    db.refresh(db_simulation)
    return db_simulation

def get_simulation(db: Session, simulation_id: int) -> Optional[Simulation]:
    return db.query(Simulation).filter(Simulation.id == simulation_id).first()

def get_simulations(
    db: Session, 
    skip: int = 0, 
    limit: int = 10000,
    status: Optional[list[str]] = []
) -> List[Simulation]:
    query = db.query(Simulation)
    if status:
        query = query.filter(
            or_(
                *[ Simulation.status == s for s in status ]
            )
        )
    return query.offset(skip).limit(limit).all()

def upsert_simulation(
    db: Session,
    simulation: SimulationUpdate
) -> Optional[Simulation]:
    db_simulation = db.query(Simulation).filter(Simulation.alpha_id == simulation.alpha_id).first()
    if db_simulation:
        update_data = simulation.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_simulation, key, value)
        
        db.commit()
        db.refresh(db_simulation)
    else:
        create_simulation(db, simulation)
    return db_simulation

def delete_simulation(db: Session, simulation_id: int) -> bool:
    db_simulation = db.query(Simulation).filter(Simulation.id == simulation_id).first()
    if db_simulation:
        db.delete(db_simulation)
        db.commit()
        return True
    return False


def get_simulations_by_alpha_id(db: Session, alpha_id: str) -> List[Simulation]:
    return db.query(Simulation).filter(Simulation.alpha_id == alpha_id).all()

def search_simulations(
    db: Session,
    keyword: Optional[str] = None,
    simulation_type: Optional[str] = None,
    region: Optional[str] = None,
    min_delay: Optional[int] = None,
    max_delay: Optional[int] = None
) -> List[Simulation]:
    query = db.query(Simulation)
    
    if keyword:
        query = query.filter(
            or_(
                Simulation.simulation_id.ilike(f"%{keyword}%"),
                Simulation.alpha.ilike(f"%{keyword}%")
            )
        )
    
    if simulation_type:
        query = query.filter(Simulation.type == simulation_type)
    
    if region:
        query = query.filter(Simulation.region == region)
    
    if min_delay is not None:
        query = query.filter(Simulation.delay >= min_delay)
    
    if max_delay is not None:
        query = query.filter(Simulation.delay <= max_delay)
    
    return query.order_by(Simulation.created_at.desc()).all()

def bulk_create_simulations(db: Session, simulations: List[SimulationCreate]) -> List[Simulation]:
    db_simulations = [Simulation(**simulation.model_dump()) for simulation in simulations]
    db.bulk_save_objects(db_simulations)
    db.commit()
    return db_simulations

def get_simulation_by_simulation_id(db: Session, simulation_id_str: str) -> Optional[Simulation]:
    return db.query(Simulation).filter(Simulation.simulation_id == simulation_id_str).first()
