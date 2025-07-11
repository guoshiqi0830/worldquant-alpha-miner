from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class SimulationBase(BaseModel):
    alpha_id: Optional[str] = None
    simulation_id: Optional[str] = None
    alpha: Optional[str] = None
    type: Optional[str] = None
    instrument_type: Optional[str] = None
    region: Optional[str] = None
    universe: Optional[str] = None
    delay: Optional[int] = None
    decay: Optional[int] = None
    neutralization: Optional[str] = None
    truncation: Optional[float] = None
    pasteurization: Optional[str] = None
    unit_handling: Optional[str] = None
    nan_handling: Optional[str] = None
    max_trade: Optional[str] = None
    language: Optional[str] = None
    visualization: Optional[bool] = None
    status: Optional[str] = None
    sharpe: Optional[float] = None
    fitness: Optional[float] = None
    turnover: Optional[float] = None
    concentrated_weight: Optional[float] = None
    sub_universe_sharpe: Optional[float] = None
    self_correlation: Optional[float] = None
    drawdown: Optional[float] = None
    long_count: Optional[float] = None
    short_count: Optional[float] = None
    returns: Optional[float] = None
    margin: Optional[float] = None
    pnl: Optional[float] = None

class SimulationCreate(SimulationBase):
    pass

class SimulationUpdate(SimulationBase):
    pass

class Simulation(SimulationBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
