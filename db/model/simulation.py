from sqlalchemy import Column, Integer, String, Text, Numeric, TIMESTAMP, Boolean, Float
from sqlalchemy.sql import func
from db.database import Base


class Simulation(Base):
    __tablename__ = "simulation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alpha_id = Column(String(10))
    simulation_id = Column(String(100))
    alpha = Column(Text)
    type = Column(String(20))
    instrument_type = Column(String(20))
    region = Column(String(10))
    universe = Column(String(10))
    delay = Column(Integer)
    decay = Column(Integer)
    neutralization = Column(String(20))
    truncation = Column(Numeric)
    pasteurization = Column(String(10))
    unit_handling = Column(String(10))
    nan_handling = Column(String(10))
    max_trade = Column(String(10))
    language = Column(String(10))
    visualization = Column(Boolean)
    status = Column(String(10))
    sharpe = Column(Float)
    fitness = Column(Float)
    turnover = Column(Float)
    concentrated_weight = Column(Float)
    sub_universe_sharpe = Column(Float)
    self_correlation = Column(Float)
    drawdown = Column(Float)
    long_count = Column(Float)
    short_count = Column(Float)
    returns = Column(Float)
    margin = Column(Float)
    pnl = Column(Float)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
