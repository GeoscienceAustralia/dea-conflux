"""Database management for the interstitial database.

Matthew Alger
Geoscience Australia
2021
"""

import os

from sqlalchemy import (
    Column, DateTime, Integer, String, Float, ForeignKey)
from sqlalchemy import create_engine
from sqlalchemy.future import Engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()


def get_engine_sqlite(path) -> Engine:
    """Get a SQLite on-disk database engine."""
    return create_engine(
        f'sqlite+pysqlite:///{path}',
        echo=True, future=True)


def get_engine_waterbodies() -> Engine:
    """Get the Waterbodies database engine.
    
    References environment variables WATERBODIES_DB_USER,
    WATERBODIES_DB_PASS, WATERBODIES_DB_HOST,
    WATERBODIES_DB_PORT, and WATERBODIES_DB_NAME.
    HOST and PORT default to localhost and 5432 respectively.
    """
    user = os.environ.get('WATERBODIES_DB_USER')
    passw = os.environ.get('WATERBODIES_DB_PASS')
    host = os.environ.get('WATERBODIES_DB_HOST', 'localhost')
    port = os.environ.get('WATERBODIES_DB_PORT', 5432)
    name = os.environ.get('WATERBODIES_DB_NAME')
    uri = f'postgresql+psycopg2://{user}:{passw}@{host}:{port}/{name}'
    return create_engine(uri, future=True)


class Waterbody(Base):
    __tablename__ = 'waterbodies'
    wb_id = Column(Integer, primary_key=True)
    wb_name = Column(String)  # elsewhere referred to as a "waterbody ID"
    geofabric_name = Column(String)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    
    def __repr__(self):
        return f'<Waterbody id={self.id}, wb_id={self.wb_id}, ...>'


class WaterbodyObservation(Base):
    __tablename__ = 'waterbody_observations'
    obs_id = Column(Integer, primary_key=True)
    wb_id = Column(Integer, ForeignKey("waterbodies.wb_id")),
    px_wet = Column(Integer)
    pc_wet = Column(Float)
    pc_missing = Column(Float)
    platform = Column(String(3))
    date = Column(DateTime)
    
    def __repr__(self):
        return f'<WaterbodyObservation id={self.id}, wb_id={self.wb_id}, ' +\
                f'date={self.date}, ...>'
