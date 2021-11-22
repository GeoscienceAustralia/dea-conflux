"""Database management for the interstitial database.

Matthew Alger
Geoscience Australia
2021
"""

import os

from sqlalchemy import (
    Column, DateTime, Integer, String, Float,
    ForeignKey)
from sqlalchemy import create_engine
from sqlalchemy.future import Engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.expression import ClauseElement

WaterbodyBase = declarative_base()


def get_engine_sqlite(path) -> Engine:
    """Get a SQLite on-disk database engine."""
    return create_engine(
        f'sqlite+pysqlite:///{path}',
        echo=True, future=True)


def get_engine_inmem() -> Engine:
    """Get a SQLite in-memory database engine."""
    return create_engine(
        'sqlite+pysqlite:///:memory:',
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


class Waterbody(WaterbodyBase):
    __tablename__ = 'waterbodies'
    wb_id = Column(Integer, primary_key=True)
    wb_name = Column(String)  # elsewhere referred to as a "waterbody ID"
    geofabric_name = Column(String)
    centroid_lat = Column(Float)
    centroid_lon = Column(Float)
    
    def __repr__(self):
        return f'<Waterbody wb_id={self.wb_id}, ' +\
               f'wb_name={self.wb_name}, ...>'


class WaterbodyObservation(WaterbodyBase):
    __tablename__ = 'waterbody_observations'
    obs_id = Column(Integer, primary_key=True)
    wb_id = Column(Integer, ForeignKey("waterbodies.wb_id"))
    px_wet = Column(Integer)
    pc_wet = Column(Float)
    pc_missing = Column(Float)
    platform = Column(String(3))
    date = Column(DateTime)
    
    def __repr__(self):
        return f'<WaterbodyObservation obs_id={self.obs_id}, wb_id={self.wb_id}, ' +\
               f'date={self.date}, ...>'


def create_waterbody_tables(engine: Engine):
    """Create all waterbody tables."""
    return WaterbodyBase.metadata.create_all(engine)


def get_or_create(session, model, defaults=None, **kwargs):
    """Query a row or create it if it doesn't exist."""
    # https://stackoverflow.com/questions/2546207/
    # does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
    instance = session.query(model).filter_by(**kwargs).one_or_none()
    if instance:
        return instance, False
    else:
        params = {k: v for k, v in kwargs.items() if not isinstance(v, ClauseElement)}
        params.update(defaults or {})
        instance = model(**params)
        try:
            session.add(instance)
            session.commit()
        except Exception:
            # The actual exception depends on the specific database
            # so we catch all exceptions. This is similar to the
            # official documentation:
            # https://docs.sqlalchemy.org/en/latest/orm/session_transaction.html
            session.rollback()
            instance = session.query(model).filter_by(**kwargs).one()
            return instance, False
        else:
            return instance, True
