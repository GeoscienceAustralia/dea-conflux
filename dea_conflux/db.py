"""Database management for the interstitial database.

Matthew Alger
Geoscience Australia
2021
"""

from sqlalchemy import create_engine
from sqlalchemy.future import Engine

def get_engine_inmem() -> Engine:
    """Get an in-memory database engine."""
    return create_engine(
        'sqlite+pysqlite:///:memory:',
        echo=True, future=True)
