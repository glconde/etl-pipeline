from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_engine(db_url: str) -> Engine:
    # pool_pre_ping helps avoid stale connections
    return create_engine(db_url, pool_pre_ping=True, future=True)


def ping_db(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))