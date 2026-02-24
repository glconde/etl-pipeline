from __future__ import annotations

import uuid
from typing import Any, cast

import pandas as pd
import math
from sqlalchemy import text
from sqlalchemy.engine import Engine

UPSERT_MOVIES_SQL = """
INSERT INTO movies (
  imdb_id, title, year, rated, runtime_minutes, genre, director, actors,
  imdb_rating, imdb_votes, box_office, released_date, raw_json
) VALUES (
  :imdb_id, :title, :year, :rated, :runtime_minutes, :genre, :director, :actors,
  :imdb_rating, :imdb_votes, :box_office, :released_date, CAST(:raw_json AS JSON)
)
ON DUPLICATE KEY UPDATE
  title = VALUES(title),
  year = VALUES(year),
  rated = VALUES(rated),
  runtime_minutes = VALUES(runtime_minutes),
  genre = VALUES(genre),
  director = VALUES(director),
  actors = VALUES(actors),
  imdb_rating = VALUES(imdb_rating),
  imdb_votes = VALUES(imdb_votes),
  box_office = VALUES(box_office),
  released_date = VALUES(released_date),
  raw_json = VALUES(raw_json);
"""

RUN_START_SQL = "INSERT INTO etl_runs (run_id) VALUES (:run_id);"

RUN_END_SQL = """
UPDATE etl_runs
SET
  finished_at = CURRENT_TIMESTAMP,
  records_extracted = :records_extracted,
  records_loaded = :records_loaded,
  `STATUS` = :status,
  error_message = :error_message
WHERE run_id = :run_id;
"""


def record_run_start(engine: Engine) -> str:
    run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(text(RUN_START_SQL), {"run_id": run_id})
    return run_id


def record_run_end(
    engine: Engine,
    *,
    run_id: str,
    records_extracted: int,
    records_loaded: int,
    status: str,
    error_message: str | None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(RUN_END_SQL),
            {
                "run_id": run_id,
                "records_extracted": records_extracted,
                "records_loaded": records_loaded,
                "status": status,
                "error_message": error_message,
            },
        )

def upsert_movies(engine: Engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    # build records first
    records = df.to_dict(orient="records")

    def _clean_value(key: str, v: Any) -> Any:
        # 1) convert any NaN -> None (MySQL NULL)
        if isinstance(v, float) and math.isnan(v):
            return None

        # 2) enforce integer storage for specific columns
        if key in {"box_office", "imdb_votes", "year", "runtime_minutes"}:
            if isinstance(v, float):
                return int(v) 
        return v

    # force keys to str and clean values
    params: list[dict[str, Any]] = []
    for row in records:
        cleaned: dict[str, Any] = {}
        for k, v in row.items():
            ks = str(k)
            cleaned[ks] = _clean_value(ks, v)
        params.append(cleaned)

    with engine.begin() as conn:
        conn.execute(text(UPSERT_MOVIES_SQL), params)

    return len(params)