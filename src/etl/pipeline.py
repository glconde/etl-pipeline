from __future__ import annotations

from pathlib import Path

from .config import get_settings
from .db import get_engine, ping_db
from .extract import extract
from .transform import transform
from .load import record_run_start, record_run_end, upsert_movies


def read_imdb_ids(path: str | Path) -> list[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")
    return [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def run_pipeline(imdb_ids: list[str]) -> None:
    s = get_settings()
    engine = get_engine(s.db_url)
    ping_db(engine)

    run_id = record_run_start(engine)
    extracted_count = 0
    loaded_count = 0

    try:
        raw = extract(
            imdb_ids,
            api_key=s.omdb_api_key,
            base_url=s.omdb_base_url,
            sleep_seconds=s.request_sleep_seconds,
            timeout_seconds=s.request_timeout_seconds,
            max_retries=s.max_retries,
        )
        extracted_count = len(raw)

        df = transform(raw)
        loaded_count = upsert_movies(engine, df)

        record_run_end(
            engine,
            run_id=run_id,
            records_extracted=extracted_count,
            records_loaded=loaded_count,
            status="success",
            error_message=None,
        )

        print(f"ETL success. run_id={run_id} extracted={extracted_count} loaded={loaded_count}")

    except Exception as e:
        record_run_end(
            engine,
            run_id=run_id,
            records_extracted=extracted_count,
            records_loaded=loaded_count,
            status="failed",
            error_message=str(e)[:2000],  # avoid gigantic stack traces in DB
        )
        raise


import sys

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "input/imdb_ids.txt"
    imdb_ids = read_imdb_ids(path)
    run_pipeline(imdb_ids)