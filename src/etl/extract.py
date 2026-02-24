from __future__ import annotations

import time
from typing import Any

import requests


class OmdbError(RuntimeError):
    pass


def fetch_movie_by_id(
    imdb_id: str,
    *,
    api_key: str,
    base_url: str,
    timeout_seconds: float,
    max_retries: int,
) -> dict[str, Any]:
    """
    Calls OMDb: ?apikey=...&i=...&plot=short
    Raises OmdbError on API-level failures (Response=False).
    Retries on transient network errors.
    """
    params = {"apikey": api_key, "i": imdb_id, "plot": "short"}

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(base_url, params=params, timeout=timeout_seconds)
            resp.raise_for_status()
            data = resp.json()

            # OMDb signals logical errors like invalid id via Response=False
            if str(data.get("Response", "")).lower() == "false":
                raise OmdbError(f"OMDb error for {imdb_id}: {data.get('Error', 'Unknown error')}")
            return data

        except (requests.RequestException, ValueError) as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(0.6 * (attempt + 1))  # tiny backoff
                continue
            raise RuntimeError(f"Network/parse failure fetching {imdb_id}: {e}") from e

    # should never reach
    raise RuntimeError(f"Failed fetching {imdb_id}: {last_exc}")


def extract(
    imdb_ids: list[str],
    *,
    api_key: str,
    base_url: str,
    sleep_seconds: float,
    timeout_seconds: float,
    max_retries: int,
) -> list[dict[str, Any]]:
    """
    Extracts raw OMDb JSON for each imdb_id.
    """
    cleaned: list[str] = []
    seen: set[str] = set()
    for x in imdb_ids:
        v = x.strip()
        if not v:
            continue
        if v in seen:
            continue
        seen.add(v)
        cleaned.append(v)

    raw: list[dict[str, Any]] = []
    for i, imdb_id in enumerate(cleaned):
        if i > 0 and sleep_seconds > 0:
            time.sleep(sleep_seconds)

        raw.append(
            fetch_movie_by_id(
                imdb_id,
                api_key=api_key,
                base_url=base_url,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
            )
        )
    return raw