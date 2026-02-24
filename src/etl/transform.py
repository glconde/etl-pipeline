from __future__ import annotations

import json
import re
from datetime import datetime, date
from typing import Any

import pandas as pd


_NA = {"N/A", "", None}


def _none_if_na(value: Any) -> Any | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in _NA:
        return None
    return value


def _parse_int_from_runtime(runtime: Any) -> int | None:
    runtime = _none_if_na(runtime)
    if runtime is None:
        return None
    # e.g. "142 min"
    m = re.search(r"(\d+)", str(runtime))
    return int(m.group(1)) if m else None


def _parse_int_with_commas(value: Any) -> int | None:
    value = _none_if_na(value)
    if value is None:
        return None
    s = str(value).replace(",", "").strip()
    return int(s) if s.isdigit() else None


def _parse_box_office(value: Any) -> int | None:
    value = _none_if_na(value)
    if value is None:
        return None
    # e.g. "$123,456,789"
    s = str(value).strip().replace("$", "").replace(",", "")
    return int(s) if s.isdigit() else None


def _parse_float(value: Any) -> float | None:
    value = _none_if_na(value)
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def _parse_released_date(value: Any) -> date | None:
    value = _none_if_na(value)
    if value is None:
        return None
    # OMDb format usually "01 Jan 2000"
    try:
        return datetime.strptime(str(value).strip(), "%d %b %Y").date()
    except ValueError:
        return None


def transform(raw: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Converts raw OMDb JSON list -> normalized dataframe matching movies table.
    """
    rows: list[dict[str, Any]] = []
    for item in raw:
        imdb_id = _none_if_na(item.get("imdbID"))
        title = _none_if_na(item.get("Title"))

        if not imdb_id or not title:
            # must have primary key + title
            continue

        year = _none_if_na(item.get("Year"))
        year_int: int | None = None
        if isinstance(year, str):
            # sometimes "2014–2019" or "2014–"
            m = re.match(r"^\d{4}", year.strip())
            year_int = int(m.group(0)) if m else None
        elif isinstance(year, int):
            year_int = year

        row = {
            "imdb_id": str(imdb_id),
            "title": str(title),
            "year": year_int,
            "rated": _none_if_na(item.get("Rated")),
            "runtime_minutes": _parse_int_from_runtime(item.get("Runtime")),
            "genre": _none_if_na(item.get("Genre")),
            "director": _none_if_na(item.get("Director")),
            "actors": _none_if_na(item.get("Actors")),
            "imdb_rating": _parse_float(item.get("imdbRating")),
            "imdb_votes": _parse_int_with_commas(item.get("imdbVotes")),
            "box_office": _parse_box_office(item.get("BoxOffice")),
            "released_date": _parse_released_date(item.get("Released")),
            # store full raw payload as JSON string (loader will CAST ... AS JSON)
            "raw_json": json.dumps(item, ensure_ascii=False),
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Ensure all expected columns exist (even if empty)
    expected = [
        "imdb_id",
        "title",
        "year",
        "rated",
        "runtime_minutes",
        "genre",
        "director",
        "actors",
        "imdb_rating",
        "imdb_votes",
        "box_office",
        "released_date",
        "raw_json",
    ]
    for col in expected:
        if col not in df.columns:
            df[col] = None

    # Basic type cleanup (keep None for SQL NULL)
    if not df.empty:
        df["imdb_id"] = df["imdb_id"].astype(str)
        df["title"] = df["title"].astype(str)

    return df[expected]