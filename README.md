# Movie Data ETL Pipeline (Python + MySQL + Docker)
**Tech Stack:** Python, Pandas, SQLAlchemy, MySQL, Docker

A modular ETL pipeline that ingests movie metadata from the OMDb API,
normalizes the data using pandas, and loads it into a Dockerized MySQL database
using idempotent upserts with execution tracking.

## Prerequisites

The following tools must be installed:

| Tool | Purpose |
|------|---------|
| Python 3.10+ | Runs ETL pipeline |
| Docker + Docker Compose | Runs MySQL database container |
| OMDb API Key | External data source |

### Verify Installations
```
python --version  
docker --version  
docker compose version
```

## Environment Setup

1. Create virtual environment:
```
   python -m venv .venv
   .\.venv\Scripts\activate
```
2. Install dependencies:
```
   pip install -r requirements.txt
```
3. Configure environment variables:
   Copy `.env.example` → `.env`
   Add your OMDb API key and database configuration.

## Architecture

Extract → Transform → Load

- Extract: OMDb API (requests, retry, rate limiting)
- Transform: pandas (data cleaning, normalization, type enforcement)
- Load: SQLAlchemy + MySQL (upsert via ON DUPLICATE KEY UPDATE)
- Orchestration: Python module entrypoint
- Infrastructure: Dockerized MySQL
- Configuration: Environment variables (.env)

```
input/imdb_ids.txt
        ↓
    extract.py
        ↓
    transform.py
        ↓
     load.py
        ↓
     MySQL (Docker)
```

## Features

- Idempotent upserts using primary key (imdb_id)
- Automatic run tracking via etl_runs table
- Environment-based configuration
- Dockerized database for reproducibility
- NaN-safe numeric handling for MySQL compatibility
- Structured project layout (src/etl separation)

## Running the Pipeline

1. Start the MySQL container:
   docker compose up -d

2. Add IMDb IDs (one per line) to:
   input/imdb_ids.txt

3. Run the pipeline:
   python -m src.etl.pipeline

Example output:
ETL success. run_id=... extracted=5 loaded=5

## Database Schema

### movies
Stores normalized movie metadata keyed by imdb_id (PRIMARY KEY).
Includes rating, vote count, runtime, box office, and raw JSON payload.

### etl_runs
Tracks each pipeline execution:
- start and end timestamps
- number of records extracted
- number of records loaded
- execution status (success / failed)

## Idempotency

The pipeline uses imdb_id as the primary key and performs
ON DUPLICATE KEY UPDATE during load.

Running the pipeline multiple times does not create duplicate records.
Existing rows are updated if data changes.

##### -GLConde