CREATE TABLE IF NOT EXISTS movies (
    imdb_id VARCHAR(16) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    year INT NULL,
    rated VARCHAR(32) NULL,
    runtime_minutes INT NULL,
    genre VARCHAR(255) NULL,
    director VARCHAR(255) NULL,
    actors TEXT NULL,
    imdb_rating DECIMAL(3, 1) NULL,
    imdb_votes INT NULL,
    box_office BIGINT NULL,
    released_date DATE NULL,
    raw_json JSON NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id CHAR(36) PRIMARY KEY,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    records_extracted INT DEFAULT 0,
    records_loaded INT DEFAULT 0,
    STATUS VARCHAR(32) DEFAULT 'running',
    error_message TEXT NULL
);