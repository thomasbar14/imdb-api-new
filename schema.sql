-- Reference schema. The ETL (etl/main.py) creates and manages these tables
-- automatically; this mirrors what it builds on a fresh database.
-- No SERIAL id columns — tconst is the natural primary key.
-- PKs are range-sharded (ASC) rather than YB's default HASH so the ETL's
-- tconst-sorted COPY and per-chunk `tconst BETWEEN` diff use range scans.
-- Search uses ILIKE, accelerated by a pg_trgm GIN index.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS titles (
    tconst VARCHAR(10) NOT NULL,
    title_type VARCHAR(20) NOT NULL,
    primary_title TEXT NOT NULL,
    start_year SMALLINT,
    runtime_minutes INTEGER,
    genres TEXT,
    PRIMARY KEY (tconst ASC)
);

CREATE TABLE IF NOT EXISTS episodes (
    tconst VARCHAR(10) NOT NULL,
    parent_tconst VARCHAR(10) NOT NULL,
    season_number INTEGER,
    episode_number INTEGER,
    PRIMARY KEY (tconst ASC)
);

-- Only ratings for titles present in `titles` are stored.
CREATE TABLE IF NOT EXISTS ratings (
    tconst VARCHAR(10) NOT NULL,
    average_rating REAL NOT NULL,
    num_votes INTEGER NOT NULL,
    PRIMARY KEY (tconst ASC)
);

CREATE INDEX IF NOT EXISTS idx_episodes_parent
    ON episodes (parent_tconst HASH, season_number ASC, episode_number ASC);
CREATE INDEX IF NOT EXISTS idx_titles_title_trgm
    ON titles USING ybgin (primary_title gin_trgm_ops);
