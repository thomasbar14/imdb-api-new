-- Reference schema (ETL script creates and manages tables automatically)
-- Run this manually only if you want to pre-create tables before the first ETL run.

CREATE TABLE IF NOT EXISTS titles (
    id SERIAL PRIMARY KEY,
    tconst VARCHAR(10) UNIQUE NOT NULL,
    title_type VARCHAR(20) NOT NULL,
    primary_title TEXT NOT NULL,
    start_year SMALLINT,
    runtime_minutes SMALLINT,
    genres TEXT
);

CREATE TABLE IF NOT EXISTS episodes (
    id SERIAL PRIMARY KEY,
    tconst VARCHAR(10) UNIQUE NOT NULL,
    parent_tconst VARCHAR(10) NOT NULL,
    season_number SMALLINT,
    episode_number SMALLINT
);

CREATE TABLE IF NOT EXISTS ratings (
    tconst VARCHAR(10) PRIMARY KEY,
    average_rating REAL NOT NULL,
    num_votes INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_titles_type ON titles(title_type);
CREATE INDEX IF NOT EXISTS idx_episodes_parent ON episodes(parent_tconst);
CREATE INDEX IF NOT EXISTS idx_titles_search ON titles USING gin(to_tsvector('english', primary_title));
