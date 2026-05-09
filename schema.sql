-- Optimized schema (ETL script creates and manages tables automatically)
-- No SERIAL id columns — tconst is the natural primary key.
-- No heavy GIN index on staging rebuilds; search uses ILIKE for simplicity.

CREATE TABLE IF NOT EXISTS titles (
    tconst VARCHAR(10) PRIMARY KEY,
    title_type VARCHAR(20) NOT NULL,
    primary_title TEXT NOT NULL,
    start_year SMALLINT,
    runtime_minutes INTEGER,
    genres TEXT
);

CREATE TABLE IF NOT EXISTS episodes (
    tconst VARCHAR(10) PRIMARY KEY,
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
