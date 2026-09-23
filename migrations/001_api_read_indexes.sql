-- One-off migration for an existing database. The daily ETL runs the
-- incremental path, which never recreates indexes, so apply this by hand:
--   psql "$DATABASE_URL" -f migrations/001_api_read_indexes.sql
--
-- Check free disk in the YugabyteDB Cloud console first. The trigram index
-- is the large one; run each step separately if disk is tight.

-- 1. Episodes: replace the single-column index with one ordered by
--    season/episode. The old index is dropped first so the node never holds
--    both copies; /series falls back to a slower plan for the few minutes
--    the build takes.
DROP INDEX IF EXISTS idx_episodes_parent;
CREATE INDEX idx_episodes_parent
    ON episodes (parent_tconst HASH, season_number ASC, episode_number ASC);

-- 2. Titles: trigram index so /search's ILIKE '%q%' doesn't scan every row.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_titles_title_trgm
    ON titles USING ybgin (primary_title gin_trgm_ops);

-- 3. Refresh planner stats so the new indexes are costed correctly.
ANALYZE titles;
ANALYZE episodes;
ANALYZE ratings;
