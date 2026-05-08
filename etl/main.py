import csv
import gzip
import hashlib
import logging
import os
import shutil
import sys
import tempfile
import time
import traceback
import urllib.request
import psycopg2

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(levelname)s] %(message)s",
    stream=sys.stdout,
)

BASE_URL = "https://datasets.imdbws.com/"
FILES = {
    "basics": ("title.basics.tsv.gz", "basics"),
    "ratings": ("title.ratings.tsv.gz", "ratings"),
    "episode": ("title.episode.tsv.gz", "episode"),
}
KEEP_TYPES = {"tvEpisode", "tvSeries", "tvMiniSeries", "movie"}


def get_conn():
    url = os.environ["DATABASE_URL"]
    return psycopg2.connect(url)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def get_stored_hash(conn, name):
    with conn.cursor() as cur:
        cur.execute("SELECT hash FROM etl_state WHERE file_name = %s", (name,))
        row = cur.fetchone()
    return row[0] if row else None


def set_stored_hash(conn, name, h):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO etl_state (file_name, hash, updated_at)
               VALUES (%s, %s, NOW())
               ON CONFLICT (file_name) DO UPDATE SET hash = EXCLUDED.hash, updated_at = NOW()""",
            (name, h),
        )
    conn.commit()


def download_file(filename):
    url = BASE_URL + filename
    local_path = os.path.join(tempfile.gettempdir(), filename)
    print(f"[DOWNLOAD] Starting {url} ...")
    urllib.request.urlretrieve(url, local_path)
    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    print(f"[DOWNLOAD] Saved {local_path} ({size_mb:.1f} MB)")
    return local_path


def sanitize(value):
    if value is None:
        return "\\N"
    # Replace literal tabs and newlines with spaces to prevent COPY breakage
    return value.replace("\t", " ").replace("\n", " ").replace("\r", "")


def is_valid_int(value):
    if value is None or value == "\\N" or value == "":
        return True
    try:
        int(value)
        return True
    except ValueError:
        return False


def transform_basics(input_path, output_path):
    print("[TRANSFORM] Starting title.basics ...")
    total = 0
    kept = 0
    skipped = 0
    with gzip.open(input_path, "rt", encoding="utf-8") as f_in, \
         open(output_path, "w", encoding="utf-8", newline="") as f_out:
        reader = csv.DictReader(f_in, delimiter="\t")
        for row in reader:
            total += 1
            ttype = row.get("titleType", "")
            if ttype not in KEEP_TYPES:
                continue
            start_year = row.get("startYear", "")
            runtime = row.get("runtimeMinutes", "")
            genres = row.get("genres", "")
            if not is_valid_int(start_year) or not is_valid_int(runtime):
                skipped += 1
                continue
            kept += 1
            f_out.write("\t".join([
                sanitize(row.get("tconst", "")),
                sanitize(ttype),
                sanitize(row.get("primaryTitle", "")),
                sanitize(start_year) if start_year != "\\N" else "\\N",
                sanitize(runtime) if runtime != "\\N" else "\\N",
                sanitize(genres) if genres != "\\N" else "\\N",
            ]) + "\n")
            if kept % 200000 == 0:
                print(f"[TRANSFORM] title.basics {kept:,} kept ({total:,} scanned, {skipped:,} skipped)")
    print(f"[TRANSFORM] title.basics complete: {kept:,} kept / {total:,} total, {skipped:,} skipped → {output_path}")
    return kept


def transform_simple(input_path, output_path, columns):
    print(f"[TRANSFORM] Starting {os.path.basename(input_path)} ...")
    count = 0
    with gzip.open(input_path, "rt", encoding="utf-8") as f_in, \
         open(output_path, "w", encoding="utf-8", newline="") as f_out:
        reader = csv.DictReader(f_in, delimiter="\t")
        for row in reader:
            count += 1
            f_out.write("\t".join([
                sanitize(row[col]) if row[col] != "\\N" else "\\N"
                for col in columns
            ]) + "\n")
            if count % 500000 == 0:
                print(f"[TRANSFORM] {os.path.basename(input_path)} {count:,} rows")
    print(f"[TRANSFORM] {os.path.basename(input_path)} complete: {count:,} rows → {output_path}")
    return count


def init_state_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_state (
                file_name VARCHAR(20) PRIMARY KEY,
                hash VARCHAR(64) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
    conn.commit()


def create_staging_tables(conn):
    print("[DB] Creating staging tables ...")
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS titles_new, episodes_new, ratings_new CASCADE")
        cur.execute("""
            CREATE TABLE titles_new (
                tconst VARCHAR(10) PRIMARY KEY,
                title_type VARCHAR(20) NOT NULL,
                primary_title TEXT NOT NULL,
                start_year SMALLINT,
                runtime_minutes SMALLINT,
                genres TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE episodes_new (
                tconst VARCHAR(10) PRIMARY KEY,
                parent_tconst VARCHAR(10) NOT NULL,
                season_number SMALLINT,
                episode_number SMALLINT
            )
        """)
        cur.execute("""
            CREATE TABLE ratings_new (
                tconst VARCHAR(10) PRIMARY KEY,
                average_rating REAL NOT NULL,
                num_votes INTEGER NOT NULL
            )
        """)
    conn.commit()


def copy_from_tsv(conn, table, path, columns):
    print(f"[DB] Bulk copying into {table} ...")
    cols = ", ".join(columns)
    # FORMAT TEXT with \N as NULL marker is the fastest PostgreSQL COPY path
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\\\N')"
    with conn.cursor() as cur, open(path, "r", encoding="utf-8") as f:
        cur.copy_expert(sql, f)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
    print(f"[DB] Copied {table}: {count:,} rows")


def build_indexes(conn):
    print("[DB] Building indexes ...")
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX idx_titles_type ON titles_new(title_type)")
        cur.execute("CREATE INDEX idx_episodes_parent ON episodes_new(parent_tconst)")
    conn.commit()
    print("[DB] Indexes built.")


def swap_tables(conn):
    print("[DB] Swapping tables atomically ...")
    with conn.cursor() as cur:
        for name in ["titles", "episodes", "ratings"]:
            cur.execute(f"DROP TABLE IF EXISTS {name}_old CASCADE")
            cur.execute(f"ALTER TABLE IF EXISTS {name} RENAME TO {name}_old")
            cur.execute(f"ALTER TABLE {name}_new RENAME TO {name}")
    conn.commit()
    print("[DB] Tables swapped.")


def run_full_load(conn, basics_tsv, episode_tsv, ratings_tsv):
    create_staging_tables(conn)
    copy_from_tsv(conn, "titles_new", basics_tsv, ("tconst", "title_type", "primary_title", "start_year", "runtime_minutes", "genres"))
    copy_from_tsv(conn, "episodes_new", episode_tsv, ("tconst", "parent_tconst", "season_number", "episode_number"))
    copy_from_tsv(conn, "ratings_new", ratings_tsv, ("tconst", "average_rating", "num_votes"))
    build_indexes(conn)
    swap_tables(conn)


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[ERROR] DATABASE_URL not set")
        sys.exit(1)

    start_time = time.time()
    conn = get_conn()
    try:
        print("[DB] Ensuring state table exists ...")
        init_state_table(conn)

        tmpdir = tempfile.mkdtemp()
        try:
            paths = {}
            hashes = {}
            changed = {}

            # Download all files and compute hashes
            for key, (filename, state_name) in FILES.items():
                paths[key] = download_file(filename)
                hashes[key] = sha256_file(paths[key])
                stored = get_stored_hash(conn, state_name)
                changed[key] = (stored != hashes[key])
                print(f"[HASH] {state_name}: current={hashes[key][:16]}... stored={stored[:16] if stored else 'None'} changed={changed[key]}")

            # If nothing changed, exit immediately
            if not any(changed.values()):
                print("[SKIP] All files unchanged. No database update needed.")
                return

            basics_tsv = os.path.join(tmpdir, "basics.tsv")
            episode_tsv = os.path.join(tmpdir, "episode.tsv")
            ratings_tsv = os.path.join(tmpdir, "ratings.tsv")

            transform_basics(paths["basics"], basics_tsv)
            transform_simple(paths["episode"], episode_tsv, ["tconst", "parentTconst", "seasonNumber", "episodeNumber"])
            transform_simple(paths["ratings"], ratings_tsv, ["tconst", "averageRating", "numVotes"])

            # If this is the very first run (no live tables exist), do a full load
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'titles'
                    )
                """)
                has_live = cur.fetchone()[0]

            # For simplicity and correctness: full rebuild if any structural file changed,
            # fast ratings upsert if ONLY ratings changed.
            only_ratings_changed = changed["ratings"] and not changed["basics"] and not changed["episode"] and has_live

            if only_ratings_changed:
                print("[INCREMENTAL] Only ratings changed. Fast upsert path ...")
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TEMP TABLE ratings_tmp (
                            tconst VARCHAR(10) PRIMARY KEY,
                            average_rating REAL NOT NULL,
                            num_votes INTEGER NOT NULL
                        ) ON COMMIT DROP
                    """)
                conn.commit()
                copy_from_tsv(conn, "ratings_tmp", ratings_tsv, ("tconst", "average_rating", "num_votes"))
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO ratings (tconst, average_rating, num_votes)
                        SELECT tconst, average_rating, num_votes FROM ratings_tmp
                        ON CONFLICT (tconst) DO UPDATE SET
                            average_rating = EXCLUDED.average_rating,
                            num_votes = EXCLUDED.num_votes
                    """)
                conn.commit()
                print("[INCREMENTAL] Ratings upserted.")
            else:
                # Full rebuild path (first run OR basics/episode changed)
                run_full_load(conn, basics_tsv, episode_tsv, ratings_tsv)

            # Update stored hashes
            for key, (filename, state_name) in FILES.items():
                set_stored_hash(conn, state_name, hashes[key])

        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
            for key, (filename, _) in FILES.items():
                p = os.path.join(tempfile.gettempdir(), filename)
                if os.path.exists(p):
                    os.remove(p)
            print("[CLEANUP] Temporary files removed.")
    finally:
        conn.close()

    elapsed = time.time() - start_time
    print(f"[DONE] ETL completed in {elapsed:.0f} seconds ({elapsed/60:.1f} minutes)")
    print(f"[DONE] Time: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        traceback.print_exc()
        sys.exit(1)
