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
    if value is None or value == "":
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
         open(output_path, "w", encoding="utf-8", newline="", buffering=1 << 20) as f_out:
        reader = csv.reader(f_in, delimiter="\t")
        header = next(reader)
        idx = {col: i for i, col in enumerate(header)}
        i_tconst = idx["tconst"]
        i_type   = idx["titleType"]
        i_title  = idx["primaryTitle"]
        i_year   = idx["startYear"]
        i_runtime = idx["runtimeMinutes"]
        i_genres = idx["genres"]
        for row in reader:
            total += 1
            ttype = row[i_type]
            if ttype not in KEEP_TYPES:
                continue
            start_year = row[i_year]
            runtime = row[i_runtime]
            if not is_valid_int(start_year) or not is_valid_int(runtime):
                skipped += 1
                continue
            kept += 1
            f_out.write("\t".join([
                sanitize(row[i_tconst]),
                sanitize(ttype),
                sanitize(row[i_title]),
                sanitize(start_year),
                sanitize(runtime),
                sanitize(row[i_genres]),
            ]) + "\n")
            if kept % 200000 == 0:
                print(f"[TRANSFORM] title.basics {kept:,} kept ({total:,} scanned, {skipped:,} skipped)")
    print(f"[TRANSFORM] title.basics complete: {kept:,} kept / {total:,} total, {skipped:,} skipped → {output_path}")
    return kept



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
        # No PK/unique constraints during load — added in bulk by build_indexes()
        # after all data is loaded, which is 5-10x faster than per-row index maintenance.
        cur.execute("""
            CREATE TABLE titles_new (
                tconst VARCHAR(10) NOT NULL,
                title_type VARCHAR(20) NOT NULL,
                primary_title TEXT NOT NULL,
                start_year SMALLINT,
                runtime_minutes INTEGER,
                genres TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE episodes_new (
                tconst VARCHAR(10) NOT NULL,
                parent_tconst VARCHAR(10) NOT NULL,
                season_number SMALLINT,
                episode_number SMALLINT
            )
        """)
        cur.execute("""
            CREATE TABLE ratings_new (
                tconst VARCHAR(10) NOT NULL,
                average_rating REAL NOT NULL,
                num_votes INTEGER NOT NULL
            )
        """)
    conn.commit()


def copy_from_tsv(conn, table, path, columns):
    print(f"[DB] Bulk copying into {table} ...")
    cols = ", ".join(columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')"
    with conn.cursor() as cur, open(path, "r", encoding="utf-8") as f:
        cur.copy_expert(sql, f)
    conn.commit()
    print(f"[DB] COPY into {table} complete.")


def stream_gz_to_table(conn, table, gz_path, columns):
    """Stream a gzip TSV file directly to PostgreSQL COPY, skipping the header row.

    Safe for files whose values cannot contain tabs/newlines (IDs, numbers).
    IMDb uses \\N natively for NULLs, which matches PostgreSQL TEXT COPY format.
    """
    print(f"[DB] Streaming {os.path.basename(gz_path)} → {table} ...")
    cols = ", ".join(columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')"
    with gzip.open(gz_path, "rb") as gz_file:
        gz_file.readline()  # skip header row
        with conn.cursor() as cur:
            cur.copy_expert(sql, gz_file)
    conn.commit()
    print(f"[DB] Stream COPY into {table} complete.")


def build_indexes(conn):
    print("[DB] Building indexes and primary keys ...")
    with conn.cursor() as cur:
        # Add PKs in bulk after load — much faster than per-row index maintenance during COPY
        cur.execute("ALTER TABLE titles_new   ADD PRIMARY KEY (tconst)")
        cur.execute("ALTER TABLE episodes_new ADD PRIMARY KEY (tconst)")
        cur.execute("ALTER TABLE ratings_new  ADD PRIMARY KEY (tconst)")
        cur.execute("CREATE INDEX idx_titles_type     ON titles_new(title_type)")
        cur.execute("CREATE INDEX idx_episodes_parent ON episodes_new(parent_tconst)")
    conn.commit()
    print("[DB] Indexes and primary keys built.")


def swap_tables(conn):
    print("[DB] Swapping tables atomically ...")
    with conn.cursor() as cur:
        for name in ["titles", "episodes", "ratings"]:
            cur.execute(f"DROP TABLE IF EXISTS {name}_old CASCADE")
            cur.execute(f"ALTER TABLE IF EXISTS {name} RENAME TO {name}_old")
            cur.execute(f"ALTER TABLE {name}_new RENAME TO {name}")
    conn.commit()
    print("[DB] Tables swapped.")


def run_full_load(conn, basics_tsv, episode_gz, ratings_gz):
    create_staging_tables(conn)
    copy_from_tsv(conn, "titles_new", basics_tsv, ("tconst", "title_type", "primary_title", "start_year", "runtime_minutes", "genres"))
    stream_gz_to_table(conn, "episodes_new", episode_gz, ("tconst", "parent_tconst", "season_number", "episode_number"))
    stream_gz_to_table(conn, "ratings_new", ratings_gz, ("tconst", "average_rating", "num_votes"))
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

            # If this is the very first run (no live tables exist), do a full load
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'titles'
                    )
                """)
                has_live = cur.fetchone()[0]

            # Full rebuild if any structural file changed;
            # ratings-only staging swap if ONLY ratings changed.
            only_ratings_changed = changed["ratings"] and not changed["basics"] and not changed["episode"] and has_live

            if only_ratings_changed:
                print("[INCREMENTAL] Only ratings changed. Ratings staging swap ...")
                with conn.cursor() as cur:
                    cur.execute("DROP TABLE IF EXISTS ratings_new CASCADE")
                    cur.execute("""
                        CREATE TABLE ratings_new (
                            tconst VARCHAR(10) NOT NULL,
                            average_rating REAL NOT NULL,
                            num_votes INTEGER NOT NULL
                        )
                    """)
                conn.commit()
                stream_gz_to_table(conn, "ratings_new", paths["ratings"], ("tconst", "average_rating", "num_votes"))
                with conn.cursor() as cur:
                    cur.execute("ALTER TABLE ratings_new ADD PRIMARY KEY (tconst)")
                    cur.execute("DROP TABLE IF EXISTS ratings_old CASCADE")
                    cur.execute("ALTER TABLE IF EXISTS ratings RENAME TO ratings_old")
                    cur.execute("ALTER TABLE ratings_new RENAME TO ratings")
                conn.commit()
                print("[INCREMENTAL] Ratings swap complete.")
            else:
                # Full rebuild path (first run OR basics/episode changed)
                basics_tsv = os.path.join(tmpdir, "basics.tsv")
                transform_basics(paths["basics"], basics_tsv)
                run_full_load(conn, basics_tsv, paths["episode"], paths["ratings"])

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
