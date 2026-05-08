import csv
import gzip
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
    "basics": "title.basics.tsv.gz",
    "ratings": "title.ratings.tsv.gz",
    "episode": "title.episode.tsv.gz",
}
KEEP_TYPES = {"tvEpisode", "tvSeries", "tvMiniSeries", "movie"}


def get_conn():
    url = os.environ["DATABASE_URL"]
    return psycopg2.connect(url)


def download_file(filename):
    url = BASE_URL + filename
    local_path = os.path.join(tempfile.gettempdir(), filename)
    print(f"[DOWNLOAD] Starting {url} ...")
    urllib.request.urlretrieve(url, local_path)
    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    print(f"[DOWNLOAD] Saved {local_path} ({size_mb:.1f} MB)")
    return local_path


def sanitize(value):
    """Remove/replace problematic characters for PostgreSQL COPY."""
    if value is None:
        return ""
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
        writer = csv.writer(f_out, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            total += 1
            ttype = row.get("titleType", "")
            if ttype not in KEEP_TYPES:
                continue
            
            start_year = row.get("startYear", "")
            runtime = row.get("runtimeMinutes", "")
            genres = row.get("genres", "")
            
            # Validate numeric fields to catch shifted/malformed rows
            if not is_valid_int(start_year) or not is_valid_int(runtime):
                skipped += 1
                if skipped <= 5:
                    print(f"[WARN] Skipping malformed row {total}: tconst={row.get('tconst')}, startYear={start_year!r}, runtimeMinutes={runtime!r}, genres={genres!r}")
                continue
            
            kept += 1
            writer.writerow([
                sanitize(row.get("tconst", "")),
                sanitize(ttype),
                sanitize(row.get("primaryTitle", "")),
                sanitize(start_year) if start_year != "\\N" else "",
                sanitize(runtime) if runtime != "\\N" else "",
                sanitize(genres) if genres != "\\N" else "",
            ])
            if kept % 100000 == 0:
                print(f"[TRANSFORM] title.basics processed {kept:,} kept rows ({total:,} total scanned, {skipped:,} skipped)")
    print(f"[TRANSFORM] title.basics complete: {kept:,} kept / {total:,} total, {skipped:,} skipped → {output_path}")


def transform_simple(input_path, output_path, columns):
    print(f"[TRANSFORM] Starting {os.path.basename(input_path)} ...")
    count = 0
    with gzip.open(input_path, "rt", encoding="utf-8") as f_in, \
         open(output_path, "w", encoding="utf-8", newline="") as f_out:
        reader = csv.DictReader(f_in, delimiter="\t")
        writer = csv.writer(f_out, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            count += 1
            writer.writerow([
                sanitize(row[col]) if row[col] != "\\N" else ""
                for col in columns
            ])
            if count % 100000 == 0:
                print(f"[TRANSFORM] {os.path.basename(input_path)} processed {count:,} rows")
    print(f"[TRANSFORM] {os.path.basename(input_path)} complete: {count:,} rows → {output_path}")


def create_staging_tables(conn):
    print("[DB] Creating staging tables ...")
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS titles_new, episodes_new, ratings_new CASCADE")
        cur.execute("""
            CREATE TABLE titles_new (
                id SERIAL,
                tconst VARCHAR(10) UNIQUE NOT NULL,
                title_type VARCHAR(20) NOT NULL,
                primary_title TEXT NOT NULL,
                start_year SMALLINT,
                runtime_minutes SMALLINT,
                genres TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE episodes_new (
                id SERIAL,
                tconst VARCHAR(10) UNIQUE NOT NULL,
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
    # We use FORMAT CSV with tab delimiter so PostgreSQL properly handles
    # quoted fields (e.g. titles containing tabs or newlines) in our TSV output.
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE E'\"', NULL '')"
    with conn.cursor() as cur, open(path, "r", encoding="utf-8") as f:
        cur.copy_expert(sql, f)
    conn.commit()
    # Get row count
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
    print(f"[DB] Copied {table}: {count:,} rows")


def build_indexes(conn):
    print("[DB] Building indexes on staging tables ...")
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE titles_new ADD PRIMARY KEY (id)")
        cur.execute("ALTER TABLE episodes_new ADD PRIMARY KEY (id)")
        cur.execute("CREATE INDEX idx_titles_type ON titles_new(title_type)")
        cur.execute("CREATE INDEX idx_episodes_parent ON episodes_new(parent_tconst)")
        cur.execute("CREATE INDEX idx_titles_search ON titles_new USING gin(to_tsvector('english', primary_title))")
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


def analyze_tables(conn):
    print("[DB] Running ANALYZE ...")
    with conn.cursor() as cur:
        cur.execute("ANALYZE titles")
        cur.execute("ANALYZE episodes")
        cur.execute("ANALYZE ratings")
    conn.commit()
    print("[DB] ANALYZE done.")


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[ERROR] DATABASE_URL not set")
        sys.exit(1)

    start_time = time.time()
    tmpdir = tempfile.mkdtemp()
    try:
        paths = {}
        for key, filename in FILES.items():
            paths[key] = download_file(filename)

        basics_tsv = os.path.join(tmpdir, "basics.tsv")
        episode_tsv = os.path.join(tmpdir, "episode.tsv")
        ratings_tsv = os.path.join(tmpdir, "ratings.tsv")

        transform_basics(paths["basics"], basics_tsv)
        transform_simple(paths["episode"], episode_tsv, ["tconst", "parentTconst", "seasonNumber", "episodeNumber"])
        transform_simple(paths["ratings"], ratings_tsv, ["tconst", "averageRating", "numVotes"])

        print("[DB] Connecting to database ...")
        conn = get_conn()
        try:
            create_staging_tables(conn)
            copy_from_tsv(conn, "titles_new", basics_tsv, ("tconst", "title_type", "primary_title", "start_year", "runtime_minutes", "genres"))
            copy_from_tsv(conn, "episodes_new", episode_tsv, ("tconst", "parent_tconst", "season_number", "episode_number"))
            copy_from_tsv(conn, "ratings_new", ratings_tsv, ("tconst", "average_rating", "num_votes"))
            build_indexes(conn)
            swap_tables(conn)
            analyze_tables(conn)
        finally:
            conn.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        for key, filename in FILES.items():
            p = os.path.join(tempfile.gettempdir(), filename)
            if os.path.exists(p):
                os.remove(p)
        print("[CLEANUP] Temporary files removed.")

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
