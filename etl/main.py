import csv
import gzip
import hashlib
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.request
import psycopg2


class ProgressFile:
    """File-like wrapper that prints bytes-read progress every `interval` seconds."""

    def __init__(self, fileobj, label, total_bytes=None, interval=5.0):
        self._f = fileobj
        self._label = label
        self._total = total_bytes
        self._interval = interval
        self._bytes = 0
        self._start = time.time()
        self._last_log = self._start

    def _maybe_log(self):
        now = time.time()
        if now - self._last_log < self._interval:
            return
        mb = self._bytes / (1024 * 1024)
        elapsed = now - self._start
        rate = mb / elapsed if elapsed > 0 else 0
        if self._total:
            total_mb = self._total / (1024 * 1024)
            pct = 100 * self._bytes / self._total
            print(f"[PROGRESS] {self._label}: {mb:,.1f}/{total_mb:,.1f} MB ({pct:.0f}%, {rate:.1f} MB/s, {elapsed:.0f}s)")
        else:
            print(f"[PROGRESS] {self._label}: {mb:,.1f} MB read ({rate:.1f} MB/s, {elapsed:.0f}s)")
        self._last_log = now

    def read(self, size=-1):
        chunk = self._f.read(size)
        self._bytes += len(chunk)
        self._maybe_log()
        return chunk

    def readline(self, size=-1):
        line = self._f.readline(size) if size != -1 else self._f.readline()
        self._bytes += len(line)
        self._maybe_log()
        return line

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
KEEP_TYPES = {"tvEpisode", "tvSeries", "tvMiniSeries"}


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
    state = {"last": time.time(), "start": time.time()}

    def hook(blocks, blocksize, total):
        now = time.time()
        if now - state["last"] < 3.0:
            return
        downloaded_mb = (blocks * blocksize) / (1024 * 1024)
        elapsed = now - state["start"]
        rate = downloaded_mb / elapsed if elapsed > 0 else 0
        if total > 0:
            total_mb = total / (1024 * 1024)
            pct = 100 * blocks * blocksize / total
            print(f"[DOWNLOAD]   {filename}: {downloaded_mb:.1f}/{total_mb:.1f} MB ({pct:.0f}%, {rate:.1f} MB/s)")
        else:
            print(f"[DOWNLOAD]   {filename}: {downloaded_mb:.1f} MB ({rate:.1f} MB/s)")
        state["last"] = now

    urllib.request.urlretrieve(url, local_path, reporthook=hook)
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


# PK is declared in the DDL so COPY writes directly into PK-organized
# DocDB tablets. A post-load ADD PRIMARY KEY would rewrite the table
# (peak disk ~2x), which Yugabyte rejects on a tight node.
TITLES_DDL = """
    CREATE TABLE titles_new (
        tconst VARCHAR(10) NOT NULL,
        title_type VARCHAR(20) NOT NULL,
        primary_title TEXT NOT NULL,
        start_year SMALLINT,
        runtime_minutes INTEGER,
        genres TEXT,
        PRIMARY KEY (tconst)
    )
"""

EPISODES_DDL = """
    CREATE TABLE episodes_new (
        tconst VARCHAR(10) NOT NULL,
        parent_tconst VARCHAR(10) NOT NULL,
        season_number INTEGER,
        episode_number INTEGER,
        PRIMARY KEY (tconst)
    )
"""

RATINGS_DDL = """
    CREATE TABLE ratings_new (
        tconst VARCHAR(10) NOT NULL,
        average_rating REAL NOT NULL,
        num_votes INTEGER NOT NULL,
        PRIMARY KEY (tconst)
    )
"""

# Stage tables have no PK or indexes — scratch space for COPY, then SQL diff.
TITLES_STAGE_DDL = """
    CREATE TABLE titles_stage (
        tconst VARCHAR(10) NOT NULL,
        title_type VARCHAR(20) NOT NULL,
        primary_title TEXT NOT NULL,
        start_year SMALLINT,
        runtime_minutes INTEGER,
        genres TEXT
    )
"""

EPISODES_STAGE_DDL = """
    CREATE TABLE episodes_stage (
        tconst VARCHAR(10) NOT NULL,
        parent_tconst VARCHAR(10) NOT NULL,
        season_number INTEGER,
        episode_number INTEGER
    )
"""

RATINGS_STAGE_DDL = """
    CREATE TABLE ratings_stage (
        tconst VARCHAR(10) NOT NULL,
        average_rating REAL NOT NULL,
        num_votes INTEGER NOT NULL
    )
"""


def copy_from_tsv(conn, table, path, columns):
    total_bytes = os.path.getsize(path)
    print(f"[DB] Bulk copying into {table} ({total_bytes / (1024*1024):.1f} MB) ...")
    cols = ", ".join(columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')"
    start = time.time()
    with conn.cursor() as cur, open(path, "r", encoding="utf-8") as f:
        wrapped = ProgressFile(f, f"COPY → {table}", total_bytes=total_bytes)
        cur.copy_expert(sql, wrapped)
    conn.commit()
    print(f"[DB] COPY into {table} complete in {time.time() - start:.0f}s.")


def gunzip_and_sort_by_tconst(gz_path, out_tsv):
    """Decompress a gzipped IMDb TSV, drop the header, sort by tconst.

    Sorting runner-side lets COPY into a PK-organized table do sequential
    DocDB inserts instead of random ones — much faster, and avoids any
    in-database rewrite.
    """
    print(f"[TRANSFORM] Decompressing+sorting {os.path.basename(gz_path)} → {out_tsv} ...")
    start = time.time()
    with gzip.open(gz_path, "rb") as gz_in, open(out_tsv, "wb") as raw_out:
        gz_in.readline()  # drop header
        shutil.copyfileobj(gz_in, raw_out)
    subprocess.run(["sort", "-k1,1", "-S", "256M", "-o", out_tsv, out_tsv], check=True)
    size_mb = os.path.getsize(out_tsv) / (1024 * 1024)
    print(f"[TRANSFORM] {os.path.basename(out_tsv)} ready ({size_mb:.1f} MB) in {time.time() - start:.0f}s.")


def _run_one(sql):
    s_start = time.time()
    print(f"[DB]   started: {sql}")
    with get_conn() as c:
        with c.cursor() as cur:
            cur.execute(sql)
        c.commit()
    print(f"[DB]   finished in {time.time() - s_start:.0f}s: {sql}")


def drop_old_tables(conn):
    print("[DB] Dropping prior *_old tables to free disk ...")
    with conn.cursor() as cur:
        for name in ["titles", "episodes", "ratings"]:
            cur.execute(f"DROP TABLE IF EXISTS {name}_old CASCADE")
    conn.commit()


def disable_txn_writes(conn):
    """Skip Yugabyte's distributed-txn machinery for the staging COPYs.
    Staging tables aren't visible to readers until the per-phase swap renames
    them, so per-row transactional guarantees buy nothing here."""
    with conn.cursor() as cur:
        cur.execute("SET yb_disable_transactional_writes = ON")
    conn.commit()


def _run_indexes_serial(statements):
    # Yugabyte serializes online schema changes per table and aborts the
    # loser of any concurrent DDL race with SerializationFailure
    # ("schema version mismatch"). Run one DDL at a time per staging table.
    start = time.time()
    completed = [0]
    done = threading.Event()

    def heartbeat():
        while not done.wait(15):
            print(f"[DB]   ... index build {time.time() - start:.0f}s elapsed, {completed[0]}/{len(statements)} done")

    hb = threading.Thread(target=heartbeat, daemon=True)
    hb.start()
    try:
        for s in statements:
            _run_one(s)
            completed[0] += 1
    finally:
        done.set()
    print(f"[DB] Indexes built in {time.time() - start:.0f}s.")


def load_and_swap_one(conn, name, ddl, tsv_path, columns, extra_indexes=()):
    """Load → secondary indexes → swap → drop-old, all for one table.

    The PK is declared in `ddl`, so COPY writes directly into PK-organized
    storage and we skip a post-load table rewrite. `tsv_path` must be a
    plain TSV sorted by tconst.

    `extra_indexes` is a sequence of (final_index_name, column_expr) pairs.
    Indexes are built on the staging table under `<final>_new` to avoid
    colliding with the same-named index attached to the live table from a
    prior run, then renamed to `<final>` atomically with the swap.
    """
    print(f"[DB] === Phase: {name} ===")
    phase_start = time.time()

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}_new CASCADE")
        cur.execute(ddl)
    conn.commit()
    disable_txn_writes(conn)

    copy_from_tsv(conn, f"{name}_new", tsv_path, columns)

    index_specs = list(extra_indexes)
    if index_specs:
        # Drop the live secondary indexes before building the staging copies
        # so the YB node isn't holding two full copies at once. Reads on the
        # live table fall back to PK / seq scan for the duration of the build;
        # the swap restores the index under its canonical name.
        with conn.cursor() as cur:
            for final, _ in index_specs:
                cur.execute(f"DROP INDEX IF EXISTS {final}")
        conn.commit()

        index_stmts = [
            f"CREATE INDEX {final}_new ON {name}_new ({col_expr})"
            for final, col_expr in index_specs
        ]
        print(f"[DB] Building {len(index_stmts)} secondary index statement(s) on {name}_new ...")
        _run_indexes_serial(index_stmts)

    print(f"[DB] Swapping {name} ...")
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}_old CASCADE")
        cur.execute(f"ALTER TABLE IF EXISTS {name} RENAME TO {name}_old")
        cur.execute(f"ALTER TABLE {name}_new RENAME TO {name}")
        cur.execute(f"DROP TABLE IF EXISTS {name}_old CASCADE")
        for final, _ in index_specs:
            cur.execute(f"ALTER INDEX {final}_new RENAME TO {final}")
    conn.commit()

    print(f"[DB] === Phase {name} complete in {time.time() - phase_start:.0f}s ===")


def load_and_diff_one(conn, name, stage_ddl, tsv_path, columns, update_cols, match_clause):
    """Incremental update for one table via staging diff.

    Loads the full new dataset into an unindexed stage table, then applies
    only the actual changes (deletes + upserts) to the live table, leaving
    its indexes intact and in-place throughout.
    """
    print(f"[DB] === Incremental diff: {name} ===")
    phase_start = time.time()

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}_stage CASCADE")
        cur.execute(stage_ddl)
    conn.commit()
    disable_txn_writes(conn)
    copy_from_tsv(conn, f"{name}_stage", tsv_path, columns)

    # Re-enable transactional writes before touching the live table.
    with conn.cursor() as cur:
        cur.execute("SET yb_disable_transactional_writes = OFF")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"""
            DELETE FROM {name}
            WHERE NOT EXISTS (
                SELECT 1 FROM {name}_stage s WHERE s.tconst = {name}.tconst
            )
        """)
        deleted = cur.rowcount
    conn.commit()
    print(f"[DB] Deleted {deleted:,} rows from {name}.")

    col_list = ", ".join(columns)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {name} ({col_list})
            SELECT {col_list} FROM {name}_stage
            ON CONFLICT (tconst) DO UPDATE
            SET {set_clause}
            WHERE {match_clause}
        """)
        upserted = cur.rowcount
    conn.commit()
    print(f"[DB] Upserted {upserted:,} rows into {name} (inserts + actual changes).")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}_stage CASCADE")
    conn.commit()

    print(f"[DB] === Incremental diff {name} complete in {time.time() - phase_start:.0f}s ===")


def run_incremental_load(conn, tmpdir, changed, paths):
    if changed["basics"]:
        basics_tsv = os.path.join(tmpdir, "basics.tsv")
        transform_basics(paths["basics"], basics_tsv)
        print("[TRANSFORM] Sorting basics.tsv by tconst ...")
        sort_start = time.time()
        subprocess.run(["sort", "-k1,1", "-S", "256M", "-o", basics_tsv, basics_tsv], check=True)
        print(f"[TRANSFORM] Sort complete in {time.time() - sort_start:.0f}s.")
        load_and_diff_one(
            conn, "titles", TITLES_STAGE_DDL, basics_tsv,
            columns=("tconst", "title_type", "primary_title", "start_year", "runtime_minutes", "genres"),
            update_cols=("title_type", "primary_title", "start_year", "runtime_minutes", "genres"),
            match_clause=(
                "title_type IS DISTINCT FROM EXCLUDED.title_type OR "
                "primary_title IS DISTINCT FROM EXCLUDED.primary_title OR "
                "start_year IS DISTINCT FROM EXCLUDED.start_year OR "
                "runtime_minutes IS DISTINCT FROM EXCLUDED.runtime_minutes OR "
                "genres IS DISTINCT FROM EXCLUDED.genres"
            ),
        )
        os.remove(basics_tsv)

    if changed["episode"]:
        episode_tsv = os.path.join(tmpdir, "episode.sorted.tsv")
        gunzip_and_sort_by_tconst(paths["episode"], episode_tsv)
        load_and_diff_one(
            conn, "episodes", EPISODES_STAGE_DDL, episode_tsv,
            columns=("tconst", "parent_tconst", "season_number", "episode_number"),
            update_cols=("parent_tconst", "season_number", "episode_number"),
            match_clause=(
                "parent_tconst IS DISTINCT FROM EXCLUDED.parent_tconst OR "
                "season_number IS DISTINCT FROM EXCLUDED.season_number OR "
                "episode_number IS DISTINCT FROM EXCLUDED.episode_number"
            ),
        )
        os.remove(episode_tsv)

    if changed["ratings"]:
        ratings_tsv = os.path.join(tmpdir, "ratings.sorted.tsv")
        gunzip_and_sort_by_tconst(paths["ratings"], ratings_tsv)
        load_and_diff_one(
            conn, "ratings", RATINGS_STAGE_DDL, ratings_tsv,
            columns=("tconst", "average_rating", "num_votes"),
            update_cols=("average_rating", "num_votes"),
            match_clause=(
                "average_rating IS DISTINCT FROM EXCLUDED.average_rating OR "
                "num_votes IS DISTINCT FROM EXCLUDED.num_votes"
            ),
        )
        os.remove(ratings_tsv)


def run_full_load(conn, tmpdir, basics_tsv, episode_gz, ratings_gz):
    load_and_swap_one(
        conn, "titles", TITLES_DDL, basics_tsv,
        ("tconst", "title_type", "primary_title", "start_year", "runtime_minutes", "genres"),
    )

    episode_tsv = os.path.join(tmpdir, "episode.sorted.tsv")
    gunzip_and_sort_by_tconst(episode_gz, episode_tsv)
    load_and_swap_one(
        conn, "episodes", EPISODES_DDL, episode_tsv,
        ("tconst", "parent_tconst", "season_number", "episode_number"),
        extra_indexes=(("idx_episodes_parent", "parent_tconst"),),
    )
    os.remove(episode_tsv)

    ratings_tsv = os.path.join(tmpdir, "ratings.sorted.tsv")
    gunzip_and_sort_by_tconst(ratings_gz, ratings_tsv)
    load_and_swap_one(
        conn, "ratings", RATINGS_DDL, ratings_tsv,
        ("tconst", "average_rating", "num_votes"),
    )
    os.remove(ratings_tsv)


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
        drop_old_tables(conn)

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

            if has_live:
                run_incremental_load(conn, tmpdir, changed, paths)
            else:
                # First run — no live tables yet; COPY+swap is fastest.
                basics_tsv = os.path.join(tmpdir, "basics.tsv")
                transform_basics(paths["basics"], basics_tsv)
                print("[TRANSFORM] Sorting basics.tsv by tconst ...")
                sort_start = time.time()
                subprocess.run(["sort", "-k1,1", "-S", "256M", "-o", basics_tsv, basics_tsv], check=True)
                print(f"[TRANSFORM] Sort complete in {time.time() - sort_start:.0f}s.")
                run_full_load(conn, tmpdir, basics_tsv, paths["episode"], paths["ratings"])

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
