import csv
import gzip
import hashlib
import io
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
#
# PKs are range-sharded (ASC), not YB's default HASH: inputs arrive
# tconst-sorted so COPY writes sequentially, and the incremental diff's
# `tconst BETWEEN` chunk DELETE becomes a range scan instead of a full scan.
TITLES_DDL = """
    CREATE TABLE titles_new (
        tconst VARCHAR(10) NOT NULL,
        title_type VARCHAR(20) NOT NULL,
        primary_title TEXT NOT NULL,
        start_year SMALLINT,
        runtime_minutes INTEGER,
        genres TEXT,
        PRIMARY KEY (tconst ASC)
    )
"""

EPISODES_DDL = """
    CREATE TABLE episodes_new (
        tconst VARCHAR(10) NOT NULL,
        parent_tconst VARCHAR(10) NOT NULL,
        season_number INTEGER,
        episode_number INTEGER,
        PRIMARY KEY (tconst ASC)
    )
"""

RATINGS_DDL = """
    CREATE TABLE ratings_new (
        tconst VARCHAR(10) NOT NULL,
        average_rating REAL NOT NULL,
        num_votes INTEGER NOT NULL,
        PRIMARY KEY (tconst ASC)
    )
"""

# Stage tables are keyed on a range-sharded row_num for keyset-paginated
# batched upserts. row_num is written client-side into the COPY stream (no
# SERIAL: YB sequences cost a master RPC per cache refill), and the ASC PK
# makes each `row_num` batch a range scan rather than a full stage scan.
TITLES_STAGE_DDL = """
    CREATE TABLE titles_stage (
        row_num BIGINT NOT NULL,
        tconst VARCHAR(10) NOT NULL,
        title_type VARCHAR(20) NOT NULL,
        primary_title TEXT NOT NULL,
        start_year SMALLINT,
        runtime_minutes INTEGER,
        genres TEXT,
        PRIMARY KEY (row_num ASC)
    )
"""

EPISODES_STAGE_DDL = """
    CREATE TABLE episodes_stage (
        row_num BIGINT NOT NULL,
        tconst VARCHAR(10) NOT NULL,
        parent_tconst VARCHAR(10) NOT NULL,
        season_number INTEGER,
        episode_number INTEGER,
        PRIMARY KEY (row_num ASC)
    )
"""

RATINGS_STAGE_DDL = """
    CREATE TABLE ratings_stage (
        row_num BIGINT NOT NULL,
        tconst VARCHAR(10) NOT NULL,
        average_rating REAL NOT NULL,
        num_votes INTEGER NOT NULL,
        PRIMARY KEY (row_num ASC)
    )
"""

UPSERT_BATCH = 25_000

# Rows per chunk in the chunked stage-and-diff loop. Each chunk is COPYed
# into a freshly TRUNCATEd stage table, so the peak on-disk staging
# footprint is bounded by this constant regardless of total dataset size.
# Sized to keep the per-chunk stage hash for the DELETE anti-join inside
# work_mem (no spill) and well under YugabyteDB's tablet disk headroom.
STAGE_CHUNK_ROWS = 1_000_000


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


def sort_tsv_by_tconst(path):
    """Sort a TSV in place by its first (tconst) column in byte order.

    LC_ALL=C is both much faster than a locale-aware sort and matches
    Python's str comparison, which filter_to_titles' merge relies on.
    """
    env = dict(os.environ, LC_ALL="C")
    subprocess.run(
        ["sort", "-t", "\t", "-k1,1", "-S", "1G", "--parallel=4", "-o", path, path],
        check=True, env=env,
    )


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
    sort_tsv_by_tconst(out_tsv)
    size_mb = os.path.getsize(out_tsv) / (1024 * 1024)
    print(f"[TRANSFORM] {os.path.basename(out_tsv)} ready ({size_mb:.1f} MB) in {time.time() - start:.0f}s.")


def filter_to_titles(titles_tsv, in_tsv, out_tsv):
    """Keep only rows of `in_tsv` whose tconst appears in `titles_tsv`.

    Both inputs must be tconst-sorted in byte order; a streaming merge keeps
    memory flat. Rows for titles we don't store (movies, shorts, ...) are
    unreachable through the API, which always joins from `titles`.
    """
    print(f"[TRANSFORM] Filtering {os.path.basename(in_tsv)} to kept titles ...")
    total = 0
    kept = 0
    with open(titles_tsv, "r", encoding="utf-8") as f_titles, \
         open(in_tsv, "r", encoding="utf-8") as f_in, \
         open(out_tsv, "w", encoding="utf-8", newline="", buffering=1 << 20) as f_out:
        title_key = ""
        for line in f_in:
            total += 1
            key = line.split("\t", 1)[0]
            while title_key is not None and title_key < key:
                title_line = f_titles.readline()
                title_key = title_line.split("\t", 1)[0] if title_line else None
            if title_key == key:
                f_out.write(line)
                kept += 1
    print(f"[TRANSFORM] {os.path.basename(out_tsv)}: {kept:,} kept / {total:,} total")


def prepare_basics(basics_gz, tmpdir):
    basics_tsv = os.path.join(tmpdir, "basics.tsv")
    transform_basics(basics_gz, basics_tsv)
    print("[TRANSFORM] Sorting basics.tsv by tconst ...")
    sort_start = time.time()
    sort_tsv_by_tconst(basics_tsv)
    print(f"[TRANSFORM] Sort complete in {time.time() - sort_start:.0f}s.")
    return basics_tsv


def prepare_ratings(ratings_gz, basics_tsv, tmpdir):
    sorted_tsv = os.path.join(tmpdir, "ratings.sorted.tsv")
    gunzip_and_sort_by_tconst(ratings_gz, sorted_tsv)
    ratings_tsv = os.path.join(tmpdir, "ratings.filtered.tsv")
    filter_to_titles(basics_tsv, sorted_tsv, ratings_tsv)
    os.remove(sorted_tsv)
    return ratings_tsv


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


def set_bulk_load_mode(conn, on):
    """Toggle Yugabyte's bulk-load settings for COPYs into staging tables.

    yb_disable_transactional_writes skips the distributed-txn machinery:
    staging tables aren't visible to readers until renamed/diffed, so
    per-row transactional guarantees buy nothing. yb_enable_upsert_mode
    skips the read-before-write PK uniqueness check on every row; safe
    only because the target is empty (or freshly TRUNCATEd), the input keys
    are unique, and it has no secondary indexes at COPY time.
    """
    value = "ON" if on else "OFF"
    with conn.cursor() as cur:
        cur.execute(f"SET yb_disable_transactional_writes = {value}")
        cur.execute(f"SET yb_enable_upsert_mode = {value}")
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

    `extra_indexes` is a sequence of (final_index_name, index_spec) pairs,
    where index_spec is everything after `ON <table>` (e.g. "(col HASH, other ASC)").
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
    set_bulk_load_mode(conn, True)
    copy_from_tsv(conn, f"{name}_new", tsv_path, columns)
    set_bulk_load_mode(conn, False)

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
            f"CREATE INDEX {final}_new ON {name}_new {spec}"
            for final, spec in index_specs
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


def load_and_diff_one(conn, name, stage_ddl, tsv_path, columns, update_cols):
    """Incremental update for one table via chunked staging diff.

    The sorted TSV is read in fixed-size row chunks. For each chunk we
    TRUNCATE the stage, COPY just that chunk, then reconcile only the
    matching tconst range of the live table (DELETE missing + UPSERT
    changed). Peak on-disk stage footprint stays at ~STAGE_CHUNK_ROWS
    regardless of dataset size — required because the full stage table
    pushed the YB tablet past its disk quota mid-COPY on titles.

    Correctness relies on the input being tconst-sorted: chunk tconst
    ranges are disjoint and contiguous, so a per-chunk DELETE bounded
    by [min_t, max_t] is equivalent to one full-table anti-join.
    """
    print(f"[DB] === Incremental diff: {name} ===")
    phase_start = time.time()

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}_stage CASCADE")
        cur.execute(stage_ddl)
    conn.commit()

    col_list = ", ".join(columns)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    # Qualify existing-row columns with the table name to avoid ambiguity
    # in ON CONFLICT DO UPDATE WHERE (required by YugabyteDB).
    where_clause = " OR ".join(f"{name}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in update_cols)
    copy_sql = f"COPY {name}_stage (row_num, {col_list}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')"

    total_deleted = 0
    total_upserted = 0
    chunk_idx = 0

    with open(tsv_path, "r", encoding="utf-8") as f:
        while True:
            lines = []
            for _ in range(STAGE_CHUNK_ROWS):
                line = f.readline()
                if not line:
                    break
                lines.append(line)
            if not lines:
                break

            chunk_idx += 1
            chunk_rows = len(lines)
            min_t = lines[0].split("\t", 1)[0]
            max_t = lines[-1].split("\t", 1)[0]
            chunk_start = time.time()

            # Reset stage. row_num restarts at 1 each chunk, letting the
            # upsert pagination use a fixed [0..chunk_rows] range instead of
            # tracking a global offset.
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE {name}_stage")
            conn.commit()

            set_bulk_load_mode(conn, True)
            with conn.cursor() as cur:
                data = "".join(f"{i}\t{line}" for i, line in enumerate(lines, 1))
                cur.copy_expert(copy_sql, io.StringIO(data))
            conn.commit()
            set_bulk_load_mode(conn, False)

            with conn.cursor() as cur:
                cur.execute(f"""
                    DELETE FROM {name}
                    WHERE tconst BETWEEN %s AND %s
                      AND NOT EXISTS (
                        SELECT 1 FROM {name}_stage s WHERE s.tconst = {name}.tconst
                      )
                """, (min_t, max_t))
                deleted = cur.rowcount
            conn.commit()
            total_deleted += deleted

            last_row = 0
            chunk_upserted = 0
            while last_row < chunk_rows:
                batch_end = last_row + UPSERT_BATCH
                with conn.cursor() as cur:
                    cur.execute(f"""
                        INSERT INTO {name} ({col_list})
                        SELECT {col_list} FROM {name}_stage
                        WHERE row_num > %s AND row_num <= %s
                        ON CONFLICT (tconst) DO UPDATE
                        SET {set_clause}
                        WHERE {where_clause}
                    """, (last_row, batch_end))
                    chunk_upserted += max(cur.rowcount, 0)
                conn.commit()
                last_row = batch_end
            total_upserted += chunk_upserted

            print(
                f"[DB] Chunk {chunk_idx} [{min_t}..{max_t}] "
                f"{chunk_rows:,} rows: deleted {deleted:,}, upserted {chunk_upserted:,} "
                f"in {time.time() - chunk_start:.0f}s"
            )

    print(
        f"[DB] {name}: {total_deleted:,} deleted, {total_upserted:,} upserted "
        f"across {chunk_idx} chunks"
    )

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}_stage CASCADE")
    conn.commit()

    print(f"[DB] === Incremental diff {name} complete in {time.time() - phase_start:.0f}s ===")


def run_incremental_load(conn, tmpdir, changed, paths):
    # Ratings are filtered to kept titles, so a titles change must re-diff
    # ratings too, and the ratings diff needs the titles TSV either way.
    reload_ratings = changed["ratings"] or changed["basics"]
    basics_tsv = None
    if changed["basics"] or reload_ratings:
        basics_tsv = prepare_basics(paths["basics"], tmpdir)

    if changed["basics"]:
        load_and_diff_one(
            conn, "titles", TITLES_STAGE_DDL, basics_tsv,
            columns=("tconst", "title_type", "primary_title", "start_year", "runtime_minutes", "genres"),
            update_cols=("title_type", "primary_title", "start_year", "runtime_minutes", "genres"),
        )

    if changed["episode"]:
        episode_tsv = os.path.join(tmpdir, "episode.sorted.tsv")
        gunzip_and_sort_by_tconst(paths["episode"], episode_tsv)
        load_and_diff_one(
            conn, "episodes", EPISODES_STAGE_DDL, episode_tsv,
            columns=("tconst", "parent_tconst", "season_number", "episode_number"),
            update_cols=("parent_tconst", "season_number", "episode_number"),
        )
        os.remove(episode_tsv)

    if reload_ratings:
        ratings_tsv = prepare_ratings(paths["ratings"], basics_tsv, tmpdir)
        load_and_diff_one(
            conn, "ratings", RATINGS_STAGE_DDL, ratings_tsv,
            columns=("tconst", "average_rating", "num_votes"),
            update_cols=("average_rating", "num_votes"),
        )
        os.remove(ratings_tsv)

    if basics_tsv:
        os.remove(basics_tsv)


def run_full_load(conn, tmpdir, basics_tsv, episode_gz, ratings_gz):
    # No trigram index for /search: a ybgin gin_trgm_ops backfill measured
    # ~475 rows/s on the free-tier node (~6h for 10M titles), past the job
    # timeout. Search stays a plain ILIKE scan.
    load_and_swap_one(
        conn, "titles", TITLES_DDL, basics_tsv,
        ("tconst", "title_type", "primary_title", "start_year", "runtime_minutes", "genres"),
    )

    episode_tsv = os.path.join(tmpdir, "episode.sorted.tsv")
    gunzip_and_sort_by_tconst(episode_gz, episode_tsv)
    # Season/episode columns in the key let /series read episodes already
    # ordered and serve the per-season filter from the index. The base PK
    # (tconst) is carried in every YB secondary index implicitly.
    load_and_swap_one(
        conn, "episodes", EPISODES_DDL, episode_tsv,
        ("tconst", "parent_tconst", "season_number", "episode_number"),
        extra_indexes=((
            "idx_episodes_parent",
            "(parent_tconst HASH, season_number ASC, episode_number ASC)",
        ),),
    )
    os.remove(episode_tsv)

    ratings_tsv = prepare_ratings(ratings_gz, basics_tsv, tmpdir)
    load_and_swap_one(
        conn, "ratings", RATINGS_DDL, ratings_tsv,
        ("tconst", "average_rating", "num_votes"),
    )
    os.remove(ratings_tsv)

    # Fresh stats so the planner costs the new indexes (notably the trigram
    # index for /search) correctly.
    print("[DB] Analyzing tables ...")
    with conn.cursor() as cur:
        for name in ("titles", "episodes", "ratings"):
            cur.execute(f"ANALYZE {name}")
    conn.commit()


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
                basics_tsv = prepare_basics(paths["basics"], tmpdir)
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
