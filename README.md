# IMDb Free API

Zero-maintenance, completely free, auto-updating IMDb API served from the edge.

## Stack

| Layer | Service | Why |
|-------|---------|-----|
| **ETL** | GitHub Actions | Free daily cron, 2,000 min/month |
| **Database** | YugabyteDB Managed | Free 10 GB PostgreSQL-compatible, no credit card |
| **API** | Deno Deploy | Global edge, native TCP to Postgres, no cold starts, free tier |

## Project Structure

```
.
├── .github/workflows/etl.yml   # Daily cron job
├── etl/
│   ├── main.py                 # Download, filter, bulk-load script
│   └── requirements.txt        # Python deps
├── api/
│   ├── main.ts                 # Hono API routes
│   └── db.ts                   # Postgres connection pool
├── schema.sql                  # Reference DB schema
└── README.md                   # This file
```

## Setup Instructions

### 1. Create a YugabyteDB Cluster

1. Go to [https://cloud.yugabyte.com/](https://cloud.yugabyte.com/) and sign up (no credit card required).
2. Create a new **free tier** cluster.
3. Once provisioned, create a database named `imdb`:
   ```sql
   CREATE DATABASE imdb;
   ```
4. Copy the **YSQL connection string**. It looks like:
   ```
   postgresql://admin:xxxxxxxx@xxx.yugabyte.cloud:5433/imdb?sslmode=require
   ```

### 2. Configure GitHub Secrets

1. Push this repo to a **public** GitHub repository (required for free Actions minutes).
2. Go to **Settings → Secrets and variables → Actions**.
3. Add a new repository secret:
   - **Name**: `DATABASE_URL`
   - **Value**: your YugabyteDB connection string

### 3. Deploy the API to Deno Deploy

1. Go to [https://dash.deno.com/](https://dash.deno.com/) and sign in with GitHub.
2. Click **New Project** → **Deploy from GitHub**.
3. Select your repository.
4. Set the **entrypoint** to `api/main.ts`.
5. Add an environment variable:
   - **Key**: `DATABASE_URL`
   - **Value**: your YugabyteDB connection string
6. Click **Deploy**.
7. Every future `git push` to `main` will auto-deploy.

### 4. Run the Initial ETL

1. In your GitHub repo, go to **Actions → Daily IMDb ETL**.
2. Click **Run workflow**.
3. Wait **~10–15 minutes** for the first run to finish. This seeds the database.
4. The workflow runs automatically every day at **09:00 UTC**.
   - **If IMDb files are unchanged:** exits in **~30 seconds** (hash-check only).
   - **If only ratings changed:** fast upsert in **~2–3 minutes**.
   - **If episodes/titles changed:** full reload in **~10–15 minutes**.

## API Endpoints

All responses include `Cache-Control: public, max-age=3600` because the data only changes daily.

### `GET /`
Health check.

```json
{
  "status": "ok",
  "source": "imdb-non-commercial-datasets"
}
```

### `GET /title/:tconst`
Get any title (movie, series, or episode) with its rating.

**Example:** `/title/tt0944947`

```json
{
  "tconst": "tt0944947",
  "title_type": "tvSeries",
  "primary_title": "Game of Thrones",
  "start_year": 2011,
  "runtime_minutes": 57,
  "genres": "Action,Adventure,Drama",
  "average_rating": 9.2,
  "num_votes": 2150000
}
```

### `GET /series/:tconst`
Get a series plus **all episodes grouped by season**.

**Example:** `/series/tt0944947`

```json
{
  "series": { ... },
  "seasons": {
    "1": [
      { "tconst": "tt1480055", "season_number": 1, "episode_number": 1, ... },
      ...
    ],
    "2": [ ... ]
  }
}
```

### `GET /series/:tconst/season/:season`
Get episodes for a specific season only.

**Example:** `/series/tt0944947/season/1`

```json
[
  { "tconst": "tt1480055", "episode_number": 1, ... },
  ...
]
```

### `GET /search?q=...`
Case-insensitive search over title names. Returns up to 20 results sorted by vote count.

**Example:** `/search?q=game%20of%20thrones`

```json
[
  { "tconst": "tt0944947", "primary_title": "Game of Thrones", ... },
  ...
]
```

## Local Testing

### Test the ETL locally

```bash
pip install -r etl/requirements.txt
DATABASE_URL="your-yugabyte-url" python etl/main.py
```

### Test the API locally

Requires [Deno](https://deno.land/):

```bash
DATABASE_URL="your-yugabyte-url" deno run --allow-net --allow-env api/main.ts
```

Then visit `http://localhost:8000/title/tt0944947`.

## How the Daily Update Works

1. **GitHub Actions** spins up an Ubuntu runner at 09:00 UTC.
2. The **ETL script** downloads the three IMDb TSV dumps and computes a SHA256 hash for each.
3. It compares hashes against the previous run stored in the database (`etl_state` table):
   - **If all hashes match:** job exits immediately (~30 seconds).
   - **If only ratings changed:** loads ratings into a temp table and upserts directly into the live `ratings` table (~2–3 minutes).
   - **If titles or episodes changed:** runs a full rebuild.
4. For full rebuilds, it filters `title.basics` and creates **staging tables** with `tconst` as the primary key (no expensive `id SERIAL` rewrite).
5. It bulk-loads data using PostgreSQL `COPY FORMAT TEXT` (the absolute fastest path).
6. It builds only the necessary B-tree indexes on staging tables (no heavy GIN index rebuilds).
7. It **swaps** staging tables with live tables in a single atomic transaction. The API experiences **near-zero downtime**.
8. Old tables are dropped and temp files are cleaned up.

## Cost & Limits

| Service | Free Tier Limit | Our Usage |
|---------|-----------------|-----------|
| YugabyteDB | 10 GB storage, 1 vCPU | ~3 GB total |
| Deno Deploy | 1M requests/day | Well within limit |
| GitHub Actions | 2,000 min/month | ~15 min/day (first run), ~2 min/day (typical) |

**Total monthly cost: $0.**

## Troubleshooting

### ETL fails with timeout
Increase `timeout-minutes` in `.github/workflows/etl.yml` (default is 120). The optimized ETL typically finishes in 10–15 minutes for a full rebuild.

### Deno Deploy can't connect to DB
Make sure your YugabyteDB cluster allows connections from your Deno Deploy project's egress IPs. In YugabyteDB **Network Access**, add `0.0.0.0/0` temporarily to test, then restrict to Deno Deploy's ranges if desired.

### Search is slow
Search uses `ILIKE` (substring match) without a GIN index. This is a trade-off for faster ETL builds. If you need faster search, you can add `CREATE INDEX idx_titles_search ON titles USING gin(to_tsvector('english', primary_title));` manually after the initial load.

### Storage grows over time
The dataset grows slowly. If you ever approach the 10 GB limit, edit `etl/main.py` and remove `"movie"` from `KEEP_TYPES` to save ~1 GB.
