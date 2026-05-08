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
   postgresql://admin:xxxxxxxx@xxx.yugabyte.cloud:5433/imdb?sslmode=require&sslrootcert=/path/to/cert.crt
   ```
   For GitHub Actions & Deno Deploy, you typically only need:
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
3. Wait ~20–40 minutes for the job to finish. This seeds the database.
4. The workflow will then run automatically every day at **09:00 UTC**.

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
Full-text search over title names. Returns up to 20 results sorted by vote count.

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
2. The **ETL script** downloads the three IMDb TSV dumps:
   - `title.basics.tsv.gz`
   - `title.ratings.tsv.gz`
   - `title.episode.tsv.gz`
3. It filters `title.basics` to keep only **movies**, **series**, **mini-series**, and **episodes** (drops shorts, videos, games, etc.).
4. It creates **staging tables** (`titles_new`, `episodes_new`, `ratings_new`).
5. It bulk-loads the data using PostgreSQL `COPY` (fastest method).
6. It builds all indexes on the staging tables.
7. It **swaps** the staging tables with the live tables in a single atomic transaction. The API experiences near-zero downtime.
8. Old tables are dropped and temp files are cleaned up.

## Cost & Limits

| Service | Free Tier Limit | Our Usage |
|---------|-----------------|-----------|
| YugabyteDB | 10 GB storage, 1 vCPU | ~3 GB total |
| Deno Deploy | 1M requests/day | Well within limit |
| GitHub Actions | 2,000 min/month | ~30 min/day |

**Total monthly cost: $0.**

## Troubleshooting

### ETL fails with timeout
Increase `timeout-minutes` in `.github/workflows/etl.yml` (default is 120).

### Deno Deploy can't connect to DB
Make sure your YugabyteDB cluster allows connections from your Deno Deploy project's egress IPs. In YugabyteDB **Network Access**, add `0.0.0.0/0` temporarily to test, then restrict to Deno Deploy's ranges if desired.

### Search is slow
The first ETL run creates a GIN index. If it is still slow, check that `ANALYZE` ran successfully (it does, in `etl/main.py`).

### Storage grows over time
The dataset grows slowly. If you ever approach the 10 GB limit, edit `etl/main.py` and remove `"movie"` from `KEEP_TYPES` to save ~1 GB.
