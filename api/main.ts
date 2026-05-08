import { Hono } from "https://deno.land/x/hono@v3.12.0/mod.ts";
import { cors } from "https://deno.land/x/hono@v3.12.0/middleware/cors/index.ts";
import { query } from "./db.ts";

const app = new Hono();

app.use("*", cors());

app.use("*", async (c, next) => {
  await next();
  if (c.res.status === 200) {
    c.res.headers.set("Cache-Control", "public, max-age=3600");
  }
});

app.get("/", (c) => {
  return c.json({
    status: "ok",
    source: "imdb-non-commercial-datasets",
    endpoints: {
      "GET /title/:tconst": "Get any title (movie, series, episode) with rating",
      "GET /series/:tconst": "Get series metadata + all episodes grouped by season",
      "GET /series/:tconst/season/:season": "Get episodes for a specific season only",
      "GET /search?q=...": "Full-text search over titles (min 2 chars)",
    },
    example_tconsts: {
      "Game of Thrones (Series)": "tt0944947",
      "Breaking Bad (Series)": "tt0903747",
      "The Office (US)": "tt0386676",
      "The Dark Knight (Movie)": "tt0468569",
    },
  });
});

app.get("/title/:tconst", async (c) => {
  const tconst = c.req.param("tconst");
  const rows = await query(
    `SELECT t.tconst, t.title_type, t.primary_title, t.start_year, t.runtime_minutes, t.genres,
            r.average_rating, r.num_votes
     FROM titles t
     LEFT JOIN ratings r ON t.tconst = r.tconst
     WHERE t.tconst = $1`,
    [tconst],
  );
  if (rows.length === 0) {
    return c.json({ error: "Not found" }, 404);
  }
  return c.json(rows[0]);
});

app.get("/series/:tconst", async (c) => {
  const tconst = c.req.param("tconst");
  const seriesRows = await query(
    `SELECT t.tconst, t.title_type, t.primary_title, t.start_year, t.runtime_minutes, t.genres,
            r.average_rating, r.num_votes
     FROM titles t
     LEFT JOIN ratings r ON t.tconst = r.tconst
     WHERE t.tconst = $1 AND t.title_type IN ('tvSeries', 'tvMiniSeries')`,
    [tconst],
  );
  if (seriesRows.length === 0) {
    return c.json({ error: "Series not found" }, 404);
  }

  const episodes = await query(
    `SELECT e.tconst, e.season_number, e.episode_number,
            t.primary_title, t.start_year, t.runtime_minutes,
            r.average_rating, r.num_votes
     FROM episodes e
     JOIN titles t ON e.tconst = t.tconst
     LEFT JOIN ratings r ON e.tconst = r.tconst
     WHERE e.parent_tconst = $1
     ORDER BY e.season_number NULLS LAST, e.episode_number NULLS LAST`,
    [tconst],
  );

  const seasons: Record<string, Record<string, unknown>[]> = {};
  for (const ep of episodes) {
    const s = ep.season_number ?? "unknown";
    if (!seasons[s]) seasons[s] = [];
    seasons[s].push(ep);
  }

  return c.json({
    series: seriesRows[0],
    seasons,
  });
});

app.get("/series/:tconst/season/:season", async (c) => {
  const tconst = c.req.param("tconst");
  const season = parseInt(c.req.param("season"), 10);
  if (isNaN(season)) {
    return c.json({ error: "Invalid season number" }, 400);
  }

  const rows = await query(
    `SELECT e.tconst, e.season_number, e.episode_number,
            t.primary_title, t.start_year, t.runtime_minutes,
            r.average_rating, r.num_votes
     FROM episodes e
     JOIN titles t ON e.tconst = t.tconst
     LEFT JOIN ratings r ON e.tconst = r.tconst
     WHERE e.parent_tconst = $1 AND e.season_number = $2
     ORDER BY e.episode_number NULLS LAST`,
    [tconst, season],
  );

  return c.json(rows);
});

app.get("/search", async (c) => {
  const q = c.req.query("q");
  if (!q || q.length < 2) {
    return c.json({ error: "Query must be at least 2 characters" }, 400);
  }

  const pattern = `%${q}%`;
  const rows = await query(
    `SELECT t.tconst, t.title_type, t.primary_title, t.start_year,
            r.average_rating, r.num_votes
     FROM titles t
     LEFT JOIN ratings r ON t.tconst = r.tconst
     WHERE t.primary_title ILIKE $1
     ORDER BY r.num_votes DESC NULLS LAST
     LIMIT 20`,
    [pattern],
  );

  return c.json(rows);
});

Deno.serve(app.fetch);
