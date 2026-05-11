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
      "GET /title/:tconst": "Get any title (series or episode) with rating",
      "GET /series/:tconst": "Get series metadata + all episodes grouped by season",
      "GET /series/:tconst/season/:season": "Get episodes for a specific season only",
      "GET /search?q=...": "Full-text search over titles (min 2 chars)",
      "GET /openapi.json": "OpenAPI 3.1 specification",
      "GET /docs": "Interactive Swagger UI",
    },
    example_tconsts: {
      "Game of Thrones (Series)": "tt0944947",
      "Breaking Bad (Series)": "tt0903747",
      "The Office (US)": "tt0386676",
    },
  });
});

const titleSchema = {
  type: "object",
  properties: {
    tconst: { type: "string", example: "tt0944947" },
    title_type: { type: "string", example: "tvSeries" },
    primary_title: { type: "string", example: "Game of Thrones" },
    start_year: { type: ["integer", "null"], example: 2011 },
    runtime_minutes: { type: ["integer", "null"], example: 57 },
    genres: { type: ["string", "null"], example: "Action,Adventure,Drama" },
    average_rating: { type: ["number", "null"], example: 9.2 },
    num_votes: { type: ["integer", "null"], example: 2150000 },
  },
  required: ["tconst", "title_type", "primary_title"],
};

const episodeSchema = {
  type: "object",
  properties: {
    tconst: { type: "string", example: "tt1480055" },
    season_number: { type: ["integer", "null"], example: 1 },
    episode_number: { type: ["integer", "null"], example: 1 },
    primary_title: { type: "string", example: "Winter Is Coming" },
    start_year: { type: ["integer", "null"], example: 2011 },
    runtime_minutes: { type: ["integer", "null"], example: 62 },
    average_rating: { type: ["number", "null"], example: 9.1 },
    num_votes: { type: ["integer", "null"], example: 50000 },
  },
  required: ["tconst", "primary_title"],
};

const errorSchema = {
  type: "object",
  properties: { error: { type: "string" } },
  required: ["error"],
};

const openApiSpec = {
  openapi: "3.1.0",
  info: {
    title: "IMDb Free API",
    version: "1.0.0",
    description:
      "Zero-maintenance, completely free, auto-updating IMDb API. Data sourced from IMDb non-commercial datasets and refreshed daily. All successful responses include `Cache-Control: public, max-age=3600`.",
    license: { name: "IMDb non-commercial datasets terms" },
  },
  servers: [
    { url: "http://localhost:8000", description: "Local dev" },
    { url: "https://imdb-api-new.thomasbar14.deno.net", description: "Deno Deploy" },
  ],
  components: {
    schemas: {
      Title: titleSchema,
      Episode: episodeSchema,
      SeriesWithSeasons: {
        type: "object",
        properties: {
          series: { $ref: "#/components/schemas/Title" },
          seasons: {
            type: "object",
            additionalProperties: {
              type: "array",
              items: { $ref: "#/components/schemas/Episode" },
            },
            description: "Episodes keyed by season number (or \"unknown\").",
          },
        },
        required: ["series", "seasons"],
      },
      Error: errorSchema,
    },
    parameters: {
      Tconst: {
        name: "tconst",
        in: "path",
        required: true,
        schema: { type: "string", pattern: "^tt[0-9]+$" },
        example: "tt0944947",
        description: "IMDb title identifier.",
      },
    },
    responses: {
      NotFound: {
        description: "Resource not found",
        content: { "application/json": { schema: { $ref: "#/components/schemas/Error" } } },
      },
      BadRequest: {
        description: "Invalid request",
        content: { "application/json": { schema: { $ref: "#/components/schemas/Error" } } },
      },
    },
  },
  paths: {
    "/": {
      get: {
        summary: "Service index",
        description: "Health check and endpoint listing.",
        responses: {
          "200": {
            description: "Service metadata",
            content: { "application/json": { schema: { type: "object" } } },
          },
        },
      },
    },
    "/title/{tconst}": {
      get: {
        summary: "Get any title",
        description: "Returns a movie, series, or episode with its rating.",
        parameters: [{ $ref: "#/components/parameters/Tconst" }],
        responses: {
          "200": {
            description: "Title found",
            content: { "application/json": { schema: { $ref: "#/components/schemas/Title" } } },
          },
          "404": { $ref: "#/components/responses/NotFound" },
        },
      },
    },
    "/series/{tconst}": {
      get: {
        summary: "Get series with all episodes grouped by season",
        parameters: [{ $ref: "#/components/parameters/Tconst" }],
        responses: {
          "200": {
            description: "Series and its episodes",
            content: {
              "application/json": { schema: { $ref: "#/components/schemas/SeriesWithSeasons" } },
            },
          },
          "404": { $ref: "#/components/responses/NotFound" },
        },
      },
    },
    "/series/{tconst}/season/{season}": {
      get: {
        summary: "Get episodes for a specific season",
        parameters: [
          { $ref: "#/components/parameters/Tconst" },
          {
            name: "season",
            in: "path",
            required: true,
            schema: { type: "integer", minimum: 1 },
            example: 1,
          },
        ],
        responses: {
          "200": {
            description: "Episodes in season order (may be empty)",
            content: {
              "application/json": {
                schema: { type: "array", items: { $ref: "#/components/schemas/Episode" } },
              },
            },
          },
          "400": { $ref: "#/components/responses/BadRequest" },
        },
      },
    },
    "/search": {
      get: {
        summary: "Search titles by name",
        description:
          "Case-insensitive substring match over `primary_title`. Returns up to 20 results sorted by vote count.",
        parameters: [
          {
            name: "q",
            in: "query",
            required: true,
            schema: { type: "string", minLength: 2 },
            example: "game of thrones",
          },
        ],
        responses: {
          "200": {
            description: "Matching titles",
            content: {
              "application/json": {
                schema: { type: "array", items: { $ref: "#/components/schemas/Title" } },
              },
            },
          },
          "400": { $ref: "#/components/responses/BadRequest" },
        },
      },
    },
  },
};

app.get("/openapi.json", (c) => c.json(openApiSpec));

app.get("/docs", (c) => {
  const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>IMDb Free API — Docs</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
<script>
  window.ui = SwaggerUIBundle({
    url: "/openapi.json",
    dom_id: "#swagger-ui",
    deepLinking: true,
  });
</script>
</body>
</html>`;
  return c.html(html);
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
