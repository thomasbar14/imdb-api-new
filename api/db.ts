import { Pool } from "https://deno.land/x/postgres@v0.17.0/mod.ts";

const DATABASE_URL = Deno.env.get("DATABASE_URL");

if (!DATABASE_URL) {
  throw new Error("DATABASE_URL is not set");
}

const DATABASE_CA_CERT = await Deno.readTextFile(
  new URL("./yugabyte-ca.crt", import.meta.url),
);

const url = new URL(DATABASE_URL);
const pool = new Pool(
  {
    hostname: url.hostname,
    port: Number(url.port) || 5433,
    database: url.pathname.slice(1).split("?")[0],
    user: decodeURIComponent(url.username),
    password: decodeURIComponent(url.password),
    tls: {
      enabled: true,
      enforce: true,
      caCertificates: [DATABASE_CA_CERT],
    },
  },
  10,
);

export async function query(sql: string, params?: unknown[]) {
  const client = await pool.connect();
  try {
    const result = await client.queryObject(sql, params);
    return result.rows as Record<string, unknown>[];
  } finally {
    client.release();
  }
}
