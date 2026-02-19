#!/usr/bin/env node
/**
 * Migrate SQLite cache.db → Postgres
 *
 * Streams rows from SQLite in batches, skips expired rows,
 * and upserts into Postgres (ON CONFLICT DO UPDATE).
 *
 * Usage:
 *   node scripts/migrate-sqlite-to-postgres.js [sqlite-path]
 *
 * Defaults to: /var/lib/docker/volumes/sootio-stremio-addon_sootio-data/_data/cache.db
 *
 * Requires env vars (or .env file) for Postgres connection:
 *   POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
 */

import Database from 'better-sqlite3';
import pg from 'pg';
import { config } from 'dotenv';
import { existsSync } from 'fs';

config(); // load .env

const BATCH_SIZE = 5000;
const PG_BATCH_SIZE = 500; // rows per INSERT statement

const sqlitePath = process.argv[2] ||
  '/var/lib/docker/volumes/sootio-stremio-addon_sootio-data/_data/cache.db';

if (!existsSync(sqlitePath)) {
  console.error(`SQLite file not found: ${sqlitePath}`);
  process.exit(1);
}

// Postgres connection
const pool = new pg.Pool({
  host: process.env.POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'sootio',
  user: process.env.POSTGRES_USER || 'sootio',
  password: process.env.POSTGRES_PASSWORD || 'sootio',
  ssl: process.env.POSTGRES_SSL === 'true' ? { rejectUnauthorized: false } : false,
  max: 5,
  connectionTimeoutMillis: 10000,
  // No statement timeout for bulk imports
  statement_timeout: 0,
});

async function ensureTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS cache (
      service TEXT NOT NULL,
      hash TEXT NOT NULL,
      file_name TEXT,
      size BIGINT,
      data TEXT,
      release_key TEXT,
      category TEXT,
      resolution TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      expires_at TIMESTAMPTZ,
      PRIMARY KEY (service, hash)
    )
  `);
  await pool.query('CREATE INDEX IF NOT EXISTS idx_cache_release_key ON cache(service, release_key)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_cache_expires_at ON cache(expires_at)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_cache_hash_service ON cache(hash, service)');
}

function buildUpsertQuery(rows) {
  const values = [];
  const placeholders = [];
  let idx = 1;

  for (const row of rows) {
    placeholders.push(
      `($${idx}, $${idx+1}, $${idx+2}, $${idx+3}, $${idx+4}, $${idx+5}, $${idx+6}, $${idx+7}, $${idx+8}, $${idx+9}, $${idx+10})`
    );
    values.push(
      (row.service || '').toLowerCase(),
      (row.hash || '').toLowerCase(),
      row.fileName || null,
      row.size != null && Number.isFinite(Number(row.size)) ? Math.round(Number(row.size)) : null,
      row.data || null,
      row.releaseKey || null,
      row.category || null,
      row.resolution || null,
      row.createdAt || new Date().toISOString(),
      row.updatedAt || new Date().toISOString(),
      row.expiresAt || null,
    );
    idx += 11;
  }

  const sql = `
    INSERT INTO cache
      (service, hash, file_name, size, data, release_key, category, resolution, created_at, updated_at, expires_at)
    VALUES ${placeholders.join(',\n')}
    ON CONFLICT (service, hash)
    DO UPDATE SET
      file_name = EXCLUDED.file_name,
      size = EXCLUDED.size,
      data = EXCLUDED.data,
      release_key = EXCLUDED.release_key,
      category = EXCLUDED.category,
      resolution = EXCLUDED.resolution,
      updated_at = EXCLUDED.updated_at,
      expires_at = EXCLUDED.expires_at
    WHERE EXCLUDED.updated_at > cache.updated_at
  `;

  return { sql, values };
}

async function migrate() {
  console.log(`Opening SQLite: ${sqlitePath}`);
  const db = new Database(sqlitePath, { readonly: true });
  db.pragma('journal_mode = WAL');
  db.pragma('cache_size = 20000');
  db.pragma('mmap_size = 268435456'); // 256MB mmap for faster reads

  // Get total count (fast with rowid scan)
  const { total } = db.prepare('SELECT MAX(id) as total FROM cache').get();
  console.log(`SQLite max ID: ${total}`);

  // Test Postgres connection
  const pgClient = await pool.connect();
  console.log('Postgres connected');
  pgClient.release();

  await ensureTable(pool);
  console.log('Postgres table verified');

  // Count existing Postgres rows
  const { rows: [{ count: pgBefore }] } = await pool.query('SELECT COUNT(*) as count FROM cache');
  console.log(`Postgres rows before: ${pgBefore}`);

  // Iterate using id ranges for predictable batching (no OFFSET)
  const stmt = db.prepare(`
    SELECT service, hash, fileName, size, data, releaseKey, category, resolution,
           createdAt, updatedAt, expiresAt
    FROM cache
    WHERE id > ? AND id <= ?
      AND (expiresAt IS NULL OR expiresAt > datetime('now'))
      AND service IS NOT NULL AND service != ''
      AND hash IS NOT NULL AND hash != ''
  `);

  let processed = 0;
  let inserted = 0;
  let skipped = 0;
  let errors = 0;
  const startTime = Date.now();
  let lastReport = startTime;

  for (let rangeStart = 0; rangeStart < total; rangeStart += BATCH_SIZE) {
    const rangeEnd = rangeStart + BATCH_SIZE;
    const rows = stmt.all(rangeStart, rangeEnd);
    processed += BATCH_SIZE;

    if (rows.length === 0) continue;

    // Deduplicate within batch: keep last occurrence per (service, hash)
    const deduped = new Map();
    for (const row of rows) {
      const key = `${(row.service || '').toLowerCase()}|${(row.hash || '').toLowerCase()}`;
      deduped.set(key, row);
    }

    const uniqueRows = [...deduped.values()];

    // Insert in sub-batches
    for (let i = 0; i < uniqueRows.length; i += PG_BATCH_SIZE) {
      const batch = uniqueRows.slice(i, i + PG_BATCH_SIZE);
      try {
        const { sql, values } = buildUpsertQuery(batch);
        await pool.query(sql, values);
        inserted += batch.length;
      } catch (err) {
        errors += batch.length;
        if (errors <= 5000) {
          console.error(`Upsert error (batch of ${batch.length}): ${err.message.slice(0, 300)}`);
          // Log first row's service/hash for debugging
          const r = batch[0];
          console.error(`  First row: service=${r.service}, hash=${(r.hash||'').slice(0,20)}, data_len=${(r.data||'').length}`);
        }
      }
    }

    // Progress report every 10 seconds
    const now = Date.now();
    if (now - lastReport > 10000) {
      const pct = ((processed / total) * 100).toFixed(1);
      const elapsed = ((now - startTime) / 1000).toFixed(0);
      const rate = (inserted / ((now - startTime) / 1000)).toFixed(0);
      console.log(`Progress: ${pct}% | Processed IDs: ${processed}/${total} | Inserted: ${inserted} | Errors: ${errors} | ${rate} rows/s | ${elapsed}s elapsed`);
      lastReport = now;
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\nMigration complete in ${elapsed}s`);
  console.log(`  Inserted/updated: ${inserted}`);
  console.log(`  Errors: ${errors}`);

  // Count final Postgres rows
  const { rows: [{ count: pgAfter }] } = await pool.query('SELECT COUNT(*) as count FROM cache');
  console.log(`  Postgres rows before: ${pgBefore}`);
  console.log(`  Postgres rows after: ${pgAfter}`);
  console.log(`  Net new rows: ${pgAfter - pgBefore}`);

  db.close();
  await pool.end();
}

migrate().catch(err => {
  console.error('Migration failed:', err);
  process.exit(1);
});
