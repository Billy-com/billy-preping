const { getPool, sql } = require('./db');

const CACHE_TTL_MS = 60_000;

// ── Fanout endpoints cache ────────────────────────────────────────────────────

let fanoutCache = { endpoints: [], loadedAt: 0 };

async function getActiveFanoutEndpoints() {
  if (Date.now() - fanoutCache.loadedAt < CACHE_TTL_MS) {
    return fanoutCache.endpoints;
  }
  const pool = await getPool();
  const result = await pool
    .request()
    .query('SELECT * FROM fanout_endpoints WHERE enabled = 1');
  fanoutCache = {
    endpoints: result.recordset.map((row) => ({
      ...row,
      rules: row.rules ? JSON.parse(row.rules) : {},
    })),
    loadedAt: Date.now(),
  };
  return fanoutCache.endpoints;
}

function invalidateFanoutCache() {
  fanoutCache.loadedAt = 0;
}

// ── System config cache ───────────────────────────────────────────────────────

let configCache = { values: {}, loadedAt: 0 };

async function getAllConfig() {
  if (Date.now() - configCache.loadedAt < CACHE_TTL_MS) {
    return configCache.values;
  }
  const pool = await getPool();
  const result = await pool.request().query('SELECT [key], value FROM system_config');
  const values = {};
  for (const row of result.recordset) {
    values[row.key] = row.value;
  }
  configCache = { values, loadedAt: Date.now() };
  return values;
}

async function getConfig(key) {
  const values = await getAllConfig();
  return values[key] ?? null;
}

function invalidateConfigCache() {
  configCache.loadedAt = 0;
}

// ── Publisher mappings cache ─────────────────────────────────────────────────
// Source of truth for publisher enabled/disabled state on the hot ping path.

let pubMappingsCache = { rows: [], loadedAt: 0 };

async function getPublisherMappings() {
  if (Date.now() - pubMappingsCache.loadedAt < CACHE_TTL_MS) {
    return pubMappingsCache.rows;
  }
  const pool = await getPool();
  const result = await pool
    .request()
    .query('SELECT publisher_id, rtb_id, campaign, enabled, sms_enabled FROM publisher_rtb_mappings');
  pubMappingsCache = { rows: result.recordset, loadedAt: Date.now() };
  return pubMappingsCache.rows;
}

/**
 * Returns false only when the publisher is explicitly disabled.
 * Unknown publishers (no row in the table) default to allowed.
 */
async function isPublisherEnabled(publisherId) {
  if (!publisherId) return true;
  const rows = await getPublisherMappings();
  const match = rows.find((r) => r.publisher_id === publisherId);
  if (!match) return true;
  return match.enabled !== false && match.enabled !== 0;
}

function invalidatePublisherMappingsCache() {
  pubMappingsCache.loadedAt = 0;
}

module.exports = {
  getActiveFanoutEndpoints,
  invalidateFanoutCache,
  getConfig,
  getAllConfig,
  invalidateConfigCache,
  getPublisherMappings,
  isPublisherEnabled,
  invalidatePublisherMappingsCache,
};
