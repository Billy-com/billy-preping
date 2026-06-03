/**
 * Mid-Term Storage (MTS) write helper.
 *
 * Called fire-and-forget from ping.js after writePingToDb completes.
 * Resolves the vertical partition from vertical_campaign_map (60s cache),
 * inserts into mts_{vertical}, then hands off to the sequencer.
 */
const { getPool, sql } = require('./db');
const { getConfig } = require('./config-cache');
const { runSequencer } = require('./sequencer');
const { v4: uuidv4 } = require('uuid');

const VERTICAL_CACHE_TTL_MS = 60_000;
let verticalCache = { map: {}, loadedAt: 0 };

async function resolveVertical(pool, campaign) {
  if (Date.now() - verticalCache.loadedAt < VERTICAL_CACHE_TTL_MS) {
    return verticalCache.map[campaign] ?? null;
  }
  const result = await pool.request().query(
    'SELECT campaign, vertical FROM vertical_campaign_map WHERE enabled = 1',
  );
  const map = {};
  for (const row of result.recordset) map[row.campaign] = row.vertical;
  verticalCache = { map, loadedAt: Date.now() };
  return map[campaign] ?? null;
}

const VALID_VERTICALS = new Set(['auto', 'health', 'medicare', 'home']);

async function writeMidTermStorage({
  pingId, phone, zip, publisher_id, subid, campaign,
  ringbaStatus, bidAmount, buyerId, routingNumber, won,
  ffgSpineId = null, ffgLineType = null,
}) {
  const enabled = await getConfig('mts_enabled');
  if (enabled !== '1') return;

  const pool = await getPool();

  // Resolve vertical — fall back to mts_default_vertical config, then 'auto'
  let vertical = await resolveVertical(pool, campaign);
  if (!vertical) {
    vertical = await getConfig('mts_default_vertical') ?? 'auto';
  }
  if (!VALID_VERTICALS.has(vertical)) vertical = 'auto';

  const mtsId = uuidv4();
  const requiresEnrichment = !zip ? 1 : 0;

  await pool.request()
    .input('id',                  sql.UniqueIdentifier, mtsId)
    .input('ping_id',             sql.UniqueIdentifier, pingId)
    .input('phone',               sql.NVarChar(20),     phone)
    .input('zip',                 sql.NVarChar(10),     zip ?? null)
    .input('publisher_id',        sql.NVarChar(255),    publisher_id)
    .input('subid',               sql.NVarChar(255),    subid ?? null)
    .input('campaign',            sql.NVarChar(255),    campaign)
    .input('rtb_status',          sql.NVarChar(20),     ringbaStatus ?? 'no_bid')
    .input('bid_amount',          sql.Decimal(10, 4),   bidAmount ?? null)
    .input('buyer_id',            sql.NVarChar(255),    buyerId ?? null)
    .input('routing_number',      sql.NVarChar(50),     routingNumber ?? null)
    .input('won',                 sql.Bit,              won ? 1 : 0)
    .input('requires_enrichment', sql.Bit,              requiresEnrichment)
    .query(`
      INSERT INTO [mts_${vertical}]
        (id, ping_id, phone, zip, publisher_id, subid, campaign,
         rtb_status, bid_amount, buyer_id, routing_number, won, requires_enrichment)
      VALUES
        (@id, @ping_id, @phone, @zip, @publisher_id, @subid, @campaign,
         @rtb_status, @bid_amount, @buyer_id, @routing_number, @won, @requires_enrichment)
    `);

  // Hand off to sequencer — fire-and-forget from caller's perspective
  runSequencer({
    mtsId, pingId, phone, vertical, campaign,
    rtbStatus: ringbaStatus, bidAmount,
    ffgSpineId, ffgLineType,
  }).catch((err) => console.error('[mts] sequencer error:', err.message));
}

module.exports = { writeMidTermStorage };
