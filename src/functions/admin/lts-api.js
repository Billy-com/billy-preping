/**
 * Long-Term Storage (LTS) Read-Only Admin API
 *
 * GET /mgmt/lts/daily-stats       ?vertical=&from=&to=&limit=90&offset=0
 * GET /mgmt/lts/publisher-quality ?min_pings=10&limit=100&offset=0
 * GET /mgmt/lts/phone-profiles    ?publisher_id=&vertical=&zip=&sms_state=&limit=100&offset=0
 * GET /mgmt/lts/geo-intelligence  ?state=&min_pings=5&limit=200&offset=0
 * GET /mgmt/lts/lead-scores       ?tier=&vertical=&limit=100&offset=0
 * GET /mgmt/lts/outcome-events    ?outcome_type=&vertical=&from=&to=&limit=200&offset=0
 *
 * All require admin key. All return JSON arrays.
 */
const { app } = require('@azure/functions');
const { getPool, sql } = require('../../lib/db');
const { requireAdminKey, corsHeaders } = require('./middleware');

// ── Helpers ───────────────────────────────────────────────────────────────────

function json(body, status = 200) {
  return {
    status,
    headers: { 'Content-Type': 'application/json', ...corsHeaders() },
    body: JSON.stringify(body),
  };
}

function parseIntClamped(val, def, min, max) {
  const n = parseInt(val ?? String(def), 10);
  return Math.min(max, Math.max(min, isNaN(n) ? def : n));
}

function parseDateParam(val) {
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d;
}

// ── OPTIONS preflight for all LTS routes ─────────────────────────────────────
app.http('ltsOptions', {
  methods: ['OPTIONS'],
  route: 'mgmt/lts/{*rest}',
  authLevel: 'anonymous',
  handler: async () => ({ status: 204, headers: corsHeaders(), body: '' }),
});

// ── GET /mgmt/lts/daily-stats ─────────────────────────────────────────────────
app.http('ltsDailyStats', {
  methods: ['GET'],
  route: 'mgmt/lts/daily-stats',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req);
    if (authError) return authError;

    const vertical = req.query.get('vertical') ?? null;
    const from     = parseDateParam(req.query.get('from'));
    const to       = parseDateParam(req.query.get('to'));
    const limit    = parseIntClamped(req.query.get('limit'),  90, 1, 500);
    const offset   = parseIntClamped(req.query.get('offset'),  0, 0, 1_000_000);

    const pool = await getPool();
    const request = pool.request()
      .input('limit',  sql.Int, limit)
      .input('offset', sql.Int, offset);

    const conditions = [];
    if (vertical) { conditions.push('vertical = @vertical');   request.input('vertical', sql.NVarChar(50), vertical); }
    if (from)     { conditions.push('stat_date >= @from');     request.input('from', sql.Date, from); }
    if (to)       { conditions.push('stat_date <= @to');       request.input('to',   sql.Date, to); }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const result = await request.query(`
      SELECT
        stat_date, vertical, campaign, publisher_id,
        total_pings, total_bids, total_wins, total_no_bids,
        total_sms_sent, total_sms_responses, total_conversions,
        avg_bid_amount, max_bid_amount, total_revenue,
        avg_response_ms, p95_response_ms,
        enrichment_rate, suppression_rate, category_match_rate,
        computed_at
      FROM lt_daily_stats
      ${where}
      ORDER BY stat_date DESC, vertical
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `);

    return json(result.recordset);
  },
});

// ── GET /mgmt/lts/publisher-quality ──────────────────────────────────────────
app.http('ltsPublisherQuality', {
  methods: ['GET'],
  route: 'mgmt/lts/publisher-quality',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req);
    if (authError) return authError;

    const minPings  = parseIntClamped(req.query.get('min_pings'), 10, 0, 100_000);
    const tier      = req.query.get('tier')      ?? null;
    const vertical  = req.query.get('vertical')  ?? null;
    const limit     = parseIntClamped(req.query.get('limit'),  100, 1, 500);
    const offset    = parseIntClamped(req.query.get('offset'),   0, 0, 1_000_000);

    const pool = await getPool();
    const request = pool.request()
      .input('min_pings', sql.Int, minPings)
      .input('limit',     sql.Int, limit)
      .input('offset',    sql.Int, offset);

    const conditions = ['total_pings >= @min_pings'];
    if (tier)     { conditions.push('quality_tier = @tier');   request.input('tier',     sql.NVarChar(20), tier); }
    if (vertical) { conditions.push('vertical = @vertical');   request.input('vertical', sql.NVarChar(50), vertical); }
    const where = `WHERE ${conditions.join(' AND ')}`;

    const result = await request.query(`
      SELECT
        publisher_id, vertical, total_pings,
        bid_rate, win_rate, duplicate_rate, invalid_phone_rate,
        avg_bid_amount, avg_response_ms,
        quality_score, quality_tier,
        first_ping_at, last_ping_at, updated_at
      FROM lt_publisher_quality
      ${where}
      ORDER BY quality_score DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `);

    return json(result.recordset);
  },
});

// ── GET /mgmt/lts/phone-profiles ─────────────────────────────────────────────
app.http('ltsPhoneProfiles', {
  methods: ['GET'],
  route: 'mgmt/lts/phone-profiles',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req);
    if (authError) return authError;

    const publisherId = req.query.get('publisher_id') ?? null;
    const zip         = req.query.get('zip')          ?? null;
    const phone       = req.query.get('phone')        ?? null;
    const limit       = parseIntClamped(req.query.get('limit'),  100, 1, 500);
    const offset      = parseIntClamped(req.query.get('offset'),   0, 0, 1_000_000);

    const pool = await getPool();
    const request = pool.request()
      .input('limit',  sql.Int, limit)
      .input('offset', sql.Int, offset);

    const conditions = [];
    if (publisherId) { conditions.push('publisher_id = @publisher_id'); request.input('publisher_id', sql.NVarChar(255), publisherId); }
    if (zip)         { conditions.push('zip = @zip');                   request.input('zip',          sql.NVarChar(10),  zip); }
    if (phone)       { conditions.push('phone = @phone');               request.input('phone',        sql.NVarChar(20),  phone); }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const result = await request.query(`
      SELECT
        phone, zip, state, city, dma, county, timezone,
        first_seen_at, last_seen_at,
        total_pings, total_bids, total_wins,
        total_sms_sent, total_sms_responses, total_conversions,
        avg_bid_amount, max_bid_amount, last_bid_amount, last_bid_at,
        sms_opt_out, sms_opt_out_at,
        last_contacted_at, last_contact_channel,
        ck_enrolled_count,
        enriched, enriched_at, enrichment_source,
        updated_at
      FROM lt_phone_profile
      ${where}
      ORDER BY last_seen_at DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `);

    return json(result.recordset);
  },
});

// ── GET /mgmt/lts/geo-intelligence ───────────────────────────────────────────
app.http('ltsGeoIntelligence', {
  methods: ['GET'],
  route: 'mgmt/lts/geo-intelligence',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req);
    if (authError) return authError;

    const state    = req.query.get('state')     ?? null;
    const dma      = req.query.get('dma')       ?? null;
    const minPings = parseIntClamped(req.query.get('min_pings'), 5, 0, 100_000);
    const limit    = parseIntClamped(req.query.get('limit'),  200, 1, 1000);
    const offset   = parseIntClamped(req.query.get('offset'),   0, 0, 1_000_000);

    const pool = await getPool();
    const request = pool.request()
      .input('min_pings', sql.Int, minPings)
      .input('limit',     sql.Int, limit)
      .input('offset',    sql.Int, offset);

    const conditions = ['total_pings >= @min_pings'];
    if (state) { conditions.push('state = @state'); request.input('state', sql.NVarChar(50), state); }
    if (dma)   { conditions.push('dma = @dma');     request.input('dma',   sql.NVarChar(100), dma); }
    const where = `WHERE ${conditions.join(' AND ')}`;

    const result = await request.query(`
      SELECT
        zip, state, dma, county, timezone,
        total_pings, total_bids, bid_rate, avg_bid_amount,
        vertical_breakdown,
        avg_sms_response_rate,
        best_contact_hour, best_contact_day,
        updated_at
      FROM lt_geo_intelligence
      ${where}
      ORDER BY total_pings DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `);

    return json(result.recordset);
  },
});

// ── GET /mgmt/lts/lead-scores ─────────────────────────────────────────────────
app.http('ltsLeadScores', {
  methods: ['GET'],
  route: 'mgmt/lts/lead-scores',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req);
    if (authError) return authError;

    const tier     = req.query.get('tier')     ?? null;
    const vertical = req.query.get('vertical') ?? null;
    const phone    = req.query.get('phone')    ?? null;
    const limit    = parseIntClamped(req.query.get('limit'),  100, 1, 500);
    const offset   = parseIntClamped(req.query.get('offset'),   0, 0, 1_000_000);

    const pool = await getPool();
    const request = pool.request()
      .input('limit',  sql.Int, limit)
      .input('offset', sql.Int, offset);

    const conditions = [];
    if (tier)     { conditions.push('score_tier = @tier');   request.input('tier',     sql.NVarChar(20), tier); }
    if (vertical) { conditions.push('vertical = @vertical'); request.input('vertical', sql.NVarChar(50), vertical); }
    if (phone)    { conditions.push('phone = @phone');       request.input('phone',    sql.NVarChar(20), phone); }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const result = await request.query(`
      SELECT
        phone, vertical,
        score, score_tier, score_factors,
        conversion_prob, estimated_value,
        model_version, scored_at, expires_at
      FROM lt_lead_scores
      ${where}
      ORDER BY score DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `);

    return json(result.recordset);
  },
});

// ── GET /mgmt/lts/outcome-events ─────────────────────────────────────────────
app.http('ltsOutcomeEvents', {
  methods: ['GET'],
  route: 'mgmt/lts/outcome-events',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req);
    if (authError) return authError;

    const outcomeType = req.query.get('outcome_type') ?? null;
    const vertical    = req.query.get('vertical')     ?? null;
    const phone       = req.query.get('phone')        ?? null;
    const from        = parseDateParam(req.query.get('from'));
    const to          = parseDateParam(req.query.get('to'));
    const limit       = parseIntClamped(req.query.get('limit'),  200, 1, 1000);
    const offset      = parseIntClamped(req.query.get('offset'),   0, 0, 1_000_000);

    const pool = await getPool();
    const request = pool.request()
      .input('limit',  sql.Int, limit)
      .input('offset', sql.Int, offset);

    const conditions = [];
    if (outcomeType) { conditions.push('outcome_type = @outcome_type'); request.input('outcome_type', sql.NVarChar(50),  outcomeType); }
    if (vertical)    { conditions.push('vertical = @vertical');         request.input('vertical',     sql.NVarChar(50),  vertical); }
    if (phone)       { conditions.push('phone = @phone');               request.input('phone',        sql.NVarChar(20),  phone); }
    if (from)        { conditions.push('occurred_at >= @from');         request.input('from',         sql.DateTimeOffset, from); }
    if (to)          { conditions.push('occurred_at <= @to');           request.input('to',           sql.DateTimeOffset, to); }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const result = await request.query(`
      SELECT
        id, phone, ping_id, mts_id,
        vertical, campaign, publisher_id, buyer_id,
        channel, outcome_type, outcome_value, duration_sec,
        occurred_at, source
      FROM lt_outcome_events
      ${where}
      ORDER BY occurred_at DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `);

    return json(result.recordset);
  },
});
