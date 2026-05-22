/**
 * Long-Term Storage (LTS) Aggregation Engine
 *
 * Timer trigger: runs nightly at 02:00 UTC
 * Manual trigger: POST /mgmt/lts/run
 *
 * Aggregates live operational tables into lt_* analytics tables:
 *   lt_daily_stats        — daily rollup per vertical
 *   lt_publisher_quality  — lifetime publisher scores
 *   lt_phone_profile      — one row per phone, latest state
 *   lt_geo_intelligence   — per-zip analytics
 *   lt_outcome_events     — delta inserts from postback_log + sequence_triggers
 *   lt_lead_scores        — simple win-rate + recency scoring per phone × vertical
 */
const { app } = require('@azure/functions');
const { getPool, sql } = require('../../lib/db');
const { requireAdminKey } = require('./middleware');

// ── Timer trigger — 02:00 UTC every night ─────────────────────────────────────
app.timer('ltsAggregationTimer', {
  schedule: '0 0 2 * * *',
  handler: async (myTimer, context) => {
    context.log('[lts] Starting nightly aggregation run');
    const result = await runAggregation(context);
    context.log('[lts] Aggregation complete', result);
  },
});

// ── Manual HTTP trigger — POST /mgmt/lts/run ──────────────────────────────────
app.http('ltsRunManual', {
  methods: ['POST'],
  route: 'mgmt/lts/run',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req);
    if (authError) return authError;

    const started = Date.now();
    let result;
    try {
      result = await runAggregation(null);
    } catch (err) {
      return {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ok: false, error: err.message }),
      };
    }

    return {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ok: true, elapsed_ms: Date.now() - started, ...result }),
    };
  },
});

// ─────────────────────────────────────────────────────────────────────────────
// Core aggregation runner
// ─────────────────────────────────────────────────────────────────────────────

async function runAggregation(context) {
  const log = (msg, ...args) => {
    if (context) context.log(msg, ...args);
    else console.log(msg, ...args);
  };

  const pool = await getPool();
  const results = {};

  // Run each aggregation independently so one failure doesn't abort the rest
  for (const [name, fn] of [
    ['lt_daily_stats',       aggregateDailyStats],
    ['lt_publisher_quality', aggregatePublisherQuality],
    ['lt_phone_profile',     aggregatePhoneProfile],
    ['lt_geo_intelligence',  aggregateGeoIntelligence],
    ['lt_outcome_events',    aggregateOutcomeEvents],
    ['lt_lead_scores',       aggregateLeadScores],
  ]) {
    const t = Date.now();
    try {
      const r = await fn(pool, log);
      results[name] = { ok: true, elapsed_ms: Date.now() - t, ...r };
      log(`[lts] ${name} done in ${Date.now() - t}ms`, r);
    } catch (err) {
      results[name] = { ok: false, error: err.message, elapsed_ms: Date.now() - t };
      log(`[lts] ${name} FAILED: ${err.message}`);
    }
  }

  return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. lt_daily_stats
// Roll up per (stat_date, vertical, campaign) from inbound_pings + responses
// ─────────────────────────────────────────────────────────────────────────────

async function aggregateDailyStats(pool, log) {
  // Compute aggregates from live tables — all history, idempotent via MERGE
  const srcResult = await pool.request().query(`
    SELECT
      CONVERT(DATE, p.created_at)              AS stat_date,
      COALESCE(cat.vertical, 'unknown')        AS vertical,
      COALESCE(p.campaign, 'unknown')          AS campaign,
      p.publisher_id,
      COUNT(p.id)                              AS total_pings,
      SUM(CASE WHEN r.bid_amount IS NOT NULL THEN 1 ELSE 0 END)  AS total_bids,
      SUM(CASE WHEN r.won = 1 THEN 1 ELSE 0 END)                 AS total_wins,
      SUM(CASE WHEN r.bid_amount IS NULL
               AND r.id IS NOT NULL THEN 1 ELSE 0 END)           AS total_no_bids,
      SUM(CASE WHEN ch.id IS NOT NULL THEN 1 ELSE 0 END)         AS total_sms_sent,
      AVG(CAST(r.bid_amount AS FLOAT))                           AS avg_bid_amount,
      MAX(r.bid_amount)                                          AS max_bid_amount,
      SUM(CAST(r.bid_amount AS FLOAT))                           AS total_revenue,
      AVG(CAST(r.response_time_ms AS FLOAT))                     AS avg_response_ms
    FROM inbound_pings p
    LEFT JOIN ringba_responses r   ON r.ping_id = p.id
    LEFT JOIN category_mappings cat ON cat.campaign = p.campaign AND cat.enabled = 1
    LEFT JOIN (
      SELECT DISTINCT ping_id, id
      FROM contact_history
      WHERE contact_type = 'sms'
    ) ch ON ch.ping_id = p.id
    GROUP BY
      CONVERT(DATE, p.created_at),
      COALESCE(cat.vertical, 'unknown'),
      COALESCE(p.campaign, 'unknown'),
      p.publisher_id
  `);

  const rows = srcResult.recordset;
  if (!rows.length) return { merged: 0 };

  let merged = 0;
  for (const row of rows) {
    await pool.request()
      .input('stat_date',     sql.Date,          row.stat_date)
      .input('vertical',      sql.NVarChar(50),   row.vertical)
      .input('campaign',      sql.NVarChar(255),  row.campaign)
      .input('publisher_id',  sql.NVarChar(255),  row.publisher_id ?? null)
      .input('total_pings',   sql.Int,            row.total_pings)
      .input('total_bids',    sql.Int,            row.total_bids)
      .input('total_wins',    sql.Int,            row.total_wins)
      .input('total_no_bids', sql.Int,            row.total_no_bids)
      .input('total_sms',     sql.Int,            row.total_sms_sent)
      .input('avg_bid',       sql.Decimal(10, 4), row.avg_bid_amount ?? null)
      .input('max_bid',       sql.Decimal(10, 4), row.max_bid_amount ?? null)
      .input('total_rev',     sql.Decimal(12, 4), row.total_revenue  ?? null)
      .input('avg_ms',        sql.Int,            row.avg_response_ms != null ? Math.round(row.avg_response_ms) : null)
      .query(`
        MERGE lt_daily_stats AS target
        USING (
          SELECT
            @stat_date    AS stat_date,
            @vertical     AS vertical,
            @campaign     AS campaign,
            @publisher_id AS publisher_id
        ) AS src
          ON  target.stat_date    = src.stat_date
          AND target.vertical     = src.vertical
          AND target.campaign     = src.campaign
          AND (target.publisher_id = src.publisher_id
               OR (target.publisher_id IS NULL AND src.publisher_id IS NULL))
        WHEN MATCHED THEN UPDATE SET
          total_pings    = @total_pings,
          total_bids     = @total_bids,
          total_wins     = @total_wins,
          total_no_bids  = @total_no_bids,
          total_sms_sent = @total_sms,
          avg_bid_amount = @avg_bid,
          max_bid_amount = @max_bid,
          total_revenue  = @total_rev,
          avg_response_ms = @avg_ms,
          computed_at    = SYSDATETIMEOFFSET()
        WHEN NOT MATCHED THEN INSERT
          (stat_date, vertical, campaign, publisher_id,
           total_pings, total_bids, total_wins, total_no_bids,
           total_sms_sent, avg_bid_amount, max_bid_amount,
           total_revenue, avg_response_ms)
        VALUES
          (@stat_date, @vertical, @campaign, @publisher_id,
           @total_pings, @total_bids, @total_wins, @total_no_bids,
           @total_sms, @avg_bid, @max_bid,
           @total_rev, @avg_ms);
      `);
    merged++;
  }

  return { merged };
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. lt_publisher_quality
// Lifetime stats per publisher_id × vertical with composite quality score
// ─────────────────────────────────────────────────────────────────────────────

async function aggregatePublisherQuality(pool, log) {
  const srcResult = await pool.request().query(`
    SELECT
      p.publisher_id,
      COALESCE(cat.vertical, 'unknown')           AS vertical,
      COUNT(p.id)                                 AS total_pings,
      SUM(CASE WHEN r.bid_amount IS NOT NULL THEN 1 ELSE 0 END) AS total_bids,
      SUM(CASE WHEN r.won = 1 THEN 1 ELSE 0 END)               AS total_wins,
      SUM(CASE WHEN p.is_duplicate = 1 THEN 1 ELSE 0 END)      AS total_duplicates,
      AVG(CAST(r.bid_amount AS FLOAT))                          AS avg_bid_amount,
      SUM(CAST(r.bid_amount AS FLOAT))                          AS total_revenue,
      AVG(CAST(r.response_time_ms AS FLOAT))                    AS avg_response_ms,
      MIN(p.created_at)                                         AS first_ping_at,
      MAX(p.created_at)                                         AS last_ping_at
    FROM inbound_pings p
    LEFT JOIN ringba_responses r   ON r.ping_id = p.id
    LEFT JOIN category_mappings cat ON cat.campaign = p.campaign AND cat.enabled = 1
    WHERE p.publisher_id IS NOT NULL
    GROUP BY p.publisher_id, COALESCE(cat.vertical, 'unknown')
  `);

  const rows = srcResult.recordset;
  if (!rows.length) return { merged: 0 };

  let merged = 0;
  for (const row of rows) {
    const pings = row.total_pings || 1;
    const bidRate = (row.total_bids  || 0) / pings;
    const winRate = (row.total_wins  || 0) / pings;
    const dupRate = (row.total_duplicates || 0) / pings;

    // Composite score: bid_rate*40 + win_rate*40 + (1-dup_rate)*20
    const score   = Math.min(100, bidRate * 40 + winRate * 40 + (1 - dupRate) * 20);
    const tier    = score >= 80 ? 'platinum' : score >= 60 ? 'gold' : score >= 40 ? 'silver' : 'bronze';

    await pool.request()
      .input('publisher_id',   sql.NVarChar(255),     row.publisher_id)
      .input('vertical',       sql.NVarChar(50),      row.vertical)
      .input('total_pings',    sql.Int,               row.total_pings)
      .input('bid_rate',       sql.Decimal(5, 4),     +bidRate.toFixed(4))
      .input('win_rate',       sql.Decimal(5, 4),     +winRate.toFixed(4))
      .input('dup_rate',       sql.Decimal(5, 4),     +dupRate.toFixed(4))
      .input('avg_bid',        sql.Decimal(10, 4),    row.avg_bid_amount ?? null)
      .input('avg_ms',         sql.Int,               row.avg_response_ms != null ? Math.round(row.avg_response_ms) : null)
      .input('quality_score',  sql.Decimal(5, 2),     +score.toFixed(2))
      .input('quality_tier',   sql.NVarChar(20),      tier)
      .input('first_ping_at',  sql.DateTimeOffset,    row.first_ping_at)
      .input('last_ping_at',   sql.DateTimeOffset,    row.last_ping_at)
      .query(`
        MERGE lt_publisher_quality AS target
        USING (
          SELECT @publisher_id AS publisher_id, @vertical AS vertical
        ) AS src
          ON target.publisher_id = src.publisher_id
         AND target.vertical     = src.vertical
        WHEN MATCHED THEN UPDATE SET
          total_pings    = @total_pings,
          bid_rate       = @bid_rate,
          win_rate       = @win_rate,
          duplicate_rate = @dup_rate,
          avg_bid_amount = @avg_bid,
          avg_response_ms = @avg_ms,
          quality_score  = @quality_score,
          quality_tier   = @quality_tier,
          first_ping_at  = @first_ping_at,
          last_ping_at   = @last_ping_at,
          updated_at     = SYSDATETIMEOFFSET()
        WHEN NOT MATCHED THEN INSERT
          (publisher_id, vertical, total_pings, bid_rate, win_rate,
           duplicate_rate, avg_bid_amount, avg_response_ms,
           quality_score, quality_tier, first_ping_at, last_ping_at)
        VALUES
          (@publisher_id, @vertical, @total_pings, @bid_rate, @win_rate,
           @dup_rate, @avg_bid, @avg_ms,
           @quality_score, @quality_tier, @first_ping_at, @last_ping_at);
      `);
    merged++;
  }

  return { merged };
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. lt_phone_profile
// One row per phone with latest state aggregated across all pings
// ─────────────────────────────────────────────────────────────────────────────

async function aggregatePhoneProfile(pool, log) {
  // Get phone-level aggregate stats from pings + responses
  const srcResult = await pool.request().query(`
    SELECT
      p.phone,
      -- Use most recent zip seen for this phone
      (SELECT TOP 1 zip FROM inbound_pings WHERE phone = p.phone
       AND zip IS NOT NULL ORDER BY created_at DESC)     AS zip,
      (SELECT TOP 1 publisher_id FROM inbound_pings WHERE phone = p.phone
       ORDER BY created_at DESC)                         AS publisher_id,
      (SELECT TOP 1 campaign FROM inbound_pings WHERE phone = p.phone
       ORDER BY created_at DESC)                         AS campaign,
      COUNT(p.id)                                        AS total_pings,
      SUM(CASE WHEN r.bid_amount IS NOT NULL THEN 1 ELSE 0 END) AS total_bids,
      SUM(CASE WHEN r.won = 1 THEN 1 ELSE 0 END)               AS total_wins,
      SUM(CAST(r.bid_amount AS FLOAT))                          AS total_revenue,
      AVG(CAST(r.bid_amount AS FLOAT))                          AS avg_bid_amount,
      MAX(r.bid_amount)                                         AS max_bid_amount,
      MIN(p.created_at)                                         AS first_seen_at,
      MAX(p.created_at)                                         AS last_seen_at,
      MAX(CASE WHEN r.bid_amount IS NOT NULL THEN r.bid_amount ELSE NULL END) AS last_bid_amount,
      MAX(CASE WHEN r.bid_amount IS NOT NULL THEN r.created_at ELSE NULL END) AS last_bid_at
    FROM inbound_pings p
    LEFT JOIN ringba_responses r ON r.ping_id = p.id
    GROUP BY p.phone
  `);

  // Get contact history per phone
  const chResult = await pool.request().query(`
    SELECT
      phone,
      COUNT(id)          AS total_contacts,
      MAX(contacted_at)  AS last_contacted_at
    FROM contact_history
    GROUP BY phone
  `);
  const chMap = {};
  for (const row of chResult.recordset) {
    chMap[row.phone] = { total_contacts: row.total_contacts, last_contacted_at: row.last_contacted_at };
  }

  // Get last sequence action per phone
  const stResult = await pool.request().query(`
    SELECT phone, MAX(action) AS last_seq_action
    FROM sequence_triggers
    GROUP BY phone
  `);
  const stMap = {};
  for (const row of stResult.recordset) { stMap[row.phone] = row.last_seq_action; }

  const rows = srcResult.recordset;
  if (!rows.length) return { merged: 0 };

  const now = Date.now();
  let merged = 0;

  for (const row of rows) {
    const ch = chMap[row.phone] || {};
    const lastContactedAt = ch.last_contacted_at ? new Date(ch.last_contacted_at) : null;

    // sms_state: active (contacted last 30d), dormant (older), never
    let smsState = 'never';
    if (lastContactedAt) {
      const daysSince = (now - lastContactedAt.getTime()) / 86_400_000;
      smsState = daysSince <= 30 ? 'active' : 'dormant';
    }

    await pool.request()
      .input('phone',            sql.NVarChar(20),     row.phone)
      .input('zip',              sql.NVarChar(10),     row.zip ?? null)
      .input('publisher_id',     sql.NVarChar(255),    row.publisher_id ?? null)
      .input('campaign',         sql.NVarChar(255),    row.campaign ?? null)
      .input('total_pings',      sql.Int,              row.total_pings)
      .input('total_bids',       sql.Int,              row.total_bids)
      .input('total_wins',       sql.Int,              row.total_wins)
      .input('total_revenue',    sql.Decimal(12, 4),   row.total_revenue ?? null)
      .input('avg_bid_amount',   sql.Decimal(10, 4),   row.avg_bid_amount ?? null)
      .input('max_bid_amount',   sql.Decimal(10, 4),   row.max_bid_amount ?? null)
      .input('last_bid_amount',  sql.Decimal(10, 4),   row.last_bid_amount ?? null)
      .input('last_bid_at',      sql.DateTimeOffset,   row.last_bid_at ?? null)
      .input('first_seen_at',    sql.DateTimeOffset,   row.first_seen_at)
      .input('last_seen_at',     sql.DateTimeOffset,   row.last_seen_at)
      .input('total_contacts',   sql.Int,              ch.total_contacts ?? 0)
      .input('last_contacted_at',sql.DateTimeOffset,   lastContactedAt)
      .input('sms_state',        sql.NVarChar(20),     smsState)
      .input('last_seq_action',  sql.NVarChar(50),     stMap[row.phone] ?? null)
      .query(`
        MERGE lt_phone_profile AS target
        USING (SELECT @phone AS phone) AS src
          ON target.phone = src.phone
        WHEN MATCHED THEN UPDATE SET
          zip                  = COALESCE(@zip, target.zip),
          total_pings          = @total_pings,
          total_bids           = @total_bids,
          total_wins           = @total_wins,
          avg_bid_amount       = @avg_bid_amount,
          max_bid_amount       = @max_bid_amount,
          last_bid_amount      = @last_bid_amount,
          last_bid_at          = @last_bid_at,
          first_seen_at        = @first_seen_at,
          last_seen_at         = @last_seen_at,
          total_sms_sent       = @total_contacts,
          last_contacted_at    = @last_contacted_at,
          updated_at           = SYSDATETIMEOFFSET()
        WHEN NOT MATCHED THEN INSERT
          (phone, zip, total_pings, total_bids, total_wins,
           avg_bid_amount, max_bid_amount, last_bid_amount, last_bid_at,
           first_seen_at, last_seen_at, total_sms_sent, last_contacted_at)
        VALUES
          (@phone, @zip, @total_pings, @total_bids, @total_wins,
           @avg_bid_amount, @max_bid_amount, @last_bid_amount, @last_bid_at,
           @first_seen_at, @last_seen_at, @total_contacts, @last_contacted_at);
      `);
    merged++;
  }

  return { merged };
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. lt_geo_intelligence
// Per-zip aggregate from inbound_pings + ringba_responses
// ─────────────────────────────────────────────────────────────────────────────

async function aggregateGeoIntelligence(pool, log) {
  const srcResult = await pool.request().query(`
    SELECT
      p.zip,
      COUNT(p.id)                                                   AS total_pings,
      SUM(CASE WHEN r.bid_amount IS NOT NULL THEN 1 ELSE 0 END)     AS total_bids,
      SUM(CASE WHEN r.won = 1 THEN 1 ELSE 0 END)                    AS total_wins,
      AVG(CAST(r.bid_amount AS FLOAT))                              AS avg_bid_amount
    FROM inbound_pings p
    LEFT JOIN ringba_responses r ON r.ping_id = p.id
    WHERE p.zip IS NOT NULL AND LEN(TRIM(p.zip)) > 0
    GROUP BY p.zip
  `);

  const rows = srcResult.recordset;
  if (!rows.length) return { merged: 0 };

  let merged = 0;
  for (const row of rows) {
    const pings   = row.total_pings || 1;
    const bidRate = (row.total_bids || 0) / pings;

    await pool.request()
      .input('zip',          sql.NVarChar(10),    row.zip)
      .input('total_pings',  sql.Int,             row.total_pings)
      .input('total_bids',   sql.Int,             row.total_bids)
      .input('bid_rate',     sql.Decimal(5, 4),   +bidRate.toFixed(4))
      .input('avg_bid',      sql.Decimal(10, 4),  row.avg_bid_amount ?? null)
      .query(`
        MERGE lt_geo_intelligence AS target
        USING (SELECT @zip AS zip) AS src
          ON target.zip = src.zip
        WHEN MATCHED THEN UPDATE SET
          total_pings    = @total_pings,
          total_bids     = @total_bids,
          bid_rate       = @bid_rate,
          avg_bid_amount = @avg_bid,
          updated_at     = SYSDATETIMEOFFSET()
        WHEN NOT MATCHED THEN INSERT
          (zip, total_pings, total_bids, bid_rate, avg_bid_amount)
        VALUES
          (@zip, @total_pings, @total_bids, @bid_rate, @avg_bid);
      `);
    merged++;
  }

  return { merged };
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. lt_outcome_events
// Delta insert from postback_log + sequence_triggers (no full merge)
// ─────────────────────────────────────────────────────────────────────────────

async function aggregateOutcomeEvents(pool, log) {
  // Check if postback_log exists before attempting to query it
  const pbCheck = await pool.request().query(`
    SELECT COUNT(1) AS cnt
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'postback_log'
  `);

  let postbackInserted = 0;
  if (pbCheck.recordset[0].cnt > 0) {
    // Insert postback_log rows not yet in lt_outcome_events
    // Match on source='postback' and raw_data containing the postback log id
    const pbResult = await pool.request().query(`
      INSERT INTO lt_outcome_events
        (phone, ping_id, vertical, campaign, publisher_id, channel,
         outcome_type, outcome_value, occurred_at, source, raw_data)
      SELECT
        pl.phone,
        pl.ping_id,
        pl.vertical,
        pl.campaign,
        pl.publisher_id,
        'rtb'                        AS channel,
        COALESCE(pl.action, 'postback') AS outcome_type,
        pl.revenue,
        pl.received_at,
        'postback'                   AS source,
        CAST(pl.id AS NVARCHAR(36))  AS raw_data
      FROM postback_log pl
      WHERE NOT EXISTS (
        SELECT 1 FROM lt_outcome_events oe
        WHERE oe.source = 'postback'
          AND oe.raw_data = CAST(pl.id AS NVARCHAR(36))
      )
    `);
    postbackInserted = pbResult.rowsAffected[0] || 0;
  }

  // Insert sequence_triggers with action='campaignkit_trigger' as sms_sent outcomes
  const stResult = await pool.request().query(`
    INSERT INTO lt_outcome_events
      (phone, ping_id, vertical, campaign, channel,
       outcome_type, occurred_at, source, raw_data)
    SELECT
      st.phone,
      st.ping_id,
      st.vertical,
      st.campaign,
      'sms'              AS channel,
      'sms_sent'         AS outcome_type,
      st.triggered_at,
      'sequence_trigger' AS source,
      CAST(st.id AS NVARCHAR(36)) AS raw_data
    FROM sequence_triggers st
    WHERE st.action = 'campaignkit_trigger'
      AND NOT EXISTS (
        SELECT 1 FROM lt_outcome_events oe
        WHERE oe.source = 'sequence_trigger'
          AND oe.raw_data = CAST(st.id AS NVARCHAR(36))
      )
  `);
  const stInserted = stResult.rowsAffected[0] || 0;

  return { postback_inserted: postbackInserted, sms_sent_inserted: stInserted };
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. lt_lead_scores
// Simple win-rate + recency scoring per phone × vertical
// score = (wins / max(pings,1)) * 60 + recency_bonus
// tier: hot >=70, warm >=40, cold >=20, dormant <20
// ─────────────────────────────────────────────────────────────────────────────

async function aggregateLeadScores(pool, log) {
  const srcResult = await pool.request().query(`
    SELECT
      p.phone,
      COALESCE(cat.vertical, 'unknown')    AS vertical,
      COUNT(p.id)                          AS total_pings,
      SUM(CASE WHEN r.won = 1 THEN 1 ELSE 0 END) AS total_wins,
      MAX(p.created_at)                    AS last_ping_at,
      AVG(CAST(r.bid_amount AS FLOAT))     AS avg_bid_amount
    FROM inbound_pings p
    LEFT JOIN ringba_responses r    ON r.ping_id  = p.id
    LEFT JOIN category_mappings cat ON cat.campaign = p.campaign AND cat.enabled = 1
    GROUP BY p.phone, COALESCE(cat.vertical, 'unknown')
  `);

  const rows = srcResult.recordset;
  if (!rows.length) return { merged: 0 };

  const now = Date.now();
  let merged = 0;

  for (const row of rows) {
    const pings   = row.total_pings || 1;
    const wins    = row.total_wins  || 0;
    const lastPingMs = row.last_ping_at ? new Date(row.last_ping_at).getTime() : 0;
    const daysSince  = lastPingMs ? (now - lastPingMs) / 86_400_000 : 9999;

    const recencyBonus = daysSince <= 7 ? 20 : daysSince <= 30 ? 10 : 0;
    const score = Math.min(100, (wins / pings) * 60 + recencyBonus);
    const tier  = score >= 70 ? 'hot' : score >= 40 ? 'warm' : score >= 20 ? 'cold' : 'dormant';

    const factors = JSON.stringify({
      win_rate:      +(wins / pings).toFixed(4),
      recency_bonus: recencyBonus,
      days_since_ping: +daysSince.toFixed(1),
      avg_bid:       row.avg_bid_amount ?? null,
    });

    await pool.request()
      .input('phone',        sql.NVarChar(20),    row.phone)
      .input('vertical',     sql.NVarChar(50),    row.vertical)
      .input('score',        sql.Decimal(5, 2),   +score.toFixed(2))
      .input('score_tier',   sql.NVarChar(20),    tier)
      .input('score_factors',sql.NVarChar(sql.MAX), factors)
      .input('model_version',sql.NVarChar(50),    'v1-simple')
      .query(`
        MERGE lt_lead_scores AS target
        USING (SELECT @phone AS phone, @vertical AS vertical) AS src
          ON target.phone    = src.phone
         AND target.vertical = src.vertical
        WHEN MATCHED THEN UPDATE SET
          score         = @score,
          score_tier    = @score_tier,
          score_factors = @score_factors,
          model_version = @model_version,
          scored_at     = SYSDATETIMEOFFSET()
        WHEN NOT MATCHED THEN INSERT
          (phone, vertical, score, score_tier, score_factors, model_version)
        VALUES
          (@phone, @vertical, @score, @score_tier, @score_factors, @model_version);
      `);
    merged++;
  }

  return { merged };
}
