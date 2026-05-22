-- ─────────────────────────────────────────────────────────────────────────────
-- 010_catchup.sql
-- Catch-up migration — safe to run multiple times (idempotent).
-- Adds missing columns to ringba_responses, creates missing tables, and
-- drops/recreates the vw_ping_master view.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 1. Add missing columns to ringba_responses ───────────────────────────────

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ringba_responses' AND COLUMN_NAME = 'ringba_status')
  ALTER TABLE ringba_responses ADD ringba_status NVARCHAR(20) NULL;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ringba_responses' AND COLUMN_NAME = 'ringba_status_code')
  ALTER TABLE ringba_responses ADD ringba_status_code INT NULL;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ringba_responses' AND COLUMN_NAME = 'outbound_payload')
  ALTER TABLE ringba_responses ADD outbound_payload NVARCHAR(MAX) NULL;

-- ── 2. phone_categories (from 003) ───────────────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'phone_categories')
BEGIN
  CREATE TABLE phone_categories (
    id            UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    phone         NVARCHAR(20)       NOT NULL,
    category_key  NVARCHAR(255)      NOT NULL,
    source        NVARCHAR(50)       NULL,
    valid_until   DATETIMEOFFSET     NULL,
    created_at    DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    CONSTRAINT PK_phone_categories PRIMARY KEY (id),
    CONSTRAINT UQ_phone_categories UNIQUE (phone, category_key)
  );
  CREATE INDEX IX_phone_categories_phone ON phone_categories (phone);
  CREATE INDEX IX_phone_categories_key   ON phone_categories (category_key);
END;

-- ── 3. sequence_triggers (from 003) ──────────────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'sequence_triggers')
BEGIN
  CREATE TABLE sequence_triggers (
    id                      UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    mts_id                  UNIQUEIDENTIFIER   NULL,
    ping_id                 UNIQUEIDENTIFIER   NULL,
    phone                   NVARCHAR(20)       NOT NULL,
    vertical                NVARCHAR(50)       NOT NULL,
    campaign                NVARCHAR(255)      NOT NULL,
    flow                    NVARCHAR(10)       NOT NULL,
    action                  NVARCHAR(50)       NOT NULL,
    was_enrichment_required BIT                NULL,
    was_contacted_30d       BIT                NULL,
    was_in_category         BIT                NULL,
    category_matched        NVARCHAR(255)      NULL,
    external_status_code    INT                NULL,
    external_response       NVARCHAR(MAX)      NULL,
    external_latency_ms     INT                NULL,
    triggered_at            DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    CONSTRAINT PK_sequence_triggers PRIMARY KEY (id)
  );
  CREATE INDEX IX_seq_triggers_mts_id    ON sequence_triggers (mts_id);
  CREATE INDEX IX_seq_triggers_ping_id   ON sequence_triggers (ping_id);
  CREATE INDEX IX_seq_triggers_phone     ON sequence_triggers (phone);
  CREATE INDEX IX_seq_triggers_flow      ON sequence_triggers (flow, action);
  CREATE INDEX IX_seq_triggers_triggered ON sequence_triggers (triggered_at DESC);
END;

-- ── 4. Drop and recreate vw_ping_master (from 004) ───────────────────────────
-- Azure SQL requires CREATE VIEW to be the first statement in a batch (GO separator).

IF OBJECT_ID('vw_ping_master', 'V') IS NOT NULL
  DROP VIEW vw_ping_master;
GO

CREATE VIEW vw_ping_master AS

-- ── Combine all vertical MTS partitions ───────────────────────────────────────
WITH mts_all AS (
  SELECT id AS mts_id, ping_id, phone, zip, publisher_id, subid, campaign,
         vertical, rtb_status, bid_amount, buyer_id, routing_number, won,
         seq_state, requires_enrichment, enriched_at, created_at, expires_at
  FROM mts_auto
  UNION ALL
  SELECT id, ping_id, phone, zip, publisher_id, subid, campaign,
         vertical, rtb_status, bid_amount, buyer_id, routing_number, won,
         seq_state, requires_enrichment, enriched_at, created_at, expires_at
  FROM mts_health
  UNION ALL
  SELECT id, ping_id, phone, zip, publisher_id, subid, campaign,
         vertical, rtb_status, bid_amount, buyer_id, routing_number, won,
         seq_state, requires_enrichment, enriched_at, created_at, expires_at
  FROM mts_medicare
  UNION ALL
  SELECT id, ping_id, phone, zip, publisher_id, subid, campaign,
         vertical, rtb_status, bid_amount, buyer_id, routing_number, won,
         seq_state, requires_enrichment, enriched_at, created_at, expires_at
  FROM mts_home
)

SELECT
  -- ── Inbound ping (layer 1) ─────────────────────────────────────────────────
  p.id                    AS ping_id,
  p.phone,
  p.zip,
  p.zip_source,
  p.publisher_id,
  p.subid,
  p.campaign,
  p.ip,
  p.is_duplicate,
  p.raw_payload,
  p.created_at            AS ping_received_at,

  -- ── Ringba RTB response (layer 2) ─────────────────────────────────────────
  rr.id                   AS ringba_response_id,
  rr.ringba_status,
  rr.ringba_status_code,
  rr.bid_amount           AS ringba_bid_amount,
  rr.buyer_id             AS ringba_buyer_id,
  rr.routing_number       AS ringba_routing_number,
  rr.won                  AS ringba_won,
  rr.response_time_ms     AS ringba_response_ms,
  rr.outbound_payload,
  rr.raw_response         AS ringba_raw_response,
  rr.created_at           AS ringba_responded_at,

  -- ── Mid-term storage (layer 3) ─────────────────────────────────────────────
  mts.mts_id,
  mts.vertical,
  mts.seq_state,
  mts.requires_enrichment,
  mts.enriched_at,
  mts.bid_amount          AS mts_bid_amount,
  mts.created_at          AS mts_stored_at,
  mts.expires_at          AS mts_expires_at,

  -- ── Sequencer / SMS flow (layer 4 — most recent trigger per ping per flow) ─
  seq_rtb.action          AS seq_rtb_action,
  seq_rtb.was_enrichment_required,
  seq_rtb.triggered_at    AS seq_rtb_triggered_at,

  seq_sms.action          AS seq_sms_action,
  seq_sms.was_contacted_30d,
  seq_sms.was_in_category,
  seq_sms.category_matched,
  seq_sms.external_status_code AS sms_ext_status_code,
  seq_sms.external_latency_ms  AS sms_ext_latency_ms,
  seq_sms.triggered_at         AS seq_sms_triggered_at

FROM inbound_pings p

-- Ringba response — 1:1 with ping
LEFT JOIN ringba_responses rr
  ON rr.ping_id = p.id

-- MTS row — 1:1 (one vertical partition row per ping)
LEFT JOIN mts_all mts
  ON mts.ping_id = p.id

-- Most recent RTB sequencer trigger for this ping
LEFT JOIN sequence_triggers seq_rtb
  ON seq_rtb.ping_id = p.id
 AND seq_rtb.flow    = 'rtb'
 AND seq_rtb.id      = (
       SELECT TOP 1 id FROM sequence_triggers
       WHERE ping_id = p.id AND flow = 'rtb'
       ORDER BY triggered_at DESC
     )

-- Most recent SMS sequencer trigger for this ping
LEFT JOIN sequence_triggers seq_sms
  ON seq_sms.ping_id = p.id
 AND seq_sms.flow    = 'sms'
 AND seq_sms.id      = (
       SELECT TOP 1 id FROM sequence_triggers
       WHERE ping_id = p.id AND flow = 'sms'
       ORDER BY triggered_at DESC
     );
GO

-- ── 5. Make sequence_triggers columns nullable (migration 009) ────────────────
-- Safe to run multiple times — columns may already be nullable.

BEGIN TRY
  ALTER TABLE sequence_triggers ALTER COLUMN mts_id UNIQUEIDENTIFIER NULL;
  ALTER TABLE sequence_triggers ALTER COLUMN ping_id UNIQUEIDENTIFIER NULL;
END TRY
BEGIN CATCH
  -- columns may already be nullable
END CATCH

-- ── 6. Tables from 006 (CampaignKit storage) — likely missing ────────────────

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'campaignkit_lists')
BEGIN
  CREATE TABLE campaignkit_lists (
    id                UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    campaignkit_id    NVARCHAR(255)      NOT NULL,
    name              NVARCHAR(255)      NOT NULL,
    description       NVARCHAR(1000)     NULL,
    vertical          NVARCHAR(50)       NULL,
    campaign          NVARCHAR(255)      NULL,
    category_key      NVARCHAR(255)      NULL,
    status            NVARCHAR(50)       NOT NULL DEFAULT 'active',
    member_count      INT                NULL,
    last_synced_at    DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    created_at        DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    updated_at        DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    raw_data          NVARCHAR(MAX)      NULL,
    CONSTRAINT PK_campaignkit_lists PRIMARY KEY (id),
    CONSTRAINT UQ_campaignkit_lists UNIQUE (campaignkit_id)
  );
  CREATE INDEX IX_ck_lists_campaign     ON campaignkit_lists (campaign);
  CREATE INDEX IX_ck_lists_vertical     ON campaignkit_lists (vertical);
  CREATE INDEX IX_ck_lists_category_key ON campaignkit_lists (category_key);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'campaignkit_contacts')
BEGIN
  CREATE TABLE campaignkit_contacts (
    id                     UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    phone                  NVARCHAR(20)       NOT NULL,
    campaignkit_list_id    NVARCHAR(255)      NOT NULL,
    campaignkit_contact_id NVARCHAR(255)      NULL,
    ping_id                UNIQUEIDENTIFIER   NULL,
    mts_id                 UNIQUEIDENTIFIER   NULL,
    campaign               NVARCHAR(255)      NULL,
    vertical               NVARCHAR(50)       NULL,
    category_key           NVARCHAR(255)      NULL,
    enroll_status          NVARCHAR(50)       NOT NULL DEFAULT 'pending',
    enrolled_at            DATETIMEOFFSET     NULL,
    last_message_at        DATETIMEOFFSET     NULL,
    last_response_at       DATETIMEOFFSET     NULL,
    message_count          INT                NOT NULL DEFAULT 0,
    response_count         INT                NOT NULL DEFAULT 0,
    opted_out_at           DATETIMEOFFSET     NULL,
    opt_out_reason         NVARCHAR(255)      NULL,
    created_at             DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    updated_at             DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    raw_data               NVARCHAR(MAX)      NULL,
    CONSTRAINT PK_campaignkit_contacts PRIMARY KEY (id),
    CONSTRAINT UQ_campaignkit_contacts UNIQUE (phone, campaignkit_list_id)
  );
  CREATE INDEX IX_ck_contacts_phone      ON campaignkit_contacts (phone);
  CREATE INDEX IX_ck_contacts_list       ON campaignkit_contacts (campaignkit_list_id);
  CREATE INDEX IX_ck_contacts_ping_id    ON campaignkit_contacts (ping_id);
  CREATE INDEX IX_ck_contacts_mts_id     ON campaignkit_contacts (mts_id);
  CREATE INDEX IX_ck_contacts_status     ON campaignkit_contacts (enroll_status);
  CREATE INDEX IX_ck_contacts_enrolled   ON campaignkit_contacts (enrolled_at DESC);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'campaignkit_messages')
BEGIN
  CREATE TABLE campaignkit_messages (
    id                     UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    campaignkit_contact_id NVARCHAR(255)      NULL,
    campaignkit_message_id NVARCHAR(255)      NULL,
    phone                  NVARCHAR(20)       NOT NULL,
    campaignkit_list_id    NVARCHAR(255)      NOT NULL,
    channel                NVARCHAR(20)       NOT NULL DEFAULT 'sms',
    direction              NVARCHAR(10)       NOT NULL DEFAULT 'outbound',
    status                 NVARCHAR(50)       NULL,
    message_body           NVARCHAR(MAX)      NULL,
    response_body          NVARCHAR(MAX)      NULL,
    sent_at                DATETIMEOFFSET     NULL,
    delivered_at           DATETIMEOFFSET     NULL,
    responded_at           DATETIMEOFFSET     NULL,
    latency_ms             INT                NULL,
    created_at             DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    raw_data               NVARCHAR(MAX)      NULL,
    CONSTRAINT PK_campaignkit_messages PRIMARY KEY (id)
  );
  CREATE INDEX IX_ck_messages_phone      ON campaignkit_messages (phone);
  CREATE INDEX IX_ck_messages_list       ON campaignkit_messages (campaignkit_list_id);
  CREATE INDEX IX_ck_messages_status     ON campaignkit_messages (status);
  CREATE INDEX IX_ck_messages_sent       ON campaignkit_messages (sent_at DESC);
  CREATE INDEX IX_ck_messages_ck_contact ON campaignkit_messages (campaignkit_contact_id);
END;

-- ── 7. Tables from 007 (Long-Term Storage) — likely missing ──────────────────

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lt_outcome_events')
BEGIN
  CREATE TABLE lt_outcome_events (
    id              UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    phone           NVARCHAR(20)       NOT NULL,
    ping_id         UNIQUEIDENTIFIER   NULL,
    mts_id          UNIQUEIDENTIFIER   NULL,
    vertical        NVARCHAR(50)       NULL,
    campaign        NVARCHAR(255)      NULL,
    publisher_id    NVARCHAR(255)      NULL,
    buyer_id        NVARCHAR(255)      NULL,
    channel         NVARCHAR(20)       NULL,
    outcome_type    NVARCHAR(50)       NOT NULL,
    outcome_value   DECIMAL(10,4)      NULL,
    duration_sec    INT                NULL,
    occurred_at     DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    source          NVARCHAR(50)       NULL,
    raw_data        NVARCHAR(MAX)      NULL,
    CONSTRAINT PK_lt_outcome_events PRIMARY KEY (id)
  );
  CREATE INDEX IX_lt_outcomes_phone      ON lt_outcome_events (phone);
  CREATE INDEX IX_lt_outcomes_ping_id    ON lt_outcome_events (ping_id);
  CREATE INDEX IX_lt_outcomes_type       ON lt_outcome_events (outcome_type);
  CREATE INDEX IX_lt_outcomes_vertical   ON lt_outcome_events (vertical, campaign);
  CREATE INDEX IX_lt_outcomes_occurred   ON lt_outcome_events (occurred_at DESC);
  CREATE INDEX IX_lt_outcomes_buyer      ON lt_outcome_events (buyer_id);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lt_daily_stats')
BEGIN
  CREATE TABLE lt_daily_stats (
    id                  UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    stat_date           DATE               NOT NULL,
    vertical            NVARCHAR(50)       NOT NULL,
    campaign            NVARCHAR(255)      NOT NULL,
    publisher_id        NVARCHAR(255)      NULL,
    total_pings         INT                NOT NULL DEFAULT 0,
    total_bids          INT                NOT NULL DEFAULT 0,
    total_wins          INT                NOT NULL DEFAULT 0,
    total_no_bids       INT                NOT NULL DEFAULT 0,
    total_errors        INT                NOT NULL DEFAULT 0,
    total_sms_sent      INT                NOT NULL DEFAULT 0,
    total_sms_responses INT                NOT NULL DEFAULT 0,
    total_conversions   INT                NOT NULL DEFAULT 0,
    avg_bid_amount      DECIMAL(10,4)      NULL,
    max_bid_amount      DECIMAL(10,4)      NULL,
    total_revenue       DECIMAL(12,4)      NULL,
    avg_response_ms     INT                NULL,
    p95_response_ms     INT                NULL,
    enrichment_rate     DECIMAL(5,4)       NULL,
    suppression_rate    DECIMAL(5,4)       NULL,
    category_match_rate DECIMAL(5,4)       NULL,
    computed_at         DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    CONSTRAINT PK_lt_daily_stats PRIMARY KEY (id),
    CONSTRAINT UQ_lt_daily_stats UNIQUE (stat_date, vertical, campaign, publisher_id)
  );
  CREATE INDEX IX_lt_daily_date      ON lt_daily_stats (stat_date DESC);
  CREATE INDEX IX_lt_daily_vertical  ON lt_daily_stats (vertical, campaign);
  CREATE INDEX IX_lt_daily_publisher ON lt_daily_stats (publisher_id);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lt_publisher_quality')
BEGIN
  CREATE TABLE lt_publisher_quality (
    id                  UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    publisher_id        NVARCHAR(255)      NOT NULL,
    vertical            NVARCHAR(50)       NOT NULL,
    total_pings         INT                NOT NULL DEFAULT 0,
    bid_rate            DECIMAL(5,4)       NULL,
    win_rate            DECIMAL(5,4)       NULL,
    avg_bid_amount      DECIMAL(10,4)      NULL,
    duplicate_rate      DECIMAL(5,4)       NULL,
    invalid_phone_rate  DECIMAL(5,4)       NULL,
    avg_response_ms     INT                NULL,
    quality_score       DECIMAL(5,2)       NULL,
    quality_tier        NVARCHAR(20)       NULL,
    first_ping_at       DATETIMEOFFSET     NULL,
    last_ping_at        DATETIMEOFFSET     NULL,
    updated_at          DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    CONSTRAINT PK_lt_publisher_quality PRIMARY KEY (id),
    CONSTRAINT UQ_lt_publisher_quality UNIQUE (publisher_id, vertical)
  );
  CREATE INDEX IX_lt_pub_quality_score ON lt_publisher_quality (quality_score DESC);
  CREATE INDEX IX_lt_pub_quality_tier  ON lt_publisher_quality (quality_tier);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lt_phone_profile')
BEGIN
  CREATE TABLE lt_phone_profile (
    id                    UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    phone                 NVARCHAR(20)       NOT NULL,
    zip                   NVARCHAR(10)       NULL,
    city                  NVARCHAR(100)      NULL,
    state                 NVARCHAR(50)       NULL,
    dma                   NVARCHAR(100)      NULL,
    county                NVARCHAR(100)      NULL,
    timezone              NVARCHAR(50)       NULL,
    first_seen_at         DATETIMEOFFSET     NULL,
    last_seen_at          DATETIMEOFFSET     NULL,
    total_pings           INT                NOT NULL DEFAULT 0,
    total_bids            INT                NOT NULL DEFAULT 0,
    total_wins            INT                NOT NULL DEFAULT 0,
    total_sms_sent        INT                NOT NULL DEFAULT 0,
    total_sms_responses   INT                NOT NULL DEFAULT 0,
    total_conversions     INT                NOT NULL DEFAULT 0,
    avg_bid_amount        DECIMAL(10,4)      NULL,
    max_bid_amount        DECIMAL(10,4)      NULL,
    last_bid_amount       DECIMAL(10,4)      NULL,
    last_bid_at           DATETIMEOFFSET     NULL,
    verticals_seen        NVARCHAR(500)      NULL,
    campaigns_seen        NVARCHAR(MAX)      NULL,
    sms_opt_out           BIT                NOT NULL DEFAULT 0,
    sms_opt_out_at        DATETIMEOFFSET     NULL,
    last_contacted_at     DATETIMEOFFSET     NULL,
    last_contact_channel  NVARCHAR(20)       NULL,
    ck_list_ids           NVARCHAR(MAX)      NULL,
    ck_enrolled_count     INT                NOT NULL DEFAULT 0,
    enriched              BIT                NOT NULL DEFAULT 0,
    enriched_at           DATETIMEOFFSET     NULL,
    enrichment_source     NVARCHAR(50)       NULL,
    updated_at            DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    CONSTRAINT PK_lt_phone_profile PRIMARY KEY (id),
    CONSTRAINT UQ_lt_phone_profile UNIQUE (phone)
  );
  CREATE INDEX IX_lt_phone_zip       ON lt_phone_profile (zip);
  CREATE INDEX IX_lt_phone_state     ON lt_phone_profile (state);
  CREATE INDEX IX_lt_phone_dma       ON lt_phone_profile (dma);
  CREATE INDEX IX_lt_phone_last_seen ON lt_phone_profile (last_seen_at DESC);
  CREATE INDEX IX_lt_phone_bids      ON lt_phone_profile (total_bids DESC);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lt_geo_intelligence')
BEGIN
  CREATE TABLE lt_geo_intelligence (
    id                  UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    zip                 NVARCHAR(10)       NOT NULL,
    state               NVARCHAR(50)       NULL,
    dma                 NVARCHAR(100)      NULL,
    county              NVARCHAR(100)      NULL,
    timezone            NVARCHAR(50)       NULL,
    total_pings         INT                NOT NULL DEFAULT 0,
    total_bids          INT                NOT NULL DEFAULT 0,
    bid_rate            DECIMAL(5,4)       NULL,
    avg_bid_amount      DECIMAL(10,4)      NULL,
    vertical_breakdown  NVARCHAR(MAX)      NULL,
    avg_sms_response_rate DECIMAL(5,4)     NULL,
    best_contact_hour   TINYINT            NULL,
    best_contact_day    TINYINT            NULL,
    updated_at          DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    CONSTRAINT PK_lt_geo_intelligence PRIMARY KEY (id),
    CONSTRAINT UQ_lt_geo_intelligence UNIQUE (zip)
  );
  CREATE INDEX IX_lt_geo_state ON lt_geo_intelligence (state);
  CREATE INDEX IX_lt_geo_dma   ON lt_geo_intelligence (dma);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lt_lead_scores')
BEGIN
  CREATE TABLE lt_lead_scores (
    id              UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    phone           NVARCHAR(20)       NOT NULL,
    vertical        NVARCHAR(50)       NOT NULL,
    score           DECIMAL(5,2)       NOT NULL,
    score_tier      NVARCHAR(20)       NULL,
    score_factors   NVARCHAR(MAX)      NULL,
    conversion_prob DECIMAL(5,4)       NULL,
    estimated_value DECIMAL(10,4)      NULL,
    model_version   NVARCHAR(50)       NULL,
    scored_at       DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    expires_at      AS DATEADD(day, 30, scored_at) PERSISTED,
    CONSTRAINT PK_lt_lead_scores PRIMARY KEY (id),
    CONSTRAINT UQ_lt_lead_scores UNIQUE (phone, vertical)
  );
  CREATE INDEX IX_lt_scores_score    ON lt_lead_scores (score DESC);
  CREATE INDEX IX_lt_scores_tier     ON lt_lead_scores (score_tier);
  CREATE INDEX IX_lt_scores_expires  ON lt_lead_scores (expires_at);
  CREATE INDEX IX_lt_scores_vertical ON lt_lead_scores (vertical, score DESC);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lt_contact_schedule')
BEGIN
  CREATE TABLE lt_contact_schedule (
    id                  UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    phone               NVARCHAR(20)       NOT NULL,
    vertical            NVARCHAR(50)       NULL,
    campaign            NVARCHAR(255)      NULL,
    recommended_channel NVARCHAR(20)       NULL,
    recommended_at      DATETIMEOFFSET     NULL,
    recommended_hour    TINYINT            NULL,
    recommended_day     TINYINT            NULL,
    confidence          DECIMAL(5,4)       NULL,
    reason              NVARCHAR(500)      NULL,
    status              NVARCHAR(20)       NOT NULL DEFAULT 'pending',
    created_at          DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    expires_at          DATETIMEOFFSET     NULL,
    CONSTRAINT PK_lt_contact_schedule PRIMARY KEY (id)
  );
  CREATE INDEX IX_lt_sched_phone     ON lt_contact_schedule (phone);
  CREATE INDEX IX_lt_sched_pending   ON lt_contact_schedule (status, recommended_at);
  CREATE INDEX IX_lt_sched_vertical  ON lt_contact_schedule (vertical, recommended_at);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lt_sequence_recommendations')
BEGIN
  CREATE TABLE lt_sequence_recommendations (
    id                  UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    phone               NVARCHAR(20)       NOT NULL,
    mts_id              UNIQUEIDENTIFIER   NULL,
    vertical            NVARCHAR(50)       NULL,
    campaign            NVARCHAR(255)      NULL,
    step_number         INT                NOT NULL DEFAULT 1,
    channel             NVARCHAR(20)       NULL,
    action              NVARCHAR(100)      NULL,
    delay_hours         INT                NULL,
    message_template    NVARCHAR(500)      NULL,
    reason              NVARCHAR(500)      NULL,
    status              NVARCHAR(20)       NOT NULL DEFAULT 'pending',
    created_at          DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    executed_at         DATETIMEOFFSET     NULL,
    CONSTRAINT PK_lt_sequence_recs PRIMARY KEY (id)
  );
  CREATE INDEX IX_lt_seqrec_phone    ON lt_sequence_recommendations (phone);
  CREATE INDEX IX_lt_seqrec_mts_id   ON lt_sequence_recommendations (mts_id);
  CREATE INDEX IX_lt_seqrec_status   ON lt_sequence_recommendations (status, created_at);
END;

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lt_messaging_performance')
BEGIN
  CREATE TABLE lt_messaging_performance (
    id                  UNIQUEIDENTIFIER   NOT NULL DEFAULT NEWID(),
    template_id         NVARCHAR(255)      NULL,
    channel             NVARCHAR(20)       NOT NULL,
    vertical            NVARCHAR(50)       NULL,
    campaign            NVARCHAR(255)      NULL,
    total_sent          INT                NOT NULL DEFAULT 0,
    total_delivered     INT                NOT NULL DEFAULT 0,
    total_failed        INT                NOT NULL DEFAULT 0,
    total_responses     INT                NOT NULL DEFAULT 0,
    total_opt_outs      INT                NOT NULL DEFAULT 0,
    total_conversions   INT                NOT NULL DEFAULT 0,
    delivery_rate       DECIMAL(5,4)       NULL,
    response_rate       DECIMAL(5,4)       NULL,
    conversion_rate     DECIMAL(5,4)       NULL,
    opt_out_rate        DECIMAL(5,4)       NULL,
    avg_response_hours  DECIMAL(8,2)       NULL,
    updated_at          DATETIMEOFFSET     NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    CONSTRAINT PK_lt_msg_perf PRIMARY KEY (id),
    CONSTRAINT UQ_lt_msg_perf UNIQUE (template_id, channel, vertical, campaign)
  );
  CREATE INDEX IX_lt_msg_perf_channel  ON lt_messaging_performance (channel, vertical);
  CREATE INDEX IX_lt_msg_perf_conv     ON lt_messaging_performance (conversion_rate DESC);
END;

-- ── 8. lt_messaging_performance already covered by section 7 above ───────────
-- (lt_messaging_performance was missing from some environments; section 7 handles it)

-- ── 9. LTS performance indexes (idempotent — IF NOT EXISTS) ──────────────────

-- lt_daily_stats
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_lt_daily_date_vertical' AND object_id = OBJECT_ID('lt_daily_stats'))
  CREATE INDEX IX_lt_daily_date_vertical ON lt_daily_stats (stat_date DESC, vertical);

-- lt_publisher_quality
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_lt_pub_score_desc' AND object_id = OBJECT_ID('lt_publisher_quality'))
  CREATE INDEX IX_lt_pub_score_desc ON lt_publisher_quality (quality_score DESC);

-- lt_phone_profile
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_lt_phone_profile_phone' AND object_id = OBJECT_ID('lt_phone_profile'))
  CREATE INDEX IX_lt_phone_profile_phone ON lt_phone_profile (phone);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_lt_phone_profile_last_ping' AND object_id = OBJECT_ID('lt_phone_profile'))
  CREATE INDEX IX_lt_phone_profile_last_ping ON lt_phone_profile (last_seen_at DESC);

-- lt_geo_intelligence
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_lt_geo_zip' AND object_id = OBJECT_ID('lt_geo_intelligence'))
  CREATE INDEX IX_lt_geo_zip ON lt_geo_intelligence (zip);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_lt_geo_pings_desc' AND object_id = OBJECT_ID('lt_geo_intelligence'))
  CREATE INDEX IX_lt_geo_pings_desc ON lt_geo_intelligence (total_pings DESC);

-- lt_lead_scores
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_lt_scores_phone_vertical' AND object_id = OBJECT_ID('lt_lead_scores'))
  CREATE INDEX IX_lt_scores_phone_vertical ON lt_lead_scores (phone, vertical);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_lt_scores_score_desc' AND object_id = OBJECT_ID('lt_lead_scores'))
  CREATE INDEX IX_lt_scores_score_desc ON lt_lead_scores (score DESC);

-- lt_outcome_events
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_lt_outcomes_type_created' AND object_id = OBJECT_ID('lt_outcome_events'))
  CREATE INDEX IX_lt_outcomes_type_created ON lt_outcome_events (outcome_type, occurred_at DESC);

-- ── 10. system_config seeds from 006 and 007 ─────────────────────────────────

INSERT INTO system_config ([key], value)
SELECT k, v FROM (VALUES
  ('campaignkit_api_base_url',       'https://api.campaignkit.com'),
  ('campaignkit_webhook_secret',     ''),
  ('lts_enabled',                    '1'),
  ('lts_score_refresh_days',         '7'),
  ('lts_stats_rollup_hour',          '2'),
  ('lts_phone_profile_enabled',      '1'),
  ('lts_geo_intelligence_enabled',   '1')
) AS s(k, v)
WHERE NOT EXISTS (SELECT 1 FROM system_config WHERE [key] = s.k);
