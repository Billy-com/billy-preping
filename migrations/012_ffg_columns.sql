-- Migration 012: Add FFG Identity Enrichment columns to inbound_pings
-- These columns are populated by ping.js when ffg_enabled=1 in system_config.
-- All nullable — pings processed before FFG was live have NULL in all four.

IF NOT EXISTS (
  SELECT 1 FROM sys.columns
  WHERE object_id = OBJECT_ID('inbound_pings') AND name = 'ffg_spine_id'
)
BEGIN
  ALTER TABLE inbound_pings ADD
    ffg_spine_id    NVARCHAR(255)  NULL,
    ffg_line_type   NVARCHAR(20)   NULL,   -- 'mobile' | 'landline' | 'voip' | NULL
    ffg_pass        BIT            NULL,   -- 1=pass, 0=rejected (landline/VoIP/TCPA flag)
    ffg_demographic NVARCHAR(MAX)  NULL;   -- JSON: { age, income, homeowner, … }
END
GO
