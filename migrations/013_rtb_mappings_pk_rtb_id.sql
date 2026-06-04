-- Migration 013: Change publisher_rtb_mappings PRIMARY KEY from publisher_id → rtb_id
--
-- The original schema had publisher_id as PK, meaning each publisher could only have
-- ONE row. A publisher with multiple campaigns (e.g. TOLM with Locksmith, Plumbing,
-- Electrician, Garage Door, Local Moving) had every sync overwrite the previous row,
-- leaving only the last-synced campaign active.
--
-- Fix: PK is now rtb_id (unique per publisher×campaign). publisher_id is indexed
-- for fast lookup by publisher. A publisher can now have N rows — one per campaign.

-- Step 1: Drop the existing PK constraint (named PK__publisher__XXX auto-generated)
DECLARE @pkName NVARCHAR(255);
SELECT @pkName = CONSTRAINT_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_NAME = 'publisher_rtb_mappings'
  AND CONSTRAINT_TYPE = 'PRIMARY KEY';

IF @pkName IS NOT NULL
BEGIN
  EXEC('ALTER TABLE publisher_rtb_mappings DROP CONSTRAINT ' + @pkName);
END
GO

-- Step 2: Make rtb_id the new PRIMARY KEY
IF NOT EXISTS (
  SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
  WHERE TABLE_NAME = 'publisher_rtb_mappings'
    AND CONSTRAINT_TYPE = 'PRIMARY KEY'
)
BEGIN
  ALTER TABLE publisher_rtb_mappings
    ALTER COLUMN rtb_id NVARCHAR(255) NOT NULL;

  ALTER TABLE publisher_rtb_mappings
    ADD CONSTRAINT PK_publisher_rtb_mappings PRIMARY KEY (rtb_id);
END
GO

-- Step 3: Add index on publisher_id for fast per-publisher lookups
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE object_id = OBJECT_ID('publisher_rtb_mappings')
    AND name = 'ix_rtb_mappings_publisher_id'
)
BEGIN
  CREATE INDEX ix_rtb_mappings_publisher_id ON publisher_rtb_mappings (publisher_id);
END
GO
