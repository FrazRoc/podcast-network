-- ============================================================
-- Migration: Add data_source column to hosts, episode_host, host_podcast
-- Stamps all existing records as 'apple_verified'
-- Run once: psql podcast_db < scraper/migrate_add_data_source.sql
-- ============================================================

-- Add columns (IF NOT EXISTS is safe to re-run)
ALTER TABLE hosts
    ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'apple_verified';

ALTER TABLE episode_host
    ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'apple_verified';

ALTER TABLE host_podcast
    ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'apple_verified';

-- Stamp all existing rows as apple_verified
UPDATE hosts SET data_source = 'apple_verified' WHERE data_source IS NULL;
UPDATE episode_host SET data_source = 'apple_verified' WHERE data_source IS NULL;
UPDATE host_podcast SET data_source = 'apple_verified' WHERE data_source IS NULL;

-- Verify
SELECT 'hosts' as table_name, data_source, COUNT(*) 
FROM hosts GROUP BY data_source
UNION ALL
SELECT 'episode_host', data_source, COUNT(*) 
FROM episode_host GROUP BY data_source
UNION ALL
SELECT 'host_podcast', data_source, COUNT(*) 
FROM host_podcast GROUP BY data_source
ORDER BY table_name, data_source;
