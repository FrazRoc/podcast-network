-- ============================================================
-- Migration: Add scrape_status table (single row tracking the
-- last time the automated scrape pipeline completed successfully)
-- Run once: psql podcast_db < scraper/migrate_add_scrape_status.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS scrape_status (
    id          INTEGER PRIMARY KEY DEFAULT 1,
    last_run_at TIMESTAMP,
    CHECK (id = 1)
);

INSERT INTO scrape_status (id, last_run_at)
VALUES (1, NULL)
ON CONFLICT (id) DO NOTHING;
