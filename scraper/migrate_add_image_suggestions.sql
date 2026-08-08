-- ============================================================
-- Migration: Add image_suggestions table for profile image review
-- Run once: psql podcast_db < scraper/migrate_add_image_suggestions.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS image_suggestions (
    id          SERIAL PRIMARY KEY,
    host_id     INTEGER REFERENCES hosts(host_id) ON DELETE CASCADE,
    image_url   TEXT NOT NULL,
    source      VARCHAR(50) NOT NULL,  -- 'wikipedia', 'google_kg', 'twitter'
    source_url  TEXT,                  -- link to the source page for verification
    status      VARCHAR(20) DEFAULT 'pending'
                CHECK (status IN ('pending', 'approved', 'rejected')),
    reviewed_at TIMESTAMP,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (host_id, image_url)
);

CREATE INDEX IF NOT EXISTS idx_image_suggestions_status
    ON image_suggestions(status, created_at);

CREATE INDEX IF NOT EXISTS idx_image_suggestions_host
    ON image_suggestions(host_id);

-- Verify
SELECT 'image_suggestions table created' as status,
       COUNT(*) as existing_rows
FROM image_suggestions;
