-- ============================================================
-- Migration: Add suggestions table for human-in-the-loop enrichment
-- Run once: psql podcast_db < scraper/migrate_add_suggestions.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS suggestions (
    suggestion_id   SERIAL PRIMARY KEY,

    -- The candidate name found in the text
    candidate_name  VARCHAR(255) NOT NULL,
    first_name      TEXT,
    last_name       TEXT,

    -- Where it was found
    episode_id      INTEGER REFERENCES episodes(episode_id),
    source          VARCHAR(50) NOT NULL,   -- 'parsed_title' | 'parsed_desc'
    matched_text    TEXT,                   -- the specific phrase that triggered the match

    -- Review state
    status          VARCHAR(20) DEFAULT 'pending'
                    CHECK (status IN ('pending', 'approved', 'rejected', 'skipped')),
    reviewed_at     TIMESTAMP,

    -- If approved, link to the created host record
    host_id         INTEGER REFERENCES hosts(host_id),

    -- Deduplication — don't re-suggest the same name for the same episode
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (candidate_name, episode_id)
);

-- Index for the admin queue query
CREATE INDEX IF NOT EXISTS idx_suggestions_status
    ON suggestions(status, created_at);

CREATE INDEX IF NOT EXISTS idx_suggestions_candidate
    ON suggestions(candidate_name);

CREATE INDEX IF NOT EXISTS idx_suggestions_episode
    ON suggestions(episode_id);

-- Also add a rejected_names table so the parser never re-suggests
-- names you've explicitly rejected
CREATE TABLE IF NOT EXISTS rejected_names (
    rejected_name_id SERIAL PRIMARY KEY,
    candidate_name   VARCHAR(255) UNIQUE NOT NULL,
    reason           TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Verify
SELECT 'suggestions table created' as status,
       COUNT(*) as existing_rows
FROM suggestions;
