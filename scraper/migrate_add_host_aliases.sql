-- ============================================================
-- Migration: Add host_aliases + not_duplicate_pairs
-- Run once: psql podcast_db < scraper/migrate_add_host_aliases.sql
-- ============================================================
--
-- Why aliases exist
-- -----------------
-- The same person is written different ways by different sources: Apple's
-- credits say "Nathaniel Bullard" while a show description says "Nat Bullard".
-- Both are correct, so neither can be fixed at the source the way a stray
-- "Dr." prefix could be.
--
-- Merging the two records is only half the job. Every name lookup keys off
-- first_name || ' ' || last_name, so if the merged-away spelling is simply
-- deleted, the next episode that uses it matches nothing — and the scraper
-- recreates the duplicate, or the scanner re-suggests the name forever.
-- Recording the spelling here keeps it matchable against the surviving person.

CREATE TABLE IF NOT EXISTS host_aliases (
    alias_id        SERIAL PRIMARY KEY,
    host_id         INTEGER NOT NULL REFERENCES hosts(host_id) ON DELETE CASCADE,
    alias_name      TEXT NOT NULL,          -- as written, for display: "Nat Bullard"
    -- Lookup key: lowercase, letters only, so spacing and punctuation don't
    -- matter. UNIQUE because one spelling must never resolve to two people.
    normalized_name TEXT NOT NULL UNIQUE,
    source          VARCHAR(50) DEFAULT 'merge',   -- 'merge' | 'manual'
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_host_aliases_host ON host_aliases(host_id);
CREATE INDEX IF NOT EXISTS idx_host_aliases_norm ON host_aliases(normalized_name);

-- Pairs a human has judged to be genuinely different people. Without this the
-- duplicate report would surface the same rejected pair after every scrape.
-- Stored low-id-first so the pair is order-independent.
CREATE TABLE IF NOT EXISTS not_duplicate_pairs (
    host_id_a  INTEGER NOT NULL REFERENCES hosts(host_id) ON DELETE CASCADE,
    host_id_b  INTEGER NOT NULL REFERENCES hosts(host_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (host_id_a, host_id_b),
    CHECK (host_id_a < host_id_b)
);

-- Verify
SELECT 'host_aliases' AS table_name, COUNT(*) AS rows FROM host_aliases
UNION ALL
SELECT 'not_duplicate_pairs', COUNT(*) FROM not_duplicate_pairs;
