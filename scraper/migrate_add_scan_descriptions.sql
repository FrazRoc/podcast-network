-- ============================================================
-- Migration: podcasts.scan_descriptions
-- Run once: psql podcast_db < scraper/migrate_add_scan_descriptions.sql
-- ============================================================
--
-- Whether to read this show's episode descriptions when looking for people.
--
-- It is off for POLITICO Energy: the show discusses politicians constantly but
-- rarely has a guest, so nearly every name found in a description belongs to
-- someone being talked about rather than someone present. The false positives
-- outnumber the real credits badly enough that the descriptions are not worth
-- reading at all.
--
-- This lived as a hardcoded set inside the scanner, where nothing else could
-- see it — the diagnostics page reported the show as an unexplained gap in
-- coverage rather than a deliberate exclusion.

-- Run this when no long scan is in flight. A scanner run holds a read lock on
-- podcasts for its whole transaction, and an ALTER waiting behind it blocks
-- every new reader of the table, including the public API.
ALTER TABLE podcasts
    ADD COLUMN IF NOT EXISTS scan_descriptions BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE podcasts SET scan_descriptions = FALSE WHERE title = 'POLITICO Energy';

SELECT title, scan_descriptions FROM podcasts WHERE NOT scan_descriptions;
