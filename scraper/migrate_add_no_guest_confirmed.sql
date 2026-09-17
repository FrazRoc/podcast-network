-- ============================================================
-- Migration: episodes.no_guest_confirmed
-- Run once: psql podcast_db < scraper/migrate_add_no_guest_confirmed.sql
-- ============================================================
--
-- A human's positive statement that this specific episode has no guest at
-- all — a solo monologue, a Q&A, a producer update — as opposed to an
-- episode where the scanner simply hasn't found the guest yet. Without this
-- distinction, "missing a guest credit" and "genuinely has none" look
-- identical everywhere: the diagnostics coverage chart counts the episode
-- against the show forever, and the scanner keeps re-scanning it on every
-- run looking for a name that was never going to be there.
--
-- Set from the admin Episodes page. The scanner (episode_name_scanner.py)
-- and the diagnostics/episode-list "missing a guest" filter both read it —
-- see get_episodes_to_scan()/get_all_episodes() and the diagnostics query
-- in backend/main.py.

SET lock_timeout = '10s';

ALTER TABLE episodes
    ADD COLUMN IF NOT EXISTS no_guest_confirmed BOOLEAN NOT NULL DEFAULT FALSE;
