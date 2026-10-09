-- A short description per topic (Oct 2026): shown under the name on the
-- topic page, written for the broad topics first. Edited in Topic Admin.
SET lock_timeout = '10s';
ALTER TABLE tags ADD COLUMN IF NOT EXISTS description TEXT;
