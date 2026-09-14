-- Deleting a row from episode_host does not stick. The scanner re-reads the
-- same episode description on the next run, re-derives the same name, and
-- re-inserts it. A morning of per-person curation — Bill Gates back to 48
-- credits from 3, Joe Manchin to 41 from 2 — was undone by a single scheduled
-- scrape, because nothing recorded that those pairs had been removed on
-- purpose.
--
-- rejected_names cannot express this: it blocks a name everywhere, and these
-- people are real guests on a handful of episodes. The fact being recorded is
-- about a (person, episode) pair, not a person.
--
-- Enforced with a trigger rather than at the call sites. There are ten places
-- that insert into episode_host across the scraper and the backend, and the
-- eleventh is the one that would forget.

SET lock_timeout = '10s';

BEGIN;

CREATE TABLE IF NOT EXISTS credit_suppressions (
    episode_id  INTEGER     NOT NULL REFERENCES episodes(episode_id) ON DELETE CASCADE,
    host_id     INTEGER     NOT NULL REFERENCES hosts(host_id)       ON DELETE CASCADE,
    reason      TEXT,
    created_at  TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (episode_id, host_id)
);

CREATE INDEX IF NOT EXISTS idx_credit_suppressions_host ON credit_suppressions (host_id);

-- Skips the row instead of raising: the scanner inserts in bulk and a
-- suppressed pair is an expected, uninteresting outcome, not an error.
CREATE OR REPLACE FUNCTION skip_suppressed_credit() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM credit_suppressions
        WHERE episode_id = NEW.episode_id AND host_id = NEW.host_id
    ) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_skip_suppressed_credit ON episode_host;
CREATE TRIGGER trg_skip_suppressed_credit
    BEFORE INSERT ON episode_host
    FOR EACH ROW EXECUTE FUNCTION skip_suppressed_credit();

COMMIT;
