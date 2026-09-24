-- Job title and organisation for each guest appearance, read from the
-- episode's own title and description.
--
-- Stored per appearance, not per person. People change jobs, and a 2021
-- episode that says "CEO of X" and a 2025 one that says "partner at Y" are
-- both true; a person's current role is simply their most recent row. Keeping
-- every observation also keeps every value auditable against the text it came
-- from.
--
-- Two tables, because "we looked and found nothing" is a result too:
--
--   affiliation_extractions — one row per (host, episode) that has been
--     processed, whatever the outcome. The snippet sent to the model is kept
--     here, so any extracted value can be checked against the exact text it
--     was read from. Without this table an appearance whose description names
--     no role would be re-sent on every run.
--
--   host_affiliations — the facts: zero or more (title, company) rows per
--     appearance. data_source follows the same precedence as episode_host:
--     'manual' rows are never overwritten by an automated pass.
--
-- Both reference episode_host rather than hosts/episodes separately, with
-- ON UPDATE / ON DELETE CASCADE. That keeps them correct through the two
-- operations that rewrite credits: merge_people() re-points episode_host to
-- the surviving host_id (the affiliations follow), and deleting or
-- suppressing a wrong credit removes the affiliation read for it.

SET lock_timeout = '10s';

BEGIN;

CREATE TABLE IF NOT EXISTS affiliation_extractions (
    episode_id    INTEGER     NOT NULL,
    host_id       INTEGER     NOT NULL,
    -- pending    submitted in a batch, result not collected yet
    -- done       result recorded (possibly with no affiliations)
    -- no_mention the person's name does not appear in the title or
    --            description, so there was nothing to send
    -- host       the text presents them as this podcast's host or producer
    --            (not in host_podcast, or those would never be selected);
    --            nothing stored, left for a separate hosts process
    -- retry      the batch request failed or the item was missing from
    --            the response; picked up again while attempts < 3
    status        VARCHAR(20) NOT NULL,
    snippet       TEXT,
    snippet_hash  CHAR(16),
    batch_id      TEXT,
    model         TEXT,
    -- Set when the result is recorded (done / host), NULL before.
    -- appears_on_episode false: the text only talks about the person (a
    --   politician whose policy is discussed, someone quoted from
    --   elsewhere). Curation signal for false guest credits; never acted on
    --   automatically.
    -- from_other_episode true: the text about them is a "past episodes"
    --   list, a link to another episode, or a rerun of an older recording,
    --   so any role is not as of this episode's date.
    appears_on_episode BOOLEAN,
    from_other_episode BOOLEAN,
    attempts      INTEGER     NOT NULL DEFAULT 0,
    created_at    TIMESTAMP   NOT NULL DEFAULT now(),
    completed_at  TIMESTAMP,
    PRIMARY KEY (episode_id, host_id),
    FOREIGN KEY (episode_id, host_id) REFERENCES episode_host (episode_id, host_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    CHECK (status IN ('pending', 'done', 'no_mention', 'host', 'retry'))
);

CREATE INDEX IF NOT EXISTS idx_affiliation_extractions_host
    ON affiliation_extractions (host_id);
CREATE INDEX IF NOT EXISTS idx_affiliation_extractions_pending
    ON affiliation_extractions (batch_id) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS host_affiliations (
    affiliation_id SERIAL      PRIMARY KEY,
    episode_id     INTEGER     NOT NULL,
    host_id        INTEGER     NOT NULL,
    title          TEXT,
    company        TEXT,
    -- 'position' (CEO, senior fellow, Senator) or 'description'
    -- ("ecologist and conservationist"); NULL when title is NULL.
    title_kind     VARCHAR(20),
    -- Role the text marks as past (former, previously, ex-).
    is_former      BOOLEAN     NOT NULL DEFAULT false,
    data_source    VARCHAR(50) NOT NULL DEFAULT 'llm_extracted',
    created_at     TIMESTAMP   NOT NULL DEFAULT now(),
    FOREIGN KEY (episode_id, host_id) REFERENCES episode_host (episode_id, host_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    CHECK (title IS NOT NULL OR company IS NOT NULL),
    CHECK (title_kind IN ('position', 'description')),
    CHECK (title_kind IS NULL OR title IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_host_affiliations_host
    ON host_affiliations (host_id);
CREATE INDEX IF NOT EXISTS idx_host_affiliations_episode
    ON host_affiliations (episode_id);
-- Same role recorded twice for one appearance is a duplicate, whichever
-- side is missing.
CREATE UNIQUE INDEX IF NOT EXISTS uq_host_affiliations_role
    ON host_affiliations (episode_id, host_id, COALESCE(title, ''), COALESCE(company, ''), is_former);

COMMIT;
