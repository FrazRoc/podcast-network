-- Topic Admin's merge-suggestion queue (Oct 2026), the Company Admin pattern
-- (migrate_add_company_merge_suggestions.sql): computed by
-- backend/topic_suggestions.py, stored, and only read by the page. Rebuilt
-- after each tagging import/collect and by the Recompute button.
--
-- Only likely synonyms are suggested — the same words reordered, the same
-- words spelled differently ("clean energy investing" / "clean energy
-- investment"), or an acronym the episodes themselves use for the phrase.
-- A narrower topic is a parent decision, not a merge, and the hierarchy
-- already holds those.
--
-- A merge deletes the dropped tag (its rows cascade away); "different" is
-- recorded in not_same_topic_pairs so the pair is never suggested again.

SET lock_timeout = '10s';

BEGIN;

CREATE TABLE IF NOT EXISTS topic_merge_suggestions (
    tag_a        INTEGER     NOT NULL REFERENCES tags(tag_id) ON DELETE CASCADE,
    tag_b        INTEGER     NOT NULL REFERENCES tags(tag_id) ON DELETE CASCADE,
    -- same_words | spelling | acronym
    reason       VARCHAR(20) NOT NULL,
    score        REAL        NOT NULL,
    -- Episodes on both sides when computed; the queue's sort order.
    episodes     INTEGER     NOT NULL,
    computed_at  TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (tag_a, tag_b),
    CHECK (tag_a < tag_b),
    CHECK (reason IN ('same_words', 'spelling', 'acronym'))
);
CREATE INDEX IF NOT EXISTS idx_topic_merge_suggestions_rank ON topic_merge_suggestions (episodes DESC, score DESC);
CREATE INDEX IF NOT EXISTS idx_topic_merge_suggestions_b ON topic_merge_suggestions (tag_b);

CREATE TABLE IF NOT EXISTS not_same_topic_pairs (
    tag_a       INTEGER   NOT NULL REFERENCES tags(tag_id) ON DELETE CASCADE,
    tag_b       INTEGER   NOT NULL REFERENCES tags(tag_id) ON DELETE CASCADE,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (tag_a, tag_b),
    CHECK (tag_a < tag_b)
);

COMMIT;
