-- The Company Admin merge-suggestion queue, stored instead of computed per
-- request.
--
-- Computing it (backend/org_suggestions.py: trigram self-join over every
-- organisation, plus acronym and name-containment passes) took ~95 s against
-- production once there were 7,500 organisations — far too slow for a page.
-- It is rebuilt by `organizations.py sync` (so after every scheduled
-- extraction) and by the Recompute button in Company Admin; the page only
-- reads.
--
-- Rows go stale in one direction only, and that is handled at write time:
-- a merge deletes the dropped organisation (its rows cascade away), "not the
-- same" and "is part of" delete their row. Pairs a merge newly creates for
-- the survivor appear at the next rebuild.

SET lock_timeout = '10s';

BEGIN;

CREATE TABLE IF NOT EXISTS company_merge_suggestions (
    org_a        INTEGER     NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    org_b        INTEGER     NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    -- acronym | similar | contains
    reason       VARCHAR(20) NOT NULL,
    score        REAL        NOT NULL,
    -- People affected (both sides) when computed; the queue's sort order.
    people       INTEGER     NOT NULL,
    computed_at  TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (org_a, org_b),
    CHECK (org_a < org_b),
    CHECK (reason IN ('acronym', 'similar', 'contains'))
);

CREATE INDEX IF NOT EXISTS idx_company_merge_suggestions_rank
    ON company_merge_suggestions (people DESC, score DESC);
CREATE INDEX IF NOT EXISTS idx_company_merge_suggestions_b ON company_merge_suggestions (org_b);

COMMIT;
