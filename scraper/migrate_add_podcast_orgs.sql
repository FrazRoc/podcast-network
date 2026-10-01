-- Organisations that are really podcasts. "Host of Drilled" made Drilled an
-- organisation with nothing behind it (no website, no Wikidata, no type to
-- speak of): 51 organisations matched a show's title in Oct 2026. Some of
-- those are the show itself; others (Pexapark, Energy Central) are real
-- companies that publish a podcast of the same name.
--
-- Recorded on the podcast, since a show belongs to at most one organisation:
--   org_id       the organisation
--   org_is_show  true: the organisation record *is* the show (its page and
--                links go to the show page instead);
--                false: the organisation publishes the show ("From Pexapark"
--                on the show page, "Podcast: ..." on the organisation's).
-- An organisation can be at most one show; a publisher can have several.

SET lock_timeout = '10s';

BEGIN;

ALTER TABLE podcasts ADD COLUMN IF NOT EXISTS org_id INTEGER
    REFERENCES organizations(org_id) ON DELETE SET NULL;
ALTER TABLE podcasts ADD COLUMN IF NOT EXISTS org_is_show BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_podcasts_org ON podcasts (org_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_podcasts_show_org ON podcasts (org_id) WHERE org_is_show;

COMMIT;
