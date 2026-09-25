-- Facts about organisations and people found outside the episode text
-- (scraper/enrich.py, Sep 2026): websites from show-note links, Clearbit's
-- autocomplete and Wikidata; organisation facts and people's photos and
-- links from Wikidata; people's LinkedIn / X / Bluesky from show notes.
--
-- Every value records where it came from, so a later run never overwrites
-- one typed in Company Admin or People Admin ('admin') and a bad source can
-- be undone as a batch.

SET lock_timeout = '10s';

BEGIN;

ALTER TABLE organizations
    -- show_notes | clearbit | wikidata | manual | admin
    ADD COLUMN IF NOT EXISTS website_source  VARCHAR(20),
    ADD COLUMN IF NOT EXISTS wikidata_id     VARCHAR(20),
    ADD COLUMN IF NOT EXISTS country         TEXT,
    ADD COLUMN IF NOT EXISTS hq_city         TEXT,
    ADD COLUMN IF NOT EXISTS hq_lat          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS hq_lon          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS founded_year    SMALLINT,
    ADD COLUMN IF NOT EXISTS wikipedia_url   TEXT,
    ADD COLUMN IF NOT EXISTS linkedin_url    TEXT,
    ADD COLUMN IF NOT EXISTS twitter_handle  TEXT,
    ADD COLUMN IF NOT EXISTS bluesky_handle  TEXT,
    -- A freely licensed logo file on Wikimedia Commons (a fallback to logo.dev).
    ADD COLUMN IF NOT EXISTS commons_logo_url TEXT,
    ADD COLUMN IF NOT EXISTS enriched_at     TIMESTAMP;

CREATE UNIQUE INDEX IF NOT EXISTS uq_organizations_wikidata
    ON organizations (wikidata_id) WHERE wikidata_id IS NOT NULL;

ALTER TABLE hosts
    ADD COLUMN IF NOT EXISTS wikidata_id    VARCHAR(20),
    -- {field: source} for values filled by enrichment, e.g.
    -- {"linkedin_url": "show_notes", "profile_image_url": "wikidata"}.
    ADD COLUMN IF NOT EXISTS field_sources  JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMIT;

-- A full link when the organisation's page isn't its domain's home page
-- ("https://www.wartsila.com/energy" for Wärtsilä Energy). website_domain
-- stays the bare domain, which is what the logo lookup uses.
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS website_url TEXT;
