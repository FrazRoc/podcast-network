-- Organisations: the canonical list behind the free-text company names in
-- host_affiliations.
--
-- The same organisation arrives spelled many ways ("BNEF", "BloombergNEF",
-- "Bloomberg NEF", "Bloomberg New Energy Finance"). Same pattern as people:
-- one row per real thing (organizations), every spelling seen mapped to it
-- (organization_aliases), and deliberate non-merges recorded so they are not
-- proposed again (not_same_org_pairs, like not_duplicate_pairs).
--
-- host_affiliations.company_key is the company string normalised
-- (backend/org_names.py: normalize_org_name) and joins to
-- organization_aliases.normalized_name. Linking goes through the alias, not
-- a stored org_id, so merging two organisations or adding an alias re-links
-- every past and future role with that spelling at once, with no backfill.
--
-- New spellings become organisations automatically (scraper/organizations.py
-- sync); review happens as merges in Company Admin, not one approval per
-- company — the long tail is thousands of names, nearly all seen once.

SET lock_timeout = '10s';

-- Similarity search for merge suggestions ("Bloomberg NEF" ~ "BloombergNEF").
CREATE EXTENSION IF NOT EXISTS pg_trgm;

BEGIN;

CREATE TABLE IF NOT EXISTS organizations (
    org_id          SERIAL      PRIMARY KEY,
    name            TEXT        NOT NULL,
    org_type        VARCHAR(20),
    -- Sub-units roll up to a parent (BloombergNEF, Bloomberg Green ->
    -- Bloomberg; DOE Loan Programs Office -> DOE). Whether a query includes
    -- sub-units is decided per query.
    parent_org_id   INTEGER     REFERENCES organizations(org_id) ON DELETE SET NULL,
    website_domain  TEXT,
    -- Extracted strings that are not organisations at all ("COP28",
    -- "Southern Nevada"). Kept, so the spelling is not re-created, but
    -- hidden from lists and never shown as someone's company.
    not_an_org      BOOLEAN     NOT NULL DEFAULT false,
    created_at      TIMESTAMP   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT now(),
    CHECK (org_type IN ('company', 'nonprofit', 'government', 'academic', 'research',
                        'media', 'investor', 'association', 'other')),
    CHECK (parent_org_id IS NULL OR parent_org_id <> org_id)
);

CREATE INDEX IF NOT EXISTS idx_organizations_parent ON organizations (parent_org_id);
CREATE INDEX IF NOT EXISTS idx_organizations_name_trgm
    ON organizations USING gin (lower(name) gin_trgm_ops);

CREATE TABLE IF NOT EXISTS organization_aliases (
    alias_id         SERIAL      PRIMARY KEY,
    org_id           INTEGER     NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    alias_name       TEXT        NOT NULL,
    normalized_name  TEXT        NOT NULL UNIQUE,
    -- auto: created by sync from an extracted spelling; merge: carried over
    -- when two organisations were merged; manual: added by hand.
    source           VARCHAR(20) NOT NULL DEFAULT 'auto',
    created_at       TIMESTAMP   NOT NULL DEFAULT now(),
    CHECK (source IN ('auto', 'merge', 'manual'))
);

CREATE INDEX IF NOT EXISTS idx_organization_aliases_org ON organization_aliases (org_id);

CREATE TABLE IF NOT EXISTS not_same_org_pairs (
    org_a       INTEGER   NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    org_b       INTEGER   NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (org_a, org_b),
    CHECK (org_a < org_b)
);

-- New and small (a few thousand rows, written only by the extraction), so
-- the ALTER is cheap; lock_timeout above still applies.
ALTER TABLE host_affiliations ADD COLUMN IF NOT EXISTS company_key TEXT;
CREATE INDEX IF NOT EXISTS idx_host_affiliations_company_key ON host_affiliations (company_key);

COMMIT;
