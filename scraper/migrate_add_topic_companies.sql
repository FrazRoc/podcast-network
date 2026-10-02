-- Companies aren't topics. A tag naming a company, investor, nonprofit or
-- media brand ("Tesla", "Breakthrough Energy Ventures") is kept, flagged
-- is_company and linked to its organisation where we have one, so the
-- organisation's page can list the episodes that discuss it. Company tags
-- are left out of every topic list, page and rollup.
SET lock_timeout = '5s';

ALTER TABLE tags ADD COLUMN IF NOT EXISTS is_company BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE tags ADD COLUMN IF NOT EXISTS org_id INTEGER REFERENCES organizations(org_id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_tags_org ON tags (org_id) WHERE org_id IS NOT NULL;
