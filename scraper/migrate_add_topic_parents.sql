-- Topics in a hierarchy (Oct 2026): category > broad topic > topic >
-- narrower topic. A broad topic ("Solar", "Energy storage and batteries")
-- is an ordinary tag flagged is_broad, filed under one of the fixed
-- categories; each topic points at its parent, and a topic's page and
-- directory counts include the episodes of everything under it. Merging
-- is for the same subject in other words; a narrower subject ("home
-- batteries") gets a parent instead.
SET lock_timeout = '10s';

ALTER TABLE tags ADD COLUMN IF NOT EXISTS parent_tag_id INTEGER REFERENCES tags(tag_id) ON DELETE SET NULL;
ALTER TABLE tags ADD COLUMN IF NOT EXISTS is_broad BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE tags DROP CONSTRAINT IF EXISTS tags_parent_not_self;
ALTER TABLE tags ADD CONSTRAINT tags_parent_not_self CHECK (parent_tag_id IS NULL OR parent_tag_id <> tag_id);
CREATE INDEX IF NOT EXISTS idx_tags_parent ON tags (parent_tag_id) WHERE parent_tag_id IS NOT NULL;
