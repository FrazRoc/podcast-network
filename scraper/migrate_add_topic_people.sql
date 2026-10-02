-- People aren't topics either. A tag naming a person ("Joe Manchin") is
-- kept, flagged is_person and linked to their profile where we have one,
-- so the person's page can list the episodes that discuss them. Like
-- company tags, person tags are left out of every topic list and rollup.
SET lock_timeout = '5s';

ALTER TABLE tags ADD COLUMN IF NOT EXISTS is_person BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE tags ADD COLUMN IF NOT EXISTS host_id INTEGER REFERENCES hosts(host_id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_tags_host ON tags (host_id) WHERE host_id IS NOT NULL;
