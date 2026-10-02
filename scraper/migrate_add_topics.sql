-- What episodes are about (Oct 2026). Topics are open-ended tags the model
-- writes per episode ("small modular reactors", "interconnection queues"),
-- each filed under one of a few fixed categories for browsing. A person's
-- topics are counted from the episodes they are credited on.
--
-- Duplicates are handled the way organisations are: every spelling is an
-- alias of one canonical tag (tag_aliases ~ organization_aliases), and
-- merging moves the aliases. episode_tag always points at canonical tags.
--
-- tags / episode_tag existed in the original schema and were never used.

SET lock_timeout = '10s';

BEGIN;

ALTER TABLE tags ALTER COLUMN name TYPE VARCHAR(200);
ALTER TABLE tags ALTER COLUMN name SET NOT NULL;
ALTER TABLE tags ADD COLUMN IF NOT EXISTS slug VARCHAR(200);
ALTER TABLE tags ADD COLUMN IF NOT EXISTS category VARCHAR(40);
ALTER TABLE tags ADD COLUMN IF NOT EXISTS not_a_topic BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE tags ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT now();
CREATE UNIQUE INDEX IF NOT EXISTS uq_tags_slug ON tags (slug);

CREATE TABLE IF NOT EXISTS tag_aliases (
    alias_id SERIAL PRIMARY KEY,
    tag_id INTEGER NOT NULL REFERENCES tags(tag_id) ON DELETE CASCADE,
    alias_name VARCHAR(200) NOT NULL,
    normalized_name VARCHAR(200) NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_tag_aliases_tag ON tag_aliases (tag_id);

ALTER TABLE episode_tag ADD COLUMN IF NOT EXISTS is_primary BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE episode_tag ADD COLUMN IF NOT EXISTS evidence TEXT;
ALTER TABLE episode_tag ADD COLUMN IF NOT EXISTS data_source VARCHAR(20) NOT NULL DEFAULT 'llm';
ALTER TABLE episode_tag DROP CONSTRAINT IF EXISTS episode_tag_episode_id_fkey;
ALTER TABLE episode_tag ADD CONSTRAINT episode_tag_episode_id_fkey
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE;
ALTER TABLE episode_tag DROP CONSTRAINT IF EXISTS episode_tag_tag_id_fkey;
ALTER TABLE episode_tag ADD CONSTRAINT episode_tag_tag_id_fkey
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id) ON DELETE CASCADE ON UPDATE CASCADE;
CREATE INDEX IF NOT EXISTS idx_episode_tag_tag ON episode_tag (tag_id);

-- One row per processed episode, whatever the outcome (mirrors
-- affiliation_extractions): the exact text sent, for auditing.
CREATE TABLE IF NOT EXISTS topic_extractions (
    episode_id INTEGER PRIMARY KEY REFERENCES episodes(episode_id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'done', 'empty', 'retry')),
    text_sent TEXT,
    batch_id TEXT,
    model TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    completed_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_topic_extractions_pending ON topic_extractions (batch_id) WHERE status = 'pending';

COMMIT;
