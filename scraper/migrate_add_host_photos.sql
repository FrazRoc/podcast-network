-- Profile photos we keep ourselves (Oct 2026): a resized copy of an approved
-- headshot, for sources the image proxy won't fetch from — episode artwork
-- on podcast CDNs, organisations' team pages — and so a photo doesn't
-- disappear when the site it came from changes. Served by
-- GET /api/people/{host_id}/photo; hosts.profile_image_url points there
-- (backend/host_photos.py). One photo per person; a merge moves it to the
-- survivor if the survivor has none.

SET lock_timeout = '10s';

BEGIN;

CREATE TABLE IF NOT EXISTS host_photos (
    host_id       INTEGER     PRIMARY KEY REFERENCES hosts(host_id) ON DELETE CASCADE,
    image         BYTEA       NOT NULL,
    content_type  TEXT        NOT NULL,
    width         INTEGER     NOT NULL,
    height        INTEGER     NOT NULL,
    -- Where it came from: episode_art, team_page, manual…
    source        TEXT        NOT NULL,
    source_url    TEXT,
    created_at    TIMESTAMP   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMP   NOT NULL DEFAULT now()
);

COMMIT;
