-- A person's displayed current role, set by hand.
--
-- The displayed role is normally derived from host_affiliations (most recent
-- appearance, current roles only — see pick_current_role() in
-- backend/role_selection.py). When that picks wrong, a pin here overrides
-- it. Nothing automated writes to this table.
--
-- A separate table rather than columns on hosts: hosts is read by every
-- scan and graph request, and an ALTER TABLE on it is exactly the lock
-- cascade the lock_timeout rule exists for.
--
-- merge_people() moves the dropped person's pin to the survivor only when
-- the survivor has none; otherwise it goes with the dropped row
-- (ON DELETE CASCADE).

SET lock_timeout = '10s';

BEGIN;

CREATE TABLE IF NOT EXISTS host_role_pins (
    host_id     INTEGER     PRIMARY KEY REFERENCES hosts(host_id) ON DELETE CASCADE,
    title       TEXT,
    company     TEXT,
    updated_at  TIMESTAMP   NOT NULL DEFAULT now(),
    CHECK (title IS NOT NULL OR company IS NOT NULL)
);

COMMIT;
