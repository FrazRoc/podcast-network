"""
Tests for backend/role_selection.py — which one role a person's panels show,
out of every role host_affiliations has on record for them — and for the
backend query that feeds it.
"""
import os
import sys
from datetime import date

import pytest
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))

from role_selection import pick_current_role  # noqa: E402


def row(episode_id, published, title=None, company=None, kind=None, former=False, other=False):
    return {'episode_id': episode_id, 'published_date': published, 'title': title,
            'company': company, 'title_kind': kind, 'is_former': former,
            'from_other_episode': other}


class TestPickCurrentRole:
    def test_nothing_on_record(self):
        assert pick_current_role([]) is None

    def test_most_recent_appearance_wins(self):
        rows = [row(1, date(2021, 3, 1), 'VP of Product', 'Sunrun', 'position'),
                row(2, date(2025, 1, 1), 'CEO', 'Acme Grid', 'position')]
        assert pick_current_role(rows)['company'] == 'Acme Grid'

    def test_former_roles_never_shown(self):
        # David Crane: "the former CEO of NRG. Today, David is a clean energy investor."
        rows = [row(1, date(2025, 1, 1), 'CEO', 'NRG', 'position', former=True),
                row(1, date(2025, 1, 1), 'clean energy investor', None, 'description')]
        assert pick_current_role(rows)['title'] == 'clean energy investor'

    def test_only_former_roles_means_nothing_to_show(self):
        assert pick_current_role([row(1, date(2025, 1, 1), 'CFO', 'Shell', 'position', former=True)]) is None

    def test_other_episode_references_are_skipped(self):
        # A "past episodes you'll love" list naming an old role, attached to a new episode.
        rows = [row(1, date(2022, 1, 1), 'CEO', 'Acme', 'position'),
                row(2, date(2025, 1, 1), 'Founder', 'OldCo', 'position', other=True)]
        assert pick_current_role(rows)['company'] == 'Acme'

    def test_within_an_appearance_position_with_org_beats_the_rest(self):
        rows = [row(1, date(2025, 1, 1), None, 'Wunder'),
                row(1, date(2025, 1, 1), 'writer and investor', None, 'description'),
                row(1, date(2025, 1, 1), 'CEO', None, 'position'),
                row(1, date(2025, 1, 1), 'CEO', 'Wunder', 'position')]
        chosen = pick_current_role(rows)
        assert (chosen['title'], chosen['company']) == ('CEO', 'Wunder')

    def test_newer_description_beats_older_position(self):
        # The agreed rule: recency first, then the kind of title.
        rows = [row(1, date(2019, 1, 1), 'CEO', 'NRG', 'position'),
                row(2, date(2025, 1, 1), 'clean energy investor', None, 'description')]
        assert pick_current_role(rows)['title'] == 'clean energy investor'

    def test_missing_date_counts_as_oldest(self):
        rows = [row(9, None, 'CEO', 'Undated', 'position'),
                row(1, date(2020, 1, 1), 'CEO', 'Dated', 'position')]
        assert pick_current_role(rows)['company'] == 'Dated'

    def test_pin_overrides_everything(self):
        rows = [row(1, date(2025, 1, 1), 'CEO', 'Acme', 'position')]
        chosen = pick_current_role(rows, {'title': 'Partner', 'company': 'Fifth Wall'})
        assert chosen == {'title': 'Partner', 'company': 'Fifth Wall', 'source': 'pinned'}

    def test_empty_pin_is_ignored(self):
        rows = [row(1, date(2025, 1, 1), 'CEO', 'Acme', 'position')]
        assert pick_current_role(rows, {'title': None, 'company': None})['source'] == 'derived'


# ------------------------------------------------------------------
# The backend query, against the disposable test database
# ------------------------------------------------------------------

pytest.importorskip("fastapi")


@pytest.fixture()
def role_db(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT to_regclass('host_affiliations') IS NOT NULL AND to_regclass('host_role_pins') IS NOT NULL")
    if not cur.fetchone()[0]:
        pytest.skip("affiliation/pin migrations not applied — re-run tests/setup_test_db.sh")
    return db_conn


def _appearance(cur, host_id, podcast_id, title, published, other=False):
    cur.execute("INSERT INTO episodes (podcast_id, title, published_date) VALUES (%s, %s, %s) "
                "RETURNING episode_id", (podcast_id, title, published))
    episode_id = cur.fetchone()[0]
    cur.execute("INSERT INTO episode_host (episode_id, host_id, is_guest) VALUES (%s, %s, true)",
                (episode_id, host_id))
    cur.execute("INSERT INTO affiliation_extractions (episode_id, host_id, status, from_other_episode, "
                "appears_on_episode) VALUES (%s, %s, 'done', %s, true)", (episode_id, host_id, other))
    return episode_id


class TestRoleQuery:
    def test_query_feeds_the_rule(self, role_db):
        from main import _role_rows, _role_pin
        cur = role_db.cursor()
        cur.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES ('Show', 'show') RETURNING podcast_id")
        podcast_id = cur.fetchone()[0]
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES ('Jane', 'Doe') RETURNING host_id")
        host_id = cur.fetchone()[0]
        old = _appearance(cur, host_id, podcast_id, 'Old', date(2021, 1, 1))
        new = _appearance(cur, host_id, podcast_id, 'New', date(2025, 1, 1))
        cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title, company, title_kind) VALUES "
                    "(%s, %s, 'VP', 'Sunrun', 'position'), (%s, %s, 'CEO', 'Acme', 'position')",
                    (old, host_id, new, host_id))
        role_db.commit()

        dict_cur = role_db.cursor(cursor_factory=RealDictCursor)
        rows = _role_rows(dict_cur, host_id)
        assert [r['company'] for r in rows] == ['Acme', 'Sunrun']      # newest first
        assert rows[0]['podcast_title'] == 'Show'
        assert pick_current_role(rows, _role_pin(dict_cur, host_id))['company'] == 'Acme'

        cur.execute("INSERT INTO host_role_pins (host_id, title, company) VALUES (%s, 'Partner', 'Fifth Wall')",
                    (host_id,))
        role_db.commit()
        chosen = pick_current_role(_role_rows(dict_cur, host_id), _role_pin(dict_cur, host_id))
        assert (chosen['source'], chosen['company']) == ('pinned', 'Fifth Wall')

    def test_pin_goes_when_the_person_does(self, role_db):
        cur = role_db.cursor()
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES ('Jane', 'Doe') RETURNING host_id")
        host_id = cur.fetchone()[0]
        cur.execute("INSERT INTO host_role_pins (host_id, title) VALUES (%s, 'CEO')", (host_id,))
        cur.execute("DELETE FROM hosts WHERE host_id = %s", (host_id,))
        role_db.commit()
        cur.execute("SELECT count(*) FROM host_role_pins")
        assert cur.fetchone()[0] == 0
