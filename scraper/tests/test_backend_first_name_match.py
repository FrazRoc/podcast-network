"""
Integration tests for backend/main.py's link_matching_episodes_by_first_name,
against the disposable Postgres database (see conftest.py).

Real incident: John Failla, created and registered as a host of Smart Energy
Voices, was linked on the 44 episodes that name him in full but not on ones
that only say "John speaks with ..." — the admin's "Rescan" button found
nothing on those, even though the scheduled scanner's run() already matches
this shape via show_host_first_names(). This function closes that gap.
"""
import os
import sys

import pytest
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))

# backend/main.py pulls in FastAPI, which isn't a scraper dependency — skip
# this module rather than forcing the scraper test suite to install it.
pytest.importorskip("fastapi")

from main import link_matching_episodes_by_first_name  # noqa: E402
from host_extractor import get_or_create_host  # noqa: E402


def _insert_podcast(cur, title):
    cur.execute(
        "INSERT INTO podcasts (title, apple_podcast_id) VALUES (%s, %s) RETURNING podcast_id",
        (title, title.lower().replace(' ', '-'))
    )
    return cur.fetchone()[0]


def _insert_episode(cur, podcast_id, title, description=''):
    cur.execute(
        "INSERT INTO episodes (podcast_id, title, description) VALUES (%s, %s, %s) RETURNING episode_id",
        (podcast_id, title, description)
    )
    return cur.fetchone()[0]


class TestLinkMatchingEpisodesByFirstName:
    def test_matches_bare_first_name_on_other_episodes(self, db_conn):
        # get_or_create_host expects a plain tuple-cursor; the function under
        # test expects a RealDictCursor — use one of each on the same connection.
        setup_cur = db_conn.cursor()
        podcast_id = _insert_podcast(setup_cur, "Smart Energy Voices")
        john_id = get_or_create_host(setup_cur, "John Failla", "manual")
        setup_cur.execute(
            "INSERT INTO host_podcast (host_id, podcast_id, role) VALUES (%s, %s, 'Host')",
            (john_id, podcast_id)
        )
        matching_ep = _insert_episode(
            setup_cur, podcast_id, "A Chat About Solar",
            "In this episode, John speaks with our guest about solar trends."
        )
        db_conn.commit()

        cur = db_conn.cursor(cursor_factory=RealDictCursor)
        results = link_matching_episodes_by_first_name(cur, john_id, "John", "Failla")
        db_conn.commit()

        assert len(results) == 1
        assert results[0]['podcast_title'] == "Smart Energy Voices"
        setup_cur.execute(
            "SELECT data_source FROM episode_host WHERE episode_id=%s AND host_id=%s",
            (matching_ep, john_id)
        )
        row = setup_cur.fetchone()
        assert row is not None
        assert row[0] == 'host_first_name'

    def test_skipped_when_another_host_shares_first_name(self, db_conn):
        # Two Johns registered on the same show — first-name matching is
        # ambiguous, so it must not fire at all (mirrors show_host_first_names).
        setup_cur = db_conn.cursor()
        podcast_id = _insert_podcast(setup_cur, "Two Johns Show")
        john_a = get_or_create_host(setup_cur, "John Smith", "manual")
        john_b = get_or_create_host(setup_cur, "John Doe", "manual")
        setup_cur.execute(
            "INSERT INTO host_podcast (host_id, podcast_id, role) VALUES (%s, %s, 'Host'), (%s, %s, 'Host')",
            (john_a, podcast_id, john_b, podcast_id)
        )
        _insert_episode(setup_cur, podcast_id, "An Episode", "John talks about batteries.")
        db_conn.commit()

        cur = db_conn.cursor(cursor_factory=RealDictCursor)
        results = link_matching_episodes_by_first_name(cur, john_a, "John", "Smith")
        assert results == []

    def test_skipped_when_first_name_belongs_to_someone_else_in_text(self, db_conn):
        # "Jordan Yates" mentioned in the text must not match host "Dan".
        setup_cur = db_conn.cursor()
        podcast_id = _insert_podcast(setup_cur, "Some Show")
        dan_id = get_or_create_host(setup_cur, "Dan Yates", "manual")
        setup_cur.execute(
            "INSERT INTO host_podcast (host_id, podcast_id, role) VALUES (%s, %s, 'Host')",
            (dan_id, podcast_id)
        )
        _insert_episode(setup_cur, podcast_id, "An Episode", "Our guest today is Jordan Yates.")
        db_conn.commit()

        cur = db_conn.cursor(cursor_factory=RealDictCursor)
        results = link_matching_episodes_by_first_name(cur, dan_id, "Dan", "Yates")
        assert results == []

    def test_excludes_given_episode_id(self, db_conn):
        setup_cur = db_conn.cursor()
        podcast_id = _insert_podcast(setup_cur, "Smart Energy Voices Two")
        john_id = get_or_create_host(setup_cur, "John Failla", "manual")
        setup_cur.execute(
            "INSERT INTO host_podcast (host_id, podcast_id, role) VALUES (%s, %s, 'Host')",
            (john_id, podcast_id)
        )
        source_ep = _insert_episode(setup_cur, podcast_id, "Source Episode", "John introduces the show.")
        db_conn.commit()

        cur = db_conn.cursor(cursor_factory=RealDictCursor)
        results = link_matching_episodes_by_first_name(
            cur, john_id, "John", "Failla", exclude_episode_id=source_ep
        )
        assert results == []

    def test_short_first_name_is_ignored(self, db_conn):
        # Guard against noisy 1-2 letter first names matching everywhere.
        setup_cur = db_conn.cursor()
        podcast_id = _insert_podcast(setup_cur, "Al's Show")
        al_id = get_or_create_host(setup_cur, "Al Gore", "manual")
        setup_cur.execute(
            "INSERT INTO host_podcast (host_id, podcast_id, role) VALUES (%s, %s, 'Host')",
            (al_id, podcast_id)
        )
        _insert_episode(setup_cur, podcast_id, "An Episode", "Al discusses climate policy.")
        db_conn.commit()

        cur = db_conn.cursor(cursor_factory=RealDictCursor)
        results = link_matching_episodes_by_first_name(cur, al_id, "Al", "Gore")
        assert results == []
