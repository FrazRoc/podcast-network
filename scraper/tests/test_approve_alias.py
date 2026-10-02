"""Approving a suggestion under an existing person's name keeps the
suggested spelling as an alias (suggestion 40170: "Cat Morehouse" approved
as Catherine Morehouse)."""
import asyncio
import os
import sys

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))


def _run(db_conn, monkeypatch, fn, *args):
    pytest.importorskip("fastapi")
    import main
    monkeypatch.setattr(main, 'get_db_connection',
                        lambda: psycopg2.connect(db_conn.dsn, cursor_factory=RealDictCursor))
    r = getattr(main, fn)(*args)
    return asyncio.run(r) if asyncio.iscoroutine(r) else r


def _setup(db_conn, scan_descriptions=True):
    cur = db_conn.cursor()
    cur.execute("INSERT INTO podcasts (apple_podcast_id, title, scan_descriptions) VALUES ('1', 'Show', %s) RETURNING podcast_id",
                (scan_descriptions,))
    pid = cur.fetchone()[0]
    cur.execute("INSERT INTO hosts (first_name, last_name) VALUES ('Catherine', 'Morehouse') RETURNING host_id")
    hid = cur.fetchone()[0]
    eps = []
    for title, desc in [('Grid news', 'Cat Morehouse sits down with a regulator.'),
                        ('More grid news', 'We talk with Cat Morehouse about transmission.'),
                        ('Cat Morehouse on the grid', '')]:
        cur.execute("INSERT INTO episodes (podcast_id, title, description, published_date) "
                    "VALUES (%s, %s, %s, '2025-01-01') RETURNING episode_id", (pid, title, desc))
        eps.append(cur.fetchone()[0])
    cur.execute("INSERT INTO suggestions (episode_id, candidate_name, first_name, last_name, source, status) "
                "VALUES (%s, 'Cat Morehouse', 'Cat', 'Morehouse', 'desc_intro', 'pending') RETURNING suggestion_id",
                (eps[0],))
    sid = cur.fetchone()[0]
    db_conn.commit()
    return hid, eps, sid


def test_approve_as_existing_person_adds_alias_and_links(db_conn, monkeypatch):
    import main
    hid, eps, sid = _setup(db_conn)
    out = _run(db_conn, monkeypatch, 'approve_suggestion', sid, main.NameOverrideRequest(name='Catherine Morehouse'))
    assert out['host_id'] == hid and out['alias_added'] == 'Cat Morehouse'
    cur = db_conn.cursor()
    cur.execute("SELECT host_id, alias_name FROM host_aliases")
    assert cur.fetchall() == [(hid, 'Cat Morehouse')]
    cur.execute("SELECT episode_id FROM episode_host WHERE host_id = %s ORDER BY episode_id", (hid,))
    assert [r[0] for r in cur.fetchall()] == eps


def test_same_name_adds_no_alias(db_conn, monkeypatch):
    import main
    hid, eps, sid = _setup(db_conn)
    cur = db_conn.cursor()
    cur.execute("UPDATE suggestions SET candidate_name = 'Catherine Morehouse'")
    db_conn.commit()
    out = _run(db_conn, monkeypatch, 'approve_suggestion', sid, main.NameOverrideRequest(name='Catherine Morehouse'))
    assert out['alias_added'] is None
    cur.execute("SELECT count(*) FROM host_aliases")
    assert cur.fetchone()[0] == 0


def test_news_show_descriptions_are_not_searched(db_conn, monkeypatch):
    """On a show with description scanning off, only a title match links."""
    import main
    hid, eps, sid = _setup(db_conn, scan_descriptions=False)
    _run(db_conn, monkeypatch, 'approve_suggestion_only', sid, main.NameOverrideRequest(name='Catherine Morehouse'))
    cur = db_conn.cursor()
    cur.execute("SELECT episode_id FROM episode_host WHERE host_id = %s", (hid,))
    assert [r[0] for r in cur.fetchall()] == [eps[2]]
