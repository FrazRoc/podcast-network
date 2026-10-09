"""The graph and Stats responses are cached in the backend (site review
Oct 2026: the graph took 10 s per visit, the Stats page ~16 s)."""
import os
import sys

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))


@pytest.fixture
def client(db_conn, monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    import main
    calls = []

    def connect():
        calls.append(1)
        return psycopg2.connect(db_conn.dsn, cursor_factory=RealDictCursor)
    monkeypatch.setattr(main, 'get_db_connection', connect)
    main._STATS_CACHE.clear()
    cur = db_conn.cursor()
    cur.execute("INSERT INTO podcasts (apple_podcast_id, title) VALUES ('1', 'Grid Talk') RETURNING podcast_id")
    pid = cur.fetchone()[0]
    ids = []
    for first in ('Ann', 'Bo'):
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES (%s, 'X') RETURNING host_id", (first,))
        ids.append(cur.fetchone()[0])
    cur.execute("INSERT INTO episodes (podcast_id, title, published_date) VALUES (%s, 'Ep', '2025-01-01') RETURNING episode_id", (pid,))
    ep = cur.fetchone()[0]
    cur.execute("INSERT INTO episode_host (episode_id, host_id, is_guest) VALUES (%s, %s, false), (%s, %s, true)",
                (ep, ids[0], ep, ids[1]))
    db_conn.commit()
    return TestClient(main.app), calls, main


def test_graph_payload_is_cached(client):
    c, calls, main = client
    r = c.get('/api/host-connections')
    assert r.status_code == 200
    body = r.json()
    assert {n['name'] for n in body['nodes']} == {'Ann X', 'Bo X'}
    assert body['links'][0]['podcast'] == 'Grid Talk'
    assert r.headers['cache-control'] == 'public, max-age=300'
    n = len(calls)
    assert c.get('/api/host-connections').json() == body
    assert len(calls) == n            # served from the cache, no query
    assert c.get('/api/show-connections').status_code == 200


def test_stats_cached_until_an_admin_edit(client):
    c, calls, main = client
    first = c.get('/api/stats/guest-roles')
    assert first.status_code == 200
    n = len(calls)
    again = c.get('/api/stats/guest-roles')
    assert again.content == first.content and len(calls) == n
    assert again.headers['cache-control'] == 'public, max-age=300'
    c.get('/api/stats/guest-roles?x=1')
    assert len(calls) > n             # another query string is another entry
    main._STATS_CACHE.clear()         # what an admin edit does (middleware)
    c.get('/api/stats/guest-roles')
    assert len(calls) > n + 1
