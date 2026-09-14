"""
Shared fixtures for the scraper test suite.

DB-backed tests run against a disposable local Postgres database
(`podcast_scanner_test`), never against `podcast_db`. Create it once with:

    createdb -h localhost podcast_scanner_test
    psql -h localhost -d podcast_scanner_test -f podcast-schema.sql
    for f in migrate_add_*.sql; do psql -h localhost -d podcast_scanner_test -f "$f"; done

Tests that need it are skipped automatically if the database doesn't exist,
so the pure-function suite still runs anywhere.
"""
import os
import sys

import psycopg2
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TEST_DB = os.getenv('SCANNER_TEST_DATABASE_URL', 'postgresql://localhost/podcast_scanner_test')

# Tables that fixtures/tests write to, in an order safe for TRUNCATE ... CASCADE.
_TABLES_TO_RESET = [
    'episode_host', 'host_podcast', 'suggestions', 'rejected_names',
    'host_aliases', 'not_duplicate_pairs', 'episodes', 'hosts', 'podcasts',
    'channels',
]


def _db_available() -> bool:
    try:
        conn = psycopg2.connect(TEST_DB)
        conn.close()
        return True
    except psycopg2.OperationalError:
        return False


@pytest.fixture()
def db_conn():
    if not _db_available():
        pytest.skip(f"test database not available at {TEST_DB} — see tests/conftest.py")
    conn = psycopg2.connect(TEST_DB)
    cur = conn.cursor()
    cur.execute(f"TRUNCATE {', '.join(_TABLES_TO_RESET)} RESTART IDENTITY CASCADE")
    conn.commit()
    cur.close()
    yield conn
    conn.close()
