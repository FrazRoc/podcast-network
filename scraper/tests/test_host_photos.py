"""Profile photos we store ourselves (backend/host_photos.py)."""
import asyncio
import io
import os
import sys

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))
PIL = pytest.importorskip('PIL')
from PIL import Image  # noqa: E402

import host_photos  # noqa: E402


def _jpeg(w=800, h=600, color=(200, 80, 40)):
    out = io.BytesIO()
    Image.new('RGB', (w, h), color).save(out, 'JPEG')
    return out.getvalue()


def test_prepare_shrinks_to_webp():
    data, ctype, w, h = host_photos.prepare(_jpeg(800, 600))
    assert ctype == 'image/webp' and (w, h) == (400, 300)
    assert Image.open(io.BytesIO(data)).format == 'WEBP'


def test_prepare_refuses_non_images_and_icons():
    with pytest.raises(ValueError):
        host_photos.prepare(b'<html>not an image</html>')
    with pytest.raises(ValueError):
        host_photos.prepare(_jpeg(40, 40))


def test_stored_photo_id():
    assert host_photos.stored_photo_id('https://x.onrender.com/api/people/42/photo?v=1') == 42
    assert host_photos.stored_photo_id('http://localhost:8000/api/people/7/photo') == 7
    assert host_photos.stored_photo_id('https://pbs.twimg.com/profile_images/1/a.jpg') is None
    assert host_photos.stored_photo_id('https://evil.example/api/people/7/photo/../x') is None


def _person(cur, first, last, image=None):
    cur.execute("INSERT INTO hosts (first_name, last_name, profile_image_url) VALUES (%s, %s, %s) RETURNING host_id",
                (first, last, image))
    return cur.fetchone()['host_id']


@pytest.fixture
def conn(db_conn):
    c = psycopg2.connect(db_conn.dsn, cursor_factory=RealDictCursor)
    yield c
    c.close()


def test_store_fills_only_a_missing_photo(conn):
    cur = conn.cursor()
    a = _person(cur, 'Ann', 'Lee')
    b = _person(cur, 'Bo', 'Diaz', 'https://pbs.twimg.com/profile_images/1/b_400x400.jpg')
    url = host_photos.store(cur, a, _jpeg(), 'episode_art', 'https://cdn.example/ep.jpg')
    assert host_photos.stored_photo_id(url) == a
    assert host_photos.store(cur, b, _jpeg(), 'episode_art') is None          # already has one
    cur.execute("SELECT profile_image_url, field_sources FROM hosts WHERE host_id = %s", (a,))
    row = cur.fetchone()
    assert row['profile_image_url'] == url and row['field_sources']['profile_image_url'] == 'episode_art'
    cur.execute("SELECT COUNT(*) AS n FROM host_photos")
    assert cur.fetchone()['n'] == 1


def test_endpoint_proxy_and_merge(conn, db_conn, monkeypatch):
    pytest.importorskip('fastapi')
    import main
    monkeypatch.setattr(main, 'get_db_connection', lambda: psycopg2.connect(db_conn.dsn, cursor_factory=RealDictCursor))
    cur = conn.cursor()
    keep = _person(cur, 'Ann', 'Lee')
    drop = _person(cur, 'Annie', 'Lee')
    url = host_photos.store(cur, drop, _jpeg(), 'team_page')
    conn.commit()

    r = asyncio.run(main.host_photo(drop))
    assert r.media_type == 'image/webp' and r.headers['access-control-allow-origin'] == '*'
    small = asyncio.run(main.proxy_image(url, w=64))          # the proxy serves it from the database
    assert Image.open(io.BytesIO(small.body)).width == 64
    with pytest.raises(main.HTTPException):
        asyncio.run(main.host_photo(keep))

    # Merging the record with the photo into one without: the photo follows.
    main.merge_people(keep, drop)
    cur.execute("SELECT profile_image_url FROM hosts WHERE host_id = %s", (keep,))
    assert host_photos.stored_photo_id(cur.fetchone()['profile_image_url']) == keep
    assert asyncio.run(main.host_photo(keep)).media_type == 'image/webp'
