"""
Profile photos we store ourselves (host_photos, migrate_add_host_photos.sql).

The image proxy only fetches from a few hosts (Apple, X, Bluesky,
Wikimedia). Approved headshots from anywhere else — episode artwork on a
podcast CDN, an organisation's team page — are resized and kept in the
database, and the person's profile_image_url becomes this backend's
/api/people/{id}/photo address. Every page already sends a full URL through
the proxy, which serves these from the database (no fetch, no allowlist
change), so nothing in the frontend needs to know.
"""
import io
import os
import re
import time

# The backend's public address, written into profile_image_url. Change it
# (and rewrite the stored URLs) if the backend ever moves.
PUBLIC_BACKEND = os.getenv('PUBLIC_BACKEND_URL', 'https://podcast-network-backend.onrender.com').rstrip('/')
MAX_SIDE = 400
_PHOTO_PATH_RE = re.compile(r'^/api/people/(\d+)/photo$')


def photo_url(host_id: int, version: int | None = None) -> str:
    """The profile_image_url for a stored photo; ?v= changes on replacement
    so browsers and the proxy cache don't keep the old one."""
    return f"{PUBLIC_BACKEND}/api/people/{host_id}/photo?v={version or int(time.time())}"


def stored_photo_id(url: str) -> int | None:
    """The host_id if `url` is one of our own stored-photo addresses
    (any host, so a local or renamed backend still resolves)."""
    from urllib.parse import urlparse
    try:
        m = _PHOTO_PATH_RE.match(urlparse(url).path or '')
    except Exception:
        return None
    return int(m.group(1)) if m else None


def prepare(content: bytes, max_side: int = MAX_SIDE):
    """(bytes, content_type, width, height): the image upright, at most
    max_side pixels on its longer side, as WebP. Raises ValueError for
    anything that isn't a readable raster image or is too small to be a photo."""
    from PIL import Image, ImageOps
    try:
        img = Image.open(io.BytesIO(content))
        img.load()
    except Exception as e:
        raise ValueError(f'not an image: {e}')
    img = ImageOps.exif_transpose(img)
    if min(img.size) < 64:
        raise ValueError(f'too small: {img.size}')
    img.thumbnail((max_side, max_side))
    if img.mode not in ('RGB', 'RGBA'):
        img = img.convert('RGBA' if 'transparency' in img.info or img.mode in ('LA', 'P') else 'RGB')
    out = io.BytesIO()
    img.save(out, 'WEBP', quality=84)
    return out.getvalue(), 'image/webp', img.width, img.height


def store(cur, host_id: int, content: bytes, source: str, source_url: str | None = None,
          replace_existing: bool = False) -> str | None:
    """Keep a photo for host_id and point profile_image_url at it. Only fills
    a missing profile_image_url unless replace_existing. Returns the new URL,
    or None when nothing changed. Raises ValueError for a bad image."""
    data, ctype, w, h = prepare(content)
    if not replace_existing:
        cur.execute("SELECT profile_image_url FROM hosts WHERE host_id = %s", (host_id,))
        row = cur.fetchone()
        current = row['profile_image_url'] if isinstance(row, dict) else (row[0] if row else None)
        if row is None or current:
            return None
    cur.execute("""
        INSERT INTO host_photos (host_id, image, content_type, width, height, source, source_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (host_id) DO UPDATE SET image = EXCLUDED.image, content_type = EXCLUDED.content_type,
            width = EXCLUDED.width, height = EXCLUDED.height, source = EXCLUDED.source,
            source_url = EXCLUDED.source_url, updated_at = now()
    """, (host_id, data, ctype, w, h, source, source_url))
    url = photo_url(host_id)
    cur.execute("""
        UPDATE hosts SET profile_image_url = %s,
               field_sources = COALESCE(field_sources, '{}'::jsonb) || jsonb_build_object('profile_image_url', %s::text)
        WHERE host_id = %s
    """, (url, source, host_id))
    return url
