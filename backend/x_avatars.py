"""
X (Twitter) profile pictures, looked up once and stored as their own
pbs.twimg.com URLs.

They used to be stored as unavatar.io/twitter/<handle> links, which fetch
the picture on every page view through unavatar's anonymous daily quota;
from Render's shared IPs that quota ran out and the photos broke. A stored
pbs.twimg.com URL needs nothing at view time, but stops working when the
person changes their picture: image_enricher.py's refresh-x command
re-resolves those weekly.

The lookup goes through api.fxtwitter.com (free, no account; it rate-limits
quick repeated calls, so batch callers pause between handles). Standard
library only, so the backend and the scraper can both import it.
"""
import json
import re
import urllib.error
import urllib.request

FX_API = 'https://api.fxtwitter.com/'
_SIZE_SUFFIX_RE = re.compile(r'_(?:normal|bigger|mini|200x200|400x400)(\.\w+)$')


def avatar_from_fx(payload: dict):
    """The 400x400 picture URL from an api.fxtwitter.com user response, or
    None: no such account, or the default "egg" picture."""
    user = (payload or {}).get('user') or {}
    avatar = user.get('avatar_url') or ''
    if not avatar or 'default_profile' in avatar:
        return None
    return _SIZE_SUFFIX_RE.sub(r'_400x400\1', avatar)


def lookup(handle: str, timeout: float = 10):
    """(status, url) for an X handle. status: 'ok' (url set), 'missing' (no
    such account, or no picture), 'rate_limited' or 'error' (try later).
    'missing' isn't proof: the service sometimes says "User not found" for
    real accounts when called too often, so never delete anything on it."""
    if not handle:
        return 'missing', None
    req = urllib.request.Request(FX_API + handle, headers={'User-Agent': 'podcast-network avatar lookup'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return 'missing', None
        return ('rate_limited' if e.code == 429 else 'error'), None
    except Exception:
        return 'error', None
    url = avatar_from_fx(payload)
    return ('ok', url) if url else ('missing', None)


def x_avatar_url(handle: str):
    """Just the URL, or None for any failure."""
    return lookup(handle)[1]


def is_x_avatar(url: str) -> bool:
    return bool(url) and url.startswith('https://pbs.twimg.com/profile_images/')
