"""
image_enricher.py

Finds profile images for hosts missing a profile_image_url.
Tries sources in order: Wikipedia → Google Knowledge Graph → Twitter/unavatar

Results go into the image_suggestions table for human review via /admin/images.

Usage:
    python3 image_enricher.py run              # search all hosts missing images
    python3 image_enricher.py run --limit 50   # limit number of people to search
    python3 image_enricher.py run --source wikipedia  # only try one source
    python3 image_enricher.py status           # show coverage stats

Requirements:
    pip install requests
    
    For Google Knowledge Graph (optional):
    Set GOOGLE_KG_API_KEY in .env or environment
    Get a free key at: https://console.cloud.google.com/apis/library/kgsearch.googleapis.com
    Free tier: 100 requests/day, no billing required
"""

import psycopg2
import requests
import time
import logging
import argparse
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB = 'postgresql://localhost/podcast_db'
REQUEST_DELAY = 1.0

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

HEADERS = {
    'User-Agent': 'ColoradoCurrent/1.0 (podcast network research tool; contact@coloradocurrent.com)'
}


# ------------------------------------------------------------------
# IMAGE SOURCES
# ------------------------------------------------------------------

def search_wikipedia(name: str) -> Optional[dict]:
    """
    Search Wikipedia for a person by name, return image URL if found.
    Only returns results where the page title closely matches the person's name
    to avoid false positives like climate charts or organization pages.
    """
    try:
        name_parts = name.lower().split()

        # Search for the person
        search_resp = requests.get(
            'https://en.wikipedia.org/w/api.php',
            params={
                'action': 'query',
                'list': 'search',
                'srsearch': name,
                'srnamespace': 0,
                'srlimit': 5,
                'format': 'json',
            },
            headers=HEADERS,
            timeout=10
        )
        search_data = search_resp.json()
        results = search_data.get('query', {}).get('search', [])
        if not results:
            return None

        # Build candidate page titles — prefer exact name match over disambiguation
        last_name = name_parts[-1] if name_parts else ''
        first_name = name_parts[0] if name_parts else ''
        candidates = []
        for result in results:
            title = result['title']
            title_lower = title.lower()
            if last_name not in title_lower:
                continue
            if len(title) > 60:
                continue
            # Prefer titles that are exactly the name (no disambiguation suffix)
            if title.lower() == name.lower():
                candidates.insert(0, title)  # put exact match first
            else:
                candidates.append(title)

        if not candidates:
            return None

        # Try each candidate until we find one with a person photo
        for page_title in candidates:
            thumb_resp = requests.get(
                'https://en.wikipedia.org/w/api.php',
                params={
                    'action': 'query',
                    'titles': page_title,
                    'prop': 'pageimages|info|categories',
                    'piprop': 'thumbnail',
                    'pithumbsize': 400,
                    'inprop': 'url',
                    'cllimit': 20,
                    'format': 'json',
                },
                headers=HEADERS,
                timeout=10
            )
            thumb_data = thumb_resp.json()
            pages = thumb_data.get('query', {}).get('pages', {})

            for page in pages.values():
                thumbnail = page.get('thumbnail', {})
                if not thumbnail.get('source'):
                    continue  # no image on this page, try next candidate

                # Check it's a person page via categories
                categories = [c['title'].lower() for c in page.get('categories', [])]
                is_person = any(
                    'births' in c or 'deaths' in c or 'living people' in c
                    for c in categories
                )
                if not is_person:
                    continue  # has image but not a person, try next

                return {
                    'image_url': thumbnail['source'],
                    'source': 'wikipedia',
                    'source_url': page.get('fullurl', f'https://en.wikipedia.org/wiki/{page_title}'),
                }
            time.sleep(0.3)

        return None

    except Exception as e:
        logger.warning(f"Wikipedia error for {name}: {e}")
        return None


def search_twitter(name: str) -> Optional[dict]:
    """
    Try to find a Twitter profile image via unavatar.io.
    Tries common handle formats and checks if the image returns successfully.
    No API key required.
    """
    # Generate candidate handles
    parts = name.lower().split()
    if len(parts) < 2:
        return None

    first, last = parts[0], parts[-1]
    candidates = [
        f'{first}{last}',
        f'{first}_{last}',
        f'{first}.{last}',
        f'{last}{first}',
        f'{first}{last[0]}',  # e.g. jigarS
    ]

    for handle in candidates:
        try:
            # unavatar.io fetches the Twitter avatar without requiring API access
            url = f'https://unavatar.io/twitter/{handle}?json'
            resp = requests.get(url, headers=HEADERS, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                img_url = data.get('url', '')
                # Skip the default Twitter egg/placeholder
                if img_url and 'default_profile' not in img_url and 'abs.twimg.com/sticky' not in img_url:
                    return {
                        'image_url': img_url,
                        'source': 'twitter',
                        'source_url': f'https://x.com/{handle}',
                    }
            time.sleep(0.3)
        except Exception:
            continue

    return None


# ------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------

def get_hosts_missing_images(conn, limit: int = None) -> list[dict]:
    """Return hosts without a profile image, ordered by most episode appearances."""
    cur = conn.cursor()
    query = """
        SELECT h.host_id,
               h.first_name || ' ' || h.last_name AS full_name,
               COUNT(DISTINCT eh.episode_id) AS appearances,
               ARRAY_AGG(DISTINCT p.title ORDER BY p.title) AS podcasts
        FROM hosts h
        LEFT JOIN episode_host eh ON h.host_id = eh.host_id
        LEFT JOIN episodes e ON eh.episode_id = e.episode_id
        LEFT JOIN podcasts p ON e.podcast_id = p.podcast_id
        WHERE h.profile_image_url IS NULL
          -- Skip if we already have pending/approved suggestions for this host
          AND NOT EXISTS (
              SELECT 1 FROM image_suggestions ims
              WHERE ims.host_id = h.host_id
                AND ims.status IN ('pending', 'approved')
          )
        GROUP BY h.host_id, h.first_name, h.last_name
        ORDER BY appearances DESC NULLS LAST
    """
    if limit:
        query += f' LIMIT {limit}'
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return [
        {'host_id': r[0], 'full_name': r[1], 'appearances': r[2], 'podcasts': r[3] or []}
        for r in rows
    ]


def save_suggestion(conn, host_id: int, result: dict) -> bool:
    """Save an image suggestion to the DB. Returns True if inserted."""
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO image_suggestions (host_id, image_url, source, source_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (host_id, image_url) DO NOTHING
        """, (host_id, result['image_url'], result['source'], result.get('source_url')))
        inserted = cur.rowcount > 0
        conn.commit()
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving suggestion: {e}")
        return False
    finally:
        cur.close()


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------

def run(limit: int = None, source_filter: str = None):
    conn = psycopg2.connect(DB)
    hosts = get_hosts_missing_images(conn, limit=limit)

    if not hosts:
        logger.info("No hosts missing images (or all already have pending suggestions)")
        conn.close()
        return

    logger.info(f"Searching for images for {len(hosts)} people...")

    found = 0
    not_found = 0

    sources = []
    if source_filter:
        sources = [source_filter]
    else:
        sources = ['wikipedia', 'twitter']

    for host in hosts:
        host_id   = host['host_id']
        name      = host['full_name']
        appear    = host['appearances'] or 0
        podcasts  = ', '.join(host['podcasts'][:2])

        result = None

        if 'wikipedia' in sources:
            result = search_wikipedia(name)
            time.sleep(REQUEST_DELAY)

        if not result and 'twitter' in sources:
            result = search_twitter(name)
            time.sleep(REQUEST_DELAY)

        if result:
            saved = save_suggestion(conn, host_id, result)
            if saved:
                logger.info(f"✅ {name} ({appear} eps, {podcasts}) → {result['source']}: {result['image_url'][:60]}")
                found += 1
            else:
                logger.info(f"⏭  {name} → already in queue")
        else:
            logger.info(f"⚪ {name} ({appear} eps) → no image found")
            not_found += 1

    conn.close()

    print(f"\nDone: {found} images found, {not_found} not found")

    # Show queue stats
    conn2 = psycopg2.connect(DB)
    cur = conn2.cursor()
    cur.execute("SELECT COUNT(*) FROM image_suggestions WHERE status = 'pending'")
    pending = cur.fetchone()[0]
    cur.close()
    conn2.close()
    print(f"Total pending image review: {pending}")


def status():
    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM hosts) as total,
            (SELECT COUNT(*) FROM hosts WHERE profile_image_url IS NOT NULL) as with_image,
            (SELECT COUNT(*) FROM image_suggestions WHERE status = 'pending') as pending_review,
            (SELECT COUNT(*) FROM image_suggestions WHERE status = 'approved') as approved,
            (SELECT COUNT(*) FROM image_suggestions WHERE status = 'rejected') as rejected
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()

    print(f"\nImage Coverage:")
    print(f"  Total people:      {row[0]}")
    print(f"  With image:        {row[1]} ({round(row[1]/row[0]*100) if row[0] else 0}%)")
    print(f"  Missing image:     {row[0] - row[1]}")
    print(f"\nImage Suggestions Queue:")
    print(f"  Pending review:    {row[2]}")
    print(f"  Approved:          {row[3]}")
    print(f"  Rejected:          {row[4]}")


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Find profile images for hosts missing photos',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
sources tried in order: wikipedia → twitter

examples:
  python3 image_enricher.py status
  python3 image_enricher.py run
  python3 image_enricher.py run --limit 50
  python3 image_enricher.py run --source wikipedia
  python3 image_enricher.py run --source twitter
        """
    )
    parser.add_argument('command', choices=['run', 'status'])
    parser.add_argument('--limit', type=int, default=None,
                        help='Max number of people to search')
    parser.add_argument('--source', choices=['wikipedia', 'twitter'], default=None,
                        help='Only try this source')
    args = parser.parse_args()

    if args.command == 'status':
        status()
    elif args.command == 'run':
        run(limit=args.limit, source_filter=args.source)
