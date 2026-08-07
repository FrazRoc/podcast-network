"""
apple_credits_scraper.py

Scrapes the "Hosts & Guests" section from Apple Podcasts episode pages
and stores credits in the episode_host and hosts tables.

Apple Podcasts episode pages are server-rendered static HTML — no headless
browser required. The Hosts & Guests section is consistently structured
but coverage varies: some episodes have full credits, others only list the
host, and older episodes may have none at all.

URL pattern:
    https://podcasts.apple.com/us/podcast/id{apple_podcast_id}?i={apple_episode_id}
"""

import psycopg2
import requests
import time
import logging
import re
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'en-US,en;q=0.9',
}

# Be polite — 1 second between requests
REQUEST_DELAY = 2.0


class AppleCreditsScraper:

    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string

    def _get_connection(self):
        return psycopg2.connect(self.db_connection_string)

    # ------------------------------------------------------------------
    # FETCHING
    # ------------------------------------------------------------------

    def fetch_episode_page(self, apple_podcast_id: str, apple_episode_id: str) -> Optional[str]:
        """Fetch the Apple Podcasts episode page HTML."""
        url = f"https://podcasts.apple.com/us/podcast/id{apple_podcast_id}?i={apple_episode_id}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 200:
                return resp.text
            else:
                logger.warning(f"HTTP {resp.status_code} for episode {apple_episode_id}")
                return None
        except Exception as e:
            logger.error(f"Request failed for episode {apple_episode_id}: {e}")
            return None

    # ------------------------------------------------------------------
    # PARSING
    # ------------------------------------------------------------------

    def parse_credits(self, html: str) -> list[dict]:
        """
        Parse the Hosts & Guests section from an Apple Podcasts episode page.

        Returns a list of dicts:
            [{'name': str, 'role': str, 'is_guest': bool, 'image_url': str|None}]

        Structure per list item:
          - Name is in <h3 data-testid="ellipse-lockup__title">
          - Role is in <p><span class="subtitle-entry">Host|Guest</span></p>
          - Real photo: <picture><source srcset="..."> — take largest from srcset
          - No photo: <svg class="monogram"> with initials — image_url = None
        """
        soup = BeautifulSoup(html, 'html.parser')
        credits = []

        # Find the Hosts & Guests section by aria-label
        section = soup.find('div', attrs={'aria-label': 'Hosts & Guests'})
        if not section:
            return credits

        for li in section.find_all('li'):
            # Name
            name_tag = li.find('h3', attrs={'data-testid': 'ellipse-lockup__title'})
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name or len(name) < 2:
                continue

            # Role
            role_tag = li.find('span', class_='subtitle-entry')
            role = role_tag.get_text(strip=True).capitalize() if role_tag else 'Guest'
            is_guest = role.lower() != 'host'

            # Profile image — present if <picture> exists, absent if only <svg class="monogram">
            image_url = None
            picture = li.find('picture')
            if picture:
                # Prefer webp source, take largest size from srcset
                source = picture.find('source', attrs={'type': 'image/webp'})
                if not source:
                    source = picture.find('source')
                if source and source.get('srcset'):
                    entries = [e.strip() for e in source['srcset'].split(',')]
                    if entries:
                        image_url = entries[-1].split(' ')[0]

            credits.append({
                'name': name,
                'role': role,
                'is_guest': is_guest,
                'image_url': image_url,
            })

        return credits

    # ------------------------------------------------------------------
    # DATABASE
    # ------------------------------------------------------------------

    def get_or_create_host(self, conn, name: str, image_url: str = None,
                           data_source: str = 'apple_verified') -> int:
        """Get host_id by full name, or create a new host record.
        If image_url is provided and the host has no image yet, update it.
        data_source tracks where this record came from.
        """
        cur = conn.cursor()
        # Split name into first/last — handle multi-word names like "Bridget van Dorsten"
        # Rule: last word is last_name, everything before is first_name
        parts = name.strip().split(' ')
        first_name = ' '.join(parts[:-1]) if len(parts) > 1 else name
        last_name = parts[-1] if len(parts) > 1 else ''

        # Try to find existing
        cur.execute(
            "SELECT host_id, profile_image_url FROM hosts WHERE first_name = %s AND last_name = %s",
            (first_name, last_name)
        )
        row = cur.fetchone()
        if row:
            host_id, existing_image = row
            # Update image if we have one and they don't
            if image_url and not existing_image:
                cur.execute(
                    "UPDATE hosts SET profile_image_url = %s WHERE host_id = %s",
                    (image_url, host_id)
                )
            return host_id

        # Create new
        cur.execute(
            """
            INSERT INTO hosts (first_name, last_name, profile_image_url, data_source, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (first_name, last_name) DO UPDATE
                SET profile_image_url = COALESCE(EXCLUDED.profile_image_url, hosts.profile_image_url)
            RETURNING host_id
            """,
            (first_name, last_name, image_url, data_source)
        )
        return cur.fetchone()[0]

    def insert_episode_credit(self, conn, episode_id: int, host_id: int,
                               is_guest: bool, role: str,
                               data_source: str = 'apple_verified'):
        """Insert a credit into episode_host, ignoring duplicates."""
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (episode_id, host_id) DO UPDATE
                SET role = EXCLUDED.role,
                    is_guest = EXCLUDED.is_guest,
                    data_source = EXCLUDED.data_source
            """,
            (episode_id, host_id, is_guest, role, data_source)
        )

    # ------------------------------------------------------------------
    # MAIN PROCESSING
    # ------------------------------------------------------------------



    def scrape_show_hosts(self, podcast_title: str = None):
        """
        Scrape the Apple Podcasts show page for each podcast to find permanent hosts.
        Populates the hosts and host_podcast tables.

        Show page URL: https://podcasts.apple.com/us/podcast/id{apple_podcast_id}
        Same Hosts & Guests HTML structure as episode pages.
        Only people labelled "Host" are inserted into host_podcast.
        """
        conn = self._get_connection()
        cur = conn.cursor()

        # Get all podcasts with an Apple ID (optionally filtered)
        if podcast_title:
            cur.execute("""
                SELECT podcast_id, apple_podcast_id, title
                FROM podcasts
                WHERE apple_podcast_id IS NOT NULL AND title = %s
                ORDER BY title
            """, (podcast_title,))
        else:
            cur.execute("""
                SELECT podcast_id, apple_podcast_id, title
                FROM podcasts
                WHERE apple_podcast_id IS NOT NULL
                ORDER BY title
            """)
        podcasts = cur.fetchall()
        cur.close()

        logger.info(f"Scraping show host pages for {len(podcasts)} podcasts...")
        found = 0
        not_found = 0

        for podcast_id, apple_podcast_id, title in podcasts:
            try:
                url = f"https://podcasts.apple.com/us/podcast/id{apple_podcast_id}"
                resp = requests.get(url, headers=HEADERS, timeout=20)

                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code} for show: {title}")
                    not_found += 1
                    time.sleep(REQUEST_DELAY)
                    continue

                soup = BeautifulSoup(resp.text, 'html.parser')
                section = soup.find('div', attrs={'aria-label': 'Hosts & Guests'})

                if not section:
                    logger.info(f"⚪ No hosts section: {title}")
                    not_found += 1
                    time.sleep(REQUEST_DELAY)
                    continue

                hosts_found = 0
                for li in section.find_all('li'):
                    name_tag = li.find('h3', attrs={'data-testid': 'ellipse-lockup__title'})
                    role_tag = li.find('span', class_='subtitle-entry')

                    if not name_tag:
                        continue

                    name = name_tag.get_text(strip=True)
                    role = role_tag.get_text(strip=True).capitalize() if role_tag else ''

                    # Only process permanent hosts, not guests
                    if role.lower() != 'host':
                        continue

                    # Get profile image if available
                    image_url = None
                    picture = li.find('picture')
                    if picture:
                        source = picture.find('source', attrs={'type': 'image/webp'})
                        if not source:
                            source = picture.find('source')
                        if source and source.get('srcset'):
                            entries = [e.strip() for e in source['srcset'].split(',')]
                            if entries:
                                image_url = entries[-1].split(' ')[0]

                    # Upsert into hosts table
                    host_id = self.get_or_create_host(conn, name, image_url)

                    # Insert into host_podcast
                    cur2 = conn.cursor()
                    cur2.execute("""
                        INSERT INTO host_podcast (host_id, podcast_id, role, data_source)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (host_id, podcast_id) DO UPDATE
                            SET role = EXCLUDED.role,
                                data_source = EXCLUDED.data_source
                    """, (host_id, podcast_id, 'Host', 'apple_verified'))
                    cur2.close()
                    hosts_found += 1

                conn.commit()

                if hosts_found:
                    logger.info(f"✅ {title} → {hosts_found} host(s)")
                    found += 1
                else:
                    logger.info(f"⚪ {title} → no hosts labelled")
                    not_found += 1

                time.sleep(REQUEST_DELAY)

            except Exception as e:
                logger.error(f"Error scraping show {title}: {e}")
                conn.rollback()
                time.sleep(REQUEST_DELAY)

        conn.close()
        logger.info(f"\nShow host scrape complete: {found} shows with hosts, {not_found} without")
        return {'with_hosts': found, 'without_hosts': not_found}

    def backfill_images(self, batch_size: int = 200):
        """
        Re-fetch episode pages for hosts who are missing profile images,
        to pick up images that weren't captured in earlier runs.
        Only fetches pages for episodes that already have credits stored.
        """
        conn = self._get_connection()
        cur = conn.cursor()

        # Find episodes that have credits but where at least one person has no image
        cur.execute("""
            SELECT DISTINCT e.episode_id, e.apple_episode_id, e.title,
                   p.apple_podcast_id, p.title as podcast_title
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            JOIN episode_host eh ON eh.episode_id = e.episode_id
            JOIN hosts h ON h.host_id = eh.host_id
            WHERE e.apple_episode_id IS NOT NULL
              AND h.profile_image_url IS NULL
            ORDER BY e.published_date DESC
            LIMIT %s
        """, (batch_size,))

        episodes = cur.fetchall()
        cur.close()

        if not episodes:
            logger.info("Backfill: no episodes need image updates")
            return

        logger.info(f"Backfill: checking {len(episodes)} episodes for missing images...")
        updated = 0

        for episode_id, apple_episode_id, title, apple_podcast_id, podcast_title in episodes:
            try:
                html = self.fetch_episode_page(apple_podcast_id, apple_episode_id)
                if not html:
                    time.sleep(REQUEST_DELAY)
                    continue

                credits = self.parse_credits(html)
                for credit in credits:
                    if credit.get('image_url'):
                        self.get_or_create_host(conn, credit['name'], credit['image_url'])
                        updated += 1

                conn.commit()
                time.sleep(REQUEST_DELAY)

            except Exception as e:
                logger.error(f"Backfill error for {apple_episode_id}: {e}")
                conn.rollback()
                time.sleep(REQUEST_DELAY)

        conn.close()
        logger.info(f"Backfill complete: {updated} image(s) updated")
        return updated

    def process_episodes(self, batch_size: int = 50, podcast_title: str = None, hosts_only: bool = False):
        """
        Process episodes that don't yet have credits scraped.

        Args:
            batch_size: how many episodes to process per run
            podcast_title: if set, only process episodes from this podcast
            hosts_only: if True, skip shows with no entry in host_podcast
                        (shows Apple has no credits data for)
        """
        conn = self._get_connection()
        cur = conn.cursor()

        # Find episodes without any credits yet
        podcast_filter = ""
        host_filter = ""
        params = []
        if podcast_title:
            podcast_filter = "AND p.title = %s"
            params.append(podcast_title)
        if hosts_only:
            host_filter = """AND EXISTS (
                  SELECT 1 FROM host_podcast hp
                  WHERE hp.podcast_id = p.podcast_id
                    AND hp.data_source = 'apple_verified'
              )"""

        cur.execute(
            f"""
            SELECT e.episode_id, e.apple_episode_id, e.title,
                   p.apple_podcast_id, p.title as podcast_title
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE e.apple_episode_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM episode_host eh WHERE eh.episode_id = e.episode_id
              )
              {podcast_filter}
              {host_filter}
            ORDER BY e.published_date DESC
            LIMIT %s
            """,
            params + [batch_size]
        )

        episodes = cur.fetchall()
        cur.close()

        if not episodes:
            logger.info("No episodes to process — all caught up!")
            return {'processed': 0, 'with_credits': 0, 'no_credits': 0, 'failed': 0}

        logger.info(f"Processing {len(episodes)} episodes for credits...")

        processed = 0
        with_credits = 0
        no_credits = 0
        failed = 0

        for episode_id, apple_episode_id, title, apple_podcast_id, podcast_title in episodes:
            try:
                html = self.fetch_episode_page(apple_podcast_id, apple_episode_id)

                if html is None:
                    failed += 1
                    time.sleep(REQUEST_DELAY)
                    continue

                credits = self.parse_credits(html)

                if credits:
                    for credit in credits:
                        host_id = self.get_or_create_host(conn, credit['name'], credit.get('image_url'))
                        self.insert_episode_credit(
                            conn, episode_id, host_id,
                            credit['is_guest'], credit['role']
                        )
                    conn.commit()
                    with_credits += 1
                    logger.info(
                        f"✅ {podcast_title} | {title[:60]} "
                        f"→ {len(credits)} credit(s): "
                        f"{[c['name'] for c in credits]}"
                    )
                else:
                    no_credits += 1
                    logger.info(f"⚪ {podcast_title} | {title[:60]} → no credits listed")

                processed += 1
                time.sleep(REQUEST_DELAY)

            except Exception as e:
                logger.error(f"Error processing episode {apple_episode_id} ({title}): {e}")
                conn.rollback()
                failed += 1
                time.sleep(REQUEST_DELAY)

        conn.close()

        summary = {
            'processed': processed,
            'with_credits': with_credits,
            'no_credits': no_credits,
            'failed': failed,
        }
        logger.info(f"\nDone: {summary}")
        return summary


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------
if __name__ == '__main__':
    import json
    import argparse

    parser = argparse.ArgumentParser(
        description='Apple Podcasts credits scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
commands:
  shows       Scrape show pages for permanent hosts (~2 min, 49 requests)
  episodes    Scrape episode pages for guest/host credits (~40 min, 2367 requests)
  backfill    Backfill missing profile images for existing hosts
  all         Run shows, then episodes, then backfill

examples:
  python3 apple_credits_scraper.py shows
  python3 apple_credits_scraper.py episodes
  python3 apple_credits_scraper.py episodes --batch 100
  python3 apple_credits_scraper.py episodes --podcast "Catalyst with Shayle Kann"
  python3 apple_credits_scraper.py backfill --batch 200
  python3 apple_credits_scraper.py all
        """
    )

    parser.add_argument('command', choices=['shows', 'episodes', 'backfill', 'all'],
                        help='What to scrape')
    parser.add_argument('--batch', type=int, default=2400,
                        help='Number of episodes to process (default: 2400)')
    parser.add_argument('--podcast', type=str, default=None,
                        help='Only process episodes from this podcast title')
    parser.add_argument('--db', type=str, default='postgresql://localhost/podcast_db',
                        help='Database connection string')
    parser.add_argument('--hosts-only', action='store_true', default=False,
                        help='Only scrape episodes from shows with Apple-verified host data (excludes itunes_artist sources)')

    args = parser.parse_args()
    scraper = AppleCreditsScraper(args.db)

    if args.command == 'shows':
        print("Scraping show pages for permanent hosts...")
        results = scraper.scrape_show_hosts(podcast_title=args.podcast)
        print(json.dumps(results, indent=2))

    elif args.command == 'episodes':
        print(f"Scraping episode credits (batch={args.batch}, podcast={args.podcast or 'all'})...")
        results = scraper.process_episodes(batch_size=args.batch, podcast_title=args.podcast, hosts_only=args.hosts_only)
        print(json.dumps(results, indent=2))

    elif args.command == 'backfill':
        print(f"Backfilling missing profile images (batch={args.batch})...")
        results = scraper.backfill_images(batch_size=args.batch)
        print(json.dumps({'images_updated': results}, indent=2))

    elif args.command == 'all':
        print("Step 1/3: Scraping show pages for permanent hosts...")
        r1 = scraper.scrape_show_hosts()
        print(json.dumps(r1, indent=2))

        print("\nStep 2/3: Scraping episode credits...")
        r2 = scraper.process_episodes(batch_size=args.batch)
        print(json.dumps(r2, indent=2))

        print("\nStep 3/3: Backfilling missing profile images...")
        r3 = scraper.backfill_images(batch_size=500)
        print(json.dumps({'images_updated': r3}, indent=2))
