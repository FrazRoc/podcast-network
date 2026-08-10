"""
episode_name_scanner.py

Scans episode titles and descriptions for names of people already in our hosts table.
Only matches known people — never creates new host records.
Excludes matches where the person is already the host of that show.
Strips boilerplate credits/footer text from descriptions before scanning.

Data source: 'parsed_title' or 'parsed_desc'

Usage:
    python3 episode_name_scanner.py dry-run              # preview all matches
    python3 episode_name_scanner.py dry-run --title-only # titles only (safer)
    python3 episode_name_scanner.py run --title-only     # insert title matches
    python3 episode_name_scanner.py run                  # insert all matches

modes:
  --title-only    Scan episode titles only (high confidence, run first)
  (no flag)       Scan both titles AND descriptions (broader, more matches)

recommended workflow:
  1. python3 episode_name_scanner.py dry-run --title-only
  2. python3 episode_name_scanner.py run --title-only
  3. python3 episode_name_scanner.py dry-run
  4. python3 episode_name_scanner.py run
"""

import psycopg2
import re
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB = 'postgresql://localhost/podcast_db'

# Shows to skip for description scanning — their descriptions contain
# news reporting, staff bios, or other non-guest name mentions
DESC_SCAN_SKIP_SHOWS = {
    'POLITICO Energy',  # journalist bylines and politician news coverage
}


# ------------------------------------------------------------------
# DESCRIPTION CLEANING
# Strip boilerplate footers before scanning so we don't match
# production staff names (credits, POLITICO staff bios, etc.)
# ------------------------------------------------------------------

STRIP_AFTER_PATTERNS = [
    r'\nCredits:',                                      # Latitude Media credits footer
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the co-host',      # POLITICO co-host bio
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the host',          # show host bio
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the .{0,30} editor',    # editor bios
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the .{0,30} producer',  # producer bios
    r'\nFollow the show on',                            # POLITICO follow footer
    r'\nFor more reporting',                            # POLITICO newsletter footer
    r'\nOur theme music',                               # POLITICO music credit
    r'\nSubscribe to',                                  # subscription CTAs
    r'Follow our co-hosts and production team',         # Post Script Audio / A Matter of Degrees
    r'\nSee Privacy Policy',                            # Art19 / Wood Mackenzie footer
    r'is produced by Columbia University',              # The Big Switch producer credit
]


def clean_description(text: str) -> str:
    """
    Strip boilerplate credits/staff bio footers from episode descriptions
    before scanning for guest names.
    """
    if not text:
        return ''
    for pattern in STRIP_AFTER_PATTERNS:
        match = re.search(pattern, text)
        if match:
            text = text[:match.start()]
    return text.strip()


# ------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------

def get_hosts(conn) -> list[dict]:
    """Load all hosts from DB, longest names first to avoid partial matches."""
    cur = conn.cursor()
    cur.execute("""
        SELECT host_id, first_name, last_name,
               first_name || ' ' || last_name AS full_name
        FROM hosts
        ORDER BY LENGTH(first_name || last_name) DESC
    """)
    rows = cur.fetchall()
    cur.close()
    return [
        {'host_id': r[0], 'first_name': r[1], 'last_name': r[2], 'full_name': r[3]}
        for r in rows
    ]


def get_uncredited_episodes(conn) -> list[dict]:
    """Load all episodes without any credits."""
    cur = conn.cursor()
    cur.execute("""
        SELECT e.episode_id, e.title, e.description,
               p.podcast_id, p.title AS podcast_title
        FROM episodes e
        JOIN podcasts p ON e.podcast_id = p.podcast_id
        WHERE NOT EXISTS (
            SELECT 1 FROM episode_host eh WHERE eh.episode_id = e.episode_id
        )
        ORDER BY p.title, e.published_date DESC
    """)
    rows = cur.fetchall()
    cur.close()
    return [
        {
            'episode_id': r[0], 'title': r[1], 'description': r[2],
            'podcast_id': r[3], 'podcast_title': r[4]
        }
        for r in rows
    ]


def get_show_hosts(conn) -> dict:
    """Return dict of podcast_id -> set of host_ids to exclude own-show matches."""
    cur = conn.cursor()
    cur.execute("SELECT podcast_id, host_id FROM host_podcast")
    result = {}
    for podcast_id, host_id in cur.fetchall():
        result.setdefault(podcast_id, set()).add(host_id)
    cur.close()
    return result


# ------------------------------------------------------------------
# SCANNING
# ------------------------------------------------------------------

def name_in_text(full_name: str, text: str) -> bool:
    """Case-insensitive whole-name match."""
    return full_name.lower() in text.lower()


def run(dry_run: bool = True, title_only: bool = False, min_length: int = 7):
    conn = psycopg2.connect(DB)

    hosts    = get_hosts(conn)
    episodes = get_uncredited_episodes(conn)
    show_hosts = get_show_hosts(conn)

    logger.info(f"Scanning {len(episodes)} uncredited episodes against {len(hosts)} known people...")

    matches = []

    for episode in episodes:
        episode_id    = episode['episode_id']
        podcast_id    = episode['podcast_id']
        podcast_title = episode['podcast_title']
        title         = episode['title'] or ''
        description   = episode['description'] or ''
        show_host_ids = show_hosts.get(podcast_id, set())

        # Clean description once per episode
        clean_desc = clean_description(description) if not title_only else ''

        for host in hosts:
            host_id   = host['host_id']
            full_name = host['full_name']

            # Skip show's own hosts
            if host_id in show_host_ids:
                continue

            # Skip very short names
            if len(full_name) < min_length:
                continue

            # Title check (high confidence)
            if name_in_text(full_name, title):
                matches.append({
                    'episode_id': episode_id, 'host_id': host_id,
                    'full_name': full_name, 'podcast_title': podcast_title,
                    'episode_title': title, 'source': 'parsed_title',
                })
                continue  # don't double-match from description

            # Description check (lower confidence, cleaned)
            # Skip shows where descriptions contain news reporting rather than guest info
            if not title_only and clean_desc and podcast_title not in DESC_SCAN_SKIP_SHOWS and name_in_text(full_name, clean_desc):
                matches.append({
                    'episode_id': episode_id, 'host_id': host_id,
                    'full_name': full_name, 'podcast_title': podcast_title,
                    'episode_title': title, 'source': 'parsed_desc',
                })

    logger.info(f"Found {len(matches)} matches")

    if not matches:
        print("No matches found.")
        conn.close()
        return

    # Print summary grouped by person
    from collections import defaultdict
    by_person = defaultdict(list)
    for m in matches:
        by_person[m['full_name']].append(m)

    print(f"\n{'DRY RUN — ' if dry_run else ''}Found {len(matches)} matches across {len(by_person)} people:\n")

    for name, person_matches in sorted(by_person.items(), key=lambda x: -len(x[1])):
        shows = set(m['podcast_title'] for m in person_matches)
        print(f"\n  {name} ({len(person_matches)} episodes across {len(shows)} show(s)):")
        for m in person_matches[:5]:
            print(f"    [{m['source']}] {m['podcast_title']}: {m['episode_title'][:70]}")
        if len(person_matches) > 5:
            print(f"    ... and {len(person_matches) - 5} more")

    if dry_run:
        print(f"\nDry run complete. Run with 'run' to insert {len(matches)} credits into DB.")
        conn.close()
        return

    # Insert
    cur = conn.cursor()
    inserted = 0
    skipped  = 0

    for m in matches:
        try:
            cur.execute(
                """
                INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
                VALUES (%s, %s, true, 'Guest', %s)
                ON CONFLICT (episode_id, host_id) DO NOTHING
                """,
                (m['episode_id'], m['host_id'], m['source'])
            )
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            logger.error(f"Error inserting {m['full_name']} on episode {m['episode_id']}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nDone: {inserted} credits inserted, {skipped} already existed")


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Scan episode titles/descriptions for known host/guest names',
        epilog="""
modes:
  --title-only    Scan episode titles only (high confidence, run first)
  (no flag)       Scan both titles AND descriptions (broader, more matches)

examples:
  python3 episode_name_scanner.py dry-run --title-only
  python3 episode_name_scanner.py dry-run --title-only --min-length 10
  python3 episode_name_scanner.py run --title-only
  python3 episode_name_scanner.py dry-run
  python3 episode_name_scanner.py run
        """
    )
    parser.add_argument('command', choices=['dry-run', 'run'])
    parser.add_argument('--title-only', action='store_true', default=False,
                        help='Only scan episode titles, not descriptions')
    parser.add_argument('--min-length', type=int, default=7,
                        help='Minimum full name length to match (default: 7)')
    args = parser.parse_args()

    run(
        dry_run=(args.command == 'dry-run'),
        title_only=args.title_only,
        min_length=args.min_length,
    )
