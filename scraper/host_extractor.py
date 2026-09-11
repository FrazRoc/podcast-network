"""
host_extractor.py

Extracts host names from three sources, in priority order:
  1. iTunes artistName (stored as channel name) — e.g. "Nick Sorokin"
  2. Podcast title "with [Name]" pattern — e.g. "The Energy Transition Show with Chris Nelder"
  3. Show description patterns — e.g. "Hosted by Amy Westervelt"

Only processes podcasts without existing host_podcast entries.
Data source is stamped on all new records:
  'itunes_artist' — from channel name or title
  'parsed_desc'   — from description pattern matching

Usage:
    python3 host_extractor.py dry-run    # preview without saving
    python3 host_extractor.py run        # insert into DB
"""

import os
import psycopg2
import re
import argparse
import logging
from html.parser import HTMLParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB = os.getenv('DATABASE_URL', 'postgresql://localhost/podcast_db')


# ------------------------------------------------------------------
# BLOCKLIST — channel names that are companies/orgs, not people
# ------------------------------------------------------------------
NOT_A_PERSON = {
    # Publishers / production companies
    'paces', 'pushkin industries', '9to5mac', 'crux and latitude studios',
    'norton rose fulbright', 'hydrogen media ltd', 'carbon suckers media',
    'xe network', 'heatmap news', 'gridx and latitude studios',
    'latitude media', 'gridx', 'latitude studios', 'colorado hydrogen network',
    'energy impact center',

    # Organizations / institutions
    'harvard business school', 'harvard business school business & environment initiative',
    'der task force', 'climate one from the commonwealth club',

    # Companies
    'aurora energy research', 'h2tech', 'synapse', 'decarbonizing commerce',
    'energy central', 'factor this', 'raptor maps', 'silicon ranch',
    'smart energy decisions', 'solar power world', 'climate capital',
    'political climate',

    # Shows whose channel name matches their own title
    'cleantech talk', 'drilled',

    # Generic descriptions
    'the founders and futurists building the hard-tech frontier.',
}

# Words that disqualify a description candidate
NOT_A_PERSON_WORDS = {
    'ceo', 'llc', 'ltd', 'inc', 'corp', 'network', 'media', 'group',
    'institute', 'foundation', 'initiative', 'association', 'council',
    'university', 'school', 'department', 'division', 'team', 'staff',
    'newsroom', 'editorial', 'production', 'studio', 'studios',
    'climate', 'energy', 'clean', 'green', 'solar', 'power', 'carbon',
    'hydrogen', 'nuclear', 'tech', 'net', 'zero', 'podcast', 'show',
    'episode', 'weekly', 'season', 'series', 'each', 'every',
}

# Description patterns that introduce host names.
# Capture groups are intentionally wide to catch "Name1, Name2, and Name3" —
# split_names_desc() handles breaking them into individual names afterward.
HOST_PATTERNS = [
    # "Hosted by Alfred Johnson" / "Co-Hosted by Greg Dalton, Ariana Brocious and Kousha Navidar"
    r'[Cc]o-?[Hh]osted? by\s+([A-Z][^.\n]{3,80})',
    # "hosts Julia Pyper, Neil Chatterjee, and Brandon Hurlbut"
    r'\b[Hh]osts?\s+([A-Z][^.\n]{3,80})',
    # "Join Cody Simms each week"
    r'[Jj]oin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+each\s+week',
    # "Zach Shahan interviews"
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+interviews',
    # "with host Todd Alexander" / "with your host Keith Anderson"
    r'with\s+(?:your\s+)?host\s+([A-Z][^.\n]{3,80})',
    # "Dana Perkins and Tom Rowlands-Rees sit down"
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+(?:\s+and\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)?)\s+sit\s+down',
    # "led by Amy Westervelt"
    r'led by\s+([A-Z][^.\n]{3,80})',
    # "Tune in with hosts X, Y and Z"
    r'[Tt]une\s+in\s+.*?with\s+hosts?\s+([A-Z][^.\n]{3,80})',
    # "Smart Energy Decisions' Debra Chanil digs deep"
    r"[A-Z][^']{3,40}'s?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+(?:digs?|sits?|talks?|speaks?|explores?|brings?)",
    # "Dana Perkins and Tom Rowlands-Rees sit down" — explicitly handles hyphenated names
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+(?:\s+and\s+[A-Z][a-z]+(?:\s+[A-Z][a-zA-Z-]+)+)?)\s+sit\s+down',
    # "hosted by partner Todd Alexander" / "hosted by climate-tech founder and author Josh Dorfman"
    # Skips any lowercase title words between "hosted by" and the capitalized name
    r'[Hh]osted? by\s+(?:[a-z][^A-Z]{0,60}?)([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
]


# ------------------------------------------------------------------
# HTML STRIPPING
# ------------------------------------------------------------------

class HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []
    def handle_data(self, data):
        self.text.append(data)
    def get_text(self):
        return ' '.join(self.text)

def strip_html(raw: str) -> str:
    s = HTMLStripper()
    s.feed(raw)
    return s.get_text()


# ------------------------------------------------------------------
# NAME VALIDATION
# ------------------------------------------------------------------

def looks_like_person_channel(name: str) -> bool:
    """Validate a name extracted from channel/title source."""
    name = name.strip()
    if name.lower() in NOT_A_PERSON:
        return False
    if len(name) > 60:
        return False
    if any(c in name for c in ['&', '@', '—', '|', '.com', 'LLC', 'Ltd', 'Inc']):
        return False
    words = name.split()
    if len(words) < 2:
        return False
    connectors = {'van', 'de', 'of', 'the', 'and', 'le', 'la', 'el'}
    for word in words:
        clean = word.rstrip('.,')
        if clean.lower() in connectors:
            continue
        if not clean[0].isupper():
            return False
    return True


def looks_like_person_desc(name: str) -> bool:
    """Validate a name extracted from description source (stricter)."""
    name = name.strip()
    words = name.split()
    if len(words) < 2 or len(words) > 4:
        return False
    for word in words:
        clean = word.rstrip('.,').lower()
        if clean in NOT_A_PERSON_WORDS:
            return False
    if not words[0][0].isupper():
        return False
    # Handle hyphenated last names like "Rowlands-Rees"
    last_clean = words[-1].replace('-', ' ').split()[0]
    if not last_clean[0].isupper():
        return False
    if name == name.upper():
        return False
    if not any(c.islower() for c in name):
        return False
    return True


# ------------------------------------------------------------------
# NAME SPLITTING
# ------------------------------------------------------------------

def split_names_channel(raw: str) -> list[str]:
    """Split channel name into individual person names."""
    # Strip role descriptions after — or |
    raw = re.split(r'[—|]', raw)[0].strip()
    # Normalize ALL CAPS
    if raw == raw.upper() and len(raw) > 2:
        raw = raw.title()
    # Split on comma or " and "
    parts = re.split(r',|\s+and\s+', raw)
    return [p.strip() for p in parts if p.strip()]


def split_names_desc(raw: str) -> list[str]:
    """Split description match into individual person names."""
    # Strip after — or |
    raw = re.split(r'\s+[-—]\s+', raw)[0]
    raw = raw.replace('&amp;', '&').replace('&nbsp;', ' ').strip()

    # Stop at phrases that signal end of name list
    stop_phrases = [
        r'\s+along\s+with\b', r'\s+as\s+well\s+as\b',
        r'\s+plus\b', r'\s+to\s+bring\b', r'\s+to\s+explore\b',
        r'\s+to\s+discuss\b', r'\s+each\s+week\b', r'\s+every\s+week\b',
        r'\s+and\s+(?:their|our|the)\b',  # "and their guests" / "and the team"
        r'\s+bring\s+you\b',              # "Kousha Navidar bring you"
        r'\s+sit\s+down\b',               # trailing "sit down"
        r'\s+(?:explore|discuss|interview|uncover|reveal|dig)s?\b',
    ]
    for phrase in stop_phrases:
        raw = re.split(phrase, raw, flags=re.IGNORECASE)[0]

    # Split on comma or " and "
    parts = re.split(r',|\s+and\s+', raw)

    # Return only parts that look like plausible name fragments (start with capital)
    result = []
    for p in parts:
        p = p.strip().rstrip('.,')
        if p and p[0].isupper():
            result.append(p)
    return result


# ------------------------------------------------------------------
# EXTRACTION
# ------------------------------------------------------------------

def extract_from_title(title: str) -> str | None:
    """Extract host name from 'with [Name]' at end of podcast title."""
    match = re.search(r'\bwith\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*$', title, re.IGNORECASE)
    if not match:
        return None
    if title.lower().startswith('with '):
        return None
    candidate = match.group(1).strip()
    return candidate if looks_like_person_channel(candidate) else None


def extract_from_description(description: str) -> list[str]:
    """Apply HOST_PATTERNS to description, return candidate names."""
    text = strip_html(description)
    candidates = []
    seen = set()

    for pattern in HOST_PATTERNS:
        for match in re.finditer(pattern, text):
            raw = match.group(1)
            for name in split_names_desc(raw):
                if name not in seen and looks_like_person_desc(name):
                    seen.add(name)
                    candidates.append(name)

    return candidates


# ------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------

def get_podcasts_without_hosts(conn) -> list[tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT p.podcast_id, p.title, c.name as channel_name, p.description
        FROM podcasts p
        LEFT JOIN channels c ON c.channel_id = p.channel_id
        WHERE NOT EXISTS (
            SELECT 1 FROM host_podcast hp WHERE hp.podcast_id = p.podcast_id
        )
        ORDER BY p.title
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


def get_existing_host_names(conn) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT first_name || ' ' || last_name FROM hosts")
    names = {row[0].lower() for row in cur.fetchall()}
    cur.close()
    return names


def get_or_create_host(cur, name: str, data_source: str) -> int:
    parts = name.strip().split(' ')
    first_name = ' '.join(parts[:-1]) if len(parts) > 1 else name
    last_name = parts[-1] if len(parts) > 1 else ''

    cur.execute(
        "SELECT host_id FROM hosts WHERE first_name = %s AND last_name = %s",
        (first_name, last_name)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO hosts (first_name, last_name, data_source, created_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (first_name, last_name) DO UPDATE
            SET data_source = hosts.data_source
        RETURNING host_id
        """,
        (first_name, last_name, data_source)
    )
    return cur.fetchone()[0]


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------

def run(dry_run: bool = True):
    conn = psycopg2.connect(DB)
    podcasts = get_podcasts_without_hosts(conn)
    existing_names = get_existing_host_names(conn)

    if not podcasts:
        print("No podcasts without host data.")
        conn.close()
        return

    print(f"\n{'DRY RUN — ' if dry_run else ''}Scanning {len(podcasts)} podcasts...\n")
    print(f"{'Podcast':<52} {'Host Name':<28} {'Source':<16} {'Confidence'}")
    print("-" * 110)

    to_insert = []

    for podcast_id, title, channel_name, description in podcasts:
        found = False

        # --- Source 1: channel name ---
        if channel_name:
            for name in split_names_channel(channel_name):
                if looks_like_person_channel(name):
                    already_known = name.lower() in existing_names
                    confidence = 'CONFIRMED ✅' if already_known else 'HIGH'
                    print(f"{title:<52} {name:<28} {'itunes_artist':<16} {confidence}")
                    to_insert.append((podcast_id, title, name, 'itunes_artist', confidence))
                    found = True

        # --- Source 2: title pattern ---
        if not found:
            name = extract_from_title(title)
            if name:
                already_known = name.lower() in existing_names
                confidence = 'CONFIRMED ✅' if already_known else 'HIGH'
                print(f"{title:<52} {name:<28} {'itunes_artist':<16} {confidence}")
                to_insert.append((podcast_id, title, name, 'itunes_artist', confidence))
                found = True

        # --- Source 3: description patterns ---
        if not found and description:
            names = extract_from_description(description)
            for name in names:
                already_known = name.lower() in existing_names
                confidence = 'CONFIRMED ✅' if already_known else 'HIGH'
                print(f"{title:<52} {name:<28} {'parsed_desc':<16} {confidence}")
                to_insert.append((podcast_id, title, name, 'parsed_desc', confidence))
                found = True

        if not found:
            print(f"{title:<52} {'— no match found':<28}")

    print(f"\n{len(to_insert)} host(s) found across {len(podcasts)} podcasts")

    if dry_run:
        print("\nDry run complete. Run with 'run' to insert into DB.")
        conn.close()
        return

    # Insert
    cur = conn.cursor()
    inserted_hosts = 0
    inserted_links = 0

    for podcast_id, title, name, source, confidence in to_insert:
        try:
            host_id = get_or_create_host(cur, name, source)
            cur.execute(
                """
                INSERT INTO host_podcast (host_id, podcast_id, role, data_source)
                VALUES (%s, %s, 'Host', %s)
                ON CONFLICT (host_id, podcast_id) DO NOTHING
                """,
                (host_id, podcast_id, source)
            )
            if cur.rowcount > 0:
                inserted_links += 1
                logger.info(f"✅ {title} → {name} ({source})")
            else:
                logger.info(f"⏭  {title} → {name} (already linked)")
            inserted_hosts += 1
        except Exception as e:
            logger.error(f"Error inserting {name} for {title}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nDone: {inserted_hosts} host(s) processed, {inserted_links} new podcast links created")


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Extract host names from channel name, title, and description',
        epilog="""
examples:
  python3 host_extractor.py dry-run    # preview without saving
  python3 host_extractor.py run        # insert into DB
        """
    )
    parser.add_argument('command', choices=['dry-run', 'run'])
    args = parser.parse_args()
    run(dry_run=(args.command == 'dry-run'))
