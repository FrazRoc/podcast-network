"""
episode_name_scanner.py

Scans episode titles and descriptions for person names.

Two modes:
  run     — matches against known hosts table, inserts episode_host links
  suggest — finds NEW names not in hosts table, writes to suggestions queue
             for human review via the admin UI

Usage:
    python3 episode_name_scanner.py dry-run              # preview known-name matches
    python3 episode_name_scanner.py dry-run --title-only
    python3 episode_name_scanner.py run --title-only
    python3 episode_name_scanner.py run
    python3 episode_name_scanner.py suggest              # populate suggestions queue
    python3 episode_name_scanner.py suggest --title-only
"""

import os
import psycopg2
import re
import argparse
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB = os.getenv('DATABASE_URL', 'postgresql://localhost/podcast_db')

# Shows to skip for description scanning
DESC_SCAN_SKIP_SHOWS = {
    'POLITICO Energy',
}

# Minimum word length to consider as a name candidate in suggest mode
MIN_NAME_LENGTH = 8

# ------------------------------------------------------------------
# DESCRIPTION CLEANING
# ------------------------------------------------------------------

STRIP_AFTER_PATTERNS = [
    r'\nCredits:',
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the co-host',
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the host',
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the .{0,30} editor',
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the .{0,30} producer',
    r'\nFollow the show on',
    r'\nFor more reporting',
    r'\nOur theme music',
    r'\nSubscribe to',
    r'Follow our co-hosts and production team',
    r'\nSee Privacy Policy',
    r'is produced by Columbia University',
    r'Explore further:',                                # Zero: The Climate Race cross-promotion
    r'Past episode with',                               # Zero: The Climate Race past episode links
    r'See omnystudio.com',                              # Omny Studio privacy footer
    r'\nProducer:',                                     # Outrage + Optimism production credits
    r'\nEdited by:',                                    # production credits
    r'\nExec Producer:',                                # production credits
    r'\nJoin the conversation:',                        # Outrage + Optimism social footer
    r'\nHosted on Acast',                               # Acast footer
    r'See acast.com/privacy',                           # Acast privacy footer
    r'\nRelated Episodes',                               # Cleaning Up: Leadership footer
    r'\nLinks\n',                                       # Cleaning Up: Leadership links section
    r'\nLinks and Related Episodes',                      # Cleaning Up: Leadership combined footer
    r'\nRelevant Guest & Topic Links',                   # Cleaning Up: Leadership links variant
    r'\nGuest Bio',                                      # Cleaning Up: Leadership guest bio section
    r'\nFor show notes',                               # Climate One footer
    r'\nLearn more about your ad choices',             # Megaphone universal footer
    r'megaphone.fm/adchoices',                          # Megaphone universal footer
    r'🎟',                                             # Climate One upcoming shows ticket promo
    r'\nSupport Climate One',                          # Climate One support/subscribe footer
    r'\nhttps://www.linkedin.com/in/',                  # CORE Knowledge LinkedIn footer
    r'\nBlue Spark\n',                                  # CORE Knowledge company links section
    r'This episode (?:of [A-Za-z ]+ )?was (?:reported and )?(?:produced|fact.?checked)',  # How to Save a Planet / Gimlet production credits
    r'How to Save a Planet is (?:a Spotify|reported|produced)',  # How to Save a Planet / Gimlet show boilerplate
    r'\nCheck out our Calls to Action archive',          # How to Save a Planet footer
]


def clean_description(text: str) -> str:
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
    """Load all known hosts, longest names first."""
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


def get_known_names(conn) -> set[str]:
    """Return lowercase set of all known host full names."""
    cur = conn.cursor()
    cur.execute("SELECT first_name || ' ' || last_name FROM hosts")
    names = {r[0].lower() for r in cur.fetchall()}
    cur.close()
    return names


def get_rejected_names(conn) -> set[str]:
    """Return lowercase set of all permanently rejected candidate names."""
    cur = conn.cursor()
    cur.execute("SELECT candidate_name FROM rejected_names")
    names = {r[0].lower() for r in cur.fetchall()}
    cur.close()
    return names


def get_pending_suggestions(conn) -> set[str]:
    """Return set of (candidate_name.lower(), episode_id) already in suggestions."""
    cur = conn.cursor()
    cur.execute("SELECT LOWER(candidate_name), episode_id FROM suggestions WHERE status = 'pending'")
    pairs = {(r[0], r[1]) for r in cur.fetchall()}
    cur.close()
    return pairs


def get_all_episodes(conn, show: str = None) -> list[dict]:
    """Load all episodes with podcast context, optionally filtered by show title."""
    cur = conn.cursor()
    if show:
        cur.execute("""
            SELECT e.episode_id, e.title, e.description,
                   p.podcast_id, p.title AS podcast_title
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE p.title ILIKE %s
            ORDER BY e.published_date DESC
        """, (f'%{show}%',))
    else:
        cur.execute("""
            SELECT e.episode_id, e.title, e.description,
                   p.podcast_id, p.title AS podcast_title
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
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


def get_uncredited_episodes(conn) -> list[dict]:
    """Load episodes without any credits yet."""
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
    """Return dict of podcast_id -> set of host_ids."""
    cur = conn.cursor()
    cur.execute("SELECT podcast_id, host_id FROM host_podcast")
    result = {}
    for podcast_id, host_id in cur.fetchall():
        result.setdefault(podcast_id, set()).add(host_id)
    cur.close()
    return result


# ------------------------------------------------------------------
# NAME EXTRACTION (for suggest mode)
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# NAME EXTRACTION (for suggest mode)
# ------------------------------------------------------------------

# Intro phrases that signal a guest is being introduced
_INTRO_RE = re.compile(
    r"""(?:with|joined by|featuring|speaks?\s+with|talks?\s+(?:to|with)|
        interviews?|welcomes?|sits?\s+down\s+with|chats?\s+with|
        talk(?:s|ed)?\s+(?:to|with)|I\s+(?:talk|chat|speak)s?\s+with)
        \s+
        # Optional title prefix
        (?:(?:Dr|Prof|Mr|Ms|Mrs|Senator|Sen|Rep|CEO|CTO|CFO|COO|Governor|Gov|
           Secretary|Director|Mayor|President)\.?\s+)*
        # The actual name: exactly 2 capitalized words (first + last only)
        ([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})
        # Stop before: " of", " at", " from", ",", possessive, title words
        (?=\s+(?:of|at|from|about|for|on|to)|,|'s|\s+(?:CEO|CTO|CFO|COO|Director|Founder)|$)
    """,
    re.VERBOSE | re.IGNORECASE
)

# "Name joins me/us"
_JOINS_RE = re.compile(
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s+joins?\s+(?:me|us|host|the\s+show)',
    re.IGNORECASE
)

# Possessive org then name: "Rewiring America's Ari Matusiak"
_POSSESSIVE_RE = re.compile(
    r"[A-Z][A-Za-z&\s,.\-]+?'s\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})"
    r"(?=\s+(?:of|at|from|about|for|,|and)|$)",
)

_FALSE_POSITIVE_WORDS = {
    'how', 'why', 'what', 'when', 'where', 'which', 'who', 'will',
    'clean', 'green', 'solar', 'wind', 'grid', 'power', 'energy',
    'climate', 'carbon', 'hydrogen', 'nuclear', 'fusion', 'battery',
    'electric', 'renewable', 'data', 'center', 'tech', 'policy',
    'market', 'supply', 'chain', 'global', 'local', 'state', 'federal',
    'new', 'old', 'big', 'small', 'best', 'next', 'last', 'first',
    'american', 'united', 'states', 'world', 'north', 'south', 'east', 'west',
    'inside', 'beyond', 'me', 'us', 'him', 'her', 'the', 'this', 'that',
    'tell', 'know', 'think', 'make', 'take', 'come', 'look',
    'wall', 'street', 'main', 'back', 'front', 'high', 'low', 'virtual',
    'taming', 'rewiring',
}


def _valid_name(name: str) -> bool:
    if not name or len(name) < 7: return False
    words = name.split()
    if len(words) < 2 or len(words) > 3: return False
    if words[0].lower() in _FALSE_POSITIVE_WORDS: return False
    for w in words:
        if not (w[0].isupper() or ord(w[0]) > 127): return False
    return True


def extract_candidate_names(text: str) -> list[tuple[str, str]]:
    """
    Extract candidate person names from text.
    Returns list of (name, matched_context) tuples.
    """
    found = []
    seen = set()

    def add(name, pos):
        name = name.strip()
        if _valid_name(name) and name.lower() not in seen:
            seen.add(name.lower())
            start = max(0, pos - 60)
            end = min(len(text), pos + 80)
            context = text[start:end].strip()
            found.append((name, context))

    _AND_RE = re.compile(
        r'\s+and\s+'
        r'(?:(?:Dr|Prof|Mr|Ms|Mrs|Senator|Sen|Rep|CEO|CTO|CFO|COO|Governor|'
        r'Director|Mayor|President)\.?\s+)*'
        r'([A-Z][A-Za-z\u00C0-\u017E-]+\s+[A-Z][A-Za-z\u00C0-\u017E-]+)',
        re.IGNORECASE
    )

    def find_and_names(text, after_pos):
        rest = text[after_pos:]
        for and_m in _AND_RE.finditer(rest):
            # Window needs to span a job-title clause between names, e.g.
            # "...Ben Chehebar, VP of Hardware at RoadRunner Recycling, and
            # Jason Gates..." — 40 chars cut that case off by one character.
            if and_m.start() > 100:
                break
            yield and_m.group(1), after_pos + and_m.start()

    for m in _INTRO_RE.finditer(text):
        add(m.group(1), m.start())
        for name, pos in find_and_names(text, m.end()):
            add(name, pos)

    for m in _JOINS_RE.finditer(text):
        add(m.group(1), m.start())
        for name, pos in find_and_names(text, m.end()):
            add(name, pos)

    for m in _POSSESSIVE_RE.finditer(text):
        add(m.group(1), m.start())
        for name, pos in find_and_names(text, m.end()):
            add(name, pos)

    return found


# ------------------------------------------------------------------
# SCANNING — known names (run mode)
# ------------------------------------------------------------------

def name_in_text(full_name: str, text: str) -> bool:
    return full_name.lower() in text.lower()


def run(dry_run: bool = True, title_only: bool = False, min_length: int = 7):
    conn = psycopg2.connect(DB)
    hosts = get_hosts(conn)
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
        clean_desc    = clean_description(description) if not title_only else ''

        for host in hosts:
            host_id   = host['host_id']
            full_name = host['full_name']
            # A name match for someone already recorded as this show's
            # official host means "Host", not "Guest" — previously this
            # case was just skipped entirely, leaving the host with no
            # episode-level credit at all rather than a wrong one.
            is_show_host = host_id in show_host_ids

            if len(full_name) < min_length:
                continue

            if name_in_text(full_name, title):
                matches.append({
                    'episode_id': episode_id, 'host_id': host_id,
                    'full_name': full_name, 'podcast_title': podcast_title,
                    'episode_title': title, 'source': 'parsed_title',
                    'is_show_host': is_show_host,
                })
                continue

            if not title_only and clean_desc and podcast_title not in DESC_SCAN_SKIP_SHOWS \
                    and name_in_text(full_name, clean_desc):
                matches.append({
                    'episode_id': episode_id, 'host_id': host_id,
                    'full_name': full_name, 'podcast_title': podcast_title,
                    'episode_title': title, 'source': 'parsed_desc',
                    'is_show_host': is_show_host,
                })

    logger.info(f"Found {len(matches)} matches")

    if not matches:
        print("No matches found.")
        conn.close()
        return

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

    cur = conn.cursor()
    inserted = skipped = 0

    for m in matches:
        try:
            is_guest = not m['is_show_host']
            role = 'Guest' if is_guest else 'Host'
            cur.execute(
                """
                INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (episode_id, host_id) DO NOTHING
                """,
                (m['episode_id'], m['host_id'], is_guest, role, m['source'])
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
# SUGGEST — find new names, write to suggestions queue
# ------------------------------------------------------------------

def suggest(title_only: bool = False, limit: int = None, show: str = None):
    """
    Scan all episodes for candidate names NOT already in the hosts table.
    Writes new candidates to the suggestions table for human review.
    Skips names that have already been rejected or are already pending.
    """
    conn = psycopg2.connect(DB)

    known_names     = get_known_names(conn)
    rejected_names  = get_rejected_names(conn)
    pending         = get_pending_suggestions(conn)
    episodes        = get_all_episodes(conn, show=show)
    show_hosts      = get_show_hosts(conn)

    if limit:
        episodes = episodes[:limit]

    logger.info(f"Scanning {len(episodes)} episodes for new candidate names...")
    logger.info(f"Known hosts: {len(known_names)} | Rejected: {len(rejected_names)} | Already pending: {len(pending)}")

    cur = conn.cursor()
    added = skipped_known = skipped_rejected = skipped_pending = 0

    for episode in episodes:
        episode_id    = episode['episode_id']
        podcast_title = episode['podcast_title']
        title         = episode['title'] or ''
        description   = episode['description'] or ''

        # Gather text sources to scan
        sources = [('parsed_title', title)]
        if not title_only and podcast_title not in DESC_SCAN_SKIP_SHOWS:
            clean_desc = clean_description(description)
            if clean_desc:
                sources.append(('parsed_desc', clean_desc))

        for source, text in sources:
            candidates = extract_candidate_names(text)

            for name, context in candidates:
                name_lower = name.lower()

                # Skip if already known
                if name_lower in known_names:
                    skipped_known += 1
                    continue

                # Skip if previously rejected
                if name_lower in rejected_names:
                    skipped_rejected += 1
                    continue

                # Skip if already pending for this episode
                if (name_lower, episode_id) in pending:
                    skipped_pending += 1
                    continue

                # Split name
                parts = name.strip().split(' ')
                first_name = ' '.join(parts[:-1]) if len(parts) > 1 else name
                last_name  = parts[-1] if len(parts) > 1 else ''

                try:
                    cur.execute(
                        """
                        INSERT INTO suggestions
                            (candidate_name, first_name, last_name, episode_id, source, matched_text, status)
                        VALUES (%s, %s, %s, %s, %s, %s, 'pending')
                        ON CONFLICT (candidate_name, episode_id) DO NOTHING
                        """,
                        (name, first_name, last_name, episode_id, source, context)
                    )
                    if cur.rowcount > 0:
                        added += 1
                        pending.add((name_lower, episode_id))
                except Exception as e:
                    logger.error(f"Error inserting suggestion '{name}': {e}")
                    conn.rollback()

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nSuggestion scan complete:")
    print(f"  ✅ Added to queue:     {added}")
    print(f"  ⏭  Already known:      {skipped_known}")
    print(f"  ❌ Previously rejected: {skipped_rejected}")
    print(f"  ⚪ Already pending:     {skipped_pending}")

    # Show pending count
    conn2 = psycopg2.connect(DB)
    cur2 = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM suggestions WHERE status = 'pending'")
    total_pending = cur2.fetchone()[0]
    cur2.close()
    conn2.close()
    print(f"\n  Total pending review: {total_pending}")


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Scan episode titles/descriptions for person names',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
commands:
  dry-run     Preview known-name matches without inserting
  run         Insert known-name matches into episode_host
  suggest     Find NEW names and add to suggestions queue for review

examples:
  python3 episode_name_scanner.py dry-run --title-only
  python3 episode_name_scanner.py run --title-only
  python3 episode_name_scanner.py run
  python3 episode_name_scanner.py suggest
  python3 episode_name_scanner.py suggest --title-only
  python3 episode_name_scanner.py suggest --limit 100
        """
    )
    parser.add_argument('command', choices=['dry-run', 'run', 'suggest'])
    parser.add_argument('--title-only', action='store_true', default=False)
    parser.add_argument('--min-length', type=int, default=7)
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of episodes to scan (suggest mode)')
    parser.add_argument('--show', type=str, default=None,
                        help='Only scan episodes from this podcast title (suggest mode)')
    args = parser.parse_args()

    if args.command == 'suggest':
        suggest(title_only=args.title_only, limit=args.limit, show=args.show)
    else:
        run(
            dry_run=(args.command == 'dry-run'),
            title_only=args.title_only,
            min_length=args.min_length,
        )# ------------------------------------------------------------------
# NAME EXTRACTION (for suggest mode)
# ------------------------------------------------------------------

_TITLE_WORDS = {
    'dr', 'prof', 'mr', 'ms', 'mrs', 'senator', 'sen', 'rep', 'representative',
    'ceo', 'cto', 'cfo', 'coo', 'governor', 'gov', 'secretary', 'director',
    'mayor', 'president', 'hawaii', 'california', 'zero', 'energyhub', 'homes',
    'camus', 'google', 'amazon', 'microsoft', 'apple', 'meta',
}

_FALSE_POSITIVE_WORDS = {
    'how', 'why', 'what', 'when', 'where', 'clean', 'green', 'solar', 'wind',
    'grid', 'power', 'energy', 'climate', 'carbon', 'hydrogen', 'nuclear',
    'data', 'center', 'tech', 'market', 'global', 'local', 'state', 'federal',
    'new', 'old', 'big', 'small', 'me', 'us', 'the', 'this', 'that', 'an', 'a',
    'taming', 'virtual', 'rewiring', 'electric', 'renewable', 'battery',
}

_INTRO_RE = re.compile(
    r'(?:with|joined by|featuring|speaks?\s+with|talks?\s+(?:to|with)|'
    r'interviews?|welcomes?|sits?\s+down\s+with|chats?\s+with|'
    r'talk(?:s|ed)?\s+(?:to|with))\s+'
    r'((?:[A-Z][A-Za-z\u00C0-\u017E-]+\s+){1,5}[A-Z][A-Za-z\u00C0-\u017E-]+)',
    re.IGNORECASE
)



_JOINS_RE = re.compile(
    # Exactly 2 words (First Last) — orgs tend to be 3+ words like "Good Food Institute"
    r'([A-Z][a-z]+\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)\s+joins?\s+(?:me|us|host|the\s+show)',
    re.IGNORECASE
)




def _extract_name(raw: str):
    """Strip title prefixes and trailing noise, return clean 2-3 word name or None."""
    words = raw.strip().split()
    # Strip leading title words
    while words and words[0].lower().rstrip('.') in _TITLE_WORDS:
        words = words[1:]
    if not words:
        return None
    # Take words until we hit a stop condition
    name_words = []
    for w in words[:4]:
        clean = w.rstrip('.,').lower()
        if clean in _FALSE_POSITIVE_WORDS or clean in _TITLE_WORDS or clean == 'of':
            break
        if not (w[0].isupper() or ord(w[0]) > 127):
            break
        name_words.append(w.rstrip('.,'))
    if len(name_words) < 2:
        return None
    return ' '.join(name_words[:3])


# ------------------------------------------------------------------
# SCANNING — known names (run mode)
# ------------------------------------------------------------------

def name_in_text(full_name: str, text: str) -> bool:
    return full_name.lower() in text.lower()


def run(dry_run: bool = True, title_only: bool = False, min_length: int = 7):
    conn = psycopg2.connect(DB)
    hosts = get_hosts(conn)
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
        clean_desc    = clean_description(description) if not title_only else ''

        for host in hosts:
            host_id   = host['host_id']
            full_name = host['full_name']

            if host_id in show_host_ids:
                continue
            if len(full_name) < min_length:
                continue

            if name_in_text(full_name, title):
                matches.append({
                    'episode_id': episode_id, 'host_id': host_id,
                    'full_name': full_name, 'podcast_title': podcast_title,
                    'episode_title': title, 'source': 'parsed_title',
                })
                continue

            if not title_only and clean_desc and podcast_title not in DESC_SCAN_SKIP_SHOWS \
                    and name_in_text(full_name, clean_desc):
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

    cur = conn.cursor()
    inserted = skipped = 0

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
# SUGGEST — find new names, write to suggestions queue
# ------------------------------------------------------------------