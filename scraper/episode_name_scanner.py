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

# How much of a description to scan. Who is on an episode is established in the
# opening summary; what follows is links, boilerplate — or, for Volts, a full
# transcript averaging 18,000 characters and running to 111,000. Scanning those
# credits everyone the guests merely talked about: Joe Manchin picks up 34
# Volts credits without ever appearing on the show.
DESC_SCAN_MAX_CHARS = 2500

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
    r'How to Save a Planet is (?:a Spotify|reported|produced|hosted)',  # How to Save a Planet / Gimlet show boilerplate
    r'\nCheck out our Calls to Action archive',          # How to Save a Planet footer
    r'The show is produced by',                          # This Week in Cleantech production credits
    r'\bwith research support from',                     # crew names, not participants
    # "Stephen Lacey is our executive editor" / "is executive producer" sits in
    # the sign-off of both Catalyst and Columbia Energy Exchange and was about
    # to credit him on 52 episodes he has no part in. Same shape as the
    # research-support credit above: crew, not participants.
    r'[A-Z][a-z]+ [A-Z][a-z]+ is (?:our |the )?executive (?:editor|producer)',
    r'\bEngineering by\b',                               # audio crew sign-off
    r'\bOriginal music (?:and|by)\b',                    # composer credit
]


def clean_description(text: str, max_chars: int = DESC_SCAN_MAX_CHARS) -> str:
    if not text:
        return ''
    for pattern in STRIP_AFTER_PATTERNS:
        match = re.search(pattern, text)
        if match:
            text = text[:match.start()]
    text = text.strip()
    return text[:max_chars] if max_chars else text


# ------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------

def get_hosts(conn) -> list[dict]:
    """Load every name we can match, longest first.

    Aliases come through as extra rows pointing at the same host_id, so an
    episode that says "Nat Bullard" credits the same person as one that says
    "Nathaniel Bullard" instead of creating a second record.
    """
    cur = conn.cursor()
    # The UNION has to be wrapped: Postgres only allows result column names in
    # an ORDER BY that follows UNION, not an expression like LENGTH(...).
    cur.execute("""
        SELECT host_id, first_name, last_name, full_name FROM (
            SELECT host_id, first_name, last_name,
                   first_name || ' ' || last_name AS full_name
            FROM hosts
            UNION ALL
            SELECT a.host_id,
                   split_part(a.alias_name, ' ', 1) AS first_name,
                   NULLIF(substr(a.alias_name, strpos(a.alias_name, ' ') + 1), a.alias_name) AS last_name,
                   a.alias_name AS full_name
            FROM host_aliases a
        ) names
        WHERE full_name IS NOT NULL
        ORDER BY LENGTH(full_name) DESC
    """)
    rows = cur.fetchall()
    cur.close()
    return [
        {'host_id': r[0], 'first_name': r[1], 'last_name': r[2], 'full_name': r[3]}
        for r in rows
    ]


def get_known_names(conn) -> set[str]:
    """Return lowercase set of all known host full names, aliases included.

    Without the aliases a merged-away spelling would be re-suggested as a new
    person after every scan.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT first_name || ' ' || last_name FROM hosts
        UNION ALL
        SELECT alias_name FROM host_aliases
    """)
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


def get_episodes_to_scan(conn, uncredited_only: bool = False) -> list[dict]:
    """Episodes to scan — every episode by default.

    Scanning only episodes with no credits used to be the default, purely for
    speed, and it quietly capped what the scanner could ever find: an episode
    that already had one credit was never looked at again, so a name missed
    the first time stayed missing. That matters most as the scanner improves —
    every fix to the matching should be able to reach the whole archive, not
    just episodes that happen to have no credits yet. The surname index made
    full scans cheap enough that there is no longer a reason to restrict it.

    Inserts are ON CONFLICT DO NOTHING, so re-scanning never disturbs an
    existing credit; it only adds ones we missed.
    """
    cur = conn.cursor()
    where = """
        WHERE NOT EXISTS (
            SELECT 1 FROM episode_host eh WHERE eh.episode_id = e.episode_id
        )""" if uncredited_only else ""
    cur.execute(f"""
        SELECT e.episode_id, e.title, e.description,
               p.podcast_id, p.title AS podcast_title
        FROM episodes e
        JOIN podcasts p ON e.podcast_id = p.podcast_id
        {where}
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


# Honorifics that show up glued to the front of a captured name. The capture
# regexes each try to skip these, but they only cover the forms they list —
# "Professor" slipped past a list containing "Prof" and created a separate
# "Professor Tristan Smith" person. Stripping here catches every path.
_HONORIFIC_RE = re.compile(
    r'^(?:(?:Dr|Prof|Professor|Mr|Ms|Mrs|Miss|Sir|Dame|Rev|Senator|Sen|'
    r'Representative|Rep|Congressman|Congresswoman|Governor|Gov|Mayor|'
    r'President|Secretary|Ambassador|Admiral|General|Captain|Lord|Lady|'
    # Job titles run straight into the name the same way an honorific does:
    # the review queue holds "Founder Oliver Katz" and "CEO Dan Shugar".
    r'(?:Co[- ]?)?Founder|CEO|CTO|CFO|COO|CMO|Chief|Vice|VP|Director|'
    r'Head|Partner|Principal|Manager|Senior|Junior|Deputy)\.?\s+)+',
    re.IGNORECASE
)

# Words that are effectively never someone's surname. Used to keep companies
# out of the review queue — episode titles are full of them ("Heart Aerospace",
# "Rigetti Computing", "Burnt Island Ventures"), and the intro patterns cannot
# tell "talks with Jane Smith" from "talks with Bedrock Robotics".
#
# Deliberately omits words that ARE real surnames: Power (Ted Power), Zero,
# Deep, Duty, Again, Lead, Health, Works. A company left in the queue costs one
# click to reject; a person filtered out is lost silently, so this errs towards
# letting things through. Checked against all 2,074 known people: no matches.
_ORG_WORDS = {
    'inc', 'llc', 'ltd', 'corp', 'corporation', 'company', 'technologies',
    'technology', 'systems', 'solutions', 'ventures', 'capital', 'partners',
    'holdings', 'industries', 'labs', 'laboratories', 'institute', 'foundation',
    'university', 'college', 'centre', 'fund', 'media', 'news', 'studios',
    'robotics', 'aerospace', 'biosciences', 'bioscience', 'sciences', 'security',
    'batteries', 'materials', 'motors', 'mobility', 'analytics', 'strategies',
    'advisors', 'advisers', 'associates', 'consulting', 'county', 'district',
    'council', 'committee', 'association', 'alliance', 'coalition', 'society',
    'agency', 'department', 'ministry', 'commission', 'logistics', 'software',
    'minerals', 'mining', 'pharma', 'airlines', 'aviation', 'shipping',
    'utilities', 'computing', 'management', 'advisory', 'enterprises',
}


def looks_like_organisation(name: str) -> bool:
    tokens = [t.lower().strip('.,') for t in name.split()]
    if not tokens:
        return True
    return tokens[-1] in _ORG_WORDS or (len(tokens) >= 2 and tokens[-2] in _ORG_WORDS)


def strip_honorific(name: str) -> str:
    """Remove any leading titles so "Dr. Leah Stokes" and "Leah Stokes" are one person."""
    return _HONORIFIC_RE.sub('', name.strip()).strip()


def _valid_name(name: str) -> bool:
    if not name or len(name) < 7: return False
    words = name.split()
    if len(words) < 2 or len(words) > 3: return False
    if words[0].lower() in _FALSE_POSITIVE_WORDS: return False
    if looks_like_organisation(name): return False
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
        name = strip_honorific(name)
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

_NAME_RE_CACHE = {}


def _name_pattern(full_name: str):
    """Match a full name only where it stands as a name in its own right.

    A plain substring test credits the wrong person: "dan yates" is inside
    "jordan yates", and "sara baldwin" inside "sara baldwin-griffin" — both
    real pairs in this database, and both produced wrong credits. \\b is not
    enough on its own, since it happily matches "Sara Baldwin" against
    "Sara Baldwin-Griffin" (the hyphen is a word boundary), so hyphens are
    excluded on either side as well.
    """
    pattern = _NAME_RE_CACHE.get(full_name)
    if pattern is None:
        pattern = re.compile(
            r'(?<![\w-])' + re.escape(full_name) + r'(?![\w-])',
            re.IGNORECASE
        )
        _NAME_RE_CACHE[full_name] = pattern
    return pattern


def name_in_text(full_name: str, text: str) -> bool:
    return bool(_name_pattern(full_name).search(text))


_WORD_RE = re.compile(r"[\w'-]+")


def build_surname_index(hosts: list) -> dict:
    """Group people by surname so an episode only tests plausible candidates.

    Checking all ~2,000 names against every episode costs 44ms per description
    — twelve minutes over the full archive, which is why descriptions have
    never been scanned in the scheduled run. A surname has to appear verbatim
    for the full name to match, so looking it up first skips almost everyone.
    """
    index = {}
    for host in hosts:
        full_name = host.get('full_name') or ''
        tokens = _WORD_RE.findall(full_name.lower())
        if tokens:
            index.setdefault(tokens[-1], []).append(host)
    return index


def candidate_hosts(text: str, index: dict) -> list:
    """People whose surname occurs in this text — the only possible matches."""
    if not text:
        return []
    seen_ids, out = set(), []
    for token in set(_WORD_RE.findall(text.lower())):
        for host in index.get(token, ()):
            marker = id(host)
            if marker not in seen_ids:
                seen_ids.add(marker)
                out.append(host)
    return out


def run(dry_run: bool = True, title_only: bool = False, min_length: int = 7,
        uncredited_only: bool = False):
    conn = psycopg2.connect(DB)
    hosts = get_hosts(conn)
    episodes = get_episodes_to_scan(conn, uncredited_only=uncredited_only)
    show_hosts = get_show_hosts(conn)
    surname_index = build_surname_index(hosts)

    scope = "uncredited episodes" if uncredited_only else "episodes"
    logger.info(f"Scanning {len(episodes)} {scope} against {len(hosts)} known names "
                f"({'titles only' if title_only else 'titles + descriptions'})...")

    matches = []

    for episode in episodes:
        episode_id    = episode['episode_id']
        podcast_id    = episode['podcast_id']
        podcast_title = episode['podcast_title']
        title         = episode['title'] or ''
        description   = episode['description'] or ''
        show_host_ids = show_hosts.get(podcast_id, set())
        clean_desc    = clean_description(description) if not title_only else ''

        # Only people whose surname appears in this episode can possibly match.
        scan_desc = bool(clean_desc) and podcast_title not in DESC_SCAN_SKIP_SHOWS
        candidates = candidate_hosts(
            title + ('\n' + clean_desc if scan_desc else ''), surname_index
        )

        for host in candidates:
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

            if scan_desc and name_in_text(full_name, clean_desc):
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

def suggest(title_only: bool = False, limit: int = None, show: str = None,
            dry_run: bool = False):
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
    by_name = {}

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

                if dry_run:
                    added += 1
                    pending.add((name_lower, episode_id))
                    by_name[name] = by_name.get(name, 0) + 1
                    continue

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

    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    cur.close()
    conn.close()

    print(f"\nSuggestion scan complete{' (DRY RUN — nothing written)' if dry_run else ''}:")
    print(f"  {'Would add' if dry_run else 'Added'} to queue:  {added}"
          f"{f' ({len(by_name)} distinct names)' if dry_run else ''}")
    if dry_run and by_name:
        print("\n  Most frequent new names:")
        for nm, c in sorted(by_name.items(), key=lambda kv: -kv[1])[:25]:
            print(f"    {c:4}x  {nm}")
        print()
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
    parser.add_argument('--uncredited-only', action='store_true', default=False,
                        help='Only scan episodes with no credits at all. Faster, but the '
                             'scanner can then never find a name it missed on an episode '
                             'that already has one credit.')
    parser.add_argument('--min-length', type=int, default=7)
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of episodes to scan (suggest mode)')
    parser.add_argument('--show', type=str, default=None,
                        help='Only scan episodes from this podcast title (suggest mode)')
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='suggest mode: report what would be queued without writing')
    args = parser.parse_args()

    if args.command == 'suggest':
        suggest(title_only=args.title_only, limit=args.limit, show=args.show,
                dry_run=args.dry_run)
    else:
        run(
            dry_run=(args.command == 'dry-run'),
            title_only=args.title_only,
            min_length=args.min_length,
            uncredited_only=args.uncredited_only,
        )
