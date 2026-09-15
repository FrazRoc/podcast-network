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
import sys
import psycopg2
import re
import argparse
import logging
from collections import defaultdict

# clean_description() and the labelled-credits extraction it enables live in
# backend/ — see that module's docstring for why this is the canonical copy
# and the scraper reaches across to it rather than the other way around.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'backend'))
from description_cleaner import (  # noqa: E402
    clean_description, strip_html, DESC_SCAN_MAX_CHARS, REMOVE_PATTERNS, STRIP_AFTER_PATTERNS,
    extract_labelled_credits, strip_honorific, _valid_name, looks_like_organisation, _ORG_WORDS,
    name_in_text, first_name_belongs_to_other,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB = os.getenv('DATABASE_URL', 'postgresql://localhost/podcast_db')

# Shows whose descriptions are not worth reading. Held in the database
# (podcasts.scan_descriptions) so the rest of the system can see the decision —
# as a constant here it was invisible, and the diagnostics page reported the
# excluded show as an unexplained hole in coverage.
#
# POLITICO Energy is the case: it discusses politicians constantly and rarely
# has a guest, so almost every name in a description belongs to someone being
# talked about rather than someone present.
DESC_SCAN_SKIP_SHOWS = set()   # filled from the database at run time


def load_desc_scan_skips(conn) -> set:
    cur = conn.cursor()
    try:
        cur.execute("SELECT title FROM podcasts WHERE NOT scan_descriptions")
        return {r[0] for r in cur.fetchall()}
    except Exception:
        # Column not present yet: scan everything rather than fail.
        conn.rollback()
        return set()
    finally:
        cur.close()

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


def get_already_credited_pairs(conn) -> set[tuple[str, int]]:
    """Return (host full name lowercased, episode_id) pairs already in episode_host.

    A belt-and-suspenders check alongside get_known_names(): real cases have
    surfaced (e.g. suggestion_id 2930) where a pending suggestion was created
    for a (name, episode) pair that was already a real credit, even though
    the host existed well before the scan ran. Whatever the cause, checking
    directly against episode_host — the source of truth — closes it without
    needing to explain the discrepancy in the cache.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT lower(h.first_name || ' ' || h.last_name), eh.episode_id
        FROM episode_host eh JOIN hosts h ON h.host_id = eh.host_id
    """)
    pairs = {(r[0], r[1]) for r in cur.fetchall()}
    cur.close()
    return pairs


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
    r"""(?:with|joined\s+by|featuring|
        # "(ft. Name - Org)" / "(feat. Name)" — Climate Insiders' entire
        # title convention (suggestion 5538, "Ben James"), abbreviations
        # "featuring" alone didn't cover.
        feat\.?|ft\.?|
        # "speak to" as well as "speak with" — "we speak to Benjamin Bartle"
        # matched nothing, because only the "with" form was listed.
        (?:speak|spoke|speaks)\s+(?:to|with)|
        (?:talk|talks|talked)\s+(?:to|with)|
        (?:sit|sits|sat)\s+down\s+with|
        (?:chat|chats|chatted)\s+(?:to|with)|
        (?:catch|catches|caught)\s+up\s+with|
        (?:hear|hears|heard)\s+from|
        interviews?|welcomes?|
        I\s+(?:talk|chat|speak)s?\s+(?:to|with))
        \s+
        # Optional title prefix
        (?:(?:Dr|Prof|Mr|Ms|Mrs|Senator|Sen|Rep|CEO|CTO|CFO|COO|Governor|Gov|
           Secretary|Director|Mayor|President)\.?\s+)*
        # The actual name: exactly 2 capitalized words (first + last only).
        # IGNORECASE is intentional here — job words not in the title-prefix
        # list above ("reporter", "activist", ...) are meant to be captured
        # as part of this group too and cleaned up by strip_honorific()
        # downstream, which knows a much longer list of them.
        ([A-Z][a-zA-ZÀ-ž\x27’-]+(?:[^\S\n]+[A-Z][a-zA-ZÀ-ž\x27’-]+){1,2})
        # Stop before: " of", " at", " from", ",", possessive, title words,
        # or a dash/closing paren — "(ft. Ben James - Climate Tech
        # specialist)" has no other stop word between the name and the dash.
        (?=\s+(?:of|at|from|about|for|on|to|and)|,|'s|\s+(?:CEO|CTO|CFO|COO|Director|Founder)|\s*[-–)]|$)
    """,
    re.VERBOSE | re.IGNORECASE
)

# "Name joins me/us"
_JOINS_RE = re.compile(
    r'([A-Z][a-zA-Z\x27’-]+(?:[^\S\n]+[A-Z][a-zA-Z\x27’-]+){1,2})\s+joins?\s+(?:me|us|host|the\s+show)',
    re.IGNORECASE
)

# Possessive org then name: "Rewiring America's Ari Matusiak"
_POSSESSIVE_RE = re.compile(
    r"[A-Z][A-Za-z&\s,.\-]+?'s\s+([A-Z][a-zA-Z\x27’-]+(?:[^\S\n]+[A-Z][a-zA-Z\x27’-]+){1,2})"
    r"(?=\s+(?:of|at|from|about|for|,|and)|$)",
)

# Episode titles are Title Cased, so common words that happen to take an
# apostrophe-s ("On What's Trending In Solar") satisfy _POSSESSIVE_RE's
# capitalized-prefix requirement just as well as a real org name would —
# SunCast ep 756 queued "Trending In Solar" as a person this way. Checked
# against the word immediately before "'s", not the captured name.
_POSSESSIVE_NON_ORG_WORDS = {
    'what', 'that', 'it', 'here', 'there', 'who', 'this', 'today', 'now',
    'one', 'everyone', 'everybody', 'someone', 'nobody',
}

_POSSESSIVE_PREFIX_RE = re.compile(r"^\S+[\x27’]s\s+")

# "We hear from three guests ...: Michele Fetting, program director at the
# Breathe Project ...; Shilpi Chhotray, co-founder ... of People Over
# Plastics ...; and Yvette Arellano, founder ... of Fenceline Watch." — none
# of the patterns above fire because the names sit after a colon, far from
# the "hear from" trigger. Scoped to an explicit guest/panelist/speaker
# mention right before the colon, and requires 2+ semicolon-separated items,
# so it doesn't fire on arbitrary colons elsewhere in a description.
_GUEST_LIST_INTRO_RE = re.compile(
    r'(?:guests?|panelists?|speakers?)\b[^.:\n]{0,90}:\s*',
    re.IGNORECASE
)

# Each item's leading "Name, " — the comma-then-lowercase-word shape is what
# distinguishes a person with an appositive title from an org name or a
# "Guests:\nName1\nName2" label list (already handled by extract_labelled_credits).
_GUEST_LIST_ITEM_RE = re.compile(
    # strip_html() leaves a space where a tag was, e.g. "Fetting</a>," becomes
    # "Fetting ," — the optional \s* before the comma absorbs that.
    r'^\s*(?:and\s+)?([A-Z][a-zA-Z\x27’-]+(?:[^\S\n]+[A-Z][a-zA-Z\x27’-]+){1,2})\s*,\s+[a-z]'
)


def find_guest_list_names(text):
    """Names from a colon-introduced, semicolon-delimited guest list (see
    _GUEST_LIST_INTRO_RE above). Yields (name, absolute_pos) pairs."""
    for m in _GUEST_LIST_INTRO_RE.finditer(text):
        list_start = m.end()
        remainder = text[list_start:list_start + 600]
        end_idx = remainder.find('. ')
        list_text = remainder if end_idx == -1 else remainder[:end_idx + 1]
        items = list_text.split(';')
        if len(items) < 2:
            continue
        pos = list_start
        for item in items:
            item_m = _GUEST_LIST_ITEM_RE.match(item)
            if item_m:
                yield item_m.group(1), pos + item_m.start(1)
            pos += len(item) + 1


def strip_possessive_prefix(name: str) -> str:
    """Drop a leading "Org's " so the person after it stands alone."""
    return _POSSESSIVE_PREFIX_RE.sub('', name).strip()


# Interview-format shows (Titans Of Nuclear, Leaders in Cleantech, ...) put
# the guest's name directly in the title with no trigger phrase at all:
# "Ep 452: Juliann Edwards - Chair, United States Women in Nuclear" or
# "Liliane Ableitner – Exnaton" (suggestion 3570 — Titans Of Nuclear had 194
# of 200 episodes with zero credits, entirely because of this). Not anchored
# to the start of the title since a leading "Show #NNN - " prefix is common
# and varies per show; searched for anywhere and only the first match is
# used, since a later "Name, Role, Org" segment would otherwise itself look
# like a second person ("... - Independent Researcher, University of...").
_TITLE_NAME_SEP_RE = re.compile(
    r'([A-Z][a-zA-ZÀ-ž\x27’-]+(?:\s+[A-Z][a-zA-ZÀ-ž\x27’-]+){1,2})'
    r'\s*[-–,]\s*(?=\S)'
)

# Words that make an otherwise name-shaped capture obviously not a person —
# "This Week In Cleantech - Episode 1" satisfies the Title-Case-words-then-
# separator shape as readily as a real name does. _valid_name only checks
# the first word against _FALSE_POSITIVE_WORDS, and re.search skips ahead to
# try "Week In Cleantech" once "This Week In" fails, dodging that filter —
# so this checks every word instead, just for this one very permissive
# pattern.
_TITLE_NAME_BAD_WORDS = {'in', 'of', 'for', 'the', 'and', 'with', 'on', 'at', 'to', 'live'}


def extract_title_name_credit(title: str) -> str | None:
    """The one name this title's leading 'Name - Org' / 'Name, Org' shape
    names, or None. Only meaningful for shows gated in by
    title_credit_shows() below — this pattern alone is far too permissive
    to run on every show's titles (see that function's docstring)."""
    m = _TITLE_NAME_SEP_RE.search(title or '')
    if not m:
        return None
    name = strip_honorific(m.group(1))
    if not _valid_name(name):
        return None
    if any(w.lower() in _TITLE_NAME_BAD_WORDS for w in name.split()):
        return None
    return name


def title_credit_shows(conn, min_episodes: int = 10, min_rate: float = 0.85) -> set:
    """Podcast IDs whose titles reliably lead with "Name - Org" / "Name, Org"
    — determined empirically per show, not applied network-wide.

    extract_title_name_credit's shape (Title-Cased words then a dash or
    comma) is common enough in ordinary headline-style titles ("Fake Meat,
    What's The Beef?", "Plasma Recycling, Hydrogen Ferry, Scotland's
    Transition...") that running it everywhere produced roughly 50% garbage
    in a random network-wide sample. But on shows that actually use this as
    their title convention, it fires on 91-100% of episodes; everything else
    checked fires on 62% or less — a clean gap. This requires a show to
    clear min_rate over a minimum sample before trusting the pattern for it,
    the same match-rate-guard shape compute_match_gate (scraper.py) uses for
    SunCast's title drift.
    """
    cur = conn.cursor()
    cur.execute("SELECT podcast_id, title FROM episodes")
    counts = defaultdict(lambda: [0, 0])  # podcast_id -> [fired, total]
    for podcast_id, title in cur.fetchall():
        c = counts[podcast_id]
        c[1] += 1
        if extract_title_name_credit(title or ''):
            c[0] += 1
    cur.close()
    return {pid for pid, (fired, total) in counts.items()
            if total >= min_episodes and fired / total >= min_rate}


def extract_candidate_names(text: str) -> list[tuple[str, str]]:
    """
    Extract candidate person names from text.
    Returns list of (name, matched_context) tuples.
    """
    found = []
    seen = set()

    def add(name, pos):
        name = strip_honorific(strip_possessive_prefix(name))
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
        prefix = m.group(0)[:m.group(0).index("'")].strip()
        prefix_last_word = prefix.split()[-1].lower() if prefix else ''
        if prefix_last_word in _POSSESSIVE_NON_ORG_WORDS:
            continue
        add(m.group(1), m.start())
        for name, pos in find_and_names(text, m.end()):
            add(name, pos)

    for name, pos in find_guest_list_names(text):
        add(name, pos)

    return found


# ------------------------------------------------------------------
# SCANNING — known names (run mode)
# ------------------------------------------------------------------

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


def show_host_first_names(conn) -> dict:
    """First names of each show's registered hosts, keyed by podcast_id.

    Shows refer to their own hosts by first name — Redefining Energy writes
    "Gerard and Laurent welcome ..." — so matching full names alone credited
    Gerard Reid on 7 of 205 episodes. A registered host is already known to
    belong to the show, which makes a first name enough inside it.

    A first name shared by two of the same show's hosts is left out: there is
    no way to tell which one is meant.

    Returns (host_id, first_name, last_name) triples — the last name is kept
    so a match can be checked against a *different* full name sharing the
    same first name elsewhere in the text (see `first_name_belongs_to_other`).
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT hp.podcast_id, hp.host_id, h.first_name, h.last_name
        FROM host_podcast hp JOIN hosts h ON h.host_id = hp.host_id
        WHERE h.first_name IS NOT NULL AND length(h.first_name) >= 3
    """)
    by_show = {}
    for podcast_id, host_id, first_name, last_name in cur.fetchall():
        by_show.setdefault(podcast_id, []).append((host_id, first_name, last_name or ''))
    cur.close()

    result = {}
    for podcast_id, entries in by_show.items():
        seen = defaultdict(int)
        for _, first_name, _ in entries:
            seen[first_name.lower()] += 1
        result[podcast_id] = [(host_id, first_name, last_name) for host_id, first_name, last_name in entries
                              if seen[first_name.lower()] == 1]
    return result


def run(dry_run: bool = True, title_only: bool = False, min_length: int = 7,
        uncredited_only: bool = False):
    conn = psycopg2.connect(DB)
    hosts = get_hosts(conn)
    episodes = get_episodes_to_scan(conn, uncredited_only=uncredited_only)
    show_hosts = get_show_hosts(conn)
    surname_index = build_surname_index(hosts)
    # Exact lookup for labelled-credit matching (see below) — a name pulled
    # from a "Guest:"/"Connect with" statement is checked directly against
    # this rather than run back through the (truncated-text) surname index.
    known_by_full_name = {h['full_name'].lower(): h for h in hosts if h['full_name']}
    host_first_names = show_host_first_names(conn)
    desc_skips = load_desc_scan_skips(conn)

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
        scan_desc = bool(clean_desc) and podcast_title not in desc_skips
        candidates = candidate_hosts(
            title + ('\n' + clean_desc if scan_desc else ''), surname_index
        )

        matched_here = set()

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
                matched_here.add(host_id)
                matches.append({
                    'episode_id': episode_id, 'host_id': host_id,
                    'full_name': full_name, 'podcast_title': podcast_title,
                    'episode_title': title, 'source': 'parsed_title',
                    'is_show_host': is_show_host,
                })
                continue

            if scan_desc and name_in_text(full_name, clean_desc):
                matched_here.add(host_id)
                matches.append({
                    'episode_id': episode_id, 'host_id': host_id,
                    'full_name': full_name, 'podcast_title': podcast_title,
                    'episode_title': title, 'source': 'parsed_desc',
                    'is_show_host': is_show_host,
                })

        # Labelled credits ("Guest:", "Connect with [Name]") are precise
        # statements, not mentions, so they're checked against the FULL
        # description with no DESC_SCAN_MAX_CHARS cap — unlike the surname
        # pre-filter above, which stays capped to avoid Joe-Manchin-style
        # over-crediting from huge transcripts. Real incident: Kulsoom Khan,
        # an already-known host, was only named in a "Connect with" footer
        # past the cap, so the truncated-text matching above never found her.
        if scan_desc:
            full_desc = clean_description(description, max_chars=None)
            for labelled_name, _ in extract_labelled_credits(full_desc):
                host = known_by_full_name.get(labelled_name.lower())
                if not host or host['host_id'] in matched_here:
                    continue
                matched_here.add(host['host_id'])
                matches.append({
                    'episode_id': episode_id, 'host_id': host['host_id'],
                    'full_name': host['full_name'], 'podcast_title': podcast_title,
                    'episode_title': title, 'source': 'parsed_desc',
                    'is_show_host': host['host_id'] in show_host_ids,
                })

        # A registered host named only by their first name still counts, but
        # only where their full name did not already match on this episode,
        # and only where that first name isn't also attached to someone
        # else's surname in the same text (see first_name_belongs_to_other).
        haystack = title + ('\n' + clean_desc if scan_desc else '')
        for host_id, first_name, last_name in host_first_names.get(podcast_id, ()):
            if host_id in matched_here:
                continue
            if not name_in_text(first_name, haystack):
                continue
            if first_name_belongs_to_other(first_name, last_name, haystack):
                continue
            matches.append({
                'episode_id': episode_id, 'host_id': host_id,
                'full_name': first_name, 'podcast_title': podcast_title,
                'episode_title': title, 'source': 'host_first_name',
                'is_show_host': True,
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

    desc_skips        = load_desc_scan_skips(conn)
    known_names       = get_known_names(conn)
    rejected_names    = get_rejected_names(conn)
    pending           = get_pending_suggestions(conn)
    already_credited  = get_already_credited_pairs(conn)
    episodes          = get_all_episodes(conn, show=show)
    show_hosts        = get_show_hosts(conn)
    title_credit_pids = title_credit_shows(conn)

    if limit:
        episodes = episodes[:limit]

    logger.info(f"Scanning {len(episodes)} episodes for new candidate names...")
    logger.info(f"Known hosts: {len(known_names)} | Rejected: {len(rejected_names)} | Already pending: {len(pending)}")

    cur = conn.cursor()
    added = skipped_known = skipped_rejected = skipped_pending = skipped_credited = 0
    by_name = {}

    for episode in episodes:
        episode_id    = episode['episode_id']
        podcast_title = episode['podcast_title']
        title         = episode['title'] or ''
        description   = episode['description'] or ''

        # Two passes per text: labelled credits (Host:/Guest:/"Connect with")
        # are precise, so they run on the full text — length doesn't matter
        # for exact statements the way it does for heuristic guessing (see
        # run()'s identical reasoning). The heuristic pass stays capped at
        # DESC_SCAN_MAX_CHARS to avoid Volts-transcript-style over-crediting
        # of people merely mentioned, not present.
        full_desc = truncated_desc = ''
        if not title_only and podcast_title not in desc_skips:
            full_desc = clean_description(description, max_chars=None)
            truncated_desc = clean_description(description)

        candidates = [('parsed_title', n, title[:160]) for n, _ in extract_labelled_credits(title)]
        candidates += [('parsed_title', n, c) for n, c in extract_candidate_names(title)]
        if episode['podcast_id'] in title_credit_pids:
            title_credit_name = extract_title_name_credit(title)
            if title_credit_name:
                candidates.append(('parsed_title', title_credit_name, title[:160]))
        if full_desc:
            candidates += [('parsed_desc', n, full_desc[:160]) for n, _ in extract_labelled_credits(full_desc)]
        if truncated_desc:
            candidates += [('parsed_desc', n, c) for n, c in extract_candidate_names(truncated_desc)]

        for source, name, context in candidates:
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

            # Skip if this exact person is already credited on this exact
            # episode — see get_already_credited_pairs().
            if (name_lower, episode_id) in already_credited:
                skipped_credited += 1
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
    print(f"  ✅ Already credited:    {skipped_credited}")

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
