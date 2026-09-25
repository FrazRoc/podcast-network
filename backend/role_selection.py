"""
The displayed "current role" for a person, derived from host_affiliations.

host_affiliations is raw data: one set of rows per appearance, worded
however each show worded it (see scraper/extract_affiliations.py). This
picks the one role to show, and is deliberately simple until there is more
real data to tune it against:

  1. A manual pin (host_role_pins) wins outright.
  2. Otherwise, only current roles count: former roles are history, and an
     appearance flagged from_other_episode ("past episodes you'll love",
     reruns) is dated wrong.
  3. The most recent appearance with any such role wins.
  4. Within that appearance, a real position at a named organisation beats
     a position alone, which beats a description ("clean energy investor"),
     which beats a bare organisation.
"""

import re
from datetime import date


def _rank(row: dict) -> int:
    kind, company = row.get('title_kind'), row.get('company')
    if kind == 'position' and company:
        return 0
    if kind == 'position':
        return 1
    if kind == 'description':
        return 2
    return 3


def pick_current_role(rows: list, pin: dict | None = None) -> dict | None:
    """rows: host_affiliations joined with their extraction and episode —
    dicts with title, company, title_kind, is_former, from_other_episode,
    published_date, episode_id (plus anything else, passed through).
    pin: {'title', 'company'} from host_role_pins, or None.

    Returns the chosen row with 'source' set to 'pinned' or 'derived', or
    None when there is nothing to show.
    """
    if pin and (pin.get('title') or pin.get('company')):
        return {'title': pin.get('title'), 'company': pin.get('company'), 'source': 'pinned'}

    # A row can be left with nothing to show once a non-organisation company
    # is blanked out (see _role_rows); it does not count as a current role.
    current = [r for r in rows if not r.get('is_former') and not r.get('from_other_episode')
               and (r.get('title') or r.get('company'))]
    if not current:
        return None

    newest = max(current, key=lambda r: (r.get('published_date') or date.min, r.get('episode_id') or 0))
    same_appearance = [r for r in current if r.get('episode_id') == newest.get('episode_id')]
    best = min(same_appearance, key=_rank)
    return {**best, 'source': 'derived'}


# ------------------------------------------------------------------
# Display formatting
# ------------------------------------------------------------------
#
# host_affiliations keeps each title exactly as the episode text wrote it —
# that is what makes every value checkable against its snippet. The tidying
# happens here, only for the one role that is shown: "a senior investigative
# data reporter" reads as a phrase, not a role. Pins are shown as typed.

_LEADING_ARTICLE_RE = re.compile(r'^(?:a|an|the)\s+', re.IGNORECASE)
_SMALL_WORDS = {'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'from', 'in', 'into',
                'nor', 'of', 'on', 'or', 'the', 'to', 'via', 'vs', 'with'}
_WORD_RE = re.compile(r"[A-Za-z\u00C0-\u024F][\w'’.]*")


def _cap(word: str) -> str:
    # A word that already has a capital is someone's deliberate spelling
    # (CEO, DOE's, McKinsey, iPhone) and is left alone.
    if any(c.isupper() for c in word):
        return word
    return word[0].upper() + word[1:]


def _title_case(text: str) -> str:
    """Title case for job titles: every word capitalised except small
    connecting words (after the first); each part of a hyphenated word
    capitalised ("co-founder" -> "Co-Founder")."""
    first = True

    def repl(m):
        nonlocal first
        word = m.group(0)
        if not first and word.lower() in _SMALL_WORDS:
            return word
        first = False
        return _cap(word)

    return _WORD_RE.sub(repl, text)


def display_title(title: str | None, title_kind: str | None = None) -> str | None:
    """How a stored title is shown: leading "a"/"an"/"the" dropped, then
    title case for a position, a capital first letter for a description
    ("Ecologist, political scientist, and author" — title case reads oddly
    on a phrase like that)."""
    if not title:
        return title
    text = _LEADING_ARTICLE_RE.sub('', title.strip()) or title.strip()
    if title_kind == 'description':
        return _cap(text)
    return _title_case(text)


def format_for_display(role: dict | None) -> dict | None:
    """The chosen role with its title tidied for display; pins untouched."""
    if not role or role.get('source') == 'pinned':
        return role
    return {**role, 'title': display_title(role.get('title'), role.get('title_kind'))}
