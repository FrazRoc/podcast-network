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

    current = [r for r in rows if not r.get('is_former') and not r.get('from_other_episode')]
    if not current:
        return None

    newest = max(current, key=lambda r: (r.get('published_date') or date.min, r.get('episode_id') or 0))
    same_appearance = [r for r in current if r.get('episode_id') == newest.get('episode_id')]
    best = min(same_appearance, key=_rank)
    return {**best, 'source': 'derived'}
