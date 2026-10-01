"""
Pure helpers behind the public person, organisation and show pages
(/people/<id>-<slug>, /orgs/<id>-<slug>, /shows/<id>-<slug>).

The id in each URL is authoritative; the slug is cosmetic, computed from the
current name on every request (no stored column, so a rename or merge never
leaves a stale slug in the database). A page loaded with an old slug just
rewrites its URL to the current one.
"""

import re
import unicodedata
from datetime import date

from role_selection import display_title


def slugify(name: str) -> str:
    """"Ólafur Teitur Guðnason" -> "olafur-teitur-gudnason". Never empty, so a
    URL always has the "<id>-<slug>" shape."""
    text = unicodedata.normalize('NFKD', name or '')
    text = text.replace('ð', 'd').replace('Ð', 'd').replace('ø', 'o').replace('Ø', 'o') \
               .replace('æ', 'ae').replace('ß', 'ss').replace('ł', 'l').replace('Ł', 'l')
    text = ''.join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r'[^A-Za-z0-9]+', '-', text).strip('-').lower()
    return text[:80].rstrip('-') or 'page'


def apple_show_url(apple_podcast_id) -> str | None:
    return f"https://podcasts.apple.com/podcast/id{apple_podcast_id}" if apple_podcast_id else None


def apple_episode_url(apple_podcast_id, apple_episode_id) -> str | None:
    """The episode on Apple Podcasts, or the show's page when the episode came
    from an RSS feed and has no Apple id (~27% of episodes)."""
    if not apple_podcast_id:
        return None
    if apple_episode_id and str(apple_episode_id).isdigit():
        return f"https://podcasts.apple.com/podcast/id{apple_podcast_id}?i={apple_episode_id}"
    return apple_show_url(apple_podcast_id)


def _norm_title(title: str) -> str:
    return re.sub(r'[^a-z0-9]+', ' ', (title or '').lower()).strip()


def _is_career_row(r: dict) -> bool:
    """A job, not a description: a stated position, or any role at a named
    organisation. "Petroleum geologist with over 40 years of experience" or
    "expert" alone (title_kind 'description', no company) describes the
    person rather than a job they held, and goes in described_as() instead."""
    return bool((r.get('company') or '').strip()) or (
        r.get('title_kind') == 'position' and bool((r.get('title') or '').strip()))


def described_as(role_rows: list, limit: int = 3) -> list:
    """The most recent distinct descriptions of the person ("Petroleum
    geologist", "Climate journalist and author") that aren't jobs at an
    organisation — shown as a short line under their name."""
    out, seen = [], set()
    for r in role_rows:   # newest first, as main._role_rows() returns them
        if _is_career_row(r) or r.get('is_former'):
            continue
        text = display_title((r.get('title') or '').strip(), 'description')
        key = _norm_title(text)
        if text and key not in seen and len(text) <= 90:
            seen.add(key)
            out.append(text)
        if len(out) >= limit:
            break
    return out


def condense_career(role_rows: list, current_role: dict | None = None) -> list:
    """Every distinct (title, organisation) a person has been introduced
    with, earliest and latest episode date each was stated, newest first.

    role_rows: main._role_rows() output (one row per affiliation per
    episode, already excluding organisations marked not an organisation).
    A role is "former" only if every mention of it said so ("former CEO
    of X"); a later "CEO of X" means they still were at that point.
    current_role: the role shown on their page header, flagged so the page
    can mark it. Titles are shown the way the header shows them
    (role_selection.display_title).
    """
    groups = {}
    for r in role_rows:
        if not _is_career_row(r):
            continue
        title = display_title((r.get('title') or '').strip(), r.get('title_kind')) or ''
        company = (r.get('company') or '').strip()
        if not title and not company:
            continue
        org_key = r.get('org_id') or company.lower()
        key = (_norm_title(title), org_key)
        d = r.get('published_date')
        g = groups.get(key)
        if g is None:
            g = groups[key] = {
                'title': title or None, 'company': company or None, 'org_id': r.get('org_id'),
                'first_date': d, 'last_date': d, 'mentions': 0, 'former': True,
            }
        g['mentions'] += 1
        if not r.get('is_former'):
            g['former'] = False
        if d and (g['first_date'] is None or d < g['first_date']):
            g['first_date'] = d
        if d and (g['last_date'] is None or d > g['last_date']):
            g['last_date'] = d
            # The most recent wording of the title wins ("Co-founder & CEO"
            # over an older "CEO").
            if title:
                g['title'] = title
    cur_key = None
    if current_role and (current_role.get('title') or current_role.get('company')):
        cur_key = (_norm_title(current_role.get('title')),
                   current_role.get('org_id') or (current_role.get('company') or '').lower())
    items = []
    for key, g in groups.items():
        g['current'] = key == cur_key
        items.append(g)
    far_past = date(1900, 1, 1)
    items.sort(key=lambda g: (not g['current'], g['former'], -((g['last_date'] or far_past).toordinal())))
    return items


def recent_cadence(month_counts: list, today: date | None = None) -> float:
    """Episodes per month over the 12 full months before `today`.
    month_counts: [(date_of_month_start, count)]."""
    today = today or date.today()
    end = date(today.year, today.month, 1)
    start = date(end.year - 1, end.month, 1)
    total = sum(n for m, n in month_counts if m and start <= m < end)
    return round(total / 12, 1)
