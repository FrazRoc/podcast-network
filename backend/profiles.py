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


# ------------------------------------------------------------------
# Directory pages (/people, /orgs, /shows): one cached list per kind,
# searched, grouped, sorted and paged in memory.
# ------------------------------------------------------------------

def _name_key(r: dict) -> str:
    return slugify(r.get('name') or r.get('title') or '')


def _recency(r: dict) -> int:
    d = r.get('last_date')
    return -d.toordinal() if d else 0


DIRECTORY_SORTS = {
    'appearances': lambda r: (-(r.get('appearances') or 0), _name_key(r)),
    'people': lambda r: (-(r.get('people') or 0), -(r.get('appearances') or 0), _name_key(r)),
    'guests': lambda r: (-(r.get('guests') or 0), _name_key(r)),
    'episodes': lambda r: (-(r.get('episodes') or 0), -(r.get('people') or 0), _name_key(r)),
    'recent': lambda r: (_recency(r), _name_key(r)),
    'name': _name_key,
}


def directory_page(rows: list, *, q: str = '', fields=('name',), group_field: str | None = None,
                   group: str = '', sort: str = 'appearances', default_sort: str = 'appearances',
                   offset: int = 0, limit: int = 50) -> dict:
    """Search `fields` for `q` (case- and accent-insensitive), count the
    matches per `group_field` (for the filter chips, so the counts follow the
    search but not the chosen chip), keep one group, sort and page.
    Returns {total, rows, counts}."""
    needle = slugify(q).replace('-', ' ') if (q or '').strip() else ''

    def matches(r):
        return any(needle in slugify(r.get(f) or '').replace('-', ' ') for f in fields)

    hits = [r for r in rows if matches(r)] if needle else list(rows)
    counts = {}
    if group_field:
        for r in hits:
            counts[r.get(group_field)] = counts.get(r.get(group_field), 0) + 1
        if group and group != 'all':
            hits = [r for r in hits if r.get(group_field) == group]
    hits.sort(key=DIRECTORY_SORTS.get(sort) or DIRECTORY_SORTS[default_sort])
    offset = max(0, offset)
    limit = max(1, min(limit, 200))
    return {'total': len(hits), 'rows': hits[offset:offset + limit], 'counts': counts}


def person_kind(title: str | None, as_host: int, as_guest: int, role_kind) -> str | None:
    """The People directory's filter group: 'host' for someone credited
    mostly as a host, otherwise the kind of their current role (role_kind,
    the Stats page's buckets), None with no role on record."""
    if as_host and as_host >= as_guest:
        return 'host'
    return role_kind(title) if title else None


# ------------------------------------------------------------------
# Person page: career grouped by organisation, and who they appear with.
# ------------------------------------------------------------------

def _later(a, b):
    """The later of two dates, either possibly None."""
    return max(filter(None, [a, b]), default=None)


def _earlier(a, b):
    return min(filter(None, [a, b]), default=None)


def _note_mention(g: dict, d, is_former: bool):
    """Fold one mention into a group's dates and former flag. Former follows
    the most recent mention: "former Vox writer" in 2025 after "Vox staff
    writer" in 2020 means they've left; a later plain mention means they
    were still there."""
    g['mentions'] += 1
    g['first_date'] = _earlier(g['first_date'], d)
    if g['last_date'] is None or (d and d > g['last_date']):
        g['last_date'] = d
        g['former'] = bool(is_former)
    elif d == g['last_date'] and not is_former:
        g['former'] = False


def _new_group(**extra) -> dict:
    return {'first_date': None, 'last_date': None, 'mentions': 0, 'former': False, **extra}


def _drop_subsumed(titles: list) -> list:
    """Drop a title whose words all appear in a longer one at the same
    organisation ("Founder" beside "Founder and Writer"), folding its dates
    into the longer title."""
    words = {id(t): set(_norm_title(t['title']).split()) for t in titles}
    keep = []
    for t in titles:
        bigger = next((o for o in titles if o is not t and words[id(t)] < words[id(o)]), None)
        if bigger is None:
            keep.append(t)
        else:
            bigger['first_date'] = _earlier(bigger['first_date'], t['first_date'])
            bigger['mentions'] += t['mentions']
    return keep


def _by_recency(items: list) -> list:
    far_past = date(1900, 1, 1)
    return sorted(items, key=lambda g: (not g.get('current'), g['former'],
                                        -((g['last_date'] or far_past).toordinal()), -g['mentions']))


def career_by_org(role_rows: list, current_role: dict | None = None) -> list:
    """A person's career as one entry per organisation, each with the
    positions they were introduced with there, newest first.

    role_rows: main._role_rows() output (newest first). Descriptions ("renowned
    climate journalist") are left out: they're not jobs, and the page shows
    them under the name (described_as). A position with no organisation
    ("host", "reporter") is only listed for someone with no organisation at
    all; otherwise it's almost always one of their organisation roles said
    without the name. An organisation named without a title still counts
    towards that organisation's dates.
    """
    orgs, loose = {}, {}
    for r in role_rows:
        d = r.get('published_date')
        raw = (r.get('title') or '').strip()
        is_position = bool(raw) and r.get('title_kind') != 'description'
        title = display_title(raw, 'position') if is_position else None
        company = (r.get('company') or '').strip()
        if company:
            g = orgs.setdefault(r.get('org_id') or company.lower(),
                                _new_group(company=company, org_id=r.get('org_id'), titles={}))
            _note_mention(g, d, r.get('is_former'))
            target = g['titles']
        elif is_position:
            target = loose
        else:
            continue
        if title:
            t = target.setdefault(_norm_title(title), _new_group(title=title))
            if t['last_date'] is None or (d and d > t['last_date']):
                t['title'] = title   # the most recent wording
            _note_mention(t, d, r.get('is_former'))

    cur_org = cur_title = None
    if current_role:
        cur_org = current_role.get('org_id') or (current_role.get('company') or '').strip().lower() or None
        cur_title = _norm_title(current_role.get('title') or '') or None

    if not orgs:
        items = [{'company': None, 'org_id': None, **t, 'titles': [],
                  'current': cur_org is None and _norm_title(t['title']) == cur_title}
                 for t in loose.values()]
        return _by_recency(items)

    items = []
    for key, g in orgs.items():
        g['current'] = key == cur_org
        titles = _drop_subsumed(list(g['titles'].values()))
        for t in titles:
            t['current'] = g['current'] and _norm_title(t['title']) == cur_title
        g['titles'] = _by_recency(titles)
        if g['current']:
            g['former'] = False
        items.append(g)
    return _by_recency(items)


def split_co_appearances(rows: list, limit: int = 12) -> dict:
    """Who a person shares episodes with, by both people's roles there.

    rows: one per (other person, my role, their role) with an episode
    count — {'host_id', 'name', 'profile_image_url', 'me_guest',
    'them_guest', 'episodes'}. Returns the people who interviewed them, the
    other guests beside them, their co-hosts, and the guests on episodes
    they hosted, each sorted by episodes then name.
    """
    buckets = {'interviewed_by': [], 'appeared_alongside': [], 'co_hosts': [], 'guests_hosted': []}
    names = {(True, False): 'interviewed_by', (True, True): 'appeared_alongside',
             (False, False): 'co_hosts', (False, True): 'guests_hosted'}
    for r in rows:
        buckets[names[(bool(r['me_guest']), bool(r['them_guest']))]].append({
            'host_id': r['host_id'], 'name': r['name'], 'slug': slugify(r['name']),
            'profile_image_url': r.get('profile_image_url'), 'episodes': r['episodes']})
    return {k: sorted(v, key=lambda p: (-p['episodes'], p['name']))[:limit] for k, v in buckets.items()}
