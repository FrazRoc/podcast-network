"""
Stats built on guests' roles and organisations (host_affiliations linked to
organizations): who each show books, career moves between kinds of
organisation, the most-booked organisations, the guest mix by year, and
the kinds of role guests hold.

Guests only — a show's own hosts are credited on every episode and would
swamp these (episode_host.is_guest). Current roles only, except where a
chart is about former roles. An organisation's type falls back to its top
parent's ("Energy Institute at UT" counts as academic through UT).
"""

import re
from collections import Counter, defaultdict

ORG_TYPES = ('company', 'investor', 'nonprofit', 'research', 'academic',
             'government', 'media', 'association')

# Every organisation with the id of the organisation at the top of its
# parent chain (itself when it has no parent).
# A company marked not an organisation never counts as a parent: whatever
# sits under it is its own top (the hidden "California" once collected three
# state agencies in Most-Booked Organisations).
_ORG_TOP = """
    org_top AS (
        WITH RECURSIVE up AS (
            SELECT o.org_id, o.org_id AS top FROM organizations o
            LEFT JOIN organizations p ON p.org_id = o.parent_org_id
            WHERE o.parent_org_id IS NULL OR p.not_an_org
            UNION ALL
            SELECT o.org_id, up.top FROM organizations o
            JOIN up ON o.parent_org_id = up.org_id
            JOIN organizations p ON p.org_id = o.parent_org_id AND NOT p.not_an_org
        )
        SELECT up.org_id, up.top FROM up
    )
"""

# One row per guest role linked to a real organisation.
_GUEST_ROLES = f"""
    WITH {_ORG_TOP},
    guest_roles AS (
        SELECT ha.affiliation_id, ha.host_id, ha.episode_id, ha.title, ha.is_former,
               e.podcast_id, e.published_date,
               o.org_id, o.name AS org_name, t.org_id AS top_id, t.name AS top_name,
               COALESCE(o.org_type, t.org_type) AS org_type
        FROM host_affiliations ha
        JOIN episode_host eh ON eh.episode_id = ha.episode_id AND eh.host_id = ha.host_id AND eh.is_guest
        JOIN episodes e ON e.episode_id = ha.episode_id
        JOIN organization_aliases a ON a.normalized_name = ha.company_key
        JOIN organizations o ON o.org_id = a.org_id AND NOT o.not_an_org
        JOIN org_top ot ON ot.org_id = o.org_id
        JOIN organizations t ON t.org_id = ot.top
    )
"""


def show_guest_mix(cur, min_guests: int = 30) -> dict:
    """Per show, its guests split by the type of organisation they work for.
    A guest counts once per show, under their first typed current role there."""
    cur.execute(_GUEST_ROLES + """
        , one AS (
            SELECT DISTINCT ON (podcast_id, host_id) podcast_id, host_id, org_type
            FROM guest_roles WHERE NOT is_former
            ORDER BY podcast_id, host_id, (org_type IS NULL), affiliation_id
        )
        SELECT p.podcast_id, p.title, one.org_type, COUNT(*) AS n
        FROM one JOIN podcasts p ON p.podcast_id = one.podcast_id
        GROUP BY p.podcast_id, p.title, one.org_type
    """)
    shows = {}
    for r in cur.fetchall():
        s = shows.setdefault(r['podcast_id'], {'podcast_id': r['podcast_id'], 'title': r['title'],
                                               'counts': {}, 'typed': 0, 'untyped': 0})
        if r['org_type']:
            s['counts'][r['org_type']] = r['n']
            s['typed'] += r['n']
        else:
            s['untyped'] += r['n']
    items = sorted((s for s in shows.values() if s['typed'] >= min_guests), key=lambda s: -s['typed'])
    return {'types': list(ORG_TYPES), 'items': items}


def revolving_door(cur, people_per_flow: int = 40) -> dict:
    """Guests whose former role was at one type of organisation and whose
    current role (the one shown on their profile) is at another."""
    from main import _all_current_roles   # the displayed current role, same rule as the profile
    current = {h: oid for h, (_, _, oid) in _all_current_roles(cur).items() if oid}
    cur.execute(_GUEST_ROLES + """
        SELECT host_id, org_type, org_name, published_date
        FROM guest_roles WHERE is_former AND org_type IS NOT NULL
        ORDER BY host_id, published_date NULLS LAST
    """)
    former = defaultdict(list)
    for r in cur.fetchall():
        former[r['host_id']].append(r)
    if not former:
        return {'flows': [], 'people': 0}
    cur.execute(f"""
        WITH {_ORG_TOP}
        SELECT o.org_id, o.name, COALESCE(o.org_type, t.org_type) AS org_type
        FROM organizations o JOIN org_top ot ON ot.org_id = o.org_id JOIN organizations t ON t.org_id = ot.top
        WHERE o.org_id = ANY(%s)
    """, (list({current[h] for h in former if h in current}),))
    cur_org = {r['org_id']: r for r in cur.fetchall()}
    cur.execute("SELECT host_id, first_name || ' ' || last_name AS name FROM hosts WHERE host_id = ANY(%s)",
                (list(former),))
    names = {r['host_id']: r['name'] for r in cur.fetchall()}

    flows = defaultdict(list)
    for host_id, rows in former.items():
        now = cur_org.get(current.get(host_id))
        if not now or not now['org_type']:
            continue
        # The type most of their former roles were at; ties go to the earliest.
        kinds = Counter(r['org_type'] for r in rows)
        top = max(kinds.values())
        was = next(r for r in rows if kinds[r['org_type']] == top)
        if was['org_type'] == now['org_type']:
            continue
        flows[(was['org_type'], now['org_type'])].append({
            'host_id': host_id, 'name': names.get(host_id), 'from_org': was['org_name'], 'to_org': now['name']})
    out = [{'from': a, 'to': b, 'count': len(ppl),
            'people': sorted(ppl, key=lambda p: p['name'] or '')[:people_per_flow]}
           for (a, b), ppl in flows.items()]
    out.sort(key=lambda f: -f['count'])
    return {'types': list(ORG_TYPES), 'flows': out, 'people': sum(f['count'] for f in out)}


def house_organisations(cur, min_guests: int = 5, min_share: float = 0.1) -> dict:
    """{podcast_id: top org_id} for shows where one organisation supplies at
    least min_guests guests and either min_share of the show's guests (BNEF
    on Switched On, Columbia on Columbia Energy Exchange) or, with 10+,
    nearly all of its own guests (Aurora on Energy Unplugged). Inferred:
    shows carry no publisher."""
    cur.execute(_GUEST_ROLES + """
        , g AS (SELECT DISTINCT podcast_id, host_id, top_id FROM guest_roles WHERE NOT is_former),
        show_n AS (SELECT podcast_id, COUNT(DISTINCT host_id) AS n FROM g GROUP BY podcast_id),
        org_n AS (SELECT top_id, COUNT(DISTINCT host_id) AS n FROM g GROUP BY top_id)
        SELECT g.podcast_id, g.top_id, COUNT(DISTINCT g.host_id) AS k
        FROM g JOIN show_n s USING (podcast_id) JOIN org_n o USING (top_id)
        GROUP BY g.podcast_id, g.top_id, s.n, o.n
        -- A big share of the show's guests, or a show nearly all of the
        -- organisation's guests appear on (Aurora on Energy Unplugged).
        HAVING COUNT(DISTINCT g.host_id) >= %(k)s
           AND (COUNT(DISTINCT g.host_id) >= %(share)s * s.n
                OR (COUNT(DISTINCT g.host_id) >= 10 AND COUNT(DISTINCT g.host_id) >= 0.8 * o.n))
        ORDER BY k DESC
    """, {'k': min_guests, 'share': min_share})
    house = {}
    for r in cur.fetchall():
        house.setdefault(r['podcast_id'], r['top_id'])
    return house


def top_organisations(cur, by: str = 'guests', exclude_in_house: bool = True, limit: int = 25,
                      offset: int = 0, org_type: str | None = None) -> dict:
    """Organisations (sub-organisations counted under their top parent) by
    distinct guests with a current role there, or by distinct shows those
    guests appeared on. exclude_in_house drops a guest's appearances on
    their own organisation's show (see house_organisations). org_type keeps
    one kind of organisation (by the top parent's type); limit/offset page
    through the ranking, and total / type_counts cover all of it."""
    house = house_organisations(cur)
    cur.execute(_GUEST_ROLES + """
        SELECT DISTINCT podcast_id, host_id, top_id, top_name, org_type
        FROM guest_roles WHERE NOT is_former
    """)
    guests, shows, in_house, kind = defaultdict(set), defaultdict(set), defaultdict(set), {}
    names = {}
    for r in cur.fetchall():
        names[r['top_id']] = r['top_name']
        if r['org_type']:
            kind.setdefault(r['top_id'], r['org_type'])
        if house.get(r['podcast_id']) == r['top_id']:
            in_house[r['top_id']].add(r['host_id'])
            if exclude_in_house:
                continue
        guests[r['top_id']].add(r['host_id'])
        shows[r['top_id']].add(r['podcast_id'])
    cur.execute("SELECT org_id, org_type FROM organizations WHERE org_id = ANY(%s)", (list(names),))
    for r in cur.fetchall():
        if r['org_type']:
            kind[r['org_id']] = r['org_type']
    ranked = sorted(guests, key=lambda o: (-(len(shows[o]) if by == 'shows' else len(guests[o])), names[o]))
    type_counts = defaultdict(int)
    for o in ranked:
        type_counts[kind.get(o) or 'none'] += 1
    if org_type:
        ranked = [o for o in ranked if (kind.get(o) or 'none') == org_type]
    ids = ranked[offset:offset + limit]
    cur.execute("SELECT podcast_id, title FROM podcasts WHERE podcast_id = ANY(%s)", (list(house),))
    titles = {r['podcast_id']: r['title'] for r in cur.fetchall()}
    return {
        'by': by, 'exclude_in_house': exclude_in_house, 'org_type': org_type,
        'total': len(ranked), 'offset': offset, 'type_counts': dict(type_counts),
        'items': [{'org_id': o, 'name': names[o], 'org_type': kind.get(o), 'guests': len(guests[o]),
                   'shows': len(shows[o]), 'in_house_guests': len(in_house[o] - guests[o]) if exclude_in_house else 0}
                  for o in ids],
        'house_shows': [{'podcast_id': p, 'title': titles.get(p), 'org_id': o, 'org': names.get(o)}
                        for p, o in house.items()],
    }


def guest_mix_by_year(cur, first_year: int = 2019) -> dict:
    """Share of guest appearances by the type of organisation the guest
    works for, per year. Each (episode, guest) counts once."""
    cur.execute(_GUEST_ROLES + """
        , one AS (
            SELECT DISTINCT ON (episode_id, host_id) episode_id, host_id, org_type, published_date
            FROM guest_roles WHERE NOT is_former AND org_type IS NOT NULL AND published_date IS NOT NULL
            ORDER BY episode_id, host_id, affiliation_id
        )
        SELECT EXTRACT(YEAR FROM published_date)::int AS year, org_type, COUNT(*) AS n
        FROM one WHERE published_date >= make_date(%s, 1, 1)
        GROUP BY 1, 2 ORDER BY 1
    """, (first_year,))
    years = defaultdict(dict)
    for r in cur.fetchall():
        years[r['year']][r['org_type']] = r['n']
    cur.execute("SELECT EXTRACT(YEAR FROM MAX(published_date))::int AS y FROM episodes")
    partial = cur.fetchone()['y']
    items = [{'year': y, 'counts': c, 'total': sum(c.values())} for y, c in sorted(years.items())]
    return {'types': list(ORG_TYPES), 'items': items, 'partial_year': partial}


# ------------------------------------------------------------------
# Roles by kind. First match wins, so "co-founder and CEO" is a founder.
# ------------------------------------------------------------------

ROLE_KINDS = [
    ('founder',    re.compile(r'\b((co-)?found(er|ers|ed|ing)?|entrepreneur)\b', re.I)),
    ('ceo',        re.compile(r'\b(CEO|president|managing director|executive director|chief executive)\b', re.I)),
    ('official',   re.compile(r'\b(senator|representative|congress(man|woman)|governor|minister|ministerial|'
                              r'secretary|commissioner|mayor|ambassador|envoy|assembly ?member|council ?member|'
                              r'legislator|regulator|administrator|attorney general|rapporteur|'
                              r'(congressional|presidential|party|mayoral|senate|gubernatorial|parliamentary|'
                              r'democratic|republican)\b[\w\s-]{0,20}candidate|'
                              r'high[- ]level champion|presidential coordinator)\b', re.I)),
    ('investor',   re.compile(r'\b(general partner|managing partner|venture partner|investors?|investing|'
                              r'investment|portfolio|fund manager|venture capital|VC|solo GP|syndicate partners?)\b',
                              re.I)),
    ('academic',   re.compile(r'\b(professor|lecturer|dean|researcher|research fellow|scientist|geoscientist|'
                              r'postdoc|phd|scholar|fellow|academic|students?|teacher|instructor|historians?|'
                              r'philosopher|ethicist|ethnobotanist|psychologist|(eco|bio|geo|climato|meteoro|cnidario)logists?|'
                              r'MBA class)\b', re.I)),
    ('analyst',    re.compile(r'\b(analysts?|economists?|strategist|modeler|modeller|research|statistician|'
                              r'associates?(?!\s+(director|vice|vp|partner|dean|professor|general|editor|producer|counsel|principal|manager)))\b', re.I)),
    ('journalist', re.compile(r'\b(reporter|journalist|editor|correspondent|columnist|writer|author|co-authors?|'
                              r'producer|host|anchor|presenter|filmmaker|cinematographer|documentarian|blogger|'
                              r'covers|media maker|creators?|co-creators?|storyteller|content (creator|architect))\b', re.I)),
    ('activist',   re.compile(r'\b(activists?|advocates?|campaigner|organi[sz]er|organi[sz]ing|environmentalist|'
                              r'conservationist|^(youth )?plaintiffs?$|influencer)\b', re.I)),
    ('advisor',    re.compile(r'\b(advis[oe]rs?|consultants?|consulting|experts?|specialists?|counsel|'
                              r'attorneys?|lawyers?|broker)\b', re.I)),
    ('executive',  re.compile(r'\b(C[A-Z]O|VP|AVP|SVP|EVP|chief|director|directors|head|manager|lead|leads|leader|'
                              r'partners?|principals?|chair|chairman|chairwoman|officer|GM|general manager|'
                              r'executive|board|owner|coordinator|supervisor)\b', re.I)),
    ('engineer',   re.compile(r'\b(engineers?|technicians?|electrician|mechanic|P\.E\.|project developer|'
                              r'evaluator|facilities management)\b', re.I)),
]
ROLE_LABELS = {'founder': 'Founder', 'ceo': 'CEO / president', 'executive': 'Executive',
               'investor': 'Investor', 'advisor': 'Advisor / lawyer', 'analyst': 'Analyst / economist',
               'academic': 'Academic / scientist', 'journalist': 'Journalist / author',
               'activist': 'Activist / advocate', 'official': 'Official',
               'engineer': 'Engineer / technician', 'other': 'Other'}


def role_kind(title: str | None) -> str | None:
    if not title:
        return None
    for kind, rx in ROLE_KINDS:
        if rx.search(title):
            return kind
    return 'other'


def guest_roles(cur, min_guests: int = 30) -> dict:
    """Guests by the kind of role they hold (from their current titles),
    overall and per show. A guest counts once overall and once per show,
    under the first kind any of their titles there matches."""
    cur.execute("""
        SELECT ha.host_id, e.podcast_id, ha.title, ha.affiliation_id
        FROM host_affiliations ha
        JOIN episode_host eh ON eh.episode_id = ha.episode_id AND eh.host_id = ha.host_id AND eh.is_guest
        JOIN episodes e ON e.episode_id = ha.episode_id
        WHERE ha.title IS NOT NULL AND NOT ha.is_former
    """)
    order = {k: i for i, (k, _) in enumerate(ROLE_KINDS)}
    order['other'] = len(order)
    best_overall, best_show = {}, {}

    def better(a, b):
        return a if b is None or order[a] < order[b] else b

    for r in cur.fetchall():
        k = role_kind(r['title'])
        best_overall[r['host_id']] = better(k, best_overall.get(r['host_id']))
        key = (r['podcast_id'], r['host_id'])
        best_show[key] = better(k, best_show.get(key))
    overall = Counter(best_overall.values())
    per_show = defaultdict(Counter)
    for (pid, _), k in best_show.items():
        per_show[pid][k] += 1
    cur.execute("SELECT podcast_id, title FROM podcasts WHERE podcast_id = ANY(%s)", (list(per_show),))
    titles = {r['podcast_id']: r['title'] for r in cur.fetchall()}
    # Display order, not matching order.
    kinds = ['founder', 'ceo', 'executive', 'investor', 'advisor', 'analyst', 'academic',
             'engineer', 'journalist', 'activist', 'official', 'other']
    shows = [{'podcast_id': p, 'title': titles.get(p), 'counts': dict(c), 'total': sum(c.values())}
             for p, c in per_show.items() if sum(c.values()) >= min_guests]
    shows.sort(key=lambda s: -s['total'])
    return {'kinds': kinds, 'labels': ROLE_LABELS, 'overall': dict(overall),
            'total': sum(overall.values()), 'shows': shows}
