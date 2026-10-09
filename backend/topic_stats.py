"""
Topic charts for the Stats page: what the network's episodes talk about,
over time and by show, and which companies and people get discussed.

Every episode was tagged by Oct 2026, but new ones wait for the next
tagging pass (topic_extractions status 'done'), so everything here is a
share of tagged episodes, never a raw count of all episodes. Company and person tags (tags.is_company / is_person) are
not topics and are left out of the topic charts; they have their own chart.
"""
import math
from datetime import date

from topic_names import CATEGORIES
from profiles import slugify

# Topic tags only: not "not a topic", not a company or person.
TOPIC = "NOT t.not_a_topic AND NOT t.is_company AND NOT t.is_person"


def broad_of_sql(seed: str) -> str:
    """A WITH clause defining broad_of(tag_id, broad_id, broad_name,
    broad_category): each tag from `seed` (a query yielding tag_id) mapped to
    its nearest broad ancestor, itself included. Tags under no broad topic
    are absent. Depth-capped so a bad parent loop can't run away."""
    return f"""
        WITH RECURSIVE anc(tag_id, ancestor_id, depth) AS (
            SELECT DISTINCT s.tag_id, s.tag_id, 0 FROM ({seed}) s
            UNION ALL
            SELECT a.tag_id, t.parent_tag_id, a.depth + 1
            FROM anc a JOIN tags t ON t.tag_id = a.ancestor_id
            WHERE t.parent_tag_id IS NOT NULL AND a.depth < 10
        ),
        broad_of AS (
            SELECT DISTINCT ON (a.tag_id) a.tag_id, b.tag_id AS broad_id, b.name AS broad_name,
                   b.category AS broad_category
            FROM anc a JOIN tags b ON b.tag_id = a.ancestor_id AND b.is_broad
            ORDER BY a.tag_id, a.depth
        )
    """


def category_by_year(cur, min_tagged: int = 40, first_year: int = 2019) -> dict:
    """Per year: tagged episodes, and how many of them touch each category
    (an episode counts once per category, however many of its topics fall
    in it). Years before first_year (2019, like the other by-year charts) or
    with fewer than min_tagged tagged episodes are left out."""
    cur.execute("""
        SELECT EXTRACT(YEAR FROM e.published_date)::int AS year, COUNT(*) AS n
        FROM topic_extractions tx JOIN episodes e ON e.episode_id = tx.episode_id
        WHERE tx.status = 'done' AND e.published_date IS NOT NULL
        GROUP BY 1
    """)
    totals = {r['year']: r['n'] for r in cur.fetchall()}
    cur.execute(f"""
        SELECT EXTRACT(YEAR FROM e.published_date)::int AS year, t.category,
               COUNT(DISTINCT e.episode_id) AS n
        FROM episode_tag et
        JOIN tags t ON t.tag_id = et.tag_id AND {TOPIC}
        JOIN episodes e ON e.episode_id = et.episode_id
        WHERE e.published_date IS NOT NULL AND t.category IS NOT NULL
        GROUP BY 1, 2
    """)
    counts = {}
    for r in cur.fetchall():
        counts.setdefault(r['year'], {})[r['category']] = r['n']
    items = [{'year': y, 'total': totals[y], 'counts': counts.get(y, {})}
             for y in sorted(totals) if y >= first_year and totals[y] >= min_tagged]
    return {'categories': list(CATEGORIES), 'items': items, 'partial_year': date.today().year}


def broad_by_year(cur, min_tagged: int = 40, first_year: int = 2019) -> dict:
    """Per year: tagged episodes, and how many of them touch each broad
    topic (anything under it counts; an episode once per broad topic).
    Same shape as category_by_year, keyed by the broad topic's tag_id."""
    cur.execute("""
        SELECT EXTRACT(YEAR FROM e.published_date)::int AS year, COUNT(*) AS n
        FROM topic_extractions tx JOIN episodes e ON e.episode_id = tx.episode_id
        WHERE tx.status = 'done' AND e.published_date IS NOT NULL
        GROUP BY 1
    """)
    totals = {r['year']: r['n'] for r in cur.fetchall()}
    cur.execute(broad_of_sql(f"SELECT t.tag_id FROM tags t WHERE {TOPIC}") + """
        SELECT EXTRACT(YEAR FROM e.published_date)::int AS year, bo.broad_id, COUNT(DISTINCT e.episode_id) AS n
        FROM episode_tag et
        JOIN broad_of bo ON bo.tag_id = et.tag_id
        JOIN episodes e ON e.episode_id = et.episode_id
        WHERE e.published_date IS NOT NULL
        GROUP BY 1, 2
    """)
    counts = {}
    for r in cur.fetchall():
        counts.setdefault(r['year'], {})[str(r['broad_id'])] = r['n']
    cur.execute("SELECT tag_id, name, category FROM tags WHERE is_broad AND NOT not_a_topic")
    order = {c: i for i, c in enumerate(CATEGORIES)}
    broad = sorted(({'key': str(r['tag_id']), 'tag_id': r['tag_id'], 'name': r['name'], 'slug': slugify(r['name']),
                     'category': r['category']} for r in cur.fetchall()),
                   key=lambda b: (order.get(b['category'], len(order)), b['name']))
    items = [{'year': y, 'total': totals[y], 'counts': counts.get(y, {})}
             for y in sorted(totals) if y >= first_year and totals[y] >= min_tagged]
    return {'broad': broad, 'categories': list(CATEGORIES), 'items': items, 'partial_year': date.today().year}


def rising_topics(cur, recent_years: int = 2, min_episodes: int = 8, min_shows: int = 3,
                  limit: int = 12, level: str = 'topic', first_year: int = 2019) -> dict:
    """Topics whose share of tagged episodes grew or shrank most between the
    last recent_years years and the years before, back to first_year (2019,
    like the other charts; the earlier episodes are a few shows' first years). Shares are smoothed (one
    pseudo-episode added each side) so a topic going from 0 to 2 doesn't top
    the list; topics need min_episodes episodes overall, on at least
    min_shows shows (so one show's running format, like a daily "EV news"
    roundup, isn't mistaken for a trend). level='broad' compares broad
    topics, each counting everything under it."""
    cutoff = date(date.today().year - recent_years + 1, 1, 1)
    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE e.published_date >= %(c)s) AS recent,
               COUNT(*) FILTER (WHERE e.published_date < %(c)s) AS earlier
        FROM topic_extractions tx JOIN episodes e ON e.episode_id = tx.episode_id
        WHERE tx.status = 'done' AND e.published_date >= %(f)s
    """, {'c': cutoff, 'f': date(first_year, 1, 1)})
    base = cur.fetchone()
    if not base['recent'] or not base['earlier']:
        return {'cutoff_year': cutoff.year, 'rising': [], 'falling': [], 'level': level, **base}
    counts = """
               COUNT(DISTINCT e.episode_id) FILTER (WHERE e.published_date >= %(c)s) AS recent,
               COUNT(DISTINCT e.episode_id) FILTER (WHERE e.published_date < %(c)s) AS earlier"""
    having = "HAVING COUNT(DISTINCT e.episode_id) >= %(m)s AND COUNT(DISTINCT e.podcast_id) >= %(s)s"
    if level == 'broad':
        cur.execute(broad_of_sql(f"SELECT t.tag_id FROM tags t WHERE {TOPIC}") + f"""
            SELECT bo.broad_id AS tag_id, bo.broad_name AS name, bo.broad_category AS category, {counts}
            FROM episode_tag et
            JOIN broad_of bo ON bo.tag_id = et.tag_id
            JOIN episodes e ON e.episode_id = et.episode_id
            WHERE e.published_date >= %(f)s
            GROUP BY 1, 2, 3 {having}
        """, {'c': cutoff, 'f': date(first_year, 1, 1), 'm': min_episodes, 's': min_shows})
    else:
        cur.execute(f"""
            SELECT t.tag_id, t.name, t.category, {counts}
            FROM episode_tag et
            JOIN tags t ON t.tag_id = et.tag_id AND {TOPIC}
            JOIN episodes e ON e.episode_id = et.episode_id
            WHERE e.published_date >= %(f)s
            GROUP BY t.tag_id, t.name, t.category {having}
        """, {'c': cutoff, 'f': date(first_year, 1, 1), 'm': min_episodes, 's': min_shows})
    rows = []
    for r in cur.fetchall():
        recent_share = 100 * r['recent'] / base['recent']
        earlier_share = 100 * r['earlier'] / base['earlier']
        change = math.log((r['recent'] + 1) / (base['recent'] + 1)) - math.log((r['earlier'] + 1) / (base['earlier'] + 1))
        rows.append({'tag_id': r['tag_id'], 'name': r['name'], 'slug': slugify(r['name']),
                     'category': r['category'], 'recent': r['recent'], 'earlier': r['earlier'],
                     'recent_share': round(recent_share, 2), 'earlier_share': round(earlier_share, 2),
                     'change': change})
    rising = sorted((r for r in rows if r['change'] > 0), key=lambda r: -r['change'])[:limit]
    falling = sorted((r for r in rows if r['change'] < 0), key=lambda r: r['change'])[:limit]
    return {'cutoff_year': cutoff.year, 'first_year': first_year, 'recent_episodes': base['recent'], 'level': level,
            'earlier_episodes': base['earlier'], 'rising': rising, 'falling': falling}


def show_category_mix(cur, min_tagged: int = 25) -> dict:
    """Per show with at least min_tagged tagged episodes: how many of its
    tagged episodes touch each category. Bars are shares of the show's
    episode-category pairs, so each bar adds up to 100%."""
    cur.execute(f"""
        SELECT p.podcast_id, p.title, t.category, COUNT(DISTINCT e.episode_id) AS n
        FROM episode_tag et
        JOIN tags t ON t.tag_id = et.tag_id AND {TOPIC}
        JOIN episodes e ON e.episode_id = et.episode_id
        JOIN podcasts p ON p.podcast_id = e.podcast_id
        WHERE t.category IS NOT NULL
        GROUP BY p.podcast_id, p.title, t.category
    """)
    shows = {}
    for r in cur.fetchall():
        s = shows.setdefault(r['podcast_id'], {'podcast_id': r['podcast_id'], 'title': r['title'],
                                               'counts': {}, 'total': 0})
        s['counts'][r['category']] = r['n']
        s['total'] += r['n']
    cur.execute("""
        SELECT e.podcast_id, COUNT(*) AS n FROM topic_extractions tx
        JOIN episodes e ON e.episode_id = tx.episode_id WHERE tx.status = 'done' GROUP BY 1
    """)
    tagged = {r['podcast_id']: r['n'] for r in cur.fetchall()}
    items = []
    for pid, s in shows.items():
        if tagged.get(pid, 0) >= min_tagged:
            items.append({**s, 'tagged_episodes': tagged[pid]})
    items.sort(key=lambda s: -s['tagged_episodes'])
    return {'categories': list(CATEGORIES), 'items': items}


def broad_guest_mix(cur, min_typed: int = 30) -> dict:
    """Per broad topic, the guests on its episodes split by the type of
    organisation they work for, with every guest appearance as the
    baseline ("all"). Each (episode, guest) counts once per broad topic,
    under their first typed current role on that episode; guests at
    untyped organisations aren't counted. Broad topics with fewer than
    min_typed counted appearances are left out."""
    from org_stats import _GUEST_ROLES, ORG_TYPES
    cur.execute(_GUEST_ROLES + """
        , one AS (
            SELECT DISTINCT ON (episode_id, host_id) episode_id, org_type
            FROM guest_roles WHERE NOT is_former AND org_type IS NOT NULL
            ORDER BY episode_id, host_id, affiliation_id
        )
        SELECT org_type, COUNT(*) AS n FROM one GROUP BY 1
    """)
    baseline = {r['org_type']: r['n'] for r in cur.fetchall()}
    # broad_of_sql opens its own WITH, so _GUEST_ROLES's CTEs follow it.
    cur.execute(broad_of_sql(f"SELECT t.tag_id FROM tags t WHERE {TOPIC}")
                + ", " + _GUEST_ROLES.strip()[len("WITH"):] + """
        , one AS (
            SELECT DISTINCT ON (episode_id, host_id) episode_id, org_type
            FROM guest_roles WHERE NOT is_former AND org_type IS NOT NULL
            ORDER BY episode_id, host_id, affiliation_id
        ),
        ep_broad AS (
            SELECT DISTINCT et.episode_id, bo.broad_id, bo.broad_name, bo.broad_category
            FROM episode_tag et JOIN broad_of bo ON bo.tag_id = et.tag_id
        )
        SELECT eb.broad_id, eb.broad_name, eb.broad_category, one.org_type, COUNT(*) AS n
        FROM one JOIN ep_broad eb ON eb.episode_id = one.episode_id
        GROUP BY 1, 2, 3, 4
    """)
    topics = {}
    for r in cur.fetchall():
        t = topics.setdefault(r['broad_id'], {'tag_id': r['broad_id'], 'name': r['broad_name'],
                                              'slug': slugify(r['broad_name']), 'category': r['broad_category'],
                                              'counts': {}, 'total': 0})
        t['counts'][r['org_type']] = r['n']
        t['total'] += r['n']
    items = sorted((t for t in topics.values() if t['total'] >= min_typed), key=lambda t: -t['total'])
    return {'types': list(ORG_TYPES), 'all': {'counts': baseline, 'total': sum(baseline.values())},
            'items': items}


def most_discussed(cur, kind: str = 'company', limit: int = 20) -> dict:
    """The companies (or people) discussed on the most tagged episodes,
    leaving out episodes where they are on themselves: the person credited,
    or a guest who works at the company. Unlinked tags count by name."""
    if kind == 'person':
        cur.execute("""
            SELECT COALESCE(h.first_name || ' ' || h.last_name, t.name) AS name, t.host_id AS link_id,
                   COUNT(DISTINCT et.episode_id) AS episodes
            FROM tags t
            JOIN episode_tag et ON et.tag_id = t.tag_id
            LEFT JOIN hosts h ON h.host_id = t.host_id
            WHERE t.is_person AND NOT t.not_a_topic
              AND NOT EXISTS (SELECT 1 FROM episode_host eh
                              WHERE eh.episode_id = et.episode_id AND eh.host_id = t.host_id)
            GROUP BY 1, 2
            ORDER BY episodes DESC, name LIMIT %s
        """, (limit,))
    else:
        cur.execute("""
            SELECT COALESCE(o.name, t.name) AS name, t.org_id AS link_id,
                   COUNT(DISTINCT et.episode_id) AS episodes
            FROM tags t
            JOIN episode_tag et ON et.tag_id = t.tag_id
            LEFT JOIN organizations o ON o.org_id = t.org_id
            WHERE t.is_company AND NOT t.not_a_topic
              AND NOT EXISTS (
                SELECT 1 FROM host_affiliations ha
                JOIN episode_host eh ON eh.episode_id = ha.episode_id AND eh.host_id = ha.host_id AND eh.is_guest
                JOIN organization_aliases a ON a.normalized_name = ha.company_key
                WHERE ha.episode_id = et.episode_id AND a.org_id = t.org_id)
            GROUP BY 1, 2
            ORDER BY episodes DESC, name LIMIT %s
        """, (limit,))
    items = [{**r, 'slug': slugify(r['name'])} for r in cur.fetchall()]
    cur.execute("SELECT COUNT(*) AS n FROM topic_extractions WHERE status = 'done'")
    return {'kind': kind, 'items': items, 'tagged_episodes': cur.fetchone()['n']}
