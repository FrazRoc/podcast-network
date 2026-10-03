"""
Topic charts for the Stats page: what the network's episodes talk about,
over time and by show, and which companies and people get discussed.

Topics cover a random sample of the archive (topic_extractions status
'done'), so everything here is a share of tagged episodes, never a raw count
of all episodes. Company and person tags (tags.is_company / is_person) are
not topics and are left out of the topic charts; they have their own chart.
"""
import math
from datetime import date

from topic_names import CATEGORIES
from profiles import slugify

# Topic tags only: not "not a topic", not a company or person.
TOPIC = "NOT t.not_a_topic AND NOT t.is_company AND NOT t.is_person"


def category_by_year(cur, min_tagged: int = 40) -> dict:
    """Per year: tagged episodes, and how many of them touch each category
    (an episode counts once per category, however many of its topics fall
    in it). Years with fewer than min_tagged tagged episodes are left out."""
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
             for y in sorted(totals) if totals[y] >= min_tagged]
    return {'categories': list(CATEGORIES), 'items': items, 'partial_year': date.today().year}


def rising_topics(cur, recent_years: int = 2, min_episodes: int = 8, min_shows: int = 3,
                  limit: int = 12) -> dict:
    """Topics whose share of tagged episodes grew or shrank most between the
    last recent_years years and everything before. Shares are smoothed (one
    pseudo-episode added each side) so a topic going from 0 to 2 doesn't top
    the list; topics need min_episodes episodes overall, on at least
    min_shows shows (so one show's running format, like a daily "EV news"
    roundup, isn't mistaken for a trend)."""
    cutoff = date(date.today().year - recent_years + 1, 1, 1)
    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE e.published_date >= %(c)s) AS recent,
               COUNT(*) FILTER (WHERE e.published_date < %(c)s) AS earlier
        FROM topic_extractions tx JOIN episodes e ON e.episode_id = tx.episode_id
        WHERE tx.status = 'done' AND e.published_date IS NOT NULL
    """, {'c': cutoff})
    base = cur.fetchone()
    if not base['recent'] or not base['earlier']:
        return {'cutoff_year': cutoff.year, 'rising': [], 'falling': [], **base}
    cur.execute(f"""
        SELECT t.tag_id, t.name, t.category,
               COUNT(DISTINCT e.episode_id) FILTER (WHERE e.published_date >= %(c)s) AS recent,
               COUNT(DISTINCT e.episode_id) FILTER (WHERE e.published_date < %(c)s) AS earlier
        FROM episode_tag et
        JOIN tags t ON t.tag_id = et.tag_id AND {TOPIC}
        JOIN episodes e ON e.episode_id = et.episode_id
        WHERE e.published_date IS NOT NULL
        GROUP BY t.tag_id, t.name, t.category
        HAVING COUNT(DISTINCT e.episode_id) >= %(m)s AND COUNT(DISTINCT e.podcast_id) >= %(s)s
    """, {'c': cutoff, 'm': min_episodes, 's': min_shows})
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
    return {'cutoff_year': cutoff.year, 'recent_episodes': base['recent'],
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
