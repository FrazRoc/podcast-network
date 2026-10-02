"""
What people, shows and organisations talk about: rollups of the topics on
the episodes they are credited on (episode_tag, written by
scraper/extract_topics.py or read by hand). Pure logic; main.py runs the
queries and passes rows in.

A topic row is one (episode, topic) pair: tag_id, name, category,
episode_id, is_primary. Each episode counts once per topic, and the
episode's main topic counts twice, so someone whose episodes are *about*
geothermal ranks it above a topic their episodes only touch on.
"""
from topic_names import CATEGORIES
from profiles import slugify

# A topic is something a person, show or organisation "talks about" only
# once it is on this many of their episodes; a single mention stays hidden.
MIN_EPISODES = 2
# The public topic directory lists topics on at least this many episodes.
MIN_PUBLIC_EPISODES = 2


def _tag_fields(r: dict) -> dict:
    return {'tag_id': r['tag_id'], 'name': r['name'], 'slug': slugify(r['name']), 'category': r['category']}


def rank_topics(rows: list, min_episodes: int = MIN_EPISODES, limit: int | None = None) -> list:
    """Topics on at least min_episodes of the rows' episodes, most discussed
    first: {tag_id, name, slug, category, episodes, as_main_topic, score}."""
    agg = {}
    for r in rows:
        t = agg.setdefault(r['tag_id'], {**_tag_fields(r), 'eps': set(), 'main': set()})
        t['eps'].add(r['episode_id'])
        if r.get('is_primary'):
            t['main'].add(r['episode_id'])
    out = []
    for t in agg.values():
        n = len(t.pop('eps'))
        main = len(t.pop('main'))
        if n >= min_episodes:
            out.append({**t, 'episodes': n, 'as_main_topic': main, 'score': n + main})
    out.sort(key=lambda t: (-t['score'], -t['episodes'], t['slug']))
    return out[:limit] if limit else out


def category_mix(rows: list) -> list:
    """Episodes per category (an episode counts once in each category its
    topics fall under), in the fixed category order, empty ones left out."""
    eps = {}
    for r in rows:
        if r.get('category'):
            eps.setdefault(r['category'], set()).add(r['episode_id'])
    order = {c: i for i, c in enumerate(CATEGORIES)}
    return [{'category': c, 'episodes': len(e)}
            for c, e in sorted(eps.items(), key=lambda kv: (order.get(kv[0], len(order)), kv[0]))]


def summary(rows: list, tagged_episodes: int, limit: int = 12, min_episodes: int = MIN_EPISODES) -> dict:
    """The "Talks about" block for a profile: the top topics with the share
    of tagged episodes each is on, the category mix, and how many episodes
    have been tagged at all (topics cover only part of the archive so far,
    so the page can say what the numbers are out of)."""
    ranked = rank_topics(rows, min_episodes, limit)
    return {
        'tagged_episodes': tagged_episodes,
        'topics': [{**t, 'share': round(100 * t['episodes'] / tagged_episodes) if tagged_episodes else None}
                   for t in ranked],
        'categories': category_mix(rows),
    }


def episode_topics(rows: list, limit: int = 3) -> dict:
    """{episode_id: [topic, ...]} with the main topic first, for listing
    under each episode."""
    by_ep = {}
    for r in rows:
        by_ep.setdefault(r['episode_id'], []).append(r)
    out = {}
    for ep, rs in by_ep.items():
        rs = sorted(rs, key=lambda r: (not r.get('is_primary'), slugify(r['name'])))
        seen, items = set(), []
        for r in rs:
            if r['tag_id'] not in seen:
                seen.add(r['tag_id'])
                items.append({**_tag_fields(r), 'primary': bool(r.get('is_primary'))})
        out[ep] = items[:limit]
    return out


def rank_people(rows: list, limit: int = 20) -> list:
    """People on a topic's episodes, most episodes first: rows are
    (host_id, name, profile_image_url, episode_id, is_primary) — the topic was
    the episode's main topic or not."""
    agg = {}
    for r in rows:
        p = agg.setdefault(r['host_id'], {'host_id': r['host_id'], 'name': r['name'],
                                          'slug': slugify(r['name']),
                                          'profile_image_url': r.get('profile_image_url'),
                                          'eps': set(), 'main': set()})
        p['eps'].add(r['episode_id'])
        if r.get('is_primary'):
            p['main'].add(r['episode_id'])
    out = []
    for p in agg.values():
        n, main = len(p.pop('eps')), len(p.pop('main'))
        out.append({**p, 'episodes': n, 'as_main_topic': main})
    out.sort(key=lambda p: (-(p['episodes'] + p['as_main_topic']), -p['episodes'], p['slug']))
    return out[:limit]
