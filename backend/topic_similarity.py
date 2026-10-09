"""
"Talks about similar things": people (or shows) whose episodes cover the
same topics, for the person and show pages. Pure logic; main.py runs the
queries, builds the index once and keeps it with the directory lists.

Each person or show gets a topic fingerprint: for every topic, the episodes
it is on, counting a narrower topic's episodes toward the topics above it
(so "UK offshore wind" and "floating offshore wind" meet at "offshore
wind"), each episode once per topic and its main topic twice, as in the
rollups (topics.py). Counts are dampened (log) so a daily show's 900
episodes don't drown its range, and weighted by rarity (IDF) so sharing
"Solar" says little and sharing "agrivoltaics" says a lot. Similarity is
the cosine of two fingerprints.

The reason shown is the most specific topics both sides talk about: the
largest shared contributions, among topics on 2+ episodes of each side,
leaving out a topic when a narrower one under it is already listed (topics
on one episode each when nothing is on two).
"""
import math

# Someone needs this many tagged episodes to get (or be) a recommendation;
# a single episode is one conversation, not what someone talks about.
MIN_EPISODES = 2
# People who share this much of the smaller one's episodes are co-hosts or
# a regular pairing: similar by construction, and already "appears with".
MAX_SHARED_EPISODES = 0.5


def expand(tag_id, parents: dict, cap: int = 10) -> list:
    """The topic and every topic above it (parents: {tag_id: parent_tag_id})."""
    out, t = [tag_id], tag_id
    while parents.get(t) and len(out) <= cap:
        t = parents[t]
        if t in out:
            break
        out.append(t)
    return out


def build_index(rows, parents: dict, min_episodes: int = MIN_EPISODES) -> dict:
    """rows: (entity_id, episode_id, tag_id, is_primary). Returns
    {'vectors': {entity: {tag: weight}}, 'counts': {entity: {tag: episodes}},
     'episodes': {entity: set}, 'postings': {tag: [entity, ...]}, 'norms': {entity: float}}."""
    eps, main = {}, {}
    episodes = {}
    for ent, ep, tag, primary in rows:
        episodes.setdefault(ent, set()).add(ep)
        for t in expand(tag, parents):
            eps.setdefault(ent, {}).setdefault(t, set()).add(ep)
            if primary:
                main.setdefault(ent, {}).setdefault(t, set()).add(ep)
    keep = {e for e, s in episodes.items() if len(s) >= min_episodes}
    df = {}
    for e in keep:
        for t in eps[e]:
            df[t] = df.get(t, 0) + 1
    n = len(keep)
    vectors, counts, norms, postings = {}, {}, {}, {}
    for e in keep:
        vec, cnt = {}, {}
        for t, s in eps[e].items():
            c = len(s) + len(main.get(e, {}).get(t, ()))
            vec[t] = (1 + math.log(c)) * math.log(1 + n / df[t])
            cnt[t] = len(s)
            postings.setdefault(t, []).append(e)
        vectors[e], counts[e] = vec, cnt
        norms[e] = math.sqrt(sum(w * w for w in vec.values())) or 1.0
    return {'vectors': vectors, 'counts': counts, 'norms': norms, 'postings': postings,
            'episodes': {e: episodes[e] for e in keep}, 'parents': parents}


def shared_topics(index: dict, a, b, limit: int = 3, min_episodes: int = 2) -> list:
    """The topics that make a and b similar, largest shared weight first: tag ids."""
    va, vb = index['vectors'][a], index['vectors'][b]
    ca, cb = index['counts'][a], index['counts'][b]
    both = [t for t in va.keys() & vb.keys() if ca[t] >= min_episodes and cb[t] >= min_episodes]
    # A topic above another shared one adds nothing ("offshore wind" next to
    # "floating offshore wind"): keep the most specific.
    above = {x for t in both for x in expand(t, index['parents'])[1:]}
    both = sorted((t for t in both if t not in above), key=lambda t: -va[t] * vb[t])
    return both[:limit]


def similar(index: dict, entity, limit: int = 8, exclude=(), min_score: float = 0.05,
            max_shared: float | None = MAX_SHARED_EPISODES) -> list:
    """The entities most like this one: [(entity, score, [shared tag ids])].
    exclude: ids never returned (e.g. people not shown publicly). max_shared:
    drop anyone sharing more than that share of the smaller episode list
    (None keeps them, for shows, which never share episodes)."""
    vec = index['vectors'].get(entity)
    if not vec:
        return []
    scores = {}
    for t, w in vec.items():
        for other in index['postings'].get(t, ()):
            if other != entity:
                scores[other] = scores.get(other, 0.0) + w * index['vectors'][other][t]
    mine = index['episodes'][entity]
    ranked = []
    for other, dot in scores.items():
        if other in exclude:
            continue
        score = dot / (index['norms'][entity] * index['norms'][other])
        if score < min_score:
            continue
        if max_shared is not None:
            theirs = index['episodes'][other]
            if len(mine & theirs) > max_shared * min(len(mine), len(theirs)):
                continue
        ranked.append((other, score))
    ranked.sort(key=lambda x: (-x[1], str(x[0])))
    out = []
    for o, s in ranked[:limit]:
        # Someone on two or three episodes rarely has a topic twice: fall
        # back to topics on one episode each rather than show no reason.
        shared = shared_topics(index, entity, o) or shared_topics(index, entity, o, min_episodes=1)
        out.append((o, round(s, 3), shared))
    return out
