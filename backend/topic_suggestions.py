"""
Topic merge suggestions: pairs of topics that are probably the same topic,
for review in Topic Admin (stored in topic_merge_suggestions, see
scraper/migrate_add_topic_merge_suggestions.sql).

Only likely synonyms. A narrower topic ("UK offshore wind" under "offshore
wind") is a parent decision the hierarchy already holds, so plain
similarity — which mostly finds those — isn't a signal here. Three are:

  same_words  the same words in another order or form ("US-EU trade" /
              "EU-US trade", "energy access in Africa" / "Africa energy
              access")
  spelling    the same words, one or more spelled differently
              ("clean energy investing" / "clean energy investment",
              "China EVs" / "Chinese EVs")
  acronym     a short form whose letters are the other's initials, and an
              episode that uses both ("IPPs" / "independent power producers")

Pairs where one topic is already under the other, or marked different, are
left out. Ranked by the episodes on both sides.
"""

import re

from psycopg2.extras import RealDictCursor, execute_values

from topic_names import topic_words

# Endings that make a different form of the same word. Not -ian (electrician
# is not electricity) or -ism (activism is not action).
_SUFFIXES = ('ation', 'ment', 'ing', 'ion', 'ese', 'ers', 'er', 'ed', 'ity', 'ics', 'ic', 'al', 'y', 'e', 'a')


def _stem(word: str) -> str:
    for s in _SUFFIXES:
        if word.endswith(s) and len(word) - len(s) >= 4:
            return word[:-len(s)]
    return word


def same_word(x: str, y: str) -> bool:
    """Two forms of one word: equal stems, or one stem the other plus a
    letter ("china" / "chinese", "investment" / "investing")."""
    if x == y:
        return True
    a, b = sorted((_stem(x), _stem(y)), key=len)
    return a == b or (len(a) >= 4 and b.startswith(a) and len(b) - len(a) <= 1)


def spelling_pair(a: set, b: set) -> bool:
    """The word sets differ only by forms of the same words."""
    da, db = a - b, b - a
    if not da or len(da) != len(db):
        return False
    return all(any(same_word(x, y) for y in db) for x in da) and \
        all(any(same_word(y, x) for x in da) for y in db)


def _initials(name: str) -> str:
    words = re.sub(r'[^A-Za-z0-9 ]+', ' ', name).split()
    return ''.join(w[0] for w in words).lower() if len(words) >= 2 else ''


def _is_acronym(name: str) -> bool:
    return bool(re.fullmatch(r'[A-Z][A-Z0-9]{1,6}s?', name))


def suggestion_pairs(tags: dict, not_same: set, texts=None) -> list:
    """tags: {tag_id: {name, parent_tag_id, episodes}}; not_same: {(a, b)}
    with a < b; texts: callable(tag_ids) -> {tag_id: [episode text, ...]}
    for confirming acronyms (None skips them). Returns dicts tag_a, tag_b,
    reason, score, episodes, strongest reason per pair."""
    def ancestors(tid):
        out, t = set(), tags.get(tid)
        while t and t.get('parent_tag_id') and len(out) < 10:
            out.add(t['parent_tag_id'])
            t = tags.get(t['parent_tag_id'])
        return out
    anc = {tid: ancestors(tid) for tid in tags}
    words = {tid: set(topic_words(t['name'])) for tid, t in tags.items()}
    found = {}

    def add(a, b, reason, score):
        a, b = sorted((a, b))
        if a == b or (a, b) in not_same or a in anc[b] or b in anc[a]:
            return
        if (a, b) not in found:
            found[(a, b)] = {'tag_a': a, 'tag_b': b, 'reason': reason, 'score': score,
                             'episodes': tags[a]['episodes'] + tags[b]['episodes']}

    # same words / spelling: candidates share at least one word.
    by_word = {}
    for tid, ws in words.items():
        for w in ws:
            by_word.setdefault(w, []).append(tid)
    for tid, ws in words.items():
        if not ws:
            continue
        for other in {o for w in ws for o in by_word[w] if o > tid}:
            if ws == words[other]:
                if tags[tid]['name'].lower() != tags[other]['name'].lower():
                    add(tid, other, 'same_words', 1.0)
            elif spelling_pair(ws, words[other]):
                add(tid, other, 'spelling', 0.9)

    if texts is not None:
        by_initials = {}
        for tid, t in tags.items():
            ini = _initials(t['name'])
            if ini:
                by_initials.setdefault(ini, []).append(tid)
        candidates = [(tid, long) for tid, t in tags.items() if _is_acronym(t['name'])
                      for long in by_initials.get(t['name'].rstrip('s').lower(), []) if long != tid]
        if candidates:
            got = texts(sorted({x for pair in candidates for x in pair}))
            for short, long in candidates:
                s = tags[short]['name'].rstrip('s').lower()
                phrase = tags[long]['name'].lower()
                pattern = re.compile(r'\b' + re.escape(s) + r's?\b')
                if any(phrase in t and pattern.search(t) for t in got.get(short, []) + got.get(long, [])):
                    add(short, long, 'acronym', 1.0)

    pairs = list(found.values())
    pairs.sort(key=lambda p: (-p['episodes'], p['tag_a'], p['tag_b']))
    return pairs


def compute_suggestions(conn) -> list:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT t.tag_id, t.name, t.parent_tag_id,
                   (SELECT COUNT(*) FROM episode_tag et WHERE et.tag_id = t.tag_id) AS episodes
            FROM tags t
            WHERE NOT t.not_a_topic AND NOT t.is_company AND NOT t.is_person
        """)
        tags = {r['tag_id']: r for r in cur.fetchall() if r['episodes'] > 0}
        cur.execute("SELECT tag_a, tag_b FROM not_same_topic_pairs")
        not_same = {(r['tag_a'], r['tag_b']) for r in cur.fetchall()}

        def texts(tag_ids):
            cur.execute("""
                SELECT et.tag_id, lower(tx.text_sent) AS text
                FROM episode_tag et JOIN topic_extractions tx ON tx.episode_id = et.episode_id
                WHERE et.tag_id = ANY(%s) AND tx.text_sent IS NOT NULL
            """, (tag_ids,))
            out = {}
            for r in cur.fetchall():
                out.setdefault(r['tag_id'], []).append(r['text'])
            return out

        return suggestion_pairs(tags, not_same, texts)
    finally:
        cur.close()


def refresh_suggestions(conn) -> int:
    """Rebuild the stored queue in one transaction; returns its size."""
    pairs = compute_suggestions(conn)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM topic_merge_suggestions")
        if pairs:
            execute_values(cur, """
                INSERT INTO topic_merge_suggestions (tag_a, tag_b, reason, score, episodes) VALUES %s
            """, [(p['tag_a'], p['tag_b'], p['reason'], p['score'], p['episodes']) for p in pairs])
        conn.commit()
        return len(pairs)
    finally:
        cur.close()
