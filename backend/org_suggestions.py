"""
Company merge suggestions: pairs of organisations that may be one
organisation, for review in Company Admin.

Computed here and stored in company_merge_suggestions
(migrate_add_company_merge_suggestions.sql) — the full pass takes around a
minute and a half at 7,500 organisations, so it runs from
scraper/organizations.py sync and from Company Admin's Recompute button,
never per page load.
"""

from psycopg2.extras import RealDictCursor, execute_values

from org_names import normalize_org_name, looks_like_acronym_of, initials


def suggestion_pairs(orgs: dict, similar: list, not_same: set) -> list:
    """Merge candidates from three signals, strongest reason kept per pair:
    'acronym' (BNEF / Bloomberg New Energy Finance), 'similar' (trigram
    similarity of names: Bloomberg NEF / BloombergNEF), 'contains' (one
    name is the other plus more words: Bloomberg / Bloomberg Green — often
    a parent rather than a duplicate). Pairs already related as parent and
    child, marked not-the-same, or involving a not-an-organisation are
    skipped. Ranked by the people affected, then by signal strength."""
    strength = {'acronym': 3, 'similar': 2, 'contains': 1}
    found = {}

    def add(a, b, reason, score):
        if a == b:
            return
        a, b = sorted((a, b))
        oa, ob = orgs.get(a), orgs.get(b)
        if not oa or not ob or oa['not_an_org'] or ob['not_an_org'] or (a, b) in not_same:
            return
        if oa['parent_org_id'] == b or ob['parent_org_id'] == a:
            return
        prev = found.get((a, b))
        if prev is None or strength[reason] > strength[prev['reason']]:
            found[(a, b)] = {'org_a': a, 'org_b': b, 'reason': reason, 'score': round(score, 2)}

    for a, b, sim in similar:
        add(a, b, 'similar', sim)

    keys = {oid: normalize_org_name(o['name']) or '' for oid, o in orgs.items()}
    single = {}
    for oid, key in keys.items():
        if key and ' ' not in key:
            single.setdefault(key, []).append(oid)
    for oid, o in orgs.items():
        for short_id in single.get(initials(o['name']) or '', []):
            if looks_like_acronym_of(orgs[short_id]['name'], o['name']):
                add(short_id, oid, 'acronym', 1.0)
    by_first_words = {}
    for oid, key in keys.items():
        if key:
            by_first_words.setdefault(key, []).append(oid)
    for oid, key in keys.items():
        words = key.split()
        for n in range(1, len(words)):
            for shorter in by_first_words.get(' '.join(words[:n]), []):
                add(shorter, oid, 'contains', n / len(words))

    pairs = list(found.values())
    for p in pairs:
        p['people'] = orgs[p['org_a']]['people'] + orgs[p['org_b']]['people']
    pairs.sort(key=lambda p: (-p['people'], -strength[p['reason']], -p['score']))
    return pairs


def compute_suggestions(conn, min_similarity: float = 0.5) -> list:
    """Every current merge suggestion, strongest and biggest first."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT o.org_id, o.name, o.parent_org_id, o.not_an_org,
                   COUNT(DISTINCT ha.host_id) AS people
            FROM organizations o
            LEFT JOIN organization_aliases a ON a.org_id = o.org_id
            LEFT JOIN host_affiliations ha ON ha.company_key = a.normalized_name
            GROUP BY o.org_id
        """)
        orgs = {r['org_id']: r for r in cur.fetchall()}
        cur.execute("SELECT set_limit(%s)", (min_similarity,))
        cur.execute("""
            SELECT a.org_id AS a, b.org_id AS b, similarity(lower(a.name), lower(b.name)) AS sim
            FROM organizations a JOIN organizations b
              ON a.org_id < b.org_id AND lower(a.name) % lower(b.name)
            WHERE NOT a.not_an_org AND NOT b.not_an_org
        """)
        similar = [(r['a'], r['b'], r['sim']) for r in cur.fetchall()]
        cur.execute("SELECT org_a, org_b FROM not_same_org_pairs")
        not_same = {(r['org_a'], r['org_b']) for r in cur.fetchall()}
        return suggestion_pairs(orgs, similar, not_same)
    finally:
        cur.close()


def refresh_suggestions(conn, min_similarity: float = 0.5) -> int:
    """Rebuild the stored queue in one transaction; returns its size."""
    pairs = compute_suggestions(conn, min_similarity)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM company_merge_suggestions")
        if pairs:
            execute_values(cur, """
                INSERT INTO company_merge_suggestions (org_a, org_b, reason, score, people)
                VALUES %s
            """, [(p['org_a'], p['org_b'], p['reason'], p['score'], p['people']) for p in pairs])
        conn.commit()
        return len(pairs)
    finally:
        cur.close()
