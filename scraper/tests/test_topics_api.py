"""The topic endpoints: the /topics directory and topic page, the "Talks
about" block on profiles, and Topic Admin's rename and merge."""
import asyncio
import os
import sys

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))


def _run(db_conn, monkeypatch, fn, *args, **kwargs):
    pytest.importorskip("fastapi")
    import main
    monkeypatch.setattr(main, 'get_db_connection',
                        lambda: psycopg2.connect(db_conn.dsn, cursor_factory=RealDictCursor))
    r = getattr(main, fn)(*args, **kwargs)
    return asyncio.run(r) if asyncio.iscoroutine(r) else r


def _setup(db_conn):
    """One show, four episodes with a guest each (Ann on 1-3, Bo on 4) and a
    host on all. Geothermal on 1-3 (main topic on 1 and 2), permitting on
    1 and 4, oil prices on 4 only."""
    cur = db_conn.cursor()
    cur.execute("INSERT INTO podcasts (apple_podcast_id, title) VALUES ('1', 'Grid Talk') RETURNING podcast_id")
    pid = cur.fetchone()[0]
    people = {}
    for first, last in [('Ann', 'Lee'), ('Bo', 'Diaz'), ('Hal', 'Host')]:
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES (%s, %s) RETURNING host_id", (first, last))
        people[first] = cur.fetchone()[0]
    eps = []
    for i in range(4):
        cur.execute("INSERT INTO episodes (podcast_id, title, published_date) VALUES (%s, %s, %s) RETURNING episode_id",
                    (pid, f'Episode {i + 1}', f'2025-0{i + 1}-01'))
        ep = cur.fetchone()[0]
        eps.append(ep)
        guest = people['Ann'] if i < 3 else people['Bo']
        cur.execute("INSERT INTO episode_host (episode_id, host_id, is_guest) VALUES (%s, %s, true), (%s, %s, false)",
                    (ep, guest, ep, people['Hal']))
        cur.execute("INSERT INTO topic_extractions (episode_id, status) VALUES (%s, 'done')", (ep,))
    tags = {}
    for name, cat in [('geothermal energy', 'Power generation'), ('permitting reform', 'Policy and politics'),
                      ('oil prices', 'Fuels'), ('geothermal', 'Power generation')]:
        cur.execute("INSERT INTO tags (name, slug, category) VALUES (%s, %s, %s) RETURNING tag_id",
                    (name, name.replace(' ', '-'), cat))
        tags[name] = cur.fetchone()[0]
        cur.execute("INSERT INTO tag_aliases (tag_id, alias_name, normalized_name) VALUES (%s, %s, %s)",
                    (tags[name], name, name))
    for ep, tag, primary in [(0, 'geothermal energy', True), (1, 'geothermal energy', True),
                             (2, 'geothermal energy', False), (0, 'permitting reform', False),
                             (3, 'permitting reform', False), (3, 'oil prices', True),
                             (2, 'geothermal', True)]:
        cur.execute("INSERT INTO episode_tag (episode_id, tag_id, is_primary, data_source) VALUES (%s, %s, %s, 'manual')",
                    (eps[ep], tags[tag], primary))
    db_conn.commit()
    return pid, people, eps, tags


def test_directory_hides_one_episode_topics(db_conn, monkeypatch):
    _setup(db_conn)
    d = _run(db_conn, monkeypatch, 'topic_directory')
    assert [r['name'] for r in d['rows']] == ['geothermal energy', 'permitting reform']
    geo = d['rows'][0]
    assert (geo['episodes'], geo['as_main_topic'], geo['people'], geo['guests']) == (3, 2, 2, 1)
    every = _run(db_conn, monkeypatch, 'topic_directory', all=True)
    assert every['total'] == 4


def test_directory_search_and_category(db_conn, monkeypatch):
    _setup(db_conn)
    d = _run(db_conn, monkeypatch, 'topic_directory', category='Policy and politics')
    assert [r['name'] for r in d['rows']] == ['permitting reform']
    assert {c['category']: c['count'] for c in d['categories']} == {'Power generation': 1, 'Policy and politics': 1}


def test_topic_page(db_conn, monkeypatch):
    pid, people, eps, tags = _setup(db_conn)
    t = _run(db_conn, monkeypatch, 'get_topic', tags['geothermal energy'])
    assert t['totals']['episodes'] == 3 and t['totals']['as_main_topic'] == 2 and t['totals']['guests'] == 1
    assert [g['name'] for g in t['guests']] == ['Ann Lee'] and t['guests'][0]['episodes'] == 3
    assert [h['name'] for h in t['hosts']] == ['Hal Host']
    assert t['shows'][0]['episodes'] == 3 and t['shows'][0]['share'] == 75
    assert [r['year'] for r in t['by_year']] == [2025]
    assert t['recent_episodes'][0]['guests'][0]['name'] == 'Ann Lee'
    # permitting shares only episode 1 with geothermal: below the related threshold.
    assert t['related'] == []


def test_not_a_topic_is_hidden(db_conn, monkeypatch):
    from fastapi import HTTPException
    pid, people, eps, tags = _setup(db_conn)
    cur = db_conn.cursor()
    cur.execute("UPDATE tags SET not_a_topic = true WHERE tag_id = %s", (tags['permitting reform'],))
    db_conn.commit()
    with pytest.raises(HTTPException):
        _run(db_conn, monkeypatch, 'get_topic', tags['permitting reform'])
    assert [r['name'] for r in _run(db_conn, monkeypatch, 'topic_directory')['rows']] == ['geothermal energy']


def test_company_tags_leave_topics_for_the_org_page(db_conn, monkeypatch):
    """A company tag drops out of the topic lists and page, shows on its
    episodes as a company, and lists those episodes on the org's page."""
    from fastapi import HTTPException
    pid, people, eps, tags = _setup(db_conn)
    cur = db_conn.cursor()
    cur.execute("INSERT INTO organizations (name) VALUES ('Fervo Energy') RETURNING org_id")
    org = cur.fetchone()[0]
    db_conn.commit()
    r = _run(db_conn, monkeypatch, 'update_topic', tags['permitting reform'],
             main_body(is_company=True, org_id=org))
    assert (r['topic']['is_company'], r['topic']['org_name']) == (True, 'Fervo Energy')
    with pytest.raises(HTTPException):
        _run(db_conn, monkeypatch, 'get_topic', tags['permitting reform'])
    assert [r['name'] for r in _run(db_conn, monkeypatch, 'topic_directory')['rows']] == ['geothermal energy']
    admin = _run(db_conn, monkeypatch, 'list_topics_admin', view='company')
    assert [t['name'] for t in admin['items']] == ['permitting reform']
    assert admin['totals']['company'] == 1
    assert 'permitting reform' not in [t['name'] for t in
                                      _run(db_conn, monkeypatch, 'list_topics_admin')['items']]

    p = _run(db_conn, monkeypatch, 'get_person_profile', people['Bo'])
    ep4 = p['appearances'][0]
    assert [t['name'] for t in ep4['topics']] == ['oil prices']
    assert [(c['name'], c['org_id']) for c in ep4['companies']] == [('Fervo Energy', org)]
    o = _run(db_conn, monkeypatch, 'get_org_profile', org)
    assert o['discussed_in']['episodes'] == 2
    assert [e['episode_id'] for e in o['discussed_in']['recent']] == [eps[3], eps[0]]

    # Once Bo is credited as a Fervo guest on episode 4, it's Fervo talking,
    # not Fervo being discussed: it leaves the org page and the episode chips.
    cur.execute("INSERT INTO organization_aliases (org_id, alias_name, normalized_name) VALUES (%s, 'Fervo', 'fervo')",
                (org,))
    cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title, company, company_key) "
                "VALUES (%s, %s, 'CEO', 'Fervo', 'fervo')", (eps[3], people['Bo']))
    db_conn.commit()
    o = _run(db_conn, monkeypatch, 'get_org_profile', org)
    assert [e['episode_id'] for e in o['discussed_in']['recent']] == [eps[0]]
    assert _run(db_conn, monkeypatch, 'get_person_profile', people['Bo'])['appearances'][0]['companies'] == []


def test_person_tags_leave_topics_for_the_person_page(db_conn, monkeypatch):
    """A person tag drops out of the topics, shows on its episodes as a
    person, lists those episodes on their page, and follows a merge."""
    from fastapi import HTTPException
    pid, people, eps, tags = _setup(db_conn)
    r = _run(db_conn, monkeypatch, 'update_topic', tags['oil prices'],
             main_body(is_person=True, host_id=people['Ann']))
    assert (r['topic']['is_person'], r['topic']['host_name']) == (True, 'Ann Lee')
    with pytest.raises(HTTPException):
        _run(db_conn, monkeypatch, 'get_topic', tags['oil prices'])
    assert _run(db_conn, monkeypatch, 'list_topics_admin', view='person')['totals']['person'] == 1
    bo = _run(db_conn, monkeypatch, 'get_person_profile', people['Bo'])
    assert [t['name'] for t in bo['appearances'][0]['topics']] == ['permitting reform']
    assert [(m['name'], m['host_id']) for m in bo['appearances'][0]['people_mentioned']] == [('Ann Lee', people['Ann'])]
    ann = _run(db_conn, monkeypatch, 'get_person_profile', people['Ann'])
    assert [e['episode_id'] for e in ann['discussed_in']['recent']] == [eps[3]]
    # Hal hosts every episode, so a tag about Hal is never "discussing" him.
    _run(db_conn, monkeypatch, 'update_topic', tags['permitting reform'],
         main_body(is_person=True, host_id=people['Hal']))
    assert _run(db_conn, monkeypatch, 'get_person_profile', people['Hal'])['discussed_in']['episodes'] == 0
    assert all(not m['host_id'] == people['Hal'] for a in bo['appearances'] for m in
               _run(db_conn, monkeypatch, 'get_person_profile', people['Bo'])['appearances'][0]['people_mentioned'])
    cur = db_conn.cursor()
    cur.execute("INSERT INTO hosts (first_name, last_name) VALUES ('Ann', 'Lee-Smith') RETURNING host_id")
    dup = cur.fetchone()[0]
    cur.execute("UPDATE tags SET host_id = %s WHERE tag_id = %s", (dup, tags['oil prices']))
    db_conn.commit()
    _run(db_conn, monkeypatch, 'merge_people', people['Ann'], dup)
    cur.execute("SELECT host_id FROM tags WHERE tag_id = %s", (tags['oil prices'],))
    assert cur.fetchone()[0] == people['Ann']


def main_body(**kw):
    import main
    return main.TopicUpdateRequest(**kw)


def test_person_profile_talks_about(db_conn, monkeypatch):
    pid, people, eps, tags = _setup(db_conn)
    p = _run(db_conn, monkeypatch, 'get_person_profile', people['Ann'])
    assert p['talks_about']['tagged_episodes'] == 3
    assert [(t['name'], t['episodes'], t['share']) for t in p['talks_about']['topics']] == [('geothermal energy', 3, 100)]
    ep3 = next(a for a in p['appearances'] if a['episode_id'] == eps[2])
    assert [t['name'] for t in ep3['topics']] == ['geothermal', 'geothermal energy']


def test_show_profile_covers(db_conn, monkeypatch):
    pid, people, eps, tags = _setup(db_conn)
    s = _run(db_conn, monkeypatch, 'get_show_profile', pid)
    assert s['covers']['tagged_episodes'] == 4
    assert [t['name'] for t in s['covers']['topics']] == ['geothermal energy', 'permitting reform']
    assert s['recent_episodes'][0]['topics'][0]['name'] == 'oil prices'


def test_people_directory_topic_filter(db_conn, monkeypatch):
    pid, people, eps, tags = _setup(db_conn)
    d = _run(db_conn, monkeypatch, 'people_directory', topic=tags['oil prices'])
    assert sorted(r['name'] for r in d['rows']) == ['Bo Diaz', 'Hal Host']


def test_merge_moves_episodes_and_spellings(db_conn, monkeypatch):
    pid, people, eps, tags = _setup(db_conn)
    r = _run(db_conn, monkeypatch, 'merge_topics', tags['geothermal energy'], tags['geothermal'])
    assert r['aliases_moved'] == 1
    cur = db_conn.cursor()
    cur.execute("SELECT episode_id, is_primary FROM episode_tag WHERE tag_id = %s ORDER BY episode_id",
                (tags['geothermal energy'],))
    # Episode 3 was on both; it is now the main topic because the dropped tag was.
    assert cur.fetchall() == [(eps[0], True), (eps[1], True), (eps[2], True)]
    cur.execute("SELECT COUNT(*) FROM tags WHERE tag_id = %s", (tags['geothermal'],))
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT tag_id FROM tag_aliases WHERE normalized_name = 'geothermal'")
    assert cur.fetchone()[0] == tags['geothermal energy']


def test_rename_adds_spelling_and_refuses_another_topics_name(db_conn, monkeypatch):
    from fastapi import HTTPException
    pid, people, eps, tags = _setup(db_conn)
    import main
    r = _run(db_conn, monkeypatch, 'update_topic', tags['oil prices'],
             main.TopicUpdateRequest(name='Oil Price', category='Finance and markets'))
    assert r['topic']['name'] == 'Oil Price' and r['topic']['category'] == 'Finance and markets'
    cur = db_conn.cursor()
    cur.execute("SELECT alias_name FROM tag_aliases WHERE tag_id = %s ORDER BY alias_name", (tags['oil prices'],))
    assert [a for (a,) in cur.fetchall()] == ['Oil Price', 'oil prices']
    with pytest.raises(HTTPException) as e:
        _run(db_conn, monkeypatch, 'update_topic', tags['oil prices'], main.TopicUpdateRequest(name='Geothermal'))
    assert e.value.status_code == 409
    with pytest.raises(HTTPException):
        _run(db_conn, monkeypatch, 'update_topic', tags['oil prices'], main.TopicUpdateRequest(category='Nope'))
