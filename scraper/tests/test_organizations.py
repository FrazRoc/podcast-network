"""
Tests for organisations: the name key (backend/org_names.py), the sync that
creates organisations from extracted company names (organizations.py), the
merge-suggestion logic, and the Company Admin endpoints against the
disposable test database. Examples are real spellings from the first
~1,400 extracted appearances.
"""
import asyncio
import os
import sys
from datetime import date

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))

from org_names import normalize_org_name, initials, looks_like_acronym_of  # noqa: E402
from organizations import pick_display_name, plan_sync, apply_sync  # noqa: E402


class TestNormalizeOrgName:
    @pytest.mark.parametrize('a, b', [
        ("BloombergNEF's", 'BloombergNEF'),
        ('the Searchlight Institute', 'Searchlight Institute'),
        ('E&E News', 'E & E News'),
        ('Acme, Inc.', 'Acme'),
        ('Acme LLC', 'ACME'),
        ('Microsoft’s', "Microsoft's"),
        ('World Wildlife Fund-US', 'World Wildlife Fund US'),
    ])
    def test_same_key(self, a, b):
        assert normalize_org_name(a) == normalize_org_name(b)

    @pytest.mark.parametrize('a, b', [
        ('BNEF', 'BloombergNEF'),                 # a merge for a person to confirm, not a key match
        ('Bloomberg', 'Bloomberg Green'),
        ('The Metals Company', 'Metals'),         # "Company" is part of the name
        ('Rhodium Group', 'Rhodium'),
    ])
    def test_different_key(self, a, b):
        assert normalize_org_name(a) != normalize_org_name(b)

    def test_suffix_alone_is_kept(self):
        # A one-word name that happens to be a suffix is still a name.
        assert normalize_org_name('Co') == 'co'

    def test_empty(self):
        assert normalize_org_name(None) is None and normalize_org_name('  ') is None


class TestAcronyms:
    def test_initials_skip_small_words(self):
        assert initials('Bloomberg New Energy Finance') == 'bnef'
        assert initials('Center on Global Energy Policy') == 'cgep'
        assert initials('Tesla') is None

    @pytest.mark.parametrize('short, long, expected', [
        ('BNEF', 'Bloomberg New Energy Finance', True),
        ('NREL', 'National Renewable Energy Laboratory', True),
        ('CGEP', 'Center on Global Energy Policy', True),
        ('BNEF', 'BloombergNEF', False),           # run-together: trigram similarity's job
        ('Acme', 'Acme Corp Energy Markets', False),
        ('US', 'University of Singapore', False),   # two letters: too loose
        ('RE +', 'Reneu Energy', False),
    ])
    def test_acronym(self, short, long, expected):
        assert looks_like_acronym_of(short, long) is expected


class TestPickDisplayName:
    def test_most_common_wins(self):
        assert pick_display_name(['BNEF', 'BNEF', 'Bnef']) == 'BNEF'

    def test_tie_prefers_brand_styling(self):
        assert pick_display_name(['bloombergnef', 'BloombergNEF']) == 'BloombergNEF'

    @pytest.mark.parametrize('spellings, expected', [
        (['Carbon Engineering', 'Carbon Engineering Ltd.'], 'Carbon Engineering'),
        (['All We Can Save Project', 'All We Can Save project', 'The All We Can Save Project'],
         'All We Can Save Project'),
        (['the World Resources Institute', 'World Resources Institute'], 'World Resources Institute'),
        (['NVIDIA', 'Nvidia'], 'NVIDIA'),
    ])
    def test_tie_prefers_bare_name(self, spellings, expected):
        # Real ties from the first ~1,400 appearances.
        assert pick_display_name(spellings) == expected


class TestSuggestionPairs:
    def _orgs(self, **named):
        return {i: {'org_id': i, 'name': n, 'parent_org_id': None, 'not_an_org': False, 'people': 1}
                for i, n in named.items()}

    def _pairs(self, orgs, similar=(), not_same=()):
        pytest.importorskip("fastapi")
        from main import _suggestion_pairs
        return _suggestion_pairs(orgs, list(similar), set(not_same))

    def test_acronym_similar_and_contains(self):
        orgs = {1: {'org_id': 1, 'name': 'BNEF', 'parent_org_id': None, 'not_an_org': False, 'people': 4},
                2: {'org_id': 2, 'name': 'Bloomberg New Energy Finance', 'parent_org_id': None,
                    'not_an_org': False, 'people': 1},
                3: {'org_id': 3, 'name': 'Bloomberg', 'parent_org_id': None, 'not_an_org': False, 'people': 1},
                4: {'org_id': 4, 'name': 'Bloomberg Green', 'parent_org_id': None, 'not_an_org': False, 'people': 1}}
        pairs = {(p['org_a'], p['org_b']): p['reason'] for p in self._pairs(orgs)}
        assert pairs[(1, 2)] == 'acronym'
        assert pairs[(3, 4)] == 'contains'
        assert pairs[(2, 3)] == 'contains'

    def test_strongest_reason_kept(self):
        orgs = self._orgs(**{'1': 'BNEF', '2': 'Bloomberg New Energy Finance'})
        orgs = {int(k): {**v, 'org_id': int(k)} for k, v in orgs.items()}
        [pair] = self._pairs(orgs, similar=[(1, 2, 0.6)])
        assert pair['reason'] == 'acronym'

    def test_not_same_parent_child_and_not_org_are_skipped(self):
        orgs = {1: {'org_id': 1, 'name': 'Bloomberg', 'parent_org_id': None, 'not_an_org': False, 'people': 1},
                2: {'org_id': 2, 'name': 'Bloomberg Green', 'parent_org_id': 1, 'not_an_org': False, 'people': 1},
                3: {'org_id': 3, 'name': 'Bloomberg Law', 'parent_org_id': None, 'not_an_org': False, 'people': 1},
                4: {'org_id': 4, 'name': 'Bloomberg Media', 'parent_org_id': None, 'not_an_org': True, 'people': 1}}
        pairs = {(p['org_a'], p['org_b']) for p in self._pairs(orgs, not_same=[(1, 3)])}
        assert pairs == set()

    def test_ranked_by_people_affected(self):
        orgs = {1: {'org_id': 1, 'name': 'Tesla', 'parent_org_id': None, 'not_an_org': False, 'people': 1},
                2: {'org_id': 2, 'name': 'Tesla Energy', 'parent_org_id': None, 'not_an_org': False, 'people': 1},
                3: {'org_id': 3, 'name': 'Aurora', 'parent_org_id': None, 'not_an_org': False, 'people': 5},
                4: {'org_id': 4, 'name': 'Aurora Energy Research', 'parent_org_id': None,
                    'not_an_org': False, 'people': 3}}
        assert [(p['org_a'], p['org_b']) for p in self._pairs(orgs)][0] == (3, 4)


# ------------------------------------------------------------------
# Against the disposable test database
# ------------------------------------------------------------------

@pytest.fixture()
def org_db(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT to_regclass('organizations') IS NOT NULL")
    if not cur.fetchone()[0]:
        pytest.skip("migrate_add_organizations.sql not applied — re-run tests/setup_test_db.sh")
    # conftest's reset cascades from hosts/episodes, which never reaches these.
    cur.execute("TRUNCATE not_same_org_pairs, organization_aliases, organizations RESTART IDENTITY CASCADE")
    db_conn.commit()
    return db_conn


def _role(cur, first, company, title='CEO', published=date(2025, 1, 1)):
    cur.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES ('Show', 'show') ON CONFLICT DO NOTHING")
    cur.execute("SELECT podcast_id FROM podcasts WHERE apple_podcast_id = 'show'")
    podcast_id = cur.fetchone()[0]
    cur.execute("SELECT host_id FROM hosts WHERE first_name = %s AND last_name = 'Test'", (first,))
    row = cur.fetchone()
    if row:
        host_id = row[0]
    else:
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES (%s, 'Test') RETURNING host_id", (first,))
        host_id = cur.fetchone()[0]
    cur.execute("INSERT INTO episodes (podcast_id, title, published_date) VALUES (%s, %s, %s) RETURNING episode_id",
                (podcast_id, f'{first} {company} {published}', published))
    episode_id = cur.fetchone()[0]
    cur.execute("INSERT INTO episode_host (episode_id, host_id, is_guest) VALUES (%s, %s, true)", (episode_id, host_id))
    cur.execute("INSERT INTO affiliation_extractions (episode_id, host_id, status, appears_on_episode, "
                "from_other_episode) VALUES (%s, %s, 'done', true, false)", (episode_id, host_id))
    cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title, company, title_kind) "
                "VALUES (%s, %s, %s, %s, 'position')", (episode_id, host_id, title, company))
    return host_id


def _run(org_db, monkeypatch, fn, *args, **kw):
    pytest.importorskip("fastapi")
    import main
    monkeypatch.setattr(main, 'get_db_connection',
                        lambda: psycopg2.connect(org_db.dsn, cursor_factory=RealDictCursor))
    return asyncio.run(getattr(main, fn)(*args, **kw))


def _org_id(cur, name):
    cur.execute("SELECT org_id FROM organizations WHERE name = %s", (name,))
    return cur.fetchone()[0]


class TestSync:
    def test_creates_one_org_per_key_and_is_idempotent(self, org_db):
        cur = org_db.cursor()
        _role(cur, 'Ann', 'BloombergNEF')
        _role(cur, 'Bob', "BloombergNEF's")
        _role(cur, 'Cat', 'BNEF')
        org_db.commit()

        plan = plan_sync(cur)
        assert len(plan['to_stamp']) == 3
        assert sorted(n for _, n in plan['new_orgs']) == ['BNEF', 'BloombergNEF']
        apply_sync(cur, plan)
        org_db.commit()

        again = plan_sync(cur)
        assert again == {'to_stamp': [], 'new_orgs': []}
        cur.execute("SELECT COUNT(*) FROM host_affiliations WHERE company_key IS NULL")
        assert cur.fetchone()[0] == 0

    def test_new_spelling_of_known_org_links_without_a_new_org(self, org_db):
        cur = org_db.cursor()
        _role(cur, 'Ann', 'Acme')
        apply_sync(cur, plan_sync(cur))
        _role(cur, 'Bob', 'ACME Inc.')
        org_db.commit()
        plan = plan_sync(cur)
        assert plan['new_orgs'] == [] and len(plan['to_stamp']) == 1


class TestCompanyEndpoints:
    def _setup(self, org_db):
        cur = org_db.cursor()
        self.ann = _role(cur, 'Ann', 'BNEF', 'Analyst')
        self.bob = _role(cur, 'Bob', 'BloombergNEF', 'Head of Research')
        self.cat = _role(cur, 'Cat', 'Bloomberg Green', 'Reporter')
        # Dan used to be at BNEF; his current role is elsewhere.
        self.dan = _role(cur, 'Dan', 'BNEF', 'Analyst', date(2020, 1, 1))
        _role(cur, 'Dan', 'Acme', 'CEO', date(2025, 6, 1))
        apply_sync(cur, plan_sync(cur))
        org_db.commit()
        return cur

    def test_merge_relinks_every_role(self, org_db, monkeypatch):
        cur = self._setup(org_db)
        bnef, bloombergnef = _org_id(cur, 'BNEF'), _org_id(cur, 'BloombergNEF')
        result = _run(org_db, monkeypatch, 'merge_companies', bloombergnef, bnef)
        assert result['aliases_moved'] == 1
        detail = _run(org_db, monkeypatch, 'get_company', bloombergnef)
        assert {p['name'] for p in detail['people']} == {'Ann Test', 'Bob Test', 'Dan Test'}
        # Dan's current role is at Acme, so he is listed but not current.
        current = {p['name']: p['is_current'] for p in detail['people']}
        assert current == {'Ann Test': True, 'Bob Test': True, 'Dan Test': False}
        assert {a['source'] for a in detail['aliases']} == {'auto', 'merge'}

    def test_future_extractions_of_a_merged_spelling_link_too(self, org_db, monkeypatch):
        cur = self._setup(org_db)
        _run(org_db, monkeypatch, 'merge_companies', _org_id(cur, 'BloombergNEF'), _org_id(cur, 'BNEF'))
        _role(cur, 'Eve', 'bnef', 'Analyst')
        org_db.commit()
        assert plan_sync(cur)['new_orgs'] == []

    def test_parent_and_sub_org_people(self, org_db, monkeypatch):
        cur = self._setup(org_db)
        cur.execute("INSERT INTO organizations (name) VALUES ('Bloomberg') RETURNING org_id")
        bloomberg = cur.fetchone()[0]
        org_db.commit()
        green = _org_id(cur, 'Bloomberg Green')
        _run(org_db, monkeypatch, 'update_company', green, main_body(parent_org_id=bloomberg, org_type='media'))
        flat = _run(org_db, monkeypatch, 'get_company', bloomberg)
        assert flat['people'] == [] and [c['name'] for c in flat['children']] == ['Bloomberg Green']
        rolled_up = _run(org_db, monkeypatch, 'get_company', bloomberg, include_sub=True)
        assert [p['name'] for p in rolled_up['people']] == ['Cat Test']

    def test_parent_cycle_refused(self, org_db, monkeypatch):
        from fastapi import HTTPException
        cur = self._setup(org_db)
        a, b = _org_id(cur, 'BNEF'), _org_id(cur, 'BloombergNEF')
        _run(org_db, monkeypatch, 'update_company', b, main_body(parent_org_id=a))
        with pytest.raises(HTTPException) as e:
            _run(org_db, monkeypatch, 'update_company', a, main_body(parent_org_id=b))
        assert e.value.status_code == 400

    def test_list_counts_views_and_not_an_org(self, org_db, monkeypatch):
        cur = self._setup(org_db)
        listing = _run(org_db, monkeypatch, 'list_companies')
        counts = {i['name']: i['people'] for i in listing['items']}
        assert counts['BNEF'] == 2 and counts['BloombergNEF'] == 1
        assert listing['totals']['active'] == 4 and listing['totals']['untyped'] == 4

        _run(org_db, monkeypatch, 'update_company', _org_id(cur, 'Acme'), main_body(not_an_org=True))
        names = {i['name'] for i in _run(org_db, monkeypatch, 'list_companies')['items']}
        assert 'Acme' not in names
        hidden = _run(org_db, monkeypatch, 'list_companies', view='not_org')['items']
        assert [i['name'] for i in hidden] == ['Acme']

    def test_domain_is_cleaned(self, org_db, monkeypatch):
        cur = self._setup(org_db)
        acme = _org_id(cur, 'Acme')
        _run(org_db, monkeypatch, 'update_company', acme, main_body(website_domain='https://www.acme.com/about'))
        cur.execute("SELECT website_domain FROM organizations WHERE org_id = %s", (acme,))
        assert cur.fetchone()[0] == 'acme.com'

    def test_suggestions_and_not_same(self, org_db, monkeypatch):
        cur = self._setup(org_db)
        cur.execute("INSERT INTO organizations (name) VALUES ('Bloomberg New Energy Finance')")
        org_db.commit()
        pairs = _run(org_db, monkeypatch, 'company_merge_suggestions')['items']
        by_names = {frozenset((p['a']['name'], p['b']['name'])): p for p in pairs}
        acronym = by_names[frozenset(('BNEF', 'Bloomberg New Energy Finance'))]
        assert acronym['reason'] == 'acronym'

        _run(org_db, monkeypatch, 'mark_companies_not_same',
             main_body_cls('NotSameOrgRequest', org_a=acronym['org_a'], org_b=acronym['org_b']))
        pairs = _run(org_db, monkeypatch, 'company_merge_suggestions')['items']
        assert frozenset(('BNEF', 'Bloomberg New Energy Finance')) not in {
            frozenset((p['a']['name'], p['b']['name'])) for p in pairs}


def main_body(**kw):
    return main_body_cls('CompanyUpdateRequest', **kw)


def main_body_cls(cls, **kw):
    import main
    return getattr(main, cls)(**kw)
