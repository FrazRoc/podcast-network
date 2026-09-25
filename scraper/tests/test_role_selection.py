"""
Tests for backend/role_selection.py — which one role a person's panels show,
out of every role host_affiliations has on record for them — and for the
backend query that feeds it.
"""
import os
import sys
from datetime import date

import pytest
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))

from role_selection import pick_current_role, display_title, format_for_display  # noqa: E402


def row(episode_id, published, title=None, company=None, kind=None, former=False, other=False,
        podcast=None, org=None, top=None):
    return {'episode_id': episode_id, 'published_date': published, 'title': title,
            'company': company, 'title_kind': kind, 'is_former': former,
            'from_other_episode': other, 'podcast_id': podcast, 'org_id': org, 'top_org_id': top}


def shown(rows):
    chosen = pick_current_role(rows)
    return chosen and (chosen['title'], chosen['company'])


class TestPickCurrentRole:
    def test_nothing_on_record(self):
        assert pick_current_role([]) is None

    def test_most_recent_appearance_wins(self):
        rows = [row(1, date(2021, 3, 1), 'VP of Product', 'Sunrun', 'position'),
                row(2, date(2025, 1, 1), 'CEO', 'Acme Grid', 'position')]
        assert pick_current_role(rows)['company'] == 'Acme Grid'

    def test_former_roles_never_shown(self):
        # David Crane: "the former CEO of NRG. Today, David is a clean energy investor."
        rows = [row(1, date(2025, 1, 1), 'CEO', 'NRG', 'position', former=True),
                row(1, date(2025, 1, 1), 'clean energy investor', None, 'description')]
        assert pick_current_role(rows)['title'] == 'clean energy investor'

    def test_only_former_roles_means_nothing_to_show(self):
        assert pick_current_role([row(1, date(2025, 1, 1), 'CFO', 'Shell', 'position', former=True)]) is None

    def test_other_episode_references_are_skipped(self):
        # A "past episodes you'll love" list naming an old role, attached to a new episode.
        rows = [row(1, date(2022, 1, 1), 'CEO', 'Acme', 'position'),
                row(2, date(2025, 1, 1), 'Founder', 'OldCo', 'position', other=True)]
        assert pick_current_role(rows)['company'] == 'Acme'

    def test_within_an_appearance_position_with_org_beats_the_rest(self):
        rows = [row(1, date(2025, 1, 1), None, 'Wunder'),
                row(1, date(2025, 1, 1), 'writer and investor', None, 'description'),
                row(1, date(2025, 1, 1), 'CEO', None, 'position'),
                row(1, date(2025, 1, 1), 'CEO', 'Wunder', 'position')]
        chosen = pick_current_role(rows)
        assert (chosen['title'], chosen['company']) == ('CEO', 'Wunder')

    def test_newer_description_beats_older_position(self):
        # The agreed rule: recency first, then the kind of title.
        rows = [row(1, date(2019, 1, 1), 'CEO', 'NRG', 'position'),
                row(2, date(2025, 1, 1), 'clean energy investor', None, 'description')]
        assert pick_current_role(rows)['title'] == 'clean energy investor'

    def test_missing_date_counts_as_oldest(self):
        rows = [row(9, None, 'CEO', 'Undated', 'position'),
                row(1, date(2020, 1, 1), 'CEO', 'Dated', 'position')]
        assert pick_current_role(rows)['company'] == 'Dated'

    # --- Sep 2026: fuller roles from earlier appearances (Evan's review) ---

    def test_title_from_an_earlier_appearance_at_the_same_company(self):
        # Julian Spector: the newest show names only Canary Media.
        rows = [row(3, date(2026, 3, 14), None, 'Canary Media', org=5),
                row(2, date(2025, 8, 22), 'Senior Reporter', 'Canary Media', 'position', org=5),
                row(1, date(2019, 10, 7), 'Staff Writer', 'Greentech Media', 'position', org=6)]
        assert shown(rows) == ('Senior Reporter', 'Canary Media')

    def test_sub_organisations_count_as_the_same_employer(self):
        # Akshat Rathi: "Bloomberg" now, "senior climate reporter at Bloomberg News" before.
        rows = [row(2, date(2026, 6, 12), None, 'Bloomberg L.P.', org=1, top=1),
                row(1, date(2026, 5, 10), 'senior climate reporter', 'Bloomberg News', 'position', org=2, top=1)]
        assert shown(rows) == ('senior climate reporter', 'Bloomberg News')

    def test_company_borrowed_from_the_same_show(self):
        # Aurora's analysts on Aurora's podcast: the company goes unsaid.
        rows = [row(2, date(2024, 1, 11), 'Co-Head of Advisory', None, 'position', podcast=9),
                row(1, date(2020, 10, 29), 'Project Leader', 'Aurora Energy Research', 'position', podcast=9, org=3)]
        assert shown(rows) == ('Co-Head of Advisory', 'Aurora Energy Research')

    def test_newest_full_role_beats_a_newer_title_alone(self):
        # Joel Edwards: "Co-founder" (no company) after "co-founder and CTO at Zanskar".
        rows = [row(2, date(2026, 9, 9), 'Co-founder', None, 'position', podcast=1),
                row(1, date(2026, 5, 27), 'co-founder and CTO', 'Zanskar', 'position', podcast=2, org=4)]
        assert shown(rows) == ('co-founder and CTO', 'Zanskar')

    def test_a_bare_word_gives_way_to_a_fuller_title_there(self):
        # Jesse Jenkins: "researcher" once, "professor" before that.
        rows = [row(2, date(2025, 7, 10), 'researcher', 'Princeton University', 'position', org=7),
                row(1, date(2024, 3, 19), 'professor', 'Princeton University', 'position', org=7)]
        assert shown(rows) == ('professor', 'Princeton University')

    def test_title_given_since_goes_with_the_company(self):
        # Alba Forns: Climatize named once, then "COO and co-founder" alone.
        rows = [row(2, date(2025, 5, 29), 'COO and co-founder', None, 'position', podcast=1),
                row(1, date(2023, 10, 26), None, 'Climatize', podcast=2, org=8)]
        chosen = pick_current_role(rows)
        assert (chosen['title'], chosen['company'], chosen['company_inferred']) == ('COO and co-founder', 'Climatize', True)

    def test_relative_clause_is_not_a_title(self):
        rows = [row(2, date(2026, 8, 20), "who lead BNEF's EV teams", None, 'position'),
                row(1, date(2025, 11, 28), 'head of intelligent mobility', 'BloombergNEF', 'position', org=9)]
        assert shown(rows) == ('head of intelligent mobility', 'BloombergNEF')

    def test_other_episode_role_when_nothing_else_is_full(self):
        # Stacey Abrams: only a description otherwise.
        rows = [row(2, date(2026, 9, 17), 'senior counsel', 'Rewiring America', 'position', other=True),
                row(1, date(2024, 7, 12), 'voting rights advocate', None, 'description')]
        assert shown(rows) == ('senior counsel', 'Rewiring America')

    def test_plural_role_made_singular(self):
        from role_selection import tidy_title
        assert tidy_title('Reporters') == 'Reporters'[:-1]
        assert tidy_title('Co-Founders') == 'Co-Founder'
        assert tidy_title('Head of Partnerships') == 'Head of Partnerships'
        assert tidy_title('Senior Analysts') == 'Senior Analyst'

    def test_pin_overrides_everything(self):
        rows = [row(1, date(2025, 1, 1), 'CEO', 'Acme', 'position')]
        chosen = pick_current_role(rows, {'title': 'Partner', 'company': 'Fifth Wall'})
        assert chosen == {'title': 'Partner', 'company': 'Fifth Wall', 'source': 'pinned'}

    def test_empty_pin_is_ignored(self):
        rows = [row(1, date(2025, 1, 1), 'CEO', 'Acme', 'position')]
        assert pick_current_role(rows, {'title': None, 'company': None})['source'] == 'derived'


class TestDisplayTitle:
    """Stored titles stay verbatim; this is only how the current one is shown.
    Real cases from the People list after stage 1."""

    @pytest.mark.parametrize('stored, kind, shown', [
        ('a senior investigative data reporter', 'description', 'Senior investigative data reporter'),
        ('a veteran college baseball coach and leadership consultant', 'description',
         'Veteran college baseball coach and leadership consultant'),
        ('ecologist, political scientist, and author', 'description',
         'Ecologist, political scientist, and author'),
        ('an energy reporter', 'position', 'Energy Reporter'),
        ('the Executive Director', 'position', 'Executive Director'),
        ('senior fellow', 'position', 'Senior Fellow'),
        ('co-founder and CEO', 'position', 'Co-Founder and CEO'),
        ('head of policy at the office of the governor', 'position',
         'Head of Policy at the Office of the Governor'),
        ("director of DOE's loan programs", 'position', "Director of DOE's Loan Programs"),
        ('VP of Grid', 'position', 'VP of Grid'),
        ('Founder & CTO', 'position', 'Founder and CTO'),
        ('Cofounder and CEO', 'position', 'Co-Founder and CEO'),
        ('guest cohost', 'position', 'Guest Co-Host'),
        ('ecologist, writer, and Greenpeace cofounder', 'description',
         'Ecologist, writer, and Greenpeace co-founder'),
    ])
    def test_tidied(self, stored, kind, shown):
        assert display_title(stored, kind) == shown

    @pytest.mark.parametrize('stored, fixed', [
        ('cofounder', 'co-founder'), ('CoFounder', 'Co-Founder'), ('co founder', 'co-founder'),
        ('co–founder', 'co-founder'), ('Coauthors', 'Co-authors'), ('Cochair', 'Co-chair'),
        ('CEO and cofounder', 'CEO and co-founder'), ('co-founder', 'co-founder'),
        # Ordinary words that start with "co" are left alone.
        ('COO and consultant', 'COO and consultant'), ('correspondent', 'correspondent'),
        ('coordinator', 'coordinator'), ('cohort lead', 'cohort lead'),
    ])
    def test_co_roles_hyphenated(self, stored, fixed):
        from role_selection import hyphenate_co
        assert hyphenate_co(stored) == fixed

    @pytest.mark.parametrize('stored, fixed', [
        ('Chief Executive Officer', 'CEO'), ('chief executive officer', 'CEO'), ('Chief Executive', 'CEO'),
        ('co-founder and Chief Technology Officer', 'co-founder and CTO'),
        ('Chairman and Chief Executive Officer (CEO)', 'Chairman and CEO'),
        ('Chief Operating Officer and Chief Technology Officer', 'COO and CTO'),
        ('former chief financial officer', 'former CFO'), ('chief executive officers', 'CEOs'),
        ('co-founder and Chief Executive Offi', 'co-founder and CEO'),   # cut off in the source
        # Other roles, and ambiguous abbreviations, stay as written.
        ('Founder and Chief Executive Director', 'Founder and Chief Executive Director'),
        ('Chief Sustainability Officer', 'Chief Sustainability Officer'),
        ('Chief Commercial Officer', 'Chief Commercial Officer'),
        ('Chief Investment Officer', 'Chief Investment Officer'),
        ('Chief Technology', 'Chief Technology'), ('Chief Scientist', 'Chief Scientist'),
    ])
    def test_chief_titles_abbreviated(self, stored, fixed):
        from role_selection import abbreviate_chiefs
        assert abbreviate_chiefs(stored) == fixed

    @pytest.mark.parametrize('stored, fixed', [
        # 1. vice presidents
        ('Vice President of Policy', 'VP of Policy'), ('Vice-President', 'VP'), ('vice presidents', 'VPs'),
        ('Senior Vice President', 'SVP'), ('Executive Vice President and COO', 'EVP and COO'),
        ('Sr VP of Business Development', 'SVP of Business Development'),
        ('Vice Presidential candidate', 'Vice Presidential candidate'),
        # 2. a spaced ampersand
        ('Founder & CEO', 'Founder and CEO'), ('Head of R&D', 'Head of R&D'), ('M&A lead', 'M&A lead'),
        # 3. short forms
        ('Sr. Director of Marketing', 'Senior Director of Marketing'), ('sr analyst', 'senior analyst'),
        ('HBS Prof.', 'HBS Professor'), ('Assoc. Professor', 'Associate Professor'),
        ('Non-exec director', 'Non-executive director'), ('Dept. Manager', 'Department Manager'),
        ('Ph.D. candidate', 'PhD candidate'), ('direct report', 'direct report'),
        # 4. hyphenated compounds
        ('Secretary General', 'Secretary-General'), ('deputy director general', 'deputy director-general'),
        ('Editor in Chief', 'Editor-in-Chief'),
        # 5. political short forms
        ('Rep. for Ohio', 'Representative for Ohio'), ('Sen. aide', 'Senator aide'), ('Senate aide', 'Senate aide'),
    ])
    def test_title_words_standardised(self, stored, fixed):
        from role_selection import tidy_title
        assert tidy_title(stored) == fixed

    def test_both_tidy_ups_reach_the_display(self):
        assert display_title('cofounder and chief executive officer', 'position') == 'Co-Founder and CEO'

    def test_a_lone_article_is_not_erased(self):
        assert display_title('the', 'position') == 'The'

    def test_empty(self):
        assert display_title(None) is None and display_title('') == ''

    def test_pins_shown_as_typed(self):
        pinned = {'title': 'a partner', 'company': 'x', 'source': 'pinned'}
        assert format_for_display(pinned) == pinned

    def test_derived_role_is_tidied_and_company_left_alone(self):
        role = {'title': 'a senior fellow', 'company': 'the Searchlight Institute',
                'title_kind': 'position', 'source': 'derived'}
        shown = format_for_display(role)
        assert (shown['title'], shown['company']) == ('Senior Fellow', 'the Searchlight Institute')
        assert role['title'] == 'a senior fellow'      # the stored row is not mutated


# ------------------------------------------------------------------
# The backend query, against the disposable test database
# ------------------------------------------------------------------

pytest.importorskip("fastapi")


@pytest.fixture()
def role_db(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT to_regclass('host_affiliations') IS NOT NULL AND to_regclass('host_role_pins') IS NOT NULL")
    if not cur.fetchone()[0]:
        pytest.skip("affiliation/pin migrations not applied — re-run tests/setup_test_db.sh")
    return db_conn


def _appearance(cur, host_id, podcast_id, title, published, other=False):
    cur.execute("INSERT INTO episodes (podcast_id, title, published_date) VALUES (%s, %s, %s) "
                "RETURNING episode_id", (podcast_id, title, published))
    episode_id = cur.fetchone()[0]
    cur.execute("INSERT INTO episode_host (episode_id, host_id, is_guest) VALUES (%s, %s, true)",
                (episode_id, host_id))
    cur.execute("INSERT INTO affiliation_extractions (episode_id, host_id, status, from_other_episode, "
                "appears_on_episode) VALUES (%s, %s, 'done', %s, true)", (episode_id, host_id, other))
    return episode_id


class TestRoleQuery:
    def test_query_feeds_the_rule(self, role_db):
        from main import _role_rows, _role_pin
        cur = role_db.cursor()
        cur.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES ('Show', 'show') RETURNING podcast_id")
        podcast_id = cur.fetchone()[0]
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES ('Jane', 'Doe') RETURNING host_id")
        host_id = cur.fetchone()[0]
        old = _appearance(cur, host_id, podcast_id, 'Old', date(2021, 1, 1))
        new = _appearance(cur, host_id, podcast_id, 'New', date(2025, 1, 1))
        cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title, company, title_kind) VALUES "
                    "(%s, %s, 'VP', 'Sunrun', 'position'), (%s, %s, 'CEO', 'Acme', 'position')",
                    (old, host_id, new, host_id))
        role_db.commit()

        dict_cur = role_db.cursor(cursor_factory=RealDictCursor)
        rows = _role_rows(dict_cur, host_id)
        assert [r['company'] for r in rows] == ['Acme', 'Sunrun']      # newest first
        assert rows[0]['podcast_title'] == 'Show'
        assert pick_current_role(rows, _role_pin(dict_cur, host_id))['company'] == 'Acme'

        cur.execute("INSERT INTO host_role_pins (host_id, title, company) VALUES (%s, 'Partner', 'Fifth Wall')",
                    (host_id,))
        role_db.commit()
        chosen = pick_current_role(_role_rows(dict_cur, host_id), _role_pin(dict_cur, host_id))
        assert (chosen['source'], chosen['company']) == ('pinned', 'Fifth Wall')

    def test_pin_goes_when_the_person_does(self, role_db):
        cur = role_db.cursor()
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES ('Jane', 'Doe') RETURNING host_id")
        host_id = cur.fetchone()[0]
        cur.execute("INSERT INTO host_role_pins (host_id, title) VALUES (%s, 'CEO')", (host_id,))
        cur.execute("DELETE FROM hosts WHERE host_id = %s", (host_id,))
        role_db.commit()
        cur.execute("SELECT count(*) FROM host_role_pins")
        assert cur.fetchone()[0] == 0


class TestPeopleListRoles:
    """The admin People list's role filters and company sort, run through the
    real list_people() query against the test database."""

    def _people(self, cur, podcast_id):
        made = {}
        for first, title, company in [('Ann', 'CEO', 'Zeta'), ('Bob', 'CEO', None),
                                      ('Cat', None, 'Acme'), ('Dan', None, None)]:
            cur.execute("INSERT INTO hosts (first_name, last_name) VALUES (%s, 'Test') RETURNING host_id",
                        (first,))
            host_id = cur.fetchone()[0]
            ep = _appearance(cur, host_id, podcast_id, f'Ep {first}', date(2025, 1, 1))
            if title or company:
                cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title, company, title_kind) "
                            "VALUES (%s, %s, %s, %s, %s)",
                            (ep, host_id, title, company, 'position' if title else None))
            made[first] = host_id
        return made

    def _list(self, role_db, monkeypatch, **kw):
        import asyncio
        import main
        monkeypatch.setattr(main, 'get_db_connection',
                            lambda: __import__('psycopg2').connect(
                                role_db.dsn, cursor_factory=RealDictCursor))
        return asyncio.run(main.list_people(**kw))['items']

    def test_filters_and_sort(self, role_db, monkeypatch):
        cur = role_db.cursor()
        cur.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES ('Show', 'show') RETURNING podcast_id")
        made = self._people(cur, cur.fetchone()[0])
        # A pin counts as the current role too.
        cur.execute("INSERT INTO host_role_pins (host_id, title, company) VALUES (%s, 'Partner', 'Beta')",
                    (made['Dan'],))
        role_db.commit()

        names = lambda items: sorted(i['first_name'] for i in items)
        assert names(self._list(role_db, monkeypatch, filter='role_title_company')) == ['Ann', 'Dan']
        assert names(self._list(role_db, monkeypatch, filter='role_title_only')) == ['Bob']
        assert names(self._list(role_db, monkeypatch, filter='role_company_only')) == ['Cat']
        # Dan has a pin, so nobody here lacks both.
        assert names(self._list(role_db, monkeypatch, filter='role_none')) == []

        by_company = self._list(role_db, monkeypatch, sort='company_asc')
        assert [i['first_name'] for i in by_company] == ['Cat', 'Dan', 'Ann', 'Bob']   # Acme, Beta, Zeta, none
        ann = next(i for i in by_company if i['first_name'] == 'Ann')
        assert (ann['current_title'], ann['current_company']) == ('CEO', 'Zeta')

    def test_total_counts_the_filtered_list(self, role_db, monkeypatch):
        # total used to count everyone matching the name search, ignoring the
        # role filter; all_total is everyone.
        import asyncio
        import main
        cur = role_db.cursor()
        cur.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES ('Show', 'show') RETURNING podcast_id")
        self._people(cur, cur.fetchone()[0])
        role_db.commit()
        self._list(role_db, monkeypatch)   # points main at the test database
        r = asyncio.run(main.list_people(filter='role_title_only'))
        assert (r['total'], r['all_total']) == (1, 4)
        r = asyncio.run(main.list_people())
        assert (r['total'], r['all_total']) == (4, 4)
        assert [i['first_name'] for i in asyncio.run(main.list_people(filter='role_none'))['items']] == ['Dan']
        # Pages never overlap or skip, even though all four tie on appearances.
        pages = [asyncio.run(main.list_people(limit=2, offset=o))['items'] for o in (0, 2, 4)]
        assert sorted(i['first_name'] for p in pages for i in p) == ['Ann', 'Bob', 'Cat', 'Dan']
        assert pages[2] == []
        assert asyncio.run(main.list_people(limit=2))['total'] == 4   # total ignores the page size

    def test_list_shows_the_tidied_title(self, role_db, monkeypatch):
        cur = role_db.cursor()
        cur.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES ('Show', 'show') RETURNING podcast_id")
        podcast_id = cur.fetchone()[0]
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES ('Siduja', 'Test') RETURNING host_id")
        host_id = cur.fetchone()[0]
        ep = _appearance(cur, host_id, podcast_id, 'Ep', date(2025, 1, 1))
        cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title, title_kind) VALUES "
                    "(%s, %s, 'a senior investigative data reporter', 'description')", (ep, host_id))
        role_db.commit()
        [row] = self._list(role_db, monkeypatch, filter='role_title_only')
        assert row['current_title'] == 'Senior investigative data reporter'

    def test_list_matches_the_panel(self, role_db):
        # The list and a person's panel must never disagree about their role.
        from main import _all_current_roles, _role_rows, _role_pin
        cur = role_db.cursor()
        cur.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES ('Show', 'show') RETURNING podcast_id")
        podcast_id = cur.fetchone()[0]
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES ('Jane', 'Doe') RETURNING host_id")
        host_id = cur.fetchone()[0]
        ep = _appearance(cur, host_id, podcast_id, 'Ep', date(2025, 1, 1))
        # Two equally ranked roles in one appearance: the tie must break the same way.
        cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title, company, title_kind) VALUES "
                    "(%s, %s, 'CEO', 'Acme', 'position'), (%s, %s, 'Chair', 'Beta', 'position')",
                    (ep, host_id, ep, host_id))
        role_db.commit()
        dict_cur = role_db.cursor(cursor_factory=RealDictCursor)
        panel = format_for_display(pick_current_role(_role_rows(dict_cur, host_id), _role_pin(dict_cur, host_id)))
        assert _all_current_roles(dict_cur)[host_id][:2] == (panel['title'], panel['company'])
