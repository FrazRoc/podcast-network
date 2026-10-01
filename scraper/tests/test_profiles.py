"""
Tests for backend/profiles.py — the pure helpers behind the public person,
organisation and show pages.
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'backend'))

from profiles import slugify, apple_episode_url, apple_show_url, condense_career, recent_cadence, described_as  # noqa: E402


class TestSlugify:
    def test_accents_and_icelandic_letters(self):
        assert slugify("Ólafur Teitur Guðnason") == "olafur-teitur-gudnason"

    def test_punctuation(self):
        assert slugify("Tamara Toles O’Laughlin") == "tamara-toles-o-laughlin"
        assert slugify("E&E News") == "e-e-news"

    def test_never_empty(self):
        assert slugify("") == "page"
        assert slugify("——") == "page"


class TestAppleUrls:
    def test_episode(self):
        assert apple_episode_url("1548554104", "1000612345") == \
            "https://podcasts.apple.com/podcast/id1548554104?i=1000612345"

    def test_rss_episode_falls_back_to_show(self):
        assert apple_episode_url("1548554104", None) == apple_show_url("1548554104")

    def test_no_show_id(self):
        assert apple_episode_url(None, "1") is None


class TestCondenseCareer:
    ROWS = [
        {'title': 'Co-founder & CEO', 'company': 'Fervo Energy', 'org_id': 7, 'is_former': False,
         'title_kind': 'position', 'published_date': date(2025, 5, 1)},
        {'title': 'CEO', 'company': 'Fervo Energy', 'org_id': 7, 'is_former': False,
         'title_kind': 'position', 'published_date': date(2023, 1, 1)},
        {'title': 'co-founder & CEO', 'company': 'Fervo Energy', 'org_id': 7, 'is_former': False,
         'title_kind': 'position', 'published_date': date(2022, 3, 1)},
        {'title': 'Engineer', 'company': 'Shell', 'org_id': 9, 'is_former': True,
         'title_kind': 'position', 'published_date': date(2024, 1, 1)},
        {'title': None, 'company': None, 'org_id': None, 'is_former': False,
         'title_kind': None, 'published_date': date(2024, 1, 1)},
        {'title': 'petroleum geologist with decades of experience', 'company': None, 'org_id': None,
         'is_former': False, 'title_kind': 'description', 'published_date': date(2026, 1, 1)},
    ]

    def test_groups_by_title_and_org_with_date_range(self):
        items = condense_career(self.ROWS)
        ceo = next(i for i in items if i['title'] == 'Co-Founder and CEO')
        assert ceo['mentions'] == 2
        assert ceo['first_date'] == date(2022, 3, 1) and ceo['last_date'] == date(2025, 5, 1)

    def test_former_only_when_every_mention_says_so(self):
        items = condense_career(self.ROWS)
        assert next(i for i in items if i['company'] == 'Shell')['former'] is True
        assert all(not i['former'] for i in items if i['company'] == 'Fervo Energy')

    def test_empty_rows_skipped(self):
        assert all(i['title'] or i['company'] for i in condense_career(self.ROWS))

    def test_current_role_first(self):
        items = condense_career(self.ROWS, {'title': 'CEO', 'company': 'Fervo Energy', 'org_id': 7})
        assert items[0]['current'] and items[0]['title'] == 'CEO'

    def test_descriptions_are_not_jobs(self):
        assert all('geologist' not in (i['title'] or '') for i in condense_career(self.ROWS))
        assert described_as(self.ROWS) == ['Petroleum geologist with decades of experience']


def test_recent_cadence():
    months = [(date(2025, m, 1), 4) for m in range(1, 13)] + [(date(2026, 1, 1), 100)]
    assert recent_cadence(months, today=date(2026, 1, 15)) == 4.0


# --- directory pages ---

from profiles import directory_page, person_kind  # noqa: E402

_ROWS = [
    {'name': 'David Roberts', 'company': 'Volts', 'kind': 'host', 'appearances': 400, 'last_date': date(2026, 9, 1)},
    {'name': 'Art Berman', 'company': None, 'kind': 'advisor', 'appearances': 88, 'last_date': date(2025, 1, 1)},
    {'name': 'Jigar Shah', 'company': 'DOE', 'kind': 'official', 'appearances': 61, 'last_date': None},
    {'name': 'Ólafur Guðnason', 'company': 'Orkuveita', 'kind': 'advisor', 'appearances': 2, 'last_date': date(2026, 1, 1)},
]


class TestDirectoryPage:
    def test_default_sort_and_counts(self):
        p = directory_page(_ROWS, group_field='kind')
        assert [r['name'] for r in p['rows']][:2] == ['David Roberts', 'Art Berman']
        assert p['total'] == 4 and p['counts'] == {'host': 1, 'advisor': 2, 'official': 1}

    def test_group_filter_keeps_counts_for_all_groups(self):
        p = directory_page(_ROWS, group_field='kind', group='advisor')
        assert p['total'] == 2 and p['counts']['host'] == 1

    def test_search_name_and_company_accent_insensitive(self):
        assert [r['name'] for r in directory_page(_ROWS, q='olafur', fields=('name', 'company'))['rows']] == ['Ólafur Guðnason']
        assert [r['name'] for r in directory_page(_ROWS, q='doe', fields=('name', 'company'))['rows']] == ['Jigar Shah']
        assert directory_page(_ROWS, q='nobody')['total'] == 0

    def test_search_narrows_counts(self):
        assert directory_page(_ROWS, q='ber', group_field='kind')['counts'] == {'host': 1, 'advisor': 1}

    def test_sorts(self):
        names = lambda s: [r['name'] for r in directory_page(_ROWS, sort=s)['rows']]  # noqa: E731
        assert names('recent') == ['David Roberts', 'Ólafur Guðnason', 'Art Berman', 'Jigar Shah']
        assert names('name') == ['Art Berman', 'David Roberts', 'Jigar Shah', 'Ólafur Guðnason']
        assert names('bogus') == names('appearances')

    def test_paging_bounds(self):
        assert [r['name'] for r in directory_page(_ROWS, offset=1, limit=2)['rows']] == ['Art Berman', 'Jigar Shah']
        assert directory_page(_ROWS, offset=10)['rows'] == []
        assert len(directory_page(_ROWS, offset=-5, limit=0)['rows']) == 1


class TestPersonKind:
    def test_host_vs_role(self):
        kind = lambda t: 'ceo' if 'CEO' in t else 'other'  # noqa: E731
        assert person_kind('CEO', as_host=5, as_guest=1, role_kind=kind) == 'host'
        assert person_kind('CEO', as_host=1, as_guest=5, role_kind=kind) == 'ceo'
        assert person_kind(None, as_host=0, as_guest=3, role_kind=kind) is None


# --- career_by_org / split_co_appearances ---

from profiles import career_by_org, split_co_appearances  # noqa: E402


def _r(d, title=None, kind=None, company=None, org_id=None, former=False):
    return {'published_date': date.fromisoformat(d), 'title': title, 'title_kind': kind,
            'company': company, 'org_id': org_id, 'is_former': former}


# David Roberts' rows, newest first, as _role_rows() returns them.
_ROBERTS = [
    _r('2025-12-17', 'renowned climate and clean energy journalist', 'description'),
    _r('2025-12-17', None, None, 'Volts', 1025),
    _r('2025-11-07', 'host', 'position', 'Volts', 1025),
    _r('2025-07-10', None, None, 'Grist', 488, former=True),
    _r('2025-07-10', None, None, 'Vox', 1028, former=True),
    _r('2025-07-10', 'renowned journalist and the author', 'description', 'Volts', 1025),
    _r('2025-07-09', 'founder', 'position', 'Volts', 1025),
    _r('2025-03-31', 'reporter', 'position'),
    _r('2025-01-16', 'respected journalist and progenitor', 'description', 'Volts', 1025),
    _r('2021-10-21', 'founder and writer', 'position', 'Volts', 1025),
    _r('2021-10-21', 'host', 'position'),
    _r('2021-10-21', 'Editor-At-Large', 'position', 'Canary Media', 164),
    _r('2021-10-14', 'host', 'position', 'Volts', 1025),
    _r('2020-11-06', 'Energy and Climate Change Writer', 'position', 'Vox', 1028),
    _r('2020-03-10', 'staff writer', 'position', 'Vox', 1028),
    _r('2019-12-19', 'energy and politics reporter', 'position', 'Vox', 1028),
]


class TestCareerByOrg:
    def test_one_entry_per_org_current_first_former_last(self):
        items = career_by_org(_ROBERTS, {'title': 'Host', 'company': 'Volts', 'org_id': 1025})
        assert [i['company'] for i in items] == ['Volts', 'Canary Media', 'Vox', 'Grist']
        volts, canary, vox, grist = items
        assert volts['current'] and not volts['former']
        assert vox['former'] and grist['former'] and not canary['former']
        assert (volts['first_date'], volts['last_date']) == (date(2021, 10, 14), date(2025, 12, 17))

    def test_titles_positions_only_subsumed_dropped(self):
        volts = career_by_org(_ROBERTS)[0]
        titles = [t['title'] for t in volts['titles']]
        # "Founder" folds into "Founder and Writer"; descriptions are left out.
        assert sorted(titles) == ['Founder and Writer', 'Host']
        fw = next(t for t in volts['titles'] if t['title'] == 'Founder and Writer')
        assert fw['last_date'] == date(2021, 10, 21) and fw['mentions'] == 2

    def test_current_title_marked(self):
        volts = career_by_org(_ROBERTS, {'title': 'Host', 'company': 'Volts', 'org_id': 1025})[0]
        assert [t['title'] for t in volts['titles'] if t['current']] == ['Host']

    def test_titles_without_an_org_dropped_when_orgs_exist(self):
        assert all(i['company'] for i in career_by_org(_ROBERTS))

    def test_titles_only_person(self):
        rows = [_r('2024-01-01', 'energy analyst', 'position'), _r('2023-01-01', 'consultant', 'position'),
                _r('2023-01-01', 'petroleum geologist', 'description')]
        items = career_by_org(rows, {'title': 'Energy Analyst', 'company': None, 'org_id': None})
        assert [i['title'] for i in items] == ['Energy Analyst', 'Consultant']
        assert items[0]['current'] and items[0]['company'] is None

    def test_later_plain_mention_undoes_former(self):
        rows = [_r('2025-01-01', 'CEO', 'position', 'Acme', 1), _r('2024-01-01', 'CEO', 'position', 'Acme', 1, former=True)]
        assert not career_by_org(rows)[0]['former']

    def test_forrest_one_org(self):
        rows = [_r('2025-10-02', 'CEO', 'position', 'Fortescue', 3518),
                _r('2025-05-28', 'Executive Chairman', 'position', 'Fortescue', 3518),
                _r('2025-05-28', 'Billionaire iron magnate', 'description'),
                _r('2024-12-12', 'Founder and Executive Chairman', 'position', 'Fortescue', 3518)]
        items = career_by_org(rows, {'title': 'CEO', 'company': 'Fortescue', 'org_id': 3518})
        assert len(items) == 1
        # "Executive Chairman" is inside "Founder and Executive Chairman".
        assert [t['title'] for t in items[0]['titles']] == ['CEO', 'Founder and Executive Chairman']


class TestSplitCoAppearances:
    def test_buckets_by_both_roles(self):
        rows = [
            {'host_id': 1, 'name': 'Giles Parkinson', 'me_guest': True, 'them_guest': False, 'episodes': 3},
            {'host_id': 2, 'name': 'Eva Hanly', 'me_guest': True, 'them_guest': True, 'episodes': 1},
            {'host_id': 3, 'name': 'Co Host', 'me_guest': False, 'them_guest': False, 'episodes': 9},
            {'host_id': 4, 'name': 'A Guest', 'me_guest': False, 'them_guest': True, 'episodes': 2},
            {'host_id': 5, 'name': 'Another', 'me_guest': True, 'them_guest': False, 'episodes': 3},
        ]
        out = split_co_appearances(rows)
        assert [p['name'] for p in out['interviewed_by']] == ['Another', 'Giles Parkinson']
        assert [p['host_id'] for p in out['appeared_alongside']] == [2]
        assert [p['host_id'] for p in out['co_hosts']] == [3]
        assert [p['host_id'] for p in out['guests_hosted']] == [4]
        assert out['interviewed_by'][0]['slug'] == 'another'
