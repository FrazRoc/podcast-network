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
