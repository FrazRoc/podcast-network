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
