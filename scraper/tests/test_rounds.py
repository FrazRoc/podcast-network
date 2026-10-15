"""Tests for backend/rounds.py — "making the rounds" bursts."""
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))

from rounds import find_bursts  # noqa: E402


def app(host, day, show):
    return {'host_id': host, 'date': date(2025, 1, 1) + timedelta(days=day), 'podcast_id': show}


class TestFindBursts:
    def test_three_shows_in_a_month(self):
        bursts = find_bursts([app(1, 0, 10), app(1, 10, 11), app(1, 25, 12)])
        assert len(bursts) == 1
        assert bursts[0]['shows'] == 3
        assert bursts[0]['start'] == date(2025, 1, 1)

    def test_spread_out_is_not_a_burst(self):
        assert find_bursts([app(1, 0, 10), app(1, 20, 11), app(1, 45, 12)]) == []

    def test_same_show_counts_once(self):
        # A regular on one show is not doing the rounds.
        assert find_bursts([app(1, 0, 10), app(1, 7, 10), app(1, 14, 10), app(1, 21, 11)]) == []

    def test_two_tours_are_two_bursts(self):
        rows = [app(1, d, s) for d, s in [(0, 10), (5, 11), (9, 12), (200, 13), (210, 14), (215, 15)]]
        bursts = find_bursts(rows)
        assert [b['start'] for b in bursts] == [date(2025, 7, 20), date(2025, 1, 1)]   # newest first

    def test_best_window_wins(self):
        # The four-show window is taken, not the earlier three-show one it overlaps.
        rows = [app(1, d, s) for d, s in [(0, 10), (12, 11), (20, 12), (28, 13), (35, 14)]]
        bursts = find_bursts(rows)
        assert bursts[0]['shows'] == 4
        assert bursts[0]['start'] == date(2025, 1, 1)

    def test_people_kept_apart(self):
        rows = [app(1, 0, 10), app(2, 1, 11), app(1, 2, 12), app(2, 3, 13)]
        assert find_bursts(rows) == []

    def test_min_shows(self):
        rows = [app(1, 0, 10), app(1, 3, 11)]
        assert find_bursts(rows) == []
        assert len(find_bursts(rows, min_shows=2)) == 1
