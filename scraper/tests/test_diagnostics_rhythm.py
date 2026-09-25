"""backend/main.py _add_publishing_rhythm: overdue / ended against each show's own rhythm."""
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))
import main  # noqa: E402


class FakeCursor:
    """Answers the two queries in order: Apple's latest dates, then episodes."""
    def __init__(self, rows, apple=None):
        self.results = [apple or [], rows]

    def execute(self, *a, **kw):
        pass

    def fetchall(self):
        return self.results.pop(0)


def weekly(podcast_id, last_days_ago, n=21, every=7):
    today = date.today()
    return [{'podcast_id': podcast_id, 'published_date': today - timedelta(days=last_days_ago + i * every)}
            for i in range(n)]


def test_freshness_by_own_rhythm():
    shows = [{'podcast_id': 1}, {'podcast_id': 2}, {'podcast_id': 3}, {'podcast_id': 4}]
    rows = weekly(1, 5) + weekly(2, 40) + weekly(3, 250) + weekly(4, 40, every=30)
    main._add_publishing_rhythm(FakeCursor(rows), shows)
    by = {s['podcast_id']: s for s in shows}
    assert by[1]['freshness'] is None and by[1]['typical_gap'] == 7
    assert by[2]['freshness'] == 'overdue'        # weekly, silent 40 days
    assert by[3]['freshness'] == 'ended'          # weekly, silent 250 days
    assert by[4]['freshness'] is None             # monthly, silent 40 days is normal


def test_too_few_episodes_to_judge():
    shows = [{'podcast_id': 1}]
    main._add_publishing_rhythm(FakeCursor(weekly(1, 100, n=3)), shows)
    assert shows[0]['typical_gap'] is None and shows[0]['freshness'] is None


def test_apple_has_newer_means_missing_not_paused():
    shows = [{'podcast_id': 1}, {'podcast_id': 2}]
    rows = weekly(1, 40) + weekly(2, 40)
    apple = [{'podcast_id': 1, 'latest_episode_date': date.today() - timedelta(days=2)},
             {'podcast_id': 2, 'latest_episode_date': date.today() - timedelta(days=40)}]
    main._add_publishing_rhythm(FakeCursor(rows, apple), shows)
    assert shows[0]['missing_newer'] is True       # the scanner is behind
    assert shows[1]['missing_newer'] is False      # the show paused
