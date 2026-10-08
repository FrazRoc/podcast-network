"""manager.refresh_limit: which shows the scheduled backfill step fetches."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from manager import refresh_limit, RECENT_REFRESH_LIMIT  # noqa: E402


def test_big_gap_fetches_the_back_catalogue():
    assert refresh_limit(344, 50, '2026-07-08', '2026-07-08') == 200


def test_new_episode_on_a_complete_show_is_fetched():
    # Volts, Sep 2026: 3 behind, never refreshed under the old gap-only rule.
    assert refresh_limit(453, 450, '2026-09-18', '2026-09-11') == RECENT_REFRESH_LIMIT
    # ev.news: Apple's count had not moved, but its newest episode had.
    assert refresh_limit(502, 502, '2026-09-25', '2026-09-14') == RECENT_REFRESH_LIMIT


def test_up_to_date_or_paused_show_is_left_alone():
    assert refresh_limit(344, 344, '2026-07-08', '2026-07-08') is None
    assert refresh_limit(101, 107, '2026-09-20', '2026-09-22') is None    # we have more than Apple lists


def test_show_with_no_episodes_yet():
    assert refresh_limit(None, 0, '2026-09-01', None) == RECENT_REFRESH_LIMIT


def test_gap_beyond_the_lookup_window_is_not_refetched():
    # Oct 2026: these re-fetched 200 episodes every run for nothing — Apple's
    # lookup only returns the newest 200, and we already have them.
    assert refresh_limit(1331, 199, '2026-10-02', '2026-10-02') is None   # Living on Earth
    assert refresh_limit(485, 201, '2026-10-05', '2026-10-05') is None    # The Most Important Question
    assert refresh_limit(983, 855, '2026-10-08', '2026-10-08') is None    # SunCast, RSS-backfilled
    # ...but a show first scraped with 50 still gets its 200.
    assert refresh_limit(485, 50, '2026-10-05', '2026-10-05') == 200
    # A real gap inside the window is still fetched.
    assert refresh_limit(114, 101, '2026-10-01', '2026-10-01') == 200
    # A new episode on a show with a big trackCount gap gets the short fetch.
    assert refresh_limit(1331, 199, '2026-10-08', '2026-10-02') == RECENT_REFRESH_LIMIT
