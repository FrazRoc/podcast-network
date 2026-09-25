"""
Pure-function tests for scraper.py: episode title keying (dedupe), duration
parsing, published_date precedence, and the SunCast-style match-rate guard.
"""
from scraper import (
    normalize_episode_title,
    pick_title_key,
    compute_duration_seconds,
    compute_published_date,
    compute_match_gate,
    _safe_int,
    MIN_TITLE_MATCH_RATIO,
    MIN_EPISODES_FOR_MATCH_CHECK,
)


class TestNormalizeEpisodeTitle:
    def test_unicode_narrow_no_break_space_does_not_create_duplicate(self):
        # One title ends in a U+202F narrow no-break space that the RSS feed
        # lacks; both must normalize to the same key.
        with_nnbsp = "Episode Ten "
        without = "Episode Ten"
        assert normalize_episode_title(with_nnbsp) == normalize_episode_title(without)

    def test_curly_quotes_and_dashes_are_flattened(self):
        assert normalize_episode_title("Grid’s Future — Part One") == \
            normalize_episode_title("Grid's Future - Part One")

    def test_strips_leading_episode_number(self):
        assert normalize_episode_title("966: The Big Show") == \
            normalize_episode_title("The Big Show")

    def test_strips_bracketed_episode_number(self):
        assert normalize_episode_title("[Episode #282] - The Big Show") == \
            normalize_episode_title("The Big Show")

    def test_strips_trailing_episode_number(self):
        assert normalize_episode_title("The Big Show, Ep #136") == \
            normalize_episode_title("The Big Show")

    def test_strip_numbering_false_keeps_numbers(self):
        assert normalize_episode_title("Ep 12: Weekly Roundup", strip_numbering=False) != \
            normalize_episode_title("Ep 13: Weekly Roundup", strip_numbering=False)

    def test_empty_title(self):
        assert normalize_episode_title("") == ''
        assert normalize_episode_title(None) == ''


class TestPickTitleKey:
    def test_keeps_numbering_when_stripping_would_collide(self):
        # "Ep 12: Weekly Roundup" and "Ep 13: Weekly Roundup" are genuinely
        # different episodes; stripping numbering would merge them.
        titles = ["Ep 12: Weekly Roundup", "Ep 13: Weekly Roundup"]
        key_of = pick_title_key(titles)
        assert key_of(titles[0]) != key_of(titles[1])

    def test_strips_numbering_when_safe(self):
        titles = ["1: Intro to Solar", "2: Intro to Wind"]
        key_of = pick_title_key(titles)
        assert key_of("1: Intro to Solar") == key_of("Intro to Solar")


class TestSafeInt:
    def test_plain_int_string(self):
        assert _safe_int('8') == 8

    def test_none_stays_none(self):
        assert _safe_int(None) is None

    def test_fractional_bonus_episode_number_becomes_none(self):
        # Real incident: Shift Key's feed uses "8.5" for a bonus episode.
        # episode_number/season_number are INTEGER columns, and passing this
        # through unchanged raised "invalid input syntax for type integer",
        # which (before the SAVEPOINT fix) aborted the whole transaction and
        # silently discarded 34 other episodes' refreshes in the same show.
        assert _safe_int('8.5') is None

    def test_non_numeric_becomes_none(self):
        assert _safe_int('bonus') is None


class TestComputeDurationSeconds:
    def test_hh_mm_ss_from_rss(self):
        assert compute_duration_seconds({}, {'duration': '01:02:03'}) == 3723

    def test_mm_ss_from_rss(self):
        assert compute_duration_seconds({}, {'duration': '05:30'}) == 330

    def test_plain_seconds_string(self):
        assert compute_duration_seconds({}, {'duration': '120'}) == 120

    def test_invalid_duration_string_is_none(self):
        assert compute_duration_seconds({}, {'duration': 'not-a-duration'}) is None

    def test_itunes_millis_converted_to_seconds(self):
        assert compute_duration_seconds({'trackTimeMillis': 60000}, None) == 60

    def test_missing_itunes_duration_is_none(self):
        assert compute_duration_seconds({}, None) is None


class TestComputePublishedDate:
    def test_rss_published_date_used_directly(self):
        # Regression: the previous one-liner "(published_date or releaseDate)
        # if releaseDate else None" always returned None for RSS episodes,
        # since they have published_date but no releaseDate.
        import datetime as dt
        d = dt.date(2024, 1, 1)
        assert compute_published_date({'published_date': d}) == d

    def test_itunes_release_date_parsed(self):
        import datetime as dt
        result = compute_published_date({'releaseDate': '2024-03-15T00:00:00Z'})
        assert result == dt.date(2024, 3, 15)

    def test_no_date_fields_is_none(self):
        assert compute_published_date({}) is None

    def test_malformed_release_date_is_none_not_an_exception(self):
        assert compute_published_date({'releaseDate': 'not-a-date'}) is None


class TestComputeMatchGate:
    def test_suncast_style_low_match_triggers_skip(self):
        # SunCast's numbered feed matched only 10 of 210 stored titles; an
        # unguarded run would have inserted 831 duplicates.
        stored = [f"Episode {i}: Title {i}" for i in range(210)]
        feed = [f"{i}: Title {i}" for i in range(210)]  # different numbering scheme
        key_of = pick_title_key(feed + stored)
        _, _, matched, skip = compute_match_gate(stored, feed, key_of)
        assert skip is True

    def test_good_match_does_not_skip(self):
        stored = [f"Title {i}" for i in range(30)]
        feed = [f"Title {i}" for i in range(30)]
        key_of = pick_title_key(feed + stored)
        _, _, matched, skip = compute_match_gate(stored, feed, key_of)
        assert matched == 30
        assert skip is False

    def test_force_overrides_the_gate(self):
        stored = [f"Episode {i}: Title {i}" for i in range(210)]
        feed = [f"{i}: Title {i}" for i in range(210)]
        key_of = pick_title_key(feed + stored)
        _, _, _, skip = compute_match_gate(stored, feed, key_of, force=True)
        assert skip is False

    def test_below_minimum_episode_count_is_never_gated(self):
        # The gate only applies once there's enough stored history to judge.
        stored = [f"Title {i}" for i in range(MIN_EPISODES_FOR_MATCH_CHECK - 1)]
        feed = ["Something Completely Different"]
        key_of = pick_title_key(feed + stored)
        _, _, _, skip = compute_match_gate(stored, feed, key_of)
        assert skip is False


class TestInsertEpisodeRetitled:
    """A retitled episode (same Apple id, new title) updates its row instead
    of failing — and a failure can't take later episodes down with it."""

    def _scraper(self, db_conn):
        from scraper import PodcastScraper
        s = PodcastScraper.__new__(PodcastScraper)
        s.conn, s.cursor = db_conn, db_conn.cursor()
        return s

    def test_retitled_episode_updates_the_same_row(self, db_conn):
        s = self._scraper(db_conn)
        s.cursor.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES ('Climate CEOs', '1') RETURNING podcast_id")
        pid = s.cursor.fetchone()[0]
        first = s.insert_episode({'trackName': 'How CEOs Hire A-Players', 'trackId': 1000784736587}, pid)
        again = s.insert_episode({'trackName': 'How CEOs Hire A-Players (#326)', 'trackId': 1000784736587}, pid)
        assert again == first
        s.cursor.execute("SELECT title FROM episodes WHERE episode_id = %s", (first,))
        assert s.cursor.fetchone()[0] == 'How CEOs Hire A-Players (#326)'
