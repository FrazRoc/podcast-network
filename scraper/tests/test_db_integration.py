"""
Integration tests against a disposable Postgres database (see conftest.py).
These cover the regressions that only show up once real tables, constraints,
and cross-table lookups are involved — alias matching, host merging, and
first-name host attribution — which the pure-function tests can't reach.
"""
from host_extractor import get_or_create_host
from episode_name_scanner import get_hosts, get_known_names, show_host_first_names, get_already_credited_pairs


def _insert_episode(cur, title="An Episode"):
    podcast_id = _insert_podcast(cur, title + " Show")
    cur.execute(
        "INSERT INTO episodes (podcast_id, title) VALUES (%s, %s) RETURNING episode_id",
        (podcast_id, title)
    )
    return cur.fetchone()[0]


def _insert_host(cur, first="Test", last="Person"):
    cur.execute(
        "INSERT INTO hosts (first_name, last_name) VALUES (%s, %s) RETURNING host_id",
        (first, last)
    )
    return cur.fetchone()[0]


def _insert_podcast(cur, title):
    cur.execute(
        "INSERT INTO podcasts (title, apple_podcast_id) VALUES (%s, %s) RETURNING podcast_id",
        (title, title.lower().replace(' ', '-'))
    )
    return cur.fetchone()[0]


class TestAlreadyCreditedPairs:
    def test_credited_pair_is_returned(self, db_conn):
        # Real incident: suggestion_id 2930 proposed "Dawn Lippert" for an
        # episode where she was already credited — get_known_names() alone
        # should have caught this and didn't, so suggest() also checks
        # episode_host directly.
        cur = db_conn.cursor()
        podcast_id = _insert_podcast(cur, "Some Show")
        cur.execute(
            "INSERT INTO episodes (podcast_id, title) VALUES (%s, %s) RETURNING episode_id",
            (podcast_id, "An Episode")
        )
        episode_id = cur.fetchone()[0]
        host_id = get_or_create_host(cur, "Dawn Lippert", "manual")
        cur.execute(
            "INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source) "
            "VALUES (%s, %s, true, 'Guest', 'manual')",
            (episode_id, host_id)
        )
        db_conn.commit()

        pairs = get_already_credited_pairs(db_conn)
        assert ("dawn lippert", episode_id) in pairs

    def test_uncredited_pair_is_absent(self, db_conn):
        cur = db_conn.cursor()
        podcast_id = _insert_podcast(cur, "Some Other Show")
        cur.execute(
            "INSERT INTO episodes (podcast_id, title) VALUES (%s, %s) RETURNING episode_id",
            (podcast_id, "Another Episode")
        )
        episode_id = cur.fetchone()[0]
        db_conn.commit()

        pairs = get_already_credited_pairs(db_conn)
        assert ("dawn lippert", episode_id) not in pairs


class TestAliasMatching:
    def test_alias_resolves_to_existing_host(self, db_conn):
        cur = db_conn.cursor()
        host_id = get_or_create_host(cur, "Nathaniel Bullard", "apple_verified")
        cur.execute(
            "INSERT INTO host_aliases (host_id, alias_name, normalized_name) "
            "VALUES (%s, %s, %s)",
            (host_id, "Nat Bullard", "natbullard")
        )
        db_conn.commit()

        # A later extraction of the alias spelling must resolve to the SAME
        # host, not create a second record.
        second_id = get_or_create_host(cur, "Nat Bullard", "parsed_desc")
        assert second_id == host_id

        cur.execute("SELECT COUNT(*) FROM hosts")
        assert cur.fetchone()[0] == 1

    def test_differently_split_name_resolves_to_same_host(self, db_conn):
        # Apple gives "Amy Myers" + "Jaffe"; the admin UI stores "Amy" +
        # "Myers Jaffe". normalize_full_name must treat both as one person.
        cur = db_conn.cursor()
        first_id = get_or_create_host(cur, "Amy Myers Jaffe", "apple_verified")
        db_conn.commit()

        second_id = get_or_create_host(cur, "AmyMyersJaffe", "parsed_desc")
        assert second_id == first_id

    def test_get_hosts_includes_aliases_for_matching(self, db_conn):
        cur = db_conn.cursor()
        host_id = get_or_create_host(cur, "Nathaniel Bullard", "apple_verified")
        cur.execute(
            "INSERT INTO host_aliases (host_id, alias_name, normalized_name) "
            "VALUES (%s, %s, %s)",
            (host_id, "Nat Bullard", "natbullard")
        )
        db_conn.commit()

        names = {h['full_name'] for h in get_hosts(db_conn)}
        assert "Nathaniel Bullard" in names
        assert "Nat Bullard" in names

    def test_known_names_includes_aliases(self, db_conn):
        # Without this, a merged-away spelling gets re-suggested as a new
        # person on every subsequent scan.
        cur = db_conn.cursor()
        host_id = get_or_create_host(cur, "Nathaniel Bullard", "apple_verified")
        cur.execute(
            "INSERT INTO host_aliases (host_id, alias_name, normalized_name) "
            "VALUES (%s, %s, %s)",
            (host_id, "Nat Bullard", "natbullard")
        )
        db_conn.commit()

        assert "nat bullard" in get_known_names(db_conn)


class TestHonorificHostDeduplication:
    def test_dr_prefixed_name_matches_existing_bare_name(self, db_conn):
        cur = db_conn.cursor()
        first_id = get_or_create_host(cur, "Melissa Lott", "apple_verified")
        db_conn.commit()

        second_id = get_or_create_host(cur, "Dr. Melissa Lott", "parsed_desc")
        assert second_id == first_id

        cur.execute("SELECT COUNT(*) FROM hosts")
        assert cur.fetchone()[0] == 1


class TestShowHostFirstNames:
    def test_shared_first_name_is_excluded(self, db_conn):
        # Redefining Energy writes "Gerard and Laurent welcome ..." — a first
        # name is only safe to match on when it's unique among that show's
        # registered hosts.
        cur = db_conn.cursor()
        podcast_id = _insert_podcast(cur, "Redefining Energy")
        gerard_id = get_or_create_host(cur, "Gerard Reid", "apple_verified")
        laurent_id = get_or_create_host(cur, "Laurent Segalen", "apple_verified")
        cur.execute(
            "INSERT INTO host_podcast (host_id, podcast_id, role) VALUES (%s, %s, 'Host'), (%s, %s, 'Host')",
            (gerard_id, podcast_id, laurent_id, podcast_id)
        )
        db_conn.commit()

        result = show_host_first_names(db_conn)
        first_names = {fn for _, fn, _ in result.get(podcast_id, [])}
        assert first_names == {"Gerard", "Laurent"}

    def test_duplicate_first_name_within_show_is_excluded(self, db_conn):
        cur = db_conn.cursor()
        podcast_id = _insert_podcast(cur, "Two Johns Show")
        john_a = get_or_create_host(cur, "John Smith", "apple_verified")
        john_b = get_or_create_host(cur, "John Doe", "apple_verified")
        cur.execute(
            "INSERT INTO host_podcast (host_id, podcast_id, role) VALUES (%s, %s, 'Host'), (%s, %s, 'Host')",
            (john_a, podcast_id, john_b, podcast_id)
        )
        db_conn.commit()

        result = show_host_first_names(db_conn)
        # There is no way to tell which "John" is meant, so neither counts.
        assert result.get(podcast_id, []) == []


class TestCreditSuppression:
    """A deleted credit has to stay deleted.

    Removing a row from episode_host does not hold on its own: the next scan
    re-reads the same description, derives the same name and re-inserts it.
    That is how a morning of curation was undone — Bill Gates back to 48
    credits from 3, Joe Manchin to 41 from 2 — by one scheduled scrape.

    The guard is a BEFORE INSERT trigger rather than a check at the call
    sites, because ten different places insert into episode_host and the
    eleventh is the one that would forget. These tests go through raw SQL for
    exactly that reason: they assert the database refuses the row no matter
    who asks.
    """

    def test_insert_is_skipped_while_suppressed(self, db_conn):
        cur = db_conn.cursor()
        ep, host = _insert_episode(cur), _insert_host(cur)
        cur.execute(
            "INSERT INTO credit_suppressions (episode_id, host_id, reason)"
            " VALUES (%s, %s, 'test')", (ep, host))
        cur.execute(
            "INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)"
            " VALUES (%s, %s, true, 'Guest', 'parsed_desc')", (ep, host))
        cur.execute(
            "SELECT COUNT(*) FROM episode_host WHERE episode_id=%s AND host_id=%s",
            (ep, host))
        assert cur.fetchone()[0] == 0

    def test_insert_lands_when_not_suppressed(self, db_conn):
        cur = db_conn.cursor()
        ep, host = _insert_episode(cur), _insert_host(cur)
        cur.execute(
            "INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)"
            " VALUES (%s, %s, true, 'Guest', 'parsed_desc')", (ep, host))
        cur.execute(
            "SELECT COUNT(*) FROM episode_host WHERE episode_id=%s AND host_id=%s",
            (ep, host))
        assert cur.fetchone()[0] == 1

    def test_bulk_insert_skips_only_the_suppressed_pair(self, db_conn):
        """The scanner inserts many rows at once; one suppressed pair must not
        fail the batch or take the others down with it."""
        cur = db_conn.cursor()
        ep = _insert_episode(cur)
        blocked = _insert_host(cur, 'Blocked', 'Person')
        allowed = _insert_host(cur, 'Allowed', 'Person')
        cur.execute(
            "INSERT INTO credit_suppressions (episode_id, host_id, reason)"
            " VALUES (%s, %s, 'test')", (ep, blocked))
        cur.execute(
            "INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)"
            " VALUES (%s, %s, true, 'Guest', 'parsed_desc'),"
            "        (%s, %s, true, 'Guest', 'parsed_desc')",
            (ep, blocked, ep, allowed))
        cur.execute(
            "SELECT host_id FROM episode_host WHERE episode_id=%s ORDER BY host_id", (ep,))
        assert [r[0] for r in cur.fetchall()] == [allowed]

    def test_lifting_the_suppression_lets_an_admin_re_add(self, db_conn):
        """Suppression must not lock a real appearance out permanently — the
        admin add-credit path deletes the suppression before inserting."""
        cur = db_conn.cursor()
        ep, host = _insert_episode(cur), _insert_host(cur)
        cur.execute(
            "INSERT INTO credit_suppressions (episode_id, host_id, reason)"
            " VALUES (%s, %s, 'test')", (ep, host))
        cur.execute(
            "DELETE FROM credit_suppressions WHERE episode_id=%s AND host_id=%s",
            (ep, host))
        cur.execute(
            "INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)"
            " VALUES (%s, %s, true, 'Guest', 'manual')", (ep, host))
        cur.execute(
            "SELECT COUNT(*) FROM episode_host WHERE episode_id=%s AND host_id=%s",
            (ep, host))
        assert cur.fetchone()[0] == 1
