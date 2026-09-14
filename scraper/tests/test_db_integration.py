"""
Integration tests against a disposable Postgres database (see conftest.py).
These cover the regressions that only show up once real tables, constraints,
and cross-table lookups are involved — alias matching, host merging, and
first-name host attribution — which the pure-function tests can't reach.
"""
from host_extractor import get_or_create_host
from episode_name_scanner import get_hosts, get_known_names, show_host_first_names


def _insert_podcast(cur, title):
    cur.execute(
        "INSERT INTO podcasts (title, apple_podcast_id) VALUES (%s, %s) RETURNING podcast_id",
        (title, title.lower().replace(' ', '-'))
    )
    return cur.fetchone()[0]


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
        first_names = {fn for _, fn in result.get(podcast_id, [])}
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
