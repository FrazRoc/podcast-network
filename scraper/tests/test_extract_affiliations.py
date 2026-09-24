"""
Tests for extract_affiliations.py — the snippet sent for each guest
appearance, the verbatim check on what comes back, and (against the
disposable test database) how results are recorded.

No test here calls the API.
"""
import json
import os
from types import SimpleNamespace

import pytest

from extract_affiliations import (
    build_snippet, group_appearances, verified_affiliations, parse_response_text,
    message_text, render_items, chunk, request_params, estimate_cost, snippet_hash,
    item_id, get_appearances_to_process, get_names_by_host, record_results,
    SNIPPET_AFTER, ITEMS_PER_REQUEST, MODEL, MODELS,
)


class TestBuildSnippet:
    def test_role_after_the_name_is_included(self):
        desc = ("Some preamble about the grid. " * 20 +
                "This week we talk to Jane Doe, VP of Grid at Fervo Energy, about geothermal.")
        snippet = build_snippet(["Jane Doe"], "Episode 12", desc)
        assert "Jane Doe, VP of Grid at Fervo Energy" in snippet
        # Only the window, not the whole description.
        assert len(snippet) < len(desc)

    def test_role_before_the_name_is_included(self):
        snippet = build_snippet(["Tim Latimer"], "", "Today, Fervo CEO Tim Latimer joins us.")
        assert "Fervo CEO Tim Latimer" in snippet

    def test_not_named_returns_none(self):
        # Nothing to send, so no API call for this appearance.
        assert build_snippet(["Jane Doe"], "Grid news", "A roundup with no guests named.") is None

    def test_word_boundary_matching(self):
        # Same regression as the scanner: "Dan Yates" is not inside "Jordan Yates".
        assert build_snippet(["Dan Yates"], "", "Our guest is Jordan Yates of Acme.") is None

    def test_hyphenated_surname_is_not_a_match(self):
        assert build_snippet(["Sara Baldwin"], "", "With Sara Baldwin-Griffin of Energy Innovation.") is None

    def test_alias_spelling_matches(self):
        snippet = build_snippet(["Nathaniel Bullard", "Nat Bullard"], "",
                                "Nat Bullard, senior contributor at BloombergNEF, joins.")
        assert "BloombergNEF" in snippet

    def test_title_included_whole_when_it_names_the_person(self):
        title = "Jane Doe, CEO of Fervo, on geothermal"
        snippet = build_snippet(["Jane Doe"], title, "")
        assert snippet == title

    def test_title_ignored_when_it_does_not_name_the_person(self):
        snippet = build_snippet(["Jane Doe"], "Geothermal's big year", "Jane Doe of Fervo joins.")
        assert "Geothermal's big year" not in snippet

    def test_html_is_stripped_without_gluing_words(self):
        snippet = build_snippet(["Jane Doe"], "", "<p>Jane Doe</p><p>CEO of Fervo</p>")
        assert "Jane Doe CEO of Fervo" in snippet
        assert "<p>" not in snippet

    def test_window_cut_to_whole_words(self):
        desc = "Jane Doe is here. " + "wordy " * 200
        snippet = build_snippet(["Jane Doe"], "", desc)
        assert snippet.split()[-1] == "wordy"
        assert len(snippet) <= len("Jane Doe") + SNIPPET_AFTER

    def test_overlapping_mentions_are_merged(self):
        desc = "Jane Doe joins us. Later, Jane Doe explains her work at Fervo."
        snippet = build_snippet(["Jane Doe"], "", desc)
        assert snippet.count("Jane Doe joins us") == 1
        assert " … " not in snippet

    def test_distant_mentions_become_separate_windows(self):
        desc = "Jane Doe joins us. " + "filler " * 150 + "Jane Doe is CEO of Fervo."
        snippet = build_snippet(["Jane Doe"], "", desc)
        assert " … " in snippet
        assert "CEO of Fervo" in snippet

    def test_labelled_guest_block_past_the_scanner_cap_is_reached(self):
        # The scanner caps descriptions at 2,500 characters; the "Guests:"
        # block is often after that, and it is where the role is written.
        desc = "Long intro. " * 300 + "\nGuests:\nJane Doe – Senior journalist, BBC Verify"
        snippet = build_snippet(["Jane Doe"], "", desc)
        assert "Senior journalist, BBC Verify" in snippet


class TestFirstNameReferences:
    DESC = ("Today on the show, Jason Bordoff speaks with Erica Downs, Tatiana Mitrova and "
            "Sergey Vakulenko about Russia and China. " + "Background on the crisis. " * 20 +
            "Tatiana is a global fellow at CGEP. Erica is a senior research scholar at CGEP. "
            "Sergey is a senior fellow at the Carnegie Russia Eurasia Center.")

    def test_later_first_name_bio_is_included(self):
        # Real case: Columbia Energy Exchange, where the full-name window
        # ends long before "Sergey is a senior fellow at ...".
        snippet = build_snippet(["Sergey Vakulenko"], "", self.DESC)
        assert "Carnegie Russia Eurasia Center" in snippet

    def test_first_name_alone_without_full_name_is_not_a_mention(self):
        assert build_snippet(["Sergey Vakulenko"], "", "Sergey is a senior fellow.") is None

    def test_first_name_shared_with_someone_else_is_not_used(self):
        desc = ("Jason Price joins Jason Bordoff today. " + "filler " * 80 +
                "Jason is CEO of Acme.")
        snippet = build_snippet(["Jason Price"], "", desc)
        assert "CEO of Acme" not in snippet


class TestGroupAppearances:
    def _app(self, episode_id, host_id, desc, title="Ep"):
        return {'episode_id': episode_id, 'host_id': host_id, 'episode_title': title,
                'description': desc, 'podcast_title': 'Show'}

    def test_identical_snippet_for_same_person_is_sent_once(self):
        bio = "Jane Doe is CEO of Fervo."
        apps = [self._app(1, 7, bio), self._app(2, 7, bio), self._app(3, 7, "Jane Doe of Acme.")]
        no_mention, items = group_appearances(apps, {7: ["Jane Doe"]})
        assert no_mention == []
        assert len(items) == 2
        shared = next(i for i in items if len(i['appearances']) == 2)
        assert {a['episode_id'] for a in shared['appearances']} == {1, 2}

    def test_same_snippet_for_different_people_stays_separate(self):
        text = "Jane Doe and John Roe of Fervo."
        apps = [self._app(1, 7, text), self._app(1, 8, text)]
        _, items = group_appearances(apps, {7: ["Jane Doe"], 8: ["John Roe"]})
        assert len(items) == 2

    def test_unnamed_appearance_goes_to_no_mention(self):
        no_mention, items = group_appearances([self._app(1, 7, "No names here.")], {7: ["Jane Doe"]})
        assert len(no_mention) == 1 and items == []

    def test_item_id_combines_host_and_snippet_hash(self):
        _, items = group_appearances([self._app(1, 7, "Jane Doe of Fervo.")], {7: ["Jane Doe"]})
        item = items[0]
        assert item['id'] == item_id(7, snippet_hash(item['snippet']))


class TestVerifiedAffiliations:
    SNIPPET = "Jane Doe, co-founder and CEO of Fervo Energy, and a fellow at the Payne Institute."

    def test_verbatim_values_are_kept(self):
        kept, dropped = verified_affiliations(
            [{'title': 'co-founder and CEO', 'company': 'Fervo Energy'}], self.SNIPPET)
        assert kept == [{'title': 'co-founder and CEO', 'company': 'Fervo Energy'}]
        assert dropped == []

    def test_invented_company_is_dropped(self):
        # The model "knows" a fuller name, but the text never says it.
        kept, dropped = verified_affiliations(
            [{'title': 'CEO', 'company': 'Fervo Energy Inc.'}], self.SNIPPET)
        assert kept == [{'title': 'CEO', 'company': None}]
        assert dropped == [('company', 'Fervo Energy Inc.')]

    def test_affiliation_with_nothing_left_is_removed(self):
        kept, _ = verified_affiliations([{'title': 'Chairman', 'company': 'Google'}], self.SNIPPET)
        assert kept == []

    def test_match_ignores_case_and_curly_quotes(self):
        kept, _ = verified_affiliations(
            [{'title': 'Head of Policy', 'company': "America's Power"}],
            "Jane Doe, head of policy at America’s Power")
        assert kept == [{'title': 'Head of Policy', 'company': "America's Power"}]

    def test_nulls_and_blanks(self):
        kept, dropped = verified_affiliations(
            [{'title': None, 'company': 'Payne Institute'}, {'title': '  ', 'company': None}],
            self.SNIPPET)
        assert kept == [{'title': None, 'company': 'Payne Institute'}]
        assert dropped == []

    def test_bare_honorific_is_not_a_title(self):
        kept, _ = verified_affiliations([{'title': 'Dr.', 'company': None}],
                                        "Dr. Kevin Surprise joins us.")
        assert kept == []

    def test_title_containing_an_honorific_word_is_kept(self):
        kept, _ = verified_affiliations([{'title': 'Senator', 'company': None}],
                                        "Senator Martin Heinrich")
        assert kept == [{'title': 'Senator', 'company': None}]

    def test_case_only_duplicates_collapse(self):
        kept, _ = verified_affiliations(
            [{'title': 'CEO and Co-Founder', 'company': 'Wunder'},
             {'title': 'CEO and co-founder', 'company': 'Wunder'}],
            "Dave Riess, CEO and Co-Founder of Wunder ... the CEO and co-founder of Wunder")
        assert kept == [{'title': 'CEO and Co-Founder', 'company': 'Wunder'}]

    def test_shorter_title_at_same_org_is_dropped(self):
        # Real case (production sample): "CEO" and "Co-Founder and CEO" at Planetary.
        kept, _ = verified_affiliations(
            [{'title': 'CEO', 'company': 'Planetary'},
             {'title': 'Co-Founder and CEO', 'company': 'Planetary'}],
            "Mike Kelland, CEO of Planetary ... Mike Kelland, Co-Founder and CEO of Planetary")
        assert kept == [{'title': 'Co-Founder and CEO', 'company': 'Planetary'}]

    def test_bare_org_dropped_when_a_titled_role_names_it(self):
        kept, _ = verified_affiliations(
            [{'title': None, 'company': 'Wunder'}, {'title': 'CEO', 'company': 'Wunder'}],
            "Dave Riess, CEO of Wunder")
        assert kept == [{'title': 'CEO', 'company': 'Wunder'}]

    def test_same_title_at_different_orgs_both_kept(self):
        kept, _ = verified_affiliations(
            [{'title': 'senior fellow', 'company': 'Searchlight Institute'},
             {'title': 'senior fellow', 'company': 'States Forum'}],
            "a senior fellow at the Searchlight Institute and the States Forum")
        assert len(kept) == 2

    def test_duplicates_collapse(self):
        aff = {'title': 'CEO', 'company': 'Fervo Energy'}
        kept, _ = verified_affiliations([aff, dict(aff)], self.SNIPPET)
        assert len(kept) == 1


class TestResponses:
    CEO = [{'title': 'CEO', 'company': None}]

    def test_parse_keeps_only_expected_ids(self):
        text = json.dumps({'results': [
            {'id': 'a', 'is_podcast_host': False, 'affiliations': self.CEO},
            {'id': 'zzz', 'is_podcast_host': False, 'affiliations': []},
        ]})
        assert parse_response_text(text, {'a', 'b'}) == {
            'a': {'is_host': False, 'affiliations': self.CEO}}

    def test_first_answer_for_a_repeated_id_wins(self):
        text = json.dumps({'results': [
            {'id': 'a', 'is_podcast_host': False, 'affiliations': self.CEO},
            {'id': 'a', 'is_podcast_host': False, 'affiliations': []},
        ]})
        assert parse_response_text(text, {'a'})['a']['affiliations'] == self.CEO

    def test_podcast_host_gets_no_affiliations(self):
        # Hosts are left to a separate process, even if a role came back.
        text = json.dumps({'results': [
            {'id': 'a', 'is_podcast_host': True, 'affiliations': self.CEO},
        ]})
        assert parse_response_text(text, {'a'}) == {'a': {'is_host': True, 'affiliations': []}}

    def _message(self, stop_reason, text='{"results": []}'):
        return SimpleNamespace(stop_reason=stop_reason,
                               content=[SimpleNamespace(type='text', text=text)])

    def test_finished_message_returns_text(self):
        assert message_text(self._message('end_turn')) == '{"results": []}'

    @pytest.mark.parametrize('reason', ['max_tokens', 'refusal'])
    def test_unfinished_message_returns_none(self, reason):
        # The JSON may be cut short; those items are retried rather than parsed.
        assert message_text(self._message(reason)) is None


class TestRequests:
    def _items(self, n):
        return [{'id': f'{i}-abc', 'person': 'Jane Doe', 'snippet': 'Jane Doe of Fervo.'}
                for i in range(n)]

    def test_chunking(self):
        groups = chunk(self._items(ITEMS_PER_REQUEST * 2 + 1))
        assert [len(g) for g in groups] == [ITEMS_PER_REQUEST, ITEMS_PER_REQUEST, 1]

    def test_render_escapes_markup(self):
        rendered = render_items([{'id': '1-a', 'person': 'Jo "JJ" Doe',
                                  'snippet': 'R&D lead at <Acme>'}])
        assert rendered == '<item id="1-a" person="Jo &quot;JJ&quot; Doe">R&amp;D lead at &lt;Acme&gt;</item>'

    def test_request_params_shape(self):
        params = request_params(self._items(2))
        assert params['model'] == MODEL
        assert params['output_config']['format']['type'] == 'json_schema'
        assert params['messages'][0]['role'] == 'user'
        assert params['messages'][0]['content'].count('<item ') == 2

    def test_sonnet_request_turns_thinking_off(self):
        # Adaptive thinking is on by default there and would bill as output.
        params = request_params(self._items(1), 'claude-sonnet-5')
        assert params['model'] == 'claude-sonnet-5'
        assert params['thinking'] == {'type': 'disabled'}
        assert 'thinking' not in request_params(self._items(1), 'claude-haiku-4-5')

    def test_estimate_scales_with_model_price(self):
        items = [{'snippet': 'x' * 400}] * 200
        haiku = estimate_cost(items, model='claude-haiku-4-5')['dollars']
        sonnet = estimate_cost(items, model='claude-sonnet-5')['dollars']
        # 2x the price, times the measured 1.5x token factor.
        assert sonnet == pytest.approx(haiku * 3, abs=0.01)
        assert set(MODELS) >= {'claude-haiku-4-5', 'claude-sonnet-5'}

    def test_estimate_batch_is_half_of_normal(self):
        items = [{'snippet': 'x' * 400}] * 200
        batch, normal = estimate_cost(items), estimate_cost(items, batch=False)
        assert batch['requests'] == 5
        assert batch['dollars'] == pytest.approx(normal['dollars'] / 2, abs=0.01)


# ------------------------------------------------------------------
# Against the disposable test database
# ------------------------------------------------------------------

def _affiliation_tables_exist(conn) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('host_affiliations') IS NOT NULL")
    exists = cur.fetchone()[0]
    cur.close()
    return exists


@pytest.fixture()
def aff_db(db_conn):
    if not _affiliation_tables_exist(db_conn):
        pytest.skip("migrate_add_host_affiliations.sql not applied — re-run tests/setup_test_db.sh")
    return db_conn


def _setup_appearance(cur, first='Jane', last='Doe', is_guest=True,
                      desc='Jane Doe, CEO of Fervo, joins us.'):
    cur.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES ('Show', 'show') "
                "ON CONFLICT DO NOTHING")
    cur.execute("SELECT podcast_id FROM podcasts WHERE apple_podcast_id = 'show'")
    podcast_id = cur.fetchone()[0]
    cur.execute("INSERT INTO episodes (podcast_id, title, description) VALUES (%s, %s, %s) "
                "RETURNING episode_id", (podcast_id, f'Ep with {first}', desc))
    episode_id = cur.fetchone()[0]
    cur.execute("INSERT INTO hosts (first_name, last_name) VALUES (%s, %s) RETURNING host_id",
                (first, last))
    host_id = cur.fetchone()[0]
    cur.execute("INSERT INTO episode_host (episode_id, host_id, is_guest) VALUES (%s, %s, %s)",
                (episode_id, host_id, is_guest))
    return episode_id, host_id


def _insert_podcast(cur, title):
    cur.execute("INSERT INTO podcasts (title, apple_podcast_id) VALUES (%s, %s) RETURNING podcast_id",
                (title, title.lower().replace(' ', '-')))
    return cur.fetchone()[0]


def _pending(cur, episode_id, host_id, snippet):
    s_hash = snippet_hash(snippet)
    cur.execute("""
        INSERT INTO affiliation_extractions (episode_id, host_id, status, snippet, snippet_hash, batch_id)
        VALUES (%s, %s, 'pending', %s, %s, 'b1')
    """, (episode_id, host_id, snippet, s_hash))
    return (episode_id, host_id, snippet, s_hash)


class TestSelectionBeforeMigration:
    def test_works_without_the_affiliation_tables(self, db_conn):
        # estimate/pilot have to run against production before the migration.
        cur = db_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS host_affiliations, affiliation_extractions")
        try:
            pair = _setup_appearance(cur)
            db_conn.commit()
            apps = get_appearances_to_process(db_conn, limit=5)
            assert [(a['episode_id'], a['host_id']) for a in apps] == [pair]
        finally:
            db_conn.rollback()
            with open(os.path.join(os.path.dirname(__file__), '..',
                                   'migrate_add_host_affiliations.sql')) as f:
                cur.execute(f.read())
            db_conn.commit()


class TestSelection:
    def test_only_unprocessed_guest_appearances(self, aff_db):
        cur = aff_db.cursor()
        guest = _setup_appearance(cur, 'Jane', 'Doe')
        _setup_appearance(cur, 'Hal', 'Host', is_guest=False)
        processed = _setup_appearance(cur, 'Pat', 'Done')
        cur.execute("INSERT INTO affiliation_extractions (episode_id, host_id, status) "
                    "VALUES (%s, %s, 'done')", processed)
        aff_db.commit()

        apps = get_appearances_to_process(aff_db)
        assert [(a['episode_id'], a['host_id']) for a in apps] == [guest]

    def test_show_host_is_skipped_on_their_own_show_only(self, aff_db):
        # Real case: Joe Batir hosts Energy Transition Solutions but was
        # credited as a guest on 195 of its episodes.
        cur = aff_db.cursor()
        episode_id, host_id = _setup_appearance(cur, 'Joe', 'Batir')
        cur.execute("SELECT podcast_id FROM episodes WHERE episode_id = %s", (episode_id,))
        own_show = cur.fetchone()[0]
        cur.execute("INSERT INTO host_podcast (host_id, podcast_id) VALUES (%s, %s)", (host_id, own_show))
        other_show = _insert_podcast(cur, 'Other Show')
        cur.execute("INSERT INTO episodes (podcast_id, title, description) VALUES (%s, 'Ep', "
                    "'Joe Batir, founder of X') RETURNING episode_id", (other_show,))
        other_episode = cur.fetchone()[0]
        cur.execute("INSERT INTO episode_host (episode_id, host_id, is_guest) VALUES (%s, %s, true)",
                    (other_episode, host_id))
        aff_db.commit()

        apps = get_appearances_to_process(aff_db)
        assert [(a['episode_id'], a['host_id']) for a in apps] == [(other_episode, host_id)]

    def test_retry_is_picked_up_until_attempts_run_out(self, aff_db):
        cur = aff_db.cursor()
        pair = _setup_appearance(cur)
        cur.execute("INSERT INTO affiliation_extractions (episode_id, host_id, status, attempts) "
                    "VALUES (%s, %s, 'retry', 1)", pair)
        aff_db.commit()
        assert len(get_appearances_to_process(aff_db)) == 1

        cur.execute("UPDATE affiliation_extractions SET attempts = 3")
        aff_db.commit()
        assert get_appearances_to_process(aff_db) == []

    def test_aliases_are_included_in_names(self, aff_db):
        cur = aff_db.cursor()
        _, host_id = _setup_appearance(cur, 'Nathaniel', 'Bullard')
        cur.execute("INSERT INTO host_aliases (host_id, alias_name, normalized_name) "
                    "VALUES (%s, 'Nat Bullard', 'nat bullard')", (host_id,))
        aff_db.commit()
        assert get_names_by_host(aff_db)[host_id] == ['Nathaniel Bullard', 'Nat Bullard']


class TestRecordResults:
    def test_answers_are_verified_and_stored(self, aff_db):
        cur = aff_db.cursor()
        pair = _setup_appearance(cur)
        row = _pending(cur, *pair, 'Jane Doe, CEO of Fervo, joins us.')
        answers = {item_id(pair[1], row[3]): {'is_host': False, 'affiliations': [
            {'title': 'CEO', 'company': 'Fervo'},
            {'title': 'Board member', 'company': 'Google'},   # not in the text
        ]}}
        assert record_results(cur, [row], answers) == (1, 1, 0, 2)
        aff_db.commit()

        cur.execute("SELECT title, company, data_source FROM host_affiliations")
        assert cur.fetchall() == [('CEO', 'Fervo', 'llm_extracted')]
        cur.execute("SELECT status, completed_at IS NOT NULL FROM affiliation_extractions")
        assert cur.fetchone() == ('done', True)

    def test_missing_answer_goes_to_retry(self, aff_db):
        cur = aff_db.cursor()
        pair = _setup_appearance(cur)
        row = _pending(cur, *pair, 'Jane Doe, CEO of Fervo, joins us.')
        assert record_results(cur, [row], {}) == (0, 0, 1, 0)
        cur.execute("SELECT status FROM affiliation_extractions")
        assert cur.fetchone()[0] == 'retry'

    def test_manual_rows_survive_reextraction(self, aff_db):
        cur = aff_db.cursor()
        pair = _setup_appearance(cur)
        cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title, company, data_source) "
                    "VALUES (%s, %s, 'Chief Scientist', 'Fervo', 'manual')", pair)
        cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title, company) "
                    "VALUES (%s, %s, 'Old guess', NULL)", pair)
        row = _pending(cur, *pair, 'Jane Doe, CEO of Fervo, joins us.')
        record_results(cur, [row], {item_id(pair[1], row[3]): {
            'is_host': False, 'affiliations': [{'title': 'CEO', 'company': 'Fervo'}]}})
        aff_db.commit()

        cur.execute("SELECT title, data_source FROM host_affiliations ORDER BY title")
        assert cur.fetchall() == [('CEO', 'llm_extracted'), ('Chief Scientist', 'manual')]


    def test_flagged_host_is_recorded_without_affiliations(self, aff_db):
        cur = aff_db.cursor()
        pair = _setup_appearance(cur)
        row = _pending(cur, *pair, 'Jane Doe, CEO of Fervo, joins us.')
        answers = {item_id(pair[1], row[3]): {'is_host': True, 'affiliations': []}}
        assert record_results(cur, [row], answers) == (1, 0, 0, 0)
        cur.execute("SELECT status FROM affiliation_extractions")
        assert cur.fetchone()[0] == 'host'
        cur.execute("SELECT count(*) FROM host_affiliations")
        assert cur.fetchone()[0] == 0


class TestFollowsCredits:
    def test_deleting_the_credit_removes_its_affiliations(self, aff_db):
        cur = aff_db.cursor()
        pair = _setup_appearance(cur)
        cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title) VALUES (%s, %s, 'CEO')", pair)
        cur.execute("INSERT INTO affiliation_extractions (episode_id, host_id, status) "
                    "VALUES (%s, %s, 'done')", pair)
        cur.execute("DELETE FROM episode_host WHERE episode_id = %s AND host_id = %s", pair)
        aff_db.commit()
        cur.execute("SELECT (SELECT count(*) FROM host_affiliations), "
                    "(SELECT count(*) FROM affiliation_extractions)")
        assert cur.fetchone() == (0, 0)

    def test_merge_repoints_affiliations(self, aff_db):
        # merge_people() moves credits with UPDATE episode_host SET host_id;
        # the affiliations have to follow without the merge code knowing
        # about them.
        cur = aff_db.cursor()
        episode_id, drop_id = _setup_appearance(cur, 'Nat', 'Bullard')
        cur.execute("INSERT INTO hosts (first_name, last_name) VALUES ('Nathaniel', 'Bullard') "
                    "RETURNING host_id")
        keep_id = cur.fetchone()[0]
        cur.execute("INSERT INTO host_affiliations (episode_id, host_id, title) VALUES (%s, %s, 'CEO')",
                    (episode_id, drop_id))
        cur.execute("UPDATE episode_host SET host_id = %s WHERE host_id = %s", (keep_id, drop_id))
        cur.execute("DELETE FROM hosts WHERE host_id = %s", (drop_id,))
        aff_db.commit()
        cur.execute("SELECT host_id FROM host_affiliations")
        assert cur.fetchone()[0] == keep_id
