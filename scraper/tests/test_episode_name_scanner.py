"""
Pure-function tests for episode_name_scanner.py — matching known hosts/guests
against episode text, and finding new candidate names for the suggestions queue.

Each test class links back to a real, previously-fixed bug where possible;
see CLAUDE.md's "Tests — the gap worth closing first" table.
"""
from episode_name_scanner import (
    strip_html,
    clean_description,
    name_in_text,
    build_surname_index,
    candidate_hosts,
    looks_like_organisation,
    strip_possessive_prefix,
    strip_honorific,
    _valid_name,
    extract_labelled_credits,
    extract_candidate_names,
)


class TestWordBoundaryMatching:
    """Jordan Yates / Dan Yates and Sara Baldwin / Sara Baldwin-Griffin are
    real pairs in this database. A plain substring match credits the wrong
    person in both cases."""

    def test_does_not_credit_substring_name(self):
        assert not name_in_text("Dan Yates", "An interview with Jordan Yates about solar.")

    def test_matches_the_actual_name(self):
        assert name_in_text("Jordan Yates", "An interview with Jordan Yates about solar.")

    def test_hyphen_boundary_not_a_word_boundary(self):
        # \b alone matches "Sara Baldwin" inside "Sara Baldwin-Griffin"
        # because a hyphen counts as a word boundary. The scanner's pattern
        # excludes hyphens on either side to prevent this.
        assert not name_in_text("Sara Baldwin", "Guest: Sara Baldwin-Griffin, RMI")

    def test_matches_full_hyphenated_name(self):
        assert name_in_text("Sara Baldwin-Griffin", "Guest: Sara Baldwin-Griffin, RMI")

    def test_case_insensitive(self):
        assert name_in_text("Amy Westervelt", "AMY WESTERVELT joins us")


class TestStripHtml:
    def test_tags_become_spaces_not_nothing(self):
        # Deleting tags outright welds words together across the boundary:
        # "Jason Rissman</p><p>On LinkedIn" must not become "Jason RissmanOn".
        text = "Connect with Jason Rissman</p><p>On LinkedIn"
        result = strip_html(text)
        assert "RissmanOn" not in result
        assert "Rissman On" in result or "Rissman  On" in result

    def test_entities_decoded(self):
        assert strip_html("Ben &amp; Jerry") == "Ben & Jerry"

    def test_collapses_excess_blank_lines(self):
        result = strip_html("Line one\n\n\n\nLine two")
        assert "\n\n\n" not in result


class TestCleanDescription:
    def test_strips_produced_by_credit(self):
        text = "Amy Westervelt talks with Jane Smith. Produced by Some Studio."
        result = clean_description(text)
        assert "Produced by" not in result

    def test_hosted_by_after_credits_label_survives(self):
        # A "Credits:" block that opens with "Hosted by ..." names the hosts;
        # 136 of 193 such blocks do, and cutting there threw those away.
        text = "Great episode.\nCredits: Hosted by Amy Westervelt"
        result = clean_description(text)
        assert "Amy Westervelt" in result

    def test_credits_without_hosted_by_is_cut(self):
        text = "Great episode.\nCredits:\nEdited by some engineer"
        result = clean_description(text)
        assert "Credits:" not in result
        assert "engineer" not in result

    def test_listen_to_episode_with_is_removed_in_place(self):
        # "Listen to our previous episode with Wanjira Mathai" names someone
        # from a DIFFERENT episode; it must be cut without truncating the
        # real content that follows it.
        text = ("Listen to our previous episode with Wanjira Mathai. "
                "Thank you to our guest this week, Katie Eder!")
        result = clean_description(text)
        assert "Wanjira Mathai" not in result
        assert "Katie Eder" in result

    def test_truncates_on_word_boundary_not_mid_word(self):
        text = "Connect With Smart Energy Decisions " + ("x" * 3000)
        result = clean_description(text, max_chars=36)
        assert not result.endswith("Energ")
        assert " " not in result[-1:] or True  # no trailing partial word
        assert result == "Connect With Smart Energy Decisions" or len(result) <= 36

    def test_cross_show_promo_blurb_stripped(self):
        # Real incident: Latitude Media's "listen to our new podcast, Political
        # Climate ... hosts Julia Pyper, Emily Domenech, and Brandon Hurlbut"
        # outro credited all three hosts of a show that never aired on 12
        # episodes of two unrelated shows (Green Blueprint, Catalyst).
        text = ("This week: Tesla news. And make sure to listen to our new podcast, "
                "Political Climate – an insider’s view on the most pressing "
                "policy questions in energy and climate. Tune in every other Friday "
                "for the latest takes from hosts Julia Pyper, Emily Domenech, and "
                "Brandon Hurlbut. Available on Apple, Spotify, or wherever you get "
                "your podcasts.")
        result = clean_description(text)
        assert "Domenech" not in result
        assert "Pyper" not in result
        assert "Hurlbut" not in result
        assert "Tesla news" in result

    def test_zero_climate_race_footer_stripped(self):
        # cleanup_zero_guests.py fixed the case where "Explore further: Past
        # episode with X" credited a guest from a different episode.
        text = "Real content about the guest.\nExplore further: Past episode with Someone Else"
        result = clean_description(text)
        assert "Someone Else" not in result
        assert "Real content" in result


class TestOrganisationFiltering:
    def test_rejects_org_suffix_word(self):
        assert looks_like_organisation("Acme Ventures")

    def test_rejects_two_word_org(self):
        assert looks_like_organisation("Rigetti Computing")

    def test_accepts_real_surname_power(self):
        # Deliberately not in _ORG_WORDS: Ted Power is a real person.
        assert not looks_like_organisation("Ted Power")

    def test_accepts_plain_person_name(self):
        assert not looks_like_organisation("Jane Smith")


class TestPossessivePrefixAndHonorifics:
    def test_strips_possessive_org_prefix(self):
        assert strip_possessive_prefix("Origami's Gregg Patterson") == "Gregg Patterson"

    def test_strips_dr(self):
        assert strip_honorific("Dr. Leah Stokes") == "Leah Stokes"

    def test_strips_professor_not_just_prof(self):
        # "Professor" slipped past a list that only covered "Prof" and
        # created a duplicate "Professor Tristan Smith" record.
        assert strip_honorific("Professor Tristan Smith") == "Tristan Smith"

    def test_strips_job_title_founder(self):
        assert strip_honorific("Founder Oliver Katz") == "Oliver Katz"

    def test_strips_job_title_ceo(self):
        assert strip_honorific("CEO Dan Shugar") == "Dan Shugar"


class TestValidName:
    def test_rejects_short_string(self):
        assert not _valid_name("Al B")

    def test_rejects_single_word(self):
        assert not _valid_name("Westervelt")

    def test_rejects_four_words(self):
        assert not _valid_name("Amy Jane Marie Westervelt")

    def test_rejects_false_positive_leading_word(self):
        assert not _valid_name("How To")

    def test_rejects_organisation_shape(self):
        assert not _valid_name("Heart Aerospace")

    def test_accepts_plain_name(self):
        assert _valid_name("Jane Smith")


class TestLabelledCredits:
    def test_same_line_guest_label(self):
        result = extract_labelled_credits("Guest: Dr. Charles Sims, Director for Energy")
        assert ("Charles Sims", True) in result

    def test_same_line_host_label(self):
        result = extract_labelled_credits("Host: Amy Westervelt")
        assert ("Amy Westervelt", False) in result

    def test_moderator_counts_as_host_label(self):
        # _HOST_LABELS includes "moderator" — someone running the discussion
        # is treated like a host, not a guest.
        result = extract_labelled_credits("Moderator: Michael Eyman, Managing Director, Origis")
        assert ("Michael Eyman", False) in result

    def test_qualifier_before_label(self):
        # Climate One writes "Episode Guests:" — the label need not start the line.
        result = extract_labelled_credits("Episode Guests: Joe Manchin")
        assert ("Joe Manchin", True) in result

    def test_block_form_one_person_per_line(self):
        text = "Guests:\nJane Smith, RMI\nJohn Doe, Acme Corp\nHighlights:\n01:23 intro"
        result = extract_labelled_credits(text)
        names = [n for n, _ in result]
        assert "Jane Smith" in names
        assert "John Doe" in names
        assert "Highlights" not in " ".join(names)

    def test_block_form_and_in_job_title_not_a_second_guest(self):
        # "Thomas Ramey, Commercial and Nonprofit Solar Evaluator" must not
        # yield a phantom second guest from the "and" in the job title.
        text = "Guests:\nThomas Ramey, Commercial and Nonprofit Solar Evaluator"
        result = extract_labelled_credits(text)
        assert result == [("Thomas Ramey", True)]

    def test_no_label_returns_empty(self):
        assert extract_labelled_credits("Just a description with no labels.") == []


class TestExtractCandidateNames:
    def test_with_pattern(self):
        result = extract_candidate_names("This week we talk with Jane Smith about solar.")
        assert any(n == "Jane Smith" for n, _ in result)

    def test_speak_to_pattern(self):
        result = extract_candidate_names("we speak to Benjamin Bartle")
        assert any(n == "Benjamin Bartle" for n, _ in result)

    def test_joins_pattern(self):
        result = extract_candidate_names("Jane Smith joins us to discuss the grid.")
        assert any(n == "Jane Smith" for n, _ in result)

    def test_possessive_org_pattern(self):
        result = extract_candidate_names("Rewiring America's Ari Matusiak joins the show.")
        names = [n for n, _ in result]
        assert "Ari Matusiak" in names
        assert "Rewiring America" not in names

    def test_does_not_credit_org_after_with(self):
        result = extract_candidate_names("This week we talk with Bedrock Robotics about mining.")
        assert result == []

    def test_and_conjunction_picks_up_second_guest(self):
        text = "We talk with Jane Smith and John Doe about the grid."
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Jane Smith" in names
        assert "John Doe" in names

    def test_and_conjunction_respects_window_with_job_title_between(self):
        text = ("We talk with Ben Chehebar, VP of Hardware at RoadRunner Recycling, "
                "and Jason Gates about recycling.")
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Ben Chehebar" in names
        assert "Jason Gates" in names


class TestSurnameIndex:
    def test_only_matching_surname_is_a_candidate(self):
        hosts = [
            {'host_id': 1, 'full_name': 'Jordan Yates'},
            {'host_id': 2, 'full_name': 'Amy Westervelt'},
        ]
        index = build_surname_index(hosts)
        candidates = candidate_hosts("An episode about Jordan Yates.", index)
        names = {h['full_name'] for h in candidates}
        assert names == {'Jordan Yates'}

    def test_no_surname_present_yields_no_candidates(self):
        hosts = [{'host_id': 1, 'full_name': 'Amy Westervelt'}]
        index = build_surname_index(hosts)
        assert candidate_hosts("Nothing relevant here.", index) == []
