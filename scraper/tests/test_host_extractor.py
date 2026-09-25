"""
Pure-function tests for host_extractor.py — the pattern that finds a show's
HOST (channel name / title / description), as opposed to episode_name_scanner.py
which finds guests inside individual episodes.
"""
from host_extractor import (
    looks_like_person_channel,
    looks_like_person_desc,
    split_names_channel,
    split_names_desc,
    extract_from_title,
    extract_from_description,
    normalize_full_name,
    strip_honorific,
)


class TestLooksLikePersonChannel:
    def test_accepts_a_plain_name(self):
        assert looks_like_person_channel("Chris Nelder")

    def test_rejects_a_known_organisation(self):
        assert not looks_like_person_channel("Pushkin Industries")

    def test_rejects_single_word(self):
        assert not looks_like_person_channel("Drilled")

    def test_rejects_show_whose_channel_name_matches_its_own_title(self):
        # Real incident: "Solar Surge" (podcast_id 1810) had no real host
        # registered on Apple, so its own show name — a plain two-word
        # Title-Cased string, indistinguishable from a real name by shape
        # alone — got stored as a fake host via the itunes_artist source,
        # then matched as a "guest" on other shows whenever their text
        # happened to contain the literal phrase ("Pakistan's Solar Surge").
        assert not looks_like_person_channel("Solar Surge")

    def test_rejects_parent_news_outlet_as_host(self):
        # Real incident (a batch of 29 shows added 2026-09-24): several had
        # no real host on Apple, so the parent outlet/institution landed in
        # itunes_artist instead of a person. "The New York Times" alone had
        # matched as a "guest" on 110 unrelated episodes across 38 other
        # shows by the time this was caught — the same collateral-damage
        # shape as an org name anywhere else in this list, just discovered
        # via a fresh batch of shows rather than one at a time.
        assert not looks_like_person_channel("The New York Times")
        assert not looks_like_person_channel("Chatham House")
        assert not looks_like_person_channel("University of Toronto Press")

    def test_rejects_producing_org_as_host_second_batch(self):
        # Same failure mode, second batch of shows added 2026-09-25: the
        # itunes_artist field held the sponsor, platform, or producing org
        # instead of a named host.
        assert not looks_like_person_channel("Modo Energy")
        assert not looks_like_person_channel("The Energy Revolution")
        assert not looks_like_person_channel("Climate Investor")
        assert not looks_like_person_channel("The Electricity Hub")

    def test_rejects_company_suffix(self):
        assert not looks_like_person_channel("Acme LLC")

    def test_accepts_name_with_connector_word(self):
        assert looks_like_person_channel("Ludwig van Beethoven")

    def test_rejects_name_over_60_chars(self):
        assert not looks_like_person_channel("A" * 61 + " Name")


class TestLooksLikePersonDesc:
    def test_accepts_two_word_name(self):
        assert looks_like_person_desc("Amy Westervelt")

    def test_rejects_org_word(self):
        # "Energy Central" — org-ish words disqualify a description candidate
        assert not looks_like_person_desc("Energy Central")

    def test_rejects_all_caps(self):
        assert not looks_like_person_desc("AMY WESTERVELT")

    def test_rejects_single_word(self):
        assert not looks_like_person_desc("Westervelt")

    def test_rejects_more_than_four_words(self):
        assert not looks_like_person_desc("Amy Jane Marie Westervelt Smith")

    def test_accepts_hyphenated_surname(self):
        assert looks_like_person_desc("Tom Rowlands-Rees")


class TestSplitNamesChannel:
    def test_splits_on_and(self):
        assert split_names_channel("Greg Dalton and Ariana Brocious") == \
            ["Greg Dalton", "Ariana Brocious"]

    def test_splits_on_comma(self):
        assert split_names_channel("Greg Dalton, Ariana Brocious") == \
            ["Greg Dalton", "Ariana Brocious"]

    def test_strips_role_after_pipe(self):
        assert split_names_channel("Amy Westervelt | Host") == ["Amy Westervelt"]

    def test_normalizes_all_caps(self):
        assert split_names_channel("AMY WESTERVELT") == ["Amy Westervelt"]


class TestSplitNamesDesc:
    def test_stops_at_along_with(self):
        result = split_names_desc("Amy Westervelt along with special guests")
        assert result == ["Amy Westervelt"]

    def test_stops_at_bring_you(self):
        # Real case: "Kousha Navidar bring you" should not read "you" as a name
        result = split_names_desc("Kousha Navidar bring you")
        assert result == ["Kousha Navidar"]

    def test_splits_three_names(self):
        result = split_names_desc("Greg Dalton, Ariana Brocious and Kousha Navidar")
        assert result == ["Greg Dalton", "Ariana Brocious", "Kousha Navidar"]


class TestExtractFromTitle:
    def test_extracts_trailing_with_name(self):
        title = "The Energy Transition Show with Chris Nelder"
        assert extract_from_title(title) == "Chris Nelder"

    def test_returns_none_when_title_starts_with_with(self):
        assert extract_from_title("With Great Power") is None

    def test_returns_none_without_with_pattern(self):
        assert extract_from_title("Volts") is None


class TestExtractFromDescription:
    def test_hosted_by_with_role_word_pattern(self):
        assert extract_from_description("Hosted by partner Todd Alexander") == ["Todd Alexander"]

    def test_with_your_host_pattern(self):
        assert extract_from_description("with your host Keith Anderson") == ["Keith Anderson"]

    def test_co_hosted_by_multiple(self):
        desc = "Co-Hosted by Greg Dalton, Ariana Brocious and Kousha Navidar"
        assert extract_from_description(desc) == \
            ["Greg Dalton", "Ariana Brocious", "Kousha Navidar"]

    def test_html_is_stripped_before_matching(self):
        desc = "<p>hosted by partner Amy Westervelt</p>"
        assert extract_from_description(desc) == ["Amy Westervelt"]

    def test_no_match_returns_empty_list(self):
        assert extract_from_description("A podcast about clean energy.") == []


class TestNormalizeFullName:
    def test_ignores_case_and_punctuation(self):
        assert normalize_full_name("Amy Myers Jaffe") == normalize_full_name("amy myers jaffe")

    def test_matches_differently_split_names(self):
        # Apple gives "Amy Myers" + "Jaffe"; the admin UI stores "Amy" + "Myers Jaffe".
        # Both must normalize identically so they resolve to the same host.
        assert normalize_full_name("Amy Myers Jaffe") == normalize_full_name("AmyMyersJaffe")

    def test_strips_punctuation_and_spacing(self):
        assert normalize_full_name("O'Brien-Smith") == normalize_full_name("OBrienSmith")


class TestStripHonorific:
    def test_strips_dr(self):
        assert strip_honorific("Dr. Melissa Lott") == "Melissa Lott"

    def test_strips_senator(self):
        assert strip_honorific("Senator Bernie Sanders") == "Bernie Sanders"

    def test_leaves_name_without_honorific(self):
        assert strip_honorific("Melissa Lott") == "Melissa Lott"

    def test_does_not_strip_when_nothing_survives(self):
        # "General Motors" is a company, not "Motors" with a stripped title.
        assert strip_honorific("General Motors") == "General Motors"
