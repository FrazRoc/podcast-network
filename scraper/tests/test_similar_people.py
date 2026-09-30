"""
Tests for backend/similar_people.py — the "possibly the same as" hints on the
suggestions review queue. Pairs are from the Sep 30 2026 queue review, where
about 30 approvals had to be pointed at an existing person by hand.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'backend'))

from similar_people import match_reason, find_possible_matches, index_people  # noqa: E402


class TestSamePersonWrittenDifferently:
    def test_middle_initial(self):
        assert match_reason("Angela Chalk", "Angela M. Chalk") == "title or middle initial differs"

    def test_title(self):
        assert match_reason("Baroness Bryony Worthington", "Bryony Worthington") == "title or middle initial differs"

    def test_extra_middle_name(self):
        assert match_reason("Abby Ross Hopper", "Abby Hopper") == "middle name or initial differs"

    def test_nickname(self):
        assert match_reason("Arthur Berman", "Art Berman").startswith("nickname")
        assert match_reason("Jen Dlouhy", "Jennifer Dlouhy").startswith("nickname")

    def test_first_name_spelling(self):
        assert match_reason("Jessika Trancik", "Jessica Trancik").startswith("spelling")

    def test_surname_spelling(self):
        assert match_reason("Susan Monroe", "Susan Munroe").startswith("surname spelling")
        assert match_reason("David Trainavvicius", "David Trainavicius").startswith("surname spelling")

    def test_accents(self):
        assert match_reason("Luiza Demoro", "Luiza Demôro") == "accents differ"


class TestDifferentPeopleNotMatched:
    def test_different_first_names(self):
        assert match_reason("Adam Bell", "Alice Bell") is None
        assert match_reason("Jordan Yates", "Dan Yates") is None
        assert match_reason("Tim Green", "Tom Green") is None

    def test_both_halves_differ(self):
        assert match_reason("Kate Gordon", "Kenzie Gordon") is None

    def test_identical_is_not_a_possible_match(self):
        assert match_reason("Angela Chalk", "angela  chalk") is None


class TestFindPossibleMatches:
    PEOPLE = [
        {"host_id": 1, "name": "Angela M. Chalk"},
        {"host_id": 2, "name": "Art Berman"},
        {"host_id": 3, "name": "Adam Berman"},
        {"host_id": 4, "name": "Arthur Burman"},
    ]

    def test_finds_and_ranks(self):
        matches = find_possible_matches("Arthur Berman", self.PEOPLE)
        assert [m["host_id"] for m in matches] == [2, 4]

    def test_index_gives_same_result(self):
        index = index_people(self.PEOPLE)
        assert find_possible_matches("Angela Chalk", index) == find_possible_matches("Angela Chalk", self.PEOPLE)

    def test_alias_rows_collapse_to_one_person(self):
        people = self.PEOPLE + [{"host_id": 2, "name": "Arthur E. Berman"}]
        matches = find_possible_matches("Arthur Berman", people)
        assert [m["host_id"] for m in matches].count(2) == 1
