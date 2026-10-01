"""scraper/enrich.py: matching websites and links to organisations and people."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from enrich import (registrable, domain_exactly_names, domain_matches_name, type_from_description,  # noqa: E402
                    one_word, plan_people_links)


@pytest.mark.parametrize('host, expected', [
    ('www.bnef.com', 'bnef.com'), ('newsletter.dunneinsights.com', 'dunneinsights.com'),
    ('ox.ac.uk', 'ox.ac.uk'), ('www.new.ox.ac.uk', 'ox.ac.uk'), ('energypolicy.columbia.edu', 'columbia.edu'),
])
def test_registrable(host, expected):
    assert registrable(host) == expected


@pytest.mark.parametrize('domain, name, exact, partial', [
    ('camus.energy', 'Camus', True, True),
    ('bnef.com', 'BNEF', True, True),
    ('sightlineclimate.com', 'Sightline', False, True),
    ('forourclimate.org', 'Solutions for Our Climate', False, True),
    ('bloomberg.com', 'BloombergNEF', False, True),       # partial: only a last resort
    ('amazonresearch.org', 'Amazon', False, True),
    ('ampsortation.com', 'AMP', False, False),           # short names must match exactly
    ('google.com', 'Tesla', False, False),
])
def test_domain_matching(domain, name, exact, partial):
    assert domain_exactly_names(domain, name) is exact
    assert domain_matches_name(domain, name) is partial


@pytest.mark.parametrize('desc, kind', [
    ('collegiate research university in Oxford', 'academic'),
    ('cabinet-level department of the United States government', 'government'),
    ('American think tank', 'research'),
    ('American daily newspaper', 'media'),
    ('American venture capital firm', 'investor'),
    ('American nonprofit environmental advocacy group', 'nonprofit'),
    ('geothermal energy company', 'company'),
    ('Middle-earth character', None),
])
def test_type_from_description(desc, kind):
    assert type_from_description(desc) == kind


def test_one_word():
    assert one_word('Terra') and one_word('The Guardian') and one_word('Ember')
    assert not one_word('Fervo Energy') and not one_word('Invenergy') and not one_word('Nexamp')


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *a, **kw):
        pass

    def fetchall(self):
        return self.rows


def person(host_id, first, last, desc, episode_id=1, **have):
    return {'host_id': host_id, 'first_name': first, 'last_name': last, 'linkedin_url': have.get('linkedin_url'),
            'twitter_handle': have.get('twitter_handle'), 'bluesky_handle': None,
            'field_sources': have.get('field_sources', {}), 'episode_id': episode_id, 'description': desc}


def test_people_links_trust_only_links_with_the_persons_name():
    desc = ('Guests: Jane Doe (https://www.linkedin.com/in/jane-doe-123) and Bob Roe '
            'https://www.linkedin.com/in/jon-alexander-11b66345 · host https://x.com/SilasMahner')
    plan, _ = plan_people_links(FakeCursor([person(1, 'Jane', 'Doe', desc), person(2, 'Bob', 'Roe', desc)]))
    got = {p['name']: {k: v for k, v in p.items() if k not in ('host_id', 'name')} for p in plan}
    # Jane's own profile; Bob gets neither the co-guest's profile nor the host's account.
    assert got == {'Jane Doe': {'linkedin_url': 'https://www.linkedin.com/in/jane-doe-123'}}


def test_people_links_never_overwrite():
    desc = 'Jane Doe https://www.linkedin.com/in/jane-doe-123 https://twitter.com/JaneDoe'
    plan, _ = plan_people_links(FakeCursor([
        person(1, 'Jane', 'Doe', desc, linkedin_url='https://www.linkedin.com/in/jane-typed',
               field_sources={'twitter_handle': 'admin'})]))
    assert plan == []


# ---- trusted_entity (Sep 30 2026 audit: 98 of 1,466 Wikidata matches wrong) ----
from enrich import trusted_entity, load_rejected_wikidata  # noqa: E402


def _entity(site=None, enwiki=True, org_claims=True):
    claims = {}
    if site:
        claims['P856'] = [{'mainsnak': {'datavalue': {'value': site}}}]
    if org_claims:
        claims['P159'] = [{'mainsnak': {'datavalue': {'value': {'id': 'Q1'}}}}]
    return {'id': 'Q9', 'claims': claims, 'sitelinks': {'enwiki': {'title': 'x'}} if enwiki else {}}


def test_show_note_link_confirms():
    e = _entity(site='https://www.fervoenergy.com')
    assert trusted_entity({'name': 'Fervo'}, [e], ['fervoenergy.com'], None) is e


def test_clearbit_guess_no_longer_confirms_a_one_word_namesake():
    # "Crux" -> the online newspaper; Clearbit guessed the newspaper's site too.
    e = _entity(site='https://cruxnow.com')
    assert trusted_entity({'name': 'Crux'}, [e], None, 'cruxnow.com') is None


def test_clearbit_still_vetoes_a_different_site():
    e = _entity(site='https://other.com')
    assert trusted_entity({'name': 'Boston Metal'}, [e], None, 'bostonmetal.com') is None


def test_word_or_concept_without_org_claims_rejected():
    # "Third Derivative" -> the calculus concept: Wikipedia article, no org claims.
    e = _entity(org_claims=False)
    assert trusted_entity({'name': 'Third Derivative'}, [e], None, None) is None


def test_multiword_organisation_fallback_still_trusted():
    e = _entity()
    assert trusted_entity({'name': 'Rhodium Group'}, [e], None, None) is e


def test_rejected_list_loads():
    rejected = load_rejected_wikidata()
    assert 'Q56277981' in rejected.get(311, set())   # Crux -> the online newspaper


# --- people-x: X handles from show notes ---

from enrich import x_link_candidates, x_name_matches  # noqa: E402

_ZACK = {'host_id': 1, 'first_name': 'Zack', 'last_name': 'Colman'}
_HANNAH = {'host_id': 2, 'first_name': 'Hannah', 'last_name': 'Ritchie'}
_HOST = {'host_id': 3, 'first_name': 'Amy', 'last_name': 'Westervelt'}


class TestXLinkCandidates:
    def test_link_text_is_the_name(self):
        d = '<p>He took a role regulating it.</p><p><a href="https://twitter.com/zcolman?lang=en">Zack Colman</a></p>'
        assert x_link_candidates(d, [_ZACK, _HOST]) == [(1, 'zcolman', 'link_text')]

    def test_generic_link_in_one_guests_paragraph(self):
        d = ('<p>Hannah Ritchie | <a href="https://ourworldindata.org/">Our World in Data</a> | '
             '<a href="https://twitter.com/_hannahritchie?lang=en">Twitter (X)</a></p>')
        assert x_link_candidates(d, [_HANNAH, _HOST]) == [(2, '_hannahritchie', 'handle_name')]

    def test_generic_link_without_name_in_handle(self):
        d = '<p>Guest: Hannah Ritchie, data scientist. <a href="https://x.com/hrdata">Twitter</a></p>'
        assert x_link_candidates(d, [_HANNAH]) == [(2, 'hrdata', 'same_paragraph')]

    def test_name_then_handle(self):
        d = 'Today we talk to Zack Colman (@zcolman) about the EPA.'
        assert x_link_candidates(d, [_ZACK]) == [(1, 'zcolman', 'name_then_handle')]

    def test_two_names_in_a_paragraph_is_not_enough(self):
        d = '<p>Zack Colman and Hannah Ritchie join us. <a href="https://twitter.com/somepod">Twitter</a></p>'
        assert x_link_candidates(d, [_ZACK, _HANNAH]) == []

    def test_show_account_in_its_own_paragraph_not_attributed(self):
        d = '<p>Zack Colman joins us.</p><p>Follow us on <a href="https://twitter.com/WeAreDrilled">Twitter.</a></p>'
        assert x_link_candidates(d, [_ZACK]) == []

    def test_guest_card_over_several_lines(self):
        d = ('<p dir="ltr">Edwina Floch</p> <p dir="ltr">Founder, The Environmental Music Prize</p> '
             '<p dir="ltr"><a href="https://au.linkedin.com/in/edwinafloch">LinkedIn</a> | '
             '<a href="https://twitter.com/EdwinaFloch">Twitter</a></p>')
        ed = {'host_id': 9, 'first_name': 'Edwina', 'last_name': 'Floch'}
        assert (9, 'EdwinaFloch', 'handle_name') in x_link_candidates(d, [ed])
        d2 = d.replace('EdwinaFloch', 'EnvMusicPrize')
        assert x_link_candidates(d2, [ed]) == [(9, 'EnvMusicPrize', 'same_paragraph')]

    def test_cited_tweets_are_not_profiles(self):
        d = '<p>Zack Colman on the map: <a href="https://x.com/MichaelFWehner/status/1840894606503821713">this</a></p>'
        assert x_link_candidates(d, [_ZACK]) == []

    def test_handle_spelling_the_name_anywhere(self):
        d = '<p>Zack Colman joins.</p><p>Links</p><p>Mentioned:</p><p><a href="https://twitter.com/ZackColman">profile</a></p>'
        assert (1, 'ZackColman', 'handle_name') in x_link_candidates(d, [_ZACK])

    def test_reserved_paths_and_emails_ignored(self):
        d = '<p>Zack Colman: <a href="https://twitter.com/intent/tweet?text=hi">share</a> mail zack@example.com</p>'
        assert x_link_candidates(d, [_ZACK]) == []


class TestXNameMatches:
    def test_plain_and_decorated_names(self):
        assert x_name_matches('Zack', 'Colman', 'Zack Colman', 'zcolman')
        assert x_name_matches('Amy', 'Westervelt', 'AmyWestervelt (🔋,🕸)', 'WesterveltAmy')
        assert x_name_matches('Robinson', 'Meyer', 'Robinson Meyer 🔥', 'robinsonmeyer')
        assert x_name_matches('Katharine', 'Wilkinson', 'Dr. Katharine Wilkinson', 'DrKWilkinson')
        assert x_name_matches('Isabel Cavelier', 'Adarve', 'Isabel Cavelier', 'isabelcavelier')
        assert x_name_matches("Tamara", "Toles O'Laughlin", "Tamara Toles O'Laughlin", 'Tamaraity')
        assert x_name_matches('Chris', 'Neidl', 'Neidl.c', 'neidl_c')

    def test_initial_yes_nickname_no(self):
        assert x_name_matches('Jennifer', 'Granholm', 'J. Granholm', 'jgranholm')
        # Nicknames aren't guessed: "Bob" for Robert is left out (cautious;
        # nothing is applied for it).
        assert not x_name_matches('Robert', 'Smith', 'Bob Smith', 'bobsmith')

    def test_other_people_and_companies_rejected(self):
        assert not x_name_matches('Zack', 'Colman', 'POLITICO', 'politicopro')
        assert not x_name_matches('Hannah', 'Ritchie', 'Our World in Data', 'OurWorldInData')
        assert not x_name_matches('Zack', 'Colman', 'Jane Colman', 'janecolman')
        # The handle can't vouch for itself.
        assert not x_name_matches('Li', 'Wang', 'lili', 'liwang22')


from enrich import bio_names_org  # noqa: E402


class TestBioNamesOrg:
    def test_whole_words_and_mentions(self):
        assert bio_names_org('Managing Partner at Energy Impact Partners. Views mine.', 'Energy Impact Partners')
        assert bio_names_org('Editor @Heatmap_News', 'Heatmap')
        assert bio_names_org('CEO of Fervo Energy', 'Fervo Energy')

    def test_not_inside_other_words_or_too_short(self):
        assert not bio_names_org('I love metadata', 'Meta')
        assert not bio_names_org('Bollywood actor and producer', 'Fervo Energy')
        assert not bio_names_org('Works at IEA', 'IEA')   # too short to be evidence


# --- show-orgs ---

from enrich import show_key, looks_like_company  # noqa: E402


class TestShowOrgs:
    def test_show_key_drops_show_words(self):
        assert show_key('The Pexapark Podcast') == show_key('Pexapark')
        assert show_key('Catalyst with Shayle Kann') == show_key('Catalyst')
        assert show_key('Cleaning Up: Leadership in an Age of Climate Change') == show_key('Cleaning Up')

    def test_company_or_show_by_peoples_roles(self):
        assert looks_like_company({'titles': ['CEO and co-founder', 'COO']})            # Pexapark
        assert not looks_like_company({'titles': ['founder and co-host', 'co-host']})   # Climate One
        assert not looks_like_company({'titles': ['creator', 'Florida solar expert']})  # Solar Surge
        assert not looks_like_company({'titles': []})
        # A website or Wikidata entry isn't evidence: shows have those too.
        assert not looks_like_company({'website_url': 'https://climateone.org', 'titles': ['co-host']})


# --- people-bsky ---

from enrich import bsky_confirms  # noqa: E402


class TestBskyConfirms:
    _JESSE = {'first_name': 'Jesse', 'last_name': 'Jenkins', 'orgs': ['Princeton University', 'ZERO Lab'], 'websites': []}

    def test_bio_names_an_organisation(self):
        a = {'handle': 'jessedjenkins.com', 'displayName': 'Jesse D. Jenkins',
             'description': 'Macro-energy systems. Associate professor at Princeton University.'}
        assert bsky_confirms(self._JESSE, a) == 'bio names Princeton University'

    def test_namesake_rejected(self):
        old = {'handle': 'jessejenkins.bsky.social', 'displayName': 'Go visit @jessedjenkins.com',
               'description': 'Assistant professor at Princeton University.'}
        namesake = {'handle': 'jj.bsky.social', 'displayName': 'Jesse Jenkins', 'description': 'Drummer. Cats.'}
        # The old account is his too (it points at the new one); the plan
        # prefers the account named "Jesse D. Jenkins" in words.
        assert bsky_confirms(self._JESSE, old) is not None
        assert bsky_confirms(self._JESSE, namesake) is None

    def test_handle_on_their_organisations_domain(self):
        p = {'first_name': 'Jane', 'last_name': 'Doe', 'orgs': ['Fervo Energy'], 'websites': ['https://www.fervoenergy.com']}
        assert bsky_confirms(p, {'handle': 'jane.fervoenergy.com', 'displayName': 'Jane Doe', 'description': ''}) \
            == 'handle on fervoenergy.com'
