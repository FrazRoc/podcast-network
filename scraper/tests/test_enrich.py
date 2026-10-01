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
