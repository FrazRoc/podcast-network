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
    assert one_word('Terra') and one_word('The Guardian') and not one_word('Fervo Energy')


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
