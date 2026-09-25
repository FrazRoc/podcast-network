"""backend/title_orgs.py: an organisation written inside a job title."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))
from title_orgs import split_org_from_title  # noqa: E402

ORGS = {
    'grist': ('Grist', 'media'), 'bbc': ('BBC News', 'media'), 'bnef': ('BloombergNEF', 'research'),
    'bloomberg opinion': ('Bloomberg Opinion', 'media'), 'doe': ('U.S. Department of Energy', 'government'),
    'goldman': ('Goldman Sachs', 'investor'), 'c40': ('C40 Cities', 'nonprofit'), 'ted': ('TED', 'media'),
    'nature is nonpartisan': ('Nature is Nonpartisan', 'nonprofit'), 'science': ('Science Corporation', 'company'),
    'earth': ('Earth', None), 'the australian': ('The Australian', 'media'), 'australian': ('The Australian', 'media'),
    'evp': ('Environmental Voter Project', 'nonprofit'), 'tesla': ('Tesla', 'company'),
    'new york times': ('New York Times', 'media'), 'cop26': ('COP26', None), 'un': ('United Nations', 'government'),
    'un secretary general': ('UN Secretary-General', 'government'),
    'sustainable energy for all': ('Sustainable Energy for All', 'nonprofit'),
    'thich nhat hanh': ('Thich Nhat Hanh', None), 'hydrogen': ('Hydrogen Science Coalition', 'nonprofit'),
}


def lookup(key):
    hit = ORGS.get(key)
    return hit and {'name': hit[0], 'org_type': hit[1]}


@pytest.mark.parametrize('title, expected', [
    ('Grist reporter', ('Grist', 'reporter')),
    ('BBC Science Correspondent', ('BBC', 'Science Correspondent')),
    ("BBC’s Science Correspondent", ('BBC', 'Science Correspondent')),
    ('BNEF specialist in renewable fuels', ('BNEF', 'specialist in renewable fuels')),
    ('Bloomberg Opinion columnist', ('Bloomberg Opinion', 'columnist')),
    ('DOE Undersecretary for Infrastructure', ('DOE', 'Undersecretary for Infrastructure')),
    ('C40 Co-Chair', ('C40', 'Co-Chair')),
    ("head of Goldman’s Sustainable Finance Group", ('Goldman', "head of Goldman’s Sustainable Finance Group")),
    ('head of TED', ('TED', 'head')),
    ('founder of Nature Is Nonpartisan', ('Nature Is Nonpartisan', 'founder')),
    ('CEO and Special Representative of the UN Secretary-General for Sustainable Energy for All',
     ('Sustainable Energy for All', 'CEO and Special Representative of the UN Secretary-General')),
])
def test_found(title, expected):
    assert split_org_from_title(title, lookup) == expected


@pytest.mark.parametrize('title', [
    'Science fiction writer',                    # an ordinary word, not Science Corporation
    'Earth scientist',
    'Australian energy analyst',                 # not The Australian
    'EVP of Digital Energy',                     # a title, not the Environmental Voter Project
    'Tesla reporter',                            # reports on Tesla
    'New York Times bestselling author',         # praise, not a post
    'COP26 President',                           # an event
    'UN Messenger of Peace',                     # an honour
    "editor of Thich Nhat Hanh’s book",          # a book
    'Lead Expert for Hydrogen',                  # a subject
    'Professor of Science',
    'senior reporter',                           # nothing named
])
def test_not_found(title):
    assert split_org_from_title(title, lookup) is None


def test_own_organisation_for_an_ambiguous_possessive():
    # "Aurora" alone is ambiguous; the person's own record says which Aurora.
    got = split_org_from_title("Aurora’s Head of Consulting in Australia", lookup, ['Aurora Energy Research'])
    assert got == ('Aurora Energy Research', 'Head of Consulting in Australia')
    assert split_org_from_title("Aurora’s Head of Consulting", lookup, []) is None
