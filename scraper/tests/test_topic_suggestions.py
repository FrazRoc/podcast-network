"""topic_suggestions: which topic pairs Topic Admin suggests merging."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))
from topic_suggestions import same_word, spelling_pair, suggestion_pairs  # noqa: E402


@pytest.mark.parametrize('x,y', [('investment', 'investing'), ('china', 'chinese'), ('transportation', 'transport'),
                                 ('upcycling', 'upcycled'), ('heat', 'heating'), ('stack', 'stacking')])
def test_same_word(x, y):
    assert same_word(x, y)


@pytest.mark.parametrize('x,y', [('electrician', 'electricity'), ('activism', 'action'), ('community', 'communication'),
                                 ('transition', 'transactive'), ('motorsport', 'motor')])
def test_different_words(x, y):
    assert not same_word(x, y)


def test_spelling_pair():
    assert spelling_pair({'clean', 'energy', 'investment'}, {'clean', 'energy', 'investing'})
    assert not spelling_pair({'clean', 'energy'}, {'clean', 'energy', 'investing'})   # narrower, not a spelling


def tag(name, episodes=1, parent=None):
    return {'name': name, 'episodes': episodes, 'parent_tag_id': parent}


def test_pairs():
    tags = {1: tag('US-EU trade', 4), 2: tag('EU-US trade', 1),
            3: tag('clean energy investment', 114), 4: tag('clean energy investing', 3),
            5: tag('offshore wind', 165), 6: tag('UK offshore wind', 27),          # narrower: not suggested
            7: tag('renewable heat', 2), 8: tag('renewable heating', 1, parent=7),  # already under it
            9: tag('IPPs', 2), 10: tag('independent power producers', 16),
            11: tag('EPA', 104), 12: tag('energy policy analysis', 1)}             # initials only
    texts = lambda ids: {9: ['independent power producers (ipps) are struggling'], 11: ['the epa said']}
    got = {(p['tag_a'], p['tag_b']): p['reason'] for p in suggestion_pairs(tags, set(), texts)}
    assert got == {(1, 2): 'same_words', (3, 4): 'spelling', (9, 10): 'acronym'}
    # A pair marked different is never suggested again.
    assert (3, 4) not in {(p['tag_a'], p['tag_b']) for p in suggestion_pairs(tags, {(3, 4)}, texts)}
    # Biggest first.
    assert suggestion_pairs(tags, set(), texts)[0]['tag_a'] == 3
