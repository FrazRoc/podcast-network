"""topic_cleanup.plan(): reading a reviewed decisions CSV into merges,
renames and marks."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from topic_cleanup import plan  # noqa: E402


def row(tag_id, name, action='keep', into='', rename=''):
    return {'tag_id': tag_id, 'name': name, 'action': action, 'into': into, 'rename': rename,
            'category': '', 'parent': ''}


def test_merge_chain_goes_to_the_final_survivor():
    p = plan([row(1, 'Trump climate rollbacks', 'merge', 'Trump climate policy'),
              row(2, 'Trump climate policy', 'merge', 'Trump energy policy'),
              row(3, 'Trump energy policy', rename='Trump energy and climate policy')])
    assert sorted(p['merges']) == [(3, 1), (3, 2)]
    assert p['renames'] == {3: 'Trump energy and climate policy'}


def test_rename_on_a_merge_row_renames_the_survivor():
    p = plan([row(1, 'political polarization', 'merge', 'polarization', 'political polarization'),
              row(2, 'polarization')])
    assert p['merges'] == [(2, 1)]
    assert p['renames'] == {2: 'political polarization'}


def test_marks():
    p = plan([row(1, 'EV news roundup', 'not_a_topic'), row(2, 'Solciety', 'company'), row(3, 'solar')])
    assert p['not_a_topic'] == [1] and p['company'] == [2] and p['merges'] == []


@pytest.mark.parametrize('rows', [
    [row(1, 'a', 'merge', 'missing')],
    [row(1, 'a', 'merge', 'b'), row(2, 'b', 'merge', 'a')],
    [row(1, 'a', 'delete')],
    [row(1, 'a', 'merge', 'c', 'x'), row(2, 'b', 'merge', 'c', 'y'), row(3, 'c')],
])
def test_bad_decisions_refused(rows):
    with pytest.raises(ValueError):
        plan(rows)


def broad(tag_id, name, category, parent=''):
    return {**row(tag_id, name, 'broad'), 'category': category, 'parent': parent}


def test_broad_topics_and_parents():
    p = plan([broad(-1, 'Solar', 'Power generation'),
              broad(5, 'Wind', 'Power generation'),
              {**row(1, 'rooftop solar'), 'parent': 'Solar'},
              {**row(2, 'solar roofs', 'merge', 'rooftop solar')},
              {**row(3, 'rooftop solar costs'), 'parent': 'solar roofs'}])
    assert p['broad'] == {-1: 'Power generation', 5: 'Power generation'}
    # A parent named by a merged-away spelling resolves to the survivor.
    assert p['parents'] == {1: -1, 3: 1}


@pytest.mark.parametrize('rows', [
    [broad(-1, 'Solar', 'Not a category')],
    [row(-1, 'no id', 'keep')],
    [{**row(1, 'a'), 'parent': 'b'}, {**row(2, 'b'), 'parent': 'a'}],
    [{**row(1, 'a'), 'parent': 'a'}],
    [{**row(1, 'a', 'merge', 'b'), 'parent': 'b'}, row(2, 'b')],
])
def test_bad_tree_refused(rows):
    with pytest.raises(ValueError):
        plan(rows)
