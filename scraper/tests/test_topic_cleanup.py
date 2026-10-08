"""topic_cleanup.plan(): reading a reviewed decisions CSV into merges,
renames and marks."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from topic_cleanup import plan  # noqa: E402


def row(tag_id, name, action='keep', into='', rename=''):
    return {'tag_id': tag_id, 'name': name, 'action': action, 'into': into, 'rename': rename}


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
