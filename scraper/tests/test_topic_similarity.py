"""topic_similarity: topic fingerprints and "talks about similar things"."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))

import topic_similarity as ts

# 10 wind, 11 offshore wind (under wind), 12 floating offshore wind (under
# offshore wind), 20 solar, 30 nuclear.
PARENTS = {10: None, 11: 10, 12: 11, 20: None, 30: None}


def _rows(entity, start, tags_per_episode):
    return [(entity, start + i, t, j == 0) for i, tags in enumerate(tags_per_episode) for j, t in enumerate(tags)]


def test_expand_walks_up_and_stops_on_loops():
    assert ts.expand(12, PARENTS) == [12, 11, 10]
    assert ts.expand(1, {1: 2, 2: 1}) == [1, 2]


def test_narrower_topics_meet_at_their_parent():
    rows = (_rows('a', 0, [[12], [12]]) + _rows('b', 10, [[11], [11]])
            + _rows('c', 20, [[30], [30]]))
    index = ts.build_index(rows, PARENTS)
    res = ts.similar(index, 'a')
    assert [r[0] for r in res] == ['b']
    assert res[0][2] == [11]


def test_rare_topics_count_for_more():
    # x shares solar (everyone has it) with y, and nuclear (rare) with z.
    rows = (_rows('x', 0, [[20, 30], [20, 30]]) + _rows('y', 10, [[20], [20]])
            + _rows('z', 20, [[30], [30]]) + _rows('w', 30, [[20], [20]])
            + _rows('v', 40, [[20], [20]]))
    order = [r[0] for r in ts.similar(ts.build_index(rows, PARENTS), 'x')]
    assert order[0] == 'z'


def test_one_episode_and_co_hosts_left_out():
    rows = (_rows('a', 0, [[20], [20], [20]]) + _rows('one', 10, [[20]])
            + [('cohost', e, 20, True) for e in (0, 1, 2)] + _rows('b', 20, [[20], [20]]))
    index = ts.build_index(rows, PARENTS)
    assert 'one' not in index['vectors']
    assert [r[0] for r in ts.similar(index, 'a')] == ['b']
    assert 'cohost' in [r[0] for r in ts.similar(index, 'a', max_shared=None)]


def test_shared_falls_back_to_single_episode_topics():
    rows = _rows('a', 0, [[20], [30]]) + _rows('b', 10, [[20], [10]])
    res = ts.similar(ts.build_index(rows, PARENTS), 'a')
    assert res[0][2] == [20]


def test_unknown_entity():
    assert ts.similar(ts.build_index([], PARENTS), 'nobody') == []
