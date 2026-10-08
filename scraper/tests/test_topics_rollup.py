"""
Tests for backend/topics.py — rolling episode topics up to people, shows
and organisations.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'backend'))

from topics import rank_topics, category_mix, summary, episode_topics, rank_people  # noqa: E402


def row(ep, tag, name, cat='Power generation', primary=False):
    return {'episode_id': ep, 'tag_id': tag, 'name': name, 'category': cat, 'is_primary': primary}


ROWS = [
    row(1, 10, 'geothermal energy', primary=True),
    row(2, 10, 'geothermal energy', primary=True),
    row(3, 10, 'geothermal energy'),
    row(1, 20, 'permitting reform', 'Policy and politics'),
    row(2, 20, 'permitting reform', 'Policy and politics'),
    row(3, 20, 'permitting reform', 'Policy and politics', primary=True),
    row(4, 20, 'permitting reform', 'Policy and politics'),
    row(4, 30, 'oil prices', 'Fuels', primary=True),
]


class TestRankTopics:
    def test_main_topic_counts_twice(self):
        ranked = rank_topics(ROWS)
        # geothermal: 3 episodes + 2 as main topic = 5; permitting: 4 + 1 = 5;
        # tie broken by episodes.
        assert [t['name'] for t in ranked] == ['permitting reform', 'geothermal energy']
        assert ranked[1]['as_main_topic'] == 2 and ranked[1]['score'] == 5

    def test_single_episode_topics_hidden(self):
        assert 'oil prices' not in [t['name'] for t in rank_topics(ROWS)]
        assert 'oil prices' in [t['name'] for t in rank_topics(ROWS, min_episodes=1)]

    def test_duplicate_rows_count_once(self):
        ranked = rank_topics(ROWS + [row(1, 10, 'geothermal energy', primary=True)])
        assert next(t for t in ranked if t['tag_id'] == 10)['episodes'] == 3

    def test_slug_and_limit(self):
        ranked = rank_topics(ROWS, limit=1)
        assert len(ranked) == 1 and ranked[0]['slug'] == 'permitting-reform'


class TestCategoryMix:
    def test_episode_counted_once_per_category_in_fixed_order(self):
        mix = category_mix(ROWS)
        assert mix == [{'category': 'Power generation', 'episodes': 3},
                       {'category': 'Fuels', 'episodes': 1},
                       {'category': 'Policy and politics', 'episodes': 4}]


class TestSummary:
    def test_shares_are_of_tagged_episodes(self):
        s = summary(ROWS, tagged_episodes=8)
        assert s['tagged_episodes'] == 8
        assert {t['name']: t['share'] for t in s['topics']} == {'permitting reform': 50, 'geothermal energy': 38}

    def test_no_tagged_episodes(self):
        assert summary([], 0) == {'tagged_episodes': 0, 'areas': [], 'topics': [], 'categories': []}

    def test_broad_areas(self):
        # Rooftop and community solar on episode 1 count once for Solar;
        # a topic with no broad topic adds no area.
        solar = {'broad_id': 9, 'broad_name': 'Solar', 'broad_category': 'Power generation'}
        rows = [{'episode_id': 1, 'tag_id': 1, 'name': 'rooftop solar', 'category': 'Power generation', **solar},
                {'episode_id': 1, 'tag_id': 2, 'name': 'community solar', 'category': 'Power generation', **solar},
                {'episode_id': 2, 'tag_id': 2, 'name': 'community solar', 'category': 'Power generation',
                 'is_primary': True, **solar},
                {'episode_id': 2, 'tag_id': 3, 'name': 'podcasting', 'category': 'Society and justice'}]
        s = summary(rows, 4)
        assert [(a['name'], a['episodes'], a['as_main_topic'], a['share']) for a in s['areas']] == [('Solar', 2, 1, 50)]


class TestEpisodeTopics:
    def test_main_topic_first(self):
        by_ep = episode_topics(ROWS)
        assert [t['name'] for t in by_ep[3]] == ['permitting reform', 'geothermal energy']
        assert by_ep[3][0]['primary'] is True
        assert [t['name'] for t in by_ep[4]] == ['oil prices', 'permitting reform']


class TestRankPeople:
    def test_ranked_by_episodes_with_main_topic_weight(self):
        rows = [
            {'host_id': 1, 'name': 'Ann Lee', 'episode_id': 1, 'is_primary': False},
            {'host_id': 1, 'name': 'Ann Lee', 'episode_id': 2, 'is_primary': False},
            {'host_id': 2, 'name': 'Bo Diaz', 'episode_id': 3, 'is_primary': True},
            {'host_id': 2, 'name': 'Bo Diaz', 'episode_id': 3, 'is_primary': True},
            {'host_id': 3, 'name': 'Cy Ong', 'episode_id': 4, 'is_primary': False},
        ]
        people = rank_people(rows)
        assert [p['name'] for p in people] == ['Ann Lee', 'Bo Diaz', 'Cy Ong']
        assert people[1]['episodes'] == 1 and people[1]['as_main_topic'] == 1
        assert people[0]['slug'] == 'ann-lee'
