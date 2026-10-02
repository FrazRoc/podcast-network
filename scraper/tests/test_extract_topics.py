"""Topic tagging: the name key, the evidence check, and recording."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend'))

from topic_names import normalize_topic, topic_slug  # noqa: E402
from extract_topics import build_text, verified_topics, parse_response_text, record_results, item_id  # noqa: E402


class TestNormalizeTopic:
    @pytest.mark.parametrize('a,b', [
        ('SMRs', 'small modular reactors'),
        ('Small Modular Reactor (SMR)', 'small modular reactor'),
        ('VPPs', 'Virtual Power Plants'),
        ('EVs', 'electric vehicles'),
        ('carbon capture & storage', 'CCS'),
        ('The Inflation Reduction Act', 'inflation reduction act'),
        ('U.S.-China trade', 'US-China trade'),
        ('batteries', 'battery'),
        ('energy policies', 'energy policy'),
    ])
    def test_same_key(self, a, b):
        assert normalize_topic(a) == normalize_topic(b)

    @pytest.mark.parametrize('a,b', [
        ('offshore wind', 'onshore wind'),
        ('natural gas', 'natural gas prices'),
    ])
    def test_different_key(self, a, b):
        assert normalize_topic(a) != normalize_topic(b)

    def test_words_ending_in_s_kept(self):
        assert normalize_topic('natural gas') == 'natural gas'
        assert normalize_topic('climate politics') == 'climate politics'
        assert normalize_topic('biomass') == 'biomass'

    def test_slug(self):
        assert topic_slug('Small Modular Reactors') == 'small-modular-reactor'


TEXT = "[Show: Catalyst] Why geothermal is heating up\nShayle talks with Tim Latimer about enhanced geothermal drilling and data center power demand."


class TestVerifiedTopics:
    def test_keeps_verbatim_evidence(self):
        kept, dropped = verified_topics([
            {'topic': 'enhanced geothermal', 'category': 'Power generation', 'primary': True,
             'evidence': 'enhanced geothermal drilling'},
            {'topic': 'data center power demand', 'category': 'Tech and AI', 'primary': False,
             'evidence': 'data center power demand'},
        ], TEXT)
        assert [k['topic'] for k in kept] == ['enhanced geothermal', 'data center power demand']
        assert dropped == []

    def test_drops_paraphrased_evidence(self):
        kept, dropped = verified_topics([
            {'topic': 'geothermal', 'category': 'Power generation', 'primary': True,
             'evidence': 'next-generation geothermal wells'}], TEXT)
        assert kept == [] and dropped[0][1] == 'evidence not in text'

    def test_unknown_category_dropped(self):
        kept, _ = verified_topics([{'topic': 'geothermal', 'category': 'Energy', 'primary': True,
                                    'evidence': 'geothermal'}], TEXT)
        assert kept == []

    def test_one_primary_and_dedupe(self):
        kept, _ = verified_topics([
            {'topic': 'EVs', 'category': 'Transport', 'primary': True, 'evidence': 'geothermal'},
            {'topic': 'electric vehicles', 'category': 'Transport', 'primary': True, 'evidence': 'geothermal'},
            {'topic': 'geothermal', 'category': 'Power generation', 'primary': True, 'evidence': 'geothermal'},
        ], TEXT)
        assert [k['topic'] for k in kept] == ['EVs', 'geothermal']
        assert [k['primary'] for k in kept] == [True, False]

    def test_first_is_primary_when_none_marked(self):
        kept, _ = verified_topics([{'topic': 'geothermal', 'category': 'Power generation',
                                    'primary': False, 'evidence': 'geothermal'}], TEXT)
        assert kept[0]['primary'] is True

    def test_whole_words_only(self):
        kept, _ = verified_topics([{'topic': 'heat', 'category': 'Buildings', 'primary': True,
                                    'evidence': 'heat'}], TEXT)
        assert kept == []   # "heating" does not contain the word "heat"


def test_build_text_strips_and_labels():
    t = build_text('Volts', 'Title', '<p>Hello   world</p>')
    assert t.startswith('[Show: Volts] Title') and 'Hello world' in t
    assert build_text('Volts', '', '') is None


def test_parse_ignores_unknown_ids():
    out = parse_response_text('{"results":[{"id":"e1","topics":[]},{"id":"e9","topics":[]}]}', {'e1'})
    assert out == {'e1': []}


def test_record_results(db_conn):
    cur = db_conn.cursor()
    cur.execute("INSERT INTO podcasts (apple_podcast_id, title) VALUES ('1', 'Catalyst') RETURNING podcast_id")
    pid = cur.fetchone()[0]
    eps = []
    for i in range(2):
        cur.execute("INSERT INTO episodes (podcast_id, title, published_date) VALUES (%s, %s, '2026-01-01') "
                    "RETURNING episode_id", (pid, f't{i}'))
        eps.append(cur.fetchone()[0])
    for e in eps:
        cur.execute("INSERT INTO topic_extractions (episode_id, status, text_sent) VALUES (%s, 'pending', %s)", (e, TEXT))
    answers = {
        item_id(eps[0]): [{'topic': 'SMRs', 'category': 'Power generation', 'primary': True, 'evidence': 'geothermal'}],
        item_id(eps[1]): [{'topic': 'small modular reactors', 'category': 'Power generation', 'primary': True,
                           'evidence': 'geothermal'}],
    }
    done, added, retry, dropped = record_results(cur, [(eps[0], TEXT), (eps[1], TEXT)], answers)
    db_conn.commit()
    assert (done, added, retry, dropped) == (2, 2, 0, 0)
    cur.execute("SELECT COUNT(*) FROM tags")
    assert cur.fetchone()[0] == 1   # both spellings landed on one tag
    cur.execute("SELECT COUNT(DISTINCT tag_id), COUNT(*) FROM episode_tag")
    assert cur.fetchone() == (1, 2)
    cur.execute("SELECT status FROM topic_extractions ORDER BY episode_id")
    assert [r[0] for r in cur.fetchall()] == ['done', 'done']
