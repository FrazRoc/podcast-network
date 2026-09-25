"""scraper/credit_cleanup.py: replays and references to other episodes."""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import credit_cleanup as cc  # noqa: E402


def credit(ep, host, title, day, show=1, source='parsed_desc'):
    return {'episode_id': ep, 'host_id': host, 'podcast_id': show, 'title': title,
            'published_date': date(2026, 1, day), 'data_source': source}


def test_titles_normalise_but_keep_parts():
    assert cc.normalised_title('REWIND: Firefight') == cc.normalised_title('Firefight')
    assert cc.normalised_title('How Base Power plans [re-published]') == cc.normalised_title('How Base Power plans')
    assert cc.normalised_title('Kelp Farming (Part II)') != cc.normalised_title('Kelp Farming')
    assert cc.normalised_title('Eric Bosworth [Part 2]') != cc.normalised_title('Eric Bosworth')


def test_replays_of_an_earlier_credit_on_the_same_show():
    rows = [credit(1, 7, 'Firefight: How to Live in the Pyrocene', 1),
            credit(2, 7, 'REWIND: Firefight: How to Live in the Pyrocene', 20),
            credit(3, 8, 'Best of 2022: seven leaders', 25),          # 8's only credit: kept
            credit(4, 9, 'How Base Power plans to use its $1B', 2),
            credit(5, 9, 'How Base Power plans to use its $1B [re-published]', 9),
            credit(6, 10, 'Kelp Farming, for the Climate', 3),
            credit(7, 10, 'Kelp Farming, for the Climate (Part II)', 10)]  # new material
    assert sorted(cc.find_replays(rows)) == [(2, 7), (5, 9)]


def test_only_inferred_credits_and_same_show():
    rows = [credit(1, 7, 'Firefight', 1),
            credit(2, 7, 'REWIND: Firefight', 20, source='apple_verified'),   # Apple's label stands
            credit(3, 7, 'REWIND: Firefight', 21, show=2)]                    # another show
    assert cc.find_replays(rows) == []
