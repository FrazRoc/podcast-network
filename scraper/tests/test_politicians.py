"""Politicians' roles normalised to one title at one organisation (backend/politicians.py)."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))
from politicians import normalize_political_role, normalize_political_roles  # noqa: E402


def one(title, company=None, context=None, person=None, places=()):
    r = normalize_political_role(title, company, context, person, places)
    return r and (r['title'], r['company'], r['is_former'])


@pytest.mark.parametrize('title, company, places, expected', [
    ('Sen.', None, ('Hawaii',), ('Senator', 'U.S. Senate', False)),
    ('U.S. Senator (D-MA)', None, (), ('Senator', 'U.S. Senate', False)),
    ('former Republican Senator', None, ('Nebraska',), ('Senator', 'U.S. Senate', True)),
    ('U.S. Senator', None, ('New Mexico',), ('Senator', 'U.S. Senate', False)),   # not Mexico
    ('Rep.', None, ('Florida',), ('Representative', 'U.S. House', False)),
    ('former six-term South Carolina Congressman', None, (), ('Representative', 'U.S. House', True)),
    ('Maine State Senator', None, (), ('State Senator', 'State of Maine', False)),
    ('Washington State House Rep.', None, (), ('State Representative', 'State of Washington', False)),
    ('Governor', 'the State of Washington', (), ('Governor', 'State of Washington', False)),
    ('Arizona Attorney General', None, (), ('Attorney General', 'State of Arizona', False)),
    ('secretary of agriculture', 'the state of Kansas', (), ('Secretary of Agriculture', 'State of Kansas', False)),
    ('Mayor', 'Boise, Idaho', (), ('Mayor', 'City of Boise', False)),
    ('Mayor of Phoenix, Arizona', None, (), ('Mayor', 'City of Phoenix', False)),
    ('Berkeley Vice Mayor', None, (), ('Vice Mayor', 'City of Berkeley', False)),
    ('mayor', 'London', (), ('Mayor', 'Greater London Authority', False)),
    ('Energy Secretary', None, (), ('Secretary of Energy', 'U.S. Department of Energy', False)),
    ('Under Secretary of Energy', None, (), ('Under Secretary of Energy', 'U.S. Department of Energy', False)),
    ('former EPA Administrator', None, (), ('Administrator', 'U.S. Environmental Protection Agency', True)),
    ('FERC Chair', None, (), ('Chair', 'FERC', False)),
])
def test_normalised(title, company, places, expected):
    assert one(title, company, places=places) == expected


@pytest.mark.parametrize('title, company, places', [
    ('Governor', 'The Bank of England', ()),               # a central bank governor
    ('Senator', 'Senate of Canada', ()),                   # another country's senate
    ('Senator', 'the Australian Capital Territory', ()),
    ('Indiana Senior Field Representative', 'Conservative Energy Network', ()),
    ('Special Representative for Climate', None, ()),
    ('Environmental Justice Advisory Council Member', 'White House', ()),
    ('Secretary of State for Energy Security and Net Zero', None, ('UK',)),   # the UK's form
    ('Senator', None, ()),                                 # no idea which senate: left alone
])
def test_left_alone(title, company, places):
    assert one(title, company, places=places) is None


def test_two_roles_in_one_title():
    roles = normalize_political_roles('Former U.S. Senator and Secretary of State', None)
    assert [(r['title'], r['company'], r['is_former']) for r in roles] == [
        ('Senator', 'U.S. Senate', True), ('Secretary of State', 'U.S. Department of State', True)]
    roles = normalize_political_roles('C40 Co-Chair and Mayor', 'London')
    assert [(r['title'], r['company']) for r in roles] == [('Mayor', 'Greater London Authority'), ('C40 Co-Chair', None)]
    roles = normalize_political_roles('U.S. Representative (D-CA 2nd District) and Ranking Member',
                                      'House Natural Resources Committee')
    assert [(r['title'], r['company']) for r in roles] == [
        ('Representative', 'U.S. House'), ('Ranking Member', 'House Natural Resources Committee')]


def test_one_title_with_and_inside_is_not_split():
    [r] = normalize_political_roles('Assistant Secretary of Energy for Energy Efficiency and Renewable Energy', None)
    assert r['company'] == 'U.S. Department of Energy'


def test_attache_is_left_alone():
    assert normalize_political_roles('attache to US Ambassadors, and Secretary of Defense Donald Rumsfeld', None) is None


def test_state_read_from_the_text_around_the_person():
    assert one('Rep.', context='Rep. Kathy Castor (D-FL) joins us', person='Kathy Castor') == \
        ('Representative', 'U.S. House', False)
