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


# ------------------------------------------------------------------
# Pass 2: other countries, diplomats, staff
# ------------------------------------------------------------------
from politicians import normalize_government_role, normalize_any_government_role  # noqa: E402


def two(title, company=None, places=()):
    r = normalize_government_role(title, company, known_places=places)
    return r and [(d['title'], d['company']) for d in r]


@pytest.mark.parametrize('title, company, places, expected', [
    ('UK Prime Minister', None, (), [('Prime Minister', 'Government of the United Kingdom')]),
    ('Prime Minister', None, ('Barbados',), [('Prime Minister', 'Government of Barbados')]),
    ('Chilean Minister of Energy and Mining', None, (), [('Minister of Energy and Mining', 'Government of Chile')]),
    ('UK Foriegn Minister', None, (), [('Foreign Minister', 'Government of the United Kingdom')]),
    ('First Minister of Scotland', None, (), [('First Minister', 'Scottish Government')]),
    ('Premier', None, ('Alberta',), [('Premier', 'Government of Alberta')]),
    ('MP', 'Kingswood', (), [('Member of Parliament', 'UK House of Commons')]),
    ('MP', 'Warringah', (), [('Member of Parliament', 'Australian House of Representatives')]),
    ('MP Shadow Minister', None, (), [('Shadow Minister', 'UK House of Commons'), ('Member of Parliament', 'UK House of Commons')]),
    ('US deputy special envoy for Iran', None, (), [('deputy special envoy for Iran', 'U.S. Department of State')]),
    ('Deputy Special Envoy for Climate Change', None, ('US',), [('Deputy Special Envoy for Climate Change', 'U.S. Department of State')]),
    ('UN Special Envoy on Climate Action and Finance', None, (), [('Special Envoy on Climate Action and Finance', 'United Nations')]),
    ('EU Ambassador to the U.S.', None, (), [('Ambassador to the U.S.', 'European Union')]),
    ('Egyptian Ambassador', None, (), [('Ambassador', 'Government of Egypt')]),
    ('President', None, ('Costa Rica',), [('President', 'Government of Costa Rica')]),
    ('US VP', None, (), [('VP', 'White House')]),
    ('White House Chief of Staff', None, (), [('Chief of Staff', 'White House')]),
    ('National Climate Advisor', None, (), [('National Climate Advisor', 'White House')]),
    ('climate adviser', 'the Obama administration', (), [('climate adviser', 'White House')]),
    ('Climate Advisor', 'Governor Newsom', (), [('Climate Advisor', 'State of California')]),
    ('climate policy advisor', 'Senator Elizabeth Warren', (), [('climate policy advisor', 'U.S. Senate')]),
    ('chief of staff', 'Prime Minister Justin Trudeau', (), [('chief of staff to the Prime Minister', 'Government of Canada')]),
])
def test_pass2_normalised(title, company, places, expected):
    assert two(title, company, places) == expected


@pytest.mark.parametrize('title, company', [
    ('President and CEO', None),                     # a company's president, company not named
    ('VP of Business Development', None),
    ('C40 Ambassador for Global Climate Diplomacy', None),
    ('UN Environment Goodwill Ambassador', None),
    ('investor, advisor and serial entrepreneur', None),
    ('Head of Advisory in Japan', None),
    ('Governor', 'The Bank of England'),
])
def test_pass2_left_alone(title, company):
    assert normalize_any_government_role(title, company) is None
