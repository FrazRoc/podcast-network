"""
Politicians' roles, normalised to one title at one organisation (Evan, Sep
2026), so the Stats charts and Company Admin count them together:

    U.S. House       Representative @ U.S. House
    U.S. Senate      Senator @ U.S. Senate
    state level      State Senator / State Representative / Assemblymember /
                     Governor / Lieutenant Governor / Attorney General
                     @ State of <State>
    city level       Mayor / Vice Mayor / City Councilmember @ City of <City>
    U.S. cabinet     Secretary of Energy @ U.S. Department of Energy (and the
                     other departments), Administrator @ U.S. Environmental
                     Protection Agency, Commissioner / Chairman @ FERC

Episode text rarely names the chamber — it says "Senator Ed Markey of
Massachusetts" or "Rep. Kathy Castor (D-FL)" — so the state comes from the
title, the company as written, or the text around the person's name.
normalize_political_role() returns None when a title isn't one of these
or the place can't be worked out; those are left as they are.

Party labels ("(D-MA)", "Republican") are dropped from the title: the
state is in the organisation now, and a party isn't a job title.
"""

import re

US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts',
    'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana',
    'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico',
    'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
}
_STATE_BY_NAME = {v.lower(): v for v in US_STATES.values()}
_STATE_BY_NAME.update({'hawai‘i': 'Hawaii', "hawai'i": 'Hawaii', 'washington state': 'Washington'})
# Longest first, so "West Virginia" wins over "Virginia".
_STATE_NAME_RE = re.compile(
    r"\b(" + '|'.join(sorted((re.escape(n) for n in _STATE_BY_NAME), key=len, reverse=True)) + r")\b", re.I)
# "(D-MA)", "(R-UT)", "D-CA 2nd District", "R-Alaska"
_PARTY_STATE_RE = re.compile(r"\(?\b[DRI]-\s?([A-Z]{2}|[A-Z][a-z]+(?: [A-Z][a-z]+)?)\b[^)]*\)?")
# Abbreviations only as whole words in capitals: "CA Senator", "PA".
_STATE_ABBR_RE = re.compile(r"\b(" + '|'.join(US_STATES) + r")\b")

_FORMER_RE = re.compile(r'^\s*(?:former|ex-)\s*', re.I)
_PARTY_WORD_RE = re.compile(r'\b(?:republican|democratic|democrat|independent|GOP)\s+', re.I)

_CABINET = {
    'energy': 'U.S. Department of Energy', 'the interior': 'U.S. Department of the Interior',
    'interior': 'U.S. Department of the Interior', 'state': 'U.S. Department of State',
    'agriculture': 'U.S. Department of Agriculture', 'transportation': 'U.S. Department of Transportation',
    'commerce': 'U.S. Department of Commerce', 'defense': 'U.S. Department of Defense',
    'the treasury': 'U.S. Department of the Treasury', 'treasury': 'U.S. Department of the Treasury',
    'labor': 'U.S. Department of Labor',
}

# Mayors of these cities work for a body that isn't called "City of …".
_CITY_BODY = {'london': 'Greater London Authority'}


def state_from(*texts) -> str | None:
    """The first U.S. state named in any of the texts: a party-state tag
    ("(D-MA)"), a full name, or a capitalised abbreviation."""
    for text in texts:
        if not text:
            continue
        for m in _PARTY_STATE_RE.finditer(text):
            s = m.group(1)
            if s.upper() in US_STATES:
                return US_STATES[s.upper()]
            if s.lower() in _STATE_BY_NAME:
                return _STATE_BY_NAME[s.lower()]
        m = _STATE_NAME_RE.search(text)
        if m:
            return _STATE_BY_NAME[m.group(1).lower()]
        m = _STATE_ABBR_RE.search(text)
        if m and m.group(1) not in ('IN', 'OR', 'ME', 'HI', 'OK'):   # also ordinary words
            return US_STATES[m.group(1)]
    return None


def _near(text: str | None, person: str | None, width: int = 160) -> str:
    """The part of the episode text around the person's surname."""
    if not text or not person:
        return ''
    surname = person.split()[-1]
    spans = [text[max(0, m.start() - width): m.end() + width]
             for m in re.finditer(re.escape(surname), text)]
    return ' … '.join(spans)


def _city(company: str | None) -> str | None:
    """"Boise, Idaho" -> "Boise"; "Freetown in Sierra Leone" -> "Freetown";
    "Rawlins WY" -> "Rawlins". None for anything that isn't a bare place."""
    if not company:
        return None
    c = re.sub(r'^(?:the\s+)?city of\s+', '', company.strip(), flags=re.I)
    c = re.split(r',|\s+in\s+|\s+\(', c)[0].strip()
    c = re.sub(r'\s+(' + '|'.join(US_STATES) + r')$', '', c)
    if not c or len(c.split()) > 3 or re.search(r"\b(council|office|department|government|authority|borough)\b", c, re.I):
        return None
    return {'LA': 'Los Angeles', 'San Jose': 'San José', 'Ft. Wayne': 'Fort Wayne'}.get(c, c)


def normalize_political_role(title: str | None, company: str | None, context: str | None = None,
                             person: str | None = None, known_places: tuple = ()) -> dict | None:
    """{'title', 'company', 'is_former'} for a politician's role, or None.

    context is the episode text, person the guest's name (to read only
    the text around them); known_places are other place names on record
    for this appearance (a company blanked by an earlier cleanup)."""
    if not title:
        return None
    t = title.strip()
    former = bool(_FORMER_RE.match(t))
    t = _FORMER_RE.sub('', t)
    party_state = None
    m = _PARTY_STATE_RE.search(t)
    if m:
        party_state = m.group(0)
        t = _PARTY_STATE_RE.sub('', t)
    t = _PARTY_WORD_RE.sub('', t).strip(' ,-')
    low = t.lower()
    # Around the person's name when known; otherwise around the title itself
    # (the extractor has the text but not the name: "Rep. Kathy Castor (D-FL)").
    if person:
        near = _near(context, person)
    else:
        near = ' … '.join(context[max(0, m.start() - 60): m.end() + 120]
                          for m in re.finditer(re.escape(title.strip()), context or ''))

    def state():
        return state_from(t, party_state, company, *known_places, near)

    def at_state(role):
        s = state()
        return {'title': role, 'company': f'State of {s}', 'is_former': former} if s else None

    # --- Cabinet and agencies (checked first: "Secretary of State" is not a state) ---
    m = re.search(r'secretary of (the interior|the treasury|energy|interior|state|agriculture|transportation|'
                  r'commerce|defense|treasury|labor)\b|\b(energy|interior|transportation|agriculture) secretary\b', low)
    us = re.search(r'\bu\.?s\.?\b|\bus\b|united states|\bDOE\b|department of energy|america', f'{title} {company or ""}', re.I)
    # "Secretary of State for Energy Security" is the UK's form, unless the title says U.S.
    if m and not re.search(r'\battach[eé]\b', low) and (us or not re.search(r'secretary of state for\b', low)):
        dept = m.group(1) or m.group(2)
        dept = {'interior': 'the interior', 'treasury': 'the treasury'}.get(dept, dept)
        name = f"Secretary of {dept.title().replace('The ', 'the ')}"
        foreign = re.search(r'\b(uk|united kingdom|britain|germany|canada|australia|india|philippines|mexico|'
                            r'france|japan|china)\b', f'{company or ""} {" ".join(known_places)}', re.I)
        # These department names are the U.S. cabinet's unless the role points elsewhere.
        if not us and not foreign and dept in ('energy', 'the interior', 'defense', 'the treasury'):
            us = True
        if not us:
            place = state_from(company, *known_places)
            if place and dept != 'state':
                return {'title': name, 'company': f'State of {place}', 'is_former': former}   # a state's own
            if not re.search(r'\b(granholm|moniz|chu|perry|brouillette|wright|salazar|haaland|zinke|'
                             r'bernhardt|burgum|jewell|vilsack|buttigieg|dabbar|sandalow|kerry|pompeo|'
                             r'blinken|rubio|hegseth|austin|mattis|hagel)\b', (person or '').lower()):
                return None                     # could be another country's
        # "Under Secretary of Energy", "Deputy Assistant Secretary of Defense for …",
        # "Counsellor to the Secretary of the Treasury" keep their own title.
        plain = re.fullmatch(r"(?:(?:u\.?s\.?|us|united states|america's)\s+)?(?:\d+(?:st|nd|rd|th)\s+"
                             r"(?:united states\s+)?)?(?:secretary of [a-z ]+|[a-z]+ secretary)", low.strip())
        kept = re.sub(r"^(?:u\.?s\.?|us|united states|america's)\s+", '', t, flags=re.I)   # the company says U.S.
        return {'title': name if plain else kept, 'company': _CABINET[dept], 'is_former': former}
    if re.search(r'\bepa administrator\b|administrator of the (epa|environmental protection agency)', low):
        return {'title': 'Administrator', 'company': 'U.S. Environmental Protection Agency', 'is_former': former}
    m = re.search(r'\bferc (commissioner|chair(?:man|woman)?)\b', low)
    if m:
        return {'title': m.group(1).capitalize(), 'company': 'FERC', 'is_former': former}

    # --- State level (before the federal forms: "state senator" is not the U.S. Senate) ---
    if re.search(r'\bstate senator\b', low):
        return at_state('State Senator')
    if re.search(r'\bstate (representative|rep\b|rep\.)|\bstate house (rep|representative)', low):
        return at_state('State Representative')
    if re.search(r'\bassembly ?(member|man|woman)\b', low):
        return at_state('Assemblymember')
    if re.search(r'\bstate legislator\b', low):
        return at_state('State Legislator')
    if re.search(r'\blieutenant governor\b|\blt\.? governor\b', low):
        return at_state('Lieutenant Governor')
    if re.search(r'\battorney general\b', low):
        if re.search(r'\bu\.?s\.? attorney general|attorney general of the united states', low):
            return {'title': 'Attorney General', 'company': 'U.S. Department of Justice', 'is_former': former}
        return at_state('Attorney General')
    if re.search(r'\bgovernor\b', low):
        if company and not state_from(company) and not re.search(r'\bstate\b', company, re.I):
            return None                         # a governor of a bank, institute or board
        return at_state('Governor')

    # --- Federal legislators ---
    if re.search(r'\bsenator\b|\bsen\.', low):
        if re.search(r'\b(canada|australia|australian|philippines|france|italy|brazil|(?<!new )mexico)\b',
                     f'{company or ""} {" ".join(known_places)}', re.I):
            return None                         # another country's senate
        if not (state() or re.search(r'\bu\.?s\.?\b|united states', f'{title} {company or ""}', re.I)):
            return None
        return {'title': 'Senator', 'company': 'U.S. Senate', 'is_former': former}
    if re.search(r'\b(field|country|regional|account|sales|special|permanent|trade)\s+(senior\s+)?rep', low):
        return None
    if re.search(r'\b(representative|rep\.?|congress(man|woman|member|person)|member of congress)\b', low):
        if not (state() or re.search(r'\bu\.?s\.?\b|united states|congress', f'{title} {company or ""}', re.I)):
            return None
        return {'title': 'Representative', 'company': 'U.S. House', 'is_former': former}

    # --- City level ---
    m = re.search(r'\b(lord mayor|deputy mayor|vice mayor|mayor)\b', low)
    if m:
        city = _city(company) or next((c for c in map(_city, known_places) if c), None)
        if not city:
            m2 = (re.search(r'(?i:mayor) of ([A-Z][\w.]+(?: [A-Z][\w.]+)?)', t)
                  or re.match(r'([A-Z][\w.]+(?: [A-Z][\w.]+)??) (?i:vice |deputy |lord )?(?i:mayor)', t))
            city = m2 and _city(m2.group(1))
            if city and city.lower() in ('vice', 'deputy', 'lord', 'former', 'acting'):
                city = None
        if not city:
            return None
        body = _CITY_BODY.get(city.lower(), f'City of {city}')
        return {'title': m.group(1).title().replace('Of', 'of'), 'company': body, 'is_former': former}
    if re.search(r'\b(city )?council ?(member|man|woman)|\bcouncilor\b|\balderman\b', low) and 'advisory' not in low:
        city = _city(company) or next((c for c in map(_city, known_places) if c), None)
        if not city:
            return None
        return {'title': 'City Councilmember', 'company': f'City of {city}', 'is_former': former}
    return None


def normalize_political_roles(title, company, context=None, person=None, known_places=()) -> list | None:
    """Like normalize_political_role, for a title that may hold two roles
    ("Former U.S. Senator and Secretary of State", "C40 Co-Chair and
    Mayor"): each part becomes its own role. A part that isn't political
    keeps the company only if no political part used it as its place.
    None when no part is political."""
    if re.search(r'\battach[eé]\b', title or '', re.I):
        return None                             # an attaché to officials, not the official
    body = _FORMER_RE.sub('', title or '')
    former = bool(_FORMER_RE.match(title or ''))
    parts = re.split(r'\s+and\s+', body)
    # "… for Energy Efficiency and Renewable Energy" is one title, not two.
    if len(parts) != 2 or re.search(r'\s(for|of)\s', parts[0], re.I):
        one = normalize_political_role(title, company, context, person, known_places)
        return [one] if one else None
    # "U.S. Senator and Secretary of State": the U.S. covers both halves.
    us = 'U.S. ' if re.search(r'\bu\.?s\.?\b|\bus\b|united states', body, re.I) else ''
    done = [normalize_political_role(('former ' if former else '')
                                     + (us if us and not re.search(r'\bu\.?s\.?\b|\bus\b|united states', p, re.I) else '')
                                     + p, company, context, person, known_places)
            for p in parts]
    if not any(done):
        return None
    out = [d for d in done if d]
    place = _city(company) or state_from(company)
    used = bool(place) and any(place in d['company'] for d in out)
    for p, d in zip(parts, done):
        if not d:
            keep = company if company and not used and not place else None
            out.append({'title': p.strip(), 'company': keep, 'is_former': former})
    return out
