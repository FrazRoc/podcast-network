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


# ------------------------------------------------------------------
# Pass 2: other countries' politicians, diplomats, and politicians' staff
# ------------------------------------------------------------------
#
#   heads of government / ministers   <title> @ Government of <Country>
#     (sub-national: Scottish Government, Government of Alberta, Victorian Government)
#   members of parliament             Member of Parliament @ UK House of Commons (etc.)
#   U.S. envoys / ambassadors         <title> @ U.S. Department of State
#   UN / EU envoys                    <title> @ United Nations / European Union
#   U.S. President / Vice President   @ White House
#   staff of a politician             <title> @ the body the politician is in

# name or adjective -> (country, the body its government is called)
_COUNTRIES = {}
for names, country in [
    (('uk', 'u.k.', 'united kingdom', 'britain', 'british', 'england', 'english'), 'the United Kingdom'),
    (('canada', 'canadian'), 'Canada'), (('australia', 'australian'), 'Australia'),
    (('new zealand',), 'New Zealand'), (('norway', 'norwegian'), 'Norway'), (('barbados',), 'Barbados'),
    (('ireland', 'irish'), 'Ireland'), (('germany', 'german'), 'Germany'), (('france', 'french'), 'France'),
    (('spain', 'spanish'), 'Spain'), (('chile', 'chilean'), 'Chile'), (('denmark', 'danish'), 'Denmark'),
    (('greece', 'greek'), 'Greece'), (('ecuador',), 'Ecuador'), (('mexico', 'mexican'), 'Mexico'),
    (('panama', 'republic of panama'), 'Panama'), (('taiwan',), 'Taiwan'), (('luxembourg',), 'Luxembourg'),
    (('costa rica', 'costa rican'), 'Costa Rica'), (('ghana', 'ghanaian'), 'Ghana'),
    (('marshall islands', 'the marshall islands', 'royal marshall islands'), 'the Marshall Islands'),
    (('saint lucia',), 'Saint Lucia'), (('liberia', 'liberian'), 'Liberia'), (('indonesia', 'indonesian'), 'Indonesia'),
    (('egypt', 'egyptian'), 'Egypt'), (('el salvador',), 'El Salvador'), (('india', 'indian'), 'India'),
    (('japan', 'japanese'), 'Japan'), (('sweden', 'swedish'), 'Sweden'), (('singapore',), 'Singapore'),
    (('netherlands', 'dutch'), 'the Netherlands'), (('italy', 'italian'), 'Italy'), (('brazil', 'brazilian'), 'Brazil'),
    (('colombia', 'colombian'), 'Colombia'), (('kenya', 'kenyan'), 'Kenya'), (('nigeria', 'nigerian'), 'Nigeria'),
    (('south africa', 'south african'), 'South Africa'), (('china', 'chinese'), 'China'), (('finland', 'finnish'), 'Finland'),
    (('portugal', 'portuguese'), 'Portugal'), (('belgium', 'belgian'), 'Belgium'), (('fiji', 'fijian'), 'Fiji'),
    (('palau',), 'Palau'), (('bangladesh',), 'Bangladesh'), (('pakistan',), 'Pakistan'), (('maldives',), 'the Maldives'),
    (('uae', 'united arab emirates', 'emirati'), 'the United Arab Emirates'), (('saudi arabia', 'saudi'), 'Saudi Arabia'),
]:
    for n in names:
        _COUNTRIES[n] = country
_SUBNATIONAL = {'scotland': 'Scottish Government', 'scottish': 'Scottish Government', 'wales': 'Welsh Government',
                'welsh': 'Welsh Government', 'alberta': 'Government of Alberta', 'victoria': 'Victorian Government',
                'ontario': 'Government of Ontario', 'quebec': 'Government of Quebec',
                'british columbia': 'Government of British Columbia'}
_COUNTRY_RE = re.compile(r"(?<![\w.])(" + '|'.join(sorted((re.escape(n) for n in list(_COUNTRIES) + list(_SUBNATIONAL)),
                                                           key=len, reverse=True)) + r")(?:['’]s)?(?![\w])", re.I)
_PARLIAMENT = {'the United Kingdom': 'UK House of Commons', 'Australia': 'Australian House of Representatives',
               'Canada': 'House of Commons of Canada', 'New Zealand': 'New Zealand Parliament', 'Ireland': 'Dáil Éireann',
               'India': 'Lok Sabha'}
# UK constituencies that were stored as if they were employers.
_UK_CONSTITUENCIES = {'kingswood', 'sedgefield', 'copeland', 'richmond park', 'brighton pavilion', 'tottenham', 'reading west'}
_AU_ELECTORATES = {'warringah'}
# Governors named as the employer ("advisor" @ "Governor Newsom"), by surname.
_GOVERNORS = {'newsom': 'California', 'brown': 'California', 'schwarzenegger': 'California', 'inslee': 'Washington',
              'polis': 'Colorado', 'whitmer': 'Michigan', 'hochul': 'New York', 'cuomo': 'New York',
              'murphy': 'New Jersey', 'pritzker': 'Illinois', 'healey': 'Massachusetts', 'moore': 'Maryland'}
# Prime ministers named in "chief of staff to Prime Minister X", by surname.
_PRIME_MINISTERS = {'trudeau': 'Canada', 'mottley': 'Barbados', 'ardern': 'New Zealand', 'albanese': 'Australia',
                    'starmer': 'the United Kingdom', 'sunak': 'the United Kingdom', 'modi': 'India'}
_US_RE = re.compile(r'\bu\.?s\.?\b|\bus\b|united states|\bamerica(n)?\b|\bUS’', re.I)


def government_of(place: str) -> str:
    return _SUBNATIONAL.get(place.lower()) or f"Government of {_COUNTRIES.get(place.lower(), place)}"


def country_from(*texts) -> str | None:
    """The first country or sub-national government named: 'the United
    Kingdom', 'Canada' … or a _SUBNATIONAL body name."""
    for text in texts:
        if not text:
            continue
        m = _COUNTRY_RE.search(text)
        if m:
            k = m.group(1).lower()
            return _SUBNATIONAL.get(k) or _COUNTRIES[k]
    return None


def _body(place: str) -> str:
    """'the United Kingdom' -> 'Government of the United Kingdom'; a
    sub-national body name passes through."""
    return place if place in _SUBNATIONAL.values() else f'Government of {place}'


_TYPO = [(re.compile(r'\bforiegn\b|\bforein\b', re.I), 'Foreign')]


def _strip_place_words(title: str) -> str:
    """'Chilean Minister of Energy' -> 'Minister of Energy'; 'UK’s new Climate
    Envoy' -> 'Climate Envoy'. The place is in the organisation now."""
    t = title
    for rx, fix in _TYPO:
        t = rx.sub(fix, t)
    t = re.sub(r"^(?:the\s+)?(?:" + _COUNTRY_RE.pattern + r"|u\.?s\.?|us|united states|america['’]?s?)\s*(?:['’]s?\s+)?", '', t, flags=re.I)
    for _ in range(2):   # "first US Special Envoy": both prefixes
        t = re.sub(r"^(?:new|current|recently promoted|then|first(?!\s+minister)|senior)\s+", '', t, flags=re.I)
        t = re.sub(r"^(?:the\s+)?(?:u\.?s\.?|us|united states)(?:['’]s?)?\s+", '', t, flags=re.I)
    t = re.sub(r"\s+of (?:the )?(?:republic of )?" + _COUNTRY_RE.pattern + r"$", '', t, flags=re.I)
    t = re.sub(r'\b(?:conservative|labour|liberal|green|tory)\s+(?=mp\b|member of parliament)', '', t, flags=re.I)
    return t.strip(' ,') or title


def normalize_government_role(title: str | None, company: str | None, context: str | None = None,
                              person: str | None = None, known_places: tuple = ()) -> list | None:
    """Pass 2 (see above). A list of {'title', 'company', 'is_former'}, or None."""
    if not title:
        return None
    t0 = title.strip()
    former = bool(_FORMER_RE.match(t0))
    t = _FORMER_RE.sub('', t0)
    t = re.sub(r'^fmr\.?\s+', '', t, flags=re.I)
    former = former or bool(re.match(r'^fmr\.?\s', t0, re.I))
    low = t.lower()
    near = _near(context, person) if person else ''
    here = f"{t} {company or ''} {' '.join(known_places)}"

    def out(ti, co):
        return [{'title': ti, 'company': co, 'is_former': former}]

    # --- staff of a politician (before the politicians themselves) ---
    if re.search(r'\b(advis[oe]r|assistant|chief of staff|aide|legislative director|counsel(l)?or)\b', low):
        co = (company or '').lower()
        foreign = country_from(t, company)
        if not (foreign and foreign != 'the United States') and (
                re.search(r'\bwhite house\b|\bpresident (obama|biden|trump|clinton|bush)\b|to the president\b|for president\b', f'{low} {co}')
                or re.search(r'\bnational (climate|security|economic) advis[oe]r\b|national economic council', low)
                or re.search(r'^(the )?(president|(obama|biden|trump|biden-harris|clinton|bush) (administration|white house))\b', co)):
            clean = re.sub(r'^(?:u\.?s\.?\s+|us\s+)?(?:the\s+)?(?:(?:biden|obama|trump)\s+)?(?:white house\s+)?', '', t, flags=re.I)
            clean = re.sub(r'^(?:first|top|influential|leading)\s+', '', clean, flags=re.I)
            return out(clean.strip() or t, 'White House')
        m = re.match(r'^(?:the )?governor\s+(?:\w+\s+)?(\w+)$', co)
        if m and m.group(1) in _GOVERNORS:
            return out(t, f'State of {_GOVERNORS[m.group(1)]}')
        if re.match(r'^(?:the )?(senator|sen\.)\s', co):
            return out(t, 'U.S. Senate')
        if re.match(r'^(?:the )?(representative|rep\.|congress(man|woman))\s', co):
            return out(t, 'U.S. House')
        if re.search(r'\bgovernor\b', low):
            s = state_from(t, company, *known_places, near)
            return out(t, f'State of {s}') if s else None
        if re.search(r'\bsenate aide\b|\bto (senator|sen\.)', low):
            return out(t, 'U.S. Senate')
        if re.search(r'\bocasio-cortez\b|\bto (representative|rep\.|congress(man|woman))', low):
            return out(t, 'U.S. House')
        m = re.search(r'\bprime minister\b', f'{low} {(company or "").lower()}')
        if m:
            c = country_from(t, company, *known_places, near) or next(
                (v for k, v in _PRIME_MINISTERS.items() if k in f'{t} {company or ""}'.lower()), None)
            if c:
                keep = t if re.search(r'prime minister', low) else f'{t} to the Prime Minister'
                return out(_strip_place_words(keep), _body(c))
        m = re.search(r'state assembly', company or '', re.I)
        if m:
            s = state_from(company)
            return out(t, f'State of {s}') if s else None
        m = re.search(r'(u\.?s\.?|us) department of (energy|state|the interior|defense)', low)
        if m:
            return out(re.sub(r'^(u\.?s\.?|us) department of \w+\s+', '', t, flags=re.I), _CABINET[m.group(2)])
        return None

    # --- members of parliament ---
    if re.search(r'\bmp\b|member of parliament|\bshadow minister\b', low):
        c = country_from(t, company, *known_places)
        co = (company or '').strip().lower()
        if co in _AU_ELECTORATES:
            c = 'Australia'
        elif not c and (co in _UK_CONSTITUENCIES
                        or re.search(r'\b(conservative|labour|tory|westminster|commons|shadow)\b', f'{t} {company or ""} {near}', re.I)):
            c = 'the United Kingdom'
        c = c or country_from(near)
        if not c or c not in _PARLIAMENT:
            return None
        roles = []
        if re.search(r'\bshadow minister\b', low):
            roles.append({'title': 'Shadow Minister', 'company': _PARLIAMENT[c], 'is_former': former})
        roles.append({'title': 'Member of Parliament', 'company': _PARLIAMENT[c], 'is_former': former})
        rest = re.sub(r'\b(senior\s+)?(conservative\s+|labour\s+)?(mp|member of parliament)\b|shadow minister', '', t, flags=re.I).strip(' ,')
        if rest:
            # "MP, Parliamentary Under Secretary of State" keeps its other half.
            other = company if company and co not in _UK_CONSTITUENCIES | _AU_ELECTORATES else _body(c)
            roles.append({'title': rest, 'company': other, 'is_former': former})
        return roles

    # --- envoys and ambassadors ---
    if re.search(r'\b(envoy|ambassador|high commissioner)\b', low):
        if re.search(r'\b(goodwill|brand|c40|business ambassador|policy ambassador|youth envoy|at large)\b', low) \
                and not re.search(r'ambassador[- ]at[- ]large', low):
            return None
        if re.search(r'^(un|u\.n\.|united nations)\b|\bun (special|high)\b', low):
            return out(re.sub(r'^(un|u\.n\.|united nations)\s+', '', t, flags=re.I), 'United Nations')
        if re.search(r'^eu\b|european union', low):
            return out(re.sub(r'^eu\s+', '', t, flags=re.I), 'European Union')
        if _US_RE.search(t) or re.search(r'special presidential envoy', low) or 'department of state' in (company or '').lower() \
                or any(re.fullmatch(r'(the )?(u\.?s\.?|us|united states)', p.strip(), re.I) for p in known_places):
            return out(_strip_place_words(t), 'U.S. Department of State')
        c = country_from(t, company, *known_places)
        if c:
            return out(_strip_place_words(t), _body(c))
        return None

    # --- heads of government and state, ministers ---
    head = re.search(r'\b(deputy prime minister|prime minister|first minister|premier|chancellor|president|vice president|VP)\b', t, re.I)
    minister = re.search(r'\bminister\b|secretary of state for\b|minister of state\b', low)
    if head or minister:
        if head and head.group(1).lower() in ('president', 'vice president', 'vp'):
            # Only a country's president: title little more than the word, and a country on record.
            if not re.fullmatch(r"(?:u\.?s\.?\s+|us\s+|[a-z ]*?\s)?(president|vice president|vp)", low):
                return None
            if _US_RE.search(t) or re.search(r'\b(biden|obama|trump|gore|harris|clinton|bush|pence|cheney)\b', (person or '').lower()):
                return out('President' if head.group(1).lower() == 'president' else 'VP', 'White House')
            c = country_from(t, company, *known_places)
            return out('President', _body(c)) if c and head.group(1).lower() == 'president' else None
        c = country_from(t, company, *known_places, near)
        if not c:
            return None
        return out(_strip_place_words(t), _body(c))
    return None


def normalize_any_government_role(title, company, context=None, person=None, known_places=()) -> list | None:
    """Pass 1 (U.S. legislators, state and city roles, cabinet), then pass 2
    (other countries, diplomats, staff). What the extractor calls."""
    return (normalize_political_roles(title, company, context, person, known_places)
            or normalize_government_role(title, company, context, person, known_places))
