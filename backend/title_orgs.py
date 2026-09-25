"""
An organisation written inside a job title, pulled out into the company.

The extractor is told to split "founder of Uplift" into title and company,
but shows often put the organisation in front of or inside the title —
"Grist reporter", "BBC Science Correspondent", "Aurora's Head of Consulting",
"head of Goldman's Sustainable Finance Group" — and those rows came back
with the company empty (Sep 2026: ~130 such rows, reviewed by hand). This
finds the organisation in a company-less title by looking its spelling up
among the known organisations.

Tuned against that review: a name that is also an ordinary word ("Science
fiction writer", "Earth scientist", "Australian energy analyst") or a title
word that happens to be an organisation's acronym ("EVP of Digital Energy")
must not become a company.
"""

import re

from org_names import normalize_org_name

_POSSESSIVE = r"(?:'s|’s|'|’)"
# Words that are titles themselves, even where an organisation shares the
# acronym (EVP = the Environmental Voter Project).
_TITLE_WORDS = {'ceo', 'cto', 'coo', 'cfo', 'cmo', 'cio', 'cso', 'cco', 'md', 'vp', 'svp', 'evp', 'avp', 'pm', 'mp',
                'phd', 'head', 'chief', 'lead', 'senior', 'director', 'founder', 'partner', 'former'}
# Ordinary words that are also the names of organisations in the directory.
_COMMON = {'science', 'earth', 'australian', 'future', 'nature', 'project', 'reservoir', 'hydrogen', 'virgin',
           'guardian', 'london', 'oxford', 'deep', 'climate', 'energy', 'solar', 'power', 'global', 'national',
           'international', 'american', 'british', 'european', 'research', 'policy', 'green', 'clean', 'carbon',
           'the', 'forbes', 'award', 'world', 'new', 'united', 'state', 'city', 'county', 'federal', 'digital',
           'nuclear', 'wind', 'water', 'ocean', 'planet', 'impact', 'capital', 'ventures', 'partners', 'labs',
           'ecology', 'generations', 'communication', 'deal', 'fiction'}
# The rest of a leading-organisation title must be a role.
_ROLE_NOUN_RE = re.compile(
    r'\b(reporter|correspondent|editor|columnist|journalist|writer|presenter|anchor|producer|professor|lecturer|'
    r'director|head|chief|analyst|specialist|associate|engineer|scientist|economist|researcher|fellow|'
    r'administrator|secretary|undersecretary|commissioner|president|chair|co-chair|chairman|chairwoman|ceo|cto|'
    r'coo|cfo|founder|co-founder|manager|lead|leader|officer|partner|adviser|advisor|employee|astronaut|'
    r'explorer|ambassador|campaigner|diplomat|spokesperson|meteorologist|climatologist|consultant|executive|'
    r'strategist|trainer|member|vp|svp|evp)s?\b', re.I)


# Journalism titles go with a news organisation, not a company that's
# reported on ("Tesla reporter").
_PRESS_RE = re.compile(r'\b(reporter|correspondent|editor|columnist|journalist|writer|presenter|anchor)s?\b', re.I)
# Honours, praise and books rather than a post ("New York Times bestselling
# author", "UN Goodwill Ambassador", "Thich Nhat Hanh's book").
_NOT_A_POST_RE = re.compile(r'^(and|or|bestselling|best-selling|award-winning)\b|\baward\b|goodwill ambassador|'
                            r'messenger of peace|\ball-star\b', re.I)
_AFTER_POSSESSIVE_NOT_ORG = {'book', 'books', 'novel', 'film', 'documentary', 'essay', 'article', 'podcast', 'show'}
# Conferences are events, not employers ("COP26 President").
_EVENT_RE = re.compile(r'^cop\s?\d*$|^cop$', re.I)


def _is_acronym(text: str) -> bool:
    letters = re.sub(r'[^A-Za-z0-9]', '', text)
    return len(letters) >= 2 and (text.isupper() or bool(re.fullmatch(r'[A-Z0-9][A-Za-z0-9&.\-]*[A-Z0-9]', text)
                                                        and sum(c.isupper() for c in text) >= 2))


def _plausible(text: str, org: dict | None, possessive: bool, own: bool) -> bool:
    """Whether a piece of a title may stand for this organisation."""
    key = normalize_org_name(text) or ''
    words = key.split()
    if not words or key in _TITLE_WORDS or words[0] in _TITLE_WORDS or _EVENT_RE.match(key):
        return False
    if own:                     # one of the person's own organisations
        return True
    if not org:
        return False
    if len(words) >= 2:         # "Bloomberg Opinion", "National Geographic"
        return not all(w in _COMMON for w in words)
    if _is_acronym(text):       # BBC, BNEF, MIT, EPA, DOE, C40, ARPA-E
        return True
    # A single ordinary word: only with a possessive or a typed, non-common name.
    return words[0] not in _COMMON and (possessive or bool(org.get('org_type')))


def split_org_from_title(title: str | None, lookup, own_orgs: list | None = None) -> tuple | None:
    """(company, new_title) for a company-less title that names an
    organisation, or None.

    lookup(normalized_key) -> {'name', 'org_type'} or None, over the known
    organisation spellings. own_orgs: names of organisations this person is
    already recorded at, so "Aurora's Head of Consulting" on Aurora's own
    show finds Aurora Energy Research even though "Aurora" alone is
    ambiguous. company is the spelling as written in the title (the row's
    company_as_written), except for an own-organisation match, which uses
    that organisation's name.
    """
    if not title or not title.strip():
        return None
    t = title.strip()
    own_keys = {}
    for name in own_orgs or []:
        k = normalize_org_name(name)
        if k:
            own_keys[k] = name

    def own_match(text):
        words = (normalize_org_name(text) or '').split()
        hits = [n for k, n in own_keys.items() if words and k.split()[:len(words)] == words]
        return hits[0] if len(hits) == 1 else None

    # 1. Leading organisation: "Grist reporter", "BBC's Science Correspondent".
    words = t.split()
    for n in range(min(5, len(words) - 1), 0, -1):
        head = ' '.join(words[:n])
        possessive = bool(re.search(_POSSESSIVE + '$', head))
        bare = re.sub(_POSSESSIVE + '$', '', head).strip(' ,')
        if not bare or not bare[0].isupper():
            continue
        rest = ' '.join(words[n:]).strip(' ,')
        if not _ROLE_NOUN_RE.search(rest) or _NOT_A_POST_RE.search(rest):
            continue
        org = lookup(normalize_org_name(bare))
        # The person's own organisation only when the title says so ("Aurora's").
        own = own_match(bare) if possessive and not org else None
        if org and _PRESS_RE.search(rest) and org.get('org_type') not in (None, 'media') and not _is_acronym(bare):
            continue
        if (org or own) and _plausible(bare, org, possessive, bool(own)):
            return (own or bare), rest
    # 2. A possessive inside the title: "head of Goldman's Sustainable Finance Group".
    if _NOT_A_POST_RE.search(t):
        return None
    for m in re.finditer(r"\b((?:[A-Z0-9][\w&.\-]*\s?){1,4}?)" + _POSSESSIVE + r"\s+(\S+)", t):
        bare = m.group(1).strip()
        if m.start() == 0 or m.group(2).lower() in _AFTER_POSSESSIVE_NOT_ORG:
            continue            # a leading one was handled above (and rejected there)
        org = lookup(normalize_org_name(bare))
        own = None if org else own_match(bare)
        if (org or own) and _plausible(bare, org, True, bool(own)):
            return (own or bare), t
    # 3. "founder of Nature Is Nonpartisan", "head of TED": only a name of two
    # or more words, or an acronym — a single word after "of" is too often
    # a subject ("Professor of Science", "Lead Expert for Hydrogen").
    # The last one named wins: "Special Representative of the UN
    # Secretary-General for Sustainable Energy for All" works at SEforALL.
    found = None
    for m in re.finditer(r"\b(?:of|at|for|with|from)\s+(?:the\s+)?", t):
        tail = t[m.end():].split()
        for k in range(min(6, len(tail)), 0, -1):
            bare = ' '.join(tail[:k]).strip(' ,;:()')
            if not bare or not bare[0].isupper() or re.search(_POSSESSIVE + '$', bare):
                continue
            org = lookup(normalize_org_name(bare))
            key_words = (normalize_org_name(bare) or '').split()
            if (org and (len(key_words) >= 2 or _is_acronym(bare)) and _plausible(bare, org, False, False)
                    and 'board of directors' not in bare.lower()):
                whole = k == len(tail)
                found = (bare, (t[:m.start()].strip(' ,') if whole else t) or None)
                break
    return found
