"""
Organisation name normalisation, shared by the extraction (which stamps
host_affiliations.company_key), the sync that creates organisations, and
Company Admin.

The key decides which spellings are *automatically* the same organisation,
so it only removes differences that never change which organisation is
meant: case, punctuation, a leading "the", a trailing possessive, and legal
suffixes. Anything cleverer ("BNEF" = "Bloomberg New Energy Finance")
is a merge suggestion for a person to confirm, not a key collision.
"""

import re
import unicodedata

# Legal-form suffixes. Deliberately not "company" or "group": "The Metals
# Company" and "Rhodium Group" are names, not legal forms.
_LEGAL_SUFFIXES = {
    'inc', 'incorporated', 'llc', 'ltd', 'limited', 'corp', 'corporation', 'co',
    'plc', 'gmbh', 'ag', 'sa', 'lp', 'llp', 'pbc', 'bv', 'nv',
}
_QUOTES = str.maketrans({'’': "'", '‘': "'", '“': '"', '”': '"', '–': '-', '—': '-'})


def normalize_org_name(name: str | None) -> str | None:
    """The key two spellings must share to be the same organisation without
    review. None for an empty name."""
    if not name:
        return None
    text = unicodedata.normalize('NFKC', name).translate(_QUOTES).casefold().strip()
    text = re.sub(r"'s$", '', text)                  # BloombergNEF's
    text = re.sub(r'^the\s+', '', text)               # the Searchlight Institute
    text = text.replace('&', ' and ')                 # E&E News / E and E News
    text = re.sub(r"[^\w\s]", ' ', text)              # punctuation -> space
    words = text.split()
    while len(words) > 1 and words[-1] in _LEGAL_SUFFIXES:
        words.pop()
    return ' '.join(words) or None


_STOPWORDS = {'of', 'the', 'and', 'for', 'on', 'in', 'at', 'to'}


def initials(name: str | None) -> str | None:
    """Initials of a multi-word name, ignoring small words:
    "Bloomberg New Energy Finance" -> "bnef", "National Renewable Energy
    Laboratory" -> "nrel". None for a one-word name."""
    key = normalize_org_name(name)
    if not key:
        return None
    words = [w for w in key.split() if w not in _STOPWORDS]
    if len(words) < 2:
        return None
    return ''.join(w[0] for w in words)


def looks_like_acronym_of(short: str | None, long: str | None) -> bool:
    """True if `short` is written as the initials of `long` ("BNEF" /
    "Bloomberg New Energy Finance", "NREL" / "National Renewable Energy
    Laboratory"). Run-together forms like "BloombergNEF" are not caught
    here; trigram similarity handles those."""
    short_key = normalize_org_name(short)
    # Two letters match far too much ("US" / "University of Singapore").
    if not short_key or ' ' in short_key or len(short_key) < 3 or not short_key.isalpha():
        return False
    return initials(long) == short_key


# ------------------------------------------------------------------
# Strings that are not organisations
# ------------------------------------------------------------------
#
# A state, country, region or their abbreviation is where someone works, not
# who they work for ("California", "UK", "AZ", "North America"). Evan's call,
# Sep 2026: never a company. A city is left alone — "Freetown" as the
# company of Freetown's mayor stands for the city government.

_PLACES = set("""
alabama alaska arizona arkansas california colorado connecticut delaware florida georgia hawaii idaho illinois indiana iowa
kansas kentucky louisiana maine maryland massachusetts michigan minnesota mississippi missouri montana nebraska nevada ohio
oklahoma oregon pennsylvania tennessee texas utah vermont virginia washington wisconsin wyoming ontario quebec alberta manitoba
saskatchewan queensland victoria tasmania scotland wales england bavaria catalonia punjab sindh kerala maharashtra gujarat
afghanistan albania algeria andorra angola argentina armenia australia austria azerbaijan bahamas bahrain bangladesh barbados
belarus belgium belize benin bhutan bolivia bosnia botswana brazil brunei bulgaria burundi cambodia cameroon canada chad chile
china colombia comoros congo croatia cuba cyprus czechia denmark djibouti dominica ecuador egypt eritrea estonia eswatini
ethiopia fiji finland france gabon gambia germany ghana greece grenada guatemala guinea guyana haiti honduras hungary iceland
india indonesia iran iraq ireland israel italy jamaica japan jordan kazakhstan kenya kiribati korea kosovo kuwait kyrgyzstan
laos latvia lebanon lesotho liberia libya liechtenstein lithuania luxembourg madagascar malawi malaysia maldives mali malta
mauritania mauritius mexico micronesia moldova monaco mongolia montenegro morocco mozambique myanmar namibia nauru nepal
netherlands nicaragua niger nigeria norway oman pakistan palau palestine panama paraguay peru philippines poland portugal qatar
romania russia rwanda samoa senegal serbia seychelles singapore slovakia slovenia somalia spain sudan suriname sweden
switzerland syria taiwan tajikistan tanzania thailand togo tonga tunisia turkey turkmenistan tuvalu uganda ukraine uruguay
uzbekistan vanuatu venezuela vietnam yemen zambia zimbabwe greenland scandinavia europe africa asia antarctica america americas
""".split()) | {
    'new hampshire', 'new jersey', 'new mexico', 'new york', 'north carolina', 'north dakota', 'rhode island',
    'south carolina', 'south dakota', 'west virginia', 'puerto rico', 'district of columbia', 'british columbia',
    'nova scotia', 'new brunswick', 'new south wales', 'northern ireland', 'tamil nadu', 'costa rica', 'el salvador',
    'south africa', 'south korea', 'north korea', 'new zealand', 'sri lanka', 'saudi arabia', 'united kingdom',
    'united states', 'united states of america', 'united arab emirates', 'great britain', 'czech republic',
    'dominican republic', 'ivory coast', 'sierra leone', 'papua new guinea', 'east timor', 'marshall islands',
    'solomon islands', 'trinidad and tobago', 'latin america', 'south america', 'north america', 'central america',
    'sub saharan africa', 'middle east', 'southeast asia', 'east africa', 'west africa',
    # abbreviations (normalised: punctuation removed)
    'us', 'u s', 'usa', 'u s a', 'uk', 'u k', 'eu', 'e u', 'uae', 'ca', 'ny', 'tx', 'fl', 'il', 'wa', 'ma', 'nj',
    'pa', 'oh', 'mi', 'ga', 'nc', 'va', 'co', 'az', 'or', 'mn', 'wi', 'md', 'mo', 'tn', 'ky', 'sc', 'al', 'ok',
    'ct', 'ut', 'nv', 'ia', 'ar', 'ms', 'ks', 'ne', 'nm', 'id', 'hi', 'me', 'nh', 'ri', 'mt', 'sd', 'nd', 'ak',
    'vt', 'wy', 'dc', 'd c', 'bc', 'qld', 'nsw',
}

# Words an organisation name does not end on: a name cut off at the edge of
# the text ("the University of", "the Institute for", "Dun &").
_DANGLING = {'of', 'for', 'the', 'and', 'at', 'with', 'de', 'del', 'di', 'des', 'du'}

# Placeholders and bare generic words that name no particular organisation.
_PLACEHOLDERS = {
    'the', 'climate', 'energy', 'hydrogen', 'solar', 'wind', 'power', 'carbon', 'nuclear', 'lab', 'labs', 'research',
    'center', 'centre', 'institute', 'institution', 'university', 'foundation', 'company', 'firm', 'government',
    'agency', 'department', 'office', 'group', 'council', 'commission', 'program', 'programme', 'network', 'coalition',
    'association', 'startup', 'a startup', 'think tank', 'nonprofit', 'people', 'president', 'podcast', 'show', 'u n',
    'administration',
}


def _plain(name: str) -> str:
    t = unicodedata.normalize('NFKC', name).translate(_QUOTES).casefold().strip()
    t = re.sub(r"'s$", '', t)
    t = re.sub(r'^the\s+', '', t)
    t = t.replace('&', ' & ')
    t = re.sub(r"[^\w\s&]", ' ', t)
    return re.sub(r'\s+', ' ', t).strip()


def is_place(name: str | None) -> bool:
    """A state, country, region or their abbreviation, and nothing else."""
    return bool(name) and _plain(name) in _PLACES


def is_fragment(name: str | None) -> bool:
    """Cut off mid-name, or a placeholder rather than a name."""
    if not name:
        return False
    raw = name.strip()
    if raw.endswith(('...', '…')):
        return True
    t = _plain(raw)
    if not t or t in _PLACEHOLDERS:
        return True
    last = t.split()[-1]
    return last in _DANGLING or last == '&'


def not_an_organisation(name: str | None) -> bool:
    return is_place(name) or is_fragment(name)
