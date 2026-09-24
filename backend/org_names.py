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
