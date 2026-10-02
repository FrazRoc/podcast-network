"""
Topic names: the fixed categories, and the key that decides whether two
spellings of a topic are the same tag.

Tags are open-ended phrases written by the model ("small modular reactors",
"SMRs", "Small Modular Reactor (SMR)"). normalize_topic() turns each into
one key, so those three land on a single tag; tag_aliases stores every
spelling under it (the organisations pattern, see org_names.py).
"""

import re
import unicodedata

# Fixed, for browsing and filters. The model files each tag under one.
CATEGORIES = (
    'Power generation',
    'Grid and storage',
    'Transport',
    'Buildings',
    'Industry and materials',
    'Fuels',
    'Carbon and land',
    'Policy and politics',
    'Finance and markets',
    'Tech and AI',
    'Climate science and impacts',
    'Society and justice',
)

# Acronyms written out, so "SMRs" and "small modular reactors" share a key.
# Only ones that are unambiguous in climate and energy talk.
_ACRONYMS = {
    'smr': 'small modular reactor',
    'vpp': 'virtual power plant',
    'ccs': 'carbon capture and storage',
    'ccus': 'carbon capture utilization and storage',
    'dac': 'direct air capture',
    'cdr': 'carbon dioxide removal',
    'ev': 'electric vehicle',
    'ira': 'inflation reduction act',
    'lng': 'liquefied natural gas',
    'saf': 'sustainable aviation fuel',
    'bess': 'battery energy storage system',
    'der': 'distributed energy resource',
    'hvdc': 'high voltage direct current',
    'ai': 'artificial intelligence',
    'esg': 'environmental social and governance',
    'cop': 'cop',
    'ppa': 'power purchase agreement',
    'egs': 'enhanced geothermal system',
}

# Words that end in s but are not plurals.
_KEEP_S = {'gas', 'bus', 'biomass', 'analysis', 'crisis', 'emissions trading', 'us', 'as',
           'is', 'news', 'lens', 'status', 'series', 'process', 'access', 'loss', 'business',
           'politics', 'economics', 'physics', 'logistics', 'ethics', 'species',
           'texas', 'paris', 'mass', 'grass', 'glass', 'class', 'progress', 'congress',
           'thesis', 'basis', 'nexus', 'census', 'consensus', 'campus', 'virus', 'focus',
           'chaos', 'canvas', 'atlas', 'fracas', 'kansas', 'arkansas', 'brussels', 'carbon dioxide'}


def _singular(word: str) -> str:
    if word in _KEEP_S or len(word) <= 3:
        return word
    if word.endswith('ies') and len(word) > 4:
        return word[:-3] + 'y'
    if word.endswith(('sses', 'shes', 'ches', 'xes', 'zes')):
        return word[:-2]
    if word.endswith('s') and not word.endswith(('ss', 'us', 'is', 'as', 'os')):
        return word[:-1]
    return word


def normalize_topic(name: str) -> str:
    """The matching key for a topic phrase ('' when nothing is left).

    Lowercases, folds accents and typographic quotes, writes '&' as 'and',
    drops a bracketed acronym after its expansion ("Small Modular Reactor
    (SMR)"), drops a leading article, spells out known acronyms and makes
    each word singular.
    """
    if not name:
        return ''
    text = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
    text = text.lower().replace('&', ' and ')
    text = re.sub(r'\([^)]*\)', ' ', text)
    text = re.sub(r"['’]s\b", '', text)
    text = re.sub(r'\bu\.\s?s\.(?:\s?a\.)?', 'us ', text)
    text = re.sub(r'[^a-z0-9]+', ' ', text).strip()
    text = re.sub(r'^(?:the|a|an)\s+', '', text)
    words = []
    for w in text.split():
        base = w[:-1] if w.endswith('s') and w[:-1] in _ACRONYMS else w
        words.extend((_ACRONYMS.get(base) or w).split())
    return ' '.join(_singular(w) for w in words)


def topic_slug(name: str) -> str:
    return normalize_topic(name).replace(' ', '-')
