"""
"Possibly the same as ..." hints for the suggestions review queue.

A suggestion is only ever made for a name that isn't already a known person
(exact match, aliases included), but a large share of what reaches the queue
is still someone we have, written differently. In the Sep 30 2026 review of
659 pending names, about 30 approvals needed pointing at an existing record:

- a stored middle initial the show notes drop ("Angela Chalk" / "Angela M.
  Chalk", "William Reilly" / "William K. Reilly")
- a nickname or short form ("Arthur Berman" / "Art Berman", "Jen Dlouhy" /
  "Jennifer Dlouhy")
- a one-letter spelling difference ("Jessika Trancik" / "Jessica Trancik",
  "Susan Monroe" / "Susan Munroe")
- an extra middle name ("Abby Ross Hopper" / "Abby Hopper")

Approving one of those as-is creates a duplicate person. This finds those
candidates so the review page can offer "Approve as <existing name>" instead.
It is a hint for a human, not an automatic merge: "Andy Smith" and "Andrew
Smith" may well be two people, which is exactly why a reviewer decides.

Pure functions only; the endpoint in main.py loads the people and counts.
"""

import re
import unicodedata

_NICKNAME_GROUPS = [
    {'arthur', 'art'}, {'jennifer', 'jen', 'jenny'}, {'joshua', 'josh'},
    {'daniel', 'dan', 'danny'}, {'david', 'dave'}, {'michael', 'mike'},
    {'william', 'bill', 'will', 'billy'}, {'robert', 'rob', 'bob', 'bobby', 'robbie'},
    {'james', 'jim', 'jimmy', 'jamie'}, {'joseph', 'joe'}, {'thomas', 'tom', 'tommy'},
    {'christopher', 'chris'}, {'christine', 'chris'}, {'nicholas', 'nick', 'nico'},
    {'alexander', 'alex'}, {'alexandra', 'alex', 'sasha'}, {'benjamin', 'ben'},
    {'samuel', 'sam'}, {'samantha', 'sam'}, {'matthew', 'matt'}, {'anthony', 'tony'},
    {'katherine', 'kate', 'katie', 'kathy', 'kat'}, {'kathryn', 'kate', 'katie', 'kathy'},
    {'catherine', 'cathy', 'kate', 'cate'}, {'elizabeth', 'liz', 'beth', 'betsy', 'eliza', 'lizzie'},
    {'abigail', 'abby', 'abbie'}, {'andrew', 'andy', 'drew'}, {'steven', 'steve'},
    {'stephen', 'steve'}, {'gregory', 'greg'}, {'jonathan', 'jon', 'jonny'},
    {'nathan', 'nate'}, {'nathaniel', 'nate', 'nat'}, {'peter', 'pete'},
    {'richard', 'rick', 'rich', 'dick', 'richie'}, {'edward', 'ed', 'ted', 'eddie'},
    {'timothy', 'tim'}, {'zachary', 'zach', 'zack'}, {'bradford', 'brad'},
    {'bradley', 'brad'}, {'douglas', 'doug'}, {'kenneth', 'ken'}, {'lawrence', 'larry'},
    {'patrick', 'pat'}, {'patricia', 'pat', 'patty', 'trish'}, {'ronald', 'ron'},
    {'frederick', 'fred'}, {'charles', 'charlie', 'chuck'}, {'rebecca', 'becky', 'becca'},
    {'margaret', 'maggie', 'meg', 'peggy'}, {'megan', 'meg'}, {'susan', 'sue', 'susie'},
    {'deborah', 'deb', 'debbie'}, {'debra', 'deb', 'debbie'}, {'jeffrey', 'jeff'},
    {'gerald', 'gerry', 'jerry'}, {'gerard', 'gerry'}, {'raymond', 'ray'},
    {'victoria', 'vicky', 'tori'}, {'jacqueline', 'jackie'}, {'jessica', 'jess'},
    {'olivia', 'liv'}, {'oliver', 'ollie'}, {'philip', 'phil'}, {'phillip', 'phil'},
    {'leonard', 'leo', 'len'}, {'benedict', 'ben'}, {'theodore', 'ted', 'theo'},
    {'kimberly', 'kim'}, {'kimberley', 'kim'}, {'cynthia', 'cindy'}, {'jacob', 'jake'},
    {'maximilian', 'max'}, {'maxwell', 'max'}, {'emily', 'em'}, {'johannes', 'hans'},
]
_NICKNAMES = {}
for _group in _NICKNAME_GROUPS:
    for _n in _group:
        _NICKNAMES.setdefault(_n, set()).update(_group)

_HONORIFICS = {'dr', 'prof', 'professor', 'mr', 'ms', 'mrs', 'sir', 'dame', 'rev',
               'baroness', 'lord', 'lady', 'hon', 'he', 'rabbi', 'sen', 'rep', 'gov'}
_SUFFIXES = {'jr', 'sr', 'ii', 'iii', 'iv', 'phd', 'md', 'obe', 'mbe', 'cbe'}


def _fold(text: str) -> str:
    text = unicodedata.normalize('NFKD', text or '')
    text = ''.join(c for c in text if not unicodedata.combining(c))
    return re.sub(r'[^a-z\s-]', '', text.lower().replace('’', '').replace("'", ''))


def name_parts(name: str) -> tuple[str, str, tuple]:
    """(first, last, all significant tokens) with honorifics, suffixes and
    middle initials removed — "Dr. Angela M. Chalk" -> ("angela", "chalk", ...)."""
    tokens = [t for t in _fold(name).split() if t]
    tokens = [t for t in tokens if t.strip('-') not in _HONORIFICS or len(tokens) <= 2]
    tokens = [t for t in tokens if t not in _SUFFIXES and len(t.strip('-')) > 1]
    if not tokens:
        return '', '', ()
    return tokens[0], tokens[-1], tuple(tokens)


def edit_distance(a: str, b: str, limit: int = 3) -> int:
    """Levenshtein distance, giving up (returning limit + 1) once it can't be
    within `limit` — most pairs differ in length or start and bail early."""
    if abs(len(a) - len(b)) > limit:
        return limit + 1
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        if min(cur) > limit:
            return limit + 1
        prev = cur
    return prev[-1]


def first_names_compatible(a: str, b: str) -> str | None:
    """Why two first names could be the same person, or None."""
    if not a or not b:
        return None
    if a == b:
        return 'same first name'
    if b in _NICKNAMES.get(a, ()):
        return 'nickname'
    short, long_ = sorted((a, b), key=len)
    if len(short) >= 3 and long_.startswith(short):
        return 'short form'
    if min(len(a), len(b)) >= 4 and a[0] == b[0] and edit_distance(a, b, 1) <= 1:
        return 'spelling'
    return None


def surnames_compatible(a: str, b: str) -> str | None:
    if not a or not b:
        return None
    if a == b:
        return 'same'
    # "Hastings-Simon" / "Hastings Simon" already fold to the same last token
    # or not; compare with hyphens removed as well.
    if a.replace('-', '') == b.replace('-', ''):
        return 'same'
    if a[0] != b[0]:
        return None
    shortest = min(len(a), len(b))
    allowed = 2 if shortest >= 9 else 1 if shortest >= 5 else 0
    if allowed and edit_distance(a, b, allowed) <= allowed:
        return 'spelling'
    return None


def match_reason(candidate: str, existing: str) -> str | None:
    """A short human-readable reason the two names might be one person, or
    None. Never matches two names that are identical once normalised —
    those are the same record already, not a "possible" match."""
    # Identical as written (case and spacing aside) is the same record, not a
    # "possible" match. Accents are NOT folded here: "Luiza Demoro" /
    # "Luiza Demôro" is exactly the kind of difference worth flagging.
    if ' '.join(candidate.lower().split()) == ' '.join(existing.lower().split()):
        return None
    c_first, c_last, c_tokens = name_parts(candidate)
    e_first, e_last, e_tokens = name_parts(existing)
    if not c_first or not e_first:
        return None
    last = surnames_compatible(c_last, e_last)
    if not last:
        return None
    first = first_names_compatible(c_first, e_first)
    if not first:
        return None
    if last == 'same' and first == 'same first name':
        if _fold(candidate).split() == _fold(existing).split():
            return 'accents differ'
        if c_tokens == e_tokens:
            # Only a title or an initial differs: "Baroness Bryony Worthington"
            # / "Bryony Worthington", "Angela Chalk" / "Angela M. Chalk".
            return 'title or middle initial differs'
        # Same first and last, differing only in the middle: "Angela Chalk" /
        # "Angela M. Chalk", "Abby Ross Hopper" / "Abby Hopper".
        return 'middle name or initial differs'
    if last == 'same':
        return f'{first} ({c_first} / {e_first})'
    if first == 'same first name':
        return f'surname spelling ({c_last} / {e_last})'
    # Both halves differ: too loose to show ("Jon Cole" / "Jonathan Cole" is
    # fine, but "Dan Colom" / "Dana Colon" isn't worth a reviewer's time).
    return None


def index_people(people: list) -> dict:
    """Group people by the first letter of their folded surname — the only
    ones find_possible_matches() can match, since surnames_compatible()
    requires a shared first letter. Build once per request."""
    index = {}
    for person in people:
        name = person.get('name') or ''
        _, last, _ = name_parts(name)
        if last:
            index.setdefault(last[0], []).append(person)
    return index


def find_possible_matches(candidate: str, people, limit: int = 3) -> list:
    """people: [{'host_id', 'name', ...}] (aliases may appear as extra rows
    with the same host_id), or an index_people() dict. Returns up to `limit`
    distinct people as [{'host_id', 'name', 'reason'}], closest first."""
    c_first, c_last, _ = name_parts(candidate)
    if not c_first or not c_last:
        return []
    if isinstance(people, dict):
        people = people.get(c_last[0], [])
    order = {'accents differ': 0, 'title or middle initial differs': 0, 'middle name or initial differs': 0}
    found = {}
    for person in people:
        name = person.get('name') or ''
        _, p_last, _ = name_parts(name)
        # Cheap prefilter before the edit-distance work: surnames must share
        # a first letter to be compatible at all.
        if not p_last or p_last[0] != c_last[0]:
            continue
        reason = match_reason(candidate, name)
        if not reason or person['host_id'] in found:
            continue
        found[person['host_id']] = {'host_id': person['host_id'], 'name': name, 'reason': reason}
    ranked = sorted(found.values(), key=lambda m: (order.get(m['reason'], 1), m['name']))
    return ranked[:limit]
