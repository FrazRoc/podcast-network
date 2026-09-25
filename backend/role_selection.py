"""
The displayed "current role" for a person, derived from host_affiliations.

host_affiliations is raw data: one set of rows per appearance, worded
however each show worded it (see scraper/extract_affiliations.py). Shows
often name only part of a role — the newest appearance says "Canary Media"
with no title, or "Co-Head of Advisory" with no company because the show is
the company's own podcast — so the newest appearance alone is a poor
answer. The rule (Sep 2026, tuned on ~40 people reviewed by hand):

  1. A manual pin (host_role_pins) wins outright.
  2. Only current roles count: former roles are history, and an appearance
     flagged from_other_episode ("past episodes you'll love", reruns) is
     dated wrong — used only when nothing else names a position and a company.
  3. A title with no company borrows the company from the person's latest
     row on the same show that names one (Aurora's analysts on Aurora's
     podcast, BNEF's on Switched On).
  4. The person's current organisation is the one on their newest row that
     names any — together with its parent and sub-organisations (Bloomberg
     L.P. / Bloomberg News). The shown role is the newest real position at
     that organisation, passing over bare words like "researcher" or
     "author" when a fuller title exists there.
  5. That organisation has no titled row: its name, with the newest title
     given without a company since the person appeared there (Alba Forns,
     "COO and Co-Founder", then Climatize).
  6. Nothing names an organisation, or the newest appearance is more than
     three years past the last one that did (a career change the shows
     haven't named): the newest appearance, where a position at a named
     organisation beats a position alone, which beats a description
     ("clean energy investor"), which beats a bare organisation.
"""

import re
from datetime import date, timedelta

# Titles too generic to prefer over a fuller one at the same organisation.
_WEAK_TITLES = {'researcher', 'expert', 'author', 'guest', 'speaker', 'member', 'commentator',
                'panelist', 'panellist', 'contributor', 'lead', 'co-lead'}
# Not a title: a relative clause the extractor kept ("who lead BNEF's EV team").
_NOT_A_TITLE_RE = re.compile(r'^(who|which|that|whose)\b', re.I)
_CAREER_CHANGE = timedelta(days=3 * 365)


def _rank(row: dict) -> int:
    kind, company = row.get('title_kind'), row.get('company')
    if kind == 'position' and company:
        return 0
    if kind == 'position':
        return 1
    if kind == 'description':
        return 2
    return 3


def _when(row: dict):
    return (row.get('published_date') or date.min, row.get('episode_id') or 0)


def _family(row: dict):
    """Organisations counted as one employer: the top of the parent chain,
    or the company text when it isn't linked to an organisation."""
    if row.get('top_org_id') or row.get('org_id'):
        return row.get('top_org_id') or row.get('org_id')
    return (row.get('company') or '').strip().lower() or None


def _is_position(row: dict) -> bool:
    return row.get('title_kind') == 'position' and bool(row.get('title'))


def _is_weak(row: dict) -> bool:
    return (row.get('title') or '').strip().lower() in _WEAK_TITLES


def _clean(rows: list) -> list:
    out = []
    for r in rows:
        if r.get('title') and _NOT_A_TITLE_RE.match(r['title'].strip()):
            r = {**r, 'title': None, 'title_kind': None}
        if r.get('title') or r.get('company'):
            out.append(r)
    return out


def _borrow_show_company(rows: list) -> list:
    """Rule 3: a company-less title takes the company of the person's latest
    row on the same show that names one."""
    by_show = {}
    for r in sorted(rows, key=_when):
        if r.get('company') and r.get('podcast_id') is not None:
            by_show[r['podcast_id']] = r
    out = []
    for r in rows:
        src = by_show.get(r.get('podcast_id')) if r.get('podcast_id') is not None else None
        if r.get('title') and not r.get('company') and src:
            r = {**r, 'company': src['company'], 'org_id': src.get('org_id'),
                 'top_org_id': src.get('top_org_id'), 'company_inferred': True}
        out.append(r)
    return out


def _newest_appearance(rows: list) -> dict:
    newest = max(rows, key=_when)
    same = [r for r in rows if r.get('episode_id') == newest.get('episode_id')]
    return min(same, key=_rank)


def _choose(rows: list) -> dict | None:
    rows = _borrow_show_company(_clean(rows))
    if not rows:
        return None
    with_org = [r for r in rows if r.get('company')]
    if not with_org:
        return _newest_appearance(rows)
    anchor = max(with_org, key=lambda r: (_when(r), -_rank(r)))
    newest = max(rows, key=_when)
    if (newest.get('published_date') and anchor.get('published_date')
            and newest['published_date'] - anchor['published_date'] > _CAREER_CHANGE):
        return _newest_appearance(rows)

    family = _family(anchor)
    titled = [r for r in with_org if _family(r) == family and _is_position(r)]
    if titled:
        strong = [r for r in titled if not _is_weak(r)] or titled
        return max(strong, key=lambda r: (_when(r), not r.get('company_inferred')))
    # Rule 5: the organisation, with a title given since without a company.
    since = [r for r in rows if not r.get('company') and _is_position(r) and _when(r) >= _when(anchor)]
    if since:
        t = max(since, key=_when)
        return {**t, 'company': anchor['company'], 'org_id': anchor.get('org_id'),
                'top_org_id': anchor.get('top_org_id'), 'company_inferred': True}
    return anchor


def pick_current_role(rows: list, pin: dict | None = None) -> dict | None:
    """rows: host_affiliations joined with their extraction and episode —
    dicts with title, company, title_kind, is_former, from_other_episode,
    published_date, episode_id, and where known podcast_id, org_id and
    top_org_id (plus anything else, passed through).
    pin: {'title', 'company'} from host_role_pins, or None.

    Returns the chosen row with 'source' set to 'pinned' or 'derived', or
    None when there is nothing to show. A company filled in from another row
    is marked company_inferred.
    """
    if pin and (pin.get('title') or pin.get('company')):
        return {'title': pin.get('title'), 'company': pin.get('company'), 'source': 'pinned'}

    live = [r for r in rows if not r.get('is_former')]
    best = _choose([r for r in live if not r.get('from_other_episode')])
    if not best or not (_is_position(best) and best.get('company')):
        # Rule 2's fallback: a reference to another episode, if it is fuller.
        other = _choose([r for r in live if r.get('from_other_episode')])
        if other and _is_position(other) and other.get('company'):
            best = other
    return {**best, 'source': 'derived'} if best else None


# ------------------------------------------------------------------
# Display formatting
# ------------------------------------------------------------------
#
# host_affiliations keeps each title exactly as the episode text wrote it —
# that is what makes every value checkable against its snippet. The tidying
# happens here, only for the one role that is shown: "a senior investigative
# data reporter" reads as a phrase, not a role. Pins are shown as typed.

_LEADING_ARTICLE_RE = re.compile(r'^(?:a|an|the)\s+', re.IGNORECASE)
_SMALL_WORDS = {'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'from', 'in', 'into',
                'nor', 'of', 'on', 'or', 'the', 'to', 'via', 'vs', 'with'}
_WORD_RE = re.compile(r"[A-Za-z\u00C0-\u024F][\w'’.]*")


def _cap(word: str) -> str:
    # A word that already has a capital is someone's deliberate spelling
    # (CEO, DOE's, McKinsey, iPhone) and is left alone.
    if any(c.isupper() for c in word):
        return word
    return word[0].upper() + word[1:]


def _title_case(text: str) -> str:
    """Title case for job titles: every word capitalised except small
    connecting words (after the first); each part of a hyphenated word
    capitalised ("co-founder" -> "Co-Founder")."""
    first = True

    def repl(m):
        nonlocal first
        word = m.group(0)
        if not first and word.lower() in _SMALL_WORDS:
            return word
        first = False
        return _cap(word)

    return _WORD_RE.sub(repl, text)


# "cofounder", "co founder", "co–founder" -> "co-founder". Only before a
# role word, so ordinary words that start with "co" (consultant, coordinator,
# correspondent, COO) are never touched.
_CO_ROLE_RE = re.compile(
    r'\b(co)(?:\s*[–—]\s*|\s+-\s*|\s*-\s+|\s+|)(?!-)'
    r'(found(?:er|ers|ed|ing)?|host(?:s|ed|ing)?|author(?:s|ed)?|creat(?:or|ors|ed)|'
    r'chair(?:s|man|woman|person)?|direct(?:or|ors|s)|head|lead(?:s|er|ers)?|owner(?:s)?|ceo|'
    r'president|manag(?:er|ing|ed)|editor(?:s)?|inventor(?:s)?|designer(?:s)?|pilot|portfolio|'
    r'executive|principal|organi[sz]er(?:s)?|producer(?:s)?|investigator(?:s)?|convener|captain)\b',
    re.IGNORECASE)


def hyphenate_co(title: str | None) -> str | None:
    """Write every co- role with a hyphen ("Cofounder and CEO" ->
    "Co-founder and CEO"), keeping the case of each part as written."""
    if not title:
        return title
    return _CO_ROLE_RE.sub(lambda m: f"{m.group(1)}-{m.group(2)}", title)


# "Chief Executive Officer" -> "CEO", and the other chief titles with one
# standard abbreviation. Ambiguous ones stay spelled out: Sustainability /
# Strategy / Science all make CSO, Commercial / Content / Customer all make
# CCO, and CIO usually means Information, not Investment.
_CHIEF_ABBREVIATIONS = {
    'executive': 'CEO', 'operating': 'COO', 'financial': 'CFO', 'technology': 'CTO',
    'technical': 'CTO', 'marketing': 'CMO', 'revenue': 'CRO', 'product': 'CPO', 'legal': 'CLO',
}
_CHIEF_RE = re.compile(
    r'\bchief\s+(' + '|'.join(_CHIEF_ABBREVIATIONS) + r')'
    # "Officer", also cut off or misspelt in the source ("Offi", "Office").
    r'(?:\s+(offi(?:cer(s)?|ce|c)?)\b)?'
    # "Chief Executive Director" / "Chief Executive Committee" are other roles.
    r'(?!\s+(?:director|committee|board|council|team|member))'
    # A repeated abbreviation after it: "Chief Executive Officer (CEO)".
    r'(?:\s*\([A-Z]{3}\))?',
    re.IGNORECASE)


def abbreviate_chiefs(title: str | None) -> str | None:
    """"Co-founder and Chief Executive Officer" -> "Co-founder and CEO".
    Only "Chief Executive" may drop "Officer" (it means the same thing);
    "Chief Technology" alone is left as written."""
    if not title:
        return title

    def repl(m):
        word, officer, plural = m.group(1).lower(), m.group(2), m.group(3)
        if not officer and word != 'executive':
            return m.group(0)
        return _CHIEF_ABBREVIATIONS[word] + ('s' if plural else '')

    return _CHIEF_RE.sub(repl, title)


def _same_case(template: str, word: str) -> str:
    """word in template's case: "sr." -> "senior", "Sr." -> "Senior"."""
    return word if template[:1].isupper() else word.lower()


# Senior/Executive first, so "Senior Vice President" becomes SVP, not
# "Senior VP". "presidents?\b" keeps "Vice Presidential" untouched.
_VP_RES = [
    (re.compile(r'\bsenior\s+vice[\s-]+president(s)?\b', re.I), 'SVP'),
    (re.compile(r'\bexecutive\s+vice[\s-]+president(s)?\b', re.I), 'EVP'),
    (re.compile(r'\bvice[\s-]+president(s)?\b', re.I), 'VP'),
]

# Short forms written out. Each needs its dot or a following space, so a
# word merely starting with the letters ("Direct", "Senate") is untouched.
_SHORT_FORMS = [
    (re.compile(r'\bsr\b\.?(?=\s)', re.I), 'Senior'),
    (re.compile(r'\bprof\b\.?(?=\s|$)', re.I), 'Professor'),
    (re.compile(r'\basst\b\.?(?=\s|$)', re.I), 'Assistant'),
    (re.compile(r'\bassoc\b\.?(?=\s|$)', re.I), 'Associate'),
    (re.compile(r'\bexec\b\.?(?=\s)', re.I), 'Executive'),
    (re.compile(r'\bdir\b\.?(?=\s|$)', re.I), 'Director'),
    (re.compile(r'\bmgr\b\.?(?=\s|$)', re.I), 'Manager'),
    (re.compile(r'\bdept\b\.?(?=\s|$)', re.I), 'Department'),
    (re.compile(r'\bRep\.(?=\s)'), 'Representative'),
    (re.compile(r'\bSen\.(?=\s)'), 'Senator'),
    (re.compile(r'\bGov\.(?=\s)'), 'Governor'),
]
_SENIOR_EXEC_VP_RE = re.compile(r'\b(senior|executive)\s+VP(s)?\b', re.I)
_PHD_RE = re.compile(r'\bph\.\s?d\b\.?', re.I)

# Compound titles written both ways, standardised on the hyphenated form.
_HYPHENATED_RE = re.compile(r'\b(director|secretary)[\s-]+(general)\b|\b(editor)[\s-]+(in)[\s-]+(chief)\b', re.I)

# "Founder & CEO" -> "Founder and CEO"; "R&D", "M&A" (no spaces) untouched.
_SPACED_AMPERSAND_RE = re.compile(r'\s+&\s+')


def standardise_title_words(title: str | None) -> str | None:
    """VP/SVP/EVP for vice presidents, "and" for a spaced "&", short forms
    written out (Sr. -> Senior, Prof. -> Professor, Rep. -> Representative,
    Ph.D. -> PhD), and Director-General / Secretary-General /
    Editor-in-Chief always hyphenated."""
    if not title:
        return title
    text = title
    for rx, abbr in _VP_RES:
        text = rx.sub(lambda m, a=abbr: a + ('s' if m.group(1) else ''), text)
    text = _SPACED_AMPERSAND_RE.sub(' and ', text)
    for rx, word in _SHORT_FORMS:
        text = rx.sub(lambda m, w=word: _same_case(m.group(0), w), text)
    # "Sr VP" became "Senior VP" just above; it and "Executive VP" are SVP / EVP.
    text = _SENIOR_EXEC_VP_RE.sub(lambda m: ('SVP' if m.group(1).lower() == 'senior' else 'EVP') + (m.group(2) or ''), text)
    text = _PHD_RE.sub('PhD', text)
    text = _HYPHENATED_RE.sub(lambda m: '-'.join(g for g in m.groups() if g), text)
    return text


# One person's title taken from a sentence about several ("the reporters X
# and Y", "co-founders A and B"): the last word back in the singular.
_PLURAL_ROLE_RE = re.compile(
    r'\b(reporter|analyst|founder|host|expert|researcher|partner|director|editor|scientist|engineer|'
    r'economist|associate|fellow|member|correspondent|journalist|writer|author|investor|advisor|adviser|'
    r'leader|student|lead)s$', re.I)


def singular_role(title: str | None) -> str | None:
    return _PLURAL_ROLE_RE.sub(lambda m: m.group(1), title) if title else title


def tidy_title(title: str | None) -> str | None:
    """The spelling fixes every stored title gets: co- roles hyphenated,
    common chief titles and vice presidents abbreviated, a plural role made
    singular, and the other standardisations in standardise_title_words()."""
    return singular_role(standardise_title_words(abbreviate_chiefs(hyphenate_co(title))))


def display_title(title: str | None, title_kind: str | None = None) -> str | None:
    """How a stored title is shown: leading "a"/"an"/"the" dropped, then
    title case for a position, a capital first letter for a description
    ("Ecologist, political scientist, and author" — title case reads oddly
    on a phrase like that)."""
    if not title:
        return title
    text = _LEADING_ARTICLE_RE.sub('', title.strip()) or title.strip()
    text = tidy_title(text)
    if title_kind == 'description':
        # Capitalise the first word only; a capital later in the phrase
        # ("… and Greenpeace co-founder") says nothing about the first.
        first, sep, rest = text.partition(' ')
        return _cap(first) + sep + rest
    return _title_case(text)


def format_for_display(role: dict | None) -> dict | None:
    """The chosen role with its title tidied for display; pins untouched."""
    if not role or role.get('source') == 'pinned':
        return role
    return {**role, 'title': display_title(role.get('title'), role.get('title_kind'))}
