"""
extract_affiliations.py

Reads each guest appearance's job title and organisation out of the episode's
own title and description, and stores one row per appearance in
host_affiliations (see migrate_add_host_affiliations.sql).

The structure of these descriptions is too varied for patterns ("Jane Doe,
VP of Grid at Fervo", "Fervo's Jane Doe", "joined by Jane Doe, who leads grid
work at Fervo..."), so a model reads them. Cost is kept down three ways:

  * Only the text around the person's name is sent — a few hundred
    characters, not the whole description (build_snippet).
  * An identical snippet for the same person is sent once. Shows repeat a
    guest's bio line across episodes; the answer is copied to every
    appearance that shares it.
  * Everything runs through the Message Batches API (Sonnet 5 by default),
    at half the normal per-token price, with ~40 snippets per request so the
    instructions are paid for once per request rather than once per person.

Every extracted value must appear verbatim in the snippet it came from, or
it is dropped (verified_affiliations). That makes invented companies
impossible to store, and makes each row checkable against
affiliation_extractions.snippet.

Usage:
    python3 extract_affiliations.py estimate [--limit N]      # no API, no writes
    python3 extract_affiliations.py pilot --limit 100 --out pilot.csv
                                                              # API, no DB writes
    python3 extract_affiliations.py submit [--limit N] [--max-cost 2.00]
    python3 extract_affiliations.py collect
    python3 extract_affiliations.py reextract [--dry-run]      # re-read bad companies with Opus
    python3 extract_affiliations.py run [--limit N]           # collect, then submit
                                                              # (scrape.yml, every 6 hours)

`estimate` only reads. `pilot` calls the API (normal, non-batch pricing — it
is small and returns immediately) and writes a CSV for review, never the
database. `submit`, `collect` and `run` write to the database.

Needs ANTHROPIC_API_KEY for everything except `estimate`.
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sys
import unicodedata

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'backend'))
from description_cleaner import (  # noqa: E402
    clean_description, _name_pattern, first_name_belongs_to_other,
)
from org_names import normalize_org_name, not_an_organisation  # noqa: E402
from role_selection import tidy_title  # noqa: E402
from politicians import normalize_any_government_role  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB = os.getenv('DATABASE_URL', 'postgresql://localhost/podcast_db')

# Per million tokens (input, output) at normal price, and any extra request
# parameters. The batch price is half. On the 88-snippet pilot Haiku credited
# another guest's role to the named person on 1-2 snippets per run (not the
# same ones each run); Sonnet 5 did not, and found more "Eversource's Eric
# Bosworth"-style company mentions, at about three times the cost.
MODELS = {
    'claude-haiku-4-5': {'price': (1.00, 5.00), 'params': {}, 'estimate_factor': 1.0},
    # Adaptive thinking is on by default for Sonnet 5; this is a short
    # extraction and thinking tokens would be billed as output.
    # estimate_factor: on the same 88 snippets Sonnet 5 used ~1.4x the input
    # tokens (different tokenizer) and ~1.6x the output, so its real cost was
    # ~3x Haiku's rather than the 2x the price table alone suggests.
    'claude-sonnet-5': {'price': (2.00, 10.00), 'params': {'thinking': {'type': 'disabled'}},
                        'estimate_factor': 1.5},
    # For re-reading the hard cases (reextract). Adaptive thinking at medium
    # effort; the factor is a conservative guess for its larger token counts
    # and thinking, not a measurement.
    'claude-opus-5': {'price': (5.00, 25.00), 'params': {}, 'output_config': {'effort': 'medium'},
                      'estimate_factor': 2.5},
}
MODEL = 'claude-sonnet-5'   # chosen Sep 2026; see the pilot notes in CLAUDE.md
DATA_SOURCE = 'llm_extracted'
MAX_ATTEMPTS = 3

BATCH_DISCOUNT = 0.5

# Roles almost always sit right after the name ("Jane Doe, CEO of X"), and
# sometimes just before it ("Fervo CEO Jane Doe"), so the window leans
# forward.
SNIPPET_BEFORE = 120
SNIPPET_AFTER = 280
SNIPPET_MAX_WINDOWS = 2
# Bios often go on by first name once the full name has been given:
# "...speaks with Erica Downs, Tatiana Mitrova and Sergey Vakulenko ...
# Sergey is a senior fellow at the Carnegie Russia Eurasia Center." The
# full-name window ends before that sentence.
SNIPPET_MAX_FIRST_NAME_WINDOWS = 1

ITEMS_PER_REQUEST = 40
# A 40-item Sonnet 5 request with the appearance flags and former roles
# overran 4,096 output tokens on the second production sample. Unused
# headroom costs nothing; a cut-off response costs the whole request.
MAX_TOKENS = 16000

# Rough sizes for `estimate`, which runs without an API key. These are
# approximations (about four characters per token for English, and a
# typical one-role answer), not measured values; the pilot reports the real
# usage.
CHARS_PER_TOKEN = 4
EST_ITEM_OVERHEAD_TOKENS = 20   # the <item id=... person=...> wrapper
# Measured on the second production sample (99 items, Sonnet 5, with the
# appearance flags and former roles): ~100 output tokens per item. Stated
# here in Haiku-equivalent terms; Sonnet's estimate_factor adds the rest.
EST_OUTPUT_TOKENS_PER_ITEM = 70
EST_SCHEMA_TOKENS = 300          # the output schema, sent with every request


SYSTEM_PROMPT = """\
You read podcast episode titles and descriptions and record the job title and \
organisation of one named person per item.

Each <item> names a person in its `person` attribute and contains text from \
one episode. For that person only, list the roles the text gives them, and \
say how they relate to the episode.

Rules:
- Only the named person. Ignore the host and any other guest in the text. \
A role belongs to the person it is written next to: in "Joe Smith talks with \
Ann Lee, CEO of Acme", CEO of Acme is Ann Lee's role, and Joe Smith has none. \
When in doubt about whose role it is, leave it out.
- Copy `title` and `company` exactly as they are written in the text, as \
verbatim substrings. Do not expand abbreviations, fix capitalisation, or \
translate "Fervo's" into "Fervo Energy".
- Split the title from the organisation: "founder of Uplift" is title \
"founder", company "Uplift"; "Assistant Secretary of DOE's Office of \
Electricity" is title "Assistant Secretary", company "DOE's Office of \
Electricity". Never leave the organisation inside the title.
- Several titles at one organisation stay together as written, e.g. \
"co-founder and CEO". Roles at different organisations are separate entries, \
one per organisation.
- `company` is always exactly one organisation. When one title covers \
several organisations ("a fellow at Columbia University and NASA", \
"reporter for the Wall Street Journal and Bloomberg"), return one entry per \
organisation, each with that title: company "Columbia University" and \
company "NASA". An "and" or "&" inside a single organisation's name stays \
("McKinsey & Company", "Black & Veatch", "Los Angeles Department of Water \
and Power").
- An organisation with no title still counts: "Eversource's Eric Bosworth" \
and "Ivan Celanovic from Typhoon" give company "Eversource" / "Typhoon" \
with title null. Episode titles often pair a guest with their organisation \
by punctuation alone, and that counts too: "Dandelion Energy: Kathy Hannun", \
"(with Ben Christensen @ Cambium)" and "Nikhil Vadhavkar (Raptor Maps)" give \
company "Dandelion Energy" / "Cambium" / "Raptor Maps".
- The text may refer back to the person by first name only ("Sergey is a \
senior fellow at ..."); that is still them.
- If the text gives only a title or only an organisation, set the other to null.
- A title is a noun phrase. When the text uses a verb instead ("Mark \
co-founded SunPower's residential business", "she runs the R&D lab at \
Vulcan"), record the organisation with title null rather than copying the \
verb.
- The text is an excerpt and may start or stop mid-sentence. Never take a \
title or organisation from a fragment cut off at either end ("roles as the \
Africa" is not a title).
- `title_kind` says what the title is. "position" is a job or office \
someone holds: CEO, partner, senior fellow, professor, commissioner, \
founder, reporter, Senator. "description" is a way of describing what they \
do rather than a post: "ecologist and conservationist", "writer and social \
justice facilitator", "researcher and author", "climate activist". Use null \
when `title` is null. Words that describe their part in the episode rather \
than their work ("guest", "speaker", "expert", "friend of the show") are not \
titles at all.
- Include roles the text marks as past (former, ex-, previously, retired, \
used to, "who led ... at") with `is_former` true. Roles held at the time of \
the episode have `is_former` false.
- If the text presents the person as a host, co-host or producer of this \
podcast, set `is_podcast_host` to true and return no affiliations for them. \
Hosting a different show does not count; neither does moderating a single \
panel.
- `appears_on_episode`: true if the text presents the person as taking part \
in this episode (guest, interviewee, panellist, speaker, "joined by", \
"talks with", or a recorded clip of them speaking). False if they are only \
talked about: a politician whose policy is discussed, a person quoted from \
elsewhere, someone named in a news item, a link or a book, a production \
credit ("Produced by ..."), or a future episode. Moderating a panel recorded \
for this episode counts as taking part, and so does a name in a list of \
this episode's guests even when the excerpt starts partway through the \
list. If you cannot tell, set it true. Still list any role the text gives \
them.
- `from_other_episode`: true if the text about this person refers to a \
different or older recording rather than this episode: a list of past or \
related episodes ("past episodes you'll love", "listen to our episode \
with ..."), or a rerun of an earlier conversation ("we're re-running our \
2019 episode"). Otherwise false.
- Skip the podcast itself and its production company unless the text says \
the person works there.
- If the text states no role for the person, return an empty list. Never use \
outside knowledge.

Return one result for every item id you were given."""

OUTPUT_SCHEMA = {
    'type': 'object',
    'properties': {
        'results': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'id': {'type': 'string'},
                    'is_podcast_host': {'type': 'boolean'},
                    'appears_on_episode': {'type': 'boolean'},
                    'from_other_episode': {'type': 'boolean'},
                    'affiliations': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'title': {'anyOf': [{'type': 'string'}, {'type': 'null'}]},
                                'company': {'anyOf': [{'type': 'string'}, {'type': 'null'}]},
                                'title_kind': {'anyOf': [
                                    {'type': 'string', 'enum': ['position', 'description']},
                                    {'type': 'null'},
                                ]},
                                'is_former': {'type': 'boolean'},
                            },
                            'required': ['title', 'company', 'title_kind', 'is_former'],
                            'additionalProperties': False,
                        },
                    },
                },
                'required': ['id', 'is_podcast_host', 'appears_on_episode',
                             'from_other_episode', 'affiliations'],
                'additionalProperties': False,
            },
        },
    },
    'required': ['results'],
    'additionalProperties': False,
}


# ------------------------------------------------------------------
# SNIPPETS
# ------------------------------------------------------------------

def _collapse(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip()


def _mention_spans(names: list, text: str) -> list:
    """(start, end) of every whole-name mention of any of these names.

    Uses the scanner's own word-boundary pattern, so "Dan Yates" is not
    found inside "Jordan Yates" here either.
    """
    spans = []
    for name in names:
        if name:
            spans.extend(m.span() for m in _name_pattern(name).finditer(text))
    return sorted(spans)


def _window(text: str, start: int, end: int) -> tuple:
    """Widen a mention to SNIPPET_BEFORE/AFTER, cut back to whole words."""
    lo = max(0, start - SNIPPET_BEFORE)
    hi = min(len(text), end + SNIPPET_AFTER)
    if lo > 0 and not text[lo - 1].isspace():
        space = text.find(' ', lo, start)
        lo = space + 1 if space != -1 else start
    if hi < len(text) and not text[hi].isspace():
        space = text.rfind(' ', end, hi)
        hi = space if space != -1 else end
    return lo, hi


def _first_name_spans(names: list, text: str, full_spans: list) -> list:
    """Mentions of the person by first name alone, after their full name.

    Only used once the full name has appeared (so the first name refers
    back to them), never inside a full-name mention, and not at all when the
    same first name is attached to a different surname in the text (a host
    "Jason Bordoff" and a guest "Jason Price" in one description).
    """
    if not full_spans or not names:
        return []
    first, _, last = names[0].partition(' ')
    if not first or not last or first_name_belongs_to_other(first, last, text):
        return []
    after = full_spans[0][1]
    return [
        (a, b) for a, b in _mention_spans([first], text)
        if a >= after and not any(fa <= a < fb for fa, fb in full_spans)
    ]


def _merge(windows: list) -> list:
    merged = []
    for lo, hi in sorted(windows):
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


def build_snippet(names: list, title: str, description: str) -> str | None:
    """The text that could say what this person does, or None if they are
    not named in the episode at all.

    The episode title is included whole when it names them (titles are short
    and often are the credit: "Jane Doe, CEO of Fervo, on geothermal").
    From the description, windows around the first SNIPPET_MAX_WINDOWS
    full-name mentions (merged where they overlap), plus up to
    SNIPPET_MAX_FIRST_NAME_WINDOWS later first-name-only mentions, in text
    order. The full cleaned description is used, not the scanner's
    2,500-character cap: a "Guest:" block past the cap is exactly where a
    role is most likely to be.
    """
    parts = []
    title = _collapse(title or '')
    if title and _mention_spans(names, title):
        parts.append(title)

    desc = clean_description(description or '', max_chars=None)
    full_spans = _mention_spans(names, desc)
    full = _merge([_window(desc, a, b) for a, b in full_spans])[:SNIPPET_MAX_WINDOWS]
    covered = lambda a: any(lo <= a < hi for lo, hi in full)
    first_only = [
        _window(desc, a, b) for a, b in _first_name_spans(names, desc, full_spans)
        if not covered(a)
    ][:SNIPPET_MAX_FIRST_NAME_WINDOWS]
    for lo, hi in _merge(full + first_only):
        parts.append(_collapse(desc[lo:hi]))

    return ' … '.join(parts) if parts else None


def snippet_hash(snippet: str) -> str:
    return hashlib.sha1(snippet.encode('utf-8')).hexdigest()[:16]


def item_id(host_id: int, s_hash: str) -> str:
    return f'{host_id}-{s_hash}'


def group_appearances(appearances: list, names_by_host: dict) -> tuple:
    """Split appearances into those with nothing to send and unique items.

    Returns (no_mention, items). Each item is one (person, snippet) pair to
    send, carrying every appearance that produced the same snippet.
    """
    no_mention, items = [], {}
    for app in appearances:
        names = names_by_host.get(app['host_id'], [])
        snippet = build_snippet(names, app['episode_title'], app['description'])
        if snippet is None:
            no_mention.append(app)
            continue
        s_hash = snippet_hash(snippet)
        key = item_id(app['host_id'], s_hash)
        item = items.get(key)
        if item is None:
            item = items[key] = {
                'id': key, 'host_id': app['host_id'], 'person': names[0],
                'snippet': snippet, 'snippet_hash': s_hash, 'appearances': [],
            }
        item['appearances'].append(app)
    return no_mention, list(items.values())


# ------------------------------------------------------------------
# REQUESTS AND RESPONSES
# ------------------------------------------------------------------

def _xml_escape(text: str) -> str:
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def render_items(items: list) -> str:
    return '\n'.join(
        f'<item id="{it["id"]}" person="{_xml_escape(it["person"])}">'
        f'{_xml_escape(it["snippet"])}</item>'
        for it in items
    )


def chunk(items: list, size: int = ITEMS_PER_REQUEST) -> list:
    return [items[i:i + size] for i in range(0, len(items), size)]


def request_params(items: list, model: str = MODEL) -> dict:
    return {
        **MODELS[model]['params'],
        'model': model,
        'max_tokens': MAX_TOKENS,
        'system': SYSTEM_PROMPT,
        'messages': [{'role': 'user', 'content': render_items(items)}],
        'output_config': {'format': {'type': 'json_schema', 'schema': OUTPUT_SCHEMA},
                          **MODELS[model].get('output_config', {})},
    }


def _normalise_for_match(text: str) -> str:
    text = unicodedata.normalize('NFKC', text)
    text = text.replace('’', "'").replace('‘', "'").replace('“', '"').replace('”', '"')
    return _collapse(text).casefold()


# A form of address is not a position. Sonnet 5 returned "Dr." as a title
# for a guest whose description gave nothing else.
# "BloombergNEF's Ash Wang" came back as company "BloombergNEF's". The
# shorter form is still a verbatim substring, so stripping it keeps the check.
_POSSESSIVE_SUFFIX_RE = re.compile(r"[’']s$")

# "a senior investigative data reporter" reads as a phrase, not a role. The
# article is dropped before storing; what remains is still a verbatim
# substring of the snippet, so the check below is unaffected.
_LEADING_ARTICLE_RE = re.compile(r'^(?:a|an|the)\s+', re.IGNORECASE)

_LEADING_FORMER_RE = re.compile(r'^\s*(?:former|ex-)\s*', re.IGNORECASE)

_HONORIFIC_ONLY_RE = re.compile(r'^(?:dr|mr|mrs|ms|mx|prof|sir|dame)\.?$', re.IGNORECASE)


def _plural_tolerant_pattern(value: str):
    """Match a title whose words may be plural in the text.

    A title shared by two guests is written in the plural ("cofounders and
    managing directors of remove, Marian Krüger and Hans Westerhof"), and
    the model reasonably returns it singular for each person. Each word may
    carry a plural ending in the text: +s, +es, or y -> ies. Nothing else
    about the words may differ, so this cannot admit a different title.
    """
    parts = []
    for word in _normalise_for_match(value).split():
        if len(word) > 2 and word.endswith('y') and word[-2] not in 'aeiou':
            parts.append(re.escape(word[:-1]) + r'(?:y|ies)')
        else:
            parts.append(re.escape(word) + r'(?:e?s)?')
    return re.compile(r'(?<!\w)' + r'\s+'.join(parts) + r'(?!\w)')


def _in_text(value: str, haystack: str, field: str) -> bool:
    """Whole words only. A bare substring test accepted "Director" from
    "Directorate" and "partner" from "partnership" — the extraction's
    version of the scanner's "Dan Yates" inside "Jordan Yates"."""
    exact = re.compile(r'(?<!\w)' + re.escape(_normalise_for_match(value)) + r'(?!\w)')
    if exact.search(haystack):
        return True
    return field == 'title' and bool(_plural_tolerant_pattern(value).search(haystack))


def verified_affiliations(affiliations: list, snippet: str) -> tuple:
    """Keep only values that occur verbatim in the snippet (titles may be the
    singular of a plural in the text — see _plural_tolerant_pattern).

    Returns (kept, dropped). A value not found in the text is set to null;
    an affiliation left with neither title nor company is dropped whole.
    Duplicate (title, company, is_former) entries are collapsed. Each kept
    entry carries title, company, title_kind (null when title is) and
    is_former.
    """
    haystack = _normalise_for_match(snippet)
    kept, dropped, seen = [], [], set()
    for aff in affiliations:
        clean = {}
        for field in ('title', 'company'):
            value = aff.get(field)
            value = _collapse(value) if isinstance(value, str) else None
            if value and field == 'title':
                value = _LEADING_ARTICLE_RE.sub('', value).strip() or None
            if value and field == 'title' and _HONORIFIC_ONLY_RE.match(value):
                value = None
            if value and field == 'company':
                value = _POSSESSIVE_SUFFIX_RE.sub('', value).strip() or None
            # A state/country/abbreviation or a cut-off fragment is not an
            # organisation ("California", "UK", "the University of").
            if value and field == 'company' and not_an_organisation(value):
                dropped.append((field, value))
                value = None
                clean[field] = None
                continue
            if value and _in_text(value, haystack, field):
                # Checked verbatim first; only then tidied ("cofounder" ->
                # "co-founder", "Chief Executive Officer" -> "CEO"), so every
                # title reads the same way.
                clean[field] = tidy_title(value) if field == 'title' else value
            else:
                clean[field] = None
                if value:
                    dropped.append((field, value))
        pair = (clean['title'], clean['company'])
        if pair == (None, None):
            continue
        clean['is_former'] = bool(aff.get('is_former'))
        # "former CEO" -> CEO, marked former; the flag already says it.
        if clean['title'] and _LEADING_FORMER_RE.match(clean['title']):
            clean['title'] = _LEADING_FORMER_RE.sub('', clean['title']).strip() or clean['title']
            clean['is_former'] = True
        kind = aff.get('title_kind')
        clean['title_kind'] = kind if clean['title'] and kind in ('position', 'description') else None
        entries = [clean]
        # A politician's role becomes one title at one organisation ("Rep." +
        # "Florida" -> Representative @ U.S. House), read with the raw company
        # even when it was a place and dropped above (backend/politicians.py).
        raw_company = _collapse(aff.get('company')) if isinstance(aff.get('company'), str) else None
        political = normalize_any_government_role(clean["title"], raw_company, snippet) if clean["title"] else None
        if political:
            entries = [{'title': d['title'], 'company': d['company'],
                        'is_former': bool(d['is_former'] or clean['is_former']),
                        'title_kind': 'position'} for d in political]
        for entry in entries:
            key = tuple(_normalise_for_match(v) if v else None
                        for v in (entry['title'], entry['company'])) + (entry['is_former'],)
            if key in seen:
                continue
            seen.add(key)
            kept.append(entry)
    return _drop_subsumed(kept), dropped


def _drop_subsumed(affiliations: list) -> list:
    """Drop a role another role at the same organisation already covers.

    Sonnet 5 on the production sample returned "CEO" and "Co-Founder and
    CEO" at Planetary as two roles for one sentence. The longer title
    wins; so does a title over a bare organisation ("— @ Wunder" next to
    "CEO @ Wunder").
    """
    norm = lambda v: _normalise_for_match(v) if v else ''
    out = []
    for i, a in enumerate(affiliations):
        at, ac = norm(a['title']), norm(a['company'])
        covered = any(
            j != i and b['is_former'] == a['is_former']
            and norm(b['company']) == ac and at in norm(b['title'])
            and len(norm(b['title'])) > len(at)
            for j, b in enumerate(affiliations)
        )
        if not covered:
            out.append(a)
    return out


def parse_response_text(text: str, expected_ids: set) -> dict:
    """{item id: {'is_host', 'appears', 'other_episode', 'affiliations'}} for
    the ids this request was sent.

    Unknown ids are ignored; ids missing from the response are simply absent
    from the result, and the caller marks them for retry. Anyone flagged as
    this podcast's host gets no affiliations, whatever else came back.
    """
    data = json.loads(text)
    out = {}
    for result in data.get('results', []):
        rid = result.get('id')
        if rid in expected_ids and rid not in out:
            is_host = bool(result.get('is_podcast_host'))
            out[rid] = {
                'is_host': is_host,
                # Missing means the schema was not followed; assume the
                # credit is right rather than flag it as mentioned-only.
                'appears': result.get('appears_on_episode', True) is not False,
                'other_episode': bool(result.get('from_other_episode')),
                'affiliations': [] if is_host else (result.get('affiliations') or []),
            }
    return out


def message_text(message) -> str | None:
    """The JSON text of a finished response, or None if it did not finish.

    max_tokens or refusal means the JSON may be cut short or off-schema.
    """
    if message.stop_reason != 'end_turn':
        return None
    return next((b.text for b in message.content if b.type == 'text'), None)


def estimate_cost(items: list, batch: bool = True, model: str = MODEL) -> dict:
    """Approximate token counts and dollars, without calling the API."""
    requests = chunk(items)
    system_tokens = (len(SYSTEM_PROMPT) // CHARS_PER_TOKEN) + EST_SCHEMA_TOKENS
    item_tokens = sum(
        len(it['snippet']) // CHARS_PER_TOKEN + EST_ITEM_OVERHEAD_TOKENS for it in items
    )
    input_tokens = system_tokens * len(requests) + item_tokens
    output_tokens = EST_OUTPUT_TOKENS_PER_ITEM * len(items)
    dollars = usage_cost(input_tokens, output_tokens, batch, model) * MODELS[model]['estimate_factor']
    return {
        'requests': len(requests), 'input_tokens': input_tokens,
        'output_tokens': output_tokens, 'dollars': round(dollars, 2),
    }


def usage_cost(input_tokens: int, output_tokens: int, batch: bool, model: str = MODEL) -> float:
    price_in, price_out = MODELS[model]['price']
    factor = BATCH_DISCOUNT if batch else 1.0
    return (input_tokens * price_in + output_tokens * price_out) / 1_000_000 * factor


# ------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------

def get_names_by_host(conn) -> dict:
    """Every spelling of each person, canonical name first, longest aliases
    after it — so a description that uses a stored nickname still yields a
    snippet."""
    cur = conn.cursor()
    cur.execute("""
        SELECT host_id, first_name || ' ' || last_name, 0 AS alias
        FROM hosts WHERE first_name IS NOT NULL AND last_name IS NOT NULL
        UNION ALL
        SELECT host_id, alias_name, 1 FROM host_aliases
        ORDER BY 1, 3, 2
    """)
    names = {}
    for host_id, name, _ in cur.fetchall():
        if name and name.strip():
            names.setdefault(host_id, []).append(name.strip())
    cur.close()
    return names


def get_appearances_to_process(conn, limit: int = None, random_sample: bool = False) -> list:
    """Guest appearances that have not been processed yet, plus retries.

    Before the migration has run there is nothing processed, so every guest
    appearance qualifies — that is what lets `estimate` and `pilot` run
    against production ahead of the migration.
    """
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('affiliation_extractions') IS NOT NULL")
    migrated = cur.fetchone()[0]
    order = 'random()' if random_sample else 'eh.episode_id, eh.host_id'
    join = """
        LEFT JOIN affiliation_extractions ax
               ON ax.episode_id = eh.episode_id AND ax.host_id = eh.host_id""" if migrated else ''
    unprocessed = """
          AND (ax.episode_id IS NULL OR (ax.status = 'retry' AND ax.attempts < %(max_attempts)s))""" if migrated else ''
    cur.execute(f"""
        SELECT eh.episode_id, eh.host_id, e.title, e.description, p.title
        FROM episode_host eh
        JOIN episodes e ON e.episode_id = eh.episode_id
        JOIN podcasts p ON p.podcast_id = e.podcast_id
        {join}
        WHERE eh.is_guest {unprocessed}
          -- Hosts credited on their own show are not guests there; their
          -- roles are left to a separate process.
          AND NOT EXISTS (
              SELECT 1 FROM host_podcast hp
              WHERE hp.host_id = eh.host_id AND hp.podcast_id = e.podcast_id
          )
        ORDER BY {order}
        {'LIMIT %(limit)s' if limit else ''}
    """, {'max_attempts': MAX_ATTEMPTS, 'limit': limit})
    rows = cur.fetchall()
    cur.close()
    return [
        {'episode_id': r[0], 'host_id': r[1], 'episode_title': r[2],
         'description': r[3], 'podcast_title': r[4]}
        for r in rows
    ]


def _upsert_extractions(cur, rows: list):
    """rows: (episode_id, host_id, status, snippet, snippet_hash, batch_id, model)."""
    execute_values(cur, """
        INSERT INTO affiliation_extractions
            (episode_id, host_id, status, snippet, snippet_hash, batch_id, model, attempts)
        VALUES %s
        ON CONFLICT (episode_id, host_id) DO UPDATE SET
            status = EXCLUDED.status, snippet = EXCLUDED.snippet,
            snippet_hash = EXCLUDED.snippet_hash, batch_id = EXCLUDED.batch_id,
            model = EXCLUDED.model,
            attempts = affiliation_extractions.attempts + 1,
            completed_at = CASE WHEN EXCLUDED.status = 'no_mention' THEN now() END
    """, [r + (1,) for r in rows], template='(%s, %s, %s, %s, %s, %s, %s, %s)')


def record_results(cur, pending: list, answers: dict) -> tuple:
    """Write one batch's answers. Does not commit.

    pending: (episode_id, host_id, snippet, snippet_hash) rows still
    'pending' for the batch. answers: parse_response_text() output.
    Appearances with no answer go to 'retry'; ones the model flagged as this
    podcast's host go to 'host' with nothing stored. The appearance-level
    flags (appears_on_episode, from_other_episode) are recorded on the
    extraction row for both. Returns
    (done, affiliations added, retried, values dropped as not verbatim).
    """
    done, retry, hosts, new_rows, dropped_total = [], [], [], [], 0
    for episode_id, host_id, snippet, s_hash in pending:
        key = item_id(host_id, s_hash)
        if key not in answers:
            retry.append((episode_id, host_id))
            continue
        answer = answers[key]
        flags = (episode_id, host_id, answer['appears'], answer['other_episode'])
        if answer['is_host']:
            hosts.append(flags)
            continue
        kept, dropped = verified_affiliations(answer['affiliations'], snippet)
        dropped_total += len(dropped)
        done.append(flags)
        new_rows.extend(
            (episode_id, host_id, a['title'], a['company'], normalize_org_name(a['company']),
             a['title_kind'], a['is_former'], DATA_SOURCE)
            for a in kept
        )

    # Replace earlier automated rows for these appearances; 'manual' rows are
    # human decisions and are never touched.
    for rows, status in ((done, 'done'), (hosts, 'host')):
        if not rows:
            continue
        # Replace earlier automated rows for these appearances; 'manual' rows
        # are human decisions and are never touched.
        execute_values(cur, """
            DELETE FROM host_affiliations ha USING (VALUES %s) AS d(episode_id, host_id)
            WHERE ha.episode_id = d.episode_id AND ha.host_id = d.host_id
              AND ha.data_source <> 'manual'
        """, [r[:2] for r in rows])
        execute_values(cur, f"""
            UPDATE affiliation_extractions ax
            SET status = '{status}', completed_at = now(),
                appears_on_episode = d.appears, from_other_episode = d.other_episode
            FROM (VALUES %s) AS d(episode_id, host_id, appears, other_episode)
            WHERE ax.episode_id = d.episode_id AND ax.host_id = d.host_id
        """, rows)
    if new_rows:
        execute_values(cur, """
            INSERT INTO host_affiliations
                (episode_id, host_id, title, company, company_key, title_kind, is_former, data_source)
            VALUES %s ON CONFLICT DO NOTHING
        """, new_rows)
    if retry:
        execute_values(cur, """
            UPDATE affiliation_extractions ax SET status = 'retry'
            FROM (VALUES %s) AS d(episode_id, host_id)
            WHERE ax.episode_id = d.episode_id AND ax.host_id = d.host_id
        """, retry)
    return len(done) + len(hosts), len(new_rows), len(retry), dropped_total


# ------------------------------------------------------------------
# COMMANDS
# ------------------------------------------------------------------

def _load(conn, limit, random_sample=False):
    names = get_names_by_host(conn)
    appearances = get_appearances_to_process(conn, limit=limit, random_sample=random_sample)
    no_mention, items = group_appearances(appearances, names)
    return appearances, no_mention, items


def _report(appearances, no_mention, items, est):
    logger.info(f"Appearances to process: {len(appearances)}")
    logger.info(f"  name not in title/description (no API call): {len(no_mention)}")
    logger.info(f"  unique (person, snippet) items to send:      {len(items)}")
    logger.info(f"  batch requests ({ITEMS_PER_REQUEST} items each):          {est['requests']}")
    logger.info(f"Estimated tokens: {est['input_tokens']:,} in, {est['output_tokens']:,} out "
                f"(approximate, ~{CHARS_PER_TOKEN} chars/token)")
    logger.info(f"Estimated cost at batch price: ${est['dollars']:.2f}")


def cmd_estimate(limit=None, model=MODEL):
    conn = psycopg2.connect(DB)
    appearances, no_mention, items = _load(conn, limit)
    conn.close()
    _report(appearances, no_mention, items, estimate_cost(items, model=model))


def cmd_pilot(limit: int, out_path: str, model: str = MODEL):
    """A random sample, sent with normal (non-batch) calls, written to CSV.

    Nothing is written to the database. The CSV has one row per extracted
    affiliation (or one empty row when none was found), next to the snippet
    it came from, for checking by hand.
    """
    import anthropic

    conn = psycopg2.connect(DB)
    appearances, no_mention, items = _load(conn, limit, random_sample=True)
    host_names = get_names_by_host(conn)
    conn.close()
    _report(appearances, no_mention, items, estimate_cost(items, batch=False, model=model))

    client = anthropic.Anthropic()
    results, tokens_in, tokens_out, failed = {}, 0, 0, 0
    for group in chunk(items):
        message = client.messages.create(**request_params(group, model))
        tokens_in += message.usage.input_tokens
        tokens_out += message.usage.output_tokens
        text = message_text(message)
        if text is None:
            failed += len(group)
            logger.warning(f"Request stopped with {message.stop_reason}; {len(group)} items skipped")
            continue
        results.update(parse_response_text(text, {it['id'] for it in group}))

    dropped_total = found = flagged_hosts = mentioned_only = other_episode = 0
    empty = {'is_host': False, 'appears': True, 'other_episode': False, 'affiliations': []}
    yes = lambda v: 'yes' if v else ''
    with open(out_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['person', 'podcast', 'episode', 'title', 'company', 'title_kind', 'former',
                         'podcast_host', 'mentioned_only', 'other_episode',
                         'dropped_unverified', 'snippet'])
        for it in items:
            answer = results.get(it['id'], empty)
            kept, dropped = verified_affiliations(answer['affiliations'], it['snippet'])
            dropped_total += len(dropped)
            found += bool(kept)
            flagged_hosts += answer['is_host']
            mentioned_only += not answer['appears']
            other_episode += answer['other_episode']
            app = it['appearances'][0]
            dropped_s = '; '.join(f'{k}={v}' for k, v in dropped)
            blank = {'title': None, 'company': None, 'title_kind': None, 'is_former': False}
            for aff in kept or [blank]:
                writer.writerow([it['person'], app['podcast_title'], app['episode_title'],
                                 aff['title'] or '', aff['company'] or '', aff['title_kind'] or '',
                                 yes(aff['is_former']), yes(answer['is_host']),
                                 yes(not answer['appears']), yes(answer['other_episode']),
                                 dropped_s, it['snippet']])
        for app in no_mention:
            name = (host_names.get(app['host_id']) or ['?'])[0]
            writer.writerow([name, app['podcast_title'], app['episode_title'],
                             '', '', '', '', '', '', '', '', '(name not in title or description)'])

    logger.info(f"Items with at least one role: {found}/{len(items)}; flagged as this podcast's "
                f"host: {flagged_hosts}; mentioned only: {mentioned_only}; from another episode: "
                f"{other_episode}; values dropped as not verbatim: {dropped_total}; "
                f"items failed: {failed}")
    logger.info(f"Actual usage: {tokens_in:,} in, {tokens_out:,} out = "
                f"${usage_cost(tokens_in, tokens_out, False, model):.4f} at normal price "
                f"(${usage_cost(tokens_in, tokens_out, True, model):.4f} at batch price) on {model}")
    logger.info(f"Wrote {out_path}")


def _send(conn, appearances, no_mention, items, model, max_cost, dry_run, over_cap_is_error,
          record_no_mention=True):
    """Estimate, check the cost cap, submit one batch, record pending rows."""
    from anthropic import Anthropic
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    est = estimate_cost(items, model=model)
    _report(appearances, no_mention, items, est)
    if dry_run:
        return
    if est['dollars'] > max_cost:
        message = (f"Estimated ${est['dollars']:.2f} is over --max-cost ${max_cost:.2f}; "
                   f"nothing submitted. Lower --limit or raise --max-cost.")
        # The scheduled run must not fail the scrape over this — it logs and
        # tries again next time. A manual submit stops with an error.
        if over_cap_is_error:
            sys.exit(message)
        logger.warning(message)
        return

    cur = conn.cursor()
    if no_mention and record_no_mention:
        _upsert_extractions(cur, [
            (a['episode_id'], a['host_id'], 'no_mention', None, None, None, None)
            for a in no_mention
        ])
        conn.commit()
    if not items:
        logger.info("Nothing to send.")
        return

    batch = Anthropic().messages.batches.create(requests=[
        Request(custom_id=f'req-{i}', params=MessageCreateParamsNonStreaming(**request_params(group, model)))
        for i, group in enumerate(chunk(items))
    ])
    logger.info(f"Submitted batch {batch.id}")
    _upsert_extractions(cur, [
        (a['episode_id'], a['host_id'], 'pending', it['snippet'], it['snippet_hash'], batch.id, model)
        for it in items for a in it['appearances']
    ])
    conn.commit()
    cur.close()


def cmd_submit(limit=None, max_cost=2.00, dry_run=False, model=MODEL, random_sample=False,
               over_cap_is_error=True):
    conn = psycopg2.connect(DB)
    try:
        appearances, no_mention, items = _load(conn, limit, random_sample)
        _send(conn, appearances, no_mention, items, model, max_cost, dry_run, over_cap_is_error)
    finally:
        conn.close()


def get_appearances_with_non_org_companies(conn) -> list:
    """Appearances whose stored company is marked "not an organisation" — a
    state or country ("California", "UK") or a cut-off fragment ("the
    University of"). The guest's real organisation was missed."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ha.episode_id, ha.host_id, e.title, e.description, p.title
        FROM host_affiliations ha
        JOIN organization_aliases oa ON oa.normalized_name = ha.company_key
        JOIN organizations o ON o.org_id = oa.org_id AND o.not_an_org
        JOIN episodes e ON e.episode_id = ha.episode_id
        JOIN podcasts p ON p.podcast_id = e.podcast_id
        WHERE ha.data_source <> 'manual'
    """)
    rows = cur.fetchall()
    cur.close()
    return [{'episode_id': r[0], 'host_id': r[1], 'episode_title': r[2], 'description': r[3],
             'podcast_title': r[4]} for r in rows]


# Re-reads look further on each side of the name: a fragment like "the
# University of" was usually the window's edge cutting the real name.
WIDE_WINDOW = {'SNIPPET_BEFORE': 300, 'SNIPPET_AFTER': 600, 'SNIPPET_MAX_WINDOWS': 3}


def cmd_reextract(model='claude-opus-5', max_cost=5.00, dry_run=False):
    """Re-read, with a stronger model and a wider window, every appearance
    whose company turned out not to be an organisation. collect replaces
    the old roles for those appearances (manual rows are kept)."""
    saved = {k: globals()[k] for k in WIDE_WINDOW}
    globals().update(WIDE_WINDOW)
    conn = psycopg2.connect(DB)
    try:
        names = get_names_by_host(conn)
        appearances = get_appearances_with_non_org_companies(conn)
        no_mention, items = group_appearances(appearances, names)
        # Already processed once; a name the wider window cannot find leaves
        # the existing row alone rather than rewriting its status.
        _send(conn, appearances, no_mention, items, model, max_cost, dry_run, True,
              record_no_mention=False)
    finally:
        conn.close()
        globals().update(saved)


def cmd_collect():
    from anthropic import Anthropic

    client = Anthropic()
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT batch_id FROM affiliation_extractions
        WHERE status = 'pending' AND batch_id IS NOT NULL
    """)
    batch_ids = [r[0] for r in cur.fetchall()]
    if not batch_ids:
        logger.info("No pending batches.")
    recorded_any = False

    for batch_id in batch_ids:
        batch = client.messages.batches.retrieve(batch_id)
        if batch.processing_status != 'ended':
            logger.info(f"Batch {batch_id}: {batch.processing_status}, "
                        f"{batch.request_counts.processing} requests still processing")
            continue

        cur.execute("""
            SELECT episode_id, host_id, snippet, snippet_hash, model FROM affiliation_extractions
            WHERE status = 'pending' AND batch_id = %s
        """, (batch_id,))
        rows = cur.fetchall()
        model = rows[0][4] if rows and rows[0][4] in MODELS else MODEL
        pending = [r[:4] for r in rows]
        snippet_by_id = {item_id(h, s_hash): snippet for _, h, snippet, s_hash in pending}
        expected = set(snippet_by_id)

        answers, tokens_in, tokens_out = {}, 0, 0
        for result in client.messages.batches.results(batch_id):
            if result.result.type != 'succeeded':
                logger.warning(f"{batch_id}/{result.custom_id}: {result.result.type}")
                continue
            message = result.result.message
            tokens_in += message.usage.input_tokens
            tokens_out += message.usage.output_tokens
            text = message_text(message)
            if text is None:
                logger.warning(f"{batch_id}/{result.custom_id}: stopped with {message.stop_reason}")
                continue
            answers.update(parse_response_text(text, expected))

        done, added, retry, dropped = record_results(cur, pending, answers)
        conn.commit()
        logger.info(f"Batch {batch_id}: {done} appearances done, {added} affiliations, "
                    f"{retry} to retry, {dropped} values dropped as not verbatim; "
                    f"${usage_cost(tokens_in, tokens_out, True, model):.4f}")
        recorded_any = True

    cur.close()
    conn.close()

    # New company spellings become organisations right away, so Company Admin
    # sees them without a separate step (see organizations.py).
    if recorded_any:
        from organizations import sync as sync_organizations
        sync_organizations()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)

    model_arg = dict(choices=sorted(MODELS), default=MODEL)

    p = sub.add_parser('estimate', help='Count work and estimate cost; no API, no writes')
    p.add_argument('--limit', type=int)
    p.add_argument('--model', **model_arg)

    p = sub.add_parser('pilot', help='Random sample to CSV; API calls, no DB writes')
    p.add_argument('--limit', type=int, default=100)
    p.add_argument('--out', default='affiliation_pilot.csv')
    p.add_argument('--model', **model_arg)

    for name in ('submit', 'run'):
        p = sub.add_parser(name)
        p.add_argument('--limit', type=int)
        p.add_argument('--max-cost', type=float, default=2.00,
                       help='Refuse to submit if the estimate is above this many dollars')
        p.add_argument('--dry-run', action='store_true', help='Report what would be sent, then stop')
        p.add_argument('--model', **model_arg)
        p.add_argument('--random', action='store_true',
                       help='Pick --limit appearances at random rather than in episode order '
                            '(for a representative quality sample)')

    sub.add_parser('collect', help='Record results of finished batches')

    p = sub.add_parser('reextract', help='Re-read appearances whose company is not an organisation')
    p.add_argument('--model', choices=sorted(MODELS), default='claude-opus-5')
    p.add_argument('--max-cost', type=float, default=5.00)
    p.add_argument('--dry-run', action='store_true')

    args = parser.parse_args()
    if args.command == 'estimate':
        cmd_estimate(args.limit, args.model)
    elif args.command == 'pilot':
        cmd_pilot(args.limit, args.out, args.model)
    elif args.command == 'submit':
        cmd_submit(args.limit, args.max_cost, args.dry_run, args.model, args.random)
    elif args.command == 'collect':
        cmd_collect()
    elif args.command == 'reextract':
        cmd_reextract(args.model, args.max_cost, args.dry_run)
    elif args.command == 'run':
        cmd_collect()
        cmd_submit(args.limit, args.max_cost, args.dry_run, args.model, args.random,
                   over_cap_is_error=False)


if __name__ == '__main__':
    main()
