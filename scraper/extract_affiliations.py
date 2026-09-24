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
  * Everything runs through the Message Batches API on Haiku 4.5, which is
    half the normal per-token price, with ~40 snippets per request so the
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
    python3 extract_affiliations.py run [--limit N]           # collect, then submit

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
from description_cleaner import clean_description, _name_pattern  # noqa: E402

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
}
MODEL = 'claude-haiku-4-5'
DATA_SOURCE = 'llm_extracted'
MAX_ATTEMPTS = 3

BATCH_DISCOUNT = 0.5

# Roles almost always sit right after the name ("Jane Doe, CEO of X"), and
# sometimes just before it ("Fervo CEO Jane Doe"), so the window leans
# forward.
SNIPPET_BEFORE = 120
SNIPPET_AFTER = 280
SNIPPET_MAX_WINDOWS = 2

ITEMS_PER_REQUEST = 40
MAX_TOKENS = 4096

# Rough sizes for `estimate`, which runs without an API key. These are
# approximations (about four characters per token for English, and a
# typical one-role answer), not measured values; the pilot reports the real
# usage.
CHARS_PER_TOKEN = 4
EST_ITEM_OVERHEAD_TOKENS = 20   # the <item id=... person=...> wrapper
EST_OUTPUT_TOKENS_PER_ITEM = 30
EST_SCHEMA_TOKENS = 300          # the output schema, sent with every request


SYSTEM_PROMPT = """\
You read podcast episode titles and descriptions and record the job title and \
organisation of one named person per item.

Each <item> names a person in its `person` attribute and contains text from \
one episode. For that person only, list the roles the text says they hold at \
the time of the episode.

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
- An organisation with no title still counts: "Eversource's Eric Bosworth" \
and "Ivan Celanovic from Typhoon" give company "Eversource" / "Typhoon" \
with title null.
- If the text gives only a title or only an organisation, set the other to null.
- A title is a position: CEO, partner, senior fellow, professor, \
commissioner, founder, reporter. Descriptions such as "expert", "leader", \
"guest" or "author" on their own are not titles.
- Skip roles the text marks as past: former, ex-, previously, retired, \
used to.
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
                    'affiliations': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'title': {'anyOf': [{'type': 'string'}, {'type': 'null'}]},
                                'company': {'anyOf': [{'type': 'string'}, {'type': 'null'}]},
                            },
                            'required': ['title', 'company'],
                            'additionalProperties': False,
                        },
                    },
                },
                'required': ['id', 'affiliations'],
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


def build_snippet(names: list, title: str, description: str) -> str | None:
    """The text that could say what this person does, or None if they are
    not named in the episode at all.

    The episode title is included whole when it names them (titles are short
    and often are the credit: "Jane Doe, CEO of Fervo, on geothermal").
    From the description, the first SNIPPET_MAX_WINDOWS mentions are taken
    with their surrounding text; overlapping windows are merged. The full
    cleaned description is used, not the scanner's 2,500-character cap: a
    "Guest:" block past the cap is exactly where a role is most likely to be.
    """
    parts = []
    title = _collapse(title or '')
    if title and _mention_spans(names, title):
        parts.append(title)

    desc = clean_description(description or '', max_chars=None)
    windows = []
    for start, end in _mention_spans(names, desc):
        lo, hi = _window(desc, start, end)
        if windows and lo <= windows[-1][1]:
            windows[-1] = (windows[-1][0], max(windows[-1][1], hi))
        else:
            windows.append((lo, hi))
    for lo, hi in windows[:SNIPPET_MAX_WINDOWS]:
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
        'output_config': {'format': {'type': 'json_schema', 'schema': OUTPUT_SCHEMA}},
    }


def _normalise_for_match(text: str) -> str:
    text = unicodedata.normalize('NFKC', text)
    text = text.replace('’', "'").replace('‘', "'").replace('“', '"').replace('”', '"')
    return _collapse(text).casefold()


# A form of address is not a position. Sonnet 5 returned "Dr." as a title
# for a guest whose description gave nothing else.
_HONORIFIC_ONLY_RE = re.compile(r'^(?:dr|mr|mrs|ms|mx|prof|sir|dame)\.?$', re.IGNORECASE)


def verified_affiliations(affiliations: list, snippet: str) -> tuple:
    """Keep only values that occur verbatim in the snippet.

    Returns (kept, dropped). A value not found in the text is set to null;
    an affiliation left with neither title nor company is dropped whole.
    Duplicate (title, company) pairs are collapsed.
    """
    haystack = _normalise_for_match(snippet)
    kept, dropped, seen = [], [], set()
    for aff in affiliations:
        clean = {}
        for field in ('title', 'company'):
            value = aff.get(field)
            value = _collapse(value) if isinstance(value, str) else None
            if value and field == 'title' and _HONORIFIC_ONLY_RE.match(value):
                value = None
            if value and _normalise_for_match(value) in haystack:
                clean[field] = value
            else:
                clean[field] = None
                if value:
                    dropped.append((field, value))
        pair = (clean['title'], clean['company'])
        if pair == (None, None) or pair in seen:
            continue
        seen.add(pair)
        kept.append(clean)
    return kept, dropped


def parse_response_text(text: str, expected_ids: set) -> dict:
    """{item id: [affiliation, ...]} for the ids this request was sent.

    Unknown ids are ignored; ids missing from the response are simply absent
    from the result, and the caller marks them for retry.
    """
    data = json.loads(text)
    out = {}
    for result in data.get('results', []):
        rid = result.get('id')
        if rid in expected_ids and rid not in out:
            out[rid] = result.get('affiliations') or []
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
    'pending' for the batch. answers: {item id: [affiliation, ...]}.
    Appearances with no answer go to 'retry'. Returns
    (done, affiliations added, retried, values dropped as not verbatim).
    """
    done, retry, new_rows, dropped_total = [], [], [], 0
    for episode_id, host_id, snippet, s_hash in pending:
        key = item_id(host_id, s_hash)
        if key not in answers:
            retry.append((episode_id, host_id))
            continue
        kept, dropped = verified_affiliations(answers[key], snippet)
        dropped_total += len(dropped)
        done.append((episode_id, host_id))
        new_rows.extend((episode_id, host_id, a['title'], a['company'], DATA_SOURCE) for a in kept)

    # Replace earlier automated rows for these appearances; 'manual' rows are
    # human decisions and are never touched.
    if done:
        execute_values(cur, """
            DELETE FROM host_affiliations ha USING (VALUES %s) AS d(episode_id, host_id)
            WHERE ha.episode_id = d.episode_id AND ha.host_id = d.host_id
              AND ha.data_source <> 'manual'
        """, done)
    if new_rows:
        execute_values(cur, """
            INSERT INTO host_affiliations (episode_id, host_id, title, company, data_source)
            VALUES %s ON CONFLICT DO NOTHING
        """, new_rows)
    if done:
        execute_values(cur, """
            UPDATE affiliation_extractions ax SET status = 'done', completed_at = now()
            FROM (VALUES %s) AS d(episode_id, host_id)
            WHERE ax.episode_id = d.episode_id AND ax.host_id = d.host_id
        """, done)
    if retry:
        execute_values(cur, """
            UPDATE affiliation_extractions ax SET status = 'retry'
            FROM (VALUES %s) AS d(episode_id, host_id)
            WHERE ax.episode_id = d.episode_id AND ax.host_id = d.host_id
        """, retry)
    return len(done), len(new_rows), len(retry), dropped_total


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

    dropped_total = 0
    with open(out_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['person', 'podcast', 'episode', 'title', 'company',
                         'dropped_unverified', 'snippet'])
        for it in items:
            kept, dropped = verified_affiliations(results.get(it['id'], []), it['snippet'])
            dropped_total += len(dropped)
            app = it['appearances'][0]
            dropped_s = '; '.join(f'{k}={v}' for k, v in dropped)
            for aff in kept or [{'title': None, 'company': None}]:
                writer.writerow([it['person'], app['podcast_title'], app['episode_title'],
                                 aff['title'] or '', aff['company'] or '',
                                 dropped_s, it['snippet']])
        for app in no_mention:
            name = (host_names.get(app['host_id']) or ['?'])[0]
            writer.writerow([name, app['podcast_title'], app['episode_title'],
                             '', '', '', '(name not in title or description)'])

    found = sum(1 for it in items if verified_affiliations(results.get(it['id'], []), it['snippet'])[0])
    logger.info(f"Items with at least one role: {found}/{len(items)}; "
                f"values dropped as not verbatim: {dropped_total}; items failed: {failed}")
    logger.info(f"Actual usage: {tokens_in:,} in, {tokens_out:,} out = "
                f"${usage_cost(tokens_in, tokens_out, False, model):.4f} at normal price "
                f"(${usage_cost(tokens_in, tokens_out, True, model):.4f} at batch price) on {model}")
    logger.info(f"Wrote {out_path}")


def cmd_submit(limit=None, max_cost=2.00, dry_run=False, model=MODEL):
    from anthropic import Anthropic
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    conn = psycopg2.connect(DB)
    appearances, no_mention, items = _load(conn, limit)
    est = estimate_cost(items, model=model)
    _report(appearances, no_mention, items, est)

    if dry_run:
        conn.close()
        return
    if est['dollars'] > max_cost:
        conn.close()
        sys.exit(f"Estimated ${est['dollars']:.2f} is over --max-cost ${max_cost:.2f}; "
                 f"nothing submitted. Lower --limit or raise --max-cost.")

    cur = conn.cursor()
    if no_mention:
        _upsert_extractions(cur, [
            (a['episode_id'], a['host_id'], 'no_mention', None, None, None, None)
            for a in no_mention
        ])
        conn.commit()

    if not items:
        logger.info("Nothing to send.")
        conn.close()
        return

    client = Anthropic()
    batch = client.messages.batches.create(requests=[
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
    conn.close()


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

    cur.close()
    conn.close()


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

    sub.add_parser('collect', help='Record results of finished batches')

    args = parser.parse_args()
    if args.command == 'estimate':
        cmd_estimate(args.limit, args.model)
    elif args.command == 'pilot':
        cmd_pilot(args.limit, args.out, args.model)
    elif args.command == 'submit':
        cmd_submit(args.limit, args.max_cost, args.dry_run, args.model)
    elif args.command == 'collect':
        cmd_collect()
    elif args.command == 'run':
        cmd_collect()
        cmd_submit(args.limit, args.max_cost, args.dry_run, args.model)


if __name__ == '__main__':
    main()
