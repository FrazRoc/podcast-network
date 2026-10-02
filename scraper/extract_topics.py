"""
extract_topics.py

What each episode is about: 2-6 open-ended topic tags per episode
("small modular reactors", "interconnection queues", "IRA tax credits"),
each filed under one of a few fixed categories (topic_names.CATEGORIES)
and backed by a phrase quoted from the episode's own title or description.
A person's topics are counted from the episodes they are credited on.

Same shape as extract_affiliations.py: the Batch API at half price, ~40
episodes per request, every value checked against the text it came from
(here the evidence phrase must occur verbatim, whole words), results
recorded per episode in topic_extractions. Tags are deduplicated through
topic_names.normalize_topic(): every spelling is an alias of one tag.

Usage:
    python3 extract_topics.py estimate [--limit N]              # no API, no writes
    python3 extract_topics.py pilot --limit 200 [--out f.csv]   # API, CSV only
    python3 extract_topics.py submit [--limit N] [--max-cost 25]
    python3 extract_topics.py collect
    python3 extract_topics.py run [--limit N] [--max-cost 2]    # collect, then submit (cron)
    python3 extract_topics.py export --out DIR / import --batch-id manual-... FILES

Needs ANTHROPIC_API_KEY for everything except `estimate`, `export` and `import`.
"""

import argparse
import csv
import json
import logging
import os
import re
import sys

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'backend'))
from description_cleaner import clean_description  # noqa: E402
from topic_names import CATEGORIES, normalize_topic, topic_slug  # noqa: E402
from extract_affiliations import (  # noqa: E402
    MODELS, BATCH_DISCOUNT, MAX_ATTEMPTS, CHARS_PER_TOKEN,
    _normalise_for_match, message_text, usage_cost,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB = os.getenv('DATABASE_URL', 'postgresql://localhost/podcast_db')

MODEL = 'claude-sonnet-5'
DATA_SOURCE = 'llm'
TEXT_MAX = 1500             # characters of cleaned description sent per episode
ITEMS_PER_REQUEST = 40
MAX_TOKENS = 16000
MIN_TOPICS, MAX_TOPICS = 2, 6
MAX_TOPIC_LEN = 60
PROMPT_TAG_LIMIT = 200       # existing tags shown to the model to reuse

# For `estimate` (no API): ~4 chars/token, ~25 output tokens per topic.
EST_OUTPUT_TOKENS_PER_ITEM = 120
EST_SCHEMA_TOKENS = 300

SYSTEM_PROMPT = """\
You read podcast episode titles and descriptions and record what each \
episode is about.

For each <item>, return 2 to 6 topics: the subjects the episode actually \
discusses. Rules:
- A topic is a short noun phrase (1-5 words), specific enough to be useful \
and general enough to recur across episodes: "small modular reactors", \
"interconnection queues", "offshore wind", "IRA tax credits", "data center \
power demand", "carbon removal markets". Not "energy", "climate", \
"sustainability" or "the future" on their own, and not a person, company \
or show name ("Tesla" is not a topic; "electric vehicles" is).
- Prefer an existing topic name from the list below when it fits; only \
write a new one when none does. Use the plural/singular and wording of the \
existing name exactly.
- `category` is one of the fixed categories given.
- Exactly one topic is `primary`: the main subject of the episode.
- `evidence` is a short phrase copied exactly, word for word, from the \
item text that shows the episode covers the topic. Never paraphrase it.
- Only what the text says the episode covers. Ignore sponsor reads, \
"subscribe" lines, links, credits and lists of other episodes.
- If the text is too thin to tell (a teaser, a trailer, only credits), \
return an empty list.

Categories: {categories}

Existing topics (reuse these names when they fit):
{existing}

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
                    'topics': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'topic': {'type': 'string'},
                                'category': {'type': 'string', 'enum': list(CATEGORIES)},
                                'primary': {'type': 'boolean'},
                                'evidence': {'type': 'string'},
                            },
                            'required': ['topic', 'category', 'primary', 'evidence'],
                            'additionalProperties': False,
                        },
                    },
                },
                'required': ['id', 'topics'],
                'additionalProperties': False,
            },
        },
    },
    'required': ['results'],
    'additionalProperties': False,
}


# ------------------------------------------------------------------
# TEXT AND REQUESTS
# ------------------------------------------------------------------

def build_text(show: str, title: str, description: str) -> str | None:
    """What the model reads for one episode: show, title, cleaned
    description (capped). None when there is nothing to read."""
    desc = clean_description(description or '', max_chars=TEXT_MAX) or ''
    desc = re.sub(r'\s+', ' ', desc).strip()
    title = re.sub(r'\s+', ' ', title or '').strip()
    if not desc and not title:
        return None
    return f"[Show: {show}] {title}\n{desc}".strip()


def item_id(episode_id: int) -> str:
    return f'e{episode_id}'


def _xml_escape(text: str) -> str:
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def render_items(items: list) -> str:
    return '\n'.join(f'<item id="{it["id"]}">{_xml_escape(it["text"])}</item>' for it in items)


def system_prompt(existing: list) -> str:
    return SYSTEM_PROMPT.format(categories='; '.join(CATEGORIES),
                                existing=', '.join(existing) if existing else '(none yet)')


def chunk(items: list, size: int = ITEMS_PER_REQUEST) -> list:
    return [items[i:i + size] for i in range(0, len(items), size)]


def request_params(items: list, existing: list, model: str = MODEL) -> dict:
    return {
        **MODELS[model]['params'],
        'model': model,
        'max_tokens': MAX_TOKENS,
        'system': system_prompt(existing),
        'messages': [{'role': 'user', 'content': render_items(items)}],
        'output_config': {'format': {'type': 'json_schema', 'schema': OUTPUT_SCHEMA},
                          **MODELS[model].get('output_config', {})},
    }


def estimate_cost(items: list, existing: list, batch: bool = True, model: str = MODEL) -> dict:
    requests = chunk(items)
    system_tokens = len(system_prompt(existing)) // CHARS_PER_TOKEN + EST_SCHEMA_TOKENS
    input_tokens = system_tokens * len(requests) + sum(len(it['text']) // CHARS_PER_TOKEN + 10 for it in items)
    output_tokens = EST_OUTPUT_TOKENS_PER_ITEM * len(items)
    dollars = usage_cost(input_tokens, output_tokens, batch, model) * MODELS[model]['estimate_factor']
    return {'requests': len(requests), 'input_tokens': input_tokens,
            'output_tokens': output_tokens, 'dollars': round(dollars, 2)}


# ------------------------------------------------------------------
# CHECKING ANSWERS
# ------------------------------------------------------------------

def verified_topics(topics: list, text: str) -> tuple:
    """Keep topics whose evidence occurs verbatim (whole words) in the text,
    whose category is one of the fixed ones, and whose name normalises to
    something. One primary at most (the first marked); duplicates by key
    collapse to the first. Returns (kept, dropped) — dropped as (topic,
    reason) pairs."""
    haystack = _normalise_for_match(text)
    kept, dropped, seen = [], [], set()
    for t in topics or []:
        name = re.sub(r'\s+', ' ', (t.get('topic') or '')).strip().strip('.')
        evidence = (t.get('evidence') or '').strip()
        key = normalize_topic(name)
        if not key or len(name) > MAX_TOPIC_LEN:
            dropped.append((name, 'empty or too long'))
            continue
        if t.get('category') not in CATEGORIES:
            dropped.append((name, 'unknown category'))
            continue
        if not evidence or not re.search(
                r'(?<!\w)' + re.escape(_normalise_for_match(evidence)) + r'(?!\w)', haystack):
            dropped.append((name, 'evidence not in text'))
            continue
        if key in seen:
            continue
        seen.add(key)
        kept.append({'topic': name, 'key': key, 'category': t['category'],
                     'primary': bool(t.get('primary')), 'evidence': evidence})
    primaries = [k for k in kept if k['primary']]
    for k in primaries[1:]:
        k['primary'] = False
    if kept and not primaries:
        kept[0]['primary'] = True
    return kept[:MAX_TOPICS], dropped


def parse_response_text(text: str, expected_ids: set) -> dict:
    """{item id: [topic dicts]} for the ids this request was sent."""
    out = {}
    for result in json.loads(text).get('results', []):
        rid = result.get('id')
        if rid in expected_ids and rid not in out:
            out[rid] = result.get('topics') or []
    return out


# ------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------

def existing_topic_names(conn, limit: int = PROMPT_TAG_LIMIT) -> list:
    """The most-used canonical tag names, for the model to reuse."""
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('topic_extractions') IS NOT NULL")
    if not cur.fetchone()[0]:
        return []
    cur.execute("""
        SELECT t.name FROM tags t JOIN episode_tag et ON et.tag_id = t.tag_id
        WHERE NOT t.not_a_topic GROUP BY t.tag_id, t.name
        ORDER BY COUNT(*) DESC, t.name LIMIT %s
    """, (limit,))
    names = [r[0] for r in cur.fetchall()]
    cur.close()
    return names


def get_episodes_to_process(conn, limit: int = None, random_sample: bool = False,
                            guests_only: bool = False) -> list:
    """Episodes not yet processed (plus retries). guests_only limits to
    episodes with a guest credit — the pilot's sample."""
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('topic_extractions') IS NOT NULL")
    migrated = cur.fetchone()[0]
    join = "LEFT JOIN topic_extractions tx ON tx.episode_id = e.episode_id" if migrated else ''
    unprocessed = ("AND (tx.episode_id IS NULL OR (tx.status = 'retry' AND tx.attempts < %(max)s))"
                   if migrated else '')
    guests = ("AND EXISTS (SELECT 1 FROM episode_host eh WHERE eh.episode_id = e.episode_id AND eh.is_guest)"
              if guests_only else '')
    cur.execute(f"""
        SELECT e.episode_id, p.title, e.title, e.description
        FROM episodes e JOIN podcasts p ON p.podcast_id = e.podcast_id
        {join}
        WHERE TRUE {unprocessed} {guests}
        ORDER BY {'random()' if random_sample else 'e.episode_id'}
        {'LIMIT %(limit)s' if limit else ''}
    """, {'max': MAX_ATTEMPTS, 'limit': limit})
    rows = cur.fetchall()
    cur.close()
    return [{'episode_id': r[0], 'show': r[1], 'title': r[2], 'description': r[3]} for r in rows]


def build_items(episodes: list) -> tuple:
    """(empty, items): episodes with nothing to read, and items to send."""
    empty, items = [], []
    for ep in episodes:
        text = build_text(ep['show'], ep['title'], ep['description'])
        if text is None:
            empty.append(ep)
        else:
            items.append({'id': item_id(ep['episode_id']), 'episode_id': ep['episode_id'],
                          'text': text, 'show': ep['show'], 'title': ep['title']})
    return empty, items


def _tag_ids(cur, kept: list) -> dict:
    """{normalized key: tag_id}, creating tags (and their first alias) for
    keys not seen before."""
    keys = list({k['key'] for k in kept})
    if not keys:
        return {}
    cur.execute("SELECT normalized_name, tag_id FROM tag_aliases WHERE normalized_name = ANY(%s)", (keys,))
    found = dict(cur.fetchall())
    for k in kept:
        if k['key'] in found:
            continue
        cur.execute("""
            INSERT INTO tags (name, slug, category) VALUES (%s, %s, %s)
            ON CONFLICT (slug) DO UPDATE SET slug = EXCLUDED.slug
            RETURNING tag_id
        """, (k['topic'], topic_slug(k['topic']), k['category']))
        tag_id = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO tag_aliases (tag_id, alias_name, normalized_name) VALUES (%s, %s, %s)
            ON CONFLICT (normalized_name) DO NOTHING
        """, (tag_id, k['topic'], k['key']))
        found[k['key']] = tag_id
    return found


def record_results(cur, pending: list, answers: dict) -> tuple:
    """Write answers for (episode_id, text_sent) rows. Does not commit.
    Episodes with no answer go to 'retry'. Manual tags are never touched.
    Returns (done, tags added, retried, dropped)."""
    done, retry, rows, dropped_total = [], [], [], 0
    for episode_id, text in pending:
        key = item_id(episode_id)
        if key not in answers:
            retry.append((episode_id,))
            continue
        kept, dropped = verified_topics(answers[key], text)
        dropped_total += len(dropped)
        ids = _tag_ids(cur, kept)
        done.append((episode_id,))
        rows.extend((episode_id, ids[k['key']], k['primary'], k['evidence'], DATA_SOURCE) for k in kept)
    if done:
        execute_values(cur, """
            DELETE FROM episode_tag et USING (VALUES %s) AS d(episode_id)
            WHERE et.episode_id = d.episode_id AND et.data_source <> 'manual'
        """, done)
        execute_values(cur, """
            UPDATE topic_extractions tx SET status = 'done', completed_at = now()
            FROM (VALUES %s) AS d(episode_id) WHERE tx.episode_id = d.episode_id
        """, done)
    if rows:
        execute_values(cur, """
            INSERT INTO episode_tag (episode_id, tag_id, is_primary, evidence, data_source)
            VALUES %s ON CONFLICT DO NOTHING
        """, rows)
    if retry:
        execute_values(cur, """
            UPDATE topic_extractions tx SET status = 'retry'
            FROM (VALUES %s) AS d(episode_id) WHERE tx.episode_id = d.episode_id
        """, retry)
    return len(done), len(rows), len(retry), dropped_total


def _upsert_extractions(cur, rows: list):
    """rows: (episode_id, status, text_sent, batch_id, model)."""
    execute_values(cur, """
        INSERT INTO topic_extractions (episode_id, status, text_sent, batch_id, model, attempts)
        VALUES %s
        ON CONFLICT (episode_id) DO UPDATE SET
            status = EXCLUDED.status, text_sent = EXCLUDED.text_sent,
            batch_id = EXCLUDED.batch_id, model = EXCLUDED.model,
            attempts = topic_extractions.attempts + 1,
            completed_at = CASE WHEN EXCLUDED.status = 'empty' THEN now() END
    """, [r + (1,) for r in rows], template='(%s, %s, %s, %s, %s, %s)')


# ------------------------------------------------------------------
# COMMANDS
# ------------------------------------------------------------------

def _report(episodes, empty, items, est):
    logger.info(f"Episodes to process: {len(episodes)}")
    logger.info(f"  nothing to read (no API call): {len(empty)}")
    logger.info(f"  items to send: {len(items)} in {est['requests']} requests")
    logger.info(f"Estimated tokens: {est['input_tokens']:,} in, {est['output_tokens']:,} out "
                f"(approximate); estimated cost at batch price: ${est['dollars']:.2f}")


def cmd_estimate(limit=None, model=MODEL):
    conn = psycopg2.connect(DB)
    try:
        episodes = get_episodes_to_process(conn, limit)
        empty, items = build_items(episodes)
        _report(episodes, empty, items, estimate_cost(items, existing_topic_names(conn), model=model))
    finally:
        conn.close()


def cmd_pilot(limit: int, out_path: str, model: str = MODEL):
    """A random sample of guest episodes, normal (non-batch) calls, written
    to CSV for checking by hand. Nothing is written to the database."""
    import anthropic

    conn = psycopg2.connect(DB)
    episodes = get_episodes_to_process(conn, limit, random_sample=True, guests_only=True)
    existing = existing_topic_names(conn)
    conn.close()
    empty, items = build_items(episodes)
    _report(episodes, empty, items, estimate_cost(items, existing, batch=False, model=model))

    client = anthropic.Anthropic()
    answers, tokens_in, tokens_out, failed = {}, 0, 0, 0
    for group in chunk(items):
        message = client.messages.create(**request_params(group, existing, model))
        tokens_in += message.usage.input_tokens
        tokens_out += message.usage.output_tokens
        text = message_text(message)
        if text is None:
            failed += len(group)
            logger.warning(f"Request stopped with {message.stop_reason}; {len(group)} items skipped")
            continue
        answers.update(parse_response_text(text, {it['id'] for it in group}))

    keys, dropped_total, with_topics = {}, 0, 0
    with open(out_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['episode_id', 'podcast', 'episode', 'topic', 'normalized', 'category',
                         'primary', 'evidence', 'dropped', 'text'])
        for it in items:
            kept, dropped = verified_topics(answers.get(it['id'], []), it['text'])
            dropped_total += len(dropped)
            with_topics += bool(kept)
            dropped_s = '; '.join(f'{n} ({why})' for n, why in dropped)
            for k in kept or [{'topic': '', 'key': '', 'category': '', 'primary': False, 'evidence': ''}]:
                if k['key']:
                    keys.setdefault(k['key'], set()).add(k['topic'])
                writer.writerow([it['episode_id'], it['show'], it['title'], k['topic'], k['key'],
                                 k['category'], 'yes' if k['primary'] else '', k['evidence'],
                                 dropped_s, it['text']])
    merged = sum(1 for v in keys.values() if len(v) > 1)
    logger.info(f"Episodes with topics: {with_topics}/{len(items)}; distinct topics: {len(keys)} "
                f"({merged} keys joined more than one spelling); dropped: {dropped_total}; "
                f"failed: {failed}")
    logger.info(f"Actual usage: {tokens_in:,} in, {tokens_out:,} out = "
                f"${usage_cost(tokens_in, tokens_out, False, model):.4f} normal "
                f"(${usage_cost(tokens_in, tokens_out, True, model):.4f} batch) on {model}")
    logger.info(f"Wrote {out_path}")


def _send(conn, model, max_cost, dry_run, over_cap_is_error, limit=None):
    from anthropic import Anthropic
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    episodes = get_episodes_to_process(conn, limit)
    existing = existing_topic_names(conn)
    empty, items = build_items(episodes)
    est = estimate_cost(items, existing, model=model)
    _report(episodes, empty, items, est)
    if dry_run:
        return
    if est['dollars'] > max_cost:
        message = (f"Estimated ${est['dollars']:.2f} is over --max-cost ${max_cost:.2f}; "
                   f"nothing submitted. Lower --limit or raise --max-cost.")
        if over_cap_is_error:
            sys.exit(message)
        logger.warning(message)
        return
    cur = conn.cursor()
    if empty:
        _upsert_extractions(cur, [(e['episode_id'], 'empty', None, None, None) for e in empty])
        conn.commit()
    if not items:
        logger.info("Nothing to send.")
        return
    batch = Anthropic().messages.batches.create(requests=[
        Request(custom_id=f'req-{i}', params=MessageCreateParamsNonStreaming(**request_params(g, existing, model)))
        for i, g in enumerate(chunk(items))
    ])
    logger.info(f"Submitted batch {batch.id}")
    _upsert_extractions(cur, [(it['episode_id'], 'pending', it['text'], batch.id, model) for it in items])
    conn.commit()
    cur.close()


def cmd_submit(limit=None, max_cost=25.0, dry_run=False, model=MODEL, over_cap_is_error=True):
    conn = psycopg2.connect(DB)
    try:
        _send(conn, model, max_cost, dry_run, over_cap_is_error, limit)
    finally:
        conn.close()


def cmd_collect():
    from anthropic import Anthropic

    client = Anthropic()
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT batch_id FROM topic_extractions
                   WHERE status = 'pending' AND batch_id IS NOT NULL AND batch_id NOT LIKE 'manual-%%'""")
    for (batch_id,) in cur.fetchall():
        batch = client.messages.batches.retrieve(batch_id)
        if batch.processing_status != 'ended':
            logger.info(f"Batch {batch_id}: {batch.processing_status}")
            continue
        cur.execute("SELECT episode_id, text_sent, model FROM topic_extractions "
                    "WHERE status = 'pending' AND batch_id = %s", (batch_id,))
        rows = cur.fetchall()
        model = rows[0][2] if rows and rows[0][2] in MODELS else MODEL
        pending = [(r[0], r[1]) for r in rows]
        expected = {item_id(e) for e, _ in pending}
        answers, tin, tout = {}, 0, 0
        for result in client.messages.batches.results(batch_id):
            if result.result.type != 'succeeded':
                logger.warning(f"{batch_id}/{result.custom_id}: {result.result.type}")
                continue
            message = result.result.message
            tin += message.usage.input_tokens
            tout += message.usage.output_tokens
            text = message_text(message)
            if text is None:
                logger.warning(f"{batch_id}/{result.custom_id}: stopped with {message.stop_reason}")
                continue
            answers.update(parse_response_text(text, expected))
        done, added, retry, dropped = record_results(cur, pending, answers)
        conn.commit()
        logger.info(f"Batch {batch_id}: {done} episodes, {added} tags, {retry} to retry, "
                    f"{dropped} dropped; ${usage_cost(tin, tout, True, model):.4f}")
    cur.close()
    conn.close()


def cmd_export(out_dir: str, limit=None, batch_id=None):
    """Items to read by hand (no API): request-sized files, rows marked
    pending under a manual-... batch id. See extract_affiliations.cmd_export."""
    from datetime import datetime
    batch_id = batch_id or 'manual-' + datetime.now().strftime('%Y%m%d-%H%M')
    conn = psycopg2.connect(DB)
    try:
        episodes = get_episodes_to_process(conn, limit)
        empty, items = build_items(episodes)
        cur = conn.cursor()
        if empty:
            _upsert_extractions(cur, [(e['episode_id'], 'empty', None, None, None) for e in empty])
        _upsert_extractions(cur, [(it['episode_id'], 'pending', it['text'], batch_id, 'claude-code')
                                  for it in items])
        conn.commit()
        os.makedirs(out_dir, exist_ok=True)
        for i, group in enumerate(chunk(items)):
            with open(os.path.join(out_dir, f'items_{i:03d}.xml'), 'w') as f:
                f.write(render_items(group) + '\n')
        logger.info(f"{batch_id}: {len(items)} items under {out_dir}")
    finally:
        conn.close()


def cmd_import(batch_id: str, paths: list):
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT episode_id, text_sent FROM topic_extractions WHERE status = 'pending' AND batch_id = %s",
                (batch_id,))
    pending = cur.fetchall()
    expected = {item_id(e) for e, _ in pending}
    answers = {}
    for p in paths:
        with open(p) as f:
            answers.update(parse_response_text(f.read(), expected))
    answered = [row for row in pending if item_id(row[0]) in answers]
    done, added, _, dropped = record_results(cur, answered, answers)
    conn.commit()
    conn.close()
    logger.info(f"{batch_id}: {done} episodes, {added} tags, {dropped} dropped; "
                f"{len(pending) - len(answered)} still pending")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    model_arg = dict(choices=sorted(MODELS), default=MODEL)

    p = sub.add_parser('estimate')
    p.add_argument('--limit', type=int)
    p.add_argument('--model', **model_arg)
    p = sub.add_parser('pilot')
    p.add_argument('--limit', type=int, default=200)
    p.add_argument('--out', default='topic_pilot.csv')
    p.add_argument('--model', **model_arg)
    for name in ('submit', 'run'):
        p = sub.add_parser(name)
        p.add_argument('--limit', type=int)
        p.add_argument('--max-cost', type=float, default=25.0 if name == 'submit' else 2.0)
        p.add_argument('--dry-run', action='store_true')
        p.add_argument('--model', **model_arg)
    sub.add_parser('collect')
    p = sub.add_parser('export')
    p.add_argument('--out', required=True)
    p.add_argument('--limit', type=int)
    p.add_argument('--batch-id')
    p = sub.add_parser('import')
    p.add_argument('--batch-id', required=True)
    p.add_argument('answers', nargs='+')

    a = parser.parse_args()
    if a.command == 'estimate':
        cmd_estimate(a.limit, a.model)
    elif a.command == 'pilot':
        cmd_pilot(a.limit, a.out, a.model)
    elif a.command == 'submit':
        cmd_submit(a.limit, a.max_cost, a.dry_run, a.model)
    elif a.command == 'collect':
        cmd_collect()
    elif a.command == 'run':
        cmd_collect()
        cmd_submit(a.limit, a.max_cost, a.dry_run, a.model, over_cap_is_error=False)
    elif a.command == 'export':
        cmd_export(a.out, a.limit, a.batch_id)
    elif a.command == 'import':
        cmd_import(a.batch_id, a.answers)


if __name__ == '__main__':
    main()
