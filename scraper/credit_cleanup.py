"""
Remove guest credits that don't mean a person was on an episode. Runs after
every scrape (see .github/workflows/scrape.yml); safe to re-run.

Two kinds, both reviewed by hand in Sep 2026 before being automated:

  replay          — a rebroadcast ("REWIND: …", "ENCORE", "Best of 2025",
                    "[re-published]") of a conversation the person is already
                    credited with on the same show. The earliest credit stays,
                    so a replay no longer counts as a second appearance.
  other episode   — the role extractor found the person only in a reference
                    to a different episode ("Past episodes you'll love: Jane
                    Doe on …", "Ep227: John Pettigrew"), not taking part in
                    this one. 26 of 27 sampled were false credits; the one
                    exception was a replayed interview, which is kept.

Only inferred credits (data_source parsed_*) are touched — Apple's own labels
and anything added by hand never are. Every removal is recorded in
credit_suppressions, so the name scanner cannot put it back.

    python3 credit_cleanup.py run [--dry-run]
"""

import argparse
import logging
import os
import re
from datetime import date

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB = os.getenv('DATABASE_URL', 'postgresql://localhost/podcast_db')

# Rerun wording in an episode title. Not "revisited": that is as often a new
# conversation about an old topic.
RERUN_RE = re.compile(
    r"\b(rewind|encore|re-?runs?|replay(ed)?|re-?air(ed|ing)?|rebroadcast|best of|classic episode|"
    r"from the (vault|archives?)|repeat|re-?releas(e|ed)|re-?publish(ed)?|throwback)\b", re.I)
# The same wording, for spotting a replay in the text the extractor read.
RERUN_TEXT_RE = re.compile(RERUN_RE.pattern + r"|listen to this (great )?interview", re.I)


def normalised_title(title: str | None) -> str:
    """A title with its rerun wording and bracketed notes removed, so
    "REWIND: Firefight" matches "Firefight". Part and episode numbers are
    kept: "Kelp Farming (Part II)" is new material, not a replay."""
    t = RERUN_RE.sub(' ', title or '')
    t = re.sub(r'\[(?![^\]]*\bpart\b)[^\]]*\]|\((?![^)]*\bpart\b)[^)]*\)', ' ', t, flags=re.I)
    return re.sub(r'[^a-z0-9]+', ' ', t.lower()).strip()


def find_replays(rows: list) -> list:
    """rows: guest credits as dicts with episode_id, host_id, podcast_id,
    title, published_date, data_source. Returns the (episode_id, host_id)
    credits that replay an earlier credit of the same person on the same show."""
    by_show = {}
    for r in rows:
        by_show.setdefault((r['host_id'], r['podcast_id']), []).append(r)
    replays = []
    for credits in by_show.values():
        if len(credits) < 2:
            continue
        credits.sort(key=lambda r: (r['published_date'] or date.min, r['episode_id']))
        earlier_titles = set()
        for i, r in enumerate(credits):
            title_key = normalised_title(r['title'])
            if i > 0 and r['data_source'].startswith('parsed') and (
                    RERUN_RE.search(r['title'] or '')
                    or (len(title_key) > 12 and title_key in earlier_titles)):
                replays.append((r['episode_id'], r['host_id']))
            else:
                earlier_titles.add(title_key)
    return replays


def plan(cur) -> dict:
    cur.execute("""
        SELECT eh.episode_id, eh.host_id, e.podcast_id, e.title, e.published_date, eh.data_source
        FROM episode_host eh JOIN episodes e ON e.episode_id = eh.episode_id
        WHERE eh.is_guest
    """)
    cols = [d[0] for d in cur.description]
    replays = find_replays([dict(zip(cols, r)) for r in cur.fetchall()])

    cur.execute("""
        SELECT eh.episode_id, eh.host_id, e.title, ax.snippet
        FROM episode_host eh
        JOIN affiliation_extractions ax ON ax.episode_id = eh.episode_id AND ax.host_id = eh.host_id
        JOIN episodes e ON e.episode_id = eh.episode_id
        WHERE eh.is_guest AND eh.data_source LIKE 'parsed%%'
          AND ax.from_other_episode AND ax.appears_on_episode = false
    """)
    other = [(e, h) for e, h, title, snippet in cur.fetchall()
             if not (RERUN_TEXT_RE.search(title or '') or RERUN_TEXT_RE.search(snippet or ''))]
    replay_set = set(replays)
    return {'replay': replays, 'refers to another episode': [p for p in other if p not in replay_set]}


def apply(cur, planned: dict) -> dict:
    counts = {}
    for reason, pairs in planned.items():
        if not pairs:
            counts[reason] = 0
            continue
        execute_values(cur, """
            INSERT INTO credit_suppressions (episode_id, host_id, reason) VALUES %s
            ON CONFLICT (episode_id, host_id) DO NOTHING
        """, [(e, h, reason) for e, h in pairs])
        cur.execute("""
            DELETE FROM episode_host eh USING unnest(%s::int[], %s::int[]) AS d(episode_id, host_id)
            WHERE eh.episode_id = d.episode_id AND eh.host_id = d.host_id
        """, ([e for e, _ in pairs], [h for _, h in pairs]))
        counts[reason] = cur.rowcount
    return counts


def run(dry_run: bool = False) -> dict:
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    try:
        cur.execute("SET lock_timeout = '10s'")
        planned = plan(cur)
        for reason, pairs in planned.items():
            logger.info(f"{reason}: {len(pairs)} guest credits")
        if dry_run:
            conn.rollback()
            return {k: len(v) for k, v in planned.items()}
        counts = apply(cur, planned)
        conn.commit()
        logger.info(f"Removed: {counts}")
        return counts
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('command', choices=['run'])
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
