#!/usr/bin/env python3
"""
Apply a reviewed topic decisions CSV: merges, renames, not-a-topic and
company marks, broad topics and parents, in one transaction.

The CSV has columns tag_id, name, action, into, rename, and optionally
category and parent:
  keep         nothing, unless `rename` or `parent` is set
  merge        fold this tag into the tag named in `into` (a chain of merges
               is followed to its end); a `rename` on a merge row renames
               the survivor
  not_a_topic  hide it everywhere (it is not a subject: a show format, ...)
  company      mark it a company tag, linked to the organisation whose
               spelling matches, if any
  broad        make it a broad topic filed under `category`; with no
               tag_id, a new tag is created for it
`parent` names the row this topic sits under (a broad topic, or a topic
for a narrower one); parents may not loop.

Every row is checked against the database first (the tag still exists under
that name), so a CSV reviewed earlier cannot act on tags that have changed
since. Dry run by default; --apply writes, after saving a JSON snapshot of
every touched tags / tag_aliases / episode_tag row to --snapshot. The dry
run carries out every change and rolls it back, so it fails the same way
--apply would.

    python3 topic_cleanup.py decisions.csv [more.csv ...]
    python3 topic_cleanup.py decisions.csv --apply --snapshot undo.json
"""

import argparse
import csv
import json
import os
import sys

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'backend'))
from org_names import normalize_org_name  # noqa: E402
from topic_merge import merge_tags, rename_conflict, rename_tag  # noqa: E402
from topic_names import CATEGORIES, normalize_topic, topic_slug  # noqa: E402

DB = os.getenv('DATABASE_URL', 'postgresql://localhost/podcast_db')


def load_decisions(paths: list) -> list:
    """Rows as dicts; a new broad topic (no tag_id) gets a negative
    placeholder id until it is created."""
    rows, new = [], 0
    for path in paths:
        with open(path, newline='') as f:
            for r in csv.DictReader(f):
                tag_id = (r.get('tag_id') or '').strip()
                if not tag_id:
                    new -= 1
                rows.append({'tag_id': int(tag_id) if tag_id else new, 'name': r['name'].strip(),
                             'action': (r.get('action') or 'keep').strip(),
                             'into': (r.get('into') or '').strip(), 'rename': (r.get('rename') or '').strip(),
                             'category': (r.get('category') or '').strip(),
                             'parent': (r.get('parent') or '').strip()})
    return rows


def plan(rows: list) -> dict:
    """Resolve names to ids and merge chains to their final survivor.
    Returns {'merges': [(keep, drop)], 'renames': {tag_id: name},
    'not_a_topic': [ids], 'company': [ids], 'broad': {tag_id: category},
    'parents': {tag_id: parent_id}} (a negative id is a broad topic still
    to be created); raises ValueError on a decision that can't be carried
    out."""
    by_name, by_id = {}, {}
    for r in rows:
        if r['name'] in by_name and by_name[r['name']]['tag_id'] != r['tag_id']:
            raise ValueError(f"two rows named {r['name']!r}")
        if r['tag_id'] in by_id and by_id[r['tag_id']] != r:
            raise ValueError(f"conflicting rows for tag {r['tag_id']}")
        by_name[r['name']] = by_id[r['tag_id']] = r

    def final(tag_id, seen=()):
        r = by_id[tag_id]
        if r['action'] != 'merge':
            return tag_id
        if tag_id in seen:
            raise ValueError(f"merge cycle through {r['name']!r}")
        if r['into'] not in by_name:
            raise ValueError(f"{r['name']!r} merges into unknown {r['into']!r}")
        return final(by_name[r['into']]['tag_id'], seen + (tag_id,))

    out = {'merges': [], 'renames': {}, 'not_a_topic': [], 'company': [], 'broad': {}, 'parents': {}}
    for r in by_id.values():
        if r['tag_id'] < 0 and r['action'] != 'broad':
            raise ValueError(f"{r['name']!r} has no tag_id but isn't a new broad topic")
        if r['action'] == 'merge':
            if r['parent']:
                raise ValueError(f"{r['name']!r} is merged away; give its survivor the parent instead")
            out['merges'].append((final(r['tag_id']), r['tag_id']))
        elif r['action'] in ('not_a_topic', 'company'):
            out[r['action']].append(r['tag_id'])
        elif r['action'] == 'broad':
            if r['category'] not in CATEGORIES:
                raise ValueError(f"broad topic {r['name']!r} needs one of the categories, not {r['category']!r}")
            out['broad'][r['tag_id']] = r['category']
        elif r['action'] != 'keep':
            raise ValueError(f"unknown action {r['action']!r} for {r['name']!r}")
        if r['parent']:
            if r['parent'] not in by_name:
                raise ValueError(f"{r['name']!r} sits under unknown {r['parent']!r}")
            parent = final(by_name[r['parent']]['tag_id'])
            if parent == r['tag_id']:
                raise ValueError(f"{r['name']!r} can't sit under itself")
            out['parents'][r['tag_id']] = parent
        if r['rename']:
            target = final(r['tag_id'])
            if out['renames'].get(target, r['rename']) != r['rename']:
                raise ValueError(f"two renames for {by_id[target]['name']!r}")
            out['renames'][target] = r['rename']
    for start in out['parents']:
        seen, node = {start}, out['parents'][start]
        while node in out['parents']:
            if node in seen:
                raise ValueError(f"parents loop through {by_id[node]['name']!r}")
            seen.add(node)
            node = out['parents'][node]
    return out


def check_against_db(cur, rows: list):
    rows = [r for r in rows if r['tag_id'] > 0]
    ids = [r['tag_id'] for r in rows]
    cur.execute("SELECT tag_id, name FROM tags WHERE tag_id = ANY(%s)", (ids,))
    current = {r['tag_id']: r['name'] for r in cur.fetchall()}
    problems = [f"#{r['tag_id']} {r['name']!r}: " + ('gone' if r['tag_id'] not in current
                                                     else f"now {current[r['tag_id']]!r}")
                for r in rows if current.get(r['tag_id']) != r['name']]
    if problems:
        raise SystemExit('The CSV no longer matches the database:\n  ' + '\n  '.join(problems))


def apply(cur, p: dict, names: dict) -> dict:
    ids = {}  # placeholder id -> created tag
    for tag_id, category in p['broad'].items():
        if tag_id > 0:
            cur.execute("UPDATE tags SET is_broad = true, category = %s WHERE tag_id = %s", (category, tag_id))
            continue
        name = names[tag_id]
        other = rename_conflict(cur, 0, name)
        if other:
            raise SystemExit(f"Can't create broad topic {name!r}: that is already #{other['tag_id']} {other['name']!r}")
        cur.execute("INSERT INTO tags (name, slug, category, is_broad) VALUES (%s, %s, %s, true) RETURNING tag_id",
                    (name, topic_slug(name), category))
        ids[tag_id] = cur.fetchone()['tag_id']
        cur.execute("INSERT INTO tag_aliases (tag_id, alias_name, normalized_name) VALUES (%s, %s, %s)",
                    (ids[tag_id], name, normalize_topic(name)))
    for keep, drop in p['merges']:
        merge_tags(cur, keep, drop)
    for tag_id, name in p['renames'].items():
        other = rename_conflict(cur, tag_id, name)
        if other:
            raise SystemExit(f"Can't rename {names[tag_id]!r} to {name!r}: that is already "
                             f"#{other['tag_id']} {other['name']!r}")
        rename_tag(cur, tag_id, name)
    if p['not_a_topic']:
        cur.execute("UPDATE tags SET not_a_topic = true WHERE tag_id = ANY(%s)", (p['not_a_topic'],))
    linked = {}
    for tag_id in p['company']:
        cur.execute("""SELECT a.org_id FROM organization_aliases a JOIN organizations o ON o.org_id = a.org_id
                       WHERE a.normalized_name = %s AND NOT o.not_an_org""", (normalize_org_name(names[tag_id]),))
        org = cur.fetchone()
        linked[tag_id] = org['org_id'] if org else None
        cur.execute("UPDATE tags SET is_company = true, org_id = COALESCE(org_id, %s) WHERE tag_id = %s",
                    (linked[tag_id], tag_id))
    if p['parents']:
        pairs = [(ids.get(c, c), ids.get(par, par)) for c, par in p['parents'].items()]
        execute_values(cur, """UPDATE tags t SET parent_tag_id = v.parent FROM (VALUES %s) AS v(child, parent)
                               WHERE t.tag_id = v.child""", pairs, page_size=5000)
    return linked


def snapshot(cur, tag_ids: list) -> dict:
    out = {}
    for table in ('tags', 'tag_aliases', 'episode_tag'):
        cur.execute(f"SELECT * FROM {table} WHERE tag_id = ANY(%s)", (tag_ids,))
        out[table] = cur.fetchall()
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('csv', nargs='+')
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--snapshot', help='where to write the undo snapshot (required with --apply)')
    args = ap.parse_args()
    if args.apply and not args.snapshot:
        ap.error('--apply needs --snapshot')

    rows = load_decisions(args.csv)
    try:
        p = plan(rows)
    except ValueError as e:
        raise SystemExit(f'Bad decisions: {e}')
    names = {r['tag_id']: r['name'] for r in rows}
    new_broad = [t for t in p['broad'] if t < 0]

    conn = psycopg2.connect(DB)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    check_against_db(cur, rows)
    touched = sorted({t for m in p['merges'] for t in m} | set(p['renames']) | set(p['not_a_topic'])
                     | set(p['company']) | set(p['broad']) | set(p['parents']) | set(p['parents'].values()))
    touched = [t for t in touched if t > 0]
    cur.execute("SELECT tag_id, count(*) AS n FROM episode_tag WHERE tag_id = ANY(%s) GROUP BY 1", (touched,))
    eps = {r['tag_id']: r['n'] for r in cur.fetchall()}
    print(f"{len(p['merges'])} merges, {len(p['renames'])} renames, "
          f"{len(p['not_a_topic'])} not a topic, {len(p['company'])} companies, "
          f"{len(p['broad'])} broad topics ({len(new_broad)} new), {len(p['parents'])} parents")
    for keep, drop in sorted(p['merges'], key=lambda m: names[m[0]].lower()):
        print(f"  merge   {names[drop]} ({eps.get(drop, 0)})  ->  {names[keep]} ({eps.get(keep, 0)})")
    for tag_id, name in p['renames'].items():
        print(f"  rename  {names[tag_id]}  ->  {name}")
    for tag_id in p['not_a_topic']:
        print(f"  hide    {names[tag_id]} ({eps.get(tag_id, 0)})")
    for tag_id in p['company']:
        print(f"  company {names[tag_id]} ({eps.get(tag_id, 0)})")
    for tag_id, category in p['broad'].items():
        print(f"  broad   {names[tag_id]} ({category}{', new' if tag_id < 0 else ''})")
    if args.apply:
        with open(args.snapshot, 'w') as f:
            json.dump(snapshot(cur, touched), f, default=str)
        print('snapshot', args.snapshot)
    cur.execute("SET lock_timeout = '10s'")
    # the dry run makes every change too, then rolls back, so a clash shows up before --apply
    linked = apply(cur, p, names)
    for tag_id, org_id in linked.items():
        print(f"  {names[tag_id]}: " + (f"linked to organisation #{org_id}" if org_id else "no organisation found"))
    cur.execute("SELECT count(*) AS n FROM tags")
    print('tags after:', cur.fetchone()['n'])
    if args.apply:
        conn.commit()
        print('applied')
    else:
        conn.rollback()
        print('Dry run; rolled back, nothing written.')


if __name__ == '__main__':
    main()
