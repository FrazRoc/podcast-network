"""Merging and renaming topics: shared by Topic Admin (main.py) and the bulk
cleanup (scraper/topic_cleanup.py), so both move episodes and spellings the
same way."""

from topic_names import normalize_topic, topic_slug


def merge_tags(cur, keep_id: int, drop_id: int) -> dict:
    """Fold one tag into another: its spellings become the survivor's (so the
    tagger files future mentions under the survivor) and its episodes move
    across. An episode on both keeps one row, the main topic if either was.
    A company or person link the survivor lacks is carried over."""
    cur.execute("""
        INSERT INTO episode_tag (episode_id, tag_id, is_primary, evidence, data_source)
        SELECT episode_id, %s, is_primary, evidence, data_source FROM episode_tag WHERE tag_id = %s
        ON CONFLICT (episode_id, tag_id) DO UPDATE
            SET is_primary = episode_tag.is_primary OR EXCLUDED.is_primary
    """, (keep_id, drop_id))
    episodes_moved = cur.rowcount
    cur.execute("""
        UPDATE tags k SET org_id = COALESCE(k.org_id, d.org_id), host_id = COALESCE(k.host_id, d.host_id)
        FROM tags d WHERE k.tag_id = %s AND d.tag_id = %s
    """, (keep_id, drop_id))
    cur.execute("UPDATE tag_aliases SET tag_id = %s WHERE tag_id = %s", (keep_id, drop_id))
    aliases_moved = cur.rowcount
    cur.execute("DELETE FROM tags WHERE tag_id = %s", (drop_id,))  # cascades its episode_tag rows
    return {"episodes_moved": episodes_moved, "aliases_moved": aliases_moved}


def rename_conflict(cur, tag_id: int, name: str):
    """The other tag (tag_id, name) that already has this spelling, or None."""
    cur.execute("""SELECT t.tag_id, t.name FROM tag_aliases a JOIN tags t ON t.tag_id = a.tag_id
                   WHERE a.normalized_name = %s AND a.tag_id <> %s""", (normalize_topic(name), tag_id))
    other = cur.fetchone()
    if not other:
        cur.execute("SELECT tag_id, name FROM tags WHERE (slug = %s OR name = %s) AND tag_id <> %s",
                    (topic_slug(name), name, tag_id))
        other = cur.fetchone()
    return other


def rename_tag(cur, tag_id: int, name: str):
    """Rename a tag and record the new spelling (the old one stays an alias).
    Call rename_conflict() first."""
    cur.execute("UPDATE tags SET name = %s, slug = %s WHERE tag_id = %s", (name, topic_slug(name), tag_id))
    cur.execute("""INSERT INTO tag_aliases (tag_id, alias_name, normalized_name) VALUES (%s, %s, %s)
                   ON CONFLICT (normalized_name) DO NOTHING""", (tag_id, name, normalize_topic(name)))
