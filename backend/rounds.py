"""
rounds.py

"Making the rounds": a guest on several different shows within a few weeks.
That is nearly always news — a book tour (Michael Grunwald, six shows in
three weeks in 2025), a new job (Jigar Shah after the Loan Programs
Office), a death (Jane Goodall's tributes), a summit (COP30's reporters).

Pure functions over (host_id, date, podcast_id, ...) rows so the rule can be
tested without a database; the endpoint in main.py does the SQL.
"""
from collections import defaultdict
from datetime import timedelta

WINDOW_DAYS = 30
MIN_SHOWS = 3


def find_bursts(appearances, window_days: int = WINDOW_DAYS, min_shows: int = MIN_SHOWS):
    """Each person's bursts: windows of `window_days` with appearances on at
    least `min_shows` different shows.

    `appearances` is an iterable of dicts with host_id, date (a date) and
    podcast_id; any other keys are carried through. A person's best window
    (most shows, then earliest) is taken first and its appearances set
    aside, then the next best from what's left — so one busy year doesn't
    become one endless burst, and two tours a year apart are two bursts.

    Returns dicts sorted newest first: host_id, start, end, shows (count),
    appearances (the rows in the window, by date).
    """
    by_person = defaultdict(list)
    for a in appearances:
        by_person[a['host_id']].append(a)

    bursts = []
    span = timedelta(days=window_days)
    for host_id, rows in by_person.items():
        rows = sorted(rows, key=lambda a: (a['date'], a['podcast_id']))
        while len(rows) >= min_shows:
            best = None
            for i, first in enumerate(rows):
                window = [a for a in rows[i:] if a['date'] - first['date'] <= span]
                shows = len({a['podcast_id'] for a in window})
                if shows >= min_shows and (best is None or shows > best[0]):
                    best = (shows, window)
            if best is None:
                break
            shows, window = best
            bursts.append({
                'host_id': host_id,
                'start': window[0]['date'],
                'end': window[-1]['date'],
                'shows': shows,
                'appearances': window,
            })
            taken = {id(a) for a in window}
            rows = [a for a in rows if id(a) not in taken]

    bursts.sort(key=lambda b: (b['start'], b['shows']), reverse=True)
    return bursts


def making_the_rounds(cur, year: int | None = None, min_shows: int = MIN_SHOWS):
    """Bursts for the Stats page, newest first, optionally one year's (by
    the burst's start). Guest credits only; a person's appearances on a show
    they host are left out (Joe Batir on his own show isn't a tour), and so
    are credits the extractor read as only talked about — a news roundup
    naming someone is not them doing the rounds."""
    cur.execute("""
        SELECT eh.host_id, e.published_date AS date, e.podcast_id,
               e.episode_id, e.title AS episode_title, p.title AS show
        FROM episode_host eh
        JOIN episodes e USING (episode_id)
        JOIN podcasts p ON p.podcast_id = e.podcast_id
        LEFT JOIN affiliation_extractions ae
               ON ae.episode_id = eh.episode_id AND ae.host_id = eh.host_id
        WHERE eh.is_guest
          AND e.published_date >= DATE '2019-01-01'
          AND ae.appears_on_episode IS DISTINCT FROM false
          AND NOT EXISTS (SELECT 1 FROM host_podcast hp
                          WHERE hp.host_id = eh.host_id AND hp.podcast_id = e.podcast_id)
    """)
    bursts = find_bursts(cur.fetchall(), min_shows=min_shows)
    years = sorted({b['start'].year for b in bursts}, reverse=True)
    if year:
        bursts = [b for b in bursts if b['start'].year == year]

    ids = list({b['host_id'] for b in bursts})
    names = {}
    if ids:
        cur.execute("SELECT host_id, first_name || ' ' || last_name AS name, profile_image_url "
                    "FROM hosts WHERE host_id = ANY(%s)", (ids,))
        names = {r['host_id']: r for r in cur.fetchall()}

    return {
        'window_days': WINDOW_DAYS,
        'min_shows': min_shows,
        'years': years,
        'year': year,
        'bursts': [{
            'host_id': b['host_id'],
            'name': names[b['host_id']]['name'],
            'profile_image_url': names[b['host_id']]['profile_image_url'],
            'start': b['start'].isoformat(),
            'end': b['end'].isoformat(),
            'shows': b['shows'],
            'appearances': [{
                'date': a['date'].isoformat(), 'podcast_id': a['podcast_id'], 'show': a['show'],
                'episode_id': a['episode_id'], 'title': a['episode_title'],
            } for a in b['appearances']],
        } for b in bursts if b['host_id'] in names],
    }
