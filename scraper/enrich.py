#!/usr/bin/env python3
"""
Fill in facts about organisations and people from outside the episode text.

    orgs         websites (show-note links, then Clearbit's autocomplete and
                 Wikidata), and from Wikidata: type (when unset), country,
                 HQ city and coordinates, founding year, Wikipedia /
                 LinkedIn / X / Bluesky, a Commons logo, parent links
    people-links LinkedIn / X / Bluesky from show-note links next to a
                 guest's name
    people-wiki  photo, Wikipedia, X / Bluesky / LinkedIn, website from
                 Wikidata — only for a person whose Wikidata entry names an
                 organisation we already have for them

Each command writes a plan (JSON) and a review list (CSV) to --out and
changes nothing unless --apply is given. Values typed in an admin page
('admin' / 'manual' sources) are never overwritten; other fields are only
filled when empty. Every web response is cached under --cache, so a rerun,
or --apply after a dry run, makes no new requests.

    python enrich.py orgs --out /tmp/enrich            # dry run
    python enrich.py orgs --out /tmp/enrich --apply
"""

import argparse
import csv
import hashlib
import html
import json
import logging
import os
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'backend'))
from org_names import normalize_org_name  # noqa: E402

log = logging.getLogger('enrich')
DB = os.getenv('DATABASE_URL', 'postgresql://localhost/podcast_db')
UA = 'PodcastNetwork/1.0 (https://github.com/FrazRoc/podcast-network; evanfrasz@gmail.com) enrichment'

# ------------------------------------------------------------------
# Web lookups, cached on disk
# ------------------------------------------------------------------

class Fetcher:
    def __init__(self, cache_dir: str, pause: float = 0.15):
        self.cache_dir = cache_dir
        self.pause = pause
        os.makedirs(cache_dir, exist_ok=True)
        self.requests = 0

    def many(self, urls: list, workers: int = 4):
        """Fetch (and cache) several URLs a few at a time."""
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for i, _ in enumerate(pool.map(self.json, urls)):
                if i and i % 500 == 0:
                    log.info('fetched %d/%d', i, len(urls))

    def json(self, url: str):
        key = hashlib.sha1(url.encode()).hexdigest()
        path = os.path.join(self.cache_dir, key[:2], key + '.json')
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
        data = None
        for attempt in range(4):
            try:
                req = urllib.request.Request(url, headers={'User-Agent': UA, 'Accept': 'application/json'})
                with urllib.request.urlopen(req, timeout=30) as r:
                    data = json.load(r)
                break
            except urllib.error.HTTPError as e:
                if e.code in (429, 503) and attempt < 3:
                    time.sleep(5 * (attempt + 1))
                    continue
                data = {'__error__': e.code}
                break
            except Exception as e:  # network hiccup
                if attempt < 3:
                    time.sleep(3)
                    continue
                data = {'__error__': str(e)}
        self.requests += 1
        time.sleep(self.pause)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            json.dump(data, f)
        return data


def squash(s: str | None) -> str:
    s = unicodedata.normalize('NFKD', s or '').encode('ascii', 'ignore').decode().lower()
    s = re.sub(r'^the\s+', '', s)
    return re.sub(r'[^a-z0-9]', '', s)


# ------------------------------------------------------------------
# Domains
# ------------------------------------------------------------------

_SECOND_LEVEL = {'co.uk', 'org.uk', 'ac.uk', 'gov.uk', 'com.au', 'org.au', 'gov.au', 'edu.au', 'co.nz', 'org.nz',
                 'co.jp', 'ac.jp', 'or.jp', 'com.br', 'gov.br', 'co.in', 'gov.in', 'ac.in', 'org.in', 'com.cn',
                 'co.za', 'org.za', 'com.mx', 'gob.mx', 'com.sg', 'gov.sg', 'co.kr', 'com.tw', 'gov.tw'}
# Links in show notes that are never a guest's organisation.
_NOT_ORG_HOSTS = re.compile(
    r'(^|\.)(apple|spotify|google|goo\.gl|youtube|youtu\.be|twitter|x\.com|linkedin|facebook|instagram|threads|bsky|'
    r'bit\.ly|ow\.ly|tinyurl|podcasts?|anchor|megaphone|simplecast|libsyn|buzzsprout|patreon|substack|acast|omny|'
    r'pod\.link|podlink|linktr|mailchi|eventbrite|amazon|wikipedia|t\.co|tiktok|soundcloud|feeds|podbean|transistor|'
    r'captivate|riverside|zencastr|medium|github|gofundme|zoom|calendly|typeform|forms|docs|drive|dropbox|vimeo|'
    r'flipboard|pca\.st|overcast|castbox|pocketcasts|iheart|stitcher|audible|art19|chartable|podtrac|spreaker|'
    r'buymeacoffee|ko-fi|gumroad|shopify|squarespace|wix|wordpress\.com|blogspot|notion|airtable|hubspot|sendgrid)\.',
    re.I)


def registrable(host: str) -> str:
    host = host.lower().strip('.').removeprefix('www.')
    parts = host.split('.')
    if len(parts) >= 3 and '.'.join(parts[-2:]) in _SECOND_LEVEL:
        return '.'.join(parts[-3:])
    return '.'.join(parts[-2:])


def host_of(url: str | None) -> str | None:
    if not url:
        return None
    try:
        h = urllib.parse.urlparse(url if '//' in url else '//' + url).hostname
    except ValueError:
        return None
    if not h:
        return None
    h = h.lower().removeprefix('www.')
    # "ar.enec.gov.ae" -> "enec.gov.ae": a language edition of the site.
    return re.sub(r'^(ar|en|fr|de|es|it|pt|ja|zh|ko|ru|nl|sv|da|no|fi)\.(?=[^.]+\.[^.]+)', '', h)


def domain_exactly_names(domain: str, name: str) -> bool:
    """camus.energy for Camus, bnef.com for BNEF: the main label is the name."""
    key, stem = squash(name), squash(registrable(domain).split('.')[0])
    return bool(key) and stem == key


def domain_matches_name(domain: str, name: str) -> bool:
    """The domain's main label is the organisation's name (or clearly
    contains it, or it the domain): camus.energy for Camus,
    sightlineclimate.com for Sightline, forourclimate.org for Solutions for
    Our Climate. Short names (< 4 letters) must match exactly."""
    key = squash(name)
    stem = squash(registrable(domain).split('.')[0])
    if not key or not stem:
        return False
    if stem == key:
        return True
    if min(len(key), len(stem)) < 5:
        return False
    return stem.startswith(key) or key.startswith(stem) or (len(stem) >= 6 and stem in key)


# ------------------------------------------------------------------
# Wikidata
# ------------------------------------------------------------------

WD_API = 'https://www.wikidata.org/w/api.php'
# Descriptions that mean the entity isn't an organisation.
_NOT_ORG_DESC = re.compile(r'\b(family name|given name|surname|film|album|song|single by|novel|book|village|town in|city in|'
                           r'river|mountain|genus|species|fictional|character|episode|asteroid|painting|human settlement|'
                           r'disambiguation|wikimedia|scientific article|video game|television series|musical group|band)\b', re.I)
_HUMAN = 'Q5'


def wd_claim_values(entity: dict, prop: str) -> list:
    out = []
    for c in entity.get('claims', {}).get(prop, []):
        if c.get('rank') == 'deprecated':
            continue
        v = c.get('mainsnak', {}).get('datavalue', {}).get('value')
        if v is not None:
            out.append((c.get('rank'), v))
    out.sort(key=lambda rv: 0 if rv[0] == 'preferred' else 1)
    return [v for _, v in out]


def wd_ids(entity: dict, prop: str, current_only: bool = False) -> list:
    if current_only:
        # Skip claims with an end date: General Atomics' parent *was* General Dynamics.
        claims = [c for c in entity.get('claims', {}).get(prop, [])
                  if c.get('rank') != 'deprecated' and 'P582' not in c.get('qualifiers', {})]
        return [c['mainsnak']['datavalue']['value']['id'] for c in claims
                if isinstance(c.get('mainsnak', {}).get('datavalue', {}).get('value'), dict)]
    return [v['id'] for v in wd_claim_values(entity, prop) if isinstance(v, dict) and 'id' in v]


class Wikidata:
    def __init__(self, fetch: Fetcher):
        self.fetch = fetch

    @staticmethod
    def search_url(name: str) -> str:
        return WD_API + '?' + urllib.parse.urlencode(
            {'action': 'wbsearchentities', 'search': name, 'language': 'en', 'uselang': 'en', 'type': 'item',
             'format': 'json', 'limit': 7})

    def search(self, name: str) -> list:
        r = self.fetch.json(self.search_url(name))
        return r.get('search', []) if isinstance(r, dict) else []

    def entities(self, ids: list) -> dict:
        out = {}
        ids = sorted(set(ids))
        for i in range(0, len(ids), 50):
            chunk = ids[i:i + 50]
            r = self.fetch.json(WD_API + '?' + urllib.parse.urlencode(
                {'action': 'wbgetentities', 'ids': '|'.join(chunk), 'props': 'labels|descriptions|aliases|claims|sitelinks',
                 'languages': 'en', 'sitefilter': 'enwiki', 'format': 'json'}))
            out.update((r or {}).get('entities', {}) if isinstance(r, dict) else {})
        return out

    def prefetch_entities(self, ids: list):
        ids = sorted(set(ids))
        self.fetch.many([WD_API + '?' + urllib.parse.urlencode(
            {'action': 'wbgetentities', 'ids': '|'.join(ids[i:i + 50]), 'props': 'labels|descriptions|aliases|claims|sitelinks',
             'languages': 'en', 'sitefilter': 'enwiki', 'format': 'json'}) for i in range(0, len(ids), 50)])

    @staticmethod
    def label(entity: dict) -> str:
        return entity.get('labels', {}).get('en', {}).get('value', '')

    @staticmethod
    def description(entity: dict) -> str:
        return entity.get('descriptions', {}).get('en', {}).get('value', '')

    @staticmethod
    def names(entity: dict) -> list:
        return [Wikidata.label(entity)] + [a['value'] for a in entity.get('aliases', {}).get('en', [])]


def commons_url(filename: str) -> str:
    return 'https://commons.wikimedia.org/wiki/Special:FilePath/' + urllib.parse.quote(filename.replace(' ', '_'))


# Type from a Wikidata description, only used when an organisation has none.
_TYPE_FROM_DESC = [
    ('academic', r'\b(university|college|school of|business school|law school|polytechnic)\b'),
    ('government', r'\b(government|agency|department|ministry|legislature|parliament|regulator|commission of|'
                   r'federal|state agency|municipal|city council|public authority)\b'),
    ('research', r'\b(think tank|research (institute|center|centre|organi[sz]ation|laboratory)|national laboratory|laboratory)\b'),
    ('media', r'\b(newspaper|magazine|news (website|agency|organi[sz]ation|outlet)|media (company|outlet)|broadcaster|'
              r'podcast|publisher|publication|radio station|television)\b'),
    ('investor', r'\b(venture capital|investment (firm|company|management)|private equity|asset manager|hedge fund)\b'),
    ('association', r'\b(trade association|industry association|professional association|trade group|federation of)\b'),
    ('nonprofit', r'\b(non-?profit|not-for-profit|charity|charitable|foundation|ngo|non-governmental|advocacy)\b'),
    ('company', r'\b(company|corporation|manufacturer|startup|business|firm|enterprise|utility|conglomerate|developer)\b'),
]


def type_from_description(desc: str) -> str | None:
    for t, rx in _TYPE_FROM_DESC:
        if re.search(rx, desc or '', re.I):
            return t
    return None


# ------------------------------------------------------------------
# Organisations
# ------------------------------------------------------------------

_INSTITUTION_TYPES = {'government', 'academic', 'nonprofit', 'research', 'association'}
_PROTECTED = {'admin', 'manual'}


def show_note_domains(cur) -> dict:
    """{org_id: Counter(domain)} — links in the descriptions of episodes a
    guest of the organisation appeared on, whose domain matches its name."""
    cur.execute("""
        SELECT o.org_id, o.name, e.description
        FROM organizations o
        JOIN organization_aliases a ON a.org_id = o.org_id
        JOIN host_affiliations ha ON ha.company_key = a.normalized_name
        JOIN episodes e ON e.episode_id = ha.episode_id
        WHERE NOT o.not_an_org AND e.description ~* 'https?://'
    """)
    exact, partial = defaultdict(Counter), defaultdict(Counter)
    for r in cur.fetchall():
        for m in re.finditer(r'https?://([a-z0-9.-]+\.[a-z]{2,})', r['description'] or '', re.I):
            host = m.group(1).lower().removeprefix('www.')
            if _NOT_ORG_HOSTS.search(host + '.'):
                continue
            if domain_exactly_names(host, r['name']):
                exact[r['org_id']][registrable(host)] += 1
            elif domain_matches_name(host, r['name']):
                partial[r['org_id']][registrable(host)] += 1
    return exact, partial


def clearbit_url(name: str) -> str:
    return 'https://autocomplete.clearbit.com/v1/companies/suggest?query=' + urllib.parse.quote(name)


def clearbit_domain(fetch: Fetcher, name: str) -> str | None:
    r = fetch.json(clearbit_url(name))
    if not isinstance(r, list):
        return None
    for s in r:
        if squash(s.get('name')) == squash(name) and s.get('domain'):
            return s['domain'].lower()
    return None


def _load_words() -> set:
    try:
        with open('/usr/share/dict/words') as f:
            return {w.strip().lower() for w in f}
    except OSError:
        return set()


_WORDS = _load_words()


def one_word(name: str) -> bool:
    """A one-word name that's an ordinary word or very short ("Ember",
    "Indigo", "Mars", "ABB"): Clearbit's guess for these is often another
    company. Coined brand names ("Invenergy", "Nexamp") are safe."""
    words = re.sub(r'^the\s+', '', name.strip(), flags=re.I).split()
    if len(words) != 1:
        return False
    w = squash(words[0])
    return len(w) <= 5 or w in _WORDS or not _WORDS


def trusted_entity(org: dict, entities: list, note_domains, clearbit: str | None):
    """The Wikidata entry that is this organisation, or None. Search finds
    namesakes ("UxC" a railway section, "The Telegraph" the Indian paper), so:
    one whose website agrees with the show notes or Clearbit is trusted;
    otherwise only the sole exact-name entry, with a Wikipedia article, for a
    name that isn't a short acronym."""
    # The show notes, when they have a link, are the only thing to check
    # against: Clearbit's guess can agree with a namesake ("Terra" -> the
    # Brazilian portal, while the show linked terra.do).
    ours = {registrable(d) for d in note_domains} if note_domains else \
        ({registrable(clearbit)} if clearbit else set())

    def sites(e):
        return {registrable(h) for h in (host_of(v) for v in wd_claim_values(e, 'P856') if isinstance(v, str)) if h}

    for e in entities:
        if ours and sites(e) & ours:
            return e
    if len(entities) != 1:
        return None
    e = entities[0]
    if ours and sites(e) and not (sites(e) & ours):
        return None                             # a different organisation's website
    acronym = len(squash(org['name'])) <= 5 and org['name'].strip().upper() == org['name'].strip()
    if acronym or 'enwiki' not in e.get('sitelinks', {}):
        return None
    return e


def plan_orgs(cur, fetch: Fetcher, limit: int | None = None, manual: dict | None = None) -> tuple:
    manual = manual or {}
    wd = Wikidata(fetch)
    cur.execute("""
        SELECT o.org_id, o.name, o.org_type, o.parent_org_id, o.website_domain, o.website_source, o.wikidata_id,
               (SELECT COUNT(DISTINCT ha.host_id) FROM organization_aliases a
                JOIN host_affiliations ha ON ha.company_key = a.normalized_name WHERE a.org_id = o.org_id) AS people
        FROM organizations o WHERE NOT o.not_an_org ORDER BY people DESC, o.org_id
    """)
    orgs = cur.fetchall()
    if limit:
        orgs = orgs[:limit]
    notes, notes_partial = show_note_domains(cur)
    fetch.many([Wikidata.search_url(o['name']) for o in orgs])
    fetch.many([clearbit_url(o['name']) for o in orgs
                if o['website_source'] not in _PROTECTED and not notes.get(o['org_id'])])

    # 1. Wikidata: search each name, keep candidates whose label or alias is the name exactly.
    cands = {}
    for i, o in enumerate(orgs):
        if i and i % 250 == 0:
            log.info('wikidata search %d/%d (%d requests)', i, len(orgs), fetch.requests)
        hits = [h for h in wd.search(o['name'])
                if squash(h.get('label')) == squash(o['name'])
                or any(squash(a) == squash(o['name']) for a in h.get('aliases', []))]
        cands[o['org_id']] = [h['id'] for h in hits]
    wd.prefetch_entities([q for qs in cands.values() for q in qs])
    ents = wd.entities([q for qs in cands.values() for q in qs])
    # Labels for the things claims point at (country, HQ, parent, types).
    ref_ids = set()
    for e in ents.values():
        for p in ('P17', 'P159', 'P31', 'P749'):
            ref_ids.update(wd_ids(e, p))
    refs = wd.entities(list(ref_ids))

    def is_org(e):
        if _HUMAN in wd_ids(e, 'P31'):
            return False
        return not _NOT_ORG_DESC.search(Wikidata.description(e))

    plan, review = [], []
    for i, o in enumerate(orgs):
        if i and i % 250 == 0:
            log.info('clearbit %d/%d (%d requests)', i, len(orgs), fetch.requests)
        entity = trusted_entity(o, [ents[q] for q in cands[o['org_id']] if q in ents and is_org(ents[q])],
                                notes.get(o['org_id']), clearbit_domain(fetch, o['name']))
        facts = {}
        wd_site = None
        if entity:
            q = entity['id']
            sites = [v for v in wd_claim_values(entity, 'P856') if isinstance(v, str)]
            wd_site = host_of(sites[0]) if sites else None
            country = [Wikidata.label(refs[c]) for c in wd_ids(entity, 'P17') if c in refs]
            hq = [refs[c] for c in wd_ids(entity, 'P159') if c in refs]
            coords = [v for v in wd_claim_values(entity, 'P625') if isinstance(v, dict)] or \
                     [v for h in hq for v in wd_claim_values(h, 'P625') if isinstance(v, dict)]
            inception = [v for v in wd_claim_values(entity, 'P571') if isinstance(v, dict)]
            year = None
            if inception:
                m = re.match(r'[+-](\d{4})', inception[0].get('time', ''))
                year = int(m.group(1)) if m and 1000 < int(m.group(1)) <= datetime.now().year else None
            enwiki = entity.get('sitelinks', {}).get('enwiki', {}).get('title')
            li = [v for v in wd_claim_values(entity, 'P4264') if isinstance(v, str)]
            tw = [v for v in wd_claim_values(entity, 'P2002') if isinstance(v, str)]
            bs = [v for v in wd_claim_values(entity, 'P12361') if isinstance(v, str)]
            logo = [v for v in wd_claim_values(entity, 'P154') if isinstance(v, str)]
            desc = Wikidata.description(entity)
            facts = {
                'wikidata_id': q,
                'country': country[0] if country else None,
                # Not when it only repeats the country ("United States" for the Senate).
                'hq_city': (Wikidata.label(hq[0]) if hq and Wikidata.label(hq[0]) not in country else None),
                'hq_lat': coords[0].get('latitude') if coords else None,
                'hq_lon': coords[0].get('longitude') if coords else None,
                'founded_year': year,
                'wikipedia_url': ('https://en.wikipedia.org/wiki/' + urllib.parse.quote(enwiki.replace(' ', '_'))) if enwiki else None,
                'linkedin_url': ('https://www.linkedin.com/company/' + li[0]) if li else None,
                'twitter_handle': tw[0] if tw else None,
                'bluesky_handle': bs[0] if bs else None,
                'commons_logo_url': commons_url(logo[0]) if logo else None,
                'wd_description': desc,
                # "parent organisation" only: "owned by" lists shareholders
                # (BlackRock "owns" Walmart), which is not a parent.
                'wd_parents': wd_ids(entity, 'P749', current_only=True),
            }
            if not o['org_type']:
                t = type_from_description(desc)
                if t:
                    facts['org_type'] = t

        # 2. Website: show notes first, then by kind of organisation.
        website, source, note = None, None, ''
        if o['website_source'] in _PROTECTED:
            pass
        elif notes.get(o['org_id']):
            website, source = notes[o['org_id']].most_common(1)[0][0], 'show_notes'
        else:
            cb = clearbit_domain(fetch, o['name'])
            wdd = wd_site
            agree = cb and wdd and registrable(cb) == registrable(wdd)
            prefer_wd = (o['org_type'] or facts.get('org_type')) in _INSTITUTION_TYPES
            first, second = (('wikidata', wdd), ('clearbit', cb)) if prefer_wd else (('clearbit', cb), ('wikidata', wdd))
            pick = first if first[1] else second
            if not pick[1] and notes_partial.get(o['org_id']):
                # A show-note link that only partly matches the name
                # (amazonresearch.org for Amazon) is the last resort.
                pick = ('show_notes', notes_partial[o['org_id']].most_common(1)[0][0])
                cb = cb or None
            if pick[1]:
                website, source = pick[1], pick[0]
                if cb and wdd and not agree:
                    note = f'sources disagree: clearbit={cb} wikidata={wdd}'
                exact_wd = source == 'wikidata' and domain_exactly_names(website, o['name'])
                if one_word(o['name']) and not agree and not exact_wd:
                    # "Terra" -> terra.com.br, "Mars" -> mars.com: a one-word name
                    # needs both sources to agree, or the show notes.
                    review.append({'org_id': o['org_id'], 'name': o['name'], 'people': o['people'], 'website': website,
                                   'source': source, 'why': 'one-word name, single source' + (f'; {note}' if note else '')})
                    website, source = None, None
                elif note:
                    review.append({'org_id': o['org_id'], 'name': o['name'], 'people': o['people'], 'website': website,
                                   'source': source, 'why': note})
        plan.append({'org_id': o['org_id'], 'name': o['name'], 'people': o['people'], 'org_type': o['org_type'],
                     'parent_org_id': o['parent_org_id'], 'website': website, 'website_source': source,
                     'current_website': o['website_domain'], **facts})
    # Decisions made by hand after a dry run (--manual), by organisation name:
    #   websites     {name: domain or full link}
    #   no_parent    [name, …]   Wikidata's parent is wrong or out of date
    #   no_wikidata  [name, …]   the Wikidata match is another organisation
    for p in plan:
        if p['name'] in manual.get('no_wikidata', []):
            for f in ('wikidata_id', 'country', 'hq_city', 'hq_lat', 'hq_lon', 'founded_year', 'wikipedia_url', 'linkedin_url',
                      'twitter_handle', 'bluesky_handle', 'commons_logo_url', 'wd_parents', 'wd_description'):
                p.pop(f, None)
            # …and so is a type read from its description: keep the one it had.
            p['org_type'] = next((o['org_type'] for o in orgs if o['org_id'] == p['org_id']), None)
        site = manual.get('websites', {}).get(p['name'])
        if site:
            full = site if re.match(r'^https?://', site) else None
            p.update(website=host_of(site) if full else site.lower(), website_url=full if full and urllib.parse.urlsplit(full).path.strip('/') else None,
                     website_source='manual')
            review[:] = [r for r in review if r['org_id'] != p['org_id']]
    # 3. Parent links where Wikidata names a parent we also matched.
    by_q = {p['wikidata_id']: p for p in plan if p.get('wikidata_id')}
    for p in plan:
        if p['parent_org_id'] or not p.get('wd_parents'):
            continue
        if p['name'] in manual.get('no_parent', []):
            continue
        parent = next((by_q[q] for q in p['wd_parents'] if q in by_q and by_q[q]['org_id'] != p['org_id']), None)
        if parent:
            p['new_parent_org_id'] = parent['org_id']
            p['new_parent_name'] = parent['name']
    return plan, review


def apply_orgs(conn, plan: list) -> Counter:
    cur = conn.cursor()
    cur.execute("SET lock_timeout = '10s'")
    stats = Counter()
    taken = set()
    for p in plan:
        sets, params = [], {'id': p['org_id']}
        if p.get('website') and not p.get('current_website'):
            sets += ['website_domain = %(website)s', 'website_source = %(website_source)s',
                     'website_url = %(website_url)s']
            params.update(website=p['website'], website_source=p['website_source'], website_url=p.get('website_url'))
            stats['website'] += 1
            stats['website_' + p['website_source']] += 1
        if p.get('wikidata_id') and p['wikidata_id'] not in taken:
            taken.add(p['wikidata_id'])
            for f in ('wikidata_id', 'country', 'hq_city', 'hq_lat', 'hq_lon', 'founded_year', 'wikipedia_url',
                      'linkedin_url', 'twitter_handle', 'bluesky_handle', 'commons_logo_url'):
                if p.get(f) is not None:
                    sets.append(f'{f} = COALESCE({f}, %({f})s)')
                    params[f] = p[f]
                    stats[f] += 1
            if p.get('org_type'):
                sets.append('org_type = COALESCE(org_type, %(org_type)s)')
                params['org_type'] = p['org_type']
                stats['org_type'] += 1
        if p.get('new_parent_org_id'):
            sets.append('parent_org_id = COALESCE(parent_org_id, %(parent)s)')
            params['parent'] = p['new_parent_org_id']
            stats['parent'] += 1
        if sets:
            cur.execute(f"UPDATE organizations SET {', '.join(sets)}, enriched_at = now(), updated_at = now() "
                        f"WHERE org_id = %(id)s", params)
    conn.commit()
    return stats


# ------------------------------------------------------------------
# People: links from show notes
# ------------------------------------------------------------------

_LINK_RE = {
    'linkedin_url': re.compile(r'https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/([A-Za-z0-9_%\-]+)/?', re.I),
    'twitter_handle': re.compile(r'https?://(?:www\.|mobile\.)?(?:twitter|x)\.com/(?!intent|share|home|search|hashtag|i/)([A-Za-z0-9_]{2,15})\b', re.I),
    'bluesky_handle': re.compile(r'https?://bsky\.app/profile/([A-Za-z0-9._:-]+)', re.I),
}
_SHOW_ACCOUNT_WORDS = re.compile(r'(pod|cast|show|radio|news|media|official|team|network|fm|hq|energy|climate)', re.I)


def ascii_fold(s: str) -> str:
    return unicodedata.normalize('NFKD', s or '').encode('ascii', 'ignore').decode().lower()


def plan_people_links(cur) -> tuple:
    """Links in the descriptions of a guest's episodes, attributed to them only
    when the link itself carries their name (linkedin.com/in/jane-doe-12,
    x.com/CampanaleMark) and nobody else's."""
    cur.execute("""
        SELECT h.host_id, h.first_name, h.last_name, h.linkedin_url, h.twitter_handle, h.bluesky_handle,
               h.field_sources, eh.episode_id, e.description
        FROM hosts h
        JOIN episode_host eh ON eh.host_id = h.host_id AND eh.is_guest
        JOIN episodes e ON e.episode_id = eh.episode_id
        WHERE e.description ~* '(linkedin\\.com/in/|twitter\\.com/|x\\.com/|bsky\\.app/profile/)'
    """)
    found = defaultdict(lambda: defaultdict(Counter))
    claims, named = defaultdict(set), defaultdict(set)   # value -> people it was attributed to
    people = {}
    for r in cur.fetchall():
        people[r['host_id']] = r
        first, last = ascii_fold(r['first_name']), ascii_fold(r['last_name'])
        if len(last) < 2:
            continue
        text = r['description'] or ''
        plain = ascii_fold(html.unescape(re.sub(r'<[^>]+>', ' ', text)))
        full = f'{first} {last}'
        spans = [m.start() for m in re.finditer(re.escape(full), plain)]
        for field, rx in _LINK_RE.items():
            for m in rx.finditer(text):
                handle = urllib.parse.unquote(m.group(1)).strip('/')
                folded = re.sub(r'[^a-z0-9]', '', ascii_fold(handle))
                by_name = last.replace(' ', '').replace('-', '') in folded and (
                    first[:1] and (first.replace(' ', '')[:4] in folded or folded.startswith(first[:1])))
                near = False
                if not by_name and spans:
                    # Position of this link in the plain text: find the handle there.
                    hp = plain.find(ascii_fold(handle))
                    anchor = hp if hp >= 0 else None
                    if anchor is None:
                        # Link only in an href: use the anchor text's position.
                        tail = text[m.end():m.end() + 200]
                        at = re.search(r'>([^<]{1,80})<', tail)
                        anchor = plain.find(ascii_fold(html.unescape(at.group(1)).strip())) if at else -1
                    if anchor is not None and anchor >= 0:
                        near = any(abs(anchor - s) <= 120 for s in spans)
                        # Not if another link of this kind sits between.
                if field == 'twitter_handle' and not by_name and _SHOW_ACCOUNT_WORDS.search(handle):
                    continue
                if by_name or near:
                    value = handle if field != 'linkedin_url' else f'https://www.linkedin.com/in/{handle}'
                    found[r['host_id']][field][value] += 2 if by_name else 1
                    claims[value].add(r['host_id'])
                    if by_name:
                        named[value].add(r['host_id'])
    # A link found only by being near someone's name is dropped when it was
    # near several people's (the show's or host's own account) or carries
    # another person's name (a co-guest's profile).
    # Only links that carry the person's own name are trusted: nearness alone
    # picked up co-guests' profiles and company accounts (Sep 2026 dry run).
    for host_id, fields in found.items():
        for field, cnt in fields.items():
            for value in list(cnt):
                if host_id not in named[value] or len(named[value]) > 1:
                    del cnt[value]
    plan, review = [], []
    for host_id, fields in found.items():
        p = people[host_id]
        change = {}
        for field, cnt in fields.items():
            if not cnt or p.get(field) or p['field_sources'].get(field) in _PROTECTED:
                continue
            best = cnt.most_common(2)
            if len(best) > 1 and best[0][1] == best[1][1]:
                review.append({'host_id': host_id, 'name': f"{p['first_name']} {p['last_name']}", 'field': field,
                               'candidates': ' | '.join(v for v, _ in best)})
                continue
            change[field] = best[0][0]
        if change:
            plan.append({'host_id': host_id, 'name': f"{p['first_name']} {p['last_name']}", **change})
    return plan, review


def apply_people(conn, plan: list, source: str) -> Counter:
    cur = conn.cursor()
    cur.execute("SET lock_timeout = '10s'")
    stats = Counter()
    fields = ('linkedin_url', 'twitter_handle', 'bluesky_handle', 'profile_image_url', 'wikipedia', 'website_url', 'wikidata_id')
    for p in plan:
        for f in fields:
            if not p.get(f):
                continue
            cur.execute(f"""UPDATE hosts SET {f} = %s,
                            field_sources = field_sources || jsonb_build_object(%s, %s)
                            WHERE host_id = %s AND ({f} IS NULL OR {f} = '')""",
                        (p[f], f, source, p['host_id']))
            stats[f] += cur.rowcount
    conn.commit()
    return stats


# ------------------------------------------------------------------
# People: Wikidata
# ------------------------------------------------------------------

def _year(entity: dict, prop: str) -> int | None:
    for v in wd_claim_values(entity, prop):
        m = re.match(r'[+-](\d{4})', v.get('time', '') if isinstance(v, dict) else '')
        if m:
            return int(m.group(1))
    return None


def _historical(entity: dict) -> bool:
    """Guests are living or recent people: the naturalist John Muir (died
    1914) is a Sierra Club member on Wikidata, not our Sierra Club guest."""
    died, born = _year(entity, 'P570'), _year(entity, 'P569')
    return (died is not None and died < 2010) or (born is not None and born < 1900)


def plan_people_wiki(cur, fetch: Fetcher, limit: int | None = None) -> tuple:
    """A person's Wikidata entry, trusted only when its employer, position
    held, membership or description names an organisation we already have
    for them — "Jigar Shah" otherwise finds a namesake."""
    wd = Wikidata(fetch)
    cur.execute("""
        SELECT h.host_id, h.first_name, h.last_name, h.profile_image_url, h.wikipedia, h.website_url,
               h.linkedin_url, h.twitter_handle, h.bluesky_handle, h.wikidata_id, h.field_sources,
               array_agg(DISTINCT o.name) FILTER (WHERE o.name IS NOT NULL) AS orgs,
               array_agg(DISTINCT o.wikidata_id) FILTER (WHERE o.wikidata_id IS NOT NULL) AS org_qids,
               array_agg(DISTINCT ha.title) FILTER (WHERE ha.title IS NOT NULL) AS titles
        FROM hosts h
        JOIN host_affiliations ha ON ha.host_id = h.host_id
        LEFT JOIN organization_aliases a ON a.normalized_name = ha.company_key
        LEFT JOIN organizations o ON o.org_id = a.org_id AND NOT o.not_an_org
        GROUP BY h.host_id
        ORDER BY COUNT(DISTINCT ha.episode_id) DESC
    """)
    people = cur.fetchall()
    if limit:
        people = people[:limit]
    fetch.many([Wikidata.search_url(f"{p['first_name']} {p['last_name']}") for p in people])
    cands = {}
    for i, p in enumerate(people):
        if i and i % 250 == 0:
            log.info('wikidata people search %d/%d (%d requests)', i, len(people), fetch.requests)
        name = f"{p['first_name']} {p['last_name']}"
        cands[p['host_id']] = [h['id'] for h in wd.search(name)
                               if squash(h.get('label')) == squash(name)
                               or any(squash(a) == squash(name) for a in h.get('aliases', []))]
    wd.prefetch_entities([q for qs in cands.values() for q in qs])
    ents = wd.entities([q for qs in cands.values() for q in qs])
    refs = wd.entities(list({q for e in ents.values() for prop in ('P108', 'P39', 'P463', 'P1416', 'P102')
                             for q in wd_ids(e, prop)}))
    plan, review = [], []
    for p in people:
        humans = [ents[q] for q in cands[p['host_id']]
                  if q in ents and _HUMAN in wd_ids(ents[q], 'P31') and not _historical(ents[q])]
        if not humans:
            continue
        ours = {squash(n) for n in (p['orgs'] or []) if n}
        our_q = set(p['org_qids'] or [])
        ours_words = [n for n in (p['orgs'] or []) if n and len(squash(n)) >= 4]
        match = None
        for e in humans:
            linked = [q for prop in ('P108', 'P39', 'P463', 'P1416') for q in wd_ids(e, prop)]
            names = {squash(Wikidata.label(refs[q])) for q in linked if q in refs}
            desc = Wikidata.description(e)
            if our_q & set(linked) or ours & names or any(squash(n) in squash(desc) for n in ours_words):
                match = e
                break
        if not match:
            if humans:
                review.append({'host_id': p['host_id'], 'name': f"{p['first_name']} {p['last_name']}",
                               'candidates': ' | '.join(f"{e['id']}: {Wikidata.description(e)}" for e in humans[:3]),
                               'our_orgs': '; '.join((p['orgs'] or [])[:4])})
            continue
        photo = [v for v in wd_claim_values(match, 'P18') if isinstance(v, str)]
        enwiki = match.get('sitelinks', {}).get('enwiki', {}).get('title')
        site = [v for v in wd_claim_values(match, 'P856') if isinstance(v, str)]
        li = [v for v in wd_claim_values(match, 'P6634') if isinstance(v, str)]
        tw = [v for v in wd_claim_values(match, 'P2002') if isinstance(v, str)]
        bs = [v for v in wd_claim_values(match, 'P12361') if isinstance(v, str)]
        change = {'host_id': p['host_id'], 'name': f"{p['first_name']} {p['last_name']}",
                  'wikidata_id': match['id'], 'wd_description': Wikidata.description(match)}
        for f, v in [('profile_image_url', commons_url(photo[0]) + '?width=400' if photo else None),
                     ('wikipedia', 'https://en.wikipedia.org/wiki/' + urllib.parse.quote(enwiki.replace(' ', '_')) if enwiki else None),
                     ('website_url', site[0] if site else None),
                     ('linkedin_url', 'https://www.linkedin.com/in/' + li[0] if li else None),
                     ('twitter_handle', tw[0] if tw else None), ('bluesky_handle', bs[0] if bs else None)]:
            if v and not p.get(f) and p['field_sources'].get(f) not in _PROTECTED:
                change[f] = v
        plan.append(change)
    return plan, review


# ------------------------------------------------------------------

def write(out_dir: str, name: str, plan: list, review: list):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f'{name}_plan.json'), 'w') as f:
        json.dump(plan, f, indent=1, default=str)
    if review:
        with open(os.path.join(out_dir, f'{name}_review.csv'), 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=list(review[0]))
            w.writeheader()
            w.writerows(review)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('command', choices=['orgs', 'people-links', 'people-wiki'])
    ap.add_argument('--out', required=True, help='directory for the plan and review files')
    ap.add_argument('--cache', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), '.enrich_cache'))
    ap.add_argument('--limit', type=int, help='only the first N (most-booked) — for trying it out')
    ap.add_argument('--apply', action='store_true', help='write the plan to the database')
    ap.add_argument('--manual', help='orgs: JSON of hand decisions (websites / no_parent / no_wikidata)')
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    conn = psycopg2.connect(DB, cursor_factory=RealDictCursor)
    cur = conn.cursor()
    fetch = Fetcher(args.cache)
    if args.command == 'orgs':
        manual = json.load(open(args.manual)) if args.manual else {}
        plan, review = plan_orgs(cur, fetch, args.limit, manual)
    elif args.command == 'people-links':
        plan, review = plan_people_links(cur)
    else:
        plan, review = plan_people_wiki(cur, fetch, args.limit)
    write(args.out, args.command, plan, review)
    log.info('%s: %d planned, %d to review, %d web requests', args.command, len(plan), len(review), fetch.requests)
    if args.apply:
        stats = apply_orgs(conn, plan) if args.command == 'orgs' else \
            apply_people(conn, plan, 'show_notes' if args.command == 'people-links' else 'wikidata')
        log.info('applied: %s', dict(stats))


if __name__ == '__main__':
    main()
