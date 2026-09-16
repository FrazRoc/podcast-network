"""
description_cleaner.py

Strips sponsor blocks, cross-show promotions, and other boilerplate out of an
episode description before it's checked for person names.

Canonical home: this file lives in backend/ (not scraper/) so it's guaranteed
present wherever the backend deploys — Render's build for the API service is
scoped to backend/, so a module living in scraper/ would not necessarily be
available at runtime. scraper/episode_name_scanner.py imports this module by
reaching across to backend/ instead; that direction is safe because the
scraper always runs from a full repository checkout (GitHub Actions clones
the whole repo), so backend/ is always present there too.

Both the scanner's suggestion queue and the admin backend's "scan other
episodes for this name" logic (approve/create-person/rescan) need the exact
same cleaning — without it, approving a name that happens to appear inside a
sponsor blurb on an unrelated episode credits that episode too. See CLAUDE.md
for the incidents (Political Climate, Shift Key's "Shocked" cross-promo, the
RCC "Listen to the episode with X" rotation) this was built to catch.
"""
import re

# How much of a description to scan. Who is on an episode is established in the
# opening summary; what follows is links, boilerplate — or, for Volts, a full
# transcript averaging 18,000 characters and running to 111,000. Scanning those
# credits everyone the guests merely talked about: Joe Manchin picks up 34
# Volts credits without ever appearing on the show.
DESC_SCAN_MAX_CHARS = 2500

# A STRIP_AFTER_PATTERNS match this early is a show naming its sponsor
# up front, not a sign-off — see clean_description's comment at the call site.
MIN_STRIP_AFTER_POSITION = 100

# ------------------------------------------------------------------
# DESCRIPTION CLEANING
# ------------------------------------------------------------------

STRIP_AFTER_PATTERNS = [
    # A credits block that opens with "Hosted by ..." names the hosts, and 136
    # of the 193 blocks do; cutting there threw those away. Blocks that go
    # straight to crew are still cut, and the crew tail is removed below.
    r'\nCredits:(?![^\n]{0,60}Hosted by)',
    r'(?i)(?:\.|\n)\s*Produced by[^.\n]{0,80}',
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the co-host',
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the host',
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the .{0,30} editor',
    r'\n[A-Z][a-z]+ [A-Z][a-z]+ is the .{0,30} producer',
    r'\nFollow the show on',
    r'\nFor more reporting',
    r'\nOur theme music',
    r'\nSubscribe to',
    r'Follow our co-hosts and production team',
    r'\nSee Privacy Policy',
    r'is produced by Columbia University',
    r'Explore further:',                                # Zero: The Climate Race cross-promotion
    r'Past episode with',                               # Zero: The Climate Race past episode links
    r'See omnystudio.com',                              # Omny Studio privacy footer
    r'\nProducer:',                                     # Outrage + Optimism production credits
    r'\nEdited by:',                                    # production credits
    r'\nExec Producer:',                                # production credits
    r'\nJoin the conversation:',                        # Outrage + Optimism social footer
    r'\nHosted on Acast',                               # Acast footer
    r'See acast.com/privacy',                           # Acast privacy footer
    r'\nRelated Episodes',                               # Cleaning Up: Leadership footer
    r'\nLinks\n',                                       # Cleaning Up: Leadership links section
    r'\nLinks and Related Episodes',                      # Cleaning Up: Leadership combined footer
    r'\nRelevant Guest & Topic Links',                   # Cleaning Up: Leadership links variant
    # Cleaning Up writes "Guest Bio" then prose about the guest's career, which
    # names companies rather than people. Electrify This! writes "Guest Bios:"
    # and then each guest's full name — truncating there threw away the best
    # guest information on that show, so the colon form is left alone.
    r'\nGuest Bio(?!s?\s*:)',
    r'\nFor show notes',                               # Climate One footer
    r'\nLearn more about your ad choices',             # Megaphone universal footer
    r'megaphone.fm/adchoices',                          # Megaphone universal footer
    r'🎟',                                             # Climate One upcoming shows ticket promo
    r'\nSupport Climate One',                          # Climate One support/subscribe footer
    r'\nhttps://www.linkedin.com/in/',                  # CORE Knowledge LinkedIn footer
    r'\nBlue Spark\n',                                  # CORE Knowledge company links section
    r'This episode (?:of [A-Za-z ]+ )?was (?:reported and )?(?:produced|fact.?checked)',  # How to Save a Planet / Gimlet production credits
    r'How to Save a Planet is (?:a Spotify|reported|produced|hosted)',  # How to Save a Planet / Gimlet show boilerplate
    r'\nCheck out our Calls to Action archive',          # How to Save a Planet footer
    r'The show is produced by',                          # This Week in Cleantech production credits
    r'\bwith research support from',                     # crew names, not participants
    # "Stephen Lacey is our executive editor" / "is executive producer" sits in
    # the sign-off of both Catalyst and Columbia Energy Exchange and was about
    # to credit him on 52 episodes he has no part in. Same shape as the
    # research-support credit above: crew, not participants.
    r'[A-Z][a-z]+ [A-Z][a-z]+ is (?:our |the )?executive (?:editor|producer)',
    r'\bEngineering by\b',                               # audio crew sign-off
    # Sponsor and partner credits name organisations, not participants:
    # "created in partnership with the public policy think tank Third Way"
    # was queueing Third Way as a guest.
    r'is (?:produced|created|made) in partnership with',
    r'\nRecommended reading',                            # link list of publications
    r'\nFurther reading',
    r'\nRelated reading',
    r'\bOriginal music (?:and|by)\b',                    # composer credit
]


# 2,327 descriptions still carry the feed's raw HTML. A tag has to become a
# space rather than vanish: dropping it outright welds the end of one block to
# the start of the next, which is how "Connect with Jason Rissman</p><p>On
# LinkedIn" turns into the name "Jason RissmanOn".
_HTML_TAG_RE = re.compile(r'<[^>]+>')
_HTML_ENTITIES = [('&nbsp;', ' '), ('&amp;', '&'), ('&#38;', '&'), ('&quot;', '"'),
                  ('&#39;', "'"), ('&rsquo;', "'"), ('&lsquo;', "'"), ('&mdash;', '-'),
                  ('&ndash;', '-'), ('&hellip;', '...'), ('&lt;', '<'), ('&gt;', '>')]


def strip_html(text: str) -> str:
    text = _HTML_TAG_RE.sub(' ', text)
    for entity, char in _HTML_ENTITIES:
        text = text.replace(entity, char)
    # Collapse the runs of spaces the tags leave behind, but keep line breaks:
    # the boilerplate patterns below anchor on them.
    text = re.sub(r'[^\S\n]+', ' ', text)
    return re.sub(r'\n{3,}', '\n\n', text)


# Phrases pointing at other episodes. Unlike the patterns above these are cut
# out in place rather than truncating the rest, because they sit around the
# middle of a description with real content after them — one reads "listen to
# our previous episode with Wanjira Mathai" and then "Thank you to our guest
# this week, Katie Eder!". The name belongs to the episode being linked to, not
# this one.
REMOVE_PATTERNS = [
    # Up to three words may sit between the verb and "episode" — Reversing
    # Climate Change writes "Listen to the RCC episode with Ryan Covington",
    # and that one line, repeated across 62 episodes, gave him 39 credits for
    # a single appearance. Up to three more may sit between "episode" and
    # "with" too — the same show also writes "the RCC episode I made with
    # David LaGreca", which the first gap alone didn't cover.
    r'(?i)(?:click here to\s+)?(?:listen to|watch|hear|check out|revisit)\s+'
    r'(?:\w+\s+){0,3}(?:episode|ep\.?)\s*#?\d*\s*(?:\w+\s+){0,3}(?:with|featuring|w/)[^.\n]{0,70}',
    r'(?i)our episodes? featuring[^.\n]{0,90}',
    r'(?i)(?:podcast )?interview with[^.\n]{0,50}(?=\s*(?:https?://|\n|$))',
    # Sponsor blocks and promotions for upcoming events name people who are not
    # in this episode. "Catalyst is supported by Origami Solar. Join Latitude
    # Media's Stephen Lacey and Origami's CEO Gregg Patterson for a live
    # Frontier Forum on May 30th" credited Lacey on three episodes he has no
    # part in.
    r'(?i)\bjoin\b[^.\n]{0,110}\bfor (?:a|our|the) (?:live|free|virtual|upcoming|special)\b[^.\n]{0,90}',
    r'(?i)[A-Z][A-Za-z\x27 ]{0,40} is supported by[^.\n]{0,70}',
    # Latitude Media cross-promoted Political Climate's launch in the outro of
    # Green Blueprint and Catalyst episodes: "Make sure to listen to our new
    # podcast, Political Climate ... Tune in every other Friday for the latest
    # takes from hosts Julia Pyper, Emily Domenech, and Brandon Hurlbut.
    # Available on Apple, Spotify, or wherever you get your podcasts." That
    # credited all three hosts of a show that never aired on 12 episodes of
    # two other shows. Not named to one show: any "listen to our new podcast"
    # cross-promotion reads the same way.
    r'(?i)(?:and\s+)?make sure to (?:also )?listen to our new podcast,[^.\n]*\.'
    r'(?:\s*Tune in[^.\n]*\.)?(?:\s*Available on[^.\n]*\.)?',
    # Shift Key's sponsor block cross-promotes a University of Chicago show,
    # "Shocked": "Listen to Shocked ... and hear journalist Amy Harder and
    # economist Michael Greenstone share new ways of thinking ... Find it
    # here." Amy Harder is a real, frequent guest on OTHER shows, so her full
    # name matched here and credited her on 4 Shift Key episodes she never
    # appeared in.
    r'(?i)Listen to Shocked,.*?Find it here\.',
]


def clean_description(text: str, max_chars: int = DESC_SCAN_MAX_CHARS) -> str:
    if not text:
        return ''
    text = strip_html(text)
    for pattern in REMOVE_PATTERNS:
        text = re.sub(pattern, ' ', text)
    for pattern in STRIP_AFTER_PATTERNS:
        match = re.search(pattern, text)
        # Real incident: Smart Energy Voices opens several descriptions with
        # "This episode ... is produced in partnership with Evergy Energy
        # Partners." as its very first sentence, then names the host and
        # guests right after — the "in partnership with" pattern is meant
        # for a sponsor credit that comes AFTER the real content (that's
        # true for the other 70+ episodes elsewhere that trigger it), not
        # before it. A match this early is a show naming its sponsor up
        # front, not a sign-off with nothing worth keeping after it.
        if match and match.start() >= MIN_STRIP_AFTER_POSITION:
            text = text[:match.start()]
    text = text.strip()
    if not max_chars or len(text) <= max_chars:
        return text
    # Cut back to a word boundary. Slicing at an exact character count splits
    # the word it lands on, and the fragment becomes a candidate name —
    # "Connect With Smart Energy Decisions" was queued as the person
    # "Smart Energ".
    cut = text[:max_chars]
    space = cut.rfind(' ')
    return cut[:space] if space > 0 else cut


# ------------------------------------------------------------------
# LABELLED CREDITS — precise, structured statements of who's on an episode
# ------------------------------------------------------------------
#
# Unlike the noisy heuristic patterns in scraper/episode_name_scanner.py's
# extract_candidate_names() (guessing from verbs like "talks with"), these
# require an explicit statement — "Guest:", "Host:", "Connect with [Name]".
# That precision is exactly why callers can safely run this against the FULL
# cleaned text with no DESC_SCAN_MAX_CHARS cap: the cap exists to stop a
# bare name *mention* deep in a huge transcript from reading as a credit
# (Joe Manchin picked up 34 false Volts credits this way), but a structured
# credit statement is never a bare mention — wherever it sits in the text,
# it's still a deliberate statement that this person is on the episode.
#
# Real incident this distinction matters for: Kulsoom Khan, an already-known
# host, wasn't found by run()'s exact-name matching on an episode that named
# her only in a "Connect with Kulsoom Khan" footer past the 2500-char cap.
# The fix isn't to drop the cap everywhere (that reopens the Manchin problem
# for ordinary full-text matching) — it's to let *labelled* credits like this
# one bypass it, while everything else stays capped. See run() and backend
# main.py's _verify_and_source() for how the two checks are combined.

_FALSE_POSITIVE_WORDS = {
    'how', 'why', 'what', 'when', 'where', 'which', 'who', 'will',
    'clean', 'green', 'solar', 'wind', 'grid', 'power', 'energy',
    'climate', 'carbon', 'hydrogen', 'nuclear', 'fusion', 'battery',
    'electric', 'renewable', 'data', 'center', 'tech', 'policy',
    'market', 'supply', 'chain', 'global', 'local', 'state', 'federal',
    'new', 'old', 'big', 'small', 'best', 'next', 'last', 'first',
    'american', 'united', 'states', 'world', 'north', 'south', 'east', 'west',
    'inside', 'beyond', 'me', 'us', 'him', 'her', 'the', 'this', 'that',
    'tell', 'know', 'think', 'make', 'take', 'come', 'look',
    'wall', 'street', 'main', 'back', 'front', 'high', 'low', 'virtual',
    'taming', 'rewiring',
}


# Honorifics that show up glued to the front of a captured name. The capture
# regexes each try to skip these, but they only cover the forms they list —
# "Professor" slipped past a list containing "Prof" and created a separate
# "Professor Tristan Smith" person. Stripping here catches every path.
_HONORIFIC_RE = re.compile(
    r'^(?:(?:Dr|Prof|Professor|Mr|Ms|Mrs|Miss|Sir|Dame|Rev|Senator|Sen|'
    r'Representative|Rep|Congressman|Congresswoman|Governor|Gov|Mayor|'
    r'President|Secretary|Ambassador|Admiral|General|Captain|Lord|Lady|'
    # Job titles run straight into the name the same way an honorific does:
    # the review queue holds "Founder Oliver Katz" and "CEO Dan Shugar".
    r'(?:Co[- ]?)?Founder|CEO|CTO|CFO|COO|CMO|Chief|Vice|VP|Director|'
    r'Head|Partner|Principal|Manager|Senior|Junior|Deputy|Writer|Reporter|Journalist|Editor|Author|Analyst|Correspondent|Columnist)\.?\s+)+',
    re.IGNORECASE
)

# Words that are effectively never someone's surname. Used to keep companies
# out of the review queue — episode titles are full of them ("Heart Aerospace",
# "Rigetti Computing", "Burnt Island Ventures"), and the intro patterns cannot
# tell "talks with Jane Smith" from "talks with Bedrock Robotics".
#
# Deliberately omits words that ARE real surnames: Power (Ted Power), Zero,
# Deep, Duty, Again, Lead, Health, Works. A company left in the queue costs one
# click to reject; a person filtered out is lost silently, so this errs towards
# letting things through. Checked against all 2,074 known people: no matches.
_ORG_WORDS = {
    'inc', 'llc', 'llp', 'plc', 'gmbh', 'ltd', 'corp', 'corporation', 'company', 'technologies',
    'technology', 'systems', 'solutions', 'ventures', 'capital', 'partners',
    'holdings', 'industries', 'labs', 'laboratories', 'institute', 'foundation',
    'university', 'college', 'centre', 'center', 'fund', 'media', 'news', 'studios',
    'robotics', 'aerospace', 'biosciences', 'bioscience', 'sciences', 'security',
    'batteries', 'materials', 'motors', 'mobility', 'analytics', 'strategies',
    'advisors', 'advisers', 'associates', 'consulting', 'county', 'district',
    'council', 'committee', 'association', 'alliance', 'coalition', 'society',
    'agency', 'department', 'ministry', 'commission', 'logistics', 'software',
    'division', 'divisions',
    'minerals', 'mining', 'pharma', 'airlines', 'aviation', 'shipping',
    'utilities', 'computing', 'management', 'advisory', 'enterprises',
    # Job-function words. "Director of Digital Transformation and Enterprise
    # Architecture at ..." makes "Enterprise Architecture" look exactly like a
    # second guest to the "X and Y" pattern. Checked against every known
    # person: the only match was "Chief Marketing", itself a bad record.
    'architecture', 'transformation', 'operations', 'development', 'engineering',
    'marketing', 'communications', 'affairs', 'relations', 'innovation',
    'sustainability', 'procurement', 'compliance', 'governance', 'infrastructure',
    'excellence', 'initiatives', 'partnerships', 'acquisition', 'intelligence',
    'experience', 'enablement', 'insights',
    # Show-note furniture that reads as a second name after "and"/"with".
    'transcript', 'bonus', 'takeaways', 'highlights', 'recap', 'roundup',
    'edition', 'special', 'series', 'episode', 'newsletter', 'webinar',
    # Title nouns that survive the role-prefix strip: "Chief Revenue Officer"
    # loses "Chief" and the rest reads as a name.
    'officer', 'president', 'chair', 'chairman', 'chairwoman', 'treasurer',
    'fellow', 'scholar', 'ambassador', 'counsel', 'administrator', 'commissioner',
    'director', 'directors', 'manager', 'strategist', 'advisor',
    # _POSSESSIVE_RE (episode_name_scanner.py) assumes whatever follows
    # "Org's" is a person, but Title Cased episode titles often follow a
    # possessive with an abstract topic noun instead: "Big Oil's Frivolous
    # Suits", "Nucera's Bold Forecast", "America's Economic Nervous System".
    # None of these are plausible surnames, unlike some already-excluded
    # words would have been.
    'solution', 'problem', 'problems', 'threat', 'threats', 'forecast',
    'forecasts', 'capacity', 'accelerator', 'accelerators', 'suits', 'system',
    # "Connect With Smart Energy Decisions" — a show's own publisher name,
    # not a person — is a recurring footer line alongside real "Connect with
    # [Guest Name]" entries _CONNECT_WITH_RE below is meant to catch.
    'decisions',
}


def looks_like_organisation(name: str) -> bool:
    tokens = [t.lower().strip('.,') for t in name.split()]
    if not tokens:
        return True
    return tokens[-1] in _ORG_WORDS or (len(tokens) >= 2 and tokens[-2] in _ORG_WORDS)


def strip_honorific(name: str) -> str:
    """Remove any leading titles so "Dr. Leah Stokes" and "Leah Stokes" are one person."""
    return _HONORIFIC_RE.sub('', name.strip()).strip()


def _valid_name(name: str) -> bool:
    if not name or len(name) < 7: return False
    words = name.split()
    if len(words) < 2 or len(words) > 3: return False
    if words[0].lower() in _FALSE_POSITIVE_WORDS: return False
    if looks_like_organisation(name): return False
    for w in words:
        if not (w[0].isupper() or ord(w[0]) > 127): return False
    return True


# Some shows state the line-up outright — "Moderator: Michael Eyman, Managing
# Director, Origis Services" / "Guest: Dr. Charles Sims, Director for ...".
# That is a statement of role, not a shape to infer one from, and it covers
# roughly 645 episodes including 354 of Climate One's.
_LABEL_RE = re.compile(
    r'(?:^|\n)[ \t]*(Hosts?|Moderators?|Guests?|Interviewee)[ \t]*:[ \t]*(.+)',
    re.IGNORECASE
)
_HOST_LABELS = {'host', 'hosts', 'moderator', 'moderators'}


# A line that is itself a section heading ends the list of people under a
# label: Climate One follows its guests with "Highlights:" and timestamps.
_SECTION_RE = re.compile(r'^\s*[A-Z][A-Za-z /&\'-]{0,28}:\s*$')
_TIMESTAMP_RE = re.compile(r'^\s*\d{1,2}:\d{2}')

# Smart Energy Decisions' shows (and others — strip_html's own docstring
# example, "Connect with Jason Rissman", is from a different one) list guests
# one per line this way, often only in a "Resources & People Mentioned"
# footer near the end of the description.
_CONNECT_WITH_RE = re.compile(
    r'(?:^|\n)[ \t]*Connect [Ww]ith\s+([A-Z][a-zA-Z\x27’-]+(?:\s+[A-Z][a-zA-Z\x27’-]+){1,2})\s*$',
    re.MULTILINE
)


def _names_from_entry(entry: str, is_guest: bool, found: list, split_on_and: bool = True):
    # In the block form each line is one person, so an "and" there belongs to
    # their job title: "Thomas Ramey, Commercial and Nonprofit Solar Evaluator"
    # otherwise yields a second, non-existent guest.
    parts = re.split(r';|\s+(?:and|&|with)\s+', entry) if split_on_and else [entry]
    for part in parts:
        # Everything after the first comma is the person's job title.
        name = strip_honorific(part.strip().split(',')[0].strip(' .'))
        if _valid_name(name):
            found.append((name, is_guest))


# suggest() tags each suggestion with which specific pattern found it
# (e.g. "title_dash", "desc_bio_sentence") so the admin review queue can
# filter/trust batches by pattern rather than only by title-vs-description.
# But suggestions.source also gets copied straight into episode_host.
# data_source on approval (see backend/main.py's approve_suggestion), and
# several places elsewhere key off that column's value EXACTLY —
# cleanup_zero_guests.py and update_person's rename cleanup both check
# `data_source IN ('parsed_desc', 'parsed_title', ...)`. Introducing new
# fine-grained values there directly would silently break those checks, so
# this coarsens back down at the one place a suggestion's source flows into
# episode_host — the granularity stays in the suggestions table only.
def coarse_source(source: str) -> str:
    if source.startswith('title_'):
        return 'parsed_title'
    if source.startswith('desc_'):
        return 'parsed_desc'
    return source


def extract_labelled_credits(text: str) -> list[tuple[str, bool]]:
    """Names from explicit Host:/Guest:/Connect-with statements, as (name, is_guest).

    Handles the shapes seen in the feeds: the names on the same line as a
    Host:/Guest: label, the label alone on its line with one person per line
    beneath (Climate One writes 354 episodes this way), and "Connect with
    [Name]" footer lines.
    """
    found = []
    lines = (text or '').split('\n')
    for i, line in enumerate(lines):
        # Climate One writes "Episode Guests:", so a qualifier may precede the
        # label — requiring the line to begin with it missed 349 episodes'
        # worth of stated credits, including Joe Manchin's real appearance.
        match = re.match(
            r"\s*(?:Episode|Show|Our|My|The|Today\x27s|This\s+week\x27s|Featured)?\s*"
            r"(Hosts?|Moderators?|Guests?|Interviewee)"
            # "Guests included:", "Our guests were:", "Host is:"
            r"(?:\s+(?:included|include|are|were|is|was|this\s+week|today))?\s*:\s*(.*)$",
            line, re.IGNORECASE)
        if not match:
            continue
        is_guest = match.group(1).lower() not in _HOST_LABELS

        if match.group(2).strip():
            _names_from_entry(match.group(2), is_guest, found)
            continue

        # Label alone: take the people listed beneath it.
        for following in lines[i + 1:]:
            if not following.strip():
                continue
            if _SECTION_RE.match(following) or _TIMESTAMP_RE.match(following):
                break
            before = len(found)
            _names_from_entry(following, is_guest, found, split_on_and=False)
            if len(found) == before:      # a line that is not a person ends the list
                break

    for m in _CONNECT_WITH_RE.finditer(text):
        name = strip_honorific(m.group(1).strip())
        if _valid_name(name):
            found.append((name, True))

    return found


# ------------------------------------------------------------------
# WORD-BOUNDARY-SAFE NAME MATCHING
# ------------------------------------------------------------------
#
# Shared so the admin backend's create/rescan-person actions check a name
# the identical way the scheduled scanner's run() does — before this was
# unified, the backend used a plain substring test with none of the
# protections below, which is exactly the class of bug run() was already
# fixed against (see name_in_text's docstring).

_NAME_RE_CACHE = {}


def _name_pattern(full_name: str):
    """Match a full name only where it stands as a name in its own right.

    A plain substring test credits the wrong person: "dan yates" is inside
    "jordan yates", and "sara baldwin" inside "sara baldwin-griffin" — both
    real pairs in this database, and both produced wrong credits. \\b is not
    enough on its own, since it happily matches "Sara Baldwin" against
    "Sara Baldwin-Griffin" (the hyphen is a word boundary), so hyphens are
    excluded on either side as well.
    """
    pattern = _NAME_RE_CACHE.get(full_name)
    if pattern is None:
        pattern = re.compile(
            r'(?<![\w-])' + re.escape(full_name) + r'(?![\w-])',
            re.IGNORECASE
        )
        _NAME_RE_CACHE[full_name] = pattern
    return pattern


def name_in_text(full_name: str, text: str) -> bool:
    return bool(_name_pattern(full_name).search(text))


_NEXT_CAPITALIZED_WORD_RE_CACHE = {}


def first_name_belongs_to_other(first_name: str, own_last_name: str, text: str) -> bool:
    """True if this first name is attached to a DIFFERENT surname anywhere in
    the text — a sign it names someone else who happens to share the host's
    first name.

    Real incident: Outrage + Optimism registers "Fiona" (McRaith) as a host,
    but "Fiona Macklin" (a Global Optimism advisor, unrelated) and "Fiona
    Morgan" (a one-off guest) each showed up in six different episodes and
    were credited to McRaith because only the first name was checked.
    """
    pattern = _NEXT_CAPITALIZED_WORD_RE_CACHE.get(first_name)
    if pattern is None:
        pattern = re.compile(re.escape(first_name) + r"\s+([A-Z][a-zA-Z'’-]+)")
        _NEXT_CAPITALIZED_WORD_RE_CACHE[first_name] = pattern
    own_last = own_last_name.strip().lower()
    for m in pattern.finditer(text):
        candidate = m.group(1).rstrip('.,').lower()
        if candidate and candidate != own_last:
            return True
    return False
