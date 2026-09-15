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
        if match:
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
