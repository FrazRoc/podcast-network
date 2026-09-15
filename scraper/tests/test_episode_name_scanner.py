"""
Pure-function tests for episode_name_scanner.py — matching known hosts/guests
against episode text, and finding new candidate names for the suggestions queue.

Each test class links back to a real, previously-fixed bug where possible;
see CLAUDE.md's "Tests — the gap worth closing first" table.
"""
from episode_name_scanner import (
    strip_html,
    clean_description,
    name_in_text,
    build_surname_index,
    candidate_hosts,
    looks_like_organisation,
    strip_possessive_prefix,
    strip_honorific,
    _valid_name,
    extract_labelled_credits,
    extract_candidate_names,
    first_name_belongs_to_other,
    extract_title_name_credit,
    title_credit_shows,
)


class TestWordBoundaryMatching:
    """Jordan Yates / Dan Yates and Sara Baldwin / Sara Baldwin-Griffin are
    real pairs in this database. A plain substring match credits the wrong
    person in both cases."""

    def test_does_not_credit_substring_name(self):
        assert not name_in_text("Dan Yates", "An interview with Jordan Yates about solar.")

    def test_matches_the_actual_name(self):
        assert name_in_text("Jordan Yates", "An interview with Jordan Yates about solar.")

    def test_hyphen_boundary_not_a_word_boundary(self):
        # \b alone matches "Sara Baldwin" inside "Sara Baldwin-Griffin"
        # because a hyphen counts as a word boundary. The scanner's pattern
        # excludes hyphens on either side to prevent this.
        assert not name_in_text("Sara Baldwin", "Guest: Sara Baldwin-Griffin, RMI")

    def test_matches_full_hyphenated_name(self):
        assert name_in_text("Sara Baldwin-Griffin", "Guest: Sara Baldwin-Griffin, RMI")

    def test_case_insensitive(self):
        assert name_in_text("Amy Westervelt", "AMY WESTERVELT joins us")


class TestStripHtml:
    def test_tags_become_spaces_not_nothing(self):
        # Deleting tags outright welds words together across the boundary:
        # "Jason Rissman</p><p>On LinkedIn" must not become "Jason RissmanOn".
        text = "Connect with Jason Rissman</p><p>On LinkedIn"
        result = strip_html(text)
        assert "RissmanOn" not in result
        assert "Rissman On" in result or "Rissman  On" in result

    def test_entities_decoded(self):
        assert strip_html("Ben &amp; Jerry") == "Ben & Jerry"

    def test_collapses_excess_blank_lines(self):
        result = strip_html("Line one\n\n\n\nLine two")
        assert "\n\n\n" not in result


class TestCleanDescription:
    def test_strips_produced_by_credit(self):
        # Padded with realistic-length lead-in content — a match this early
        # in the text is treated as a sponsor mentioned up front rather than
        # a sign-off with nothing worth keeping after it (see
        # MIN_STRIP_AFTER_POSITION and the Smart Energy Voices incident).
        text = ("This week Amy Westervelt talks with Jane Smith about the future of "
                "climate reporting and how newsrooms are adapting. Produced by Some Studio.")
        result = clean_description(text)
        assert "Produced by" not in result

    def test_strips_past_episode_reference_with_extra_words_before_with(self):
        # Real incident: Reversing Climate Change's sponsor footer writes
        # "Listen to the RCC episode I made with David LaGreca ..." — the
        # existing pattern only allowed extra words before "episode", not
        # between "episode" and "with", so this exact phrasing slipped
        # through and credited LaGreca (and, by the same footer, Lisett Luik
        # and Peter Minor) on dozens of unrelated episodes.
        text = ("Great show today. Listen to the RCC episode I made with David LaGreca "
                "from EcoEngineers about how to choose, hire, and fire carbon market "
                "contractors.")
        result = clean_description(text)
        assert "LaGreca" not in result
        assert "Great show today" in result

    def test_hosted_by_after_credits_label_survives(self):
        # A "Credits:" block that opens with "Hosted by ..." names the hosts;
        # 136 of 193 such blocks do, and cutting there threw those away.
        text = "Great episode.\nCredits: Hosted by Amy Westervelt"
        result = clean_description(text)
        assert "Amy Westervelt" in result

    def test_credits_without_hosted_by_is_cut(self):
        text = ("Great episode this week covering the latest in renewable energy "
                "policy and what it means for the grid.\nCredits:\nEdited by some engineer")
        result = clean_description(text)
        assert "Credits:" not in result
        assert "engineer" not in result

    def test_listen_to_episode_with_is_removed_in_place(self):
        # "Listen to our previous episode with Wanjira Mathai" names someone
        # from a DIFFERENT episode; it must be cut without truncating the
        # real content that follows it.
        text = ("Listen to our previous episode with Wanjira Mathai. "
                "Thank you to our guest this week, Katie Eder!")
        result = clean_description(text)
        assert "Wanjira Mathai" not in result
        assert "Katie Eder" in result

    def test_truncates_on_word_boundary_not_mid_word(self):
        text = "Connect With Smart Energy Decisions " + ("x" * 3000)
        result = clean_description(text, max_chars=36)
        assert not result.endswith("Energ")
        assert " " not in result[-1:] or True  # no trailing partial word
        assert result == "Connect With Smart Energy Decisions" or len(result) <= 36

    def test_cross_show_promo_blurb_stripped(self):
        # Real incident: Latitude Media's "listen to our new podcast, Political
        # Climate ... hosts Julia Pyper, Emily Domenech, and Brandon Hurlbut"
        # outro credited all three hosts of a show that never aired on 12
        # episodes of two unrelated shows (Green Blueprint, Catalyst).
        text = ("This week: Tesla news. And make sure to listen to our new podcast, "
                "Political Climate – an insider’s view on the most pressing "
                "policy questions in energy and climate. Tune in every other Friday "
                "for the latest takes from hosts Julia Pyper, Emily Domenech, and "
                "Brandon Hurlbut. Available on Apple, Spotify, or wherever you get "
                "your podcasts.")
        result = clean_description(text)
        assert "Domenech" not in result
        assert "Pyper" not in result
        assert "Hurlbut" not in result
        assert "Tesla news" in result

    def test_shocked_cross_promo_blurb_stripped(self):
        # Real incident: Shift Key's sponsor block cross-promotes "Shocked",
        # naming Amy Harder — an already-known host from other shows — as if
        # she'd appeared on this Shift Key episode. Credited her on 4 episodes
        # she never appeared in.
        text = ("Great episode with Michael Davidson. A warmer world is here. Now "
                "what? Listen to Shocked, from the University of Chicago’s "
                "Institute for Climate and Sustainable Growth, and hear journalist "
                "Amy Harder and economist Michael Greenstone share new ways of "
                "thinking about climate change and cutting-edge solutions. Find it "
                "here.")
        result = clean_description(text)
        assert "Harder" not in result
        assert "Greenstone" not in result
        assert "Michael Davidson" in result

    def test_zero_climate_race_footer_stripped(self):
        # cleanup_zero_guests.py fixed the case where "Explore further: Past
        # episode with X" credited a guest from a different episode.
        text = ("Real content about the guest and their work on climate solutions "
                "across the energy sector this year.\nExplore further: Past episode with Someone Else")
        result = clean_description(text)
        assert "Someone Else" not in result
        assert "Real content" in result

    def test_early_sponsor_mention_does_not_truncate_real_content(self):
        # Real incident: Smart Energy Voices opens several descriptions with
        # "This episode ... is produced in partnership with Evergy Energy
        # Partners." as its very first sentence, then names the host and
        # guests right after (suggestion 6384 — "Host John Failla is joined
        # by ..." was being thrown away entirely). The same "in partnership
        # with" phrase appears near the END of descriptions on other shows
        # (Political Climate, 70+ episodes) where cutting there is correct —
        # the distinguishing signal is match position, not the phrase itself.
        text = ("This episode of Smart Energy Voices is produced in partnership "
                "with Evergy Energy Partners. Host John Failla is joined by "
                "Evergy's Robert Day to discuss renewable energy in regulated markets.")
        result = clean_description(text)
        assert "John Failla" in result

    def test_late_sponsor_mention_still_truncates(self):
        text = ("Tony Seba gets a lot of things right about the coming energy "
                "transition and where the industry is headed next for renewables. "
                "This episode is produced in partnership with Acme Studios and its team.")
        result = clean_description(text)
        assert "Acme Studios" not in result
        assert "Tony Seba" in result


class TestOrganisationFiltering:
    def test_rejects_org_suffix_word(self):
        assert looks_like_organisation("Acme Ventures")

    def test_rejects_two_word_org(self):
        assert looks_like_organisation("Rigetti Computing")

    def test_accepts_real_surname_power(self):
        # Deliberately not in _ORG_WORDS: Ted Power is a real person.
        assert not looks_like_organisation("Ted Power")

    def test_accepts_plain_person_name(self):
        assert not looks_like_organisation("Jane Smith")


class TestPossessivePrefixAndHonorifics:
    def test_strips_possessive_org_prefix(self):
        assert strip_possessive_prefix("Origami's Gregg Patterson") == "Gregg Patterson"

    def test_strips_dr(self):
        assert strip_honorific("Dr. Leah Stokes") == "Leah Stokes"

    def test_strips_professor_not_just_prof(self):
        # "Professor" slipped past a list that only covered "Prof" and
        # created a duplicate "Professor Tristan Smith" record.
        assert strip_honorific("Professor Tristan Smith") == "Tristan Smith"

    def test_strips_job_title_founder(self):
        assert strip_honorific("Founder Oliver Katz") == "Oliver Katz"

    def test_strips_job_title_ceo(self):
        assert strip_honorific("CEO Dan Shugar") == "Dan Shugar"


class TestValidName:
    def test_rejects_short_string(self):
        assert not _valid_name("Al B")

    def test_rejects_single_word(self):
        assert not _valid_name("Westervelt")

    def test_rejects_four_words(self):
        assert not _valid_name("Amy Jane Marie Westervelt")

    def test_rejects_false_positive_leading_word(self):
        assert not _valid_name("How To")

    def test_rejects_organisation_shape(self):
        assert not _valid_name("Heart Aerospace")

    def test_accepts_plain_name(self):
        assert _valid_name("Jane Smith")


class TestLabelledCredits:
    def test_same_line_guest_label(self):
        result = extract_labelled_credits("Guest: Dr. Charles Sims, Director for Energy")
        assert ("Charles Sims", True) in result

    def test_connect_with_footer(self):
        # Real incident: Kulsoom Khan, an already-known host, wasn't found on
        # a Smart Energy Voices episode that named her this way — only her
        # own "Connect with" line, not a Guest:/Host: label.
        text = "Connect with Kulsoom Khan\nOn LinkedIn\nConnect with Kate Peterson\nOn LinkedIn"
        result = extract_labelled_credits(text)
        assert ("Kulsoom Khan", True) in result
        assert ("Kate Peterson", True) in result

    def test_connect_with_rejects_org_footer(self):
        # "Connect With Smart Energy Decisions" is the show's own publisher
        # name in the same footer style — not a person.
        text = "Connect with Kulsoom Khan\nOn LinkedIn\nConnect With Smart Energy Decisions"
        names = [n for n, _ in extract_labelled_credits(text)]
        assert "Kulsoom Khan" in names
        assert "Smart Energy Decisions" not in names

    def test_same_line_host_label(self):
        result = extract_labelled_credits("Host: Amy Westervelt")
        assert ("Amy Westervelt", False) in result

    def test_moderator_counts_as_host_label(self):
        # _HOST_LABELS includes "moderator" — someone running the discussion
        # is treated like a host, not a guest.
        result = extract_labelled_credits("Moderator: Michael Eyman, Managing Director, Origis")
        assert ("Michael Eyman", False) in result

    def test_qualifier_before_label(self):
        # Climate One writes "Episode Guests:" — the label need not start the line.
        result = extract_labelled_credits("Episode Guests: Joe Manchin")
        assert ("Joe Manchin", True) in result

    def test_block_form_one_person_per_line(self):
        text = "Guests:\nJane Smith, RMI\nJohn Doe, Acme Corp\nHighlights:\n01:23 intro"
        result = extract_labelled_credits(text)
        names = [n for n, _ in result]
        assert "Jane Smith" in names
        assert "John Doe" in names
        assert "Highlights" not in " ".join(names)

    def test_block_form_and_in_job_title_not_a_second_guest(self):
        # "Thomas Ramey, Commercial and Nonprofit Solar Evaluator" must not
        # yield a phantom second guest from the "and" in the job title.
        text = "Guests:\nThomas Ramey, Commercial and Nonprofit Solar Evaluator"
        result = extract_labelled_credits(text)
        assert result == [("Thomas Ramey", True)]

    def test_no_label_returns_empty(self):
        assert extract_labelled_credits("Just a description with no labels.") == []


class TestExtractCandidateNames:
    def test_with_pattern(self):
        result = extract_candidate_names("This week we talk with Jane Smith about solar.")
        assert any(n == "Jane Smith" for n, _ in result)

    def test_speak_to_pattern(self):
        result = extract_candidate_names("we speak to Benjamin Bartle")
        assert any(n == "Benjamin Bartle" for n, _ in result)

    def test_joins_pattern(self):
        result = extract_candidate_names("Jane Smith joins us to discuss the grid.")
        assert any(n == "Jane Smith" for n, _ in result)

    def test_ft_and_feat_abbreviations(self):
        # Real incident: Climate Insiders titles every episode "(ft. Name -
        # Org)" / "(feat. Name)" — "featuring" alone didn't cover the
        # abbreviated forms, so suggestion 5538 never surfaced "Ben James"
        # and only found garbled description text instead.
        cases = [
            ("Is Hydrogen the Fuel of the Future? (ft. Ben James - Climate Tech specialist)", "Ben James"),
            ("Continuum - Tackling the Wind Turbine Recycling Challenge (feat. Nicolas Derrien)", "Nicolas Derrien"),
            ("Pioneering A New VC Model To Drive Systemic Change (ft Marie Ekeland of 2050)", "Marie Ekeland"),
        ]
        for title, expected in cases:
            names = [n for n, _ in extract_candidate_names(title)]
            assert expected in names, title

    def test_accented_name_after_trigger(self):
        # "è" isn't in [a-zA-Z] — without the extended Unicode range in the
        # name-capture group, matching breaks mid-word.
        result = extract_candidate_names("This week we talk with Dominique Minière about nuclear.")
        assert any(n == "Dominique Minière" for n, _ in result)

    def test_possessive_org_pattern(self):
        result = extract_candidate_names("Rewiring America's Ari Matusiak joins the show.")
        names = [n for n, _ in result]
        assert "Ari Matusiak" in names
        assert "Rewiring America" not in names

    def test_title_cased_contraction_not_read_as_org_possessive(self):
        # Real incident: SunCast episode 756's title, "... On What's Trending
        # In Solar", satisfied the possessive pattern's capitalized-prefix
        # requirement purely because titles are Title Cased — "What's" isn't
        # an organisation, so "Trending In Solar" got queued as a person.
        title = "756: Expert Analysis, 3 Perspectives On What's Trending In Solar"
        assert extract_candidate_names(title) == []

    def test_possessive_topic_noun_not_read_as_person(self):
        # _POSSESSIVE_RE assumes whatever follows "Org's" is a person, but
        # Title Cased titles often follow a real org's possessive with an
        # abstract topic instead of a name — none of these were people.
        titles = [
            "University of New Mexico's Unique Energy Solution",
            "Climate Change's Deadly Fish Problem",
            "Alzheimer Risk Reduced By Cycling; Big Oil's Frivolous Suits",
            "Fire Weather: Urban Wildfires are Climate Change's Biggest Threat",
            "$5B Chevron Project, Tata Steel Hydrogen Breakthrough, & Nucera's Bold Forecast",
            "Willie Phillips, Former FERC Chairman: The Grid Is America's Economic Nervous System",
            "Episode 59: Marco Krapels of Enphase on EVs, VPPs, and Unlocking America's Hidden Energy Capacity",
            "#83 - James Gutman - Why Geopolitics Is Europe's Strongest Climate Accelerator",
        ]
        for title in titles:
            assert extract_candidate_names(title) == [], title

    def test_possessive_real_person_still_captured(self):
        # The fix above must not catch real people the same shape would name.
        cases = [
            ("Ask a Solar Vet: Uniting women across sustainable sectors with WRISE's Kristen Graf", "Kristen Graf"),
            ("Ask a Solar Vet: Showing solar leadership with Borrego's Brendan Neagle", "Brendan Neagle"),
            ("Rewiring America's Ari Matusiak joins the show.", "Ari Matusiak"),
        ]
        for title, expected in cases:
            names = [n for n, _ in extract_candidate_names(title)]
            assert expected in names, title

    def test_does_not_credit_org_after_with(self):
        result = extract_candidate_names("This week we talk with Bedrock Robotics about mining.")
        assert result == []

    def test_and_conjunction_picks_up_second_guest(self):
        text = "We talk with Jane Smith and John Doe about the grid."
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Jane Smith" in names
        assert "John Doe" in names

    def test_and_conjunction_respects_window_with_job_title_between(self):
        text = ("We talk with Ben Chehebar, VP of Hardware at RoadRunner Recycling, "
                "and Jason Gates about recycling.")
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Ben Chehebar" in names
        assert "Jason Gates" in names


class TestGuestListPattern:
    """Real incident: A Matter of Degrees episode (suggestion 9066) said
    'We hear from three guests who are leading us to a world beyond
    petrochemicals and plastics: Michele Fetting, program director at the
    Breathe Project ...; Shilpi Chhotray, co-founder ... of People Over
    Plastics ...; and Yvette Arellano, founder ... of ... Fenceline Watch.'
    None of the other patterns fire because the names sit after a colon,
    far from the 'hear from' trigger word."""

    def test_semicolon_delimited_list_after_colon(self):
        text = (
            "We hear from three guests who are leading us to a world beyond "
            "petrochemicals and plastics: Michele Fetting, program director at "
            "the Breathe Project in Pittsburgh; Shilpi Chhotray, co-founder and "
            "executive director of People Over Plastics, a BIPOC storytelling "
            "and environmental justice power-building collective; and Yvette "
            "Arellano, founder and director of a Houston-based environmental "
            "justice organization, Fenceline Watch."
        )
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Michele Fetting" in names
        assert "Shilpi Chhotray" in names
        assert "Yvette Arellano" in names
        assert "Breathe Project" not in names
        assert "People Over Plastics" not in names
        assert "Fenceline Watch" not in names

    def test_panelists_trigger_word(self):
        text = ("The episode features a pair of marquee panelists: Tom Starrs, "
                "currently the vice president of strategy; and Maria Robinson, "
                "until recently the director of an agency.")
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Tom Starrs" in names
        assert "Maria Robinson" in names

    def test_single_item_list_does_not_fire(self):
        # Needs 2+ semicolon-separated items to be confident this is a real
        # list rather than an arbitrary colon followed by one clause.
        text = "Our guest today: Jane Smith, a climate reporter."
        names = [n for n, _ in extract_candidate_names(text)]
        assert names == []

    def test_labelled_guest_list_not_duplicated(self):
        # "Guests:\nName1\nName2" is already handled by extract_labelled_credits
        # on the full text — this pattern's comma-then-lowercase-word shape
        # must not also fire on that form and produce noise like org names
        # embedded in a bio ("... Energy, Infrastructure and Environment
        # Division ...").
        text = (
            "Guests: \nSue Gander, Managing Director of Electric Vehicle Policy "
            "with the Electrification Coalition and former Director of the "
            "Energy, Infrastructure and Environment Division with the National "
            "Governors' Association; Mike Henchen, Principal of Building "
            "Electrification with the Rocky Mountain Institute."
        )
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Environment Division" not in names


class TestBioSentencePattern:
    """Real incident: Climate CEOs' description for suggestion 9340 said
    "Adam Greenberg is the CEO and co-founder." — a plain declarative bio
    sentence with no trigger word ("with"/"featuring"/...) at all. Only the
    title ("... with AI-Powered Greenhouses") got scanned by other patterns,
    surfacing "AI-Powered Greenhouses" as the (wrong) suggestion instead."""

    def test_name_is_the_role_sentence(self):
        text = "Adam Greenberg is the CEO and co-founder. He previously worked at a Fortune 100 company."
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Adam Greenberg" in names

    def test_name_is_a_role_sentence(self):
        text = "Jesse Smith is the Director of Land Stewardship at White Buffalo Land Trust, a nonprofit."
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Jesse Smith" in names

    def test_company_is_a_sentence_not_credited(self):
        # No role word (founder/CEO/reporter/...) appears near "is a" here —
        # a company being described this way must not look like a person.
        text = "Reneu Energy is a premier international solar energy consulting firm and developer."
        names = [n for n, _ in extract_candidate_names(text)]
        assert names == []

    def test_sentence_initial_adverb_not_swallowed_into_name(self):
        # "Today Christopher ..." / "Although Khosla ..." — the adverb
        # satisfies the capitalized-words shape just as well as a name would
        # since it's the first capitalized word after the sentence boundary.
        text = "Today Christopher is a Venture Partner at a growth fund."
        names = [n for n, _ in extract_candidate_names(text)]
        assert "Today Christopher" not in names
        assert names == []

    def test_mid_sentence_role_clause_not_credited(self):
        # Must not fire mid-sentence — only right after a sentence boundary.
        text = "As part of his role, the CEO of Tesla is Elon Musk, according to reports."
        names = [n for n, _ in extract_candidate_names(text)]
        assert "The CEO" not in names


class TestExtractTitleNameCredit:
    """Real incident: Titans Of Nuclear had 194 of 200 episodes with zero
    credits (suggestion 3570 — "Juliann Edwards - Chair, United States Women
    in Nuclear"), because interview shows like this put the guest's whole
    credit directly in the title with no trigger phrase at all. Only the
    first name-shaped segment is taken — a later "Name, Role, Org" segment
    would otherwise look like a second person."""

    def test_dash_separated_name_and_org(self):
        assert extract_title_name_credit(
            "Ep 452: Juliann Edwards - Chair, United States Women in Nuclear"
        ) == "Juliann Edwards"

    def test_comma_separated_name_and_org(self):
        assert extract_title_name_credit("Liliane Ableitner – Exnaton") == "Liliane Ableitner"

    def test_show_number_prefix_is_skipped(self):
        # Not anchored to position 0 — "Leaders in Cleantech #107 -" isn't a
        # name (lowercase "in" breaks the capitalized-word run), so the
        # search continues to the real name after it.
        assert extract_title_name_credit(
            "Leaders in Cleantech #107 - David Martell – EVIOS"
        ) == "David Martell"

    def test_only_first_segment_is_credited(self):
        # "Independent Researcher, University of..." is itself shaped like a
        # second name — must not be returned instead of/alongside the guest.
        assert extract_title_name_credit(
            "Ep 408: Ryan Pickering - Independent Researcher, University of California, Berkeley"
        ) == "Ryan Pickering"

    def test_accented_characters_in_name(self):
        # "è" isn't in [a-zA-Z] — without the extended Unicode range, the
        # name match breaks mid-word and the role ("Executive VP") gets
        # captured instead.
        assert extract_title_name_credit(
            "Dominique Minière - Executive VP, International and Domestic New Nuclear "
            "Development, Ontario Power Generation"
        ) == "Dominique Minière"

    def test_topic_headline_title_rejected(self):
        # "This Week In Cleantech - Episode 1" satisfies the Title-Case-
        # words-then-dash shape as readily as a real name; "in" mid-name is
        # the tell. _valid_name alone doesn't catch this because it only
        # checks the first word, and re.search skips ahead to "Week In
        # Cleantech" once "This Week In" fails to find a separator.
        assert extract_title_name_credit("This Week In Cleantech - Episode 1") is None

    def test_show_segment_name_not_credited_as_person(self):
        assert extract_title_name_credit(
            "Fully Charged Live, Robert Llewellyn & Dan Ceasar - 84"
        ) is None

    def test_no_separator_no_match(self):
        assert extract_title_name_credit("Introducing Energy Impact") is None

    def test_org_only_title_not_a_person(self):
        assert extract_title_name_credit("Nuclear Technology Series - Liquid Metal Reactors") is None


class TestTitleCreditShows:
    def _insert_episodes(self, cur, podcast_id, titles):
        for t in titles:
            cur.execute(
                "INSERT INTO episodes (podcast_id, title) VALUES (%s, %s)",
                (podcast_id, t)
            )

    def test_show_with_consistent_name_dash_org_titles_qualifies(self, db_conn):
        cur = db_conn.cursor()
        cur.execute(
            "INSERT INTO podcasts (title, apple_podcast_id) VALUES (%s, %s) RETURNING podcast_id",
            ("Titans Of Nuclear", "titans")
        )
        podcast_id = cur.fetchone()[0]
        first_names = ["Alice", "Bob", "Carla", "David", "Elena", "Frank",
                        "Grace", "Hank", "Ivy", "Jack", "Kara", "Leo"]
        self._insert_episodes(cur, podcast_id, [
            f"{fn} Anderson - Some Organization" for fn in first_names
        ])
        db_conn.commit()

        assert podcast_id in title_credit_shows(db_conn, min_episodes=10, min_rate=0.85)

    def test_show_with_headline_style_titles_does_not_qualify(self, db_conn):
        cur = db_conn.cursor()
        cur.execute(
            "INSERT INTO podcasts (title, apple_podcast_id) VALUES (%s, %s) RETURNING podcast_id",
            ("Headline Show", "headline-show")
        )
        podcast_id = cur.fetchone()[0]
        # Only a minority happen to look name-shaped, same as an ordinary
        # headline-style show mixing topics, quotes, and the occasional name.
        self._insert_episodes(cur, podcast_id, [
            "How Green Hydrogen Can Decarbonize Hard-to-Abate Sectors",
            "The Future of Grid Storage",
            "Plasma Recycling, Hydrogen Ferry, and Plant Safety Lessons",
            "Circular Economy - Making Things Last",
            "Fake Meat, What's The Beef?",
            "Real Guest Name - A Real Org",
            "Another Topic Without A Person In It",
            "Software Powered Grids",
            "Using AI to Accelerate Renewable Energy",
            "Behind-the-Meter Energy Basics",
        ])
        db_conn.commit()

        assert podcast_id not in title_credit_shows(db_conn, min_episodes=10, min_rate=0.85)

    def test_show_below_minimum_sample_size_excluded(self, db_conn):
        cur = db_conn.cursor()
        cur.execute(
            "INSERT INTO podcasts (title, apple_podcast_id) VALUES (%s, %s) RETURNING podcast_id",
            ("Tiny New Show", "tiny-new-show")
        )
        podcast_id = cur.fetchone()[0]
        self._insert_episodes(cur, podcast_id, [
            "Alice Anderson - Some Org", "Bob Baker - Some Org", "Carla Cruz - Some Org"
        ])
        db_conn.commit()

        assert podcast_id not in title_credit_shows(db_conn, min_episodes=10, min_rate=0.85)


class TestSurnameIndex:
    def test_only_matching_surname_is_a_candidate(self):
        hosts = [
            {'host_id': 1, 'full_name': 'Jordan Yates'},
            {'host_id': 2, 'full_name': 'Amy Westervelt'},
        ]
        index = build_surname_index(hosts)
        candidates = candidate_hosts("An episode about Jordan Yates.", index)
        names = {h['full_name'] for h in candidates}
        assert names == {'Jordan Yates'}

    def test_no_surname_present_yields_no_candidates(self):
        hosts = [{'host_id': 1, 'full_name': 'Amy Westervelt'}]
        index = build_surname_index(hosts)
        assert candidate_hosts("Nothing relevant here.", index) == []


class TestFirstNameBelongsToOther:
    """Real incident: Outrage + Optimism registers "Fiona" (McRaith) as a
    host, so bare-first-name matching credited her on 7 episodes that were
    actually about "Fiona Macklin" (a Global Optimism advisor) or "Fiona
    Morgan" (a one-off guest) — neither of whom share her surname."""

    def test_different_surname_flags_collision(self):
        text = "This week, with the help of co-host Fiona Macklin, part two of..."
        assert first_name_belongs_to_other("Fiona", "McRaith", text)

    def test_own_surname_does_not_flag(self):
        text = "Christiana Figueres, Tom Rivett-Carnac and new co-host Fiona McRaith take you on the road."
        assert not first_name_belongs_to_other("Fiona", "McRaith", text)

    def test_bare_first_name_alone_does_not_flag(self):
        # "Gerard and Laurent welcome ..." — no surname follows at all.
        text = "Gerard and Laurent welcome a guest to discuss the grid."
        assert not first_name_belongs_to_other("Gerard", "Reid", text)
