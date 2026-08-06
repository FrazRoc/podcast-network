-- ============================================================
-- Colorado Current: Clean Energy Podcast Network Schema
-- Branch: clean-energy-podcasts
-- Key decisions:
--   - apple_podcast_id is the PRIMARY external key (stable, universally available)
--   - podchaser_id is OPTIONAL enrichment (added when available)
--   - This allows scraping to proceed even before Podchaser IDs are confirmed
--   - Fixed: channels table moved before podcasts (FK ordering bug)
--   - Fixed: DROP order includes channels
--   - Added: linkedin_url on hosts (for LinkedIn export feature)
--   - Added: focus_area, target_audience on podcasts (from curated sheet)
-- ============================================================

-- Drop existing tables (order matters for FK dependencies)
DROP TABLE IF EXISTS episode_tag CASCADE;
DROP TABLE IF EXISTS tags CASCADE;
DROP TABLE IF EXISTS episode_host CASCADE;
DROP TABLE IF EXISTS host_podcast CASCADE;
DROP TABLE IF EXISTS episodes CASCADE;
DROP TABLE IF EXISTS podcasts CASCADE;
DROP TABLE IF EXISTS hosts CASCADE;
DROP TABLE IF EXISTS podcast_tracking CASCADE;
DROP TABLE IF EXISTS host_social_links CASCADE;
DROP TABLE IF EXISTS host_roles CASCADE;
DROP TABLE IF EXISTS genres CASCADE;
DROP TABLE IF EXISTS podcast_genres CASCADE;
DROP TABLE IF EXISTS channels CASCADE;

-- ============================================================
-- CHANNELS (must come before podcasts due to FK reference)
-- ============================================================
CREATE TABLE channels (
    channel_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- HOSTS
-- ============================================================
CREATE TABLE hosts (
    host_id SERIAL PRIMARY KEY,
    podchaser_id VARCHAR(100) UNIQUE,           -- optional: populated via Podchaser enrichment
    first_name TEXT,
    last_name TEXT,
    email TEXT UNIQUE,
    bio TEXT,
    profile_image_url TEXT,
    twitter_handle VARCHAR(100),
    linkedin_url TEXT,                          -- for LinkedIn export feature
    wikipedia TEXT,
    website_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (first_name, last_name)
);

-- ============================================================
-- PODCASTS
-- ============================================================
CREATE TABLE podcasts (
    podcast_id SERIAL PRIMARY KEY,
    apple_podcast_id VARCHAR(100) UNIQUE NOT NULL,  -- PRIMARY external key
    podchaser_id VARCHAR(100) UNIQUE,               -- optional enrichment
    title VARCHAR(500) NOT NULL UNIQUE,
    description TEXT,
    focus_area TEXT,                                -- from curated sheet
    target_audience TEXT,                           -- from curated sheet
    cover_art_url TEXT,
    rss_feed_url TEXT,
    website_url TEXT,
    language VARCHAR(100),
    channel_id INTEGER REFERENCES channels(channel_id),
    rating_count INTEGER,
    average_rating DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- EPISODES
-- ============================================================
CREATE TABLE episodes (
    episode_id SERIAL PRIMARY KEY,
    podcast_id INTEGER REFERENCES podcasts(podcast_id),
    apple_episode_id VARCHAR(100) UNIQUE,           -- PRIMARY external key for episodes
    podchaser_id VARCHAR(100) UNIQUE,               -- optional enrichment
    title VARCHAR(500) NOT NULL,
    episode_number INTEGER,
    season_number INTEGER,
    description TEXT,
    audio_url TEXT,
    duration_seconds INTEGER,
    published_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (podcast_id, title)
);

-- ============================================================
-- JUNCTION: HOST <-> PODCAST (regular hosts/co-hosts)
-- ============================================================
CREATE TABLE host_podcast (
    host_id INTEGER REFERENCES hosts(host_id),
    podcast_id INTEGER REFERENCES podcasts(podcast_id),
    role VARCHAR(100),
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (host_id, podcast_id)
);

-- ============================================================
-- JUNCTION: HOST <-> EPISODE (hosts + guests per episode)
-- This is the core table for the network graph edges
-- ============================================================
CREATE TABLE episode_host (
    episode_id INTEGER REFERENCES episodes(episode_id),
    host_id INTEGER REFERENCES hosts(host_id),
    is_guest BOOLEAN DEFAULT FALSE,
    role VARCHAR(100),
    PRIMARY KEY (episode_id, host_id)
);

-- ============================================================
-- TAGS
-- ============================================================
CREATE TABLE tags (
    tag_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE
);

CREATE TABLE episode_tag (
    episode_id INTEGER REFERENCES episodes(episode_id),
    tag_id INTEGER REFERENCES tags(tag_id),
    PRIMARY KEY (episode_id, tag_id)
);

-- ============================================================
-- HOST SOCIAL LINKS
-- ============================================================
CREATE TABLE host_social_links (
    link_id SERIAL PRIMARY KEY,
    host_id INTEGER REFERENCES hosts(host_id),
    platform VARCHAR(50),
    url TEXT,
    UNIQUE(host_id, platform)
);

-- ============================================================
-- HOST ROLES (cross-podcast role history)
-- ============================================================
CREATE TABLE host_roles (
    role_id SERIAL PRIMARY KEY,
    host_id INTEGER REFERENCES hosts(host_id),
    podcast_name TEXT,
    role_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    UNIQUE(host_id, podcast_name, role_name)
);

-- ============================================================
-- GENRES
-- ============================================================
CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE podcast_genres (
    podcast_id INTEGER REFERENCES podcasts(podcast_id),
    genre_id INTEGER REFERENCES genres(genre_id),
    is_primary BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (podcast_id, genre_id)
);

-- ============================================================
-- PODCAST TRACKING (scrape state management)
-- Keyed on apple_podcast_id; podchaser_id populated when available
-- ============================================================
CREATE TABLE podcast_tracking (
    tracking_id SERIAL PRIMARY KEY,
    apple_podcast_id VARCHAR(100) UNIQUE,           -- primary key for tracking; NULL until looked up
    podchaser_id VARCHAR(100) UNIQUE,               -- optional: filled in after lookup
    podcast_title VARCHAR(500),                     -- human-readable label for logging
    last_scraped_at TIMESTAMP,
    scrape_count INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'pending',           -- 'pending', 'in_progress', 'success', 'failed'
    error_message TEXT,
    total_episodes INTEGER,
    latest_episode_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX idx_episodes_podcast_id ON episodes(podcast_id);
CREATE INDEX idx_episode_host_host_id ON episode_host(host_id);
CREATE INDEX idx_host_podcast_podcast_id ON host_podcast(podcast_id);
CREATE INDEX idx_episodes_published_date ON episodes(published_date);
CREATE INDEX idx_podcast_tracking_apple_id ON podcast_tracking(apple_podcast_id);
CREATE INDEX idx_podcast_tracking_podchaser_id ON podcast_tracking(podchaser_id);
CREATE INDEX idx_podcast_apple_id ON podcasts(apple_podcast_id);
CREATE INDEX idx_podcast_podchaser_id ON podcasts(podchaser_id);
CREATE INDEX idx_host_podchaser_id ON hosts(podchaser_id);
CREATE INDEX idx_episode_apple_id ON episodes(apple_episode_id);
CREATE INDEX idx_episode_podchaser_id ON episodes(podchaser_id);
CREATE INDEX idx_genre_name ON genres(name);
CREATE INDEX idx_podcast_genres_primary ON podcast_genres(podcast_id) WHERE is_primary = true;
CREATE INDEX idx_podcast_channel_id ON podcasts(channel_id);

-- ============================================================
-- SEED DATA: clean energy podcast list
-- apple_podcast_id is required; podchaser_id filled in where confirmed
-- ============================================================
INSERT INTO podcast_tracking (apple_podcast_id, podchaser_id, podcast_title, status) VALUES
    -- Apple ID             Podchaser ID   Title
    (NULL,                  '851883',      'Inevitable (formerly My Climate Journey)',  'pending'),
    ('1593204897',          '2153206',     'Catalyst with Shayle Kann',                'pending'),
    (NULL,                  '1359342',     'Cleaning Up',                              'pending'),
    ('663379413',           '109600',      'The Energy Gang',                          'pending'),
    (NULL,                  '3737431',     'Volts',                                    'pending'),
    (NULL,                  '877656',      'Switched On',                              'pending'),
    (NULL,                  '378051',      'The Interchange',                          'pending'),
    (NULL,                  '541293',      'The Interchange: Recharged',               'pending'),
    (NULL,                  '854652',      'Watt It Takes',                            'pending'),
    (NULL,                  '656398',      'Political Climate',                        'pending'),
    (NULL,                  '2153207',     'The Carbon Copy',                          'pending'),
    (NULL,                  '4826131',     'Zero: The Climate Race',                   'pending'),
    (NULL,                  '742294',      'Redefining Energy',                        'pending'),
    (NULL,                  '1979929',     'Climate Tech Cocktails',                   'pending'),
    (NULL,                  '934211',      'Climate Rising',                           'pending'),
    (NULL,                  '197171',      'The Energy Transition Show',               'pending'),
    (NULL,                  '1501741',     'A Matter of Degrees',                      'pending'),
    (NULL,                  '873859',      'Outrage + Optimism',                       'pending'),
    (NULL,                  '1544490',     'Leaders in Cleantech',                     'pending'),
    (NULL,                  '5769839',     'Supercool',                                'pending'),
    (NULL,                  '1531238',     'Climate Question',                         'pending'),
    (NULL,                  '805995',      'Energy Unplugged',                         'pending'),
    (NULL,                  '5985497',     'Open Circuit',                             'pending'),
    (NULL,                  '4288186',     'Factor This!',                             'pending'),
    (NULL,                  NULL,          'Critical Capital',                         'pending'),
    ('1081481629',          '40484',       'Columbia Energy Exchange',                 'pending'),
    (NULL,                  '463390',      'CleanTech Talk',                           'pending'),
    (NULL,                  '1934775',     'The Big Switch',                           'pending'),
    (NULL,                  '1944634',     'How We Survive',                           'pending'),
    (NULL,                  '748053',      'Drilled',                                  'pending'),
    ('1593203014',          '4007204',     'The Green Blueprint',                      'pending'),
    (NULL,                  '1257328',     'Build Repeat.',                            'pending'),
    (NULL,                  '4752417',     'Hardware to Save a Planet',                'pending'),
    (NULL,                  '5370888',     'With Great Power',                         'pending'),
    (NULL,                  '2020250',     'Energy Transition Solutions',              'pending'),
    (NULL,                  '4147481',     'The Great Simplification',                 'pending'),
    (NULL,                  '2199181',     'Smart Energy Voices',                      'pending'),
    (NULL,                  NULL,          'DER Task Force Podcast',                   'pending'),
    (NULL,                  NULL,          'The Carbon Removal Show',                  'pending'),
    (NULL,                  NULL,          'ClimateBiz',                               'pending'),
    (NULL,                  NULL,          'Nuclear Barbarians',                       'pending'),
    (NULL,                  NULL,          'Energy Central Power Perspectives',        'pending'),
    (NULL,                  NULL,          'Decarbonizing Commerce',                   'pending'),
    (NULL,                  NULL,          'The Clean Energy Show',                    'pending'),
    (NULL,                  NULL,          'Climate Capital Podcast',                  'pending'),
    ('1561411048',          '2176818',     'Climate CEOs',                             'pending'),
    (NULL,                  '604598',      'Titans of Nuclear',                        'pending'),
    ('296762605',           '14444',       'Climate One',                              'pending'),
    ('1541394865',          '1569388',     'Where the Internet Lives',                 'pending'),
    (NULL,                  NULL,          'Climate Insiders',                         'pending'),
    (NULL,                  NULL,          'The Tech 4 Climate Podcast',               'pending')
ON CONFLICT DO NOTHING;
