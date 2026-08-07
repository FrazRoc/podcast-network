-- ============================================================
-- Colorado Current: Clean Energy Podcast Network Schema
-- Branch: clean-energy-podcasts
-- Key decisions:
--   - apple_podcast_id is the PRIMARY external key (stable, universally available)
--   - podchaser_id is OPTIONAL enrichment (added when available)
--   - Fixed: channels table moved before podcasts (FK ordering bug)
--   - Fixed: DROP order includes channels
--   - Added: linkedin_url on hosts (for LinkedIn export feature)
--   - Added: focus_area, target_audience on podcasts (from curated sheet)
-- ============================================================

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
-- CHANNELS
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
    podchaser_id VARCHAR(100) UNIQUE,
    first_name TEXT,
    last_name TEXT,
    email TEXT UNIQUE,
    bio TEXT,
    profile_image_url TEXT,
    twitter_handle VARCHAR(100),
    linkedin_url TEXT,
    wikipedia TEXT,
    website_url TEXT,
    data_source VARCHAR(50) DEFAULT 'apple_verified',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (first_name, last_name)
);

-- ============================================================
-- PODCASTS
-- ============================================================
CREATE TABLE podcasts (
    podcast_id SERIAL PRIMARY KEY,
    apple_podcast_id VARCHAR(100) UNIQUE NOT NULL,
    podchaser_id VARCHAR(100) UNIQUE,
    title VARCHAR(500) NOT NULL UNIQUE,
    description TEXT,
    focus_area TEXT,
    target_audience TEXT,
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
    apple_episode_id VARCHAR(100) UNIQUE,
    podchaser_id VARCHAR(100) UNIQUE,
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
-- JUNCTION: HOST <-> PODCAST
-- ============================================================
CREATE TABLE host_podcast (
    host_id INTEGER REFERENCES hosts(host_id),
    podcast_id INTEGER REFERENCES podcasts(podcast_id),
    role VARCHAR(100),
    start_date DATE,
    end_date DATE,
    data_source VARCHAR(50) DEFAULT 'apple_verified',
    PRIMARY KEY (host_id, podcast_id)
);

-- ============================================================
-- JUNCTION: HOST <-> EPISODE (core network graph edges)
-- ============================================================
CREATE TABLE episode_host (
    episode_id INTEGER REFERENCES episodes(episode_id),
    host_id INTEGER REFERENCES hosts(host_id),
    is_guest BOOLEAN DEFAULT FALSE,
    role VARCHAR(100),
    data_source VARCHAR(50) DEFAULT 'apple_verified',
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
-- HOST ROLES
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
    apple_genre_id VARCHAR(50),
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
-- PODCAST TRACKING
-- ============================================================
CREATE TABLE podcast_tracking (
    tracking_id SERIAL PRIMARY KEY,
    apple_podcast_id VARCHAR(100) UNIQUE NOT NULL,
    podchaser_id VARCHAR(100) UNIQUE,
    podcast_title VARCHAR(500),
    last_scraped_at TIMESTAMP,
    scrape_count INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'pending',
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
-- SEED DATA: 49 clean energy podcasts
-- apple_podcast_id required; podchaser_id where confirmed
-- ============================================================
INSERT INTO podcast_tracking (apple_podcast_id, podchaser_id, podcast_title, status) VALUES
    ('1462776122', '851883',  'Inevitable (formerly My Climate Journey)', 'pending'),
    ('1593204897', '2153206', 'Catalyst with Shayle Kann',                'pending'),
    ('1524683327', '1359342', 'Cleaning Up',                              'pending'),
    ('663379413',  '109600',  'The Energy Gang',                          'pending'),
    ('1548554104', '3737431', 'Volts',                                    'pending'),
    ('1469286286', '877656',  'Switched On',                              'pending'),
    ('1221460035', '541293',  'The Interchange: Recharged',               'pending'),
    ('1554962073', '854652',  'Watt It Takes',                            'pending'),
    ('1371456031', '656398',  'Political Climate',                        'pending'),
    ('1621556928', '4826131', 'Zero: The Climate Race',                   'pending'),
    ('1439197083', '742294',  'Redefining Energy',                        'pending'),
    ('1576295837', '1979929', 'Climate Tech Cocktails',                   'pending'),
    ('1482781075', '934211',  'Climate Rising',                           'pending'),
    ('1042713378', '197171',  'The Energy Transition Show',               'pending'),
    ('1534829787', '1501741', 'A Matter of Degrees',                      'pending'),
    ('1459416461', '873859',  'Outrage + Optimism',                       'pending'),
    ('1442356042', '1544490', 'Leaders in Cleantech',                     'pending'),
    ('1758404839', '5769839', 'Supercool',                                'pending'),
    ('1538415261', '1531238', 'The Climate Question',                     'pending'),
    ('1453416874', '805995',  'Energy Unplugged',                         'pending'),
    ('1794164180', '5985497', 'Open Circuit',                             'pending'),
    ('1620424225', '4288186', 'Factor This!',                             'pending'),
    ('1892925092', NULL,      'Critical Capital',                         'pending'),
    ('1081481629', '40484',   'Columbia Energy Exchange',                 'pending'),
    ('976204876',  '463390',  'CleanTech Talk',                           'pending'),
    ('1571177675', '1934775', 'The Big Switch',                           'pending'),
    ('1586892518', '1944634', 'How We Survive',                           'pending'),
    ('1439735906', '748053',  'Drilled',                                  'pending'),
    ('1593203014', '4007204', 'The Green Blueprint',                      'pending'),
    ('1518148418', '1257328', 'Build Repeat.',                            'pending'),
    ('1628554388', '4752417', 'Hardware to Save a Planet',                'pending'),
    ('1653296244', '5370888', 'With Great Power',                         'pending'),
    ('1580441929', '2020250', 'Energy Transition Solutions',              'pending'),
    ('1622351773', NULL,      'Climate Insiders',                         'pending'),
    ('1562824449', NULL,      'The Tech 4 Climate Podcast',               'pending'),
    ('1604218333', '4147481', 'The Great Simplification',                 'pending'),
    ('1526186333', '2199181', 'Smart Energy Voices',                      'pending'),
    ('1511642490', NULL,      'DER Task Force Podcast',                   'pending'),
    ('1590522755', NULL,      'The Carbon Removal Show',                  'pending'),
    ('1550647922', NULL,      'Understanding Climate Finance',            'pending'),
    ('1590198129', NULL,      'Nuclear Barbarians',                       'pending'),
    ('1488804391', NULL,      'Energy Central',                           'pending'),
    ('1700565143', NULL,      'Decarbonizing Commerce',                   'pending'),
    ('1498854987', NULL,      'The Clean Energy Show',                    'pending'),
    ('1709396247', NULL,      'Climate Capital Podcast',                  'pending'),
    ('1561411048', '2176818', 'Climate CEOs',                             'pending'),
    ('1331598443', '604598',  'Titans of Nuclear',                        'pending'),
    ('296762605',  '14444',   'Climate One',                              'pending'),
    ('1541394865', '1569388', 'Where the Internet Lives',                 'pending'),
    ('1805010154', NULL,      'NET-0 | Inside ClimateTech, Startups & Venture Capital', 'pending'),
    ('1614789563', NULL,      'The Startup Tank',                                        'pending'),
    ('1321759767', '595385',  'Reversing Climate Change',                                'pending'),
    ('1523015060', NULL,      'Going Negative',                                          'pending'),
    ('1167164482', NULL,      'Currents',                                                'pending'),
    ('1480605295', NULL,      'Politico Energy',                                         'pending')
ON CONFLICT (apple_podcast_id) DO NOTHING;
