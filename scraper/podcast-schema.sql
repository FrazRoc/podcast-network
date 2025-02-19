-- Drop existing tables if they exist
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



-- Create Hosts table
CREATE TABLE hosts (
    host_id SERIAL PRIMARY KEY,
    podchaser_id VARCHAR(100),
    first_name TEXT, -- Changed to TEXT
    last_name TEXT, -- Changed to TEXT
    email TEXT UNIQUE, -- Changed to TEXT
    bio TEXT,
    profile_image_url TEXT,  -- Changed to TEXT
    twitter_handle VARCHAR(100),
    wikipedia TEXT,
    website_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (first_name, last_name)
);


-- Create Podcasts table
CREATE TABLE podcasts (
    podcast_id SERIAL PRIMARY KEY,
    apple_podcast_id VARCHAR(100),
    podchaser_id VARCHAR(100),
    title VARCHAR(500) NOT NULL UNIQUE,  -- Increased length
    description TEXT,
    cover_art_url TEXT,  -- Changed to TEXT
    rss_feed_url TEXT,   -- Changed to TEXT
    website_url TEXT,    -- Changed to TEXT
    language VARCHAR(100),
    channel_id INTEGER REFERENCES channels(channel_id),
    rating_count INTEGER,
    average_rating DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Episodes table
CREATE TABLE episodes (
    episode_id SERIAL PRIMARY KEY,
    podcast_id INTEGER REFERENCES podcasts(podcast_id),
    apple_episode_id VARCHAR(100),
    podchaser_id VARCHAR(100),
    title VARCHAR(500) NOT NULL,  -- Increased length
    episode_number INTEGER,
    season_number INTEGER,
    description TEXT,
    audio_url TEXT,     -- Changed to TEXT
    duration_seconds INTEGER,
    published_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (podcast_id, title)
);

-- Create Host_Podcast junction table
CREATE TABLE host_podcast (
    host_id INTEGER REFERENCES hosts(host_id),
    podcast_id INTEGER REFERENCES podcasts(podcast_id),
    role VARCHAR(100),  -- Increased length
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (host_id, podcast_id)
);

-- Create Episode_Host junction table
CREATE TABLE episode_host (
    episode_id INTEGER REFERENCES episodes(episode_id),
    host_id INTEGER REFERENCES hosts(host_id),
    is_guest BOOLEAN DEFAULT FALSE,
    role VARCHAR(100),
    PRIMARY KEY (episode_id, host_id)
);

-- Create Tags table
CREATE TABLE tags (
    tag_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE  -- Increased length
);

-- Create Episode_Tag junction table
CREATE TABLE episode_tag (
    episode_id INTEGER REFERENCES episodes(episode_id),
    tag_id INTEGER REFERENCES tags(tag_id),
    PRIMARY KEY (episode_id, tag_id)
);

-- Create Podcast Tracking table
CREATE TABLE podcast_tracking (
    tracking_id SERIAL PRIMARY KEY,
    apple_podcast_id VARCHAR(100) UNIQUE NOT NULL,
    last_scraped_at TIMESTAMP,
    scrape_count INTEGER DEFAULT 0,
    status VARCHAR(50),  -- 'success', 'failed', 'in_progress'
    error_message TEXT,
    total_episodes INTEGER,
    latest_episode_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- Create table for social links
CREATE TABLE IF NOT EXISTS host_social_links (
    link_id SERIAL PRIMARY KEY,
    host_id INTEGER REFERENCES hosts(host_id),
    platform VARCHAR(50),
    url TEXT,
    UNIQUE(host_id, platform)
);

CREATE TABLE IF NOT EXISTS host_roles (
    role_id SERIAL PRIMARY KEY,
    host_id INTEGER REFERENCES hosts(host_id),
    podcast_name TEXT,
    role_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    UNIQUE(host_id, podcast_name, role_name)
);

CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    apple_genre_id VARCHAR(20) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE podcast_genres (
    podcast_id INTEGER REFERENCES podcasts(podcast_id),
    genre_id INTEGER REFERENCES genres(genre_id),
    is_primary BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (podcast_id, genre_id)
);

-- Create channels table
CREATE TABLE channels (
    channel_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Create indexes for better performance
CREATE INDEX idx_episodes_podcast_id ON episodes(podcast_id);
CREATE INDEX idx_episode_host_host_id ON episode_host(host_id);
CREATE INDEX idx_host_podcast_podcast_id ON host_podcast(podcast_id);
CREATE INDEX idx_episodes_published_date ON episodes(published_date);
CREATE INDEX idx_apple_podcast_id ON podcast_tracking(apple_podcast_id);

-- Create index for Podchaser IDs
CREATE INDEX IF NOT EXISTS idx_host_podchaser_id ON hosts(podchaser_id);
CREATE INDEX IF NOT EXISTS idx_podcast_podchaser_id ON podcasts(podchaser_id);
-- Create index for Podchaser IDs
CREATE INDEX IF NOT EXISTS idx_episode_podchaser_id ON episodes(podchaser_id);
-- Add index for better performance
CREATE INDEX IF NOT EXISTS idx_apple_episode_id ON episodes(apple_episode_id);

-- Create indexes for Genres and channels
CREATE INDEX idx_genre_apple_id ON genres(apple_genre_id);
CREATE INDEX idx_podcast_genres_primary ON podcast_genres(podcast_id) WHERE is_primary = true;
CREATE INDEX idx_podcast_channel_id ON podcasts(channel_id);