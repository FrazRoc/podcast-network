-- Create Hosts table
CREATE TABLE hosts (
    host_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    bio TEXT,
    twitter_handle VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Podcasts table
CREATE TABLE podcasts (
    podcast_id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    cover_art_url VARCHAR(255),
    rss_feed_url VARCHAR(255),
    website_url VARCHAR(255),
    language VARCHAR(50),
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Episodes table
CREATE TABLE episodes (
    episode_id SERIAL PRIMARY KEY,
    podcast_id INTEGER REFERENCES podcasts(podcast_id),
    title VARCHAR(200) NOT NULL,
    episode_number INTEGER,
    season_number INTEGER,
    description TEXT,
    audio_url VARCHAR(255) NOT NULL,
    duration_seconds INTEGER,
    published_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Host_Podcast junction table (for handling multiple hosts per podcast)
CREATE TABLE host_podcast (
    host_id INTEGER REFERENCES hosts(host_id),
    podcast_id INTEGER REFERENCES podcasts(podcast_id),
    role VARCHAR(50), -- e.g., 'main host', 'co-host', 'recurring guest'
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (host_id, podcast_id)
);

-- Create Episode_Host junction table (for tracking which hosts appear in which episodes)
CREATE TABLE episode_host (
    episode_id INTEGER REFERENCES episodes(episode_id),
    host_id INTEGER REFERENCES hosts(host_id),
    is_guest BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (episode_id, host_id)
);

-- Create Tags table
CREATE TABLE tags (
    tag_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE
);

-- Create Episode_Tag junction table
CREATE TABLE episode_tag (
    episode_id INTEGER REFERENCES episodes(episode_id),
    tag_id INTEGER REFERENCES tags(tag_id),
    PRIMARY KEY (episode_id, tag_id)
);

-- Optional: Create indexes for better query performance
CREATE INDEX idx_episodes_podcast_id ON episodes(podcast_id);
CREATE INDEX idx_episode_host_host_id ON episode_host(host_id);
CREATE INDEX idx_host_podcast_podcast_id ON host_podcast(podcast_id);
CREATE INDEX idx_episodes_published_date ON episodes(published_date);
