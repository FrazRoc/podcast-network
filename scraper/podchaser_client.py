import requests
import json
import logging
import time
from datetime import datetime
import re
from typing import Dict, List, Optional, Tuple
import psycopg2
from ratelimit import limits, sleep_and_retry
from difflib import SequenceMatcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PodchaserAPIError(Exception):
    """Custom exception for Podchaser API errors"""
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 response_text: Optional[str] = None, query: Optional[str] = None):
        self.status_code = status_code
        self.response_text = response_text
        self.query = query
        self.message = self._format_message(message)
        super().__init__(self.message)

    def _format_message(self, message: str) -> str:
        """Format the error message with available details"""
        details = [message]
        if self.status_code:
            details.append(f"Status Code: {self.status_code}")
        if self.response_text:
            details.append(f"Response: {self.response_text[:500]}...")  # Truncate long responses
        if self.query:
            details.append(f"Query: {self.query}")
        return "\n".join(details)

    def __str__(self):
        return self.message

class PodchaserClient:
    def __init__(self, client_id: str, api_key: str, db_connection_string: str):
        self.client_id = client_id
        self.api_key = api_key
        self.db_connection_string = db_connection_string
        self.conn = psycopg2.connect(db_connection_string)
        self.cursor = self.conn.cursor()
        self.base_url = "https://api.podchaser.com/graphql"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Client-ID": self.client_id
        }
        # Add retry configuration
        self.retry_count = 3
        self.retry_delay = 5  # seconds between retries
        self.calls_per_minute = 100  # Rate limiting
        
        logger.debug("Initialized PodchaserClient with:")
        logger.debug(f"- Max retries: {self.retry_count}")
        logger.debug(f"- Retry delay: {self.retry_delay} seconds")
        logger.debug(f"- Rate limit: {self.calls_per_minute} calls per minute")

    def execute_query(self, query: str, variables: Dict = None) -> Dict:
        """Execute a GraphQL query with enhanced error handling"""
        for attempt in range(self.retry_count):
            try:
                logger.debug(f"Executing Podchaser query (attempt {attempt + 1}/{self.retry_count})")
                logger.debug(f"Query: {query}")
                logger.debug(f"Variables: {json.dumps(variables, indent=2)}")

                response = requests.post(
                    self.base_url,
                    headers=self.headers,
                    json={"query": query, "variables": variables or {}}
                )

                # Log response details
                logger.debug(f"Response Status: {response.status_code}")
                logger.debug(f"Response Headers: {dict(response.headers)}")

                if response.status_code == 429:  # Too Many Requests
                    wait_time = self.retry_delay * (attempt + 1)
                    logger.warning(f"Rate limit hit, waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue

                if response.status_code != 200:
                    raise PodchaserAPIError(
                        "Query failed",
                        status_code=response.status_code,
                        response_text=response.text,
                        query=query
                    )

                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response: {response.text[:500]}...")
                    raise PodchaserAPIError(
                        "Invalid JSON response",
                        status_code=response.status_code,
                        response_text=response.text,
                        query=query
                    )

                if 'errors' in data:
                    error_messages = [error.get('message', 'Unknown error') 
                                   for error in data['errors']]
                    raise PodchaserAPIError(
                        "GraphQL errors",
                        response_text="\n".join(error_messages),
                        query=query
                    )

                logger.debug("Query executed successfully")
                return data

            except requests.exceptions.RequestException as e:
                logger.error(f"Network error on attempt {attempt + 1}: {str(e)}")
                if attempt == self.retry_count - 1:
                    raise PodchaserAPIError(
                        f"Network error after {self.retry_count} attempts",
                        response_text=str(e),
                        query=query
                    )
                time.sleep(self.retry_delay * (attempt + 1))

    def search_and_match_creator(self, name: str, threshold: float = 0.85) -> Optional[Dict]:
        """Search for a creator by name with enhanced error handling"""
        pcid = "727911"
        try:
            logger.info(f"Searching for creator: {name}")
            
            query = """
            query SearchCreator($name: String!) {
                creators(searchTerm: $name ) {
                    paginatorInfo {
                        count,
                        currentPage,
                        firstItem,
                        hasMorePages,
                        lastItem,
                        lastPage,
                        perPage,
                        total
                    }
                    data {
                        pcid
                        name
                        bio
                        imageUrl
                        url
                        episodeAppearanceCount
                        socialLinks {
                            twitter
                            wikipedia
                        }
                        # we could get credits here, but will need to figure out pagination
                        #credits {
                        #    data {
                        #
                        #    }
                        #}
                    }
                }
            }
            """
            
            result = self.execute_query(query, {"name": name})
            
            if not result.get('data'):
                logger.warning(f"No data returned for creator search: {name}")
                return None
             
            creators = result.get('data', {}).get('creators', {}).get('data', [])
            #logger.info(f"Found {len(creators)} potential matches for {name}")
            
            best_match = None
            best_score = 0
            
            for creator in creators:
                score = SequenceMatcher(None, name.lower(), creator['name'].lower()).ratio()
                logger.debug(f"Match score for {creator['name']}: {score:.2f}")
                
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = creator
            
            if best_match:
                logger.info(f"Best match found: {best_match['name']} (score: {best_score:.2f})")
                return best_match
            
            logger.info(f"No match found above threshold ({threshold}) for {name}")
            return None
            
        except PodchaserAPIError as e:
            logger.error(f"Podchaser API error while searching for {name}:\n{str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while searching for {name}: {str(e)}")
            raise PodchaserAPIError(
                f"Error searching creator '{name}'",
                response_text=str(e)
            )

    def enrich_host_data(self, host_id: int, force_update: bool = False) -> Dict:
        """
        Enrich existing host data with Podchaser information
        """
        conn = psycopg2.connect(self.db_connection_string)
        cur = conn.cursor()
        
        try:
            # Get host information
            cur.execute("""
                SELECT first_name, last_name, podchaser_id 
                FROM hosts 
                WHERE host_id = %s
            """, (host_id,))
            
            result = cur.fetchone()
            if not result:
                raise ValueError(f"No host found with ID: {host_id}")
                
            first_name, last_name, podchaser_id = result
            full_name = f"{first_name} {last_name}".strip()
            
            # Skip if already has Podchaser data and not forcing update
            if podchaser_id and not force_update:
                logger.info(f"Host {full_name} already has Podchaser data")
                return {"status": "skipped", "host_id": host_id}
            
            # Search for creator
            creator_data = self.search_and_match_creator(full_name)
            if not creator_data:
                logger.warning(f"No Podchaser match found for host: {full_name}")
                return {"status": "not_found", "host_id": host_id}

            # Update host information
            cur.execute("""
                UPDATE hosts 
                SET 
                    podchaser_id = %s,
                    bio = %s,
                    profile_image_url = %s,
                    website_url = %s
                WHERE host_id = %s
            """, (
                creator_data['pcid'],
                creator_data['bio'],
                creator_data['imageUrl'],
                creator_data['url'],
                host_id
            ))
            
            # Process social links
            #self._process_social_links(cur, host_id, creator_data['socialLinks'])
            
            # Process roles across different podcasts
            #roles = self._extract_creator_roles(creator_data['credits']['edges'])
            #self._update_host_roles(cur, host_id, roles)
            
            conn.commit()
            
            return {
                "status": "updated",
                "host_id": host_id,
                "podchaser_id": creator_data['pcid'],
                #"roles_found": len(roles)
            }
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error enriching host {host_id}: {str(e)}")
            return {"status": "error", "host_id": host_id, "error": str(e)}
            
        finally:
            cur.close()
            conn.close()

    def _process_social_links(self, cur, host_id: int, social_links: List[Dict]):
        """Process social media links for a host"""
        logger.info(f"Processing {len(social_links)} social links for host {host_id}")
        try:
            # First, remove existing links
            cur.execute("""
                DELETE FROM host_social_links 
                WHERE host_id = %s
            """, (host_id,))
            
            # Insert new links
            for link in social_links:
                if not isinstance(link, dict):
                    logger.warning(f"Invalid social link data for host {host_id}: {link}")
                    continue
                    
                platform = link.get('platform')
                url = link.get('url')
                if not platform or not url:
                    logger.warning(f"Missing platform or URL in social link: {link}")
                    continue
                    
                cur.execute("""
                    INSERT INTO host_social_links (host_id, platform, url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (host_id, platform) DO UPDATE
                    SET url = EXCLUDED.url
                """, (host_id, platform, url))
                
                logger.debug(f"Added {platform} link for host {host_id}")
                
        except Exception as e:
            logger.error(f"Error processing social links for host {host_id}: {str(e)}")
            raise


    def _extract_creator_roles(self, credits: List[Dict]) -> List[Dict]:
        """Extract and categorize creator roles from credits"""
        roles = []
        for edge in credits:
            roles.append({
                'role': edge['role'],
                'podcast': edge['podcast']['title'],
                'start_date': edge['credit'].get('startDate'),
                'end_date': edge['credit'].get('endDate')
            })
        return roles

    def _update_host_roles(self, cur, host_id: int, roles: List[Dict]):
        """Update host roles in the database"""

        # Insert roles
        for role in roles:
            cur.execute("""
                INSERT INTO host_roles (
                    host_id, podcast_name, role_name, 
                    start_date, end_date
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (host_id, podcast_name, role_name) 
                DO UPDATE SET 
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date
            """, (
                host_id,
                role['podcast'],
                role['role'],
                role['start_date'],
                role['end_date']
            ))

    def enrich_all_hosts(self, batch_size: int = 10):
        """Enrich all hosts in the database with Podchaser data"""
        conn = psycopg2.connect(self.db_connection_string)
        cur = conn.cursor()
        
        try:
            # Get all hosts without Podchaser data
            cur.execute("""
                SELECT host_id 
                FROM hosts 
                WHERE podchaser_id IS NULL 
                LIMIT %s
            """, (batch_size,))
            
            hosts = cur.fetchall()
            
            results = {
                'processed': 0,
                'failed': 0,
                'skipped': 0,
                'not_found': 0,
                'details': []
            }
            
            for (host_id,) in hosts:
                try:
                    result = self.enrich_host_data(host_id)
                    results['details'].append(result)
                    
                    if result['status'] == 'updated':
                        results['processed'] += 1
                    elif result['status'] == 'skipped':
                        results['skipped'] += 1
                    elif result['status'] == 'not_found':
                        results['not_found'] += 1
                    else:
                        results['failed'] += 1
                    
                    time.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error processing host {host_id}: {str(e)}")
                    results['failed'] += 1
            
            return results
            
        finally:
            cur.close()
            conn.close()

    def search_guests_by_episode(self, episode_id: int):
        """Search for guests mentioned in an episode"""
        conn = psycopg2.connect(self.db_connection_string)
        cur = conn.cursor()
        
        try:
            # Get episode description and guest mentions
            cur.execute("""
                SELECT title, description 
                FROM episodes 
                WHERE episode_id = %s
            """, (episode_id,))
            
            result = cur.fetchone()
            if not result:
                raise ValueError(f"No episode found with ID: {episode_id}")
                
            title, description = result
            
            # Extract potential guest names using NLP or pattern matching
            guest_names = self._extract_potential_guests(title, description)
            
            guest_results = []
            for name in guest_names:
                creator_data = self.search_and_match_creator(name)
                if creator_data:
                    guest_results.append({
                        'name': name,
                        'podchaser_data': creator_data,
                        'confidence': SequenceMatcher(
                            None, name.lower(), 
                            creator_data['name'].lower()
                        ).ratio()
                    })
            
            return guest_results
            
        finally:
            cur.close()
            conn.close()

    def _extract_potential_guests(self, title: str, description: str) -> List[str]:
        """Extract potential guest names from episode title and description"""
        guest_patterns = [
            r'(?:special guest|guest|featuring|feat\.|ft\.)[:\s]+([^\.!?\n]+)',
            r'(?:interview with|in conversation with)[:\s]+([^\.!?\n]+)',
            r'(?:joined by|welcomes)[:\s]+([^\.!?\n]+)',
        ]
        
        guests = set()
        text = f"{title}\n{description}"
        
        for pattern in guest_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                names = re.split(r'\s*(?:,|\sand\s|&)\s*', match.group(1))
                for name in names:
                    name = name.strip()
                    if (name and 
                        len(name.split()) >= 2 and
                        not re.match(r'^(this|the|our|we|you|their|podcast|show|episode)', name.lower())):
                        guests.add(name)
        
        return list(guests)

    def get_podcast_by_id(self, podchaser_id: str) -> Optional[Dict]:
        """
        Look up a specific podcast by Podchaser ID
        Returns podcast data or None if not found
        """
        try:
            logger.info(f"Looking up podcast with ID: {podchaser_id}")
            
            query = """
            query {
                podcasts(searchTerm: "Reply All") {
                    data {    
                        id
                        title
                        description
                        url
                        webUrl
                        rssUrl
                        imageUrl
                        language
                        latestEpisodeDate
                        # socialLinks
                        categories {
                            title
                            slug
                        }
                        hasGuests
                        applePodcastsId

                    }
                }
            }
            """
            
            variables = {"podchaserId": podchaser_id}
            result = self.execute_query(query, variables)
            #print(result)
            #print("result.get('data')", result.get('data'))
            #print("result['data'].get('podcast')", result['data'].get('podcasts'))

            if not result.get('data') or not result['data'].get('podcasts'):
                logger.warning(f"No podcast found with ID: {podchaser_id}")
                return None
                
            podcast_data = result['data']['podcasts']
            logger.info(f"Successfully retrieved podcast: {podcast_data['title']}")
            
            return podcast_data
            
        except PodchaserAPIError as e:
            logger.error(f"Podchaser API error while looking up podcast {podchaser_id}:\n{str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while looking up podcast {podchaser_id}: {str(e)}")
            raise PodchaserAPIError(
                f"Error looking up podcast '{podchaser_id}'",
                response_text=str(e)
            )

    def sync_episode_credits(self, batch_size: int = 10):
        """
        Sync credits for episodes in our database with Podchaser data
        """
        try:
            logger.info("Starting episode credits sync")
            
            query = """
            query GetEpisodeCredits($episodeId: ID!) {
                episode(identifier: { id: $episodeId }) {
                    id
                    title
                    credits(first: 100) {  # Increased to get more credits
                        edges {
                            node {
                                role
                                creator {
                                    id
                                    name
                                    bio
                                    imageUrl
                                    websiteUrl
                                    socialLinks {
                                        platform
                                        url
                                    }
                                }
                                startTime
                                endTime
                            }
                        }
                    }
                }
            }
            """
            
            conn = psycopg2.connect(self.db_connection_string)
            cur = conn.cursor()
            
            try:
                # Get episodes that need processing
                cur.execute("""
                    SELECT e.episode_id, e.title, e.podchaser_id
                    FROM episodes e
                    LEFT JOIN episode_host eh ON e.episode_id = eh.episode_id
                    WHERE e.podchaser_id IS NOT NULL
                    AND eh.episode_id IS NULL
                    LIMIT %s
                """, (batch_size,))
                
                episodes = cur.fetchall()
                logger.info(f"Found {len(episodes)} episodes to process")
                
                results = {
                    'processed': 0,
                    'failed': 0,
                    'credits_added': 0,
                    'hosts_created': 0,
                    'details': []
                }
                
                for episode_id, title, podchaser_id in episodes:
                    try:
                        logger.info(f"Processing episode: {title} (ID: {episode_id})")
                        
                        # Query Podchaser for episode credits
                        variables = {"episodeId": podchaser_id}
                        response = self.execute_query(query, variables)
                        
                        if not response.get('data') or not response['data'].get('episode'):
                            logger.warning(f"No data found for episode {title}")
                            continue
                        
                        episode_data = response['data']['episode']
                        credits = episode_data['credits']['edges']
                        
                        episode_result = {
                            'episode_id': episode_id,
                            'title': title,
                            'credits_processed': 0,
                            'hosts_added': 0
                        }
                        
                        # Process each credit
                        for credit in credits:
                            try:
                                creator = credit['node']['creator']
                                role = credit['node']['role']
                                
                                # Insert or update host
                                cur.execute("""
                                    INSERT INTO hosts (
                                        podchaser_id, first_name, last_name, 
                                        bio, image_url, website_url
                                    )
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (podchaser_id) 
                                    DO UPDATE SET
                                        bio = EXCLUDED.bio,
                                        image_url = EXCLUDED.image_url,
                                        website_url = EXCLUDED.website_url
                                    RETURNING host_id
                                """, (
                                    creator['id'],
                                    *self._split_name(creator['name']),
                                    creator.get('bio'),
                                    creator.get('imageUrl'),
                                    creator.get('websiteUrl')
                                ))
                                
                                host_id = cur.fetchone()[0]
                                
                                # Link host to episode
                                is_guest = role.lower() in ['guest', 'interviewee', 'featured']
                                cur.execute("""
                                    INSERT INTO episode_host (episode_id, host_id, is_guest)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (episode_id, host_id) DO UPDATE
                                    SET is_guest = EXCLUDED.is_guest
                                """, (episode_id, host_id, is_guest))
                                
                                # Process social links
                                if creator.get('socialLinks'):
                                    self._process_social_links(cur, host_id, creator['socialLinks'])
                                
                                episode_result['credits_processed'] += 1
                                episode_result['hosts_added'] += 1
                                
                            except Exception as e:
                                logger.error(f"Error processing credit in episode {title}: {str(e)}")
                                continue
                        
                        results['processed'] += 1
                        results['credits_added'] += episode_result['credits_processed']
                        results['hosts_created'] += episode_result['hosts_added']
                        results['details'].append(episode_result)
                        
                        conn.commit()
                        logger.info(f"Successfully processed episode {title}")
                        
                    except Exception as e:
                        logger.error(f"Error processing episode {title}: {str(e)}")
                        conn.rollback()
                        results['failed'] += 1
                        results['details'].append({
                            'episode_id': episode_id,
                            'title': title,
                            'error': str(e)
                        })
                        continue
                
                return results
                
            finally:
                cur.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error in sync_episode_credits: {str(e)}")
            raise

    def _split_name(self, full_name: str) -> tuple:
        """Split full name into first and last name"""
        parts = full_name.split(' ', 1)
        return (parts[0], parts[1] if len(parts) > 1 else '')


    def find_podcast_podchaser_ids(self, batch_size: int = 10) -> Dict:
        """
        Find Podchaser IDs for podcasts in our database
        """
        try:
            logger.info("Starting podcast Podchaser ID lookup")
            
            query = """
            query SearchPodcast($searchTerm: String!) {
                podcasts( searchTerm: $searchTerm ) {
                    paginatorInfo {
                        currentPage,
                        hasMorePages,
                        lastPage,
                    },
                    data {
                        id
                        title
                        applePodcastsId
                        description
                        webUrl
                        imageUrl
                    }
                }
            }
            """
            
            conn = psycopg2.connect(self.db_connection_string)
            cur = conn.cursor()
            
            try:
                # Get podcasts that need Podchaser IDs
                cur.execute("""
                    SELECT podcast_id, title, apple_podcast_id
                    FROM podcasts
                    WHERE podchaser_id IS NULL
                    LIMIT %s
                """, (batch_size,))
                
                podcasts = cur.fetchall()
                logger.info(f"Found {len(podcasts)} podcasts without Podchaser IDs")
                
                results = {
                    'processed': 0,
                    'matched': 0,
                    'not_found': 0,
                    'failed': 0,
                    'details': []
                }
                
                for podcast_id, title, apple_podcast_id in podcasts:
                    try:
                        logger.info(f"Looking up Podchaser ID for podcast: {title}")
                        
                        # Search for podcast
                        variables = {
                            "searchTerm": apple_podcast_id
                        }
                        
                        response = self.execute_query(query, variables)
                        #logger.info(f"Podchaser API response: {json.dumps(response, indent=2)}")
                        
                        if not response.get('data') or not response['data'].get('podcasts'):
                            logger.warning(f"No results found for podcast: {title}")
                            results['not_found'] += 1
                            results['details'].append({
                                'podcast_id': podcast_id,
                                'title': title,
                                'status': 'not_found'
                            })
                            continue
                       
                        edges = response['data']['podcasts'].get('data', [])
                        if not edges:
                            logger.warning(f"No matches found for podcast: {title}")
                            results['not_found'] += 1
                            results['details'].append({
                                'podcast_id': podcast_id,
                                'title': title,
                                'status': 'not_found'
                            })
                            continue
                        
                        # Get the first (best) match
                        podcast_data = edges[0]
                        podchaser_id = podcast_data['id']
                        
                        # Update the podcast with Podchaser data
                        cur.execute("""
                            UPDATE podcasts 
                            SET 
                                podchaser_id = %s,
                                website_url = COALESCE(%s, website_url),
                                cover_art_url = COALESCE(%s, cover_art_url)
                            WHERE podcast_id = %s
                        """, (
                            podchaser_id,
                            podcast_data.get('webUrl'),
                            podcast_data.get('imageUrl'),
                            podcast_id
                        ))
                        
                        conn.commit()
                        
                        results['matched'] += 1
                        results['details'].append({
                            'podcast_id': podcast_id,
                            'title': title,
                            'status': 'matched',
                            'podchaser_id': podchaser_id
                        })
                        
                        #logger.info(f"Successfully matched podcast {title} to Podchaser ID {podchaser_id}")
                        
                    except Exception as e:
                        logger.error(f"Error processing podcast {title}: {str(e)}")
                        conn.rollback()
                        results['failed'] += 1
                        results['details'].append({
                            'podcast_id': podcast_id,
                            'title': title,
                            'status': 'error',
                            'error': str(e)
                        })
                        continue
                    
                    results['processed'] += 1
                
                return results
                
            finally:
                cur.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error in find_podcast_podchaser_ids: {str(e)}")
            raise

    def find_episode_podchaser_ids(self, batch_size: int = 10) -> Dict:
        """
        Find Podchaser IDs for episodes in our database.
        """
        try:
            logger.info("Starting episode Podchaser ID lookup")
            
            # GraphQL query for looking up an episode
            query = """
            query SearchEpisode($searchTerm: String!) {
                episodes( searchTerm: $searchTerm ){
                    paginatorInfo {
                        currentPage,
                        hasMorePages,
                        lastPage,
                    },
                    data {
                        id
                        title
                        airDate
                        description
                        podcast {
                            id
                            title
                        }
                        credits {
                            data {
                                id
                                creator {
                                    name
                                }
                                role {
                                    title
                                }
                            }
                        }
                    } 
                }
            }
            """
            
            conn = psycopg2.connect(self.db_connection_string)
            cur = conn.cursor()
            
            try:
                # Get episodes that need Podchaser IDs
                cur.execute("""
                    SELECT e.episode_id, e.title, e.published_date, p.title as podcast_title, p.podchaser_id as podcast_podchaser_id
                    FROM episodes e
                    JOIN podcasts p ON e.podcast_id = p.podcast_id
                    WHERE e.podchaser_id IS NULL
                    AND p.podchaser_id IS NOT NULL
                    LIMIT %s
                """, (batch_size,))
                
                episodes = cur.fetchall()
                logger.info(f"Found {len(episodes)} episodes without Podchaser IDs")
                
                results = {
                    'processed': 0,
                    'matched': 0,
                    'not_found': 0,
                    'failed': 0,
                    'details': []
                }
                
                for episode_id, title, published_date, podcast_title, podcast_podchaser_id in episodes:
                    try:
                        logger.info(f"Looking up Podchaser ID for episode: {title}")
                        
                        # Search for episode
                        variables = {
                            "searchTerm": title
                        }
                        
                        response = self.execute_query(query, variables)
                        print ()
                        print ()
                        print (response['data'].get('episodes').get('data', []))
                        print ()
                        if not response.get('data') or not response['data'].get('episodes'):
                            logger.warning(f"No results found for episode: {title}")
                            results['not_found'] += 1
                            results['details'].append({
                                'episode_id': episode_id,
                                'title': title,
                                'status': 'not_found'
                            })
                            continue
                        
                        edges = response['data']['episodes'].get('data', [])
                        if not edges:
                            logger.warning(f"No matches found for episode: {title}")
                            results['not_found'] += 1
                            results['details'].append({
                                'episode_id': episode_id,
                                'title': title,
                                'status': 'not_found'
                            })
                            continue
                        
                        # Get the first (best) match
                        podchaser_id = edges[0].get('id')
                        
                        # Update the episode with the Podchaser ID
                        cur.execute("""
                            UPDATE episodes 
                            SET podchaser_id = %s
                            WHERE episode_id = %s
                        """, (podchaser_id, episode_id))
                        
                        conn.commit()

                        print("-------CREDITS------")
                        #print (edges[0].get("credits").get("data"))
                        people = edges[0].get("credits").get("data")
                        for person in people:
                            print (person)
                            # Insert person as host
                            host_id = self.insert_person(person.get("creator").get("name"))
                            print ("here", host_id)
                            #add TRY or IF logic here
                            self.link_person_to_episode(host_id, episode_id, is_guest=False)
                            # Process roles across different podcasts
                            #roles = self._extract_creator_roles(creator_data['credits']['edges'])
                            #self._update_host_roles(cur, host_id, roles)

                        
                        results['matched'] += 1
                        results['details'].append({
                            'episode_id': episode_id,
                            'title': title,
                            'status': 'matched',
                            'podchaser_id': podchaser_id
                        })
                        
                        logger.info(f"Successfully matched episode {title} to Podchaser ID {podchaser_id}")
                        
                    except Exception as e:
                        logger.error(f"Error processing episode {title}: {str(e)}")
                        conn.rollback()
                        results['failed'] += 1
                        results['details'].append({
                            'episode_id': episode_id,
                            'title': title,
                            'status': 'error',
                            'error': str(e)
                        })
                        continue
                    
                    results['processed'] += 1
                
                return results
                
            finally:
                cur.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error in find_episode_podchaser_ids: {str(e)}")
            raise

    def insert_person(self, name: str) -> int:
        """Insert a person (host or guest) into database"""
        name_parts = name.split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ''

        print ("Attempting to insert person", first_name, last_name)
        query = """
        INSERT INTO hosts (first_name, last_name)
        VALUES (%s, %s)
        ON CONFLICT (first_name, last_name) DO UPDATE 
        SET first_name = EXCLUDED.first_name
        RETURNING host_id
        """
        
        self.cursor.execute(query, (first_name, last_name))
        result = self.cursor.fetchone()
        if result is None:
            logger.error("No results returned from database query")
            raise ValueError("Database query returned no results")
        logger.info("RESULTS:",result[0])

        host_id = result[0]
        return host_id

    def link_person_to_episode(self, person_id: int, episode_id: int, is_guest: bool):
        """Link person to episode"""
        print ("Attempting to link person to episode", person_id, episode_id, is_guest)
        query = """
        INSERT INTO episode_host (episode_id, host_id, is_guest)
        VALUES (%s, %s, %s)
        ON CONFLICT (episode_id, host_id) DO UPDATE 
        SET is_guest = EXCLUDED.is_guest
        """
        self.cursor.execute(query, (episode_id, person_id, is_guest))

# Example usage
if __name__ == "__main__":
    client = PodchaserClient(
        client_id="your_client_id",
        api_key="your_api_key",
        db_connection_string="postgresql://localhost/podcast_db"
    )
    
    try:
        # Enrich all hosts
        results = client.enrich_all_hosts(batch_size=5)
        print("Host enrichment results:")
        print(json.dumps(results, indent=2))
        
        # Search for guests in a specific episode
        guests = client.search_guests_by_episode(1)  # Replace with actual episode ID
        print("\nGuest search results:")
        print(json.dumps(guests, indent=2))
        
    except Exception as e:
        print(f"Error: {str(e)}")