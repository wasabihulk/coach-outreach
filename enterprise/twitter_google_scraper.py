"""
Google-Based Twitter Handle Scraper
============================================================================
Finds coach Twitter handles by searching Google instead of scraping staff pages.
No API required - uses web scraping with anti-detection measures.

Features:
- Multiple search query variations per coach
- Anti-detection: randomized delays, rotating user agents
- Disk caching with TTL
- Confidence scoring based on name/school matching
- Handles both twitter.com and x.com URLs

Strategy:
1. Search: "{coach_name}" "{school}" football twitter
2. Search: "{coach_name}" coach twitter  
3. Search: site:twitter.com "{coach_name}" "{school}"
4. Parse results for twitter.com/x.com URLs
5. Score and validate handles
6. Cache results to disk

Author: Coach Outreach System
Version: 2.0.0 (Enterprise)
============================================================================
"""

import re
import time
import random
import logging
import json
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field, asdict
from urllib.parse import quote_plus, urlparse, unquote
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass 
class TwitterSearchConfig:
    """Configuration for Twitter search"""
    min_delay: float = 2.5  # Min seconds between searches
    max_delay: float = 6.0  # Max seconds between searches
    max_retries: int = 3
    timeout: int = 12
    max_searches_per_session: int = 100
    cache_ttl_days: int = 7  # How long to cache results
    min_confidence: float = 0.3  # Minimum confidence to return result
    cache_dir: Path = field(default_factory=lambda: Path.home() / '.coach_outreach' / 'twitter_cache')

# User agents to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ============================================================================
# TWITTER HANDLE VALIDATION
# ============================================================================

def validate_twitter_handle(handle: str) -> Optional[str]:
    """
    Validate and normalize a Twitter handle.
    Returns cleaned handle or None if invalid.
    """
    if not handle:
        return None
    
    # Remove @ prefix
    handle = handle.lstrip('@')
    
    # Twitter handles: 1-30 chars (was 15, now increased), alphanumeric + underscore
    if not re.match(r'^[A-Za-z0-9_]{1,30}$', handle):
        return None
    
    # Filter out common non-coach handles
    invalid_handles = {
        'twitter', 'x', 'home', 'search', 'explore', 'notifications',
        'messages', 'settings', 'intent', 'share', 'i', 'hashtag',
        'login', 'signup', 'tos', 'privacy', 'about', 'status',
        'following', 'followers', 'likes', 'lists', 'moments'
    }
    
    if handle.lower() in invalid_handles:
        return None
    
    return handle


def extract_handle_from_url(url: str) -> Optional[str]:
    """Extract Twitter handle from a URL"""
    if not url:
        return None
    
    # Patterns for twitter.com and x.com URLs
    patterns = [
        r'(?:twitter\.com|x\.com)/(@?[A-Za-z0-9_]{1,15})(?:\?|/|$)',
        r'(?:twitter\.com|x\.com)/intent/\w+\?.*?screen_name=([A-Za-z0-9_]{1,15})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url, re.IGNORECASE)
        if match:
            handle = match.group(1)
            return validate_twitter_handle(handle)
    
    return None


# ============================================================================
# GOOGLE SEARCH SCRAPER
# ============================================================================

class GoogleTwitterScraper:
    """
    Enterprise-grade Twitter handle scraper with caching and confidence scoring.
    
    Features:
    - Multiple search query variations per coach
    - Anti-detection: randomized delays, rotating user agents
    - Disk caching with configurable TTL
    - Confidence scoring based on multiple signals
    - Handles both twitter.com and x.com URLs
    """
    
    def __init__(self, config: TwitterSearchConfig = None):
        self.config = config or TwitterSearchConfig()
        self.session = requests.Session()
        self.search_count = 0
        self._memory_cache: Dict[str, Optional[str]] = {}
        
        # Initialize disk cache directory
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_cache_key(self, coach_name: str, school: str) -> str:
        """Generate cache key from coach info."""
        raw = f"{coach_name.lower().strip()}|{school.lower().strip()}"
        return hashlib.md5(raw.encode()).hexdigest()
    
    def _load_from_disk_cache(self, coach_name: str, school: str) -> Optional[Dict]:
        """Load result from disk cache if valid."""
        cache_key = self._get_cache_key(coach_name, school)
        cache_file = self.config.cache_dir / f"{cache_key}.json"
        
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            
            # Check TTL
            cached_time = datetime.fromisoformat(data.get('timestamp', '2000-01-01'))
            if datetime.now() - cached_time > timedelta(days=self.config.cache_ttl_days):
                return None  # Expired
            
            logger.debug(f"Cache hit for {coach_name}: @{data.get('handle')}")
            return data
        except Exception as e:
            logger.warning(f"Cache read error: {e}")
            return None
    
    def _save_to_disk_cache(self, coach_name: str, school: str, handle: Optional[str], 
                            confidence: float, query_used: str) -> None:
        """Save result to disk cache."""
        cache_key = self._get_cache_key(coach_name, school)
        cache_file = self.config.cache_dir / f"{cache_key}.json"
        
        try:
            data = {
                'handle': handle,
                'confidence': confidence,
                'query_used': query_used,
                'coach_name': coach_name,
                'school': school,
                'timestamp': datetime.now().isoformat()
            }
            with open(cache_file, 'w') as f:
                json.dump(data, f)
            logger.debug(f"Cached result for {coach_name}")
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
    
    def _get_headers(self) -> dict:
        """Get randomized headers with anti-detection measures."""
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        }
    
    def _random_delay(self):
        """Wait random amount between requests"""
        delay = random.uniform(self.config.min_delay, self.config.max_delay)
        time.sleep(delay)
    
    def _search_google(self, query: str) -> Optional[str]:
        """
        Perform Google search and return HTML response.
        """
        self.search_count += 1
        
        if self.search_count > self.config.max_searches_per_session:
            logger.warning("Max searches per session reached")
            return None
        
        logger.info(f"Searching Google: {query}")
        
        # Use Google Search
        try:
            url = f"https://www.google.com/search?q={quote_plus(query)}&num=20"
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self.config.timeout
            )
            
            if response.status_code == 200 and len(response.text) > 1000:
                logger.info(f"Google success: {len(response.text)} chars")
                return response.text
            elif response.status_code == 429:
                logger.warning("Google rate limited, waiting...")
                time.sleep(10)
            else:
                logger.warning(f"Google returned {response.status_code}")
                
        except Exception as e:
            logger.warning(f"Google search failed: {e}")
        
        # Fallback to DuckDuckGo
        try:
            url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self.config.timeout
            )
            
            if response.status_code == 200 and len(response.text) > 1000:
                logger.info(f"DuckDuckGo success: {len(response.text)} chars")
                return response.text
                
        except Exception as e:
            logger.warning(f"DuckDuckGo failed: {e}")
        
        return None
    
    def _parse_search_results(self, html: str) -> List[str]:
        """Parse search results HTML to extract Twitter URLs"""
        if not html:
            return []
        
        urls = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Handle Google redirect URLs like /url?q=https://twitter.com/...
            if href.startswith('/url?'):
                match = re.search(r'[?&]q=([^&]+)', href)
                if match:
                    href = unquote(match.group(1))
            
            # Check for Twitter/X URLs in href
            if 'twitter.com/' in href or 'x.com/' in href:
                # Handle DuckDuckGo redirect URLs
                if 'uddg=' in href:
                    match = re.search(r'uddg=([^&]+)', href)
                    if match:
                        href = unquote(match.group(1))
                
                # Clean the URL
                href = href.split('&')[0]  # Remove tracking params
                urls.append(href)
                logger.debug(f"Found Twitter URL: {href}")
        
        # Also search in text content for @handles (handles up to 30 chars now)
        text = soup.get_text()
        handle_matches = re.findall(r'@([A-Za-z0-9_]{1,30})', text)
        for handle in handle_matches:
            if validate_twitter_handle(handle):
                urls.append(f"https://twitter.com/{handle}")
        
        # Also look for twitter.com/username patterns in text
        url_matches = re.findall(r'(?:twitter\.com|x\.com)/([A-Za-z0-9_]{1,30})', text)
        for handle in url_matches:
            if validate_twitter_handle(handle):
                urls.append(f"https://twitter.com/{handle}")
        
        logger.info(f"Found {len(urls)} Twitter URLs in results")
        return list(set(urls))  # Remove duplicates
    
    def find_twitter_handle(self, coach_name: str, school: str, 
                           title: str = "", use_cache: bool = True) -> Optional[str]:
        """
        Find Twitter handle for a coach using multiple search strategies.
        
        Args:
            coach_name: Full name of coach (e.g., "John Smith")
            school: School name (e.g., "Ohio State")
            title: Optional title (e.g., "Offensive Line Coach")
            use_cache: Whether to use disk cache (default True)
        
        Returns:
            Twitter handle without @ or None if not found
        """
        # Clean inputs
        coach_name = coach_name.strip()
        school = school.strip()
        
        if not coach_name or not school:
            return None
        
        # Check disk cache first
        if use_cache:
            cached = self._load_from_disk_cache(coach_name, school)
            if cached:
                return cached.get('handle')
        
        # Check memory cache
        cache_key = f"{coach_name}|{school}".lower()
        if cache_key in self._memory_cache:
            return self._memory_cache[cache_key]
        
        # Get name parts
        name_parts = coach_name.split()
        first_name = name_parts[0] if name_parts else ''
        last_name = name_parts[-1] if len(name_parts) > 1 else name_parts[0]
        
        # Clean school name for searching
        school_clean = school.replace('University', '').replace('College', '').replace('State', 'St').strip()
        
        # Better search queries - more specific to Twitter/X
        queries = [
            # Most specific - quoted name + school + twitter
            f'"{coach_name}" {school} twitter.com',
            f'"{coach_name}" {school} x.com',
            # With football context
            f'{coach_name} {school_clean} football twitter',
            # Last name focused
            f'coach {last_name} {school_clean} twitter',
            # Site-specific searches
            f'site:x.com {coach_name} {school_clean}',
            f'site:twitter.com {coach_name} {school_clean}',
        ]
        
        # Add title-specific query if provided
        if title:
            title_short = title.replace('Coach', '').replace('Coordinator', '').strip()
            queries.insert(2, f'{coach_name} {title_short} {school_clean} twitter')
        
        all_urls = []
        best_handle = None
        best_score = 0
        best_query = ''
        
        for query in queries[:6]:  # Limit to 6 queries max
            logger.info(f"Query: {query}")
            
            html = self._search_google(query)
            if html:
                urls = self._parse_search_results(html)
                logger.info(f"Found {len(urls)} Twitter URLs")
                all_urls.extend(urls)
                
                # If we found Twitter URLs, try to extract handle
                if urls:
                    handle, score = self._extract_best_handle_with_score(urls, coach_name, school)
                    if handle and score > best_score:
                        best_handle = handle
                        best_score = score
                        best_query = query
                        
                        # High confidence - stop early
                        if score >= 15:
                            logger.info(f"High confidence match @{handle} for {coach_name} (score: {score})")
                            break
            
            # Delay between searches
            self._random_delay()
        
        # Try with all collected URLs if no high-confidence match
        if not best_handle and all_urls:
            best_handle, best_score = self._extract_best_handle_with_score(all_urls, coach_name, school)
        
        # Calculate confidence (0-1 scale)
        confidence = min(best_score / 30.0, 1.0) if best_score > 0 else 0
        
        # Only return if confidence meets threshold
        if best_handle and confidence >= self.config.min_confidence:
            logger.info(f"Found @{best_handle} for {coach_name} ({school}) - confidence: {confidence:.2f}")
            self._memory_cache[cache_key] = best_handle
            self._save_to_disk_cache(coach_name, school, best_handle, confidence, best_query)
            return best_handle
        
        logger.debug(f"No confident Twitter handle found for {coach_name} ({school})")
        self._memory_cache[cache_key] = None
        self._save_to_disk_cache(coach_name, school, None, 0, '')
        return None
    
    def _extract_best_handle_with_score(self, urls: List[str], coach_name: str, 
                                         school: str) -> Tuple[Optional[str], int]:
        """Extract the best matching handle from URLs with score."""
        handles_with_scores = []
        
        coach_parts = coach_name.lower().split()
        school_lower = school.lower()
        
        for url in urls:
            handle = extract_handle_from_url(url)
            if not handle:
                continue
            
            handle_lower = handle.lower()
            score = 0
            
            # Score based on name match
            for part in coach_parts:
                if len(part) > 2 and part in handle_lower:
                    score += 10
            
            # Score based on school match
            school_words = school_lower.replace('university', '').replace('college', '').split()
            for word in school_words:
                if len(word) > 2 and word in handle_lower:
                    score += 5
            
            # Bonus for "coach" in handle
            if 'coach' in handle_lower:
                score += 8
            
            # Bonus for "football" or "fb" in handle
            if 'football' in handle_lower or 'fb' in handle_lower:
                score += 5
            
            # Bonus for position-related terms
            if any(term in handle_lower for term in ['oline', 'ol', 'oc', 'recruit']):
                score += 4
            
            # Penalty for generic handles
            if handle_lower in ['football', 'coach', 'sports', 'athletics']:
                score -= 20
            
            if score > 0:
                handles_with_scores.append((handle, score))
        
        if not handles_with_scores:
            return None, 0
        
        # Sort by score and return best
        handles_with_scores.sort(key=lambda x: x[1], reverse=True)
        return handles_with_scores[0]
    
    def find_handles_batch(self, coaches: List[Dict[str, str]], 
                          callback=None) -> Dict[str, str]:
        """
        Find Twitter handles for multiple coaches.
        
        Args:
            coaches: List of dicts with 'name', 'school', 'title' keys
            callback: Optional callback(coach_name, handle) for progress
        
        Returns:
            Dict mapping coach names to handles
        """
        results = {}
        
        for i, coach in enumerate(coaches):
            name = coach.get('name', '')
            school = coach.get('school', '')
            title = coach.get('title', '')
            
            if not name or not school:
                continue
            
            handle = self.find_twitter_handle(name, school, title)
            
            if handle:
                results[name] = handle
            
            if callback:
                callback(name, handle)
            
            # Progress logging
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {i + 1}/{len(coaches)} coaches")
        
        return results
    
    def reset_session(self):
        """Reset session and search count"""
        self.session = requests.Session()
        self.search_count = 0
    
    def clear_cache(self) -> int:
        """Clear all cached results. Returns number of entries cleared."""
        count = 0
        for cache_file in self.config.cache_dir.glob('*.json'):
            try:
                cache_file.unlink()
                count += 1
            except:
                pass
        self._memory_cache.clear()
        return count
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics."""
        cache_files = list(self.config.cache_dir.glob('*.json'))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        valid = 0
        expired = 0
        with_handle = 0
        
        for cache_file in cache_files:
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                cached_time = datetime.fromisoformat(data.get('timestamp', '2000-01-01'))
                if datetime.now() - cached_time > timedelta(days=self.config.cache_ttl_days):
                    expired += 1
                else:
                    valid += 1
                    if data.get('handle'):
                        with_handle += 1
            except:
                expired += 1
        
        return {
            'total_entries': len(cache_files),
            'valid_entries': valid,
            'expired_entries': expired,
            'entries_with_handle': with_handle,
            'total_size_kb': round(total_size / 1024, 1),
            'cache_dir': str(self.config.cache_dir)
        }


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_scraper = None

def get_scraper() -> GoogleTwitterScraper:
    """Get singleton scraper instance"""
    global _scraper
    if _scraper is None:
        _scraper = GoogleTwitterScraper()
    return _scraper


def find_coach_twitter(coach_name: str, school: str, 
                       title: str = "") -> Optional[str]:
    """
    Quick function to find a coach's Twitter handle.
    
    Example:
        handle = find_coach_twitter("Lincoln Riley", "USC")
        # Returns: "LincolnRiley" or None
    """
    scraper = get_scraper()
    return scraper.find_twitter_handle(coach_name, school, title)


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=== Google Twitter Scraper Test ===\n")
    
    # Test coaches
    test_coaches = [
        {"name": "Lincoln Riley", "school": "USC", "title": "Head Coach"},
        {"name": "Ryan Day", "school": "Ohio State", "title": "Head Coach"},
        {"name": "Kirby Smart", "school": "Georgia", "title": "Head Coach"},
    ]
    
    scraper = GoogleTwitterScraper()
    
    for coach in test_coaches:
        print(f"Searching for {coach['name']} ({coach['school']})...")
        handle = scraper.find_twitter_handle(
            coach['name'], 
            coach['school'],
            coach['title']
        )
        if handle:
            print(f"  ✓ Found: @{handle}")
        else:
            print(f"  ✗ Not found")
        print()
