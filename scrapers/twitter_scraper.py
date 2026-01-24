"""
scrapers/twitter_scraper.py - Enterprise Twitter Handle Extraction
============================================================================
Extracts Twitter/X handles from college athletic staff directory pages.

Features:
- Multi-strategy extraction (direct links, text patterns, JavaScript)
- Handle validation and normalization
- Confidence scoring
- Anti-detection measures
- Rate limiting and retry logic

Usage:
    from scrapers.twitter_scraper import TwitterScraper
    scraper = TwitterScraper()
    scraper.run()

Author: Coach Outreach System
Version: 3.0.0
============================================================================
"""

import re
import sys
import time
import logging
from typing import Optional, List, Dict, Any, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, unquote

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from browser.manager import BrowserManager, BrowserConfig
from sheets.manager import SheetsManager, SheetsConfig

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class TwitterScraperConfig:
    """Configuration for Twitter handle scraping."""
    # Processing
    batch_size: int = 10
    max_schools: int = 0  # 0 = no limit
    reverse: bool = False  # Start from bottom
    
    # Delays (anti-detection)
    min_delay: float = 2.0
    max_delay: float = 5.0
    batch_break_min: float = 15.0
    batch_break_max: float = 30.0
    
    # Retry
    max_retries: int = 3
    retry_delay: float = 5.0


# ============================================================================
# TWITTER PATTERNS
# ============================================================================

# Patterns to find Twitter handles
TWITTER_URL_PATTERNS = [
    # Direct URL patterns
    r'(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/(@?[\w]{1,15})(?:\?|/|$)',
    r'(?:https?://)?(?:mobile\.)?(?:twitter\.com|x\.com)/(@?[\w]{1,15})(?:\?|/|$)',
]

# Text patterns for handles
TWITTER_TEXT_PATTERNS = [
    r'(?:twitter|x)[\s:]*@([\w]{1,15})',
    r'@([\w]{1,15})[\s]*(?:on\s+)?(?:twitter|x)',
    r'follow[\s:]+@([\w]{1,15})',
    r'tweet[\s:]+@([\w]{1,15})',
]

# Handles to ignore (generic/invalid)
INVALID_HANDLES = {
    'home', 'share', 'intent', 'login', 'signup', 'explore',
    'search', 'settings', 'messages', 'notifications', 'compose',
    'i', 'hashtag', 'following', 'followers', 'lists', 'moments',
    'twitter', 'x', 'help', 'about', 'tos', 'privacy', 'status',
}


# ============================================================================
# HANDLE VALIDATOR
# ============================================================================

class HandleValidator:
    """Validates and normalizes Twitter handles."""
    
    @staticmethod
    def normalize(handle: str) -> str:
        """Normalize a Twitter handle."""
        if not handle:
            return ""
        
        # Remove @ prefix if present
        handle = handle.lstrip('@')
        
        # Remove any URL components
        if '/' in handle:
            handle = handle.split('/')[0]
        if '?' in handle:
            handle = handle.split('?')[0]
        
        # Clean up
        handle = handle.strip().lower()
        
        return handle
    
    @staticmethod
    def is_valid(handle: str) -> bool:
        """Check if a handle is valid."""
        if not handle:
            return False
        
        handle = HandleValidator.normalize(handle)
        
        # Check length (1-15 chars)
        if len(handle) < 1 or len(handle) > 15:
            return False
        
        # Check characters (alphanumeric and underscore only)
        if not re.match(r'^[\w]+$', handle):
            return False
        
        # Check against invalid handles
        if handle.lower() in INVALID_HANDLES:
            return False
        
        return True
    
    @staticmethod
    def format_for_display(handle: str) -> str:
        """Format handle for display (with @)."""
        handle = HandleValidator.normalize(handle)
        return f"@{handle}" if handle else ""


# ============================================================================
# TWITTER EXTRACTOR
# ============================================================================

class TwitterExtractor:
    """
    Extracts Twitter handles from HTML content.
    
    Uses multiple strategies:
    1. Direct href links to twitter.com/x.com
    2. Data attributes containing handles
    3. Text patterns with @ mentions
    4. Social icon links
    """
    
    def __init__(self):
        self.validator = HandleValidator()
    
    def extract_all(self, html: str, page_url: str = "") -> List[Dict[str, Any]]:
        """
        Extract all Twitter handles from HTML.
        
        Returns list of dicts with:
        - handle: The Twitter handle
        - confidence: Confidence score (0-100)
        - source: How it was found
        - context: Surrounding text/element
        """
        results = []
        seen = set()
        
        # Strategy 1: Direct links
        for match in self._extract_from_links(html):
            if match['handle'] not in seen:
                seen.add(match['handle'])
                results.append(match)
        
        # Strategy 2: Data attributes
        for match in self._extract_from_data_attrs(html):
            if match['handle'] not in seen:
                seen.add(match['handle'])
                results.append(match)
        
        # Strategy 3: Text patterns
        for match in self._extract_from_text(html):
            if match['handle'] not in seen:
                seen.add(match['handle'])
                results.append(match)
        
        # Strategy 4: Social icons
        for match in self._extract_from_social_icons(html):
            if match['handle'] not in seen:
                seen.add(match['handle'])
                results.append(match)
        
        # Sort by confidence
        results.sort(key=lambda x: x['confidence'], reverse=True)
        
        return results
    
    def _extract_from_links(self, html: str) -> List[Dict[str, Any]]:
        """Extract from <a href> links."""
        results = []
        
        # Find all href attributes pointing to Twitter/X
        href_pattern = r'href=["\']([^"\']*(?:twitter\.com|x\.com)/[^"\']*)["\']'
        
        for match in re.finditer(href_pattern, html, re.IGNORECASE):
            url = match.group(1)
            handle = self._extract_handle_from_url(url)
            
            if handle and self.validator.is_valid(handle):
                results.append({
                    'handle': self.validator.normalize(handle),
                    'confidence': 90,
                    'source': 'direct_link',
                    'context': url[:100],
                })
        
        return results
    
    def _extract_from_data_attrs(self, html: str) -> List[Dict[str, Any]]:
        """Extract from data-* attributes."""
        results = []
        
        # Look for data attributes with Twitter URLs or handles
        data_pattern = r'data-[\w-]+=["\']([^"\']*(?:twitter|@[\w]{1,15})[^"\']*)["\']'
        
        for match in re.finditer(data_pattern, html, re.IGNORECASE):
            value = match.group(1)
            
            # Try to extract handle
            if 'twitter.com' in value or 'x.com' in value:
                handle = self._extract_handle_from_url(value)
            elif value.startswith('@'):
                handle = value[1:]
            else:
                continue
            
            if handle and self.validator.is_valid(handle):
                results.append({
                    'handle': self.validator.normalize(handle),
                    'confidence': 80,
                    'source': 'data_attribute',
                    'context': value[:100],
                })
        
        return results
    
    def _extract_from_text(self, html: str) -> List[Dict[str, Any]]:
        """Extract from visible text patterns."""
        results = []
        
        # Remove HTML tags for text analysis
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text)
        
        for pattern in TWITTER_TEXT_PATTERNS:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                handle = match.group(1)
                
                if handle and self.validator.is_valid(handle):
                    # Get context
                    start = max(0, match.start() - 30)
                    end = min(len(text), match.end() + 30)
                    context = text[start:end].strip()
                    
                    results.append({
                        'handle': self.validator.normalize(handle),
                        'confidence': 70,
                        'source': 'text_pattern',
                        'context': context[:100],
                    })
        
        return results
    
    def _extract_from_social_icons(self, html: str) -> List[Dict[str, Any]]:
        """Extract from social media icon links."""
        results = []
        
        # Pattern for social icon containers
        icon_patterns = [
            r'class=["\'][^"\']*(?:social|twitter|icon)[^"\']*["\'][^>]*>.*?href=["\']([^"\']*twitter\.com[^"\']*)["\']',
            r'href=["\']([^"\']*twitter\.com[^"\']*)["\'][^>]*class=["\'][^"\']*(?:social|twitter|icon)',
            r'class=["\'][^"\']*(?:social|twitter|icon)[^"\']*["\'][^>]*>.*?href=["\']([^"\']*x\.com[^"\']*)["\']',
        ]
        
        for pattern in icon_patterns:
            for match in re.finditer(pattern, html, re.IGNORECASE | re.DOTALL):
                url = match.group(1)
                handle = self._extract_handle_from_url(url)
                
                if handle and self.validator.is_valid(handle):
                    results.append({
                        'handle': self.validator.normalize(handle),
                        'confidence': 85,
                        'source': 'social_icon',
                        'context': url[:100],
                    })
        
        return results
    
    def _extract_handle_from_url(self, url: str) -> Optional[str]:
        """Extract handle from a Twitter/X URL."""
        if not url:
            return None
        
        # Decode URL
        url = unquote(url)
        
        # Try each pattern
        for pattern in TWITTER_URL_PATTERNS:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                handle = match.group(1)
                return self.validator.normalize(handle)
        
        return None


# ============================================================================
# TWITTER SCRAPER
# ============================================================================

class TwitterScraper:
    """
    Enterprise-grade Twitter handle scraper.
    
    Scrapes Twitter handles for coaches from staff directory pages.
    Updates Google Sheet with found handles.
    """
    
    def __init__(self, config: Optional[TwitterScraperConfig] = None):
        self.config = config or TwitterScraperConfig()
        self.browser = BrowserManager(BrowserConfig(headless=False))
        self.sheets = SheetsManager()
        self.extractor = TwitterExtractor()
        
        # Progress tracking
        self.processed = 0
        self.found = 0
        self.errors = 0
        self._running = True
        
        # Logging
        self.logger = logging.getLogger(__name__)
    
    def run(
        self,
        callback: Optional[Callable[[str, Dict], None]] = None,
        resume: bool = False
    ) -> Dict[str, int]:
        """
        Run the Twitter handle scraper.
        
        Args:
            callback: Function to receive progress updates
            resume: Whether to resume from last position
            
        Returns:
            Dict with counts: processed, found, errors
        """
        self._running = True
        
        try:
            # Connect to sheets
            self._emit(callback, 'status', {'message': 'Connecting to Google Sheets...'})
            if not self.sheets.connect():
                self._emit(callback, 'error', {'message': 'Failed to connect to Sheets'})
                return self._get_stats()
            
            # Get schools to process
            self._emit(callback, 'status', {'message': 'Finding schools without Twitter handles...'})
            schools = self._get_schools_needing_twitter()
            
            if self.config.reverse:
                schools.reverse()
            
            if self.config.max_schools > 0:
                schools = schools[:self.config.max_schools]
            
            if not schools:
                self._emit(callback, 'status', {'message': 'No schools need Twitter handles!'})
                return self._get_stats()
            
            self._emit(callback, 'schools_found', {'count': len(schools)})
            
            # Start browser
            self._emit(callback, 'status', {'message': 'Starting browser...'})
            if not self.browser.start():
                self._emit(callback, 'error', {'message': 'Failed to start browser'})
                return self._get_stats()
            
            # Process schools
            total = len(schools)
            for idx, school in enumerate(schools):
                if not self._running:
                    break
                
                self._emit(callback, 'processing', {
                    'school': school['name'],
                    'current': idx + 1,
                    'total': total
                })
                
                self._process_school(school, callback)
                
                # Delay between schools
                self._smart_delay(idx, total)
            
            self._emit(callback, 'completed', self._get_stats())
            
        except KeyboardInterrupt:
            self._emit(callback, 'stopped', {'reason': 'user'})
        except Exception as e:
            self.logger.error(f"Error: {e}")
            self._emit(callback, 'error', {'message': str(e)})
        finally:
            self._cleanup()
        
        return self._get_stats()
    
    def stop(self):
        """Stop the scraper."""
        self._running = False
    
    def _get_schools_needing_twitter(self) -> List[Dict]:
        """
        Get schools that need Twitter handles.

        Includes schools where:
        - Coach has name but no Twitter
        - Twitter Status is 'wrong' (bad handle, needs re-scrape)
        """
        data = self.sheets.get_all_data()
        if len(data) < 2:
            return []

        headers = data[0]
        rows = data[1:]

        # Find columns
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower()
                for kw in keywords:
                    if kw in h_lower:
                        return i
            return -1

        school_col = find_col(['school'])
        url_col = find_col(['url'])
        ol_name_col = find_col(['oline', 'ol coach'])
        rc_name_col = find_col(['recruiting'])
        ol_twitter_col = find_col(['oc twitter', 'ol twitter'])
        rc_twitter_col = find_col(['rc twitter'])
        # New: Twitter status columns for re-scraping
        ol_twitter_status_col = find_col(['ol twitter status'])
        rc_twitter_status_col = find_col(['rc twitter status'])

        schools = []

        for row_idx, row in enumerate(rows):
            row_num = row_idx + 2

            school = row[school_col] if school_col >= 0 and school_col < len(row) else ''
            url = row[url_col] if url_col >= 0 and url_col < len(row) else ''
            ol_name = row[ol_name_col] if ol_name_col >= 0 and ol_name_col < len(row) else ''
            rc_name = row[rc_name_col] if rc_name_col >= 0 and rc_name_col < len(row) else ''
            ol_twitter = row[ol_twitter_col] if ol_twitter_col >= 0 and ol_twitter_col < len(row) else ''
            rc_twitter = row[rc_twitter_col] if rc_twitter_col >= 0 and rc_twitter_col < len(row) else ''
            ol_twitter_status = row[ol_twitter_status_col] if ol_twitter_status_col >= 0 and ol_twitter_status_col < len(row) else ''
            rc_twitter_status = row[rc_twitter_status_col] if rc_twitter_status_col >= 0 and rc_twitter_status_col < len(row) else ''

            if not url or not url.startswith('http'):
                continue

            # Check if Twitter status is "wrong" (needs re-scrape)
            ol_twitter_wrong = ol_twitter_status.strip().lower() == 'wrong'
            rc_twitter_wrong = rc_twitter_status.strip().lower() == 'wrong'

            # Need to scrape if: no Twitter OR Twitter marked as wrong
            needs_ol = ol_name and not ol_name.startswith('REVIEW:') and (not ol_twitter or ol_twitter_wrong)
            needs_rc = rc_name and not rc_name.startswith('REVIEW:') and (not rc_twitter or rc_twitter_wrong)

            if needs_ol or needs_rc:
                schools.append({
                    'row': row_num,
                    'name': school,
                    'url': url,
                    'ol_name': ol_name if needs_ol else '',
                    'rc_name': rc_name if needs_rc else '',
                    'ol_twitter_col': ol_twitter_col + 1,  # 1-indexed
                    'rc_twitter_col': rc_twitter_col + 1,
                    'ol_twitter_status_col': ol_twitter_status_col + 1 if ol_twitter_status_col >= 0 else None,
                    'rc_twitter_status_col': rc_twitter_status_col + 1 if rc_twitter_status_col >= 0 else None,
                    'ol_needs_rescrape': ol_twitter_wrong,
                    'rc_needs_rescrape': rc_twitter_wrong,
                })

        return schools
    
    def _process_school(self, school: Dict, callback: Callable):
        """Process a single school."""
        try:
            # Load page
            html = self.browser.get_page(school['url'])
            if not html:
                self.errors += 1
                return
            
            # Extract handles
            handles = self.extractor.extract_all(html, school['url'])
            
            ol_found = False
            rc_found = False
            
            # Try to match handles to coaches
            for handle_data in handles:
                handle = handle_data['handle']
                
                # Check if this handle is likely for OL coach
                if school.get('ol_name') and not ol_found:
                    # Look for name match in context
                    ol_last = school['ol_name'].split()[-1].lower() if school['ol_name'] else ''
                    if ol_last and ol_last in handle_data.get('context', '').lower():
                        self.sheets.update_cell(
                            school['row'],
                            school['ol_twitter_col'],
                            f"@{handle}"
                        )
                        # Clear "wrong" status if this was a re-scrape
                        if school.get('ol_needs_rescrape') and school.get('ol_twitter_status_col'):
                            self.sheets.update_cell(
                                school['row'],
                                school['ol_twitter_status_col'],
                                ''  # Clear the "wrong" status
                            )
                        ol_found = True
                        self.found += 1
                        self._emit(callback, 'found', {
                            'school': school['name'],
                            'type': 'OL',
                            'handle': f"@{handle}",
                            'was_rescrape': school.get('ol_needs_rescrape', False)
                        })

                # Check RC
                if school.get('rc_name') and not rc_found:
                    rc_last = school['rc_name'].split()[-1].lower() if school['rc_name'] else ''
                    if rc_last and rc_last in handle_data.get('context', '').lower():
                        self.sheets.update_cell(
                            school['row'],
                            school['rc_twitter_col'],
                            f"@{handle}"
                        )
                        # Clear "wrong" status if this was a re-scrape
                        if school.get('rc_needs_rescrape') and school.get('rc_twitter_status_col'):
                            self.sheets.update_cell(
                                school['row'],
                                school['rc_twitter_status_col'],
                                ''  # Clear the "wrong" status
                            )
                        rc_found = True
                        self.found += 1
                        self._emit(callback, 'found', {
                            'school': school['name'],
                            'type': 'RC',
                            'handle': f"@{handle}",
                            'was_rescrape': school.get('rc_needs_rescrape', False)
                        })
            
            # FALLBACK: If not found on staff page, try Google search
            if not ol_found and school.get('ol_name'):
                handle = self._google_search_twitter(school['ol_name'], school['name'])
                if handle:
                    self.sheets.update_cell(school['row'], school['ol_twitter_col'], f"@{handle}")
                    # Clear "wrong" status if this was a re-scrape
                    if school.get('ol_needs_rescrape') and school.get('ol_twitter_status_col'):
                        self.sheets.update_cell(school['row'], school['ol_twitter_status_col'], '')
                    ol_found = True
                    self.found += 1
                    self._emit(callback, 'found', {
                        'school': school['name'],
                        'type': 'OL (Google)',
                        'handle': f"@{handle}",
                        'was_rescrape': school.get('ol_needs_rescrape', False)
                    })

            if not rc_found and school.get('rc_name'):
                handle = self._google_search_twitter(school['rc_name'], school['name'])
                if handle:
                    self.sheets.update_cell(school['row'], school['rc_twitter_col'], f"@{handle}")
                    # Clear "wrong" status if this was a re-scrape
                    if school.get('rc_needs_rescrape') and school.get('rc_twitter_status_col'):
                        self.sheets.update_cell(school['row'], school['rc_twitter_status_col'], '')
                    rc_found = True
                    self.found += 1
                    self._emit(callback, 'found', {
                        'school': school['name'],
                        'type': 'RC (Google)',
                        'handle': f"@{handle}",
                        'was_rescrape': school.get('rc_needs_rescrape', False)
                    })
            
            self.processed += 1
            
            self._emit(callback, 'school_processed', {
                'school': school['name'],
                'ol_found': ol_found,
                'rc_found': rc_found
            })
            
        except Exception as e:
            self.logger.error(f"Error processing {school['name']}: {e}")
            self.errors += 1
    
    def _google_search_twitter(self, coach_name: str, school_name: str) -> Optional[str]:
        """
        Search Google for a coach's Twitter handle.
        
        Searches: "coach name" + school + (twitter OR x.com)
        """
        if not coach_name:
            return None
        
        try:
            import random
            
            # Build search query
            query = f'"{coach_name}" {school_name} (twitter OR x.com)'
            search_url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
            
            # Small delay before Google search
            time.sleep(random.uniform(1.0, 2.0))
            
            html = self.browser.get_page(search_url)
            if not html:
                return None
            
            # Look for Twitter/X URLs in results
            handles = self.extractor.extract_all(html, search_url)
            
            if handles:
                # Return highest confidence handle
                return handles[0]['handle']
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Google search failed for {coach_name}: {e}")
            return None
    
    def _smart_delay(self, current: int, total: int):
        """Smart delay between requests."""
        import random
        
        # Regular delay
        delay = random.uniform(self.config.min_delay, self.config.max_delay)
        
        # Batch break
        if current > 0 and current % self.config.batch_size == 0:
            delay = random.uniform(self.config.batch_break_min, self.config.batch_break_max)
        
        time.sleep(delay)
    
    def _emit(self, callback: Optional[Callable], event: str, data: Dict):
        """Emit event to callback."""
        if callback:
            callback(event, data)
    
    def _get_stats(self) -> Dict[str, int]:
        """Get current stats."""
        return {
            'processed': self.processed,
            'found': self.found,
            'errors': self.errors
        }
    
    def _cleanup(self):
        """Clean up resources."""
        try:
            self.browser.stop()
        except:
            pass
        try:
            self.sheets.disconnect()
        except:
            pass


# ============================================================================
# CLI
# ============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Twitter Handle Scraper')
    parser.add_argument('--reverse', action='store_true', help='Start from bottom')
    parser.add_argument('--limit', type=int, default=0, help='Max schools')
    args = parser.parse_args()
    
    config = TwitterScraperConfig(
        reverse=args.reverse,
        max_schools=args.limit
    )
    
    scraper = TwitterScraper(config)
    
    def callback(event, data):
        if event == 'processing':
            print(f"[{data['current']}/{data['total']}] {data['school']}")
        elif event == 'found':
            print(f"  ✓ {data['type']}: {data['handle']}")
        elif event == 'error':
            print(f"  ✗ Error: {data['message']}")
    
    result = scraper.run(callback=callback)
    print(f"\nDone! Processed: {result['processed']}, Found: {result['found']}, Errors: {result['errors']}")


if __name__ == '__main__':
    main()
