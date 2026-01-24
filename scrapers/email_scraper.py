"""
scrapers/email_scraper.py - Enterprise Email Address Extraction
============================================================================
Extracts email addresses for coaches from college athletic staff pages.

Features:
- Multi-strategy extraction (mailto links, text patterns, obfuscated emails)
- Email validation and normalization
- Domain verification against school domains
- Confidence scoring based on name matching
- Bio page deep-linking for individual coach pages

Usage:
    from scrapers.email_scraper import EmailScraper
    scraper = EmailScraper()
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
from urllib.parse import urlparse, unquote, urljoin
from html import unescape

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from browser.manager import BrowserManager, BrowserConfig
from sheets.manager import SheetsManager, SheetsConfig

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class EmailScraperConfig:
    """Configuration for email scraping."""
    # Processing
    batch_size: int = 10
    max_schools: int = 0  # 0 = no limit
    reverse: bool = False  # Start from bottom
    follow_bio_links: bool = True  # Visit individual coach bio pages
    
    # Delays (anti-detection)
    min_delay: float = 2.0
    max_delay: float = 5.0
    batch_break_min: float = 15.0
    batch_break_max: float = 30.0
    
    # Retry
    max_retries: int = 3
    retry_delay: float = 5.0


# ============================================================================
# EMAIL PATTERNS
# ============================================================================

# Standard email pattern
EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

# Obfuscated patterns
OBFUSCATED_PATTERNS = [
    # JavaScript obfuscation
    r"(?:&#\d+;)+",  # HTML entities like &#109;&#97;&#105;&#108;
    r"(?:\\x[0-9a-f]{2})+",  # Hex escapes
    # Common text obfuscations
    r'(\w+)\s*\[\s*at\s*\]\s*(\w+)\s*\[\s*dot\s*\]\s*(\w+)',  # name [at] domain [dot] edu
    r'(\w+)\s*\(\s*at\s*\)\s*(\w+)\s*\(\s*dot\s*\)\s*(\w+)',  # name (at) domain (dot) edu
    r'(\w+)\s+at\s+(\w+)\s+dot\s+(\w+)',  # name at domain dot edu
]

# Common .edu domains for college athletics
EDU_DOMAINS = {
    'edu', 'athletics', 'sports', 'football'
}

# Invalid email patterns to filter
INVALID_EMAIL_PATTERNS = [
    r'example\.com',
    r'test\.com',
    r'sample\.',
    r'noreply',
    r'donotreply',
    r'no-reply',
    r'placeholder',
    r'someone@',
    r'user@',
    r'email@',
]


# ============================================================================
# EMAIL VALIDATOR
# ============================================================================

class EmailValidator:
    """Validates and normalizes email addresses."""
    
    @staticmethod
    def normalize(email: str) -> str:
        """Normalize an email address."""
        if not email:
            return ""
        
        # Lowercase
        email = email.lower().strip()
        
        # Remove any surrounding brackets/parens
        email = re.sub(r'^[<\[\(]+|[>\]\)]+$', '', email)
        
        # Remove mailto: prefix
        if email.startswith('mailto:'):
            email = email[7:]
        
        # Remove any query params
        if '?' in email:
            email = email.split('?')[0]
        
        return email
    
    @staticmethod
    def is_valid(email: str) -> bool:
        """Check if email is valid."""
        if not email:
            return False
        
        email = EmailValidator.normalize(email)
        
        # Check basic format
        if not re.match(EMAIL_PATTERN, email):
            return False
        
        # Check for invalid patterns
        for pattern in INVALID_EMAIL_PATTERNS:
            if re.search(pattern, email, re.IGNORECASE):
                return False
        
        # Must have at least one dot in domain
        parts = email.split('@')
        if len(parts) != 2:
            return False
        if '.' not in parts[1]:
            return False
        
        return True
    
    @staticmethod
    def is_edu_email(email: str) -> bool:
        """Check if email is from an educational institution."""
        if not email:
            return False
        email = EmailValidator.normalize(email)
        domain = email.split('@')[-1] if '@' in email else ''
        return '.edu' in domain.lower()
    
    @staticmethod
    def get_domain(email: str) -> str:
        """Extract domain from email."""
        if not email or '@' not in email:
            return ""
        return email.split('@')[-1].lower()


# ============================================================================
# EMAIL EXTRACTOR
# ============================================================================

class EmailExtractor:
    """
    Extracts email addresses from HTML content.
    
    Strategies:
    1. mailto: links
    2. Plain text email patterns
    3. Obfuscated emails (JavaScript, HTML entities)
    4. Contact page links
    """
    
    def __init__(self):
        self.validator = EmailValidator()
    
    def extract_all(self, html: str, base_url: str = "") -> List[Dict[str, Any]]:
        """
        Extract all email addresses from HTML.
        
        Returns list of dicts with:
        - email: The email address
        - confidence: Confidence score (0-100)
        - source: How it was found
        - context: Surrounding text
        """
        results = []
        seen = set()
        
        # Strategy 1: mailto links (highest confidence)
        for match in self._extract_from_mailto(html):
            email = match['email']
            if email not in seen and self.validator.is_valid(email):
                seen.add(email)
                results.append(match)
        
        # Strategy 2: Plain text emails
        for match in self._extract_from_text(html):
            email = match['email']
            if email not in seen and self.validator.is_valid(email):
                seen.add(email)
                results.append(match)
        
        # Strategy 3: Obfuscated emails
        for match in self._extract_obfuscated(html):
            email = match['email']
            if email not in seen and self.validator.is_valid(email):
                seen.add(email)
                results.append(match)
        
        # Boost .edu emails
        for result in results:
            if self.validator.is_edu_email(result['email']):
                result['confidence'] = min(100, result['confidence'] + 10)
        
        # Sort by confidence
        results.sort(key=lambda x: x['confidence'], reverse=True)
        
        return results
    
    def _extract_from_mailto(self, html: str) -> List[Dict[str, Any]]:
        """Extract from mailto: links."""
        results = []
        
        # Find mailto links
        pattern = r'href=["\']mailto:([^"\'?]+)(?:\?[^"\']*)?["\']'
        
        for match in re.finditer(pattern, html, re.IGNORECASE):
            email = self.validator.normalize(match.group(1))
            
            if email:
                # Try to get surrounding context
                start = max(0, match.start() - 100)
                end = min(len(html), match.end() + 100)
                context = re.sub(r'<[^>]+>', ' ', html[start:end])
                context = re.sub(r'\s+', ' ', context).strip()[:100]
                
                results.append({
                    'email': email,
                    'confidence': 95,
                    'source': 'mailto_link',
                    'context': context,
                })
        
        return results
    
    def _extract_from_text(self, html: str) -> List[Dict[str, Any]]:
        """Extract from visible text."""
        results = []
        
        # Remove script/style content
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        
        # Decode HTML entities
        text = unescape(text)
        
        # Find emails
        for match in re.finditer(EMAIL_PATTERN, text):
            email = self.validator.normalize(match.group(0))
            
            if email:
                # Get context
                start = max(0, match.start() - 50)
                end = min(len(text), match.end() + 50)
                context = text[start:end].strip()
                context = re.sub(r'\s+', ' ', context)[:100]
                
                results.append({
                    'email': email,
                    'confidence': 80,
                    'source': 'text_pattern',
                    'context': context,
                })
        
        return results
    
    def _extract_obfuscated(self, html: str) -> List[Dict[str, Any]]:
        """Extract obfuscated emails."""
        results = []
        
        # HTML entity decode
        decoded = unescape(html)
        
        # Look for entity-encoded emails
        entity_pattern = r'((?:&#\d+;){5,50})'
        for match in re.finditer(entity_pattern, html):
            try:
                decoded_text = unescape(match.group(1))
                if '@' in decoded_text:
                    email_match = re.search(EMAIL_PATTERN, decoded_text)
                    if email_match:
                        email = self.validator.normalize(email_match.group(0))
                        if email:
                            results.append({
                                'email': email,
                                'confidence': 75,
                                'source': 'html_entities',
                                'context': decoded_text[:100],
                            })
            except:
                pass
        
        # Text obfuscation patterns
        for pattern in OBFUSCATED_PATTERNS[2:]:  # Skip JS patterns
            for match in re.finditer(pattern, decoded, re.IGNORECASE):
                try:
                    groups = match.groups()
                    if len(groups) >= 3:
                        email = f"{groups[0]}@{groups[1]}.{groups[2]}"
                        email = self.validator.normalize(email)
                        if email:
                            results.append({
                                'email': email,
                                'confidence': 70,
                                'source': 'text_obfuscation',
                                'context': match.group(0),
                            })
                except:
                    pass
        
        return results
    
    def find_bio_links(self, html: str, base_url: str) -> List[str]:
        """Find links to individual coach bio pages."""
        links = []
        
        # Patterns for bio links
        bio_patterns = [
            r'href=["\']([^"\']*(?:bio|profile|staff|coach)[^"\']*)["\']',
            r'href=["\']([^"\']*\.aspx\?[^"\']*)["\']',  # Common in SIDEARM
        ]
        
        for pattern in bio_patterns:
            for match in re.finditer(pattern, html, re.IGNORECASE):
                href = match.group(1)
                
                # Make absolute
                if href.startswith('/'):
                    parsed = urlparse(base_url)
                    href = f"{parsed.scheme}://{parsed.netloc}{href}"
                elif not href.startswith('http'):
                    href = urljoin(base_url, href)
                
                if href not in links:
                    links.append(href)
        
        return links[:10]  # Limit to 10 links


# ============================================================================
# EMAIL SCRAPER
# ============================================================================

class EmailScraper:
    """
    Enterprise-grade email scraper for coach contact info.
    
    Scrapes email addresses from staff directory and bio pages.
    Matches emails to coaches by name proximity.
    """
    
    def __init__(self, config: Optional[EmailScraperConfig] = None):
        self.config = config or EmailScraperConfig()
        self.browser = BrowserManager(BrowserConfig(headless=False))
        self.sheets = SheetsManager()
        self.extractor = EmailExtractor()
        
        # Progress
        self.processed = 0
        self.found = 0
        self.errors = 0
        self._running = True
        
        self.logger = logging.getLogger(__name__)
    
    def run(
        self,
        callback: Optional[Callable[[str, Dict], None]] = None,
        resume: bool = False
    ) -> Dict[str, int]:
        """Run the email scraper."""
        self._running = True
        
        try:
            # Connect
            self._emit(callback, 'status', {'message': 'Connecting to Google Sheets...'})
            if not self.sheets.connect():
                self._emit(callback, 'error', {'message': 'Failed to connect to Sheets'})
                return self._get_stats()
            
            # Get schools
            self._emit(callback, 'status', {'message': 'Finding schools without emails...'})
            schools = self._get_schools_needing_email()
            
            if self.config.reverse:
                schools.reverse()
            
            if self.config.max_schools > 0:
                schools = schools[:self.config.max_schools]
            
            if not schools:
                self._emit(callback, 'status', {'message': 'No schools need emails!'})
                return self._get_stats()
            
            self._emit(callback, 'schools_found', {'count': len(schools)})
            
            # Start browser
            self._emit(callback, 'status', {'message': 'Starting browser...'})
            if not self.browser.start():
                self._emit(callback, 'error', {'message': 'Failed to start browser'})
                return self._get_stats()
            
            # Process
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
        """Stop scraper."""
        self._running = False
    
    def _get_schools_needing_email(self) -> List[Dict]:
        """
        Get schools that need email addresses.

        Includes schools where:
        - Coach has name but no email
        - Email Status is 'wrong' (bad email, needs re-scrape)
        """
        data = self.sheets.get_all_data()
        if len(data) < 2:
            return []

        headers = data[0]
        rows = data[1:]

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
        ol_email_col = find_col(['oc email', 'ol email'])
        rc_email_col = find_col(['rc email'])
        # New: Email status columns for re-scraping
        ol_email_status_col = find_col(['ol email status'])
        rc_email_status_col = find_col(['rc email status'])

        schools = []

        for row_idx, row in enumerate(rows):
            row_num = row_idx + 2

            school = row[school_col] if school_col >= 0 and school_col < len(row) else ''
            url = row[url_col] if url_col >= 0 and url_col < len(row) else ''
            ol_name = row[ol_name_col] if ol_name_col >= 0 and ol_name_col < len(row) else ''
            rc_name = row[rc_name_col] if rc_name_col >= 0 and rc_name_col < len(row) else ''
            ol_email = row[ol_email_col] if ol_email_col >= 0 and ol_email_col < len(row) else ''
            rc_email = row[rc_email_col] if rc_email_col >= 0 and rc_email_col < len(row) else ''
            ol_email_status = row[ol_email_status_col] if ol_email_status_col >= 0 and ol_email_status_col < len(row) else ''
            rc_email_status = row[rc_email_status_col] if rc_email_status_col >= 0 and rc_email_status_col < len(row) else ''

            if not url or not url.startswith('http'):
                continue

            # Check if email status is "wrong" (needs re-scrape)
            ol_email_wrong = ol_email_status.strip().lower() == 'wrong'
            rc_email_wrong = rc_email_status.strip().lower() == 'wrong'

            # Need to scrape if: no email OR email marked as wrong
            needs_ol = ol_name and not ol_name.startswith('REVIEW:') and (not ol_email or ol_email_wrong)
            needs_rc = rc_name and not rc_name.startswith('REVIEW:') and (not rc_email or rc_email_wrong)

            if needs_ol or needs_rc:
                schools.append({
                    'row': row_num,
                    'name': school,
                    'url': url,
                    'ol_name': ol_name if needs_ol else '',
                    'rc_name': rc_name if needs_rc else '',
                    'ol_email_col': ol_email_col + 1,
                    'rc_email_col': rc_email_col + 1,
                    'ol_email_status_col': ol_email_status_col + 1 if ol_email_status_col >= 0 else None,
                    'rc_email_status_col': rc_email_status_col + 1 if rc_email_status_col >= 0 else None,
                    'ol_needs_rescrape': ol_email_wrong,
                    'rc_needs_rescrape': rc_email_wrong,
                })

        return schools
    
    def _process_school(self, school: Dict, callback: Callable):
        """Process a single school."""
        try:
            # Load main page
            html = self.browser.get_page(school['url'])
            if not html:
                self.errors += 1
                return
            
            # Extract emails from main page
            all_emails = self.extractor.extract_all(html, school['url'])
            
            # Optionally check bio pages
            if self.config.follow_bio_links and len(all_emails) < 2:
                bio_links = self.extractor.find_bio_links(html, school['url'])
                for link in bio_links[:3]:  # Check up to 3 bio pages
                    if not self._running:
                        break
                    try:
                        bio_html = self.browser.get_page(link)
                        if bio_html:
                            bio_emails = self.extractor.extract_all(bio_html, link)
                            all_emails.extend(bio_emails)
                        time.sleep(1)
                    except:
                        pass
            
            ol_found = False
            rc_found = False
            
            # Match emails to coaches
            for email_data in all_emails:
                email = email_data['email']
                context = email_data.get('context', '').lower()
                
                # Try OL match
                if school.get('ol_name') and not ol_found:
                    ol_parts = school['ol_name'].lower().split()
                    for part in ol_parts:
                        if len(part) > 2 and (part in email.lower() or part in context):
                            self.sheets.update_cell(
                                school['row'],
                                school['ol_email_col'],
                                email
                            )
                            # Clear "wrong" status if this was a re-scrape
                            if school.get('ol_needs_rescrape') and school.get('ol_email_status_col'):
                                self.sheets.update_cell(
                                    school['row'],
                                    school['ol_email_status_col'],
                                    ''  # Clear the "wrong" status
                                )
                            ol_found = True
                            self.found += 1
                            self._emit(callback, 'found', {
                                'school': school['name'],
                                'type': 'OL',
                                'email': email,
                                'was_rescrape': school.get('ol_needs_rescrape', False)
                            })
                            break

                # Try RC match
                if school.get('rc_name') and not rc_found:
                    rc_parts = school['rc_name'].lower().split()
                    for part in rc_parts:
                        if len(part) > 2 and (part in email.lower() or part in context):
                            self.sheets.update_cell(
                                school['row'],
                                school['rc_email_col'],
                                email
                            )
                            # Clear "wrong" status if this was a re-scrape
                            if school.get('rc_needs_rescrape') and school.get('rc_email_status_col'):
                                self.sheets.update_cell(
                                    school['row'],
                                    school['rc_email_status_col'],
                                    ''  # Clear the "wrong" status
                                )
                            rc_found = True
                            self.found += 1
                            self._emit(callback, 'found', {
                                'school': school['name'],
                                'type': 'RC',
                                'email': email,
                                'was_rescrape': school.get('rc_needs_rescrape', False)
                            })
                            break
            
            self.processed += 1
            
            self._emit(callback, 'school_processed', {
                'school': school['name'],
                'ol_found': ol_found,
                'rc_found': rc_found
            })
            
        except Exception as e:
            self.logger.error(f"Error processing {school['name']}: {e}")
            self.errors += 1
    
    def _smart_delay(self, current: int, total: int):
        """Delay between requests."""
        import random
        delay = random.uniform(self.config.min_delay, self.config.max_delay)
        if current > 0 and current % self.config.batch_size == 0:
            delay = random.uniform(self.config.batch_break_min, self.config.batch_break_max)
        time.sleep(delay)
    
    def _emit(self, callback: Optional[Callable], event: str, data: Dict):
        """Emit event."""
        if callback:
            callback(event, data)
    
    def _get_stats(self) -> Dict[str, int]:
        """Get stats."""
        return {
            'processed': self.processed,
            'found': self.found,
            'errors': self.errors
        }
    
    def _cleanup(self):
        """Cleanup."""
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
    
    parser = argparse.ArgumentParser(description='Email Scraper')
    parser.add_argument('--reverse', action='store_true', help='Start from bottom')
    parser.add_argument('--limit', type=int, default=0, help='Max schools')
    parser.add_argument('--no-bio', action='store_true', help='Skip bio pages')
    args = parser.parse_args()
    
    config = EmailScraperConfig(
        reverse=args.reverse,
        max_schools=args.limit,
        follow_bio_links=not args.no_bio
    )
    
    scraper = EmailScraper(config)
    
    def callback(event, data):
        if event == 'processing':
            print(f"[{data['current']}/{data['total']}] {data['school']}")
        elif event == 'found':
            print(f"  ✓ {data['type']}: {data['email']}")
        elif event == 'error':
            print(f"  ✗ Error: {data['message']}")
    
    result = scraper.run(callback=callback)
    print(f"\nDone! Processed: {result['processed']}, Found: {result['found']}, Errors: {result['errors']}")


if __name__ == '__main__':
    main()
