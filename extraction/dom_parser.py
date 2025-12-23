"""
extraction/dom_parser.py - DOM Parsing and Staff Extraction Engine
============================================================================
Enterprise-grade HTML parsing with multi-strategy staff extraction.

This module provides comprehensive DOM parsing capabilities for extracting
staff information from college athletic websites. It employs multiple
extraction strategies with fallback mechanisms.

Extraction Strategies (in order of reliability):
1. Structured Data (JSON-LD, Microdata) - Most reliable when present
2. Staff Cards - Common layout pattern in modern athletic sites
3. DOM Proximity - Correlate elements by tree position
4. Table Parsing - Handle tabular staff directories
5. Text Pattern - Raw text analysis as fallback
6. Heuristic Scan - Last resort full-page analysis

Design Principles:
- Multiple strategies with automatic fallback
- Confidence scoring for all extractions
- Full diagnostic output for debugging
- No silent failures
- Comprehensive logging

Author: Coach Outreach System  
Version: 2.0.0
============================================================================
"""

import re
import json
import hashlib
from typing import (
    List, Dict, Optional, Tuple, Set, Any,
    Iterator, Callable, Union
)
from dataclasses import dataclass, field
from datetime import datetime
from bs4 import BeautifulSoup, Tag, NavigableString, Comment
from urllib.parse import urljoin, urlparse
import logging

from core.types import (
    StaffMember, 
    ContactInfo,
    ExtractionResult,
    ExtractionStrategy,
    CanonicalRole,
    ConfidenceLevel,
)
from core.normalizer import (
    normalize_name,
    normalize_title,
    is_valid_name,
    normalize_whitespace,
)
from core.classifier import (
    classify_role,
    is_ol_coach,
    is_recruiting_coordinator,
    get_classifier,
)

# Configure logging
logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

# Minimum confidence thresholds
MIN_NAME_CONFIDENCE = 50
MIN_TITLE_CONFIDENCE = 30

# Maximum elements to process (prevents runaway on huge pages)
MAX_CARDS_TO_PROCESS = 200
MAX_TABLES_TO_PROCESS = 50
MAX_TEXT_LINES = 5000

# CSS selectors for finding staff cards (ordered by specificity)
STAFF_CARD_SELECTORS = [
    # Sidearm Sports (very common for college athletics)
    '.sidearm-roster-coach',
    '.sidearm-roster-coach-container',
    '.s-person-card',
    '.s-staff-bio',
    
    # SIDEARM specific patterns
    '[class*="sidearm"][class*="coach"]',
    '[class*="sidearm"][class*="staff"]',
    
    # Generic coach/staff patterns
    '[class*="coach-card"]',
    '[class*="staff-card"]',
    '[class*="coach-bio"]',
    '[class*="staff-bio"]',
    '[class*="coach-item"]',
    '[class*="staff-item"]',
    '[class*="coach-member"]',
    '[class*="staff-member"]',
    
    # Person/profile patterns
    '[class*="person-card"]',
    '[class*="profile-card"]',
    '[class*="team-member"]',
    '[class*="directory-item"]',
    
    # Generic card patterns (lower priority)
    '.coach',
    '.staff',
    '.bio-card',
    '.member-card',
    '.profile',
    
    # List item patterns
    'li[class*="coach"]',
    'li[class*="staff"]',
    
    # Article patterns
    'article[class*="coach"]',
    'article[class*="staff"]',
    
    # ID-based patterns
    '[id*="coach"]',
    '[id*="staff"]',
]

# Elements typically containing names
NAME_ELEMENT_SELECTORS = [
    'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
    '[class*="name"]',
    '[class*="title"]',  # Sometimes name is in title class
    '[class*="person"]',
    '[itemprop="name"]',
    'strong',
    'b',
]

# Elements typically containing job titles
TITLE_ELEMENT_SELECTORS = [
    '[class*="position"]',
    '[class*="title"]',
    '[class*="role"]',
    '[class*="job"]',
    '[itemprop="jobTitle"]',
    '.coach-title',
    '.staff-title',
    '.position',
    'em',
    'span[class*="title"]',
]

# Email detection patterns
EMAIL_PATTERN = re.compile(
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    re.IGNORECASE
)

# Mailto link pattern
MAILTO_PATTERN = re.compile(
    r'mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
    re.IGNORECASE
)

# Phone pattern (US format)
PHONE_PATTERN = re.compile(
    r'(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}'
)

# Twitter/X URL pattern
TWITTER_PATTERN = re.compile(
    r'(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/([A-Za-z0-9_]+)',
    re.IGNORECASE
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def compute_html_hash(html: str) -> str:
    """Compute a hash of HTML content for caching/validation."""
    return hashlib.sha256(html.encode('utf-8')).hexdigest()[:16]


def clean_soup(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Remove non-content elements from soup.
    
    Removes:
    - Script and style tags
    - Comments
    - Navigation and footer elements
    - Hidden elements
    """
    # Remove script, style, and other non-content
    for tag in soup.find_all(['script', 'style', 'noscript', 'iframe']):
        tag.decompose()
    
    # Remove comments
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()
    
    # Remove navigation and footer (often contain noise)
    for tag in soup.find_all(['nav', 'footer', 'aside']):
        # Only remove if they don't contain staff info
        if not any(kw in str(tag).lower() for kw in ['coach', 'staff', 'directory']):
            tag.decompose()
    
    # Remove hidden elements
    for tag in soup.find_all(style=re.compile(r'display\s*:\s*none', re.I)):
        tag.decompose()
    for tag in soup.find_all(attrs={'hidden': True}):
        tag.decompose()
    for tag in soup.find_all(attrs={'aria-hidden': 'true'}):
        # Only remove if not containing staff info
        if not any(kw in str(tag).lower() for kw in ['coach', 'staff']):
            tag.decompose()
    
    return soup


def extract_text_content(element: Tag) -> str:
    """
    Extract clean text content from an element.
    
    Handles nested elements and normalizes whitespace.
    """
    if element is None:
        return ""
    
    # Get text with separator to preserve structure
    text = element.get_text(separator=' ', strip=True)
    
    # Normalize whitespace
    text = normalize_whitespace(text)
    
    return text


def extract_emails_from_element(element: Tag) -> List[str]:
    """
    Extract all email addresses from an element.
    
    Checks:
    - Text content
    - Mailto links
    - Data attributes
    """
    emails: Set[str] = set()
    
    if element is None:
        return []
    
    # 1. Check text content
    text = element.get_text()
    for match in EMAIL_PATTERN.finditer(text):
        email = match.group(0).lower()
        if is_valid_email(email):
            emails.add(email)
    
    # 2. Check mailto links
    for link in element.find_all('a', href=True):
        href = link.get('href', '')
        mailto_match = MAILTO_PATTERN.search(href)
        if mailto_match:
            email = mailto_match.group(1).lower()
            if is_valid_email(email):
                emails.add(email)
    
    # 3. Check data attributes
    for elem in element.find_all(True):
        for attr, value in elem.attrs.items():
            if isinstance(value, str) and 'email' in attr.lower():
                if '@' in value:
                    email = value.lower().strip()
                    if is_valid_email(email):
                        emails.add(email)
    
    return list(emails)


def extract_phone_from_element(element: Tag) -> Optional[str]:
    """Extract phone number from element."""
    if element is None:
        return None
    
    text = element.get_text()
    match = PHONE_PATTERN.search(text)
    if match:
        # Normalize to digits only
        digits = re.sub(r'[^\d]', '', match.group(0))
        if len(digits) >= 10:
            return digits
    
    return None


def extract_twitter_from_element(element: Tag) -> Optional[str]:
    """Extract Twitter/X handle from element."""
    if element is None:
        return None
    
    # Check links
    for link in element.find_all('a', href=True):
        href = link.get('href', '')
        match = TWITTER_PATTERN.search(href)
        if match:
            handle = match.group(1)
            return f"https://x.com/{handle}"
    
    # Check text
    text = element.get_text()
    match = TWITTER_PATTERN.search(text)
    if match:
        handle = match.group(1)
        return f"https://x.com/{handle}"
    
    return None


def is_valid_email(email: str) -> bool:
    """
    Validate an email address.
    
    Rejects:
    - Obviously fake domains
    - Generic addresses (info@, admin@, etc.)
    """
    if not email or '@' not in email:
        return False
    
    email_lower = email.lower()
    
    # Check for fake domains
    fake_domains = ['example.com', 'test.com', 'domain.com', 'email.com', 'sample.com']
    for fake in fake_domains:
        if fake in email_lower:
            return False
    
    # Check for generic prefixes
    generic_prefixes = [
        'info@', 'admin@', 'webmaster@', 'support@', 'contact@',
        'general@', 'help@', 'media@', 'tickets@', 'sales@',
        'marketing@', 'admissions@', 'athletics@', 'sports@',
        'noreply@', 'no-reply@', 'donotreply@'
    ]
    for prefix in generic_prefixes:
        if email_lower.startswith(prefix):
            return False
    
    return True


def get_element_depth(element: Tag) -> int:
    """Get the depth of an element in the DOM tree."""
    depth = 0
    parent = element.parent
    while parent:
        depth += 1
        parent = parent.parent
    return depth


def elements_are_siblings(elem1: Tag, elem2: Tag) -> bool:
    """Check if two elements are siblings (same parent)."""
    if elem1 is None or elem2 is None:
        return False
    return elem1.parent == elem2.parent


def find_common_ancestor(elem1: Tag, elem2: Tag) -> Optional[Tag]:
    """Find the closest common ancestor of two elements."""
    if elem1 is None or elem2 is None:
        return None
    
    ancestors1 = set()
    parent = elem1
    while parent:
        ancestors1.add(parent)
        parent = parent.parent
    
    parent = elem2
    while parent:
        if parent in ancestors1:
            return parent
        parent = parent.parent
    
    return None


def dom_distance(elem1: Tag, elem2: Tag) -> int:
    """
    Calculate the DOM distance between two elements.
    
    Distance is the sum of steps to reach the common ancestor
    from each element.
    """
    ancestor = find_common_ancestor(elem1, elem2)
    if ancestor is None:
        return float('inf')
    
    dist1 = 0
    parent = elem1
    while parent != ancestor:
        dist1 += 1
        parent = parent.parent
    
    dist2 = 0
    parent = elem2
    while parent != ancestor:
        dist2 += 1
        parent = parent.parent
    
    return dist1 + dist2


# ============================================================================
# STRATEGY 1: STRUCTURED DATA EXTRACTION
# ============================================================================

class StructuredDataExtractor:
    """
    Extract staff from structured data (JSON-LD, Microdata).
    
    This is the most reliable extraction method when available,
    as structured data follows defined schemas.
    """
    
    def extract(self, soup: BeautifulSoup, url: str) -> List[StaffMember]:
        """Extract staff from structured data."""
        staff: List[StaffMember] = []
        
        # Try JSON-LD
        staff.extend(self._extract_json_ld(soup, url))
        
        # Try Microdata
        staff.extend(self._extract_microdata(soup, url))
        
        return staff
    
    def _extract_json_ld(self, soup: BeautifulSoup, url: str) -> List[StaffMember]:
        """Extract from JSON-LD script tags."""
        staff: List[StaffMember] = []
        
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or '')
                staff.extend(self._process_json_ld(data, url))
            except (json.JSONDecodeError, TypeError):
                continue
        
        return staff
    
    def _process_json_ld(self, data: Any, url: str) -> List[StaffMember]:
        """Process JSON-LD data recursively."""
        staff: List[StaffMember] = []
        
        if isinstance(data, list):
            for item in data:
                staff.extend(self._process_json_ld(item, url))
        
        elif isinstance(data, dict):
            # Check if this is a Person
            obj_type = data.get('@type', '')
            if obj_type == 'Person' or 'Person' in str(obj_type):
                member = self._parse_person_object(data, url)
                if member:
                    staff.append(member)
            
            # Check nested objects
            for value in data.values():
                if isinstance(value, (dict, list)):
                    staff.extend(self._process_json_ld(value, url))
        
        return staff
    
    def _parse_person_object(self, data: Dict, url: str) -> Optional[StaffMember]:
        """Parse a schema.org Person object."""
        name = data.get('name', '')
        if not name:
            return None
        
        # Validate name
        is_name, name_conf, _ = is_valid_name(name)
        if not is_name:
            return None
        
        title = data.get('jobTitle', '')
        email = data.get('email', '')
        phone = data.get('telephone', '')
        
        # Clean email
        if email and email.startswith('mailto:'):
            email = email[7:]
        
        member = StaffMember(
            name=normalize_name(name),
            raw_title=title,
            normalized_title=normalize_title(title),
            contact=ContactInfo(
                email=email if is_valid_email(email) else None,
                phone=phone,
            ),
            extraction_method=ExtractionStrategy.STRUCTURED_DATA,
            extraction_confidence=95,  # High confidence for structured data
            source_url=url,
        )
        
        # Classify roles
        if title:
            member.roles = classify_role(title)
        
        return member
    
    def _extract_microdata(self, soup: BeautifulSoup, url: str) -> List[StaffMember]:
        """Extract from HTML Microdata."""
        staff: List[StaffMember] = []
        
        # Find Person items
        for person in soup.find_all(itemtype=re.compile(r'schema\.org/Person', re.I)):
            member = self._parse_microdata_person(person, url)
            if member:
                staff.append(member)
        
        return staff
    
    def _parse_microdata_person(self, element: Tag, url: str) -> Optional[StaffMember]:
        """Parse a microdata Person element."""
        name_elem = element.find(itemprop='name')
        name = name_elem.get_text(strip=True) if name_elem else ''
        
        if not name:
            return None
        
        is_name, _, _ = is_valid_name(name)
        if not is_name:
            return None
        
        title_elem = element.find(itemprop='jobTitle')
        title = title_elem.get_text(strip=True) if title_elem else ''
        
        email_elem = element.find(itemprop='email')
        email = ''
        if email_elem:
            email = email_elem.get('content', email_elem.get_text(strip=True))
        
        member = StaffMember(
            name=normalize_name(name),
            raw_title=title,
            normalized_title=normalize_title(title),
            contact=ContactInfo(
                email=email if is_valid_email(email) else None,
            ),
            extraction_method=ExtractionStrategy.STRUCTURED_DATA,
            extraction_confidence=90,
            source_url=url,
        )
        
        if title:
            member.roles = classify_role(title)
        
        return member


# ============================================================================
# STRATEGY 2: STAFF CARD EXTRACTION
# ============================================================================

class StaffCardExtractor:
    """
    Extract staff from card-based layouts.
    
    Most modern athletic websites use card layouts for staff directories.
    This extractor finds card containers and extracts name, title, and
    contact info from each.
    """
    
    def extract(self, soup: BeautifulSoup, url: str) -> List[StaffMember]:
        """Extract staff from card layouts."""
        staff: List[StaffMember] = []
        processed_elements: Set[int] = set()  # Track by element id
        
        for selector in STAFF_CARD_SELECTORS:
            try:
                cards = soup.select(selector)[:MAX_CARDS_TO_PROCESS]
                
                for card in cards:
                    # Skip if already processed
                    card_id = id(card)
                    if card_id in processed_elements:
                        continue
                    processed_elements.add(card_id)
                    
                    member = self._extract_from_card(card, url)
                    if member:
                        staff.append(member)
                        
            except Exception as e:
                logger.debug(f"Selector '{selector}' failed: {e}")
                continue
        
        return staff
    
    def _extract_from_card(self, card: Tag, url: str) -> Optional[StaffMember]:
        """Extract staff member from a single card element."""
        # Try to find name
        name, name_confidence = self._find_name_in_card(card)
        if not name or name_confidence < MIN_NAME_CONFIDENCE:
            return None
        
        # Find title
        title = self._find_title_in_card(card)
        
        # Find contact info
        emails = extract_emails_from_element(card)
        phone = extract_phone_from_element(card)
        twitter = extract_twitter_from_element(card)
        
        member = StaffMember(
            name=normalize_name(name),
            raw_title=title,
            normalized_title=normalize_title(title),
            contact=ContactInfo(
                email=emails[0] if emails else None,
                phone=phone,
                twitter=twitter,
            ),
            extraction_method=ExtractionStrategy.STAFF_CARDS,
            extraction_confidence=name_confidence,
            dom_context=str(card)[:500],  # Save context for debugging
            source_url=url,
        )
        
        # Classify roles
        if title:
            member.roles = classify_role(title)
        
        return member
    
    def _find_name_in_card(self, card: Tag) -> Tuple[str, int]:
        """
        Find the name within a card element.
        
        Returns (name, confidence).
        """
        candidates: List[Tuple[str, int, str]] = []  # (name, confidence, source)
        
        # Strategy 1: Check specific name elements
        for selector in NAME_ELEMENT_SELECTORS:
            for elem in card.select(selector)[:10]:
                text = extract_text_content(elem)
                is_name, conf, _ = is_valid_name(text)
                if is_name and conf >= MIN_NAME_CONFIDENCE:
                    candidates.append((text, conf + 10, f"selector:{selector}"))
        
        # Strategy 2: Check headings (names often in headings)
        for heading in card.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])[:5]:
            text = extract_text_content(heading)
            is_name, conf, _ = is_valid_name(text)
            if is_name:
                candidates.append((text, conf + 15, "heading"))
        
        # Strategy 3: Check first significant text block
        all_text = []
        for child in card.children:
            if isinstance(child, Tag):
                text = extract_text_content(child)
                if text and len(text) > 2:
                    all_text.append(text)
        
        if all_text:
            # First text is often the name
            first_text = all_text[0]
            is_name, conf, _ = is_valid_name(first_text)
            if is_name:
                candidates.append((first_text, conf, "first_text"))
        
        # Select best candidate
        if not candidates:
            return "", 0
        
        candidates.sort(key=lambda x: x[1], reverse=True)
        best_name, best_conf, _ = candidates[0]
        
        return best_name, best_conf
    
    def _find_title_in_card(self, card: Tag) -> str:
        """Find the job title within a card element."""
        candidates: List[Tuple[str, int]] = []  # (title, score)
        
        # Strategy 1: Check specific title elements
        for selector in TITLE_ELEMENT_SELECTORS:
            for elem in card.select(selector)[:10]:
                text = extract_text_content(elem)
                if text and len(text) > 3:
                    # Score based on whether it looks like a title
                    score = 50
                    if any(kw in text.lower() for kw in ['coach', 'coordinator', 'director']):
                        score += 30
                    if any(kw in text.lower() for kw in ['offensive', 'defensive', 'recruiting']):
                        score += 20
                    candidates.append((text, score))
        
        # Strategy 2: Find text after name that looks like a title
        card_text = card.get_text(separator='\n')
        lines = [l.strip() for l in card_text.split('\n') if l.strip()]
        
        for line in lines:
            # Skip if it looks like a name
            is_name, _, _ = is_valid_name(line)
            if is_name:
                continue
            
            # Check if it looks like a title
            if any(kw in line.lower() for kw in ['coach', 'coordinator', 'director', 'assistant']):
                candidates.append((line, 70))
        
        # Select best candidate
        if not candidates:
            return ""
        
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]


# ============================================================================
# STRATEGY 3: DOM PROXIMITY CORRELATION
# ============================================================================

class DOMProximityExtractor:
    """
    Extract staff by correlating nearby DOM elements.
    
    This strategy finds elements containing names and titles,
    then correlates them based on DOM tree proximity.
    """
    
    def extract(self, soup: BeautifulSoup, url: str) -> List[StaffMember]:
        """Extract staff using DOM proximity correlation."""
        staff: List[StaffMember] = []
        
        # Find all potential name elements
        name_elements = self._find_name_elements(soup)
        
        # Find all potential title elements
        title_elements = self._find_title_elements(soup)
        
        # Correlate names with titles
        used_titles: Set[int] = set()
        
        for name_elem, name_text, name_conf in name_elements:
            # Find closest title element
            best_title = None
            best_distance = float('inf')
            best_title_idx = -1
            
            for idx, (title_elem, title_text) in enumerate(title_elements):
                if idx in used_titles:
                    continue
                
                distance = dom_distance(name_elem, title_elem)
                if distance < best_distance:
                    best_distance = distance
                    best_title = title_text
                    best_title_idx = idx
            
            # Only accept if distance is reasonable (within same card/section)
            if best_distance > 6:
                best_title = None
            
            if best_title_idx >= 0 and best_distance <= 6:
                used_titles.add(best_title_idx)
            
            # Find email near name
            parent = name_elem.parent
            search_area = parent if parent else name_elem
            emails = extract_emails_from_element(search_area)
            
            member = StaffMember(
                name=normalize_name(name_text),
                raw_title=best_title or "",
                normalized_title=normalize_title(best_title or ""),
                contact=ContactInfo(
                    email=emails[0] if emails else None,
                ),
                extraction_method=ExtractionStrategy.DOM_PROXIMITY,
                extraction_confidence=name_conf - 10,  # Slight penalty for this method
                source_url=url,
            )
            
            if best_title:
                member.roles = classify_role(best_title)
            
            staff.append(member)
        
        return staff
    
    def _find_name_elements(self, soup: BeautifulSoup) -> List[Tuple[Tag, str, int]]:
        """Find all elements that appear to contain names."""
        results: List[Tuple[Tag, str, int]] = []
        seen_names: Set[str] = set()
        
        # Check headings
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            text = extract_text_content(heading)
            if text and text not in seen_names:
                is_name, conf, _ = is_valid_name(text)
                if is_name and conf >= MIN_NAME_CONFIDENCE:
                    seen_names.add(text)
                    results.append((heading, text, conf))
        
        # Check elements with name-related classes
        for selector in ['[class*="name"]', '[itemprop="name"]']:
            for elem in soup.select(selector):
                text = extract_text_content(elem)
                if text and text not in seen_names:
                    is_name, conf, _ = is_valid_name(text)
                    if is_name and conf >= MIN_NAME_CONFIDENCE:
                        seen_names.add(text)
                        results.append((elem, text, conf))
        
        return results
    
    def _find_title_elements(self, soup: BeautifulSoup) -> List[Tuple[Tag, str]]:
        """Find all elements that appear to contain job titles."""
        results: List[Tuple[Tag, str]] = []
        seen_titles: Set[str] = set()
        
        for selector in TITLE_ELEMENT_SELECTORS:
            for elem in soup.select(selector):
                text = extract_text_content(elem)
                if text and text not in seen_titles:
                    # Check if it looks like a title
                    if len(text) > 3 and len(text) < 200:
                        if any(kw in text.lower() for kw in ['coach', 'coordinator', 'director', 'assistant']):
                            seen_titles.add(text)
                            results.append((elem, text))
        
        return results


# ============================================================================
# STRATEGY 4: TABLE PARSING
# ============================================================================

class TableExtractor:
    """
    Extract staff from HTML tables.
    
    Some athletic websites use tables for staff directories.
    This extractor handles both header-based and headerless tables.
    """
    
    def extract(self, soup: BeautifulSoup, url: str) -> List[StaffMember]:
        """Extract staff from tables."""
        staff: List[StaffMember] = []
        
        for table in soup.find_all('table')[:MAX_TABLES_TO_PROCESS]:
            # Skip navigation/layout tables
            if self._is_layout_table(table):
                continue
            
            # Try to extract from this table
            table_staff = self._extract_from_table(table, url)
            staff.extend(table_staff)
        
        return staff
    
    def _is_layout_table(self, table: Tag) -> bool:
        """Check if table is likely used for layout, not data."""
        # Check for layout-related classes
        classes = ' '.join(table.get('class', []))
        if any(kw in classes.lower() for kw in ['layout', 'nav', 'menu', 'footer']):
            return True
        
        # Check if it has very few rows (likely layout)
        rows = table.find_all('tr')
        if len(rows) < 2:
            return True
        
        return False
    
    def _extract_from_table(self, table: Tag, url: str) -> List[StaffMember]:
        """Extract staff from a single table."""
        staff: List[StaffMember] = []
        
        rows = table.find_all('tr')
        if not rows:
            return []
        
        # Try to detect headers
        headers = self._detect_headers(rows[0])
        data_rows = rows[1:] if headers else rows
        
        for row in data_rows:
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue
            
            member = self._extract_from_row(cells, headers, url)
            if member:
                staff.append(member)
        
        return staff
    
    def _detect_headers(self, row: Tag) -> Optional[List[str]]:
        """Detect column headers from first row."""
        cells = row.find_all(['th', 'td'])
        if not cells:
            return None
        
        headers = [extract_text_content(cell).lower() for cell in cells]
        
        # Check if this looks like a header row
        header_keywords = ['name', 'title', 'position', 'email', 'phone', 'role']
        if any(any(kw in h for kw in header_keywords) for h in headers):
            return headers
        
        return None
    
    def _extract_from_row(
        self, 
        cells: List[Tag], 
        headers: Optional[List[str]], 
        url: str
    ) -> Optional[StaffMember]:
        """Extract staff member from a table row."""
        name = ""
        title = ""
        email = ""
        
        if headers:
            # Use headers to identify columns
            for idx, cell in enumerate(cells):
                if idx >= len(headers):
                    break
                
                header = headers[idx]
                text = extract_text_content(cell)
                
                if 'name' in header:
                    name = text
                elif 'title' in header or 'position' in header or 'role' in header:
                    title = text
                elif 'email' in header:
                    emails = extract_emails_from_element(cell)
                    email = emails[0] if emails else ""
        else:
            # No headers - try to infer from content
            for cell in cells:
                text = extract_text_content(cell)
                
                # Check if it's a name
                if not name:
                    is_name, conf, _ = is_valid_name(text)
                    if is_name and conf >= MIN_NAME_CONFIDENCE:
                        name = text
                        continue
                
                # Check if it's a title
                if not title and any(kw in text.lower() for kw in ['coach', 'coordinator', 'director']):
                    title = text
                    continue
                
                # Check for email
                if not email:
                    emails = extract_emails_from_element(cell)
                    if emails:
                        email = emails[0]
        
        if not name:
            return None
        
        member = StaffMember(
            name=normalize_name(name),
            raw_title=title,
            normalized_title=normalize_title(title),
            contact=ContactInfo(email=email if is_valid_email(email) else None),
            extraction_method=ExtractionStrategy.TABLE_PARSING,
            extraction_confidence=70,
            source_url=url,
        )
        
        if title:
            member.roles = classify_role(title)
        
        return member


# ============================================================================
# STRATEGY 5: TEXT PATTERN EXTRACTION
# ============================================================================

class TextPatternExtractor:
    """
    Extract staff using text pattern analysis.
    
    This is a fallback strategy that analyzes raw text
    line by line to find name-title pairs.
    """
    
    def extract(self, soup: BeautifulSoup, url: str) -> List[StaffMember]:
        """Extract staff using text patterns."""
        staff: List[StaffMember] = []
        
        # Get all text lines
        text = soup.get_text(separator='\n')
        lines = [l.strip() for l in text.split('\n') if l.strip()][:MAX_TEXT_LINES]
        
        # Find name-title pairs
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Check if this line is a name
            is_name, name_conf, _ = is_valid_name(line)
            if is_name and name_conf >= MIN_NAME_CONFIDENCE:
                # Look for title in next few lines
                title = ""
                for j in range(i + 1, min(i + 4, len(lines))):
                    next_line = lines[j]
                    if any(kw in next_line.lower() for kw in ['coach', 'coordinator', 'director', 'assistant']):
                        title = next_line
                        break
                
                # Also check if title is on same line (separated by dash or similar)
                if not title:
                    parts = re.split(r'[-–—|]', line)
                    if len(parts) > 1:
                        potential_name = parts[0].strip()
                        potential_title = parts[1].strip()
                        is_name, conf, _ = is_valid_name(potential_name)
                        if is_name and any(kw in potential_title.lower() for kw in ['coach', 'coordinator', 'director']):
                            line = potential_name
                            title = potential_title
                
                member = StaffMember(
                    name=normalize_name(line),
                    raw_title=title,
                    normalized_title=normalize_title(title),
                    extraction_method=ExtractionStrategy.TEXT_PATTERN,
                    extraction_confidence=name_conf - 15,  # Lower confidence for text-only
                    source_url=url,
                )
                
                if title:
                    member.roles = classify_role(title)
                
                staff.append(member)
            
            i += 1
        
        return staff


# ============================================================================
# MAIN DOM PARSER
# ============================================================================

class DOMParser:
    """
    Main DOM parsing orchestrator.
    
    Coordinates multiple extraction strategies and combines results.
    
    Usage:
        parser = DOMParser()
        result = parser.parse(html, url, school_name)
        
        print(f"OL Coach: {result.ol_coach.name if result.ol_coach else 'Not found'}")
        print(f"RC: {result.rc.name if result.rc else 'Not found'}")
    """
    
    def __init__(self):
        """Initialize the DOM parser with all extraction strategies."""
        self.structured_extractor = StructuredDataExtractor()
        self.card_extractor = StaffCardExtractor()
        self.proximity_extractor = DOMProximityExtractor()
        self.table_extractor = TableExtractor()
        self.text_extractor = TextPatternExtractor()
    
    def parse(
        self, 
        html: str, 
        url: str, 
        school_name: str = ""
    ) -> ExtractionResult:
        """
        Parse HTML and extract staff information.
        
        Args:
            html: HTML content to parse
            url: Source URL
            school_name: Name of the school (for result)
            
        Returns:
            ExtractionResult with all extracted data
        """
        start_time = datetime.now()
        result = ExtractionResult(url=url, school_name=school_name)
        result.html_hash = compute_html_hash(html)
        
        try:
            # Parse HTML
            soup = BeautifulSoup(html, 'html.parser')
            soup = clean_soup(soup)
            
            # Track all extracted staff and which strategies worked
            all_staff: List[StaffMember] = []
            
            # Strategy 1: Structured Data (most reliable)
            try:
                structured_staff = self.structured_extractor.extract(soup, url)
                if structured_staff:
                    all_staff.extend(structured_staff)
                    result.strategies_used.append((ExtractionStrategy.STRUCTURED_DATA, len(structured_staff)))
            except Exception as e:
                result.strategies_failed.append((ExtractionStrategy.STRUCTURED_DATA, str(e)))
            
            # Strategy 2: Staff Cards
            try:
                card_staff = self.card_extractor.extract(soup, url)
                if card_staff:
                    all_staff.extend(card_staff)
                    result.strategies_used.append((ExtractionStrategy.STAFF_CARDS, len(card_staff)))
            except Exception as e:
                result.strategies_failed.append((ExtractionStrategy.STAFF_CARDS, str(e)))
            
            # Strategy 3: DOM Proximity (if cards didn't find much)
            if len(all_staff) < 5:
                try:
                    proximity_staff = self.proximity_extractor.extract(soup, url)
                    if proximity_staff:
                        all_staff.extend(proximity_staff)
                        result.strategies_used.append((ExtractionStrategy.DOM_PROXIMITY, len(proximity_staff)))
                except Exception as e:
                    result.strategies_failed.append((ExtractionStrategy.DOM_PROXIMITY, str(e)))
            
            # Strategy 4: Tables
            try:
                table_staff = self.table_extractor.extract(soup, url)
                if table_staff:
                    all_staff.extend(table_staff)
                    result.strategies_used.append((ExtractionStrategy.TABLE_PARSING, len(table_staff)))
            except Exception as e:
                result.strategies_failed.append((ExtractionStrategy.TABLE_PARSING, str(e)))
            
            # Strategy 5: Text Patterns (fallback)
            if len(all_staff) < 3:
                try:
                    text_staff = self.text_extractor.extract(soup, url)
                    if text_staff:
                        all_staff.extend(text_staff)
                        result.strategies_used.append((ExtractionStrategy.TEXT_PATTERN, len(text_staff)))
                except Exception as e:
                    result.strategies_failed.append((ExtractionStrategy.TEXT_PATTERN, str(e)))
            
            # Deduplicate staff by name
            result.staff = self._deduplicate_staff(all_staff)
            
            # Collect all raw titles for debugging
            for member in result.staff:
                if member.raw_title:
                    result.raw_titles_found.append(member.raw_title)
            
            # Find best OL coach
            ol_candidates = [
                (m, m.get_role_confidence(CanonicalRole.OFFENSIVE_LINE_COACH))
                for m in result.staff
                if m.get_role_confidence(CanonicalRole.OFFENSIVE_LINE_COACH) > 0
            ]
            if ol_candidates:
                ol_candidates.sort(key=lambda x: x[1], reverse=True)
                result.ol_coach = ol_candidates[0][0]
                result.ol_confidence = ol_candidates[0][1]
            
            # Find best RC
            rc_candidates = [
                (m, m.get_role_confidence(CanonicalRole.RECRUITING_COORDINATOR))
                for m in result.staff
                if m.get_role_confidence(CanonicalRole.RECRUITING_COORDINATOR) > 0
            ]
            if rc_candidates:
                rc_candidates.sort(key=lambda x: x[1], reverse=True)
                result.rc = rc_candidates[0][0]
                result.rc_confidence = rc_candidates[0][1]
            
            # Determine review status
            result.determine_review_status()
            
        except Exception as e:
            result.errors.append(f"Parse error: {str(e)}")
            logger.error(f"DOM parse error for {url}: {e}")
        
        # Calculate processing time
        end_time = datetime.now()
        result.processing_time_ms = int((end_time - start_time).total_seconds() * 1000)
        
        return result
    
    def _deduplicate_staff(self, staff: List[StaffMember]) -> List[StaffMember]:
        """
        Deduplicate staff members by name.
        
        When duplicates are found, keep the one with highest confidence
        or most complete information.
        """
        by_name: Dict[str, StaffMember] = {}
        
        for member in staff:
            name_key = member.name.lower().strip()
            if not name_key:
                continue
            
            existing = by_name.get(name_key)
            if existing is None:
                by_name[name_key] = member
            else:
                # Keep the better one
                if self._is_better_member(member, existing):
                    by_name[name_key] = member
        
        return list(by_name.values())
    
    def _is_better_member(self, new: StaffMember, existing: StaffMember) -> bool:
        """Determine if new member data is better than existing."""
        # Prefer higher extraction confidence
        if new.extraction_confidence > existing.extraction_confidence + 10:
            return True
        
        # Prefer more complete data
        new_completeness = sum([
            bool(new.raw_title),
            bool(new.contact.email),
            bool(new.contact.phone),
            bool(new.roles),
        ])
        existing_completeness = sum([
            bool(existing.raw_title),
            bool(existing.contact.email),
            bool(existing.contact.phone),
            bool(existing.roles),
        ])
        
        if new_completeness > existing_completeness:
            return True
        
        # Prefer more specific extraction method
        method_priority = {
            ExtractionStrategy.STRUCTURED_DATA: 5,
            ExtractionStrategy.STAFF_CARDS: 4,
            ExtractionStrategy.TABLE_PARSING: 3,
            ExtractionStrategy.DOM_PROXIMITY: 2,
            ExtractionStrategy.TEXT_PATTERN: 1,
            ExtractionStrategy.FALLBACK_SCAN: 0,
        }
        
        new_priority = method_priority.get(new.extraction_method, 0)
        existing_priority = method_priority.get(existing.extraction_method, 0)
        
        return new_priority > existing_priority


# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    'DOMParser',
    'ExtractionResult',
    'StaffMember',
    'StructuredDataExtractor',
    'StaffCardExtractor',
    'DOMProximityExtractor',
    'TableExtractor',
    'TextPatternExtractor',
]


# ============================================================================
# SELF-TEST
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("DOM PARSER SELF-TEST")
    print("=" * 70)
    
    # Test HTML
    test_html = """
    <!DOCTYPE html>
    <html>
    <head><title>Football Staff</title></head>
    <body>
        <div class="staff-container">
            <div class="coach-card">
                <h3>Adrian Brunori</h3>
                <p class="position">Run Game Coordinator / Offensive Line Coach</p>
                <a href="mailto:abrunori@newhaven.edu">Email</a>
            </div>
            <div class="coach-card">
                <h3>Nick Amendola</h3>
                <p class="position">Director of Football Operations</p>
                <a href="mailto:namendola@newhaven.edu">Email</a>
            </div>
            <div class="coach-card">
                <h3>Tim Zetts</h3>
                <p class="position">Offensive Coordinator / Quarterbacks Coach</p>
                <a href="mailto:tzetts@newhaven.edu">Email</a>
            </div>
            <div class="coach-card">
                <h3>Joe Vitale</h3>
                <p class="position">Director of Player Development</p>
                <a href="mailto:jvitale@newhaven.edu">Email</a>
            </div>
        </div>
    </body>
    </html>
    """
    
    parser = DOMParser()
    result = parser.parse(test_html, "https://test.edu/staff", "Test University")
    
    print(f"\n📊 Extraction Results")
    print(f"{'='*50}")
    print(f"Staff found: {len(result.staff)}")
    print(f"Strategies used: {[str(s) for s, c in result.strategies_used]}")
    print(f"Processing time: {result.processing_time_ms}ms")
    
    print(f"\n👥 All Staff:")
    for member in result.staff:
        print(f"\n  Name: {member.name}")
        print(f"  Title: {member.raw_title}")
        print(f"  Email: {member.contact.email}")
        print(f"  Roles: {[r.role.value for r in member.roles]}")
    
    print(f"\n🎯 Target Coaches:")
    if result.ol_coach:
        print(f"  OL Coach: {result.ol_coach.name} ({result.ol_confidence}%)")
        print(f"    Title: {result.ol_coach.raw_title}")
    else:
        print(f"  OL Coach: NOT FOUND")
    
    if result.rc:
        print(f"  RC: {result.rc.name} ({result.rc_confidence}%)")
        print(f"    Title: {result.rc.raw_title}")
    else:
        print(f"  RC: NOT FOUND")
    
    print(f"\n📋 Review Status:")
    print(f"  Needs Review: {result.needs_review}")
    if result.review_reasons:
        for reason in result.review_reasons:
            print(f"    - {reason}")
    
    print("\n" + "=" * 70)
    print("SELF-TEST COMPLETE")
    print("=" * 70)
