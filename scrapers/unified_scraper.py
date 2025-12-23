"""
scrapers/unified_scraper.py - Unified Coach + Email Scraper
============================================================================
Improved scraper that extracts names AND emails in one pass.
Handles various formats including:
- Standard staff cards
- Plain text lists (like Holmes CC)
- Names as links
- Tabular layouts
- Mixed formats

Author: Coach Outreach System
Version: 3.3.0
============================================================================
"""

import re
import time
import logging
from typing import List, Dict, Optional, Tuple, Set, Any
from dataclasses import dataclass, field
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


# ============================================================================
# PATTERNS
# ============================================================================

EMAIL_PATTERN = re.compile(
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    re.IGNORECASE
)

PHONE_PATTERN = re.compile(
    r'(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}'
)

TWITTER_PATTERN = re.compile(
    r'(?:twitter\.com|x\.com)/([A-Za-z0-9_]+)',
    re.IGNORECASE
)

# OL Coach patterns
OL_PATTERNS = [
    r'offensive\s*line',
    r'\bol\b.*coach',
    r'o[\-\.]?line',
    r'oline',
    r'o\.l\.',
]

# Recruiting Coordinator patterns
RC_PATTERNS = [
    r'recruit(?:ing)?\s*coordinator',
    r'director\s+of\s+recruit',
    r'recruiting\s+director',
    r'\brc\b',
]

# Name validation
def is_valid_name(text: str) -> Tuple[bool, int]:
    """Check if text looks like a person's name. Returns (is_name, confidence)."""
    if not text or len(text) < 3 or len(text) > 50:
        return False, 0
    
    # Must have at least 2 words for full name
    words = text.split()
    if len(words) < 2:
        return False, 0
    
    # Check for obvious non-names
    lower = text.lower()
    skip_words = [
        'football', 'coach', 'staff', 'athletics', 'university', 'college',
        'contact', 'email', 'phone', 'office', 'coordinator', 'director',
        'head', 'assistant', 'offensive', 'defensive', 'special', 'teams',
        'click', 'here', 'view', 'bio', 'read', 'more', 'full',
        'http', 'www', '.com', '.edu', '@',
    ]
    
    if any(skip in lower for skip in skip_words):
        return False, 0
    
    # All words should be capitalized (names are proper nouns)
    capitalized = sum(1 for w in words if w[0].isupper())
    if capitalized < len(words) * 0.5:
        return False, 0
    
    # Check for typical name patterns
    confidence = 50
    
    # 2-3 word names are most common
    if 2 <= len(words) <= 3:
        confidence += 20
    
    # First word starts with capital
    if words[0][0].isupper():
        confidence += 10
    
    # No numbers in name
    if not any(c.isdigit() for c in text):
        confidence += 10
    
    # Reasonable word lengths
    if all(2 <= len(w) <= 15 for w in words):
        confidence += 10
    
    return True, min(confidence, 95)


def is_ol_coach(title: str) -> bool:
    """Check if title indicates offensive line coach."""
    lower = title.lower()
    for pattern in OL_PATTERNS:
        if re.search(pattern, lower):
            return True
    return False


def is_recruiting_coordinator(title: str) -> bool:
    """Check if title indicates recruiting coordinator."""
    lower = title.lower()
    for pattern in RC_PATTERNS:
        if re.search(pattern, lower):
            return True
    return False


# ============================================================================
# COACH RECORD
# ============================================================================

@dataclass
class CoachRecord:
    """Represents an extracted coach."""
    name: str = ""
    title: str = ""
    email: str = ""
    phone: str = ""
    twitter: str = ""
    is_ol: bool = False
    is_rc: bool = False
    confidence: int = 0
    source: str = ""  # extraction method


# ============================================================================
# UNIFIED EXTRACTOR
# ============================================================================

class UnifiedCoachExtractor:
    """
    Extracts coach names AND emails in one pass.
    
    Uses multiple strategies:
    1. Block-based extraction (groups of related elements)
    2. Text proximity (name near email)
    3. Link extraction (names as links)
    4. Card-based extraction
    5. Plain text parsing
    """
    
    def __init__(self):
        self.extracted: List[CoachRecord] = []
    
    def extract(self, html: str, url: str = "") -> List[CoachRecord]:
        """
        Extract all coaches from HTML.
        
        Returns list of CoachRecord with names, titles, and emails.
        """
        self.extracted = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script/style
        for tag in soup.find_all(['script', 'style', 'noscript']):
            tag.decompose()
        
        # Try multiple strategies
        self._extract_from_blocks(soup)
        self._extract_from_text_blocks(soup)
        self._extract_from_links(soup)
        self._extract_from_plain_text(soup)
        
        # Deduplicate by name
        seen_names = set()
        unique = []
        for coach in self.extracted:
            name_key = coach.name.lower().strip()
            if name_key and name_key not in seen_names:
                seen_names.add(name_key)
                unique.append(coach)
        
        # Classify roles
        for coach in unique:
            if coach.title:
                coach.is_ol = is_ol_coach(coach.title)
                coach.is_rc = is_recruiting_coordinator(coach.title)
        
        return unique
    
    def _extract_from_blocks(self, soup: BeautifulSoup):
        """
        Extract from block-level groupings.
        
        This handles formats where coaches are in divs/sections with
        name, title, and contact info grouped together.
        """
        # Find containers that might hold coach info
        containers = []
        
        # Look for coach-related class names
        for elem in soup.find_all(['div', 'article', 'section', 'li']):
            classes = ' '.join(elem.get('class', []))
            elem_id = elem.get('id', '')
            
            if any(kw in classes.lower() or kw in elem_id.lower() 
                   for kw in ['coach', 'staff', 'person', 'bio', 'member', 'card']):
                containers.append(elem)
        
        # Also look for elements containing coach keywords in text
        for elem in soup.find_all(['div', 'p', 'section']):
            text = elem.get_text().lower()
            if 'coach' in text and len(text) < 500:
                # This might be a coach block
                if elem not in containers:
                    containers.append(elem)
        
        for container in containers[:100]:  # Limit to prevent runaway
            coach = self._parse_block(container)
            if coach and coach.name:
                self.extracted.append(coach)
    
    def _parse_block(self, elem: Tag) -> Optional[CoachRecord]:
        """Parse a single block element for coach info."""
        text = elem.get_text(separator='\n')
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        
        if not lines:
            return None
        
        coach = CoachRecord(source='block')
        
        # Find email in this block
        emails = EMAIL_PATTERN.findall(text)
        if emails:
            coach.email = emails[0]
        else:
            # Check for mailto links
            for a in elem.find_all('a', href=True):
                href = a['href']
                if href.startswith('mailto:'):
                    email = href.replace('mailto:', '').split('?')[0]
                    if '@' in email:
                        coach.email = email
                        break
        
        # Find phone
        phones = PHONE_PATTERN.findall(text)
        if phones:
            coach.phone = phones[0]
        
        # Find Twitter
        for a in elem.find_all('a', href=True):
            match = TWITTER_PATTERN.search(a['href'])
            if match:
                coach.twitter = '@' + match.group(1)
                break
        
        # Find name - usually first reasonable name in block
        for line in lines:
            is_name, conf = is_valid_name(line)
            if is_name and conf >= 60:
                coach.name = line
                coach.confidence = conf
                break
        
        # Find title - look for line with coach/coordinator keywords
        for line in lines:
            lower = line.lower()
            if any(kw in lower for kw in ['coach', 'coordinator', 'director']):
                # Make sure it's not the name line
                if line != coach.name:
                    coach.title = line
                    break
        
        return coach if coach.name else None
    
    def _extract_from_text_blocks(self, soup: BeautifulSoup):
        """
        Extract from consecutive text elements.
        
        Handles formats like:
            John Smith
            Offensive Line Coach
            jsmith@school.edu
            555-123-4567
        """
        # Get all text-containing elements
        text_elements = []
        for elem in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div', 'span', 'strong', 'b', 'a']):
            text = elem.get_text(strip=True)
            if text and len(text) < 200:
                text_elements.append((elem, text))
        
        i = 0
        while i < len(text_elements):
            elem, text = text_elements[i]
            
            # Check if this looks like a name
            is_name, conf = is_valid_name(text)
            if is_name and conf >= 60:
                coach = CoachRecord(name=text, confidence=conf, source='text_block')
                
                # Look at next few elements for title, email, phone
                for j in range(i + 1, min(i + 6, len(text_elements))):
                    _, next_text = text_elements[j]
                    lower = next_text.lower()
                    
                    # Email?
                    emails = EMAIL_PATTERN.findall(next_text)
                    if emails and not coach.email:
                        coach.email = emails[0]
                        continue
                    
                    # Phone?
                    phones = PHONE_PATTERN.findall(next_text)
                    if phones and not coach.phone:
                        coach.phone = phones[0]
                        continue
                    
                    # Title?
                    if any(kw in lower for kw in ['coach', 'coordinator', 'director']) and not coach.title:
                        coach.title = next_text
                        continue
                    
                    # Another name means new person
                    is_next_name, _ = is_valid_name(next_text)
                    if is_next_name:
                        break
                
                if coach.name:
                    self.extracted.append(coach)
            
            i += 1
    
    def _extract_from_links(self, soup: BeautifulSoup):
        """
        Extract from anchor tags where name is link text.
        
        Many sites have coach names as clickable links to bio pages.
        """
        for a in soup.find_all('a', href=True):
            text = a.get_text(strip=True)
            href = a['href']
            
            # Skip navigation/menu links
            if len(text) < 4 or len(text) > 50:
                continue
            
            # Check if link text is a name
            is_name, conf = is_valid_name(text)
            if not is_name or conf < 60:
                continue
            
            # Check if href suggests a bio/staff page
            href_lower = href.lower()
            if not any(kw in href_lower for kw in ['staff', 'coach', 'bio', 'roster', 'person']):
                # Still might be valid - check surrounding context
                parent = a.parent
                if parent:
                    parent_text = parent.get_text().lower()
                    if not any(kw in parent_text for kw in ['coach', 'staff', 'football']):
                        continue
            
            coach = CoachRecord(name=text, confidence=conf, source='link')
            
            # Check parent for more info
            if a.parent:
                parent_text = a.parent.get_text(separator='\n')
                
                # Email
                emails = EMAIL_PATTERN.findall(parent_text)
                if emails:
                    coach.email = emails[0]
                
                # Title
                for line in parent_text.split('\n'):
                    lower = line.lower()
                    if any(kw in lower for kw in ['coach', 'coordinator', 'director']):
                        if line.strip() != coach.name:
                            coach.title = line.strip()
                            break
            
            self.extracted.append(coach)
    
    def _extract_from_plain_text(self, soup: BeautifulSoup):
        """
        Last resort: scan all text for name patterns.
        """
        text = soup.get_text(separator='\n')
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        
        # Build list of (line_idx, email) for proximity matching
        email_lines = []
        for i, line in enumerate(lines):
            emails = EMAIL_PATTERN.findall(line)
            if emails:
                email_lines.append((i, emails[0]))
        
        # Find names and match with nearby emails
        for i, line in enumerate(lines):
            is_name, conf = is_valid_name(line)
            if not is_name or conf < 65:
                continue
            
            # Check if we already have this name
            if any(c.name.lower() == line.lower() for c in self.extracted):
                continue
            
            coach = CoachRecord(name=line, confidence=conf, source='plain_text')
            
            # Find closest email (within 5 lines)
            for email_idx, email in email_lines:
                if abs(email_idx - i) <= 5:
                    coach.email = email
                    break
            
            # Find title in nearby lines
            for j in range(max(0, i-2), min(len(lines), i+4)):
                if j == i:
                    continue
                nearby = lines[j].lower()
                if any(kw in nearby for kw in ['coach', 'coordinator', 'director']):
                    coach.title = lines[j]
                    break
            
            self.extracted.append(coach)
    
    def find_ol_coach(self) -> Optional[CoachRecord]:
        """Find the offensive line coach from extracted records."""
        for coach in self.extracted:
            if coach.is_ol:
                return coach
        return None
    
    def find_rc(self) -> Optional[CoachRecord]:
        """Find the recruiting coordinator from extracted records."""
        for coach in self.extracted:
            if coach.is_rc:
                return coach
        return None


# ============================================================================
# CONVENIENCE FUNCTION
# ============================================================================

def extract_coaches(html: str, url: str = "") -> Dict[str, Any]:
    """
    Extract coaches from HTML and return structured result.
    
    Returns dict with:
        - all_coaches: List of all found coaches
        - ol_coach: Offensive line coach (if found)
        - rc: Recruiting coordinator (if found)
    """
    extractor = UnifiedCoachExtractor()
    coaches = extractor.extract(html, url)
    
    return {
        'all_coaches': coaches,
        'ol_coach': extractor.find_ol_coach(),
        'rc': extractor.find_rc(),
        'count': len(coaches),
    }
