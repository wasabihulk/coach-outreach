"""
core/normalizer.py - Text Normalization Pipeline
============================================================================
Enterprise-grade text normalization for consistent data processing.

This module handles all text normalization including:
- Unicode normalization (NFC/NFKC)
- Whitespace handling
- Separator normalization
- Character encoding issues
- Case normalization

Design Principles:
- Idempotent operations (normalize(normalize(x)) == normalize(x))
- Preserves semantic meaning
- Handles all known edge cases
- Fully documented transformations

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

import re
import unicodedata
from typing import List, Tuple, Optional, Callable
from functools import lru_cache


# ============================================================================
# UNICODE NORMALIZATION
# ============================================================================

def normalize_unicode(text: str) -> str:
    """
    Apply Unicode normalization (NFKC form).
    
    NFKC normalization:
    - Decomposes characters by compatibility
    - Recomposes by canonical equivalence
    - Converts full-width characters to ASCII equivalents
    - Normalizes ligatures
    
    Examples:
        'ﬁ' -> 'fi'
        '①' -> '1'
        'Ａ' -> 'A' (full-width to ASCII)
    
    Args:
        text: Input string
        
    Returns:
        Unicode-normalized string
    """
    if not text:
        return ""
    return unicodedata.normalize('NFKC', text)


# ============================================================================
# SEPARATOR NORMALIZATION
# ============================================================================

# All known separator characters that should be normalized
SEPARATOR_CHARS = {
    # Slashes
    '/': '/',           # Forward slash (U+002F)
    '\\': '/',          # Backslash (U+005C)
    '⁄': '/',           # Fraction slash (U+2044)
    '∕': '/',           # Division slash (U+2215)
    
    # Dashes and hyphens
    '-': ' / ',         # Hyphen-minus (U+002D) - when used as separator
    '‐': ' / ',         # Hyphen (U+2010)
    '‑': ' / ',         # Non-breaking hyphen (U+2011)
    '‒': ' / ',         # Figure dash (U+2012)
    '–': ' / ',         # En dash (U+2013)
    '—': ' / ',         # Em dash (U+2014)
    '―': ' / ',         # Horizontal bar (U+2015)
    '−': ' / ',         # Minus sign (U+2212)
    
    # Vertical bars
    '|': ' / ',         # Vertical line (U+007C)
    '¦': ' / ',         # Broken bar (U+00A6)
    '│': ' / ',         # Box drawings light vertical (U+2502)
    '∣': ' / ',         # Divides (U+2223)
    
    # Bullets and dots
    '•': ' / ',         # Bullet (U+2022)
    '·': ' / ',         # Middle dot (U+00B7)
    '◦': ' / ',         # White bullet (U+25E6)
    '‣': ' / ',         # Triangular bullet (U+2023)
    '⁃': ' / ',         # Hyphen bullet (U+2043)
    
    # Other separators
    '→': ' / ',         # Rightwards arrow (U+2192)
    '⇒': ' / ',         # Rightwards double arrow (U+21D2)
    '»': ' / ',         # Right-pointing double angle (U+00BB)
    '›': ' / ',         # Single right-pointing angle (U+203A)
}

# Regex pattern for detecting separator contexts
SEPARATOR_CONTEXT_PATTERN = re.compile(
    r'(?<=[a-zA-Z])\s*[/\-–—|•]\s*(?=[a-zA-Z])'
)


def normalize_separators(text: str, preserve_hyphens_in_words: bool = True) -> str:
    """
    Normalize all separator characters to a canonical form.
    
    This function:
    1. Replaces all dash variants (en-dash, em-dash, etc.) with ' / '
    2. Normalizes multiple slashes to single slash
    3. Preserves hyphens within hyphenated words (optional)
    
    Args:
        text: Input text
        preserve_hyphens_in_words: If True, keeps hyphens in "Co-Offensive"
        
    Returns:
        Text with normalized separators
        
    Examples:
        "Run Game Coordinator – Offensive Line" -> "Run Game Coordinator / Offensive Line"
        "Co-Offensive Coordinator" -> "Co-Offensive Coordinator" (hyphen preserved)
    """
    if not text:
        return ""
    
    result = text
    
    # First pass: Replace all separator characters
    for char, replacement in SEPARATOR_CHARS.items():
        if char in result:
            # For hyphen, check if it's word-internal
            if char == '-' and preserve_hyphens_in_words:
                # Only replace hyphens that are surrounded by spaces or at boundaries
                result = re.sub(r'(?<!\w)-(?!\w)', replacement, result)
                result = re.sub(r'\s+-\s+', replacement, result)
            else:
                result = result.replace(char, replacement)
    
    # Second pass: Normalize multiple slashes/spaces
    result = re.sub(r'\s*/\s*', ' / ', result)  # Normalize slash spacing
    result = re.sub(r'(/\s*)+', ' / ', result)  # Multiple slashes to one
    result = re.sub(r'\s+', ' ', result)        # Multiple spaces to one
    
    return result.strip()


def split_multi_role_title(title: str) -> List[str]:
    """
    Split a multi-role title into individual role segments.
    
    This handles all known separator patterns and returns
    clean, individual role strings.
    
    Args:
        title: Full title string (possibly containing multiple roles)
        
    Returns:
        List of individual role strings
        
    Examples:
        "Run Game Coordinator / Offensive Line Coach" 
            -> ["Run Game Coordinator", "Offensive Line Coach"]
        "Offensive Coordinator – Quarterbacks"
            -> ["Offensive Coordinator", "Quarterbacks"]
        "OL / Recruiting Coordinator"
            -> ["OL", "Recruiting Coordinator"]
    """
    if not title:
        return []
    
    # Normalize separators first
    normalized = normalize_separators(title)
    
    # Split on the canonical separator
    segments = normalized.split(' / ')
    
    # Clean each segment
    cleaned = []
    for segment in segments:
        segment = segment.strip()
        if segment and len(segment) >= 2:
            cleaned.append(segment)
    
    return cleaned


# ============================================================================
# WHITESPACE NORMALIZATION
# ============================================================================

# All Unicode whitespace characters
WHITESPACE_CHARS = {
    ' ',        # Space (U+0020)
    '\t',       # Tab (U+0009)
    '\n',       # Newline (U+000A)
    '\r',       # Carriage return (U+000D)
    '\f',       # Form feed (U+000C)
    '\v',       # Vertical tab (U+000B)
    '\xa0',     # Non-breaking space (U+00A0)
    '\u1680',   # Ogham space mark
    '\u2000',   # En quad
    '\u2001',   # Em quad
    '\u2002',   # En space
    '\u2003',   # Em space
    '\u2004',   # Three-per-em space
    '\u2005',   # Four-per-em space
    '\u2006',   # Six-per-em space
    '\u2007',   # Figure space
    '\u2008',   # Punctuation space
    '\u2009',   # Thin space
    '\u200a',   # Hair space
    '\u200b',   # Zero-width space
    '\u202f',   # Narrow no-break space
    '\u205f',   # Medium mathematical space
    '\u3000',   # Ideographic space
    '\ufeff',   # Zero-width no-break space (BOM)
}


def normalize_whitespace(text: str) -> str:
    """
    Normalize all whitespace characters to standard spaces.
    
    This function:
    1. Converts all Unicode whitespace to standard space
    2. Collapses multiple spaces to single space
    3. Strips leading/trailing whitespace
    4. Removes zero-width characters
    
    Args:
        text: Input text
        
    Returns:
        Text with normalized whitespace
    """
    if not text:
        return ""
    
    # Replace all whitespace chars with standard space
    result = text
    for ws_char in WHITESPACE_CHARS:
        result = result.replace(ws_char, ' ')
    
    # Remove zero-width characters
    result = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', result)
    
    # Collapse multiple spaces
    result = re.sub(r' +', ' ', result)
    
    return result.strip()


# ============================================================================
# CASE NORMALIZATION
# ============================================================================

def normalize_case(text: str, mode: str = 'lower') -> str:
    """
    Normalize text case.
    
    Args:
        text: Input text
        mode: One of 'lower', 'upper', 'title', 'preserve'
        
    Returns:
        Case-normalized text
    """
    if not text:
        return ""
    
    if mode == 'lower':
        return text.lower()
    elif mode == 'upper':
        return text.upper()
    elif mode == 'title':
        return text.title()
    else:  # preserve
        return text


# ============================================================================
# NAME NORMALIZATION
# ============================================================================

# Common name prefixes to handle
NAME_PREFIXES = {'dr', 'mr', 'mrs', 'ms', 'prof', 'coach'}

# Common name suffixes to handle
NAME_SUFFIXES = {'jr', 'sr', 'ii', 'iii', 'iv', 'v', 'phd', 'md'}

# Words that should never be in a name
NAME_BLACKLIST = {
    # Titles and roles
    'coach', 'coordinator', 'director', 'assistant', 'associate', 'head',
    'offensive', 'defensive', 'special', 'teams', 'strength', 'conditioning',
    'recruiting', 'operations', 'analyst', 'graduate', 'quality', 'control',
    
    # Position names
    'linebackers', 'receivers', 'running', 'backs', 'quarterbacks', 'tight', 'ends',
    'secondary', 'safeties', 'cornerbacks', 'line', 'linemen',
    
    # Contact/web
    'phone', 'email', 'fax', 'office', 'contact', 'http', 'www', 
    'twitter', 'facebook', 'instagram', 'bio', 'profile', 'read', 'more',
    
    # Other
    'football', 'athletic', 'university', 'college', 'schedule', 'roster',
    'news', 'video', 'photo', 'ticket', 'donate', 'shop', 'menu', 'home',
    'click', 'here', 'view', 'full', 'page', 'staff', 'team',
}


def normalize_name(name: str) -> str:
    """
    Normalize a person's name to consistent format.
    
    This function:
    1. Removes titles (Dr., Coach, etc.)
    2. Normalizes whitespace
    3. Applies proper capitalization
    4. Handles special cases (Mc, Mac, O', etc.)
    
    Args:
        name: Input name string
        
    Returns:
        Normalized name
        
    Examples:
        "coach john smith" -> "John Smith"
        "JOHN MCCARTHY" -> "John McCarthy"
        "O'BRIEN" -> "O'Brien"
    """
    if not name:
        return ""
    
    # Basic normalization
    name = normalize_unicode(name)
    name = normalize_whitespace(name)
    
    # Remove common prefixes
    words = name.split()
    if words and words[0].lower().rstrip('.') in NAME_PREFIXES:
        words = words[1:]
    
    if not words:
        return ""
    
    # Remove suffixes (but keep them noted)
    while words and words[-1].lower().rstrip('.') in NAME_SUFFIXES:
        words = words[:-1]
    
    if not words:
        return ""
    
    # Rejoin and title case
    name = ' '.join(words)
    name = name.title()
    
    # Handle special capitalization patterns
    # Mc prefix: McDonald, McCarthy
    name = re.sub(
        r'\bMc([a-z])',
        lambda m: f"Mc{m.group(1).upper()}",
        name
    )
    
    # Mac prefix: MacArthur, MacGregor
    name = re.sub(
        r'\bMac([a-z])',
        lambda m: f"Mac{m.group(1).upper()}",
        name
    )
    
    # O' prefix: O'Brien, O'Connor
    name = re.sub(
        r"\bO'([a-z])",
        lambda m: f"O'{m.group(1).upper()}",
        name
    )
    
    # De prefix: De La Cruz
    name = re.sub(
        r'\bDe ([a-z])',
        lambda m: f"De {m.group(1).upper()}",
        name
    )
    
    return name


def is_valid_name(text: str) -> Tuple[bool, int, List[str]]:
    """
    Validate if text is likely a person's name.
    
    Returns:
        Tuple of (is_valid, confidence_score, reasons)
        
    Confidence scoring:
    - Base score: 50
    - +20 for 2-3 word names
    - +15 for proper capitalization
    - +10 for all alphabetic
    - +5 for reasonable length
    - -50 for blacklisted words
    - -30 for special characters
    """
    reasons = []
    
    if not text:
        return False, 0, ["Empty text"]
    
    text = normalize_whitespace(text)
    
    # Length checks
    if len(text) < 3:
        return False, 0, ["Too short"]
    if len(text) > 60:
        return False, 0, ["Too long"]
    
    # Must have space (first + last name)
    words = text.split()
    if len(words) < 2:
        return False, 0, ["Single word (no space)"]
    
    # Check for blacklisted words
    text_lower = text.lower()
    for word in NAME_BLACKLIST:
        if word in text_lower:
            return False, 0, [f"Contains blacklisted word: {word}"]
    
    # Check for URLs/emails
    if any(c in text for c in ['@', 'http', '.com', '.edu', '.org']):
        return False, 0, ["Contains URL/email characters"]
    
    # Start calculating confidence
    confidence = 50
    
    # Word count bonus
    if 2 <= len(words) <= 4:
        confidence += 20
        reasons.append("+20: Good word count")
    elif len(words) > 4:
        confidence -= 10
        reasons.append("-10: Too many words")
    
    # Capitalization check
    properly_capitalized = all(
        w[0].isupper() for w in words if w and w[0].isalpha()
    )
    if properly_capitalized:
        confidence += 15
        reasons.append("+15: Proper capitalization")
    
    # Alphabetic ratio
    alpha_count = sum(1 for c in text if c.isalpha() or c.isspace())
    alpha_ratio = alpha_count / len(text) if text else 0
    
    if alpha_ratio >= 0.9:
        confidence += 10
        reasons.append("+10: High alphabetic ratio")
    elif alpha_ratio < 0.7:
        confidence -= 20
        reasons.append("-20: Low alphabetic ratio")
    
    # Length bonus
    if 5 <= len(text) <= 35:
        confidence += 5
        reasons.append("+5: Good length")
    
    # No digits
    if not any(c.isdigit() for c in text):
        confidence += 5
        reasons.append("+5: No digits")
    else:
        confidence -= 10
        reasons.append("-10: Contains digits")
    
    # Clamp confidence
    confidence = max(0, min(100, confidence))
    
    return confidence >= 50, confidence, reasons


# ============================================================================
# TITLE NORMALIZATION
# ============================================================================

def normalize_title(title: str) -> str:
    """
    Normalize a job/role title for consistent matching.
    
    This function:
    1. Applies all text normalizations
    2. Normalizes separators
    3. Lowercase for matching
    4. Removes extra punctuation
    
    Args:
        title: Input title string
        
    Returns:
        Normalized title suitable for pattern matching
    """
    if not title:
        return ""
    
    # Apply normalizations
    result = normalize_unicode(title)
    result = normalize_whitespace(result)
    result = normalize_separators(result)
    
    # Lowercase for matching
    result = result.lower()
    
    # Remove trailing punctuation
    result = result.rstrip('.,;:')
    
    return result


# ============================================================================
# COMPREHENSIVE NORMALIZATION PIPELINE
# ============================================================================

class TextNormalizer:
    """
    Comprehensive text normalization pipeline.
    
    This class provides a configurable pipeline for normalizing
    text with caching for performance.
    
    Usage:
        normalizer = TextNormalizer()
        normalized = normalizer.normalize("Run Game Coordinator – OL")
        segments = normalizer.split_roles("RC / Offensive Line Coach")
    """
    
    def __init__(
        self,
        normalize_case: bool = True,
        normalize_separators: bool = True,
        preserve_word_hyphens: bool = True,
    ):
        """
        Initialize normalizer with configuration.
        
        Args:
            normalize_case: Convert to lowercase
            normalize_separators: Normalize separator characters
            preserve_word_hyphens: Keep hyphens in hyphenated words
        """
        self.normalize_case_flag = normalize_case
        self.normalize_separators_flag = normalize_separators
        self.preserve_word_hyphens = preserve_word_hyphens
    
    @lru_cache(maxsize=1000)
    def normalize(self, text: str) -> str:
        """
        Apply full normalization pipeline.
        
        Results are cached for performance.
        """
        if not text:
            return ""
        
        result = normalize_unicode(text)
        result = normalize_whitespace(result)
        
        if self.normalize_separators_flag:
            result = normalize_separators(result, self.preserve_word_hyphens)
        
        if self.normalize_case_flag:
            result = result.lower()
        
        return result
    
    def split_roles(self, title: str) -> List[str]:
        """Split multi-role title into segments."""
        return split_multi_role_title(title)
    
    def normalize_name(self, name: str) -> str:
        """Normalize a person's name."""
        return normalize_name(name)
    
    def validate_name(self, name: str) -> Tuple[bool, int, List[str]]:
        """Validate if text is a name."""
        return is_valid_name(name)
    
    def clear_cache(self) -> None:
        """Clear the normalization cache."""
        self.normalize.cache_clear()


# ============================================================================
# MODULE-LEVEL SINGLETON
# ============================================================================

_default_normalizer: Optional[TextNormalizer] = None


def get_normalizer() -> TextNormalizer:
    """Get the default normalizer instance (singleton)."""
    global _default_normalizer
    if _default_normalizer is None:
        _default_normalizer = TextNormalizer()
    return _default_normalizer


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def normalize(text: str) -> str:
    """Convenience function for full normalization."""
    return get_normalizer().normalize(text)


def split_roles(title: str) -> List[str]:
    """Convenience function for splitting multi-role titles."""
    return get_normalizer().split_roles(title)


# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    # Main classes
    'TextNormalizer',
    
    # Core functions
    'normalize_unicode',
    'normalize_whitespace', 
    'normalize_separators',
    'normalize_case',
    'normalize_name',
    'normalize_title',
    'is_valid_name',
    'split_multi_role_title',
    
    # Convenience
    'normalize',
    'split_roles',
    'get_normalizer',
]


# ============================================================================
# SELF-TEST
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("TEXT NORMALIZER SELF-TEST")
    print("=" * 70)
    
    # Test cases
    test_cases = [
        # Separator normalization
        ("Run Game Coordinator – Offensive Line Coach", "run game coordinator / offensive line coach"),
        ("Offensive Coordinator — Quarterbacks", "run game coordinator / quarterbacks"),
        ("RC | Tight Ends", "rc / tight ends"),
        ("OL • Recruiting", "ol / recruiting"),
        
        # Whitespace
        ("  Multiple   Spaces  ", "multiple spaces"),
        ("Tab\tSeparated", "tab separated"),
        
        # Unicode
        ("Ｆｕｌｌ　Ｗｉｄｔｈ", "full width"),  # Full-width chars
    ]
    
    normalizer = TextNormalizer()
    
    print("\n--- Separator & Whitespace Tests ---")
    for input_text, _ in test_cases[:7]:
        result = normalizer.normalize(input_text)
        print(f"Input:  '{input_text}'")
        print(f"Output: '{result}'")
        print()
    
    print("\n--- Multi-Role Splitting Tests ---")
    split_tests = [
        "Run Game Coordinator / Offensive Line Coach",
        "Offensive Coordinator – Quarterbacks Coach",
        "RC / OL / Tight Ends",
        "Director of Football Operations",
    ]
    
    for title in split_tests:
        segments = split_multi_role_title(title)
        print(f"Title: '{title}'")
        print(f"Segments: {segments}")
        print()
    
    print("\n--- Name Validation Tests ---")
    name_tests = [
        "John Smith",
        "Adrian Brunori",
        "O'Brien McCarthy",
        "Coach John Smith",  # Should strip "Coach"
        "john.smith@email.com",  # Should fail
        "Offensive Line Coach",  # Should fail (not a name)
        "Dr. James Wilson Jr.",
    ]
    
    for name in name_tests:
        is_valid, confidence, reasons = is_valid_name(name)
        normalized = normalize_name(name)
        print(f"Name: '{name}'")
        print(f"  Valid: {is_valid}, Confidence: {confidence}")
        print(f"  Normalized: '{normalized}'")
        print(f"  Reasons: {reasons[:3]}...")
        print()
    
    print("=" * 70)
    print("SELF-TEST COMPLETE")
    print("=" * 70)
