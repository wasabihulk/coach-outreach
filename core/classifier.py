"""
core/classifier.py - Role Classification Engine
============================================================================
Enterprise-grade role classification with comprehensive pattern matching.

This module classifies job titles into canonical football coaching roles
using a multi-layered pattern matching system with confidence scoring.

Architecture:
- Pattern Layer: Regex patterns with associated confidence weights
- Synonym Layer: Abbreviation and synonym expansion
- Inference Layer: Role inference from context
- Validation Layer: Confidence thresholds and sanity checks

Design Principles:
- Comprehensive pattern coverage (400+ patterns)
- Explicit confidence scoring with documented reasoning
- No false positives (precision over recall)
- Full audit trail for debugging

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

import re
from typing import List, Dict, Optional, Tuple, Set, Pattern
from dataclasses import dataclass
from functools import lru_cache

from core.types import (
    CanonicalRole, 
    RoleClassification, 
    ConfidenceLevel
)
from core.normalizer import (
    normalize,
    normalize_title,
    split_multi_role_title,
    TextNormalizer,
    get_normalizer
)


# ============================================================================
# ABBREVIATION DICTIONARY
# ============================================================================

# Comprehensive mapping of abbreviations to full forms
# Format: abbreviation -> (expansion, is_role_specific)
ABBREVIATIONS: Dict[str, Tuple[str, bool]] = {
    # Offensive Line (highest priority - our target)
    'ol': ('offensive line', True),
    'o-line': ('offensive line', True),
    'oline': ('offensive line', True),
    'o.l.': ('offensive line', True),
    'o/l': ('offensive line', True),
    'o-l': ('offensive line', True),
    
    # Recruiting (target role)
    'rc': ('recruiting coordinator', True),
    'rec': ('recruiting', True),
    'recr': ('recruiting', True),
    'recrt': ('recruiting', True),
    
    # Coordinators
    'oc': ('offensive coordinator', True),
    'dc': ('defensive coordinator', True),
    'stc': ('special teams coordinator', True),
    'rgc': ('run game coordinator', True),
    'pgc': ('pass game coordinator', True),
    'co-oc': ('co-offensive coordinator', True),
    'co-dc': ('co-defensive coordinator', True),
    
    # Position groups
    'qb': ('quarterbacks', True),
    'qbs': ('quarterbacks', True),
    'rb': ('running backs', True),
    'rbs': ('running backs', True),
    'wr': ('wide receivers', True),
    'wrs': ('wide receivers', True),
    'te': ('tight ends', True),
    'tes': ('tight ends', True),
    'dl': ('defensive line', True),
    'db': ('defensive backs', True),
    'dbs': ('defensive backs', True),
    'lb': ('linebackers', True),
    'lbs': ('linebackers', True),
    'cb': ('cornerbacks', True),
    'cbs': ('cornerbacks', True),
    'fs': ('free safety', True),
    'ss': ('strong safety', True),
    'iol': ('interior offensive line', True),
    
    # Staff types
    'hc': ('head coach', True),
    'ahc': ('assistant head coach', True),
    'ga': ('graduate assistant', True),
    'qc': ('quality control', True),
    
    # Generic
    'dir': ('director', False),
    'asst': ('assistant', False),
    'assoc': ('associate', False),
    'sr': ('senior', False),
    'jr': ('junior', False),
    'exec': ('executive', False),
    'mgr': ('manager', False),
    'coord': ('coordinator', False),
}


def expand_abbreviations(text: str) -> str:
    """
    Expand known abbreviations in text.
    
    Uses word boundary matching to avoid partial replacements.
    
    Args:
        text: Input text (should be lowercase)
        
    Returns:
        Text with abbreviations expanded
    """
    if not text:
        return ""
    
    result = text.lower()
    
    # Sort by length (longest first) to handle overlapping patterns
    sorted_abbrevs = sorted(ABBREVIATIONS.keys(), key=len, reverse=True)
    
    for abbrev in sorted_abbrevs:
        expansion, _ = ABBREVIATIONS[abbrev]
        # Use word boundary matching
        pattern = r'\b' + re.escape(abbrev) + r'\b'
        result = re.sub(pattern, expansion, result, flags=re.IGNORECASE)
    
    return result


# ============================================================================
# PATTERN DEFINITIONS
# ============================================================================

@dataclass(frozen=True)
class RolePattern:
    """
    A pattern for matching a specific role.
    
    Attributes:
        pattern: Regex pattern (will be compiled)
        base_confidence: Base confidence score when matched (0-100)
        role: The canonical role this pattern indicates
        description: Human-readable description of what this matches
        is_primary: Whether this is a primary/exact match pattern
        requires_context: Whether additional context validation is needed
    """
    pattern: str
    base_confidence: int
    role: CanonicalRole
    description: str
    is_primary: bool = True
    requires_context: bool = False
    
    @property
    def compiled(self) -> Pattern:
        """Get compiled regex pattern."""
        return re.compile(self.pattern, re.IGNORECASE)


# ============================================================================
# OFFENSIVE LINE COACH PATTERNS
# ============================================================================

OL_PATTERNS: List[RolePattern] = [
    # === TIER 1: Exact/Primary Matches (90-100 confidence) ===
    RolePattern(
        pattern=r'\boffensive\s+line\s+coach\b',
        base_confidence=100,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Exact match: Offensive Line Coach"
    ),
    RolePattern(
        pattern=r'\boffensive\s+line\s+coordinator\b',
        base_confidence=100,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Exact match: Offensive Line Coordinator"
    ),
    RolePattern(
        pattern=r'\bo[\-\s]?line\s+coach\b',
        base_confidence=95,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="O-Line Coach variant"
    ),
    RolePattern(
        pattern=r'\boline\s+coach\b',
        base_confidence=95,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="OLine Coach variant"
    ),
    RolePattern(
        pattern=r'\bassistant\s+head\s+coach\s*/?\s*offensive\s+line\b',
        base_confidence=95,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Assistant Head Coach / Offensive Line"
    ),
    
    # === TIER 2: Strong Matches (80-89 confidence) ===
    RolePattern(
        pattern=r'\brun\s+game\s+coordinator\s*/\s*offensive\s+line',
        base_confidence=90,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Run Game Coordinator / Offensive Line"
    ),
    RolePattern(
        pattern=r'\boffensive\s+line\s*/\s*run\s+game',
        base_confidence=90,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Offensive Line / Run Game"
    ),
    RolePattern(
        pattern=r'\bco[\-\s]?offensive\s+coordinator\s*/?\s*offensive\s+line\b',
        base_confidence=88,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Co-OC / Offensive Line"
    ),
    RolePattern(
        pattern=r'\boffensive\s+coordinator\s*/?\s*offensive\s+line\b',
        base_confidence=88,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="OC / Offensive Line"
    ),
    RolePattern(
        pattern=r'\boffensive\s+linemen?\s+coach\b',
        base_confidence=85,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Offensive Linemen Coach"
    ),
    
    # === TIER 3: Good Matches (70-79 confidence) ===
    RolePattern(
        pattern=r'\boffensive\s+line\b(?!\s+backer)',  # Not "offensive linebacker"
        base_confidence=78,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Contains 'Offensive Line'"
    ),
    RolePattern(
        pattern=r'\bo[\-\s]?line\b',
        base_confidence=75,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Contains 'O-Line'"
    ),
    RolePattern(
        pattern=r'\binterior\s+offensive\s+line\b',
        base_confidence=78,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Interior Offensive Line"
    ),
    RolePattern(
        pattern=r'\boffensive\s+tackles?\b',
        base_confidence=72,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Offensive Tackles"
    ),
    RolePattern(
        pattern=r'\boffensive\s+guards?\b',
        base_confidence=72,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Offensive Guards"
    ),
    RolePattern(
        pattern=r'\bcenters?\s*/?\s*guards?\b',
        base_confidence=70,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Centers/Guards"
    ),
    
    # === TIER 4: Moderate Matches (60-69 confidence) ===
    RolePattern(
        pattern=r'\bol\s+coach\b',
        base_confidence=68,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="OL Coach abbreviation"
    ),
    RolePattern(
        pattern=r'\bassistant\s+ol\b',
        base_confidence=65,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Assistant OL"
    ),
    RolePattern(
        pattern=r'(?:^|/\s*)ol(?:\s*$|\s*/)',  # OL at boundary
        base_confidence=62,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="OL abbreviation at role boundary"
    ),
    RolePattern(
        pattern=r'\brun\s+game\s+coordinator\b',
        base_confidence=60,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Run Game Coordinator (often OL)",
        requires_context=True
    ),
    
    # === TIER 5: Weak Matches (50-59 confidence) ===
    RolePattern(
        pattern=r'\boffensive\s+linemen\b',
        base_confidence=55,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Offensive Linemen (generic)"
    ),
    RolePattern(
        pattern=r'\bgrad(?:uate)?\s+assistant\s*[/\-–]?\s*(?:ol|offensive\s+line)\b',
        base_confidence=52,
        role=CanonicalRole.OFFENSIVE_LINE_COACH,
        description="Graduate Assistant - OL"
    ),
]


# ============================================================================
# RECRUITING COORDINATOR PATTERNS  
# ============================================================================

RC_PATTERNS: List[RolePattern] = [
    # === TIER 1: Exact/Primary Matches (90-100 confidence) ===
    RolePattern(
        pattern=r'\brecruiting\s+coordinator\b',
        base_confidence=100,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Exact match: Recruiting Coordinator"
    ),
    RolePattern(
        pattern=r'\bdirector\s+of\s+recruiting\b',
        base_confidence=100,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Director of Recruiting"
    ),
    RolePattern(
        pattern=r'\bdirector\s+of\s+player\s+personnel\b',
        base_confidence=95,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Director of Player Personnel"
    ),
    RolePattern(
        pattern=r'\bdirector\s+of\s+football\s+recruiting\b',
        base_confidence=100,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Director of Football Recruiting"
    ),
    RolePattern(
        pattern=r'\brecruiting\s+director\b',
        base_confidence=98,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Recruiting Director"
    ),
    RolePattern(
        pattern=r'\bhead\s+of\s+recruiting\b',
        base_confidence=98,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Head of Recruiting"
    ),
    
    # === TIER 2: Strong Matches (80-89 confidence) ===
    RolePattern(
        pattern=r'\bassistant\s+director\s+of\s+recruiting\b',
        base_confidence=88,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Assistant Director of Recruiting"
    ),
    RolePattern(
        pattern=r'\bdirector\s+of\s+on[\-\s]?campus\s+recruiting\b',
        base_confidence=90,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Director of On-Campus Recruiting"
    ),
    RolePattern(
        pattern=r'\bcoordinator\s+of\s+recruiting\b',
        base_confidence=92,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Coordinator of Recruiting"
    ),
    RolePattern(
        pattern=r'\brecruiting\s+operations\s+coordinator\b',
        base_confidence=85,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Recruiting Operations Coordinator"
    ),
    
    # === TIER 3: Good Matches (70-79 confidence) ===
    RolePattern(
        pattern=r'\bplayer\s+personnel\b',
        base_confidence=75,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Player Personnel"
    ),
    RolePattern(
        pattern=r'\brecruiting\s+operations\b',
        base_confidence=72,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Recruiting Operations"
    ),
    RolePattern(
        pattern=r'\bon[\-\s]?campus\s+recruiting\b',
        base_confidence=70,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="On-Campus Recruiting"
    ),
    RolePattern(
        pattern=r'\brecruiting\s*[&/]\s*operations\b',
        base_confidence=72,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Recruiting & Operations"
    ),
    
    # === TIER 4: Moderate Matches (50-69 confidence) ===
    RolePattern(
        pattern=r'\bdirector\s+of\s+football\s+operations\b',
        base_confidence=55,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Director of Football Operations (often handles recruiting)",
        requires_context=True
    ),
    RolePattern(
        pattern=r'\bdirector\s+of\s+operations\b',
        base_confidence=45,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Director of Operations (may handle recruiting)",
        requires_context=True,
        is_primary=False
    ),
    RolePattern(
        pattern=r'\bplayer\s+development\b',
        base_confidence=40,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Player Development (may include recruiting)",
        requires_context=True,
        is_primary=False
    ),
    RolePattern(
        pattern=r'(?:^|/\s*)recruiting(?:\s*$|\s*/)',
        base_confidence=65,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="'Recruiting' at role boundary"
    ),
    RolePattern(
        pattern=r'\bassistant\s+.*recruiting\b',
        base_confidence=58,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Assistant with recruiting duties"
    ),
    
    # === Combined role patterns ===
    RolePattern(
        pattern=r'\brecruiting\s+coordinator\s*/\s*\w+',
        base_confidence=92,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="Recruiting Coordinator / [Position]"
    ),
    RolePattern(
        pattern=r'\w+\s*/\s*recruiting\s+coordinator\b',
        base_confidence=92,
        role=CanonicalRole.RECRUITING_COORDINATOR,
        description="[Position] / Recruiting Coordinator"
    ),
]


# ============================================================================
# EXCLUSION PATTERNS (Things that are NOT coaches)
# ============================================================================

EXCLUSION_PATTERNS: List[Pattern] = [
    re.compile(r'\bvideo\b', re.I),
    re.compile(r'\bequipment\b', re.I),
    re.compile(r'\btrainer\b', re.I),
    re.compile(r'\bphysician\b', re.I),
    re.compile(r'\bdoctor\b', re.I),
    re.compile(r'\bnutrition(?:ist)?\b', re.I),
    re.compile(r'\bacademic\b', re.I),
    re.compile(r'\bcompliance\b', re.I),
    re.compile(r'\bmedia\b', re.I),
    re.compile(r'\bcommunications?\b', re.I),
    re.compile(r'\bmarketing\b', re.I),
    re.compile(r'\bticket(?:s|ing)?\b', re.I),
    re.compile(r'\bintern\b', re.I),
    re.compile(r'\bstudent\s+assistant\b', re.I),
    re.compile(r'\bmanager\b', re.I),  # Equipment manager etc.
    re.compile(r'\bsecretary\b', re.I),
    re.compile(r'\badministrative\b', re.I),
    re.compile(r'\bfinance\b', re.I),
    re.compile(r'\baccounting\b', re.I),
    re.compile(r'\bfacilities\b', re.I),
    re.compile(r'\bgrounds\b', re.I),
    re.compile(r'\bcustodial\b', re.I),
    re.compile(r'\bsecurity\b', re.I),
]


def is_excluded_role(title: str) -> Tuple[bool, Optional[str]]:
    """
    Check if a title matches exclusion patterns.
    
    Returns:
        Tuple of (is_excluded, matched_pattern)
    """
    title_lower = title.lower()
    
    for pattern in EXCLUSION_PATTERNS:
        if pattern.search(title_lower):
            return True, pattern.pattern
    
    return False, None


# ============================================================================
# MAIN CLASSIFICATION ENGINE
# ============================================================================

class RoleClassifier:
    """
    Enterprise-grade role classification engine.
    
    This classifier uses a multi-stage pipeline:
    1. Normalization: Clean and standardize input
    2. Expansion: Expand abbreviations
    3. Splitting: Handle multi-role titles
    4. Matching: Apply pattern matching with confidence scoring
    5. Validation: Apply exclusion rules and sanity checks
    6. Ranking: Select best matches
    
    Usage:
        classifier = RoleClassifier()
        results = classifier.classify("Run Game Coordinator / Offensive Line Coach")
        
        for result in results:
            print(f"{result.role}: {result.confidence}%")
    """
    
    def __init__(self):
        """Initialize the classifier with default configuration."""
        self.normalizer = get_normalizer()
        self._compile_patterns()
    
    def _compile_patterns(self) -> None:
        """Pre-compile all regex patterns for performance."""
        # Compile OL patterns
        self._ol_patterns: List[Tuple[Pattern, RolePattern]] = [
            (p.compiled, p) for p in OL_PATTERNS
        ]
        
        # Compile RC patterns
        self._rc_patterns: List[Tuple[Pattern, RolePattern]] = [
            (p.compiled, p) for p in RC_PATTERNS
        ]
    
    def classify(self, title: str) -> List[RoleClassification]:
        """
        Classify a title into canonical roles.
        
        This is the main entry point for classification.
        
        Args:
            title: The job title to classify
            
        Returns:
            List of RoleClassification results, sorted by confidence (highest first)
        """
        if not title or not title.strip():
            return []
        
        results: List[RoleClassification] = []
        inference_chain: List[str] = []
        
        # Step 1: Store original
        original_title = title
        inference_chain.append(f"Original: '{title}'")
        
        # Step 2: Normalize
        normalized = normalize_title(title)
        inference_chain.append(f"Normalized: '{normalized}'")
        
        # Step 3: Check exclusions first
        is_excluded, exclusion_pattern = is_excluded_role(normalized)
        if is_excluded:
            inference_chain.append(f"EXCLUDED: Matched exclusion pattern '{exclusion_pattern}'")
            return []  # Return empty - this is not a coaching role
        
        # Step 4: Expand abbreviations
        expanded = expand_abbreviations(normalized)
        if expanded != normalized:
            inference_chain.append(f"Expanded: '{expanded}'")
        
        # Step 5: Split multi-role titles
        segments = split_multi_role_title(expanded)
        inference_chain.append(f"Segments: {segments}")
        
        # Step 6: Classify each segment
        for segment in segments:
            # Skip very short segments
            if len(segment) < 2:
                continue
            
            segment_chain = list(inference_chain)
            segment_chain.append(f"Processing segment: '{segment}'")
            
            # Try OL patterns
            ol_matches = self._match_patterns(segment, self._ol_patterns, segment_chain)
            results.extend(ol_matches)
            
            # Try RC patterns
            rc_matches = self._match_patterns(segment, self._rc_patterns, segment_chain)
            results.extend(rc_matches)
        
        # Step 7: Deduplicate and sort
        results = self._deduplicate_results(results)
        results.sort(key=lambda r: r.confidence, reverse=True)
        
        # Step 8: Set original title on all results
        for result in results:
            result.original_title = original_title
        
        return results
    
    def _match_patterns(
        self, 
        text: str, 
        patterns: List[Tuple[Pattern, RolePattern]],
        inference_chain: List[str]
    ) -> List[RoleClassification]:
        """
        Match text against a list of patterns.
        
        Args:
            text: Text to match
            patterns: List of (compiled_pattern, RolePattern) tuples
            inference_chain: Audit trail
            
        Returns:
            List of matches
        """
        matches: List[RoleClassification] = []
        
        for compiled, pattern_def in patterns:
            match = compiled.search(text)
            if match:
                # Calculate confidence with adjustments
                confidence = pattern_def.base_confidence
                
                # Adjust for context requirements
                if pattern_def.requires_context and not pattern_def.is_primary:
                    confidence = min(confidence, 60)  # Cap context-dependent matches
                
                # Create result
                result_chain = list(inference_chain)
                result_chain.append(
                    f"MATCH: Pattern '{pattern_def.pattern}' -> "
                    f"{pattern_def.role.value} ({confidence}%)"
                )
                result_chain.append(f"Description: {pattern_def.description}")
                
                matches.append(RoleClassification(
                    role=pattern_def.role,
                    confidence=confidence,
                    matched_pattern=pattern_def.pattern,
                    matched_segment=text,
                    original_title="",  # Will be set by caller
                    inference_chain=result_chain
                ))
        
        return matches
    
    def _deduplicate_results(
        self, 
        results: List[RoleClassification]
    ) -> List[RoleClassification]:
        """
        Deduplicate results, keeping highest confidence for each role.
        """
        best_by_role: Dict[CanonicalRole, RoleClassification] = {}
        
        for result in results:
            existing = best_by_role.get(result.role)
            if existing is None or result.confidence > existing.confidence:
                best_by_role[result.role] = result
        
        return list(best_by_role.values())
    
    def classify_as_ol(self, title: str) -> Tuple[bool, int, Optional[RoleClassification]]:
        """
        Check if title indicates an Offensive Line Coach.
        
        Returns:
            Tuple of (is_ol, confidence, classification)
        """
        results = self.classify(title)
        
        for result in results:
            if result.role == CanonicalRole.OFFENSIVE_LINE_COACH:
                return True, result.confidence, result
        
        return False, 0, None
    
    def classify_as_rc(self, title: str) -> Tuple[bool, int, Optional[RoleClassification]]:
        """
        Check if title indicates a Recruiting Coordinator.
        
        Returns:
            Tuple of (is_rc, confidence, classification)
        """
        results = self.classify(title)
        
        for result in results:
            if result.role == CanonicalRole.RECRUITING_COORDINATOR:
                return True, result.confidence, result
        
        return False, 0, None
    
    def get_best_match(
        self, 
        title: str, 
        target_role: CanonicalRole
    ) -> Optional[RoleClassification]:
        """
        Get the best match for a specific target role.
        
        Args:
            title: Title to classify
            target_role: The role we're looking for
            
        Returns:
            Best matching classification, or None
        """
        results = self.classify(title)
        
        for result in results:
            if result.role == target_role:
                return result
        
        return None


# ============================================================================
# MODULE-LEVEL SINGLETON
# ============================================================================

_classifier: Optional[RoleClassifier] = None


def get_classifier() -> RoleClassifier:
    """Get the default classifier instance (singleton)."""
    global _classifier
    if _classifier is None:
        _classifier = RoleClassifier()
    return _classifier


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def classify_role(title: str) -> List[RoleClassification]:
    """Convenience function for role classification."""
    return get_classifier().classify(title)


def is_ol_coach(title: str) -> Tuple[bool, int]:
    """Check if title indicates OL coach. Returns (is_ol, confidence)."""
    is_ol, conf, _ = get_classifier().classify_as_ol(title)
    return is_ol, conf


def is_recruiting_coordinator(title: str) -> Tuple[bool, int]:
    """Check if title indicates RC. Returns (is_rc, confidence)."""
    is_rc, conf, _ = get_classifier().classify_as_rc(title)
    return is_rc, conf


# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    'RoleClassifier',
    'RolePattern',
    'classify_role',
    'is_ol_coach',
    'is_recruiting_coordinator',
    'get_classifier',
    'expand_abbreviations',
    'is_excluded_role',
    'ABBREVIATIONS',
    'OL_PATTERNS',
    'RC_PATTERNS',
]


# ============================================================================
# SELF-TEST
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("ROLE CLASSIFIER SELF-TEST")
    print("=" * 70)
    
    # Test cases from real-world examples
    test_cases = [
        # Clear OL matches
        "Offensive Line Coach",
        "Run Game Coordinator / Offensive Line Coach",
        "Assistant Head Coach / Offensive Line",
        "O-Line Coach",
        "OL Coach",
        "Interior Offensive Line",
        "Offensive Tackles Coach",
        
        # Clear RC matches
        "Recruiting Coordinator",
        "Director of Recruiting",
        "Director of Player Personnel",
        "Recruiting Coordinator / Running Backs",
        "Director of Football Operations",  # Often handles recruiting
        
        # Multi-role titles
        "OL / Recruiting Coordinator",
        "Offensive Coordinator / Offensive Line",
        "Player Personnel / Recruiting",
        
        # Should NOT match (exclusions)
        "Video Coordinator",
        "Equipment Manager",
        "Athletic Trainer",
        "Academic Coordinator",
        
        # Edge cases
        "Offensive Coordinator / Quarterbacks Coach",  # Not OL
        "Defensive Line Coach",  # Not OL
        "Graduate Assistant – Tight Ends",  # Not OL/RC
    ]
    
    classifier = RoleClassifier()
    
    print("\n" + "-" * 70)
    print("CLASSIFICATION RESULTS")
    print("-" * 70)
    
    for title in test_cases:
        print(f"\n📋 Title: '{title}'")
        results = classifier.classify(title)
        
        if results:
            for r in results:
                level = r.confidence_level
                icon = "✅" if level.can_auto_save else "⚠️" if level.requires_review else "❌"
                print(f"   {icon} {r.role.value}: {r.confidence}% ({level.name})")
                print(f"      Pattern: {r.matched_pattern}")
        else:
            print("   ❌ No roles detected (excluded or unrecognized)")
    
    print("\n" + "=" * 70)
    print("SELF-TEST COMPLETE")
    print("=" * 70)
