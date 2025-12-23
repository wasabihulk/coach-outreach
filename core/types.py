"""
core/types.py - Core Type Definitions and Data Structures
============================================================================
Enterprise-grade type definitions for the Coach Outreach Automation System.

This module defines all data structures used throughout the application,
ensuring type safety, validation, and consistent data handling.

Design Principles:
- Immutable where possible
- Self-validating
- Fully documented
- Serializable for logging/debugging

Author: Coach Outreach System
Version: 2.0.0
License: Proprietary
============================================================================
"""

from __future__ import annotations

import re
import json
from dataclasses import dataclass, field, asdict
from typing import (
    List, Dict, Optional, Tuple, Set, Any, 
    Union, Callable, TypeVar, Generic
)
from enum import Enum, auto
from datetime import datetime
import hashlib


# ============================================================================
# ENUMERATIONS
# ============================================================================

class CanonicalRole(Enum):
    """
    Canonical role classifications for football coaching staff.
    
    These represent the normalized role categories that all detected
    titles will be mapped to. Each role has a clear, unambiguous definition.
    """
    # Primary target roles
    OFFENSIVE_LINE_COACH = "Offensive Line Coach"
    RECRUITING_COORDINATOR = "Recruiting Coordinator"
    
    # Other coaching roles (for completeness and future expansion)
    HEAD_COACH = "Head Coach"
    OFFENSIVE_COORDINATOR = "Offensive Coordinator"
    DEFENSIVE_COORDINATOR = "Defensive Coordinator"
    SPECIAL_TEAMS_COORDINATOR = "Special Teams Coordinator"
    QUARTERBACKS_COACH = "Quarterbacks Coach"
    RUNNING_BACKS_COACH = "Running Backs Coach"
    WIDE_RECEIVERS_COACH = "Wide Receivers Coach"
    TIGHT_ENDS_COACH = "Tight Ends Coach"
    DEFENSIVE_LINE_COACH = "Defensive Line Coach"
    LINEBACKERS_COACH = "Linebackers Coach"
    SECONDARY_COACH = "Secondary Coach"
    STRENGTH_COACH = "Strength & Conditioning Coach"
    
    # Support staff roles
    DIRECTOR_OF_OPERATIONS = "Director of Operations"
    QUALITY_CONTROL = "Quality Control"
    GRADUATE_ASSISTANT = "Graduate Assistant"
    ANALYST = "Analyst"
    
    # Unknown/unclassified
    UNKNOWN = "Unknown"
    
    @classmethod
    def is_target_role(cls, role: 'CanonicalRole') -> bool:
        """Check if this is a primary target role we're searching for."""
        return role in (cls.OFFENSIVE_LINE_COACH, cls.RECRUITING_COORDINATOR)


class ExtractionStrategy(Enum):
    """
    Enumeration of extraction strategies used by the DOM parser.
    
    Each strategy represents a different approach to finding staff
    information on a webpage. Strategies are tried in order of
    reliability and specificity.
    """
    STRUCTURED_DATA = auto()    # JSON-LD, microdata (most reliable)
    STAFF_CARDS = auto()        # Card-based layouts
    DOM_PROXIMITY = auto()      # Element proximity correlation
    TABLE_PARSING = auto()      # HTML table structures
    TEXT_PATTERN = auto()       # Raw text pattern matching
    FALLBACK_SCAN = auto()      # Last-resort full-page scan
    
    def __str__(self) -> str:
        return self.name.lower().replace('_', ' ').title()


class ConfidenceLevel(Enum):
    """
    Confidence level categories for classification decisions.
    
    Each level has associated thresholds and determines how
    the system handles the extraction result.
    """
    VERY_HIGH = (90, 100, "auto_save", "Direct match with primary pattern")
    HIGH = (70, 89, "auto_save", "Strong match with secondary patterns")
    MEDIUM = (50, 69, "review_suggested", "Moderate match, verification recommended")
    LOW = (30, 49, "review_required", "Weak match, manual review required")
    VERY_LOW = (1, 29, "likely_incorrect", "Unlikely match, probably wrong")
    NONE = (0, 0, "not_found", "No match detected")
    
    def __init__(self, min_score: int, max_score: int, action: str, description: str):
        self.min_score = min_score
        self.max_score = max_score
        self.action = action
        self.description = description
    
    @classmethod
    def from_score(cls, score: int) -> 'ConfidenceLevel':
        """Get the confidence level for a given score."""
        for level in cls:
            if level.min_score <= score <= level.max_score:
                return level
        return cls.NONE
    
    @property
    def requires_review(self) -> bool:
        """Check if this confidence level requires manual review."""
        return self.action in ('review_suggested', 'review_required', 'likely_incorrect')
    
    @property
    def can_auto_save(self) -> bool:
        """Check if this confidence level allows automatic saving."""
        return self.action == 'auto_save'


class ProcessingStatus(Enum):
    """Status of a school's processing state."""
    NOT_PROCESSED = "not_processed"
    PROCESSING = "processing"
    COMPLETED = "completed"
    PARTIAL = "partial"           # Some data found, some missing
    NEEDS_REVIEW = "needs_review"
    FAILED = "failed"
    SKIPPED = "skipped"


# ============================================================================
# DATA CLASSES - Core Entities
# ============================================================================

@dataclass
class RoleClassification:
    """
    Result of classifying a title string into a canonical role.
    
    Attributes:
        role: The canonical role this title maps to
        confidence: Confidence score (0-100)
        matched_pattern: The regex pattern that matched
        matched_segment: The specific text segment that matched
        original_title: The complete original title string
        inference_chain: List of reasoning steps (for debugging)
    """
    role: CanonicalRole
    confidence: int
    matched_pattern: str
    matched_segment: str
    original_title: str
    inference_chain: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Validate confidence score range."""
        if not 0 <= self.confidence <= 100:
            raise ValueError(f"Confidence must be 0-100, got {self.confidence}")
    
    @property
    def confidence_level(self) -> ConfidenceLevel:
        """Get the confidence level category."""
        return ConfidenceLevel.from_score(self.confidence)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'role': self.role.value,
            'confidence': self.confidence,
            'confidence_level': self.confidence_level.name,
            'matched_pattern': self.matched_pattern,
            'matched_segment': self.matched_segment,
            'original_title': self.original_title,
            'inference_chain': self.inference_chain,
        }


@dataclass
class ContactInfo:
    """
    Contact information for a staff member.
    
    All fields are validated and normalized upon creation.
    """
    email: Optional[str] = None
    phone: Optional[str] = None
    twitter: Optional[str] = None
    office: Optional[str] = None
    
    def __post_init__(self):
        """Normalize and validate all contact fields."""
        if self.email:
            self.email = self._normalize_email(self.email)
        if self.phone:
            self.phone = self._normalize_phone(self.phone)
        if self.twitter:
            self.twitter = self._normalize_twitter(self.twitter)
    
    @staticmethod
    def _normalize_email(email: str) -> Optional[str]:
        """Normalize email address."""
        if not email:
            return None
        email = email.lower().strip()
        # Basic validation
        if '@' not in email or '.' not in email.split('@')[1]:
            return None
        return email
    
    @staticmethod
    def _normalize_phone(phone: str) -> Optional[str]:
        """Normalize phone number to digits only."""
        if not phone:
            return None
        digits = re.sub(r'[^\d]', '', phone)
        if len(digits) < 10:
            return None
        return digits
    
    @staticmethod
    def _normalize_twitter(twitter: str) -> Optional[str]:
        """Normalize Twitter/X handle."""
        if not twitter:
            return None
        # Extract handle from URL or raw handle
        match = re.search(r'(?:twitter\.com|x\.com)/([A-Za-z0-9_]+)', twitter)
        if match:
            return f"https://x.com/{match.group(1)}"
        # Raw handle
        handle = twitter.strip().lstrip('@')
        if re.match(r'^[A-Za-z0-9_]+$', handle):
            return f"https://x.com/{handle}"
        return None
    
    @property
    def has_any(self) -> bool:
        """Check if any contact info is present."""
        return any([self.email, self.phone, self.twitter, self.office])
    
    def to_dict(self) -> Dict[str, Optional[str]]:
        """Convert to dictionary."""
        return {
            'email': self.email,
            'phone': self.phone,
            'twitter': self.twitter,
            'office': self.office,
        }


@dataclass
class StaffMember:
    """
    Represents a single staff member extracted from a webpage.
    
    This is the primary data structure for storing extracted staff
    information. Each instance represents one person with their
    name, title, contact info, and role classifications.
    
    Attributes:
        name: Full name of the staff member
        raw_title: Original title string as found on page
        normalized_title: Title after normalization processing
        contact: Contact information
        roles: List of role classifications
        extraction_method: How this data was extracted
        extraction_confidence: Overall confidence in extraction accuracy
        dom_context: HTML context for debugging
        source_url: URL where this was extracted from
        extracted_at: Timestamp of extraction
    """
    name: str
    raw_title: str = ""
    normalized_title: str = ""
    contact: ContactInfo = field(default_factory=ContactInfo)
    roles: List[RoleClassification] = field(default_factory=list)
    extraction_method: ExtractionStrategy = ExtractionStrategy.FALLBACK_SCAN
    extraction_confidence: int = 0
    dom_context: str = ""
    source_url: str = ""
    extracted_at: datetime = field(default_factory=datetime.now)
    
    # Internal tracking
    _id: str = field(default="", repr=False)
    
    def __post_init__(self):
        """Generate unique ID and validate data."""
        # Generate deterministic ID
        id_source = f"{self.name}:{self.raw_title}:{self.source_url}"
        self._id = hashlib.md5(id_source.encode()).hexdigest()[:12]
        
        # Normalize name
        self.name = self._normalize_name(self.name)
        
        # Set normalized title if not provided
        if not self.normalized_title and self.raw_title:
            self.normalized_title = self.raw_title.lower().strip()
    
    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize a person's name."""
        if not name:
            return ""
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Title case
        name = name.title()
        
        # Handle special cases (Mc, Mac, O', etc.)
        name = re.sub(r"\bMc(\w)", lambda m: f"Mc{m.group(1).upper()}", name)
        name = re.sub(r"\bMac(\w)", lambda m: f"Mac{m.group(1).upper()}", name)
        name = re.sub(r"\bO'(\w)", lambda m: f"O'{m.group(1).upper()}", name)
        
        return name.strip()
    
    @property
    def id(self) -> str:
        """Unique identifier for this staff member."""
        return self._id
    
    @property
    def first_name(self) -> str:
        """Extract first name."""
        parts = self.name.split()
        return parts[0] if parts else ""
    
    @property
    def last_name(self) -> str:
        """Extract last name."""
        parts = self.name.split()
        return parts[-1] if len(parts) > 1 else parts[0] if parts else ""
    
    @property
    def primary_role(self) -> Optional[RoleClassification]:
        """Get the highest-confidence role classification."""
        if not self.roles:
            return None
        return max(self.roles, key=lambda r: r.confidence)
    
    def has_role(self, role: CanonicalRole) -> bool:
        """Check if this staff member has a specific role."""
        return any(r.role == role for r in self.roles)
    
    def get_role_confidence(self, role: CanonicalRole) -> int:
        """Get confidence score for a specific role (0 if not found)."""
        for r in self.roles:
            if r.role == role:
                return r.confidence
        return 0
    
    def is_ol_coach(self) -> Tuple[bool, int]:
        """Check if this is an OL coach. Returns (is_ol, confidence)."""
        conf = self.get_role_confidence(CanonicalRole.OFFENSIVE_LINE_COACH)
        return conf > 0, conf
    
    def is_recruiting_coordinator(self) -> Tuple[bool, int]:
        """Check if this is an RC. Returns (is_rc, confidence)."""
        conf = self.get_role_confidence(CanonicalRole.RECRUITING_COORDINATOR)
        return conf > 0, conf
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'id': self._id,
            'name': self.name,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'raw_title': self.raw_title,
            'normalized_title': self.normalized_title,
            'contact': self.contact.to_dict(),
            'roles': [r.to_dict() for r in self.roles],
            'primary_role': self.primary_role.to_dict() if self.primary_role else None,
            'extraction_method': str(self.extraction_method),
            'extraction_confidence': self.extraction_confidence,
            'source_url': self.source_url,
            'extracted_at': self.extracted_at.isoformat(),
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


@dataclass
class ExtractionResult:
    """
    Complete result of extracting staff from a single webpage.
    
    This is the primary output of the extraction pipeline, containing
    all extracted staff members plus metadata about the extraction
    process itself.
    
    Attributes:
        url: Source URL
        school_name: Name of the school
        staff: All extracted staff members
        ol_coach: Best OL coach candidate
        ol_confidence: OL coach match confidence
        rc: Best RC candidate
        rc_confidence: RC match confidence
        strategies_used: Which extraction strategies succeeded
        strategies_failed: Which strategies failed and why
        raw_titles_found: All title strings found (for debugging)
        processing_time_ms: How long extraction took
        errors: Any errors encountered
        warnings: Non-fatal issues
        html_hash: Hash of source HTML (for cache validation)
        needs_review: Whether manual review is needed
        review_reasons: Why review is needed
    """
    url: str
    school_name: str = ""
    staff: List[StaffMember] = field(default_factory=list)
    
    # Target coach results
    ol_coach: Optional[StaffMember] = None
    ol_confidence: int = 0
    rc: Optional[StaffMember] = None
    rc_confidence: int = 0
    
    # Extraction metadata
    strategies_used: List[Tuple[ExtractionStrategy, int]] = field(default_factory=list)  # (strategy, count)
    strategies_failed: List[Tuple[ExtractionStrategy, str]] = field(default_factory=list)  # (strategy, reason)
    raw_titles_found: List[str] = field(default_factory=list)
    processing_time_ms: int = 0
    
    # Issues
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Validation
    html_hash: str = ""
    extracted_at: datetime = field(default_factory=datetime.now)
    
    # Review status
    needs_review: bool = False
    review_reasons: List[str] = field(default_factory=list)
    
    @property
    def is_successful(self) -> bool:
        """Check if extraction found usable results."""
        return len(self.staff) > 0 and len(self.errors) == 0
    
    @property
    def found_ol(self) -> bool:
        """Check if OL coach was found with acceptable confidence."""
        return self.ol_coach is not None and self.ol_confidence >= 50
    
    @property
    def found_rc(self) -> bool:
        """Check if RC was found with acceptable confidence."""
        return self.rc is not None and self.rc_confidence >= 50
    
    @property
    def ol_confidence_level(self) -> ConfidenceLevel:
        """Get OL confidence level category."""
        return ConfidenceLevel.from_score(self.ol_confidence)
    
    @property
    def rc_confidence_level(self) -> ConfidenceLevel:
        """Get RC confidence level category."""
        return ConfidenceLevel.from_score(self.rc_confidence)
    
    def determine_review_status(self) -> None:
        """Determine if this result needs manual review and why."""
        self.review_reasons = []
        
        # Check OL
        if self.ol_confidence == 0:
            self.review_reasons.append("OL Coach not found")
        elif self.ol_confidence_level.requires_review:
            self.review_reasons.append(
                f"OL Coach low confidence ({self.ol_confidence}%): {self.ol_confidence_level.description}"
            )
        
        # Check RC
        if self.rc_confidence == 0:
            self.review_reasons.append("Recruiting Coordinator not found")
        elif self.rc_confidence_level.requires_review:
            self.review_reasons.append(
                f"RC low confidence ({self.rc_confidence}%): {self.rc_confidence_level.description}"
            )
        
        # Check for errors
        if self.errors:
            self.review_reasons.append(f"Extraction errors: {len(self.errors)}")
        
        # Check for no staff found
        if len(self.staff) == 0:
            self.review_reasons.append("No staff members extracted")
        
        self.needs_review = len(self.review_reasons) > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'url': self.url,
            'school_name': self.school_name,
            'staff_count': len(self.staff),
            'staff': [s.to_dict() for s in self.staff],
            'ol_coach': self.ol_coach.to_dict() if self.ol_coach else None,
            'ol_confidence': self.ol_confidence,
            'ol_confidence_level': self.ol_confidence_level.name,
            'rc': self.rc.to_dict() if self.rc else None,
            'rc_confidence': self.rc_confidence,
            'rc_confidence_level': self.rc_confidence_level.name,
            'strategies_used': [(str(s), c) for s, c in self.strategies_used],
            'strategies_failed': [(str(s), r) for s, r in self.strategies_failed],
            'raw_titles_found': self.raw_titles_found,
            'processing_time_ms': self.processing_time_ms,
            'errors': self.errors,
            'warnings': self.warnings,
            'needs_review': self.needs_review,
            'review_reasons': self.review_reasons,
            'extracted_at': self.extracted_at.isoformat(),
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)
    
    def get_summary(self) -> str:
        """Get a human-readable summary."""
        lines = [
            f"Extraction Result for: {self.school_name}",
            f"URL: {self.url}",
            f"Staff found: {len(self.staff)}",
            f"OL Coach: {self.ol_coach.name if self.ol_coach else 'NOT FOUND'} ({self.ol_confidence}%)",
            f"RC: {self.rc.name if self.rc else 'NOT FOUND'} ({self.rc_confidence}%)",
            f"Needs Review: {'YES' if self.needs_review else 'NO'}",
        ]
        if self.review_reasons:
            lines.append("Review Reasons:")
            for reason in self.review_reasons:
                lines.append(f"  - {reason}")
        return '\n'.join(lines)


@dataclass  
class SchoolRecord:
    """
    Represents a school's record in the database/spreadsheet.
    
    This tracks the current state of data collection for a school,
    including what has been found, what's missing, and processing status.
    """
    row_index: int  # Row in spreadsheet (1-indexed)
    school_name: str
    staff_url: str
    
    # Current data
    rc_name: str = ""
    rc_email: str = ""
    rc_twitter: str = ""
    rc_contacted: str = ""
    
    ol_name: str = ""
    ol_email: str = ""
    ol_twitter: str = ""
    ol_contacted: str = ""
    
    # Notes
    rc_notes: str = ""
    ol_notes: str = ""
    
    # Processing status
    status: ProcessingStatus = ProcessingStatus.NOT_PROCESSED
    last_processed: Optional[datetime] = None
    
    @property
    def needs_rc(self) -> bool:
        """Check if RC data is needed."""
        return not self.rc_name
    
    @property
    def needs_ol(self) -> bool:
        """Check if OL data is needed."""
        return not self.ol_name
    
    @property
    def needs_processing(self) -> bool:
        """Check if this school needs any processing."""
        return self.needs_rc or self.needs_ol
    
    @property
    def is_complete(self) -> bool:
        """Check if all data has been collected."""
        return bool(self.rc_name and self.ol_name)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'row_index': self.row_index,
            'school_name': self.school_name,
            'staff_url': self.staff_url,
            'rc_name': self.rc_name,
            'rc_email': self.rc_email,
            'rc_twitter': self.rc_twitter,
            'rc_contacted': self.rc_contacted,
            'ol_name': self.ol_name,
            'ol_email': self.ol_email,
            'ol_twitter': self.ol_twitter,
            'ol_contacted': self.ol_contacted,
            'rc_notes': self.rc_notes,
            'ol_notes': self.ol_notes,
            'status': self.status.value,
            'needs_rc': self.needs_rc,
            'needs_ol': self.needs_ol,
            'needs_processing': self.needs_processing,
            'is_complete': self.is_complete,
        }


# ============================================================================
# VALIDATION UTILITIES
# ============================================================================

class ValidationResult:
    """Result of validating data."""
    
    def __init__(self):
        self.is_valid: bool = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def add_error(self, message: str) -> None:
        """Add an error (marks result as invalid)."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str) -> None:
        """Add a warning (does not mark as invalid)."""
        self.warnings.append(message)
    
    def merge(self, other: 'ValidationResult') -> None:
        """Merge another validation result into this one."""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        if not other.is_valid:
            self.is_valid = False


def validate_staff_member(member: StaffMember) -> ValidationResult:
    """Validate a StaffMember instance."""
    result = ValidationResult()
    
    # Name validation
    if not member.name:
        result.add_error("Name is required")
    elif len(member.name) < 2:
        result.add_error(f"Name too short: {member.name}")
    elif len(member.name) > 100:
        result.add_error(f"Name too long: {len(member.name)} chars")
    
    # Check for suspicious name content
    suspicious_patterns = ['http', 'www', '@', '.com', '.edu', 'coach', 'coordinator']
    for pattern in suspicious_patterns:
        if pattern in member.name.lower():
            result.add_warning(f"Name contains suspicious pattern '{pattern}': {member.name}")
    
    # Role validation
    if not member.roles:
        result.add_warning("No roles classified")
    else:
        for role in member.roles:
            if role.confidence < 0 or role.confidence > 100:
                result.add_error(f"Invalid confidence score: {role.confidence}")
    
    # Contact validation
    if member.contact.email:
        if not re.match(r'^[^@]+@[^@]+\.[^@]+$', member.contact.email):
            result.add_error(f"Invalid email format: {member.contact.email}")
    
    return result


def validate_extraction_result(result: ExtractionResult) -> ValidationResult:
    """Validate an ExtractionResult instance."""
    validation = ValidationResult()
    
    # URL validation
    if not result.url:
        validation.add_error("URL is required")
    elif not result.url.startswith(('http://', 'https://')):
        validation.add_error(f"Invalid URL format: {result.url}")
    
    # Staff validation
    for member in result.staff:
        member_validation = validate_staff_member(member)
        if not member_validation.is_valid:
            validation.add_error(f"Invalid staff member '{member.name}': {member_validation.errors}")
        validation.warnings.extend(member_validation.warnings)
    
    # Confidence validation
    if result.ol_confidence < 0 or result.ol_confidence > 100:
        validation.add_error(f"Invalid OL confidence: {result.ol_confidence}")
    if result.rc_confidence < 0 or result.rc_confidence > 100:
        validation.add_error(f"Invalid RC confidence: {result.rc_confidence}")
    
    return validation


# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    # Enums
    'CanonicalRole',
    'ExtractionStrategy', 
    'ConfidenceLevel',
    'ProcessingStatus',
    
    # Data classes
    'RoleClassification',
    'ContactInfo',
    'StaffMember',
    'ExtractionResult',
    'SchoolRecord',
    
    # Validation
    'ValidationResult',
    'validate_staff_member',
    'validate_extraction_result',
]
