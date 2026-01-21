"""
core/__init__.py - Core Module Initialization
============================================================================
Enterprise-grade core components for the Coach Outreach System.

This module provides:
- Type definitions and data structures
- Text normalization pipeline
- Role classification engine

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

from core.types import (
    # Enums
    CanonicalRole,
    ExtractionStrategy,
    ConfidenceLevel,
    ProcessingStatus,
    
    # Data classes
    RoleClassification,
    ContactInfo,
    StaffMember,
    ExtractionResult,
    SchoolRecord,
    
    # Validation
    ValidationResult,
    validate_staff_member,
    validate_extraction_result,
)

from core.normalizer import (
    TextNormalizer,
    normalize,
    normalize_unicode,
    normalize_whitespace,
    normalize_separators,
    normalize_name,
    normalize_title,
    is_valid_name,
    split_multi_role_title,
    split_roles,
    get_normalizer,
)

from core.classifier import (
    RoleClassifier,
    classify_role,
    is_ol_coach,
    is_recruiting_coordinator,
    get_classifier,
    expand_abbreviations,
)

__all__ = [
    # Types
    'CanonicalRole',
    'ExtractionStrategy',
    'ConfidenceLevel',
    'ProcessingStatus',
    'RoleClassification',
    'ContactInfo',
    'StaffMember',
    'ExtractionResult',
    'SchoolRecord',
    'ValidationResult',
    'validate_staff_member',
    'validate_extraction_result',
    
    # Normalizer
    'TextNormalizer',
    'normalize',
    'normalize_unicode',
    'normalize_whitespace',
    'normalize_separators',
    'normalize_name',
    'normalize_title',
    'is_valid_name',
    'split_multi_role_title',
    'split_roles',
    'get_normalizer',
    
    # Classifier
    'RoleClassifier',
    'classify_role',
    'is_ol_coach',
    'is_recruiting_coordinator',
    'get_classifier',
    'expand_abbreviations',
]

__version__ = '2.0.0'
