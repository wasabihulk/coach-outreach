"""
extraction/__init__.py - Extraction Module
============================================================================
HTML parsing and staff extraction components.

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

from extraction.dom_parser import (
    DOMParser,
    StructuredDataExtractor,
    StaffCardExtractor,
    DOMProximityExtractor,
    TableExtractor,
    TextPatternExtractor,
)

__all__ = [
    'DOMParser',
    'StructuredDataExtractor',
    'StaffCardExtractor',
    'DOMProximityExtractor',
    'TableExtractor',
    'TextPatternExtractor',
]
