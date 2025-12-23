"""
data/__init__.py - Data Module
"""

from data.schools import (
    School,
    SchoolDatabase,
    NaturalLanguageFilter,
    get_school_database,
    ALL_SCHOOLS,
    FBS_SCHOOLS,
    FCS_SCHOOLS,
    D2_SCHOOLS,
    D3_SCHOOLS,
    DIVISIONS,
    REGIONS,
    STATE_NAMES,
    WARM_STATES,
    FBS_CONFERENCES,
    FCS_CONFERENCES,
)

__all__ = [
    'School',
    'SchoolDatabase',
    'NaturalLanguageFilter',
    'get_school_database',
    'ALL_SCHOOLS',
    'FBS_SCHOOLS',
    'FCS_SCHOOLS',
    'D2_SCHOOLS',
    'D3_SCHOOLS',
    'DIVISIONS',
    'REGIONS',
    'STATE_NAMES',
    'WARM_STATES',
    'FBS_CONFERENCES',
    'FCS_CONFERENCES',
]
