"""
data/schools.py - College Football School Database
============================================================================
Contains comprehensive data for all college football programs.

Features:
- All FBS (D1-FBS), FCS (D1-FCS), D2, D3 schools
- Conference affiliations
- State, region mapping
- Public/Private status
- Enrollment size categories
- Academic rankings (approximated)
- Tuition ranges

Author: Coach Outreach System
Version: 3.2.0
============================================================================
"""

from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
import json
import re

# ============================================================================
# CONSTANTS
# ============================================================================

DIVISIONS = ["FBS", "FCS", "D2", "D3"]

REGIONS = {
    "Northeast": ["CT", "DE", "MA", "MD", "ME", "NH", "NJ", "NY", "PA", "RI", "VT"],
    "Southeast": ["AL", "AR", "FL", "GA", "KY", "LA", "MS", "NC", "SC", "TN", "VA", "WV"],
    "Midwest": ["IA", "IL", "IN", "KS", "MI", "MN", "MO", "ND", "NE", "OH", "SD", "WI"],
    "Southwest": ["AZ", "NM", "OK", "TX"],
    "West": ["AK", "CA", "CO", "HI", "ID", "MT", "NV", "OR", "UT", "WA", "WY"],
}

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "Washington D.C."
}

# Climate classifications for "warm states" filter
WARM_STATES = ["FL", "TX", "AZ", "CA", "LA", "MS", "AL", "GA", "SC", "HI", "NV"]
COLD_STATES = ["MN", "WI", "MI", "ND", "SD", "MT", "WY", "ME", "VT", "NH", "AK"]

# FBS Conferences
FBS_CONFERENCES = [
    "SEC", "Big Ten", "Big 12", "ACC", "Pac-12", 
    "American", "Mountain West", "Sun Belt", "MAC", "C-USA",
    "Independent"
]

# FCS Conferences  
FCS_CONFERENCES = [
    "Big Sky", "CAA", "Missouri Valley", "Southland", "Ohio Valley",
    "Pioneer", "Patriot League", "Ivy League", "MEAC", "SWAC",
    "NEC", "Big South", "Southern", "Independent FCS"
]

# ============================================================================
# SCHOOL DATA
# ============================================================================

# Comprehensive FBS Schools
FBS_SCHOOLS = [
    # SEC
    {"name": "Alabama", "state": "AL", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Auburn", "state": "AL", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Arkansas", "state": "AR", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Florida", "state": "FL", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "low"},
    {"name": "Georgia", "state": "GA", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "low"},
    {"name": "Kentucky", "state": "KY", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "LSU", "state": "LA", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Mississippi State", "state": "MS", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Missouri", "state": "MO", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Ole Miss", "state": "MS", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "South Carolina", "state": "SC", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Tennessee", "state": "TN", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Texas A&M", "state": "TX", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "low"},
    {"name": "Vanderbilt", "state": "TN", "conference": "SEC", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Texas", "state": "TX", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "low"},
    {"name": "Oklahoma", "state": "OK", "conference": "SEC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    
    # Big Ten
    {"name": "Illinois", "state": "IL", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    {"name": "Indiana", "state": "IN", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Iowa", "state": "IA", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Maryland", "state": "MD", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Michigan", "state": "MI", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    {"name": "Michigan State", "state": "MI", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Minnesota", "state": "MN", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Nebraska", "state": "NE", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Northwestern", "state": "IL", "conference": "Big Ten", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Ohio State", "state": "OH", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    {"name": "Penn State", "state": "PA", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    {"name": "Purdue", "state": "IN", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    {"name": "Rutgers", "state": "NJ", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Wisconsin", "state": "WI", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    {"name": "UCLA", "state": "CA", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    {"name": "USC", "state": "CA", "conference": "Big Ten", "public": False, "enrollment": "large", "academic_tier": 1, "tuition": "high"},
    {"name": "Oregon", "state": "OR", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Washington", "state": "WA", "conference": "Big Ten", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    
    # Big 12
    {"name": "Arizona", "state": "AZ", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Arizona State", "state": "AZ", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Baylor", "state": "TX", "conference": "Big 12", "public": False, "enrollment": "medium", "academic_tier": 2, "tuition": "high"},
    {"name": "BYU", "state": "UT", "conference": "Big 12", "public": False, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Cincinnati", "state": "OH", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Colorado", "state": "CO", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Houston", "state": "TX", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Iowa State", "state": "IA", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Kansas", "state": "KS", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Kansas State", "state": "KS", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Oklahoma State", "state": "OK", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "TCU", "state": "TX", "conference": "Big 12", "public": False, "enrollment": "medium", "academic_tier": 2, "tuition": "high"},
    {"name": "Texas Tech", "state": "TX", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "UCF", "state": "FL", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Utah", "state": "UT", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "West Virginia", "state": "WV", "conference": "Big 12", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    
    # ACC
    {"name": "Boston College", "state": "MA", "conference": "ACC", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "California", "state": "CA", "conference": "ACC", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    {"name": "Clemson", "state": "SC", "conference": "ACC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Duke", "state": "NC", "conference": "ACC", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Florida State", "state": "FL", "conference": "ACC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Georgia Tech", "state": "GA", "conference": "ACC", "public": True, "enrollment": "medium", "academic_tier": 1, "tuition": "medium"},
    {"name": "Louisville", "state": "KY", "conference": "ACC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Miami", "state": "FL", "conference": "ACC", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "NC State", "state": "NC", "conference": "ACC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "North Carolina", "state": "NC", "conference": "ACC", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "low"},
    {"name": "Notre Dame", "state": "IN", "conference": "ACC", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Pittsburgh", "state": "PA", "conference": "ACC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "SMU", "state": "TX", "conference": "ACC", "public": False, "enrollment": "medium", "academic_tier": 2, "tuition": "high"},
    {"name": "Stanford", "state": "CA", "conference": "ACC", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Syracuse", "state": "NY", "conference": "ACC", "public": False, "enrollment": "medium", "academic_tier": 2, "tuition": "high"},
    {"name": "Virginia", "state": "VA", "conference": "ACC", "public": True, "enrollment": "large", "academic_tier": 1, "tuition": "medium"},
    {"name": "Virginia Tech", "state": "VA", "conference": "ACC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Wake Forest", "state": "NC", "conference": "ACC", "public": False, "enrollment": "small", "academic_tier": 1, "tuition": "high"},
    
    # American Athletic Conference
    {"name": "Army", "state": "NY", "conference": "American", "public": True, "enrollment": "small", "academic_tier": 1, "tuition": "free"},
    {"name": "Charlotte", "state": "NC", "conference": "American", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "East Carolina", "state": "NC", "conference": "American", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "FAU", "state": "FL", "conference": "American", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Memphis", "state": "TN", "conference": "American", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Navy", "state": "MD", "conference": "American", "public": True, "enrollment": "small", "academic_tier": 1, "tuition": "free"},
    {"name": "North Texas", "state": "TX", "conference": "American", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Rice", "state": "TX", "conference": "American", "public": False, "enrollment": "small", "academic_tier": 1, "tuition": "high"},
    {"name": "South Florida", "state": "FL", "conference": "American", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Temple", "state": "PA", "conference": "American", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "medium"},
    {"name": "Tulane", "state": "LA", "conference": "American", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Tulsa", "state": "OK", "conference": "American", "public": False, "enrollment": "small", "academic_tier": 2, "tuition": "high"},
    {"name": "UAB", "state": "AL", "conference": "American", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "UTSA", "state": "TX", "conference": "American", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    
    # Mountain West
    {"name": "Air Force", "state": "CO", "conference": "Mountain West", "public": True, "enrollment": "small", "academic_tier": 1, "tuition": "free"},
    {"name": "Boise State", "state": "ID", "conference": "Mountain West", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Colorado State", "state": "CO", "conference": "Mountain West", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Fresno State", "state": "CA", "conference": "Mountain West", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Hawaii", "state": "HI", "conference": "Mountain West", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Nevada", "state": "NV", "conference": "Mountain West", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "New Mexico", "state": "NM", "conference": "Mountain West", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "San Diego State", "state": "CA", "conference": "Mountain West", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "San Jose State", "state": "CA", "conference": "Mountain West", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "UNLV", "state": "NV", "conference": "Mountain West", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Utah State", "state": "UT", "conference": "Mountain West", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Wyoming", "state": "WY", "conference": "Mountain West", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    
    # Sun Belt
    {"name": "Appalachian State", "state": "NC", "conference": "Sun Belt", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Arkansas State", "state": "AR", "conference": "Sun Belt", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Coastal Carolina", "state": "SC", "conference": "Sun Belt", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Georgia Southern", "state": "GA", "conference": "Sun Belt", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Georgia State", "state": "GA", "conference": "Sun Belt", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "James Madison", "state": "VA", "conference": "Sun Belt", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Louisiana", "state": "LA", "conference": "Sun Belt", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Marshall", "state": "WV", "conference": "Sun Belt", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Old Dominion", "state": "VA", "conference": "Sun Belt", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "South Alabama", "state": "AL", "conference": "Sun Belt", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Southern Miss", "state": "MS", "conference": "Sun Belt", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Texas State", "state": "TX", "conference": "Sun Belt", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Troy", "state": "AL", "conference": "Sun Belt", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "UL Monroe", "state": "LA", "conference": "Sun Belt", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    
    # MAC
    {"name": "Akron", "state": "OH", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Ball State", "state": "IN", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Bowling Green", "state": "OH", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Buffalo", "state": "NY", "conference": "MAC", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Central Michigan", "state": "MI", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Eastern Michigan", "state": "MI", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Kent State", "state": "OH", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Miami (OH)", "state": "OH", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 2, "tuition": "medium"},
    {"name": "Northern Illinois", "state": "IL", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Ohio", "state": "OH", "conference": "MAC", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Toledo", "state": "OH", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Western Michigan", "state": "MI", "conference": "MAC", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    
    # C-USA
    {"name": "FIU", "state": "FL", "conference": "C-USA", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Jacksonville State", "state": "AL", "conference": "C-USA", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Kennesaw State", "state": "GA", "conference": "C-USA", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Liberty", "state": "VA", "conference": "C-USA", "public": False, "enrollment": "large", "academic_tier": 3, "tuition": "medium"},
    {"name": "Louisiana Tech", "state": "LA", "conference": "C-USA", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Middle Tennessee", "state": "TN", "conference": "C-USA", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "New Mexico State", "state": "NM", "conference": "C-USA", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Sam Houston", "state": "TX", "conference": "C-USA", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "UTEP", "state": "TX", "conference": "C-USA", "public": True, "enrollment": "large", "academic_tier": 3, "tuition": "low"},
    {"name": "Western Kentucky", "state": "KY", "conference": "C-USA", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
]

# Add division to FBS schools
for school in FBS_SCHOOLS:
    school["division"] = "FBS"

# Sample FCS Schools (can be expanded)
FCS_SCHOOLS = [
    {"name": "North Dakota State", "state": "ND", "conference": "Missouri Valley", "division": "FCS", "public": True, "enrollment": "medium", "academic_tier": 2, "tuition": "low"},
    {"name": "South Dakota State", "state": "SD", "conference": "Missouri Valley", "division": "FCS", "public": True, "enrollment": "medium", "academic_tier": 2, "tuition": "low"},
    {"name": "Montana", "state": "MT", "conference": "Big Sky", "division": "FCS", "public": True, "enrollment": "medium", "academic_tier": 2, "tuition": "low"},
    {"name": "Montana State", "state": "MT", "conference": "Big Sky", "division": "FCS", "public": True, "enrollment": "medium", "academic_tier": 2, "tuition": "low"},
    {"name": "James Madison", "state": "VA", "conference": "CAA", "division": "FCS", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Villanova", "state": "PA", "conference": "CAA", "division": "FCS", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Delaware", "state": "DE", "conference": "CAA", "division": "FCS", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "medium"},
    {"name": "Harvard", "state": "MA", "conference": "Ivy League", "division": "FCS", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Yale", "state": "CT", "conference": "Ivy League", "division": "FCS", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Princeton", "state": "NJ", "conference": "Ivy League", "division": "FCS", "public": False, "enrollment": "small", "academic_tier": 1, "tuition": "high"},
    {"name": "Penn", "state": "PA", "conference": "Ivy League", "division": "FCS", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Columbia", "state": "NY", "conference": "Ivy League", "division": "FCS", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Brown", "state": "RI", "conference": "Ivy League", "division": "FCS", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Dartmouth", "state": "NH", "conference": "Ivy League", "division": "FCS", "public": False, "enrollment": "small", "academic_tier": 1, "tuition": "high"},
    {"name": "Cornell", "state": "NY", "conference": "Ivy League", "division": "FCS", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
]

# Sample D2 Schools
D2_SCHOOLS = [
    {"name": "Ferris State", "state": "MI", "conference": "GLIAC", "division": "D2", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Grand Valley State", "state": "MI", "conference": "GLIAC", "division": "D2", "public": True, "enrollment": "large", "academic_tier": 2, "tuition": "low"},
    {"name": "Northwest Missouri State", "state": "MO", "conference": "MIAA", "division": "D2", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Pittsburg State", "state": "KS", "conference": "MIAA", "division": "D2", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Shepherd", "state": "WV", "conference": "PSAC", "division": "D2", "public": True, "enrollment": "small", "academic_tier": 3, "tuition": "low"},
    {"name": "Slippery Rock", "state": "PA", "conference": "PSAC", "division": "D2", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "West Florida", "state": "FL", "conference": "Gulf South", "division": "D2", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Valdosta State", "state": "GA", "conference": "Gulf South", "division": "D2", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
]

# Sample D3 Schools
D3_SCHOOLS = [
    {"name": "Mount Union", "state": "OH", "conference": "OAC", "division": "D3", "public": False, "enrollment": "small", "academic_tier": 2, "tuition": "medium"},
    {"name": "Wisconsin-Whitewater", "state": "WI", "conference": "WIAC", "division": "D3", "public": True, "enrollment": "medium", "academic_tier": 3, "tuition": "low"},
    {"name": "Mary Hardin-Baylor", "state": "TX", "conference": "ASC", "division": "D3", "public": False, "enrollment": "small", "academic_tier": 2, "tuition": "medium"},
    {"name": "Johns Hopkins", "state": "MD", "conference": "Centennial", "division": "D3", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Carnegie Mellon", "state": "PA", "conference": "PAC", "division": "D3", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "MIT", "state": "MA", "conference": "NEWMAC", "division": "D3", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Emory", "state": "GA", "conference": "UAA", "division": "D3", "public": False, "enrollment": "medium", "academic_tier": 1, "tuition": "high"},
    {"name": "Washington & Lee", "state": "VA", "conference": "ODAC", "division": "D3", "public": False, "enrollment": "small", "academic_tier": 1, "tuition": "high"},
]

# Combine all schools
ALL_SCHOOLS = FBS_SCHOOLS + FCS_SCHOOLS + D2_SCHOOLS + D3_SCHOOLS


# ============================================================================
# SCHOOL DATABASE CLASS
# ============================================================================

@dataclass
class School:
    """Represents a college football program."""
    name: str
    state: str
    conference: str
    division: str
    public: bool
    enrollment: str  # small, medium, large
    academic_tier: int  # 1=top, 2=good, 3=standard
    tuition: str  # low, medium, high, free
    staff_url: str = ""
    ol_coach: str = ""
    rc: str = ""
    ol_email: str = ""
    rc_email: str = ""
    ol_twitter: str = ""
    rc_twitter: str = ""
    favorited: bool = False
    
    @property
    def region(self) -> str:
        """Get region for this school."""
        for region, states in REGIONS.items():
            if self.state in states:
                return region
        return "Unknown"
    
    @property
    def state_name(self) -> str:
        """Get full state name."""
        return STATE_NAMES.get(self.state, self.state)
    
    @property
    def is_warm_state(self) -> bool:
        """Check if in a warm climate state."""
        return self.state in WARM_STATES
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        d = asdict(self)
        d['region'] = self.region
        d['state_name'] = self.state_name
        d['is_warm_state'] = self.is_warm_state
        return d


class SchoolDatabase:
    """
    Database of college football schools with filtering capabilities.
    """
    
    def __init__(self):
        self.schools: List[School] = []
        self.favorites: set = set()
        self._load_schools()
    
    def _load_schools(self):
        """Load all schools into database."""
        for data in ALL_SCHOOLS:
            school = School(
                name=data["name"],
                state=data["state"],
                conference=data["conference"],
                division=data["division"],
                public=data["public"],
                enrollment=data["enrollment"],
                academic_tier=data["academic_tier"],
                tuition=data["tuition"],
            )
            self.schools.append(school)
    
    def search(self, query: str) -> List[School]:
        """Search schools by name."""
        query = query.lower()
        return [s for s in self.schools if query in s.name.lower()]
    
    def filter(
        self,
        divisions: Optional[List[str]] = None,
        conferences: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        regions: Optional[List[str]] = None,
        public_only: Optional[bool] = None,
        private_only: Optional[bool] = None,
        enrollment: Optional[List[str]] = None,
        academic_tier: Optional[List[int]] = None,
        tuition: Optional[List[str]] = None,
        warm_states_only: bool = False,
        favorites_only: bool = False,
    ) -> List[School]:
        """Filter schools by multiple criteria."""
        results = self.schools
        
        if divisions:
            results = [s for s in results if s.division in divisions]
        
        if conferences:
            results = [s for s in results if s.conference in conferences]
        
        if states:
            results = [s for s in results if s.state in states]
        
        if regions:
            results = [s for s in results if s.region in regions]
        
        if public_only:
            results = [s for s in results if s.public]
        
        if private_only:
            results = [s for s in results if not s.public]
        
        if enrollment:
            results = [s for s in results if s.enrollment in enrollment]
        
        if academic_tier:
            results = [s for s in results if s.academic_tier in academic_tier]
        
        if tuition:
            results = [s for s in results if s.tuition in tuition]
        
        if warm_states_only:
            results = [s for s in results if s.is_warm_state]
        
        if favorites_only:
            results = [s for s in results if s.name in self.favorites]
        
        return results
    
    def get_all_conferences(self) -> List[str]:
        """Get all unique conferences."""
        return sorted(set(s.conference for s in self.schools))
    
    def get_all_states(self) -> List[str]:
        """Get all states with schools."""
        return sorted(set(s.state for s in self.schools))
    
    def add_favorite(self, school_name: str):
        """Add school to favorites."""
        self.favorites.add(school_name)
    
    def remove_favorite(self, school_name: str):
        """Remove school from favorites."""
        self.favorites.discard(school_name)
    
    def get_favorites(self) -> List[School]:
        """Get all favorited schools."""
        return [s for s in self.schools if s.name in self.favorites]
    
    def to_list(self, schools: Optional[List[School]] = None) -> List[Dict]:
        """Convert schools to list of dicts."""
        if schools is None:
            schools = self.schools
        return [s.to_dict() for s in schools]


# ============================================================================
# NATURAL LANGUAGE FILTER PARSER
# ============================================================================

class NaturalLanguageFilter:
    """
    Parses natural language queries into structured filters.
    
    Examples:
    - "Show me D1 schools in the Southeast"
    - "Private schools in warm states"
    - "Small D3 schools with good academics"
    """
    
    DIVISION_KEYWORDS = {
        "d1": ["FBS", "FCS"],
        "d1-fbs": ["FBS"],
        "fbs": ["FBS"],
        "d1-fcs": ["FCS"],
        "fcs": ["FCS"],
        "d2": ["D2"],
        "d3": ["D3"],
        "division 1": ["FBS", "FCS"],
        "division 2": ["D2"],
        "division 3": ["D3"],
    }
    
    REGION_KEYWORDS = {
        "southeast": "Southeast",
        "south": "Southeast",
        "southern": "Southeast",
        "northeast": "Northeast",
        "east coast": "Northeast",
        "midwest": "Midwest",
        "midwestern": "Midwest",
        "southwest": "Southwest",
        "west": "West",
        "west coast": "West",
        "pacific": "West",
    }
    
    ACADEMIC_KEYWORDS = {
        "good academics": [1, 2],
        "great academics": [1],
        "top academics": [1],
        "elite academics": [1],
        "academic": [1, 2],
    }
    
    SIZE_KEYWORDS = {
        "small": ["small"],
        "medium": ["medium"],
        "large": ["large"],
        "big": ["large"],
    }
    
    CONFERENCE_KEYWORDS = {
        "sec": "SEC",
        "southeastern": "SEC",
        "big ten": "Big Ten",
        "big 10": "Big Ten",
        "b1g": "Big Ten",
        "big 12": "Big 12",
        "big twelve": "Big 12",
        "acc": "ACC",
        "atlantic coast": "ACC",
        "pac-12": "Pac-12",
        "pac 12": "Pac-12",
        "mountain west": "Mountain West",
        "mwc": "Mountain West",
        "sun belt": "Sun Belt",
        "sunbelt": "Sun Belt",
        "mac": "MAC",
        "mid-american": "MAC",
        "american": "American",
        "aac": "American",
        "c-usa": "C-USA",
        "conference usa": "C-USA",
        "ivy": "Ivy League",
        "ivy league": "Ivy League",
        "big sky": "Big Sky",
        "caa": "CAA",
        "colonial": "CAA",
        "missouri valley": "Missouri Valley",
        "mvfc": "Missouri Valley",
    }
    
    @classmethod
    def parse(cls, query: str) -> Dict[str, Any]:
        """
        Parse natural language query into filter parameters.
        
        Returns dict of filter parameters for SchoolDatabase.filter()
        """
        query = query.lower()
        filters = {}
        
        # Check divisions
        for keyword, divisions in cls.DIVISION_KEYWORDS.items():
            if keyword in query:
                filters['divisions'] = divisions
                break
        
        # Check regions
        for keyword, region in cls.REGION_KEYWORDS.items():
            if keyword in query:
                filters['regions'] = [region]
                break
        
        # Check public/private
        if "private" in query:
            filters['private_only'] = True
        elif "public" in query:
            filters['public_only'] = True
        
        # Check warm states
        if "warm" in query:
            filters['warm_states_only'] = True
        
        # Check academics
        for keyword, tiers in cls.ACADEMIC_KEYWORDS.items():
            if keyword in query:
                filters['academic_tier'] = tiers
                break
        
        # Check size
        for keyword, sizes in cls.SIZE_KEYWORDS.items():
            if keyword in query:
                filters['enrollment'] = sizes
                break
        
        # Check for specific states
        for abbrev, name in STATE_NAMES.items():
            if name.lower() in query or f" {abbrev.lower()} " in f" {query} ":
                if 'states' not in filters:
                    filters['states'] = []
                filters['states'].append(abbrev)
        
        # Check conferences
        for keyword, conf in cls.CONFERENCE_KEYWORDS.items():
            if keyword in query:
                filters['conferences'] = [conf]
                break
        
        return filters


# Singleton instance
_db_instance = None

def get_school_database() -> SchoolDatabase:
    """Get the school database singleton."""
    global _db_instance
    if _db_instance is None:
        _db_instance = SchoolDatabase()
    return _db_instance

