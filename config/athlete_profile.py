"""
config/athlete_profile.py - Extended Athlete Profile
============================================================================
Complete athlete profile with all fields commonly found on recruiting forms.

Sections:
1. Basic Information
2. Contact Information
3. Academic Information
4. Athletic Information
5. Family Information
6. Additional Questions

Author: Coach Outreach System
Version: 3.3.0
============================================================================
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict
import json
import os

# ============================================================================
# PROFILE DATA STRUCTURE
# ============================================================================

@dataclass
class ExtendedAthleteProfile:
    """Complete athlete profile for recruiting forms."""
    
    # === BASIC INFORMATION ===
    first_name: str = ""
    last_name: str = ""
    preferred_name: str = ""  # Nickname
    date_of_birth: str = ""  # MM/DD/YYYY
    graduation_year: str = ""
    
    # === CONTACT INFORMATION ===
    email: str = ""
    phone: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    country: str = "USA"
    
    # === ACADEMIC INFORMATION ===
    high_school: str = ""
    high_school_city: str = ""
    high_school_state: str = ""
    high_school_coach: str = ""
    high_school_coach_phone: str = ""
    high_school_coach_email: str = ""
    
    gpa: str = ""
    gpa_scale: str = "4.0"  # 4.0, 5.0, 100
    weighted_gpa: str = ""
    class_rank: str = ""
    class_size: str = ""
    
    sat_score: str = ""
    sat_math: str = ""
    sat_reading: str = ""
    act_score: str = ""
    act_english: str = ""
    act_math: str = ""
    act_reading: str = ""
    act_science: str = ""
    
    intended_major: str = ""
    academic_interests: str = ""
    
    ncaa_id: str = ""
    ncaa_eligibility_center: bool = False
    core_gpa: str = ""  # NCAA core GPA
    
    # === ATHLETIC INFORMATION ===
    primary_position: str = ""
    secondary_position: str = ""
    positions: str = ""  # All positions comma separated
    
    height: str = ""  # e.g., "6'3" or "6-3"
    height_feet: str = ""
    height_inches: str = ""
    weight: str = ""  # e.g., "285"
    
    forty_yard: str = ""
    shuttle: str = ""
    vertical: str = ""
    broad_jump: str = ""
    bench_press: str = ""
    squat: str = ""
    deadlift: str = ""
    power_clean: str = ""
    
    jersey_number: str = ""
    years_playing: str = ""
    years_starting: str = ""
    
    club_team: str = ""
    club_coach: str = ""
    club_coach_phone: str = ""
    club_coach_email: str = ""
    
    highlight_url: str = ""
    hudl_url: str = ""
    maxpreps_url: str = ""
    
    twitter_handle: str = ""
    instagram_handle: str = ""
    
    # === FAMILY INFORMATION ===
    parent1_name: str = ""
    parent1_relationship: str = "Father"  # Father, Mother, Guardian
    parent1_phone: str = ""
    parent1_email: str = ""
    parent1_occupation: str = ""
    parent1_college: str = ""
    parent1_college_sport: str = ""
    
    parent2_name: str = ""
    parent2_relationship: str = "Mother"
    parent2_phone: str = ""
    parent2_email: str = ""
    parent2_occupation: str = ""
    parent2_college: str = ""
    parent2_college_sport: str = ""
    
    # === ADDITIONAL QUESTIONS ===
    why_interested: str = ""  # Why are you interested in our program?
    career_goals: str = ""
    describe_yourself: str = ""
    leadership_experience: str = ""
    community_service: str = ""
    other_sports: str = ""  # Other sports played
    honors_awards: str = ""
    
    # === PREFERENCES ===
    preferred_regions: List[str] = field(default_factory=list)
    preferred_divisions: List[str] = field(default_factory=list)
    preferred_school_size: str = ""  # Small, Medium, Large
    scholarship_required: bool = False
    willing_to_walk_on: bool = True
    
    # === TIMESTAMPS ===
    created_at: str = ""
    updated_at: str = ""
    
    @property
    def full_name(self) -> str:
        """Get full name."""
        return f"{self.first_name} {self.last_name}".strip()
    
    @property
    def city_state(self) -> str:
        """Get city, state formatted."""
        if self.city and self.state:
            return f"{self.city}, {self.state}"
        return self.city or self.state or ""
    
    @property
    def height_formatted(self) -> str:
        """Get height in standard format (6'3\")."""
        if self.height:
            return self.height
        if self.height_feet and self.height_inches:
            return f"{self.height_feet}'{self.height_inches}\""
        return ""
    
    def get_height_parts(self) -> tuple:
        """Parse height into (feet, inches)."""
        h = self.height or ""
        
        # Try various formats
        import re
        
        # 6'3" or 6'3
        m = re.match(r"(\d+)'(\d+)\"?", h)
        if m:
            return m.group(1), m.group(2)
        
        # 6-3
        m = re.match(r"(\d+)-(\d+)", h)
        if m:
            return m.group(1), m.group(2)
        
        # 6 3
        m = re.match(r"(\d+)\s+(\d+)", h)
        if m:
            return m.group(1), m.group(2)
        
        # Just use stored values
        if self.height_feet and self.height_inches:
            return self.height_feet, self.height_inches
        
        return "", ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ExtendedAthleteProfile':
        """Create from dictionary."""
        # Filter to only valid fields
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)
    
    def save(self, filepath: str = None):
        """Save profile to JSON file."""
        if filepath is None:
            filepath = os.path.expanduser("~/.coach_outreach/athlete_profile.json")
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
    
    @classmethod
    def load(cls, filepath: str = None) -> 'ExtendedAthleteProfile':
        """Load profile from JSON file."""
        if filepath is None:
            filepath = os.path.expanduser("~/.coach_outreach/athlete_profile.json")
        
        if not os.path.exists(filepath):
            return cls()
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        return cls.from_dict(data)


# ============================================================================
# FORM FIELD MAPPING
# ============================================================================

# Common form field names mapped to profile attributes
FORM_FIELD_MAP = {
    # Names
    'first_name': ['first_name', 'firstname', 'first', 'fname', 'given_name'],
    'last_name': ['last_name', 'lastname', 'last', 'lname', 'family_name', 'surname'],
    'full_name': ['full_name', 'fullname', 'name', 'player_name', 'athlete_name'],
    'preferred_name': ['preferred_name', 'nickname', 'preferred', 'goes_by'],
    
    # Contact
    'email': ['email', 'email_address', 'e_mail', 'player_email', 'athlete_email'],
    'phone': ['phone', 'phone_number', 'telephone', 'cell', 'mobile', 'cell_phone'],
    'address': ['address', 'street_address', 'street', 'address1', 'address_line_1'],
    'city': ['city', 'town'],
    'state': ['state', 'province', 'region'],
    'zip_code': ['zip', 'zip_code', 'zipcode', 'postal', 'postal_code'],
    
    # Academic
    'high_school': ['high_school', 'highschool', 'school', 'hs_name', 'current_school'],
    'graduation_year': ['graduation_year', 'grad_year', 'class_year', 'year', 'class_of'],
    'gpa': ['gpa', 'grade_point_average', 'cumulative_gpa'],
    'sat_score': ['sat', 'sat_score', 'sat_total'],
    'act_score': ['act', 'act_score', 'act_composite'],
    'intended_major': ['major', 'intended_major', 'area_of_study', 'field_of_study'],
    'ncaa_id': ['ncaa_id', 'ncaa_number', 'eligibility_center_id', 'clearinghouse_id'],
    
    # Athletic
    'primary_position': ['position', 'primary_position', 'pos', 'playing_position'],
    'height': ['height'],
    'height_feet': ['height_feet', 'feet', 'ft'],
    'height_inches': ['height_inches', 'inches', 'in'],
    'weight': ['weight', 'wt'],
    'forty_yard': ['forty', 'forty_yard', '40_yard', '40_time', 'forty_time'],
    'shuttle': ['shuttle', 'pro_agility', '5_10_5'],
    'vertical': ['vertical', 'vertical_jump', 'vert'],
    'bench_press': ['bench', 'bench_press', 'bench_max'],
    'squat': ['squat', 'squat_max', 'back_squat'],
    'highlight_url': ['highlight', 'highlight_url', 'film', 'video', 'hudl', 'video_link'],
    'jersey_number': ['jersey', 'jersey_number', 'number'],
    'twitter_handle': ['twitter', 'twitter_handle', 'twitter_username'],
    
    # Parent/Guardian
    'parent1_name': ['parent_name', 'guardian_name', 'father_name', 'mother_name', 'parent1_name'],
    'parent1_phone': ['parent_phone', 'guardian_phone', 'parent1_phone'],
    'parent1_email': ['parent_email', 'guardian_email', 'parent1_email'],
    
    # Coach
    'high_school_coach': ['coach_name', 'hs_coach', 'high_school_coach', 'head_coach'],
    'high_school_coach_phone': ['coach_phone', 'hs_coach_phone'],
    'high_school_coach_email': ['coach_email', 'hs_coach_email'],
}

def get_field_value(profile: ExtendedAthleteProfile, field_name: str) -> str:
    """
    Get the value for a form field name.
    
    Handles variations in field naming conventions.
    """
    field_lower = field_name.lower().replace(' ', '_').replace('-', '_')
    
    # Direct match
    if hasattr(profile, field_lower):
        val = getattr(profile, field_lower)
        if val:
            return str(val)
    
    # Check mapping
    for attr, variations in FORM_FIELD_MAP.items():
        if field_lower in variations or any(v in field_lower for v in variations):
            val = getattr(profile, attr, '')
            if val:
                return str(val)
    
    # Special cases
    if 'height' in field_lower and 'feet' in field_lower:
        feet, _ = profile.get_height_parts()
        return feet
    
    if 'height' in field_lower and 'inch' in field_lower:
        _, inches = profile.get_height_parts()
        return inches
    
    if 'name' in field_lower and 'full' in field_lower:
        return profile.full_name
    
    return ""
