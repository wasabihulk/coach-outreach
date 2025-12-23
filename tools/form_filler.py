"""
tools/form_filler.py - Recruiting Questionnaire Auto-Filler
============================================================================
Automatically fills out college recruiting questionnaires.

Features:
- Detects common form fields
- Auto-fills athlete information
- Handles various form types (embed, redirect, direct)
- Saves progress for multi-page forms
- Supports Sidearm, JumpForward, and custom forms

Usage:
    from tools.form_filler import FormFiller
    filler = FormFiller(athlete_info)
    filler.fill_form(url)

Author: Coach Outreach System
Version: 3.0.0
============================================================================
"""

import os
import sys
import re
import time
import json
import logging
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urljoin

sys.path.insert(0, str(Path(__file__).parent.parent))

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException,
    ElementNotInteractableException
)

logger = logging.getLogger(__name__)


# ============================================================================
# FIELD MAPPINGS
# ============================================================================

# Common field patterns and their corresponding athlete info keys
FIELD_PATTERNS = {
    # Name fields
    'first_name': [
        'first_name', 'firstname', 'first', 'fname', 'given_name',
        'player_first', 'athlete_first', 'prospect_first'
    ],
    'last_name': [
        'last_name', 'lastname', 'last', 'lname', 'surname', 'family_name',
        'player_last', 'athlete_last', 'prospect_last'
    ],
    'full_name': [
        'full_name', 'fullname', 'name', 'player_name', 'athlete_name',
        'prospect_name', 'your_name'
    ],
    
    # Contact
    'email': [
        'email', 'e-mail', 'email_address', 'player_email', 'prospect_email',
        'contact_email', 'your_email'
    ],
    'phone': [
        'phone', 'telephone', 'mobile', 'cell', 'phone_number', 'tel',
        'player_phone', 'contact_phone', 'home_phone', 'cell_phone'
    ],
    'address': [
        'address', 'street', 'street_address', 'address1', 'mailing_address'
    ],
    'city': [
        'city', 'town', 'home_city'
    ],
    'state': [
        'state', 'province', 'region', 'home_state'
    ],
    'zip': [
        'zip', 'zipcode', 'zip_code', 'postal', 'postal_code'
    ],
    
    # Academic
    'high_school': [
        'high_school', 'highschool', 'school', 'school_name', 'hs_name',
        'current_school', 'prep_school'
    ],
    'graduation_year': [
        'grad_year', 'graduation_year', 'class_year', 'year', 'class_of',
        'graduating_class', 'grad', 'expected_graduation'
    ],
    'gpa': [
        'gpa', 'grade_point', 'academic_gpa', 'cumulative_gpa', 'grades'
    ],
    'sat': [
        'sat', 'sat_score', 'sat_total'
    ],
    'act': [
        'act', 'act_score', 'act_composite'
    ],
    
    # Athletic - Physical
    'height': [
        'height', 'ht', 'player_height'
    ],
    'height_feet': [
        'height_feet', 'feet', 'ft', 'height_ft'
    ],
    'height_inches': [
        'height_inches', 'inches', 'in', 'height_in'
    ],
    'weight': [
        'weight', 'wt', 'player_weight', 'lbs', 'pounds'
    ],
    
    # Athletic - Football
    'position': [
        'position', 'pos', 'positions', 'primary_position', 'playing_position',
        'football_position'
    ],
    'jersey_number': [
        'jersey', 'number', 'jersey_number', 'uniform_number'
    ],
    'highlight_url': [
        'highlight', 'video', 'film', 'hudl', 'highlight_link', 'video_link',
        'film_link', 'hudl_link', 'youtube', 'highlight_url'
    ],
    
    # Parent/Guardian
    'parent_name': [
        'parent_name', 'guardian_name', 'parent', 'guardian', 
        'father_name', 'mother_name', 'parent_guardian'
    ],
    'parent_email': [
        'parent_email', 'guardian_email', 'parent_contact'
    ],
    'parent_phone': [
        'parent_phone', 'guardian_phone', 'parent_cell'
    ],
    
    # Coach
    'coach_name': [
        'coach_name', 'head_coach', 'hs_coach', 'high_school_coach',
        'hc_name', 'coach'
    ],
    'coach_email': [
        'coach_email', 'hc_email', 'coach_contact'
    ],
    'coach_phone': [
        'coach_phone', 'hc_phone'
    ],
}

# State abbreviations
STATES = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
    'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
    'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
    'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
    'wisconsin': 'WI', 'wyoming': 'WY'
}


# ============================================================================
# ATHLETE DATA
# ============================================================================

@dataclass
class AthleteFormData:
    """All athlete data needed for forms."""
    # Name
    first_name: str = ""
    last_name: str = ""
    
    # Contact
    email: str = ""
    phone: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    
    # Academic
    high_school: str = ""
    hs_city: str = ""
    hs_state: str = ""
    graduation_year: str = "2026"
    gpa: str = ""
    sat_score: str = ""
    act_score: str = ""
    
    # Athletic
    height: str = ""  # e.g., "6'3"
    weight: str = ""  # e.g., "295"
    positions: str = ""
    jersey_number: str = ""
    highlight_url: str = ""
    
    # 40 time, bench, squat, etc.
    forty_time: str = ""
    bench_press: str = ""
    squat: str = ""
    
    # Parent/Guardian
    parent_name: str = ""
    parent_email: str = ""
    parent_phone: str = ""
    
    # Coach
    coach_name: str = ""
    coach_email: str = ""
    coach_phone: str = ""
    
    # Twitter/Social
    twitter_handle: str = ""
    
    def get_height_parts(self) -> Tuple[str, str]:
        """Parse height into feet and inches."""
        if not self.height:
            return "", ""
        
        # Handle various formats: 6'3", 6-3, 6'3, 6 3
        match = re.match(r"(\d+)['\-\s](\d+)", self.height)
        if match:
            return match.group(1), match.group(2)
        
        return "", ""
    
    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()
    
    @property
    def state_abbrev(self) -> str:
        """Get state abbreviation."""
        if len(self.state) == 2:
            return self.state.upper()
        return STATES.get(self.state.lower(), self.state)


# ============================================================================
# FORM FILLER
# ============================================================================

class FormFiller:
    """
    Automatically fills recruiting questionnaires.
    
    Detects form fields and maps them to athlete data.
    Handles text inputs, selects, checkboxes, and radio buttons.
    """
    
    def __init__(self, athlete_data: AthleteFormData):
        self.athlete = athlete_data
        self.driver = None
        self.filled_count = 0
        self.skipped_count = 0
        self.errors = []
    
    def start_browser(self, headless: bool = False) -> bool:
        """Start browser for form filling."""
        try:
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            
            options = webdriver.ChromeOptions()
            
            if headless:
                options.add_argument('--headless')
            
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option('excludeSwitches', ['enable-automation'])
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            self.driver.implicitly_wait(5)
            
            return True
        except Exception as e:
            logger.error(f"Failed to start browser: {e}")
            return False
    
    def stop_browser(self):
        """Stop browser."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
    
    def fill_form(self, url: str, auto_submit: bool = False) -> Dict[str, Any]:
        """
        Fill a recruiting form.
        
        Args:
            url: Form URL
            auto_submit: Whether to submit automatically (default: False for review)
        
        Returns:
            Dict with filled, skipped, errors counts and field details
        """
        self.filled_count = 0
        self.skipped_count = 0
        self.errors = []
        filled_fields = []
        
        try:
            # Navigate to form
            self.driver.get(url)
            time.sleep(2)  # Wait for page load
            
            # Find all form inputs
            inputs = self._find_all_inputs()
            
            for element in inputs:
                try:
                    field_info = self._identify_field(element)
                    
                    if field_info:
                        value = self._get_value_for_field(field_info['type'])
                        
                        if value:
                            success = self._fill_field(element, value, field_info)
                            
                            if success:
                                self.filled_count += 1
                                filled_fields.append({
                                    'field': field_info['type'],
                                    'value': value[:50] + '...' if len(str(value)) > 50 else value
                                })
                            else:
                                self.skipped_count += 1
                        else:
                            self.skipped_count += 1
                    else:
                        self.skipped_count += 1
                        
                except Exception as e:
                    self.errors.append(str(e))
                    self.skipped_count += 1
            
            # Auto-submit if requested
            if auto_submit and self.filled_count > 0:
                self._submit_form()
            
            return {
                'success': True,
                'filled': self.filled_count,
                'skipped': self.skipped_count,
                'errors': len(self.errors),
                'fields': filled_fields,
                'url': url
            }
            
        except Exception as e:
            logger.error(f"Form fill error: {e}")
            return {
                'success': False,
                'error': str(e),
                'filled': self.filled_count,
                'skipped': self.skipped_count,
                'url': url
            }
    
    def _find_all_inputs(self) -> List:
        """Find all fillable form elements."""
        elements = []
        
        # Text inputs
        elements.extend(self.driver.find_elements(By.CSS_SELECTOR, 'input[type="text"]'))
        elements.extend(self.driver.find_elements(By.CSS_SELECTOR, 'input[type="email"]'))
        elements.extend(self.driver.find_elements(By.CSS_SELECTOR, 'input[type="tel"]'))
        elements.extend(self.driver.find_elements(By.CSS_SELECTOR, 'input[type="number"]'))
        elements.extend(self.driver.find_elements(By.CSS_SELECTOR, 'input[type="url"]'))
        elements.extend(self.driver.find_elements(By.CSS_SELECTOR, 'input:not([type])'))
        
        # Textareas
        elements.extend(self.driver.find_elements(By.TAG_NAME, 'textarea'))
        
        # Selects
        elements.extend(self.driver.find_elements(By.TAG_NAME, 'select'))
        
        return elements
    
    def _identify_field(self, element) -> Optional[Dict]:
        """
        Identify what type of field this is.
        
        Returns dict with 'type' key or None if unknown.
        """
        # Get identifying attributes
        name = (element.get_attribute('name') or '').lower()
        id_attr = (element.get_attribute('id') or '').lower()
        placeholder = (element.get_attribute('placeholder') or '').lower()
        label_text = self._get_label_text(element).lower()
        
        # Combine all text for matching
        all_text = f"{name} {id_attr} {placeholder} {label_text}"
        
        # Check against patterns
        for field_type, patterns in FIELD_PATTERNS.items():
            for pattern in patterns:
                if pattern in all_text:
                    return {
                        'type': field_type,
                        'name': name,
                        'id': id_attr,
                        'tag': element.tag_name
                    }
        
        return None
    
    def _get_label_text(self, element) -> str:
        """Get associated label text for an element."""
        try:
            # Try by 'for' attribute
            element_id = element.get_attribute('id')
            if element_id:
                labels = self.driver.find_elements(By.CSS_SELECTOR, f'label[for="{element_id}"]')
                if labels:
                    return labels[0].text
            
            # Try parent label
            parent = element.find_element(By.XPATH, './..')
            if parent.tag_name == 'label':
                return parent.text
            
            # Try preceding label
            prev = element.find_element(By.XPATH, './preceding-sibling::label[1]')
            if prev:
                return prev.text
                
        except:
            pass
        
        return ""
    
    def _get_value_for_field(self, field_type: str) -> Optional[str]:
        """Get athlete value for field type."""
        mapping = {
            'first_name': self.athlete.first_name,
            'last_name': self.athlete.last_name,
            'full_name': self.athlete.full_name,
            'email': self.athlete.email,
            'phone': self.athlete.phone,
            'address': self.athlete.address,
            'city': self.athlete.city,
            'state': self.athlete.state_abbrev,
            'zip': self.athlete.zip_code,
            'high_school': self.athlete.high_school,
            'graduation_year': self.athlete.graduation_year,
            'gpa': self.athlete.gpa,
            'sat': self.athlete.sat_score,
            'act': self.athlete.act_score,
            'height': self.athlete.height,
            'height_feet': self.athlete.get_height_parts()[0],
            'height_inches': self.athlete.get_height_parts()[1],
            'weight': self.athlete.weight.replace(' lbs', '').replace('lbs', ''),
            'position': self.athlete.positions,
            'jersey_number': self.athlete.jersey_number,
            'highlight_url': self.athlete.highlight_url,
            'parent_name': self.athlete.parent_name,
            'parent_email': self.athlete.parent_email,
            'parent_phone': self.athlete.parent_phone,
            'coach_name': self.athlete.coach_name,
            'coach_email': self.athlete.coach_email,
            'coach_phone': self.athlete.coach_phone,
        }
        
        return mapping.get(field_type)
    
    def _fill_field(self, element, value: str, field_info: Dict) -> bool:
        """Fill a form field with value."""
        try:
            tag = element.tag_name.lower()
            
            if tag == 'select':
                return self._fill_select(element, value)
            else:
                return self._fill_input(element, value)
                
        except ElementNotInteractableException:
            return False
        except Exception as e:
            self.errors.append(f"Fill error: {e}")
            return False
    
    def _fill_input(self, element, value: str) -> bool:
        """Fill a text input."""
        try:
            # Clear existing value
            element.clear()
            
            # Type new value
            element.send_keys(value)
            
            return True
        except:
            return False
    
    def _fill_select(self, element, value: str) -> bool:
        """Fill a select dropdown."""
        try:
            select = Select(element)
            
            # Try exact match first
            try:
                select.select_by_value(value)
                return True
            except:
                pass
            
            # Try visible text
            try:
                select.select_by_visible_text(value)
                return True
            except:
                pass
            
            # Try partial match
            for option in select.options:
                if value.lower() in option.text.lower():
                    select.select_by_visible_text(option.text)
                    return True
            
            return False
        except:
            return False
    
    def _submit_form(self):
        """Submit the form."""
        try:
            # Find submit button
            submit_buttons = self.driver.find_elements(
                By.CSS_SELECTOR, 
                'button[type="submit"], input[type="submit"], button:contains("Submit")'
            )
            
            if submit_buttons:
                submit_buttons[0].click()
                time.sleep(2)
        except:
            pass
    
    def get_form_preview(self, url: str) -> Dict[str, Any]:
        """
        Preview what fields would be filled without actually filling.
        
        Returns list of field mappings.
        """
        preview = []
        
        try:
            self.driver.get(url)
            time.sleep(2)
            
            inputs = self._find_all_inputs()
            
            for element in inputs:
                try:
                    field_info = self._identify_field(element)
                    
                    if field_info:
                        value = self._get_value_for_field(field_info['type'])
                        
                        preview.append({
                            'field_type': field_info['type'],
                            'name': field_info.get('name', ''),
                            'will_fill': bool(value),
                            'value': value[:30] + '...' if value and len(value) > 30 else value
                        })
                        
                except:
                    pass
            
            return {
                'success': True,
                'url': url,
                'fields': preview,
                'fillable': sum(1 for f in preview if f['will_fill']),
                'total': len(preview)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'url': url
            }


# ============================================================================
# BATCH PROCESSOR
# ============================================================================

class FormFillerBatch:
    """Process multiple recruiting forms."""
    
    def __init__(self, athlete_data: AthleteFormData):
        self.athlete = athlete_data
        self.filler = FormFiller(athlete_data)
        self.results = []
    
    def process_urls(
        self, 
        urls: List[str], 
        auto_submit: bool = False,
        callback = None
    ) -> List[Dict]:
        """
        Process multiple form URLs.
        
        Args:
            urls: List of form URLs
            auto_submit: Whether to auto-submit (default False)
            callback: Progress callback(event, data)
        """
        self.results = []
        
        if not self.filler.start_browser(headless=False):
            return [{'success': False, 'error': 'Failed to start browser'}]
        
        try:
            for i, url in enumerate(urls):
                if callback:
                    callback('processing', {
                        'current': i + 1,
                        'total': len(urls),
                        'url': url
                    })
                
                result = self.filler.fill_form(url, auto_submit=auto_submit)
                self.results.append(result)
                
                if callback:
                    callback('completed', result)
                
                # Delay between forms
                if i < len(urls) - 1:
                    time.sleep(3)
        
        finally:
            self.filler.stop_browser()
        
        return self.results
    
    def get_summary(self) -> Dict[str, int]:
        """Get summary of all results."""
        return {
            'total_forms': len(self.results),
            'successful': sum(1 for r in self.results if r.get('success')),
            'failed': sum(1 for r in self.results if not r.get('success')),
            'fields_filled': sum(r.get('filled', 0) for r in self.results),
        }


# ============================================================================
# CLI
# ============================================================================

def main():
    """CLI for form filler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Recruiting Form Filler')
    parser.add_argument('url', help='Form URL to fill')
    parser.add_argument('--preview', action='store_true', help='Preview without filling')
    parser.add_argument('--submit', action='store_true', help='Auto-submit form')
    args = parser.parse_args()
    
    # Example athlete data
    athlete = AthleteFormData(
        first_name="John",
        last_name="Smith",
        email="john.smith@email.com",
        phone="555-123-4567",
        graduation_year="2026",
        height="6'3",
        weight="295",
        positions="Center, Guard, Tackle",
        high_school="Lincoln High School",
        city="Dallas",
        state="TX",
    )
    
    filler = FormFiller(athlete)
    
    if not filler.start_browser():
        print("Failed to start browser")
        return
    
    try:
        if args.preview:
            result = filler.get_form_preview(args.url)
            print(f"\nForm Preview: {args.url}")
            print(f"Fillable fields: {result.get('fillable', 0)}/{result.get('total', 0)}")
            for field in result.get('fields', []):
                status = "✓" if field['will_fill'] else "✗"
                print(f"  {status} {field['field_type']}: {field.get('value', 'N/A')}")
        else:
            result = filler.fill_form(args.url, auto_submit=args.submit)
            print(f"\nForm filled: {result.get('filled', 0)} fields")
            print(f"Skipped: {result.get('skipped', 0)} fields")
            if result.get('errors'):
                print(f"Errors: {result.get('errors')}")
            
            if not args.submit:
                input("\nReview the form and press Enter to close browser...")
    
    finally:
        filler.stop_browser()


if __name__ == '__main__':
    main()
