"""
Enterprise Email Templates v2.0
============================================================================
Features:
- Toggle templates on/off
- Create custom user templates  
- Delete user templates (system templates protected)
- Auto-rotate through enabled templates OR manual selection
- 2 default templates per category (reduced from 4)
- Twitter DM templates included

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

import json
import uuid
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class EmailTemplate:
    """Email template with metadata"""
    id: str
    name: str
    template_type: str  # rc, ol, followup, dm
    category: str       # system or user
    subject: str
    body: str
    enabled: bool = True
    usage_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def render(self, variables: Dict[str, str]) -> Tuple[str, str]:
        """Render template with variables, returns (subject, body)"""
        subject = self.subject
        body = self.body
        
        # Support both {var} and {{var}} syntax
        for key, value in variables.items():
            for pattern in ["{" + key + "}", "{{" + key + "}}"]:
                subject = subject.replace(pattern, str(value) if value else "")
                body = body.replace(pattern, str(value) if value else "")
        
        # Clean up remaining placeholders
        import re
        subject = re.sub(r'\{\{?[^}]+\}?\}', '', subject)
        body = re.sub(r'\{\{?[^}]+\}?\}', '', body)
        
        return subject.strip(), body.strip()
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'name': self.name,
            'template_type': self.template_type,
            'category': self.category,
            'subject': self.subject,
            'body': self.body,
            'enabled': self.enabled,
            'usage_count': self.usage_count,
            'created_at': self.created_at
        }


# ============================================================================
# DEFAULT TEMPLATES - Professional, formal emails
# ============================================================================

RC_TEMPLATES = [
    EmailTemplate(
        id="rc_intro_1",
        name="Professional Introduction",
        template_type="rc",
        category="system",
        subject="Coach {coach_name} - 2026 OL from Florida",
        body="""Coach {coach_name},

I hope this email finds you well. My name is {athlete_name}, a 2026 offensive lineman from {high_school} in Florida.

I have been researching {school} and I am very interested in your program. I wanted to reach out and introduce myself.

I am {height}, {weight} lbs with a {gpa} GPA. Here is my film: {hudl_link}

I would appreciate the opportunity to learn more about {school} and what you look for in your offensive linemen. Please let me know if there is any additional information I can provide.

Thank you for your time,
{athlete_name}
{phone}"""
    ),
    EmailTemplate(
        id="rc_intro_2",
        name="Direct Introduction",
        template_type="rc",
        category="system",
        subject="2026 OL - {athlete_name} - Interested in {school}",
        body="""Coach {coach_name},

My name is {athlete_name}, a 2026 offensive lineman from {high_school} in Florida. {school} is a program I have been following and I wanted to introduce myself.

I am {height}, {weight} lbs with a {gpa} GPA. I take pride in my technique and being coachable.

Here is my film: {hudl_link}

I would greatly appreciate any feedback or information about your program. Thank you for your consideration.

Respectfully,
{athlete_name}
{phone}"""
    ),
]

OC_TEMPLATES = [
    EmailTemplate(
        id="ol_intro_1",
        name="OL Coach Introduction",
        template_type="ol",
        category="system",
        subject="Coach {coach_name} - 2026 OL Film",
        body="""Coach {coach_name},

I hope you are doing well. My name is {athlete_name}, a 2026 offensive lineman from {high_school} in Florida.

I wanted to reach out and make sure you had my film. I know offensive line coaches watch a lot of tape, and I would be grateful for any feedback you might have.

Film: {hudl_link}
Size: {height}, {weight} lbs

I finish every block and I am always looking to improve my technique. {school} is a program I am very interested in and I would appreciate the opportunity to learn more.

Thank you for your time,
{athlete_name}
{phone}"""
    ),
    EmailTemplate(
        id="ol_intro_2",
        name="Film-First Introduction",
        template_type="ol",
        category="system",
        subject="2026 OL Prospect - {athlete_name}",
        body="""Coach {coach_name},

My name is {athlete_name}, a 2026 offensive lineman from {high_school} in Florida. I wanted to get my film in front of you: {hudl_link}

I am {height}, {weight} lbs. I take pride in my physicality and technique, and I am committed to improving every day.

{school} is a program that stands out to me and I would appreciate any feedback on my film or information about your program.

Thank you for your consideration,
{athlete_name}
{phone}"""
    ),
]

FOLLOWUP_TEMPLATES = [
    EmailTemplate(
        id="followup_1",
        name="First Follow-Up",
        template_type="followup",
        category="system",
        subject="Following Up - {athlete_name} (2026 OL)",
        body="""Coach {coach_name},

I wanted to follow up on my previous email. I remain very interested in {school} and would appreciate the opportunity to connect when you have a chance.

Here is my film again: {hudl_link}

Please let me know if there is any additional information I can provide.

Thank you,
{athlete_name}
{phone}"""
    ),
    EmailTemplate(
        id="followup_2",
        name="Second Follow-Up",
        template_type="followup",
        category="system",
        subject="Checking In - {athlete_name}",
        body="""Coach {coach_name},

I wanted to check in one more time regarding my interest in {school}. I understand you are busy, but I would greatly appreciate any feedback or direction you could provide.

Film: {hudl_link}

Thank you for your time and consideration.

Respectfully,
{athlete_name}
{phone}"""
    ),
]

DM_TEMPLATES = [
    EmailTemplate(
        id="dm_casual",
        name="Professional DM",
        template_type="dm",
        category="system",
        subject="",
        body="""Coach, I am {athlete_name}, a 2026 OL from {high_school} in Florida ({height}/{weight}). I am very interested in {school}. Here is my film: {hudl_link} - I would appreciate the opportunity to connect."""
    ),
    EmailTemplate(
        id="dm_with_question",
        name="DM with Interest",
        template_type="dm",
        category="system",
        subject="",
        body="""Coach, my name is {athlete_name} - 2026 OL, {height}/{weight} from Florida. {school} is a program I am very interested in. Film: {hudl_link} - Thank you for your time."""
    ),
]

# Combined for backward compat
OL_TEMPLATES = OC_TEMPLATES


class TemplateManager:
    """Manages templates - NO LOCAL FILES, rotation stored in Google Sheets."""

    def __init__(self, data_dir: Path = None):
        # NO LOCAL STORAGE - Everything in Google Sheets or code
        self.templates: Dict[str, EmailTemplate] = {}
        self.auto_rotate = True
        self._rotation_index: Dict[str, int] = {}

        self._load()
        # Load rotation index from Google Sheets for Railway persistence
        self._load_rotation_from_sheets()

    def _load_rotation_from_sheets(self):
        """Load rotation index from Google Sheets Settings tab for Railway persistence."""
        try:
            import gspread
            import os

            google_creds = os.environ.get('GOOGLE_CREDENTIALS', '')
            if not google_creds:
                return

            import json
            import tempfile
            from google.oauth2.service_account import Credentials

            creds_str = google_creds.strip()
            if creds_str.startswith('"') and creds_str.endswith('"'):
                creds_str = creds_str[1:-1]
            creds_str = creds_str.replace('\\\\n', '\\n')

            temp_creds = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            temp_creds.write(creds_str)
            temp_creds.close()

            scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file(temp_creds.name, scopes=scope)
            client = gspread.authorize(creds)

            spreadsheet = client.open('bardeen')
            try:
                settings_sheet = spreadsheet.worksheet('Settings')
                all_data = settings_sheet.get_all_values()
                for row in all_data[1:]:
                    if len(row) >= 2:
                        if row[0] == 'template_rotation_rc':
                            self._rotation_index['rc'] = int(row[1]) if row[1] else 0
                        elif row[0] == 'template_rotation_ol':
                            self._rotation_index['ol'] = int(row[1]) if row[1] else 0
                logger.info(f"Loaded rotation index from sheet: {self._rotation_index}")
            except Exception as e:
                logger.warning(f"Settings sheet not found, will create on first save: {e}")
        except Exception as e:
            logger.warning(f"Could not load rotation from sheets: {e}")

    def _save_rotation_to_sheets(self):
        """Save rotation index to Google Sheets Settings tab for Railway persistence."""
        try:
            import gspread
            import os

            google_creds = os.environ.get('GOOGLE_CREDENTIALS', '')
            if not google_creds:
                return

            import json
            import tempfile
            from google.oauth2.service_account import Credentials

            creds_str = google_creds.strip()
            if creds_str.startswith('"') and creds_str.endswith('"'):
                creds_str = creds_str[1:-1]
            creds_str = creds_str.replace('\\\\n', '\\n')

            temp_creds = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            temp_creds.write(creds_str)
            temp_creds.close()

            scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file(temp_creds.name, scopes=scope)
            client = gspread.authorize(creds)

            spreadsheet = client.open('bardeen')
            try:
                settings_sheet = spreadsheet.worksheet('Settings')
            except:
                settings_sheet = spreadsheet.add_worksheet(title='Settings', rows=10, cols=2)
                settings_sheet.update('A1:B1', [['Key', 'Value']])
                settings_sheet.update('A2:B2', [['last_send_date', '']])
                settings_sheet.update('A3:B3', [['template_rotation_rc', '0']])
                settings_sheet.update('A4:B4', [['template_rotation_ol', '0']])

            # Update rotation values
            all_data = settings_sheet.get_all_values()
            for i, row in enumerate(all_data):
                if len(row) >= 1:
                    if row[0] == 'template_rotation_rc':
                        settings_sheet.update_cell(i + 1, 2, str(self._rotation_index.get('rc', 0)))
                    elif row[0] == 'template_rotation_ol':
                        settings_sheet.update_cell(i + 1, 2, str(self._rotation_index.get('ol', 0)))
            logger.info(f"Saved rotation index to sheet: {self._rotation_index}")
        except Exception as e:
            logger.warning(f"Could not save rotation to sheets: {e}")
    
    def _load(self):
        """Load templates - system templates from code, NO LOCAL FILES."""
        # Load system templates from code (hardcoded)
        for t in RC_TEMPLATES + OC_TEMPLATES + FOLLOWUP_TEMPLATES + DM_TEMPLATES:
            self.templates[t.id] = t

        # All templates are enabled by default - no local file storage

    def _save(self):
        """Save template state - NO LOCAL FILES, rotation saved to Sheets."""
        # Template definitions are in code, rotation is saved to Sheets
        # No local file operations needed
        pass

    def get_templates_by_type(self, template_type: str, enabled_only: bool = False) -> List[EmailTemplate]:
        """Get all templates of a type."""
        templates = [t for t in self.templates.values() if t.template_type == template_type]
        if enabled_only:
            templates = [t for t in templates if t.enabled]
        return templates
    
    def get_template(self, template_id: str) -> Optional[EmailTemplate]:
        """Get template by ID."""
        return self.templates.get(template_id)
    
    def toggle_template(self, template_id: str, enabled: bool) -> bool:
        """Enable or disable a template."""
        if template_id in self.templates:
            self.templates[template_id].enabled = enabled
            self._save()
            return True
        return False
    
    def create_template(self, name: str, template_type: str, subject: str, body: str) -> EmailTemplate:
        """Create a new user template."""
        template = EmailTemplate(
            id=f"user_{uuid.uuid4().hex[:8]}",
            name=name,
            template_type=template_type,
            category='user',
            subject=subject,
            body=body,
            enabled=True
        )
        self.templates[template.id] = template
        self._save()
        return template
    
    def update_template(self, template_id: str, name: str = None, subject: str = None, 
                        body: str = None) -> bool:
        """Update any template (user or system)."""
        template = self.templates.get(template_id)
        if not template:
            return False
        
        if name: template.name = name
        if subject is not None: template.subject = subject
        if body is not None: template.body = body
        self._save()
        return True
    
    def delete_template(self, template_id: str) -> bool:
        """Delete a user template. Cannot delete system templates."""
        template = self.templates.get(template_id)
        if template and template.category == 'user':
            del self.templates[template_id]
            self._save()
            return True
        return False
    
    def set_auto_rotate(self, enabled: bool):
        """Set auto-rotate mode."""
        self.auto_rotate = enabled
        self._save()
    
    def get_next_template(self, template_type: str, school: str = None) -> Optional[EmailTemplate]:
        """Get next template for auto-rotation."""
        templates = self.get_templates_by_type(template_type, enabled_only=True)
        if not templates:
            return None

        if not self.auto_rotate:
            return random.choice(templates)

        # Rotate globally across all emails of this type
        key = template_type
        idx = self._rotation_index.get(key, 0)

        template = templates[idx % len(templates)]
        self._rotation_index[key] = idx + 1

        template.usage_count += 1
        self._save()

        # Persist rotation to Google Sheets for Railway
        self._save_rotation_to_sheets()

        return template
    
    def get_all_templates(self) -> List[dict]:
        """Get all templates as dicts for API."""
        return [t.to_dict() for t in self.templates.values()]
    
    def get_followup_template(self, followup_number: int = 1) -> Optional[EmailTemplate]:
        """Get follow-up template by number (1 or 2)."""
        templates = self.get_templates_by_type('followup', enabled_only=True)
        if not templates:
            return None
        idx = min(followup_number - 1, len(templates) - 1)
        return templates[idx] if idx >= 0 else templates[0]
    
    def reset_to_defaults(self):
        """Remove all user templates and reset enabled states."""
        self.templates = {tid: t for tid, t in self.templates.items() if t.category == 'system'}
        for t in self.templates.values():
            t.enabled = True
            t.usage_count = 0
        self._rotation_index.clear()
        self._save()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_manager = None

def get_template_manager() -> TemplateManager:
    """Get singleton template manager."""
    global _manager
    if _manager is None:
        _manager = TemplateManager()
    return _manager


def get_random_template_for_coach(coach_type: str, school: str = None) -> Optional[EmailTemplate]:
    """Get a template for a coach type."""
    manager = get_template_manager()
    
    type_map = {
        'rc': 'rc', 'recruiting': 'rc', 'recruiting coordinator': 'rc',
        'ol': 'ol', 'oc': 'ol', 'oline': 'ol', 'offensive line': 'ol', 'position': 'ol'
    }
    template_type = type_map.get(coach_type.lower(), 'ol')
    
    return manager.get_next_template(template_type, school)


def render_email(coach_type: str, variables: Dict[str, str], school: str = None, 
                 template_id: str = None) -> Optional[Dict[str, str]]:
    """Render an email template. Returns {'subject': ..., 'body': ...} or None"""
    manager = get_template_manager()
    
    if template_id:
        template = manager.get_template(template_id)
    else:
        template = get_random_template_for_coach(coach_type, school)
    
    if not template:
        return None
    
    subject, body = template.render(variables)
    return {'subject': subject, 'body': body, 'template_id': template.id}


def render_dm(variables: Dict[str, str], template_id: str = None) -> Optional[str]:
    """Render a DM template. Returns body text only."""
    manager = get_template_manager()
    
    if template_id:
        template = manager.get_template(template_id)
    else:
        template = manager.get_next_template('dm')
    
    if not template:
        return None
    
    _, body = template.render(variables)
    return body
