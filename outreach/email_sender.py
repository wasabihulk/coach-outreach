"""
outreach/email_sender.py - Smart Email Sending with Deduplication
============================================================================
Sends personalized emails to coaches with intelligent deduplication.

Features:
- Detects when same person is both OL and RC (sends only one email)
- Tracks sent emails to prevent duplicates
- Customizable templates
- Rate limiting
- Detailed logging

Author: Coach Outreach System
Version: 3.0.0
============================================================================
"""

import os
import sys
import json
import time
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

# State file for tracking
STATE_DIR = Path.home() / '.coach_outreach'
SENT_EMAILS_FILE = STATE_DIR / 'sent_emails.json'
ANALYTICS_FILE = STATE_DIR / 'analytics.json'


# ============================================================================
# DEFAULT TEMPLATES
# ============================================================================

DEFAULT_RC_TEMPLATE = """Dear Coach {last_name},

My name is {athlete_name}, and I am a {graduation_year} offensive lineman from {high_school} in {city_state}.

I am very interested in {school}'s football program and would love the opportunity to be recruited by your team.

Here are my stats:
• Height: {height}
• Weight: {weight}
• Positions: {positions}
• GPA: {gpa}

You can view my highlight film here: {highlight_url}

I would greatly appreciate any information about {school}'s football program and what it takes to be recruited.

Thank you for your time and consideration.

Respectfully,
{athlete_name}
{phone}"""

DEFAULT_OL_TEMPLATE = """Dear Coach {last_name},

My name is {athlete_name}, and I'm a {graduation_year} offensive lineman from {high_school} in {city_state}.

I'm reaching out because I am very interested in playing for {school} and learning from your coaching.

My stats:
• Height: {height}
• Weight: {weight}
• Positions: {positions}

Here's my film: {highlight_url}

I would love the chance to speak with you about the program.

Thank you for your time.

Best regards,
{athlete_name}
{phone}"""

DEFAULT_DUAL_ROLE_TEMPLATE = """Dear Coach {last_name},

My name is {athlete_name}, and I am a {graduation_year} offensive lineman from {high_school} in {city_state}.

I noticed you serve as both the offensive line coach and recruiting coordinator at {school}, and I wanted to reach out about the opportunity to join your program.

Here are my stats:
• Height: {height}
• Weight: {weight}
• Positions: {positions}
• GPA: {gpa}

You can view my highlight film here: {highlight_url}

I would be grateful for any information about {school}'s football program and recruiting process.

Thank you for your time and consideration.

Respectfully,
{athlete_name}
{phone}"""


# ============================================================================
# TRACKING
# ============================================================================

class EmailTracker:
    """Tracks sent emails and analytics."""
    
    def __init__(self):
        self.sent_emails = {}  # email -> {date, school, type}
        self.daily_count = 0
        self.last_date = date.today().isoformat()
        self._load()
    
    def _load(self):
        """Load state from disk."""
        if SENT_EMAILS_FILE.exists():
            try:
                with open(SENT_EMAILS_FILE, 'r') as f:
                    data = json.load(f)
                    self.sent_emails = data.get('sent_emails', {})
                    self.daily_count = data.get('daily_count', 0)
                    self.last_date = data.get('last_date', date.today().isoformat())
            except:
                pass
        
        # Reset daily count if new day
        if self.last_date != date.today().isoformat():
            self.daily_count = 0
            self.last_date = date.today().isoformat()
    
    def save(self):
        """Save state to disk."""
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        try:
            with open(SENT_EMAILS_FILE, 'w') as f:
                json.dump({
                    'sent_emails': self.sent_emails,
                    'daily_count': self.daily_count,
                    'last_date': self.last_date,
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save email state: {e}")
    
    def has_sent_to(self, email: str) -> bool:
        """Check if we've sent to this email."""
        return email.lower() in self.sent_emails
    
    def mark_sent(self, email: str, school: str, coach_type: str):
        """Mark email as sent."""
        self.sent_emails[email.lower()] = {
            'date': datetime.now().isoformat(),
            'school': school,
            'type': coach_type,
        }
        self.daily_count += 1
        self.save()
    
    def get_daily_count(self) -> int:
        """Get number of emails sent today."""
        if self.last_date != date.today().isoformat():
            self.daily_count = 0
            self.last_date = date.today().isoformat()
            self.save()
        return self.daily_count
    
    def get_total_sent(self) -> int:
        """Get total emails ever sent."""
        return len(self.sent_emails)


class AnalyticsTracker:
    """Tracks outreach analytics."""
    
    def __init__(self):
        self.data = {
            'emails_sent': 0,
            'emails_by_date': {},
            'schools_contacted': set(),
            'responses_received': 0,
            'offers_received': 0,
            'schools_by_status': {
                'contacted': [],
                'responded': [],
                'interested': [],
                'offered': [],
            }
        }
        self._load()
    
    def _load(self):
        """Load analytics from disk."""
        if ANALYTICS_FILE.exists():
            try:
                with open(ANALYTICS_FILE, 'r') as f:
                    data = json.load(f)
                    self.data = data
                    # Convert schools_contacted back to set
                    if isinstance(self.data.get('schools_contacted'), list):
                        self.data['schools_contacted'] = set(self.data['schools_contacted'])
            except:
                pass
    
    def save(self):
        """Save analytics to disk."""
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        try:
            # Convert set to list for JSON
            save_data = self.data.copy()
            save_data['schools_contacted'] = list(self.data.get('schools_contacted', set()))
            
            with open(ANALYTICS_FILE, 'w') as f:
                json.dump(save_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save analytics: {e}")
    
    def record_email_sent(self, school: str, coach_type: str):
        """Record an email was sent."""
        self.data['emails_sent'] = self.data.get('emails_sent', 0) + 1
        
        # Track by date
        today = date.today().isoformat()
        if 'emails_by_date' not in self.data:
            self.data['emails_by_date'] = {}
        self.data['emails_by_date'][today] = self.data['emails_by_date'].get(today, 0) + 1
        
        # Track schools
        if 'schools_contacted' not in self.data:
            self.data['schools_contacted'] = set()
        self.data['schools_contacted'].add(school)
        
        self.save()
    
    def record_response(self, school: str):
        """Record a response received."""
        self.data['responses_received'] = self.data.get('responses_received', 0) + 1
        
        if 'schools_by_status' not in self.data:
            self.data['schools_by_status'] = {'contacted': [], 'responded': [], 'interested': [], 'offered': []}
        
        if school not in self.data['schools_by_status']['responded']:
            self.data['schools_by_status']['responded'].append(school)
        
        self.save()
    
    def record_offer(self, school: str):
        """Record an offer received."""
        self.data['offers_received'] = self.data.get('offers_received', 0) + 1
        
        if 'schools_by_status' not in self.data:
            self.data['schools_by_status'] = {'contacted': [], 'responded': [], 'interested': [], 'offered': []}
        
        if school not in self.data['schools_by_status']['offered']:
            self.data['schools_by_status']['offered'].append(school)
        
        self.save()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get analytics summary."""
        schools_contacted = self.data.get('schools_contacted', set())
        if isinstance(schools_contacted, list):
            schools_contacted = set(schools_contacted)
        
        return {
            'emails_sent': self.data.get('emails_sent', 0),
            'schools_contacted': len(schools_contacted),
            'responses_received': self.data.get('responses_received', 0),
            'offers_received': self.data.get('offers_received', 0),
            'response_rate': round(
                (self.data.get('responses_received', 0) / max(len(schools_contacted), 1)) * 100, 1
            ),
            'emails_by_date': self.data.get('emails_by_date', {}),
            'schools_by_status': self.data.get('schools_by_status', {}),
        }


# ============================================================================
# EMAIL SENDER
# ============================================================================

@dataclass
class EmailConfig:
    """Email configuration."""
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    email_address: str = ""
    app_password: str = ""
    max_per_day: int = 50
    delay_seconds: float = 5.0
    
    # Templates
    rc_template: str = DEFAULT_RC_TEMPLATE
    ol_template: str = DEFAULT_OL_TEMPLATE
    dual_role_template: str = DEFAULT_DUAL_ROLE_TEMPLATE
    
    rc_subject: str = "Recruiting Inquiry - {graduation_year} OL - {athlete_name}"
    ol_subject: str = "OL Recruiting Inquiry - {graduation_year} - {athlete_name}"
    dual_subject: str = "Recruiting Inquiry - {graduation_year} OL - {athlete_name}"
    
    # Enterprise features
    use_randomized_templates: bool = True  # Use enterprise random templates
    enable_followups: bool = True  # Create follow-up reminders


@dataclass
class AthleteInfo:
    """Athlete information for templates."""
    name: str = ""
    graduation_year: str = "2026"
    height: str = ""
    weight: str = ""
    positions: str = ""
    high_school: str = ""
    city: str = ""
    state: str = ""
    gpa: str = ""
    highlight_url: str = ""
    phone: str = ""
    email: str = ""
    
    @property
    def city_state(self) -> str:
        if self.city and self.state:
            return f"{self.city}, {self.state}"
        return self.city or self.state or ""


def test_email_connection(email_address: str, app_password: str) -> Tuple[bool, str]:
    """
    Test SMTP connection without sending an email.
    Returns (success, message)
    """
    try:
        connection = smtplib.SMTP("smtp.gmail.com", 587)
        connection.starttls()
        connection.login(email_address, app_password)
        connection.quit()
        return True, "Connection successful!"
    except smtplib.SMTPAuthenticationError:
        return False, "Authentication failed. Check your email and app password."
    except smtplib.SMTPException as e:
        return False, f"SMTP error: {str(e)}"
    except Exception as e:
        return False, f"Connection error: {str(e)}"


class SmartEmailSender:
    """
    Intelligent email sender with deduplication.
    
    Key features:
    - Detects when same person is OL and RC
    - Uses special template for dual-role coaches
    - Tracks all sent emails
    - Prevents duplicate sends
    - Records analytics
    """
    
    def __init__(self, config: EmailConfig, athlete: AthleteInfo):
        self.config = config
        self.athlete = athlete
        self.tracker = EmailTracker()
        self.analytics = AnalyticsTracker()
        self._connection = None
    
    def connect(self) -> bool:
        """Connect to SMTP server."""
        try:
            self._connection = smtplib.SMTP(self.config.smtp_server, self.config.smtp_port)
            self._connection.starttls()
            self._connection.login(self.config.email_address, self.config.app_password)
            logger.info("Connected to SMTP server")
            return True
        except Exception as e:
            logger.error(f"SMTP connection failed: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from SMTP server."""
        if self._connection:
            try:
                self._connection.quit()
            except:
                pass
            self._connection = None
    
    def get_coaches_to_email(self, sheet_data: List[List[str]], headers: List[str]) -> List[Dict]:
        """
        Get list of coaches to email with deduplication.
        
        Returns list of dicts with:
        - email: email address
        - name: coach name
        - last_name: last name only
        - school: school name
        - type: 'ol', 'rc', or 'dual' (if same person does both)
        """
        import re
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower().strip()
                for kw in keywords:
                    if kw in h_lower:
                        return i
            return -1
        
        def is_valid_email(email):
            """Validate email format"""
            if not email or not isinstance(email, str):
                return False
            email = email.strip()
            # Basic email validation - must have @ and domain
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return bool(re.match(pattern, email)) and len(email) < 100
        
        def clean_email(email):
            """Clean and validate a single email"""
            if not email:
                return ''
            email = str(email).strip().lower()
            # Remove any newlines or extra whitespace
            email = email.replace('\n', '').replace('\r', '').replace(' ', '')
            # If multiple emails separated by comma or semicolon, take first one
            for sep in [',', ';', '\n']:
                if sep in email:
                    email = email.split(sep)[0].strip()
            return email if is_valid_email(email) else ''
        
        def is_contacted(value):
            """Check if coach has been contacted - handles various formats"""
            if not value:
                return False
            v = str(value).strip().lower()
            # Consider contacted if: has any text like "yes", "followed", "sent", "done", "x", "true", or a date
            contacted_indicators = ['yes', 'followed', 'sent', 'done', 'x', 'true', 'emailed', 'contacted']
            return any(ind in v for ind in contacted_indicators) or bool(v and v not in ['no', 'false', ''])
        
        # Find columns - match user's actual headers
        # School column
        school_col = find_col(['school'])
        
        # Coach name columns
        ol_name_col = find_col(['oline coach', 'ol coach', 'o-line coach', 'offensive line coach', 'oline', 'position coach'])
        rc_name_col = find_col(['recruiting coordinator', 'recruiting coord', 'rc name'])
        
        # Email columns - be specific
        ol_email_col = find_col(['oc email', 'ol email', 'oline email', 'position coach email'])
        rc_email_col = find_col(['rc email', 'recruiting coordinator email', 'recruiting email'])
        
        # Contacted columns
        ol_contacted_col = find_col(['ol contacted', 'oc contacted', 'oline contacted', 'position contacted'])
        rc_contacted_col = find_col(['rc contacted', 'recruiting contacted'])
        
        # Log column detection for debugging
        logger.info(f"=== COLUMN DETECTION ===")
        logger.info(f"Headers: {headers}")
        logger.info(f"School col: {school_col} = '{headers[school_col] if school_col >= 0 else 'NOT FOUND'}'")
        logger.info(f"OL Name col: {ol_name_col} = '{headers[ol_name_col] if ol_name_col >= 0 else 'NOT FOUND'}'")
        logger.info(f"RC Name col: {rc_name_col} = '{headers[rc_name_col] if rc_name_col >= 0 else 'NOT FOUND'}'")
        logger.info(f"OL Email col: {ol_email_col} = '{headers[ol_email_col] if ol_email_col >= 0 else 'NOT FOUND'}'")
        logger.info(f"RC Email col: {rc_email_col} = '{headers[rc_email_col] if rc_email_col >= 0 else 'NOT FOUND'}'")
        logger.info(f"OL Contacted col: {ol_contacted_col} = '{headers[ol_contacted_col] if ol_contacted_col >= 0 else 'NOT FOUND'}'")
        logger.info(f"RC Contacted col: {rc_contacted_col} = '{headers[rc_contacted_col] if rc_contacted_col >= 0 else 'NOT FOUND'}'")
        
        coaches = []
        seen_emails = set()
        skipped_contacted = 0
        skipped_invalid = 0
        
        for row_idx, row in enumerate(sheet_data):
            try:
                school = row[school_col].strip() if school_col >= 0 and school_col < len(row) else ''
                ol_name = row[ol_name_col].strip() if ol_name_col >= 0 and ol_name_col < len(row) else ''
                rc_name = row[rc_name_col].strip() if rc_name_col >= 0 and rc_name_col < len(row) else ''
                ol_email_raw = row[ol_email_col] if ol_email_col >= 0 and ol_email_col < len(row) else ''
                rc_email_raw = row[rc_email_col] if rc_email_col >= 0 and rc_email_col < len(row) else ''
                ol_contacted_raw = row[ol_contacted_col] if ol_contacted_col >= 0 and ol_contacted_col < len(row) else ''
                rc_contacted_raw = row[rc_contacted_col] if rc_contacted_col >= 0 and rc_contacted_col < len(row) else ''
                
                # Skip if no school
                if not school:
                    continue
                
                # Clean and validate emails
                ol_email = clean_email(ol_email_raw)
                rc_email = clean_email(rc_email_raw)
                
                # Check contacted status
                ol_contacted = is_contacted(ol_contacted_raw)
                rc_contacted = is_contacted(rc_contacted_raw)
                
                # Log first few rows for debugging
                if row_idx < 3:
                    logger.info(f"Row {row_idx+2}: {school} | OL: {ol_email} (contacted: {ol_contacted}) | RC: {rc_email} (contacted: {rc_contacted})")
                
                # Log any invalid emails for debugging
                if ol_email_raw and not ol_email:
                    logger.warning(f"Row {row_idx+2}: Invalid OL email for {school}: '{ol_email_raw}'")
                    skipped_invalid += 1
                if rc_email_raw and not rc_email:
                    logger.warning(f"Row {row_idx+2}: Invalid RC email for {school}: '{rc_email_raw}'")
                    skipped_invalid += 1
                
                # Check if same person (same email for both roles)
                is_dual_role = ol_email and rc_email and ol_email == rc_email
                
                if is_dual_role:
                    # Same person does both - send one email with dual template
                    if ol_email and not ol_contacted and not rc_contacted:
                        if ol_email not in seen_emails and not self.tracker.has_sent_to(ol_email):
                            seen_emails.add(ol_email)
                            name = ol_name or rc_name
                            coaches.append({
                                'email': ol_email,
                                'name': name,
                                'last_name': name.split()[-1] if name else 'Coach',
                                'school': school,
                                'type': 'dual',
                                'row_idx': row_idx + 2,  # 1-indexed, skip header
                                'row_ol_contacted_col': ol_contacted_col + 1 if ol_contacted_col >= 0 else None,
                                'row_rc_contacted_col': rc_contacted_col + 1 if rc_contacted_col >= 0 else None,
                            })
                    else:
                        skipped_contacted += 1
                else:
                    # Different people or only one role
                    
                    # OL Coach
                    if ol_email and not ol_contacted:
                        if ol_email not in seen_emails and not self.tracker.has_sent_to(ol_email):
                            seen_emails.add(ol_email)
                            coaches.append({
                                'email': ol_email,
                                'name': ol_name,
                                'last_name': ol_name.split()[-1] if ol_name else 'Coach',
                                'school': school,
                                'type': 'ol',
                                'row_idx': row_idx + 2,
                                'contacted_col': ol_contacted_col + 1 if ol_contacted_col >= 0 else None,
                            })
                    elif ol_email and ol_contacted:
                        skipped_contacted += 1
                    
                    # RC
                    if rc_email and not rc_contacted:
                        if rc_email not in seen_emails and not self.tracker.has_sent_to(rc_email):
                            seen_emails.add(rc_email)
                            coaches.append({
                                'email': rc_email,
                                'name': rc_name,
                                'last_name': rc_name.split()[-1] if rc_name else 'Coach',
                                'school': school,
                                'type': 'rc',
                                'row_idx': row_idx + 2,
                                'contacted_col': rc_contacted_col + 1 if rc_contacted_col >= 0 else None,
                            })
                    elif rc_email and rc_contacted:
                        skipped_contacted += 1
                        
            except Exception as e:
                logger.error(f"Error processing row {row_idx+2}: {e}")
                continue
        
        logger.info(f"=== RESULTS ===")
        logger.info(f"Found {len(coaches)} valid coaches to email")
        logger.info(f"Skipped {skipped_contacted} already contacted")
        logger.info(f"Skipped {skipped_invalid} invalid emails")
        return coaches
    
    def prepare_email(self, coach: Dict) -> Tuple[str, str]:
        """
        Prepare email subject and body for a coach.
        Uses enterprise randomized templates if enabled.
        
        Returns (subject, body)
        """
        # Build template variables
        variables = {
            'coach_name': coach.get('last_name', 'Coach'),
            'school': coach['school'],
            'athlete_name': self.athlete.name,
            'position': self.athlete.positions or 'Athlete',
            'grad_year': self.athlete.graduation_year,
            'height': self.athlete.height or '',
            'weight': self.athlete.weight or '',
            'gpa': self.athlete.gpa or '',
            'hudl_link': self.athlete.highlight_url or '',
            'high_school': self.athlete.high_school or '',
            'city_state': self.athlete.city_state or '',
            'phone': self.athlete.phone or '',
            'email': self.athlete.email or '',
            # Legacy variables for backward compatibility
            'last_name': coach.get('last_name', 'Coach'),
            'graduation_year': self.athlete.graduation_year,
            'positions': self.athlete.positions or '',
            'highlight_url': self.athlete.highlight_url or '',
        }
        
        # Try enterprise randomized templates first
        if self.config.use_randomized_templates:
            try:
                from enterprise.templates import get_random_template_for_coach
                
                # Determine coach type for template selection
                coach_type = 'rc' if coach['type'] in ['rc', 'dual'] else 'oc'
                
                # Get random template (different per school)
                template = get_random_template_for_coach(coach_type, coach['school'])
                subject, body = template.render(variables)
                
                # Store template ID for tracking
                coach['template_id'] = template.id
                
                logger.debug(f"Using enterprise template: {template.id} for {coach['school']}")
                return subject, body
                
            except ImportError:
                logger.debug("Enterprise templates not available, using default")
            except Exception as e:
                logger.warning(f"Error with enterprise templates: {e}, using default")
        
        # Fall back to default templates
        if coach['type'] == 'dual':
            template = self.config.dual_role_template
            subject_template = self.config.dual_subject
        elif coach['type'] == 'ol':
            template = self.config.ol_template
            subject_template = self.config.ol_subject
        else:
            template = self.config.rc_template
            subject_template = self.config.rc_subject
        
        # Apply replacements (legacy format)
        subject = subject_template
        body = template
        
        for key, value in variables.items():
            subject = subject.replace('{' + key + '}', str(value))
            body = body.replace('{' + key + '}', str(value))
            body = body.replace('{' + key + '}', value)
        
        return subject, body
    
    def send_email(self, to_email: str, subject: str, body: str) -> Tuple[bool, str]:
        """
        Send an email.
        
        Returns (success, error_message)
        """
        try:
            if not self._connection:
                if not self.connect():
                    return False, "Not connected to SMTP"
            
            msg = MIMEMultipart()
            msg['From'] = self.config.email_address
            msg['To'] = to_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            self._connection.sendmail(
                self.config.email_address,
                to_email,
                msg.as_string()
            )
            
            return True, ""
            
        except smtplib.SMTPException as e:
            self.disconnect()
            return False, str(e)
        except Exception as e:
            return False, str(e)
    
    def send_to_coaches(
        self,
        coaches: List[Dict],
        sheet,  # gspread sheet object for updating contacted columns
        callback: Optional[Callable[[str, Dict], None]] = None
    ) -> Dict[str, int]:
        """
        Send emails to list of coaches.
        
        Args:
            coaches: List from get_coaches_to_email()
            sheet: gspread sheet to update contacted columns
            callback: Progress callback
        
        Returns:
            Dict with sent, errors, skipped counts
        """
        sent = 0
        errors = 0
        skipped = 0
        
        # Check daily limit
        daily_sent = self.tracker.get_daily_count()
        remaining = self.config.max_per_day - daily_sent
        
        if remaining <= 0:
            if callback:
                callback('error', {'message': f'Daily limit reached ({self.config.max_per_day})'})
            return {'sent': 0, 'errors': 0, 'skipped': len(coaches)}
        
        # Connect
        if not self.connect():
            if callback:
                callback('error', {'message': 'Failed to connect to email server'})
            return {'sent': 0, 'errors': 1, 'skipped': len(coaches)}
        
        import re
        def is_single_valid_email(email):
            """Ensure email is valid and contains only ONE email address"""
            if not email or not isinstance(email, str):
                return False
            email = email.strip()
            # Must not contain multiple @ symbols (indicates concatenated emails)
            if email.count('@') != 1:
                return False
            # Must not contain spaces or newlines
            if ' ' in email or '\n' in email or '\r' in email:
                return False
            # Basic format check
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return bool(re.match(pattern, email)) and len(email) < 100
        
        try:
            for i, coach in enumerate(coaches):
                # Check limit
                if sent >= remaining:
                    skipped += len(coaches) - i
                    break
                
                # CRITICAL: Validate email one more time before sending
                email = coach['email'].strip().lower()
                if not is_single_valid_email(email):
                    logger.error(f"INVALID EMAIL BLOCKED: '{email}' for {coach['school']}")
                    errors += 1
                    if callback:
                        callback('email_error', {
                            'school': coach['school'],
                            'email': email,
                            'error': f'Invalid email format: {email}'
                        })
                    continue
                
                # Prepare email
                subject, body = self.prepare_email(coach)
                
                # Send
                success, error = self.send_email(email, subject, body)
                
                if success:
                    sent += 1
                    
                    # Track
                    self.tracker.mark_sent(email, coach['school'], coach['type'])
                    self.analytics.record_email_sent(coach['school'], coach['type'])
                    
                    # Create follow-up reminders if enabled
                    if self.config.enable_followups:
                        try:
                            from enterprise.followups import get_followup_manager
                            followup_mgr = get_followup_manager()
                            followup_mgr.record_email_sent(
                                coach_name=coach.get('name', 'Coach'),
                                coach_email=email,
                                school=coach['school'],
                                coach_type=coach['type'],
                                subject=subject,
                                template_id=coach.get('template_id', '')
                            )
                            logger.info(f"Follow-ups scheduled for {coach['school']}")
                        except ImportError:
                            pass  # Enterprise module not available
                        except Exception as e:
                            logger.warning(f"Failed to create follow-up: {e}")
                    
                    # Update sheet - mark as contacted
                    if sheet and coach.get('row_idx'):
                        try:
                            today = date.today().strftime('%m/%d/%Y')
                            row_num = coach['row_idx']
                            if coach['type'] == 'dual':
                                if coach.get('row_ol_contacted_col'):
                                    sheet.update_cell(row_num, coach['row_ol_contacted_col'], today)
                                if coach.get('row_rc_contacted_col'):
                                    sheet.update_cell(row_num, coach['row_rc_contacted_col'], today)
                            else:
                                if coach.get('contacted_col'):
                                    sheet.update_cell(row_num, coach['contacted_col'], today)
                        except Exception as e:
                            logger.warning(f"Failed to update sheet: {e}")
                    
                    if callback:
                        callback('email_sent', {
                            'school': coach['school'],
                            'type': coach['type'],
                            'email': email,
                            'template': coach.get('template_id', 'default')
                        })
                else:
                    errors += 1
                    if callback:
                        callback('email_error', {
                            'school': coach['school'],
                            'email': email,
                            'error': error
                        })
                
                # Delay
                if i < len(coaches) - 1:
                    time.sleep(self.config.delay_seconds)
        
        finally:
            self.disconnect()
        
        return {'sent': sent, 'errors': errors, 'skipped': skipped}


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_email_tracker() -> EmailTracker:
    """Get email tracker instance."""
    return EmailTracker()

def get_analytics() -> AnalyticsTracker:
    """Get analytics tracker instance."""
    return AnalyticsTracker()


# ============================================================================
# GMAIL RESPONSE CHECKER
# ============================================================================

class GmailResponseChecker:
    """
    Check Gmail inbox for responses from coaches.
    Uses IMAP to read inbox and match against sent emails.
    """
    
    def __init__(self, email_address: str, app_password: str):
        self.email_address = email_address
        self.app_password = app_password
        self._connection = None
    
    def connect(self) -> bool:
        """Connect to Gmail IMAP"""
        try:
            import imaplib
            self._connection = imaplib.IMAP4_SSL('imap.gmail.com')
            self._connection.login(self.email_address, self.app_password)
            return True
        except Exception as e:
            logger.error(f"IMAP connection failed: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from IMAP"""
        if self._connection:
            try:
                self._connection.logout()
            except:
                pass
            self._connection = None
    
    def check_for_responses(self, coach_emails: List[str], since_days: int = 30) -> Dict[str, Dict]:
        """
        Check inbox for emails from the given coach addresses.
        
        Args:
            coach_emails: List of coach email addresses to check for
            since_days: Only check emails from the last N days
            
        Returns:
            Dict mapping coach_email -> {responded: bool, subject: str, date: str}
        """
        import imaplib
        import email
        from email.header import decode_header
        from datetime import datetime, timedelta
        
        results = {e.lower(): {'responded': False, 'subject': '', 'date': ''} for e in coach_emails}
        
        if not self.connect():
            return results
        
        try:
            # Select inbox
            self._connection.select('INBOX')
            
            # Search for emails since date
            since_date = (datetime.now() - timedelta(days=since_days)).strftime('%d-%b-%Y')
            _, message_nums = self._connection.search(None, f'(SINCE {since_date})')
            
            coach_emails_lower = {e.lower() for e in coach_emails}
            
            for num in message_nums[0].split():
                try:
                    _, msg_data = self._connection.fetch(num, '(RFC822)')
                    email_body = msg_data[0][1]
                    msg = email.message_from_bytes(email_body)
                    
                    # Get sender email
                    from_header = msg['From']
                    if '<' in from_header:
                        sender_email = from_header.split('<')[1].split('>')[0].lower()
                    else:
                        sender_email = from_header.lower()
                    
                    # Check if from a coach we emailed
                    if sender_email in coach_emails_lower:
                        # Get subject
                        subject = msg['Subject'] or ''
                        if isinstance(subject, bytes):
                            subject = subject.decode()
                        
                        # Decode if needed
                        decoded = decode_header(subject)
                        if decoded:
                            subject = decoded[0][0]
                            if isinstance(subject, bytes):
                                subject = subject.decode()
                        
                        # Get date
                        date_str = msg['Date'] or ''
                        
                        results[sender_email] = {
                            'responded': True,
                            'subject': str(subject)[:100],
                            'date': date_str[:30]
                        }
                        
                except Exception as e:
                    logger.debug(f"Error processing email: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Error checking responses: {e}")
        
        finally:
            self.disconnect()
        
        return results
    
    def get_response_count(self, coach_emails: List[str], since_days: int = 30) -> int:
        """Quick count of how many coaches have responded"""
        results = self.check_for_responses(coach_emails, since_days)
        return sum(1 for r in results.values() if r['responded'])


def check_gmail_responses(email_address: str, app_password: str, coach_emails: List[str]) -> Dict[str, Dict]:
    """
    Convenience function to check for responses.
    
    Usage:
        responses = check_gmail_responses('you@gmail.com', 'apppassword', ['coach1@school.edu', 'coach2@school.edu'])
        for email, info in responses.items():
            if info['responded']:
                print(f"{email} responded on {info['date']}: {info['subject']}")
    """
    checker = GmailResponseChecker(email_address, app_password)
    return checker.check_for_responses(coach_emails)
