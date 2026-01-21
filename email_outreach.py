"""
email_outreach.py - Automated Email Outreach System
============================================================================
Automated email sending to college football coaches.

Features:
- Personalized email templates
- Rate limiting to avoid spam filters
- Tracking of sent emails
- Gmail SMTP integration
- Dry run mode for testing

Usage:
    python email_outreach.py              # Run outreach
    python email_outreach.py --dry-run    # Preview without sending
    python email_outreach.py --test       # Send test email

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

import os
import sys
import time
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from sheets.manager import SheetsManager, SheetsConfig


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class EmailConfig:
    """Email outreach configuration."""
    # SMTP Settings
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    email_address: str = ""  # Your email
    email_password: str = ""  # Gmail App Password (NOT regular password!)
    
    # Rate Limiting
    max_emails_per_run: int = 50
    delay_between_emails: float = 3.0  # Seconds
    
    # Athlete Info (customize these)
    athlete_name: str = "Keelan Underwood"
    graduation_year: str = "2026"
    height: str = "6'3\""
    weight: str = "295 lbs"
    positions: str = "Center, Guard, and Tackle"
    highlight_url: str = "https://www.hudl.com/your-profile"
    
    # Contact Info
    phone: str = ""
    parent_email: str = ""


# Email templates
RC_EMAIL_SUBJECT = "Recruiting Inquiry - {grad_year} OL - {athlete_name}"

RC_EMAIL_TEMPLATE = """Dear {title} {last_name},

My name is {athlete_name}, and I am a {grad_year} offensive lineman from [HIGH SCHOOL NAME] in [CITY, STATE].

I am very interested in {school}'s football program and would love the opportunity to be recruited by your team.

Here are my stats:
• Height: {height}
• Weight: {weight}
• Positions: {positions}
• GPA: [YOUR GPA]
• ACT/SAT: [YOUR SCORE]

You can view my highlight film here: {highlight_url}

I would greatly appreciate any information about {school}'s football program and what it takes to be recruited. I am committed to both my academic and athletic development.

Thank you for your time and consideration. I look forward to hearing from you.

Respectfully,

{athlete_name}
{phone}
{parent_email}
"""

OL_COACH_EMAIL_SUBJECT = "OL Recruiting Inquiry - {grad_year} - {athlete_name}"

OL_COACH_EMAIL_TEMPLATE = """Dear Coach {last_name},

My name is {athlete_name}, and I am a {grad_year} offensive lineman from [HIGH SCHOOL NAME] in [CITY, STATE]. I am reaching out because I am very interested in playing offensive line at {school}.

Physical Stats:
• Height: {height}
• Weight: {weight}
• Positions: {positions}

I've been working hard on my technique and strength, and I believe I have the work ethic and determination to contribute to {school}'s offensive line.

Here is my highlight film: {highlight_url}

I would be grateful for any feedback on my film or information about what you look for in offensive linemen. I am eager to learn and improve.

Thank you for taking the time to review my information. I hope to have the opportunity to discuss {school} football with you.

Best regards,

{athlete_name}
{phone}
{parent_email}
"""


# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('email_outreach.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# EMAIL FUNCTIONS
# ============================================================================

def extract_name_parts(full_name: str) -> tuple:
    """Extract first and last name from full name."""
    if not full_name:
        return "Coach", ""
    
    parts = full_name.strip().split()
    if len(parts) == 0:
        return "Coach", ""
    
    # Handle "Coach Smith" -> first="Coach", last="Smith"
    if parts[0].lower() == 'coach':
        if len(parts) > 1:
            return "Coach", parts[-1]
        return "Coach", ""
    
    # Regular name
    first = parts[0]
    last = parts[-1] if len(parts) > 1 else parts[0]
    
    return first, last


def personalize_email(
    template: str,
    coach_name: str,
    school_name: str,
    coach_type: str,
    config: EmailConfig
) -> str:
    """
    Personalize email template.
    
    Args:
        template: Email template string
        coach_name: Full name of coach
        school_name: Name of school
        coach_type: 'RC' or 'OL'
        config: Email configuration
        
    Returns:
        Personalized email body
    """
    first_name, last_name = extract_name_parts(coach_name)
    
    # Determine title
    title = "Coach" if coach_type == 'OL' else ""
    
    return template.format(
        title=title,
        first_name=first_name,
        last_name=last_name,
        athlete_name=config.athlete_name,
        grad_year=config.graduation_year,
        height=config.height,
        weight=config.weight,
        positions=config.positions,
        highlight_url=config.highlight_url,
        school=school_name,
        phone=config.phone or "[YOUR PHONE]",
        parent_email=config.parent_email or "[PARENT EMAIL]",
    )


def send_email(
    to_email: str,
    subject: str,
    body: str,
    config: EmailConfig
) -> bool:
    """
    Send an email using Gmail SMTP.
    
    Args:
        to_email: Recipient email address
        subject: Email subject
        body: Email body text
        config: Email configuration
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = config.email_address
        msg['To'] = to_email
        msg['Subject'] = subject
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Connect to SMTP server
        server = smtplib.SMTP(config.smtp_server, config.smtp_port)
        server.starttls()
        server.login(config.email_address, config.email_password)
        
        # Send
        server.send_message(msg)
        server.quit()
        
        logger.info(f"✅ Sent to {to_email}")
        return True
        
    except smtplib.SMTPAuthenticationError:
        logger.error("❌ Authentication failed!")
        logger.error("   Make sure you're using a Gmail App Password")
        logger.error("   Get one at: https://myaccount.google.com/apppasswords")
        return False
        
    except Exception as e:
        logger.error(f"❌ Failed to send to {to_email}: {e}")
        return False


# ============================================================================
# EMAIL OUTREACH CLASS
# ============================================================================

class EmailOutreach:
    """
    Automated email outreach manager.
    
    Sends personalized emails to coaches and tracks progress.
    """
    
    def __init__(
        self,
        config: Optional[EmailConfig] = None,
        sheets_config: Optional[SheetsConfig] = None
    ):
        """
        Initialize email outreach.
        
        Args:
            config: Email configuration
            sheets_config: Google Sheets configuration
        """
        self.config = config or EmailConfig()
        self.sheets_config = sheets_config or SheetsConfig()
        self.sheets = SheetsManager(self.sheets_config)
        
        # Stats
        self.sent_count = 0
        self.failed_count = 0
        self.skipped_count = 0
    
    def run(self, dry_run: bool = False) -> None:
        """
        Run email outreach.
        
        Args:
            dry_run: If True, preview emails without sending
        """
        self._print_header()
        
        # Validate config
        if not dry_run:
            if not self._validate_config():
                return
        
        # Connect to sheets
        logger.info("📊 Connecting to Google Sheets...")
        if not self.sheets.connect():
            logger.error("❌ Failed to connect to Google Sheets")
            return
        logger.info("✅ Connected\n")
        
        # Get coaches to email
        coaches = self._get_coaches_to_email()
        
        if not coaches:
            logger.info("✅ No coaches to email!")
            return
        
        logger.info(f"📋 Found {len(coaches)} coaches to email")
        
        if len(coaches) > self.config.max_emails_per_run:
            logger.info(f"⚠️ Limiting to {self.config.max_emails_per_run} this run")
            coaches = coaches[:self.config.max_emails_per_run]
        
        print()
        
        if dry_run:
            self._dry_run(coaches)
        else:
            self._send_emails(coaches)
        
        self._print_summary()
        self.sheets.disconnect()
    
    def _print_header(self) -> None:
        """Print welcome header."""
        print("\n" + "=" * 70)
        print("📧 AUTOMATED EMAIL OUTREACH")
        print("   Coach Contact System")
        print("=" * 70 + "\n")
    
    def _validate_config(self) -> bool:
        """Validate email configuration."""
        if not self.config.email_address or '@' not in self.config.email_address:
            logger.error("❌ Email address not configured!")
            self._print_setup_instructions()
            return False
        
        if not self.config.email_password or len(self.config.email_password) < 10:
            logger.error("❌ Email password not configured!")
            self._print_setup_instructions()
            return False
        
        return True
    
    def _print_setup_instructions(self) -> None:
        """Print Gmail setup instructions."""
        print("\n" + "=" * 70)
        print("📧 GMAIL SETUP REQUIRED")
        print("=" * 70)
        print("\n⚠️ You need to configure your email credentials!\n")
        print("Steps:")
        print("1. Go to: https://myaccount.google.com/apppasswords")
        print("2. Sign in to your Google Account")
        print("3. Select 'Mail' and 'Other (Custom name)'")
        print("4. Name it 'Coach Outreach' and click Generate")
        print("5. Copy the 16-character password")
        print("6. Update the EmailConfig in this script:")
        print("   - email_address = 'your.email@gmail.com'")
        print("   - email_password = 'xxxx xxxx xxxx xxxx'")
        print("\n" + "=" * 70 + "\n")
    
    def _get_coaches_to_email(self) -> List[Dict[str, Any]]:
        """Get list of coaches to email."""
        coaches = []
        
        try:
            data = self.sheets.get_all_data()
            if len(data) < 2:
                return []
            
            headers = data[0]
            rows = data[1:]
            
            # Find column indices
            col_indices = {}
            header_lower = [h.lower() for h in headers]
            
            for i, h in enumerate(header_lower):
                if 'school' in h:
                    col_indices['school'] = i
                elif 'rc email' in h:
                    col_indices['rc_email'] = i
                elif 'oc email' in h or 'ol email' in h:
                    col_indices['ol_email'] = i
                elif 'recruiting coordinator' in h and 'name' in h:
                    col_indices['rc_name'] = i
                elif 'oline' in h and 'coach' in h or 'ol' in h and 'name' in h:
                    col_indices['ol_name'] = i
                elif 'rc contacted' in h:
                    col_indices['rc_contacted'] = i
                elif 'ol contacted' in h:
                    col_indices['ol_contacted'] = i
            
            # Process rows
            for idx, row in enumerate(rows, start=2):
                def safe_get(key: str) -> str:
                    col = col_indices.get(key, -1)
                    if col >= 0 and col < len(row):
                        return row[col].strip()
                    return ""
                
                school = safe_get('school')
                
                # Check RC
                rc_email = safe_get('rc_email')
                rc_name = safe_get('rc_name')
                rc_contacted = safe_get('rc_contacted')
                
                if rc_email and '@' in rc_email and not rc_contacted:
                    coaches.append({
                        'type': 'RC',
                        'row': idx,
                        'school': school,
                        'name': rc_name or 'Coach',
                        'email': rc_email,
                        'col_contacted': col_indices.get('rc_contacted', -1) + 1,
                    })
                
                # Check OL
                ol_email = safe_get('ol_email')
                ol_name = safe_get('ol_name')
                ol_contacted = safe_get('ol_contacted')
                
                if ol_email and '@' in ol_email and not ol_contacted:
                    coaches.append({
                        'type': 'OL',
                        'row': idx,
                        'school': school,
                        'name': ol_name or 'Coach',
                        'email': ol_email,
                        'col_contacted': col_indices.get('ol_contacted', -1) + 1,
                    })
            
            return coaches
            
        except Exception as e:
            logger.error(f"Failed to get coaches: {e}")
            return []
    
    def _dry_run(self, coaches: List[Dict[str, Any]]) -> None:
        """Preview emails without sending."""
        logger.info("🔍 DRY RUN - Previewing emails (not sending)\n")
        
        for i, coach in enumerate(coaches[:5], 1):  # Show first 5
            print(f"\n{'='*60}")
            print(f"Email {i}: {coach['type']} at {coach['school']}")
            print(f"{'='*60}")
            print(f"To: {coach['email']}")
            
            # Get template
            if coach['type'] == 'RC':
                subject = RC_EMAIL_SUBJECT.format(
                    grad_year=self.config.graduation_year,
                    athlete_name=self.config.athlete_name
                )
                body = personalize_email(
                    RC_EMAIL_TEMPLATE,
                    coach['name'],
                    coach['school'],
                    'RC',
                    self.config
                )
            else:
                subject = OL_COACH_EMAIL_SUBJECT.format(
                    grad_year=self.config.graduation_year,
                    athlete_name=self.config.athlete_name
                )
                body = personalize_email(
                    OL_COACH_EMAIL_TEMPLATE,
                    coach['name'],
                    coach['school'],
                    'OL',
                    self.config
                )
            
            print(f"Subject: {subject}")
            print(f"\nBody:\n{'-'*40}")
            print(body[:500] + "..." if len(body) > 500 else body)
            print(f"{'-'*40}")
        
        if len(coaches) > 5:
            print(f"\n... and {len(coaches) - 5} more emails")
        
        print("\n✅ Dry run complete. No emails were sent.")
    
    def _send_emails(self, coaches: List[Dict[str, Any]]) -> None:
        """Send emails to coaches."""
        for i, coach in enumerate(coaches, 1):
            logger.info(f"\n[{i}/{len(coaches)}] 📤 {coach['name']} ({coach['type']}) at {coach['school']}")
            
            # Get template
            if coach['type'] == 'RC':
                subject = RC_EMAIL_SUBJECT.format(
                    grad_year=self.config.graduation_year,
                    athlete_name=self.config.athlete_name
                )
                body = personalize_email(
                    RC_EMAIL_TEMPLATE,
                    coach['name'],
                    coach['school'],
                    'RC',
                    self.config
                )
            else:
                subject = OL_COACH_EMAIL_SUBJECT.format(
                    grad_year=self.config.graduation_year,
                    athlete_name=self.config.athlete_name
                )
                body = personalize_email(
                    OL_COACH_EMAIL_TEMPLATE,
                    coach['name'],
                    coach['school'],
                    'OL',
                    self.config
                )
            
            # Send email
            success = send_email(coach['email'], subject, body, self.config)
            
            if success:
                self.sent_count += 1
                
                # Mark as contacted
                if coach['col_contacted'] > 0:
                    timestamp = datetime.now().strftime('%Y-%m-%d')
                    self.sheets.update_cell(
                        coach['row'],
                        coach['col_contacted'],
                        f'Emailed - {timestamp}'
                    )
            else:
                self.failed_count += 1
            
            # Delay between emails
            if i < len(coaches):
                logger.info(f"⏳ Waiting {self.config.delay_between_emails}s...")
                time.sleep(self.config.delay_between_emails)
    
    def _print_summary(self) -> None:
        """Print session summary."""
        print("\n" + "=" * 70)
        print("📊 EMAIL OUTREACH SUMMARY")
        print("=" * 70)
        print(f"Emails Sent:   {self.sent_count}")
        print(f"Failed:        {self.failed_count}")
        print(f"Remaining:     Check spreadsheet for more coaches")
        print("=" * 70 + "\n")


# ============================================================================
# TEST EMAIL
# ============================================================================

def send_test_email(config: EmailConfig, test_recipient: str) -> None:
    """Send a test email to verify configuration."""
    logger.info(f"\n📧 Sending test email to {test_recipient}...")
    
    subject = "Test Email - Coach Outreach System"
    body = f"""This is a test email from the Coach Outreach System.

If you received this, your email configuration is working correctly!

Configuration:
- SMTP Server: {config.smtp_server}
- From: {config.email_address}
- Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

You can now run the full email outreach.
"""
    
    success = send_email(test_recipient, subject, body, config)
    
    if success:
        logger.info("✅ Test email sent successfully!")
    else:
        logger.error("❌ Test email failed. Check your configuration.")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Email Outreach System')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview emails without sending')
    parser.add_argument('--test', type=str, metavar='EMAIL',
                       help='Send test email to specified address')
    parser.add_argument('--max', type=int, default=50,
                       help='Maximum emails to send')
    parser.add_argument('--email', type=str,
                       help='Your Gmail address')
    parser.add_argument('--password', type=str,
                       help='Your Gmail App Password')
    
    args = parser.parse_args()
    
    # Build config
    config = EmailConfig(
        max_emails_per_run=args.max,
    )
    
    if args.email:
        config.email_address = args.email
    if args.password:
        config.email_password = args.password
    
    # Handle test mode
    if args.test:
        if not config.email_address or not config.email_password:
            logger.error("❌ Email credentials required for test!")
            logger.error("   Use: --email your@gmail.com --password 'xxxx xxxx xxxx xxxx'")
            return
        send_test_email(config, args.test)
        return
    
    # Run outreach
    outreach = EmailOutreach(config)
    outreach.run(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
