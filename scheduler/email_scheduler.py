"""
scheduler/email_scheduler.py - Automated Email Scheduling System
============================================================================
Schedules and sends daily emails to coaches automatically.

Features:
- Daily scheduling with configurable time
- Rate limiting to avoid spam filters
- Tracks sent emails to prevent duplicates
- Retry logic for failed sends
- Detailed logging and status reporting

Usage:
    from scheduler.email_scheduler import EmailScheduler
    scheduler = EmailScheduler()
    scheduler.start()  # Runs in background

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
import threading
import schedule
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sheets.manager import SheetsManager

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class EmailSchedulerConfig:
    """Configuration for email scheduling."""
    # SMTP Settings
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    email_address: str = ""
    email_password: str = ""  # Gmail App Password
    
    # Schedule
    send_time: str = "09:00"  # 24-hour format
    enabled: bool = True
    
    # Rate Limiting
    max_emails_per_day: int = 50
    delay_between_emails: float = 5.0  # seconds
    
    # Retry
    max_retries: int = 3
    retry_delay: float = 60.0  # seconds
    
    # Athlete Info (for templates)
    athlete_name: str = "Keelan Underwood"
    graduation_year: str = "2026"
    height: str = "6'3\""
    weight: str = "295 lbs"
    positions: str = "Center, Guard, and Tackle"
    highlight_url: str = ""
    phone: str = ""
    high_school: str = ""
    city_state: str = ""
    gpa: str = ""
    
    # Persistence
    state_file: str = "email_scheduler_state.json"


# ============================================================================
# EMAIL TEMPLATES
# ============================================================================

RC_SUBJECT = "Recruiting Inquiry - {grad_year} OL - {athlete_name}"

RC_TEMPLATE = """Dear Coach {last_name},

My name is {athlete_name}, and I am a {grad_year} offensive lineman from {high_school} in {city_state}.

I am very interested in {school}'s football program and would love the opportunity to be recruited by your team.

Here are my stats:
• Height: {height}
• Weight: {weight}
• Positions: {positions}
• GPA: {gpa}

You can view my highlight film here: {highlight_url}

I would greatly appreciate any information about {school}'s football program and what it takes to be recruited. I am committed to both my academic and athletic development.

Thank you for your time and consideration. I look forward to hearing from you.

Respectfully,

{athlete_name}
{phone}
"""

OL_SUBJECT = "OL Recruiting Inquiry - {grad_year} - {athlete_name}"

OL_TEMPLATE = """Dear Coach {last_name},

My name is {athlete_name}, and I'm a {grad_year} offensive lineman from {high_school} in {city_state}.

I'm reaching out because I am very interested in playing for {school} and learning from your offensive line coaching.

My stats:
• Height: {height}
• Weight: {weight}
• Positions: {positions}

Here's my film: {highlight_url}

I would love the chance to speak with you about the program and how I can contribute to {school} football.

Thank you for your time.

Best regards,

{athlete_name}
{phone}
"""


# ============================================================================
# STATE MANAGER
# ============================================================================

class SchedulerState:
    """Manages persistent state for the scheduler."""
    
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.sent_emails = {}  # email -> timestamp
        self.daily_count = 0
        self.last_reset = datetime.now().date().isoformat()
        self.errors = []
        self._load()
    
    def _load(self):
        """Load state from file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    self.sent_emails = data.get('sent_emails', {})
                    self.daily_count = data.get('daily_count', 0)
                    self.last_reset = data.get('last_reset', datetime.now().date().isoformat())
                    self.errors = data.get('errors', [])[-100:]  # Keep last 100
            except:
                pass
    
    def save(self):
        """Save state to file."""
        try:
            with open(self.state_file, 'w') as f:
                json.dump({
                    'sent_emails': self.sent_emails,
                    'daily_count': self.daily_count,
                    'last_reset': self.last_reset,
                    'errors': self.errors[-100:],
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def reset_daily_if_needed(self):
        """Reset daily count if it's a new day."""
        today = datetime.now().date().isoformat()
        if today != self.last_reset:
            self.daily_count = 0
            self.last_reset = today
            self.save()
    
    def has_sent_to(self, email: str) -> bool:
        """Check if we've already sent to this email."""
        return email.lower() in self.sent_emails
    
    def mark_sent(self, email: str):
        """Mark an email as sent."""
        self.sent_emails[email.lower()] = datetime.now().isoformat()
        self.daily_count += 1
        self.save()
    
    def add_error(self, email: str, error: str):
        """Record an error."""
        self.errors.append({
            'email': email,
            'error': error,
            'time': datetime.now().isoformat()
        })
        self.save()


# ============================================================================
# EMAIL SENDER
# ============================================================================

class EmailSender:
    """Handles SMTP email sending."""
    
    def __init__(self, config: EmailSchedulerConfig):
        self.config = config
        self._connection = None
    
    def connect(self) -> bool:
        """Connect to SMTP server."""
        try:
            self._connection = smtplib.SMTP(self.config.smtp_server, self.config.smtp_port)
            self._connection.starttls()
            self._connection.login(self.config.email_address, self.config.email_password)
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
    
    def send(
        self,
        to_email: str,
        subject: str,
        body: str,
        reply_to: str = None
    ) -> Tuple[bool, str]:
        """
        Send an email.
        
        Returns:
            Tuple of (success, error_message)
        """
        try:
            if not self._connection:
                if not self.connect():
                    return False, "Not connected to SMTP"
            
            msg = MIMEMultipart()
            msg['From'] = self.config.email_address
            msg['To'] = to_email
            msg['Subject'] = subject
            if reply_to:
                msg['Reply-To'] = reply_to
            
            msg.attach(MIMEText(body, 'plain'))
            
            self._connection.sendmail(
                self.config.email_address,
                to_email,
                msg.as_string()
            )
            
            return True, ""
            
        except smtplib.SMTPException as e:
            error = str(e)
            logger.error(f"SMTP error sending to {to_email}: {error}")
            # Try to reconnect for next send
            self.disconnect()
            return False, error
        except Exception as e:
            error = str(e)
            logger.error(f"Error sending to {to_email}: {error}")
            return False, error


# ============================================================================
# EMAIL SCHEDULER
# ============================================================================

class EmailScheduler:
    """
    Automated email scheduling system.
    
    Sends emails to coaches daily at a configured time.
    Tracks sent emails to prevent duplicates.
    Respects rate limits.
    """
    
    def __init__(self, config: Optional[EmailSchedulerConfig] = None):
        self.config = config or EmailSchedulerConfig()
        self.state = SchedulerState(self.config.state_file)
        self.sender = EmailSender(self.config)
        self.sheets = SheetsManager()
        
        self._running = False
        self._thread = None
        self._callbacks = []
        
        self.logger = logging.getLogger(__name__)
    
    def add_callback(self, callback: Callable[[str, Dict], None]):
        """Add a callback for events."""
        self._callbacks.append(callback)
    
    def start(self) -> bool:
        """Start the scheduler in background."""
        if self._running:
            return True
        
        self._running = True
        
        # Schedule daily job
        schedule.every().day.at(self.config.send_time).do(self._run_daily_job)
        
        # Start background thread
        self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._thread.start()
        
        self._emit('started', {'time': self.config.send_time})
        self.logger.info(f"Scheduler started, will run daily at {self.config.send_time}")
        
        return True
    
    def stop(self):
        """Stop the scheduler."""
        self._running = False
        schedule.clear()
        self.sender.disconnect()
        self._emit('stopped', {})
        self.logger.info("Scheduler stopped")
    
    def run_now(self) -> Dict[str, int]:
        """Run email sending immediately (for testing)."""
        return self._run_daily_job()
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        self.state.reset_daily_if_needed()
        
        return {
            'enabled': self.config.enabled,
            'running': self._running,
            'send_time': self.config.send_time,
            'daily_sent': self.state.daily_count,
            'daily_limit': self.config.max_emails_per_day,
            'total_sent': len(self.state.sent_emails),
            'recent_errors': self.state.errors[-5:],
            'next_run': schedule.next_run().isoformat() if schedule.jobs else None,
        }
    
    def get_pending_emails(self) -> List[Dict]:
        """Get coaches who haven't been emailed yet."""
        if not self.sheets.connect():
            return []
        
        try:
            data = self.sheets.get_all_data()
            if len(data) < 2:
                return []
            
            headers = data[0]
            rows = data[1:]
            
            def find_col(keywords):
                for i, h in enumerate(headers):
                    h_lower = h.lower()
                    for kw in keywords:
                        if kw in h_lower:
                            return i
                return -1
            
            school_col = find_col(['school'])
            ol_name_col = find_col(['oline', 'ol coach'])
            rc_name_col = find_col(['recruiting'])
            ol_email_col = find_col(['oc email', 'ol email'])
            rc_email_col = find_col(['rc email'])
            ol_contacted_col = find_col(['ol contacted'])
            rc_contacted_col = find_col(['rc contacted'])
            
            pending = []
            
            for row_idx, row in enumerate(rows):
                school = row[school_col] if school_col >= 0 and school_col < len(row) else ''
                ol_name = row[ol_name_col] if ol_name_col >= 0 and ol_name_col < len(row) else ''
                rc_name = row[rc_name_col] if rc_name_col >= 0 and rc_name_col < len(row) else ''
                ol_email = row[ol_email_col] if ol_email_col >= 0 and ol_email_col < len(row) else ''
                rc_email = row[rc_email_col] if rc_email_col >= 0 and rc_email_col < len(row) else ''
                ol_contacted = row[ol_contacted_col] if ol_contacted_col >= 0 and ol_contacted_col < len(row) else ''
                rc_contacted = row[rc_contacted_col] if rc_contacted_col >= 0 and rc_contacted_col < len(row) else ''
                
                # OL pending?
                if ol_email and not ol_contacted and not self.state.has_sent_to(ol_email):
                    pending.append({
                        'row': row_idx + 2,
                        'school': school,
                        'name': ol_name,
                        'email': ol_email,
                        'type': 'OL',
                        'contacted_col': ol_contacted_col + 1,
                    })
                
                # RC pending?
                if rc_email and not rc_contacted and not self.state.has_sent_to(rc_email):
                    pending.append({
                        'row': row_idx + 2,
                        'school': school,
                        'name': rc_name,
                        'email': rc_email,
                        'type': 'RC',
                        'contacted_col': rc_contacted_col + 1,
                    })
            
            return pending
            
        finally:
            self.sheets.disconnect()
    
    def _scheduler_loop(self):
        """Background scheduler loop."""
        while self._running:
            schedule.run_pending()
            time.sleep(30)  # Check every 30 seconds
    
    def _run_daily_job(self) -> Dict[str, int]:
        """Execute the daily email job."""
        if not self.config.enabled:
            self.logger.info("Scheduler disabled, skipping")
            return {'sent': 0, 'errors': 0}
        
        self.state.reset_daily_if_needed()
        
        self._emit('job_started', {})
        self.logger.info("Starting daily email job")
        
        sent = 0
        errors = 0
        
        try:
            pending = self.get_pending_emails()
            
            if not pending:
                self.logger.info("No pending emails")
                return {'sent': 0, 'errors': 0}
            
            # Connect
            if not self.sender.connect():
                self._emit('error', {'message': 'Failed to connect to SMTP'})
                return {'sent': 0, 'errors': 1}
            
            if not self.sheets.connect():
                self._emit('error', {'message': 'Failed to connect to Sheets'})
                return {'sent': 0, 'errors': 1}
            
            # Send emails
            for coach in pending:
                # Check daily limit
                if self.state.daily_count >= self.config.max_emails_per_day:
                    self.logger.info(f"Daily limit reached ({self.config.max_emails_per_day})")
                    break
                
                # Prepare email
                last_name = coach['name'].split()[-1] if coach['name'] else 'Coach'
                
                if coach['type'] == 'RC':
                    subject = RC_SUBJECT.format(
                        grad_year=self.config.graduation_year,
                        athlete_name=self.config.athlete_name
                    )
                    body = RC_TEMPLATE.format(
                        last_name=last_name,
                        athlete_name=self.config.athlete_name,
                        grad_year=self.config.graduation_year,
                        high_school=self.config.high_school or "[HIGH SCHOOL]",
                        city_state=self.config.city_state or "[CITY, STATE]",
                        school=coach['school'],
                        height=self.config.height,
                        weight=self.config.weight,
                        positions=self.config.positions,
                        gpa=self.config.gpa or "[GPA]",
                        highlight_url=self.config.highlight_url or "[HIGHLIGHT LINK]",
                        phone=self.config.phone or ""
                    )
                else:
                    subject = OL_SUBJECT.format(
                        grad_year=self.config.graduation_year,
                        athlete_name=self.config.athlete_name
                    )
                    body = OL_TEMPLATE.format(
                        last_name=last_name,
                        athlete_name=self.config.athlete_name,
                        grad_year=self.config.graduation_year,
                        high_school=self.config.high_school or "[HIGH SCHOOL]",
                        city_state=self.config.city_state or "[CITY, STATE]",
                        school=coach['school'],
                        height=self.config.height,
                        weight=self.config.weight,
                        positions=self.config.positions,
                        highlight_url=self.config.highlight_url or "[HIGHLIGHT LINK]",
                        phone=self.config.phone or ""
                    )
                
                # Send
                success, error = self.sender.send(coach['email'], subject, body)
                
                if success:
                    self.state.mark_sent(coach['email'])
                    
                    # Mark as contacted in sheet
                    self.sheets.update_cell(
                        coach['row'],
                        coach['contacted_col'],
                        datetime.now().strftime('%Y-%m-%d')
                    )
                    
                    sent += 1
                    self._emit('email_sent', {
                        'school': coach['school'],
                        'type': coach['type'],
                        'email': coach['email']
                    })
                    self.logger.info(f"Sent to {coach['email']}")
                else:
                    self.state.add_error(coach['email'], error)
                    errors += 1
                    self._emit('email_error', {
                        'school': coach['school'],
                        'email': coach['email'],
                        'error': error
                    })
                
                # Rate limit delay
                time.sleep(self.config.delay_between_emails)
            
        except Exception as e:
            self.logger.error(f"Job error: {e}")
            errors += 1
        finally:
            self.sender.disconnect()
            self.sheets.disconnect()
        
        self._emit('job_completed', {'sent': sent, 'errors': errors})
        self.logger.info(f"Daily job complete: sent={sent}, errors={errors}")
        
        return {'sent': sent, 'errors': errors}
    
    def _emit(self, event: str, data: Dict):
        """Emit event to callbacks."""
        for callback in self._callbacks:
            try:
                callback(event, data)
            except:
                pass


# ============================================================================
# CLI
# ============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Email Scheduler')
    parser.add_argument('--run-now', action='store_true', help='Run immediately')
    parser.add_argument('--status', action='store_true', help='Show status')
    parser.add_argument('--pending', action='store_true', help='Show pending emails')
    args = parser.parse_args()
    
    scheduler = EmailScheduler()
    
    if args.status:
        status = scheduler.get_status()
        print(json.dumps(status, indent=2))
    elif args.pending:
        pending = scheduler.get_pending_emails()
        print(f"Pending emails: {len(pending)}")
        for p in pending[:10]:
            print(f"  {p['school']} - {p['type']}: {p['email']}")
    elif args.run_now:
        def callback(event, data):
            print(f"[{event}] {data}")
        scheduler.add_callback(callback)
        result = scheduler.run_now()
        print(f"Result: {result}")
    else:
        scheduler.start()
        print("Scheduler running. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            scheduler.stop()


if __name__ == '__main__':
    main()
