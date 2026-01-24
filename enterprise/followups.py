"""
Smart Follow-Up System
============================================================================
Automatically creates and manages follow-up reminders after emails are sent.
Tracks response status and suggests when to follow up.

Default cycle (every 3 days):
- Day 0: Intro email
- Day 3: 1st follow-up
- Day 6: 2nd follow-up
- Day 9: Restart (send intro again)

Features:
- Auto-create follow-ups after email sent
- Automatic cycle restart after follow-ups complete
- Track response status
- Configurable intervals
- Integration with CRM and reminders system

Author: Coach Outreach System
Version: 1.1.0
============================================================================
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS AND DATA CLASSES
# ============================================================================

class ResponseStatus(Enum):
    """Status of coach response"""
    PENDING = "pending"          # No response yet
    OPENED = "opened"            # Email opened (if tracking available)
    RESPONDED = "responded"      # Coach responded
    POSITIVE = "positive"        # Positive response
    NEGATIVE = "negative"        # Not interested
    NO_RESPONSE = "no_response"  # No response after all follow-ups


class FollowUpStatus(Enum):
    """Status of a follow-up"""
    SCHEDULED = "scheduled"
    DUE = "due"
    OVERDUE = "overdue"
    SENT = "sent"
    CANCELLED = "cancelled"


@dataclass
class EmailRecord:
    """Record of an email sent to a coach"""
    id: str
    coach_name: str
    coach_email: str
    school: str
    coach_type: str  # rc, oc, position
    subject: str
    sent_at: str  # ISO format
    template_id: str = ""
    response_status: str = "pending"
    response_date: Optional[str] = None
    notes: str = ""
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'EmailRecord':
        return cls(**data)


@dataclass
class FollowUp:
    """A scheduled follow-up"""
    id: str
    email_record_id: str
    coach_name: str
    coach_email: str
    school: str
    follow_up_number: int  # 1, 2, or 'restart' (0 = restart/intro)
    due_date: str  # ISO format
    status: str = "scheduled"
    sent_at: Optional[str] = None
    is_restart: bool = False  # True if this is a cycle restart (send intro again)
    
    @property
    def is_due(self) -> bool:
        due = datetime.fromisoformat(self.due_date)
        return datetime.now() >= due and self.status == "scheduled"
    
    @property
    def is_overdue(self) -> bool:
        due = datetime.fromisoformat(self.due_date)
        return datetime.now() > due + timedelta(days=1) and self.status == "scheduled"
    
    @property
    def days_until_due(self) -> int:
        due = datetime.fromisoformat(self.due_date)
        delta = due - datetime.now()
        return delta.days
    
    def to_dict(self) -> dict:
        d = asdict(self)
        d['is_due'] = self.is_due
        d['is_overdue'] = self.is_overdue
        d['days_until_due'] = self.days_until_due
        return d
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FollowUp':
        # Remove computed properties
        data.pop('is_due', None)
        data.pop('is_overdue', None)
        data.pop('days_until_due', None)
        # Handle old data without is_restart field
        if 'is_restart' not in data:
            data['is_restart'] = False
        return cls(**data)


@dataclass
class FollowUpConfig:
    """Configuration for follow-up system"""
    enabled: bool = True
    intervals_days: List[int] = field(default_factory=lambda: [3, 6])  # 3 days after intro, then 3 more days
    max_followups: int = 2  # 2 follow-ups before restart
    restart_cycle: bool = True  # After follow-ups, restart with intro
    restart_delay_days: int = 3  # Days after last follow-up to restart
    auto_cancel_on_response: bool = True
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FollowUpConfig':
        return cls(**data)


# ============================================================================
# FOLLOW-UP MANAGER
# ============================================================================

class FollowUpManager:
    """Manages follow-up scheduling and tracking"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.emails_file = os.path.join(data_dir, "email_records.json")
        self.followups_file = os.path.join(data_dir, "followups.json")
        self.config_file = os.path.join(data_dir, "followup_config.json")
        
        os.makedirs(data_dir, exist_ok=True)
        
        self.emails: Dict[str, EmailRecord] = {}
        self.followups: Dict[str, FollowUp] = {}
        self.config = FollowUpConfig()
        
        self._load_data()
    
    def _load_data(self):
        """Load data from files"""
        # Load emails
        if os.path.exists(self.emails_file):
            try:
                with open(self.emails_file, 'r') as f:
                    data = json.load(f)
                    self.emails = {k: EmailRecord.from_dict(v) for k, v in data.items()}
            except Exception as e:
                logger.error(f"Error loading emails: {e}")
        
        # Load follow-ups
        if os.path.exists(self.followups_file):
            try:
                with open(self.followups_file, 'r') as f:
                    data = json.load(f)
                    self.followups = {k: FollowUp.from_dict(v) for k, v in data.items()}
            except Exception as e:
                logger.error(f"Error loading followups: {e}")
        
        # Load config
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    data = json.load(f)
                    self.config = FollowUpConfig.from_dict(data)
            except Exception as e:
                logger.error(f"Error loading config: {e}")
    
    def _save_emails(self):
        """Save emails to file"""
        with open(self.emails_file, 'w') as f:
            json.dump({k: v.to_dict() for k, v in self.emails.items()}, f, indent=2)
    
    def _save_followups(self):
        """Save follow-ups to file"""
        with open(self.followups_file, 'w') as f:
            json.dump({k: v.to_dict() for k, v in self.followups.items()}, f, indent=2)
    
    def _save_config(self):
        """Save config to file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.config.to_dict(), f, indent=2)
    
    def record_email_sent(self, coach_name: str, coach_email: str, school: str,
                          coach_type: str, subject: str, 
                          template_id: str = "") -> EmailRecord:
        """
        Record that an email was sent and schedule follow-ups.
        
        Args:
            coach_name: Name of coach
            coach_email: Email address
            school: School name
            coach_type: 'rc' or 'oc'
            subject: Email subject
            template_id: ID of template used
        
        Returns:
            EmailRecord object
        """
        import uuid
        
        email_id = str(uuid.uuid4())[:8]
        
        record = EmailRecord(
            id=email_id,
            coach_name=coach_name,
            coach_email=coach_email,
            school=school,
            coach_type=coach_type,
            subject=subject,
            sent_at=datetime.now().isoformat(),
            template_id=template_id
        )
        
        self.emails[email_id] = record
        self._save_emails()
        
        # Schedule follow-ups if enabled
        if self.config.enabled:
            self._schedule_followups(record)
        
        logger.info(f"Recorded email to {coach_name} ({school})")
        return record
    
    def _schedule_followups(self, email_record: EmailRecord):
        """
        Schedule follow-up reminders for an email.

        Default cycle (every 3 days):
        - Day 0: Intro email (already sent)
        - Day 3: 1st follow-up
        - Day 6: 2nd follow-up
        - Day 9: Restart (send intro again)
        """
        import uuid

        sent_date = datetime.fromisoformat(email_record.sent_at)

        # Schedule follow-ups at intervals (default: day 3 and day 6)
        for i, days in enumerate(self.config.intervals_days[:self.config.max_followups]):
            followup_id = str(uuid.uuid4())[:8]
            due_date = sent_date + timedelta(days=days)

            followup = FollowUp(
                id=followup_id,
                email_record_id=email_record.id,
                coach_name=email_record.coach_name,
                coach_email=email_record.coach_email,
                school=email_record.school,
                follow_up_number=i + 1,
                due_date=due_date.isoformat(),
                is_restart=False
            )

            self.followups[followup_id] = followup

        # Schedule restart (send intro again) if enabled
        if self.config.restart_cycle:
            restart_id = str(uuid.uuid4())[:8]
            # Restart happens after last follow-up + restart_delay_days
            last_followup_day = self.config.intervals_days[-1] if self.config.intervals_days else 0
            restart_day = last_followup_day + self.config.restart_delay_days
            restart_date = sent_date + timedelta(days=restart_day)

            restart_followup = FollowUp(
                id=restart_id,
                email_record_id=email_record.id,
                coach_name=email_record.coach_name,
                coach_email=email_record.coach_email,
                school=email_record.school,
                follow_up_number=0,  # 0 indicates restart
                due_date=restart_date.isoformat(),
                is_restart=True
            )

            self.followups[restart_id] = restart_followup

        self._save_followups()
        scheduled_count = len(self.config.intervals_days) + (1 if self.config.restart_cycle else 0)
        logger.info(f"Scheduled {scheduled_count} follow-ups for {email_record.coach_name} (includes restart)")
    
    def mark_response_received(self, email_id: str, 
                               status: str = "responded",
                               notes: str = "") -> Optional[EmailRecord]:
        """
        Mark that a response was received for an email.
        Optionally cancels pending follow-ups.
        """
        if email_id not in self.emails:
            return None
        
        record = self.emails[email_id]
        record.response_status = status
        record.response_date = datetime.now().isoformat()
        record.notes = notes
        
        self._save_emails()
        
        # Cancel pending follow-ups if configured
        if self.config.auto_cancel_on_response:
            self._cancel_followups_for_email(email_id)
        
        return record
    
    def mark_response_by_coach(self, coach_email: str, 
                               status: str = "responded") -> List[EmailRecord]:
        """Mark response received by coach email address"""
        updated = []
        for record in self.emails.values():
            if record.coach_email.lower() == coach_email.lower():
                if record.response_status == "pending":
                    record.response_status = status
                    record.response_date = datetime.now().isoformat()
                    updated.append(record)
        
        if updated:
            self._save_emails()
            if self.config.auto_cancel_on_response:
                for record in updated:
                    self._cancel_followups_for_email(record.id)
        
        return updated
    
    def _cancel_followups_for_email(self, email_id: str):
        """Cancel all pending follow-ups for an email"""
        for followup in self.followups.values():
            if followup.email_record_id == email_id:
                if followup.status == "scheduled":
                    followup.status = "cancelled"
        
        self._save_followups()
    
    def get_due_followups(self) -> List[FollowUp]:
        """Get all follow-ups that are due or overdue"""
        due = []
        for followup in self.followups.values():
            if followup.status == "scheduled" and followup.is_due:
                due.append(followup)
        
        # Sort by due date
        due.sort(key=lambda f: f.due_date)
        return due
    
    def get_overdue_followups(self) -> List[FollowUp]:
        """Get overdue follow-ups only"""
        return [f for f in self.get_due_followups() if f.is_overdue]

    def get_due_restarts(self) -> List[FollowUp]:
        """Get all restarts that are due (should send intro email again)"""
        return [f for f in self.get_due_followups() if f.is_restart]

    def get_due_regular_followups(self) -> List[FollowUp]:
        """Get due follow-ups that are NOT restarts"""
        return [f for f in self.get_due_followups() if not f.is_restart]
    
    def get_upcoming_followups(self, days: int = 7) -> List[FollowUp]:
        """Get follow-ups due in the next N days"""
        cutoff = datetime.now() + timedelta(days=days)
        upcoming = []
        
        for followup in self.followups.values():
            if followup.status == "scheduled":
                due = datetime.fromisoformat(followup.due_date)
                if datetime.now() <= due <= cutoff:
                    upcoming.append(followup)
        
        upcoming.sort(key=lambda f: f.due_date)
        return upcoming
    
    def mark_followup_sent(self, followup_id: str) -> Optional[FollowUp]:
        """Mark a follow-up as sent"""
        if followup_id not in self.followups:
            return None
        
        followup = self.followups[followup_id]
        followup.status = "sent"
        followup.sent_at = datetime.now().isoformat()
        
        self._save_followups()
        return followup
    
    def skip_followup(self, followup_id: str) -> Optional[FollowUp]:
        """Skip/cancel a follow-up"""
        if followup_id not in self.followups:
            return None
        
        followup = self.followups[followup_id]
        followup.status = "cancelled"
        
        self._save_followups()
        return followup
    
    def snooze_followup(self, followup_id: str, days: int = 3) -> Optional[FollowUp]:
        """Snooze a follow-up by N days"""
        if followup_id not in self.followups:
            return None
        
        followup = self.followups[followup_id]
        current_due = datetime.fromisoformat(followup.due_date)
        new_due = current_due + timedelta(days=days)
        followup.due_date = new_due.isoformat()
        
        self._save_followups()
        return followup
    
    def get_email_history(self, school: str = None, 
                          coach_email: str = None) -> List[EmailRecord]:
        """Get email history, optionally filtered"""
        records = list(self.emails.values())
        
        if school:
            records = [r for r in records if school.lower() in r.school.lower()]
        
        if coach_email:
            records = [r for r in records if r.coach_email.lower() == coach_email.lower()]
        
        # Sort by sent date, newest first
        records.sort(key=lambda r: r.sent_at, reverse=True)
        return records
    
    def get_pending_responses(self) -> List[EmailRecord]:
        """Get all emails still awaiting response"""
        return [r for r in self.emails.values() if r.response_status == "pending"]
    
    def get_stats(self) -> dict:
        """Get follow-up system statistics"""
        total_emails = len(self.emails)
        pending = len([e for e in self.emails.values() if e.response_status == "pending"])
        responded = len([e for e in self.emails.values() if e.response_status in ["responded", "positive"]])
        
        total_followups = len(self.followups)
        due_followups = len(self.get_due_followups())
        overdue = len(self.get_overdue_followups())
        
        response_rate = (responded / total_emails * 100) if total_emails > 0 else 0
        
        return {
            "total_emails_sent": total_emails,
            "awaiting_response": pending,
            "responses_received": responded,
            "response_rate": round(response_rate, 1),
            "total_followups_scheduled": total_followups,
            "followups_due": due_followups,
            "followups_overdue": overdue,
        }
    
    def get_dashboard_data(self) -> dict:
        """Get data for dashboard display"""
        return {
            "stats": self.get_stats(),
            "due_followups": [f.to_dict() for f in self.get_due_followups()[:10]],
            "upcoming_followups": [f.to_dict() for f in self.get_upcoming_followups(7)[:10]],
            "recent_emails": [e.to_dict() for e in self.get_email_history()[:10]],
            "config": self.config.to_dict()
        }
    
    def update_config(self, **kwargs) -> FollowUpConfig:
        """Update follow-up configuration"""
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        
        self._save_config()
        return self.config


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_manager = None

def get_followup_manager(data_dir: str = "data") -> FollowUpManager:
    """Get singleton follow-up manager"""
    global _manager
    if _manager is None:
        _manager = FollowUpManager(data_dir)
    return _manager


def record_email_sent(coach_name: str, coach_email: str, school: str,
                      coach_type: str, subject: str) -> EmailRecord:
    """Quick function to record an email sent"""
    manager = get_followup_manager()
    return manager.record_email_sent(coach_name, coach_email, school, coach_type, subject)


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    import tempfile
    
    print("=== Follow-Up System Test ===\n")
    
    # Use temp directory for test
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = FollowUpManager(tmpdir)
        
        # Record some emails
        print("Recording test emails...")
        e1 = manager.record_email_sent(
            "Coach Smith", "smith@osu.edu", "Ohio State",
            "rc", "Recruiting Inquiry"
        )
        print(f"  Recorded: {e1.coach_name} ({e1.school})")
        
        e2 = manager.record_email_sent(
            "Coach Jones", "jones@um.edu", "Michigan", 
            "oc", "Film Review Request"
        )
        print(f"  Recorded: {e2.coach_name} ({e2.school})")
        
        # Check follow-ups created
        print(f"\nTotal follow-ups scheduled: {len(manager.followups)}")
        
        # Get stats
        print("\nStats:")
        stats = manager.get_stats()
        for k, v in stats.items():
            print(f"  {k}: {v}")
        
        # Test response
        print("\nMarking response from Coach Smith...")
        manager.mark_response_received(e1.id, "positive", "Interested in seeing more film")
        
        stats = manager.get_stats()
        print(f"  Response rate now: {stats['response_rate']}%")
        
        print("\n✓ Follow-up system working!")
