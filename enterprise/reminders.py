"""
Reminders System - Follow-up notifications, task tracking, overdue alerts
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import json
import os

class ReminderType(Enum):
    FOLLOW_UP = "follow_up"
    SEND_EMAIL = "send_email"
    CALL = "call"
    VISIT = "visit"
    DEADLINE = "deadline"
    CAMP = "camp"
    CUSTOM = "custom"
    
    @property
    def label(self) -> str:
        labels = {
            "follow_up": "Follow Up",
            "send_email": "Send Email",
            "call": "Make Call",
            "visit": "Schedule Visit",
            "deadline": "Deadline",
            "camp": "Camp/Event",
            "custom": "Custom"
        }
        return labels.get(self.value, self.value.title())
    
    @property
    def icon(self) -> str:
        icons = {
            "follow_up": "🔔",
            "send_email": "📧",
            "call": "📞",
            "visit": "🏟️",
            "deadline": "⏰",
            "camp": "🏈",
            "custom": "📝"
        }
        return icons.get(self.value, "📌")
    
    @property
    def color(self) -> str:
        colors = {
            "follow_up": "#3b82f6",
            "send_email": "#8b5cf6",
            "call": "#22c55e",
            "visit": "#f59e0b",
            "deadline": "#ef4444",
            "camp": "#06b6d4",
            "custom": "#6b7280"
        }
        return colors.get(self.value, "#6b7280")

class ReminderPriority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    URGENT = 4

@dataclass
class Reminder:
    id: str
    title: str
    reminder_type: ReminderType
    due_date: datetime
    school_name: str = ""
    coach_name: str = ""
    contact_id: str = ""
    notes: str = ""
    priority: ReminderPriority = ReminderPriority.MEDIUM
    completed: bool = False
    completed_at: Optional[datetime] = None
    snoozed_until: Optional[datetime] = None
    recurring: bool = False
    recurring_days: int = 0  # Days between recurrences
    created_at: datetime = field(default_factory=datetime.now)
    
    @property
    def is_overdue(self) -> bool:
        if self.completed:
            return False
        check_time = self.snoozed_until or self.due_date
        return check_time < datetime.now()
    
    @property
    def is_due_today(self) -> bool:
        if self.completed:
            return False
        check_time = self.snoozed_until or self.due_date
        today = datetime.now().date()
        return check_time.date() == today
    
    @property
    def is_due_this_week(self) -> bool:
        if self.completed:
            return False
        check_time = self.snoozed_until or self.due_date
        now = datetime.now()
        week_end = now + timedelta(days=7)
        return now <= check_time <= week_end
    
    def days_until_due(self) -> int:
        check_time = self.snoozed_until or self.due_date
        return (check_time.date() - datetime.now().date()).days
    
    def snooze(self, hours: int = 24):
        """Snooze reminder for specified hours"""
        self.snoozed_until = datetime.now() + timedelta(hours=hours)
    
    def complete(self) -> Optional['Reminder']:
        """Mark as complete, returns new reminder if recurring"""
        self.completed = True
        self.completed_at = datetime.now()
        
        if self.recurring and self.recurring_days > 0:
            # Create next occurrence
            return Reminder(
                id=f"{self.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                title=self.title,
                reminder_type=self.reminder_type,
                due_date=self.due_date + timedelta(days=self.recurring_days),
                school_name=self.school_name,
                coach_name=self.coach_name,
                contact_id=self.contact_id,
                notes=self.notes,
                priority=self.priority,
                recurring=True,
                recurring_days=self.recurring_days
            )
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "reminder_type": self.reminder_type.value,
            "due_date": self.due_date.isoformat(),
            "school_name": self.school_name,
            "coach_name": self.coach_name,
            "contact_id": self.contact_id,
            "notes": self.notes,
            "priority": self.priority.value,
            "completed": self.completed,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "snoozed_until": self.snoozed_until.isoformat() if self.snoozed_until else None,
            "recurring": self.recurring,
            "recurring_days": self.recurring_days,
            "created_at": self.created_at.isoformat(),
            "is_overdue": self.is_overdue,
            "is_due_today": self.is_due_today,
            "days_until_due": self.days_until_due()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Reminder':
        return cls(
            id=data["id"],
            title=data["title"],
            reminder_type=ReminderType(data["reminder_type"]),
            due_date=datetime.fromisoformat(data["due_date"]),
            school_name=data.get("school_name", ""),
            coach_name=data.get("coach_name", ""),
            contact_id=data.get("contact_id", ""),
            notes=data.get("notes", ""),
            priority=ReminderPriority(data.get("priority", 2)),
            completed=data.get("completed", False),
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            snoozed_until=datetime.fromisoformat(data["snoozed_until"]) if data.get("snoozed_until") else None,
            recurring=data.get("recurring", False),
            recurring_days=data.get("recurring_days", 0),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now()
        )

class ReminderManager:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.reminders_file = os.path.join(data_dir, "reminders.json")
        self.reminders: Dict[str, Reminder] = {}
        self._load_data()
    
    def _load_data(self):
        """Load reminders from file"""
        if os.path.exists(self.reminders_file):
            try:
                with open(self.reminders_file, 'r') as f:
                    data = json.load(f)
                    for item in data:
                        reminder = Reminder.from_dict(item)
                        self.reminders[reminder.id] = reminder
            except Exception as e:
                print(f"Error loading reminders: {e}")
    
    def _save_data(self):
        """Save reminders to file"""
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.reminders_file, 'w') as f:
            json.dump([r.to_dict() for r in self.reminders.values()], f, indent=2)
    
    def add_reminder(self, reminder: Reminder) -> Reminder:
        """Add a new reminder"""
        self.reminders[reminder.id] = reminder
        self._save_data()
        return reminder
    
    def update_reminder(self, reminder_id: str, updates: Dict[str, Any]) -> Optional[Reminder]:
        """Update an existing reminder"""
        if reminder_id not in self.reminders:
            return None
        
        reminder = self.reminders[reminder_id]
        for key, value in updates.items():
            if hasattr(reminder, key):
                if key == "reminder_type":
                    value = ReminderType(value)
                elif key == "priority":
                    value = ReminderPriority(value)
                elif key == "due_date" and isinstance(value, str):
                    value = datetime.fromisoformat(value)
                setattr(reminder, key, value)
        
        self._save_data()
        return reminder
    
    def delete_reminder(self, reminder_id: str) -> bool:
        """Delete a reminder"""
        if reminder_id in self.reminders:
            del self.reminders[reminder_id]
            self._save_data()
            return True
        return False
    
    def complete_reminder(self, reminder_id: str) -> Optional[Reminder]:
        """Complete a reminder, may create recurring follow-up"""
        if reminder_id not in self.reminders:
            return None
        
        reminder = self.reminders[reminder_id]
        next_reminder = reminder.complete()
        
        if next_reminder:
            self.reminders[next_reminder.id] = next_reminder
        
        self._save_data()
        return reminder
    
    def snooze_reminder(self, reminder_id: str, hours: int = 24) -> Optional[Reminder]:
        """Snooze a reminder"""
        if reminder_id not in self.reminders:
            return None
        
        self.reminders[reminder_id].snooze(hours)
        self._save_data()
        return self.reminders[reminder_id]
    
    def get_reminder(self, reminder_id: str) -> Optional[Reminder]:
        """Get a reminder by ID"""
        return self.reminders.get(reminder_id)
    
    def get_active_reminders(self) -> List[Reminder]:
        """Get all non-completed reminders"""
        return [r for r in self.reminders.values() if not r.completed]
    
    def get_overdue(self) -> List[Reminder]:
        """Get all overdue reminders"""
        return sorted([r for r in self.reminders.values() if r.is_overdue],
                     key=lambda x: x.due_date)
    
    def get_due_today(self) -> List[Reminder]:
        """Get reminders due today"""
        return sorted([r for r in self.reminders.values() if r.is_due_today],
                     key=lambda x: x.due_date)
    
    def get_due_this_week(self) -> List[Reminder]:
        """Get reminders due this week"""
        return sorted([r for r in self.reminders.values() if r.is_due_this_week and not r.is_due_today],
                     key=lambda x: x.due_date)
    
    def get_by_school(self, school_name: str) -> List[Reminder]:
        """Get reminders for a specific school"""
        return [r for r in self.reminders.values() 
                if r.school_name.lower() == school_name.lower() and not r.completed]
    
    def get_by_type(self, reminder_type: ReminderType) -> List[Reminder]:
        """Get reminders by type"""
        return [r for r in self.reminders.values() 
                if r.reminder_type == reminder_type and not r.completed]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get reminder summary counts"""
        active = self.get_active_reminders()
        return {
            "total_active": len(active),
            "overdue": len(self.get_overdue()),
            "due_today": len(self.get_due_today()),
            "due_this_week": len(self.get_due_this_week()),
            "by_type": {
                t.value: len([r for r in active if r.reminder_type == t])
                for t in ReminderType
            },
            "by_priority": {
                p.value: len([r for r in active if r.priority == p])
                for p in ReminderPriority
            }
        }
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for reminders dashboard"""
        return {
            "summary": self.get_summary(),
            "overdue": [r.to_dict() for r in self.get_overdue()[:5]],
            "due_today": [r.to_dict() for r in self.get_due_today()],
            "due_this_week": [r.to_dict() for r in self.get_due_this_week()[:10]],
            "all_active": [r.to_dict() for r in sorted(
                self.get_active_reminders(),
                key=lambda x: (x.priority.value * -1, x.due_date)
            )]
        }
    
    def create_follow_up_from_email(self, school_name: str, coach_name: str, 
                                     days: int = 7, contact_id: str = "") -> Reminder:
        """Create a follow-up reminder after sending an email"""
        reminder = Reminder(
            id=f"followup_{school_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            title=f"Follow up with {coach_name} at {school_name}",
            reminder_type=ReminderType.FOLLOW_UP,
            due_date=datetime.now() + timedelta(days=days),
            school_name=school_name,
            coach_name=coach_name,
            contact_id=contact_id,
            notes=f"Follow up on email sent {datetime.now().strftime('%m/%d/%Y')}"
        )
        return self.add_reminder(reminder)
