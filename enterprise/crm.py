"""
CRM System - Contact tracking, notes, pipeline stages, interaction history
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
import json
import os

class PipelineStage(Enum):
    PROSPECT = "prospect"
    CONTACTED = "contacted"
    INTERESTED = "interested"
    EVALUATING = "evaluating"
    VERBAL_OFFER = "verbal_offer"
    COMMITTED = "committed"
    SIGNED = "signed"
    DECLINED = "declined"
    
    @property
    def label(self) -> str:
        labels = {
            "prospect": "Prospect",
            "contacted": "Contacted",
            "interested": "Interested",
            "evaluating": "Evaluating",
            "verbal_offer": "Verbal Offer",
            "committed": "Committed",
            "signed": "Signed",
            "declined": "Declined"
        }
        return labels.get(self.value, self.value.title())
    
    @property
    def color(self) -> str:
        colors = {
            "prospect": "#6b7280",
            "contacted": "#3b82f6",
            "interested": "#8b5cf6",
            "evaluating": "#f59e0b",
            "verbal_offer": "#10b981",
            "committed": "#22c55e",
            "signed": "#059669",
            "declined": "#ef4444"
        }
        return colors.get(self.value, "#6b7280")

class InteractionType(Enum):
    EMAIL = "email"
    PHONE = "phone"
    TEXT = "text"
    TWITTER_DM = "twitter_dm"
    VISIT = "visit"
    VIDEO_CALL = "video_call"
    IN_PERSON = "in_person"
    CAMP = "camp"
    NOTE = "note"

@dataclass
class Interaction:
    id: str
    contact_id: str
    type: InteractionType
    date: datetime
    summary: str
    notes: str = ""
    outcome: str = ""
    follow_up_needed: bool = False
    follow_up_date: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "contact_id": self.contact_id,
            "type": self.type.value,
            "date": self.date.isoformat(),
            "summary": self.summary,
            "notes": self.notes,
            "outcome": self.outcome,
            "follow_up_needed": self.follow_up_needed,
            "follow_up_date": self.follow_up_date.isoformat() if self.follow_up_date else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Interaction':
        return cls(
            id=data["id"],
            contact_id=data["contact_id"],
            type=InteractionType(data["type"]),
            date=datetime.fromisoformat(data["date"]),
            summary=data["summary"],
            notes=data.get("notes", ""),
            outcome=data.get("outcome", ""),
            follow_up_needed=data.get("follow_up_needed", False),
            follow_up_date=datetime.fromisoformat(data["follow_up_date"]) if data.get("follow_up_date") else None
        )

@dataclass
class Contact:
    id: str
    school_name: str
    coach_name: str
    title: str = ""
    email: str = ""
    phone: str = ""
    twitter: str = ""
    stage: PipelineStage = PipelineStage.PROSPECT
    notes: str = ""
    tags: List[str] = field(default_factory=list)
    interactions: List[str] = field(default_factory=list)  # List of interaction IDs
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    last_contact: Optional[datetime] = None
    priority: int = 1  # 1=High, 2=Medium, 3=Low
    scholarship_offered: bool = False
    interest_level: int = 0  # 0-10 scale
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "school_name": self.school_name,
            "coach_name": self.coach_name,
            "title": self.title,
            "email": self.email,
            "phone": self.phone,
            "twitter": self.twitter,
            "stage": self.stage.value,
            "notes": self.notes,
            "tags": self.tags,
            "interactions": self.interactions,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "last_contact": self.last_contact.isoformat() if self.last_contact else None,
            "priority": self.priority,
            "scholarship_offered": self.scholarship_offered,
            "interest_level": self.interest_level
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Contact':
        return cls(
            id=data["id"],
            school_name=data["school_name"],
            coach_name=data["coach_name"],
            title=data.get("title", ""),
            email=data.get("email", ""),
            phone=data.get("phone", ""),
            twitter=data.get("twitter", ""),
            stage=PipelineStage(data.get("stage", "prospect")),
            notes=data.get("notes", ""),
            tags=data.get("tags", []),
            interactions=data.get("interactions", []),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else datetime.now(),
            last_contact=datetime.fromisoformat(data["last_contact"]) if data.get("last_contact") else None,
            priority=data.get("priority", 1),
            scholarship_offered=data.get("scholarship_offered", False),
            interest_level=data.get("interest_level", 0)
        )

class CRMManager:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.contacts_file = os.path.join(data_dir, "crm_contacts.json")
        self.interactions_file = os.path.join(data_dir, "crm_interactions.json")
        self.contacts: Dict[str, Contact] = {}
        self.interactions: Dict[str, Interaction] = {}
        self._load_data()
    
    def _load_data(self):
        """Load CRM data from files"""
        if os.path.exists(self.contacts_file):
            try:
                with open(self.contacts_file, 'r') as f:
                    data = json.load(f)
                    for item in data:
                        contact = Contact.from_dict(item)
                        self.contacts[contact.id] = contact
            except Exception as e:
                print(f"Error loading contacts: {e}")
        
        if os.path.exists(self.interactions_file):
            try:
                with open(self.interactions_file, 'r') as f:
                    data = json.load(f)
                    for item in data:
                        interaction = Interaction.from_dict(item)
                        self.interactions[interaction.id] = interaction
            except Exception as e:
                print(f"Error loading interactions: {e}")
    
    def _save_data(self):
        """Save CRM data to files"""
        os.makedirs(self.data_dir, exist_ok=True)
        
        with open(self.contacts_file, 'w') as f:
            json.dump([c.to_dict() for c in self.contacts.values()], f, indent=2)
        
        with open(self.interactions_file, 'w') as f:
            json.dump([i.to_dict() for i in self.interactions.values()], f, indent=2)
    
    def add_contact(self, contact: Contact) -> Contact:
        """Add a new contact"""
        self.contacts[contact.id] = contact
        self._save_data()
        return contact
    
    def update_contact(self, contact_id: str, updates: Dict[str, Any]) -> Optional[Contact]:
        """Update an existing contact"""
        if contact_id not in self.contacts:
            return None
        
        contact = self.contacts[contact_id]
        for key, value in updates.items():
            if hasattr(contact, key):
                if key == "stage":
                    value = PipelineStage(value)
                setattr(contact, key, value)
        contact.updated_at = datetime.now()
        self._save_data()
        return contact
    
    def delete_contact(self, contact_id: str) -> bool:
        """Delete a contact"""
        if contact_id in self.contacts:
            del self.contacts[contact_id]
            # Also delete related interactions
            to_delete = [k for k, v in self.interactions.items() if v.contact_id == contact_id]
            for k in to_delete:
                del self.interactions[k]
            self._save_data()
            return True
        return False
    
    def get_contact(self, contact_id: str) -> Optional[Contact]:
        """Get a contact by ID"""
        return self.contacts.get(contact_id)
    
    def get_contacts_by_stage(self, stage: PipelineStage) -> List[Contact]:
        """Get all contacts in a specific pipeline stage"""
        return [c for c in self.contacts.values() if c.stage == stage]
    
    def get_contacts_by_school(self, school_name: str) -> List[Contact]:
        """Get all contacts from a specific school"""
        return [c for c in self.contacts.values() if c.school_name.lower() == school_name.lower()]
    
    def add_interaction(self, interaction: Interaction) -> Interaction:
        """Add a new interaction"""
        self.interactions[interaction.id] = interaction
        
        # Update contact's interaction list and last_contact
        if interaction.contact_id in self.contacts:
            contact = self.contacts[interaction.contact_id]
            contact.interactions.append(interaction.id)
            contact.last_contact = interaction.date
            contact.updated_at = datetime.now()
        
        self._save_data()
        return interaction
    
    def get_contact_interactions(self, contact_id: str) -> List[Interaction]:
        """Get all interactions for a contact"""
        return [i for i in self.interactions.values() if i.contact_id == contact_id]
    
    def get_follow_ups_due(self, before: Optional[datetime] = None) -> List[Interaction]:
        """Get all interactions that need follow-up"""
        if before is None:
            before = datetime.now()
        return [
            i for i in self.interactions.values()
            if i.follow_up_needed and i.follow_up_date and i.follow_up_date <= before
        ]
    
    def get_pipeline_summary(self) -> Dict[str, int]:
        """Get count of contacts in each pipeline stage"""
        summary = {}
        for stage in PipelineStage:
            summary[stage.value] = len(self.get_contacts_by_stage(stage))
        return summary
    
    def search_contacts(self, query: str) -> List[Contact]:
        """Search contacts by name, school, or notes"""
        query = query.lower()
        results = []
        for contact in self.contacts.values():
            if (query in contact.coach_name.lower() or
                query in contact.school_name.lower() or
                query in contact.notes.lower() or
                query in contact.email.lower()):
                results.append(contact)
        return results
    
    def get_all_contacts(self) -> List[Contact]:
        """Get all contacts sorted by last updated"""
        return sorted(self.contacts.values(), key=lambda x: x.updated_at, reverse=True)
    
    def import_from_schools(self, schools_data: List[Dict], coach_data: Dict[str, List[Dict]]) -> int:
        """Import contacts from school and coach data"""
        imported = 0
        for school in schools_data:
            school_name = school.get("name", "")
            coaches = coach_data.get(school_name, [])
            for coach in coaches:
                contact_id = f"{school_name}_{coach.get('name', '')}".replace(" ", "_").lower()
                if contact_id not in self.contacts:
                    contact = Contact(
                        id=contact_id,
                        school_name=school_name,
                        coach_name=coach.get("name", ""),
                        title=coach.get("title", ""),
                        email=coach.get("email", ""),
                        phone=coach.get("phone", ""),
                        twitter=coach.get("twitter", "")
                    )
                    self.add_contact(contact)
                    imported += 1
        return imported
