"""
Response Tracking & Analytics
============================================================================
Track sent emails, detect responses via Gmail IMAP, calculate response rates.

Features:
- Track all sent emails with metadata
- Scan Gmail inbox for responses
- Response rate by division (FBS, FCS, D2, D3, NAIA, JUCO)
- Hot leads identification
- Recent responses with snippets

Author: Coach Outreach System  
Version: 1.0.0
============================================================================
"""

import json
import imaplib
import email
from email.header import decode_header
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
import logging
import re

logger = logging.getLogger(__name__)


@dataclass
class SentEmail:
    """Record of a sent email."""
    coach_email: str
    coach_name: str
    school: str
    division: str
    coach_type: str  # rc, ol
    template_id: str
    followup_number: int  # 0 = initial, 1+ = follow-up
    sent_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass 
class Response:
    """A detected response from a coach."""
    coach_email: str
    coach_name: str
    school: str
    subject: str
    snippet: str  # First ~100 chars of body
    received_at: str
    
    def to_dict(self) -> dict:
        return asdict(self)


class GmailResponseChecker:
    """Check Gmail for responses via IMAP."""
    
    def __init__(self, email_address: str, app_password: str):
        self.email = email_address
        self.password = app_password
        self.imap = None
    
    def connect(self) -> bool:
        """Connect to Gmail IMAP."""
        try:
            self.imap = imaplib.IMAP4_SSL('imap.gmail.com')
            self.imap.login(self.email, self.password)
            return True
        except Exception as e:
            logger.error(f"IMAP connection failed: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from IMAP."""
        if self.imap:
            try:
                self.imap.logout()
            except:
                pass
            self.imap = None
    
    def check_for_responses(self, coach_emails: List[str], 
                           since_days: int = 30) -> List[Dict]:
        """
        Check inbox for emails from any of the coach emails.
        Returns list of response dicts.
        """
        if not self.imap:
            if not self.connect():
                return []
        
        responses = []
        since_date = (datetime.now() - timedelta(days=since_days)).strftime("%d-%b-%Y")
        
        try:
            self.imap.select('INBOX')
            
            for coach_email in coach_emails:
                # Search for emails from this address
                search_criteria = f'(FROM "{coach_email}" SINCE {since_date})'
                
                try:
                    _, message_ids = self.imap.search(None, search_criteria)
                    
                    for msg_id in message_ids[0].split():
                        _, msg_data = self.imap.fetch(msg_id, '(RFC822)')
                        
                        for part in msg_data:
                            if isinstance(part, tuple):
                                msg = email.message_from_bytes(part[1])
                                
                                # Get subject
                                subject = decode_header(msg['Subject'])[0][0]
                                if isinstance(subject, bytes):
                                    subject = subject.decode('utf-8', errors='ignore')
                                
                                # Get date
                                date_str = msg['Date']
                                
                                # Get snippet
                                snippet = self._get_email_snippet(msg)
                                
                                responses.append({
                                    'coach_email': coach_email,
                                    'subject': subject or '',
                                    'snippet': snippet,
                                    'date': date_str
                                })
                except Exception as e:
                    logger.warning(f"Error checking {coach_email}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"IMAP search error: {e}")
        
        return responses
    
    def _get_email_snippet(self, msg, max_length: int = 150) -> str:
        """Extract text snippet from email."""
        try:
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain':
                        payload = part.get_payload(decode=True)
                        if payload:
                            text = payload.decode('utf-8', errors='ignore')
                            # Clean up
                            text = re.sub(r'\s+', ' ', text).strip()
                            return text[:max_length] + ('...' if len(text) > max_length else '')
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    text = payload.decode('utf-8', errors='ignore')
                    text = re.sub(r'\s+', ' ', text).strip()
                    return text[:max_length] + ('...' if len(text) > max_length else '')
        except:
            pass
        return ''


class ResponseTracker:
    """Track sent emails and responses with analytics."""
    
    def __init__(self, data_dir: Path = None):
        self.data_dir = data_dir or Path.home() / '.coach_outreach'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.data_file = self.data_dir / 'response_tracking.json'
        
        self.sent_emails: List[SentEmail] = []
        self.responses: List[Response] = []
        self._load()
    
    def _load(self):
        """Load data from disk."""
        if self.data_file.exists():
            try:
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                
                self.sent_emails = [
                    SentEmail(**e) for e in data.get('sent_emails', [])
                ]
                self.responses = [
                    Response(**r) for r in data.get('responses', [])
                ]
            except Exception as e:
                logger.error(f"Error loading response data: {e}")
    
    def _save(self):
        """Save data to disk."""
        try:
            data = {
                'sent_emails': [e.to_dict() for e in self.sent_emails],
                'responses': [r.to_dict() for r in self.responses]
            }
            with open(self.data_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving response data: {e}")
    
    def record_sent(self, coach_email: str, coach_name: str, school: str,
                    division: str, coach_type: str, template_id: str = '',
                    followup_number: int = 0) -> None:
        """Record a sent email."""
        self.sent_emails.append(SentEmail(
            coach_email=coach_email.lower().strip(),
            coach_name=coach_name,
            school=school,
            division=division,
            coach_type=coach_type,
            template_id=template_id,
            followup_number=followup_number
        ))
        self._save()
    
    def record_response(self, coach_email: str, subject: str, snippet: str,
                       received_at: str = None) -> None:
        """Record a response."""
        # Find coach info from sent emails
        coach_name = ''
        school = ''
        
        for sent in reversed(self.sent_emails):
            if sent.coach_email.lower() == coach_email.lower():
                coach_name = sent.coach_name
                school = sent.school
                break
        
        self.responses.append(Response(
            coach_email=coach_email.lower().strip(),
            coach_name=coach_name,
            school=school,
            subject=subject,
            snippet=snippet,
            received_at=received_at or datetime.now().isoformat()
        ))
        self._save()
    
    def has_responded(self, coach_email: str) -> bool:
        """Check if a coach has responded."""
        email_lower = coach_email.lower().strip()
        return any(r.coach_email.lower() == email_lower for r in self.responses)
    
    def get_stats(self) -> Dict:
        """Get overall statistics."""
        unique_coaches = set(e.coach_email for e in self.sent_emails)
        unique_responders = set(r.coach_email for r in self.responses)
        
        initial_emails = [e for e in self.sent_emails if e.followup_number == 0]
        followup_emails = [e for e in self.sent_emails if e.followup_number > 0]
        
        response_rate = (len(unique_responders) / len(unique_coaches) * 100) if unique_coaches else 0
        
        return {
            'total_emails_sent': len(self.sent_emails),
            'unique_coaches_contacted': len(unique_coaches),
            'initial_emails': len(initial_emails),
            'followup_emails': len(followup_emails),
            'total_responses': len(self.responses),
            'unique_responders': len(unique_responders),
            'response_rate': round(response_rate, 1)
        }
    
    def get_stats_by_division(self) -> Dict[str, Dict]:
        """Get response rates by division."""
        divisions = {}
        
        for sent in self.sent_emails:
            div = sent.division or 'Unknown'
            if div not in divisions:
                divisions[div] = {'coaches': set(), 'responders': set()}
            divisions[div]['coaches'].add(sent.coach_email)
        
        for resp in self.responses:
            # Find division for this responder
            for sent in self.sent_emails:
                if sent.coach_email.lower() == resp.coach_email.lower():
                    div = sent.division or 'Unknown'
                    divisions[div]['responders'].add(resp.coach_email)
                    break
        
        # Calculate rates
        result = {}
        for div, data in divisions.items():
            coaches = len(data['coaches'])
            responders = len(data['responders'])
            rate = (responders / coaches * 100) if coaches > 0 else 0
            result[div] = {
                'coaches': coaches,
                'responders': responders,
                'rate': round(rate, 1)
            }
        
        return result
    
    def get_recent_responses(self, limit: int = 10) -> List[Dict]:
        """Get most recent responses."""
        sorted_responses = sorted(
            self.responses, 
            key=lambda r: r.received_at, 
            reverse=True
        )[:limit]
        
        return [r.to_dict() for r in sorted_responses]
    
    def get_hot_leads(self, limit: int = 10) -> List[Dict]:
        """
        Get coaches who should be followed up with.
        Prioritizes: contacted multiple times, no response yet, higher divisions.
        """
        # Get coaches who haven't responded
        responded_emails = set(r.coach_email.lower() for r in self.responses)
        
        # Count contacts per coach
        coach_contacts: Dict[str, Dict] = {}
        
        for sent in self.sent_emails:
            email_lower = sent.coach_email.lower()
            if email_lower in responded_emails:
                continue  # Skip responded
            
            if email_lower not in coach_contacts:
                coach_contacts[email_lower] = {
                    'coach_email': sent.coach_email,
                    'coach_name': sent.coach_name,
                    'school': sent.school,
                    'division': sent.division,
                    'times_contacted': 0,
                    'last_contact': sent.sent_at
                }
            
            coach_contacts[email_lower]['times_contacted'] += 1
            if sent.sent_at > coach_contacts[email_lower]['last_contact']:
                coach_contacts[email_lower]['last_contact'] = sent.sent_at
        
        # Score and sort
        def score(coach: Dict) -> int:
            s = 0
            # More contacts = higher priority (up to 3)
            s += min(coach['times_contacted'], 3) * 10
            # Division scoring
            div_scores = {'NAIA': 30, 'JUCO': 30, 'D3': 25, 'D2': 20, 'FCS': 15, 'FBS': 10}
            s += div_scores.get(coach['division'], 5)
            # Recency (contacted in last 7 days = +20, 14 days = +10)
            try:
                last = datetime.fromisoformat(coach['last_contact'].replace('Z', '+00:00'))
                days_ago = (datetime.now() - last.replace(tzinfo=None)).days
                if days_ago <= 7:
                    s += 20
                elif days_ago <= 14:
                    s += 10
            except:
                pass
            return s
        
        leads = sorted(coach_contacts.values(), key=score, reverse=True)[:limit]
        return leads
    
    def check_gmail_for_responses(self, email_address: str, 
                                   app_password: str) -> Tuple[int, List[Dict]]:
        """
        Check Gmail for new responses from coaches we've emailed.
        Returns (new_count, list of new responses)
        """
        # Get all coach emails we've contacted
        coach_emails = list(set(e.coach_email for e in self.sent_emails))
        
        if not coach_emails:
            return 0, []
        
        # Get emails we've already recorded as responses
        known_responses = set(
            (r.coach_email.lower(), r.subject) for r in self.responses
        )
        
        checker = GmailResponseChecker(email_address, app_password)
        
        try:
            raw_responses = checker.check_for_responses(coach_emails)
            
            new_responses = []
            for resp in raw_responses:
                key = (resp['coach_email'].lower(), resp['subject'])
                if key not in known_responses:
                    # New response
                    self.record_response(
                        coach_email=resp['coach_email'],
                        subject=resp['subject'],
                        snippet=resp['snippet'],
                        received_at=resp.get('date', datetime.now().isoformat())
                    )
                    new_responses.append(resp)
            
            return len(new_responses), new_responses
            
        finally:
            checker.disconnect()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_tracker = None

def get_response_tracker() -> ResponseTracker:
    """Get singleton response tracker."""
    global _tracker
    if _tracker is None:
        _tracker = ResponseTracker()
    return _tracker
