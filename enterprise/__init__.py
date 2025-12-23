"""
Coach Outreach Pro - Enterprise Features Module
Includes: CRM, Reminders, Templates, Follow-ups, Twitter Scraper, Response Tracking
"""

from .crm import CRMManager, Contact, Interaction, PipelineStage
from .reminders import ReminderManager, Reminder, ReminderType
from .schools_expanded import EXPANDED_SCHOOLS, get_all_schools
from .reports import ReportGenerator
from .templates import (
    TemplateManager, EmailTemplate, get_template_manager,
    get_random_template_for_coach, render_email, render_dm,
    RC_TEMPLATES, OC_TEMPLATES, FOLLOWUP_TEMPLATES
)
from .followups import (
    FollowUpManager, EmailRecord, FollowUp, FollowUpConfig,
    get_followup_manager, record_email_sent
)
from .twitter_google_scraper import (
    GoogleTwitterScraper, find_coach_twitter, get_scraper as get_twitter_scraper
)
from .responses import (
    ResponseTracker, SentEmail, Response, GmailResponseChecker,
    get_response_tracker
)

__all__ = [
    # CRM
    'CRMManager', 'Contact', 'Interaction', 'PipelineStage',
    # Reminders
    'ReminderManager', 'Reminder', 'ReminderType',
    # Schools
    'EXPANDED_SCHOOLS', 'get_all_schools',
    # Reports
    'ReportGenerator',
    # Templates
    'TemplateManager', 'EmailTemplate', 'get_template_manager',
    'get_random_template_for_coach', 'render_email', 'render_dm',
    'RC_TEMPLATES', 'OC_TEMPLATES', 'FOLLOWUP_TEMPLATES',
    # Follow-ups
    'FollowUpManager', 'EmailRecord', 'FollowUp', 'FollowUpConfig',
    'get_followup_manager', 'record_email_sent',
    # Twitter Scraper
    'GoogleTwitterScraper', 'find_coach_twitter', 'get_twitter_scraper',
    # Response Tracking
    'ResponseTracker', 'SentEmail', 'Response', 'GmailResponseChecker',
    'get_response_tracker',
]
