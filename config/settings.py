"""
config/settings.py - Centralized Configuration Management
============================================================================
Handles all user-configurable settings with persistence and validation.

Features:
- JSON-based persistence
- Validation with defaults
- Secure credential storage
- Easy access throughout app

Author: Coach Outreach System
Version: 3.0.0
============================================================================
"""

import os
import json
import logging
from typing import Any, Dict, Optional
from dataclasses import dataclass, field, asdict
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

# Config directory
CONFIG_DIR = Path.home() / '.coach_outreach'
CONFIG_FILE = CONFIG_DIR / 'settings.json'
CREDENTIALS_FILE = CONFIG_DIR / 'google_credentials.json'


@dataclass
class AthleteProfile:
    """Athlete information for email templates."""
    name: str = ""
    graduation_year: str = "2026"
    height: str = ""
    weight: str = ""
    positions: str = ""
    high_school: str = ""
    city: str = ""
    state: str = ""
    gpa: str = ""
    sat_act: str = ""
    highlight_url: str = ""
    phone: str = ""
    email: str = ""
    parent_name: str = ""
    parent_email: str = ""
    parent_phone: str = ""
    
    def is_complete(self) -> bool:
        """Check if minimum required fields are filled."""
        return bool(self.name and self.graduation_year and self.positions)
    
    @property
    def city_state(self) -> str:
        """Get formatted city, state."""
        if self.city and self.state:
            return f"{self.city}, {self.state}"
        return self.city or self.state or ""


@dataclass
class EmailSettings:
    """Email/SMTP configuration."""
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    email_address: str = ""
    app_password: str = ""  # Gmail App Password
    
    # Sending settings
    max_per_day: int = 50
    delay_seconds: float = 5.0
    
    # Schedule
    schedule_enabled: bool = False
    schedule_time: str = "09:00"
    
    def is_configured(self) -> bool:
        """Check if email is properly configured."""
        return bool(self.email_address and self.app_password)


@dataclass
class SheetSettings:
    """Google Sheets configuration."""
    spreadsheet_name: str = "bardeen"
    sheet_name: str = "Sheet1"
    credentials_configured: bool = False
    
    # Column mappings (can be customized)
    school_column: str = "School"
    url_column: str = "URL"
    rc_name_column: str = "recruiting coordinator name"
    ol_name_column: str = "Oline Coach"
    rc_twitter_column: str = "RC twitter"
    ol_twitter_column: str = "OC twitter"
    rc_email_column: str = "RC email"
    ol_email_column: str = "OC email"
    rc_contacted_column: str = "RC Contacted"
    ol_contacted_column: str = "OL Contacted"


@dataclass 
class ScraperSettings:
    """Scraper behavior configuration."""
    start_from_bottom: bool = False
    batch_size: int = 10
    auto_continue: bool = False
    
    # Delays (seconds)
    min_delay: float = 2.0
    max_delay: float = 5.0
    batch_break_min: float = 15.0
    batch_break_max: float = 30.0
    
    # Confidence thresholds
    auto_save_threshold: int = 60
    review_threshold: int = 30


@dataclass
class AppSettings:
    """Main application settings."""
    athlete: AthleteProfile = field(default_factory=AthleteProfile)
    email: EmailSettings = field(default_factory=EmailSettings)
    sheets: SheetSettings = field(default_factory=SheetSettings)
    scraper: ScraperSettings = field(default_factory=ScraperSettings)
    
    # App state
    setup_complete: bool = False
    first_run: bool = True
    last_updated: str = ""
    version: str = "3.0.0"
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'athlete': asdict(self.athlete),
            'email': asdict(self.email),
            'sheets': asdict(self.sheets),
            'scraper': asdict(self.scraper),
            'setup_complete': self.setup_complete,
            'first_run': self.first_run,
            'last_updated': self.last_updated,
            'version': self.version,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'AppSettings':
        """Create from dictionary."""
        settings = cls()
        
        if 'athlete' in data:
            settings.athlete = AthleteProfile(**data['athlete'])
        if 'email' in data:
            settings.email = EmailSettings(**data['email'])
        if 'sheets' in data:
            settings.sheets = SheetSettings(**data['sheets'])
        if 'scraper' in data:
            settings.scraper = ScraperSettings(**data['scraper'])
        
        settings.setup_complete = data.get('setup_complete', False)
        settings.first_run = data.get('first_run', True)
        settings.last_updated = data.get('last_updated', '')
        settings.version = data.get('version', '3.0.0')
        
        return settings
    
    def is_ready(self) -> bool:
        """Check if app is ready to use."""
        return (
            self.sheets.credentials_configured and
            self.athlete.is_complete()
        )


class SettingsManager:
    """
    Manages application settings with persistence.
    
    Usage:
        manager = SettingsManager()
        manager.load()
        
        # Access settings
        print(manager.settings.athlete.name)
        
        # Update settings
        manager.settings.athlete.name = "John Doe"
        manager.save()
    """
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self.settings = AppSettings()
        self._ensure_config_dir()
        self.load()
    
    def _ensure_config_dir(self):
        """Create config directory if it doesn't exist."""
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    def load(self) -> bool:
        """Load settings from disk."""
        try:
            if CONFIG_FILE.exists():
                with open(CONFIG_FILE, 'r') as f:
                    data = json.load(f)
                    self.settings = AppSettings.from_dict(data)
                    logger.info("Settings loaded successfully")
                    return True
            else:
                logger.info("No settings file found, using defaults")
                return False
        except Exception as e:
            logger.error(f"Error loading settings: {e}")
            return False
    
    def save(self) -> bool:
        """Save settings to disk."""
        try:
            self.settings.last_updated = datetime.now().isoformat()
            
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.settings.to_dict(), f, indent=2)
            
            logger.info("Settings saved successfully")
            return True
        except Exception as e:
            logger.error(f"Error saving settings: {e}")
            return False
    
    def reset(self):
        """Reset to default settings."""
        self.settings = AppSettings()
        self.save()
    
    def save_google_credentials(self, credentials_json: str) -> bool:
        """Save Google service account credentials."""
        try:
            # Validate JSON
            creds = json.loads(credentials_json)
            
            # Check required fields
            required = ['type', 'project_id', 'private_key', 'client_email']
            for field in required:
                if field not in creds:
                    raise ValueError(f"Missing required field: {field}")
            
            # Save to file
            with open(CREDENTIALS_FILE, 'w') as f:
                json.dump(creds, f, indent=2)
            
            self.settings.sheets.credentials_configured = True
            self.save()
            
            logger.info("Google credentials saved successfully")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"Error saving credentials: {e}")
            return False
    
    def get_credentials_path(self) -> Optional[str]:
        """Get path to credentials file if it exists."""
        if CREDENTIALS_FILE.exists():
            return str(CREDENTIALS_FILE)
        
        # Fall back to local credentials.json
        local_creds = Path('credentials.json')
        if local_creds.exists():
            return str(local_creds)
        
        return None
    
    def has_credentials(self) -> bool:
        """Check if Google credentials are configured."""
        return self.get_credentials_path() is not None


# Global instance
_settings_manager = None

def get_settings() -> AppSettings:
    """Get current settings."""
    global _settings_manager
    if _settings_manager is None:
        _settings_manager = SettingsManager()
    return _settings_manager.settings

def get_settings_manager() -> SettingsManager:
    """Get settings manager instance."""
    global _settings_manager
    if _settings_manager is None:
        _settings_manager = SettingsManager()
    return _settings_manager
