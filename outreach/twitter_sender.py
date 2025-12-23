"""
outreach/twitter_sender.py - Twitter/X Direct Message Sender
============================================================================
Sends personalized DMs to coaches via Twitter/X.

Features:
- Selenium-based browser automation
- Template-based messages with variable substitution
- Rate limiting to avoid spam detection
- Tracking of sent messages
- Manual login support (no API needed)

Usage:
    sender = TwitterDMSender()
    sender.login()  # Opens browser for manual login
    sender.send_dm('@coachhandle', 'Hello Coach...')

Author: Coach Outreach System
Version: 3.3.0
============================================================================
"""

import os
import re
import json
import time
import random
import logging
from typing import Optional, Dict, List, Any, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class TwitterConfig:
    """Configuration for Twitter DM sender."""
    # Rate limiting
    max_dms_per_day: int = 20  # Twitter is strict about DM limits
    min_delay_seconds: int = 30  # Minimum delay between DMs
    max_delay_seconds: int = 90  # Maximum delay between DMs
    
    # Browser settings
    headless: bool = False  # Must be False for login
    browser_profile_dir: str = ""  # Store cookies/session
    
    # Message templates
    default_template: str = """Hey Coach {last_name}! 

I'm {athlete_name}, a {graduation_year} OL from {high_school} ({city_state}).

I'm very interested in {school}'s program. Here's my film: {highlight_url}

Would love to connect about opportunities. Thanks!"""


@dataclass 
class DMRecord:
    """Record of a sent DM."""
    handle: str
    school: str
    coach_name: str
    sent_at: str
    message_preview: str  # First 50 chars


class TwitterDMTracker:
    """
    Tracks sent Twitter DMs to prevent duplicates.
    """
    
    def __init__(self, storage_path: str = None):
        if storage_path is None:
            storage_path = os.path.expanduser("~/.coach_outreach/twitter_dms.json")
        self.storage_path = storage_path
        self.sent_dms: Dict[str, DMRecord] = {}
        self.daily_count: int = 0
        self.last_reset_date: str = ""
        self._load()
    
    def _load(self):
        """Load tracking data from disk."""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r') as f:
                    data = json.load(f)
                    self.sent_dms = {
                        k: DMRecord(**v) for k, v in data.get('sent_dms', {}).items()
                    }
                    self.daily_count = data.get('daily_count', 0)
                    self.last_reset_date = data.get('last_reset_date', '')
            except Exception as e:
                logger.error(f"Error loading DM tracker: {e}")
        
        # Reset daily count if new day
        today = date.today().isoformat()
        if self.last_reset_date != today:
            self.daily_count = 0
            self.last_reset_date = today
            self._save()
    
    def _save(self):
        """Save tracking data to disk."""
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        data = {
            'sent_dms': {k: asdict(v) for k, v in self.sent_dms.items()},
            'daily_count': self.daily_count,
            'last_reset_date': self.last_reset_date,
        }
        with open(self.storage_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def has_sent_to(self, handle: str) -> bool:
        """Check if we've already DM'd this handle."""
        handle = handle.lower().lstrip('@')
        return handle in self.sent_dms
    
    def mark_sent(self, handle: str, school: str, coach_name: str, message: str):
        """Mark a DM as sent."""
        handle = handle.lower().lstrip('@')
        self.sent_dms[handle] = DMRecord(
            handle=handle,
            school=school,
            coach_name=coach_name,
            sent_at=datetime.now().isoformat(),
            message_preview=message[:50] + '...' if len(message) > 50 else message
        )
        self.daily_count += 1
        self._save()
    
    def get_daily_count(self) -> int:
        """Get number of DMs sent today."""
        today = date.today().isoformat()
        if self.last_reset_date != today:
            self.daily_count = 0
            self.last_reset_date = today
            self._save()
        return self.daily_count
    
    def get_sent_list(self) -> List[DMRecord]:
        """Get list of all sent DMs."""
        return list(self.sent_dms.values())


# ============================================================================
# TWITTER DM SENDER
# ============================================================================

class TwitterDMSender:
    """
    Sends direct messages via Twitter/X using browser automation.
    
    This uses Selenium to control a browser, allowing:
    - Manual login (no API credentials needed)
    - Session persistence (stay logged in)
    - Human-like behavior
    """
    
    def __init__(self, config: TwitterConfig = None):
        self.config = config or TwitterConfig()
        self.tracker = TwitterDMTracker()
        self.driver = None
        self.logged_in = False
    
    def start_browser(self) -> bool:
        """Start the browser for Twitter automation."""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            
            options = Options()
            
            # Use profile directory to persist cookies/login
            if self.config.browser_profile_dir:
                options.add_argument(f"user-data-dir={self.config.browser_profile_dir}")
            else:
                # Default profile location
                profile_dir = os.path.expanduser("~/.coach_outreach/twitter_profile")
                os.makedirs(profile_dir, exist_ok=True)
                options.add_argument(f"user-data-dir={profile_dir}")
            
            # Make browser look normal
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument("--window-size=1200,800")
            
            if self.config.headless:
                options.add_argument("--headless")
            
            self.driver = webdriver.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return True
            
        except ImportError:
            logger.error("Selenium not installed. Run: pip install selenium")
            return False
        except Exception as e:
            logger.error(f"Failed to start browser: {e}")
            return False
    
    def stop_browser(self):
        """Close the browser."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
        self.logged_in = False
    
    def open_login_page(self) -> bool:
        """
        Open Twitter login page for manual login.
        
        Returns True when user is logged in.
        """
        if not self.driver:
            if not self.start_browser():
                return False
        
        try:
            self.driver.get("https://twitter.com/login")
            return True
        except Exception as e:
            logger.error(f"Failed to open login page: {e}")
            return False
    
    def check_logged_in(self) -> bool:
        """Check if currently logged into Twitter."""
        if not self.driver:
            return False
        
        try:
            # Navigate to home and check for login indicators
            self.driver.get("https://twitter.com/home")
            time.sleep(3)
            
            # Check URL - if redirected to login, not logged in
            if "login" in self.driver.current_url.lower():
                self.logged_in = False
                return False
            
            # Check for compose tweet button or other logged-in indicators
            page_source = self.driver.page_source.lower()
            if "compose" in page_source or "what is happening" in page_source or "post" in page_source:
                self.logged_in = True
                return True
            
            self.logged_in = False
            return False
            
        except Exception as e:
            logger.error(f"Error checking login status: {e}")
            return False
    
    def wait_for_login(self, timeout: int = 300) -> bool:
        """
        Wait for user to complete manual login.
        
        Args:
            timeout: Maximum seconds to wait
            
        Returns:
            True if login successful
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if self.check_logged_in():
                logger.info("Successfully logged into Twitter")
                return True
            time.sleep(5)
        
        logger.warning("Login timeout")
        return False
    
    def send_dm(
        self, 
        handle: str, 
        message: str,
        school: str = "",
        coach_name: str = ""
    ) -> Dict[str, Any]:
        """
        Send a direct message to a Twitter handle.
        
        Args:
            handle: Twitter handle (with or without @)
            message: Message to send
            school: School name (for tracking)
            coach_name: Coach name (for tracking)
            
        Returns:
            Dict with success status and details
        """
        handle = handle.lstrip('@')
        
        # Check if already sent
        if self.tracker.has_sent_to(handle):
            return {
                'success': False,
                'error': 'Already sent DM to this handle',
                'handle': handle
            }
        
        # Check daily limit
        if self.tracker.get_daily_count() >= self.config.max_dms_per_day:
            return {
                'success': False,
                'error': f'Daily DM limit reached ({self.config.max_dms_per_day})',
                'handle': handle
            }
        
        if not self.driver or not self.logged_in:
            return {
                'success': False,
                'error': 'Not logged into Twitter',
                'handle': handle
            }
        
        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.common.keys import Keys
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            # Navigate to DM page for this user
            dm_url = f"https://twitter.com/messages/compose?recipient_id={handle}"
            # Alternative: go to user profile and click message
            profile_url = f"https://twitter.com/{handle}"
            
            self.driver.get(profile_url)
            time.sleep(3)
            
            # Look for message button
            try:
                # Try to find message/DM button
                message_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[@data-testid='sendDMFromProfile']"))
                )
                message_btn.click()
                time.sleep(2)
            except:
                # Try alternative method - direct DM URL
                self.driver.get(f"https://twitter.com/messages/{handle}")
                time.sleep(3)
            
            # Find message input
            try:
                msg_input = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@data-testid='dmComposerTextInput']"))
                )
            except:
                # Try alternative selector
                msg_input = self.driver.find_element(By.XPATH, "//div[@role='textbox']")
            
            # Type message (human-like)
            for char in message:
                msg_input.send_keys(char)
                time.sleep(random.uniform(0.02, 0.08))
            
            time.sleep(1)
            
            # Send message
            try:
                send_btn = self.driver.find_element(By.XPATH, "//button[@data-testid='dmComposerSendButton']")
                send_btn.click()
            except:
                # Try pressing Enter
                msg_input.send_keys(Keys.RETURN)
            
            time.sleep(2)
            
            # Mark as sent
            self.tracker.mark_sent(handle, school, coach_name, message)
            
            logger.info(f"Successfully sent DM to @{handle}")
            
            return {
                'success': True,
                'handle': handle,
                'school': school,
                'coach_name': coach_name
            }
            
        except Exception as e:
            logger.error(f"Failed to send DM to @{handle}: {e}")
            return {
                'success': False,
                'error': str(e),
                'handle': handle
            }
    
    def prepare_message(
        self,
        template: str,
        coach_last_name: str,
        school: str,
        athlete_info: Dict[str, str]
    ) -> str:
        """
        Prepare a message from template with variable substitution.
        
        Args:
            template: Message template with {variables}
            coach_last_name: Coach's last name
            school: School name
            athlete_info: Dict with athlete details
            
        Returns:
            Formatted message
        """
        message = template
        
        # Standard substitutions
        replacements = {
            '{last_name}': coach_last_name,
            '{school}': school,
            '{athlete_name}': athlete_info.get('name', ''),
            '{graduation_year}': athlete_info.get('graduation_year', ''),
            '{height}': athlete_info.get('height', ''),
            '{weight}': athlete_info.get('weight', ''),
            '{positions}': athlete_info.get('positions', ''),
            '{high_school}': athlete_info.get('high_school', ''),
            '{city_state}': athlete_info.get('city_state', ''),
            '{highlight_url}': athlete_info.get('highlight_url', ''),
            '{gpa}': athlete_info.get('gpa', ''),
            '{phone}': athlete_info.get('phone', ''),
        }
        
        for var, value in replacements.items():
            message = message.replace(var, str(value))
        
        return message
    
    def send_to_coaches(
        self,
        coaches: List[Dict[str, str]],
        template: str,
        athlete_info: Dict[str, str],
        callback: Callable = None
    ) -> Dict[str, Any]:
        """
        Send DMs to multiple coaches.
        
        Args:
            coaches: List of dicts with 'handle', 'school', 'name'
            template: Message template
            athlete_info: Athlete details for template
            callback: Optional callback(event, data) for progress
            
        Returns:
            Summary dict with sent/errors counts
        """
        sent = 0
        errors = 0
        skipped = 0
        
        for i, coach in enumerate(coaches):
            handle = coach.get('handle', '').lstrip('@')
            if not handle:
                skipped += 1
                continue
            
            # Check daily limit
            if self.tracker.get_daily_count() >= self.config.max_dms_per_day:
                if callback:
                    callback('limit_reached', {'daily_count': self.tracker.get_daily_count()})
                break
            
            # Check if already sent
            if self.tracker.has_sent_to(handle):
                skipped += 1
                continue
            
            # Prepare message
            last_name = coach.get('name', '').split()[-1] if coach.get('name') else ''
            message = self.prepare_message(template, last_name, coach.get('school', ''), athlete_info)
            
            if callback:
                callback('sending', {
                    'current': i + 1,
                    'total': len(coaches),
                    'handle': handle,
                    'school': coach.get('school', '')
                })
            
            # Send DM
            result = self.send_dm(
                handle=handle,
                message=message,
                school=coach.get('school', ''),
                coach_name=coach.get('name', '')
            )
            
            if result['success']:
                sent += 1
                if callback:
                    callback('sent', result)
            else:
                errors += 1
                if callback:
                    callback('error', result)
            
            # Delay between messages
            delay = random.uniform(
                self.config.min_delay_seconds,
                self.config.max_delay_seconds
            )
            time.sleep(delay)
        
        return {
            'sent': sent,
            'errors': errors,
            'skipped': skipped,
            'daily_total': self.tracker.get_daily_count()
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get DM sending statistics."""
        sent_list = self.tracker.get_sent_list()
        return {
            'total_sent': len(sent_list),
            'sent_today': self.tracker.get_daily_count(),
            'daily_limit': self.config.max_dms_per_day,
            'remaining_today': max(0, self.config.max_dms_per_day - self.tracker.get_daily_count()),
            'recent': [asdict(dm) for dm in sent_list[-10:]]  # Last 10
        }


# ============================================================================
# CONVENIENCE FUNCTION
# ============================================================================

_sender_instance = None

def get_twitter_sender() -> TwitterDMSender:
    """Get the singleton Twitter DM sender."""
    global _sender_instance
    if _sender_instance is None:
        _sender_instance = TwitterDMSender()
    return _sender_instance
