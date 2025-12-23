"""
twitter_helper.py - Twitter DM Outreach Helper
============================================================================
Semi-automated Twitter DM sending to college football coaches.

This tool:
- Opens coach Twitter profiles in your browser
- Copies personalized messages to clipboard
- Tracks who has been contacted
- Handles various outcomes (sent, no DMs, wrong profile)

Usage:
    python twitter_helper.py

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

import os
import sys
import time
import webbrowser
import subprocess
import platform
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    import pyperclip
    HAS_CLIPBOARD = True
except ImportError:
    HAS_CLIPBOARD = False

from sheets.manager import SheetsManager, SheetsConfig


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class TwitterConfig:
    """Configuration for Twitter DM helper."""
    # Message templates
    athlete_name: str = "Keelan Underwood"
    graduation_year: str = "2026"
    height: str = "6'3\""
    weight: str = "295 lbs"
    positions: str = "C, G, and T"
    highlight_link: str = "https://x.com/UnderwoodKeelan/status/1975252755699659008"
    
    # Browser
    preferred_browser: str = 'chrome'  # chrome, safari, firefox
    
    # Delays
    open_delay: float = 0.5  # Delay after opening browser


# Default message template
MESSAGE_TEMPLATE = """Good Morning Coach {last_name}, my name is {athlete_name}, class of {grad_year}. I'm {height}, {weight}, and I play {positions}. I'd love for you to check out my highlights here: {highlight_link}. I'd really appreciate the chance to connect and talk about {school} football!"""


# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# BROWSER HELPERS
# ============================================================================

def open_url(url: str, browser: str = 'chrome') -> bool:
    """
    Open URL in specified browser.
    
    Args:
        url: URL to open
        browser: Browser to use (chrome, safari, firefox)
        
    Returns:
        True if successful
    """
    system = platform.system()
    
    try:
        if system == 'Darwin':  # macOS
            if browser == 'chrome':
                chrome_paths = [
                    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
                    '/Applications/Brave Browser.app/Contents/MacOS/Brave Browser',
                ]
                for path in chrome_paths:
                    if os.path.exists(path):
                        subprocess.Popen([path, url])
                        return True
                # Fallback to default
                subprocess.Popen(['open', url])
                return True
                
            elif browser == 'safari':
                subprocess.Popen(['open', '-a', 'Safari', url])
                return True
                
            elif browser == 'firefox':
                subprocess.Popen(['open', '-a', 'Firefox', url])
                return True
                
        elif system == 'Windows':
            if browser == 'chrome':
                chrome_path = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
                if os.path.exists(chrome_path):
                    subprocess.Popen([chrome_path, url])
                    return True
            # Fallback
            os.startfile(url)
            return True
            
        # Fallback for all systems
        webbrowser.open(url)
        return True
        
    except Exception as e:
        logger.error(f"Failed to open URL: {e}")
        return False


def copy_to_clipboard(text: str) -> bool:
    """
    Copy text to clipboard.
    
    Args:
        text: Text to copy
        
    Returns:
        True if successful
    """
    if not HAS_CLIPBOARD:
        logger.warning("pyperclip not installed - cannot copy to clipboard")
        return False
    
    try:
        pyperclip.copy(text)
        return True
    except Exception as e:
        logger.error(f"Failed to copy to clipboard: {e}")
        return False


# ============================================================================
# MESSAGE GENERATION
# ============================================================================

def extract_last_name(full_name: str) -> str:
    """Extract last name from full name."""
    if not full_name:
        return ""
    
    parts = full_name.strip().split()
    if len(parts) == 0:
        return full_name
    
    # Handle "Coach Smith" -> "Smith"
    if parts[0].lower() == 'coach' and len(parts) > 1:
        return parts[-1]
    
    return parts[-1]


def generate_message(
    coach_name: str,
    school_name: str,
    config: TwitterConfig
) -> str:
    """
    Generate personalized DM message.
    
    Args:
        coach_name: Full name of coach
        school_name: Name of school
        config: Configuration with athlete info
        
    Returns:
        Personalized message
    """
    last_name = extract_last_name(coach_name)
    
    message = MESSAGE_TEMPLATE.format(
        last_name=last_name,
        athlete_name=config.athlete_name,
        grad_year=config.graduation_year,
        height=config.height,
        weight=config.weight,
        positions=config.positions,
        highlight_link=config.highlight_link,
        school=school_name,
    )
    
    return message


# ============================================================================
# TWITTER DM HELPER
# ============================================================================

class TwitterDMHelper:
    """
    Interactive Twitter DM helper.
    
    Guides user through sending DMs to coaches:
    1. Opens Twitter profile in browser
    2. Copies personalized message to clipboard
    3. User sends DM manually
    4. Records outcome in spreadsheet
    """
    
    def __init__(
        self,
        config: Optional[TwitterConfig] = None,
        sheets_config: Optional[SheetsConfig] = None
    ):
        """
        Initialize the helper.
        
        Args:
            config: Twitter configuration
            sheets_config: Google Sheets configuration
        """
        self.config = config or TwitterConfig()
        self.sheets_config = sheets_config or SheetsConfig()
        self.sheets = SheetsManager(self.sheets_config)
        
        # Stats
        self.dms_sent = 0
        self.followed_no_dm = 0
        self.wrong_profiles = 0
        self.skipped = 0
    
    def run(self) -> None:
        """Run the interactive DM helper."""
        self._print_header()
        
        # Connect to sheets
        logger.info("📊 Connecting to Google Sheets...")
        if not self.sheets.connect():
            logger.error("❌ Failed to connect to Google Sheets")
            return
        logger.info("✅ Connected\n")
        
        # Get coaches to contact
        coaches = self._get_coaches_to_dm()
        
        if not coaches:
            logger.info("✅ No coaches to DM!")
            return
        
        logger.info(f"📋 Found {len(coaches)} coaches to contact\n")
        
        self._print_instructions()
        input("\nPress ENTER to start...")
        
        try:
            for coach in coaches:
                result = self._process_coach(coach)
                
                if result == 'quit':
                    break
                    
        except KeyboardInterrupt:
            logger.info("\n\n⚠️ Stopped by user")
        
        finally:
            self._print_summary()
            self.sheets.disconnect()
    
    def _print_header(self) -> None:
        """Print welcome header."""
        print("\n" + "=" * 70)
        print("🐦 TWITTER DM HELPER")
        print("   Semi-Automated Coach Outreach")
        print("=" * 70 + "\n")
    
    def _print_instructions(self) -> None:
        """Print usage instructions."""
        print("=" * 70)
        print("INSTRUCTIONS")
        print("=" * 70)
        print("1. I'll open each coach's Twitter in your browser")
        print("2. The personalized message will be copied to your clipboard")
        print("3. Follow the coach, click 'Message', and paste (Cmd+V / Ctrl+V)")
        print()
        print("OPTIONS:")
        print("  ENTER     = Sent DM successfully - mark as contacted")
        print("  N         = No DMs - followed but messages not enabled")
        print("  W         = Wrong Twitter - delete handle & track it")
        print("  S         = Skip without marking")
        print("  Q         = Quit")
        print("=" * 70)
    
    def _get_coaches_to_dm(self) -> List[Dict[str, Any]]:
        """Get list of coaches who haven't been DM'd yet."""
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
                elif 'rc twitter' in h or 'recruiting' in h and 'twitter' in h:
                    col_indices['rc_twitter'] = i
                elif 'oc twitter' in h or 'ol twitter' in h or 'oline' in h and 'twitter' in h:
                    col_indices['ol_twitter'] = i
                elif 'recruiting coordinator' in h and 'name' in h or h == 'rc name' or 'rc' in h and 'name' in h:
                    col_indices['rc_name'] = i
                elif 'oline' in h and 'coach' in h or 'ol' in h and ('name' in h or 'coach' in h):
                    col_indices['ol_name'] = i
                elif 'rc contacted' in h:
                    col_indices['rc_contacted'] = i
                elif 'ol contacted' in h:
                    col_indices['ol_contacted'] = i
                elif 'rc notes' in h:
                    col_indices['rc_notes'] = i
                elif 'ol notes' in h:
                    col_indices['ol_notes'] = i
            
            # Process rows
            for idx, row in enumerate(rows, start=2):
                def safe_get(key: str) -> str:
                    col = col_indices.get(key, -1)
                    if col >= 0 and col < len(row):
                        return row[col].strip()
                    return ""
                
                school = safe_get('school')
                
                # Check RC
                rc_twitter = safe_get('rc_twitter')
                rc_name = safe_get('rc_name')
                rc_contacted = safe_get('rc_contacted')
                
                if rc_twitter and not rc_contacted:
                    coaches.append({
                        'type': 'RC',
                        'row': idx,
                        'school': school,
                        'name': rc_name or 'Coach',
                        'twitter': rc_twitter,
                        'col_contacted': col_indices.get('rc_contacted', -1) + 1,
                        'col_twitter': col_indices.get('rc_twitter', -1) + 1,
                        'col_notes': col_indices.get('rc_notes', -1) + 1,
                    })
                
                # Check OL
                ol_twitter = safe_get('ol_twitter')
                ol_name = safe_get('ol_name')
                ol_contacted = safe_get('ol_contacted')
                
                if ol_twitter and not ol_contacted:
                    coaches.append({
                        'type': 'OL',
                        'row': idx,
                        'school': school,
                        'name': ol_name or 'Coach',
                        'twitter': ol_twitter,
                        'col_contacted': col_indices.get('ol_contacted', -1) + 1,
                        'col_twitter': col_indices.get('ol_twitter', -1) + 1,
                        'col_notes': col_indices.get('ol_notes', -1) + 1,
                    })
            
            return coaches
            
        except Exception as e:
            logger.error(f"Failed to get coaches: {e}")
            return []
    
    def _process_coach(self, coach: Dict[str, Any]) -> str:
        """
        Process a single coach.
        
        Returns:
            'continue', 'skip', or 'quit'
        """
        print("\n" + "=" * 70)
        print(f"🏫 SCHOOL: {coach['school']}")
        print(f"👤 COACH: {coach['name']} ({coach['type']})")
        print(f"🐦 TWITTER: {coach['twitter']}")
        print("=" * 70)
        
        # Generate and copy message
        message = generate_message(coach['name'], coach['school'], self.config)
        
        if copy_to_clipboard(message):
            print("✅ Message copied to clipboard!")
        else:
            print("⚠️ Could not copy to clipboard - here's the message:")
        
        print("\nMESSAGE PREVIEW:")
        print("-" * 70)
        print(message)
        print("-" * 70)
        
        # Open Twitter
        twitter_url = coach['twitter']
        if not twitter_url.startswith('http'):
            twitter_url = f"https://x.com/{twitter_url.lstrip('@')}"
        
        print("\n🌐 Opening Twitter profile...")
        open_url(twitter_url, self.config.preferred_browser)
        time.sleep(self.config.open_delay)
        
        # Get user input
        print("\nENTER=Sent | N=No DMs | W=Wrong Twitter | S=Skip | Q=Quit")
        user_input = input("\n> ").strip().lower()
        
        if user_input == 'q':
            return 'quit'
            
        elif user_input == 's':
            print("⏭️ Skipped")
            self.skipped += 1
            return 'skip'
            
        elif user_input == 'n':
            # Followed but no DMs
            if coach['col_contacted'] > 0:
                self.sheets.update_cell(coach['row'], coach['col_contacted'], 'Followed')
            if coach['col_notes'] > 0:
                self.sheets.update_cell(coach['row'], coach['col_notes'], 'No DMs enabled')
            print("✅ Marked as Followed (No DMs)")
            self.followed_no_dm += 1
            time.sleep(0.5)
            return 'continue'
            
        elif user_input == 'w':
            # Wrong Twitter handle
            old_twitter = coach['twitter']
            if coach['col_twitter'] > 0:
                self.sheets.update_cell(coach['row'], coach['col_twitter'], '')
            if coach['col_notes'] > 0:
                self.sheets.update_cell(coach['row'], coach['col_notes'], f'Wrong Twitter: {old_twitter}')
            print("✅ Deleted Twitter handle and saved to notes")
            self.wrong_profiles += 1
            time.sleep(0.5)
            return 'continue'
            
        else:
            # Assume sent successfully
            timestamp = datetime.now().strftime('%Y-%m-%d')
            if coach['col_contacted'] > 0:
                self.sheets.update_cell(coach['row'], coach['col_contacted'], f'Yes - {timestamp}')
            print("✅ Marked as contacted!")
            self.dms_sent += 1
            time.sleep(0.5)
            return 'continue'
    
    def _print_summary(self) -> None:
        """Print session summary."""
        print("\n" + "=" * 70)
        print("📊 SESSION SUMMARY")
        print("=" * 70)
        print(f"DMs Sent:           {self.dms_sent}")
        print(f"Followed (No DMs):  {self.followed_no_dm}")
        print(f"Wrong Profiles:     {self.wrong_profiles}")
        print(f"Skipped:            {self.skipped}")
        print("=" * 70)
        print("\n🏈 Keep grinding! Good luck with your recruiting!\n")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Twitter DM Helper')
    parser.add_argument('--name', type=str, help='Athlete name')
    parser.add_argument('--year', type=str, help='Graduation year')
    parser.add_argument('--height', type=str, help='Height')
    parser.add_argument('--weight', type=str, help='Weight')
    parser.add_argument('--positions', type=str, help='Positions played')
    parser.add_argument('--highlights', type=str, help='Highlight video URL')
    parser.add_argument('--browser', type=str, default='chrome', 
                       choices=['chrome', 'safari', 'firefox'],
                       help='Browser to use')
    
    args = parser.parse_args()
    
    # Build config from arguments
    config = TwitterConfig(
        preferred_browser=args.browser,
    )
    
    if args.name:
        config.athlete_name = args.name
    if args.year:
        config.graduation_year = args.year
    if args.height:
        config.height = args.height
    if args.weight:
        config.weight = args.weight
    if args.positions:
        config.positions = args.positions
    if args.highlights:
        config.highlight_link = args.highlights
    
    # Run helper
    helper = TwitterDMHelper(config)
    helper.run()


if __name__ == '__main__':
    main()
