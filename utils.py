"""
utils.py - Shared utilities for all coach outreach scripts
"""

import os
import re
import json
import time
import random
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import *

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(script_name: str) -> logging.Logger:
    """Set up logging to file and console"""
    ensure_dirs()
    
    log_file = os.path.join(LOG_DIR, f'{script_name}_{datetime.now():%Y%m%d}.log')
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Console handler (simpler format)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    
    # Get logger
    logger = logging.getLogger(script_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# ============================================================================
# GOOGLE SHEETS CONNECTION
# ============================================================================

def connect_to_sheet():
    """Connect to Google Sheet and return sheet object"""
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"❌ '{CREDENTIALS_FILE}' not found! Please add your Google Service Account credentials.")
    
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).sheet1
    return sheet

def get_column_indices(headers: List[str]) -> Dict[str, int]:
    """Get column indices for all required columns"""
    indices = {}
    for key, col_name in COLUMNS.items():
        try:
            indices[key] = headers.index(col_name)
        except ValueError:
            indices[key] = -1  # Column not found
    return indices

def safe_get(row: List, index: int, default: str = '') -> str:
    """Safely get a value from a row, extending if necessary"""
    if index < 0:
        return default
    while len(row) <= index:
        row.append('')
    return str(row[index]).strip() if row[index] else default

# ============================================================================
# CACHE MANAGEMENT
# ============================================================================

def load_json_file(filepath: str, default=None):
    """Load JSON file, return default if not found or error"""
    if default is None:
        default = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except:
            return default
    return default

def save_json_file(filepath: str, data):
    """Save data to JSON file"""
    ensure_dirs()
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def load_cache(cache_type: str = 'email') -> Dict:
    """Load cache file"""
    if cache_type == 'email':
        return load_json_file(EMAIL_CACHE_FILE, {})
    elif cache_type == 'twitter':
        return load_json_file(TWITTER_CACHE_FILE, {})
    return {}

def save_cache(cache: Dict, cache_type: str = 'email'):
    """Save cache file"""
    if cache_type == 'email':
        save_json_file(EMAIL_CACHE_FILE, cache)
    elif cache_type == 'twitter':
        save_json_file(TWITTER_CACHE_FILE, cache)

def load_progress() -> Dict:
    """Load progress file"""
    return load_json_file(PROGRESS_FILE, {'last_row': 0, 'script': None})

def save_progress(row: int, script: str):
    """Save progress file"""
    save_json_file(PROGRESS_FILE, {
        'last_row': row,
        'script': script,
        'timestamp': datetime.now().isoformat()
    })

def add_to_review_queue(item: Dict):
    """Add item to manual review queue"""
    queue = load_json_file(REVIEW_QUEUE_FILE, [])
    item['timestamp'] = datetime.now().isoformat()
    queue.append(item)
    save_json_file(REVIEW_QUEUE_FILE, queue)

# ============================================================================
# NAME PROCESSING
# ============================================================================

def normalize_name(name: str) -> str:
    """Clean and normalize a coach name"""
    if not name:
        return ""
    
    # Remove titles
    name = re.sub(r'^(Coach|Mr\.|Mrs\.|Ms\.|Dr\.)\s+', '', name, flags=re.I)
    # Remove suffixes
    name = re.sub(r'\s+(Jr\.?|Sr\.?|II|III|IV|V)$', '', name, flags=re.I)
    # Clean whitespace, newlines, tabs
    name = re.sub(r'[\n\r\t]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def extract_name_parts(name: str) -> Tuple[str, str, str]:
    """Extract first, last, and middle name from full name"""
    normalized = normalize_name(name)
    parts = normalized.split()
    
    if len(parts) == 0:
        return '', '', ''
    elif len(parts) == 1:
        return parts[0], parts[0], ''
    elif len(parts) == 2:
        return parts[0], parts[1], ''
    else:
        return parts[0], parts[-1], ' '.join(parts[1:-1])

def get_first_name(name: str) -> str:
    """Get first name from full name"""
    first, _, _ = extract_name_parts(name)
    return first if first else 'Coach'

def get_last_name(name: str) -> str:
    """Get last name from full name"""
    _, last, _ = extract_name_parts(name)
    return last if last else ''

# ============================================================================
# DELAYS AND RATE LIMITING
# ============================================================================

def smart_delay(min_sec: float = None, max_sec: float = None):
    """Random delay to seem human and avoid rate limits"""
    if min_sec is None:
        min_sec = MIN_DELAY
    if max_sec is None:
        max_sec = MAX_DELAY
    delay = random.uniform(min_sec, max_sec)
    time.sleep(delay)

def long_break():
    """Take a longer break between batches"""
    delay = random.uniform(LONG_BREAK_MIN, LONG_BREAK_MAX)
    print(f"  💤 Taking {int(delay)}s break...")
    time.sleep(delay)

def rate_limited_update(sheet, row: int, col: int, value: str, retries: int = 3):
    """Update a cell with rate limit handling"""
    for attempt in range(retries):
        try:
            sheet.update_cell(row, col, value)
            smart_delay(0.5, 1.5)
            return True
        except Exception as e:
            if '429' in str(e) or 'Quota' in str(e):
                wait = 60 * (attempt + 1)
                print(f"  ⏸️  Rate limit hit. Waiting {wait}s...")
                time.sleep(wait)
            else:
                raise
    return False

# ============================================================================
# MESSAGE PERSONALIZATION
# ============================================================================

def personalize_message(template: str, coach_name: str, school: str) -> str:
    """Personalize a message template with coach and player info"""
    last_name = get_last_name(coach_name) or 'Coach'
    
    return template.format(
        last_name=last_name,
        first_name=get_first_name(coach_name),
        coach_name=coach_name,
        school=school,
        player_name=PLAYER_INFO['name'],
        class_year=PLAYER_INFO['class_year'],
        height=PLAYER_INFO['height'],
        weight=PLAYER_INFO['weight'],
        positions=PLAYER_INFO['positions'],
        highlight_link=PLAYER_INFO['highlight_link']
    )

# ============================================================================
# EMAIL VALIDATION
# ============================================================================

def is_valid_email(email: str) -> bool:
    """Check if email is valid and not generic"""
    if not email or len(email) < 5 or '@' not in email:
        return False
    
    email_lower = email.lower()
    
    # Check for generic patterns
    for pattern in GENERIC_EMAIL_PATTERNS:
        if pattern in email_lower:
            return False
    
    # Basic format check
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        return False
    
    return True

# ============================================================================
# TWITTER URL HANDLING
# ============================================================================

def clean_twitter_url(url: str) -> str:
    """Clean and normalize a Twitter/X URL"""
    if not url:
        return ''
    
    url = url.strip()
    
    # Decode URL encoding
    from urllib.parse import unquote
    if '%' in url:
        url = unquote(url)
    
    # Remove query params and fragments
    url = re.sub(r'\?.*$', '', url)
    url = re.sub(r'#.*$', '', url)
    
    # Remove trailing paths
    url = re.sub(r'/(with_replies|highlights|status|media|likes|followers|following)(/.*)?$', '', url)
    
    # Fix mobile URLs
    url = url.replace('mobile.twitter.com', 'twitter.com')
    url = url.replace('mobile.x.com', 'x.com')
    
    # Remove trailing slash
    url = url.rstrip('/')
    
    # Ensure https
    if url.startswith('twitter.com') or url.startswith('x.com'):
        url = 'https://' + url
    
    # Standardize to x.com
    url = url.replace('https://twitter.com/', 'https://x.com/')
    url = url.replace('http://twitter.com/', 'https://x.com/')
    url = url.replace('http://x.com/', 'https://x.com/')
    
    return url

def is_valid_twitter_url(url: str) -> bool:
    """Check if URL is a valid Twitter profile URL"""
    if not url:
        return False
    pattern = r'^https://(?:twitter\.com|x\.com)/[a-zA-Z0-9_]+$'
    return bool(re.match(pattern, url))

def extract_twitter_handle(url: str) -> str:
    """Extract the handle from a Twitter URL"""
    if not url:
        return ''
    match = re.search(r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)', url)
    return match.group(1) if match else ''

# ============================================================================
# DISPLAY HELPERS
# ============================================================================

def print_header(title: str, char: str = '='):
    """Print a formatted header"""
    width = 70
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}\n")

def print_stats(stats: Dict):
    """Print statistics in a nice format"""
    print("\n" + "=" * 50)
    print("📊 SUMMARY")
    print("=" * 50)
    for key, value in stats.items():
        label = key.replace('_', ' ').title()
        print(f"  {label}: {value}")
    print("=" * 50)
