"""
config.py - Central configuration for all coach outreach scripts
"""

import os

# ============================================================================
# GOOGLE SHEETS CONFIGURATION
# ============================================================================
SHEET_NAME = 'bardeen'
CREDENTIALS_FILE = 'credentials.json'

# Column names (must match your spreadsheet)
COLUMNS = {
    'school': 'School',
    'url': 'URL',
    'rc_name': 'recruiting coordinator name',
    'ol_name': 'Oline Coach',
    'rc_twitter': 'RC twitter',
    'ol_twitter': 'OC twitter',
    'rc_email': 'RC email',
    'ol_email': 'OC email',
    'rc_contacted': 'RC Contacted',
    'ol_contacted': 'OL Contacted',
    'rc_notes': 'RC Notes',
    'ol_notes': 'OL Notes'
}

# ============================================================================
# EMAIL CONFIGURATION
# ============================================================================
EMAIL_ADDRESS = os.environ.get('OUTREACH_EMAIL', 'your_email@gmail.com')
EMAIL_PASSWORD = os.environ.get('OUTREACH_PASSWORD', 'your_app_password')
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

# Rate limits
MAX_EMAILS_PER_RUN = 50
DELAY_BETWEEN_EMAILS = 3  # seconds

# ============================================================================
# SCRAPING CONFIGURATION
# ============================================================================
BATCH_SIZE = 10  # Process this many before taking a break
MAX_RETRIES = 2
CONFIDENCE_THRESHOLD = 60  # Minimum score to accept an email match

# Delays (in seconds)
MIN_DELAY = 2
MAX_DELAY = 4
LONG_BREAK_MIN = 15
LONG_BREAK_MAX = 25

# ============================================================================
# FILE PATHS
# ============================================================================
CACHE_DIR = 'cache'
LOG_DIR = 'logs'

EMAIL_CACHE_FILE = os.path.join(CACHE_DIR, 'email_cache.json')
TWITTER_CACHE_FILE = os.path.join(CACHE_DIR, 'twitter_cache.json')
PROGRESS_FILE = os.path.join(CACHE_DIR, 'progress.json')
REVIEW_QUEUE_FILE = os.path.join(CACHE_DIR, 'manual_review.json')

# ============================================================================
# YOUR PERSONAL INFO (UPDATE THIS!)
# ============================================================================
PLAYER_INFO = {
    'name': 'Keelan Underwood',
    'class_year': '2026',
    'height': "6'3\"",
    'weight': '295 lbs',
    'positions': 'C, G, and T',
    'highlight_link': 'https://x.com/UnderwoodKeelan/status/1975252755699659008'
}

# ============================================================================
# COACH SEARCH KEYWORDS
# ============================================================================
RC_KEYWORDS = [
    'recruiting coordinator', 'director of recruiting', 
    'recruiting director', 'director of player personnel',
    'player personnel', 'recruiting'
]

OL_KEYWORDS = [
    'offensive line', 'o-line', 'oline', 
    'offensive line coach', 'offensive line coordinator'
]

# Generic emails to skip
GENERIC_EMAIL_PATTERNS = [
    'noreply', 'no-reply', 'donotreply', 'info@', 'admin@',
    'webmaster@', 'support@', 'contact@', 'general@', 'help@',
    'media@', 'tickets@', 'sales@', 'marketing@', 'admissions@',
    'athletics@', 'sports@', 'football@'
]

# ============================================================================
# MESSAGE TEMPLATES
# ============================================================================

# Twitter DM Template
TWITTER_DM_TEMPLATE = """Good Morning Coach {last_name}, my name is {player_name}, class of {class_year}. I'm {height}, {weight}, and I play {positions}. I'd love for you to check out my highlights here: {highlight_link}. I'd really appreciate the chance to connect and talk about {school} football!"""

# Email Templates
EMAIL_TEMPLATES = {
    'RC': {
        'subject': 'Recruiting Inquiry - {school}',
        'body': """Good Morning Coach {last_name},

My name is {player_name}, and I'm a class of {class_year} offensive lineman. I'm {height}, {weight}, and I play {positions}.

I'm very interested in {school}'s football program and would love the opportunity to learn more about your team.

Here's a link to my highlights: {highlight_link}

I'd really appreciate the chance to connect and discuss {school} football!

Thank you for your time,
{player_name}"""
    },
    'OL': {
        'subject': 'Offensive Line Prospect - {school}',
        'body': """Good Morning Coach {last_name},

My name is {player_name}, and I'm a class of {class_year} offensive lineman. I'm {height}, {weight}, and I play {positions}.

I've been following {school}'s offensive line and would love to learn from a coach of your caliber.

Here's a link to my highlights: {highlight_link}

I'd really appreciate the opportunity to connect and discuss what it takes to be part of {school}'s O-Line.

Thank you for your time,
{player_name}"""
    }
}

# Create directories if they don't exist
def ensure_dirs():
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
