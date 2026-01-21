"""
Coach Outreach Pro - Enterprise Edition
============================================================================
Professional recruiting outreach platform for college athletes.

Features:
- Intelligent school search with natural language processing
- Smart email campaigns with deduplication
- Twitter/X direct messaging integration
- Automated form filling
- Analytics and tracking dashboard
- Extended athlete profiles
- Auto-send with random timing
- Phone notifications via ntfy.sh
- Gmail API for sending/reading emails (works on Railway)

Version: 8.2.0 Enterprise (Railway/Render Ready)
============================================================================
"""

import os
import sys
import json
import time
import queue
import logging
import threading
import webbrowser
import traceback
import smtplib
import re
import random
import base64
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load .env file for local development (if exists)
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print("Loaded .env file")
except ImportError:
    pass  # dotenv not installed, will use environment variables directly

from flask import Flask, render_template_string, jsonify, request, Response, stream_with_context

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent))

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG_DIR = Path.home() / '.coach_outreach'
CONFIG_FILE = CONFIG_DIR / 'settings.json'
CREDENTIALS_FILE = CONFIG_DIR / 'google_credentials.json'
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# ENVIRONMENT VARIABLES (for deployment - keeps secrets out of code)
# ============================================================================
# These are loaded from environment variables when deployed to Railway/Render
# Locally, they fall back to defaults or .env file

def get_env(key: str, default: str = '') -> str:
    """Get environment variable with fallback to default."""
    return os.environ.get(key, default)

# Sensitive credentials from environment (NEVER hardcode in production)
ENV_EMAIL_ADDRESS = get_env('EMAIL_ADDRESS', '')
ENV_APP_PASSWORD = get_env('APP_PASSWORD', '')
ENV_GOOGLE_CREDENTIALS = get_env('GOOGLE_CREDENTIALS', '')  # JSON string of service account
ENV_NTFY_CHANNEL = get_env('NTFY_CHANNEL', '')

# Gmail API credentials (for Railway - SMTP is blocked)
ENV_GMAIL_CLIENT_ID = get_env('GMAIL_CLIENT_ID', '')
ENV_GMAIL_CLIENT_SECRET = get_env('GMAIL_CLIENT_SECRET', '')
ENV_GMAIL_REFRESH_TOKEN = get_env('GMAIL_REFRESH_TOKEN', '')

def has_gmail_api():
    """Check if Gmail API credentials are configured."""
    return bool(ENV_GMAIL_CLIENT_ID and ENV_GMAIL_CLIENT_SECRET and ENV_GMAIL_REFRESH_TOKEN)

# Athlete info from environment (persists on Railway)
ENV_ATHLETE_NAME = get_env('ATHLETE_NAME', 'Keelan Underwood')
ENV_ATHLETE_EMAIL = get_env('ATHLETE_EMAIL', 'underwoodkeelan@gmail.com')
ENV_ATHLETE_PHONE = get_env('ATHLETE_PHONE', '9107471140')
ENV_ATHLETE_GRAD_YEAR = get_env('ATHLETE_GRAD_YEAR', '2026')
ENV_ATHLETE_HEIGHT = get_env('ATHLETE_HEIGHT', "6'3")
ENV_ATHLETE_WEIGHT = get_env('ATHLETE_WEIGHT', '295')
ENV_ATHLETE_POSITION = get_env('ATHLETE_POSITION', 'OL')
ENV_ATHLETE_GPA = get_env('ATHLETE_GPA', '3.0')
ENV_ATHLETE_SCHOOL = get_env('ATHLETE_SCHOOL', 'The Benjamin School')
ENV_ATHLETE_STATE = get_env('ATHLETE_STATE', 'FL')
ENV_HUDL_LINK = get_env('HUDL_LINK', 'https://x.com/underwoodkeelan/status/1995522905841746075?s=46')
ENV_AUTO_SEND = get_env('AUTO_SEND_ENABLED', 'true').lower() == 'true'
ENV_NOTIFICATIONS = get_env('NOTIFICATIONS_ENABLED', 'true').lower() == 'true'

DEFAULT_SETTINGS = {
    'athlete': {
        'name': ENV_ATHLETE_NAME, 
        'graduation_year': ENV_ATHLETE_GRAD_YEAR, 
        'height': ENV_ATHLETE_HEIGHT, 
        'weight': ENV_ATHLETE_WEIGHT,
        'positions': ENV_ATHLETE_POSITION, 
        'high_school': ENV_ATHLETE_SCHOOL, 
        'city': '', 
        'state': ENV_ATHLETE_STATE,
        'gpa': ENV_ATHLETE_GPA, 
        'highlight_url': ENV_HUDL_LINK, 
        'phone': ENV_ATHLETE_PHONE, 
        'email': ENV_ATHLETE_EMAIL,
    },
    'email': {
        'smtp_server': 'smtp.gmail.com', 'smtp_port': 587,
        'email_address': ENV_EMAIL_ADDRESS, 
        'app_password': ENV_APP_PASSWORD,
        'max_per_day': 100,  # Gmail limit
        'delay_seconds': 3,
        'days_between_emails': 3,  # Wait 3 days before next email to same coach
        'followup_sequence': ['intro', 'followup_1', 'followup_2'],  # Email sequence
        'auto_send_enabled': ENV_AUTO_SEND,
        'auto_send_count': 100,
    },
    'sheets': {'spreadsheet_name': 'bardeen', 'credentials_configured': False},
    'scraper': {'start_from_bottom': False, 'batch_size': 10, 'min_delay': 2.0, 'max_delay': 5.0},
    'notifications': {'enabled': ENV_NOTIFICATIONS, 'channel': ENV_NTFY_CHANNEL},
    'setup_complete': True, 'first_run': False,
}

def load_settings() -> Dict:
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r') as f:
                saved = json.load(f)
                settings = json.loads(json.dumps(DEFAULT_SETTINGS))
                for key in saved:
                    if isinstance(saved[key], dict) and key in settings:
                        settings[key].update(saved[key])
                    else:
                        settings[key] = saved[key]
                
                # ALWAYS prioritize environment variables over saved settings
                if ENV_EMAIL_ADDRESS:
                    settings['email']['email_address'] = ENV_EMAIL_ADDRESS
                if ENV_APP_PASSWORD:
                    settings['email']['app_password'] = ENV_APP_PASSWORD
                if ENV_NTFY_CHANNEL:
                    if 'notifications' not in settings:
                        settings['notifications'] = {}
                    settings['notifications']['channel'] = ENV_NTFY_CHANNEL
                
                # Fix corrupted/empty password - use env var or default if invalid
                if 'email' in settings:
                    pwd = settings['email'].get('app_password')
                    email = settings['email'].get('email_address')
                    if not pwd or pwd is True or pwd == '********' or not isinstance(pwd, str) or len(pwd) < 5:
                        if ENV_APP_PASSWORD:
                            settings['email']['app_password'] = ENV_APP_PASSWORD
                        else:
                            settings['email']['app_password'] = DEFAULT_SETTINGS['email']['app_password']
                        logger.warning("Using env/default app_password (saved was invalid)")
                    if not email or not isinstance(email, str) or '@' not in email:
                        if ENV_EMAIL_ADDRESS:
                            settings['email']['email_address'] = ENV_EMAIL_ADDRESS
                        else:
                            settings['email']['email_address'] = DEFAULT_SETTINGS['email']['email_address']
                        logger.warning("Using env/default email_address (saved was invalid)")
                
                return settings
        except: pass
    
    # No saved settings - return defaults (which already use env vars)
    return json.loads(json.dumps(DEFAULT_SETTINGS))

def save_settings(s: Dict):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(s, f, indent=2)

settings = load_settings()

# ============================================================================
# LOGGING & STATE
# ============================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24).hex()

# Register enterprise blueprint
try:
    from enterprise.routes import enterprise_bp
    app.register_blueprint(enterprise_bp)
    logger.info("Enterprise features loaded")
except ImportError as e:
    logger.warning(f"Enterprise features not available: {e}")

event_queue = queue.Queue()
active_task = None
task_thread = None
stop_requested = False
cached_responses = []  # Store found responses for display

def add_log(msg: str, level: str = 'info'):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'msg': msg, 'level': level}
    event_queue.put({'type': 'log', 'data': entry})
    getattr(logger, level, logger.info)(msg)


# ============================================================================
# GMAIL API FUNCTIONS (for Railway - SMTP is blocked)
# ============================================================================

def get_gmail_service():
    """Get authenticated Gmail API service."""
    if not has_gmail_api():
        return None
    
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        
        creds = Credentials(
            token=None,
            refresh_token=ENV_GMAIL_REFRESH_TOKEN,
            client_id=ENV_GMAIL_CLIENT_ID,
            client_secret=ENV_GMAIL_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token"
        )
        
        service = build('gmail', 'v1', credentials=creds)
        return service
    except Exception as e:
        logger.error(f"Gmail API error: {e}")
        return None


def send_email_gmail_api(to_email: str, subject: str, body: str, from_email: str = None) -> bool:
    """Send email using Gmail API (works on Railway)."""
    service = get_gmail_service()
    if not service:
        logger.error("Gmail API not configured")
        return False
    
    try:
        from_email = from_email or ENV_EMAIL_ADDRESS
        
        # Create message
        message = MIMEMultipart()
        message['to'] = to_email
        message['from'] = from_email
        message['subject'] = subject
        message.attach(MIMEText(body, 'plain'))
        
        # Encode in base64
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        
        # Send via Gmail API
        result = service.users().messages().send(
            userId='me',
            body={'raw': raw}
        ).execute()
        
        logger.info(f"Email sent via Gmail API to {to_email}, ID: {result.get('id')}")
        return True
        
    except Exception as e:
        logger.error(f"Gmail API send error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def check_inbox_gmail_api(query: str = "is:unread") -> list:
    """Check inbox for emails using Gmail API."""
    service = get_gmail_service()
    if not service:
        return []
    
    try:
        results = service.users().messages().list(
            userId='me',
            q=query,
            maxResults=20
        ).execute()
        
        messages = results.get('messages', [])
        emails = []
        
        for msg in messages:
            msg_data = service.users().messages().get(
                userId='me',
                id=msg['id'],
                format='metadata',
                metadataHeaders=['From', 'Subject', 'Date']
            ).execute()
            
            headers = {h['name']: h['value'] for h in msg_data.get('payload', {}).get('headers', [])}
            emails.append({
                'id': msg['id'],
                'from': headers.get('From', ''),
                'subject': headers.get('Subject', ''),
                'date': headers.get('Date', ''),
                'snippet': msg_data.get('snippet', '')
            })
        
        return emails
        
    except Exception as e:
        logger.error(f"Gmail API inbox error: {e}")
        return []


def send_email_auto(to_email: str, subject: str, body: str, from_email: str = None) -> bool:
    """
    Send email using best available method:
    1. Gmail API (if configured - works on Railway)
    2. SMTP (fallback for local/Render)
    """
    from_email = from_email or ENV_EMAIL_ADDRESS
    
    # Try Gmail API first (works on Railway)
    if has_gmail_api():
        logger.info(f"Sending via Gmail API to {to_email}")
        return send_email_gmail_api(to_email, subject, body, from_email)
    
    # Fallback to SMTP
    app_pass = ENV_APP_PASSWORD
    if not from_email or not app_pass:
        logger.error("No email credentials configured")
        return False
    
    try:
        logger.info(f"Sending via SMTP to {to_email}")
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        smtp = smtplib.SMTP('smtp.gmail.com', 587, timeout=30)
        smtp.starttls()
        smtp.login(from_email, app_pass)
        smtp.sendmail(from_email, to_email, msg.as_string())
        smtp.quit()
        
        logger.info(f"Email sent via SMTP to {to_email}")
        return True
        
    except Exception as e:
        logger.error(f"SMTP error: {e}")
        return False


# ============================================================================
# OPTIONAL IMPORTS
# ============================================================================

HAS_SCRAPER = False
HAS_TWITTER_SCRAPER = False
HAS_EMAIL_SCRAPER = False
HAS_SHEETS = False

try:
    from scrapers import CoachScraper, ScraperConfig
    HAS_SCRAPER = True
except Exception as e:
    logger.warning(f"Name scraper unavailable: {e}")

try:
    from scrapers import TwitterScraper, TwitterScraperConfig
    HAS_TWITTER_SCRAPER = True
except Exception as e:
    logger.warning(f"Twitter scraper unavailable: {e}")

try:
    from scrapers import EmailScraper, EmailScraperConfig
    HAS_EMAIL_SCRAPER = True
except Exception as e:
    logger.warning(f"Email scraper unavailable: {e}")

try:
    from sheets.manager import SheetsManager, SheetsConfig
    HAS_SHEETS = True
except Exception as e:
    logger.warning(f"Google Sheets unavailable: {e}")

def get_sheet():
    if not HAS_SHEETS:
        return None
    try:
        # Load saved settings
        config_file = Path.home() / '.coach_outreach' / 'settings.json'
        spreadsheet_name = 'bardeen'  # default
        
        if config_file.exists():
            with open(config_file) as f:
                file_settings = json.load(f)
                spreadsheet_name = file_settings.get('sheets', {}).get('spreadsheet_name', 'bardeen')
        
        # Check for GOOGLE_CREDENTIALS environment variable (Railway deployment)
        credentials_file = 'credentials.json'
        if ENV_GOOGLE_CREDENTIALS:
            # Clean up potential Railway escaping issues
            import tempfile
            creds_str = ENV_GOOGLE_CREDENTIALS.strip()
            
            # If Railway wrapped it in extra quotes, remove them
            if creds_str.startswith('"') and creds_str.endswith('"'):
                creds_str = creds_str[1:-1]
            
            # Replace double-escaped newlines
            creds_str = creds_str.replace('\\\\n', '\\n')
            
            temp_creds = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            temp_creds.write(creds_str)
            temp_creds.close()
            credentials_file = temp_creds.name
            logger.info("Using GOOGLE_CREDENTIALS from environment variable")
        
        # Create config with saved spreadsheet name
        config = SheetsConfig(spreadsheet_name=spreadsheet_name, credentials_file=credentials_file)
        manager = SheetsManager(config=config)
        
        if manager.connect():
            return manager._sheet  # Access internal sheet object
    except Exception as e:
        logger.error(f"Sheets error: {e}")
    return None


def is_railway_deployment():
    """Check if we're running on Railway with env credentials."""
    return bool(ENV_GOOGLE_CREDENTIALS and ENV_EMAIL_ADDRESS and ENV_APP_PASSWORD)

# ============================================================================
# HTML TEMPLATE - ENTERPRISE UI
# ============================================================================


HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Coach Outreach Pro</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        :root {
            --bg: #0f0f14; --bg2: #16161d; --bg3: #1c1c26;
            --border: #2a2a3a; --text: #fff; --muted: #888;
            --accent: #6366f1; --success: #22c55e; --warn: #f59e0b; --err: #ef4444;
        }
        body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }
        
        /* Layout */
        .app { display: flex; flex-direction: column; height: 100vh; }
        header { background: var(--bg2); border-bottom: 1px solid var(--border); padding: 12px 24px; display: flex; align-items: center; justify-content: space-between; }
        header h1 { font-size: 18px; font-weight: 600; }
        .header-actions { display: flex; gap: 12px; align-items: center; }
        .gear-btn { background: none; border: none; color: var(--muted); font-size: 20px; cursor: pointer; padding: 8px; }
        .gear-btn:hover { color: var(--text); }
        
        /* Tabs */
        nav { background: var(--bg2); border-bottom: 1px solid var(--border); display: flex; padding: 0 24px; }
        .tab { padding: 14px 20px; cursor: pointer; color: var(--muted); border-bottom: 2px solid transparent; transition: all 0.2s; }
        .tab:hover { color: var(--text); }
        .tab.active { color: var(--accent); border-bottom-color: var(--accent); }
        
        /* Main content */
        main { flex: 1; overflow-y: auto; padding: 24px; }
        .page { display: none; }
        .page.active { display: block; }
        
        /* Cards */
        .card { background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin-bottom: 16px; }
        .card-header { font-size: 14px; font-weight: 600; color: var(--muted); margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.5px; }
        
        /* Stats grid */
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .stat { background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; padding: 16px; text-align: center; }
        .stat-value { font-size: 32px; font-weight: 700; color: var(--accent); }
        .stat-label { font-size: 12px; color: var(--muted); margin-top: 4px; }
        
        /* Buttons */
        .btn { background: var(--accent); color: white; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 500; }
        .btn:hover { opacity: 0.9; }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .btn-outline { background: transparent; border: 1px solid var(--border); color: var(--text); }
        .btn-sm { padding: 6px 12px; font-size: 13px; }
        .btn-success { background: var(--success); }
        .btn-warn { background: var(--warn); }
        
        /* Inputs */
        input, select, textarea { background: var(--bg); border: 1px solid var(--border); color: var(--text); padding: 10px 12px; border-radius: 6px; width: 100%; font-size: 14px; }
        input:focus, select:focus, textarea:focus { outline: none; border-color: var(--accent); }
        label { display: block; font-size: 13px; color: var(--muted); margin-bottom: 6px; }
        .form-group { margin-bottom: 16px; }
        
        /* Table */
        table { width: 100%; border-collapse: collapse; }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid var(--border); }
        th { color: var(--muted); font-size: 12px; text-transform: uppercase; font-weight: 500; }
        tr:hover { background: var(--bg3); }
        
        /* Modal */
        .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.7); display: none; align-items: center; justify-content: center; z-index: 1000; }
        .modal-overlay.active { display: flex; }
        .modal { background: var(--bg2); border-radius: 12px; padding: 24px; width: 90%; max-width: 600px; max-height: 90vh; overflow-y: auto; }
        .modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
        .modal-title { font-size: 18px; font-weight: 600; }
        .modal-close { background: none; border: none; color: var(--muted); font-size: 24px; cursor: pointer; }
        
        /* Toast */
        .toast { position: fixed; bottom: 24px; right: 24px; background: var(--bg2); border: 1px solid var(--border); padding: 12px 20px; border-radius: 8px; z-index: 2000; animation: slideIn 0.3s; }
        .toast.success { border-color: var(--success); }
        .toast.error { border-color: var(--err); }
        @keyframes slideIn { from { transform: translateX(100px); opacity: 0; } }
        
        /* Template toggle */
        .template-item { display: flex; align-items: center; justify-content: space-between; padding: 12px; border: 1px solid var(--border); border-radius: 6px; margin-bottom: 8px; }
        .template-info { flex: 1; }
        .template-name { font-weight: 500; }
        .template-type { font-size: 12px; color: var(--muted); }
        .toggle { position: relative; width: 44px; height: 24px; }
        .toggle input { opacity: 0; width: 0; height: 0; }
        .toggle-slider { position: absolute; inset: 0; background: var(--bg); border: 1px solid var(--border); border-radius: 24px; cursor: pointer; transition: 0.2s; }
        .toggle-slider:before { content: ''; position: absolute; height: 18px; width: 18px; left: 2px; bottom: 2px; background: var(--muted); border-radius: 50%; transition: 0.2s; }
        .toggle input:checked + .toggle-slider { background: var(--accent); border-color: var(--accent); }
        .toggle input:checked + .toggle-slider:before { transform: translateX(20px); background: white; }
        
        /* DM Card */
        .dm-card { background: var(--bg3); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 12px; }
        .dm-header { display: flex; justify-content: space-between; margin-bottom: 12px; }
        .dm-school { font-weight: 600; }
        .dm-coach { color: var(--muted); font-size: 14px; }
        .dm-textarea { min-height: 80px; resize: vertical; margin-bottom: 8px; }
        .char-count { text-align: right; font-size: 12px; color: var(--muted); }
        .char-count.over { color: var(--err); }
        .dm-actions { display: flex; gap: 8px; margin-top: 12px; }
        
        /* Pipeline */
        .pipeline { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; }
        .pipeline-col { background: var(--bg2); border-radius: 8px; padding: 12px; min-height: 300px; }
        .pipeline-title { font-size: 12px; font-weight: 600; color: var(--muted); text-transform: uppercase; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid var(--border); }
        .pipeline-card { background: var(--bg3); border: 1px solid var(--border); border-radius: 6px; padding: 10px; margin-bottom: 8px; cursor: pointer; }
        .pipeline-card:hover { border-color: var(--accent); }
        .pipeline-school { font-weight: 500; font-size: 13px; }
        .pipeline-coach { font-size: 12px; color: var(--muted); }
        
        /* Response list */
        .response-item { display: flex; gap: 12px; padding: 12px 0; border-bottom: 1px solid var(--border); }
        .response-avatar { width: 40px; height: 40px; background: var(--accent); border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: 600; }
        .response-content { flex: 1; }
        .response-school { font-weight: 500; }
        .response-snippet { font-size: 13px; color: var(--muted); margin-top: 4px; }
        .response-time { font-size: 12px; color: var(--muted); }
        
        /* Division stats */
        .div-stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
        .div-stat { background: var(--bg3); padding: 12px; border-radius: 6px; text-align: center; }
        .div-name { font-size: 11px; color: var(--muted); text-transform: uppercase; }
        .div-rate { font-size: 20px; font-weight: 700; margin-top: 4px; }
        
        /* Hot leads */
        .lead-item { display: flex; justify-content: space-between; align-items: center; padding: 10px; border-bottom: 1px solid var(--border); }
        .lead-info { }
        .lead-school { font-weight: 500; }
        .lead-coach { font-size: 13px; color: var(--muted); }
        .lead-badge { background: var(--warn); color: #000; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
        
        /* Search */
        .search-box { position: relative; margin-bottom: 16px; }
        .search-box input { padding-left: 40px; }
        .search-icon { position: absolute; left: 12px; top: 50%; transform: translateY(-50%); color: var(--muted); }
        
        /* Utility */
        .flex { display: flex; }
        .gap-2 { gap: 8px; }
        .gap-4 { gap: 16px; }
        .mt-4 { margin-top: 16px; }
        .mb-4 { margin-bottom: 16px; }
        .text-center { text-align: center; }
        .text-muted { color: var(--muted); }
        .text-sm { font-size: 13px; }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    </style>
</head>
<body>
    <div class="app">
        <header>
            <h1>🏈 <span id="header-name">Keelan Underwood</span> <span class="text-muted" style="font-size:0.6em;font-weight:normal;" id="header-info">2026 OL</span></h1>
            <div class="header-actions">
                <span id="connection-status" class="text-sm text-muted">Connecting...</span>
                <button class="gear-btn" onclick="openSettings()">⚙️</button>
            </div>
        </header>
        
        <nav>
            <div class="tab active" data-page="home">Home</div>
            <div class="tab" data-page="find">Find</div>
            <div class="tab" data-page="email">Email</div>
            <div class="tab" data-page="dms">DMs</div>
            <div class="tab" data-page="track">Track</div>
        </nav>
        
        <main>
            <!-- HOME PAGE -->
            <div id="page-home" class="page active">
                <div class="stats">
                    <div class="stat">
                        <div class="stat-value" id="stat-sent">0</div>
                        <div class="stat-label">Emails Sent</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value" id="stat-responses">0</div>
                        <div class="stat-label">Responses</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value" id="stat-rate">0%</div>
                        <div class="stat-label">Response Rate</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value" id="stat-followups">0</div>
                        <div class="stat-label">Follow-ups Due</div>
                    </div>
                </div>
                
                <div class="grid-2">
                    <div class="card">
                        <div class="card-header">Recent Responses</div>
                        <div id="recent-responses">
                            <p class="text-muted text-sm">No responses yet</p>
                        </div>
                        <div class="flex gap-2 mt-4">
                            <button class="btn btn-outline btn-sm" onclick="checkInbox()">Check Inbox</button>
                            <button class="btn btn-outline btn-sm" onclick="testResponseTracking()">Test Tracking</button>
                        </div>
                    </div>
                    
                    <div class="card">
                        <div class="card-header">Auto-Send Settings</div>
                        <div class="form-group" style="margin-bottom:12px;">
                            <label style="display:flex;align-items:center;gap:12px;cursor:pointer;">
                                <input type="checkbox" id="auto-send-toggle" onchange="toggleAutoSend(this.checked)" style="width:18px;height:18px;">
                                <span>Send emails automatically daily</span>
                            </label>
                        </div>
                        <div class="form-group">
                            <label>Emails per day</label>
                            <input type="number" id="auto-send-count" value="100" min="5" max="100" style="width:80px">
                        </div>
                        <p class="text-sm text-muted" style="margin-bottom:12px;">Intro → Follow-up 1 (3 days) → Follow-up 2 (3 days) → Restart (3 days)</p>
                        <p class="text-sm text-muted" style="margin-bottom:12px;">Prioritizes coaches who haven't been emailed longest.</p>
                        <div class="flex gap-2">
                            <button class="btn btn-sm btn-primary" onclick="runAutoSendNow()">Run Now</button>
                        </div>
                        <div id="auto-send-status" class="text-sm text-muted mt-2"></div>
                        
                        <hr style="margin:16px 0;border-color:#333;">
                        <div class="card-header" style="padding:0;margin-bottom:12px;">Phone Notifications</div>
                        <div class="form-group" style="margin-bottom:12px;">
                            <label style="display:flex;align-items:center;gap:12px;cursor:pointer;">
                                <input type="checkbox" id="notify-toggle" onchange="toggleNotifications(this.checked)" style="width:18px;height:18px;">
                                <span>Send daily reminder to my phone</span>
                            </label>
                        </div>
                        <div class="form-group">
                            <label>Notification Channel (for ntfy.sh app)</label>
                            <input type="text" id="notify-channel" placeholder="keelan-coach-outreach" style="width:200px;" onchange="saveNotifyChannel(this.value)">
                        </div>
                        <p class="text-sm text-muted">1. Install <a href="https://ntfy.sh" target="_blank" style="color:#6C5CE7;">ntfy.sh</a> app on your phone</p>
                        <p class="text-sm text-muted">2. Subscribe to your channel name above</p>
                        <p class="text-sm text-muted">3. You'll get a notification when it's time to send emails</p>
                        <button class="btn btn-sm btn-outline mt-2" onclick="testNotification()">Test Notification</button>
                    </div>
                </div>
                
                <div class="card">
                    <div class="card-header">Response Rate by Division</div>
                    <div class="div-stats" id="division-stats">
                        <div class="div-stat"><div class="div-name">FBS</div><div class="div-rate">-</div></div>
                        <div class="div-stat"><div class="div-name">FCS</div><div class="div-rate">-</div></div>
                        <div class="div-stat"><div class="div-name">D2</div><div class="div-rate">-</div></div>
                        <div class="div-stat"><div class="div-name">D3</div><div class="div-rate">-</div></div>
                        <div class="div-stat"><div class="div-name">NAIA</div><div class="div-rate">-</div></div>
                        <div class="div-stat"><div class="div-name">JUCO</div><div class="div-rate">-</div></div>
                    </div>
                </div>
            </div>
            
            <!-- FIND PAGE -->
            <div id="page-find" class="page">
                <div class="card">
                    <div class="card-header">Search Schools</div>
                    <div class="flex gap-4 mb-4">
                        <input type="text" id="school-search" placeholder="Search by name..." style="flex:2">
                        <select id="division-filter" style="flex:1">
                            <option value="">All Divisions</option>
                            <option value="FBS">FBS</option>
                            <option value="FCS">FCS</option>
                            <option value="D2">D2</option>
                            <option value="D3">D3</option>
                            <option value="NAIA">NAIA</option>
                            <option value="JUCO">JUCO</option>
                        </select>
                        <select id="state-filter" style="flex:1">
                            <option value="">All States</option>
                        </select>
                        <button class="btn" onclick="searchSchools()">Search</button>
                    </div>
                    
                    <table id="schools-table">
                        <thead>
                            <tr><th>School</th><th>Division</th><th>State</th><th>Conference</th><th>Actions</th></tr>
                        </thead>
                        <tbody id="schools-body">
                            <tr><td colspan="5" class="text-center text-muted">Search for schools above</td></tr>
                        </tbody>
                    </table>
                </div>
                
                <div class="card mt-4">
                    <div class="card-header">Scraper Tools</div>
                    <p class="text-sm text-muted mb-4">Scrape coach names, emails, and Twitter handles from your Google Sheet schools</p>
                    
                    <div class="grid-2">
                        <div>
                            <div class="form-group">
                                <label>What to scrape</label>
                                <select id="scrape-type">
                                    <option value="emails">Coach Emails</option>
                                    <option value="twitter">Twitter Handles</option>
                                    <option value="names">Coach Names</option>
                                    <option value="all">All Info</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label>Schools to scrape</label>
                                <select id="scrape-scope">
                                    <option value="missing">Only missing data</option>
                                    <option value="all">All schools</option>
                                    <option value="selected">Selected school only</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label>Batch size</label>
                                <input type="number" id="scrape-batch" value="10" min="1" max="50">
                            </div>
                        </div>
                        <div>
                            <div class="form-group">
                                <label>Selected school (optional)</label>
                                <input type="text" id="scrape-school" placeholder="Enter school name">
                            </div>
                            <button class="btn" onclick="startScraper()">Start Scraping</button>
                            <button class="btn btn-outline" onclick="stopScraper()">Stop</button>
                            <div id="scraper-status" class="mt-4 text-sm"></div>
                        </div>
                    </div>
                    
                    <div id="scraper-log" class="mt-4" style="max-height:200px;overflow:auto;font-family:monospace;font-size:12px;background:var(--bg3);padding:8px;border-radius:4px;"></div>
                </div>
            </div>
            
            <!-- EMAIL PAGE -->
            <div id="page-email" class="page">
                <div class="stats">
                    <div class="stat">
                        <div class="stat-value" id="email-ready">0</div>
                        <div class="stat-label">Ready to Send</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value" id="email-today">0</div>
                        <div class="stat-label">Sent Today</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value" id="email-followups">0</div>
                        <div class="stat-label">Follow-ups Due</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value" id="email-responded">0</div>
                        <div class="stat-label">Responded ✓</div>
                    </div>
                </div>
                
                <div class="card mb-4" style="background:var(--bg3);">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <div>
                            <strong>Email Queue Summary</strong>
                            <div class="text-sm text-muted" id="email-queue-summary">Loading...</div>
                        </div>
                        <div class="flex gap-2">
                            <button class="btn btn-sm btn-outline" onclick="scanPastResponses()">🔍 Scan Past Responses</button>
                            <button class="btn btn-sm btn-outline" onclick="cleanupSheet()">🧹 Cleanup Sheet</button>
                            <button class="btn btn-sm btn-outline" onclick="loadEmailQueueStatus()">↻ Refresh</button>
                        </div>
                    </div>
                </div>
                
                <div class="grid-2">
                    <div class="card">
                        <div class="card-header">Send Emails</div>
                        <div id="auto-send-info" class="mb-4 p-2" style="background:var(--bg3);border-radius:6px;font-size:13px;">
                            <div>Last auto-send: <span id="last-auto-send">Never</span></div>
                            <div>Next scheduled: <span id="next-auto-send">Not scheduled</span></div>
                        </div>
                        <div class="form-group">
                            <label>Max emails to send</label>
                            <input type="number" id="email-limit" value="100" min="1" max="100">
                        </div>
                        <div class="form-group">
                            <label>Template Mode</label>
                            <select id="template-mode">
                                <option value="auto">Auto (sends correct template per coach)</option>
                                <option value="manual">Choose specific template</option>
                            </select>
                        </div>
                        <div id="template-select-wrapper" style="display:none" class="form-group">
                            <label>Select Template</label>
                            <select id="template-select"></select>
                        </div>
                        <p class="text-sm text-muted mb-2">Sends to coaches not emailed recently, oldest first. Each coach gets the next email in their sequence (Intro → Follow-up 1 → Follow-up 2).</p>
                        <div class="flex gap-2">
                            <button class="btn btn-outline" onclick="previewEmail()">Preview</button>
                            <button class="btn btn-outline" onclick="sendTestEmail()">Test (to me)</button>
                            <button class="btn btn-success" onclick="sendEmails()">Send Emails</button>
                        </div>
                        <div id="email-log" class="mt-4 text-sm text-muted"></div>
                    </div>
                    
                    <div class="card">
                        <div class="card-header">Templates</div>
                        <div id="email-templates"></div>
                        <button class="btn btn-outline btn-sm mt-4" onclick="openCreateTemplate('email')">+ Create Template</button>
                    </div>
                </div>
                
                <div class="card">
                    <div class="card-header">Follow-up Queue</div>
                    <div id="followup-queue">
                        <p class="text-muted text-sm">No follow-ups due</p>
                    </div>
                </div>
            </div>
            
            <!-- DMS PAGE -->
            <div id="page-dms" class="page">
                <div class="stats">
                    <div class="stat">
                        <div class="stat-value" id="dm-queue">0</div>
                        <div class="stat-label">In Queue</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value" id="dm-sent">0</div>
                        <div class="stat-label">DMs Sent</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value" id="dm-replied">0</div>
                        <div class="stat-label">Replied ❤️</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value" id="dm-no-handle">0</div>
                        <div class="stat-label">No Twitter</div>
                    </div>
                </div>
                
                <!-- Current Coach Card -->
                <div class="card mb-4" id="current-dm-card">
                    <div class="card-header">Current Coach</div>
                    <div id="current-coach-info" style="padding:16px 0;">
                        <p class="text-muted">Click "Start DM Session" to begin</p>
                    </div>
                    <div id="dm-message-preview" style="background:var(--card);padding:12px;border-radius:8px;margin-bottom:16px;display:none;">
                        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                            <div class="text-sm text-muted">Message (copied to clipboard):</div>
                            <button class="btn btn-sm btn-outline" onclick="reCopyMessage()">📋 Re-copy</button>
                        </div>
                        <div id="dm-message-text" style="white-space:pre-wrap;font-size:13px;"></div>
                    </div>
                    <div id="keyboard-shortcuts" style="background:var(--bg3);padding:10px;border-radius:6px;margin-bottom:12px;display:none;">
                        <div class="text-sm"><strong>Keyboard Shortcuts:</strong> <kbd>M</kbd> = Messaged | <kbd>F</kbd> = Followed Only | <kbd>S</kbd> = Skip | <kbd>W</kbd> = Wrong Twitter | <kbd>C</kbd> = Re-copy</div>
                    </div>
                    <div class="flex gap-2" style="flex-wrap:wrap;">
                        <button class="btn" id="btn-start-dm" onclick="startDMSession()">▶ Start DM Session</button>
                        <button class="btn btn-outline" id="btn-end-dm" onclick="endDMSession()" style="display:none;">⏹ End Session</button>
                        <button class="btn btn-success" id="btn-followed-messaged" onclick="markDM('messaged')" style="display:none;">✓ Messaged (M)</button>
                        <button class="btn btn-outline" id="btn-followed-only" onclick="markDM('followed')" style="display:none;">👤 Followed Only (F)</button>
                        <button class="btn btn-outline" id="btn-skip" onclick="markDM('skipped')" style="display:none;">→ Skip (S)</button>
                        <button class="btn" id="btn-wrong-twitter" onclick="markWrongTwitter()" style="display:none;background:var(--err);">✗ Wrong Twitter (W)</button>
                    </div>
                    <div id="dm-progress" class="text-sm text-muted mt-4" style="display:none;"></div>
                </div>
                
                <div class="grid-2">
                    <div class="card">
                        <div class="card-header" style="display:flex;justify-content:space-between;align-items:center;">
                            DM Queue
                            <button class="btn btn-sm btn-outline" onclick="refreshDMQueue()">↻ Refresh</button>
                        </div>
                        <div id="dm-queue-list" style="max-height:300px;overflow-y:auto;">
                            <p class="text-muted text-sm">Loading...</p>
                        </div>
                    </div>
                    
                    <div class="card">
                        <div class="card-header">DM Templates</div>
                        <div id="dm-templates"></div>
                        <button class="btn btn-outline btn-sm mt-4" onclick="openCreateTemplate('dm')">+ Create Template</button>
                        
                        <div class="card-header mt-4">Keyboard Shortcuts</div>
                        <div class="text-sm">
                            <p><kbd>M</kbd> - Followed & Messaged</p>
                            <p><kbd>F</kbd> - Followed Only</p>
                            <p><kbd>S</kbd> or <kbd>→</kbd> - Skip to Next</p>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- TRACK PAGE -->
            <div id="page-track" class="page">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
                    <div class="card-header" style="margin:0;">Pipeline</div>
                    <button class="btn btn-sm" onclick="addToPipeline()">+ Add Coach</button>
                </div>
                
                <!-- Quick Response Tracker -->
                <div class="card mb-4">
                    <div class="card-header">Mark Coach Response</div>
                    <p class="text-sm text-muted mb-2">When a coach replies to your DM or email, mark it here:</p>
                    <div class="flex gap-2" style="flex-wrap:wrap;">
                        <input type="text" id="response-school" placeholder="School name" style="flex:1;min-width:150px;">
                        <select id="response-type" style="width:120px;">
                            <option value="dm_reply">DM Reply</option>
                            <option value="email_reply">Email Reply</option>
                            <option value="interested">Interested!</option>
                            <option value="not_interested">Not Interested</option>
                        </select>
                        <button class="btn btn-sm btn-success" onclick="markCoachResponse()">✓ Mark Response</button>
                    </div>
                    <div id="response-result" class="text-sm mt-2"></div>
                </div>
                
                <div class="pipeline">
                    <div class="pipeline-col">
                        <div class="pipeline-title">Not Contacted</div>
                        <div id="pipeline-not-contacted"></div>
                    </div>
                    <div class="pipeline-col">
                        <div class="pipeline-title">Contacted</div>
                        <div id="pipeline-contacted"></div>
                    </div>
                    <div class="pipeline-col">
                        <div class="pipeline-title">Replied</div>
                        <div id="pipeline-replied"></div>
                    </div>
                    <div class="pipeline-col">
                        <div class="pipeline-title">Interested</div>
                        <div id="pipeline-interested"></div>
                    </div>
                </div>
            </div>
        </main>
    </div>
    
    <!-- Settings Modal -->
    <div class="modal-overlay" id="settings-modal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Settings</span>
                <button class="modal-close" onclick="closeSettings()">&times;</button>
            </div>
            
            <div class="card-header">Athlete Profile</div>
            <div class="grid-2 mb-4">
                <div class="form-group"><label>Name</label><input type="text" id="s-name"></div>
                <div class="form-group"><label>Grad Year</label><input type="text" id="s-year" value="2026"></div>
                <div class="form-group"><label>Position</label><input type="text" id="s-position" placeholder="OL"></div>
                <div class="form-group"><label>High School</label><input type="text" id="s-school"></div>
                <div class="form-group"><label>Height</label><input type="text" id="s-height" placeholder="6'3&quot;"></div>
                <div class="form-group"><label>Weight</label><input type="text" id="s-weight" placeholder="295"></div>
                <div class="form-group"><label>GPA</label><input type="text" id="s-gpa"></div>
                <div class="form-group"><label>Hudl Link</label><input type="text" id="s-hudl"></div>
                <div class="form-group"><label>Phone</label><input type="text" id="s-phone"></div>
                <div class="form-group"><label>Email</label><input type="email" id="s-email"></div>
            </div>
            
            <!-- Railway Banner (hidden by default, shown when on Railway) -->
            <div id="railway-banner" style="display:none;background:linear-gradient(135deg,#6366f1,#8b5cf6);padding:16px;border-radius:8px;margin-bottom:16px;">
                <div style="display:flex;align-items:center;gap:12px;">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="white"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>
                    <div>
                        <div style="font-weight:600;color:white;">Running on Railway</div>
                        <div style="font-size:13px;color:rgba(255,255,255,0.8);">Credentials are securely managed via environment variables</div>
                    </div>
                </div>
            </div>
            
            <!-- Credentials Section (hidden when on Railway) -->
            <div id="credentials-section">
                <div class="card-header">Email Settings</div>
                <div class="grid-2 mb-4">
                    <div class="form-group"><label>Gmail Address</label><input type="email" id="s-gmail"></div>
                    <div class="form-group"><label>App Password</label><input type="password" id="s-gmail-pass"></div>
                </div>
                
                <div class="card-header">Google Sheets Connection</div>
                <div id="sheets-status" class="mb-4" style="padding:10px;background:var(--bg3);border-radius:6px;">
                    <span id="sheets-connection-text">Checking...</span>
                </div>
                <div class="form-group">
                    <label>Spreadsheet Name or ID</label>
                    <input type="text" id="s-sheet" value="bardeen" placeholder="bardeen or spreadsheet ID">
                    <p class="text-sm text-muted">Enter the name (e.g. "bardeen") or the ID from the URL</p>
                </div>
                <div class="form-group">
                    <label>credentials.json</label>
                    <p class="text-sm text-muted mb-4">
                        To connect Google Sheets:<br>
                        1. Go to <a href="https://console.cloud.google.com" target="_blank">Google Cloud Console</a><br>
                        2. Create a project → Enable Google Sheets API<br>
                        3. Create Service Account → Download JSON key<br>
                        4. Share your spreadsheet with the service account email<br>
                        5. Place credentials.json in the app folder
                    </p>
                    <input type="file" id="credentials-file" accept=".json">
                    <button class="btn btn-sm btn-outline mt-4" onclick="uploadCredentials()">Upload Credentials</button>
                </div>
                <button class="btn btn-outline mb-4" onclick="testSheetConnection()">Test Connection</button>
            </div>
            
            <div class="card-header">Response Tracking</div>
            <div class="grid-2 mb-4">
                <div class="form-group"><label>Gmail for checking replies</label><input type="email" id="s-inbox-email" placeholder="Same as above or different"></div>
                <div class="form-group"><label>Gmail App Password</label><input type="password" id="s-inbox-pass" placeholder="For IMAP access"></div>
            </div>
            <button class="btn btn-outline mb-4" onclick="testInboxConnection()">Test Inbox Connection</button>
            
            <button class="btn" onclick="saveSettings()">Save Settings</button>
        </div>
    </div>
    
    <!-- Create Template Modal -->
    <div class="modal-overlay" id="template-modal">
        <div class="modal">
            <div class="modal-header">
                <span class="modal-title">Create Template</span>
                <button class="modal-close" onclick="closeTemplateModal()">&times;</button>
            </div>
            <div class="form-group">
                <label>Type</label>
                <select id="new-tpl-type">
                    <option value="rc">Recruiting Coordinator</option>
                    <option value="ol">O-Line Coach</option>
                    <option value="followup">Follow-up</option>
                    <option value="dm">Twitter DM</option>
                </select>
            </div>
            <div class="form-group"><label>Name</label><input type="text" id="new-tpl-name" placeholder="My Template"></div>
            <div class="form-group" id="tpl-subject-group"><label>Subject</label><input type="text" id="new-tpl-subject" placeholder="{grad_year} {position} - {athlete_name}"></div>
            <div class="form-group"><label>Body</label><textarea id="new-tpl-body" rows="10" placeholder="Coach {coach_name},..."></textarea></div>
            <p class="text-sm text-muted mb-4">Variables: {coach_name}, {school}, {athlete_name}, {position}, {grad_year}, {height}, {weight}, {gpa}, {hudl_link}, {phone}, {email}</p>
            <button class="btn" id="tpl-save-btn" onclick="createTemplate()">Save Template</button>
        </div>
    </div>
    
    <div id="toast" class="toast" style="display:none"></div>

    <script>
        // State
        let settings = {};
        let templates = [];
        let dmQueue = [];
        
        // Tab switching
        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
                tab.classList.add('active');
                document.getElementById('page-' + tab.dataset.page).classList.add('active');
                loadPageData(tab.dataset.page);
            });
        });
        
        // Load page data
        function loadPageData(page) {
            if (page === 'home') loadDashboard();
            if (page === 'find') initSchoolSearch();
            if (page === 'email') { loadEmailPage(); loadTemplates('email'); loadEmailQueueStatus(); }
            if (page === 'dms') { loadDMQueue(); loadTemplates('dm'); }
            if (page === 'track') loadPipeline();
        }
        
        // Dashboard
        async function loadDashboard() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                document.getElementById('stat-sent').textContent = data.emails_sent || 0;
                document.getElementById('stat-responses').textContent = data.responses || 0;
                document.getElementById('stat-rate').textContent = (data.response_rate || 0) + '%';
                document.getElementById('stat-followups').textContent = data.followups_due || 0;
                
                // Load responses
                loadRecentResponses();
                loadHotLeads();
                loadDivisionStats();
            } catch(e) { console.error(e); }
        }
        
        async function loadRecentResponses() {
            try {
                const res = await fetch('/api/responses/recent');
                const data = await res.json();
                const el = document.getElementById('recent-responses');
                if (data.responses && data.responses.length) {
                    el.innerHTML = data.responses.slice(0, 5).map(r => `
                        <div class="response-item">
                            <div class="response-avatar">${(r.school || '?')[0]}</div>
                            <div class="response-content">
                                <div class="response-school">${r.school || r.coach_email}</div>
                                <div class="response-snippet">${r.snippet || ''}</div>
                            </div>
                        </div>
                    `).join('');
                } else {
                    el.innerHTML = '<p class="text-muted text-sm">No responses yet</p>';
                }
            } catch(e) { console.error(e); }
        }
        
        async function loadHotLeads() {
            try {
                const res = await fetch('/api/responses/hot-leads');
                const data = await res.json();
                const el = document.getElementById('hot-leads');
                if (data.leads && data.leads.length) {
                    el.innerHTML = data.leads.slice(0, 5).map(l => `
                        <div class="lead-item">
                            <div class="lead-info">
                                <div class="lead-school">${l.school}</div>
                                <div class="lead-coach">${l.coach_name}</div>
                            </div>
                            <span class="lead-badge">${l.times_contacted}x</span>
                        </div>
                    `).join('');
                } else {
                    el.innerHTML = '<p class="text-muted text-sm">Send some emails first</p>';
                }
            } catch(e) { console.error(e); }
        }
        
        async function loadDivisionStats() {
            try {
                const res = await fetch('/api/responses/by-division');
                const data = await res.json();
                if (data.divisions) {
                    ['FBS','FCS','D2','D3','NAIA','JUCO'].forEach(div => {
                        const stat = data.divisions[div];
                        const el = document.querySelector(`.div-stat:has(.div-name:contains('${div}')) .div-rate`);
                        // Simple approach - update all
                    });
                    const container = document.getElementById('division-stats');
                    container.innerHTML = ['FBS','FCS','D2','D3','NAIA','JUCO'].map(div => {
                        const stat = data.divisions[div] || {rate: 0};
                        return `<div class="div-stat"><div class="div-name">${div}</div><div class="div-rate">${stat.rate || 0}%</div></div>`;
                    }).join('');
                }
            } catch(e) { console.error(e); }
        }
        
        async function loadEmailQueueStatus() {
            try {
                const res = await fetch('/api/email/queue-status');
                const data = await res.json();
                if (data.success) {
                    document.getElementById('email-ready').textContent = data.ready || 0;
                    document.getElementById('email-followups').textContent = data.followups_due || 0;
                    document.getElementById('email-responded').textContent = data.responded || 0;
                    document.getElementById('email-queue-summary').textContent = data.summary || '';
                }
            } catch(e) { console.error(e); }
        }
        
        async function scanPastResponses() {
            showToast('Scanning past 2 weeks for responses...');
            try {
                const res = await fetch('/api/email/scan-past-responses', { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    showToast(data.message, 'success');
                    loadDashboard();
                    loadEmailQueueStatus();
                } else {
                    showToast(data.error || 'Scan failed', 'error');
                }
            } catch(e) { showToast('Error scanning responses', 'error'); }
        }
        
        async function cleanupSheet() {
            showToast('Cleaning up sheet...');
            try {
                const res = await fetch('/api/sheet/cleanup', { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    showToast(data.message, 'success');
                } else {
                    showToast(data.error || 'Cleanup failed', 'error');
                }
            } catch(e) { showToast('Error cleaning sheet', 'error'); }
        }
        
        async function checkInbox() {
            showToast('Checking inbox...');
            try {
                const res = await fetch('/api/email/check-responses', { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    showToast(`Found ${data.new_count || 0} new responses`, 'success');
                    loadDashboard();
                } else {
                    showToast(data.error || 'Failed to check inbox', 'error');
                }
            } catch(e) { showToast('Error checking inbox', 'error'); }
        }
        
        async function testResponseTracking() {
            showToast('Testing response tracking...');
            try {
                const res = await fetch('/api/email/test-tracking');
                const data = await res.json();
                
                let msg = 'Response Tracking Test:\\n\\n';
                msg += `Gmail API Connected: ${data.gmail_connected ? 'Yes' : 'No'}\\n`;
                msg += `Can Check Inbox: ${data.imap_working ? 'Yes' : 'No'}\\n`;
                msg += `Emails Sent (from sheet): ${data.emails_sent || 0}\\n`;
                msg += `Responses Found: ${data.responses_found || 0}\\n`;
                
                if (data.error) msg += `\\nError: ${data.error}`;
                if (data.gmail_api_error) msg += `\\nGmail API Error: ${data.gmail_api_error}`;
                
                alert(msg);
            } catch(e) { 
                alert('Test failed: ' + e.message);
            }
        }
        
        async function toggleAutoSend(enabled) {
            const count = parseInt(document.getElementById('auto-send-count').value) || 100;
            
            try {
                const res = await fetch('/api/auto-send/toggle', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ enabled, count })
                });
                const data = await res.json();
                
                if (data.success) {
                    showToast(enabled ? 'Auto-send enabled - will send daily when app is running' : 'Auto-send disabled', 'success');
                } else {
                    showToast('Failed to toggle auto-send', 'error');
                }
            } catch(e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        async function runAutoSendNow() {
            showToast('Starting auto-send...', 'info');
            try {
                const res = await fetch('/api/auto-send/run-now', { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    showToast('Auto-send started in background', 'success');
                    // Check status after a few seconds
                    setTimeout(checkAutoSendStatus, 5000);
                } else {
                    showToast(data.error || 'Failed to start', 'error');
                }
            } catch(e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        async function checkAutoSendStatus() {
            try {
                const res = await fetch('/api/auto-send/status');
                const data = await res.json();
                if (data.last_result && data.last_result.sent > 0) {
                    showToast(`Auto-send complete: ${data.last_result.sent} emails sent`, 'success');
                    loadStats();
                }
            } catch(e) {}
        }
        
        async function toggleNotifications(enabled) {
            const channel = document.getElementById('notify-channel').value || 'keelan-coach-outreach';
            try {
                await fetch('/api/notifications/toggle', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ enabled, channel })
                });
                showToast(enabled ? 'Phone notifications enabled' : 'Notifications disabled', 'success');
            } catch(e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        async function saveNotifyChannel(channel) {
            try {
                await fetch('/api/notifications/toggle', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ enabled: document.getElementById('notify-toggle').checked, channel })
                });
            } catch(e) {}
        }
        
        async function testNotification() {
            const channel = document.getElementById('notify-channel').value;
            if (!channel) {
                showToast('Enter a channel name first', 'error');
                return;
            }
            showToast('Sending test notification...', 'info');
            try {
                const res = await fetch('/api/notifications/test', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ channel })
                });
                const data = await res.json();
                if (data.success) {
                    showToast('Test notification sent! Check your phone.', 'success');
                } else {
                    showToast(data.error || 'Failed to send', 'error');
                }
            } catch(e) {
                showToast('Error: ' + e.message, 'error');
            }
        }
        
        // School search
        async function initSchoolSearch() {
            // Populate states
            const states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'];
            const sel = document.getElementById('state-filter');
            sel.innerHTML = '<option value="">All States</option>' + states.map(s => `<option value="${s}">${s}</option>`).join('');
        }
        
        async function searchSchools() {
            const query = document.getElementById('school-search').value;
            const division = document.getElementById('division-filter').value;
            const state = document.getElementById('state-filter').value;
            
            try {
                const res = await fetch('/api/schools/search', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ query, division, state })
                });
                const data = await res.json();
                const tbody = document.getElementById('schools-body');
                
                if (data.schools && data.schools.length) {
                    tbody.innerHTML = data.schools.slice(0, 50).map(s => `
                        <tr>
                            <td>${s.name}</td>
                            <td>${s.division || '-'}</td>
                            <td>${s.state || '-'}</td>
                            <td>${s.conference || '-'}</td>
                            <td>
                                <button class="btn btn-sm btn-outline" onclick="addToSheet('${s.name}')">Add</button>
                                <button class="btn btn-sm" onclick="findCoaches('${s.name}')">Find Coaches</button>
                            </td>
                        </tr>
                    `).join('');
                } else {
                    tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">No schools found</td></tr>';
                }
            } catch(e) { console.error(e); }
        }
        
        async function addToSheet(schoolName) {
            try {
                await fetch('/api/schools/add-to-sheet', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ schools: [schoolName] })
                });
                showToast('Added to sheet', 'success');
            } catch(e) { showToast('Failed to add', 'error'); }
        }
        
        // Email
        async function loadEmailPage() {
            try {
                const res = await fetch('/api/spreadsheet');
                const data = await res.json();
                document.getElementById('email-ready').textContent = data.ready_to_send || 0;
                document.getElementById('email-today').textContent = data.sent_today || 0;
                document.getElementById('email-followups').textContent = data.followups_due || 0;
            } catch(e) {}
            // Also load followup queue
            loadFollowupQueue();
        }
        
        async function loadTemplates(type) {
            try {
                const res = await fetch('/api/templates');
                const data = await res.json();
                templates = data.templates || [];
                
                const emailTypes = ['rc', 'ol', 'followup'];
                const dmTypes = ['dm'];
                const filterTypes = type === 'dm' ? dmTypes : emailTypes;
                
                const filtered = templates.filter(t => filterTypes.includes(t.template_type));
                const container = document.getElementById(type === 'dm' ? 'dm-templates' : 'email-templates');
                
                container.innerHTML = filtered.map(t => `
                    <div class="template-item">
                        <div class="template-info">
                            <div class="template-name">${t.name}</div>
                            <div class="template-type">${t.template_type.toUpperCase()} ${t.category === 'user' ? '(Custom)' : ''}</div>
                        </div>
                        <label class="toggle">
                            <input type="checkbox" ${t.enabled ? 'checked' : ''} onchange="toggleTemplate('${t.id}', this.checked)">
                            <span class="toggle-slider"></span>
                        </label>
                        <button class="btn btn-sm btn-outline" style="margin-left:8px" onclick="editTemplate('${t.id}')">✎</button>
                        ${t.category === 'user' ? `<button class="btn btn-sm btn-outline" style="margin-left:4px" onclick="deleteTemplate('${t.id}')">×</button>` : ''}
                    </div>
                `).join('');
                
                // Populate template select
                const sel = document.getElementById('template-select');
                sel.innerHTML = filtered.filter(t => t.enabled).map(t => `<option value="${t.id}">${t.name}</option>`).join('');
            } catch(e) { console.error(e); }
        }
        
        async function editTemplate(id) {
            const t = templates.find(x => x.id === id);
            if (!t) return;
            
            document.getElementById('template-modal').classList.add('active');
            document.getElementById('new-tpl-type').value = t.template_type;
            document.getElementById('new-tpl-name').value = t.name;
            document.getElementById('new-tpl-subject').value = t.subject || '';
            document.getElementById('new-tpl-body').value = t.body || '';
            
            // Store editing ID
            document.getElementById('template-modal').dataset.editId = id;
            document.querySelector('#template-modal .modal-title').textContent = 'Edit Template';
            document.getElementById('tpl-save-btn').textContent = 'Save Changes';
            
            // Show/hide subject based on type
            document.getElementById('tpl-subject-group').style.display = t.template_type === 'dm' ? 'none' : 'block';
        }
        
        async function toggleTemplate(id, enabled) {
            try {
                await fetch('/api/templates/toggle', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ id, enabled })
                });
            } catch(e) { showToast('Failed to toggle', 'error'); }
        }
        
        async function deleteTemplate(id) {
            if (!confirm('Delete this template?')) return;
            try {
                await fetch('/api/templates/' + id, { method: 'DELETE' });
                loadTemplates('email');
                showToast('Deleted', 'success');
            } catch(e) { showToast('Failed to delete', 'error'); }
        }
        
        document.getElementById('template-mode').addEventListener('change', (e) => {
            document.getElementById('template-select-wrapper').style.display = e.target.value === 'manual' ? 'block' : 'none';
        });
        
        async function sendEmails() {
            const limit = document.getElementById('email-limit').value;
            const mode = document.getElementById('template-mode').value;
            const templateId = mode === 'manual' ? document.getElementById('template-select').value : null;
            
            // Confirmation dialog (Fix #9)
            if (!confirm(`Send up to ${limit} emails now?\\n\\nThis will email coaches from your sheet who haven't been contacted yet.`)) {
                return;
            }
            
            document.getElementById('email-log').innerHTML = 'Sending...';
            
            try {
                const res = await fetch('/api/email/send', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ limit: parseInt(limit), template_id: templateId })
                });
                const data = await res.json();
                if (data.success) {
                    document.getElementById('email-log').innerHTML = `✓ Sent: ${data.sent || 0}, Errors: ${data.errors || 0}`;
                } else {
                    document.getElementById('email-log').innerHTML = `Error: ${data.error || 'Unknown error'}`;
                }
                loadEmailPage();
                loadFollowupQueue();
            } catch(e) { 
                document.getElementById('email-log').innerHTML = 'Error sending emails';
            }
        }
        
        // DMs
        async function loadDMQueue() {
            try {
                const res = await fetch('/api/twitter/coaches');
                const data = await res.json();
                dmQueue = (data.coaches || []).filter(c => c.twitter && !c.dm_sent);
                
                document.getElementById('dm-queue').textContent = dmQueue.length;
                document.getElementById('dm-sent').textContent = (data.coaches || []).filter(c => c.dm_sent).length;
                document.getElementById('dm-need-handle').textContent = (data.coaches || []).filter(c => !c.twitter).length;
                
                const container = document.getElementById('dm-queue-list');
                if (dmQueue.length) {
                    container.innerHTML = dmQueue.slice(0, 5).map((c, i) => `
                        <div class="dm-card" data-index="${i}">
                            <div class="dm-header">
                                <div>
                                    <div class="dm-school">${c.school}</div>
                                    <div class="dm-coach">${c.name} • @${c.twitter}</div>
                                </div>
                            </div>
                            <textarea class="dm-textarea" id="dm-text-${i}" oninput="updateCharCount(${i})">${getDMText(c)}</textarea>
                            <div class="char-count" id="char-count-${i}">0/500</div>
                            <div class="dm-actions">
                                <button class="btn btn-sm" onclick="copyAndOpen(${i})">📋 Copy & Open Twitter</button>
                                <button class="btn btn-sm btn-success" onclick="markDMSent(${i})">✓ Mark Sent</button>
                            </div>
                        </div>
                    `).join('');
                    dmQueue.slice(0, 5).forEach((_, i) => updateCharCount(i));
                } else {
                    container.innerHTML = '<p class="text-muted text-sm">No coaches in DM queue</p>';
                }
            } catch(e) { console.error(e); }
        }
        
        function getDMText(coach) {
            const s = settings.athlete || {};
            return `Coach, I'm ${s.name || '[Name]'}, ${s.graduation_year || '2026'} ${s.positions || '[Position]'} (${s.height || '[Height]'}/${s.weight || '[Weight]'}). Interested in ${coach.school}. Film: ${s.highlight_url || '[Hudl]'}`;
        }
        
        function updateCharCount(i) {
            const textarea = document.getElementById('dm-text-' + i);
            const text = textarea.value;
            const el = document.getElementById('char-count-' + i);
            el.textContent = text.length + '/500';
            el.classList.toggle('over', text.length > 500);
            
            // Enforce limit (Fix #15)
            if (text.length > 500) {
                textarea.value = text.substring(0, 500);
                el.textContent = '500/500';
            }
        }
        
        function copyAndOpen(i) {
            const text = document.getElementById('dm-text-' + i).value;
            if (text.length > 500) {
                showToast('DM too long! Max 500 chars', 'error');
                return;
            }
            navigator.clipboard.writeText(text);
            const coach = dmQueue[i];
            // Use x.com (Twitter's new domain) - works better for DMs
            window.open('https://x.com/' + coach.twitter, '_blank');
            showToast('Copied! Opening Twitter...', 'success');
        }
        
        async function markDMSent(i) {
            const coach = dmQueue[i];
            try {
                await fetch('/api/twitter/mark-dm-sent', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ email: coach.email, school: coach.school })
                });
                showToast('Marked as sent', 'success');
                loadDMQueue();
            } catch(e) { showToast('Error', 'error'); }
        }
        
        // Pipeline
        async function loadPipeline() {
            try {
                const res = await fetch('/api/crm/contacts');
                const data = await res.json();
                const contacts = data.contacts || [];
                
                // Map CRM stages to pipeline columns
                const stages = {
                    'prospect': 'pipeline-not-contacted',
                    'contacted': 'pipeline-contacted', 
                    'interested': 'pipeline-replied',
                    'evaluating': 'pipeline-interested',
                    'verbal_offer': 'pipeline-interested',
                    'committed': 'pipeline-interested',
                    'signed': 'pipeline-interested',
                    'declined': 'pipeline-not-contacted'
                };
                
                Object.values(stages).forEach(id => {
                    const el = document.getElementById(id);
                    if (el) el.innerHTML = '';
                });
                
                contacts.forEach(c => {
                    const stage = (c.stage || 'prospect').toLowerCase().replace(' ', '_');
                    const containerId = stages[stage] || 'pipeline-not-contacted';
                    const el = document.getElementById(containerId);
                    if (el) {
                        el.innerHTML += `
                            <div class="pipeline-card" onclick="openContact('${c.id}')">
                                <div class="pipeline-school">${c.school_name || c.school || ''}</div>
                                <div class="pipeline-coach">${c.coach_name || c.name || ''}</div>
                            </div>
                        `;
                    }
                });
            } catch(e) { console.error(e); }
        }
        
        // Settings
        async function loadSettings() {
            try {
                const res = await fetch('/api/settings');
                settings = await res.json();
                
                const a = settings.athlete || {};
                document.getElementById('s-name').value = a.name || '';
                document.getElementById('s-year').value = a.graduation_year || '2026';
                document.getElementById('s-position').value = a.positions || '';
                document.getElementById('s-school').value = a.high_school || '';
                document.getElementById('s-height').value = a.height || '';
                document.getElementById('s-weight').value = a.weight || '';
                document.getElementById('s-gpa').value = a.gpa || '';
                document.getElementById('s-hudl').value = a.highlight_url || '';
                document.getElementById('s-phone').value = a.phone || '';
                document.getElementById('s-email').value = a.email || '';
                
                const e = settings.email || {};
                document.getElementById('s-gmail').value = e.email_address || '';
                document.getElementById('s-gmail-pass').value = e.app_password || '';
                
                document.getElementById('s-sheet').value = (settings.sheets || {}).spreadsheet_name || 'bardeen';
                
                // Load toggle states
                const autoSendToggle = document.getElementById('auto-send-toggle');
                if (autoSendToggle) autoSendToggle.checked = e.auto_send_enabled || false;
                
                const notifyToggle = document.getElementById('notify-toggle');
                if (notifyToggle) notifyToggle.checked = (settings.notifications || {}).enabled || false;
                
                // Update header with athlete info
                document.getElementById('header-name').textContent = a.name || 'Coach Outreach';
                document.getElementById('header-info').textContent = `${a.graduation_year || '2026'} ${a.positions || 'OL'}`;
                
                // Update connection status
                const connected = e.email_address && e.app_password;
                document.getElementById('connection-status').textContent = connected ? '✓ Ready' : 'Setup needed';
            } catch(e) { console.error(e); }
        }
        
        async function saveSettings() {
            settings.athlete = {
                name: document.getElementById('s-name').value,
                graduation_year: document.getElementById('s-year').value,
                positions: document.getElementById('s-position').value,
                high_school: document.getElementById('s-school').value,
                height: document.getElementById('s-height').value,
                weight: document.getElementById('s-weight').value,
                gpa: document.getElementById('s-gpa').value,
                highlight_url: document.getElementById('s-hudl').value,
                phone: document.getElementById('s-phone').value,
                email: document.getElementById('s-email').value,
            };
            
            // Only update password if user actually changed it (not masked value)
            const newPassword = document.getElementById('s-gmail-pass').value;
            settings.email = {
                ...settings.email,
                email_address: document.getElementById('s-gmail').value,
            };
            // Only send password if it's not the masked placeholder
            if (newPassword && newPassword !== '********') {
                settings.email.app_password = newPassword;
            }
            
            settings.sheets = {
                ...settings.sheets,
                spreadsheet_name: document.getElementById('s-sheet').value,
            };
            
            try {
                await fetch('/api/settings', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(settings)
                });
                showToast('Settings saved', 'success');
                closeSettings();
                loadSettings();
            } catch(e) { showToast('Failed to save', 'error'); }
        }
        
        // openSettings is defined below with extra functionality
        function closeSettings() { document.getElementById('settings-modal').classList.remove('active'); }
        
        // Template modal
        function openCreateTemplate(type) {
            const modal = document.getElementById('template-modal');
            modal.classList.add('active');
            modal.dataset.editId = '';  // Clear edit state
            document.querySelector('#template-modal .modal-title').textContent = 'Create Template';
            document.getElementById('tpl-save-btn').textContent = 'Create Template';
            document.getElementById('new-tpl-type').value = type === 'dm' ? 'dm' : 'rc';
            document.getElementById('new-tpl-name').value = '';
            document.getElementById('new-tpl-subject').value = '';
            document.getElementById('new-tpl-body').value = '';
            document.getElementById('tpl-subject-group').style.display = type === 'dm' ? 'none' : 'block';
        }
        function closeTemplateModal() { 
            const modal = document.getElementById('template-modal');
            modal.classList.remove('active'); 
            modal.dataset.editId = '';
        }
        
        document.getElementById('new-tpl-type').addEventListener('change', (e) => {
            document.getElementById('tpl-subject-group').style.display = e.target.value === 'dm' ? 'none' : 'block';
        });
        
        async function createTemplate() {
            const modal = document.getElementById('template-modal');
            const editId = modal.dataset.editId;
            const type = document.getElementById('new-tpl-type').value;
            const name = document.getElementById('new-tpl-name').value;
            const subject = document.getElementById('new-tpl-subject').value;
            const body = document.getElementById('new-tpl-body').value;
            
            if (!name || !body) { showToast('Name and body required', 'error'); return; }
            
            try {
                if (editId) {
                    // Update existing template
                    await fetch('/api/templates/' + editId, {
                        method: 'PUT',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({ name, subject, body })
                    });
                    showToast('Template updated', 'success');
                } else {
                    // Create new template
                    await fetch('/api/templates', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({ template_type: type, name, subject, body })
                    });
                    showToast('Template created', 'success');
                }
                closeTemplateModal();
                loadTemplates(type === 'dm' ? 'dm' : 'email');
            } catch(e) { showToast('Failed to save', 'error'); }
        }
        
        // Find coaches for a school (Fix #3)
        async function findCoaches(schoolName) {
            showToast('Finding coaches for ' + schoolName + '...');
            try {
                // First add school to sheet, then scrape
                await fetch('/api/schools/add-to-sheet', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ schools: [schoolName] })
                });
                
                // Trigger scraper for this school
                const res = await fetch('/api/run', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ task: 'scrape_emails', school: schoolName })
                });
                const data = await res.json();
                if (data.success) {
                    showToast('Scraping started - check Email tab soon', 'success');
                } else {
                    showToast('Added to sheet. Run scraper manually.', 'success');
                }
            } catch(e) { 
                showToast('Added to sheet', 'success');
            }
        }
        
        // Open contact in pipeline (Fix #4)
        function openContact(contactId) {
            // For now, show contact details in alert - could make modal later
            fetch('/api/crm/contacts/' + contactId)
                .then(r => r.json())
                .then(data => {
                    if (data.contact) {
                        const c = data.contact;
                        alert(`${c.coach_name || c.name}\\n${c.school_name || c.school}\\n${c.email || ''}\\n\\nStage: ${c.stage}\\nNotes: ${c.notes || 'None'}`);
                    }
                })
                .catch(() => showToast('Could not load contact', 'error'));
        }
        
        // Add coach to pipeline manually (Fix #14)
        async function addToPipeline() {
            const school = prompt('School name:');
            if (!school) return;
            const coach = prompt('Coach name:');
            if (!coach) return;
            const email = prompt('Coach email (optional):') || '';
            
            try {
                await fetch('/api/crm/contacts', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ school_name: school, coach_name: coach, email: email, stage: 'prospect' })
                });
                showToast('Added to pipeline', 'success');
                loadPipeline();
            } catch(e) { showToast('Failed to add', 'error'); }
        }
        
        // Mark coach response
        async function markCoachResponse() {
            const school = document.getElementById('response-school').value.trim();
            const type = document.getElementById('response-type').value;
            
            if (!school) {
                showToast('Enter school name', 'error');
                return;
            }
            
            try {
                const res = await fetch('/api/coach/response', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ school, response_type: type })
                });
                const data = await res.json();
                
                if (data.success) {
                    const labels = {
                        'dm_reply': 'DM Reply',
                        'email_reply': 'Email Reply', 
                        'interested': 'Interested',
                        'not_interested': 'Not Interested'
                    };
                    document.getElementById('response-result').innerHTML = 
                        '<span style="color:var(--success)">✓ Marked ' + school + ' as: ' + labels[type] + '</span>';
                    document.getElementById('response-school').value = '';
                    showToast('Response recorded!', 'success');
                    loadPipeline();
                } else {
                    document.getElementById('response-result').innerHTML = 
                        '<span style="color:var(--err)">✗ ' + (data.error || 'School not found') + '</span>';
                }
            } catch(e) {
                showToast('Failed to record response', 'error');
            }
        }
        
        // Preview email before sending (Fix #8)
        async function previewEmail() {
            const templateId = document.getElementById('template-select').value;
            try {
                const res = await fetch('/api/email/preview', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ template_id: templateId })
                });
                const data = await res.json();
                if (data.success) {
                    alert('Subject: ' + data.subject + '\\n\\n' + data.body);
                } else {
                    showToast('Could not preview', 'error');
                }
            } catch(e) { showToast('Preview error', 'error'); }
        }
        
        // Load followup queue (Fix #13)
        async function loadFollowupQueue() {
            try {
                const res = await fetch('/api/followups/due');
                const data = await res.json();
                const el = document.getElementById('followup-queue');
                if (data.followups && data.followups.length) {
                    el.innerHTML = data.followups.map(f => `
                        <div class="lead-item">
                            <div class="lead-info">
                                <div class="lead-school">${f.school}</div>
                                <div class="lead-coach">${f.coach_name} - ${f.followup_type}</div>
                            </div>
                            <button class="btn btn-sm" onclick="sendFollowup('${f.id}')">Send</button>
                        </div>
                    `).join('');
                } else {
                    el.innerHTML = '<p class="text-muted text-sm">No follow-ups due</p>';
                }
            } catch(e) { console.error(e); }
        }
        
        async function sendFollowup(id) {
            try {
                await fetch('/api/followups/send', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ followup_id: id })
                });
                showToast('Follow-up sent', 'success');
                loadFollowupQueue();
                loadEmailPage();
            } catch(e) { showToast('Failed to send', 'error'); }
        }
        
        // Refresh DM queue (Fix #12)
        function refreshDMQueue() {
            loadDMQueue();
            showToast('Queue refreshed', 'success');
        }
        
        // ========== NEW SCRAPER FUNCTIONS ==========
        let scraperRunning = false;
        
        async function startScraper() {
            const type = document.getElementById('scrape-type').value;
            const scope = document.getElementById('scrape-scope').value;
            const batch = document.getElementById('scrape-batch').value;
            const school = document.getElementById('scrape-school').value;
            
            scraperRunning = true;
            document.getElementById('scraper-status').innerHTML = '<span style="color:var(--success)">● Running...</span>';
            document.getElementById('scraper-log').innerHTML = 'Starting scraper...';
            
            try {
                const res = await fetch('/api/scraper/start', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ type, scope, batch: parseInt(batch), school })
                });
                const data = await res.json();
                
                if (data.success) {
                    showToast('Scraper started', 'success');
                    pollScraperStatus();
                } else {
                    document.getElementById('scraper-status').innerHTML = '<span style="color:var(--err)">● Error</span>';
                    document.getElementById('scraper-log').innerHTML += 'Error: ' + (data.error || 'Unknown error');
                    scraperRunning = false;
                }
            } catch(e) {
                showToast('Failed to start scraper', 'error');
                scraperRunning = false;
            }
        }
        
        async function stopScraper() {
            scraperRunning = false;
            try {
                await fetch('/api/scraper/stop', { method: 'POST' });
                document.getElementById('scraper-status').innerHTML = '<span style="color:var(--warn)">● Stopped</span>';
                showToast('Scraper stopped', 'success');
            } catch(e) {}
        }
        
        async function pollScraperStatus() {
            if (!scraperRunning) return;
            
            try {
                const res = await fetch('/api/scraper/status');
                const data = await res.json();
                
                if (data.log) {
                    document.getElementById('scraper-log').innerHTML = data.log.join('<br>');
                }
                
                if (data.running) {
                    document.getElementById('scraper-status').innerHTML = 
                        `<span style="color:var(--success)">● Running</span> - ${data.processed || 0}/${data.total || '?'} schools`;
                    setTimeout(pollScraperStatus, 2000);
                } else {
                    document.getElementById('scraper-status').innerHTML = 
                        `<span style="color:var(--muted)">● Complete</span> - ${data.processed || 0} schools processed`;
                    scraperRunning = false;
                    showToast('Scraping complete', 'success');
                }
            } catch(e) {
                setTimeout(pollScraperStatus, 3000);
            }
        }
        
        async function scrapeTwitterHandles() {
            document.getElementById('scrape-type').value = 'twitter';
            document.getElementById('scrape-scope').value = 'missing';
            startScraper();
        }
        
        // ========== TEST EMAIL FUNCTION ==========
        async function sendTestEmail() {
            const templateId = document.getElementById('template-select').value;
            
            if (!confirm('Send a test email to yourself using the selected template?')) return;
            
            try {
                const res = await fetch('/api/email/test', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ template_id: templateId })
                });
                const data = await res.json();
                
                if (data.success) {
                    showToast('Test email sent to ' + data.sent_to, 'success');
                } else {
                    showToast('Failed: ' + (data.error || 'Unknown error'), 'error');
                }
            } catch(e) {
                showToast('Failed to send test email', 'error');
            }
        }
        
        // ========== TWITTER/DM FUNCTIONS ==========
        dmQueue = [];  // Reset queue
        let dmCurrentIndex = -1;
        let dmSessionActive = false;
        
        async function refreshDMQueue() {
            try {
                const res = await fetch('/api/dm/queue');
                const data = await res.json();
                dmQueue = data.queue || [];
                
                document.getElementById('dm-queue').textContent = dmQueue.length;
                document.getElementById('dm-sent').textContent = data.sent || 0;
                document.getElementById('dm-replied').textContent = data.replied || 0;
                document.getElementById('dm-no-handle').textContent = data.no_handle || 0;
                
                const list = document.getElementById('dm-queue-list');
                if (dmQueue.length === 0) {
                    list.innerHTML = '<p class="text-muted text-sm">No coaches in queue yet.</p>';
                } else {
                    list.innerHTML = dmQueue.map((c, i) => `
                        <div class="flex" style="justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);">
                            <div>
                                <strong>${c.coach_name}</strong><br>
                                <span class="text-sm text-muted">${c.school} - @${c.twitter}</span>
                            </div>
                            <span class="text-sm ${i === dmCurrentIndex ? 'text-success' : 'text-muted'}">${i === dmCurrentIndex ? '→ Current' : ''}</span>
                        </div>
                    `).join('');
                }
            } catch(e) {
                console.error(e);
            }
        }
        
        async function startDMSession() {
            await refreshDMQueue();
            if (dmQueue.length === 0) {
                showToast('No coaches in DM queue', 'error');
                return;
            }
            dmSessionActive = true;
            dmCurrentIndex = 0;
            showCurrentCoach();
            
            document.getElementById('btn-start-dm').style.display = 'none';
            document.getElementById('btn-end-dm').style.display = '';
            document.getElementById('btn-followed-messaged').style.display = '';
            document.getElementById('btn-followed-only').style.display = '';
            document.getElementById('btn-skip').style.display = '';
            document.getElementById('btn-wrong-twitter').style.display = '';
            document.getElementById('dm-progress').style.display = '';
            document.getElementById('keyboard-shortcuts').style.display = '';
        }
        
        // Store the Twitter popup window reference
        let twitterPopup = null;
        
        async function showCurrentCoach() {
            if (dmCurrentIndex >= dmQueue.length) {
                endDMSession();
                return;
            }
            
            const coach = dmQueue[dmCurrentIndex];
            const twitterUrl = `https://x.com/${coach.twitter}`;
            
            const info = document.getElementById('current-coach-info');
            info.innerHTML = `
                <div style="font-size:1.5em;font-weight:bold;margin-bottom:8px;">${coach.coach_name}</div>
                <div class="text-muted">${coach.school}</div>
                <div style="margin-top:12px;">
                    <button class="btn btn-sm btn-outline" onclick="openTwitterPopup('${twitterUrl}')">
                        Re-open @${coach.twitter} ↗
                    </button>
                </div>
            `;
            
            // Get DM message and copy to clipboard FIRST (before opening Twitter)
            try {
                const res = await fetch('/api/dm/message', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ coach_name: coach.coach_name, school: coach.school })
                });
                const data = await res.json();
                
                if (data.message) {
                    document.getElementById('dm-message-text').textContent = data.message;
                    document.getElementById('dm-message-preview').style.display = 'block';
                    
                    // Copy to clipboard BEFORE opening popup
                    await copyToClipboard(data.message);
                    showToast('✓ Message copied! Opening Twitter...', 'success');
                    
                    // Now open Twitter in a popup window (positioned to the right)
                    setTimeout(() => {
                        openTwitterPopup(twitterUrl);
                    }, 300);
                }
            } catch(e) {
                console.error(e);
                // Still open Twitter even if message fails
                openTwitterPopup(twitterUrl);
            }
            
            // Update progress
            document.getElementById('dm-progress').textContent = `Coach ${dmCurrentIndex + 1} of ${dmQueue.length}`;
            
            // Refresh queue display to show current marker
            refreshDMQueue();
        }
        
        async function copyToClipboard(text) {
            try {
                await navigator.clipboard.writeText(text);
                return true;
            } catch(e) {
                // Fallback method
                const textarea = document.createElement('textarea');
                textarea.value = text;
                textarea.style.cssText = 'position:fixed;left:-9999px;top:0;';
                document.body.appendChild(textarea);
                textarea.focus();
                textarea.select();
                try {
                    document.execCommand('copy');
                } catch(e2) {
                    console.error('Copy failed:', e2);
                }
                document.body.removeChild(textarea);
                return true;
            }
        }
        
        function openTwitterPopup(url) {
            // Open in a popup window positioned to the right side of screen
            const width = 500;
            const height = 700;
            const left = window.screen.width - width - 50;
            const top = 50;
            
            twitterPopup = window.open(
                url, 
                'twitter_dm',
                `width=${width},height=${height},left=${left},top=${top},resizable=yes,scrollbars=yes`
            );
            
            // Focus back to main window after a short delay so keyboard shortcuts work
            setTimeout(() => {
                window.focus();
            }, 500);
        }
        
        async function markDM(status) {
            if (!dmSessionActive || dmCurrentIndex >= dmQueue.length) return;
            
            const coach = dmQueue[dmCurrentIndex];
            
            try {
                await fetch('/api/dm/mark', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ 
                        coach_name: coach.coach_name,
                        school: coach.school,
                        twitter: coach.twitter,
                        status: status  // 'messaged', 'followed', 'skipped'
                    })
                });
                
                if (status === 'messaged') {
                    showToast('Marked as messaged ✓', 'success');
                } else if (status === 'followed') {
                    showToast('Marked as followed only', 'success');
                }
            } catch(e) {
                console.error(e);
            }
            
            // Close the Twitter popup if it's open
            if (twitterPopup && !twitterPopup.closed) {
                twitterPopup.close();
            }
            
            // Move to next
            dmCurrentIndex++;
            showCurrentCoach();
        }
        
        function endDMSession() {
            dmSessionActive = false;
            dmCurrentIndex = -1;
            
            // Close popup if open
            if (twitterPopup && !twitterPopup.closed) {
                twitterPopup.close();
            }
            
            document.getElementById('current-coach-info').innerHTML = '<p class="text-success">✓ Session ended.</p>';
            document.getElementById('dm-message-preview').style.display = 'none';
            document.getElementById('btn-start-dm').style.display = '';
            document.getElementById('btn-end-dm').style.display = 'none';
            document.getElementById('btn-followed-messaged').style.display = 'none';
            document.getElementById('btn-followed-only').style.display = 'none';
            document.getElementById('btn-skip').style.display = 'none';
            document.getElementById('btn-wrong-twitter').style.display = 'none';
            document.getElementById('keyboard-shortcuts').style.display = 'none';
            document.getElementById('dm-progress').style.display = 'none';
            
            refreshDMQueue();
            showToast('DM session ended', 'success');
        }
        
        // Keyboard shortcuts for DM session
        document.addEventListener('keydown', (e) => {
            if (!dmSessionActive) return;
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
            
            if (e.key === 'm' || e.key === 'M') {
                markDM('messaged');
            } else if (e.key === 'f' || e.key === 'F') {
                markDM('followed');
            } else if (e.key === 's' || e.key === 'S' || e.key === 'ArrowRight') {
                markDM('skipped');
            } else if (e.key === 'c' || e.key === 'C') {
                reCopyMessage();
            } else if (e.key === 'w' || e.key === 'W') {
                markTwitterWrong();
            }
        });
        
        async function reCopyMessage() {
            const msgText = document.getElementById('dm-message-text').textContent;
            if (!msgText) {
                showToast('No message to copy', 'error');
                return;
            }
            try {
                await navigator.clipboard.writeText(msgText);
                showToast('✓ Message re-copied!', 'success');
            } catch(e) {
                const textarea = document.createElement('textarea');
                textarea.value = msgText;
                textarea.style.position = 'fixed';
                textarea.style.left = '-9999px';
                document.body.appendChild(textarea);
                textarea.select();
                document.execCommand('copy');
                document.body.removeChild(textarea);
                showToast('✓ Message re-copied!', 'success');
            }
        }
        
        async function markTwitterWrong() {
            if (!dmSessionActive || dmCurrentIndex >= dmQueue.length) return;
            
            const coach = dmQueue[dmCurrentIndex];
            try {
                await fetch('/api/twitter/mark-wrong', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ 
                        school: coach.school,
                        twitter: coach.twitter,
                        coach_name: coach.coach_name
                    })
                });
                showToast('Marked as wrong handle - will retry next scrape', 'success');
                
                // Move to next
                dmCurrentIndex++;
                showCurrentCoach();
            } catch(e) {
                showToast('Error marking', 'error');
            }
        }
        
        // ========== SHEETS CONNECTION FUNCTIONS ==========
        async function testSheetConnection() {
            showToast('Testing connection...', 'success');
            try {
                const res = await fetch('/api/sheets/test');
                const data = await res.json();
                
                const el = document.getElementById('sheets-connection-text');
                if (data.connected) {
                    el.innerHTML = '<span style="color:var(--success)">● Connected</span> - ' + data.rows + ' rows in sheet';
                    showToast('Connected to Google Sheets!', 'success');
                } else {
                    el.innerHTML = '<span style="color:var(--err)">● Not connected</span> - ' + (data.error || 'Check credentials');
                    showToast('Connection failed: ' + (data.error || 'Unknown'), 'error');
                }
            } catch(e) {
                showToast('Connection test failed', 'error');
            }
        }
        
        async function uploadCredentials() {
            const input = document.getElementById('credentials-file');
            if (!input.files.length) {
                showToast('Select a file first', 'error');
                return;
            }
            
            const file = input.files[0];
            const reader = new FileReader();
            reader.onload = async (e) => {
                try {
                    const content = e.target.result;
                    const res = await fetch('/api/sheets/credentials', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({ credentials: content })
                    });
                    const data = await res.json();
                    if (data.success) {
                        showToast('Credentials saved! Test connection now.', 'success');
                    } else {
                        showToast(data.error || 'Failed to save', 'error');
                    }
                } catch(e) {
                    showToast('Failed to upload credentials', 'error');
                }
            };
            reader.readAsText(file);
        }
        
        async function testInboxConnection() {
            showToast('Testing inbox connection...', 'success');
            try {
                const res = await fetch('/api/inbox/test');
                const data = await res.json();
                if (data.success) {
                    showToast('Inbox connected! Found ' + data.count + ' recent emails', 'success');
                } else {
                    showToast('Failed: ' + (data.error || 'Check credentials'), 'error');
                }
            } catch(e) {
                showToast('Inbox test failed', 'error');
            }
        }
        
        // Check sheet status on settings open
        async function openSettings() { 
            document.getElementById('settings-modal').classList.add('active'); 
            
            // Check if running on Railway
            try {
                const res = await fetch('/api/deployment-info');
                const info = await res.json();
                
                if (info.on_railway) {
                    // Hide credentials section, show Railway banner
                    document.getElementById('credentials-section').style.display = 'none';
                    document.getElementById('railway-banner').style.display = 'block';
                } else {
                    // Show credentials section, hide Railway banner
                    document.getElementById('credentials-section').style.display = 'block';
                    document.getElementById('railway-banner').style.display = 'none';
                }
            } catch(e) {
                // Default to showing credentials section
                document.getElementById('credentials-section').style.display = 'block';
                document.getElementById('railway-banner').style.display = 'none';
            }
            
            testSheetConnection();
            checkTwitterStatus();
        }
        
        // Toast
        function showToast(msg, type = '') {
            const el = document.getElementById('toast');
            el.textContent = msg;
            el.className = 'toast ' + type;
            el.style.display = 'block';
            setTimeout(() => el.style.display = 'none', 3000);
        }
        
        // Check sheet connection and update header
        async function checkSheetConnection() {
            try {
                const res = await fetch('/api/sheets/test');
                const data = await res.json();
                const el = document.getElementById('connection-status');
                if (data.connected) {
                    el.innerHTML = '<span style="color:var(--success)">✓ Sheet Connected</span>';
                } else {
                    el.innerHTML = '<span style="color:var(--err)">✗ Sheet: ' + (data.error || 'Not connected').substring(0, 30) + '</span>';
                }
            } catch(e) {
                document.getElementById('connection-status').textContent = '✗ Sheet error';
            }
        }
        
        // Auto-check inbox for responses (runs silently)
        async function autoCheckInbox() {
            try {
                const res = await fetch('/api/email/check-responses', { method: 'POST' });
                const data = await res.json();
                if (data.success && data.new_count > 0) {
                    showToast(`📬 ${data.new_count} coach(es) replied!`, 'success');
                    loadDashboard();
                }
            } catch(e) { /* silent fail */ }
        }
        
        async function loadAutoSendStatus() {
            try {
                const res = await fetch('/api/auto-send/status');
                const data = await res.json();
                
                document.getElementById('last-auto-send').textContent = 
                    data.last_run ? new Date(data.last_run).toLocaleString() : 'Never';
                document.getElementById('next-auto-send').textContent = 
                    data.next_run ? new Date(data.next_run).toLocaleString() : 
                    (data.enabled ? 'Today (random time)' : 'Auto-send disabled');
                    
                // Update toggle state
                const toggle = document.getElementById('auto-send-toggle');
                if (toggle) toggle.checked = data.enabled;
            } catch(e) { /* silent fail */ }
        }
        
        // Init
        loadSettings();
        loadDashboard();
        checkSheetConnection();
        loadAutoSendStatus();
        loadEmailQueueStatus();
        // Check for responses after 3 seconds (let sheet connect first)
        setTimeout(autoCheckInbox, 3000);
    </script>
</body>
</html>
'''


# ============================================================================
# API ROUTES
# ============================================================================

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route('/api/deployment-info')
def api_deployment_info():
    """Return info about deployment environment."""
    on_railway = is_railway_deployment()
    return jsonify({
        'on_railway': on_railway,
        'env_email_set': bool(ENV_EMAIL_ADDRESS),
        'env_password_set': bool(ENV_APP_PASSWORD),
        'env_google_creds_set': bool(ENV_GOOGLE_CREDENTIALS),
        'env_ntfy_set': bool(ENV_NTFY_CHANNEL),
    })


@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    global settings
    
    if request.method == 'POST':
        data = request.get_json()
        for key in data:
            if isinstance(data[key], dict) and key in settings:
                settings[key].update(data[key])
            else:
                settings[key] = data[key]
        save_settings(settings)
        return jsonify({'success': True})
    
    # Return settings but mask sensitive data for display
    import copy
    safe_settings = copy.deepcopy(settings)
    if 'email' in safe_settings and safe_settings['email'].get('app_password'):
        # Mask password for display - use string not boolean
        safe_settings['email']['app_password_set'] = True
        safe_settings['email']['app_password'] = '********'
    
    return jsonify(safe_settings)


@app.route('/api/stats')
def api_stats():
    """Get dashboard stats from the Google Sheet."""
    stats = {
        'success': True,
        'total_schools': 0,
        'emails_found': 0,
        'emails_sent': 0,
        'responses': 0,
        'response_rate': 0,
        'followups_due': 0,
        'dms_sent': 0
    }
    
    # Get sheet stats
    sheet = get_sheet()
    if sheet:
        try:
            data = sheet.get_all_values()
            if len(data) < 2:
                return jsonify(stats)
            
            headers = [h.lower() for h in data[0]]
            rows = data[1:]
            stats['total_schools'] = len(rows)
            
            # Find column indices
            def find_col(keywords):
                for i, h in enumerate(headers):
                    for kw in keywords:
                        if kw in h:
                            return i
                return -1
            
            rc_email_col = find_col(['rc email'])
            ol_email_col = find_col(['oc email', 'ol email'])
            rc_contacted_col = find_col(['rc contacted'])
            ol_contacted_col = find_col(['ol contacted', 'oc contacted'])
            rc_notes_col = find_col(['rc notes'])
            ol_notes_col = find_col(['ol notes', 'oc notes'])
            
            emails_found = 0
            emails_sent = 0
            responses_count = 0
            dms_sent = 0
            
            for row in rows:
                # Count emails found
                if rc_email_col >= 0 and rc_email_col < len(row) and row[rc_email_col].strip() and '@' in row[rc_email_col]:
                    emails_found += 1
                if ol_email_col >= 0 and ol_email_col < len(row) and row[ol_email_col].strip() and '@' in row[ol_email_col]:
                    emails_found += 1
                
                # Count emails sent (has date in contacted column OR "sent" in notes)
                rc_contacted = row[rc_contacted_col].strip() if rc_contacted_col >= 0 and rc_contacted_col < len(row) else ''
                ol_contacted = row[ol_contacted_col].strip() if ol_contacted_col >= 0 and ol_contacted_col < len(row) else ''
                rc_notes = row[rc_notes_col].strip().lower() if rc_notes_col >= 0 and rc_notes_col < len(row) else ''
                ol_notes = row[ol_notes_col].strip().lower() if ol_notes_col >= 0 and ol_notes_col < len(row) else ''
                
                if rc_contacted or 'sent' in rc_notes:
                    emails_sent += 1
                if ol_contacted or 'sent' in ol_notes:
                    emails_sent += 1
                
                # Count responses (has "responded" or "replied" in notes)
                if 'responded' in rc_notes or 'replied' in rc_notes or 'response' in rc_notes:
                    responses_count += 1
                if 'responded' in ol_notes or 'replied' in ol_notes or 'response' in ol_notes:
                    responses_count += 1
                
                # Count DMs sent
                if 'dm' in rc_notes or 'messaged' in rc_notes:
                    dms_sent += 1
                if 'dm' in ol_notes or 'messaged' in ol_notes:
                    dms_sent += 1
            
            stats['emails_found'] = emails_found
            stats['emails_sent'] = emails_sent
            stats['responses'] = responses_count + len(cached_responses)
            stats['dms_sent'] = dms_sent
            stats['response_rate'] = round((stats['responses'] / emails_sent * 100), 1) if emails_sent > 0 else 0
            
        except Exception as e:
            logger.error(f"Sheet stats error: {e}")
    
    return jsonify(stats)


@app.route('/api/sheet/debug')
def api_sheet_debug():
    """Debug endpoint to see sheet column mapping"""
    sheet = get_sheet()
    if not sheet:
        return jsonify({'success': False, 'error': 'Could not connect to sheet'})
    
    try:
        data = sheet.get_all_values()
        if not data:
            return jsonify({'success': False, 'error': 'Sheet is empty'})
        
        headers = data[0]
        sample_rows = data[1:4] if len(data) > 1 else []  # First 3 data rows
        
        # Detect columns
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower().strip()
                for kw in keywords:
                    if kw in h_lower:
                        return {'index': i, 'header': h}
            return None
        
        columns = {
            'school': find_col(['school']),
            'ol_name': find_col(['oline', 'ol coach', 'o-line', 'offensive line']),
            'rc_name': find_col(['recruiting coordinator', 'recruiting']),
            'ol_email': find_col(['oc email', 'ol email', 'oline email', 'o-line email']),
            'rc_email': find_col(['rc email', 'recruiting email']),
            'ol_contacted': find_col(['ol contacted', 'oc contacted']),
            'rc_contacted': find_col(['rc contacted']),
        }
        
        return jsonify({
            'success': True,
            'total_rows': len(data) - 1,
            'total_columns': len(headers),
            'headers': headers,
            'detected_columns': columns,
            'sample_data': [
                {headers[i]: row[i] if i < len(row) else '' for i in range(min(len(headers), len(row)))}
                for row in sample_rows
            ]
        })
    except Exception as e:
        logger.error(f"Sheet debug error: {e}")
        return jsonify({'success': False, 'error': str(e)})
    
    try:
        from outreach.email_sender import get_analytics
        analytics = get_analytics()
        stats = analytics.get_stats()
        sent = stats.get('emails_sent', 0)
        responses = stats.get('responses_received', 0)
    except:
        pass
    
    return jsonify({
        'total': total,
        'emails': emails,
        'sent': sent,
        'responses': responses
    })


@app.route('/api/schools')
def api_schools():
    try:
        from data.schools import get_school_database
        db = get_school_database()
        schools = [
            {
                'name': s.name,
                'state': s.state,
                'division': s.division,
                'conference': s.conference,
                'public': s.public,
                'enrollment': s.enrollment,
                'region': s.region,
            }
            for s in db.schools
        ]
        return jsonify({'schools': schools})
    except Exception as e:
        logger.error(f"Schools API error: {e}")
        return jsonify({'schools': [], 'error': str(e)})


@app.route('/api/schools/search', methods=['POST'])
def api_schools_search():
    try:
        from data.schools import get_school_database, NaturalLanguageFilter
        
        data = request.get_json()
        query = data.get('query', '')
        
        db = get_school_database()
        
        # Parse natural language query
        filters = NaturalLanguageFilter.parse(query)
        
        # Also check for school name matches
        query_lower = query.lower()
        
        # Apply filters
        if filters:
            results = db.filter(**filters)
        else:
            results = db.schools
        
        # Also filter by name if no structured filters matched
        if not filters:
            results = [s for s in results if query_lower in s.name.lower() or 
                      query_lower in s.conference.lower() or
                      query_lower in s.state.lower()]
        
        schools = [
            {
                'name': s.name,
                'state': s.state,
                'division': s.division,
                'conference': s.conference,
                'public': s.public,
            }
            for s in results
        ]
        
        return jsonify({'schools': schools, 'query': query, 'filters_applied': filters})
    except Exception as e:
        logger.error(f"Search error: {e}")
        return jsonify({'schools': [], 'error': str(e)})


@app.route('/api/schools/add-to-sheet', methods=['POST'])
def api_add_schools_to_sheet():
    sheet = get_sheet()
    if not sheet:
        return jsonify({'success': False, 'error': 'Google Sheets not connected'})
    
    try:
        from data.schools import get_school_database
        
        data = request.get_json()
        school_names = data.get('schools', [])
        
        db = get_school_database()
        added = 0
        
        # Get existing schools
        existing_data = sheet.get_all_values()
        existing_schools = set()
        if len(existing_data) > 1:
            school_col = 0
            for i, h in enumerate(existing_data[0]):
                if 'school' in h.lower():
                    school_col = i
                    break
            existing_schools = {row[school_col].lower() for row in existing_data[1:] if len(row) > school_col}
        
        # Add new schools
        for name in school_names:
            if name.lower() not in existing_schools:
                school = next((s for s in db.schools if s.name == name), None)
                if school:
                    new_row = [school.name, '', '', '', '', '', '', '']
                    sheet.append_row(new_row)
                    added += 1
                    existing_schools.add(name.lower())
        
        add_log(f"Added {added} schools to spreadsheet", 'success')
        return jsonify({'success': True, 'added': added})
    except Exception as e:
        logger.error(f"Add schools error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/spreadsheet')
def api_spreadsheet():
    """Get spreadsheet data with ready_to_send and sent_today stats."""
    sheet = get_sheet()
    if not sheet:
        return jsonify({'rows': [], 'ready_to_send': 0, 'sent_today': 0, 'followups_due': 0, 'error': 'No sheet connection'})
    
    try:
        data = sheet.get_all_values()
        if len(data) < 2:
            return jsonify({'rows': [], 'ready_to_send': 0, 'sent_today': 0, 'followups_due': 0})
        
        headers = data[0]
        rows = data[1:]
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower()
                for kw in keywords:
                    if kw in h_lower:
                        return i
            return -1
        
        school_col = find_col(['school'])
        url_col = find_col(['url', 'staff'])
        ol_col = find_col(['oline', 'ol coach', 'oc '])
        rc_col = find_col(['recruiting', 'rc '])
        ol_email_col = find_col(['oc email', 'ol email'])
        rc_email_col = find_col(['rc email'])
        ol_contacted_col = find_col(['ol contact', 'oc contact'])
        rc_contacted_col = find_col(['rc contact'])
        
        result = []
        ready_to_send = 0
        
        for row in rows:
            def get_val(col):
                if col >= 0 and col < len(row):
                    return row[col].strip()
                return ''
            
            school = get_val(school_col)
            if not school:
                continue
            
            ol_email = get_val(ol_email_col)
            rc_email = get_val(rc_email_col)
            ol_contacted = get_val(ol_contacted_col)
            rc_contacted = get_val(rc_contacted_col)
            
            # Count ready to send (has email, not contacted)
            if ol_email and '@' in ol_email and not ol_contacted:
                ready_to_send += 1
            if rc_email and '@' in rc_email and not rc_contacted:
                ready_to_send += 1
            
            result.append({
                'school': school,
                'url': get_val(url_col),
                'ol_coach': get_val(ol_col),
                'rc': get_val(rc_col),
                'ol_email': ol_email,
                'rc_email': rc_email,
            })
        
        # Get sent today from response tracker
        sent_today = 0
        followups_due = 0
        try:
            from enterprise.responses import get_response_tracker
            from datetime import date
            tracker = get_response_tracker()
            today = date.today().isoformat()
            sent_today = sum(1 for e in tracker.sent_emails if e.sent_at.startswith(today))
        except: pass
        
        try:
            from enterprise.followups import get_followup_manager
            fm = get_followup_manager()
            followups_due = len(fm.get_due_followups())
        except: pass
        
        return jsonify({
            'rows': result, 
            'ready_to_send': ready_to_send,
            'sent_today': sent_today,
            'followups_due': followups_due
        })
    except Exception as e:
        logger.error(f"Spreadsheet error: {e}")
        return jsonify({'rows': [], 'ready_to_send': 0, 'sent_today': 0, 'followups_due': 0, 'error': str(e)})


@app.route('/api/analytics')
def api_analytics():
    try:
        from outreach.email_sender import get_analytics
        analytics = get_analytics()
        return jsonify(analytics.get_stats())
    except Exception as e:
        logger.error(f"Analytics error: {e}")
        return jsonify({'emails_sent': 0, 'schools_contacted': 0, 'responses_received': 0, 'response_rate': 0})


@app.route('/api/analytics/response', methods=['POST'])
def api_record_response():
    try:
        from outreach.email_sender import get_analytics
        
        data = request.get_json()
        school = data.get('school', '')
        response_type = data.get('type', 'response')
        
        analytics = get_analytics()
        
        if response_type == 'offer':
            analytics.record_offer(school)
        else:
            analytics.record_response(school)
        
        add_log(f"Recorded {response_type} from {school}", 'success')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/templates', methods=['GET', 'POST'])
def api_templates():
    """Get all templates or create new user template."""
    try:
        from enterprise.templates import get_template_manager
        manager = get_template_manager()
        
        if request.method == 'POST':
            data = request.get_json()
            template = manager.create_template(
                name=data.get('name', 'New Template'),
                template_type=data.get('template_type', 'rc'),
                subject=data.get('subject', ''),
                body=data.get('body', '')
            )
            return jsonify({'success': True, 'template': template.to_dict()})
        
        templates = manager.get_all_templates()
        return jsonify({'success': True, 'templates': templates})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'templates': []})


@app.route('/api/templates/<template_id>', methods=['GET', 'PUT', 'DELETE'])
def api_template_single(template_id):
    """Get, update, or delete a single template."""
    try:
        from enterprise.templates import get_template_manager
        manager = get_template_manager()
        
        if request.method == 'GET':
            template = manager.get_template(template_id)
            if template:
                return jsonify({'success': True, 'template': template.to_dict()})
            return jsonify({'success': False, 'error': 'Template not found'})
        
        elif request.method == 'PUT':
            data = request.get_json() or {}
            success = manager.update_template(
                template_id,
                name=data.get('name'),
                subject=data.get('subject'),
                body=data.get('body')
            )
            if success:
                template = manager.get_template(template_id)
                return jsonify({'success': True, 'template': template.to_dict()})
            return jsonify({'success': False, 'error': 'Failed to update template'})
        
        elif request.method == 'DELETE':
            success = manager.delete_template(template_id)
            return jsonify({'success': success})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/twitter/connect', methods=['POST'])
def api_twitter_connect():
    try:
        from outreach.twitter_sender import get_twitter_sender
        sender = get_twitter_sender()
        if sender.open_login_page():
            return jsonify({'success': True, 'message': 'Browser opened for login'})
        return jsonify({'success': False, 'error': 'Failed to open browser'})
    except ImportError:
        return jsonify({'success': False, 'error': 'Twitter module not available. Install selenium.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/twitter/status')
def api_twitter_status():
    try:
        from outreach.twitter_sender import get_twitter_sender
        sender = get_twitter_sender()
        logged_in = sender.check_logged_in()
        stats = sender.get_stats()
        return jsonify({
            'logged_in': logged_in,
            'sent_today': stats.get('sent_today', 0),
            'daily_limit': stats.get('daily_limit', 20),
            'total_sent': stats.get('total_sent', 0)
        })
    except ImportError:
        return jsonify({'logged_in': False, 'error': 'Twitter module not available'})
    except Exception as e:
        return jsonify({'logged_in': False, 'error': str(e)})


@app.route('/api/twitter/coaches')
def api_twitter_coaches():
    sheet = get_sheet()
    if not sheet:
        return jsonify({'coaches': []})
    
    try:
        data = sheet.get_all_values()
        if len(data) < 2:
            return jsonify({'coaches': []})
        
        headers = data[0]
        rows = data[1:]
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower()
                for kw in keywords:
                    if kw in h_lower:
                        return i
            return -1
        
        school_col = find_col(['school'])
        ol_col = find_col(['oline', 'ol coach'])
        rc_col = find_col(['recruiting'])
        ol_twitter_col = find_col(['oc twitter', 'ol twitter'])
        rc_twitter_col = find_col(['rc twitter'])
        
        coaches = []
        for row in rows:
            def get_val(col):
                if col >= 0 and col < len(row):
                    return row[col].strip()
                return ''
            
            school = get_val(school_col)
            ol_twitter = get_val(ol_twitter_col)
            rc_twitter = get_val(rc_twitter_col)
            
            if ol_twitter:
                coaches.append({
                    'school': school,
                    'name': get_val(ol_col),
                    'handle': ol_twitter,
                    'type': 'OL',
                    'dm_sent': False
                })
            
            if rc_twitter:
                coaches.append({
                    'school': school,
                    'name': get_val(rc_col),
                    'handle': rc_twitter,
                    'type': 'RC',
                    'dm_sent': False
                })
        
        return jsonify({'coaches': coaches})
    except Exception as e:
        logger.error(f"Twitter coaches error: {e}")
        return jsonify({'coaches': [], 'error': str(e)})


# ============================================================================
# DM WORKFLOW ROUTES
# ============================================================================

@app.route('/api/dm/queue')
def api_dm_queue():
    """Get coaches who need DMs (have Twitter, not yet DM'd, haven't replied)."""
    sheet = get_sheet()
    if not sheet:
        return jsonify({'queue': [], 'sent': 0, 'no_handle': 0, 'replied': 0})
    
    try:
        data = sheet.get_all_values()
        if len(data) < 2:
            return jsonify({'queue': [], 'sent': 0, 'no_handle': 0, 'replied': 0})
        
        headers = data[0]
        rows = data[1:]
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower()
                for kw in keywords:
                    if kw in h_lower:
                        return i
            return -1
        
        def clean_twitter_handle(handle):
            """Clean Twitter handle - extract just the username."""
            if not handle:
                return ''
            handle = handle.strip()
            # Remove @ prefix
            handle = handle.lstrip('@')
            # If it's a full URL, extract the handle
            if 'twitter.com/' in handle or 'x.com/' in handle:
                # Extract handle from URL like https://twitter.com/CoachJohnDoe or https://x.com/CoachJohnDoe
                match = re.search(r'(?:twitter\.com|x\.com)/(@?[A-Za-z0-9_]+)', handle, re.IGNORECASE)
                if match:
                    handle = match.group(1).lstrip('@')
            # Remove any trailing slashes, query params, or fragments
            handle = handle.split('/')[0].split('?')[0].split('#')[0]
            # Remove common suffixes that might be attached
            handle = re.sub(r'[^\w].*$', '', handle)
            # Only return if valid (alphanumeric + underscore, 1-30 chars - Twitter increased limit)
            if handle and re.match(r'^[A-Za-z0-9_]{1,30}$', handle):
                logger.debug(f"Cleaned Twitter handle: '{handle}'")
                return handle
            logger.warning(f"Invalid Twitter handle after cleaning: '{handle}' from original")
            return ''
        
        school_col = find_col(['school'])
        ol_col = find_col(['oline', 'ol coach', 'oc '])
        ol_twitter_col = find_col(['oc twitter', 'ol twitter'])
        ol_contacted_col = find_col(['ol contacted', 'oc contacted'])
        rc_col = find_col(['recruiting', 'rc '])
        rc_twitter_col = find_col(['rc twitter'])
        rc_contacted_col = find_col(['rc contacted'])
        
        queue = []
        sent = 0
        no_handle = 0
        replied = 0
        
        for row in rows:
            def get_val(col_idx):
                if col_idx < 0 or col_idx >= len(row):
                    return ''
                return row[col_idx].strip()
            
            school = get_val(school_col)
            if not school:
                continue
            
            # Check OL coach
            ol_twitter = clean_twitter_handle(get_val(ol_twitter_col))
            ol_contacted = get_val(ol_contacted_col).lower()
            ol_name = get_val(ol_col)
            
            # Skip if coach has REPLIED - don't contact them again
            if 'replied' in ol_contacted:
                replied += 1
            elif ol_twitter and 'dm' not in ol_contacted:
                queue.append({
                    'school': school,
                    'coach_name': ol_name or 'OL Coach',
                    'twitter': ol_twitter,
                    'type': 'OL'
                })
            elif ol_twitter and 'dm' in ol_contacted:
                sent += 1
            elif not ol_twitter and ol_name:
                no_handle += 1
            
            # Check RC
            rc_twitter = clean_twitter_handle(get_val(rc_twitter_col))
            rc_contacted = get_val(rc_contacted_col).lower()
            rc_name = get_val(rc_col)
            
            # Skip if coach has REPLIED
            if 'replied' in rc_contacted:
                replied += 1
            elif rc_twitter and 'dm' not in rc_contacted:
                queue.append({
                    'school': school,
                    'coach_name': rc_name or 'Recruiting Coordinator',
                    'twitter': rc_twitter,
                    'type': 'RC'
                })
            elif rc_twitter and 'dm' in rc_contacted:
                sent += 1
            elif not rc_twitter and rc_name:
                no_handle += 1
        
        return jsonify({'queue': queue, 'sent': sent, 'no_handle': no_handle, 'replied': replied})
    except Exception as e:
        logger.error(f"DM queue error: {e}")
        return jsonify({'queue': [], 'error': str(e)})


@app.route('/api/debug/twitter-handles')
def api_debug_twitter():
    """Debug endpoint to see raw Twitter handles from sheet."""
    sheet = get_sheet()
    if not sheet:
        return jsonify({'error': 'Sheet not connected'})
    
    try:
        data = sheet.get_all_values()
        headers = data[0] if data else []
        rows = data[1:11]  # First 10 rows only
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower()
                for kw in keywords:
                    if kw in h_lower:
                        return i
            return -1
        
        school_col = find_col(['school'])
        ol_twitter_col = find_col(['oc twitter', 'ol twitter'])
        rc_twitter_col = find_col(['rc twitter'])
        ol_name_col = find_col(['oline', 'ol coach', 'oc '])
        rc_name_col = find_col(['recruiting', 'rc '])
        
        debug_info = []
        for row in rows:
            school = row[school_col] if school_col >= 0 and school_col < len(row) else ''
            ol_twitter_raw = row[ol_twitter_col] if ol_twitter_col >= 0 and ol_twitter_col < len(row) else ''
            rc_twitter_raw = row[rc_twitter_col] if rc_twitter_col >= 0 and rc_twitter_col < len(row) else ''
            ol_name = row[ol_name_col] if ol_name_col >= 0 and ol_name_col < len(row) else ''
            rc_name = row[rc_name_col] if rc_name_col >= 0 and rc_name_col < len(row) else ''
            
            debug_info.append({
                'school': school,
                'ol_name': ol_name,
                'ol_twitter_raw': ol_twitter_raw,
                'ol_twitter_url': f'https://x.com/{ol_twitter_raw.lstrip("@").split("/")[0].split("?")[0]}' if ol_twitter_raw else '',
                'rc_name': rc_name,
                'rc_twitter_raw': rc_twitter_raw,
            })
        
        return jsonify({
            'headers': headers,
            'columns': {
                'school': school_col,
                'ol_twitter': ol_twitter_col,
                'rc_twitter': rc_twitter_col
            },
            'sample_rows': debug_info
        })
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/dm/message', methods=['POST'])
def api_dm_message():
    """Generate DM message for a coach."""
    global settings
    data = request.get_json() or {}
    coach_name = data.get('coach_name', 'Coach')
    school = data.get('school', '')
    
    # Get coach's last name
    coach_last = coach_name.split()[-1] if coach_name else 'Coach'
    
    athlete = settings.get('athlete', {})
    
    # Keelan's preferred DM format
    message = f"""Coach {coach_last}, {athlete.get('name', 'Keelan Underwood')} '{athlete.get('graduation_year', '26')[-2:]} {athlete.get('positions', 'OL')} {athlete.get('height', "6'3")} {athlete.get('weight', '295')}lbs {athlete.get('gpa', '3.0')} GPA
{athlete.get('high_school', 'The Benjamin School')} ({athlete.get('state', 'FL')}). I'd love to be considered as a {school} recruit.
X post with my film is below and recruiting form is already completed.
Would love your take.
Thanks so much.
{athlete.get('name', 'Keelan Underwood')}
{athlete.get('phone', '9107471140')}
{athlete.get('highlight_url', 'https://x.com/underwoodkeelan/status/1995522905841746075?s=46')}"""
    
    return jsonify({'message': message.strip()})


@app.route('/api/dm/mark', methods=['POST'])
def api_dm_mark():
    """Mark a coach's DM status in the sheet."""
    data = request.get_json() or {}
    coach_name = data.get('coach_name', '')
    school = data.get('school', '')
    twitter = data.get('twitter', '')
    status = data.get('status', 'messaged')  # 'messaged', 'followed', 'skipped'
    
    sheet = get_sheet()
    if not sheet:
        return jsonify({'success': False, 'error': 'Sheet not connected'})
    
    try:
        all_data = sheet.get_all_values()
        if len(all_data) < 2:
            return jsonify({'success': False, 'error': 'No data in sheet'})
        
        headers = all_data[0]
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower()
                for kw in keywords:
                    if kw in h_lower:
                        return i
            return -1
        
        school_col = find_col(['school'])
        ol_twitter_col = find_col(['oc twitter', 'ol twitter'])
        ol_contacted_col = find_col(['ol contacted', 'oc contacted'])
        rc_twitter_col = find_col(['rc twitter'])
        rc_contacted_col = find_col(['rc contacted'])
        
        # Find the row
        for row_idx, row in enumerate(all_data[1:], start=2):
            if row_idx - 1 >= len(all_data) - 1:
                break
            
            row_school = row[school_col].strip() if school_col < len(row) else ''
            if row_school.lower() != school.lower():
                continue
            
            # Check which column matches the twitter handle
            ol_twitter = row[ol_twitter_col].replace('@', '').strip() if ol_twitter_col < len(row) else ''
            rc_twitter = row[rc_twitter_col].replace('@', '').strip() if rc_twitter_col < len(row) else ''
            
            if ol_twitter.lower() == twitter.lower() and ol_contacted_col >= 0:
                # Update OL contacted column
                current = row[ol_contacted_col] if ol_contacted_col < len(row) else ''
                if status == 'messaged':
                    new_val = 'DM sent' if not current else current + ', DM sent'
                elif status == 'followed':
                    new_val = 'Followed' if not current else current + ', Followed'
                else:
                    new_val = current  # Skip doesn't change anything
                
                sheet.update_cell(row_idx, ol_contacted_col + 1, new_val)
                return jsonify({'success': True, 'updated': 'OL'})
            
            elif rc_twitter.lower() == twitter.lower() and rc_contacted_col >= 0:
                # Update RC contacted column
                current = row[rc_contacted_col] if rc_contacted_col < len(row) else ''
                if status == 'messaged':
                    new_val = 'DM sent' if not current else current + ', DM sent'
                elif status == 'followed':
                    new_val = 'Followed' if not current else current + ', Followed'
                else:
                    new_val = current
                
                sheet.update_cell(row_idx, rc_contacted_col + 1, new_val)
                return jsonify({'success': True, 'updated': 'RC'})
        
        return jsonify({'success': False, 'error': 'Coach not found in sheet'})
    except Exception as e:
        logger.error(f"DM mark error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/coach/response', methods=['POST'])
def api_coach_response():
    """Mark a coach's response in the sheet."""
    data = request.get_json() or {}
    school = data.get('school', '')
    response_type = data.get('response_type', 'dm_reply')
    
    if not school:
        return jsonify({'success': False, 'error': 'School name required'})
    
    sheet = get_sheet()
    if not sheet:
        return jsonify({'success': False, 'error': 'Sheet not connected'})
    
    try:
        all_data = sheet.get_all_values()
        if len(all_data) < 2:
            return jsonify({'success': False, 'error': 'No data in sheet'})
        
        headers = all_data[0]
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower()
                for kw in keywords:
                    if kw in h_lower:
                        return i
            return -1
        
        school_col = find_col(['school'])
        ol_notes_col = find_col(['ol notes', 'oc notes'])
        rc_notes_col = find_col(['rc notes'])
        ol_contacted_col = find_col(['ol contacted', 'oc contacted'])
        rc_contacted_col = find_col(['rc contacted'])
        
        # Response labels
        labels = {
            'dm_reply': 'REPLIED to DM',
            'email_reply': 'REPLIED to email',
            'interested': 'INTERESTED!',
            'not_interested': 'Not interested'
        }
        label = labels.get(response_type, response_type)
        
        # Find the school row
        for row_idx, row in enumerate(all_data[1:], start=2):
            row_school = row[school_col].strip() if school_col < len(row) else ''
            
            if row_school.lower() == school.lower():
                # Update notes column (prefer OL notes, fall back to RC notes)
                notes_col = ol_notes_col if ol_notes_col >= 0 else rc_notes_col
                contacted_col = ol_contacted_col if ol_contacted_col >= 0 else rc_contacted_col
                
                if notes_col >= 0:
                    current = row[notes_col] if notes_col < len(row) else ''
                    from datetime import datetime
                    timestamp = datetime.now().strftime('%m/%d')
                    new_val = f"{timestamp}: {label}" if not current else f"{current}, {timestamp}: {label}"
                    sheet.update_cell(row_idx, notes_col + 1, new_val)
                
                # Also update contacted column if interested
                if response_type == 'interested' and contacted_col >= 0:
                    current = row[contacted_col] if contacted_col < len(row) else ''
                    new_val = 'INTERESTED' if not current else current + ', INTERESTED'
                    sheet.update_cell(row_idx, contacted_col + 1, new_val)
                
                return jsonify({'success': True, 'school': row_school, 'status': label})
        
        return jsonify({'success': False, 'error': f'School "{school}" not found in sheet'})
    except Exception as e:
        logger.error(f"Coach response error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/connections/sheets', methods=['POST'])
def api_connections_sheets():
    global settings
    
    try:
        data = request.get_json()
        creds_json = data.get('credentials', '')
        sheet_name = data.get('spreadsheet_name', 'bardeen')
        
        if creds_json:
            creds = json.loads(creds_json)
            with open(CREDENTIALS_FILE, 'w') as f:
                json.dump(creds, f)
            
            settings['sheets']['credentials_configured'] = True
            settings['sheets']['spreadsheet_name'] = sheet_name
            save_settings(settings)
            
            return jsonify({'success': True})
        
        return jsonify({'success': False, 'error': 'No credentials provided'})
    except json.JSONDecodeError:
        return jsonify({'success': False, 'error': 'Invalid JSON'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/email/connection-test', methods=['POST'])
def api_email_connection_test():
    """Test email connection without sending"""
    try:
        from outreach.email_sender import test_email_connection
        
        data = request.get_json()
        email_addr = data.get('email', settings['email'].get('email_address', ''))
        password = data.get('password', settings['email'].get('app_password', ''))
        
        if not email_addr or not password:
            return jsonify({'success': False, 'error': 'Email and password required'})
        
        success, message = test_email_connection(email_addr, password)
        return jsonify({'success': success, 'message': message})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/email/check-responses', methods=['POST'])
def api_check_responses():
    """Check Gmail inbox for responses from coaches using Gmail API"""
    try:
        # Use Gmail API if available
        if has_gmail_api():
            logger.info("Checking responses via Gmail API")
            
            # Get coaches we've emailed from the Google Sheet
            sheet = get_sheet()
            if not sheet:
                return jsonify({'success': False, 'error': 'Sheet not connected'})
            
            all_data = sheet.get_all_values()
            if len(all_data) < 2:
                return jsonify({'success': True, 'message': 'No data in sheet', 'responses': []})
            
            headers = [h.lower() for h in all_data[0]]
            rows = all_data[1:]
            
            logger.info(f"Sheet headers: {headers}")
            
            # Find columns - be more flexible with matching
            def find_col(keywords):
                for i, h in enumerate(headers):
                    for kw in keywords:
                        if kw in h:
                            return i
                return -1
            
            rc_email_col = find_col(['rc email', 'rc_email'])
            ol_email_col = find_col(['oc email', 'ol email', 'ol_email'])
            rc_contacted_col = find_col(['rc contacted', 'rc_contacted'])
            ol_contacted_col = find_col(['ol contacted', 'oc contacted', 'ol_contacted'])
            rc_notes_col = find_col(['rc notes', 'rc_notes'])
            ol_notes_col = find_col(['ol notes', 'oc notes', 'ol_notes'])
            school_col = find_col(['school'])
            
            logger.info(f"Columns - RC email: {rc_email_col}, OL email: {ol_email_col}, RC contacted: {rc_contacted_col}, OL contacted: {ol_contacted_col}")
            
            # Collect all coach emails we've contacted
            # Check EITHER contacted column OR notes column for evidence of email sent
            coach_emails = []
            for row_idx, row in enumerate(rows):
                school = row[school_col] if school_col >= 0 and school_col < len(row) else ''
                
                # RC email - check if we have evidence of contact
                if rc_email_col >= 0 and rc_email_col < len(row):
                    rc_email = row[rc_email_col].strip()
                    rc_contacted = row[rc_contacted_col].strip() if rc_contacted_col >= 0 and rc_contacted_col < len(row) else ''
                    rc_notes = row[rc_notes_col].strip() if rc_notes_col >= 0 and rc_notes_col < len(row) else ''
                    
                    # Coach was contacted if there's a date OR notes mention "sent"
                    was_contacted = bool(rc_contacted) or 'sent' in rc_notes.lower() or 'intro' in rc_notes.lower()
                    
                    if rc_email and '@' in rc_email and was_contacted:
                        coach_emails.append({'email': rc_email, 'school': school, 'type': 'rc', 'row': row_idx + 2})
                
                # OL email
                if ol_email_col >= 0 and ol_email_col < len(row):
                    ol_email = row[ol_email_col].strip()
                    ol_contacted = row[ol_contacted_col].strip() if ol_contacted_col >= 0 and ol_contacted_col < len(row) else ''
                    ol_notes = row[ol_notes_col].strip() if ol_notes_col >= 0 and ol_notes_col < len(row) else ''
                    
                    was_contacted = bool(ol_contacted) or 'sent' in ol_notes.lower() or 'intro' in ol_notes.lower()
                    
                    if ol_email and '@' in ol_email and was_contacted:
                        coach_emails.append({'email': ol_email, 'school': school, 'type': 'ol', 'row': row_idx + 2})
            
            logger.info(f"Found {len(coach_emails)} coaches we've emailed")
            
            if not coach_emails:
                return jsonify({
                    'success': True, 
                    'message': 'No coaches found with sent emails. Make sure contacted column or notes have data.',
                    'responses': [], 
                    'total_checked': 0,
                    'debug': {
                        'rc_email_col': rc_email_col,
                        'ol_email_col': ol_email_col,
                        'rc_contacted_col': rc_contacted_col,
                        'ol_contacted_col': ol_contacted_col
                    }
                })
            
            logger.info(f"Found {len(coach_emails)} coaches we've emailed, checking inbox...")
            
            # Search inbox for replies from these coaches
            responses = []
            service = get_gmail_service()
            if service:
                for coach in coach_emails:
                    try:
                        # Search for emails from this coach
                        query = f"from:{coach['email']}"
                        results = service.users().messages().list(userId='me', q=query, maxResults=5).execute()
                        messages = results.get('messages', [])
                        
                        if messages:
                            # Get the most recent message
                            msg = service.users().messages().get(userId='me', id=messages[0]['id'], format='metadata', metadataHeaders=['Subject', 'Date']).execute()
                            headers_dict = {h['name']: h['value'] for h in msg.get('payload', {}).get('headers', [])}
                            
                            responses.append({
                                'email': coach['email'],
                                'school': coach['school'],
                                'type': coach['type'],
                                'row': coach['row'],
                                'subject': headers_dict.get('Subject', 'No subject'),
                                'date': headers_dict.get('Date', ''),
                                'snippet': msg.get('snippet', '')[:100]
                            })
                            logger.info(f"Found response from {coach['email']} at {coach['school']}")
                            
                            # Mark response in sheet
                            try:
                                notes_col = rc_notes_col + 1 if coach['type'] == 'rc' else ol_notes_col + 1
                                current_notes = sheet.cell(coach['row'], notes_col).value or ''
                                if 'RESPONDED' not in current_notes.upper():
                                    new_notes = f"RESPONDED {datetime.now().strftime('%m/%d')}; {current_notes}"
                                    sheet.update_cell(coach['row'], notes_col, new_notes)
                                    logger.info(f"Marked response in sheet for {coach['school']}")
                            except Exception as e:
                                logger.warning(f"Could not mark response in sheet: {e}")
                                
                    except Exception as e:
                        logger.warning(f"Error checking {coach['email']}: {e}")
            
            # Cache responses for display
            global cached_responses
            cached_responses = responses
            
            return jsonify({
                'success': True,
                'responses': responses,
                'total_checked': len(coach_emails),
                'total_responses': len(responses),
                'method': 'Gmail API'
            })
        
        else:
            # Fallback to IMAP (may not work on Railway)
            return jsonify({'success': False, 'error': 'Gmail API not configured. Add GMAIL_REFRESH_TOKEN to Railway.'})
    
    except Exception as e:
        logger.error(f"Response check error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/email/test-tracking')
def api_test_tracking():
    """Test if response tracking is working."""
    result = {
        'gmail_connected': False,
        'imap_working': False,
        'emails_sent': 0,
        'responses_found': 0,
        'error': None
    }
    
    try:
        # Check Gmail API first (works on Railway)
        if has_gmail_api():
            result['gmail_connected'] = True
            result['using_gmail_api'] = True
            
            service = get_gmail_service()
            if service:
                result['imap_working'] = True  # Gmail API is working
                
                # Count emails in inbox
                try:
                    results = service.users().messages().list(userId='me', maxResults=10).execute()
                    result['inbox_count'] = results.get('resultSizeEstimate', 0)
                except Exception as e:
                    result['gmail_api_error'] = str(e)
                
                # Get response count from cache
                global cached_responses
                if 'cached_responses' in globals() and cached_responses:
                    result['responses_found'] = len(cached_responses)
            else:
                result['error'] = 'Gmail API configured but could not connect'
        else:
            result['error'] = 'Gmail API not configured. Add GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REFRESH_TOKEN to Railway.'
            result['gmail_api_configured'] = False
        
        # Get sheet stats for emails sent count
        sheet = get_sheet()
        if sheet:
            try:
                data = sheet.get_all_values()
                if len(data) > 1:
                    headers = [h.lower() for h in data[0]]
                    
                    def find_col(keywords):
                        for i, h in enumerate(headers):
                            for kw in keywords:
                                if kw in h:
                                    return i
                        return -1
                    
                    rc_notes_col = find_col(['rc notes'])
                    ol_notes_col = find_col(['ol notes'])
                    
                    sent_count = 0
                    for row in data[1:]:
                        rc_notes = row[rc_notes_col].lower() if rc_notes_col >= 0 and rc_notes_col < len(row) else ''
                        ol_notes = row[ol_notes_col].lower() if ol_notes_col >= 0 and ol_notes_col < len(row) else ''
                        if 'sent' in rc_notes:
                            sent_count += 1
                        if 'sent' in ol_notes:
                            sent_count += 1
                    
                    result['emails_sent'] = sent_count
            except Exception as e:
                result['sheet_error'] = str(e)
        
        return jsonify(result)
    
    except Exception as e:
        result['error'] = str(e)
        return jsonify(result)


def mark_coach_replied_in_sheet(sheet, coach_email: str, school_name: str):
    """Mark a coach as REPLIED in the Google Sheet so they don't get contacted again."""
    try:
        all_data = sheet.get_all_values()
        if len(all_data) < 2:
            return
        
        headers = all_data[0]
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                h_lower = h.lower()
                for kw in keywords:
                    if kw in h_lower:
                        return i
            return -1
        
        school_col = find_col(['school'])
        rc_email_col = find_col(['rc email'])
        ol_email_col = find_col(['ol email', 'oc email'])
        rc_contacted_col = find_col(['rc contacted'])
        ol_contacted_col = find_col(['ol contacted', 'oc contacted'])
        rc_notes_col = find_col(['rc notes'])
        ol_notes_col = find_col(['ol notes', 'oc notes'])
        
        coach_email_lower = coach_email.lower()
        
        for row_idx, row in enumerate(all_data[1:], start=2):
            row_school = row[school_col].strip() if school_col >= 0 and school_col < len(row) else ''
            
            # Check RC email
            rc_email = row[rc_email_col].strip().lower() if rc_email_col >= 0 and rc_email_col < len(row) else ''
            if rc_email == coach_email_lower:
                # Update RC contacted and notes
                if rc_contacted_col >= 0:
                    current = row[rc_contacted_col] if rc_contacted_col < len(row) else ''
                    if 'REPLIED' not in current.upper():
                        new_val = 'REPLIED' if not current else current + ', REPLIED'
                        sheet.update_cell(row_idx, rc_contacted_col + 1, new_val)
                if rc_notes_col >= 0:
                    from datetime import datetime
                    current = row[rc_notes_col] if rc_notes_col < len(row) else ''
                    note = f"Response received {datetime.now().strftime('%m/%d/%Y')}"
                    # Don't add duplicate
                    if 'response received' not in current.lower():
                        new_val = note if not current else note + '; ' + current
                        sheet.update_cell(row_idx, rc_notes_col + 1, new_val)
                logger.info(f"Marked RC at {row_school} as REPLIED")
                return
            
            # Check OL email
            ol_email = row[ol_email_col].strip().lower() if ol_email_col >= 0 and ol_email_col < len(row) else ''
            if ol_email == coach_email_lower:
                # Update OL contacted and notes
                if ol_contacted_col >= 0:
                    current = row[ol_contacted_col] if ol_contacted_col < len(row) else ''
                    if 'REPLIED' not in current.upper():
                        new_val = 'REPLIED' if not current else current + ', REPLIED'
                        sheet.update_cell(row_idx, ol_contacted_col + 1, new_val)
                if ol_notes_col >= 0:
                    from datetime import datetime
                    current = row[ol_notes_col] if ol_notes_col < len(row) else ''
                    note = f"Response received {datetime.now().strftime('%m/%d/%Y')}"
                    # Don't add duplicate
                    if 'response received' not in current.lower():
                        new_val = note if not current else note + '; ' + current
                        sheet.update_cell(row_idx, ol_notes_col + 1, new_val)
                logger.info(f"Marked OL at {row_school} as REPLIED")
                return
    except Exception as e:
        logger.error(f"Error marking coach replied: {e}")


@app.route('/api/email/settings', methods=['GET', 'POST'])
def api_email_settings():
    """Get or update email settings including enterprise features"""
    global settings
    
    if request.method == 'POST':
        data = request.get_json()
        
        if 'email' not in settings:
            settings['email'] = {}
        
        # Update email settings
        for key in ['email_address', 'app_password', 'max_per_day', 'delay_seconds',
                    'use_randomized_templates', 'enable_followups']:
            if key in data:
                settings['email'][key] = data[key]
        
        save_settings(settings)
        return jsonify({'success': True})
    
    # GET - return current settings (without password)
    email_settings = settings.get('email', {}).copy()
    email_settings['app_password'] = '********' if email_settings.get('app_password') else ''
    email_settings.setdefault('use_randomized_templates', True)
    email_settings.setdefault('enable_followups', True)
    
    return jsonify({'success': True, 'settings': email_settings})


@app.route('/api/run', methods=['POST'])
def api_run():
    global active_task, task_thread, stop_requested
    
    if active_task is not None:
        return jsonify({'error': 'Task already running'})
    
    data = request.get_json()
    tool = data.get('tool', 'staff')
    
    stop_requested = False
    active_task = tool
    
    task_thread = threading.Thread(target=run_task, args=(tool,), daemon=True)
    task_thread.start()
    
    return jsonify({'success': True, 'tool': tool})


@app.route('/api/stop', methods=['POST'])
def api_stop():
    global stop_requested
    stop_requested = True
    add_log("Stop requested...", 'warning')
    return jsonify({'success': True})


@app.route('/api/events')
def api_events():
    def generate():
        while True:
            try:
                evt = event_queue.get(timeout=30)
                yield f"data: {json.dumps(evt)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'ping'})}\n\n"
    
    return Response(stream_with_context(generate()), mimetype='text/event-stream')


# ============================================================================
# TASK RUNNERS
# ============================================================================

def run_task(tool: str):
    global active_task, stop_requested
    
    try:
        event_queue.put({'type': 'progress', 'data': {'percent': 0, 'title': 'Starting...'}})
        
        if tool == 'staff':
            run_staff_scraper()
        elif tool == 'twitter':
            run_twitter_scraper()
        elif tool == 'email':
            run_email_scraper()
        elif tool == 'pipeline':
            run_pipeline()
        elif tool == 'email_send':
            run_email_send()
        else:
            add_log(f"Unknown tool: {tool}", 'error')
    except Exception as e:
        add_log(f"Error: {e}", 'error')
        logger.error(traceback.format_exc())
    finally:
        active_task = None
        event_queue.put({'type': 'done'})


def run_staff_scraper():
    global stop_requested
    
    if not HAS_SCRAPER:
        add_log("Staff scraper not available - install selenium", 'error')
        return
    
    add_log("Starting staff name scraper...")
    
    found = 0
    total = 0
    
    def callback(event, data):
        nonlocal found, total
        
        if stop_requested:
            raise KeyboardInterrupt()
        
        if event == 'schools_found':
            total = data.get('count', 0)
            add_log(f"Found {total} schools to process")
        elif event == 'processing':
            current = data.get('current', 0)
            school = data.get('school', '?')
            add_log(f"[{current}/{total}] {school}")
            if total > 0:
                event_queue.put({'type': 'progress', 'data': {'percent': int((current/total)*100)}})
        elif event == 'school_processed':
            if data.get('ol_found') or data.get('rc_found'):
                found += 1
                add_log(f"✓ {data.get('school', '?')}", 'success')
    
    try:
        config = ScraperConfig(reverse=settings['scraper'].get('start_from_bottom', False))
        scraper = CoachScraper(config)
        scraper.run(callback=callback)
        add_log(f"Done! Found {found} coaches", 'success')
    except KeyboardInterrupt:
        add_log("Stopped by user")
    except Exception as e:
        add_log(f"Scraper error: {e}", 'error')


def run_twitter_scraper():
    global stop_requested
    
    if not HAS_TWITTER_SCRAPER:
        add_log("Twitter scraper not available", 'error')
        return
    
    add_log("Starting Twitter handle scraper...")
    
    found = 0
    total = 0
    
    def callback(event, data):
        nonlocal found, total
        
        if stop_requested:
            raise KeyboardInterrupt()
        
        if event == 'schools_found':
            total = data.get('count', 0)
            add_log(f"Found {total} schools needing Twitter handles")
        elif event == 'processing':
            current = data.get('current', 0)
            if total > 0:
                event_queue.put({'type': 'progress', 'data': {'percent': int((current/total)*100)}})
        elif event == 'found':
            found += 1
            add_log(f"✓ {data.get('school')} - {data.get('type')}: {data.get('handle')}", 'success')
    
    try:
        config = TwitterScraperConfig(reverse=settings['scraper'].get('start_from_bottom', False))
        scraper = TwitterScraper(config)
        scraper.run(callback=callback)
        add_log(f"Done! Found {found} handles", 'success')
    except KeyboardInterrupt:
        add_log("Stopped by user")
    except Exception as e:
        add_log(f"Twitter scraper error: {e}", 'error')


def run_email_scraper():
    global stop_requested
    
    if not HAS_EMAIL_SCRAPER:
        add_log("Email scraper not available", 'error')
        return
    
    add_log("Starting email scraper...")
    
    found = 0
    total = 0
    
    def callback(event, data):
        nonlocal found, total
        
        if stop_requested:
            raise KeyboardInterrupt()
        
        if event == 'schools_found':
            total = data.get('count', 0)
            add_log(f"Found {total} schools needing emails")
        elif event == 'processing':
            current = data.get('current', 0)
            if total > 0:
                event_queue.put({'type': 'progress', 'data': {'percent': int((current/total)*100)}})
        elif event == 'found':
            found += 1
            add_log(f"✓ {data.get('school')} - {data.get('email')}", 'success')
    
    try:
        config = EmailScraperConfig(reverse=settings['scraper'].get('start_from_bottom', False))
        scraper = EmailScraper(config)
        scraper.run(callback=callback)
        add_log(f"Done! Found {found} emails", 'success')
    except KeyboardInterrupt:
        add_log("Stopped by user")
    except Exception as e:
        add_log(f"Email scraper error: {e}", 'error')


def run_pipeline():
    add_log("Starting full pipeline...")
    run_staff_scraper()
    if not stop_requested:
        run_email_scraper()
    if not stop_requested:
        run_twitter_scraper()
    add_log("Pipeline complete!", 'success')


def run_email_send():
    global stop_requested
    
    add_log("Starting email campaign...")
    
    email_addr = settings['email'].get('email_address', '')
    password = settings['email'].get('app_password', '')
    
    if not email_addr or not password:
        add_log("Email not configured - go to Connections", 'error')
        return
    
    athlete_name = settings['athlete'].get('name', '')
    if not athlete_name:
        add_log("Athlete profile not complete - go to Profile", 'error')
        return
    
    try:
        from outreach.email_sender import SmartEmailSender, EmailConfig, AthleteInfo
        
        config = EmailConfig(
            email_address=email_addr,
            app_password=password,
            max_per_day=settings['email'].get('max_per_day', 50),
            delay_seconds=settings['email'].get('delay_seconds', 5),
            use_randomized_templates=settings['email'].get('use_randomized_templates', True),
            enable_followups=settings['email'].get('enable_followups', True),
        )
        
        name_parts = athlete_name.split()
        city_state = settings['athlete'].get('city', '')
        if settings['athlete'].get('state'):
            city_state += ', ' + settings['athlete'].get('state', '')
        
        athlete = AthleteInfo(
            name=athlete_name,
            graduation_year=settings['athlete'].get('graduation_year', '2026'),
            height=settings['athlete'].get('height', ''),
            weight=settings['athlete'].get('weight', ''),
            positions=settings['athlete'].get('positions', ''),
            high_school=settings['athlete'].get('high_school', ''),
            city=settings['athlete'].get('city', ''),
            state=settings['athlete'].get('state', ''),
            gpa=settings['athlete'].get('gpa', ''),
            highlight_url=settings['athlete'].get('highlight_url', ''),
            phone=settings['athlete'].get('phone', ''),
        )
        
        sheet = get_sheet()
        if not sheet:
            add_log("Could not connect to Google Sheet", 'error')
            return
        
        data = sheet.get_all_values()
        if len(data) < 2:
            add_log("No data in sheet", 'error')
            return
        
        headers = data[0]
        rows = data[1:]
        
        sender = SmartEmailSender(config, athlete)
        coaches = sender.get_coaches_to_email(rows, headers)
        
        if not coaches:
            add_log("No coaches to email (all contacted or missing data)", 'warning')
            return
        
        add_log(f"Found {len(coaches)} coaches to email")
        
        def callback(event, data):
            if event == 'email_sent':
                add_log(f"✓ Sent to {data['school']} ({data['type']})", 'success')
            elif event == 'email_error':
                add_log(f"✗ Failed: {data['school']} - {data['error']}", 'error')
        
        results = sender.send_to_coaches(coaches, sheet, callback)
        
        add_log(f"Done! Sent: {results['sent']}, Errors: {results['errors']}", 'success')
        
    except ImportError as e:
        add_log(f"Email module not available: {e}", 'error')
    except Exception as e:
        add_log(f"Email error: {e}", 'error')
        logger.error(traceback.format_exc())


# ============================================================================
# NEW STREAMLINED API ROUTES
# ============================================================================

@app.route('/api/responses/recent')
def api_responses_recent():
    """Get recent responses - returns cached responses from last check."""
    try:
        # Return cached responses if available
        global cached_responses
        if 'cached_responses' not in globals():
            cached_responses = []
        
        if cached_responses:
            return jsonify({'success': True, 'responses': cached_responses})
        
        # If no cache, return empty and suggest checking
        return jsonify({'success': True, 'responses': [], 'message': 'Click "Check for Responses" to scan inbox'})
    except Exception as e:
        logger.error(f"Responses error: {e}")
        return jsonify({'success': False, 'error': str(e), 'responses': []})


@app.route('/api/email/queue-status')
def api_email_queue_status():
    """Get detailed email queue status - how many coaches ready, responded, etc."""
    try:
        sheet = get_sheet()
        if not sheet:
            return jsonify({'success': False, 'error': 'Sheet not connected'})
        
        data = sheet.get_all_values()
        if len(data) < 2:
            return jsonify({'success': True, 'ready': 0, 'followups_due': 0, 'responded': 0, 'total': 0})
        
        headers = [h.lower() for h in data[0]]
        rows = data[1:]
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                for kw in keywords:
                    if kw in h:
                        return i
            return -1
        
        rc_email_col = find_col(['rc email'])
        ol_email_col = find_col(['oc email', 'ol email'])
        rc_contacted_col = find_col(['rc contacted'])
        ol_contacted_col = find_col(['ol contacted'])
        rc_notes_col = find_col(['rc notes'])
        ol_notes_col = find_col(['ol notes'])
        
        from datetime import date, datetime, timedelta
        today = date.today()
        days_between = 3
        
        ready_count = 0
        followup_count = 0
        responded_count = 0
        new_coaches = 0
        
        for row in rows:
            def check_coach(email_col, contacted_col, notes_col):
                nonlocal ready_count, followup_count, responded_count, new_coaches
                
                if email_col < 0 or email_col >= len(row):
                    return
                
                email = row[email_col].strip() if email_col < len(row) else ''
                if not email or '@' not in email:
                    return
                
                contacted = row[contacted_col].strip().lower() if contacted_col >= 0 and contacted_col < len(row) else ''
                notes = row[notes_col].strip().lower() if notes_col >= 0 and notes_col < len(row) else ''
                
                # Check if responded
                if 'responded' in notes or 'replied' in notes or 'response' in notes:
                    responded_count += 1
                    return
                
                # Parse last contact date
                last_contact = None
                if contacted:
                    try:
                        last_contact = datetime.strptime(contacted[:10], '%Y-%m-%d').date()
                    except:
                        try:
                            last_contact = datetime.strptime(contacted.split()[0], '%m/%d/%Y').date()
                        except:
                            pass
                
                if not last_contact and not contacted:
                    # Never contacted - ready for intro
                    new_coaches += 1
                    ready_count += 1
                elif last_contact:
                    days_since = (today - last_contact).days
                    if days_since >= days_between:
                        followup_count += 1
                        ready_count += 1
            
            check_coach(rc_email_col, rc_contacted_col, rc_notes_col)
            check_coach(ol_email_col, ol_contacted_col, ol_notes_col)
        
        return jsonify({
            'success': True,
            'ready': ready_count,
            'new_coaches': new_coaches,
            'followups_due': followup_count,
            'responded': responded_count,
            'total_coaches': len(rows),
            'summary': f"{ready_count} coaches ready ({new_coaches} new intros, {followup_count} follow-ups). {responded_count} have responded."
        })
    except Exception as e:
        logger.error(f"Queue status error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/email/scan-past-responses', methods=['POST'])
def api_scan_past_responses():
    """Scan Gmail for past responses and mark them in the sheet."""
    try:
        if not has_gmail_api():
            return jsonify({'success': False, 'error': 'Gmail API not configured'})
        
        sheet = get_sheet()
        if not sheet:
            return jsonify({'success': False, 'error': 'Sheet not connected'})
        
        data = sheet.get_all_values()
        if len(data) < 2:
            return jsonify({'success': False, 'error': 'No data in sheet'})
        
        headers = [h.lower() for h in data[0]]
        rows = data[1:]
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                for kw in keywords:
                    if kw in h:
                        return i
            return -1
        
        rc_email_col = find_col(['rc email'])
        ol_email_col = find_col(['oc email', 'ol email'])
        rc_notes_col = find_col(['rc notes'])
        ol_notes_col = find_col(['ol notes'])
        
        # Collect all coach emails
        coach_emails = []
        for row_idx, row in enumerate(rows):
            school = row[0] if len(row) > 0 else ''
            
            if rc_email_col >= 0 and rc_email_col < len(row):
                email = row[rc_email_col].strip()
                notes = row[rc_notes_col].strip() if rc_notes_col >= 0 and rc_notes_col < len(row) else ''
                if email and '@' in email and 'responded' not in notes.lower():
                    coach_emails.append({'email': email, 'school': school, 'type': 'rc', 'row': row_idx + 2, 'notes_col': rc_notes_col + 1})
            
            if ol_email_col >= 0 and ol_email_col < len(row):
                email = row[ol_email_col].strip()
                notes = row[ol_notes_col].strip() if ol_notes_col >= 0 and ol_notes_col < len(row) else ''
                if email and '@' in email and 'responded' not in notes.lower():
                    coach_emails.append({'email': email, 'school': school, 'type': 'ol', 'row': row_idx + 2, 'notes_col': ol_notes_col + 1})
        
        service = get_gmail_service()
        if not service:
            return jsonify({'success': False, 'error': 'Could not connect to Gmail'})
        
        responses_found = []
        marked_count = 0
        
        for coach in coach_emails:
            try:
                # Search for emails from this coach in the past 2 weeks
                query = f"from:{coach['email']} newer_than:14d"
                results = service.users().messages().list(userId='me', q=query, maxResults=3).execute()
                messages = results.get('messages', [])
                
                if messages:
                    responses_found.append({
                        'school': coach['school'],
                        'email': coach['email'],
                        'type': coach['type']
                    })
                    
                    # Mark in sheet
                    try:
                        current_notes = sheet.cell(coach['row'], coach['notes_col']).value or ''
                        if 'RESPONDED' not in current_notes.upper():
                            new_notes = f"RESPONDED; {current_notes}" if current_notes else "RESPONDED"
                            sheet.update_cell(coach['row'], coach['notes_col'], new_notes)
                            marked_count += 1
                            logger.info(f"Marked response from {coach['school']}")
                    except Exception as e:
                        logger.warning(f"Could not update sheet: {e}")
                        
            except Exception as e:
                logger.warning(f"Error checking {coach['email']}: {e}")
        
        global cached_responses
        cached_responses = responses_found
        
        return jsonify({
            'success': True,
            'responses_found': len(responses_found),
            'marked_in_sheet': marked_count,
            'responses': responses_found,
            'message': f"Found {len(responses_found)} responses, marked {marked_count} in sheet"
        })
    except Exception as e:
        logger.error(f"Scan past responses error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/sheet/cleanup', methods=['POST'])
def api_sheet_cleanup():
    """Clean up the sheet - remove duplicates, consolidate notes."""
    try:
        sheet = get_sheet()
        if not sheet:
            return jsonify({'success': False, 'error': 'Sheet not connected'})
        
        data = sheet.get_all_values()
        if len(data) < 2:
            return jsonify({'success': False, 'error': 'No data in sheet'})
        
        headers = [h.lower() for h in data[0]]
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                for kw in keywords:
                    if kw in h:
                        return i
            return -1
        
        rc_notes_col = find_col(['rc notes'])
        ol_notes_col = find_col(['ol notes'])
        
        fixes_made = 0
        
        for row_idx, row in enumerate(data[1:], start=2):
            # Clean up notes - remove duplicates separated by semicolons
            for notes_col in [rc_notes_col, ol_notes_col]:
                if notes_col >= 0 and notes_col < len(row):
                    notes = row[notes_col]
                    if notes and ';' in notes:
                        # Split by semicolon and remove duplicates
                        parts = [p.strip() for p in notes.split(';') if p.strip()]
                        seen = set()
                        unique_parts = []
                        for p in parts:
                            p_lower = p.lower()
                            if p_lower not in seen:
                                seen.add(p_lower)
                                unique_parts.append(p)
                        
                        new_notes = '; '.join(unique_parts)
                        if new_notes != notes:
                            sheet.update_cell(row_idx, notes_col + 1, new_notes)
                            fixes_made += 1
        
        return jsonify({
            'success': True,
            'fixes_made': fixes_made,
            'message': f"Cleaned up {fixes_made} cells"
        })
    except Exception as e:
        logger.error(f"Sheet cleanup error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/responses/hot-leads')
def api_responses_hot_leads():
    """Get hot leads."""
    try:
        from enterprise.responses import get_response_tracker
        tracker = get_response_tracker()
        leads = tracker.get_hot_leads(10)
        return jsonify({'success': True, 'leads': leads})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'leads': []})


@app.route('/api/responses/by-division')
def api_responses_by_division():
    """Get response stats by division."""
    try:
        from enterprise.responses import get_response_tracker
        tracker = get_response_tracker()
        divisions = tracker.get_stats_by_division()
        return jsonify({'success': True, 'divisions': divisions})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'divisions': {}})


@app.route('/api/templates/toggle', methods=['POST'])
def api_templates_toggle():
    """Toggle template enabled/disabled."""
    try:
        from enterprise.templates import get_template_manager
        data = request.get_json()
        manager = get_template_manager()
        success = manager.toggle_template(data['id'], data['enabled'])
        return jsonify({'success': success})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/templates/<template_id>', methods=['DELETE'])
def api_templates_delete(template_id):
    """Delete a user template."""
    try:
        from enterprise.templates import get_template_manager
        manager = get_template_manager()
        success = manager.delete_template(template_id)
        return jsonify({'success': success})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/templates/<template_id>', methods=['PUT'])
def api_templates_update(template_id):
    """Update a template."""
    try:
        from enterprise.templates import get_template_manager
        manager = get_template_manager()
        data = request.get_json() or {}
        
        template = manager.get_template(template_id)
        if not template:
            return jsonify({'success': False, 'error': 'Template not found'})
        
        # Update the template
        if data.get('name'):
            template.name = data['name']
        if 'subject' in data:
            template.subject = data['subject']
        if data.get('body'):
            template.body = data['body']
        
        manager._save()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/email/send', methods=['POST'])
def api_email_send():
    """Send emails to coaches from sheet - handles intro and follow-up campaigns."""
    # Reload settings fresh to avoid stale/corrupted data
    current_settings = load_settings()
    
    data = request.get_json() or {}
    limit = data.get('limit', 10)
    template_id = data.get('template_id')
    campaign_type = data.get('campaign_type', 'intro')  # 'intro', 'followup_1', 'followup_2', 'smart'
    
    try:
        sheet = get_sheet()
        if not sheet:
            return jsonify({'success': False, 'error': 'Sheet not connected', 'sent': 0, 'errors': 0})
        
        all_data = sheet.get_all_values()
        if not all_data:
            return jsonify({'success': False, 'error': 'No data in sheet', 'sent': 0, 'errors': 0})
        
        headers = [h.lower().strip() for h in all_data[0]]
        rows = all_data[1:]
        
        logger.info(f"Sheet headers: {headers}")
        
        def find_col(keywords):
            for i, h in enumerate(headers):
                for kw in keywords:
                    if kw in h:
                        return i
            return -1
        
        # Match YOUR actual column names from "bardeen" sheet
        school_col = find_col(['school'])
        rc_name_col = find_col(['recruiting coordinator'])
        rc_email_col = find_col(['rc email'])
        rc_contacted_col = find_col(['rc contacted'])
        rc_notes_col = find_col(['rc notes'])
        ol_name_col = find_col(['oline coach', 'oline'])
        ol_email_col = find_col(['oc email'])
        ol_contacted_col = find_col(['ol contacted'])
        ol_notes_col = find_col(['ol notes'])
        
        logger.info(f"Columns: school={school_col}, rc_email={rc_email_col}, ol_email={ol_email_col}")
        
        if school_col == -1:
            return jsonify({'success': False, 'error': 'No school column found', 'sent': 0, 'errors': 0})
        
        if ol_email_col == -1 and rc_email_col == -1:
            return jsonify({'success': False, 'error': f'No email column found. Headers: {headers}', 'sent': 0, 'errors': 0})
        
        athlete = current_settings.get('athlete', {})
        email_settings = current_settings.get('email', {})
        days_between = email_settings.get('days_between_emails', 3)
        
        email_addr = email_settings.get('email_address', '')
        app_password = email_settings.get('app_password', '')
        
        # Validate password is a string (not boolean from corrupted settings)
        if not isinstance(app_password, str) or app_password in [True, '********', ''] or len(app_password) < 5:
            app_password = DEFAULT_SETTINGS['email']['app_password']
            email_addr = DEFAULT_SETTINGS['email']['email_address']
            logger.warning("Using default credentials due to corrupted settings")
        
        if not email_addr or not app_password:
            return jsonify({'success': False, 'error': 'Email not configured in settings', 'sent': 0, 'errors': 0})
        
        from datetime import date, datetime, timedelta
        today = date.today()
        
        def parse_date(date_str):
            """Parse date from contacted column"""
            if not date_str:
                return None
            try:
                # Try MM/DD/YYYY format
                return datetime.strptime(date_str.split(',')[0].strip(), '%m/%d/%Y').date()
            except:
                try:
                    # Try other formats
                    return datetime.strptime(date_str.strip()[:10], '%Y-%m-%d').date()
                except:
                    return None
        
        def get_email_stage(contacted_str, notes_str):
            """Determine what stage the coach is at: 'new', 'intro_sent', 'followup1_sent', 'followup2_sent', 'replied'"""
            contacted = contacted_str.lower() if contacted_str else ''
            notes = notes_str.lower() if notes_str else ''
            
            # Check if coach has responded - skip them!
            if 'replied' in contacted or 'replied' in notes or 'responded' in notes or 'response' in notes:
                return 'replied'
            if 'followup 2' in notes or 'follow-up 2' in notes or 'f2' in notes:
                return 'followup2_sent'
            if 'followup 1' in notes or 'follow-up 1' in notes or 'f1' in notes:
                return 'followup1_sent'
            if contacted and parse_date(contacted):
                return 'intro_sent'
            return 'new'
        
        def days_since_contact(contacted_str):
            """Get days since last contact"""
            last_date = parse_date(contacted_str)
            if not last_date:
                return 999
            return (today - last_date).days
        
        coaches = []
        for row_idx, row in enumerate(rows):
            if len(coaches) >= limit * 2:
                break
            
            school = row[school_col] if school_col < len(row) else ''
            if not school:
                continue
            
            # Check RC
            if rc_email_col >= 0 and rc_email_col < len(row):
                rc_email = row[rc_email_col].strip()
                rc_contacted = row[rc_contacted_col].strip() if rc_contacted_col >= 0 and rc_contacted_col < len(row) else ''
                rc_notes = row[rc_notes_col].strip() if rc_notes_col >= 0 and rc_notes_col < len(row) else ''
                rc_name = row[rc_name_col] if rc_name_col >= 0 and rc_name_col < len(row) else 'Coach'
                
                if rc_email and '@' in rc_email:
                    stage = get_email_stage(rc_contacted, rc_notes)
                    days = days_since_contact(rc_contacted)
                    
                    # Determine what email to send
                    should_send = False
                    email_type = None
                    
                    if stage == 'replied':
                        pass  # Skip - they replied
                    elif stage == 'new':
                        should_send = True
                        email_type = 'intro'
                    elif stage == 'intro_sent' and days >= days_between:
                        should_send = True
                        email_type = 'followup_1'
                    elif stage == 'followup1_sent' and days >= days_between:
                        should_send = True
                        email_type = 'followup_2'
                    elif stage == 'followup2_sent' and days >= days_between * 3:
                        # Restart cycle after longer wait
                        should_send = True
                        email_type = 'intro'
                    
                    if should_send:
                        coaches.append({
                            'email': rc_email, 'name': rc_name, 'school': school, 'type': 'rc',
                            'row_idx': row_idx + 2, 
                            'contacted_col': rc_contacted_col + 1 if rc_contacted_col >= 0 else None,
                            'notes_col': rc_notes_col + 1 if rc_notes_col >= 0 else None,
                            'email_type': email_type, 'current_notes': rc_notes,
                            'days_since': days  # Track days since last contact for sorting
                        })
            
            # Check OL/OC
            if ol_email_col >= 0 and ol_email_col < len(row):
                ol_email = row[ol_email_col].strip()
                ol_contacted = row[ol_contacted_col].strip() if ol_contacted_col >= 0 and ol_contacted_col < len(row) else ''
                ol_notes = row[ol_notes_col].strip() if ol_notes_col >= 0 and ol_notes_col < len(row) else ''
                ol_name = row[ol_name_col] if ol_name_col >= 0 and ol_name_col < len(row) else 'Coach'
                
                if ol_email and '@' in ol_email:
                    stage = get_email_stage(ol_contacted, ol_notes)
                    days = days_since_contact(ol_contacted)
                    
                    should_send = False
                    email_type = None
                    
                    if stage == 'replied':
                        pass
                    elif stage == 'new':
                        should_send = True
                        email_type = 'intro'
                    elif stage == 'intro_sent' and days >= days_between:
                        should_send = True
                        email_type = 'followup_1'
                    elif stage == 'followup1_sent' and days >= days_between:
                        should_send = True
                        email_type = 'followup_2'
                    elif stage == 'followup2_sent' and days >= days_between * 3:
                        should_send = True
                        email_type = 'intro'
                    
                    if should_send:
                        coaches.append({
                            'email': ol_email, 'name': ol_name, 'school': school, 'type': 'ol',
                            'row_idx': row_idx + 2,
                            'contacted_col': ol_contacted_col + 1 if ol_contacted_col >= 0 else None,
                            'notes_col': ol_notes_col + 1 if ol_notes_col >= 0 else None,
                            'email_type': email_type, 'current_notes': ol_notes,
                            'days_since': days  # Track days since last contact for sorting
                        })
        
        # Sort coaches by days since last contact (oldest first, then new coaches)
        # This ensures coaches who haven't been contacted in a while get priority
        coaches.sort(key=lambda c: (-c['days_since'] if c['days_since'] < 900 else 1000))
        
        logger.info(f"Found {len(coaches)} coaches to email (sorted by oldest first)")
        
        if not coaches:
            return jsonify({'success': True, 'sent': 0, 'errors': 0, 
                           'message': 'No coaches to email right now - all either replied, recently contacted, or no valid emails'})
        
        from enterprise.templates import get_template_manager
        from enterprise.responses import get_response_tracker
        
        sent = 0
        errors = 0
        intro_count = 0
        followup1_count = 0
        followup2_count = 0
        
        template_mgr = get_template_manager()
        response_tracker = get_response_tracker()
        
        variables = {
            'athlete_name': athlete.get('name', ''),
            'position': athlete.get('positions', 'OL'),
            'grad_year': athlete.get('graduation_year', '2026'),
            'height': athlete.get('height', ''),
            'weight': athlete.get('weight', ''),
            'gpa': athlete.get('gpa', ''),
            'hudl_link': athlete.get('highlight_url', ''),
            'high_school': athlete.get('high_school', ''),
            'phone': athlete.get('phone', ''),
            'email': athlete.get('email', ''),
        }
        
        # Use Gmail API if available (works on Railway), otherwise SMTP
        use_gmail_api = has_gmail_api()
        smtp = None
        
        if not use_gmail_api:
            # Fallback to SMTP
            try:
                smtp = smtplib.SMTP('smtp.gmail.com', 587)
                smtp.starttls()
                smtp.login(email_addr, app_password)
                logger.info("Using SMTP for sending emails")
            except Exception as e:
                logger.error(f"SMTP connection failed: {e}")
                return jsonify({'success': False, 'error': f'SMTP failed: {str(e)}. Try configuring Gmail API for Railway.', 'sent': 0, 'errors': 0})
        else:
            logger.info("Using Gmail API for sending emails")
        
        for coach in coaches[:limit]:
            try:
                variables['coach_name'] = coach['name'].split()[-1] if coach['name'] else 'Coach'
                variables['school'] = coach['school']
                
                # Get appropriate template based on email type
                email_type = coach.get('email_type', 'intro')
                if email_type == 'followup_1':
                    template = template_mgr.get_followup_template(1)
                elif email_type == 'followup_2':
                    template = template_mgr.get_followup_template(2)
                else:
                    template = template_mgr.get_next_template(coach['type'], coach['school'])
                
                if not template:
                    errors += 1
                    continue
                
                subject, body = template.render(variables)
                coach_email = coach['email'].strip()

                # Validate email format before sending
                import re
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                if not re.match(email_pattern, coach_email):
                    logger.error(f"Invalid email format for {coach['school']}: '{coach_email}'")
                    errors += 1
                    continue

                logger.info(f"Attempting to send to: {coach_email} at {coach['school']}")

                # Send email
                if use_gmail_api:
                    success = send_email_gmail_api(coach_email, subject, body, email_addr)
                else:
                    msg = MIMEMultipart()
                    msg['From'] = email_addr
                    msg['To'] = coach_email
                    msg['Subject'] = subject
                    msg.attach(MIMEText(body, 'plain'))
                    smtp.sendmail(email_addr, coach_email, msg.as_string())
                    success = True
                
                if success:
                    sent += 1
                    
                    # Track counts
                    if email_type == 'followup_1':
                        followup1_count += 1
                    elif email_type == 'followup_2':
                        followup2_count += 1
                    else:
                        intro_count += 1
                    
                    response_tracker.record_sent(
                        coach_email=coach_email, coach_name=coach['name'],
                        school=coach['school'], division='',
                        coach_type=coach['type'], template_id=template.id
                    )
                    
                    # Update sheet
                    if coach.get('contacted_col'):
                        try:
                            sheet.update_cell(coach['row_idx'], coach['contacted_col'], today.strftime('%m/%d/%Y'))
                        except: pass
                    
                    if coach.get('notes_col'):
                        try:
                            current_notes = coach.get('current_notes', '')
                            if email_type == 'followup_1':
                                new_note = f"Follow-up 1 sent {today.strftime('%m/%d')}"
                            elif email_type == 'followup_2':
                                new_note = f"Follow-up 2 sent {today.strftime('%m/%d')}"
                            else:
                                new_note = f"Intro sent {today.strftime('%m/%d')}"
                            
                            # Don't add duplicate notes
                            if new_note.lower() not in current_notes.lower():
                                updated_notes = f"{new_note}; {current_notes}" if current_notes else new_note
                                sheet.update_cell(coach['row_idx'], coach['notes_col'], updated_notes)
                        except: pass
                else:
                    errors += 1
                
                time.sleep(email_settings.get('delay_seconds', 3))
                
            except Exception as e:
                logger.error(f"Error sending to {coach['email']}: {e}")
                errors += 1
        
        if smtp:
            smtp.quit()
        
        return jsonify({
            'success': True, 'sent': sent, 'errors': errors,
            'intro': intro_count, 'followup1': followup1_count, 'followup2': followup2_count,
            'method': 'Gmail API' if use_gmail_api else 'SMTP',
            'message': f'Sent {intro_count} intros, {followup1_count} follow-up 1s, {followup2_count} follow-up 2s'
        })
    except Exception as e:
        logger.error(f"Email send error: {e}")
        return jsonify({'success': False, 'error': str(e), 'sent': 0, 'errors': 0})


@app.route('/api/twitter/mark-dm-sent', methods=['POST'])
def api_twitter_mark_dm_sent():
    """Mark a DM as sent."""
    data = request.get_json()
    try:
        dm_file = CONFIG_DIR / 'dm_sent.json'
        sent = {}
        if dm_file.exists():
            with open(dm_file) as f:
                sent = json.load(f)
        sent[f"{data.get('school')}:{data.get('email')}"] = datetime.now().isoformat()
        with open(dm_file, 'w') as f:
            json.dump(sent, f)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/twitter/mark-wrong', methods=['POST'])
def api_twitter_mark_wrong():
    """Mark a Twitter handle as wrong so it gets re-scraped."""
    data = request.get_json() or {}
    school = data.get('school', '')
    twitter = data.get('twitter', '')
    
    try:
        # Clear from Twitter scraper cache
        try:
            from enterprise.twitter_google_scraper import get_scraper
            scraper = get_scraper()
            # Clear cache for this school
            cache_key = scraper._get_cache_key(data.get('coach_name', ''), school)
            cache_file = scraper.config.cache_dir / f"{cache_key}.json"
            if cache_file.exists():
                cache_file.unlink()
                logger.info(f"Cleared Twitter cache for {school}")
        except Exception as e:
            logger.warning(f"Could not clear Twitter cache: {e}")
        
        # Also clear from sheet
        sheet = get_sheet()
        if sheet:
            all_data = sheet.get_all_values()
            headers = [h.lower() for h in all_data[0]]
            rows = all_data[1:]
            
            def find_col(keywords):
                for i, h in enumerate(headers):
                    for kw in keywords:
                        if kw in h:
                            return i
                return -1
            
            school_col = find_col(['school'])
            ol_twitter_col = find_col(['oc twitter', 'ol twitter'])
            rc_twitter_col = find_col(['rc twitter'])
            
            for row_idx, row in enumerate(rows):
                row_school = row[school_col] if school_col >= 0 and school_col < len(row) else ''
                if row_school.lower() == school.lower():
                    # Clear the Twitter handle
                    if ol_twitter_col >= 0:
                        ol_twitter = row[ol_twitter_col] if ol_twitter_col < len(row) else ''
                        if ol_twitter.replace('@', '').lower() == twitter.lower():
                            sheet.update_cell(row_idx + 2, ol_twitter_col + 1, '')
                            logger.info(f"Cleared OL Twitter for {school}")
                    if rc_twitter_col >= 0:
                        rc_twitter = row[rc_twitter_col] if rc_twitter_col < len(row) else ''
                        if rc_twitter.replace('@', '').lower() == twitter.lower():
                            sheet.update_cell(row_idx + 2, rc_twitter_col + 1, '')
                            logger.info(f"Cleared RC Twitter for {school}")
                    break
        
        return jsonify({'success': True, 'message': f'Cleared @{twitter} for {school}'})
    except Exception as e:
        logger.error(f"Mark wrong error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/email/preview', methods=['POST'])
def api_email_preview():
    """Preview an email with current settings."""
    global settings
    data = request.get_json() or {}
    template_id = data.get('template_id')
    
    try:
        from enterprise.templates import get_template_manager
        manager = get_template_manager()
        
        template = manager.get_template(template_id) if template_id else manager.get_next_template('rc')
        if not template:
            return jsonify({'success': False, 'error': 'No template found'})
        
        athlete = settings.get('athlete', {})
        variables = {
            'athlete_name': athlete.get('name', '[Your Name]'),
            'position': athlete.get('positions', '[Position]'),
            'grad_year': athlete.get('graduation_year', '2026'),
            'height': athlete.get('height', '[Height]'),
            'weight': athlete.get('weight', '[Weight]'),
            'gpa': athlete.get('gpa', '[GPA]'),
            'hudl_link': athlete.get('highlight_url', '[Hudl Link]'),
            'high_school': athlete.get('high_school', '[High School]'),
            'phone': athlete.get('phone', '[Phone]'),
            'email': athlete.get('email', '[Email]'),
            'coach_name': '[Coach Name]',
            'school': '[School Name]',
        }
        
        subject, body = template.render(variables)
        return jsonify({'success': True, 'subject': subject, 'body': body, 'template_name': template.name})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/followups/due')
def api_followups_due():
    """Get due follow-ups."""
    try:
        from enterprise.followups import get_followup_manager
        fm = get_followup_manager()
        due = fm.get_due_followups()
        return jsonify({
            'success': True, 
            'followups': [
                {
                    'id': f.id if hasattr(f, 'id') else str(i),
                    'school': f.school if hasattr(f, 'school') else '',
                    'coach_name': f.coach_name if hasattr(f, 'coach_name') else '',
                    'coach_email': f.coach_email if hasattr(f, 'coach_email') else '',
                    'followup_type': f.followup_type if hasattr(f, 'followup_type') else 'followup',
                    'due_date': f.due_date if hasattr(f, 'due_date') else '',
                } for i, f in enumerate(due)
            ]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'followups': []})


@app.route('/api/followups/send', methods=['POST'])
def api_followups_send():
    """Send a follow-up email."""
    data = request.get_json() or {}
    followup_id = data.get('followup_id')
    
    # For now, just mark as sent - actual sending would need more implementation
    return jsonify({'success': True, 'message': 'Follow-up marked (full send not implemented yet)'})


@app.route('/api/crm/contacts/<contact_id>')
def api_crm_contact_detail(contact_id):
    """Get single contact details."""
    try:
        from enterprise.crm import CRMManager
        crm = CRMManager()
        contact = crm.get_contact(contact_id)
        if contact:
            return jsonify({'success': True, 'contact': contact.to_dict() if hasattr(contact, 'to_dict') else contact})
        return jsonify({'success': False, 'error': 'Contact not found'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ============================================================================
# SCRAPER ROUTES
# ============================================================================

scraper_state = {'running': False, 'log': [], 'processed': 0, 'total': 0}

@app.route('/api/scraper/start', methods=['POST'])
def api_scraper_start():
    """Start the scraper to find Twitter handles."""
    global scraper_state
    data = request.get_json() or {}
    
    scraper_type = data.get('type', 'twitter')
    batch = data.get('batch', 10)
    
    scraper_state = {'running': True, 'log': ['Starting Twitter scraper...'], 'processed': 0, 'total': 0, 'found': 0}
    
    try:
        # Get sheet
        scraper_state['log'].append('Connecting to Google Sheet...')
        sheet = get_sheet()
        if not sheet:
            scraper_state['running'] = False
            scraper_state['log'].append('ERROR: Cannot connect to Google Sheet')
            return jsonify({'success': False, 'error': 'Sheet not connected'})
        
        scraper_state['log'].append('Connected to sheet!')
        
        all_data = sheet.get_all_values()
        if len(all_data) < 2:
            scraper_state['running'] = False
            scraper_state['log'].append('ERROR: No data in sheet')
            return jsonify({'success': False, 'error': 'No data in sheet'})
        
        headers = [h.lower() for h in all_data[0]]
        rows = all_data[1:]
        scraper_state['log'].append(f'Sheet has {len(rows)} rows')
        
        # Find columns - match your exact column names
        def find_col(keywords):
            for i, h in enumerate(headers):
                for kw in keywords:
                    if kw in h:
                        return i
            return -1
        
        school_col = find_col(['school'])
        ol_name_col = find_col(['oline coach', 'oline'])
        ol_twitter_col = find_col(['oc twitter'])
        rc_name_col = find_col(['recruiting coordinator'])
        rc_twitter_col = find_col(['rc twitter'])
        
        scraper_state['log'].append(f'Columns: school={school_col}, ol_name={ol_name_col}, ol_twitter={ol_twitter_col}, rc_name={rc_name_col}, rc_twitter={rc_twitter_col}')
        
        if school_col == -1:
            scraper_state['running'] = False
            scraper_state['log'].append('ERROR: No school column found')
            return jsonify({'success': False, 'error': 'No school column'})
        
        # Find coaches needing Twitter handles
        coaches_to_scrape = []
        for row_idx, row in enumerate(rows, start=2):
            school = row[school_col] if school_col < len(row) else ''
            if not school:
                continue
            
            # Check OL coach
            if ol_name_col >= 0 and ol_twitter_col >= 0:
                ol_name = row[ol_name_col] if ol_name_col < len(row) else ''
                ol_twitter = row[ol_twitter_col] if ol_twitter_col < len(row) else ''
                
                if ol_name and ol_name.strip() and not ol_twitter.strip():
                    coaches_to_scrape.append({
                        'name': ol_name.strip(),
                        'school': school.strip(),
                        'row_idx': row_idx,
                        'twitter_col': ol_twitter_col + 1,
                        'type': 'OL'
                    })
            
            # Check RC
            if rc_name_col >= 0 and rc_twitter_col >= 0:
                rc_name = row[rc_name_col] if rc_name_col < len(row) else ''
                rc_twitter = row[rc_twitter_col] if rc_twitter_col < len(row) else ''
                
                if rc_name and rc_name.strip() and not rc_twitter.strip():
                    coaches_to_scrape.append({
                        'name': rc_name.strip(),
                        'school': school.strip(),
                        'row_idx': row_idx,
                        'twitter_col': rc_twitter_col + 1,
                        'type': 'RC'
                    })
            
            if len(coaches_to_scrape) >= batch:
                break
        
        scraper_state['total'] = len(coaches_to_scrape)
        scraper_state['log'].append(f'Found {len(coaches_to_scrape)} coaches needing Twitter handles')
        
        if not coaches_to_scrape:
            scraper_state['running'] = False
            scraper_state['log'].append('All coaches already have Twitter handles!')
            return jsonify({'success': True, 'message': 'Nothing to scrape'})
        
        # List coaches to scrape
        for c in coaches_to_scrape[:5]:
            scraper_state['log'].append(f'  - {c["name"]} ({c["school"]})')
        if len(coaches_to_scrape) > 5:
            scraper_state['log'].append(f'  ... and {len(coaches_to_scrape) - 5} more')
        
        # Import scraper
        scraper_state['log'].append('Loading Twitter scraper...')
        try:
            from enterprise.twitter_google_scraper import GoogleTwitterScraper
            scraper = GoogleTwitterScraper()
            scraper_state['log'].append('Scraper loaded!')
        except Exception as e:
            scraper_state['running'] = False
            scraper_state['log'].append(f'ERROR: Could not load scraper: {e}')
            return jsonify({'success': False, 'error': str(e)})
        
        # Run scraping
        found_count = 0
        for i, coach in enumerate(coaches_to_scrape):
            if not scraper_state['running']:
                scraper_state['log'].append('Scraping stopped by user')
                break
            
            scraper_state['processed'] = i + 1
            search_query = f'{coach["name"]} {coach["school"]} football twitter'
            scraper_state['log'].append(f'[{i+1}/{len(coaches_to_scrape)}] Searching: "{search_query}"')
            
            try:
                handle = scraper.find_twitter_handle(coach['name'], coach['school'])
                
                if handle:
                    # Update sheet
                    try:
                        sheet.update_cell(coach['row_idx'], coach['twitter_col'], f'@{handle}')
                        scraper_state['log'].append(f'  ✓ Found @{handle} - updated row {coach["row_idx"]}')
                        found_count += 1
                        scraper_state['found'] = found_count
                    except Exception as sheet_err:
                        scraper_state['log'].append(f'  ✓ Found @{handle} but failed to update sheet: {sheet_err}')
                else:
                    scraper_state['log'].append(f'  ✗ No Twitter found for {coach["name"]}')
            except Exception as e:
                scraper_state['log'].append(f'  ERROR: {e}')
        
        scraper_state['running'] = False
        scraper_state['log'].append(f'')
        scraper_state['log'].append(f'=== DONE ===')
        scraper_state['log'].append(f'Found {found_count} Twitter handles out of {len(coaches_to_scrape)} coaches')
        
        return jsonify({'success': True, 'found': found_count, 'total': len(coaches_to_scrape)})
        
    except Exception as e:
        scraper_state['running'] = False
        scraper_state['log'].append(f'ERROR: {e}')
        logger.error(f"Scraper error: {e}")
        import traceback
        scraper_state['log'].append(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/scraper/stop', methods=['POST'])
def api_scraper_stop():
    """Stop the scraper."""
    global scraper_state
    scraper_state['running'] = False
    scraper_state['log'].append('Scraper stopped by user')
    return jsonify({'success': True})


@app.route('/api/scraper/status')
def api_scraper_status():
    """Get scraper status."""
    return jsonify(scraper_state)


# ============================================================================
# TEST EMAIL ROUTE
# ============================================================================

@app.route('/api/email/test', methods=['POST'])
def api_email_test():
    """Send a test email to yourself."""
    data = request.get_json() or {}
    template_id = data.get('template_id')
    
    # Reload settings to get env vars
    current_settings = load_settings()
    athlete = current_settings.get('athlete', {})
    
    email_addr = ENV_EMAIL_ADDRESS
    
    logger.info(f"Test email - using email: {email_addr}, Gmail API: {has_gmail_api()}")
    
    if not email_addr:
        return jsonify({'success': False, 'error': 'Email address not set - check EMAIL_ADDRESS env var'})
    
    if not has_gmail_api() and not ENV_APP_PASSWORD:
        return jsonify({'success': False, 'error': 'No email credentials - set GMAIL_REFRESH_TOKEN or APP_PASSWORD'})
    
    try:
        from enterprise.templates import get_template_manager
        
        manager = get_template_manager()
        template = manager.get_template(template_id) if template_id else manager.get_next_template('rc')
        
        if not template:
            return jsonify({'success': False, 'error': 'No template found'})
        
        variables = {
            'athlete_name': athlete.get('name', 'Test'),
            'position': athlete.get('positions', 'OL'),
            'grad_year': athlete.get('graduation_year', '2026'),
            'height': athlete.get('height', ''),
            'weight': athlete.get('weight', ''),
            'gpa': athlete.get('gpa', ''),
            'hudl_link': athlete.get('highlight_url', ''),
            'high_school': athlete.get('high_school', ''),
            'phone': athlete.get('phone', ''),
            'email': athlete.get('email', ''),
            'coach_name': 'Test Coach',
            'school': 'Test University',
        }
        
        subject, body = template.render(variables)
        subject = '[TEST] ' + subject
        
        # Use auto method (Gmail API or SMTP)
        success = send_email_auto(email_addr, subject, body, email_addr)
        
        if success:
            method = "Gmail API" if has_gmail_api() else "SMTP"
            return jsonify({'success': True, 'sent_to': email_addr, 'method': method})
        else:
            return jsonify({'success': False, 'error': 'Failed to send email - check logs'})
            
    except Exception as e:
        logger.error(f"Test email error: {type(e).__name__}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': f'{type(e).__name__}: {str(e)}'})


# ============================================================================
# SHEETS CONNECTION ROUTES
# ============================================================================

@app.route('/api/sheets/test')
def api_sheets_test():
    """Test Google Sheets connection with detailed error reporting."""
    import os
    
    # Check prerequisites - either env var or local file
    has_creds = bool(ENV_GOOGLE_CREDENTIALS) or os.path.exists('credentials.json')
    if not has_creds:
        return jsonify({
            'connected': False, 
            'error': 'No Google credentials configured',
            'help': 'Set GOOGLE_CREDENTIALS env var or place credentials.json in app folder'
        })
    
    # Check if gspread is available
    if not HAS_SHEETS:
        return jsonify({
            'connected': False,
            'error': 'Google Sheets library not installed',
            'help': 'Run: pip install gspread google-auth'
        })
    
    # Try to get sheet
    sheet = get_sheet()
    if sheet:
        try:
            data = sheet.get_all_values()
            headers = data[0] if data else []
            return jsonify({
                'connected': True, 
                'rows': len(data),
                'headers': headers,  # Show ALL headers
                'spreadsheet': settings.get('sheets', {}).get('spreadsheet_name', 'bardeen'),
                'using_env': bool(ENV_GOOGLE_CREDENTIALS)
            })
        except Exception as e:
            return jsonify({'connected': False, 'error': str(e)})
    
    # Get more detailed error
    try:
        from sheets.manager import SheetsManager, SheetsConfig
        config = SheetsConfig(spreadsheet_name=settings.get('sheets', {}).get('spreadsheet_name', 'bardeen'))
        manager = SheetsManager(config=config)
        connected = manager.connect()
        if not connected:
            error = getattr(manager, '_connection_error', 'Unknown connection error')
            
            return jsonify({
                'connected': False,
                'error': error,
                'help': 'Check GOOGLE_CREDENTIALS env var or share spreadsheet with service account'
            })
    except Exception as e:
        return jsonify({'connected': False, 'error': str(e)})


@app.route('/api/sheets/credentials', methods=['POST'])
def api_sheets_credentials():
    """Save Google Sheets credentials."""
    data = request.get_json() or {}
    credentials_content = data.get('credentials', '')
    
    if not credentials_content:
        return jsonify({'success': False, 'error': 'No credentials provided'})
    
    try:
        # Validate JSON
        import json
        creds = json.loads(credentials_content)
        
        # Save to credentials.json
        creds_path = Path('credentials.json')
        with open(creds_path, 'w') as f:
            json.dump(creds, f, indent=2)
        
        return jsonify({'success': True, 'message': 'Credentials saved'})
    except json.JSONDecodeError:
        return jsonify({'success': False, 'error': 'Invalid JSON'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ============================================================================
# INBOX/RESPONSE CHECKING ROUTE
# ============================================================================

@app.route('/api/inbox/test')
def api_inbox_test():
    """Test inbox connection for response checking."""
    
    # Use Gmail API (works on Railway)
    if has_gmail_api():
        try:
            service = get_gmail_service()
            if service:
                results = service.users().messages().list(userId='me', maxResults=10).execute()
                count = results.get('resultSizeEstimate', 0)
                return jsonify({'success': True, 'count': min(count, 100), 'method': 'Gmail API'})
            else:
                return jsonify({'success': False, 'error': 'Could not connect to Gmail API'})
        except Exception as e:
            return jsonify({'success': False, 'error': f'Gmail API error: {str(e)}'})
    else:
        return jsonify({'success': False, 'error': 'Gmail API not configured. Add GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REFRESH_TOKEN.'})


@app.route('/api/twitter/send-dm', methods=['POST'])
def api_twitter_send_dm():
    """Send a Twitter DM."""
    data = request.get_json() or {}
    handle = data.get('handle', '').strip().lstrip('@')
    message = data.get('message', '')
    
    if not handle or not message:
        return jsonify({'success': False, 'error': 'Handle and message required'})
    
    if len(message) > 500:
        return jsonify({'success': False, 'error': 'Message too long (max 500 chars)'})
    
    try:
        from outreach.twitter_sender import get_twitter_sender
        sender = get_twitter_sender()
        
        if not sender.check_logged_in():
            return jsonify({'success': False, 'error': 'Not logged in to Twitter'})
        
        success = sender.send_dm(handle, message)
        if success:
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': 'Failed to send DM'})
    except ImportError:
        return jsonify({'success': False, 'error': 'Twitter module not available - install selenium'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ============================================================================
# MAIN
# ============================================================================

# ============================================================================
# AUTO-SEND SCHEDULER
# ============================================================================

import threading

auto_send_state = {
    'enabled': False,
    'last_run': None,
    'next_run': None,
    'last_result': None,
    'running': False
}

def auto_send_emails():
    """Background task to auto-send emails."""
    global auto_send_state
    
    if auto_send_state['running']:
        return  # Already running
    
    auto_send_state['running'] = True
    auto_send_state['last_run'] = datetime.now().isoformat()
    
    try:
        current_settings = load_settings()
        
        # Check if auto-send is enabled
        if not current_settings.get('email', {}).get('auto_send_enabled', False):
            auto_send_state['running'] = False
            return
        
        limit = current_settings.get('email', {}).get('auto_send_count', 100)
        
        # Use the same logic as manual send
        with app.test_request_context():
            sheet = get_sheet()
            if not sheet:
                auto_send_state['last_result'] = {'error': 'Sheet not connected'}
                auto_send_state['running'] = False
                return
            
            # Make a fake request to the send endpoint
            with app.test_client() as client:
                response = client.post('/api/email/send', 
                    json={'limit': limit},
                    content_type='application/json'
                )
                result = response.get_json()
                auto_send_state['last_result'] = result
                
                if result.get('sent', 0) > 0:
                    logger.info(f"Auto-send: Sent {result['sent']} emails")
                    
                    # Send notification if enabled
                    if current_settings.get('notifications', {}).get('enabled'):
                        send_phone_notification(
                            title="Emails Sent!",
                            message=f"Auto-sent {result['sent']} emails to coaches. {result.get('intro', 0)} intros, {result.get('followup1', 0)} follow-up 1s, {result.get('followup2', 0)} follow-up 2s."
                        )
    
    except Exception as e:
        logger.error(f"Auto-send error: {e}")
        auto_send_state['last_result'] = {'error': str(e)}
    
    finally:
        auto_send_state['running'] = False
        auto_send_state['next_run'] = (datetime.now() + timedelta(hours=24)).isoformat()


def send_daily_reminder():
    """Send a daily reminder notification if auto-send is not enabled."""
    try:
        current_settings = load_settings()
        
        # Only send reminder if notifications are enabled but auto-send is not
        if current_settings.get('notifications', {}).get('enabled'):
            if not current_settings.get('email', {}).get('auto_send_enabled', False):
                send_phone_notification(
                    title="Time to Send Emails!",
                    message="Open Coach Outreach Pro and click 'Send Emails' to reach out to coaches today."
                )
    except Exception as e:
        logger.error(f"Reminder error: {e}")


def check_responses_background():
    """Check for coach responses in background and cache results."""
    global cached_responses
    
    try:
        with app.app_context():
            if not has_gmail_api():
                return
            
            sheet = get_sheet()
            if not sheet:
                return
            
            all_data = sheet.get_all_values()
            if len(all_data) < 2:
                return
            
            headers = [h.lower() for h in all_data[0]]
            rows = all_data[1:]
            
            def find_col(keywords):
                for i, h in enumerate(headers):
                    for kw in keywords:
                        if kw in h:
                            return i
                return -1
            
            rc_email_col = find_col(['rc email'])
            ol_email_col = find_col(['oc email', 'ol email'])
            rc_contacted_col = find_col(['rc contacted'])
            ol_contacted_col = find_col(['ol contacted', 'oc contacted'])
            school_col = find_col(['school'])
            
            # Get coaches we've contacted
            coach_emails = []
            for row in rows:
                school = row[school_col] if school_col >= 0 and school_col < len(row) else ''
                
                if rc_email_col >= 0 and rc_email_col < len(row):
                    rc_email = row[rc_email_col].strip()
                    rc_contacted = row[rc_contacted_col].strip() if rc_contacted_col >= 0 and rc_contacted_col < len(row) else ''
                    if rc_email and '@' in rc_email and rc_contacted:
                        coach_emails.append({'email': rc_email, 'school': school})
                
                if ol_email_col >= 0 and ol_email_col < len(row):
                    ol_email = row[ol_email_col].strip()
                    ol_contacted = row[ol_contacted_col].strip() if ol_contacted_col >= 0 and ol_contacted_col < len(row) else ''
                    if ol_email and '@' in ol_email and ol_contacted:
                        coach_emails.append({'email': ol_email, 'school': school})
            
            # Check Gmail for responses
            responses = []
            service = get_gmail_service()
            if service:
                for coach in coach_emails:
                    try:
                        query = f"from:{coach['email']}"
                        results = service.users().messages().list(userId='me', q=query, maxResults=1).execute()
                        messages = results.get('messages', [])
                        
                        if messages:
                            msg = service.users().messages().get(userId='me', id=messages[0]['id'], format='metadata', metadataHeaders=['Subject', 'Date']).execute()
                            headers_dict = {h['name']: h['value'] for h in msg.get('payload', {}).get('headers', [])}
                            
                            responses.append({
                                'email': coach['email'],
                                'school': coach['school'],
                                'subject': headers_dict.get('Subject', 'No subject'),
                                'date': headers_dict.get('Date', ''),
                                'snippet': msg.get('snippet', '')[:100]
                            })
                    except:
                        pass
            
            # Cache and notify
            old_count = len(cached_responses)
            cached_responses = responses
            new_count = len(responses)
            
            if new_count > old_count:
                # New responses found!
                current_settings = load_settings()
                if current_settings.get('notifications', {}).get('enabled'):
                    send_phone_notification(
                        title="New Coach Response!",
                        message=f"You have {new_count} responses from coaches. Check the app!"
                    )
            
            logger.info(f"Response check complete: {new_count} responses found")
            
    except Exception as e:
        logger.error(f"Background response check error: {e}")


def start_auto_send_scheduler():
    """Start the background scheduler for auto-sending and reminders with random timing."""
    def scheduler_loop():
        last_send_date = None
        last_reminder_date = None
        last_response_check = None
        
        # Random hour between 8am and 6pm for sending
        send_hour = random.randint(8, 18)
        send_minute = random.randint(0, 59)
        logger.info(f"Today's auto-send scheduled for {send_hour}:{send_minute:02d}")
        
        while True:
            try:
                current_settings = load_settings()
                today = datetime.now().date()
                current_hour = datetime.now().hour
                current_minute = datetime.now().minute
                now = datetime.now()
                
                # Pick new random time each day
                if last_send_date != today:
                    send_hour = random.randint(8, 18)
                    send_minute = random.randint(0, 59)
                    logger.info(f"New day - auto-send scheduled for {send_hour}:{send_minute:02d}")
                
                # Auto-send at the random time if enabled
                if current_settings.get('email', {}).get('auto_send_enabled', False):
                    if last_send_date != today:
                        if current_hour > send_hour or (current_hour == send_hour and current_minute >= send_minute):
                            logger.info(f"Auto-send triggered at {current_hour}:{current_minute:02d}")
                            auto_send_emails()
                            last_send_date = today
                
                # Send daily reminder at random morning time (8-10am) if auto-send is OFF
                reminder_hour = 9  # Could also randomize this
                if current_hour >= reminder_hour and last_reminder_date != today:
                    if not current_settings.get('email', {}).get('auto_send_enabled', False):
                        send_daily_reminder()
                    last_reminder_date = today
                
                # Check for responses every hour
                if last_response_check is None or (now - last_response_check).seconds >= 3600:
                    if has_gmail_api():
                        logger.info("Hourly response check starting...")
                        try:
                            check_responses_background()
                        except Exception as e:
                            logger.error(f"Response check error: {e}")
                    last_response_check = now
                    
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            
            # Check every 15 minutes for more precise timing
            time.sleep(900)
    
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    logger.info("Auto-send scheduler started (random daily timing enabled)")


@app.route('/api/auto-send/status')
def api_auto_send_status():
    """Get auto-send status."""
    current_settings = load_settings()
    return jsonify({
        **auto_send_state,
        'enabled': current_settings.get('email', {}).get('auto_send_enabled', False),
        'emails_per_day': current_settings.get('email', {}).get('auto_send_count', 100)
    })


@app.route('/api/auto-send/toggle', methods=['POST'])
def api_auto_send_toggle():
    """Toggle auto-send on/off."""
    global settings
    data = request.get_json() or {}
    enabled = data.get('enabled', False)
    count = data.get('count', 100)
    
    settings['email']['auto_send_enabled'] = enabled
    settings['email']['auto_send_count'] = count
    save_settings(settings)
    
    if enabled:
        logger.info(f"Auto-send enabled: {count} emails/day")
    else:
        logger.info("Auto-send disabled")
    
    return jsonify({'success': True, 'enabled': enabled})


@app.route('/api/auto-send/run-now', methods=['POST'])
def api_auto_send_run_now():
    """Manually trigger auto-send."""
    if auto_send_state['running']:
        return jsonify({'success': False, 'error': 'Already running'})
    
    # Run in background thread
    thread = threading.Thread(target=auto_send_emails)
    thread.start()
    
    return jsonify({'success': True, 'message': 'Auto-send started'})


# ============================================================================
# PHONE NOTIFICATIONS (via ntfy.sh)
# ============================================================================

def send_phone_notification(title: str, message: str, channel: str = None):
    """Send a push notification via ntfy.sh (free service)."""
    try:
        current_settings = load_settings()
        channel = channel or current_settings.get('notifications', {}).get('channel', '')
        
        if not channel:
            logger.warning("No notification channel configured")
            return False
        
        import requests
        response = requests.post(
            f"https://ntfy.sh/{channel}",
            data=message.encode('utf-8'),
            headers={
                "Title": title,
                "Priority": "default",
                "Tags": "football"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"Notification sent to channel: {channel}")
            return True
        else:
            logger.warning(f"Notification failed: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Notification error: {e}")
        return False


@app.route('/api/notifications/toggle', methods=['POST'])
def api_notifications_toggle():
    """Toggle phone notifications."""
    global settings
    data = request.get_json() or {}
    enabled = data.get('enabled', False)
    channel = data.get('channel', '')
    
    if 'notifications' not in settings:
        settings['notifications'] = {}
    
    settings['notifications']['enabled'] = enabled
    settings['notifications']['channel'] = channel
    save_settings(settings)
    
    return jsonify({'success': True, 'enabled': enabled, 'channel': channel})


@app.route('/api/notifications/test', methods=['POST'])
def api_notifications_test():
    """Send a test notification."""
    data = request.get_json() or {}
    channel = data.get('channel', '')
    
    if not channel:
        return jsonify({'success': False, 'error': 'No channel provided'})
    
    success = send_phone_notification(
        title="Coach Outreach Pro",
        message="Test notification working! You'll get reminders when it's time to send emails.",
        channel=channel
    )
    
    return jsonify({'success': success, 'error': None if success else 'Failed to send notification'})


# ============================================================================
# MAIN
# ============================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Coach Outreach Pro')
    parser.add_argument('--port', type=int, default=5001, help='Port to run on')
    parser.add_argument('--debug', action='store_true', help='Debug mode')
    args = parser.parse_args()
    
    # Start auto-send scheduler
    start_auto_send_scheduler()
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║           Coach Outreach Pro - Enterprise Edition            ║
║                        Version 8.2.0                         ║
╠══════════════════════════════════════════════════════════════╣
║  Open in browser: http://localhost:{args.port}                    ║
║  Auto-send scheduler: ACTIVE                                 ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Don't auto-open browser here - launch scripts handle it
    # This prevents double-opening when using start.command
    app.run(host='0.0.0.0', port=args.port, debug=args.debug, threaded=True)


if __name__ == '__main__':
    main()
