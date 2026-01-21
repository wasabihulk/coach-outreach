"""
sheets/manager.py - Google Sheets Management (Simplified)
============================================================================
Handles reading and writing to the main Google Sheet.
NO separate review sheet - all data stays in Sheet 1.

Author: Coach Outreach System
Version: 2.3.0
============================================================================
"""

import os
import time
import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps

try:
    import gspread
    from gspread import Worksheet, Spreadsheet
    from gspread.exceptions import GSpreadException, APIError, SpreadsheetNotFound
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False
    gspread = None

# Try modern google-auth first, fall back to oauth2client
try:
    from google.oauth2.service_account import Credentials
    USE_GOOGLE_AUTH = True
except ImportError:
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        USE_GOOGLE_AUTH = False
    except ImportError:
        USE_GOOGLE_AUTH = None

# Import types
try:
    from core.types import SchoolRecord, StaffMember, ExtractionResult, ProcessingStatus
except ImportError:
    # Fallback if core not available
    SchoolRecord = dict
    StaffMember = dict
    ExtractionResult = dict
    ProcessingStatus = str

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class SheetsConfig:
    """Configuration for Google Sheets."""
    credentials_file: str = 'credentials.json'
    spreadsheet_name: str = 'bardeen'
    main_sheet_name: str = 'Sheet1'
    
    # Rate limiting
    min_request_delay: float = 0.5
    max_retries: int = 5
    
    # Column mappings (0-indexed)
    column_map: Dict[str, int] = field(default_factory=lambda: {
        'school': 0,
        'url': 1,
        'rc_name': 2,
        'ol_name': 3,
        'rc_twitter': 4,
        'ol_twitter': 5,
        'rc_email': 6,
        'ol_email': 7,
        'rc_contacted': 8,
        'ol_contacted': 9,
        'rc_notes': 10,
        'ol_notes': 11,
    })


DEFAULT_HEADERS = [
    'School', 'URL', 'recruiting coordinator name', 'Oline Coach',
    'RC twitter', 'OC twitter', 'RC email', 'OC email',
    'RC Contacted', 'OL Contacted', 'RC Notes', 'OL Notes',
]


# ============================================================================
# DECORATORS
# ============================================================================

def retry_on_error(max_retries: int = 3, delay: float = 1.0):
    """Retry on transient errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except APIError as e:
                    last_error = e
                    if '429' in str(e) or 'quota' in str(e).lower():
                        wait = delay * (2 ** attempt)
                        logger.warning(f"Rate limited, waiting {wait}s...")
                        time.sleep(wait)
                        continue
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                        continue
                    raise
                except GSpreadException as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                        continue
                    raise
            raise last_error or Exception("Max retries exceeded")
        return wrapper
    return decorator


def rate_limited(min_delay: float = 0.5):
    """Add minimum delay between operations."""
    def decorator(func):
        last_call = [0.0]
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_call[0]
            if elapsed < min_delay:
                time.sleep(min_delay - elapsed)
            result = func(*args, **kwargs)
            last_call[0] = time.time()
            return result
        return wrapper
    return decorator


# ============================================================================
# SHEETS MANAGER
# ============================================================================

class SheetsManager:
    """
    Manages Google Sheets operations.
    All data stays in Sheet 1 - no separate review sheet.
    """
    
    def __init__(self, config: Optional[SheetsConfig] = None):
        self.config = config or SheetsConfig()
        self._client = None
        self._spreadsheet: Optional[Spreadsheet] = None
        self._sheet: Optional[Worksheet] = None
        self._connected = False
        self._connection_error = None
        
        # Stats
        self._reads = 0
        self._writes = 0
        self._errors = 0
    
    def connect(self) -> bool:
        """Connect to Google Sheets."""
        if not HAS_GSPREAD:
            self._connection_error = "gspread not installed. Run: pip install gspread google-auth"
            logger.error(self._connection_error)
            return False
            
        try:
            # Try environment variable first (for Railway/Render deployment)
            google_creds_json = os.environ.get('GOOGLE_CREDENTIALS', '')
            
            if google_creds_json:
                # Load from environment variable (JSON string)
                import json
                
                # Clean up potential Railway escaping issues
                creds_str = google_creds_json.strip()
                
                # If Railway wrapped it in extra quotes, remove them
                if creds_str.startswith('"') and creds_str.endswith('"'):
                    creds_str = creds_str[1:-1]
                
                # Replace escaped newlines
                creds_str = creds_str.replace('\\\\n', '\\n')
                
                try:
                    creds_dict = json.loads(creds_str)
                except json.JSONDecodeError as e:
                    # Log first 200 chars for debugging
                    logger.error(f"JSON parse error: {e}")
                    logger.error(f"First 200 chars: {creds_str[:200]}")
                    self._connection_error = f"Invalid JSON in GOOGLE_CREDENTIALS: {e}"
                    return False
                
                # Verify required fields
                required = ['client_email', 'token_uri', 'private_key']
                missing = [f for f in required if f not in creds_dict]
                if missing:
                    logger.error(f"Missing fields in credentials: {missing}")
                    logger.error(f"Found keys: {list(creds_dict.keys())}")
                    self._connection_error = f"Service account info was not in the expected format, missing fields {', '.join(missing)}."
                    return False
                
                if USE_GOOGLE_AUTH:
                    scopes = [
                        'https://www.googleapis.com/auth/spreadsheets',
                        'https://www.googleapis.com/auth/drive'
                    ]
                    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
                elif USE_GOOGLE_AUTH is False:
                    from oauth2client.service_account import ServiceAccountCredentials
                    scope = [
                        'https://spreadsheets.google.com/feeds',
                        'https://www.googleapis.com/auth/drive'
                    ]
                    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
                else:
                    self._connection_error = "No auth library installed"
                    return False
                    
                logger.info("Using credentials from environment variable")
                
            elif os.path.exists(self.config.credentials_file):
                # Fall back to file (for local development)
                if USE_GOOGLE_AUTH:
                    scopes = [
                        'https://www.googleapis.com/auth/spreadsheets',
                        'https://www.googleapis.com/auth/drive'
                    ]
                    creds = Credentials.from_service_account_file(
                        self.config.credentials_file, scopes=scopes
                    )
                elif USE_GOOGLE_AUTH is False:
                    scope = [
                        'https://spreadsheets.google.com/feeds',
                        'https://www.googleapis.com/auth/drive'
                    ]
                    creds = ServiceAccountCredentials.from_json_keyfile_name(
                        self.config.credentials_file, scope
                    )
                else:
                    self._connection_error = "No auth library installed. Run: pip install google-auth"
                    logger.error(self._connection_error)
                    return False
                    
                logger.info(f"Using credentials from file: {self.config.credentials_file}")
            else:
                self._connection_error = f"No credentials found. Set GOOGLE_CREDENTIALS env var or provide {self.config.credentials_file}"
                logger.error(self._connection_error)
                return False
                
            self._client = gspread.authorize(creds)
            
            sheet_identifier = self.config.spreadsheet_name
            
            # Check if it looks like a Sheet ID
            is_sheet_id = (
                len(sheet_identifier) > 20 and 
                all(c.isalnum() or c in '-_' for c in sheet_identifier) and
                ' ' not in sheet_identifier
            )
            
            if is_sheet_id:
                logger.info(f"Opening sheet by ID: {sheet_identifier[:20]}...")
                self._spreadsheet = self._client.open_by_key(sheet_identifier)
            else:
                logger.info(f"Opening sheet by name: {sheet_identifier}")
                self._spreadsheet = self._client.open(sheet_identifier)
            
            self._sheet = self._spreadsheet.sheet1
            self._connected = True
            self._connection_error = None
            
            logger.info(f"Connected to spreadsheet successfully")
            return True
            
        except SpreadsheetNotFound:
            self._connection_error = f"Spreadsheet '{self.config.spreadsheet_name}' not found. Make sure you've shared it with the service account email."
            logger.error(self._connection_error)
            return False
        except Exception as e:
            self._connection_error = str(e)
            logger.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from sheets."""
        self._sheet = None
        self._spreadsheet = None
        self._client = None
        self._connected = False
        logger.info("Disconnected from sheets")
    
    @property
    def is_connected(self) -> bool:
        return self._connected and self._sheet is not None
    
    def get_col_index(self, col_name: str) -> int:
        """Get 0-indexed column index."""
        return self.config.column_map.get(col_name, -1)
    
    # =========================================================================
    # READ OPERATIONS
    # =========================================================================
    
    @retry_on_error(max_retries=3)
    @rate_limited(min_delay=0.5)
    def get_all_data(self) -> List[List[str]]:
        """Get all data from sheet."""
        if not self._sheet:
            return []
        try:
            data = self._sheet.get_all_values()
            self._reads += 1
            return data
        except Exception as e:
            logger.error(f"Failed to get data: {e}")
            self._errors += 1
            return []
    
    def get_schools_to_process(self, reverse: bool = False) -> List:
        """
        Get schools that need processing.
        
        Args:
            reverse: If True, start from bottom of sheet (newest schools first)
        """
        data = self.get_all_data()
        if len(data) < 2:
            logger.info("No data in sheet")
            return []
        
        headers = data[0]
        rows = data[1:]
        
        logger.info(f"Found {len(rows)} total rows in sheet")
        
        schools = []
        for row_idx, row in enumerate(rows):
            row_num = row_idx + 2  # 1-indexed + header
            
            school = self._safe_get(row, self.get_col_index('school'))
            url = self._safe_get(row, self.get_col_index('url'))
            rc_name = self._safe_get(row, self.get_col_index('rc_name'))
            ol_name = self._safe_get(row, self.get_col_index('ol_name'))
            
            if not url or not url.startswith('http'):
                continue
            
            # Check what needs processing:
            # - Empty = needs processing
            # - "REVIEW:..." = needs re-processing (low confidence)
            # - Any other value = already done
            rc_done = rc_name and not rc_name.startswith('REVIEW:')
            ol_done = ol_name and not ol_name.startswith('REVIEW:')
            
            # Skip if both already done
            if rc_done and ol_done:
                continue
            
            # Create SchoolRecord with empty names so needs_rc/needs_ol properties work
            try:
                from core.types import SchoolRecord, ProcessingStatus
                record = SchoolRecord(
                    row_index=row_num,
                    school_name=school,
                    staff_url=url,
                    rc_name=rc_name if rc_done else "",  # Empty if needs processing
                    ol_name=ol_name if ol_done else "",  # Empty if needs processing
                    status=ProcessingStatus.NOT_PROCESSED
                )
                schools.append(record)
            except ImportError:
                # Fallback to dict if types not available
                schools.append({
                    'row_index': row_num,
                    'school_name': school,
                    'staff_url': url,
                    'needs_rc': not rc_done,
                    'needs_ol': not ol_done,
                })
        
        if reverse:
            schools.reverse()
            logger.info(f"Found {len(schools)} schools (starting from bottom)")
        else:
            logger.info(f"Found {len(schools)} schools (starting from top)")
        
        return schools
    
    def _safe_get(self, row: List, idx: int, default: str = '') -> str:
        """Safely get value from row."""
        if idx < 0 or idx >= len(row):
            return default
        return str(row[idx]).strip() if row[idx] else default
    
    # =========================================================================
    # WRITE OPERATIONS
    # =========================================================================
    
    @retry_on_error(max_retries=3)
    @rate_limited(min_delay=0.5)
    def update_cell(self, row: int, col: int, value: str) -> bool:
        """Update a single cell."""
        if not self._sheet:
            return False
        try:
            self._sheet.update_cell(row, col, value)
            self._writes += 1
            return True
        except Exception as e:
            logger.error(f"Failed to update cell ({row}, {col}): {e}")
            self._errors += 1
            return False
    
    def update_rc(self, row_index: int, name: str, email: str = None) -> bool:
        """Update RC name and optionally email."""
        success = True
        
        col = self.get_col_index('rc_name') + 1  # 1-indexed
        if col > 0:
            success &= self.update_cell(row_index, col, name)
        
        if email:
            col = self.get_col_index('rc_email') + 1
            if col > 0:
                success &= self.update_cell(row_index, col, email)
        
        return success
    
    def update_ol(self, row_index: int, name: str, email: str = None) -> bool:
        """Update OL name and optionally email."""
        success = True
        
        col = self.get_col_index('ol_name') + 1  # 1-indexed
        if col > 0:
            success &= self.update_cell(row_index, col, name)
        
        if email:
            col = self.get_col_index('ol_email') + 1
            if col > 0:
                success &= self.update_cell(row_index, col, email)
        
        return success
    
    # =========================================================================
    # STATS
    # =========================================================================
    
    def get_stats(self) -> Dict[str, int]:
        """Get basic stats from sheet."""
        try:
            data = self.get_all_data()
            if len(data) < 2:
                return {}
            
            rows = data[1:]
            
            rc_col = self.get_col_index('rc_name')
            ol_col = self.get_col_index('ol_name')
            rc_email_col = self.get_col_index('rc_email')
            ol_email_col = self.get_col_index('ol_email')
            rc_twitter_col = self.get_col_index('rc_twitter')
            ol_twitter_col = self.get_col_index('ol_twitter')
            
            total = len(rows)
            rc_count = 0
            ol_count = 0
            rc_review = 0
            ol_review = 0
            emails = 0
            twitter = 0
            
            for row in rows:
                # RC
                rc_val = self._safe_get(row, rc_col)
                if rc_val:
                    if rc_val.startswith('REVIEW:'):
                        rc_review += 1
                    else:
                        rc_count += 1
                
                # OL
                ol_val = self._safe_get(row, ol_col)
                if ol_val:
                    if ol_val.startswith('REVIEW:'):
                        ol_review += 1
                    else:
                        ol_count += 1
                
                # Emails
                if self._safe_get(row, rc_email_col):
                    emails += 1
                if self._safe_get(row, ol_email_col):
                    emails += 1
                
                # Twitter
                if self._safe_get(row, rc_twitter_col):
                    twitter += 1
                if self._safe_get(row, ol_twitter_col):
                    twitter += 1
            
            return {
                'total': total,
                'rc': rc_count,
                'ol': ol_count,
                'review': rc_review + ol_review,
                'emails': emails,
                'twitter': twitter,
            }
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
