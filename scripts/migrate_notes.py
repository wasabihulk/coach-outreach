"""
migrate_notes.py - Migrate Notes data to new tracking columns
============================================================================
This script cleans up the Notes columns and moves data to proper columns:
- Follow-up tracking → removed (new Stage/Next Contact columns handle this)
- RESPONDED → RC/OL Responded column
- DM sent → Twitter Status = "messaged"
- Followed only → Twitter Status = "followed"
- Wrong Twitter → Twitter Status = "wrong"

Run this ONCE after adding the new column headers to your sheet.

Author: Coach Outreach System
Version: 1.0.0
============================================================================
"""

import re
import sys
import os
import time
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sheets.manager import SheetsManager, SheetsConfig


# New column indices (0-indexed) - UPDATE THESE IF YOUR SHEET IS DIFFERENT
NEW_COLUMNS = {
    'rc_stage': 12,          # M
    'rc_next_contact': 13,   # N
    'ol_stage': 14,          # O
    'ol_next_contact': 15,   # P
    'rc_responded': 16,      # Q
    'ol_responded': 17,      # R
    'rc_twitter_status': 18, # S
    'ol_twitter_status': 19, # T
    'rc_email_status': 20,   # U
    'ol_email_status': 21,   # V
}

# Patterns to extract from notes
PATTERNS = {
    'responded': [
        r'RESPONDED\s*(\d{1,2}/\d{1,2}(?:/\d{2,4})?)?',
        r'Response received\s*(\d{1,2}/\d{1,2}(?:/\d{2,4})?)?',
        r'responded\s*(\d{1,2}/\d{1,2}(?:/\d{2,4})?)?',
    ],
    'dm_sent': [
        r'DM sent\s*(\d{1,2}/\d{1,2}(?:/\d{2,4})?)?',
        r'messaged\s*(\d{1,2}/\d{1,2}(?:/\d{2,4})?)?',
    ],
    'followed': [
        r'Followed only',
        r'followed only',
        r'can only follow',
    ],
    'wrong_twitter': [
        r'Wrong Twitter[:\s]*(https?://[^\s;]+)?',
        r'wrong twitter[:\s]*(https?://[^\s;]+)?',
        r'Twitter wrong',
    ],
    # These patterns will be REMOVED (no longer needed)
    'followup_tracking': [
        r'Intro sent\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?',
        r'Follow-up \d+ sent\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?',
        r'Follow-up \d+\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?',
        r'Skipped\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?',
    ],
}


def parse_notes(notes: str) -> dict:
    """
    Parse notes and extract structured data.

    Returns:
        dict with keys: responded, twitter_status, remaining_notes
    """
    if not notes:
        return {'responded': '', 'twitter_status': '', 'remaining_notes': ''}

    result = {
        'responded': '',
        'twitter_status': '',
        'remaining_notes': notes
    }

    # Check for responded
    for pattern in PATTERNS['responded']:
        match = re.search(pattern, notes, re.IGNORECASE)
        if match:
            date = match.group(1) if match.lastindex else ''
            result['responded'] = date if date else 'yes'
            # Remove from notes
            result['remaining_notes'] = re.sub(pattern, '', result['remaining_notes'], flags=re.IGNORECASE)
            break

    # Check for DM sent (twitter_status = messaged)
    if not result['twitter_status']:
        for pattern in PATTERNS['dm_sent']:
            match = re.search(pattern, notes, re.IGNORECASE)
            if match:
                result['twitter_status'] = 'messaged'
                result['remaining_notes'] = re.sub(pattern, '', result['remaining_notes'], flags=re.IGNORECASE)
                break

    # Check for followed only
    if not result['twitter_status']:
        for pattern in PATTERNS['followed']:
            if re.search(pattern, notes, re.IGNORECASE):
                result['twitter_status'] = 'followed'
                result['remaining_notes'] = re.sub(pattern, '', result['remaining_notes'], flags=re.IGNORECASE)
                break

    # Check for wrong twitter
    if not result['twitter_status']:
        for pattern in PATTERNS['wrong_twitter']:
            if re.search(pattern, notes, re.IGNORECASE):
                result['twitter_status'] = 'wrong'
                result['remaining_notes'] = re.sub(pattern, '', result['remaining_notes'], flags=re.IGNORECASE)
                break

    # Remove follow-up tracking (no longer needed)
    for pattern in PATTERNS['followup_tracking']:
        result['remaining_notes'] = re.sub(pattern, '', result['remaining_notes'], flags=re.IGNORECASE)

    # Clean up remaining notes
    # Remove extra semicolons and whitespace
    result['remaining_notes'] = re.sub(r'[;\s]+', ' ', result['remaining_notes']).strip()
    result['remaining_notes'] = re.sub(r'\s+', ' ', result['remaining_notes']).strip()

    # If only punctuation left, clear it
    if result['remaining_notes'] and not re.search(r'[a-zA-Z0-9]', result['remaining_notes']):
        result['remaining_notes'] = ''

    return result


def migrate_sheet(dry_run: bool = True):
    """
    Migrate notes data to new columns.

    Args:
        dry_run: If True, just print what would happen without making changes
    """
    print("=" * 60)
    print("NOTES MIGRATION SCRIPT")
    print("=" * 60)
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (making changes)'}")
    print()

    # Connect to sheet
    manager = SheetsManager()
    if not manager.connect():
        print(f"ERROR: Could not connect to sheet: {manager._connection_error}")
        return False

    print(f"Connected to sheet: {manager.config.spreadsheet_name}")

    # Get all data
    data = manager.get_all_data()
    if len(data) < 2:
        print("ERROR: No data in sheet")
        return False

    headers = data[0]
    rows = data[1:]

    print(f"Found {len(rows)} rows")
    print(f"Current headers: {headers}")
    print()

    # Check if new headers exist (strip whitespace for comparison)
    expected_new_headers = ['RC Stage', 'RC Next Contact', 'OL Stage', 'OL Next Contact',
                           'RC Responded', 'OL Responded', 'RC Twitter Status', 'OL Twitter Status',
                           'RC Email Status', 'OL Email Status']

    # Normalize headers (strip whitespace)
    normalized_headers = [h.strip().lower() if h else '' for h in headers]

    missing_headers = []
    for h in expected_new_headers:
        if h.lower() not in normalized_headers:
            missing_headers.append(h)

    if missing_headers:
        print(f"WARNING: Missing headers in sheet: {missing_headers}")
        print("Please add these headers to your Google Sheet first!")
        print()
        if dry_run:
            print("Continuing with dry run to show what would be migrated...")
        else:
            print("Aborting. Add the headers and run again.")
            return False

    # Find column indices (strip whitespace for matching)
    def find_col(name):
        for i, h in enumerate(headers):
            if h and name.lower() in h.strip().lower():
                return i
        return -1

    rc_notes_col = find_col('rc notes')
    ol_notes_col = find_col('ol notes')
    school_col = find_col('school')

    # New columns (1-indexed for gspread)
    rc_responded_col = NEW_COLUMNS['rc_responded'] + 1
    ol_responded_col = NEW_COLUMNS['ol_responded'] + 1
    rc_twitter_col = NEW_COLUMNS['rc_twitter_status'] + 1
    ol_twitter_col = NEW_COLUMNS['ol_twitter_status'] + 1
    rc_notes_col_write = rc_notes_col + 1
    ol_notes_col_write = ol_notes_col + 1

    print(f"RC Notes column: {rc_notes_col} (0-indexed)")
    print(f"OL Notes column: {ol_notes_col} (0-indexed)")
    print()

    # Process each row
    changes = []
    stats = {
        'rc_responded': 0,
        'ol_responded': 0,
        'rc_twitter_messaged': 0,
        'ol_twitter_messaged': 0,
        'rc_twitter_followed': 0,
        'ol_twitter_followed': 0,
        'rc_twitter_wrong': 0,
        'ol_twitter_wrong': 0,
        'notes_cleaned': 0,
    }

    for row_idx, row in enumerate(rows):
        row_num = row_idx + 2  # 1-indexed, skip header
        school = row[school_col] if school_col >= 0 and school_col < len(row) else f'Row {row_num}'

        rc_notes = row[rc_notes_col] if rc_notes_col >= 0 and rc_notes_col < len(row) else ''
        ol_notes = row[ol_notes_col] if ol_notes_col >= 0 and ol_notes_col < len(row) else ''

        row_changes = []

        # Parse RC notes
        if rc_notes:
            parsed = parse_notes(rc_notes)
            if parsed['responded']:
                row_changes.append(('RC Responded', rc_responded_col, parsed['responded']))
                stats['rc_responded'] += 1
            if parsed['twitter_status']:
                row_changes.append(('RC Twitter Status', rc_twitter_col, parsed['twitter_status']))
                stats[f'rc_twitter_{parsed["twitter_status"]}'] = stats.get(f'rc_twitter_{parsed["twitter_status"]}', 0) + 1
            if parsed['remaining_notes'] != rc_notes:
                row_changes.append(('RC Notes', rc_notes_col_write, parsed['remaining_notes']))
                stats['notes_cleaned'] += 1

        # Parse OL notes
        if ol_notes:
            parsed = parse_notes(ol_notes)
            if parsed['responded']:
                row_changes.append(('OL Responded', ol_responded_col, parsed['responded']))
                stats['ol_responded'] += 1
            if parsed['twitter_status']:
                row_changes.append(('OL Twitter Status', ol_twitter_col, parsed['twitter_status']))
                stats[f'ol_twitter_{parsed["twitter_status"]}'] = stats.get(f'ol_twitter_{parsed["twitter_status"]}', 0) + 1
            if parsed['remaining_notes'] != ol_notes:
                row_changes.append(('OL Notes', ol_notes_col_write, parsed['remaining_notes']))
                stats['notes_cleaned'] += 1

        if row_changes:
            changes.append((row_num, school, row_changes))

    # Print summary
    print("=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    print(f"RC Responded found: {stats['rc_responded']}")
    print(f"OL Responded found: {stats['ol_responded']}")
    print(f"RC Twitter 'messaged': {stats.get('rc_twitter_messaged', 0)}")
    print(f"OL Twitter 'messaged': {stats.get('ol_twitter_messaged', 0)}")
    print(f"RC Twitter 'followed': {stats.get('rc_twitter_followed', 0)}")
    print(f"OL Twitter 'followed': {stats.get('ol_twitter_followed', 0)}")
    print(f"RC Twitter 'wrong': {stats.get('rc_twitter_wrong', 0)}")
    print(f"OL Twitter 'wrong': {stats.get('ol_twitter_wrong', 0)}")
    print(f"Notes to clean: {stats['notes_cleaned']}")
    print(f"Total rows with changes: {len(changes)}")
    print()

    # Show sample changes
    print("SAMPLE CHANGES (first 10):")
    print("-" * 60)
    for row_num, school, row_changes in changes[:10]:
        print(f"Row {row_num}: {school[:30]}")
        for col_name, col_idx, value in row_changes:
            print(f"  {col_name} -> '{value[:50] if value else '(empty)'}'")
    print()

    if dry_run:
        print("DRY RUN COMPLETE - No changes made")
        print("Run with --live to apply changes")
        return True

    # Apply changes with rate limiting
    print("APPLYING CHANGES (with rate limiting)...")
    applied = 0
    errors = 0

    for row_num, school, row_changes in changes:
        for col_name, col_idx, value in row_changes:
            try:
                manager._sheet.update_cell(row_num, col_idx, value)
                applied += 1
                # Rate limit: ~50 writes per minute to stay under 60/min limit
                time.sleep(1.2)
            except Exception as e:
                print(f"  ERROR row {row_num} {col_name}: {e}")
                errors += 1
                # If rate limited, wait longer
                if '429' in str(e):
                    print("  Rate limited, waiting 60 seconds...")
                    time.sleep(60)

        # Progress
        if applied % 20 == 0:
            print(f"  Applied {applied} changes...")

    print()
    print(f"DONE! Applied {applied} changes, {errors} errors")
    return errors == 0


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Migrate notes to new tracking columns')
    parser.add_argument('--live', action='store_true', help='Actually make changes (default is dry run)')
    args = parser.parse_args()

    success = migrate_sheet(dry_run=not args.live)
    sys.exit(0 if success else 1)
