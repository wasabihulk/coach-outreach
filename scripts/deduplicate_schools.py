#!/usr/bin/env python3
"""
Deduplicate Schools Script
============================================================================
Removes duplicate school rows from the Google Sheet, keeping the row with
the most complete data.

Usage:
    python scripts/deduplicate_schools.py

Author: Coach Outreach System
============================================================================
"""

import os
import sys
import json
import tempfile
from datetime import datetime

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("Error: gspread or google-auth not installed.")
    print("Run: pip install gspread google-auth")
    sys.exit(1)


def get_sheet():
    """Connect to Google Sheet."""
    # Try environment variable first
    google_creds = os.environ.get('GOOGLE_CREDENTIALS', '')

    if google_creds:
        creds_str = google_creds.strip()
        if creds_str.startswith('"') and creds_str.endswith('"'):
            creds_str = creds_str[1:-1]
        creds_str = creds_str.replace('\\\\n', '\\n')

        temp_creds = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        temp_creds.write(creds_str)
        temp_creds.close()
        credentials_file = temp_creds.name
    else:
        # Try local credentials file
        credentials_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'credentials.json')
        if not os.path.exists(credentials_file):
            print(f"Error: No credentials found. Set GOOGLE_CREDENTIALS env var or add credentials.json")
            sys.exit(1)

    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
    client = gspread.authorize(creds)

    return client.open('bardeen').sheet1


def count_non_empty(row):
    """Count non-empty cells in a row."""
    return sum(1 for cell in row if cell and str(cell).strip())


def has_contacted_data(row, headers):
    """Check if row has been contacted (more valuable to keep)."""
    try:
        rc_contacted_idx = None
        ol_contacted_idx = None
        for i, h in enumerate(headers):
            h_lower = h.lower()
            if 'rc contacted' in h_lower:
                rc_contacted_idx = i
            elif 'ol contacted' in h_lower:
                ol_contacted_idx = i

        has_rc = rc_contacted_idx is not None and rc_contacted_idx < len(row) and row[rc_contacted_idx].strip()
        has_ol = ol_contacted_idx is not None and ol_contacted_idx < len(row) and row[ol_contacted_idx].strip()
        return has_rc or has_ol
    except:
        return False


def main():
    print("=" * 60)
    print("SCHOOL DEDUPLICATION SCRIPT")
    print("=" * 60)
    print()

    print("Connecting to Google Sheet...")
    sheet = get_sheet()

    print("Fetching all data...")
    all_data = sheet.get_all_values()

    if len(all_data) < 2:
        print("Sheet has no data rows.")
        return

    headers = all_data[0]
    rows = all_data[1:]

    print(f"Found {len(rows)} rows")

    # Find school column
    school_col = None
    for i, h in enumerate(headers):
        if 'school' in h.lower():
            school_col = i
            break

    if school_col is None:
        print("Error: No 'School' column found")
        return

    # Group rows by school name (normalized)
    schools = {}
    for row_idx, row in enumerate(rows):
        school_name = row[school_col].strip().lower() if school_col < len(row) else ''
        if not school_name:
            continue

        if school_name not in schools:
            schools[school_name] = []
        schools[school_name].append({
            'row_idx': row_idx + 2,  # 1-indexed, +1 for header
            'data': row,
            'non_empty_count': count_non_empty(row),
            'has_contacted': has_contacted_data(row, headers)
        })

    # Find duplicates
    duplicates = {name: rows for name, rows in schools.items() if len(rows) > 1}

    if not duplicates:
        print("\nNo duplicate schools found!")
        return

    print(f"\nFound {len(duplicates)} schools with duplicates:")
    print("-" * 60)

    rows_to_delete = []

    for school_name, dupe_rows in duplicates.items():
        print(f"\n{school_name.title()}:")

        # Sort by: has_contacted (True first), then non_empty_count (higher first)
        dupe_rows.sort(key=lambda x: (-int(x['has_contacted']), -x['non_empty_count']))

        keep_row = dupe_rows[0]
        delete_rows = dupe_rows[1:]

        print(f"  KEEP: Row {keep_row['row_idx']} ({keep_row['non_empty_count']} fields, contacted: {keep_row['has_contacted']})")

        for del_row in delete_rows:
            print(f"  DELETE: Row {del_row['row_idx']} ({del_row['non_empty_count']} fields, contacted: {del_row['has_contacted']})")
            rows_to_delete.append(del_row['row_idx'])

    print()
    print("-" * 60)
    print(f"\nTotal rows to delete: {len(rows_to_delete)}")

    # Confirm deletion
    response = input("\nProceed with deletion? (yes/no): ").strip().lower()

    if response != 'yes':
        print("Aborted.")
        return

    # Delete rows in reverse order (to avoid index shifting)
    rows_to_delete.sort(reverse=True)

    print("\nDeleting rows...")
    for row_idx in rows_to_delete:
        try:
            sheet.delete_rows(row_idx)
            print(f"  Deleted row {row_idx}")
        except Exception as e:
            print(f"  Error deleting row {row_idx}: {e}")

    print("\nDone!")
    print(f"Removed {len(rows_to_delete)} duplicate rows.")


if __name__ == '__main__':
    main()
