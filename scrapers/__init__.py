"""
scrapers/__init__.py - Scraper Modules
============================================================================
Enterprise-grade scrapers for extracting coach information.

Modules:
- twitter_scraper: Extract Twitter/X handles from staff pages
- email_scraper: Extract email addresses from staff pages
- unified_scraper: Combined name + email extraction

Author: Coach Outreach System
Version: 3.3.0
============================================================================
"""

from scrapers.twitter_scraper import TwitterScraper, TwitterScraperConfig
from scrapers.email_scraper import EmailScraper, EmailScraperConfig

try:
    from scrapers.unified_scraper import UnifiedCoachExtractor, CoachRecord, extract_coaches
except ImportError:
    UnifiedCoachExtractor = None
    CoachRecord = None
    extract_coaches = None

__all__ = [
    'TwitterScraper', 'TwitterScraperConfig',
    'EmailScraper', 'EmailScraperConfig',
    'UnifiedCoachExtractor', 'CoachRecord', 'extract_coaches',
]

