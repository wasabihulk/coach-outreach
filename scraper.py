"""
scraper.py - Main Scraper Orchestrator
============================================================================
Enterprise-grade scraper that coordinates all components.

This is the main entry point for scraping operations. It orchestrates:
- Browser management
- Page loading and parsing
- Data extraction
- Google Sheets updates
- Review queue management
- Progress tracking
- Error handling

Usage:
    python scraper.py                    # Run full scraping
    python scraper.py --test-url <url>   # Test single URL
    python scraper.py --stats            # Show progress stats
    python scraper.py --resume           # Resume from last position

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

import os
import sys
import time
import json
import random
import argparse
import logging
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from core.types import (
    ExtractionResult,
    StaffMember,
    SchoolRecord,
    ConfidenceLevel,
    ProcessingStatus,
    CanonicalRole,
)
from core.classifier import classify_role
from extraction.dom_parser import DOMParser
from browser.manager import BrowserManager, BrowserConfig, smart_delay, long_break
from sheets.manager import SheetsManager, SheetsConfig


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class ScraperConfig:
    """Configuration for the scraper."""
    # Confidence thresholds
    auto_save_threshold: int = 60      # Auto-save if confidence >= this
    review_threshold: int = 30         # Add to review if confidence >= this
    
    # Batch processing
    batch_size: int = 10               # Schools per batch
    break_between_batches: bool = True
    min_batch_break: float = 15.0
    max_batch_break: float = 30.0
    
    # Progress saving
    save_progress: bool = True
    progress_file: str = 'scraper_progress.json'
    
    # Limits
    max_schools: int = 0               # 0 = no limit
    max_errors: int = 10               # Stop after this many consecutive errors
    
    # Direction
    reverse: bool = False              # Start from bottom of sheet
    
    # Logging
    log_file: str = 'scraper.log'
    log_level: str = 'INFO'


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(config: ScraperConfig) -> logging.Logger:
    """Configure logging for the scraper."""
    logger = logging.getLogger('scraper')
    logger.setLevel(getattr(logging, config.log_level))
    
    # Clear existing handlers
    logger.handlers = []
    
    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console_fmt = logging.Formatter('%(message)s')
    console.setFormatter(console_fmt)
    logger.addHandler(console)
    
    # File handler
    if config.log_file:
        file_handler = logging.FileHandler(config.log_file)
        file_handler.setLevel(logging.DEBUG)
        file_fmt = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_fmt)
        logger.addHandler(file_handler)
    
    return logger


# ============================================================================
# PROGRESS TRACKING
# ============================================================================

@dataclass
class ScraperProgress:
    """Tracks scraping progress for resume capability."""
    last_processed_row: int = 0
    last_processed_school: str = ""
    schools_processed: int = 0
    ol_found: int = 0
    rc_found: int = 0
    sent_to_review: int = 0
    errors: int = 0
    started_at: str = ""
    last_updated: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'last_processed_row': self.last_processed_row,
            'last_processed_school': self.last_processed_school,
            'schools_processed': self.schools_processed,
            'ol_found': self.ol_found,
            'rc_found': self.rc_found,
            'sent_to_review': self.sent_to_review,
            'errors': self.errors,
            'started_at': self.started_at,
            'last_updated': self.last_updated,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScraperProgress':
        return cls(**data)
    
    def save(self, filepath: str) -> None:
        """Save progress to file."""
        self.last_updated = datetime.now().isoformat()
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
    
    @classmethod
    def load(cls, filepath: str) -> Optional['ScraperProgress']:
        """Load progress from file."""
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            return cls.from_dict(data)
        except Exception:
            return None


# ============================================================================
# MAIN SCRAPER CLASS
# ============================================================================

class CoachScraper:
    """
    Main scraper orchestrator.
    
    Coordinates all scraping operations including:
    - Browser management
    - Page loading
    - DOM parsing
    - Data extraction
    - Sheet updates
    - Progress tracking
    
    Usage:
        scraper = CoachScraper()
        scraper.run()
    """
    
    def __init__(
        self,
        config: Optional[ScraperConfig] = None,
        browser_config: Optional[BrowserConfig] = None,
        sheets_config: Optional[SheetsConfig] = None,
    ):
        """
        Initialize the scraper.
        
        Args:
            config: Scraper configuration
            browser_config: Browser configuration
            sheets_config: Google Sheets configuration
        """
        self.config = config or ScraperConfig()
        self.browser_config = browser_config or BrowserConfig()
        self.sheets_config = sheets_config or SheetsConfig()
        
        # Initialize components
        self.logger = setup_logging(self.config)
        self.browser = BrowserManager(self.browser_config)
        self.sheets = SheetsManager(self.sheets_config)
        self.parser = DOMParser()
        
        # Progress tracking
        self.progress = ScraperProgress()
        
        # State
        self._running = False
        self._consecutive_errors = 0
    
    # =========================================================================
    # PUBLIC API
    # =========================================================================
    
    def run(
        self, 
        resume: bool = False,
        callback: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ) -> ScraperProgress:
        """
        Run the scraper.
        
        Args:
            resume: Whether to resume from last position
            callback: Optional callback function(event, data) for progress updates
            
        Returns:
            Final progress statistics
        """
        self._running = True
        self.progress = ScraperProgress()
        self.progress.started_at = datetime.now().isoformat()
        
        self._emit(callback, 'started', {'time': self.progress.started_at})
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("🏈 COACH OUTREACH SCRAPER")
        self.logger.info("   Enterprise-Grade Staff Extraction")
        self.logger.info("=" * 70 + "\n")
        
        try:
            # Load previous progress if resuming
            if resume and self.config.save_progress:
                prev_progress = ScraperProgress.load(self.config.progress_file)
                if prev_progress:
                    self.progress = prev_progress
                    self.logger.info(f"📍 Resuming from row {self.progress.last_processed_row}")
            
            # Connect to Google Sheets
            self.logger.info("📊 Connecting to Google Sheets...")
            if not self.sheets.connect():
                self._emit(callback, 'error', {'message': 'Failed to connect to Sheets'})
                return self.progress
            self.logger.info("✅ Connected\n")
            
            # Get schools to process
            self.logger.info("📋 Getting schools to process...")
            schools = self.sheets.get_schools_to_process(reverse=self.config.reverse)
            
            # Filter if resuming
            if resume and self.progress.last_processed_row > 0:
                schools = [s for s in schools if s.row_index > self.progress.last_processed_row]
            
            # Apply limit if configured
            if self.config.max_schools > 0:
                schools = schools[:self.config.max_schools]
            
            if not schools:
                self.logger.info("✅ No schools to process!")
                return self.progress
            
            self.logger.info(f"📋 Found {len(schools)} schools to process\n")
            self._emit(callback, 'schools_found', {'count': len(schools)})
            
            # Start browser
            self.logger.info("🌐 Starting browser...")
            if not self.browser.start():
                self._emit(callback, 'error', {'message': 'Failed to start browser'})
                return self.progress
            self.logger.info("✅ Browser ready\n")
            
            # Process schools
            self._process_schools(schools, callback)
            
        except KeyboardInterrupt:
            self.logger.info("\n\n⚠️ Stopped by user")
            self._emit(callback, 'stopped', {'reason': 'user_interrupt'})
            
        except Exception as e:
            self.logger.error(f"\n❌ Fatal error: {e}")
            self._emit(callback, 'error', {'message': str(e)})
            
        finally:
            self._cleanup()
            self._print_summary()
            
            if self.config.save_progress:
                self.progress.save(self.config.progress_file)
            
            self._emit(callback, 'completed', self.progress.to_dict())
        
        return self.progress
    
    def stop(self) -> None:
        """Stop the scraper gracefully."""
        self._running = False
    
    def test_url(self, url: str) -> ExtractionResult:
        """
        Test scraping a single URL.
        
        Args:
            url: URL to test
            
        Returns:
            Extraction result
        """
        self.logger.info(f"\n🧪 Testing URL: {url}\n")
        
        try:
            # Start browser
            if not self.browser.start():
                self.logger.error("Failed to start browser")
                return ExtractionResult(url=url, errors=["Failed to start browser"])
            
            # Load page
            self.logger.info("📄 Loading page...")
            html = self.browser.get_page(url)
            
            if not html:
                self.logger.error("Failed to load page")
                return ExtractionResult(url=url, errors=["Failed to load page"])
            
            self.logger.info(f"   Loaded {len(html)} bytes")
            
            # Parse
            self.logger.info("🔍 Parsing content...")
            result = self.parser.parse(html, url, "Test School")
            
            # Print results
            self._print_test_results(result)
            
            return result
            
        finally:
            self.browser.stop()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current progress statistics."""
        if not self.sheets.connect():
            return {}
        
        stats = self.sheets.get_progress_stats()
        self.sheets.disconnect()
        return stats
    
    # =========================================================================
    # INTERNAL METHODS
    # =========================================================================
    
    def _process_schools(
        self,
        schools: List[SchoolRecord],
        callback: Optional[Callable]
    ) -> None:
        """Process a list of schools."""
        total = len(schools)
        
        for i, school in enumerate(schools):
            if not self._running:
                break
            
            # Check consecutive errors
            if self._consecutive_errors >= self.config.max_errors:
                self.logger.error(f"\n❌ Too many consecutive errors ({self._consecutive_errors}), stopping")
                break
            
            # Process school
            self._process_school(school, i + 1, total, callback)
            
            # Take breaks between batches
            if (i + 1) % self.config.batch_size == 0:
                if self.config.break_between_batches and self._running:
                    long_break(self.config.min_batch_break, self.config.max_batch_break)
            
            # Save progress periodically
            if self.config.save_progress and (i + 1) % 5 == 0:
                self.progress.save(self.config.progress_file)
    
    def _process_school(
        self,
        school: SchoolRecord,
        index: int,
        total: int,
        callback: Optional[Callable]
    ) -> None:
        """Process a single school."""
        self.logger.info(f"\n[{index}/{total}] 🏫 {school.school_name}")
        self._emit(callback, 'processing', {
            'index': index,
            'total': total,
            'school': school.school_name,
        })
        
        try:
            # Load page
            html = self.browser.get_page(school.staff_url)
            
            if not html:
                self.logger.warning(f"   ⚠️ Failed to load page")
                self._consecutive_errors += 1
                self.progress.errors += 1
                return
            
            self._consecutive_errors = 0  # Reset on success
            
            # Parse
            result = self.parser.parse(html, school.staff_url, school.school_name)
            
            # Log what we found
            self.logger.info(f"   📋 Found {len(result.staff)} staff members")
            if result.strategies_used:
                strategies = [str(s) for s, _ in result.strategies_used]
                self.logger.info(f"   📊 Strategies: {', '.join(strategies)}")
            
            # Handle results
            self._handle_result(school, result, callback)
            
            # Update progress
            self.progress.last_processed_row = school.row_index
            self.progress.last_processed_school = school.school_name
            self.progress.schools_processed += 1
            
        except Exception as e:
            self.logger.error(f"   ❌ Error: {str(e)[:60]}")
            self._consecutive_errors += 1
            self.progress.errors += 1
    
    def _handle_result(
        self,
        school: SchoolRecord,
        result: ExtractionResult,
        callback: Optional[Callable]
    ) -> None:
        """
        Handle extraction result - save to main sheet.
        
        LOGIC:
        - High confidence (>=60%) → Save name directly
        - Low confidence (30-59%) → Save as "REVIEW: Name (XX%)" placeholder
        - Not found → Leave blank
        
        NO separate review sheet - everything stays in Sheet 1.
        """
        ol_saved = False
        rc_saved = False
        
        # Handle OL Coach
        if school.needs_ol:
            if result.ol_coach and result.ol_confidence >= self.config.auto_save_threshold:
                # High confidence - save directly
                self.sheets.update_ol(
                    school.row_index,
                    result.ol_coach.name,
                    result.ol_coach.contact.email if result.ol_coach.contact else None,
                )
                self.logger.info(f"   ✅ OL Coach: {result.ol_coach.name} ({result.ol_confidence}%)")
                self.progress.ol_found += 1
                ol_saved = True
                
            elif result.ol_coach and result.ol_confidence >= self.config.review_threshold:
                # Low confidence - save with REVIEW prefix as placeholder
                placeholder = f"REVIEW: {result.ol_coach.name} ({result.ol_confidence}%)"
                self.sheets.update_ol(
                    school.row_index,
                    placeholder,
                    None,  # Don't save email for unconfirmed
                )
                self.logger.info(f"   ⚠️ OL Coach: {placeholder}")
                self.progress.sent_to_review += 1
                
            else:
                # Not found - leave blank
                self.logger.info("   ⏭️ OL Coach: Not found")
        
        # Handle RC
        if school.needs_rc:
            if result.rc and result.rc_confidence >= self.config.auto_save_threshold:
                # High confidence - save directly
                self.sheets.update_rc(
                    school.row_index,
                    result.rc.name,
                    result.rc.contact.email if result.rc.contact else None,
                )
                self.logger.info(f"   ✅ RC: {result.rc.name} ({result.rc_confidence}%)")
                self.progress.rc_found += 1
                rc_saved = True
                
            elif result.rc and result.rc_confidence >= self.config.review_threshold:
                # Low confidence - save with REVIEW prefix as placeholder
                placeholder = f"REVIEW: {result.rc.name} ({result.rc_confidence}%)"
                self.sheets.update_rc(
                    school.row_index,
                    placeholder,
                    None,  # Don't save email for unconfirmed
                )
                self.logger.info(f"   ⚠️ RC: {placeholder}")
                self.progress.sent_to_review += 1
                
            else:
                # Not found - leave blank
                self.logger.info("   ⏭️ RC: Not found")
        
        # Emit event
        self._emit(callback, 'school_processed', {
            'school': school.school_name,
            'ol_found': ol_saved,
            'rc_found': rc_saved,
        })
    
    def _cleanup(self) -> None:
        """Clean up resources."""
        self.browser.stop()
        self.sheets.disconnect()
        self._running = False
    
    def _emit(
        self,
        callback: Optional[Callable],
        event: str,
        data: Dict[str, Any]
    ) -> None:
        """Emit an event to callback if provided."""
        if callback:
            try:
                callback(event, data)
            except Exception as e:
                self.logger.debug(f"Callback error: {e}")
    
    def _print_summary(self) -> None:
        """Print final summary."""
        self.logger.info("\n" + "=" * 50)
        self.logger.info("📊 SCRAPING SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"   Schools Processed: {self.progress.schools_processed}")
        self.logger.info(f"   OL Coaches Found:  {self.progress.ol_found}")
        self.logger.info(f"   RCs Found:         {self.progress.rc_found}")
        self.logger.info(f"   Sent to Review:    {self.progress.sent_to_review}")
        self.logger.info(f"   Errors:            {self.progress.errors}")
        self.logger.info("=" * 50 + "\n")
    
    def _print_test_results(self, result: ExtractionResult) -> None:
        """Print test URL results."""
        self.logger.info("\n" + "=" * 50)
        self.logger.info("📊 EXTRACTION RESULTS")
        self.logger.info("=" * 50)
        
        self.logger.info(f"\n📋 Staff Found: {len(result.staff)}")
        for member in result.staff[:15]:
            self.logger.info(f"\n   Name: {member.name}")
            self.logger.info(f"   Title: {member.raw_title}")
            if member.contact.email:
                self.logger.info(f"   Email: {member.contact.email}")
            if member.roles:
                roles = [f"{r.role.value} ({r.confidence}%)" for r in member.roles]
                self.logger.info(f"   Roles: {', '.join(roles)}")
        
        self.logger.info("\n" + "-" * 50)
        self.logger.info("🎯 TARGET COACHES")
        self.logger.info("-" * 50)
        
        if result.ol_coach:
            self.logger.info(f"\n   OL Coach: {result.ol_coach.name}")
            self.logger.info(f"   Title: {result.ol_coach.raw_title}")
            self.logger.info(f"   Confidence: {result.ol_confidence}%")
            if result.ol_coach.contact.email:
                self.logger.info(f"   Email: {result.ol_coach.contact.email}")
        else:
            self.logger.info("\n   OL Coach: NOT FOUND")
        
        if result.rc:
            self.logger.info(f"\n   RC: {result.rc.name}")
            self.logger.info(f"   Title: {result.rc.raw_title}")
            self.logger.info(f"   Confidence: {result.rc_confidence}%")
            if result.rc.contact.email:
                self.logger.info(f"   Email: {result.rc.contact.email}")
        else:
            self.logger.info("\n   RC: NOT FOUND")
        
        if result.needs_review:
            self.logger.info("\n" + "-" * 50)
            self.logger.info("⚠️ NEEDS REVIEW")
            for reason in result.review_reasons:
                self.logger.info(f"   - {reason}")
        
        self.logger.info("\n" + "=" * 50)


# ============================================================================
# CLI
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Coach Outreach Scraper - Enterprise-Grade Staff Extraction'
    )
    
    parser.add_argument(
        '--test-url',
        type=str,
        help='Test scraping a single URL'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show progress statistics'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from last position'
    )
    
    parser.add_argument(
        '--max-schools',
        type=int,
        default=0,
        help='Maximum schools to process (0 = no limit)'
    )
    
    parser.add_argument(
        '--headless',
        action='store_true',
        help='Run browser in headless mode'
    )
    
    parser.add_argument(
        '--credentials',
        type=str,
        default='credentials.json',
        help='Path to Google credentials file'
    )
    
    args = parser.parse_args()
    
    # Configure based on arguments
    config = ScraperConfig(max_schools=args.max_schools)
    browser_config = BrowserConfig(headless=args.headless)
    sheets_config = SheetsConfig(credentials_file=args.credentials)
    
    scraper = CoachScraper(config, browser_config, sheets_config)
    
    if args.test_url:
        # Test single URL
        scraper.test_url(args.test_url)
        
    elif args.stats:
        # Show statistics
        print("\n📊 Progress Statistics\n")
        stats = scraper.get_stats()
        if stats:
            for key, value in stats.items():
                print(f"   {key}: {value}")
        else:
            print("   Failed to get statistics")
        print()
        
    else:
        # Run full scraping
        scraper.run(resume=args.resume)


if __name__ == "__main__":
    main()
