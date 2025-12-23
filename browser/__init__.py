"""
browser/__init__.py - Browser Module
============================================================================
Selenium browser management components.

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

from browser.manager import (
    BrowserManager,
    BrowserConfig,
    smart_delay,
    long_break,
)

__all__ = [
    'BrowserManager',
    'BrowserConfig',
    'smart_delay',
    'long_break',
]
