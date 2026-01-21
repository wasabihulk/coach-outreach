"""
outreach/__init__.py - Outreach Module
"""

from outreach.email_sender import (
    SmartEmailSender,
    EmailConfig,
    AthleteInfo,
    EmailTracker,
    AnalyticsTracker,
    get_email_tracker,
    get_analytics,
    DEFAULT_RC_TEMPLATE,
    DEFAULT_OL_TEMPLATE,
    DEFAULT_DUAL_ROLE_TEMPLATE,
)

try:
    from outreach.twitter_sender import (
        TwitterDMSender,
        TwitterConfig,
        TwitterDMTracker,
        get_twitter_sender,
    )
except ImportError:
    TwitterDMSender = None
    TwitterConfig = None
    TwitterDMTracker = None
    get_twitter_sender = None

__all__ = [
    'SmartEmailSender',
    'EmailConfig',
    'AthleteInfo',
    'EmailTracker',
    'AnalyticsTracker',
    'get_email_tracker',
    'get_analytics',
    'DEFAULT_RC_TEMPLATE',
    'DEFAULT_OL_TEMPLATE',
    'DEFAULT_DUAL_ROLE_TEMPLATE',
    'TwitterDMSender',
    'TwitterConfig',
    'TwitterDMTracker',
    'get_twitter_sender',
]

