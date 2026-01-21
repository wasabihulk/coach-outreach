"""
browser/manager.py - Browser Management System
============================================================================
Enterprise-grade browser automation with Selenium.

This module provides:
- Stealth browser configuration
- Anti-detection measures
- Page loading and scrolling
- Hidden content expansion
- Error handling and recovery
- Resource management

Design Principles:
- Undetectable automation
- Robust error recovery
- Efficient resource usage
- Comprehensive logging

Author: Coach Outreach System
Version: 2.0.0
============================================================================
"""

import os
import time
import random
import logging
from typing import Optional, List, Callable, Any
from dataclasses import dataclass
from contextlib import contextmanager
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
)

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

try:
    from fake_useragent import UserAgent
    HAS_FAKE_UA = True
except ImportError:
    HAS_FAKE_UA = False

# Configure logging
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class BrowserConfig:
    """Configuration for browser behavior."""
    # Timeouts
    page_load_timeout: int = 30
    implicit_wait: int = 5
    script_timeout: int = 30
    
    # Display
    headless: bool = False
    window_width: int = 1920
    window_height: int = 1080
    
    # Anti-detection
    use_stealth: bool = True
    randomize_user_agent: bool = True
    disable_webdriver_flag: bool = True
    
    # Performance
    disable_images: bool = False
    disable_javascript: bool = False
    block_ads: bool = True
    
    # Delays
    min_page_delay: float = 2.0
    max_page_delay: float = 5.0
    min_scroll_delay: float = 0.2
    max_scroll_delay: float = 0.5
    
    # Retries
    max_retries: int = 3
    retry_delay: float = 5.0


# Default user agents (fallback if fake_useragent not available)
DEFAULT_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]


# ============================================================================
# BROWSER MANAGER
# ============================================================================

class BrowserManager:
    """
    Manages browser lifecycle and operations.
    
    This class handles:
    - Browser creation with stealth settings
    - Page loading with retries
    - Content scrolling and expansion
    - Resource cleanup
    
    Usage:
        manager = BrowserManager()
        
        with manager.session() as driver:
            html = manager.get_page("https://example.com")
            # Process html...
        
        # Or manual management:
        manager.start()
        try:
            html = manager.get_page("https://example.com")
        finally:
            manager.stop()
    """
    
    def __init__(self, config: Optional[BrowserConfig] = None):
        """
        Initialize browser manager.
        
        Args:
            config: Browser configuration (uses defaults if None)
        """
        self.config = config or BrowserConfig()
        self.driver: Optional[webdriver.Chrome] = None
        self._is_running = False
        self._pages_loaded = 0
        self._errors_count = 0
        
        # Initialize user agent generator
        if HAS_FAKE_UA and self.config.randomize_user_agent:
            try:
                self._ua = UserAgent()
            except Exception:
                self._ua = None
        else:
            self._ua = None
    
    @property
    def is_running(self) -> bool:
        """Check if browser is currently running."""
        return self._is_running and self.driver is not None
    
    @property
    def stats(self) -> dict:
        """Get browser statistics."""
        return {
            'is_running': self.is_running,
            'pages_loaded': self._pages_loaded,
            'errors_count': self._errors_count,
        }
    
    def start(self) -> bool:
        """
        Start the browser.
        
        Returns:
            True if successful, False otherwise
        """
        if self.is_running:
            logger.warning("Browser already running")
            return True
        
        try:
            self.driver = self._create_driver()
            self._is_running = True
            logger.info("Browser started successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to start browser: {e}")
            self._errors_count += 1
            return False
    
    def stop(self) -> None:
        """Stop the browser and clean up resources."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
        
        self.driver = None
        self._is_running = False
        logger.info("Browser stopped")
    
    @contextmanager
    def session(self):
        """
        Context manager for browser session.
        
        Usage:
            with manager.session() as driver:
                driver.get("https://example.com")
        """
        try:
            if not self.start():
                raise RuntimeError("Failed to start browser")
            yield self.driver
        finally:
            self.stop()
    
    def _create_driver(self) -> webdriver.Chrome:
        """Create and configure Chrome driver with stealth settings."""
        options = self._build_options()
        
        # Get Chrome driver
        if HAS_WEBDRIVER_MANAGER:
            logger.info("Installing/updating ChromeDriver...")
            service = Service(ChromeDriverManager().install())
        else:
            # Fall back to system Chrome driver
            service = Service()
        
        logger.info("Launching Chrome...")
        driver = webdriver.Chrome(service=service, options=options)
        
        # Apply stealth settings
        if self.config.use_stealth:
            self._apply_stealth(driver)
        
        # Configure timeouts
        driver.set_page_load_timeout(self.config.page_load_timeout)
        driver.implicitly_wait(self.config.implicit_wait)
        driver.set_script_timeout(self.config.script_timeout)
        
        return driver
    
    def _build_options(self) -> Options:
        """Build Chrome options with all configurations."""
        options = Options()
        
        # Core stability options
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        
        # Window size
        options.add_argument(f'--window-size={self.config.window_width},{self.config.window_height}')
        
        # Headless mode
        if self.config.headless:
            options.add_argument('--headless=new')
        
        # User agent
        user_agent = self._get_user_agent()
        options.add_argument(f'--user-agent={user_agent}')
        
        # Anti-detection
        if self.config.use_stealth:
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            options.add_experimental_option('useAutomationExtension', False)
        
        # Performance options
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--allow-running-insecure-content')
        
        # Disable images if configured
        if self.config.disable_images:
            prefs = {'profile.managed_default_content_settings.images': 2}
            options.add_experimental_option('prefs', prefs)
        
        # Page load strategy
        options.page_load_strategy = 'eager'  # Don't wait for all resources
        
        return options
    
    def _get_user_agent(self) -> str:
        """Get a user agent string."""
        if self._ua:
            try:
                return self._ua.random
            except Exception:
                pass
        
        return random.choice(DEFAULT_USER_AGENTS)
    
    def _apply_stealth(self, driver: webdriver.Chrome) -> None:
        """Apply stealth JavaScript to avoid detection."""
        stealth_scripts = [
            # Remove webdriver flag
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})",
            
            # Fake plugins
            "Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})",
            
            # Fake languages
            "Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})",
            
            # Fake platform
            "Object.defineProperty(navigator, 'platform', {get: () => 'Win32'})",
            
            # Remove automation indicators
            """
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            """,
            
            # Chrome runtime
            """
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            """,
        ]
        
        for script in stealth_scripts:
            try:
                driver.execute_script(script)
            except Exception:
                pass  # Some scripts may fail, that's okay
    
    def get_page(
        self, 
        url: str, 
        scroll: bool = True,
        expand_content: bool = True,
        retry: bool = True
    ) -> Optional[str]:
        """
        Load a page and return its HTML content.
        
        Args:
            url: URL to load
            scroll: Whether to scroll the page
            expand_content: Whether to try expanding hidden content
            retry: Whether to retry on failure
            
        Returns:
            HTML content, or None if failed
        """
        if not self.is_running:
            logger.error("Browser not running")
            return None
        
        attempts = self.config.max_retries if retry else 1
        
        for attempt in range(attempts):
            try:
                logger.debug(f"Loading page (attempt {attempt + 1}): {url}")
                
                # Load the page
                self.driver.get(url)
                
                # Random delay to appear human
                delay = random.uniform(self.config.min_page_delay, self.config.max_page_delay)
                time.sleep(delay)
                
                # Scroll to load lazy content
                if scroll:
                    self._scroll_page()
                
                # Try to expand hidden content
                if expand_content:
                    self._expand_content()
                
                # Get HTML
                html = self.driver.page_source
                
                self._pages_loaded += 1
                return html
                
            except TimeoutException:
                logger.warning(f"Page load timeout: {url}")
                if attempt < attempts - 1:
                    time.sleep(self.config.retry_delay)
                    
            except WebDriverException as e:
                logger.error(f"WebDriver error loading {url}: {e}")
                self._errors_count += 1
                if attempt < attempts - 1:
                    time.sleep(self.config.retry_delay)
                    
            except Exception as e:
                logger.error(f"Unexpected error loading {url}: {e}")
                self._errors_count += 1
                break
        
        return None
    
    def _scroll_page(self) -> None:
        """Scroll the page to trigger lazy loading."""
        if not self.driver:
            return
        
        try:
            # Get page height
            page_height = self.driver.execute_script("return document.body.scrollHeight")
            
            # Scroll in increments
            scroll_positions = [
                300, 600, 900, 1200, 1500, 2000, 2500, 3000,
                page_height // 2, page_height
            ]
            
            for pos in scroll_positions:
                if pos > page_height:
                    break
                    
                self.driver.execute_script(f"window.scrollTo(0, {pos})")
                delay = random.uniform(
                    self.config.min_scroll_delay, 
                    self.config.max_scroll_delay
                )
                time.sleep(delay)
            
            # Scroll to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.5)
            
            # Scroll back to top (some sites load content differently)
            self.driver.execute_script("window.scrollTo(0, 0)")
            time.sleep(0.3)
            
        except Exception as e:
            logger.debug(f"Error during scroll: {e}")
    
    def _expand_content(self) -> None:
        """Try to click buttons that expand hidden content."""
        if not self.driver:
            return
        
        # XPath selectors for expand buttons
        expand_selectors = [
            # Load more buttons
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'load more')]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more')]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show all')]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view all')]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'expand')]",
            
            # Links that expand
            "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view all')]",
            "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'see all')]",
            "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show all')]",
            
            # Class-based selectors
            "//button[contains(@class, 'load-more')]",
            "//button[contains(@class, 'show-more')]",
            "//button[contains(@class, 'expand')]",
            "//*[contains(@class, 'accordion')]//button",
            "//*[contains(@class, 'collapsible')]//button",
        ]
        
        for selector in expand_selectors:
            try:
                elements = self.driver.find_elements(By.XPATH, selector)
                for elem in elements[:3]:  # Click at most 3 per selector
                    if elem.is_displayed() and elem.is_enabled():
                        try:
                            elem.click()
                            time.sleep(0.5)
                            logger.debug("Clicked expand button")
                        except (ElementClickInterceptedException, StaleElementReferenceException):
                            pass
            except Exception:
                pass
    
    def wait_for_element(
        self, 
        selector: str, 
        by: By = By.CSS_SELECTOR,
        timeout: int = 10
    ) -> bool:
        """
        Wait for an element to be present.
        
        Args:
            selector: Element selector
            by: Selector type (CSS, XPATH, etc.)
            timeout: Maximum wait time in seconds
            
        Returns:
            True if element found, False otherwise
        """
        if not self.driver:
            return False
        
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            return True
        except TimeoutException:
            return False
    
    def execute_script(self, script: str, *args) -> Any:
        """
        Execute JavaScript in the browser.
        
        Args:
            script: JavaScript code to execute
            *args: Arguments to pass to the script
            
        Returns:
            Result of script execution
        """
        if not self.driver:
            return None
        
        try:
            return self.driver.execute_script(script, *args)
        except Exception as e:
            logger.error(f"Script execution failed: {e}")
            return None
    
    def take_screenshot(self, filename: str) -> bool:
        """
        Take a screenshot of the current page.
        
        Args:
            filename: Path to save screenshot
            
        Returns:
            True if successful
        """
        if not self.driver:
            return False
        
        try:
            self.driver.save_screenshot(filename)
            return True
        except Exception as e:
            logger.error(f"Screenshot failed: {e}")
            return False


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def smart_delay(min_sec: float = 2.0, max_sec: float = 5.0) -> None:
    """Sleep for a random duration to appear human."""
    delay = random.uniform(min_sec, max_sec)
    time.sleep(delay)


def long_break(min_sec: float = 15.0, max_sec: float = 30.0) -> None:
    """Take a longer break between batches."""
    delay = random.uniform(min_sec, max_sec)
    logger.info(f"Taking a {int(delay)}s break...")
    time.sleep(delay)


# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    'BrowserManager',
    'BrowserConfig',
    'smart_delay',
    'long_break',
]


# ============================================================================
# SELF-TEST
# ============================================================================

if __name__ == "__main__":
    import sys
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 70)
    print("BROWSER MANAGER SELF-TEST")
    print("=" * 70)
    
    # Test configuration
    config = BrowserConfig(
        headless=True,
        page_load_timeout=15,
    )
    
    manager = BrowserManager(config)
    
    print("\n📊 Testing browser startup...")
    
    # Test with context manager
    with manager.session() as driver:
        print("✅ Browser started")
        
        # Test page load
        print("\n📄 Testing page load...")
        html = manager.get_page("https://httpbin.org/html")
        
        if html:
            print(f"✅ Page loaded ({len(html)} bytes)")
        else:
            print("❌ Page load failed")
        
        print(f"\n📊 Stats: {manager.stats}")
    
    print("✅ Browser stopped")
    
    print("\n" + "=" * 70)
    print("SELF-TEST COMPLETE")
    print("=" * 70)
