"""
Shared browser session management for newspapers.com.

Provides the proven login/logout, Cloudflare handling, account rotation,
and browser lifecycle functions extracted from clip_and_extract.py.

Usage:
    from browser_session import BrowserSession

    session = BrowserSession(db_path=r"c:\\lake_worth\\lake_worth.db",
                             profile_dir=r"c:\\lake_worth_runtime\\chrome_profile_myapp")
    driver = session.setup_driver(preferred_account="user@example.com")
    # ... do work with driver ...
    # On throttle:
    session.switch_account(driver, exclude_email="user@example.com")
    # When done:
    session.shutdown(driver)
"""

import logging
import os
import re
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path

from seleniumbase import Driver
from selenium.webdriver.common.by import By

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _db_retry(func, *args, max_retries=5, base_delay=1.0, **kwargs):
    """Retry a database operation that may fail with 'database is locked'."""
    import random as _random
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt) + _random.uniform(0, 0.5)
                log.warning(
                    f"  DB locked (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                raise


def _db_commit(conn):
    """Commit with retry on 'database is locked'."""
    _db_retry(conn.commit)


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Cloudflare handling
# ---------------------------------------------------------------------------

def is_cloudflare(driver):
    """Check if the current page is a Cloudflare challenge."""
    try:
        title = (driver.title or "").lower()
        if "just a moment" in title:
            return True
        page_text = driver.execute_script(
            "return document.body.innerText || '';"
        ).lower()
        if ("security verification" in page_text
                or "checking if the site connection is secure" in page_text
                or "verify you are human" in page_text):
            return True
        # Check for Turnstile iframe
        for sel in (
            "iframe[src*='challenges.cloudflare.com']",
            "iframe[src*='turnstile']",
            "div.cf-turnstile",
        ):
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    if el.is_displayed():
                        return True
            except Exception:
                continue
    except Exception:
        pass
    return False


def _click_turnstile_checkbox(driver):
    """Find the Turnstile iframe and click its checkbox using PyAutoGUI.
    Bypasses SeleniumBase's captcha-type detection which may not recognise
    the Turnstile markers on newspapers.com."""
    import pyautogui
    selectors = [
        "iframe[src*='challenges.cloudflare.com']",
        "iframe[src*='turnstile']",
        "div.cf-turnstile iframe",
    ]
    iframe_el = None
    for sel in selectors:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                if el.is_displayed():
                    iframe_el = el
                    break
        except Exception:
            continue
        if iframe_el:
            break
    if not iframe_el:
        log.info("  No visible Turnstile iframe found for click.")
        return False

    # Get iframe position on screen
    rect = iframe_el.rect  # {x, y, width, height} in viewport coords
    win_rect = driver.get_window_rect()
    # viewport offset: window position + browser chrome (~toolbar height)
    # On Chrome the viewport top is roughly window y + (window height - viewport height)
    viewport_height = driver.execute_script("return window.innerHeight;")
    chrome_height = win_rect["height"] - viewport_height
    screen_x = win_rect["x"] + rect["x"] + 25  # checkbox is ~25px from left edge
    screen_y = win_rect["y"] + chrome_height + rect["y"] + (rect["height"] / 2)

    # Account for Windows DPI scaling
    scr_width = pyautogui.size().width
    try:
        driver.maximize_window()
        win_width = driver.get_window_size()["width"]
        width_ratio = round(float(scr_width) / float(win_width), 2) + 0.01
        if width_ratio < 0.45 or width_ratio > 2.55:
            width_ratio = 1.01
        driver.set_window_rect(
            win_rect["x"], win_rect["y"],
            win_rect["width"], win_rect["height"],
        )
    except Exception:
        width_ratio = 1.01

    click_x = int(screen_x * width_ratio)
    click_y = int(screen_y * width_ratio)
    log.info(
        f"  Turnstile click: iframe rect={rect}, "
        f"win=({win_rect['x']},{win_rect['y']}), "
        f"chrome_h={chrome_height}, ratio={width_ratio}, "
        f"clicking ({click_x},{click_y})"
    )

    # Bring window to front
    try:
        from selenium.webdriver.common.action_chains import ActionChains
        ActionChains(driver).move_to_element(iframe_el).perform()
    except Exception:
        pass
    time.sleep(0.3)
    pyautogui.moveTo(click_x, click_y, duration=0.3)
    time.sleep(0.2)
    pyautogui.click()
    return True


def solve_cloudflare(driver, max_attempts=20):
    """Detect and solve Cloudflare challenge. Returns True if solved or none."""
    if not is_cloudflare(driver):
        return True
    log.info("  Cloudflare challenge detected — solving...")
    for attempt in range(max_attempts):
        try:
            _click_turnstile_checkbox(driver)
            time.sleep(3)
            if not is_cloudflare(driver):
                log.info(f"  Cloudflare solved (attempt {attempt + 1})")
                return True
            log.info(
                f"  Cloudflare still present after click "
                f"{attempt + 1}/{max_attempts}"
            )
        except Exception as e:
            log.warning(f"  Cloudflare solve error: {e}")
        time.sleep(2)
    log.warning(f"  Could not solve Cloudflare after {max_attempts} attempts")
    return False


def navigate(driver, url):
    """Navigate to URL and handle Cloudflare if it appears."""
    driver.get(url)
    time.sleep(3)
    if is_cloudflare(driver):
        if not solve_cloudflare(driver):
            return False
    return True


# ---------------------------------------------------------------------------
# Tab management
# ---------------------------------------------------------------------------

def close_extra_tabs(driver):
    """Close every browser tab except the currently focused one.
    Re-maximizes the surviving tab afterward."""
    try:
        handles = driver.window_handles
        closed_any = len(handles) > 1
        if closed_any:
            current = driver.current_window_handle
            for h in handles:
                if h == current:
                    continue
                try:
                    driver.switch_to.window(h)
                    driver.close()
                except Exception:
                    pass
            try:
                driver.switch_to.window(current)
            except Exception:
                remaining = driver.window_handles
                if remaining:
                    driver.switch_to.window(remaining[0])
        try:
            driver.maximize_window()
        except Exception:
            pass
    except Exception as e:
        log.info(f"  close_extra_tabs skipped: {e}")


# ---------------------------------------------------------------------------
# Chrome profile helpers
# ---------------------------------------------------------------------------

def clean_chrome_profile_locks(profile_dir):
    """Remove Chrome singleton lock files."""
    for lock_name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        lock_path = os.path.join(str(profile_dir), lock_name)
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
                log.info(f"  Removed stale lock: {lock_path}")
        except Exception as e:
            log.warning(f"  Could not remove {lock_path}: {e}")


def patch_chrome_preferences(profile_dir):
    """Patch Chrome Preferences to prevent session restore / crash bubble."""
    import json as _json
    prefs_path = os.path.join(str(profile_dir), "Default", "Preferences")
    prefs = {}
    if os.path.isfile(prefs_path):
        try:
            with open(prefs_path, "r", encoding="utf-8") as f:
                prefs = _json.load(f)
        except Exception:
            prefs = {}
    prefs.setdefault("profile", {})
    prefs["profile"]["exit_type"] = "none"
    prefs["profile"]["exited_cleanly"] = True
    os.makedirs(os.path.dirname(prefs_path), exist_ok=True)
    try:
        with open(prefs_path, "w", encoding="utf-8") as f:
            _json.dump(prefs, f)
        log.info("  Patched Chrome Preferences: exit_type=none, exited_cleanly=true")
    except Exception as e:
        log.warning(f"  Could not patch Chrome Preferences: {e}")


def kill_chrome_for_profile(profile_dir):
    """Kill any Chrome processes using the given profile directory."""
    profile_str = str(profile_dir).replace("\\", "/").lower()
    profile_str_bs = str(profile_dir).lower()
    try:
        out = subprocess.run(
            ["wmic", "process", "where", "name='chrome.exe'", "get",
             "ProcessId,CommandLine", "/format:csv"],
            capture_output=True, text=True, timeout=10,
        )
        for line in (out.stdout or "").splitlines():
            lower = line.lower()
            if profile_str in lower or profile_str_bs in lower:
                parts = line.strip().split(",")
                try:
                    pid = int(parts[-1])
                    os.kill(pid, 9)
                    log.info(f"  Killed stale Chrome PID {pid}")
                except Exception:
                    pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Login detection
# ---------------------------------------------------------------------------

def detect_logged_in_account(driver):
    """Check the account page for the logged-in email address."""
    try:
        driver.execute_script(
            "window.location.href = 'https://www.newspapers.com/account/';"
        )
        time.sleep(3)
        if is_cloudflare(driver):
            solve_cloudflare(driver)
            time.sleep(2)
        page_text = driver.execute_script(
            "return document.body.innerText || '';"
        )
        emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', page_text)
        if emails:
            log.info(f"  Detected logged-in account: {emails[0]}")
            return emails[0]
        log.info("  Could not detect logged-in account from page text.")
    except Exception as e:
        log.warning(f"  Error detecting account: {e}")
    return None


# ---------------------------------------------------------------------------
# Login / Logout
# ---------------------------------------------------------------------------

def do_login(driver, email, password):
    """Login to newspapers.com. Returns True on success.

    Handles nag modals, Cloudflare full-page and Turnstile challenges,
    and verifies the login succeeded.
    """
    log.info(f"  Logging in as {email}...")

    # Navigate to sign-in page
    log.info("  Navigating to sign-in page...")
    try:
        driver.uc_open_with_reconnect(
            "https://www.newspapers.com/signin/", 4
        )
    except Exception:
        driver.execute_script(
            "window.location.href = 'https://www.newspapers.com/signin/';"
        )
    close_extra_tabs(driver)
    time.sleep(5)

    # Dismiss subscription / upsell nag modal if present
    try:
        nag_js = r"""
            const wanted = ['sign in','log in','sign-in','log-in'];
            const nodes = document.querySelectorAll(
                "a, button, [role='button'], span, div"
            );
            for (const el of nodes) {
                const txt = (el.innerText || el.textContent || '').trim().toLowerCase();
                if (!txt || txt.length > 24) continue;
                if (!wanted.some(w => txt === w || txt.startsWith(w))) continue;
                let p = el, inModal = false;
                for (let i = 0; i < 8 && p; i++, p = p.parentElement) {
                    const cls = (p.className || '') + ' ' + (p.id || '');
                    const role = p.getAttribute && p.getAttribute('role');
                    if (/modal|dialog|overlay|popup|paywall|upsell|subscribe|nag|interstitial/i.test(cls)
                        || role === 'dialog') {
                        inModal = true;
                        break;
                    }
                }
                if (!inModal) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 2 || r.height < 2) continue;
                el.click();
                return true;
            }
            return false;
        """
        for _ in range(3):
            if driver.find_elements(By.CSS_SELECTOR, "input[type='password']"):
                break
            try:
                nag_clicked = bool(driver.execute_script(nag_js))
            except Exception:
                nag_clicked = False
            if nag_clicked:
                log.info("  Dismissed subscription nag (clicked in-modal Sign in).")
                time.sleep(2)
                break
            time.sleep(1)
    except Exception as e:
        log.info(f"  Nag-dismiss scan skipped: {e}")

    # Handle full-page Cloudflare challenge (first gate)
    if is_cloudflare(driver):
        log.info("  Full-page Cloudflare on sign-in — solving...")
        if not solve_cloudflare(driver):
            log.warning("  Cloudflare on sign-in page could not be solved.")
            return False
        time.sleep(3)

    # Wait for login form to render
    has_form = False
    for _ in range(15):
        if len(driver.find_elements(
            By.CSS_SELECTOR,
            "input[type='password'], input[name='email']"
        )) > 0:
            has_form = True
            break
        if is_cloudflare(driver):
            solve_cloudflare(driver)
        time.sleep(1)

    if not has_form:
        page_text = driver.execute_script(
            "return document.body.innerText || '';"
        ).lower()
        if "sign in" not in page_text and "log in" not in page_text:
            detected = detect_logged_in_account(driver)
            if detected:
                log.info(f"  Already logged in as {detected} (session restored).")
                return True
            log.warning("  Not actually logged in — no account detected.")
            return False
        log.warning("  No login form found on sign-in page.")
        return False

    # Enter email
    try:
        email_field = None
        for sel in ["input[name='email']", "input[id='email']",
                     "input[type='email']", "input[type='text']"]:
            fields = driver.find_elements(By.CSS_SELECTOR, sel)
            for f in fields:
                if f.is_displayed():
                    email_field = f
                    break
            if email_field:
                break
        if not email_field:
            log.warning("  Could not find email field.")
            return False
        email_field.clear()
        email_field.send_keys(email)
    except Exception as e:
        log.warning(f"  Email entry failed: {e}")
        return False

    # Enter password
    try:
        pw_field = None
        for sel in ["input[type='password']", "input[name='password']"]:
            fields = driver.find_elements(By.CSS_SELECTOR, sel)
            for f in fields:
                if f.is_displayed():
                    pw_field = f
                    break
            if pw_field:
                break
        if not pw_field:
            log.warning("  Could not find password field.")
            return False
        pw_field.clear()
        pw_field.send_keys(password)
    except Exception as e:
        log.warning(f"  Password entry failed: {e}")
        return False

    # Solve Cloudflare Turnstile on the login form (screenshot-based)
    log.info("  Checking for Turnstile on login form...")
    time.sleep(3)  # Give Turnstile widget time to render

    # Step 1: Screenshot to see if the checkbox is there
    log_dir = Path(r"c:\lake_worth\collector_logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    shot1 = str(log_dir / f"turnstile_before_{ts}.png")
    driver.save_screenshot(shot1)
    log.info(f"  Turnstile check screenshot: {shot1}")

    page_text = driver.execute_script(
        "return document.body.innerText || '';"
    ).lower()
    if "verify you are human" not in page_text:
        log.info("  No 'Verify you are human' text — skipping Turnstile.")
    else:
        log.info("  'Verify you are human' found — locating checkbox...")
        import pyautogui

        # Step 2: Calculate click position from page layout
        # Find the widget container by looking for the element with that text
        # The checkbox square is at the left edge of the widget
        widget_info = driver.execute_script("""
            var els = document.querySelectorAll('*');
            for (var i = 0; i < els.length; i++) {
                var el = els[i];
                if (el.children.length > 10) continue;
                var t = (el.innerText || '').toLowerCase();
                if (t.indexOf('verify you are human') > -1
                    && t.indexOf('cloudflare') > -1) {
                    var r = el.getBoundingClientRect();
                    if (r.width > 50 && r.height > 20) {
                        return {x: r.x, y: r.y, w: r.width, h: r.height,
                                tag: el.tagName, cls: el.className};
                    }
                }
            }
            // Broader search without requiring "cloudflare"
            for (var i = 0; i < els.length; i++) {
                var el = els[i];
                if (el.children.length > 10) continue;
                var t = (el.innerText || '').toLowerCase();
                if (t.indexOf('verify you are human') > -1) {
                    var r = el.getBoundingClientRect();
                    if (r.width > 50 && r.height > 20) {
                        return {x: r.x, y: r.y, w: r.width, h: r.height,
                                tag: el.tagName, cls: el.className};
                    }
                }
            }
            return null;
        """)

        if not widget_info:
            log.warning("  Could not locate Turnstile widget element.")
        else:
            win_rect = driver.get_window_rect()
            viewport_h = driver.execute_script("return window.innerHeight;")
            chrome_h = win_rect["height"] - viewport_h

            # Checkbox is ~25px from left edge, vertically centred
            click_x = int(win_rect["x"] + widget_info["x"] + 25)
            click_y = int(win_rect["y"] + chrome_h
                          + widget_info["y"] + widget_info["h"] / 2)

            log.info(
                f"  Widget: tag={widget_info['tag']} "
                f"class={widget_info.get('cls','')} "
                f"rect=({widget_info['x']},{widget_info['y']}) "
                f"{widget_info['w']}x{widget_info['h']}  "
                f"win=({win_rect['x']},{win_rect['y']}) chrome_h={chrome_h}"
            )
            log.info(f"  Moving mouse to ({click_x}, {click_y})...")

            # Step 3: Move mouse to position, screenshot to verify
            pyautogui.moveTo(click_x, click_y, duration=0.4)
            time.sleep(0.5)
            shot2 = str(log_dir / f"turnstile_cursor_{ts}.png")
            driver.save_screenshot(shot2)
            log.info(f"  Cursor position screenshot: {shot2}")

            # Step 4: Click
            pyautogui.click()
            log.info("  Clicked Turnstile checkbox.")
            time.sleep(5)

    # Click sign-in button
    clicked = False
    for sel in ["button[type='submit']", "input[type='submit']"]:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            if btn.is_displayed():
                btn.click()
                clicked = True
                break
        except Exception:
            pass
    if not clicked:
        try:
            buttons = driver.find_elements(By.CSS_SELECTOR, "button")
            for b in buttons:
                if "sign in" in (b.text or "").lower():
                    b.click()
                    clicked = True
                    break
        except Exception:
            pass
    if not clicked:
        log.warning("  Could not find sign-in button.")
        return False

    time.sleep(3)

    # Handle Cloudflare after submission
    if is_cloudflare(driver):
        solve_cloudflare(driver)
        time.sleep(3)

    # Wait for redirect away from sign-in page
    for _ in range(15):
        if "signin" not in driver.current_url.lower():
            break
        if is_cloudflare(driver):
            solve_cloudflare(driver)
        time.sleep(1)

    if "signin" in driver.current_url.lower():
        log.warning("  LOGIN FAILED — still on sign-in page.")
        return False

    log.info(f"  LOGIN SUCCESS as {email}")
    return True


def do_logout(driver):
    """Log out of newspapers.com by clearing cookies. Returns True if logged out."""
    log.info("  Logging out of newspapers.com...")
    try:
        driver.delete_all_cookies()
        log.info("  Cookies cleared.")

        try:
            driver.uc_open_with_reconnect("https://www.newspapers.com/", 4)
        except Exception:
            driver.get("https://www.newspapers.com/")
        close_extra_tabs(driver)
        time.sleep(5)

        if is_cloudflare(driver):
            solve_cloudflare(driver)
            time.sleep(3)

        page_text = driver.execute_script(
            "return document.body.innerText || '';"
        ).lower()
        if "sign in" in page_text or "log in" in page_text:
            log.info("  Logged out successfully.")
            return True

        # Second attempt
        log.info("  Still appears logged in, clearing cookies again...")
        driver.delete_all_cookies()
        driver.execute_script(
            "window.localStorage.clear(); window.sessionStorage.clear();"
        )
        driver.get("https://www.newspapers.com/")
        time.sleep(5)
        if is_cloudflare(driver):
            solve_cloudflare(driver)
            time.sleep(3)

        page_text = driver.execute_script(
            "return document.body.innerText || '';"
        ).lower()
        if "sign in" in page_text or "log in" in page_text:
            log.info("  Logged out successfully.")
            return True

        log.warning("  Could not confirm logout.")
        return False
    except Exception as e:
        log.warning(f"  Logout error: {e}")
        return False


# ===========================================================================
# BrowserSession — the main entry point for consuming apps
# ===========================================================================

class BrowserSession:
    """Manages a browser session with account rotation for newspapers.com.

    Handles:
      - Browser setup with Cloudflare bypass
      - Login/logout
      - Account selection, rotation, and cooldown tracking
      - Throttle detection and account switching

    The calling app provides its own work loop. This class manages everything
    about getting and staying logged in.

    Example:
        session = BrowserSession(db_path="lake_worth.db",
                                 profile_dir="chrome_profile")
        driver = session.setup_driver(preferred_account="user@email.com")
        # ... do work ...
        if throttled:
            session.switch_account(driver)
        session.shutdown(driver)
    """

    def __init__(self, db_path, profile_dir):
        self.db_path = db_path
        self.profile_dir = str(profile_dir)
        self.current_email = None
        self.current_clips = 0

    # --- DB access ---

    def _get_db(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _get_daily_clip_limit(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            row = conn.execute(
                "SELECT value FROM clipper_state WHERE key = 'daily_clip_limit'"
            ).fetchone()
            conn.close()
            if row and row[0]:
                return int(row[0])
        except Exception:
            pass
        return 0

    # --- Account selection ---

    def get_next_account(self, exclude_email=None, exclude_emails=None):
        """Get the next active, eligible account."""
        clip_limit = self._get_daily_clip_limit() or 999999
        conn = self._get_db()
        try:
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            if "accounts" not in tables:
                return None

            sql = """SELECT * FROM accounts WHERE active = 1
                     AND (
                         clips_today < ?
                         OR clips_today IS NULL
                         OR last_clip_time IS NULL
                         OR last_clip_time < datetime('now','localtime','-24 hours')
                     )"""
            params = [clip_limit]
            if exclude_email:
                sql += " AND email != ?"
                params.append(exclude_email)
            # Exclude a list of emails (e.g. accounts over gathered URL limit)
            all_excluded = list(exclude_emails or [])
            if all_excluded:
                placeholders = ",".join("?" * len(all_excluded))
                sql += f" AND email NOT IN ({placeholders})"
                params.extend(all_excluded)
            sql += " ORDER BY clips_today ASC NULLS FIRST, total_clips ASC LIMIT 1"
            row = conn.execute(sql, params).fetchone()
            if row:
                return dict(row)
            log.warning("  All active accounts have hit the daily limit.")
            return None
        finally:
            conn.close()

    def get_all_active_accounts(self):
        """Get all active accounts ordered by preference."""
        conn = self._get_db()
        try:
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            if "accounts" not in tables:
                return []
            rows = conn.execute(
                "SELECT * FROM accounts WHERE active = 1 "
                "ORDER BY last_throttle_time ASC NULLS FIRST, total_clips ASC"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def claim_account(self, slot_id, pid, exclude_emails=None):
        """Atomically claim the next eligible account for a slot."""
        clip_limit = self._get_daily_clip_limit() or 999999
        conn = self._get_db()
        try:
            conn.isolation_level = None
            conn.execute("BEGIN IMMEDIATE")
            sql = """
                SELECT * FROM accounts
                 WHERE active = 1
                   AND in_use_by IS NULL
                   AND (
                       clips_today < ?
                       OR clips_today IS NULL
                       OR last_clip_time IS NULL
                       OR last_clip_time < datetime('now','localtime','-24 hours')
                   )
            """
            params = [clip_limit]
            all_excluded = list(exclude_emails or [])
            if all_excluded:
                placeholders = ",".join("?" * len(all_excluded))
                sql += f" AND email NOT IN ({placeholders})"
                params.extend(all_excluded)
            sql += " ORDER BY clips_today ASC NULLS FIRST, total_clips ASC LIMIT 1"
            row = conn.execute(sql, params).fetchone()
            if not row:
                conn.execute("COMMIT")
                return None
            now = _now_str()
            conn.execute(
                """UPDATE accounts
                      SET in_use_by = ?, in_use_since = ?, in_use_pid = ?
                    WHERE id = ?""",
                (slot_id, now, pid, row["id"]),
            )
            conn.execute("COMMIT")
            return dict(row)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def release_account(self, email, slot_id=None):
        """Clear the claim on an account."""
        if not email:
            return
        conn = self._get_db()
        try:
            if slot_id:
                conn.execute(
                    """UPDATE accounts
                          SET in_use_by = NULL, in_use_since = NULL, in_use_pid = NULL
                        WHERE email = ? AND in_use_by = ?""",
                    (email, slot_id),
                )
            else:
                conn.execute(
                    """UPDATE accounts
                          SET in_use_by = NULL, in_use_since = NULL, in_use_pid = NULL
                        WHERE email = ?""",
                    (email,),
                )
            _db_commit(conn)
        finally:
            conn.close()

    # --- Account stats ---

    def update_account_login(self, email):
        """Record login time and reset per-session counters."""
        conn = self._get_db()
        try:
            now = _now_str()
            conn.execute(
                """UPDATE accounts SET last_login_time = ?, updated_at = ?,
                                       clips_this_session = 0,
                                       articles_this_session = 0
                   WHERE email = ?""",
                (now, now, email),
            )
            _db_commit(conn)
        finally:
            conn.close()

    def update_account_logout(self, email):
        """Record logout time."""
        conn = self._get_db()
        try:
            now = _now_str()
            conn.execute(
                "UPDATE accounts SET last_logout_time = ?, updated_at = ? "
                "WHERE email = ?",
                (now, now, email),
            )
            _db_commit(conn)
        finally:
            conn.close()

    def update_account_stats(self, email, clips_added=0, articles_added=0,
                             throttled=False):
        """Update account statistics after clipping or throttle."""
        conn = self._get_db()
        try:
            now = _now_str()
            if clips_added > 0:
                # Reset clips_today only when last_clip_time is 24+ hours old
                conn.execute("""
                    UPDATE accounts SET clips_today = 0
                     WHERE email = ? AND last_clip_time IS NOT NULL
                       AND last_clip_time < datetime('now', 'localtime', '-24 hours')
                """, (email,))
                conn.execute("""
                    UPDATE accounts SET
                        total_clips = total_clips + ?,
                        clips_this_session = clips_this_session + ?,
                        clips_today = clips_today + ?,
                        last_clip_time = ?,
                        updated_at = ?
                    WHERE email = ?
                """, (clips_added, clips_added, clips_added, now, now, email))
            if articles_added > 0:
                conn.execute("""
                    UPDATE accounts SET
                        articles_this_session = articles_this_session + ?,
                        updated_at = ?
                    WHERE email = ?
                """, (articles_added, now, email))
            if throttled:
                row = conn.execute(
                    "SELECT throttle_count, avg_clips_before_throttle, "
                    "clips_this_session FROM accounts WHERE email = ?",
                    (email,),
                ).fetchone()
                if row:
                    old_count = row["throttle_count"] or 0
                    old_avg = row["avg_clips_before_throttle"] or 0
                    session_clips = row["clips_this_session"] or 0
                    new_count = old_count + 1
                    new_avg = (
                        ((old_avg * old_count) + session_clips) / new_count
                        if new_count > 0 else session_clips
                    )
                    conn.execute("""
                        UPDATE accounts SET
                            last_throttle_time = ?,
                            throttle_count = ?,
                            avg_clips_before_throttle = ?,
                            clips_this_session = 0,
                            articles_this_session = 0,
                            updated_at = ?
                        WHERE email = ?
                    """, (now, new_count, round(new_avg, 1), now, email))
            _db_commit(conn)
        finally:
            conn.close()

    # --- Browser lifecycle ---

    def setup_driver(self, preferred_account=None, exclude_emails=None):
        """Create browser, navigate to newspapers.com, handle Cloudflare,
        and log in. Returns the driver (logged in) or None on failure.

        If preferred_account is given, logs in as that account.
        Otherwise picks the next eligible account automatically.
        exclude_emails: list of emails to skip during account selection.
        """
        kill_chrome_for_profile(self.profile_dir)
        time.sleep(1)
        os.makedirs(self.profile_dir, exist_ok=True)
        clean_chrome_profile_locks(self.profile_dir)
        patch_chrome_preferences(self.profile_dir)

        driver = Driver(
            uc=True, headed=True,
            user_data_dir=self.profile_dir,
            chromium_arg="--disable-session-crashed-bubble",
        )
        driver.set_window_size(1920, 1080)
        try:
            driver.maximize_window()
        except Exception:
            pass
        driver.implicitly_wait(5)

        # Initial navigation with Cloudflare bypass
        driver.uc_open_with_reconnect(
            "https://star-telegram.newspapers.com/", 4
        )
        close_extra_tabs(driver)
        time.sleep(3)
        solve_cloudflare(driver)

        # Check login state
        page_text = driver.execute_script(
            "return document.body.innerText || '';"
        ).lower()

        if "sign in" in page_text or "log in" in page_text:
            # Not logged in — pick an account and login
            acct = None
            if preferred_account:
                conn = self._get_db()
                try:
                    row = conn.execute(
                        "SELECT * FROM accounts WHERE email = ? AND active = 1",
                        (preferred_account,),
                    ).fetchone()
                    if row:
                        acct = dict(row)
                    else:
                        log.warning(
                            f"Preferred account {preferred_account} "
                            f"not found or inactive."
                        )
                finally:
                    conn.close()
            if not acct:
                acct = self.get_next_account(exclude_emails=exclude_emails)

            if acct:
                log.info(f"Not logged in — logging in as {acct['email']}...")
                if do_login(driver, acct["email"], acct["password"]):
                    self.current_email = acct["email"]
                    self.current_clips = 0
                    self.update_account_login(acct["email"])
                    navigate(driver, "https://star-telegram.newspapers.com/")
                    time.sleep(2)
                else:
                    log.warning(
                        f"Login failed for {acct['email']}. "
                        f"Closing browser."
                    )
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    return None
            else:
                log.warning("No eligible accounts — closing browser.")
                try:
                    driver.quit()
                except Exception:
                    pass
                return None
        else:
            # Already logged in — verify which account
            log.info("Already logged in.")
            current_user = detect_logged_in_account(driver)
            target = preferred_account

            if not target:
                # Auto mode: check if current session is eligible
                _excluded_set = set(e.lower() for e in (exclude_emails or []))
                if current_user:
                    if current_user.lower() in _excluded_set:
                        log.info(
                            f"  Current session ({current_user}) "
                            f"is excluded — will switch."
                        )
                    else:
                        conn = self._get_db()
                        try:
                            row = conn.execute(
                                "SELECT * FROM accounts WHERE email = ? AND active = 1 "
                                "AND (last_throttle_time IS NULL "
                                "OR last_throttle_time < datetime('now', 'localtime', '-24 hours'))",
                                (current_user,),
                            ).fetchone()
                            if row:
                                log.info(f"  Current session ({current_user}) is eligible.")
                                target = current_user
                            else:
                                log.info(
                                    f"  Current session ({current_user}) "
                                    f"is in cooldown or inactive."
                                )
                        finally:
                            conn.close()

                if not target:
                    next_acct = self.get_next_account(exclude_emails=exclude_emails)
                    if next_acct:
                        log.info(
                            f"  Switching to eligible account: "
                            f"{next_acct['email']}..."
                        )
                        do_logout(driver)
                        time.sleep(2)
                        if do_login(driver, next_acct["email"],
                                    next_acct["password"]):
                            target = next_acct["email"]
                            self.update_account_login(target)
                            navigate(
                                driver,
                                "https://star-telegram.newspapers.com/"
                            )
                            time.sleep(2)
                        else:
                            log.warning(
                                f"  Could not login as {next_acct['email']}."
                            )
                    else:
                        log.warning("  No eligible accounts available.")

            elif preferred_account:
                if (current_user
                        and current_user.lower() != preferred_account.lower()):
                    log.info(
                        f"  Logged in as {current_user}, "
                        f"but need {preferred_account} — switching..."
                    )
                    do_logout(driver)
                    time.sleep(2)
                    conn = self._get_db()
                    try:
                        row = conn.execute(
                            "SELECT * FROM accounts "
                            "WHERE email = ? AND active = 1",
                            (preferred_account,),
                        ).fetchone()
                        if row:
                            if do_login(driver, row["email"], row["password"]):
                                target = preferred_account
                                self.update_account_login(target)
                                navigate(
                                    driver,
                                    "https://star-telegram.newspapers.com/"
                                )
                                time.sleep(2)
                            else:
                                log.warning(
                                    f"  Could not login as {preferred_account}."
                                )
                                target = current_user
                    finally:
                        conn.close()

            if target:
                self.current_email = target
                self.current_clips = 0
                log.info(f"  Tracking as account: {target}")

        return driver

    def switch_account(self, driver, exclude_email=None, exclude_emails=None):
        """Log out current account, mark it throttled, log into next available.
        Returns True if switched successfully."""
        if self.current_email:
            self.update_account_stats(self.current_email, throttled=True)
            self.update_account_logout(self.current_email)

        accounts = self.get_all_active_accounts()
        if not accounts:
            log.info("  No accounts configured — cannot switch.")
            return False

        exclude = exclude_email or self.current_email
        _excluded_set = set(e.lower() for e in (exclude_emails or []))
        if exclude:
            _excluded_set.add(exclude.lower())
        candidates = [a for a in accounts if a["email"].lower() not in _excluded_set]
        if not candidates:
            log.info("  No other active accounts available to switch to.")
            return False

        do_logout(driver)
        navigate(driver, "https://www.newspapers.com/")
        time.sleep(2)

        for acct in candidates:
            log.info(f"  Trying account: {acct['email']}")
            if do_login(driver, acct["email"], acct["password"]):
                self.current_email = acct["email"]
                self.current_clips = 0
                self.update_account_login(acct["email"])
                navigate(driver, "https://star-telegram.newspapers.com/")
                time.sleep(2)
                return True
            else:
                log.warning(f"  Failed to login as {acct['email']}, trying next...")
                do_logout(driver)

        log.warning("  All account login attempts failed.")
        return False

    def record_clip(self, email=None):
        """Record a clip for the current account. Call after each successful clip."""
        email = email or self.current_email
        if email:
            self.update_account_stats(email, clips_added=1)
            self.current_clips += 1

    def shutdown(self, driver):
        """Clean up: release account, quit browser."""
        if self.current_email:
            self.release_account(self.current_email)
            self.current_email = None
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
