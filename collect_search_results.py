"""
Collect search result URLs from newspapers.com.

Scrapes search result pages to harvest image URLs and metadata,
creating entries in the database for the clipper to process.
Does NOT visit individual result pages — extracts metadata from
the search results DOM directly.

Usage (called by dashboard server):
    python collect_search_results.py --start 1924-01-01 --end 1960-12-31 \
        --email acct@example.com --password pass123 --restart-every 100
"""

import argparse
import base64
import os
import re
import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timedelta

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# === CONFIGURATION ===
DB_PATH = r"c:\lake_worth\lake_worth.db"
LOG_DIR = r"c:\lake_worth\collector_logs"
RUNTIME_DIR = Path(r"c:\lake_worth_runtime")
PROFILE_DIR = RUNTIME_DIR / "chrome_temp_profile_collector"
STOP_FLAG = RUNTIME_DIR / "stop_collector"
SEARCH_TERM = "lake worth"
THUMB_DIR = Path(r"c:\lake_worth\thumbnails")

# === LOGGING ===
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(
    LOG_DIR, f"collector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("collector")

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


# === DATABASE ===

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def write_status(**kwargs):
    """Write collector status keys to clipper_state table."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        for key, value in kwargs.items():
            conn.execute(
                "INSERT OR REPLACE INTO clipper_state (key, value) VALUES (?, ?)",
                (f"collector_{key}", str(value)),
            )
        conn.commit()
    except Exception as e:
        log.warning(f"write_status failed: {e}")
    finally:
        conn.close()


def check_stop():
    return STOP_FLAG.exists()


def get_resume_date(conn, start_date):
    """Find the latest date we've already collected, or fall back to start_date."""
    row = conn.execute(
        "SELECT MAX(date_str) FROM processed_pdfs "
        "WHERE date_str IS NOT NULL AND search_term = ?",
        (SEARCH_TERM,),
    ).fetchone()
    latest = row[0] if row and row[0] else None
    if latest and latest >= start_date:
        return latest
    return start_date


def entry_exists(conn, pdf_filename):
    return conn.execute(
        "SELECT 1 FROM processed_pdfs WHERE pdf_filename = ?", (pdf_filename,)
    ).fetchone() is not None


def create_entry(conn, meta, thumbnail_path=None):
    """Create a processed_pdfs entry with metadata from search results."""
    conn.execute(
        "INSERT OR IGNORE INTO processed_pdfs "
        "(pdf_filename, articles_found, search_term, url, date_str, thumbnail_path) "
        "VALUES (?, 0, ?, ?, ?, ?)",
        (meta["pdf_filename"], SEARCH_TERM, meta["url"], meta["date_str"], thumbnail_path),
    )
    conn.commit()


def update_url(conn, pdf_filename, url):
    """Backfill the URL for an existing entry if missing."""
    if url:
        conn.execute(
            "UPDATE processed_pdfs SET url = ? "
            "WHERE pdf_filename = ? AND (url IS NULL OR url = '')",
            (url, pdf_filename),
        )
        conn.commit()


def reserve_account(email):
    """Mark an account as in-use by the collector."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "UPDATE accounts SET in_use_by = 'collector', in_use_since = ?, "
            "in_use_pid = ? WHERE email = ?",
            (now, os.getpid(), email),
        )
        conn.commit()
    finally:
        conn.close()


def release_account(email):
    """Release the collector's claim on an account."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute(
            "UPDATE accounts SET in_use_by = NULL, in_use_since = NULL, "
            "in_use_pid = NULL WHERE email = ? AND in_use_by = 'collector'",
            (email,),
        )
        conn.commit()
    finally:
        conn.close()


# === BROWSER ===

def _clean_profile_locks():
    """Remove Chrome singleton lock files that prevent a clean new browser launch."""
    for lock_name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        lock_path = PROFILE_DIR / lock_name
        try:
            if lock_path.exists():
                lock_path.unlink()
                log.info(f"  Removed stale lock: {lock_path}")
        except Exception as e:
            log.warning(f"  Could not remove {lock_path}: {e}")


def _patch_chrome_preferences():
    """Patch Chrome Preferences to prevent session restore / crash bubble."""
    import json as _json
    prefs_path = PROFILE_DIR / "Default" / "Preferences"
    prefs = {}
    if prefs_path.is_file():
        try:
            prefs = _json.loads(prefs_path.read_text(encoding="utf-8"))
        except Exception:
            prefs = {}
    prefs.setdefault("profile", {})
    prefs["profile"]["exit_type"] = "none"
    prefs["profile"]["exited_cleanly"] = True
    os.makedirs(prefs_path.parent, exist_ok=True)
    try:
        prefs_path.write_text(_json.dumps(prefs), encoding="utf-8")
        log.info("  Patched Chrome Preferences: exit_type=none, exited_cleanly=true")
    except Exception as e:
        log.warning(f"  Could not patch Chrome Preferences: {e}")


def _kill_chrome_for_profile():
    """Kill any Chrome processes using the collector profile directory."""
    import subprocess as _sp
    profile_str = str(PROFILE_DIR).replace("\\", "/").lower()
    try:
        out = _sp.run(
            ["wmic", "process", "where", "name='chrome.exe'", "get",
             "ProcessId,CommandLine", "/format:csv"],
            capture_output=True, text=True, timeout=10,
        )
        for line in (out.stdout or "").splitlines():
            if profile_str in line.lower() or str(PROFILE_DIR).lower() in line.lower():
                parts = line.strip().split(",")
                try:
                    pid = int(parts[-1])
                    os.kill(pid, 9)
                    log.info(f"  Killed stale Chrome PID {pid}")
                except Exception:
                    pass
    except Exception:
        pass


def setup_driver():
    # Kill any leftover Chrome using our profile
    _kill_chrome_for_profile()
    time.sleep(1)
    # Keep profile (like clipper) — just clean locks and patch prefs
    os.makedirs(PROFILE_DIR, exist_ok=True)
    _clean_profile_locks()
    _patch_chrome_preferences()
    driver = Driver(
        uc=True, headed=True,
        user_data_dir=str(PROFILE_DIR),
        chromium_arg="--disable-session-crashed-bubble",
    )
    try:
        driver.set_window_size(1920, 1080)
    except Exception:
        pass
    try:
        driver.maximize_window()
    except Exception:
        pass
    driver.implicitly_wait(5)
    # Initial navigation — match clipper: go to star-telegram subdomain
    driver.uc_open_with_reconnect("https://star-telegram.newspapers.com/", 4)
    _close_extra_tabs(driver)
    time.sleep(3)
    # Always attempt cloudflare solve (like clipper)
    _solve_cloudflare(driver)
    return driver


def _is_cloudflare(driver):
    """Check if the current page is a Cloudflare challenge."""
    try:
        page_text = driver.execute_script(
            "return document.body ? document.body.innerText : '';"
        ).lower()
        return ("security verification" in page_text
                or "checking if the site connection is secure" in page_text
                or "just a moment" in page_text)
    except Exception:
        return False


def _detect_logged_in_account(driver, expected_email=None):
    """Verify login by checking the account page for an email address."""
    try:
        driver.execute_script(
            "window.location.href = 'https://www.newspapers.com/account/';"
        )
        time.sleep(3)
        if _is_cloudflare(driver):
            _solve_cloudflare(driver)
            time.sleep(2)
        page_text = driver.execute_script(
            "return document.body ? document.body.innerText : '';"
        )
        emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', page_text)
        if emails:
            log.info(f"  Detected logged-in account: {emails[0]}")
            return emails[0]
        log.info("  Could not detect logged-in account from page text.")
    except Exception as e:
        log.warning(f"  Error detecting account: {e}")
    return None


def _solve_cloudflare(driver):
    """Attempt to solve Cloudflare challenge."""
    for attempt in range(3):
        try:
            driver.uc_gui_click_captcha()
            time.sleep(5)
            if not _is_cloudflare(driver):
                return True
        except Exception:
            time.sleep(2)
    return False


def _close_extra_tabs(driver):
    """Close any extra tabs, keeping only the current one."""
    try:
        handles = driver.window_handles
        if len(handles) > 1:
            current = driver.current_window_handle
            for h in handles:
                if h != current:
                    driver.switch_to.window(h)
                    driver.close()
            driver.switch_to.window(current)
    except Exception:
        pass


def do_login(driver, email, password):
    """Login to newspapers.com. Matches the proven clipper login flow."""
    log.info(f"  Logging in as {email}...")

    # Navigate to sign-in page
    try:
        driver.uc_open_with_reconnect(
            "https://www.newspapers.com/signin/", 4
        )
    except Exception:
        driver.execute_script(
            "window.location.href = 'https://www.newspapers.com/signin/';"
        )
    _close_extra_tabs(driver)
    time.sleep(5)

    # Dismiss subscription nag modal if present
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
                if driver.execute_script(nag_js):
                    log.info("  Dismissed subscription nag modal.")
                    time.sleep(2)
                    break
            except Exception:
                pass
            time.sleep(1)
    except Exception:
        pass

    # Handle Cloudflare challenge
    if _is_cloudflare(driver):
        log.info("  Cloudflare challenge on sign-in — solving...")
        _solve_cloudflare(driver)
        time.sleep(3)

    # Wait for login form
    has_form = False
    for _ in range(15):
        if driver.find_elements(By.CSS_SELECTOR,
                "input[type='password'], input[name='email']"):
            has_form = True
            break
        if _is_cloudflare(driver):
            _solve_cloudflare(driver)
        time.sleep(1)

    if not has_form:
        page_text = driver.execute_script(
            "return document.body ? document.body.innerText : '';"
        ).lower()
        if "sign in" not in page_text and "log in" not in page_text:
            # Verify actually logged in by checking account page
            detected = _detect_logged_in_account(driver, email)
            if detected:
                log.info(f"  Already logged in as {detected}.")
                return True
            log.warning("  Not actually logged in — no account detected.")
            return False
        log.warning("  No login form found.")
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

    # Solve Cloudflare Turnstile on the login form (second gate)
    log.info("  Solving Turnstile on login form...")
    time.sleep(2)
    turnstile_selectors = [
        "iframe[src*='challenges.cloudflare.com']",
        "iframe[src*='turnstile']",
        "iframe[title*='Turnstile' i]",
        "iframe[title*='challenge' i]",
        "div.cf-turnstile",
        "div[class*='turnstile']",
    ]
    turnstile_detected = False
    for sel in turnstile_selectors:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                try:
                    if el.is_displayed():
                        turnstile_detected = True
                        break
                except Exception:
                    continue
            if turnstile_detected:
                break
        except Exception:
            continue
    log.info(f"  Turnstile widget detected by selectors: {turnstile_detected}")
    if turnstile_detected:
        try:
            driver.uc_gui_click_captcha()
            time.sleep(5)
            log.info("  Turnstile click done.")
        except Exception as e:
            log.info(f"  Turnstile click attempt: {e}")
    else:
        log.info("  No Turnstile widget visible — skipping captcha click.")

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
    if _is_cloudflare(driver):
        _solve_cloudflare(driver)
        time.sleep(3)

    # Wait for redirect away from sign-in page
    for _ in range(15):
        if "signin" not in driver.current_url.lower():
            break
        if _is_cloudflare(driver):
            _solve_cloudflare(driver)
        time.sleep(1)

    if "signin" in driver.current_url.lower():
        log.warning("  LOGIN FAILED — still on sign-in page.")
        return False

    log.info(f"  Login successful as {email}.")
    return True


# === SEARCH RESULTS SCRAPING ===

def build_search_url(start_date, end_date):
    return (
        "https://star-telegram.newspapers.com/search/results/"
        f"?date-end={end_date}&date-start={start_date}"
        f"&keyword=%22lake+worth%22"
        "&sort=paper-date-asc"
    )


def scrape_results(driver):
    """Extract URLs and metadata from visible search results.

    Uses JavaScript to find all image links and extract text from their
    parent result cards, without navigating to individual pages.
    """
    data = driver.execute_script("""
        var results = [];
        var links = document.querySelectorAll('a[href*="/image/"]');
        var seen = {};
        // Build thumbnail lookup: image_id -> thumbnail src
        var thumbLookup = {};
        var imgs = document.querySelectorAll('img[src*="/img/thumbnail/"]');
        imgs.forEach(function(img) {
            var src = img.getAttribute('src') || '';
            var m = src.match(/\\/img\\/thumbnail\\/(\\d+)\\//);
            if (m) thumbLookup[m[1]] = src;
        });
        links.forEach(function(link) {
            var href = link.getAttribute('href');
            if (!href || seen[href]) return;
            // Normalize relative URLs
            if (href.startsWith('/')) href = window.location.origin + href;
            seen[href] = true;
            // Walk up DOM to find a container with meaningful text
            var el = link;
            for (var i = 0; i < 6; i++) {
                if (el.parentElement) el = el.parentElement;
                var txt = el.innerText || '';
                if (txt.length > 30) break;
            }
            // Find thumbnail URL for this result
            var imgMatch = href.match(/\\/image\\/(\\d+)/);
            var thumbUrl = imgMatch ? (thumbLookup[imgMatch[1]] || '') : '';
            results.push({url: href, text: (el.innerText || '').substring(0, 500), thumb_url: thumbUrl});
        });
        return results;
    """)
    return data or []


def parse_result(item):
    """Parse a search result item into metadata."""
    url = item.get("url", "")
    text = item.get("text", "")

    # Parse date: "Apr 15, 1924" or "April 15, 1924"
    date_str = ""
    date_match = re.search(r'(\w{3,9})\s+(\d{1,2}),?\s+(\d{4})', text)
    if date_match:
        month_str = date_match.group(1)[:3].lower()
        month = MONTH_MAP.get(month_str, "")
        if month:
            day = date_match.group(2).zfill(2)
            year = date_match.group(3)
            date_str = f"{year}-{month}-{day}"

    # Parse page number
    page_match = re.search(r'page\s*(\d+)', text, re.IGNORECASE)
    page = int(page_match.group(1)) if page_match else 0

    # Parse newspaper name
    newspaper = "Fort Worth Star Telegram"
    np_match = re.search(
        r'(Fort Worth (?:Star-Telegram|Record-Telegram|Record Telegram|'
        r'Star Telegram|Record))',
        text, re.IGNORECASE,
    )
    if np_match:
        newspaper = np_match.group(1)

    # Build pdf_filename consistent with clipper naming
    paper_clean = re.sub(r'[^a-zA-Z0-9]+', '_', newspaper).strip('_')
    if date_str and page:
        pdf_filename = f"{paper_clean}_{date_str.replace('-', '_')}_{page}.pdf"
    else:
        img_match = re.search(r'/image/(\d+)', url)
        img_id = img_match.group(1) if img_match else str(int(time.time()))
        pdf_filename = f"{paper_clean}_{img_id}.pdf"

    return {
        "url": url,
        "newspaper": newspaper,
        "date_str": date_str,
        "page": page,
        "pdf_filename": pdf_filename,
        "thumb_url": item.get("thumb_url", ""),
    }


def click_show_more(driver):
    """Try to click a 'Show More' / 'Load More' button."""
    try:
        for tag in ["button", "a", "[role='button']"]:
            elements = driver.find_elements(By.CSS_SELECTOR, tag)
            for el in elements:
                try:
                    text = el.text.strip().lower()
                    if "show more" in text or "load more" in text:
                        if el.is_displayed() and el.is_enabled():
                            driver.execute_script(
                                "arguments[0].scrollIntoView({block:'center'});",
                                el,
                            )
                            time.sleep(1)
                            el.click()
                            log.info("    Clicked 'Show More'")
                            time.sleep(3)
                            return True
                except Exception:
                    continue
    except Exception:
        pass
    return False


def scroll_for_more(driver, prev_count):
    """Scroll down / Page Down to trigger lazy loading. Returns new result count."""
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(1)
    # Also send Page Down a few times
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        for _ in range(3):
            body.send_keys(Keys.PAGE_DOWN)
            time.sleep(0.5)
    except Exception:
        pass
    time.sleep(2)
    new_results = scrape_results(driver)
    return len(new_results)


# === SESSION ===

def download_thumbnails(driver, entries):
    """Download thumbnail images via browser JS fetch. Returns dict: pdf_filename -> relative path.
    entries: list of {pdf_filename, thumb_url}
    Must be called while browser is still on the search results page.
    """
    THUMB_DIR.mkdir(parents=True, exist_ok=True)
    to_fetch = [(e["pdf_filename"], e["thumb_url"]) for e in entries if e.get("thumb_url")]
    if not to_fetch:
        return {}

    url_list = [url for _, url in to_fetch]
    try:
        b64_list = driver.execute_async_script("""
            const urls = arguments[0];
            const callback = arguments[arguments.length - 1];
            (async () => {
                const results = [];
                for (let i = 0; i < urls.length; i++) {
                    try {
                        const resp = await fetch(urls[i]);
                        const blob = await resp.blob();
                        const b64 = await new Promise(resolve => {
                            const reader = new FileReader();
                            reader.onloadend = () => resolve(reader.result.split(',')[1]);
                            reader.readAsDataURL(blob);
                        });
                        results.push(b64);
                    } catch(e) {
                        results.push('');
                    }
                }
                callback(results);
            })();
        """, url_list)
    except Exception as e:
        log.warning(f"  Thumbnail batch download failed: {e}")
        return {}

    results = {}
    for i, (fname, thumb_url) in enumerate(to_fetch):
        b64_data = b64_list[i] if i < len(b64_list) else ""
        if b64_data:
            try:
                img_bytes = base64.b64decode(b64_data)
                if len(img_bytes) > 100:
                    out_fname = fname.replace(".pdf", ".jpg")
                    fpath = THUMB_DIR / out_fname
                    fpath.write_bytes(img_bytes)
                    results[fname] = f"thumbnails/{out_fname}"
            except Exception as e:
                log.warning(f"  Thumbnail save failed for {fname}: {e}")
    return results


def run_session(conn, driver, start_date, end_date, restart_every):
    """Collect URLs from search results. Returns (new_entries, latest_date).

    Stays on the search results page, scrolling/loading more results.
    Restarts after restart_every new entries.
    """
    search_url = build_search_url(start_date, end_date)
    log.info(f"  Search: {start_date} to {end_date}")
    log.info(f"  URL: {search_url}")

    _close_extra_tabs(driver)
    try:
        driver.get(search_url)
    except Exception as e:
        log.warning(f"  driver.get failed ({e}), retrying with JS navigation...")
        _close_extra_tabs(driver)
        try:
            driver.execute_script(f"window.location.href = '{search_url}';")
        except Exception as e2:
            log.error(f"  JS navigation also failed: {e2}")
            raise
    time.sleep(4)

    seen_urls = set()
    new_entries = 0
    skipped = 0
    latest_date = start_date
    no_new_rounds = 0
    zero_new_batches = 0  # consecutive batches with results but 0 new DB entries

    while True:
        if check_stop():
            log.info("  Stop flag detected.")
            break

        results = scrape_results(driver)
        new_results = [r for r in results if r["url"] not in seen_urls]

        if not new_results:
            # Try loading more
            prev_count = len(results)
            new_count = scroll_for_more(driver, prev_count)
            if new_count > prev_count:
                continue
            if click_show_more(driver):
                continue
            no_new_rounds += 1
            if no_new_rounds >= 3:
                log.info("  No more results to load.")
                break
            continue

        no_new_rounds = 0
        batch_new = 0

        # First pass: parse results and collect new entries needing thumbnails
        new_metas = []
        for item in new_results:
            seen_urls.add(item["url"])
            meta = parse_result(item)

            if not meta["date_str"]:
                skipped += 1
                continue

            if meta["date_str"] > latest_date:
                latest_date = meta["date_str"]

            if entry_exists(conn, meta["pdf_filename"]):
                update_url(conn, meta["pdf_filename"], meta["url"])
                skipped += 1
                continue

            new_metas.append(meta)

        # Download thumbnails for new entries (while still on search page)
        thumb_paths = {}
        if new_metas:
            thumb_paths = download_thumbnails(driver, new_metas)

        # Save entries with thumbnail paths
        for meta in new_metas:
            thumb = thumb_paths.get(meta["pdf_filename"])
            create_entry(conn, meta, thumbnail_path=thumb)
            new_entries += 1
            batch_new += 1

            if new_entries % 50 == 0:
                write_status(
                    new_entries=new_entries,
                    current_date=latest_date,
                    status="running",
                )
                log.info(
                    f"  Progress: {new_entries} new, {skipped} skipped, "
                    f"at {latest_date}"
                )

            if restart_every and new_entries % restart_every == 0:
                log.info(
                    f"  Restart threshold ({restart_every}) — "
                    f"restarting from {latest_date}"
                )
                return new_entries, latest_date

        log.info(
            f"  Batch: {batch_new} new, {len(new_results) - batch_new} skipped"
        )

        # Throttle detection: many results but nothing new to save
        if batch_new == 0 and len(new_results) > 0:
            zero_new_batches += 1
            if zero_new_batches >= 5:
                log.error(
                    f"  THROTTLE DETECTED: {zero_new_batches} consecutive batches "
                    f"with 0 new entries. Stopping."
                )
                write_status(status="throttled")
                break
        else:
            zero_new_batches = 0

        # Scroll to load more results
        scroll_for_more(driver, len(results))

    return new_entries, latest_date


# === MAIN ===

def main():
    parser = argparse.ArgumentParser(
        description="Collect search result URLs from newspapers.com"
    )
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--email", required=True, help="Account email")
    parser.add_argument("--password", required=True, help="Account password")
    parser.add_argument(
        "--restart-every", type=int, default=100,
        help="Restart browser every N new entries",
    )
    parser.add_argument(
        "--max-urls", type=int, default=1500,
        help="Stop after this many new URLs (switch account)",
    )
    args = parser.parse_args()

    # Clear stop flag if leftover
    try:
        if STOP_FLAG.exists():
            if STOP_FLAG.is_dir():
                STOP_FLAG.rmdir()
            else:
                STOP_FLAG.unlink()
    except Exception:
        pass

    log.info("=" * 60)
    log.info("URL Collector")
    log.info("=" * 60)
    log.info(f"  Date range: {args.start} to {args.end}")
    log.info(f"  Account: {args.email}")
    log.info(f"  Restart every: {args.restart_every}")
    log.info(f"  Max URLs: {args.max_urls}")
    log.info(f"  Log: {log_filename}")

    reserve_account(args.email)
    write_status(
        status="running",
        pid=os.getpid(),
        account=args.email,
        date_start=args.start,
        date_end=args.end,
        restart_every=args.restart_every,
        new_entries=0,
        current_date=args.start,
    )

    conn = get_db()
    total_new = 0
    session_num = 0
    consecutive_failures = 0
    driver = None

    try:
        while True:
            if check_stop():
                log.info("Stop flag set — exiting.")
                break

            session_num += 1
            resume_date = get_resume_date(conn, args.start)
            if resume_date > args.end:
                log.info(f"  Past end date ({args.end}). Done.")
                break

            log.info(f"\n>>> SESSION {session_num} (from {resume_date})")

            # Only create a new browser if we don't have one
            if driver is None:
                try:
                    driver = setup_driver()
                    if not do_login(driver, args.email, args.password):
                        log.error("  Login failed.")
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            log.error("  3 consecutive login failures. Stopping.")
                            break
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = None
                        time.sleep(10)
                        continue
                except Exception as e:
                    log.error(f"  Browser setup error: {e}", exc_info=True)
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        log.error("  3 consecutive failures. Stopping.")
                        break
                    if driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = None
                    time.sleep(10)
                    continue

            try:
                consecutive_failures = 0
                new, latest = run_session(
                    conn, driver, resume_date, args.end, args.restart_every
                )
                total_new += new

                write_status(new_entries=total_new, current_date=latest)
                log.info(
                    f"  Session {session_num}: {new} new "
                    f"(total: {total_new}, at {latest})"
                )

                # Check URL limit
                if args.max_urls and total_new >= args.max_urls:
                    log.info(
                        f"  URL LIMIT REACHED: {total_new} >= {args.max_urls}. "
                        f"Switch account and restart."
                    )
                    write_status(status="limit_reached", new_entries=total_new)
                    break

                if new == 0:
                    # Advance past current date to avoid getting stuck
                    try:
                        next_dt = datetime.strptime(
                            latest, "%Y-%m-%d"
                        ) + timedelta(days=1)
                        next_date = next_dt.strftime("%Y-%m-%d")
                        if next_date <= args.end:
                            log.info(f"  No new results. Advancing to {next_date}.")
                            consecutive_failures += 1
                            if consecutive_failures >= 5:
                                log.info("  5 sessions with no new results. Done.")
                                break
                        else:
                            log.info("  Past end date. Done.")
                            break
                    except Exception:
                        break
                else:
                    consecutive_failures = 0

                # Restart threshold hit — reuse browser, just loop back
                if new > 0:
                    log.info("  Continuing with same browser session...")
                    time.sleep(2)

            except Exception as e:
                log.error(f"  Session error: {e}", exc_info=True)
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    log.error("  3 consecutive failures. Stopping.")
                    break
                # Kill broken browser, will recreate next loop
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = None
                log.info("  Restarting browser in 3 seconds...")
                time.sleep(3)

    except KeyboardInterrupt:
        log.info("\nStopped by user.")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        write_status(status="stopped", new_entries=total_new)
        release_account(args.email)
        conn.close()

    log.info("=" * 60)
    log.info("DONE")
    log.info(f"  Sessions: {session_num}")
    log.info(f"  Total new URLs: {total_new}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
